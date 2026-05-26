**Root-Cause Confirmations / Corrections**

**Bug A: confirmed, with one important expansion.**  
`_on_folder_selection_changed()` calls `_unified_tree.populate_for_folder()` directly on the Qt UI thread, and that recursively calls `_populate_node()` with `os.listdir`, `os.path.isdir`, `os.path.isfile`, `_canonical_filepath()`, `QTreeWidgetItem` creation, and `expandAll()` in the same call path. The traceback is consistent with that.

Pushback: there is no evidence that `Path.resolve()` is the dominant cost. One traceback stops in `Path.resolve()`, the other in `os.path.isfile()`. The real bug is the whole eager UI-thread traversal plus widget construction. Moving only `_canonical_filepath()` out of the walk would reduce some Windows pain but would not fix the freeze.

Also worse than the brief says: `_populate_node()` uses `os.path.isdir(full)`, which follows symlinks/junctions, unlike the indexer’s `os.walk(..., followlinks=False)`. That can walk outside the selected tree or recurse badly on junction-heavy folders.

**Bug B: mostly confirmed, but the reported disable is probably two causes, not one.**  
The `start_recovery_probe()` check inside `_update_reset_button_state()` is wrong. Reset is exactly the recovery escape hatch for stale `scan_runs.status='running'` rows, so stale rows must not disable it.

However, in the user’s restart flow the immediate disable is likely also caused by `_auto_rescan_on_startup()` starting a new worker after Skip. While that worker is running, disabling Reset is correct. So fix B alone may not make Reset available after Skip unless Bug D is also fixed.

There is no tiny ordering hole for a single run: `_end_scan_run()` commits synchronously before `_show_recovery_modal()` returns. But `_show_recovery_modal()` only handles `running_runs[0]`, and `start_recovery_probe()` has no `ORDER BY`, so multiple orphan rows can remain visible.

**Bug C: confirmed as likely benign stderr noise.**  
`shared/local_indexer.py` imports `fitz` and extracts every PDF page through PyMuPDF. The warnings are not handled by repo code. They are noisy, probably page-correlated, and not evidence of a Phase 97.2 regression.

**Bug D: correct mechanism, wrong severity.**  
Yes, Skip is followed by `_auto_rescan_on_startup()`, which starts a new scan if folders exist. I would not classify this as “not a bug.” In the crash-recovery context, Skip should return control to the user, not immediately re-enter the same heavy folder and disable Reset.

**Recommended Fix Sequence**

| Bug | Fix | Files touched | Risk | Est. commits |
|---|---|---:|---|---:|
| B + D | Remove orphan-probe reset guard; suppress startup auto-rescan after recovery Skip; add explicit status text for Restart/Resume fresh scan | `desktop/my_library_tab.py`, tests | Low | 1 |
| A | Replace synchronous tree population with cancellable worker + batched UI insertion; stop following symlink dirs; avoid `expandAll()` on large trees | `desktop/my_library_tab.py`, tests | Medium-high | 2-3 |
| A adjunct | Add scan enumeration progress or “Discovering files…” state before indexing progress starts | `shared/local_indexer.py`, `desktop/my_library_tab.py` | Medium | 1 |
| C | Suppress MuPDF console warnings conditionally; log warning summaries at debug/info | `shared/local_indexer.py` | Low | 1 |
| Near-misses | Handle all recovery run IDs, not only first; align UI supported extensions with indexer | `desktop/my_library_tab.py`, tests | Low-medium | 1 |

**Bug A Architecture Recommendation**

Use option **(a)**, but reuse the existing `FolderWalkWorker` idea rather than the current implementation as-is. `QFileSystemModel` is not a good fit because the app needs opt-out state, canonical path keys, prior status overlays, and exact `_leaf_by_path` mapping. Option (d) is insufficient.

Concrete pattern:

```python
@dataclass(frozen=True)
class FolderTreeRow:
    canonical: str
    rel_parts: tuple[str, ...]
    pages: int
    status: str
    checked: bool


class FolderTreeBuildWorker(QThread):
    batch_ready = pyqtSignal(int, list)
    finished_signal = pyqtSignal(int, int)
    error_signal = pyqtSignal(int, str)

    def __init__(self, token, folder_path, optouts, prior_status):
        super().__init__()
        self._token = token
        self._folder_path = folder_path
        self._optouts = set(optouts)
        self._prior_status = dict(prior_status)
        self._cancel = False

    def cancel(self):
        self._cancel = True

    def run(self):
        # Use os.scandir stack, not recursive QWidget mutation.
        # entry.is_dir(follow_symlinks=False)
        # entry.is_file(follow_symlinks=False)
        # canonicalize only supported file leaves, off the UI thread.
        # emit batches of FolderTreeRow-like tuples.
        ...
```

UI side:

```python
def populate_for_folder(self, folder_path: str, prior_status: dict = None):
    self._cancel_existing_tree_worker()
    self._tree_token += 1
    token = self._tree_token

    self.clear()
    self._displayed_paths = set()
    self._leaf_by_path = {}

    prior_status = prior_status or self._load_prior_status(folder_path)
    optouts = getattr(self._app, "_local_file_optouts", set())

    self._tree_worker = FolderTreeBuildWorker(token, folder_path, optouts, prior_status)
    self._tree_worker.batch_ready.connect(self._on_tree_batch)
    self._tree_worker.finished_signal.connect(self._on_tree_finished)
    self._tree_worker.error_signal.connect(self._on_tree_error)
    self._tree_worker.start()
```

`_on_tree_batch()` should be the only place that creates `QTreeWidgetItem`s. It should ignore stale tokens, create parent folder nodes from `rel_parts`, populate `_displayed_paths` and `_leaf_by_path`, and keep `_suppress_signals=True` while applying batches. Do not call `expandAll()` for large trees; expand root or recently selected path only.

**Bug B Corrected Guard**

```python
def _update_reset_button_state(self) -> None:
    if not hasattr(self, "_btn_reset") or self._btn_reset is None:
        return

    if self._indexer is None:
        self._btn_reset.setEnabled(False)
        self._btn_reset.setToolTip(tr("Local index unavailable"))
        return

    worker_running = self._worker is not None and self._worker.isRunning()
    if worker_running:
        self._btn_reset.setEnabled(False)
        self._btn_reset.setToolTip(tr("Stop the active scan before resetting My Library"))
        return

    self._btn_reset.setEnabled(True)
    self._btn_reset.setToolTip(
        tr("Reset deletes LOCAL/LAB index data only. Source files and Genizah corpus are preserved.")
    )
```

Do not probe `scan_runs` here. An external Tantivy lock should be handled by `reset_my_library()` itself. A guard that tries to acquire a writer would be side-effectful and can create the same lock class of bugs. The reset path already closes internal handles, asks the engine to close searchers, retries Windows rename, and fails loud if another process is holding the directory.

**Bug C + D Answers**

For Bug C: use conditional PyMuPDF suppression, not global stderr capture.

```python
try:
    fitz.TOOLS.mupdf_display_warnings(False)
except Exception:
    pass
```

Then optionally call `fitz.TOOLS.mupdf_warnings()` after a PDF and log a one-line debug summary. Redirecting `stderr` process-wide is too blunt and can hide unrelated failures.

For Bug D: Skip should suppress the same-launch auto-rescan. Add a flag from `_show_recovery_modal()`:

```python
self._skip_startup_rescan_once = clicked is btn_skip
```

and in `_auto_rescan_on_startup()`:

```python
if getattr(self, "_skip_startup_rescan_once", False):
    self._show_status_message(tr("Recovery skipped. Use Refresh to rescan My Library."))
    return
```

Restart can intentionally allow the fresh scan, but it should say so. Resume is currently misleading because it marks the old run completed and relies on a fresh scan; either implement real resume later or relabel the action.

**Anything Else Noticed**

`FolderWalkWorker` already exists and has tests, but nothing wires it into `_UnifiedFileTreeWidget`. That is partial infrastructure, not a fix.

`prescan_count_all()` is synchronous on the UI thread during manual Refresh. Large registered libraries can freeze before the worker starts. Add an aggregate `PrescanWorker` or skip the aggregate prescan when startup/refresh is already protected by worker UX.

`scan_all()` builds the full `disk_files` dict before emitting indexing progress. That explains “0%” during large discovery. The worker is alive, but the UI does not say “Discovering files.”

`_show_recovery_modal()` handles only one `running_runs[0]` and assumes it is the most recent, but `start_recovery_probe()` has no ordering. Handle all rows or order by `started_at DESC`.

The UI tree supports only `.pdf/.docx/.txt`, while `_SUPPORTED_EXTENSIONS` now includes `.html/.xlsx/.csv`. That means newer indexed formats can be invisible or unmanageable in the opt-out/status UI.

**Final Estimate**

Phase 97.3 cleanup: **5-7 commits, roughly 1.5-2.5 engineering days**.

Fast hotfix: Bug B + D + C in **2-4 hours** with tests.  
Full UAT-grade cleanup including Bug A workerized tree, progress states, recovery-row cleanup, and extension parity: **10-18 hours**, depending on how much UI test coverage you want around Qt worker cancellation and large-tree batching.