# Codex Brief — Post-97.2 UAT Bugs

**Date:** 2026-05-26
**Branch:** `phase-98-nli-resilience` (Phase 97.2 just shipped + Phase 98 shipped 2026-05-25)
**Requesting deep analysis from:** Codex
**Returning artifact:** `.planning/debug/post-97.2-uat-bugs-codex-critique.md`

---

## What just shipped (so you have causal context)

**Phase 97.2 (commits `a93322b2..2d7f7d7a`, 25 commits)** landed today as an internal hotfix on top of the v7.14 "My Library" milestone:

- **Plan 97.2-01** — 8 LOCAL recovery-cascade fixes in `shared/local_indexer.py` + `genizah_core.py`:
  - R97.2-F: schema-marker absence triggers rebuild (was crashing with "Schema error")
  - R97.2-A: deleted a redundant `tantivy.Index` reopen leaking a writer lock; temp indexer now closed via `_close_internal_writer_index()`
  - R97.2-B: explicit `_index = None` + `gc.collect()` at the rebuild_main_index_atomic swap point
  - R97.2-C: `discard_run` step 2 detects Phase 95 schema (no `scan_run_id` field) via try/except and falls back to per-uid `delete_documents("unique_id", uid)` loop
  - R97.2-G: `_del_writer = None` + `gc.collect()` between discard_run step 2 and step 5
  - R97.2-D: new `LocalIndexerError(RuntimeError)` class + `_ensure_writer()` helper raising `LocalIndexerError` on schema mismatch or `LockBusy` with NO silent retry, wired at 4 call sites (`_delete_file`, `remove_folder`, `_recover_pending_deletes`)
  - R97.2-H: short-circuit SQLite delete if Tantivy delete failed (raises `LocalIndexerError` BEFORE the SQLite transaction)
- **Plan 97.2-02** — new "Reset My Library" / "אפס ספריה שלי" toolbar button in `desktop/my_library_tab.py`:
  - `_btn_reset` + `_on_reset_clicked` + `_show_reset_confirm_dialog` (custom QDialog with typed `RESET`/`אפס` confirmation, NOT QMessageBox) + `_perform_reset` + `_update_reset_button_state` proactive guard
  - `LocalIndexer.reset_my_library()` 7-step teardown (close handles → path-safety pre-check → rename-aside LOCAL+LAB with rollback → recreate → deferred GC via `pending_dir_cleanup` → `self.__init__()` migration ladder → `reload_searcher_cb()`)
- **Plan 97.2-03** — docs only (CHANGELOG, OPEN_ISSUES, CLAUDE.md, 97-VERIFICATION.md). No version bump, no GitHub release (internal hotfix).

Verification: all 5 RED tests GREEN, REVIEWS HIGH #3/#5/#6 closed, no version bump.

**Phase 97 (parent) and Phase 96 (My Library feature) preceded this.** The freeze trace lands on `desktop/my_library_tab.py:213/244/254` which is **Phase 96 D-F1 territory**, not Phase 97.2 territory.

---

## User UAT report (verbatim)

User started the desktop app and added a small folder. Console printed `MuPDF error: syntax error: unknown keyword: 'TF'` **624 times** (probably one per PDF page).

Then user tried to add a mega-heavy folder. **Stuck at 0%. Cancel froze the window.** Force-closing the app produced this traceback:

```
Traceback (most recent call last):
  File "C:\GenizahSearch\desktop\my_library_tab.py", line 981, in _on_folder_selection_changed
    self._unified_tree.populate_for_folder(selected_path)
  File "C:\GenizahSearch\desktop\my_library_tab.py", line 213, in populate_for_folder
    self._populate_node(root_item, folder_path, optouts, prior_status)
  File "C:\GenizahSearch\desktop\my_library_tab.py", line 244, in _populate_node
    self._populate_node(sub, full, optouts, prior_status)
  [Previous line repeated 3 more times]
  File "C:\GenizahSearch\desktop\my_library_tab.py", line 254, in _populate_node
    canonical = _canonical_filepath(full)
  File "C:\GenizahSearch\shared\local_sys_id.py", line 100, in _canonical_filepath
    resolved = Path(p).resolve(strict=False)
  File "<frozen ntpath>", line 723, in realpath
KeyboardInterrupt
```

(Stack repeats — both threads of Python were stuck in the same recursive walk when SIGINT hit.)

**Restart attempt:** App took **~40s to start up**. Progress bar at **70%** even though user clicked **"Skip"** in the recovery dialog. **Reset button was unavailable.** Clicking on the library name froze again with a similar but distinct trace:

```
File "C:\GenizahSearch\desktop\my_library_tab.py", line 981, in _on_folder_selection_changed
    self._unified_tree.populate_for_folder(selected_path)
File "C:\GenizahSearch\desktop\my_library_tab.py", line 213, in populate_for_folder
    self._populate_node(root_item, folder_path, optouts, prior_status)
File "C:\GenizahSearch\desktop\my_library_tab.py", line 244, in _populate_node
    self._populate_node(sub, full, optouts, prior_status)
File "C:\GenizahSearch\desktop\my_library_tab.py", line 248, in _populate_node
    if not os.path.isfile(full):
File "<frozen genericpath>", line 30, in isfile
KeyboardInterrupt
```

---

## Triage so far (my best read — please challenge)

### Bug A — UI-thread synchronous recursive filesystem walk (CRITICAL, pre-existing from Phase 96 D-F1, NOT Phase 97.2)

`_on_folder_selection_changed` (line 968, a Qt slot on the main thread) calls `populate_for_folder` → `_populate_node` recursively. Every directory and every file goes through `os.listdir` + `os.path.isfile` + `_canonical_filepath` (which does `Path(p).resolve(strict=False)` → `os.path.realpath()`) on the main thread. For a mega folder this freezes the UI for minutes. Cancel is also on the main thread, so the cancel button cannot respond while the walk is in flight. This was introduced by Phase 96 plan 96-08 ("96-08-WIRING-NOTES.md §Plan 96-06 wiring") — the line-number proximity to Phase 97.2-02's Reset button code is coincidence.

### Bug B — Reset button blocked precisely when needed (Phase 97.2-INTRODUCED UX hole)

`_update_reset_button_state` (my_library_tab.py:1317) disables `_btn_reset` if **either**:
1. `self._worker is not None and self._worker.isRunning()` (a worker IS actually running) — correct
2. `self._indexer.start_recovery_probe()` returns non-empty (stale orphan `scan_runs` rows with `status='running'`) — **WRONG for the recovery scenario**

After a crash, the orphan running rows are *exactly the symptom Reset is supposed to fix*. The guard conflates "live worker running" with "stale DB orphan row exists". The plan reviewer (codex revision-2) did not flag this — the active-scan guard intent was stated as preventing concurrent destructive ops, but the corner case "stale orphan after crash" was not exercised.

The recovery probe modal at line 1023 (`_show_recovery_modal`) presents Resume/Restart/Skip; both Restart and Skip flip the orphan row's status (Restart calls `discard_run`, Skip calls `_end_scan_run(run_id, "canceled")`). **After the modal dismisses, the orphan should be gone — but Reset's guard runs immediately and sees the row before the modal touches it.** Construction-end call (line 733) runs `_update_reset_button_state()` AFTER the recovery probe in the constructor, but only if the probe returns empty.

Actually re-reading the constructor (lines 714–733):
- Line 717: `running_runs = self._indexer.start_recovery_probe()`
- Line 721: if non-empty → `self._show_recovery_modal(running_runs)` (modal flips status)
- Line 729: `self._auto_rescan_on_startup()` — silent worker start (D-25)
- Line 733: `self._update_reset_button_state()` — but the silent worker is now running, so worker_idle=False → Reset stays disabled

So in the user's repro:
1. App opens with orphan rows from crash
2. Recovery modal: user clicks Skip → orphan row flipped to `canceled`
3. `_auto_rescan_on_startup` → silent worker starts running (this is the "stuck at 70%" mystery — see Bug D below)
4. `_update_reset_button_state` runs, sees worker still running → Reset stays disabled

### Bug C — MuPDF "unknown keyword: 'TF'" × 624 (pre-existing benign stderr noise)

PyMuPDF logs a parse warning when it sees a `Tf` operator with malformed casing or a stream that breaks its expectations. 624 occurrences suggests **one warning per PDF page across the indexed files**, not a regression. Doesn't block. Silenceable with `fitz.TOOLS.mupdf_display_warnings(False)` at indexer init.

### Bug D — "Skip" but progress at 70% (NOT a bug — D-25 silent auto-rescan kicks in, but UX-confusing)

Constructor order at lines 714–729:
1. Recovery probe runs
2. Recovery modal: user clicks Skip → orphan flipped to canceled
3. `_auto_rescan_on_startup()` at line 729 → calls `_start_worker(toast_on_complete=True)` if folders are registered
4. Worker starts, progress bar appears at 0% then climbs

So the user clicked Skip on **recovery** and then `_auto_rescan_on_startup` silently started a **NEW** scan. From the user's POV, "I clicked Skip but the scan is running anyway" — this is intentional D-25 behavior but indistinguishable from "Skip ignored". And the new auto-rescan is ALSO running on the registered folders, which is what's stuck at 70% (probably one of the same mega-PDFs that froze in scenario 1).

The 40s startup is `_init_indexer` + `_canonical_filepath` resolving every previously-known file path during DB migration / probe.

---

## What I need from Codex (deep root-cause + correction path)

For each of the four bugs above, please:

1. **Confirm or correct my root-cause analysis.** Identify any second-order effects or interactions I missed (especially: does Bug A's main-thread walk interact with Phase 97 R-01 recovery probe? Does Bug B's `start_recovery_probe()` re-query make the auto-rescan race worse? Does the recovery modal's `_end_scan_run` actually leave the orphan visible to `_update_reset_button_state` if there's a tiny ordering hole?)

2. **Rank fixes by risk × impact** with sequencing. I'm leaning:
   - Bug B inline (Phase 97.2 hotfix, ~10 lines) — change guard to "worker actually running OR active scan probe (NOT orphan row probe)"
   - Bug A in a new tracked phase 97.3 — move `_populate_node` to a `QThread`, with progress + cancel, plus a file-count guard
   - Bug D documentation fix + maybe a tiny UI hint ("Skipped — starting fresh rescan…")
   - Bug C 1-line silencer
   - But push back if you see a better order or a hidden dependency

3. **For Bug A specifically:** propose the architecture. Options I see:
   - (a) Thread `_populate_node` off main, signal results back as Qt model rows
   - (b) Lazy-loading `QFileSystemModel` (built-in, threaded by default) — but it doesn't support our opt-out + canonical-path + status overlay columns natively
   - (c) Bounded eager walk with a "this folder has 30K+ files — view as flat list?" prompt above some threshold
   - (d) Move `_canonical_filepath` out of the walk entirely (do it lazily on file click), keep just `os.path.isfile` on main thread — much cheaper
   
   Which is most robust given the existing codebase (Phase 96 D-F1 wiring, Phase 97 LD-9 status overlay, Phase 97.2 opt-out optimization)? Watch for: `_displayed_paths` set, `_leaf_by_path` map, `prior_status` plumbing, opt-out checkboxes with `ItemIsUserCheckable` / `ItemIsAutoTristate`.

4. **For Bug B specifically:** propose the corrected guard. My draft:
   ```python
   worker_running = self._worker is not None and self._worker.isRunning()
   if worker_running:
       self._btn_reset.setEnabled(False)
       # ... existing tooltip
       return
   # Worker idle → Reset is ALWAYS available. Orphan scan_runs rows are
   # what Reset cleans up; blocking on them defeats the feature.
   self._btn_reset.setEnabled(True)
   # ... existing reassuring tooltip
   ```
   Are there any code paths where orphan rows in the DB but a live external process holds an exclusive Tantivy lock that Reset's `_retry_windows_rename` would deadlock on? Should the guard instead be "worker running OR `LocalIndexer` writer is currently held by another LocalIndexer instance"? The 7-step reset already does path-safety + handle-close in step 1; is that enough?

5. **For Bug D specifically:** should `_auto_rescan_on_startup` skip if the user just clicked Skip on the recovery modal in the same launch? Or is the right answer to add a transient status-bar message ("Recovery skipped — running scheduled rescan…") so the user knows the progress bar is a new operation, not the resumed one?

6. **For Bug C specifically:** is `fitz.TOOLS.mupdf_display_warnings(False)` the right answer, or should we capture stderr to a file so warnings remain debuggable for actually-broken PDFs?

7. **Surface anything else.** Look at the surrounding code (the constructor at lines 700–733, the auto-rescan path, the worker mutex, the recovery probe flow) and flag anything that smells like a near-miss bug we should fix while we're in here.

8. **Estimate the total work** in commits / hours for a "Phase 97.3 — UAT Bug Cleanup" milestone fix.

---

## Relevant code excerpts

### `desktop/my_library_tab.py:218-280` (`_populate_node`)

```python
def _populate_node(self, parent_item, dirpath: str, optouts: set, prior_status: dict = None):
    """Recursively add files and subdirs to parent_item.

    Phase 96 fix-2: prior_status (canonical_filepath -> {pages, status})
    is threaded down so leaves show scan results from the last scan
    immediately on tab open, not only after a new scan completes.
    """
    import os
    from shared.local_sys_id import _canonical_filepath
    SUPPORTED = {'.pdf', '.docx', '.txt'}
    if prior_status is None:
        prior_status = {}
    try:
        entries = sorted(os.listdir(dirpath))
    except (OSError, PermissionError):
        return
    # Add subdirs first
    for name in entries:
        full = os.path.join(dirpath, name)
        if os.path.isdir(full):
            sub = QTreeWidgetItem(parent_item, [name, '', ''])
            sub.setFlags(
                sub.flags()
                | Qt.ItemFlag.ItemIsUserCheckable
                | Qt.ItemFlag.ItemIsAutoTristate
            )
            self._populate_node(sub, full, optouts, prior_status)
    # Add files
    for name in entries:
        full = os.path.join(dirpath, name)
        if not os.path.isfile(full):
            continue
        ext = os.path.splitext(name)[1].lower()
        if ext not in SUPPORTED:
            continue
        # Phase 96 fix-2: pre-populate Pages + Status from last scan if known.
        canonical = _canonical_filepath(full)
        file_info = prior_status.get(canonical, {})
        # ... (status overlay logic, ~30 lines)
```

### `shared/local_sys_id.py:80-112` (`_canonical_filepath`)

```python
def _canonical_filepath(p: str) -> str:
    """Resolves symlinks/junctions, normalizes case + separators, applies long-path prefix.

    Phase 97.1: On Windows, applies the `\\?\` long-path prefix when the resolved
    absolute path exceeds 248 chars.
    """
    resolved = Path(p).resolve(strict=False)
    canonical = os.path.normcase(str(resolved))
    if (
        sys.platform == "win32"
        and len(canonical) > _WIN_LONG_PATH_THRESHOLD
        and not canonical.startswith(_WIN_LONG_PATH_PREFIX)
        and os.path.isabs(canonical)
    ):
        canonical = _WIN_LONG_PATH_PREFIX + canonical
    return canonical
```

### `desktop/my_library_tab.py:1317-1369` (`_update_reset_button_state`)

```python
def _update_reset_button_state(self) -> None:
    """Phase 97.2 REVIEWS Codex MEDIUM — proactively enable/disable Reset.

    Conditions for ENABLED:
      (a) self._worker is None OR not self._worker.isRunning()
      AND
      (b) self._indexer.start_recovery_probe() returns empty list
          (no orphan scan_runs rows with status='running')
    """
    if not hasattr(self, "_btn_reset") or self._btn_reset is None:
        return
    worker_idle = self._worker is None or not self._worker.isRunning()
    if not worker_idle:
        self._btn_reset.setEnabled(False)
        self._btn_reset.setToolTip(
            tr("Stop or resolve the active scan first")
            + " / "
            + "עצור או פתור את הסריקה הפעילה תחילה"
        )
        return
    # Worker is idle — check for orphan scan_runs.
    running: list = []
    if self._indexer is not None:
        try:
            running = self._indexer.start_recovery_probe() or []
        except Exception:
            running = ["__probe_failed__"]
    if running:
        self._btn_reset.setEnabled(False)
        # ... same tooltip
        return
    # All-clear: enable
    self._btn_reset.setEnabled(True)
    # ... reassuring tooltip
```

### `desktop/my_library_tab.py:714-733` (constructor order around recovery)

```python
# Phase 97 R-01: run recovery probe after indexer init.
if self._indexer is not None:
    try:
        running_runs = self._indexer.start_recovery_probe()
        if not running_runs:
            self.is_searchable = True
        else:
            self._show_recovery_modal(running_runs)
    except Exception as exc:
        logger.warning("MyLibraryTab: start_recovery_probe failed: %s", exc)
        self.is_searchable = True
else:
    self.is_searchable = True

# D-25: silent background rescan at startup
self._auto_rescan_on_startup()

# Phase 97.2 R97.2-E — initial Reset button state after recovery probe.
self._update_reset_button_state()
```

### `desktop/my_library_tab.py:1023-1078` (`_show_recovery_modal`)

```python
def _show_recovery_modal(self, running_runs: list) -> None:
    # ... QMessageBox setup with Resume/Restart/Skip ...
    run_id = running_runs[0]
    if clicked is btn_restart:
        try:
            self._indexer.discard_run(run_id)
        except Exception as exc:
            logger.warning("_show_recovery_modal: discard_run(restart) failed: %s", exc)
            try:
                self._indexer._end_scan_run(run_id, "canceled")
            except Exception:
                pass
    elif clicked is btn_skip:
        try:
            self._indexer._end_scan_run(run_id, "canceled")
        except Exception as exc:
            logger.warning("_show_recovery_modal: _end_scan_run(skip) failed: %s", exc)
    else:
        # Resume — full resume logic deferred; mark completed so gate lifts.
        try:
            self._indexer._end_scan_run(run_id, "completed")
        except Exception as exc:
            logger.warning("_show_recovery_modal: _end_scan_run(resume) failed: %s", exc)
    self.is_searchable = True
```

---

## Output format

Please return a Markdown critique with:

1. **Root-cause confirmations / corrections** per bug
2. **Recommended fix sequence** (table with: bug, fix, files touched, risk, est. commits)
3. **Bug A architecture recommendation** with concrete code pattern (Qt threading + tree population)
4. **Bug B corrected guard** (the exact code block to land)
5. **Bug C + D answers**
6. **Anything else you noticed** in the surrounding code
7. **Final estimate** for a "Phase 97.3" cleanup milestone

Be skeptical. Push back on my analysis where it's lazy. Specifically — I have NOT verified by reading the code whether the `_populate_node` walk was always recursive on the main thread (could have been refactored from a worker), and I have NOT measured whether `_canonical_filepath`'s `Path.resolve()` is actually the dominant cost vs. the recursion itself. Please verify or correct.

Brief ends here.
