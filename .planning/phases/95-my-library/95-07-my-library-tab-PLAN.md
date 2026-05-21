---
phase: 95
plan: 07
type: execute
wave: 3
depends_on: [03, 05, 06]
files_modified:
  - desktop/my_library_tab.py
  - genizah_app.py
  - tests/test_my_library_tab.py
  - tests/test_local_indexer_mutex.py
  - tests/test_local_ceiling_enforcement.py
autonomous: false
requirements: [REQ-7, REQ-8, REQ-10]
must_haves:
  truths:
    - "MyLibraryTab is registered as the 7th tab in the desktop QTabWidget (Pitfall #4 — NOT 6th)"
    - "MyLibraryTab UI: folder list (QListWidget), Add Folder / Remove, Refresh / Cancel, progress bar, per-file status QTableWidget"
    - "App-start auto-rescan runs in background QThread; non-modal toast on completion (D-25)"
    - "LocalIndexerWorker(QThread) wraps shared/local_indexer.LocalIndexer with per-file + intra-file cancellation (D-24)"
    - "QMutex gates all indexer mutations; concurrent Refresh/Add/Remove requests queue FIFO max-depth 1 (D-25)"
    - "Add Folder triggers a SINGLE-FOLDER pre-scan dialog showing file_count + total_bytes; thresholds: >5000 files OR >2GB (D-26 + D-41)"
    - "Refresh triggers a MULTI-FOLDER aggregate pre-scan: file_count and total_bytes are SUMMED across ALL registered folders (per D-16 multi-folder support); thresholds apply to the aggregate; D-26/D-41 are PER-TRIGGER, not per-folder (W8 RESOLVED)"
    - "Unavailable folders are excluded from Refresh aggregate (they're skipped per D-40); but registered + available folders contribute to the sum"
    - "Unavailable folders shown with warning icon + tooltip; rows preserved (D-40)"
  artifacts:
    - path: "desktop/my_library_tab.py"
      provides: "MyLibraryTab(QWidget), LocalIndexerWorker(QThread)"
      contains: "class MyLibraryTab"
      min_lines: 300
    - path: "genizah_app.py"
      provides: "Tab registration as 7th tab"
      contains: "MyLibraryTab(self)"
  key_links:
    - from: "desktop/my_library_tab.py:LocalIndexerWorker"
      to: "shared/local_indexer.LocalIndexer"
      via: "Qt-side wraps Qt-free indexer"
      pattern: "from shared.local_indexer import LocalIndexer"
    - from: "genizah_app.py:3091+"
      to: "MyLibraryTab"
      via: "self.tabs.addTab(self.my_library_tab, tr('My Library'))"
      pattern: "self.tabs.addTab"
---

<objective>
Build the desktop UI for My Library: the `MyLibraryTab(QWidget)` with multi-folder management (D-16), a QThread worker wrapping the Qt-free indexer from Plan 03, cooperative cancellation per D-24 Codex revision (between files AND between pages), QMutex gating all mutations per D-25 Codex revision, pre-scan ceiling dialog per D-26 + D-41 (both file_count and total_bytes shown), unavailable-folder handling per D-40, and tab registration as the 7th tab in `genizah_app.py:3091` (Pitfall #4 — NOT 6th).

**W8 RESOLVED — Per-folder vs cumulative ceiling-check specification for multi-folder Refresh (D-16):**

The original plan was ambiguous about whether the ceiling check is per-folder or aggregated across the multi-folder set. Specification, locked:

| Trigger | Ceiling check scope | Notes |
|---------|--------------------|----|
| **Add Folder** | SINGLE-FOLDER (just the new path) | The new folder is pre-scanned ONLY; existing folders are not re-walked. Threshold applies to the candidate folder alone. |
| **Refresh (all folders)** | **AGGREGATE across ALL registered, AVAILABLE folders** | Indexer walks every registered folder once; the resulting `(file_count, total_bytes)` is the SUM. Threshold (>5000 files OR >2GB) applies to the aggregate. |
| **Refresh (folder is unavailable per D-40)** | Excluded from aggregate | An unavailable folder contributes 0 files / 0 bytes (the indexer's `os.path.isdir` check skips it before walk). |

This matches CONTEXT D-26 and D-41 (which speak of "before scanning begins") — the threshold is **per-trigger**, not per-folder. A user with 10 folders each holding 600 files would see the aggregate dialog at 6000 files total, even though no individual folder crosses the threshold alone.

The pre-scan implementation in `shared/local_indexer.py` exposes BOTH a per-folder method (used by Add Folder) and a multi-folder aggregator (used by Refresh):

- `LocalIndexer.prescan_count(folder_path) -> (file_count, total_bytes)` — single folder.
- `LocalIndexer.prescan_count_all() -> (file_count, total_bytes)` — iterates all registered + available folders and sums the per-folder results. Plan 03 may already have `prescan_count`; the executor adds `prescan_count_all` if not yet present.

Output: New file `desktop/my_library_tab.py` + small edit to `genizah_app.py` (3 lines for tab registration) + 3 GREEN test files.

Two human checkpoints: (a) verify the tab is visible and the folder picker works (REQ-8 manual smoke); (b) verify mid-file cancellation is responsive (D-24 P1 fix manual smoke).
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/PROJECT.md
@.planning/phases/95-my-library/95-CONTEXT.md
@.planning/phases/95-my-library/95-PATTERNS.md
@shared/local_indexer.py
@genizah_app.py
@gui_threads.py

<interfaces>
Existing 6-tab registration (genizah_app.py:3079-3091 — already verified):

```python
self.tabs = QTabWidget()
self.search_tab = self.create_search_tab()
self.composition_tab = self.create_composition_tab()
self.browse_tab = self.create_browse_tab()
self.catalog_browse_tab = self.create_catalog_browse_tab()
self.lists_tab = self.create_lists_tab()
self.community_tab = self.create_community_tab()
self.tabs.addTab(self.search_tab, tr("Search"))
self.tabs.addTab(self.composition_tab, tr("Composition Search"))
self.tabs.addTab(self.browse_tab, tr("Browse by Shelfmark"))
self.tabs.addTab(self.catalog_browse_tab, tr("Browse by Identification"))
self.tabs.addTab(self.lists_tab, tr("Personal Lists"))
self.tabs.addTab(self.community_tab, tr("Community"))
# Phase 95: INSERT 7th tab AFTER line 3091 (NOT 6th per Pitfall #4)
```

IndexerThread template (gui_threads.py:33-48):

```python
class IndexerThread(QThread):
    progress_signal = pyqtSignal(int, int)
    finished_signal = pyqtSignal(int)
    error_signal = pyqtSignal(str)

    def __init__(self, meta_mgr):
        super().__init__()
        self.indexer = Indexer(meta_mgr)

    def run(self):
        try:
            def callback(curr, total): self.progress_signal.emit(curr, total)
            total_docs = self.indexer.create_index(progress_callback=callback)
            self.finished_signal.emit(total_docs)
        except Exception as e: self.error_signal.emit(str(e))
```

SearchThread cancel_flag pattern (gui_threads.py:63-90):
```python
self.cancel_flag = False
...
def cb(curr, total):
    if self.cancel_flag:
        raise InterruptedError("cancelled")
```

W8 — Multi-folder aggregate pre-scan signature:
```python
# In shared/local_indexer.py
def prescan_count_all(self) -> tuple[int, int]:
    """Walk every registered, available folder once; return aggregate (file_count, total_bytes).
    Unavailable folders (D-40) are excluded from the sum.
    Used by MyLibraryTab Refresh per D-16 multi-folder support. The aggregate is
    the input to the D-26/D-41 ceiling-check dialog — thresholds apply to the
    AGGREGATE, not per-folder."""
    total_files = 0
    total_bytes = 0
    for folder in self.list_folders():
        path = folder["path"]
        if folder.get("status") == "unavailable":
            continue
        if not os.path.isdir(path):
            continue  # treat as unavailable
        f, b = self.prescan_count(path)
        total_files += f
        total_bytes += b
    return total_files, total_bytes
```
</interfaces>
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: Create desktop/my_library_tab.py with MyLibraryTab + LocalIndexerWorker + W8 ceiling logic</name>
  <read_first>
    - shared/local_indexer.py (LocalIndexer public API from Plan 03)
    - gui_threads.py:33-48 (IndexerThread shape — template)
    - gui_threads.py:63-90 (SearchThread.cancel_flag pattern)
    - genizah_app.py:11621+ (create_lists_tab — QWidget layout template)
    - .planning/phases/95-my-library/95-PATTERNS.md ("desktop/my_library_tab.py (new QWidget)" section + "QThread worker + cooperative cancellation")
    - .planning/phases/95-my-library/95-CONTEXT.md (D-15..D-26, D-40, D-41)
  </read_first>
  <behavior>
    Test `test_my_library_tab_registered`:
    - Construct a minimal MainWindow.
    - Assert `main_window.my_library_tab` is a `MyLibraryTab` instance.
    - Assert `main_window.tabs.count() >= 7`.
    - Assert at least one tab has text "My Library" (iterate `tabs.tabText(i)`).
    - DO NOT pin a specific index (Pitfall #4).

    Test `test_my_library_tab_has_folder_list_widget`:
    - `tab = MyLibraryTab(parent_mock)`.
    - Assert it has `QListWidget` (folder list), Add Folder / Remove buttons, `QProgressBar`, Refresh button, `QTableWidget` with 3 columns (Filename / Pages / Status per REQ-8).

    Test `test_concurrent_refresh_no_interleave` (tests/test_local_indexer_mutex.py):
    - Construct `MyLibraryTab` with a mock indexer.
    - Fire 3 Refresh requests in quick succession.
    - Assert QMutex serializes them; only one worker runs at a time; additional requests collapse into FIFO queue max depth 1.

    Test `test_prescan_warning_above_5000_files` (single folder, Add Folder path):
    - Mock `LocalIndexer.prescan_count` → `(5001, 1_000_000_000)`.
    - Trigger Add Folder with a fake path.
    - Assert `QMessageBox.question` called with a string containing "5,001" and "performance" or similar.

    Test `test_prescan_warning_above_2gb` (single folder, Add Folder path):
    - Mock prescan → `(100, 2_500_000_000)`.
    - Assert `QMessageBox.question` called with a string containing "2.5 GB" or "2,500".

    Test `test_refresh_aggregates_prescan_across_all_folders` (W8 — NEW):
    - Mock `LocalIndexer.list_folders()` → returns 3 folder dicts, all `status="ok"`.
    - Mock `LocalIndexer.prescan_count_all()` to internally sum per-folder results: e.g., `(3000, 1_500_000_000)` aggregate. Verify behavior by mocking `prescan_count(p)` to return `(1000, 500_000_000)` per folder and asserting the aggregator sums them.
    - Trigger Refresh.
    - Assert `QMessageBox.question` is NOT called (aggregate 3000 files < 5000 and 1.5 GB < 2 GB — under threshold).
    - Re-mock with `prescan_count_all` → `(6000, 2_500_000_000)`. Trigger Refresh.
    - Assert `QMessageBox.question` IS called with the aggregate values (6000 files, 2.5 GB).

    Test `test_refresh_aggregate_excludes_unavailable_folders` (W8 — NEW):
    - Mock `list_folders()` returns 3 folders: 2 with `status="ok"`, 1 with `status="unavailable"`.
    - Mock `prescan_count` per folder. The unavailable folder is NOT pre-scanned.
    - Assert the aggregator's call list to `prescan_count` only includes the 2 available paths.
    - Assert the aggregate count equals the sum of just those 2.
  </behavior>
  <action>
    Create `desktop/my_library_tab.py` with the FULL implementation per PATTERNS.md ("desktop/my_library_tab.py" section + "QThread worker + cooperative cancellation"). Critical components:

    1. Module imports (PyQt6.QtCore: Qt, QThread, QMutex, pyqtSignal, QSettings; PyQt6.QtWidgets: QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QListWidget, QListWidgetItem, QLabel, QProgressBar, QTableWidget, QTableWidgetItem, QFileDialog, QMessageBox, QHeaderView; PyQt6.QtGui: QIcon, QColor) + `from shared.local_indexer import LocalIndexer` + `from genizah_core import Config`.

    2. Constants: `_MAX_FILES_CEILING = 5000` (D-26), `_MAX_BYTES_CEILING = 2 * 1024 ** 3` (D-41).

    3. `class LocalIndexerWorker(QThread)`:
       - Signals: `progress_updated = pyqtSignal(int, int, str)`, `file_finished = pyqtSignal(str, str, int, str)`, `finished_signal = pyqtSignal(dict)`, `error_signal = pyqtSignal(str)` (D-23).
       - `__init__(self, indexer)` stores reference; `self._cancel_requested = False` (D-24).
       - `cancel(self)` sets the flag.
       - `run(self)`: calls `self.indexer.scan_all(cancel_check=lambda: self._cancel_requested)`; emits finished or error.

    4. `class MyLibraryTab(QWidget)`:
       - `__init__(self, parent=None)`: builds UI, inits indexer, calls `_auto_rescan_on_startup`.
       - Instance state: `self._indexer`, `self._worker`, `self._indexer_mutex = QMutex()` (D-25), `self._queued_action = None` (max queue depth 1 per D-25 Codex revision), `self._settings = QSettings("Dicta", "GenizahSearchPro")` (D-15 — UI prefs only; folder list lives in SQLite per D-15).
       - `_build_ui()`: QVBoxLayout with three sections (folder list + Add/Remove buttons; Refresh/Cancel + QProgressBar; per-file status QTableWidget cols `Filename | Pages | Status`).
       - `_init_indexer()`: instantiate `LocalIndexer(index_dir=Config.LOCAL_INDEX_DIR, lab_index_dir=Config.LOCAL_LAB_INDEX_DIR, db_path=...)`; call `startup_recovery()` (D-21).
       - `_auto_rescan_on_startup()`: D-25 silent background rescan with toast on completion.
       - `_start_worker(toast_on_complete)`: `self._indexer_mutex.tryLock()`; if held, queue the request (collapse if already queued); else spawn `LocalIndexerWorker`, disable mutation buttons, enable Cancel.
       - `_on_worker_finished(result, toast)`: unlock mutex, re-enable buttons, show status bar toast `"My Library updated: N new files indexed"` (D-25), process queued action if present.
       - `_on_worker_error(msg)`: unlock mutex, show `QMessageBox.warning`.

       **W8 RESOLVED — Two ceiling-check entry points, AGGREGATE for Refresh:**

       - `_check_ceiling_single_folder(folder_path)` — for Add Folder. Calls `indexer.prescan_count(folder_path)`, gets `(file_count, total_bytes)`, applies D-26/D-41 thresholds to JUST that folder.

       ```python
       def _check_ceiling_single_folder(self, folder_path: str) -> bool:
           """W8: SINGLE-folder ceiling check for Add Folder.
           Thresholds apply to the candidate folder alone (D-26 + D-41)."""
           file_count, total_bytes = self._indexer.prescan_count(folder_path)
           if file_count > _MAX_FILES_CEILING or total_bytes > _MAX_BYTES_CEILING:
               return self._show_ceiling_confirm_dialog(
                   file_count, total_bytes,
                   self.tr("Add folder — pre-scan"),
                   self.tr("Adding folder '%s' will index %d files (%s).") % (
                       folder_path, file_count, self._human_bytes(total_bytes)
                   ),
               )
           return True  # under threshold, proceed without dialog
       ```

       - `_check_ceiling_refresh_aggregate()` — for Refresh. **AGGREGATES across all registered, available folders per D-16.**

       ```python
       def _check_ceiling_refresh_aggregate(self) -> bool:
           """W8: MULTI-FOLDER aggregate ceiling check for Refresh.
           Iterates all registered + available folders; thresholds apply to
           the AGGREGATE sum per D-16 multi-folder support. Unavailable folders
           (D-40) are excluded from the sum."""
           total_files, total_bytes = self._indexer.prescan_count_all()
           if total_files > _MAX_FILES_CEILING or total_bytes > _MAX_BYTES_CEILING:
               folder_count = sum(
                   1 for f in self._indexer.list_folders()
                   if f.get("status") != "unavailable"
               )
               return self._show_ceiling_confirm_dialog(
                   total_files, total_bytes,
                   self.tr("Refresh — pre-scan"),
                   self.tr("Refreshing %d folders will index %d files total (%s).") % (
                       folder_count, total_files, self._human_bytes(total_bytes)
                   ),
               )
           return True
       ```

       - `_show_ceiling_confirm_dialog(file_count, total_bytes, title, body)` — shared dialog helper. Returns True if user confirms, False if cancels.

       ```python
       def _show_ceiling_confirm_dialog(self, file_count: int, total_bytes: int, title: str, body: str) -> bool:
           formatted = "%s\n\n%s\n\n%s" % (
               body,
               self.tr("Files: {:,}").format(file_count),
               self.tr("Total size: {}").format(self._human_bytes(total_bytes)),
           )
           reply = QMessageBox.question(
               self, title, formatted,
               QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel,
               QMessageBox.StandardButton.Cancel,
           )
           return reply == QMessageBox.StandardButton.Yes
       ```

       - `_on_add_folder_clicked()`: `QFileDialog.getExistingDirectory`; call `_check_ceiling_single_folder(path)`; if `True`, `indexer.add_folder(path)`; on overlap, show "This folder is already covered by an existing entry."
       - `_on_remove_folder_clicked()`: `indexer.remove_folder(current_item_path)` (D-20 synchronous delete).
       - `_on_refresh_clicked()`: call `_check_ceiling_refresh_aggregate()`; if `True`, `_start_worker(toast_on_complete=False)`.
       - `_on_cancel_clicked()`: `self._worker.cancel()` (D-24 cooperative flag).
       - `_on_progress_updated(current, total, filename)`: update progress bar.
       - `_on_file_finished(filename, status, pages, err)`: append row to status QTableWidget; D-22 two-stage status (`"Indexing…"` then `"OK"`); color error rows red.
       - `_refresh_folder_list_ui()`: clear, iterate `indexer.list_folders()`; D-40 unavailable folders shown with warning color (`#f39c12`) + tooltip `"Folder not found at <path> — files remain indexed from last scan."`

    5. **W8 — Ensure `prescan_count_all()` exists on `LocalIndexer`.** If Plan 03 did not add it, the executor adds it to `shared/local_indexer.py`:

    ```python
    def prescan_count_all(self) -> tuple[int, int]:
        """W8: aggregate prescan across all registered, available folders.
        Unavailable folders (status='unavailable' OR not os.path.isdir) excluded."""
        total_files = 0
        total_bytes = 0
        for folder in self.list_folders():
            path = folder["path"]
            if folder.get("status") == "unavailable":
                continue
            if not os.path.isdir(path):
                continue
            f, b = self.prescan_count(path)
            total_files += f
            total_bytes += b
        return total_files, total_bytes
    ```

    6. Add `list_folders()` to `shared/local_indexer.py` if not already added in Plan 03:
       ```python
       def list_folders(self) -> list[dict]:
           """SELECT * FROM folders ORDER BY added_at."""
           rows = self._sqlite_conn.execute(
               "SELECT folder_id, path, added_at, last_scanned_at, status FROM folders ORDER BY added_at"
           ).fetchall()
           return [
               {"folder_id": r[0], "path": r[1], "added_at": r[2], "last_scanned_at": r[3], "status": r[4]}
               for r in rows
           ]
       ```

    DO NOT add a LOCAL filter button on this tab — the filter button lives on the search / Composition / Parallels result toolbars (Plan 08).

    Stale LOCAL LAB banner (D-09 + D-38): if `parent_window.engine.local_lab_searcher_stale` is True (set in Plan 06), surface a non-modal toast / inline chip on the Composition Search / Parallels tabs (not MyLibraryTab) — wiring done in Plan 08.
  </action>
  <verify>
    <automated>python -m pytest tests/test_my_library_tab.py tests/test_local_indexer_mutex.py tests/test_local_ceiling_enforcement.py -x -q</automated>
  </verify>
  <acceptance_criteria>
    - File `desktop/my_library_tab.py` exists, ≥ 300 lines.
    - `grep -c "^class MyLibraryTab\\|^class LocalIndexerWorker" desktop/my_library_tab.py` returns 2.
    - `grep -c "QMutex\\|_indexer_mutex" desktop/my_library_tab.py` returns ≥ 3.
    - `grep -c "_check_ceiling_single_folder" desktop/my_library_tab.py` returns ≥ 2 (definition + call from Add Folder).
    - `grep -c "_check_ceiling_refresh_aggregate" desktop/my_library_tab.py` returns ≥ 2 (W8 — definition + call from Refresh).
    - `grep -c "prescan_count_all" desktop/my_library_tab.py shared/local_indexer.py` returns ≥ 2 (W8 — defined in indexer + invoked from tab).
    - `grep -cE "(5000|MAX_FILES_CEILING)" desktop/my_library_tab.py` returns ≥ 1.
    - `grep -cE "(2 \\* 1024|MAX_BYTES_CEILING)" desktop/my_library_tab.py` returns ≥ 1.
    - `grep -c "_cancel_requested\\|cancel_check" desktop/my_library_tab.py` returns ≥ 2.
    - `grep -c "_auto_rescan_on_startup" desktop/my_library_tab.py` returns ≥ 2.
    - `python -m pytest tests/test_my_library_tab.py tests/test_local_indexer_mutex.py tests/test_local_ceiling_enforcement.py -x -q` exits 0.
    - W8 — `python -m pytest tests/test_local_ceiling_enforcement.py::test_refresh_aggregates_prescan_across_all_folders tests/test_local_ceiling_enforcement.py::test_refresh_aggregate_excludes_unavailable_folders -x -q` exits 0.
    - `python -m ruff check desktop/my_library_tab.py` exits 0.
  </acceptance_criteria>
  <done>MyLibraryTab + LocalIndexerWorker shipped; QMutex gates mutations; ceiling dialog implemented with TWO entry points (single-folder for Add, aggregate for Refresh per W8); 3 tests green.</done>
</task>

<task type="auto">
  <name>Task 2: Register MyLibraryTab as the 7th tab in genizah_app.py</name>
  <read_first>
    - genizah_app.py:3079-3091 (the 6-tab block — exact excerpt in interfaces)
    - .planning/phases/95-my-library/95-PATTERNS.md ("Modification 1: Register MyLibraryTab as 7th tab")
  </read_first>
  <action>
    1. Add import at the TOP of `genizah_app.py` (near other module-level imports, alongside `from desktop.*` if present):
    ```python
    from desktop.my_library_tab import MyLibraryTab
    ```

    2. AFTER line 3085 (after `self.community_tab = self.create_community_tab()`), add tab construction:
    ```python
    self.my_library_tab = MyLibraryTab(self)
    ```

    3. AFTER line 3091 (after the existing 6 `self.tabs.addTab(...)` calls), add tab registration:
    ```python
    self.tabs.addTab(self.my_library_tab, tr("My Library"))
    ```

    Final state of lines 3079-3093 should match the example in the interfaces block.

    The tab is the 7th — DO NOT pin a specific index in any test. Pitfall #4: SPEC REQ-8 says "6th" but the codebase has 6 tabs today, so MyLibraryTab is the 7th.
  </action>
  <verify>
    <automated>python -c "import re; src=open('genizah_app.py',encoding='utf-8').read(); assert 'from desktop.my_library_tab import MyLibraryTab' in src; assert 'self.my_library_tab = MyLibraryTab(self)' in src; assert 'self.tabs.addTab(self.my_library_tab' in src; print('OK')"</automated>
  </verify>
  <acceptance_criteria>
    - `grep -c "from desktop.my_library_tab import MyLibraryTab" genizah_app.py` returns 1.
    - `grep -c "self.my_library_tab = MyLibraryTab(self)" genizah_app.py` returns 1.
    - `grep -c "self.tabs.addTab(self.my_library_tab" genizah_app.py` returns 1.
    - `python -c "import ast; ast.parse(open('genizah_app.py',encoding='utf-8').read())"` exits 0 (file still parses).
    - `python -m ruff check genizah_app.py 2>&amp;1 | grep -v 'F401\|F841' | head -5` shows no NEW errors related to the changes (ruff baseline noise tolerated).
  </acceptance_criteria>
  <done>3 small edits done; genizah_app.py still parses; my_library_tab integrated.</done>
</task>

<task type="checkpoint:human-verify" gate="blocking">
  <name>Task 3: Manual smoke test — MyLibraryTab visible + folder picker + cancel responsive + W8 aggregate ceiling</name>
  <what-built>
    - Plan 03 Qt-free indexer (`shared/local_indexer.py`) — already verified by Wave-1 automated tests.
    - Plan 07 desktop UI: `MyLibraryTab` as 7th tab, multi-folder list, Refresh/Cancel, per-file status panel, pre-scan ceiling dialog (per-folder for Add, AGGREGATE for Refresh per W8), QMutex serialization, mid-file cancellation (D-24 Codex revision).
  </what-built>
  <how-to-verify>
    Launch the desktop app: `python genizah_app.py`.

    **A) Tab presence (REQ-8 acceptance):**
    1. Verify a 7th tab labeled "My Library" appears to the right of "Community".
    2. Click the tab — it should render with: a folder list (top), Refresh/Cancel buttons + progress bar (middle), per-file status table (bottom). Do NOT panic if the folder list is empty on first launch.

    **B) Add Folder + first scan:**
    3. Click "Add Folder…". A standard folder picker opens.
    4. Select a small test folder (5-10 .docx/.pdf/.txt files, mixed). For Hebrew testing, include at least one Hebrew-content PDF if available.
    5. If THIS folder alone has > 5,000 files OR > 2 GB, the per-folder ceiling dialog should appear with BOTH counts. Cancel → no scan; Yes → scan proceeds.
    6. Scan runs in background. Progress bar advances. Per-file status table fills in. Each row shows "OK" (or specific error) when committed (D-22 two-stage).
    7. On completion, a non-modal toast or status bar message appears: "My Library updated: N new files indexed".

    **B2) W8 Aggregate ceiling on Refresh (NEW):**
    7a. Add 3 small folders (each under threshold individually — e.g., 2000 files each, 700 MB each).
    7b. Click Refresh.
    7c. **W8 — Critical:** The AGGREGATE ceiling dialog should appear with file_count = sum across all 3 folders (~6000) and total_bytes = sum (~2.1 GB). Dialog title should indicate "Refresh — pre-scan" and body shows the aggregate.
    7d. Click Cancel → no scan; click Yes → scan proceeds across all folders.

    **C) Mid-file cancellation (D-24 P1 fix):**
    8. Add a LARGE PDF (≥ 100 pages, Hebrew if available) or a folder with a single huge PDF.
    9. Click Refresh; while the large file is being extracted, click Cancel.
    10. **Critical:** Cancel should respond within a few seconds (NOT wait for the whole file). The status row for the in-flight file should change to "Cancelled" or similar. The next Refresh should re-extract that file.

    **D) Remove Folder (D-20):**
    11. Select a folder in the list; click Remove. The folder + its files should disappear from the index immediately (run a search containing a known phrase from that folder — no LOCAL hits should appear).

    **E) Concurrent operations (D-25 mutex):**
    12. Click Refresh, then immediately click Refresh again, then Refresh a third time.
    13. **Critical:** Only ONE scan should run at a time. The 2nd and 3rd clicks should be either ignored or queued (max queue depth 1). The UI should not freeze; the status table should not show duplicate entries.

    **F) Unavailable folder (D-40 + W8):**
    14. Add a folder; let the scan complete.
    15. Close the app. Rename or delete the folder on disk. Restart the app.
    16. **Critical:** The folder should appear in the list with a warning icon / orange color + tooltip "Folder not found at <path> — files remain indexed from last scan." The previously-indexed files should STILL be searchable (try a search hitting their content — LOCAL hits should appear).
    17. **W8 follow-up:** Click Refresh with the unavailable folder still listed. The aggregate ceiling check should EXCLUDE that folder from its sum (file_count + total_bytes reflect only available folders).
  </how-to-verify>
  <resume-signal>Reply "approved" if all 6 checks pass. If anything fails, describe the failure (e.g., "Refresh ceiling shows per-folder not aggregate" or "tab not visible") so the executor can patch before proceeding to Plans 08-09.</resume-signal>
  <acceptance_criteria>
    - All 6 smoke-test sections pass.
    - User confirms with "approved".
  </acceptance_criteria>
  <done>Human smoke test passed.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| User folder picker → indexer | Untrusted folder path; D-17 + D-42 normalization + overlap checks |
| QThread worker → main UI thread | Signals are the only cross-thread channel; data passed by value or immutable Python objects |
| QMutex held → all mutation sites | Single source of truth; concurrent Refresh/Add/Remove cannot interleave |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-95-25 | Tampering | Concurrent indexer writes corrupt Tantivy / SQLite | mitigate | D-25 QMutex; tests/test_local_indexer_mutex.py asserts no interleave |
| T-95-26 | Denial of service | Huge single file (1000-page PDF) blocks UI cancellation | mitigate | D-24 Codex revision: intra-file cancel check between PDF pages / DOCX chunks; partial pages rolled back via `writer.rollback()` |
| T-95-27 | Tampering | Junction loops / UNC paths in folder list crash `os.walk` | mitigate | D-26 hardened pre-scan: `os.walk(followlinks=False)` + per-directory try/except OSError |
| T-95-28 | Elevation of privilege | `os.startfile()` invokes registered app (handler in Plan 08 browse panel) | accept | We only call on already-indexed files of types {.docx, .pdf, .txt}; user controls which apps are registered |
| T-95-29 | Information disclosure | Status bar toast leaks filenames to bystanders | accept | Personal-machine context; status bar visible only on user's screen |
| T-95-35 | Denial of service | Multi-folder Refresh on 10× registered folders crosses ceiling silently (no warning) | mitigate | W8 — `prescan_count_all()` aggregates BEFORE scan starts; D-26/D-41 thresholds apply to aggregate; user explicitly confirms via dialog |
</threat_model>

<verification>
- `python -m pytest tests/test_my_library_tab.py tests/test_local_indexer_mutex.py tests/test_local_ceiling_enforcement.py -x -q` exits 0.
- `python -m pytest tests/ -q` exits 0 (no regressions).
- `python -m ruff check desktop/my_library_tab.py genizah_app.py` exits 0.
- Manual smoke (Task 3) passes all 6 checks.
- App still launches without LOCAL data (first-launch case): `python genizah_app.py` boots, MyLibraryTab visible, no errors in stdout.
- W8 — `prescan_count_all()` aggregates across registered + available folders; unavailable folders contribute 0.
</verification>

<success_criteria>
- `desktop/my_library_tab.py` shipped with MyLibraryTab + LocalIndexerWorker.
- 7th tab registered in `genizah_app.py` (Pitfall #4 respected — assertion is by text, not index).
- QMutex serialization works (D-25).
- Mid-file cancellation works (D-24 Codex revision).
- Pre-scan ceiling dialog has TWO entry points (W8): single-folder for Add Folder + AGGREGATE for Refresh (per D-16 multi-folder support; D-26/D-41 are per-trigger).
- Unavailable folders preserved with warning UI (D-40); excluded from Refresh aggregate.
- 3 Wave-0 stub files green.
- Manual smoke approved.
- No regressions in existing tests.
</success_criteria>

<output>
After completion, create `.planning/phases/95-my-library/95-07-SUMMARY.md` documenting:
- Final UI shape (any deviations from PATTERNS.md template)
- Whether `list_folders()` + `prescan_count_all()` were added to `shared/local_indexer.py` here or in Plan 03
- **W8 confirmation:** `_check_ceiling_single_folder` (Add Folder) AND `_check_ceiling_refresh_aggregate` (Refresh) both shipped; aggregate excludes unavailable folders
- Manual smoke verdict (per Task 3 checklist)
- Any UI polish open questions for Plan 09
</output>
