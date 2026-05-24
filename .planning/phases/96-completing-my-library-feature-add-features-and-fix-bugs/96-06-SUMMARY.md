---
phase: 96
plan: "06"
subsystem: desktop/my_library_tab
tags: [phase-96, my-library, ui, tree-widget, tri-state, opt-out, splitter]
dependency_graph:
  requires: [96-04, 96-05]
  provides: [D-F1-tree-widget-ui]
  affects: [desktop/my_library_tab.py, genizah_app.py, shared/local_indexer.py, genizah_translations.py]
tech_stack:
  added: [QSplitter, QTreeWidget, QTreeWidgetItem, QTimer]
  patterns: [tri-state-checkbox, debounced-commit, set-difference-union-update, minimal-blast-radius-layout]
key_files:
  created: []
  modified:
    - desktop/my_library_tab.py
    - genizah_app.py
    - shared/local_indexer.py
    - genizah_translations.py
decisions:
  - "Layout: QSplitter(Horizontal) inside existing QVBoxLayout (Option 1 minimal-blast-radius — outer layout unchanged)"
  - "Opt-out update strategy: SET-DIFFERENCE/UNION scoped to displayed paths (NOT clear+rebuild — Codex HIGH #1 closure)"
  - "Debounce: 150ms QTimer single-shot coalesceces rapid multi-checkbox toggles"
  - "Filesystem walk (not indexer enumeration) so ignored files are also visible for pre-emptive opt-out"
  - "Scan-complete callback is _on_worker_finished (actual Phase 95 name, not _on_indexer_finished as stated in wiring notes)"
  - "list_all_filepaths() added as public API to LocalIndexer (option A per wiring notes recommendation)"
metrics:
  duration: "~25 minutes"
  completed: "2026-05-24T10:19:56Z"
  tasks_completed: 2
  files_changed: 4
---

# Phase 96 Plan 06: Opt-Out Tree Widget UI Summary

Per-file opt-out tree widget with tri-state folder checkboxes in MyLibraryTab, wiring the D-F1 persistence layer (plan 96-04) and filter cascade (plan 96-05) into a user-facing QSplitter bottom panel with SET-DIFFERENCE/UNION cross-folder preservation.

## What Was Built

### Task 1: `_OptoutTreeWidget` class + QSplitter bottom panel (`desktop/my_library_tab.py`)

**Layout change (RESEARCH §3 Option 1):**
- The existing `QVBoxLayout` (outer) is untouched
- Section 3 now contains `QSplitter(Qt.Orientation.Horizontal)` with:
  - Left (40%): `_OptoutTreeWidget` — new tri-state tree
  - Right (60%): `_status_table` — existing Phase 95 widget, parent changed only
- Initial split 400/600; user can drag the handle

**`_OptoutTreeWidget` design:**
- `QTreeWidget` subclass with `setHeaderLabel(tr("Folder contents"))`
- `populate_for_folder(path)`: walks filesystem (not indexer) so all supported files appear, enabling pre-emptive opt-out before the indexer touches them
- Folder nodes: `ItemIsUserCheckable | ItemIsAutoTristate` — Qt-native tri-state
- File leaves: `ItemIsUserCheckable`, `UserRole` = canonical filepath, `CheckState` = Checked (included) or Unchecked (opted-out)
- `itemChanged` signal debounced 150ms via `QTimer.setSingleShot(True)` — coalesces rapid toggles

**SET-DIFFERENCE/UNION update (Codex HIGH #1 closure):**
- `_commit_changes()` walks ONLY the currently displayed tree to build `currently_unchecked` and `currently_checked` leaf sets
- Applies `existing.difference_update(currently_checked)` then `existing.update(currently_unchecked)`
- Paths NOT in `_displayed_paths` (belonging to other indexed folders) are untouched
- Verified by `test_folder_a_optout_survives_folder_b_toggle` (plan 96-01 Wave 0 regression test)

**Path canonicalization (Codex MEDIUM #9 closure):**
- `_canonical_filepath` from `shared/local_sys_id` is applied at populate time (leaf `UserRole` value)
- `_displayed_paths` set stores canonical forms
- Set membership against `_local_file_optouts` is reliable on Windows (case + slash drift handled)

**Wiring identifiers (Codex MEDIUM #10 closure):**
- Folder selection: `self._folder_list.currentItemChanged` → `_on_folder_selection_changed`
- Selected path extraction: `item.data(Qt.ItemDataRole.UserRole)` (Phase 95 stashes fs path here, not display label)
- Rescan-complete callback: `_on_worker_finished` — this is the actual Phase 95 method name (wiring notes listed `_on_indexer_finished` which does not exist; see Deviations)
- Indexer attribute: `self._indexer`

**Rescan prune wiring:**
- Appended to `_on_worker_finished` after the queued-action processing
- Calls `indexer.list_all_filepaths()` (public API, option A)
- Falls back to direct `indexer._conn.execute(...)` if method absent (defensive)
- Calls `_prune_optouts_to_disk(app._local_file_optouts, on_disk)` — logs count change
- Calls `app._save_session()` after pruning

**`list_all_filepaths()` added to `LocalIndexer` (`shared/local_indexer.py`):**
- `SELECT filepath FROM local_files` — returns list of canonical filepaths
- Clean public API preferred over direct `_conn` access from the tab

**Hebrew translations added (`genizah_translations.py`):**
- `"File status & opt-outs:"` → `"סטטוס קבצים ואפשרויות ביטול:"`
- `"Folder contents"` → `"תכולת תיקייה"`

### Task 2: `_reapply_filters_for_optout_change` (`genizah_app.py`)

Added immediately after `_apply_local_optout_filter` (plan 96-05, line 17351):
- Calls `_apply_results_table_filters()` — refreshes Search tab
- Calls `_apply_comp_tree_filters()` — refreshes Composition + Parallels tabs
- Each call wrapped in its own `try/except Exception: pass` so a failure on one tab does not block the other
- Called by `_OptoutTreeWidget._commit_changes()` via `app._reapply_filters_for_optout_change()`

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Wiring notes listed incorrect scan-complete callback name**
- **Found during:** Task 1 Step 5 implementation
- **Issue:** `96-08-WIRING-NOTES.md §Plan 96-06 wiring` pins `_on_indexer_finished` as the scan-complete callback "connected to LocalIndexerWorker.finished signal in `MyLibraryTab.__init__`". The actual code has `_on_worker_finished` connected in `_start_worker()`. `_on_indexer_finished` does not exist in `desktop/my_library_tab.py`.
- **Fix:** Used `_on_worker_finished` (the actual method). Documented in plan notes via comment: `# PINNED (96-08-WIRING-NOTES.md §Plan 96-06 wiring): scan-complete callback = _on_worker_finished (Phase 95 actual name)`
- **Files modified:** `desktop/my_library_tab.py` (inline comment only)
- **Commit:** b4e0a215

**2. [Rule 2 - Missing critical functionality] `_parent_window` used instead of `_app`**
- **Found during:** Task 1 implementation — the `_OptoutTreeWidget` constructor receives `app` parameter but `MyLibraryTab.__init__` takes `parent` only and stores it as `self._parent_window`
- **Fix:** `_OptoutTreeWidget(self._bottom_splitter, self._parent_window)` — passes `self._parent_window` as the `app` arg; works correctly since `parent` IS the main app
- **Files modified:** `desktop/my_library_tab.py`
- **Commit:** b4e0a215

## Known Stubs

None — the tree widget reads from and writes to `self._app._local_file_optouts` (provided by plan 96-04), and the re-filter calls `_apply_results_table_filters` / `_apply_comp_tree_filters` (provided by plan 96-05). No placeholder data.

## Checkpoint Pending

**This plan is `autonomous: false`.** Tasks 1 and 2 are complete and committed. The human-verify checkpoint (Task 3) requires launching the desktop app and performing 12 visual/functional checks including:
1. Splitter layout visible (40/60 split with draggable handle)
2. Tree populates when a folder is selected
3. Unchecking a file hides it from search results after 150ms debounce
4. Parent folder tri-state indicator shows partial-check state
5. Cross-folder persistence (Codex HIGH #1 manual check)
6. Session persistence (unchecked files survive app restart)
7. Rescan drops stale opt-outs for deleted files
8. Status table still shows file statuses (no regression)
9. Hebrew RTL rendering in tree widget

## Self-Check: PASSED

| Item | Status |
|------|--------|
| desktop/my_library_tab.py exists | FOUND |
| genizah_app.py exists | FOUND |
| shared/local_indexer.py exists | FOUND |
| genizah_translations.py exists | FOUND |
| Commit b4e0a215 (Task 1) | FOUND |
| Commit 350d5317 (Task 2) | FOUND |
| 37 tests pass | PASSED |
| ruff clean | PASSED |
