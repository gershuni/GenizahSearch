---
phase: 129-library-filter-search-browse-by-identification-seed-026
plan: "07"
subsystem: desktop
tags: [gap-closure, library-filter, dialog, chip-recompute, tdd]
dependency_graph:
  requires: [129-04]
  provides: [LibraryFilterDialog, library_apply_selection, FilterCountWorker-meta_mgr]
  affects: [genizah_app.py, desktop/dialogs_filter.py, gui_threads.py]
tech_stack:
  added: []
  patterns: [QListWidget checkbox dialog mirroring DomainFilterDialog, FilterCountWorker keyword meta_mgr param]
key_files:
  created: []
  modified:
    - desktop/dialogs_filter.py
    - genizah_app.py
    - gui_threads.py
    - tests/test_libfilter_desktop.py
decisions:
  - "LibraryFilterDialog uses QListWidget (flat) mirroring DomainFilterDialog — no QMenu/QAction"
  - "OK button guard (disabled at zero-checked) is the primary FINDING 1 control; no Select None affordance"
  - "FilterCountWorker accepts keyword-only meta_mgr=None so dialogs_filter.py caller FilterCountWorker(filters, self) is unaffected"
  - "library intersection in _catalog_search/parallels_in_results runs on UI click thread (one-shot, ms-scale O(255K) scan — acceptable per T-129-07-02)"
  - "Four FilterCountWorker recompute/restore sites all pass meta_mgr=self.meta_mgr; dialogs_filter.py site stays default None"
metrics:
  duration: "~35 minutes"
  completed: "2026-06-28T20:22:21Z"
  task_count: 3
  file_count: 4
---

# Phase 129 Plan 07: Desktop Library Filter Gap Closure (GAP-G/H) Summary

Desktop catalog library filter control redesigned from QPushButton+QMenu to a checkbox QDialog (LibraryFilterDialog), with an all-unchecked OK guard; search-within and parallels-within now thread the library filter into the search scope; the chip-removal recompute path (FilterCountWorker) now preserves the library restriction via meta_mgr injection.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | RED tests — LibraryFilterDialog + guard + threading + recompute | d5f77e81 | tests/test_libfilter_desktop.py |
| 2 | GREEN — LibraryFilterDialog (GAP-G + FINDING 1) + catalog button | 851eeabc | desktop/dialogs_filter.py, genizah_app.py |
| 3 | GREEN — search-within threading (GAP-H) + recompute-preserves-library (FINDING 2) | 20991e44 | genizah_app.py, gui_threads.py, tests/test_libfilter_desktop.py |

## What Was Built

**GAP-G (desktop catalog library control):** Replaced the `QPushButton+QMenu` of checkable `QAction`s at `genizah_app.py:9837-9861` with a button connected to `_open_catalog_library_dialog()`. That method opens a new `LibraryFilterDialog` (in `desktop/dialogs_filter.py`) — a flat checkable `QListWidget` of all `LIBRARY_CODES` except `LOCAL`, with labels via `get_library_display(code, short=False)` (auto-Hebrew via `CURRENT_LANG`). On accept, `library_apply_selection(checked, all_codes)` maps the result to the filter sentinel (`[] = show all`, strict subset = subset).

**FINDING 1 (all-unchecked OK guard):** `LibraryFilterDialog` connects `itemChanged` to `_update_ok_button()`, which disables the OK button whenever zero items are checked. A hint label appears (`tr("Select at least one library, or check all to clear the filter")`). The `_on_accept()` slot also guards defensively. No "Select None"/deselect-all affordance is provided, so the all-unchecked state is structurally unreachable as a committed filter.

**GAP-H (search-within threading):** `_catalog_build_browse_filters()` now appends `filters['library'] = list(self._catalog_library_filter)` when the filter is active. Both `_catalog_search_in_results()` and `_catalog_parallels_in_results()` call `resolve_library_sys_ids(self._catalog_library_filter, self.meta_mgr)` after `get_filter_sys_ids()` and intersect the result (`None → set; else &= lib_ids`) into `pre_search_restrict_sys_ids`, mirroring the `&=` idiom at line 3849.

**FINDING 2 (recompute-preserves-library):** `FilterCountWorker.__init__` gains a keyword-only `meta_mgr=None` parameter stored as `self._meta_mgr`. In `run()`, after `get_filter_sys_ids()`, if `self.filters.get('library')` and `self._meta_mgr is not None`, `resolve_library_sys_ids` is called on the worker `QThread` (off the UI thread) and the result is intersected. Four `genizah_app.py` sites pass `meta_mgr=self.meta_mgr`: `_remove_filter` (15451), two `_restore_*_search` history sites (24557, 24625), and the session-restore site (25261). The `desktop/dialogs_filter.py:1593` caller `FilterCountWorker(filters, self)` is unaffected (meta_mgr defaults to None).

## Deviations from Plan

None — plan executed exactly as written.

## Verification Results

- `pytest tests/test_libfilter_desktop.py tests/test_catalog_availability_filter.py -q -m gui`: **17 passed** (3 original LIBFILTER-03 worker tests + 14 new GAP-G/H/FINDING-1/2 tests + 4 GUARD-02 catalog availability tests)
- `ruff check genizah_app.py desktop/dialogs_filter.py gui_threads.py`: **All checks passed**

## Manual Smoke Test Required

Cannot verify headlessly:
- Button opens checkbox dialog (not QMenu) with Hebrew library names
- OK disabled when nothing checked
- LOCAL absent from dialog
- Applying subset narrows catalog; chips show; clicking x restores
- "Search within these results" narrows by library
- Removing a non-library chip after catalog→search handoff preserves library restriction

## Known Stubs

None.

## Threat Flags

None. T-129-07-01 mitigated (dialog only offers LIBRARY_CODES minus LOCAL; resolve validates). T-129-07-02 accepted (one-shot UI click). T-129-07-03 mitigated (OK guard + defensive accept).

## Self-Check: PASSED

- FOUND: tests/test_libfilter_desktop.py (modified)
- FOUND: desktop/dialogs_filter.py (class LibraryFilterDialog added)
- FOUND: genizah_app.py (_open_catalog_library_dialog + GAP-H + FINDING 2 sites)
- FOUND: gui_threads.py (FilterCountWorker meta_mgr param + library intersection)
- FOUND commit d5f77e81 (RED tests)
- FOUND commit 851eeabc (LibraryFilterDialog GREEN)
- FOUND commit 20991e44 (GAP-H + FINDING 2 GREEN)
- 17 tests pass; ruff clean on all modified files
