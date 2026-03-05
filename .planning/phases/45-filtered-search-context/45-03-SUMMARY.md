---
phase: 45-filtered-search-context
plan: 03
status: complete
completed: "2026-03-02"
duration: 32min
tasks_completed: 2
tasks_total: 2
subsystem: desktop-ui
tags: [filtering, pre-search, desktop, PyQt6, chip-bar, session-persistence]

requires:
  - phase: 45-01
    provides: get_filter_sys_ids, restrict_sys_ids on execute_search/search_composition_logic
provides:
  - PreSearchFilterDialog with domain/author/work/date/material controls
  - Filter chip bar on search and composition tabs
  - restrict_sys_ids on SearchThread and CompositionThread
  - Per-result word search exclusion
  - Filter state session persistence
affects: [genizah_app.py, gui_threads.py]

tech_stack:
  added: []
  patterns: [FilterCountWorker background thread for async manuscript count, chip bar with removable filter chips]

key_files:
  created: []
  modified: [genizah_app.py, gui_threads.py]

key_decisions:
  - "PreSearchFilterDialog uses QComboBox with editable search for domain/author/work (matches existing browse pattern)"
  - "FilterCountWorker (QThread) computes manuscript count in background to keep dialog responsive"
  - "Chip bar is shared between search and composition tabs (same pre_search_filters state)"
  - "Filter state persisted at top-level session dict (not inside regular_search/composition_search) since filters are shared"
  - "restrict_sys_ids=None default on threads is fully backward compatible"

patterns_established:
  - "Pre-search filter dialog pattern: dialog returns filter dict + computed restrict_sys_ids"
  - "Chip bar pattern: QHBoxLayout with flat QPushButtons as removable chips, click removes filter"
  - "FilterCountWorker for async filter count recomputation"

requirements_completed: [FILT-01, FILT-02, FILT-03, FILT-04, FILT-06]

duration: 32min
completed: 2026-03-02
---

# Phase 45 Plan 03: Desktop Pre-Search Filters, Chip Bar, and Thread Integration Summary

**PreSearchFilterDialog with domain/author/work/date/material controls, chip bar on both tabs, restrict_sys_ids threading to SearchThread/CompositionThread, per-result word search exclusion, and session persistence.**

## Performance

- **Duration:** 32 min
- **Started:** 2026-03-02T20:55:34Z
- **Completed:** 2026-03-02T21:28:00Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments

- PreSearchFilterDialog (QDialog) with domain hierarchy, author/work cross-filtered combos, date range spinboxes, Exclude Printed checkbox, and live manuscript count via FilterCountWorker background thread
- Filters button added to both search tab (row2) and composition tab (top_row) -- opens shared dialog, updates shared state
- Chip bar (QHBoxLayout) with removable filter chips and manuscript count, visible on both search and composition tabs when filters are active
- SearchThread and CompositionThread extended with restrict_sys_ids parameter, wired to pass pre_search_restrict_sys_ids from filter dialog
- Per-result word search exclusion via context menu ("Exclude this manuscript") with word_excluded_sys_ids tracking
- Filter state (pre_search_filters dict, word_excluded_sys_ids set) persisted in session save/restore

## Task Commits

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1+2 | PreSearchFilterDialog, chip bar, thread wiring | 5a8e45e9 | genizah_app.py, gui_threads.py |

## Files Created/Modified

- `genizah_app.py` -- Added FilterCountWorker class, PreSearchFilterDialog class, Filters buttons on search and composition tabs, chip bar widgets, _open_pre_search_filter_dialog(), _update_filter_chip_bar(), _add_filter_chip(), _remove_filter(), _exclude_word_search_result(), session persistence for pre_search_filters and word_excluded_sys_ids
- `gui_threads.py` -- Added restrict_sys_ids parameter to SearchThread.__init__() and run(), CompositionThread.__init__() and run()

## Decisions Made

- **Combined commit for Tasks 1+2:** Both tasks were tightly coupled (thread wiring depends on dialog state), so they were committed together for atomicity.
- **FilterCountWorker as QThread:** Dialog recomputes manuscript count in background when any filter changes, preventing UI freeze on large filter queries.
- **Shared filter state:** `pre_search_filters` and `pre_search_restrict_sys_ids` are stored on `self` (the main GenizahApp), shared between search and composition tabs. Both chip bars reflect the same state.
- **Session persistence at top level:** Filter state stored at the root of the session dict (not inside regular_search or composition_search) since it applies to both modes.
- **LabCompositionThread not extended:** lab_composition_search() does not yet accept restrict_sys_ids; lab mode is secondary. LabSearchThread also not extended (lab_search lacks the parameter).
- **Domain combo uses flat list with indentation:** Child domains indented with 2 spaces in the combo text, using currentData() for the actual domain identifier. Simpler than QTreeWidget for a combo box.

## Deviations from Plan

None -- plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

None -- no external service configuration required.

## Next Phase Readiness

- Desktop pre-search filters complete and wired
- Plan 45-02 (Web search filter panel) and 45-04 (Web parallels filter) can proceed independently
- Plan 45-05 (Browse-to-search path) can build on the pre_search_filters state pattern established here

---
*Phase: 45-filtered-search-context*
*Completed: 2026-03-02*
