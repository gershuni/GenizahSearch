---
phase: 42-search-ux-composition-polish
plan: 07
subsystem: ui
tags: [nicegui, pyqt6, translations, printed-filter, click-handler, hebrew]

# Dependency graph
requires:
  - phase: 42-04
    provides: Desktop composition polish (persistent summary, search timer, excluded grouping, printed column)
  - phase: 42-05
    provides: Web excluded width fix, 3-state printed filter toggle, initial Hebrew translations
provides:
  - Clickable excluded result items in web search
  - Consistent "Filter Printed" label across both apps
  - Desktop 3-state printed filter on search results (all/hide/only)
  - Complete Hebrew translation coverage for Phase 42 search status strings
affects: [phase-43-session-persistence, phase-44-quick-ux]

# Tech tracking
tech-stack:
  added: []
  patterns: [3-state-filter-intercept-pattern, click-handler-on-excluded-items]

key-files:
  created: []
  modified:
    - web/pages/search.py
    - genizah_app.py
    - genizah_translations.py

key-decisions:
  - "Reuse load_in_viewer for excluded items click handler (same as regular results)"
  - "Intercept COL_PRINTED in _open_results_filter_dialog instead of adding separate header click handler"
  - "Show statusbar message for printed filter state changes on desktop"

patterns-established:
  - "3-state filter intercept: cycle states in filter dialog callback, update header icon, re-apply filters"

requirements-completed: [UAT-gaps]

# Metrics
duration: 3min
completed: 2026-03-01
---

# Phase 42 Plan 07: UAT Gap Closure -- Excluded Clickable, Printed Filter Label, Desktop Printed Filter, Translations

**Clickable excluded items in web, consistent "Filter Printed"/"סנן דפוסים" label, desktop 3-state printed filter on search results, and 5 missing Hebrew translation keys**

## Performance

- **Duration:** 3 min
- **Started:** 2026-03-01T17:28:31Z
- **Completed:** 2026-03-01T17:31:50Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- Web excluded result items are now clickable -- clicking opens them in the viewer panel, same as regular results
- Web printed filter button now shows "Filter Printed" / "סנן דפוסים" instead of just "Printed"
- Desktop search results have a 3-state printed filter via the column header icon (all/hide printed/only printed)
- All Phase 42 translation gaps closed: 5 new keys added (3 search status variants, "Filter Printed", "Showing all")

## Task Commits

Each task was committed atomically:

1. **Task 1: Web excluded items clickable + printed filter label + desktop 3-state printed filter** - `faef3976` (feat)
2. **Task 2: Missing translation keys for desktop search status and printed filter** - `dde432f6` (feat)

## Files Created/Modified
- `web/pages/search.py` - Added cursor-pointer + click handler on excluded items, changed printed filter button label to "Filter Printed"
- `genizah_app.py` - Added _printed_filter_state, COL_PRINTED to filter_columns, 3-state cycle in _open_results_filter_dialog, printed filter logic in _apply_results_table_filters, reset on new search
- `genizah_translations.py` - Added 5 missing translation keys: "Showing {} of {} results" (3 variants), "Filter Printed", "Showing all"

## Decisions Made
- Reused `load_in_viewer` for excluded items click handler -- same function used by regular results, maintains consistent behavior
- Intercepted COL_PRINTED clicks in `_open_results_filter_dialog` rather than adding a separate header click handler -- cleaner integration with existing CheckBoxHeader filter_columns system
- Desktop statusbar shows state label ("Showing all"/"Hiding printed"/"Only printed") for 3 seconds after each filter toggle

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- All 7 plans in Phase 42 are now addressable (01-05 complete, 06-07 complete)
- Phase 42 UAT gaps fully addressed across plans 04-07
- Ready for Phase 43 (Session Persistence & Search History)

---
## Self-Check: PASSED

- All 3 modified files exist on disk
- Commit faef3976 found in git log
- Commit dde432f6 found in git log
- SUMMARY.md created at expected path

---
*Phase: 42-search-ux-composition-polish*
*Completed: 2026-03-01*
