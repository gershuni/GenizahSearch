---
phase: 44-quick-ux-wins
plan: 01
subsystem: ui
tags: [pyqt6, windows-api, toast-notification, sleep-prevention, context-menu, clipboard]

# Dependency graph
requires: []
provides:
  - "OS sleep prevention during all search threads via SetThreadExecutionState"
  - "Windows toast notification on search completion when app is unfocused"
  - "Notification toggle in desktop settings dialog"
  - "Right-click copy menu on search result rows (Shelfmark, Title, Library, Sys ID, Row)"
affects: []

# Tech tracking
tech-stack:
  added: [ctypes.windll.kernel32.SetThreadExecutionState, QSystemTrayIcon]
  patterns: [try/finally sleep prevention wrapper, tray icon lazy initialization]

key-files:
  created: []
  modified: [gui_threads.py, genizah_app.py, genizah_translations.py]

key-decisions:
  - "Skip on_comp_finished notification (only on_comp_scan_finished) to avoid double notification"
  - "Also wrap LabCompositionThread with sleep prevention (not just the 3 threads mentioned in plan)"

patterns-established:
  - "_prevent_sleep/_allow_sleep: module-level helpers for Windows sleep prevention via ctypes"
  - "QSystemTrayIcon lazy-init pattern for desktop notifications"

requirements-completed: [QUX-01, QUX-02, QUX-04]

# Metrics
duration: 4min
completed: 2026-03-02
---

# Phase 44 Plan 01: Desktop Notification, Sleep Prevention & Copy Menu Summary

**Windows toast notification on search complete, OS sleep prevention via SetThreadExecutionState in all search threads, and right-click copy options on search result rows**

## Performance

- **Duration:** 4 min
- **Started:** 2026-03-02T09:03:53Z
- **Completed:** 2026-03-02T09:08:41Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- Sleep prevention wraps all 4 search thread classes (SearchThread, LabSearchThread, CompositionThread, LabCompositionThread) with try/finally to ensure cleanup
- Toast notification via QSystemTrayIcon fires when search completes and app is not focused, with configurable toggle in settings
- Context menu on search result rows offers Copy Shelfmark, Copy Title, Copy Library, Copy Sys ID, and Copy Row (tab-separated)
- 9 new Hebrew translations for notification and copy menu strings

## Task Commits

Each task was committed atomically:

1. **Task 1: Sleep prevention + toast notification + settings toggle + translations** - `346ed74f` (feat)
2. **Task 2: Copy context menu on search results** - `bdca6c05` (feat)

## Files Created/Modified
- `gui_threads.py` - Added _prevent_sleep/_allow_sleep helpers, wrapped 4 thread run() methods
- `genizah_app.py` - Added _notify_search_complete method, notification calls in handlers, settings toggle, copy context menu items
- `genizah_translations.py` - 9 new Hebrew translation entries for notification and copy strings

## Decisions Made
- Skipped notification from on_comp_finished (grouping complete) since on_comp_scan_finished already fires a notification for composition searches -- avoids double notification
- Also wrapped LabCompositionThread with sleep prevention (plan only specified 3 threads, but this 4th thread also runs long searches)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Added sleep prevention to LabCompositionThread**
- **Found during:** Task 1 (Sleep prevention)
- **Issue:** Plan only specified SearchThread, LabSearchThread, CompositionThread but LabCompositionThread also runs long searches
- **Fix:** Added _prevent_sleep/_allow_sleep wrapper to LabCompositionThread.run()
- **Files modified:** gui_threads.py
- **Verification:** _prevent_sleep/_allow_sleep appear 4 times each in gui_threads.py
- **Committed in:** 346ed74f (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 missing critical)
**Impact on plan:** Essential for completeness -- all long-running search threads now prevent sleep. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Phase 44 Plan 02 (Hebrew library names) can proceed independently
- All notification infrastructure is in place for future search types

---
*Phase: 44-quick-ux-wins*
*Completed: 2026-03-02*
