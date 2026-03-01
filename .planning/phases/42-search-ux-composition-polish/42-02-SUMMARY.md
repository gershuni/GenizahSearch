---
phase: 42-search-ux-composition-polish
plan: 02
subsystem: ui
tags: [cancel, partial-results, composition-search, collapsible-excluded, filter-reason]

# Dependency graph
requires:
  - phase: 42-01
    provides: "Elapsed timer, ETA, chunk count state variables on p_state and comp_search_start_time"
provides:
  - "Cancel with partial results for composition search (web + desktop)"
  - "Collapsible excluded/filtered results section on both apps"
  - "Per-item filter reason annotation (source_text, high_frequency)"
  - "Escape keyboard shortcut for desktop cancel"
affects: [42-03]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "InterruptedError catch in chunk loop returns accumulated partial results"
    - "cancel_flag pattern on QThread subclasses checked in progress callback"
    - "Collapsible ui.expansion for excluded/filtered results (NiceGUI)"
    - "domain_excluded_results state tracking with per-item reasons"

key-files:
  created: []
  modified:
    - genizah_core.py
    - gui_threads.py
    - genizah_app.py
    - web/pages/parallels.py
    - web/pages/search.py

key-decisions:
  - "Core catches InterruptedError at chunk loop level, not in run_search wrapper -- preserves accumulated doc_hits"
  - "Filtered section uses ui.expansion collapsed by default rather than separator + header"
  - "Desktop uses cancel_flag on thread instead of terminate() for graceful shutdown with partial results"
  - "Domain excluded results tracked with per-item reason strings for UI display"

patterns-established:
  - "cancel_flag + InterruptedError: thread sets flag, callback raises, core catches and returns partial"
  - "filter_reason annotation: items carry 'source_text' or 'high_frequency' for UI chips"

requirements-completed: [UX-03, UX-04]

# Metrics
duration: 22min
completed: 2026-03-01
---

# Phase 42 Plan 02: Cancel with Partial Results + Excluded Section Summary

**Cancel produces usable partial results with banner in both apps; excluded/filtered results in collapsible sections with per-item reason chips**

## Performance

- **Duration:** 22 min
- **Started:** 2026-03-01T14:19:22Z
- **Completed:** 2026-03-01T14:41:41Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments
- Cancelling composition search (web or desktop) returns partial results with yellow banner showing chunks searched
- Filtered/excluded results in parallels and search appear in collapsible sections, collapsed by default
- Each excluded item shows reason chip (source text match, high frequency, domain exclusion)
- Desktop has cancel_flag on threads for graceful interruption + Escape shortcut
- Domain-excluded results in regular search tracked with per-item reason for display

## Task Commits

Each task was committed atomically:

1. **Task 1: Cancel with partial results -- web parallels and regular search** - `b1b5365e` (feat)
2. **Task 2: Cancel with partial results and excluded section -- desktop app and core engine** - `7bb6d365` (feat)

## Files Created/Modified
- `genizah_core.py` - InterruptedError catch in search_composition_logic chunk loop; filter_reason annotation; partial flag in return dict; lab_composition_search filter_reason
- `gui_threads.py` - cancel_flag on CompositionThread and LabCompositionThread; InterruptedError raise in progress callbacks
- `genizah_app.py` - Graceful cancel via cancel_flag; partial result detection in on_comp_scan_finished; Escape shortcut; filtered section collapsed with amber header and filter reasons; _get_filter_reason helper
- `web/pages/parallels.py` - Collapsible ui.expansion for filtered results; exclusion reason chips; InterruptedError handling in run_search
- `web/pages/search.py` - domain_excluded_results state; collapsible excluded section in render_results; _apply_domain_exclusions tracks excluded with reasons

## Decisions Made
- Core catches InterruptedError at chunk loop level to preserve accumulated doc_hits, rather than catching in the web run_search wrapper
- Desktop uses cancel_flag pattern instead of thread.terminate() for graceful shutdown that returns partial results
- Filtered section uses NiceGUI ui.expansion (collapsed by default) instead of separator + flat header -- cleaner UX
- Domain excluded results stored as list of {result, reason} dicts on state for render_results to display

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Core engine InterruptedError handling needed for web partial results**
- **Found during:** Task 1 (web parallels cancel)
- **Issue:** search_composition_logic did not catch InterruptedError; exception propagated to run_search which caught it as generic Exception and returned None, losing partial results
- **Fix:** Added try/except InterruptedError around chunk loop in search_composition_logic (Task 2 work done in Task 1 because it was blocking)
- **Files modified:** genizah_core.py
- **Verification:** Import test passes, partial results flow end-to-end
- **Committed in:** b1b5365e (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Core fix was prerequisite for web cancel to work. No scope creep.

## Issues Encountered
- Concurrent 42-03 session committed web/pages changes (parallels.py, search.py) under different commit hash (e258de94) due to race condition on git staging. Changes are correctly in repo.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Cancel with partial results works end-to-end on both apps
- Excluded section collapsible on both apps
- Ready for 42-03 (CreationType badge)

## Self-Check: PASSED

All 5 modified files exist. Both commit hashes (b1b5365e, 7bb6d365) verified in git log.
Key patterns verified: was_cancelled (3), cancel_flag (4), filter_reason (5), domain_excluded_results (5), Excluded Results (2).

---
*Phase: 42-search-ux-composition-polish*
*Completed: 2026-03-01*
