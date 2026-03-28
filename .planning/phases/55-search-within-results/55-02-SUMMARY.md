---
phase: 55-search-within-results
plan: 02
subsystem: search
tags: [refinement, search-within-results, breadcrumb, nicegui, session-persistence]

# Dependency graph
requires:
  - phase: 55-01
    provides: RefinementStep dataclass, compute_effective_restrict, replay_chain, scope_signature
provides:
  - Web search refinement execution plumbing (effective restrict, chain state, session persistence)
  - Web search refinement UI (breadcrumb strip, refine mode badge, search-within button, zero-result recovery)
affects: [55-03 desktop-ui]

# Tech tracking
tech-stack:
  added: []
  patterns: [refinement-chain-state-in-SearchUIState, effective-restrict-intersection, raw-result-scope]

key-files:
  created: []
  modified:
    - web/pages/search.py

key-decisions:
  - "Refinement scope based on RAW results (all_sys_ids before post-filters like domain/printed/measurement)"
  - "Stale filter detection via _recompute_filter_count (single hook point for all filter handlers)"
  - "Deferred chain replay at 0.3s delay using existing _after_delay pattern"
  - "History guard: refined searches do not enter search history (D-15)"

patterns-established:
  - "Effective restrict pattern: compute_effective_restrict(filter_restrict, refinement_restrict) before every search"
  - "Refinement breadcrumb strip on dedicated row below results header (not inside header)"

requirements-completed: [SRCH-01, SRCH-02, SRCH-03]

# Metrics
duration: 8min
completed: 2026-03-28
---

# Phase 55 Plan 02: Web Search-Within-Results Summary

**Web search refinement with breadcrumb chain, refine mode toggle, session persistence with replay, and zero-result recovery**

## Performance

- **Duration:** 8 min
- **Started:** 2026-03-28T19:11:31Z
- **Completed:** 2026-03-28T19:19:30Z
- **Tasks:** 2 completed + 1 checkpoint (human-verify)
- **Files modified:** 1

## Accomplishments
- Wired refinement chain into search execution: effective_restrict = intersection of filter + refinement restrict sets
- Refinement scope defined as RAW results (all_sys_ids before domain/printed/measurement post-filters)
- Full breadcrumb strip UI with per-chip removal, "Clear all", stale indicator, and result count
- "Search within N" button in results header, refine mode badge + cancel near search bar with scroll-to-focus
- Zero-result refinement recovery with "Back to previous step" button
- Session persistence: chain metadata persisted and replayed on restore with "Restoring refinement chain..." feedback
- History guard: refined searches excluded from search history dropdown
- Stale filter detection via scope_signature comparison in centralized _recompute_filter_count

## Task Commits

Each task was committed atomically:

1. **Task 1: Search execution plumbing, state, persistence, and replay** - `651d1437` (feat)
2. **Task 2: Breadcrumb strip UI, refine mode badge, search-within button, zero-result recovery** - `424eb8f1` (feat)
3. **Task 3: Visual verification** - checkpoint:human-verify (awaiting user testing)

## Files Created/Modified
- `web/pages/search.py` - SearchUIState refinement fields, effective_restrict computation, refinement chain state management, breadcrumb strip UI, refine mode badge, search-within button, zero-result recovery, session persistence with replay, history guard, stale filter detection

## Decisions Made
- Refinement scope is RAW result sys_ids (all_sys_ids computed before any post-search filters) -- this ensures the chain is stable regardless of domain/printed/measurement toggles
- Stale filter detection hooks into _recompute_filter_count rather than each individual filter handler -- single point of integration
- Deferred chain replay uses 0.3s delay (after 0.2s transcription restore) to ensure UI widgets exist
- Search history guard wraps the entire history block including the try/except to cleanly skip all history logic during refinement

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed text_position variable scope in refinement step creation**
- **Found during:** Task 1
- **Issue:** Plan referenced `tp` variable which is defined inside `run_core_search()` closure, not accessible at the `execute_search` scope where refinement step is created
- **Fix:** Used `text_position_select.value` directly instead of `tp`
- **Files modified:** web/pages/search.py
- **Committed in:** 651d1437 (Task 1 commit)

**2. [Rule 1 - Bug] Fixed indentation after effective_restrict refactor**
- **Found during:** Task 1
- **Issue:** The zero-check block body was at 16-space indent (from original `if _has_active_filters():` nesting) but the new `if effective_restrict` statement was moved to 8-space indent level
- **Fix:** Re-indented the block body to match the new parent `if` statement
- **Files modified:** web/pages/search.py
- **Committed in:** 651d1437 (Task 1 commit)

---

**Total deviations:** 2 auto-fixed (2 bugs)
**Impact on plan:** Both fixes necessary for correctness. No scope creep.

## Known Stubs

None - all functions are fully implemented.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Checkpoint Status

Task 3 (checkpoint:human-verify) is awaiting user testing. The user should:
1. Start the web app (`python -m web.main`)
2. Follow the verification steps in the plan (search, click "Search within", test breadcrumb chain, etc.)
3. Approve or report issues

## Next Phase Readiness
- Web refinement complete, ready for desktop UI (55-03) which follows same pattern
- shared/refinement.py imports verified working in web context
- Session persistence round-trips chain metadata correctly

---
*Phase: 55-search-within-results*
*Completed: 2026-03-28*
