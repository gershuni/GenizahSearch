---
phase: 20-test-suite-green
plan: 01
subsystem: testing
tags: [pytest, export-service, boundary-search, test-maintenance]

# Dependency graph
requires:
  - phase: 19-search-normalization
    provides: "make_safe_filename now uses underscores; Library column added to Excel headers"
provides:
  - "All export service tests passing (50/50)"
  - "All boundary search tests passing (34/34)"
  - "Obsolete backend test files removed from repo"
affects: [20-02-PLAN]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Test expectations must track production behavior changes"
    - "min_distance parameter requires sufficient words between boundaries in test inputs"

key-files:
  created: []
  modified:
    - tests/test_export_service.py
    - tests/test_boundary_search.py

key-decisions:
  - "Updated test expectations rather than reverting production code -- production behavior is correct"
  - "Used longer test input texts for boundary tests to satisfy min_distance=3 rather than lowering min_distance"

patterns-established:
  - "Boundary search tests use >= 3 words per part to satisfy default min_distance"

# Metrics
duration: 3min
completed: 2026-02-11
---

# Phase 20 Plan 01: Fix Failing Tests Summary

**Fixed 7 test failures across export service and boundary search by updating expectations to match production behavior changes**

## Performance

- **Duration:** 3 min
- **Started:** 2026-02-11T08:45:39Z
- **Completed:** 2026-02-11T08:48:34Z
- **Tasks:** 2
- **Files modified:** 2 (plus 3 deleted by parallel plan 20-02)

## Accomplishments
- Fixed 5 export service test failures: filename underscore expectations, browse filename, Excel column index
- Fixed 2 boundary search test failures: input texts now have enough words for min_distance=3
- Verified 3 obsolete backend test files (test_api_flow.py, test_corrections_api.py, test_corrections_integration.py) already deleted by plan 20-02
- All 91 tests pass across test_export_service.py, test_boundary_search.py, and test_excel_logic.py

## Task Commits

Each task was committed atomically:

1. **Task 1: Delete obsolete backend test files and fix export service test expectations** - `b080fc5` (fix)
2. **Task 2: Fix boundary search test expectations** - `9183b01` (fix)

**Plan metadata:** (pending)

## Files Created/Modified
- `tests/test_export_service.py` - Fixed 5 test expectations: filename underscores, browse filename, column index
- `tests/test_boundary_search.py` - Fixed 2 test inputs: paragraph and line break boundary texts
- `tests/test_api_flow.py` - Deleted (already removed by plan 20-02)
- `tests/test_corrections_api.py` - Deleted (already removed by plan 20-02)
- `tests/test_corrections_integration.py` - Deleted (already removed by plan 20-02)

## Decisions Made
- Updated test expectations to match production behavior rather than reverting production code changes
- Used longer test input texts (>= 3 words per part) for boundary tests rather than changing the min_distance default
- Noted 3 obsolete backend test files were already deleted by parallel plan 20-02 execution

## Deviations from Plan

None - plan executed exactly as written. The only minor difference was that the 3 obsolete backend test files had already been deleted by the parallel plan 20-02, so the `git rm` was a no-op for those files.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Plan 20-01 complete, addresses TEST-01, TEST-02, TEST-03, TEST-04
- Plan 20-02 (responsa integration tests) already executed in parallel
- Full test suite should be green after both plans complete

## Self-Check: PASSED

- [x] tests/test_export_service.py exists
- [x] tests/test_boundary_search.py exists
- [x] 20-01-SUMMARY.md exists
- [x] test_api_flow.py deleted
- [x] test_corrections_api.py deleted
- [x] test_corrections_integration.py deleted
- [x] Commit b080fc5 exists
- [x] Commit 9183b01 exists

---
*Phase: 20-test-suite-green*
*Completed: 2026-02-11*
