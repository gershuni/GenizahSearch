---
phase: 26-scientific-joins
plan: 02
subsystem: database
tags: [fjms, joins, deduplication, group-concat, aggregation, sqlite]

# Dependency graph
requires:
  - phase: 26-scientific-joins
    plan: 01
    provides: "FJMS joins visible in web and desktop with get_join_group() single-row-per-entry"
provides:
  - "Deduplicated get_join_group() returning unique AlmaIds with aggregated scholar_names and join_types as lists"
  - "Web and desktop display all contributing scholars and join types comma-separated"
  - "6 new tests (4 unit + 2 integration) covering multi-group deduplication and aggregation"
affects: [27-domain-classification, 28-catalog-metadata]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "GROUP BY + GROUP_CONCAT(DISTINCT) for SQLite deduplication with metadata aggregation"
    - "_split_concat helper for parsing GROUP_CONCAT results into Python lists"

key-files:
  created: []
  modified:
    - shared/fjms_service.py
    - web/components/joins_panel.py
    - corrections_ui.py
    - tests/test_fjms_service.py
    - tests/test_fjms_joins_integration.py

key-decisions:
  - "Aggregation at SQL level (GROUP BY + GROUP_CONCAT) rather than Python post-processing for efficiency"
  - "Return lists for scholar_names, join_types, join_group_ids -- consumers join with comma for display"
  - "NULL values filtered out of aggregated lists by _split_concat helper"
  - "Comments joined with '; ' separator when multiple"

patterns-established:
  - "_split_concat pattern for parsing GROUP_CONCAT results into clean Python lists"

# Metrics
duration: 3min
completed: 2026-02-12
---

# Phase 26 Plan 02: FJMS Join Deduplication Summary

**GROUP BY + GROUP_CONCAT deduplication in get_join_group() so multi-group manuscripts show each partner once with all scholars and join types aggregated**

## Performance

- **Duration:** 3 min
- **Started:** 2026-02-12T06:23:47Z
- **Completed:** 2026-02-12T06:27:06Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments
- Rewrote get_join_group() SQL to GROUP BY AlmaId with GROUP_CONCAT(DISTINCT) for deduplication and metadata aggregation
- Return format changed from singular fields (scholar_name, join_type) to aggregated lists (scholar_names, join_types)
- Web and desktop consumers display comma-separated scholars and join types for multi-group partners
- 6 new tests: 4 unit tests for dedup/aggregation/null-filtering + 2 integration tests for multi-group display in web and desktop
- Full suite: 507 passed, 5 skipped, 0 failures

## Task Commits

Each task was committed atomically:

1. **Task 1: Deduplicate get_join_group() with aggregated metadata and add unit tests** - `7e03329` (feat)
2. **Task 2: Update web and desktop consumers to display aggregated metadata** - `fa5dd6c` (feat)

## Files Created/Modified
- `shared/fjms_service.py` - Rewrote get_join_group() with GROUP BY + GROUP_CONCAT, added _split_concat helper
- `web/components/joins_panel.py` - Updated FJMS merge block to read list fields and comma-join for display
- `corrections_ui.py` - Updated _get_fjms_joins() to read list fields and comma-join for display
- `tests/test_fjms_service.py` - Added multi-group fixture data + 4 new dedup/aggregation unit tests
- `tests/test_fjms_joins_integration.py` - Added multi-group fixture data + 2 new integration tests, updated existing tests for new format

## Decisions Made
- Aggregation done at SQL level (GROUP BY + GROUP_CONCAT) rather than Python post-processing for efficiency and correctness
- Return lists for aggregated fields: consumers are responsible for formatting (comma-join)
- NULL values filtered out by _split_concat helper (empty strings also excluded)
- Comments joined with '; ' separator when multiple distinct comments exist

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- FJMS join deduplication complete for all multi-group scenarios
- get_join_group() API stable with list-based return format
- Ready for Phase 27 (Domain Classification)
- No blockers for next phase

## Self-Check: PASSED

- All 6 modified/created files verified on disk
- Both task commits verified: 7e03329, fa5dd6c
- Full test suite: 507 passed, 5 skipped, 0 failures

---
*Phase: 26-scientific-joins*
*Completed: 2026-02-12*
