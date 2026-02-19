---
phase: 39-bug-fixing-cleanup-performance-improving
plan: 04
subsystem: performance
tags: [caching, threading, sqlite, fjms, domain-hierarchy]

# Dependency graph
requires:
  - phase: 25-fjms-integration
    provides: FjmsService with get_domain_hierarchy() method
provides:
  - In-memory hierarchy cache with thread-safe double-checked locking
  - Optimized SQL query (COUNT(*) instead of COUNT(DISTINCT AlmaId))
affects: [web-domain-filter, desktop-domain-filter]

# Tech tracking
tech-stack:
  added: []
  patterns: [double-checked locking for thread-safe caching of static data]

key-files:
  created: []
  modified:
    - shared/fjms_service.py
    - tests/test_fjms_service.py

key-decisions:
  - "Double-checked locking pattern for thread-safe caching (not functools.lru_cache) to keep cache semantics explicit and avoid pickling issues with sqlite3.Row"
  - "COUNT(*) replaces COUNT(DISTINCT AlmaId) because no duplicate (AlmaId, Domain, ParentDomain) tuples exist in the domains table"
  - "Empty result from no-connection path not cached (avoids masking later reconnection)"

patterns-established:
  - "Double-checked locking: check cache, if miss acquire lock, check again, compute and store"

requirements-completed: []

# Metrics
duration: 5min
completed: 2026-02-19
---

# Phase 39 Plan 04: Domain Hierarchy Cache Summary

**Thread-safe in-memory cache for get_domain_hierarchy() with double-checked locking and COUNT(*) SQL optimization, eliminating ~5-second domain filter dialog lag on repeat opens**

## Performance

- **Duration:** 5 min
- **Started:** 2026-02-19
- **Completed:** 2026-02-19
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- Added in-memory hierarchy cache that returns instantly on second+ access (~0ms vs ~5s)
- Implemented thread-safe double-checked locking pattern for concurrent NiceGUI async handlers
- Optimized SQL query from COUNT(DISTINCT AlmaId) to COUNT(*) for faster GROUP BY
- Added 2 new tests verifying caching behavior (identity check and no-connection edge case)

## Task Commits

Each task was committed atomically:

1. **Task 1: Add thread-safe hierarchy cache and optimize query in FjmsService** - `371a9724` (fix)
2. **Task 2: Add test for hierarchy caching behavior** - `bf2bc12e` (test)

## Files Created/Modified
- `shared/fjms_service.py` - Added threading import, _hierarchy_cache and _hierarchy_lock instance vars, double-checked locking in get_domain_hierarchy(), COUNT(*) optimization
- `tests/test_fjms_service.py` - Added test_hierarchy_cache_returns_same_object and test_hierarchy_cache_not_set_when_no_connection

## Decisions Made
- Used double-checked locking pattern instead of functools.lru_cache to keep cache semantics explicit and avoid pickling issues with sqlite3.Row objects
- Replaced COUNT(DISTINCT AlmaId) with COUNT(*) because the domains table has no duplicate (AlmaId, Domain, ParentDomain) tuples
- No-connection empty result ({}) is not cached, so if a service is later initialized with a valid connection, it will compute and cache correctly

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
- Pre-existing test failures found in test_desktop_folio_navigation.py (KTIV button style) and test_responsa_core.py (Hebrew encoding in assertion) -- both unrelated to this plan's changes

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Domain filter dialog performance issue resolved
- All existing FJMS service tests continue to pass (60 total including 2 new)
- Cache is transparent to callers -- no API changes needed

## Self-Check: PASSED

- FOUND: shared/fjms_service.py
- FOUND: tests/test_fjms_service.py
- FOUND: .planning/phases/39-bug-fixing-cleanup-performance-improving/39-04-SUMMARY.md
- FOUND: commit 371a9724 (Task 1)
- FOUND: commit bf2bc12e (Task 2)

---
*Phase: 39-bug-fixing-cleanup-performance-improving*
*Completed: 2026-02-19*
