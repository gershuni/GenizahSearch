---
phase: 36-pgp-service-layer
plan: 01
subsystem: database
tags: [sqlite, pgp, sidecar, service-layer, json-each, singleton]

# Dependency graph
requires:
  - phase: 35-pgp-sidecar-export
    provides: "pgp_data/pgp.db sidecar (146.6 MB, 35K documents, 36K fragments, 9K sources)"
provides:
  - "PgpService class reading from pgp.db via read-only SQLite"
  - "14 backward-compatible module-level functions (zero consumer changes)"
  - "get_pgp_service() singleton factory"
  - "json_each() tag search and distinct tag queries"
  - "Batch sys_id lookup with 500-row chunking"
affects: [36-02, 37, 38, web-pages, desktop-app]

# Tech tracking
tech-stack:
  added: []
  patterns: ["PgpService class-based sidecar service (matches FjmsService/NliCrossrefService)"]

key-files:
  created: []
  modified:
    - "shared/document_service.py"
    - "web/document_service.py"

key-decisions:
  - "get_pgp_service() defaults to thread_safe=True since read-only SQLite is safe across threads"
  - "get_all_sources_for_fragment optimized from N+1 to 2 queries using batch IN"
  - "_row_to_dict helper centralizes JSON deserialization for tags and sections columns"

patterns-established:
  - "PgpService singleton: same pattern as FjmsService and NliCrossrefService"
  - "_row_to_dict(row, json_columns) for SQLite TEXT-to-Python JSON round-trip"

requirements-completed: [MIGR-02, MIGR-03, MIGR-05, MIGR-06]

# Metrics
duration: 5min
completed: 2026-02-17
---

# Phase 36 Plan 01: PGP Service Layer Summary

**PgpService class reading from pgp.db sidecar via SQLite, replacing all 11 Supabase REST API calls with sub-millisecond local queries while preserving identical 14-function public API**

## Performance

- **Duration:** 5 min
- **Started:** 2026-02-17T05:19:17Z
- **Completed:** 2026-02-17T05:24:29Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- Rewrote all 11 Supabase-querying functions to SQLite queries against pgp_data/pgp.db
- JSON columns (tags as list, sections as list of dicts) correctly deserialized via _row_to_dict helper
- Optimized get_all_sources_for_fragment from N+1 Supabase calls to 2 SQLite queries
- Web shim re-exports get_pgp_service; both import paths verified identical
- Zero Supabase imports remaining in document_service.py

## Task Commits

Each task was committed atomically:

1. **Task 1: Rewrite shared/document_service.py to PgpService class with SQLite backend** - `a4988f0d` (feat)
2. **Task 2: Update web shim and verify both import paths** - `24839d1c` (feat)

## Files Created/Modified
- `shared/document_service.py` - PgpService class + 14 module-level backward-compatible wrappers (692 lines, replaces 375 lines of Supabase code)
- `web/document_service.py` - Added get_pgp_service to re-exports

## Decisions Made
- `get_pgp_service()` defaults to `thread_safe=True` rather than False -- read-only SQLite connections are inherently safe across threads, and this eliminates the web/desktop initialization ordering concern
- Optimized `get_all_sources_for_fragment` from N+1 to 2 queries -- with local SQLite, batch IN queries are trivial and eliminate sequential round-trips
- Used `_row_to_dict(row, json_columns)` helper rather than inline json.loads() -- centralizes the critical JSON deserialization logic that prevents the #1 breakage risk (tags/sections returned as strings)
- Kept all docstrings on both class methods AND module-level wrappers -- ensures IDE tooltips work regardless of which path consumers use

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- PgpService is live and serving all PGP data from pgp.db
- Phase 36-02 (test rewrite) can now update tests to use in-memory SQLite fixtures
- Phase 37 (browse integration) and Phase 38 (desktop bundling) can proceed
- Supabase PGP tables remain intact for legacy desktop users per prior decision

## Self-Check: PASSED

- shared/document_service.py: FOUND
- web/document_service.py: FOUND
- 36-01-SUMMARY.md: FOUND
- Commit a4988f0d: FOUND
- Commit 24839d1c: FOUND

---
*Phase: 36-pgp-service-layer*
*Completed: 2026-02-17*
