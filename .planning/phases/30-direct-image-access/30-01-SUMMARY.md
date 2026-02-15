---
phase: 30-direct-image-access
plan: 01
subsystem: api
tags: [nli, iiif, image-resolution, sqlite, sidecar, fl-id]

# Dependency graph
requires:
  - phase: 29-data-infrastructure
    provides: NliCrossrefService with get_images() for local FL ID lookup
provides:
  - Local-first FL ID resolution in web API image endpoint
  - Eliminated network round-trip for ~766K NLI manuscripts
affects: [30-02, 31-image-availability, browse, image-loading]

# Tech tracking
tech-stack:
  added: []
  patterns: [local-sidecar-first-with-network-fallback]

key-files:
  created: []
  modified:
    - web/api.py
    - web/pages/browse.py

key-decisions:
  - "FGPImageNumberId values used directly as FL IDs (no transformation needed)"
  - "Local resolution added as first-try path, all existing fallback logic preserved unchanged"
  - "Sidecar initialized once at route registration, shared across all requests via closure"

patterns-established:
  - "Local-first resolution: check SQLite sidecar before network fetch, cache results in same in-memory cache"

# Metrics
duration: 2min
completed: 2026-02-15
---

# Phase 30 Plan 01: NLI Image Resolution Summary

**Local-first FL ID resolution via NLI crossref sidecar, eliminating network manifest fetch for 766K+ manuscripts**

## Performance

- **Duration:** 2 min
- **Started:** 2026-02-15T13:59:35Z
- **Completed:** 2026-02-15T14:01:38Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- web/api.py fetch_fl_ids_from_nli now checks local SQLite sidecar before network fetch
- FGPImageNumberId values extracted from get_images() and used directly as FL IDs
- Full network fallback chain preserved (IIIF manifest + MARC API) for uncovered manuscripts
- Client-side JS documented with new architecture explanation

## Task Commits

Each task was committed atomically:

1. **Task 1: Add local FL ID resolution to web API image endpoint** - `59df5e2` (feat)
2. **Task 2: Update client-side JS to accept pre-resolved FL IDs from server** - `5efe11a` (docs)

## Files Created/Modified
- `web/api.py` - Added NliCrossrefService import/init, local sidecar lookup as first resolution path in fetch_fl_ids_from_nli
- `web/pages/browse.py` - Added architecture comments explaining server-side local resolution via crossref sidecar

## Decisions Made
- FGPImageNumberId values from get_images() are used directly as FL IDs without any transformation -- they are the same numeric strings (e.g., "421365") that go into FL{id} URLs
- Local resolution is inserted before the existing network fetch, not replacing it -- ensures graceful degradation if sidecar is unavailable
- NliCrossrefService initialized with thread_safe=True inside init_api_routes() and captured in closure scope for all nested functions

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required. The NLI crossref sidecar database (nli_crossref.db) was already created in Phase 29.

## Next Phase Readiness
- NLI local FL ID resolution complete (IMG-01)
- Ready for Plan 02: Cambridge IIIF local resolution at genizah_core level (IMG-02)
- Image loading pipeline now: cache -> sidecar -> network manifest -> MARC fallback

## Self-Check: PASSED

- FOUND: web/api.py
- FOUND: web/pages/browse.py
- FOUND: 30-01-SUMMARY.md
- FOUND: commit 59df5e2
- FOUND: commit 5efe11a

---
*Phase: 30-direct-image-access*
*Completed: 2026-02-15*
