---
phase: 32-metadata-display
plan: 03
subsystem: ui
tags: [url-fix, library-links, manchester-luna, bl-searcharchives, jts-dpul, gap-closure]

# Dependency graph
requires:
  - phase: 32-01
    provides: "Library URL method in NliCrossrefService with initial URL patterns"
provides:
  - "Working Manchester LUNA search URLs via servlet/view/search"
  - "Working BL searcharchives URLs with leaf suffix stripping"
  - "Working JTS/DPUL URLs with correct cairo_geniza collection slug"
affects: []

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "BL leaf suffix stripping via regex before URL construction"
    - "Search-based fallback URLs for libraries without direct-link patterns"

key-files:
  created: []
  modified:
    - shared/nli_crossref_service.py
    - tests/test_nli_crossref_service.py

key-decisions:
  - "Manchester uses servlet/view/search path with query params (servlet/s/ was non-functional)"
  - "BL switched from bl.uk/manuscripts (down after cyber-attack) to searcharchives.bl.uk"
  - "BL shelfmarks have leaf suffix stripped via regex and spaces URL-encoded (not underscored)"
  - "JTS collection slug corrected from /geniza to /cairo_geniza"

patterns-established:
  - "BL leaf suffix stripping: re.sub(r'\\.\\d+$', '', shelfmark) before URL encoding"

# Metrics
duration: 1min
completed: 2026-02-16
---

# Phase 32 Plan 03: Fix Broken Library URL Patterns Summary

**Fixed 3 broken library digital collection URLs (Manchester, BL, JTS) -- search-based fallback URLs now open working pages**

## Performance

- **Duration:** 1 min
- **Started:** 2026-02-16T02:51:41Z
- **Completed:** 2026-02-16T02:53:03Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- Manchester LUNA links now use `/servlet/view/search?q=` path producing working search results
- BL links switched from defunct `bl.uk/manuscripts` to `searcharchives.bl.uk` with leaf suffix stripping and proper URL encoding
- JTS/DPUL links corrected from `/geniza/catalog` (404) to `/cairo_geniza/catalog`
- New test for BL leaf suffix stripping verifies no underscore conversion (which returns zero results on searcharchives)

## Task Commits

Each task was committed atomically:

1. **Task 1: Fix library URL patterns in get_library_viewer_url()** - `55846fa8` (fix)
2. **Task 2: Update library URL tests to verify correct patterns** - `997880ae` (test)

## Files Created/Modified
- `shared/nli_crossref_service.py` - Fixed Manchester, BL, JTS URL patterns in get_library_viewer_url()
- `tests/test_nli_crossref_service.py` - Updated assertions for all 3 libraries, added BL leaf suffix test (38 total tests)

## Decisions Made
- Manchester: Changed from `servlet/s/{shelfmark}` to `servlet/view/search?q={shelfmark}` -- the /s/ path was non-functional
- BL: Switched from `bl.uk/manuscripts/FullDisplay.aspx` (down after cyber-attack) to `searcharchives.bl.uk/?q=` with leaf suffix stripping
- BL: Spaces URL-encoded (not underscored) -- verified live that underscores return zero results on searcharchives
- JTS: Corrected collection slug from `/geniza` to `/cairo_geniza` -- old path returns 404

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Phase 32 gap closure complete -- all 3 broken library URL patterns fixed
- Ready for Phase 33 (relationships/cross-references)
- All 38 NLI crossref service tests passing

## Self-Check: PASSED

All files exist, all commits verified.

---
*Phase: 32-metadata-display*
*Completed: 2026-02-16*
