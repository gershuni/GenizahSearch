---
phase: 34-library-iiif-integration
plan: 03
subsystem: service-layer
tags: [manchester, luna, jts, dpul, princeton, iiif, figgy, manifest, sidecar, sqlite]

# Dependency graph
requires:
  - phase: 34-01
    provides: "manchester_luna table with 27,940 LUNA ID mappings in nli_crossref.db"
  - phase: 34-02
    provides: "jts_dpul table with ARK suffixes, Figgy manifest URLs, DPUL catalog URLs"
provides:
  - "get_manchester_luna_id and get_manchester_manifest_url service methods"
  - "get_jts_manifest_url and get_jts_dpul_url service methods with base shelfmark fallback"
  - "Library viewer URLs upgraded from search to detail/catalog pages when sidecar data exists"
  - "get_image_sources reports manchester and jts availability"
  - "enrich_metadata discovers Manchester LUNA and JTS Figgy manifests as external image sources"
affects: [34-04, 34-05]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Detail page URL pattern: luna_id lookup for Manchester, dpul_url lookup for JTS, search URL as fallback"
    - "Base shelfmark fallback: strip trailing .N suffix to find parent shelfmark in sidecar"
    - "external_provider key in enrich_metadata for downstream UI labeling"

key-files:
  created: []
  modified:
    - shared/nli_crossref_service.py
    - genizah_core.py
    - tests/test_nli_crossref_service.py

key-decisions:
  - "Manchester detail URL uses luna.manchester.ac.uk/luna/servlet/detail/{luna_id} -- direct detail page, not search"
  - "JTS uses dpul_url from sidecar directly -- catalog page, not search"
  - "Both fall back to search URLs when sidecar data is missing for a specific manuscript"
  - "JTS shelfmark lookup tries full shelfmark first, then strips .N leaf suffix for base fallback"
  - "get_image_sources uses JOIN for Manchester (nli_images.ImageSourceName -> manchester_luna) and direct lookup for JTS"

patterns-established:
  - "Detail page URL with search fallback: prefer sidecar-sourced direct link, degrade to search"
  - "external_provider metadata key set in enrich_metadata for UI to differentiate image source origin"

# Metrics
duration: 3min
completed: 2026-02-16
---

# Phase 34 Plan 03: Manchester/JTS Service Integration Summary

**Manchester LUNA and JTS/Princeton DPUL service methods with detail page URLs, image source reporting, and enrich_metadata manifest discovery**

## Performance

- **Duration:** 3 min
- **Started:** 2026-02-16T04:02:42Z
- **Completed:** 2026-02-16T04:05:42Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- Added 4 new service methods for Manchester and JTS data access (luna_id, manifest URL, dpul URL, catalog page)
- Upgraded library viewer URLs from generic search links to detail/catalog page URLs when sidecar data exists
- Extended get_image_sources to report manchester and jts availability for UI badge rendering
- Integrated Manchester LUNA and JTS Figgy manifest discovery into enrich_metadata, feeding into existing IIIF v2 processing
- Added 20 new tests covering all new methods, URL patterns, fallback logic, and graceful degradation

## Task Commits

Each task was committed atomically:

1. **Task 1: Add Manchester and JTS methods to NliCrossrefService** - `7cda65f7` (feat)
2. **Task 2: Integrate Manchester and JTS manifest discovery into enrich_metadata** - `f900497b` (feat)

## Files Created/Modified
- `shared/nli_crossref_service.py` - 4 new methods, updated get_library_viewer_url with detail/catalog URLs, extended get_image_sources with manchester/jts checks
- `genizah_core.py` - Manchester and JTS manifest discovery in enrich_metadata before existing IIIF fetch
- `tests/test_nli_crossref_service.py` - 20 new tests (55 total), manchester_luna and jts_dpul test fixture tables

## Decisions Made
- Manchester detail URL uses `luna.manchester.ac.uk/luna/servlet/detail/{luna_id}` for direct item page
- JTS uses the dpul_url from sidecar directly for catalog page link
- Both libraries fall back to search URLs when sidecar data is missing for a specific manuscript
- JTS shelfmark lookup tries full shelfmark first, then strips trailing `.N` leaf suffix for base fallback
- get_image_sources uses JOIN between nli_images and manchester_luna for Manchester checks, direct shelfmark lookup for JTS
- external_provider key ('manchester' or 'jts') set in enrich_metadata for downstream UI labeling

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Service layer fully provides Manchester and JTS data access through NliCrossrefService
- enrich_metadata discovers Manchester and JTS manifests as external image sources
- Ready for Plan 04 (web app UI integration) and Plan 05 (desktop app UI integration)
- Both Manchester LUNA and JTS Figgy manifests are standard IIIF v2, so existing fetch_external_iiif_data handles them without modification

## Self-Check: PASSED

- FOUND: shared/nli_crossref_service.py
- FOUND: genizah_core.py
- FOUND: tests/test_nli_crossref_service.py
- FOUND: 34-03-SUMMARY.md
- FOUND: commit 7cda65f7
- FOUND: commit f900497b
- All 55 tests pass

---
*Phase: 34-library-iiif-integration*
*Completed: 2026-02-16*
