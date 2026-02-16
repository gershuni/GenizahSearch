---
phase: 33-metadata-enrichment
plan: 02
subsystem: service-layer
tags: [sqlite, bibliography, catalog-refs, crossref, service, metadata, enrichment]

# Dependency graph
requires:
  - phase: 33-01
    provides: "bibliography (542K rows) and catalog_refs (64K rows) tables in fjms_enrichment.db v2.0.0"
  - phase: 29-02
    provides: "NliCrossrefService with nli_images table containing IsNotGenizah, CatalogEntry, CollectionName, OBBox/Volume/Folio columns"
provides:
  - "FjmsService.get_bibliography() for denormalized bibliography lookups"
  - "FjmsService.get_catalog_refs() for scholarly catalog cross-references"
  - "FjmsService.get_source_names() for filtered scholarly source classifications"
  - "NliCrossrefService.get_is_not_genizah() for non-Genizah badge"
  - "NliCrossrefService.get_catalog_entry() for Neubauer-Cowley references"
  - "NliCrossrefService.get_collection_storage() for physical storage references"
  - "enrich_metadata populates current_meta with bibliography, catalog_refs, source_names, is_not_genizah, catalog_entry, collection_storage"
affects: [33-03, 33-04, web_browse_page, desktop_browse_tab]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Phase 33 metadata enrichment: NLI crossref + FJMS sidecar both queried in enrich_metadata"
    - "_get_fjms_service() lazy accessor following _get_crossref_service() pattern"

key-files:
  created: []
  modified:
    - "shared/fjms_service.py"
    - "shared/nli_crossref_service.py"
    - "genizah_core.py"
    - "tests/test_fjms_service.py"
    - "tests/test_nli_crossref_service.py"

key-decisions:
  - "FJMS bibliography uses CASE ordering: Discussion > Mentioned > others, then by RunningTitle"
  - "Generic source names (Catalogs, Institution, Collection, Other) filtered from get_source_names"
  - "_get_fjms_service() added as module-level lazy accessor with thread_safe=True, matching _get_crossref_service() pattern"
  - "NLI crossref metadata (is_not_genizah, catalog_entry, collection_storage) added inside existing crossref try block"
  - "FJMS metadata (bibliography, catalog_refs, source_names) added as separate block after crossref block"

patterns-established:
  - "Dual-sidecar enrichment: enrich_metadata queries both nli_crossref.db and fjms_enrichment.db in single pass"

# Metrics
duration: 6min
completed: 2026-02-16
---

# Phase 33 Plan 02: Service Layer & enrich_metadata Wiring Summary

**Bibliography, catalog refs, source names, IsNotGenizah flag, Neubauer-Cowley entry, and collection storage methods added to service layer and wired into enrich_metadata for both apps**

## Performance

- **Duration:** 6 min
- **Started:** 2026-02-16T07:27:41Z
- **Completed:** 2026-02-16T07:34:28Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments
- Added 3 new FjmsService methods (get_bibliography, get_catalog_refs, get_source_names) with graceful error handling and missing-table tolerance
- Added 3 new NliCrossrefService methods (get_is_not_genizah, get_catalog_entry, get_collection_storage) with same graceful degradation pattern
- Wired all 6 new data fields into genizah_core.py's enrich_metadata, making them available to both web and desktop apps through current_meta dict
- Added 15 new unit tests (9 for FJMS, 6 for NLI crossref) covering happy paths, empty results, missing tables, and generic filtering
- Verified with real data: bibliography entries sort Discussion first, IsNotGenizah flags correctly, catalog entries and collection storage populated

## Task Commits

Each task was committed atomically:

1. **Task 1: Add FjmsService and NliCrossrefService methods + tests** - `69b28a05` (feat)
2. **Task 2: Wire new service methods into enrich_metadata** - `0785c256` (feat)

## Files Created/Modified
- `shared/fjms_service.py` - Added get_bibliography(), get_catalog_refs(), get_source_names() with GENERIC_SOURCE_NAMES filter
- `shared/nli_crossref_service.py` - Added get_is_not_genizah(), get_catalog_entry(), get_collection_storage()
- `genizah_core.py` - Added _get_fjms_service() lazy accessor, Phase 33 enrichment blocks in enrich_metadata
- `tests/test_fjms_service.py` - 9 new tests for bibliography, catalog_refs, source_names (including missing-table tolerance)
- `tests/test_nli_crossref_service.py` - 6 new tests for IsNotGenizah, catalog entry, collection storage

## Decisions Made
- **Bibliography ordering:** CASE expression puts Discussion entries first, then Mentioned, then others -- most useful scholarly citations appear first
- **Generic source name filtering:** frozenset {'Catalogs', 'Institution', 'Collection', 'Other'} excluded from get_source_names -- these don't provide meaningful scholarly classification
- **Lazy accessor pattern:** _get_fjms_service() follows exact same pattern as _get_crossref_service() -- module-level singleton with thread_safe=True for NiceGUI web app
- **Enrichment placement:** NLI crossref fields (is_not_genizah, catalog_entry, collection_storage) added inside existing crossref try block; FJMS fields (bibliography, catalog_refs, source_names) in separate block after it
- **Test fixture version bump:** Changed test fixture from 1.0.0 to 2.0.0 to match sidecar version with bibliography/catalog_refs tables

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- All 6 metadata fields now populated in current_meta for any manuscript with FJMS or NLI crossref data
- Plan 03 can build web UI sections consuming bibliography, catalog_refs, is_not_genizah, catalog_entry, collection_storage, source_names
- Plan 04 can build desktop UI sections using same data fields

## Self-Check: PASSED

- shared/fjms_service.py: FOUND
- shared/nli_crossref_service.py: FOUND
- genizah_core.py: FOUND
- tests/test_fjms_service.py: FOUND
- tests/test_nli_crossref_service.py: FOUND
- 33-02-SUMMARY.md: FOUND
- Commit 69b28a05: FOUND
- Commit 0785c256: FOUND
- All 6 service methods: VERIFIED
- _get_fjms_service accessor: VERIFIED

---
*Phase: 33-metadata-enrichment*
*Completed: 2026-02-16*
