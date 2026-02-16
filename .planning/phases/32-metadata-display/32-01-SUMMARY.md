---
phase: 32-metadata-display
plan: 01
subsystem: ui
tags: [nli-crossref, metadata, browse, library-urls, iiif]

# Dependency graph
requires:
  - phase: 29-data-infrastructure
    provides: NLI crossref sidecar with nli_images table and NliCrossrefService
  - phase: 31-image-navigation-indicators
    provides: BrowsePage with image_source_info/folio_images fields, crossref enrichment in get_browse_page
provides:
  - get_library_viewer_url() method for CUL, JTS, Manchester, BL digital collection URLs
  - physical_metadata and library_viewer_url fields on BrowsePage dataclass
  - Material type, folio count, and library link display in web browse metadata panel
  - Library link button in compact browse header
affects: [32-metadata-display, desktop-browse]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Library URL construction via NLI crossref LibraryAbbrev mapping"
    - "Guard pattern: skip library link when Oxford/Cambridge already shown via existing path"

key-files:
  created: []
  modified:
    - shared/nli_crossref_service.py
    - tests/test_nli_crossref_service.py
    - web/services.py
    - web/pages/browse.py
    - genizah_translations.py

key-decisions:
  - "Search-based fallback URLs for CUL, JTS, Manchester (no reliable direct-link patterns)"
  - "Guard against duplicate links: skip library_viewer_url when is_oxford/is_cambridge with existing external_url"
  - "Material value passed through tr() for Hebrew translation of Paper/Parchment/Vellum"

patterns-established:
  - "Library URL construction: map LibraryAbbrev to search URL pattern"
  - "Physical metadata enrichment: populate alongside folio_images in same crossref try/except block"

# Metrics
duration: 3min
completed: 2026-02-16
---

# Phase 32 Plan 01: Metadata Display Summary

**Physical metadata (material, folios) and library digital collection links in web browse via NLI crossref enrichment**

## Performance

- **Duration:** 3 min
- **Started:** 2026-02-16T01:17:33Z
- **Completed:** 2026-02-16T01:21:08Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments
- NliCrossrefService.get_library_viewer_url() constructs URLs for CUL, JTS, Manchester, and BL digital collections
- BrowsePage dataclass extended with physical_metadata and library_viewer_url, populated in both get_browse_page and get_browse_page_by_fl
- Web browse metadata panel shows material type (Paper/Parchment/Vellum) and folio/bifolio counts from NLI crossref
- Library link appears in both compact header and external links section, with guard against duplicating Oxford/Cambridge links
- 5 new tests for library URL construction (37 total, all passing)
- Hebrew translations added for Material, Paper, Parchment, Vellum, Papyrus, Folios, Bifolios, Size, View in Library Catalog, Material Type

## Task Commits

Each task was committed atomically:

1. **Task 1: Add library viewer URL helper and physical metadata enrichment** - `852e68b1` (feat)
2. **Task 2: Display physical metadata and library links in browse panel** - `d686b955` (feat)

## Files Created/Modified
- `shared/nli_crossref_service.py` - Added get_library_viewer_url() with URL construction for 4 library types
- `tests/test_nli_crossref_service.py` - 5 new tests for library URL (CUL, Manchester, BL, unknown, missing)
- `web/services.py` - Added physical_metadata and library_viewer_url to BrowsePage, populated in both browse methods
- `web/pages/browse.py` - Material, folio count display in metadata grid; library link in header and external links
- `genizah_translations.py` - 11 new Hebrew translation entries for metadata display strings

## Decisions Made
- Used search-based fallback URLs for all libraries (no reliable direct-link patterns available for CUL CUDL label construction from shelfmarks, JTS identifiers, or Manchester LUNA collection IDs)
- Guard pattern checks both is_oxford/is_cambridge AND external_url to avoid showing duplicate links
- Material value passed through tr() to support Hebrew translation of known material types (Paper, Parchment, Vellum, Papyrus)

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- Physical metadata and library links now visible on web browse for manuscripts with NLI crossref data
- Desktop app integration (32-02) can follow the same pattern using get_library_viewer_url() and get_physical_metadata()
- KTIV link continues to work in both header and metadata panel

## Self-Check: PASSED

- All 6 files verified present on disk
- Both commit hashes (852e68b1, d686b955) verified in git log
- Must-have artifacts confirmed: get_library_viewer_url in service, physical_metadata in BrowsePage, material display in browse.py, 5 test functions

---
*Phase: 32-metadata-display*
*Completed: 2026-02-16*
