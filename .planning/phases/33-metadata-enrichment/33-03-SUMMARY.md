---
phase: 33-metadata-enrichment
plan: 03
subsystem: web-ui
tags: [browse, bibliography, catalog-refs, badges, metadata, nicegui, collapsible]

# Dependency graph
requires:
  - phase: 33-02
    provides: "FjmsService.get_bibliography(), get_catalog_refs(), get_source_names(); NliCrossrefService.get_is_not_genizah(), get_catalog_entry(), get_collection_storage()"
provides:
  - "Browse page IsNotGenizah orange badge for flagged manuscripts"
  - "Browse page Neubauer-Cowley catalog number display for Oxford manuscripts"
  - "Browse page collapsible bibliography section with FJMS badge, mention type badges, transcription/translation badges"
  - "Browse page catalog cross-references section with catalog acronym and entry number"
  - "Browse page scholarly source names section"
  - "Browse page collection & storage section with box, volume, folio references"
affects: [33-04, web_browse_page]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Phase 33 web browse: direct service calls for metadata (same pattern as FJMS catalog/domains)"
    - "Helper function format_bib_entry for bibliography formatting with page references"
    - "Collapsible expansion pattern: first N entries shown, remainder in ui.expansion"

key-files:
  created: []
  modified:
    - "web/pages/browse.py"

key-decisions:
  - "Service methods called directly in browse page (not via BrowsePage dataclass) -- matches existing FJMS catalog/domains pattern"
  - "NLI crossref metadata fetched once at shelfmark display for reuse in collection/storage section below"
  - "format_bib_entry helper defined inline within rendering block for locality"
  - "_render_bib_entry helper avoids code duplication between initial and expanded entry rendering"
  - "Variable names vol_cs/folio_cs used to avoid shadowing page-level 'page' variable and other locals"

patterns-established:
  - "Phase 33 metadata sections: placed after FJMS domains, before Related Fragments in browse panel"

# Metrics
duration: 8min
completed: 2026-02-16
---

# Phase 33 Plan 03: Web Browse Scholarly Metadata Summary

**IsNotGenizah badge, Neubauer-Cowley catalog entry, collapsible bibliography with mention type badges, catalog cross-references, scholarly sources, and collection storage added to web browse page**

## Performance

- **Duration:** 8 min
- **Started:** 2026-02-16T07:38:58Z
- **Completed:** 2026-02-16T07:46:48Z
- **Tasks:** 2
- **Files modified:** 1

## Accomplishments
- Added IsNotGenizah orange outline badge next to shelfmark for manuscripts flagged as not from the Cairo Genizah (304K flagged records in crossref)
- Added Neubauer-Cowley catalog entry display below shelfmark in tertiary text for Oxford manuscripts
- Added collapsible bibliography section with FJMS badge, count badge, and per-entry mention type (Discussion/Mentioned/Index), transcription type, and translation badges
- Added catalog cross-references section showing catalog acronym and entry number (e.g., "Neubauer-Cowley #2918")
- Added scholarly source names section with inline labels
- Added collection & storage section showing collection name, box, volume, and folio references
- All sections render only when data is available and degrade gracefully when absent

## Task Commits

Each task was committed atomically:

1. **Task 1: Add IsNotGenizah badge and Neubauer-Cowley display near shelfmark** - `4a2ae306` (feat)
2. **Task 2: Add bibliography, catalog refs, source names, and collection/storage sections** - `2d1bb7da` (feat)

## Files Created/Modified
- `web/pages/browse.py` - Added 6 new metadata display sections: IsNotGenizah badge, Neubauer-Cowley catalog entry, bibliography references (collapsible), catalog cross-references, scholarly source names, collection & storage

## Decisions Made
- **Direct service calls:** Service methods called directly in browse page rather than adding fields to BrowsePage dataclass -- matches existing FJMS catalog/domains pattern where data is fetched inline
- **Single crossref fetch:** NLI crossref metadata (is_not_genizah, catalog_entry, collection_storage) fetched once at shelfmark display area, reused in collection/storage section below
- **Inline helper functions:** format_bib_entry and _render_bib_entry defined within rendering block for locality and to avoid code duplication between initial and expanded entry lists
- **Variable naming:** Used vol_cs/folio_cs to avoid shadowing the page-level variables

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- All 6 categories of scholarly metadata now display on the web browse page
- Plan 04 can build the same sections for the desktop app using identical service methods
- Service methods (from Plan 02) provide data to both apps through the same API

## Self-Check: PASSED

- web/pages/browse.py: FOUND
- Commit 4a2ae306: FOUND
- Commit 2d1bb7da: FOUND
- IsNotGenizah badge code: VERIFIED
- Neubauer-Cowley display code: VERIFIED
- Bibliography section code: VERIFIED
- Catalog cross-references code: VERIFIED
- Scholarly sources code: VERIFIED
- Collection & storage code: VERIFIED

---
*Phase: 33-metadata-enrichment*
*Completed: 2026-02-16*
