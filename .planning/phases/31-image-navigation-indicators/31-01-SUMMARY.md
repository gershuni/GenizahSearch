---
phase: 31-image-navigation-indicators
plan: 01
subsystem: ui
tags: [folio-navigation, source-indicators, nli-crossref, iiif, nicegui]

# Dependency graph
requires:
  - phase: 29-data-infrastructure
    provides: "NliCrossrefService with get_images, get_image_sources"
  - phase: 30-direct-image-access
    provides: "FL ID resolution from sidecar, image proxy endpoints"
provides:
  - "parse_folio_label() for NLI ImageName -> folio notation (1r/1v)"
  - "get_folio_images() returning enriched image dicts with folio_label"
  - "BrowsePage.folio_label, image_source_info, folio_images fields"
  - "Web browse page folio navigation bar with dropdown"
  - "Clickable source indicator chips (NLI, CUDL, Oxford)"
affects: [31-02, 32-metadata-display, 33-fragment-relationships]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Folio label extraction via regex on NLI ImageName L/F/B/S pattern"
    - "Source indicator chips as styled NiceGUI buttons with border-radius badges"
    - "Folio dropdown using ui.select with {p_num: label} options dict"

key-files:
  created: []
  modified:
    - shared/nli_crossref_service.py
    - tests/test_nli_crossref_service.py
    - web/services.py
    - web/pages/browse.py
    - genizah_translations.py

key-decisions:
  - "Folio label falls back to extract_folio_number then Page N when crossref unavailable"
  - "Source indicators are flat NiceGUI buttons styled as outlined chips rather than QLabel badges"
  - "Folio dropdown maps string p_num keys to folio labels, falls back to number input when no folio data"
  - "Single-page detection uses both image_count and total_pages to disable navigation"

patterns-established:
  - "NLI crossref enrichment in get_browse_page with try/except guard for graceful degradation"
  - "Source indicator chip pattern: colored border, dense flat button, tooltip, window.open on click"

# Metrics
duration: 5min
completed: 2026-02-15
---

# Phase 31 Plan 01: Image Navigation & Source Indicators Summary

**Folio label parsing from NLI ImageName patterns with navigation dropdown and clickable NLI/CUDL/Oxford source indicator chips on web browse page**

## Performance

- **Duration:** 5 min
- **Started:** 2026-02-15T16:24:39Z
- **Completed:** 2026-02-15T16:30:34Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments
- parse_folio_label extracts recto/verso notation (1r, 1v, 2r) from NLI ImageName L/F/B/S pattern
- get_folio_images enriches image records with folio_label, with sequential fallback for unrecognized patterns
- Web browse page shows "Folio 1r of 12 pages" with source chips (NLI green, CUDL blue, Oxford amber)
- Folio dropdown replaces numeric page input when crossref data is available
- Single-page manuscripts have disabled navigation arrows

## Task Commits

Each task was committed atomically:

1. **Task 1: Add folio label parsing and folio-ready image query** - `a8dee840` (feat)
2. **Task 2: Add folio navigation bar and source indicators to browse page** - `9bbf3825` (feat)

## Files Created/Modified
- `shared/nli_crossref_service.py` - Added parse_folio_label() function and get_folio_images() method
- `tests/test_nli_crossref_service.py` - 7 new tests (32 total), updated test fixture ImageNames to real NLI pattern
- `web/services.py` - Added folio_label, image_source_info, folio_images to BrowsePage; NLI crossref enrichment in both get_browse_page methods
- `web/pages/browse.py` - Folio navigation bar: label display, page count, source chips, folio dropdown, single-page handling
- `genizah_translations.py` - Hebrew translations for source indicator tooltips

## Decisions Made
- Folio label has three-tier fallback: crossref folio_label -> extract_folio_number (header regex) -> "Page N"
- Source indicator chips use styled flat buttons (not NiceGUI chip component) for consistent cross-browser appearance
- Cambridge/CUDL detection now combines MARC cache check AND sidecar crossref check for better coverage
- Folio dropdown uses string keys to avoid NiceGUI type coercion issues with numeric select values

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Added Hebrew translations for source indicator tooltips**
- **Found during:** Task 2
- **Issue:** Plan did not specify adding translations for the new tooltip strings
- **Fix:** Added "Open in NLI KTIV", "Open in Cambridge Digital Library", "Open in Bodleian Libraries" to genizah_translations.py
- **Files modified:** genizah_translations.py
- **Verification:** All strings have Hebrew translations; tr() returns English fallback for untranslated
- **Committed in:** 9bbf3825 (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (1 missing critical)
**Impact on plan:** Essential for Hebrew-language users. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Folio navigation and source indicators are live on the web browse page
- Phase 31 Plan 02 (desktop parity) can reuse parse_folio_label and get_folio_images from the shared service layer
- image_source_info provides the data needed for Phase 32 metadata display

## Self-Check: PASSED

All files verified present. All commit hashes verified in git log.

---
*Phase: 31-image-navigation-indicators*
*Completed: 2026-02-15*
