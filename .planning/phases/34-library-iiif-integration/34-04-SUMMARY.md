---
phase: 34-library-iiif-integration
plan: 04
subsystem: web-ui
tags: [manchester, luna, jts, dpul, princeton, iiif, proxy, source-chips, browse, image-viewer]

# Dependency graph
requires:
  - phase: 34-03
    provides: "Manchester LUNA and JTS Figgy manifest discovery in enrich_metadata, external_provider key, get_image_sources"
provides:
  - "/api/manchester_image/{sys_id} proxy endpoint for Manchester LUNA IIIF images"
  - "/api/jts_image/{sys_id} proxy endpoint for JTS/Princeton Figgy IIIF images"
  - "Manchester source chip (pink #e91e63) with LUNA detail page external link"
  - "JTS source chip (orange #ff9800) with Princeton DPUL external link"
  - "Source switching between NLI and Manchester/JTS image sources"
  - "external_provider field on BrowsePage dataclass for UI differentiation"
affects: [34-05]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "External provider differentiation: external_provider key from nli_cache determines proxy endpoint and chip label"
    - "Mutual exclusivity: images_ext holds one provider at a time (Cambridge OR Manchester OR JTS)"
    - "Same proxy pattern for all libraries: cache dict, nli_cache lookup, IIIF Image API URL construction"

key-files:
  created: []
  modified:
    - web/api.py
    - web/pages/browse.py
    - web/services.py

key-decisions:
  - "Reuse cambridge_images field (images_ext) for all external providers -- external_provider key differentiates"
  - "Manchester pink (#e91e63), JTS orange (#ff9800) chip colors match plan specification"
  - "JTS and Oxford share orange color safely since manuscripts are mutually exclusive by library"
  - "Source chips show toggle button when both NLI and external images exist, external-link-only otherwise"

patterns-established:
  - "External image proxy: same cache/lookup/fetch pattern for Cambridge, Manchester, JTS"
  - "Source chip rendering: toggle when NLI + external available, external-link-only when single source"

# Metrics
duration: 4min
completed: 2026-02-16
---

# Phase 34 Plan 04: Web App Manchester/JTS Integration Summary

**Manchester LUNA and JTS/Princeton Figgy image proxy endpoints with source switching chips on the browse page viewer**

## Performance

- **Duration:** 4 min
- **Started:** 2026-02-16T04:08:38Z
- **Completed:** 2026-02-16T04:12:57Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- Added Manchester and JTS IIIF image proxy endpoints following the established Cambridge pattern
- Added Manchester (pink) and JTS (orange) source chips to browse page with external link buttons
- Implemented source switching between NLI and Manchester/JTS image sources
- Added external_provider field to BrowsePage dataclass for UI differentiation of image sources
- Updated ALLOWED_IMAGE_DOMAINS with Manchester LUNA and Princeton Figgy/IIIF-cloud domains

## Task Commits

Each task was committed atomically:

1. **Task 1: Add Manchester and JTS image proxy endpoints** - `52b39fd5` (feat)
2. **Task 2: Add Manchester and JTS source chips to browse page** - `cc05522b` (feat)

## Files Created/Modified
- `web/api.py` - Two new proxy endpoints (/api/manchester_image, /api/jts_image), ALLOWED_IMAGE_DOMAINS updated with 3 new domains
- `web/pages/browse.py` - Manchester and JTS source chips, switch_to_manchester/jts functions, image URL routing for both sources
- `web/services.py` - external_provider field on BrowsePage, propagated from nli_cache in both browse page constructors

## Decisions Made
- Reuse `cambridge_images` field (which holds `images_ext` from nli_cache) for all external providers -- the `external_provider` key differentiates which library's canvases are stored
- Manchester chip uses pink (#e91e63), JTS uses orange (#ff9800) following plan specification
- JTS and Oxford share the same orange color since manuscripts are mutually exclusive by library (never both JTS and Oxford)
- Source chips show as toggle buttons when both NLI and external images are available, and as external-link-only buttons when only one source exists

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Added external_provider field to BrowsePage dataclass**
- **Found during:** Task 2 (Browse page source chips)
- **Issue:** Plan mentioned checking external_provider but BrowsePage dataclass didn't have the field, and services.py didn't propagate it from nli_cache
- **Fix:** Added `external_provider: str = ''` to BrowsePage dataclass, propagated from `nli_cache[sys_id]['external_provider']` in both get_browse_page and get_browse_page_by_fl methods
- **Files modified:** web/services.py
- **Verification:** services.py parses, external_provider field accessible on page object
- **Committed in:** cc05522b (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (1 missing critical)
**Impact on plan:** Essential for correct source differentiation. No scope creep.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Web app fully supports Manchester LUNA and JTS/Princeton Figgy image viewing
- Source switching and external links work for all 4 image sources (NLI, Cambridge, Manchester, JTS)
- Ready for Plan 05 (desktop app Manchester/JTS integration)
- 55 NLI crossref tests pass with no regressions

## Self-Check: PASSED

- FOUND: web/api.py
- FOUND: web/pages/browse.py
- FOUND: web/services.py
- FOUND: 34-04-SUMMARY.md
- FOUND: commit 52b39fd5
- FOUND: commit cc05522b
- All 55 NLI crossref tests pass

---
*Phase: 34-library-iiif-integration*
*Completed: 2026-02-16*
