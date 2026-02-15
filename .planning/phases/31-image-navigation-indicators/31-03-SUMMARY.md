---
phase: 31-image-navigation-indicators
plan: 03
subsystem: ui
tags: [iiif, cambridge, image-proxy, source-switching, nicegui]

requires:
  - phase: 31-01
    provides: Web folio navigation and source indicator chips
  - phase: 31-02
    provides: Desktop folio navigation and source switching (reference implementation)
  - phase: 30
    provides: NLI crossref sidecar with Cambridge manifest lookup
provides:
  - Web source switching between NLI and Cambridge images within viewer
  - Cambridge IIIF image proxy endpoint with caching
  - BrowsePage cambridge_images field for canvas URL access
affects: [browse, image-viewer, phase-32]

tech-stack:
  added: []
  patterns:
    - "Source switching via active_source state on BrowseState"
    - "Cambridge image proxy following Oxford proxy pattern (cache + timeout + IIIF)"
    - "Toggle chips: filled active, outlined inactive, external-link icon always available"

key-files:
  created: []
  modified:
    - web/api.py
    - web/services.py
    - web/pages/browse.py

key-decisions:
  - "Cambridge proxy fetches images_ext canvas URLs from nli_cache (populated by enrich_metadata) -- no new network fetches needed for canvas discovery"
  - "Source chips become toggles only when both NLI and Cambridge are available; single-source manuscripts keep external-link-only behavior"
  - "External viewer links preserved as small open_in_new icon buttons next to each chip"

duration: 3min
completed: 2026-02-15
---

# Phase 31 Plan 03: Web Source Switching Summary

**Cambridge/NLI image source toggle via styled chips with cached IIIF proxy endpoint**

## Performance

- **Duration:** 3 min
- **Started:** 2026-02-15T17:18:45Z
- **Completed:** 2026-02-15T17:22:26Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- Cambridge IIIF image proxy endpoint at `/api/cambridge_image/{sys_id}` with cache (mirrors Oxford proxy pattern)
- Source chips toggle active image source when both NLI and Cambridge available -- active chip filled, inactive outlined
- External viewer links preserved as small icon buttons next to source chips
- Source selection persists across folio navigation (Prev/Next/dropdown)
- Closes verification gap #4 from 31-VERIFICATION.md (web source switching)

## Task Commits

Each task was committed atomically:

1. **Task 1: Add Cambridge image proxy and BrowsePage cambridge_images field** - `a916cb93` (feat)
2. **Task 2: Implement source switching logic in web browse page** - `23e5af8c` (feat)

## Files Created/Modified
- `web/api.py` - Cambridge IIIF image proxy endpoint with cache, follows Oxford proxy pattern
- `web/services.py` - BrowsePage.cambridge_images field, populated from nli_cache images_ext in both get_browse_page methods
- `web/pages/browse.py` - active_source state, source switching handlers, toggle chip styling, Cambridge image URL override

## Decisions Made
- Cambridge proxy fetches from nli_cache images_ext (populated by enrich_metadata on first visit) rather than adding new canvas discovery logic
- Source chips only become toggles when both NLI and Cambridge images are available; single-source manuscripts keep the simpler external-link-only behavior
- External viewer links moved to small icon buttons next to chips, preserving access regardless of toggle state

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Phase 31 complete with all 3 plans delivered (gap closure plan closes the single verification gap)
- Ready for Phase 32 (Metadata Display)

## Self-Check: PASSED

All files exist on disk. All commit hashes verified in git log.

---
*Phase: 31-image-navigation-indicators*
*Completed: 2026-02-15*
