---
phase: 34-library-iiif-integration
plan: 05
subsystem: desktop-ui
tags: [manchester, luna, jts, princeton, dpul, iiif, pyqt6, desktop, manuscript-viewer]

# Dependency graph
requires:
  - phase: 34-03
    provides: "enrich_metadata sets external_provider, library_viewer_url, images_ext for Manchester/JTS"
provides:
  - "Desktop ManuscriptViewerWidget detects Manchester and JTS as external image providers"
  - "Source combo box shows Manchester/JTS/Princeton labels alongside NLI"
  - "External button opens LUNA detail page for Manchester and DPUL catalog for JTS"
affects: []

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "external_provider key from enrich_metadata drives desktop UI labeling (same as web)"
    - "library_viewer_url preferred over raw manifest URL for Manchester/JTS external button"

key-files:
  created: []
  modified:
    - genizah_app.py

key-decisions:
  - "Prefer library_viewer_url for Manchester/JTS external button -- opens detail/catalog page instead of raw IIIF manifest"
  - "external_provider explicit key checked first in _detect_external_provider, URL-based detection as fallback"
  - "No special IIIF headers needed for Manchester LUNA or JTS Figgy -- standard direct fetch works"

patterns-established:
  - "Provider detection: explicit key first, then URL pattern matching as fallback"

# Metrics
duration: 2min
completed: 2026-02-16
---

# Phase 34 Plan 05: Desktop ManuscriptViewer Manchester/JTS Integration Summary

**Manchester LUNA and JTS/Princeton IIIF image source switching in desktop ManuscriptViewerWidget with detail page external links**

## Performance

- **Duration:** 2 min
- **Started:** 2026-02-16T04:08:32Z
- **Completed:** 2026-02-16T04:10:43Z
- **Tasks:** 2
- **Files modified:** 1

## Accomplishments
- Updated _detect_external_provider to check explicit external_provider key from enrich_metadata and detect Manchester/JTS URLs
- Added Manchester and JTS/Princeton labels to the source combo box dropdown
- External button opens LUNA detail page for Manchester and DPUL catalog for JTS via library_viewer_url
- Verified BL links remain unchanged as searcharchives.bl.uk search URLs (SC7)
- Full test suite passes (580 passed, 2 pre-existing failures in unrelated Responsa explosion guard tests)

## Task Commits

Each task was committed atomically:

1. **Task 1: Update ManuscriptViewerWidget for Manchester and JTS sources** - `215d18d0` (feat)
2. **Task 2: Verify BL remains as search URL and run full test suite** - verification only, no code changes

## Files Created/Modified
- `genizah_app.py` - _detect_external_provider with Manchester/JTS URL detection and explicit provider key, combo box labels, external button labels, library_viewer_url preference for Manchester/JTS

## Decisions Made
- Prefer library_viewer_url (detail/catalog page) over raw manifest URL for Manchester and JTS external button -- better user experience
- Check explicit external_provider key first in _detect_external_provider, falling back to URL pattern matching for backward compatibility
- No special headers needed for Manchester LUNA or JTS Figgy IIIF endpoints -- standard desktop direct fetch works

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Phase 34 (Library IIIF Integration) is now complete: all 5 plans executed
- Manchester LUNA and JTS/Princeton IIIF images integrated in both web (Plan 04) and desktop (Plan 05) apps
- Service layer, web UI, and desktop UI all support Manchester and JTS alongside existing NLI, Cambridge, and Oxford sources

## Self-Check: PASSED

- FOUND: genizah_app.py
- FOUND: 34-05-SUMMARY.md
- FOUND: commit 215d18d0
- All 580 tests pass (2 pre-existing failures in unrelated Responsa tests)

---
*Phase: 34-library-iiif-integration*
*Completed: 2026-02-16*
