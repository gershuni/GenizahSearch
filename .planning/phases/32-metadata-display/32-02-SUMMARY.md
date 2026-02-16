---
phase: 32-metadata-display
plan: 02
subsystem: ui
tags: [nli-crossref, metadata, desktop, browse, library-urls, pyqt6]

# Dependency graph
requires:
  - phase: 32-metadata-display
    plan: 01
    provides: get_physical_metadata() and get_library_viewer_url() in NliCrossrefService, Hebrew translations
  - phase: 31-image-navigation-indicators
    provides: crossref enrichment block in enrich_metadata, ManuscriptViewerWidget KTIV button
provides:
  - physical_metadata and library_viewer_url populated in enrich_metadata for both web and desktop
  - Desktop browse extended info panel shows material type, folio/bifolio counts, size, and library link
affects: [desktop-browse, manuscript-viewer]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Keyword args for optional enrichment data in _build_browse_enriched_html"
    - "phys_html prepended to enriched HTML for consistent display ordering"

key-files:
  created: []
  modified:
    - genizah_core.py
    - genizah_app.py

key-decisions:
  - "Physical metadata and library URL added to same crossref try/except block for unified error handling"
  - "phys_html prepended before KTI/Oxford/Cambridge table for consistent top-of-panel display"
  - "Library link uses existing _on_browse_ext_link_clicked handler (already supports http URLs via QDesktopServices)"

patterns-established:
  - "Optional keyword args for enrichment data in desktop HTML builders"

# Metrics
duration: 2min
completed: 2026-02-16
---

# Phase 32 Plan 02: Desktop Metadata Display Summary

**Physical metadata (material, folios, size) and library digital collection links in desktop browse extended info panel via enrich_metadata enrichment**

## Performance

- **Duration:** 2 min
- **Started:** 2026-02-16T01:23:58Z
- **Completed:** 2026-02-16T01:26:15Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- enrich_metadata now populates physical_metadata and library_viewer_url from NLI crossref sidecar, making data available to both web and desktop
- Desktop browse extended info panel shows material type (Paper/Parchment/Vellum with tr() translation), folio/bifolio counts, size, and library digital collection link
- Library link clickable via existing QDesktopServices handler -- opens CUL, JTS, Manchester, BL search URLs in browser
- Graceful degradation: no empty sections shown when crossref data unavailable
- KTIV button in ManuscriptViewerWidget completely unaffected (separate component)

## Task Commits

Each task was committed atomically:

1. **Task 1: Add physical metadata and library viewer URL to enrich_metadata** - `663e1ef5` (feat)
2. **Task 2: Display physical metadata and library link in desktop browse panel** - `273f7436` (feat)

## Files Created/Modified
- `genizah_core.py` - Added physical_metadata and library_viewer_url enrichment in crossref block of enrich_metadata
- `genizah_app.py` - _build_browse_enriched_html accepts and renders physical metadata and library link; on_browse_enriched_loaded passes data from meta dict

## Decisions Made
- Physical metadata and library URL queries added to the existing crossref try/except block rather than a new block -- same error handling, no duplicate availability checks
- phys_html prepended before KTI/Oxford/Cambridge enrichment table so physical metadata appears at the top of the extended info panel
- Library links reuse the existing `_on_browse_ext_link_clicked` handler which already routes `http` URLs to QDesktopServices.openUrl

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- All 4 META requirements (META-01 through META-04) now satisfied in both web (Plan 01) and desktop (Plan 02)
- Physical metadata and library links visible wherever NLI crossref data exists
- Phase 32 complete -- ready for Phase 33

## Self-Check: PASSED

- All 2 modified files verified present on disk
- Both commit hashes (663e1ef5, 273f7436) verified in git log
- Must-have artifacts confirmed: physical_metadata in enrich_metadata, physical_metadata rendering in _build_browse_enriched_html, library_viewer_url passing in on_browse_enriched_loaded

---
*Phase: 32-metadata-display*
*Completed: 2026-02-16*
