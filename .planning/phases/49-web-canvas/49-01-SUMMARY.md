---
phase: 49-web-canvas
plan: 01
subsystem: ui
tags: [fabric-js, canvas, puzzle, nicegui, iiif, image-proxy]

requires:
  - phase: 47-shared-engine
    provides: PuzzleImageService (IIIF fetch + bg removal + disk cache)
  - phase: 47-shared-engine
    provides: PuzzleFragment/PuzzleDocument data models
  - phase: 48-desktop-canvas
    provides: Desktop puzzle interaction model (validation of UX patterns)
provides:
  - "/puzzle page with Fabric.js canvas for fragment manipulation"
  - "/api/puzzle_image endpoint wrapping PuzzleImageService"
  - "/api/puzzle_folios/{sys_id} endpoint returning ordered FL ID list"
  - "Nav menu integration with Fragment Puzzle link"
  - "Session state persistence via app.storage.tab"
  - "Snap guides for edge/center alignment"
  - "Folio navigation via API"
affects: [49-02 (add-to-puzzle buttons), 50-join-documents, 51-recto-verso]

tech-stack:
  added: [fabric-js-6.4.3-cdn]
  patterns: [window.puzzleCanvas JS global object, server-side image proxy for CORS-free canvas images, app.storage.tab session persistence]

key-files:
  created:
    - web/pages/puzzle.py
    - tests/test_puzzle_web_api.py
  modified:
    - web/api.py
    - web/main.py
    - genizah_translations.py

key-decisions:
  - "Used fetch_fl_ids_from_nli (IIIF manifest FL IDs) instead of nli_crossref fgp_image_number_id for puzzle_folios endpoint -- FGP numbers are NOT NLI FL IDs"
  - "Folio labels use recto/verso pattern (1r, 1v, 2r, 2v) based on page index parity"
  - "Snap guides included (CANV-08 nice-to-have) with 8px threshold, edge/center alignment"
  - "Folio navigation built into JS object with async image swap preserving position/rotation/scale"

patterns-established:
  - "window.puzzleCanvas global JS object: matches window.manuscriptViewer pattern from browse.py"
  - "Server-side image proxy for canvas: /api/puzzle_image avoids all CORS issues with NLI/Cambridge IIIF"
  - "Fabric.js flipX/flipY properties instead of negative scaleX/scaleY for correct flip behavior"
  - "perPixelTargetFind: true for transparent PNG click-through on bg-removed images"

requirements-completed: [PLAT-01]

duration: 12min
completed: 2026-03-16
---

# Phase 49 Plan 01: Web Canvas API + Puzzle Page Summary

**Fabric.js puzzle canvas at /puzzle with server-side image proxy, full spatial manipulation (drag/rotate/flip/resize/zoom/pan), 6 background modes, keyboard shortcuts, snap guides, folio navigation, and session persistence**

## Performance

- **Duration:** 12 min
- **Started:** 2026-03-16T11:07:01Z
- **Completed:** 2026-03-16T11:19:00Z
- **Tasks:** 2
- **Files modified:** 5

## Accomplishments
- /api/puzzle_image endpoint serving processed PNG or original JPEG via PuzzleImageService with disk caching
- /api/puzzle_folios/{sys_id} returning ordered NLI FL IDs from IIIF manifest
- Full Fabric.js puzzle page with drag, rotate, flip, resize, zoom, pan, multi-select
- All 6 background modes: dark gray, black, white, checkerboard, light table, grid
- Keyboard shortcuts: Delete, arrows (1px/10px), R/Shift+R rotate
- Right-click context menu: Flip H/V, Delete, Toggle Background
- Snap guides for edge and center alignment (8px threshold)
- Folio navigation (prev/next) via /api/puzzle_folios with image swap
- Session state auto-saved every 30s, restored on page revisit
- Nav menu "Fragment Puzzle" link with puzzle icon
- Hebrew translations for all puzzle UI strings
- 11 API tests passing, full suite (814 tests) green

## Task Commits

Each task was committed atomically:

1. **Task 1: API endpoints for puzzle images and folios** - `133f93f3` (feat)
2. **Task 2: Puzzle page with Fabric.js canvas, full manipulation, nav integration** - `57287357` (feat)

## Files Created/Modified
- `web/pages/puzzle.py` - Full Fabric.js puzzle page (canvas, toolbar, sliders, JS bridge)
- `web/api.py` - /api/puzzle_image and /api/puzzle_folios endpoints
- `web/main.py` - /puzzle route + nav menu entry
- `genizah_translations.py` - Hebrew translations for puzzle UI strings
- `tests/test_puzzle_web_api.py` - 11 tests for puzzle API endpoints

## Decisions Made
- Used `fetch_fl_ids_from_nli()` (IIIF manifest) for puzzle_folios instead of `nli_crossref_service.get_folio_images()` which returns FGP image numbers (NOT FL IDs). This was a necessary correction to the plan's interface specification.
- Folio labels assigned as recto/verso (1r, 1v, 2r, 2v) based on page index parity, matching manuscript convention.
- Snap guides included despite being "nice-to-have" priority -- implementation was straightforward with Fabric.js object:moving handler.
- httpx used for internal API call (shelfmark -> folios) to avoid blocking the event loop.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed folio endpoint data source**
- **Found during:** Task 1 (API endpoints)
- **Issue:** Plan specified using `nli_crossref_service.get_folio_images()` which returns `fgp_image_number_id` (Friedberg photo numbers), NOT NLI IIIF FL IDs needed by /api/puzzle_image
- **Fix:** Used `fetch_fl_ids_from_nli()` which resolves real NLI FL IDs from IIIF manifest
- **Files modified:** web/api.py
- **Verification:** Tests pass, FL IDs are valid for puzzle_image endpoint
- **Committed in:** 133f93f3

**2. [Rule 2 - Missing Critical] Added snap guides (CANV-08)**
- **Found during:** Task 2 (Puzzle page)
- **Issue:** CANV-08 snap guides were nice-to-have but trivial to implement with Fabric.js
- **Fix:** Added setupSnapGuides() with 8px threshold for left/right/top/bottom/center alignment
- **Files modified:** web/pages/puzzle.py
- **Committed in:** 57287357

**3. [Rule 2 - Missing Critical] Added folio navigation (CANV-07)**
- **Found during:** Task 2 (Puzzle page)
- **Issue:** Folio prev/next navigation needed for full puzzle workflow
- **Fix:** Added folioData tracking, loadFolios(), navigateFolio() with async image swap
- **Files modified:** web/pages/puzzle.py
- **Committed in:** 57287357

---

**Total deviations:** 3 auto-fixed (1 bug fix, 2 missing critical)
**Impact on plan:** All fixes necessary for correctness and feature completeness. FL ID fix prevents broken image loading. Snap guides and folio nav add planned Phase 49 requirements.

## Issues Encountered
None -- implementation followed established patterns from browse.py and desktop puzzle.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Puzzle canvas fully functional at /puzzle
- "Add to Puzzle" buttons needed on Browse, Search results, and Lists pages (49-02-PLAN)
- Save/load puzzle documents planned for Phase 50

---
*Phase: 49-web-canvas*
*Completed: 2026-03-16*
