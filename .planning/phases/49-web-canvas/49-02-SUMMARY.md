---
phase: 49-web-canvas
plan: 02
subsystem: ui
tags: [fabric-js, canvas, puzzle, nicegui, folio-nav, snap-guides, entry-points, docstrings]

requires:
  - phase: 49-web-canvas
    provides: "/puzzle page with Fabric.js canvas, /api/puzzle_image, /api/puzzle_folios"
  - phase: 48-desktop-canvas
    provides: Desktop puzzle UX patterns (crop, flip, Add from Joins)
provides:
  - "Folio navigation (prev/next) within selected fragment"
  - "Snap guides for edge/center alignment when dragging"
  - "'Add to Puzzle' entry points from Browse, Search, and Lists pages"
  - "Add from List dialog (multi-select from personal lists)"
  - "Add from Known Joins button (PGP + FJMS scientific joins)"
  - "Crop mode with per-edge keyboard/mouse cropping"
  - "Flip recto/verso (single fragment and whole puzzle)"
  - "Comprehensive docstrings for all Python functions and JSDoc for JS methods"
affects: [50-join-documents, 51-recto-verso, 52-community]

tech-stack:
  added: []
  patterns: [per-edge crop via scaling interception, flip-all with visual center mirroring, CUL auto-threshold 150]

key-files:
  created: []
  modified:
    - web/pages/puzzle.py
    - web/pages/browse.py
    - web/pages/search.py
    - web/pages/lists.py

key-decisions:
  - "CUL/T-S fragments default to threshold 150 (blue scanning backgrounds), others default to 30"
  - "Crop mode intercepts Fabric.js scaling events and converts them to cropEdge calls for intuitive mouse-drag cropping"
  - "Flip-all mirrors visual centers (getCenterPoint) and negates rotation angles, then navigates each fragment to recto/verso counterpart"
  - "Add from Known Joins fetches both PGP user joins and FJMS scientific joins, deduplicates by sys_id"

patterns-established:
  - "Per-edge crop: object:scaling handler intercepts scale changes and redirects to cropEdge with computed pixel deltas"
  - "Visual center mirroring for flip-all: snapshot getCenterPoint before any mutations, compute mirror axis, apply atomically"
  - "fetch_connected_fragments runs in UI context (not io_bound) to access app.storage.user for auth"

requirements-completed: [CANV-07, CANV-08, PLAT-01]

duration: 120min
completed: 2026-03-16
---

# Phase 49 Plan 02: Folio Navigation, Snap Guides, Entry Points + Documentation Summary

**Folio prev/next navigation, bounding-box snap guides (8px threshold, cyan dashed lines), "Add to Puzzle" buttons on Browse/Search/Lists, Add from List/Joins dialogs, per-edge crop mode, flip recto/verso, and comprehensive docstrings for all Python and JS functions**

## Performance

- **Duration:** ~120 min (extensive interactive testing and iterative bug fixing)
- **Started:** 2026-03-16T11:19:00Z
- **Completed:** 2026-03-16T14:00:00Z
- **Tasks:** 2 (plan task 1: folio nav + snap guides + entry points; documentation task)
- **Files modified:** 4

## Accomplishments
- Folio navigation (prev/next) on selected fragment with folio label display in toolbar
- Snap guides showing cyan dashed alignment lines when dragging near another fragment's edges or center (8px threshold)
- "Add to Puzzle" entry points from Browse detail view, Search expanded view, and Lists page
- "Add from List" dialog for batch-adding fragments from personal lists with checkboxes
- "Add from Known Joins" button fetching PGP + FJMS scientific joins for the selected fragment
- Per-edge crop mode with keyboard arrows (20/40px steps) and mouse-drag cropping via scaling interception
- Flip recto/verso for single fragment and flip-all-puzzle (mirror positions + navigate to other side)
- CUL/T-S auto-threshold detection (150 for blue backgrounds)
- Comprehensive docstrings added to all Python functions and JSDoc comments to all major JS methods

## Task Commits

The plan 02 work was done through extensive interactive testing with 26 iterative commits:

1. **Task 1: Folio nav, snap guides, entry points, crop, flip** - Multiple commits from `71866384` through `5702cce3` (feat + fix iterations during interactive testing)
2. **Documentation: Add docstrings to all functions** - `f9fe43d2` (docs)

Key commits:
- `cdb0e4b6` fix(49): fix folio resolution + add 'Add from List' button
- `b2bb9d35` fix(49): proportional resize, CUL threshold, auth, fit-to-screen, RTL nav, flip buttons
- `5fe1203c` fix(49): flip recto/verso, crop mode, list dialog, input styling, threshold
- `1ed36dfb` feat(49): add 'Add from Known Joins' button to puzzle toolbar
- `02ce6209` fix(49): flip puzzle -- snapshot visual centers before changes, apply atomically
- `f9fe43d2` docs(49-02): add comprehensive docstrings to puzzle.py functions and JS methods

## Files Created/Modified
- `web/pages/puzzle.py` - Folio nav, snap guides, crop mode, flip recto/verso, Add from List/Joins, docstrings (~2100 lines)
- `web/pages/browse.py` - "Add to Puzzle" button in detail view
- `web/pages/search.py` - "Add to Puzzle" button in expanded result view
- `web/pages/lists.py` - "Add to Puzzle" button per list item

## Decisions Made
- CUL/T-S fragments auto-detected by library_code or shelfmark prefix and given threshold 150 (matching desktop defaults for blue scanning backgrounds)
- Crop mode reuses Fabric.js edge handles by intercepting object:scaling events rather than implementing a custom overlay
- Flip-all uses getCenterPoint() for visual center calculation (avoiding bounding rect inflation from rotation) and applies all position changes atomically
- fetch_connected_fragments must run in UI context (not run.io_bound) because it needs access to app.storage.user for authentication

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed flip puzzle position drift**
- **Found during:** Task 1 (interactive testing)
- **Issue:** flipAllPuzzle used getBoundingRect which inflates bounds on rotated objects, causing cumulative position drift on repeated flips
- **Fix:** Switched to getCenterPoint() for visual center calculation, snapshot-then-apply pattern for atomic position updates
- **Files modified:** web/pages/puzzle.py (JS)
- **Commits:** `6249aeeb`, `cae5963b`, `02ce6209`

**2. [Rule 2 - Missing Critical] Added crop mode with per-edge cropping**
- **Found during:** Task 1 (feature parity with desktop)
- **Issue:** Desktop canvas had full crop mode; web needed parity
- **Fix:** Added toggleCropMode, cropEdge, cropConfirm, cropRevert with both keyboard and mouse-drag cropping
- **Files modified:** web/pages/puzzle.py (JS + Python)
- **Commits:** `5fe1203c`, `a9e3ca16`, `8c9b27a4`

**3. [Rule 2 - Missing Critical] Added "Add from Known Joins" button**
- **Found during:** Task 1 (feature parity with desktop)
- **Issue:** Desktop had "Add Joins" functionality; web needed equivalent
- **Fix:** Added button that fetches PGP joins via fetch_connected_fragments + FJMS scientific joins, shows checkbox dialog
- **Files modified:** web/pages/puzzle.py
- **Commits:** `1ed36dfb`, `a074ba19`, `194596b7`, `5702cce3`

**4. [Rule 3 - Blocking] Fixed canvas lifecycle and JS timeout errors**
- **Found during:** Task 1 (initial testing)
- **Issue:** Fabric.js CDN loading was async, causing race conditions when addFragment was called before init
- **Fix:** Added pending queue for pre-init fragment adds, fire-and-forget retry loop for CDN loading, proper cleanup on destroy
- **Files modified:** web/pages/puzzle.py
- **Commits:** `e2a4cfd9`, `df200648`, `44a94ef8`

---

**Total deviations:** 4 categories of auto-fixes (1 bug, 2 missing critical, 1 blocking)
**Impact on plan:** All fixes necessary for correctness and feature parity with desktop. Crop mode and Add from Joins were essential for a complete puzzle experience.

## Issues Encountered
- Fabric.js CDN loading race condition required a retry loop pattern rather than awaited initialization
- fetch_connected_fragments accesses app.storage.user internally, so it cannot run in run.io_bound (thread context has no NiceGUI storage access)
- getCenterPoint vs getBoundingRect: rotated objects inflate their bounding rect, causing cumulative drift in flip-all calculations

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Complete web puzzle experience functional at /puzzle with full manipulation parity to desktop
- Phase 49 (Web Canvas) complete -- all 3 requirements satisfied (CANV-07, CANV-08, PLAT-01)
- Ready for Phase 50 (Join Documents): save/load puzzle arrangements to joins.db

---
*Phase: 49-web-canvas*
*Completed: 2026-03-16*
