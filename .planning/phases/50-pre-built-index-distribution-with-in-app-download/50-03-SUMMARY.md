---
phase: 50-join-documents
plan: 03
subsystem: web-ui
tags: [nicegui, fabric-js, puzzle, left-drawer, auto-save, export-png, crud]

requires:
  - phase: 50-01
    provides: PuzzleService, PuzzleExport, PuzzleModel with crop/processed fields
  - phase: 49-01
    provides: Puzzle page with Fabric.js canvas, puzzle_image API, puzzle_folios API
provides:
  - Web puzzle left drawer with saved document list (thumbnails, titles, dates)
  - Save/Load/New/Export toolbar buttons
  - Event-driven auto-save via Fabric.js object:modified
  - Crop state persistence via per-object Fabric.js properties (getCropState/pending_crops)
  - Full-resolution PNG export via run.io_bound
  - 6 API endpoints for puzzle document CRUD, export, and thumbnails
affects: [50-04, web-puzzle, desktop-puzzle]

tech-stack:
  added: []
  patterns: [event-driven-auto-save-with-loading-guard, callback-based-crop-restore, debounced-asyncio-auto-save]

key-files:
  created: []
  modified: [web/api.py, web/pages/puzzle.py]

key-decisions:
  - "getCropState reads per-object Fabric.js properties (obj.cropX, obj._originalWidth), NOT transient _cropOffsets"
  - "Crop restore in on_puzzle_add_result callback (not setTimeout) for reliability with slow image loads"
  - "Loading guard (doc_state['loading']) prevents auto-save from overwriting partially-loaded documents"
  - "Auto-save debounced 1.5s via asyncio.create_task, not ui.timer"

patterns-established:
  - "Event-driven auto-save: Fabric.js object:modified -> CustomEvent -> Python schedule_auto_save"
  - "Loading guard pattern: set loading=True before fragment load, decrement in add_result callback"
  - "Pending crops pattern: store crop state in dict, apply in add_result callback after image loads"

requirements-completed: [JDOC-01, JDOC-02, JDOC-04, JDOC-05]

duration: 5min
completed: 2026-03-16
---

# Phase 50 Plan 03: Web Join Document UI Summary

**Web puzzle left drawer with saved documents, save/load/export buttons, event-driven auto-save via Fabric.js object:modified, and reliable callback-based crop restore**

## Performance

- **Duration:** 5 min
- **Started:** 2026-03-16T18:12:51Z
- **Completed:** 2026-03-16T18:17:47Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- 6 API endpoints for puzzle document CRUD, composite PNG export, and thumbnail serving
- Left drawer with saved documents list showing thumbnails, titles, shelfmarks, and dates
- Save Join dialog with auto-suggested title, Load restores full fragment state including crop
- Event-driven auto-save via Fabric.js object:modified with debounced asyncio task and loading guard
- Full-resolution PNG export via run.io_bound (no httpx dependency)
- getCropState JS method reads per-object Fabric.js properties (not transient _cropOffsets)
- Crop restore in on_puzzle_add_result callback (reliable, not setTimeout-based)

## Task Commits

Each task was committed atomically:

1. **Task 1: Add API endpoints for puzzle document operations and export** - `bfe60df3` (feat)
2. **Task 2: Add left drawer, save/load/new/export UI, and event-driven auto-save** - `0bf1a1d3` (feat)

## Files Created/Modified
- `web/api.py` - 6 new endpoints: puzzle_documents, puzzle_document CRUD, puzzle_export, puzzle_thumbnail
- `web/pages/puzzle.py` - Left drawer, doc_state, build_fragments_list, schedule_auto_save, save/load/new/export functions, clearAll/getCropState JS methods, object:modified event bridge

## Decisions Made
- getCropState reads from per-object Fabric.js properties (obj.cropX, obj.cropY, obj.width, obj.height, obj._originalWidth, obj._originalHeight) -- NOT from the transient _cropOffsets global which is nulled when crop mode exits
- Crop restore happens in on_puzzle_add_result callback after each fragment image loads, using pending_crops dict -- not via fragile setTimeout that races with slow IIIF image loads
- Loading guard (doc_state['loading'] + load_pending counter) prevents auto-save from firing while a document is still loading, avoiding partial-state overwrites
- Auto-save uses asyncio.create_task with 1.5s sleep for debounce, cancelling previous task on rapid changes

## Deviations from Plan

None - plan executed exactly as written.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- Web puzzle page now has full document persistence: save, load, auto-save, export
- Desktop persistence (Plan 02) already committed separately
- Ready for Phase 51 (Verso Pairing) or Phase 52 (Community + Integration)

---
*Phase: 50-join-documents*
*Completed: 2026-03-16*

## Self-Check: PASSED
- Both modified files exist (web/api.py, web/pages/puzzle.py)
- Both task commits verified (bfe60df3, 0bf1a1d3)
