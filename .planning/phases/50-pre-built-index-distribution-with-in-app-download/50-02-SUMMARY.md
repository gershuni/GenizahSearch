---
phase: 50-join-documents
plan: 02
subsystem: ui
tags: [pyqt6, qdockwidget, puzzle-canvas, auto-save, desktop]

requires:
  - phase: 50-01
    provides: "PuzzleService, PuzzleDocument model, puzzle_export functions"
provides:
  - "Desktop PuzzleCanvasWindow with QDockWidget side panel for saved join documents"
  - "Save/load/new/export buttons with auto-save on canvas changes"
  - "Metadata editing (title, notes) with details panel"
  - "Loading guard preventing partial-state auto-save overwrites"
affects: [50-03, 50-04, web-puzzle-persistence]

tech-stack:
  added: []
  patterns: ["QDockWidget side panel for document management", "Loading guard pattern with pending count for async loads", "Dual-timer debounce chain: scene.changed 500ms -> auto-save 1500ms"]

key-files:
  created: []
  modified: ["genizah_app.py"]

key-decisions:
  - "Event-driven auto-save via QGraphicsScene.changed signal with dual-timer debounce (500ms+1500ms)"
  - "Loading guard using _load_pending_count prevents auto-save during partial document loads"
  - "Reuse existing _on_image_loaded pipeline for document load (no custom loading code)"
  - "Deferred imports for puzzle_service/puzzle_export to avoid import-time overhead"

patterns-established:
  - "Loading guard pattern: set _loading_document=True before async loads, decrement counter in callbacks, clear when count reaches 0"
  - "Meta loader for folio list rebuild on document load via _spawn_meta_loader"

requirements-completed: [JDOC-01, JDOC-02, JDOC-05]

duration: 3min
completed: 2026-03-16
---

# Phase 50 Plan 02: Desktop Join Document UI Summary

**QDockWidget side panel with save/load/new/export, event-driven auto-save with loading guard, metadata editing, and full-res PNG export for desktop puzzle canvas**

## Performance

- **Duration:** 3 min
- **Started:** 2026-03-16T18:12:46Z
- **Completed:** 2026-03-16T18:15:51Z
- **Tasks:** 1
- **Files modified:** 1

## Accomplishments
- QDockWidget side panel showing saved join documents with thumbnails, titles, shelfmarks, dates
- Save Join prompts for title (auto-suggested from shelfmarks), generates thumbnail, persists to joins.db
- Load document restores all fragment transforms via existing _pending_fragments + _on_image_loaded pipeline
- Loading guard prevents auto-save from overwriting partially-loaded documents
- Event-driven auto-save via scene.changed signal with 500ms+1500ms dual-timer debounce
- Full-resolution PNG export with progress dialog
- Details panel with editable title, notes, read-only fragment list
- Delete with confirmation, rename preserving existing thumbnails
- Folio list rebuild on document load via PuzzleMetaLoaderThread

## Task Commits

Each task was committed atomically:

1. **Task 1: Add QDockWidget side panel and save/load/new/export buttons** - `04101264` (feat)

## Files Created/Modified
- `genizah_app.py` - Added ~480 lines to PuzzleCanvasWindow: QDockWidget side panel, join document management methods, auto-save system, loading guard, export

## Decisions Made
- Used deferred imports (inside methods) for puzzle_service, puzzle_export, puzzle_image_service to avoid import-time overhead when puzzle window is not opened
- Dual-timer debounce chain (scene.changed 500ms -> auto-save 1500ms) batches rapid canvas changes while keeping responsiveness
- Loading guard decrement added to both _on_image_loaded (new path + update path) and _on_image_failed for complete coverage
- Rename uses save_document with default thumbnail_b64=None to preserve existing thumbnail

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Desktop puzzle persistence UI complete, ready for web puzzle persistence (Plan 03/04)
- joins.db sidecar created automatically on first save
- Auto-save and loading guard patterns established for reference

---
*Phase: 50-join-documents*
*Completed: 2026-03-16*
