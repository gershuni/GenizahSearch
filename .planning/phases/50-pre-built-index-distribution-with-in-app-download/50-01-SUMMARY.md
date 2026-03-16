---
phase: 50-join-documents
plan: 01
subsystem: shared-services
tags: [pillow, sqlite, puzzle, export, composite, thumbnail, base64]

requires:
  - phase: 47-foundation
    provides: PuzzleImageService, PuzzleModel, background_removal
provides:
  - PuzzleDocument with simplified model (join_type='physical', crop fields, processed flag)
  - PuzzleService schema v2 with thumbnail_b64, shelfmark serialization, thumbnail-safe saves
  - Composite export engine (RGBA PNG, top-left origin, centered rotation pivot)
  - Thumbnail generation as base64 PNG
  - Auto-suggest title from fragment shelfmarks
affects: [50-02, 50-03, web-puzzle, desktop-puzzle]

tech-stack:
  added: []
  patterns: [schema-migration-via-alter-table, thumbnail-preservation-on-metadata-save, crop-scale-from-canvas-to-export]

key-files:
  created: [shared/puzzle_export.py]
  modified: [shared/puzzle_model.py, shared/puzzle_service.py]

key-decisions:
  - "Crop offsets stored in 800px canvas-pixel space, scaled to export resolution at compose time"
  - "Thumbnail preserved on metadata-only saves by reading existing value when thumbnail_b64=None"
  - "PIL.rotate negated angle to match clockwise-positive convention of Fabric.js and PyQt"

patterns-established:
  - "Schema migration: try SELECT on new column, ALTER TABLE ADD COLUMN on OperationalError"
  - "Coordinate mapping: coord_scale = export_img.width / 800.0 for canvas-to-export translation"

requirements-completed: [JDOC-03, JDOC-04]

duration: 3min
completed: 2026-03-16
---

# Phase 50 Plan 01: Shared Persistence & Export Layer Summary

**PuzzleDocument simplified to physical-only joins with crop/processed fields, schema v2 with thumbnail persistence, and RGBA composite export engine with top-left origin and centered rotation pivot**

## Performance

- **Duration:** 3 min
- **Started:** 2026-03-16T18:07:50Z
- **Completed:** 2026-03-16T18:10:21Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- PuzzleFragment extended with crop_top/bottom/left/right and processed fields; PuzzleDocument defaults join_type to 'physical'
- PuzzleService schema v2: thumbnail_b64 column with safe ALTER TABLE migration, shelfmark+crop+processed in fragments_json, thumbnail preservation on metadata-only saves
- Composite export engine composes full-resolution RGBA PNG from positioned fragments with correct coordinate mapping, crop scaling, and centered rotation pivot
- Thumbnail generation produces base64-encoded PNG from low-res composite

## Task Commits

Each task was committed atomically:

1. **Task 1: Update puzzle model and service schema** - `84771256` (feat)
2. **Task 2: Create composite export and thumbnail service** - `db363965` (feat)

## Files Created/Modified
- `shared/puzzle_model.py` - Added crop fields, processed flag, changed join_type default to 'physical'
- `shared/puzzle_service.py` - Schema v2 migration, fixed fragments_json serialization, thumbnail-safe saves, enriched list_documents
- `shared/puzzle_export.py` - New: auto_suggest_title, compose_puzzle_export, generate_thumbnail

## Decisions Made
- Crop offsets stored in 800px canvas-pixel space, scaled proportionally to export resolution at compose time
- Thumbnail preserved on metadata-only saves by reading existing DB value when thumbnail_b64 param is None
- PIL.rotate angle negated to match clockwise-positive convention used by both Fabric.js and PyQt

## Deviations from Plan

None - plan executed exactly as written.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- Shared persistence layer ready for wiring into web and desktop save/load/export UI
- Both apps can now save documents with thumbnails, export composites at full resolution
- Schema migration handles existing joins.db databases seamlessly

---
*Phase: 50-join-documents*
*Completed: 2026-03-16*

## Self-Check: PASSED
- All 3 files exist (puzzle_model.py, puzzle_service.py, puzzle_export.py)
- Both task commits verified (84771256, db363965)
