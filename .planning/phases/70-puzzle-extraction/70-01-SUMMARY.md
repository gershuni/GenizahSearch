---
phase: 70-puzzle-extraction
plan: 01
subsystem: desktop
status: complete
tags: [extraction, decomposition, desktop, puzzle]
dependency_graph:
  requires: []
  provides: [desktop/puzzle.py]
  affects: [genizah_app.py]
tech_stack:
  added: []
  patterns: [module-extraction, lazy-import, re-export]
key_files:
  created:
    - desktop/puzzle.py
  modified:
    - genizah_app.py
decisions:
  - "ShelfmarkCompleter imported lazily inside PuzzleCanvasWindow.__init__ (D-04) to avoid circular import"
  - "Removed 15 unused imports from genizah_app.py that were exclusively used by puzzle classes"
  - "PuzzleExportThread and PuzzlePublishThread stay in desktop/puzzle.py per D-01 (domain-specific threads)"
metrics:
  duration_seconds: 915
  completed: "2026-04-16"
  tasks_completed: 1
  tasks_total: 2
  files_created: 1
  files_modified: 1
---

# Phase 70 Plan 01: Extract Puzzle Classes Summary

5 puzzle/join canvas classes (PuzzleFragmentItem, PuzzleCanvasView, PuzzleExportThread, PuzzlePublishThread, PuzzleCanvasWindow) extracted from genizah_app.py into desktop/puzzle.py with lazy ShelfmarkCompleter import and back-compat re-exports.

## What Was Done

### Task 1: Create desktop/puzzle.py with 5 puzzle classes (COMPLETE)

Created `desktop/puzzle.py` (2669 lines) containing all 5 puzzle classes in dependency order per D-06:
1. **PuzzleFragmentItem** - QGraphicsPixmapItem with drag, rotate, resize, crop, flip
2. **PuzzleCanvasView** - QGraphicsView with zoom, pan, background modes
3. **PuzzleExportThread** - QThread for composite PNG export
4. **PuzzlePublishThread** - QThread for Supabase publish/unpublish
5. **PuzzleCanvasWindow** - QMainWindow composing the full puzzle workspace

Modifications to `genizah_app.py`:
- Removed ~2642 lines (the 5 class definitions)
- Added re-export: `from desktop.puzzle import PuzzleFragmentItem, PuzzleCanvasView, PuzzleExportThread, PuzzlePublishThread, PuzzleCanvasWindow  # noqa: F401`
- Cleaned up 15 unused imports that were exclusively used by puzzle classes:
  - `import math` (top-level; function-local re-import at line 1224 covers remaining use)
  - `QGraphicsView`, `QGraphicsScene`, `QGraphicsPixmapItem`, `QGraphicsItem`, `QGraphicsTextItem`, `QDockWidget` (QtWidgets)
  - `QSize`, `QRectF`, `QPointF` (QtCore)
  - `QImage`, `QTransform`, `QPainter`, `QAction` (QtGui)
  - `PuzzleImageLoaderThread` (gui_threads -- still used by desktop/puzzle.py directly)

### Task 2: Puzzle desktop smoke test (PENDING)

Awaiting human verification of runtime behavior.

## Verification Results

- `ruff check genizah_app.py desktop/puzzle.py`: All checks passed
- `pytest tests/ -q`: 1066 passed, 9 skipped (matches pre-extraction baseline)
- Import smoke (D-12): Both `from desktop.puzzle import ...` and `from genizah_app import ...` succeed
- All 5 classes present in desktop/puzzle.py, zero in genizah_app.py
- Re-export line present in genizah_app.py

## Deviations from Plan

None - plan executed exactly as written.

## Decisions Made

1. **ShelfmarkCompleter lazy import (D-04)**: Function-local import inside `PuzzleCanvasWindow.__init__` avoids circular import at module load time. No issues encountered.
2. **Import cleanup**: Removed 15 imports from genizah_app.py that became unused after extraction. All were verified via ruff (no pre-existing F401 errors existed before extraction).
3. **Test baseline**: Actual baseline is 1066 passed, 9 skipped (plan stated 1067/8, which was slightly stale). No change caused by extraction.

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1 | 78c0eeb8 | feat(70-01): extract 5 puzzle classes into desktop/puzzle.py |

## Self-Check: PENDING

Will be completed after Task 2 verification.
