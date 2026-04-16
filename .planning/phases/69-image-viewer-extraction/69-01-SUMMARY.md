---
phase: 69-image-viewer-extraction
plan: 01
subsystem: desktop
tags: [refactor, extraction, viewers, decomposition]
dependency_graph:
  requires: [phase-68-desktop-dialog-extractions]
  provides: [desktop/viewers.py]
  affects: [genizah_app.py, desktop/result_dialog.py]
tech_stack:
  added: []
  patterns: [module-extraction, re-export-back-compat]
key_files:
  created: [desktop/viewers.py]
  modified: [genizah_app.py, desktop/result_dialog.py, tests/test_desktop_folio_navigation.py]
decisions:
  - Moved _make_scrollable_row and _generate_oxford_dynamic_url helpers alongside viewer classes (used by ManuscriptViewerWidget, also re-exported for genizah_app.py callers)
  - Updated 3 source-scanning tests to read from desktop/viewers.py instead of genizah_app.py (Rule 1 auto-fix)
metrics:
  duration: 610s
  completed: 2026-04-16
  tasks_completed: 2
  tasks_total: 2
  status: complete
---

# Phase 69 Plan 01: Image Viewer Extraction Summary

Extracted ZoomableScrollArea, FullscreenImageWindow, ManuscriptViewerWidget (~1160 lines) from genizah_app.py into desktop/viewers.py with re-exports for back-compat and retargeted lazy import in result_dialog.py.

## Tasks Completed

### Task 1: Create desktop/viewers.py with 3 image viewer classes
- **Commit:** fef70ae6
- **Status:** Complete
- Created `desktop/viewers.py` with 3 classes ordered per D-09: ZoomableScrollArea (base) -> FullscreenImageWindow -> ManuscriptViewerWidget
- Moved helper functions `_make_scrollable_row` and `_generate_oxford_dynamic_url` into viewers.py (both used by ManuscriptViewerWidget; `_make_scrollable_row` also used by GenizahGUI reading desk via re-export)
- Removed ~1160 lines from genizah_app.py (26580 -> 25420 lines)
- Added re-export line in genizah_app.py: `from desktop.viewers import ZoomableScrollArea, FullscreenImageWindow, ManuscriptViewerWidget, _make_scrollable_row, _generate_oxford_dynamic_url  # noqa: F401`
- Retargeted `desktop/result_dialog.py:489` lazy import from `genizah_app` to `desktop.viewers`
- Confirmed `desktop/result_dialog.py:645` still has `from genizah_app import DesktopVSCache` (untouched, noted for Phase 71)
- Removed unused `QGraphicsSimpleTextItem` import from genizah_app.py (ruff F401)

### Task 2: Image viewer desktop smoke test (D-12)
- **Status:** Pending human verification

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Updated 3 source-scanning tests**
- **Found during:** Task 1 verification (pytest)
- **Issue:** `tests/test_desktop_folio_navigation.py` had 3 tests that read `genizah_app.py` as text to verify ManuscriptViewerWidget methods (btn_ktiv, load_images, etc.). After extraction, those methods are in `desktop/viewers.py`.
- **Fix:** Added `viewers_source` fixture reading `desktop/viewers.py`; updated `test_msviewer_ktiv_button_exists`, `test_msviewer_source_combo_enhanced`, `test_ktiv_button_visibility_logic` to use it.
- **Files modified:** `tests/test_desktop_folio_navigation.py`
- **Commit:** fef70ae6

**2. [Rule 2 - Missing functionality] Moved helper functions alongside viewer classes**
- **Found during:** Task 1 implementation
- **Issue:** `_make_scrollable_row` and `_generate_oxford_dynamic_url` are called by ManuscriptViewerWidget but were module-level functions in genizah_app.py. Moving only classes would create a back-edge import from desktop.viewers to genizah_app.
- **Fix:** Moved both functions to desktop/viewers.py and added them to the re-export line in genizah_app.py (GenizahGUI also calls `_make_scrollable_row` at 4 sites via re-export).
- **Files modified:** `desktop/viewers.py`, `genizah_app.py`
- **Commit:** fef70ae6

## Verification Results

- ruff check: All 3 files clean (E9/F401/F811/F821)
- Import smoke (D-11): direct import from desktop.viewers OK, re-export from genizah_app OK, full chain OK
- pytest: 1066 passed, 9 skipped (baseline 1067/8 -- 1 unrelated flaky skip difference)
- Desktop smoke (D-12): Pending human verification

## Self-Check: PASSED
