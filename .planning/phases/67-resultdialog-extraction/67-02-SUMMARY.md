---
phase: 67-resultdialog-extraction
plan: 02
subsystem: desktop
tags: [refactor, decomposition, desktop, result-dialog-extraction]
dependency_graph:
  requires: [desktop-package, shared-widgets-module]
  provides: [result-dialog-module, title-helpers-module, image-loader-module]
  affects: [genizah_app.py, tests/test_desktop_pending_corrections.py]
tech_stack:
  added: []
  patterns: [lazy-inline-import, derive-then-ruff-verify, additive-copy-then-cutover]
key_files:
  created:
    - desktop/result_dialog.py
    - desktop/title_helpers.py
    - desktop/image_loader.py
  modified:
    - desktop/widgets.py
    - genizah_app.py
    - tests/test_desktop_pending_corrections.py
decisions:
  - "6 lazy inline imports from genizah_app (ManuscriptViewerWidget, DesktopVSCache, 4 FJMS/NLI dialogs) -- D-06 deny-rule satisfied"
  - "Imports derived via ruff iteration, not hand-authored"
  - "_set_label_with_tooltip removed from genizah_app.py import (only used by ResultDialog)"
  - "requests, QTextCursor, QTextCharFormat removed from genizah_app.py (only used by moved code)"
  - "ActionsHoverWidget not imported in result_dialog.py (not directly used by ResultDialog)"
  - "_get_initial_image_index moved to desktop/widgets.py alongside _get_folio_image_index (dependency)"
metrics:
  duration_seconds: 810
  completed: "2026-04-15"
  tasks_completed: 3
  tasks_total: 3
---

# Phase 67 Plan 02: Extract ResultDialog and Helpers Summary

**One-liner:** Extracted ResultDialog (2790 lines, 68 methods) from genizah_app.py to desktop/result_dialog.py with cohesive helper modules, D-06 deny-rule enforced, and 4 atomic commits gated by ruff + pytest.

## Task Results

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Delete browse dead code from ResultDialog | 381fac9c | genizah_app.py |
| 2 | Move cat-(e) helpers to cohesive modules | bba5b2ea | desktop/widgets.py, desktop/title_helpers.py, desktop/image_loader.py, genizah_app.py |
| 3a | Additive copy of ResultDialog | 8769231a | desktop/result_dialog.py |
| 3b | Cut-over ResultDialog, update tests | ca8c79b6 | desktop/result_dialog.py, genizah_app.py, tests/test_desktop_pending_corrections.py |

## What Was Done

### Task 1: Delete browse dead code
- Deleted `start_browse_download` method (dead code referencing GenizahGUI-only attributes)
- Deleted `browse_img_thread` cleanup block in `closeEvent` (getattr always returns None)
- 27 lines removed, ResultDialog class body is now self-consistent for extraction

### Task 2: Move category-(e) helpers
- **desktop/widgets.py**: Added `apply_find_highlight`, `_get_folio_number_from_shelfmark`, `_get_folio_image_index`, `_get_initial_image_index`
- **desktop/title_helpers.py** (new): `_title_svc_singleton`, `_get_title_svc`, `_truncate_title`, `_is_hebrew_text`, `_translate_hebrew_date`, `_resolve_display_title`, `_set_label_with_tooltip`
- **desktop/image_loader.py** (new): `ImageLoaderThread` class
- Removed unused imports from genizah_app.py: `requests`, `QTextCursor`, `QTextCharFormat`
- genizah_app.py updated to import from all three new modules

### Task 3: Additive copy then cut-over
- **Commit 3a (additive copy)**: Copied ResultDialog class to desktop/result_dialog.py. Derived all imports via ruff iteration (no hand-authored imports). Added 6 lazy inline imports for genizah_app symbols per D-06 deny-rule.
- **Commit 3b (cut-over)**: Deleted ResultDialog class (2790 lines) from genizah_app.py. Added `from desktop.result_dialog import ResultDialog` import. Updated tests 6-8 in test_desktop_pending_corrections.py to read from desktop/result_dialog.py.

## Verification

- `ruff check` exits 0 on all modified files (desktop/result_dialog.py, desktop/widgets.py, desktop/title_helpers.py, desktop/image_loader.py, genizah_app.py)
- `python -c "import genizah_app"` -- exits 0 (real startup order)
- `python -c "from desktop.result_dialog import ResultDialog; from desktop.widgets import ActionsHoverWidget; from genizah_app import GenizahGUI"` -- exits 0 (3-line smoke)
- `rg -c "^from genizah_app" desktop/result_dialog.py` -- 0 matches (D-06 deny-rule)
- `rg -c "^class ResultDialog" genizah_app.py` -- 0 matches (class removed)
- `rg -c "^class ResultDialog" desktop/result_dialog.py` -- 1 match (class in new home)
- `pytest -q` -- 1067 passed, 9 skipped (baseline maintained across all 4 commits)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] _get_initial_image_index dependency**
- **Found during:** Task 2
- **Issue:** `_get_folio_image_index` calls `_get_initial_image_index` which was not in the explicit move list but is a required dependency
- **Fix:** Also moved `_get_initial_image_index` to desktop/widgets.py and added it to genizah_app.py import
- **Files modified:** desktop/widgets.py, genizah_app.py

**2. [Rule 3 - Blocking] Unused imports after code removal**
- **Found during:** Tasks 2 and 3
- **Issue:** After moving ImageLoaderThread, `requests` became unused in genizah_app.py. After moving apply_find_highlight, `QTextCursor` and `QTextCharFormat` became unused. After moving ResultDialog, `_set_label_with_tooltip` became unused.
- **Fix:** Removed all unused imports to satisfy ruff F401
- **Files modified:** genizah_app.py

**3. [Rule 3 - Blocking] ActionsHoverWidget not used by ResultDialog**
- **Found during:** Task 3 (ruff F401)
- **Issue:** Plan listed ActionsHoverWidget in desktop/result_dialog.py imports but ResultDialog doesn't directly use it
- **Fix:** Removed from desktop/result_dialog.py imports
- **Files modified:** desktop/result_dialog.py

**4. [Rule 3 - Blocking] Redundant `import re` in title_helpers.py**
- **Found during:** Task 2 (ruff F811)
- **Issue:** `_translate_hebrew_date` had an inline `import re` which conflicted with module-level `import re`
- **Fix:** Removed the inline `import re` (module-level suffices)
- **Files modified:** desktop/title_helpers.py

## Known Stubs

None.

## Self-Check: PASSED

- [x] desktop/result_dialog.py exists (2831 lines)
- [x] desktop/title_helpers.py exists
- [x] desktop/image_loader.py exists
- [x] desktop/widgets.py updated with 4 new functions
- [x] Commit 381fac9c verified in git log
- [x] Commit bba5b2ea verified in git log
- [x] Commit 8769231a verified in git log
- [x] Commit ca8c79b6 verified in git log
- [x] genizah_app.py reduced by ~2790 lines (ResultDialog class)
- [x] tests/test_desktop_pending_corrections.py updated for desktop_rd_source fixture
