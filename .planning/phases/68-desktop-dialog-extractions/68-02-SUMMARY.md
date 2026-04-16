---
phase: 68-desktop-dialog-extractions
plan: 02
title: Extract filter dialogs to desktop/dialogs_filter.py
status: pending-verification
completed: null
duration: ~13 min (Task 1 only)
tasks_completed: 1
tasks_total: 2
tags: [decomposition, extraction, desktop, filter-dialogs, qthread-relocation]
dependency_graph:
  requires: [68-01]
  provides: [desktop/dialogs_filter.py, FilterCountWorker-in-gui_threads]
  affects: [genizah_app.py, gui_threads.py]
tech_stack:
  added: []
  patterns: [module-extraction, qthread-relocation, cargo-cult-import-deletion, re-export-for-backcompat]
key_files:
  created: [desktop/dialogs_filter.py]
  modified: [genizah_app.py, gui_threads.py]
decisions:
  - "Test baseline 1067 passed / 8 skipped (slight variance from Plan 01's 1066/9 -- environment-dependent, no regression)"
  - "Cleaned up 3 now-unused imports from genizah_app.py (QFontMetrics, parse_csv_shelfmarks, resolve_shelfmarks)"
metrics:
  duration_seconds: 783
  completed_date: null
  lines_moved: 1696
  genizah_app_reduction: "28314 -> 26616 lines"
requirements: [DESK-04]
---

# Phase 68 Plan 02: Extract Filter Dialogs Summary

Moved 3 filter dialog classes (ExcludeDialog, DomainFilterDialog, PreSearchFilterDialog) from genizah_app.py to desktop/dialogs_filter.py, relocated FilterCountWorker to gui_threads.py, deleted 2 cargo-cult self-imports, and added re-exports for back-compat.

## One-liner

1696-line extraction of 3 filter dialogs to desktop/dialogs_filter.py plus FilterCountWorker relocation to gui_threads.py, with cargo-cult self-import cleanup.

## Task Results

| Task | Name | Status | Commit | Key Changes |
|------|------|--------|--------|-------------|
| 1 | Move FilterCountWorker and create desktop/dialogs_filter.py | Done | 4f16c599 | Created module, moved 3 classes + 1 QThread, deleted 2 self-imports, added re-exports, cleaned unused imports |
| 2 | Filter slice desktop smoke test (D-14) | Pending | -- | Awaiting user verification |

## Verification Results

- ruff check: PASSED (all 3 files clean)
- pytest: PASSED (1067 passed, 8 skipped -- no regression from baseline)
- Import smoke (D-16): PASSED
  - `from desktop.dialogs_filter import ExcludeDialog, DomainFilterDialog, PreSearchFilterDialog` -- OK
  - `from gui_threads import FilterCountWorker` -- OK
  - `from genizah_app import ExcludeDialog, DomainFilterDialog, PreSearchFilterDialog, FilterCountWorker, GenizahGUI` -- OK (re-exports work)
- Acceptance criteria: All 12 grep checks passed
  - Classes in new locations: 4/4
  - Classes removed from genizah_app.py: 4/4
  - Self-imports deleted: confirmed (grep returns 0)
  - Re-export line present: confirmed
  - FilterCountWorker in gui_threads import: confirmed
  - FilterCountWorker import in dialogs_filter.py: confirmed

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing imports] Module-level import derivation for desktop/dialogs_filter.py**
- **Found during:** Task 1 (file creation)
- **Issue:** The 3 filter dialog classes used many Qt and utility names from genizah_app.py's top-level imports that needed to be explicitly imported in the new module: QEvent, QToolTip, QFontMetrics, QGridLayout, QPlainTextEdit, QSpinBox, QDoubleSpinBox, QColor, QCursor, re, ExclusionSource, parse_csv_shelfmarks, resolve_shelfmarks
- **Fix:** Added all missing imports to desktop/dialogs_filter.py header, iterated with ruff until clean
- **Files modified:** desktop/dialogs_filter.py
- **Commit:** 4f16c599

**2. [Rule 2 - Cleanup] Removed 3 now-unused imports from genizah_app.py**
- **Found during:** Task 1 (post-deletion ruff check)
- **Issue:** QFontMetrics, parse_csv_shelfmarks, resolve_shelfmarks were only used by the deleted classes
- **Fix:** Removed from genizah_app.py import lines
- **Files modified:** genizah_app.py
- **Commit:** 4f16c599

**3. [Observation] Test baseline count variance**
- **Found during:** Task 1 verification
- **Issue:** pytest reports 1067 passed / 8 skipped. Plan 01 reported 1066/9. Plan text expected 1067/9. Minor environment-dependent variance, no failures.
- **Fix:** None needed -- documented for accuracy.

## Pending Verification

Task 2 (D-14 filter slice smoke test) requires manual desktop app testing:
1. Launch `python genizah_app.py`
2. Open PreSearchFilterDialog -- apply a filter, close
3. Open DomainFilterDialog -- pick a domain, close
4. Open ExcludeDialog -- add an item, close
5. Click a saved entry in regular search history menu (exercises deleted self-import path at former line 28658)
6. Click a saved entry in composition search history menu (exercises deleted self-import path at former line 28695)
7. Close and re-open app (exercises session-restore FilterCountWorker path)
8. Close app

Expected: No crash, no regression at any step.

## Known Stubs

None.

## Threat Flags

None -- no new network endpoints, auth paths, or schema changes introduced.

## Self-Check: PASSED
