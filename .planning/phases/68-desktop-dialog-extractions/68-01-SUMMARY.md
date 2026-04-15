---
phase: 68-desktop-dialog-extractions
plan: 01
title: Extract scholarly dialogs to desktop/dialogs_scholarly.py
status: complete
completed: 2026-04-15
duration: ~3 min
tasks_completed: 2
tasks_total: 2
tags: [decomposition, extraction, desktop, scholarly-dialogs]
dependency_graph:
  requires: []
  provides: [desktop/dialogs_scholarly.py, scholarly-dialog-re-exports]
  affects: [genizah_app.py, desktop/result_dialog.py]
tech_stack:
  added: []
  patterns: [module-extraction, lazy-import-retarget, re-export-for-backcompat]
key_files:
  created: [desktop/dialogs_scholarly.py]
  modified: [genizah_app.py, desktop/result_dialog.py]
decisions:
  - "QUrl belongs in PyQt6.QtCore, not QtGui -- fixed during import derivation (Rule 1 bug fix)"
  - "Baseline test count is 1066 not 1067 as plan stated -- verified pre-existing, no regression"
metrics:
  duration_seconds: 194
  completed_date: 2026-04-15
  lines_moved: 1298
  genizah_app_reduction: "29611 -> 28314 lines"
requirements: [DESK-05]
---

# Phase 68 Plan 01: Extract Scholarly Dialogs Summary

Moved 4 scholarly dialog classes (FjmsBibliographyDialog, FjmsCatalogDialog, FjmsMeasurementsDialog, NliBibliographyDialog) from genizah_app.py to desktop/dialogs_scholarly.py, retargeted 4 lazy imports in desktop/result_dialog.py, and added re-exports in genizah_app.py for back-compat.

## One-liner

1298-line extraction of 4 FJMS/NLI scholarly dialog classes to desktop/dialogs_scholarly.py, eliminating the desktop.result_dialog -> genizah_app back-edge.

## Task Results

| Task | Name | Status | Commit | Key Changes |
|------|------|--------|--------|-------------|
| 1 | Create desktop/dialogs_scholarly.py with 4 scholarly dialog classes | Done | be56dddb | Created module, moved 4 classes, retargeted 4 lazy imports, added re-exports |
| 2 | Scholarly slice desktop smoke test (D-13) | Verified | -- | User-approved 2026-04-15, no crash, no regression |

## Verification Results

- ruff check: PASSED (all 3 files clean)
- pytest: PASSED (1066 passed, 9 skipped -- pre-existing baseline; plan stated 1067 but baseline confirmed at 1066)
- Import smoke (D-16): PASSED
  - `from desktop.dialogs_scholarly import ...` -- OK
  - `from genizah_app import ...` (re-export) -- OK
  - `from desktop.result_dialog import ResultDialog` -- OK
- Acceptance criteria: All 18 grep checks passed

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] QUrl import location**
- **Found during:** Task 1 (import derivation)
- **Issue:** QUrl is in PyQt6.QtCore, not PyQt6.QtGui. Initial import placement caused ImportError.
- **Fix:** Moved QUrl to the QtCore import line.
- **Files modified:** desktop/dialogs_scholarly.py
- **Commit:** be56dddb

**2. [Observation] Test baseline count discrepancy**
- **Found during:** Task 1 verification
- **Issue:** Plan stated 1067 passed, 9 skipped. Actual pre-existing baseline is 1066 passed, 9 skipped. Verified by running pytest on clean HEAD before changes.
- **Fix:** None needed -- no regression. Documented for accuracy.

## Verification

D-13 scholarly slice smoke test approved by user on 2026-04-15. No crash, no regression.

## Known Stubs

None.

## Threat Flags

None -- no new network endpoints, auth paths, or schema changes introduced.

## Self-Check: PASSED
