---
phase: 71-genizahgui-consolidation-smoke-tests
plan: 02
subsystem: desktop
status: pending-verification
tags: [smoke-test, checklist, desktop, verification, DESK-07]
dependency_graph:
  requires: [desktop/vs_cache.py, desktop/result_dialog.py, desktop/puzzle.py, desktop/viewers.py, desktop/dialogs_filter.py, desktop/dialogs_scholarly.py]
  provides: [docs/desktop-smoke-checklist.md]
  affects: []
tech_stack:
  added: []
  patterns: [manual-smoke-checklist]
key_files:
  created:
    - docs/desktop-smoke-checklist.md
  modified: []
decisions:
  - "D-07: Manual checklist, not PyQt automation script"
  - "D-08: 10 sections covering all post-extraction code paths"
  - "D-09: Smoke checklist is the DESK-07 deliverable"
  - "D-10/D-11: Re-exports documented as intentional back-compat shims"
metrics:
  duration_seconds: 120
  completed: null
  tasks_completed: 1
  tasks_total: 2
  files_created: 1
  files_modified: 0
---

# Phase 71 Plan 02: Desktop Smoke Test Checklist Summary

Manual smoke test checklist created at docs/desktop-smoke-checklist.md covering 10 post-extraction desktop code paths with architecture summary (genizah_app.py 22,541 lines + desktop/ 10,384 lines). Awaiting human verification walkthrough.

## Task Results

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Create docs/desktop-smoke-checklist.md | 81e9c490 | docs/desktop-smoke-checklist.md (created) |
| 2 | Desktop smoke test walkthrough | -- | Awaiting human verification |

## Checklist Coverage (D-08)

| # | Section | Extracted Module Exercised |
|---|---------|---------------------------|
| 1 | Application Startup | genizah_app.py (coordinator) |
| 2 | Basic Search | desktop/result_dialog.py |
| 3 | Responsa / Tabular Query Builder | genizah_app.py (TabularQueryBuilderDialog) |
| 4 | Browse Navigation | desktop/viewers.py, desktop/image_loader.py |
| 5 | Visual Similarity | desktop/vs_cache.py |
| 6 | Settings, Help & About | genizah_app.py (small dialogs) |
| 7 | List Management | genizah_app.py (Lists tab) |
| 8 | Puzzle Window | desktop/puzzle.py |
| 9 | Filter Dialogs | desktop/dialogs_filter.py |
| 10 | Scholarly Dialogs | desktop/dialogs_scholarly.py |

## Verification Results

- **pytest**: 1067 passed, 8 skipped (green baseline maintained)
- **Checklist structure**: Automated check passed (all 10 sections present)
- **Human walkthrough**: PENDING

## Deviations from Plan

None -- plan executed exactly as written.

## Known Stubs

None.

## Self-Check: PASSED
