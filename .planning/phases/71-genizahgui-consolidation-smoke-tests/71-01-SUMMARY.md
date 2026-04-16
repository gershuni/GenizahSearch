---
phase: 71-genizahgui-consolidation-smoke-tests
plan: 01
subsystem: desktop
status: complete
tags: [extraction, import-graph, back-edge, vs-cache]
dependency_graph:
  requires: []
  provides: [desktop/vs_cache.py, zero-back-edges]
  affects: [genizah_app.py, desktop/result_dialog.py, docs/OPEN_ISSUES.md]
tech_stack:
  added: []
  patterns: [module-extraction, re-export-shim, function-local-import]
key_files:
  created:
    - desktop/vs_cache.py
  modified:
    - genizah_app.py
    - desktop/result_dialog.py
    - docs/OPEN_ISSUES.md
decisions:
  - "D-04: Module named exactly vs_cache (lowercase underscore)"
  - "D-03: Re-export all 3 classes in genizah_app.py for back-compat"
  - "D-02: Retarget result_dialog.py to import from desktop.vs_cache"
  - "D-17: Fixed OPEN_ISSUES.md Related Documents paths to archived locations"
metrics:
  duration_seconds: 175
  completed: 2026-04-16T10:14:00Z
  tasks_completed: 2
  tasks_total: 2
  files_created: 1
  files_modified: 3
---

# Phase 71 Plan 01: Extract DesktopVSCache & Kill Last Back-Edge Summary

DesktopVSCache, VSFetchThread, VSDownloadThread (~207 lines) extracted from genizah_app.py to desktop/vs_cache.py, eliminating the last desktop/ -> genizah_app back-edge. Import graph is now strictly one-directional.

## Task Results

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Extract DesktopVSCache trio to desktop/vs_cache.py | f87bdb82 | desktop/vs_cache.py (created), genizah_app.py, desktop/result_dialog.py |
| 2 | Fix docs/OPEN_ISSUES.md path reference (D-17) | 531c87b8 | docs/OPEN_ISSUES.md |

## Verification Results

- **ruff check**: All 3 files clean (genizah_app.py, desktop/vs_cache.py, desktop/result_dialog.py)
- **pytest**: 1066 passed, 9 skipped (no regression from baseline)
- **Import cycle check**: All 9 desktop/ modules importable in single Python command
- **Zero back-edges**: No desktop/*.py file contains `from genizah_app import`
- **Re-export**: `from genizah_app import DesktopVSCache, VSFetchThread, VSDownloadThread` works
- **Direct import**: `from desktop.vs_cache import DesktopVSCache` works

## Deviations from Plan

None -- plan executed exactly as written.

## Known Stubs

None.

## Self-Check: PASSED
