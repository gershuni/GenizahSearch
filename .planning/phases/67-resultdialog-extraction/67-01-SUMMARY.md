---
phase: 67-resultdialog-extraction
plan: 01
subsystem: desktop
tags: [refactor, decomposition, desktop, package-skeleton]
dependency_graph:
  requires: []
  provides: [desktop-package, shared-widgets-module]
  affects: [genizah_app.py]
tech_stack:
  added: []
  patterns: [desktop-package-skeleton, shared-helper-extraction]
key_files:
  created:
    - desktop/__init__.py
    - desktop/widgets.py
  modified:
    - genizah_app.py
decisions:
  - "ActionsHoverWidget and _format_add_to_list_label moved to desktop/widgets.py per D-03, D-04"
  - "_format_list_star kept in genizah_app.py (only 1 caller in GenizahGUI, not in D-03)"
  - "ListsTreeWidget kept in genizah_app.py (not used by ResultDialog per Codex investigation)"
metrics:
  duration_seconds: 134
  completed: "2026-04-15"
  tasks_completed: 1
  tasks_total: 1
---

# Phase 67 Plan 01: Create desktop/ Package and Move Shared Helpers Summary

**One-liner:** Created desktop/ package skeleton and extracted ActionsHoverWidget + _format_add_to_list_label to desktop/widgets.py as the first step of v7.9 decomposition.

## Task Results

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Create desktop/ package and move shared helpers | ca4995bc | desktop/__init__.py, desktop/widgets.py, genizah_app.py |

## What Was Done

1. Created `desktop/__init__.py` with module docstring for v7.9 decomposition package.
2. Created `desktop/widgets.py` containing:
   - `ActionsHoverWidget` class (verbatim copy from genizah_app.py)
   - `_format_add_to_list_label` function (verbatim copy)
   - Minimal imports: QWidget, QHBoxLayout, Qt from PyQt6; tr from genizah_core
3. Added `from desktop.widgets import ActionsHoverWidget, _format_add_to_list_label` to genizah_app.py (after gui_threads import).
4. Deleted original definitions from genizah_app.py.
5. Kept `_format_list_star` and `ListsTreeWidget` in genizah_app.py as specified.

## Verification

- `python -c "from desktop.widgets import ActionsHoverWidget, _format_add_to_list_label"` -- exits 0
- `rg -c "class ActionsHoverWidget" desktop/widgets.py` -- returns 1
- `rg -c "def _format_add_to_list_label" desktop/widgets.py` -- returns 1
- `rg -c "class ActionsHoverWidget" genizah_app.py` -- returns 0 (deleted)
- `rg -c "def _format_add_to_list_label" genizah_app.py` -- returns 0 (deleted)
- `rg -c "def _format_list_star" genizah_app.py` -- returns 1 (kept)
- `ruff check desktop/widgets.py` -- All checks passed
- `ruff check genizah_app.py` -- All checks passed
- `pytest -q` -- 1071 passed, 8 skipped (baseline was 1067; 4 additional tests from prior commits)

## Deviations from Plan

None -- plan executed exactly as written.

## Known Stubs

None.

## Self-Check: PASSED

- [x] desktop/__init__.py exists
- [x] desktop/widgets.py exists
- [x] genizah_app.py modified (import added, definitions removed)
- [x] Commit ca4995bc verified in git log
