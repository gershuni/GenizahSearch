---
phase: 11
plan: 10
subsystem: web-ui
tags: [reading-desk, dialog, checkbox, ux-refinement, list-integration]

dependency-graph:
  requires: [11-09, 11-02]
  provides: [per-manuscript-selection-in-add-from-list]
  affects: []

tech-stack:
  added: []
  patterns:
    - "ui.checkbox for per-item selection in NiceGUI dialogs"
    - "Factory pattern (make_add_selected_handler) for closure capture in loops"

file-tracking:
  key-files:
    created: []
    modified:
      - web/pages/browse.py

decisions: []

metrics:
  duration: "<1 min"
  completed: "2026-02-08"
---

# Phase 11 Plan 10: Per-Manuscript Selection in Add from List Dialog Summary

**One-liner:** Checkbox-based individual manuscript selection in Add from List dialog with Add Selected button alongside Add All

## What Was Done

### Task 1: Add per-manuscript checkboxes and Add Selected button (50deb46)

Modified the `show_add_from_list_dialog()` function in `web/pages/browse.py` to support granular manuscript selection:

1. **Per-manuscript checkboxes:** Each manuscript not already in the reading desk now shows a `ui.checkbox` instead of a plain label. Users can tick individual manuscripts they want to add.

2. **Already-in-desk indicator:** Manuscripts already present in the reading desk show a green check icon with tooltip "Already in Reading Desk" -- no checkbox, since re-adding is not possible.

3. **"Add Selected" button:** New button alongside existing "Add All" that adds only checked manuscripts. Uses the same factory pattern (`make_add_selected_handler`) as the existing `make_add_list_handler` for correct closure capture.

4. **Selection tracker:** A `selections` dictionary maps `list_id` to a list of `(sys_id, shelfmark, checkbox)` tuples, enabling the Add Selected handler to read checkbox states at click time.

Key code structure:
```python
# Shared tracker across all lists
selections = {}  # list_id -> [(sys_id, shelfmark, checkbox), ...]

# Per manuscript: checkbox if not in desk, check icon if already in
if already_in:
    ui.icon('check', ...).tooltip('Already in Reading Desk')
    ui.label(shelfmark)
else:
    cb = ui.checkbox(shelfmark)
    selections[list_id].append((sys_id, shelfmark, cb))

# Button row with both options
ui.button('Add Selected', on_click=make_add_selected_handler())
ui.button('Add All (N)', on_click=make_add_list_handler())
```

## Task Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1 | 50deb46 | feat(11-10): add per-manuscript checkboxes to Add from List dialog |

## Verification Results

| Check | Result |
|-------|--------|
| `python -c "from web.pages.browse import create_browse_page; print('OK')"` | PASSED |
| `Add Selected` present in browse.py | PASSED |
| `ui.checkbox` used in dialog | PASSED |
| `add_selected_items` handler defined | PASSED |

## Deviations from Plan

None -- plan executed exactly as written.

## UAT Gap Closure Status

This plan addresses **Test 7** from the UAT (11-UAT.md): "Add from List dialog allows selecting individual manuscripts." Previously only "Add All" was available per list. Now users have granular control via checkboxes.

## Next Phase Readiness

Phase 11 UAT gap closure is now complete (plans 08-11 all done). Phase 11 Virtual Reading Desk is fully implemented for both web and desktop.

## Self-Check: PASSED
