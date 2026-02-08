---
phase: 11-virtual-reading-desk
plan: 02
subsystem: web-browse
tags: [reading-desk, entry-points, toolbar, add-from-list, remove, state-persistence]
dependency-graph:
  requires: [11-01-shared-model-web-dual-pane]
  provides: [web-reading-desk-entry-points, web-reading-desk-management]
  affects: [11-05-verification]
tech-stack:
  added: []
  patterns: [app-storage-user-persistence, shelfmark-search-add, list-items-bulk-add]
key-files:
  created: []
  modified:
    - web/pages/browse.py
decisions:
  - id: DEC-11-02-01
    description: "Persist reading desk state to app.storage.user for language-switch preservation"
  - id: DEC-11-02-02
    description: "Header shows 'Document #X' for join-context, 'Reading Desk' for standalone entry"
metrics:
  duration: ~8 min
  completed: 2026-02-08
---

# Phase 11 Plan 02: Web Entry Points and Dynamic Management Summary

**One-liner:** Add to View button, toolbar shelfmark input, Add from List dialog, per-fragment remove buttons, and language-switch state preservation for web reading desk

## Task Commits

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Add entry points for the reading desk | e34d3a7 | web/pages/browse.py |
| 2 | Add toolbar, add-from-list, remove buttons, and state preservation | d9f9c26 | web/pages/browse.py |

## What Was Built

### Task 1: Reading Desk Entry Points

Added the "Add to View" button and supporting infrastructure:

- **add_to_reading_desk()**: Main entry point function -- if reading desk is inactive, starts it with current manuscript; if active, adds current manuscript (with duplicate detection)
- **_add_sys_id_to_reading_desk()**: Internal helper for programmatic additions by sys_id/shelfmark
- **_persist_reading_desk_state()**: Saves reading desk entries (sys_id + shelfmark pairs) to `app.storage.user['reading_desk_state']` for language-switch persistence
- **_restore_reading_desk_state()**: Restores reading desk from storage on page load (called before normal browse position restore)
- **"Add to View" button**: `library_add` icon button in the single-page header bar, placed after the joins button -- visible whenever a manuscript is loaded
- **exit_joined_view()**: Updated to clear persisted state

### Task 2: Toolbar, Add from List, Remove, State Preservation

**Reading Desk Toolbar** (between header bar and dual-pane content):
- Shelfmark text input + "Add" button -- searches by shelfmark and adds found manuscript
- Enter key support on the input field
- "Add from List" button opens the list dialog
- Fragment count badge on the right side
- Visual separator between sections

**Add from List Dialog** (show_add_from_list_dialog):
- Shows all user's personal lists (excluding recent/system lists) with name and color
- Each list is clickable -- adds ALL items from that list to the reading desk
- Bulk add with duplicate detection (skips items already in desk)
- Resolves shelfmarks from metadata manager when not stored on item
- Loads full manuscript data (pages, PGP sources, pgp_doc) for each added entry
- Notification shows count of manuscripts added

**Per-fragment Remove Buttons**:
- Small X/close button on the right side of every fragment header in both image and text panes
- remove_from_desk(sys_id) filters entries and re-renders
- If all entries removed, automatically exits reading desk mode

**Header Bar Updates**:
- Shows "Document #X" when opened from joins context (state.joined_pgpid set)
- Shows "Reading Desk" for standalone/manual context (no pgpid)

**Language Switch State Preservation**:
- enter_joined_view() now calls _persist_reading_desk_state()
- On page load, _restore_reading_desk_state() checks app.storage.user before normal position restore
- exit_joined_view() clears the persisted state

## Decisions Made

| ID | Decision | Rationale |
|----|----------|-----------|
| DEC-11-02-01 | Persist reading desk state to app.storage.user | NiceGUI recreates page on language switch; app.storage.user survives page recreations within same session |
| DEC-11-02-02 | Header shows "Document #X" for joins, "Reading Desk" for standalone | Distinguishes context -- joins are about a specific PGP document, standalone is general research workspace |

## Deviations from Plan

None - plan executed exactly as written.

## Verification

- [x] `python -c "from web.pages.browse import create_browse_page; print('OK')"` succeeds
- [x] "Add to View" button (library_add icon) visible in single-page browse header bar
- [x] add_to_reading_desk() starts reading desk or extends it with duplicate check
- [x] Toolbar with shelfmark input + Add button rendered between header and panes
- [x] toolbar_add_by_shelfmark() searches and adds manuscripts
- [x] show_add_from_list_dialog() displays personal lists with add capability
- [x] Per-fragment remove buttons (X) in both image and text pane headers
- [x] remove_from_desk() removes entries and exits if all removed
- [x] Header shows "Reading Desk" for non-join context, "Document #X" for joins
- [x] State persisted to app.storage.user and restored on page reload
- [x] "View All Fragments" from joins panel still works (on_view_all=enter_joined_view unchanged)

## Next Phase Readiness

- Plan 11-03 (Desktop Reading Desk Rendering) can proceed independently
- Plan 11-04 (Desktop Entry Points) can proceed independently
- Plan 11-05 (Human Verification) will verify both web and desktop implementations

## Self-Check: PASSED
