---
phase: 11-virtual-reading-desk
plan: 04
subsystem: desktop-browse
tags: [reading-desk, entry-points, toolbar, add-to-view, joins-integration]
dependency-graph:
  requires: [11-03-desktop-rendering]
  provides: [desktop-reading-desk-entry-points, reading-desk-toolbar, joins-to-desk]
  affects: [11-05-verification]
tech-stack:
  added: []
  patterns: [duplicate-safe-add, shelfmark-resolve-then-add, list-item-intercept-for-desk]
key-files:
  created: []
  modified:
    - genizah_app.py
decisions: []
metrics:
  duration: ~12 min
  completed: 2026-02-08
---

# Phase 11 Plan 04: Desktop Reading Desk Entry Points & Toolbar Summary

**One-liner:** "Add to View" button, green reading desk toolbar with shelfmark add/list add/exit, joins dropdown "Open in Reading Desk", and list item intercept for desk mode

## Task Commits

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Add entry points, toolbar, and dynamic manuscript management | 25e0622 | genizah_app.py |

## What Was Built

### "Add to View" Button (Row 1 in browse tab)
- Added between Find Parallels and Add to List buttons
- Disabled by default, enabled when a manuscript is loaded (in `on_browse_enriched_loaded`)
- When clicked with no reading desk active: enters reading desk with current manuscript
- When clicked with reading desk already active: adds current manuscript (duplicate-safe)

### Reading Desk Toolbar (Green Bar)
- QWidget with `#2d6a4f` green background, hidden by default
- Shown when `_browse_enter_reading_desk()` is called, hidden on `_browse_exit_reading_desk()`
- Contains:
  - "Reading Desk" label (bold white)
  - Fragment count label (updates on each render)
  - Shelfmark QLineEdit with "Add" button (resolves via `resolve_system_by_shelfmark`, handles disambiguation dialog)
  - "Add from List" button (opens browse lists panel)
  - "Exit Reading Desk" button (red, calls `_browse_exit_reading_desk`)

### New Methods Added

**`_browse_add_to_view()`** - Button click handler; enters desk or adds to existing desk

**`_browse_rd_add_entry(sys_id, shelfmark, sequence_order=None)`** - Core add method:
- Checks for duplicates (skips if sys_id already in desk)
- Loads pages from searcher
- Assigns sequence_order (after last entry if not specified)
- Creates ReadingDeskEntry and appends to state
- Launches ReadingDeskWorker for PGP sources
- Re-renders immediately

**`_browse_rd_add_by_shelfmark()`** - Toolbar shelfmark add:
- Reads text from `browse_rd_shelf_input`
- Resolves via `meta_mgr.resolve_system_by_shelfmark()`
- Handles disambiguation dialog for multiple matches
- Calls `_browse_rd_add_entry()` on success
- Clears input after add

**`_browse_rd_add_from_list()`** - Opens lists panel via `browse_set_lists_panel_visible(True)`

**`_browse_open_joins_in_reading_desk()`** - Opens all joined fragments:
- Gets connected fragments from JoinsManager (by ID, then by shelfmark)
- Builds fragments_info from fragment_details (resolves shelfmarks to sys_ids)
- Falls back to `_shelf_to_sys` map for resolution
- Attempts to get PGP document ID via `get_document_for_fragment`
- Calls `_browse_enter_reading_desk(fragments_info, pgpid)`

### Modified Methods

**`browse_on_list_item_clicked()`** - Added reading desk intercept:
- At top of method, checks `self.browse_reading_desk_active`
- If active: extracts sys_id from list entry, resolves shelfmark, calls `_browse_rd_add_entry()`, returns early
- If not active: proceeds with normal navigation behavior

**`_update_joins_dropdown()`** - Added "Open in Reading Desk" action:
- Appears after "View all joins..." in the joins dropdown
- Connected to `_browse_open_joins_in_reading_desk()`
- Only shows when joins exist (method returns early for no-joins case)

**`on_browse_enriched_loaded()`** - Enables `btn_b_add_to_view` when manuscript loads

**`_browse_enter_reading_desk()`** - Shows `browse_rd_toolbar`

**`_browse_exit_reading_desk()`** - Hides `browse_rd_toolbar`

**`_browse_rd_render()`** - Updates `browse_rd_count_label` with fragment count

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Plan referenced non-existent meta_mgr.search_variants method**
- **Found during:** Task 1, Step 3 implementation
- **Issue:** Plan specified `meta_mgr.search_variants(text, limit=5)` but no such method exists on MetadataManager
- **Fix:** Used existing `meta_mgr.resolve_system_by_shelfmark(text)` instead, which is the correct API for shelfmark resolution with disambiguation support
- **Files modified:** genizah_app.py
- **Commit:** 25e0622

## Verification

- [x] `python -c "from genizah_app import *; print('Import OK')"` succeeds
- [x] All 5 new methods present on GenizahGUI class
- [x] "Add to View" button added to row1 in create_browse_tab
- [x] Reading desk toolbar with shelfmark input, Add, Add from List, Exit buttons
- [x] Toolbar hidden by default, shown/hidden on enter/exit reading desk
- [x] Count label updated in _browse_rd_render
- [x] browse_on_list_item_clicked intercepts for reading desk mode
- [x] "Open in Reading Desk" action in joins dropdown
- [x] btn_b_add_to_view enabled when manuscript loads

## Next Phase Readiness

- Plan 11-05 (Verification) can proceed -- all desktop entry points are in place
- Both web (11-01, 11-02) and desktop (11-03, 11-04) reading desk features are complete
- Desktop reading desk can be entered via: "Add to View" button, joins dropdown, list items
- Dynamic add/remove works with duplicate checking and PGP source fetching

## Self-Check: PASSED
