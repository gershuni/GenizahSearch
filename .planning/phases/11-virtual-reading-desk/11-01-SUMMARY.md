---
phase: 11-virtual-reading-desk
plan: 01
subsystem: web-browse
tags: [reading-desk, joined-view, browse, fragments, nicegui]
depends_on: []
provides:
  - shared reading desk data model (ReadingDeskEntry, ReadingDeskState)
  - enhanced web joined fragments view with add/remove capabilities
affects:
  - 11-02 (desktop reading desk will use same shared model)
tech-stack:
  added: []
  patterns:
    - pre-loaded page data in state entries (avoid re-fetching during render)
    - inline zoom controls for per-image zoom handles
    - list picker dialog for add-from-list
key-files:
  created:
    - shared/reading_desk_model.py
  modified:
    - shared/__init__.py
    - web/pages/browse.py
decisions:
  - DEC-11-01-01: Use dict-based reading_desk_entries instead of ReadingDeskEntry dataclass in browse state (simpler for NiceGUI reactive rendering; dataclass is available for desktop)
  - DEC-11-01-02: Pre-load pages in enter_joined_view rather than during render (avoids repeated Supabase calls on each UI refresh)
metrics:
  duration: ~5 min
  completed: 2026-02-08
---

# Phase 11 Plan 01: Web Reading Desk Enhancement Summary

**One-liner:** Shared reading desk model + enhanced web joined view with add-by-shelfmark, add-from-lists, remove, and per-image zoom controls

## What Was Done

### Task 1: Create shared ReadingDeskEntry/ReadingDeskState data model
- Created `shared/reading_desk_model.py` with two dataclasses:
  - `ReadingDeskEntry`: sys_id, shelfmark, title, library_code, pgpid, pages, sources, pgp_doc, sequence_order
  - `ReadingDeskState`: entries list, source_description, pgpid
- Updated `shared/__init__.py` to export both classes
- Pure data module with no UI or Supabase dependencies -- usable by both web and desktop

### Task 2: Enhance web browse tab joined view with add/remove capabilities
- **State additions**: Added `reading_desk_entries` field to `BrowseState` for pre-loaded page data
- **enter_joined_view()**: Now populates `reading_desk_entries` by calling `service.get_full_manuscript()` for each fragment on entry
- **exit_joined_view()**: Now clears `reading_desk_entries` on exit
- **New helper functions**:
  - `add_to_joined_view_by_shelfmark()`: Uses `service.search_by_shelfmark()` to find and add manuscripts with duplicate detection
  - `add_to_joined_view_by_sysid()`: Direct sys_id lookup with metadata resolution for shelfmark display
  - `add_from_list_to_joined_view()`: Adds all items from a personal list, resolving shelfmarks from metadata
  - `remove_from_joined_view()`: Removes entry by sys_id, auto-exits to single view when last entry removed
- **Enhanced rendering**:
  - Green gradient header with "Reading Desk" title and fragment count badge
  - Toolbar row with shelfmark input, sys_id input, and "Add from List" button
  - Per-fragment header with shelfmark, "Current" badge, and remove (X) button
  - Per-image zoom controls (+/-/1:1) as overlay buttons
  - Correct image URLs for Oxford vs NLI with onerror fallback
  - RTL Hebrew text display with proper styling
  - Full PGP transcription section at bottom (preserved from existing code)

## Task Commits

| Task | Name | Commit | Key Files |
|------|------|--------|-----------|
| 1 | Create shared ReadingDeskEntry/ReadingDeskState data model | 30130e6 | shared/reading_desk_model.py, shared/__init__.py |
| 2 | Enhance web browse tab joined view with add/remove capabilities | 5fbf38e | web/pages/browse.py |

## Decisions Made

1. **DEC-11-01-01**: Used dict-based `reading_desk_entries` in BrowseState rather than ReadingDeskEntry dataclass objects. Simpler for NiceGUI reactive rendering patterns. The dataclass is still available for desktop (Phase 11-02).
2. **DEC-11-01-02**: Pre-load pages in `enter_joined_view()` rather than fetching during render. This avoids repeated Supabase calls on each UI refresh and makes the rendering code cleaner.

## Deviations from Plan

None -- plan executed exactly as written.

## Verification Results

1. `python -c "from shared.reading_desk_model import ReadingDeskEntry, ReadingDeskState; print('OK')"` -- PASSED
2. `python -c "from web.pages.browse import create_browse_page; print('Import OK')"` -- PASSED
3. AST parse confirms all 6 required functions exist in browse.py
4. `py_compile.compile('web/pages/browse.py')` -- PASSED

## Next Phase Readiness

- Phase 11-02 (desktop reading desk) can proceed: shared model is in place
- No blockers identified

## Self-Check: PASSED
