---
phase: 11
plan: 01
subsystem: web-reading-desk
tags: [reading-desk, multi-fragment, nicegui, dataclass]
depends_on:
  requires: [08-foundation, 09-data-import, 10-desktop-pgp-core]
  provides: [web-reading-desk-page, shared-reading-desk-model, reading-desk-entry-points]
  affects: [11-02-desktop-reading-desk]
tech_stack:
  added: []
  patterns: [shared-dataclass-model, per-image-zoom-js, version-selector-integration]
key_files:
  created:
    - shared/reading_desk_model.py
    - web/pages/reading_desk.py
  modified:
    - web/main.py
    - web/components/joins_panel.py
    - web/pages/lists.py
decisions:
  - id: DEC-11-01-01
    summary: Use teal color scheme for reading desk to distinguish from browse (green)
metrics:
  duration: 4 min
  completed: 2026-02-08
---

# Phase 11 Plan 01: Web Reading Desk Page Summary

Web virtual reading desk with shared data model, full multi-fragment viewer page, and three entry points from joins panel, lists, and direct URL.

## Task Commits

| Task | Name | Commit | Key Files |
|------|------|--------|-----------|
| 1 | Create shared reading desk model and web page | 0ee8141 | shared/reading_desk_model.py, web/pages/reading_desk.py |
| 2 | Add route and entry points | c01764f | web/main.py, web/components/joins_panel.py, web/pages/lists.py |

## What Was Built

### Shared Reading Desk Model (`shared/reading_desk_model.py`)
- `ReadingDeskEntry` dataclass: sys_id, shelfmark, title, library_code, pgpid, pages, sources, pgp_doc, sequence_order
- `ReadingDeskState` dataclass: entries list, source_description, pgpid
- Pure data containers with no UI or Supabase dependencies -- ready for desktop reuse in Plan 02

### Web Reading Desk Page (`web/pages/reading_desk.py`)
- Full 310-line page with header bar, add-manuscript toolbar, and scrollable fragment display
- Three entry points:
  - **VIEW-01**: `/reading-desk?pgpid=1234` loads all fragments for a PGP document
  - **VIEW-02**: `/reading-desk?sys_ids=a,b,c` loads specific manuscripts
  - **VIEW-03**: `/reading-desk?list_id=abc` loads all items from a personal list
- Per-fragment display with recto/verso pages showing:
  - Image panel (left 50%) with per-image zoom in/out, reset, rotate controls via scoped JS
  - Text panel (right 50%) with version selector for editions/translations
  - Per-source directionality per DEC-10-01-02: Hebrew editions RTL, English translations LTR
- Add manuscript by shelfmark/sys_id with search resolution
- Remove manuscript with X button on each fragment header
- Full PGP transcription section at bottom when loaded via pgpid
- Image fallback onerror chain (NLI -> Oxford -> hide) matching browse.py pattern

### Route and Entry Points
- `/reading-desk` route registered in web/main.py with pgpid, sys_ids, list_id query params
- "Reading Desk" added to navigation sidebar with auto_stories icon
- "Open in Reading Desk" button in joins panel (after View All Fragments)
- "Reading Desk" button in lists page detail header (next to Export)

## Decisions Made

| ID | Decision | Rationale |
|----|----------|-----------|
| DEC-11-01-01 | Use teal color scheme for reading desk | Distinguishes reading desk (teal) from browse page (green) for clear visual identity |

## Deviations from Plan

None -- plan executed exactly as written.

## Next Phase Readiness

Plan 02 (Desktop Reading Desk) can proceed:
- `shared/reading_desk_model.py` provides the data model for desktop to consume
- Desktop can use same `_load_entry_from_sys_id` pattern adapted for QThread workers
- No blockers identified

## Self-Check: PASSED
