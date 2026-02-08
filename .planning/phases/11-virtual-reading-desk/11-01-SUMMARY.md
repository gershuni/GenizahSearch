---
phase: 11-virtual-reading-desk
plan: 01
subsystem: web-browse
tags: [reading-desk, dual-pane, synchronized-scrolling, nicegui]
dependency-graph:
  requires: [phase-8-foundation, phase-9-data-import, phase-10-desktop-pgp]
  provides: [shared-reading-desk-model, web-dual-pane-reading-desk]
  affects: [11-02-desktop-reading-desk, 11-03-entry-points, 11-04-lazy-loading]
tech-stack:
  added: []
  patterns: [IntersectionObserver-sync-scrolling, per-image-viewer-state, dual-pane-layout]
key-files:
  created:
    - shared/reading_desk_model.py
  modified:
    - shared/__init__.py
    - web/pages/browse.py
decisions:
  - id: DEC-11-01-01
    description: "Per-image viewer state in JS (rdViewers) rather than Python state for responsiveness"
  - id: DEC-11-01-02
    description: "IntersectionObserver on fragment headers for sync scrolling (not scroll events)"
metrics:
  duration: ~4 min
  completed: 2026-02-08
---

# Phase 11 Plan 01: Shared Model + Web Dual-Pane Reading Desk Summary

**One-liner:** Shared reading desk data model + web dual-pane synchronized reading desk with per-image zoom/rotate/drag and per-fragment PGP version selectors

## Task Commits

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Create shared ReadingDeskEntry/ReadingDeskState data model | 7843cd2 | shared/reading_desk_model.py, shared/__init__.py |
| 2 | Rewrite web joined view as dual-pane synchronized reading desk | 2773590 | web/pages/browse.py |

## What Was Built

### Task 1: Shared Data Model
Created `shared/reading_desk_model.py` with two dataclasses:
- **ReadingDeskEntry**: Per-fragment data container (sys_id, shelfmark, pages, sources, pgp_doc, sequence_order)
- **ReadingDeskState**: Multi-fragment view container (entries list, source_description, pgpid)

Updated `shared/__init__.py` to export both classes.

### Task 2: Web Dual-Pane Reading Desk
Completely replaced the old single-scroll joined fragments view in browse.py with a v3 dual-pane synchronized layout:

**Left Pane (Image Stack):**
- All fragment images stacked vertically in a scrollable area
- Each image has its own zoom in/out, rotate left/right, and reset controls
- Per-image drag support (grab to pan) and mouse wheel zoom
- Image state managed in JavaScript (`window.rdViewers[viewerId]`) for instant responsiveness
- Lazy loading on all images
- Image error fallback (NLI -> Oxford proxy)

**Right Pane (Text Stack):**
- All fragment texts stacked vertically in a scrollable area
- Per-fragment PGP version selector dropdown (editions, translations, V0.8 fallback)
- Default selection: first PGP edition, else first translation, else V0.8
- Per-source directionality (RTL for Hebrew editions, LTR for English translations)
- Version changes update only the affected fragment's text display

**Synchronized Scrolling:**
- IntersectionObserver watches fragment header elements in both panes
- Scrolling past a fragment boundary in one pane auto-scrolls the other pane to the matching header
- Debounced sync (600ms) prevents infinite loops

**Fragment Headers:**
- Clickable shelfmark text in both panes
- "Current" badge on the fragment that matches the active browse page
- Clicking exits reading desk and navigates to that fragment in single-page view

**Entry/Exit:**
- `enter_joined_view()` now pre-loads page data and PGP sources for all fragments
- `exit_joined_view()` clears reading desk state
- "Back to Page View" button in header bar returns to normal browse

## Decisions Made

| ID | Decision | Rationale |
|----|----------|-----------|
| DEC-11-01-01 | Per-image JS state (`rdViewers`) instead of Python state | Image manipulation needs instant feedback; round-trips to Python would be laggy |
| DEC-11-01-02 | IntersectionObserver for sync scrolling (not scroll events) | More efficient, less CPU usage, naturally tracks which fragment is visible |

## Deviations from Plan

None - plan executed exactly as written.

## Verification

- [x] `python -c "from shared.reading_desk_model import ReadingDeskEntry, ReadingDeskState; print('OK')"` succeeds
- [x] `python -c "from web.pages.browse import create_browse_page; print('Import OK')"` succeeds
- [x] Joined view uses TWO panes (image left, text right)
- [x] All fragments' images stacked in image pane with lazy loading
- [x] Each image has independent zoom/rotate/drag controls
- [x] All fragments' texts stacked in text pane with RTL styling
- [x] Each fragment has a PGP version selector dropdown
- [x] Synchronized scrolling via IntersectionObserver
- [x] Fragment headers are clickable links that exit and navigate
- [x] "Back to Page View" returns to normal single-page browse
- [x] No new web page or route created -- all within /browse state.view_joined

## Next Phase Readiness

- Plan 11-02 (Desktop Reading Desk) can proceed -- shared model is available
- Plan 11-03 (Entry Points) can proceed -- web reading desk is functional
- Plan 11-04 (Lazy Loading) can refine the reading desk's image loading strategy

## Self-Check: PASSED
