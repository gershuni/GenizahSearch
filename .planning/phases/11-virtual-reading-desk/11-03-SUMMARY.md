---
phase: 11-virtual-reading-desk
plan: 03
subsystem: desktop-browse
tags: [reading-desk, dual-pane, synchronized-scrolling, pyqt6, qthread]
dependency-graph:
  requires: [11-01-shared-model]
  provides: [desktop-reading-desk-rendering, reading-desk-worker]
  affects: [11-04-desktop-entry-points, 11-05-verification]
tech-stack:
  added: []
  patterns: [proportional-scroll-sync, per-image-ZoomableScrollArea, mouseReleaseEvent-link-handler]
key-files:
  created: []
  modified:
    - genizah_app.py
    - gui_threads.py
decisions:
  - id: DEC-11-03-01
    description: "mouseReleaseEvent + anchorAt() for link clicks in QTextEdit (no anchorClicked signal)"
  - id: DEC-11-03-02
    description: "Proportional scroll ratio sync between text/image panes (not IntersectionObserver like web)"
  - id: DEC-11-03-03
    description: "QInputDialog for version selection per fragment (matches desktop UX patterns)"
metrics:
  duration: ~9 min
  completed: 2026-02-08
---

# Phase 11 Plan 03: Desktop Dual-Pane Reading Desk Rendering Summary

**One-liner:** Desktop reading desk with stacked images per fragment (ZoomableScrollArea + zoom/rotate), stacked texts with PGP version selectors, and proportional synchronized scrolling between panes

## Task Commits

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Create ReadingDeskWorker QThread in gui_threads.py | b0ef5a6 | gui_threads.py, genizah_app.py (import) |
| 2 | Implement desktop reading desk dual-pane rendering with synchronized scrolling | dc7dd66 | genizah_app.py |

## What Was Built

### Task 1: ReadingDeskWorker QThread
Added `ReadingDeskWorker` to gui_threads.py, following the existing PGPSourceWorker pattern:
- Takes a list of sys_ids
- Batch-loads PGP sources and document metadata for all fragments in background
- Emits `finished` signal with list of (sys_id, sources, pgp_doc) tuples
- Emits `error` signal on failure
- Lazy-imports from shared.document_service to avoid Supabase dependency at import time
- Updated genizah_app.py import line to include ReadingDeskWorker

### Task 2: Desktop Reading Desk Dual-Pane Rendering

**State Variables (added to browse tab init):**
- `browse_reading_desk_active` -- boolean flag
- `browse_reading_desk_state` -- ReadingDeskState instance
- `browse_reading_desk_pgpid` -- optional PGP document ID
- `_browse_rd_worker` -- ReadingDeskWorker reference
- `_browse_rd_image_widgets` -- list of (sys_id, ZoomableScrollArea, ImageLoaderThread)
- `_browse_rd_image_scroll` -- QScrollArea for stacked images
- `_browse_rd_syncing` -- flag to prevent infinite scroll sync loop

**Entry Point (`_browse_enter_reading_desk`):**
- Accepts fragments_info list and optional pgpid
- Builds ReadingDeskEntry objects from fragment metadata and page text
- Disables normal page navigation (prev/next/combo/view-all)
- Renders initial view with V0.8 text
- Launches ReadingDeskWorker for background PGP source loading
- When worker finishes, updates entries and re-renders with PGP data

**Text Pane (`_browse_rd_render` -- left side of splitter):**
- Header bar with "Reading Desk" title, PGP ID, fragment count
- Per-fragment sections with clickable shelfmark links (genizah://rd-navigate/)
- "Current" badge on fragment matching active browse page
- [remove] link per fragment (genizah://rd-remove/)
- [change version] link when PGP sources available (genizah://rd-version/)
- Text content: prefers PGP edition, falls back to V0.8
- Proper RTL/LTR directionality based on source language

**Image Pane (`_browse_rd_render_images` -- right side of splitter):**
- Hides normal ManuscriptViewerWidget
- Creates QScrollArea with stacked fragment images
- Per-fragment header labels
- Each image page gets its own ZoomableScrollArea (400-600px height)
- Per-image controls: zoom in (+), zoom out (-), rotate left, rotate right
- Image loading via ImageLoaderThread with IIIF URL resolution
- Fallback to FL ID images when NLI/external images unavailable

**Synchronized Scrolling (`_browse_rd_setup_sync_scroll`):**
- Connects verticalScrollBar.valueChanged on both text and image panes
- Proportional ratio mapping: scrolling one pane moves the other to matching position
- `_browse_rd_syncing` flag prevents infinite feedback loop

**Link Handling:**
- Installed mouseReleaseEvent handler on browse_text at init time
- Uses QTextEdit.anchorAt(pos) to detect genizah:// URL clicks
- Extended `_on_browse_link_clicked` to handle:
  - `genizah://rd-navigate/{sid}` -- exit desk, navigate to fragment
  - `genizah://rd-remove/{sid}` -- remove fragment from desk
  - `genizah://rd-version/{sid}/{idx}` -- open version selector dialog

**Version Selection (`_browse_rd_show_version_dialog`):**
- QInputDialog with all PGP sources for the fragment
- Labels: "Edition: {scholar}" or "Translation ({lang}): {scholar}"
- V0.8 (HTR) fallback always available
- Reorders entry.sources so selected version renders first on re-render

**Exit (`_browse_exit_reading_desk`):**
- Clears reading desk state
- Stops and disconnects worker
- Removes stacked image scroll area
- Restores normal ManuscriptViewerWidget visibility
- Re-enables page navigation
- Reloads current page in normal mode

## Decisions Made

| ID | Decision | Rationale |
|----|----------|-----------|
| DEC-11-03-01 | mouseReleaseEvent + anchorAt() for QTextEdit link clicks | QTextEdit lacks anchorClicked signal (only QTextBrowser has it); anchorAt() detects href at click position |
| DEC-11-03-02 | Proportional scroll ratio sync (not IntersectionObserver) | Desktop Qt doesn't have IntersectionObserver; proportional mapping is the PyQt equivalent |
| DEC-11-03-03 | QInputDialog for per-fragment version selection | Consistent with existing desktop UX patterns (shelfmark disambiguation uses same approach) |

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed View All mode dead links (genizah://load/)**
- **Found during:** Task 2 (link handler implementation)
- **Issue:** `_on_browse_link_clicked` was defined but never connected to any signal. View All mode's genizah://load/ links were rendered in HTML but clicking them did nothing.
- **Fix:** Installed mouseReleaseEvent handler on browse_text at init time (not just during reading desk mode), making all genizah:// links clickable in both View All and Reading Desk modes.
- **Files modified:** genizah_app.py
- **Commit:** dc7dd66

## Verification

- [x] `python -c "from gui_threads import ReadingDeskWorker; print('OK')"` succeeds
- [x] `python -c "from genizah_app import *; print('Import OK')"` succeeds
- [x] genizah_app.py has _browse_rd_render, _browse_enter_reading_desk, _browse_exit_reading_desk
- [x] _browse_rd_render_images creates stacked ZoomableScrollArea per image
- [x] Each image has zoom in/out and rotate left/right controls
- [x] Synchronized scrolling via proportional ratio mapping
- [x] Link handlers: rd-navigate, rd-remove, rd-version
- [x] _browse_exit_reading_desk restores normal ManuscriptViewerWidget

## Next Phase Readiness

- Plan 11-04 (Desktop Entry Points) can proceed -- all rendering methods are ready
- Entry point: `_browse_enter_reading_desk(fragments_info, pgpid)` accepts any list of fragments
- Plan 11-05 (Verification) can verify both web and desktop reading desk functionality

## Self-Check: PASSED
