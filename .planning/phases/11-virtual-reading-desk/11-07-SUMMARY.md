---
phase: 11-virtual-reading-desk
plan: 07
subsystem: desktop-ui
tags: [pyqt6, scroll-sync, ux, gap-closure]
dependency_graph:
  requires: [11-03, 11-04, 11-05]
  provides: [desktop-reading-desk-gap-fixes]
  affects: [12-desktop-pgp-discovery]
tech_stack:
  added: []
  patterns:
    - signal-disconnect-before-reconnect
    - deferred-sync-via-qtimer
key_files:
  created: []
  modified:
    - genizah_app.py
decisions: []
metrics:
  duration: "~2 min"
  completed: "2026-02-08"
---

# Phase 11 Plan 07: Desktop Reading Desk Gap Fixes Summary

**Desktop scroll sync fixed, button placement and labeling improved -- 3 verified gaps (D1, D2, D4) closed.**

## What Was Done

### Task 1: Fix scroll sync (D1) and improve button UX (D2, D4)

Fixed three bugs found during Phase 11 human verification:

**D1 -- Scroll sync broken (bidirectional sync between text and image panes):**
- Root cause: `_browse_rd_setup_sync_scroll()` was called multiple times during render lifecycle but never disconnected old `valueChanged` signal connections, causing stale handler accumulation
- Fix 1: Added `disconnect()` calls at the start of `_browse_rd_setup_sync_scroll()` for both text_bar and image_bar `valueChanged` signals (with try/except guard)
- Fix 2: In `_browse_rd_render_images()`, disconnect old image scroll bar signals and call `setParent(None)` before `deleteLater()` for immediate widget removal
- Fix 3: Added `QTimer.singleShot(500, self._browse_rd_setup_sync_scroll)` after initial sync setup to re-establish sync after images load (scroll maximums change when images render)

**D4 -- "Add to View" button position:**
- Moved `btn_b_add_to_view` addWidget call to immediately after `btn_browse_go`, before `btn_find_parallels`
- Button order is now: Go -> Add to View -> Find parallels -> Add to List

**D2 -- Toolbar "Add" button confusing:**
- Renamed from `tr("Add")` to `tr("Add to Desk")` in the reading desk toolbar
- Eliminates confusion with the separate "Add to List" button

## Task Commits

| Task | Name | Commit | Files Modified |
|------|------|--------|----------------|
| 1 | Fix scroll sync, button UX (D1, D2, D4) | 5598d2c | genizah_app.py |

## Deviations from Plan

None -- plan executed exactly as written.

## Verification

- `python -c "from genizah_app import *; print('OK')"` -- PASSED
- `disconnect()` present in `_browse_rd_setup_sync_scroll` -- CONFIRMED (lines 7902, 7906)
- `"Add to Desk"` label present -- CONFIRMED (line 6833)
- Button order Go -> Add to View -> Find parallels -- CONFIRMED (lines 6582-6591)

## Next Phase Readiness

All 3 desktop gaps (D1, D2, D4) are resolved. Combined with 11-06 (web gaps), Phase 11 gap closure should be complete. Ready for re-verification or progression to Phase 12.

## Self-Check: PASSED
