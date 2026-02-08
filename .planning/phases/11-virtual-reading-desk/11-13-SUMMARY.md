---
phase: 11-virtual-reading-desk
plan: 13
subsystem: desktop-ui
tags: [reading-desk, browse, navigation-guard, splitter, UAT-gap-closure]
dependency-graph:
  requires: [11-11]
  provides: [desktop-reading-desk-navigation-safe, desktop-reading-desk-lists-panel-aware]
  affects: [12, 13]
tech-stack:
  patterns: [navigation-guard, splitter-size-detection]
key-files:
  modified: [genizah_app.py]
decisions: []
metrics:
  duration: ~5 min
  completed: 2026-02-08
---

# Phase 11 Plan 13: Desktop UAT Gap Closure (Tests 11, 12, 14) Summary

**Guards browse_load() and enrichment from overwriting reading desk, and fixes 4-widget splitter sizing in lists panel toggle.**

## What Was Done

### Task 1: Guard browse_load() and on_browse_enriched_loaded() (Tests 11+12)

**Root cause:** `browse_load()` ran its full normal flow when reading desk was active, overwriting `browse_text` with single-page HTML and clearing `browse_viewer` images. This destroyed the reading desk visual rendering. Similarly, `on_browse_enriched_loaded()` loaded images into the normal `browse_viewer` during reading desk mode, clobbering the stacked image layout.

**Fix:** Added 4 guards checking `self.browse_reading_desk_active`:

1. **Line 16045** - Guard `browse_text.setText("Loading metadata...")` -- skip during reading desk to preserve stacked text view
2. **Line 16047** - Guard `browse_viewer.load_images({})` -- skip viewer clear during reading desk
3. **Line 16174** - Redirect render/load to `_browse_rd_add_entry()` when reading desk active -- newly resolved manuscript is auto-added to the reading desk instead of overwriting the view
4. **Line 7153** - Guard `browse_viewer.load_images(meta, idx, ...)` in `on_browse_enriched_loaded()` at the call site -- handles both new requests and in-flight enrichment threads that complete after reading desk activation

The lookup/disambiguation logic in `browse_load()` runs unchanged, correctly resolving shelfmark input to `self.current_browse_sid`. Only the 3 visual rendering operations are skipped when reading desk is active.

**Commit:** `4a92952`

### Task 2: Make browse_set_lists_panel_visible() reading-desk-aware (Test 14)

**Root cause:** `browse_set_lists_panel_visible()` computed a 3-element splitter sizes array `[lists, text, viewer]` for the normal 3-widget splitter. During reading desk mode, the splitter has 4 widgets (4th being `_browse_rd_image_scroll`). Calling `setSizes()` with 3 elements on a 4-widget splitter collapsed the 4th widget (image scroll) to 0 width.

**Fix:** Added reading desk detection in the "show" branch:
- When no cached sizes exist and reading desk is active: compute 4-element array `[lists(20%), text(35%), hidden-viewer(0), image_scroll(45%)]`
- When cached sizes from normal mode have 3 elements: expand to 4-element array
- The "hide" branch already reads sizes dynamically from the splitter, naturally handling any widget count

**Commit:** `2083c56`

## Deviations from Plan

None -- plan executed exactly as written.

## UAT Gaps Closed

| Test | Description | Fix |
|------|-------------|-----|
| Test 11 | Add to View shows only 1 fragment after navigation | browse_load() guards preserve stacked view |
| Test 12 | Second manuscript not stacked after Go navigation | browse_load() redirects to _browse_rd_add_entry() |
| Test 14 | Image pane disappears on Add from List | 4-element splitter sizes in browse_set_lists_panel_visible() |

## Task Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1 | `4a92952` | Guard browse_load and enrichment from overwriting reading desk |
| 2 | `2083c56` | Make browse_set_lists_panel_visible reading-desk-aware |

## Key Implementation Details

- `browse_reading_desk_active` now has 13 total occurrences (was 8 before this plan, +5 new guards)
- The enrichment guard is at the `load_images()` call site, NOT an early return -- this correctly handles in-flight enrichment threads
- The splitter size fix uses `self._browse_rd_image_scroll is not None` as a secondary check to confirm the 4th widget exists
- Browse viewer (widget index 2) gets size 0 during reading desk because it is hidden/replaced by `_browse_rd_image_scroll` (widget index 3)

## Self-Check: PASSED
