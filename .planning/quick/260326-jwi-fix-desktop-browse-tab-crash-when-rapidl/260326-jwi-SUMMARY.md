---
phase: quick
plan: 260326-jwi
subsystem: desktop-browse
tags: [bugfix, race-condition, desktop, image-loading]
dependency_graph:
  requires: []
  provides: [race-condition-safe-image-navigation]
  affects: [genizah_app.py]
tech_stack:
  added: []
  patterns: [generation-guard, debounce-timer, signal-disconnect]
key_files:
  modified:
    - genizah_app.py
decisions:
  - "150ms debounce chosen as balance between responsiveness and thread reduction"
  - "Signal disconnect instead of blocking wait(500) to prevent UI freeze during rapid nav"
  - "Generation guard on display_image lambda matches existing _on_thumbnail_ready pattern"
metrics:
  duration: 3min
  completed: 2026-03-26
  tasks_completed: 2
  tasks_total: 2
  files_modified: 1
---

# Quick Task 260326-jwi: Fix Desktop Browse Tab Crash on Rapid Navigation

Generation-guarded image loading with 150ms debounce eliminates 0xC0000409 crash from overlapping async threads during rapid prev/next clicking.

## What Changed

### Task 1: Generation guard and non-blocking cancellation (26bd024a)

Fixed 5 race conditions in ManuscriptViewerWidget:

1. **Generation guard on display_image** -- `loader_thread.image_loaded` now connects through a lambda that checks `_load_generation`, rejecting stale thread callbacks (matching existing `_on_thumbnail_ready` pattern)
2. **Non-blocking cancel in set_page** -- Replaced `wait(500)` with signal disconnect. No more UI-thread blocking during navigation.
3. **Preload worker cancellation** -- `_preload()` now cancels the previous preload thread before creating a new one, preventing GC of running QThread
4. **Thumbnail thread tracking** -- `_thumb_threads` list prevents premature garbage collection of daemon threads
5. **Browse image thread fixes** -- `cancel_browse_image_thread` uses disconnect instead of blocking wait; main app `closeEvent` adds 500ms timeout (was indefinite)

### Task 2: Navigation debounce (1ee0cf74)

Added 150ms debounce to `set_page`:
- `current_idx` and `_load_generation` update immediately (UI stays responsive)
- Actual image thread spawn deferred 150ms via `QTimer.singleShot`
- Rapid clicking coalesces to single network request for final destination page
- Timer cleanup in `stop_threads` prevents post-destruction callbacks

## Deviations from Plan

None - plan executed exactly as written.

## Known Stubs

None.
