---
phase: 11
plan: 11
subsystem: desktop-reading-desk
tags: [pyqt6, scrollarea, signal-handling, splitter, scroll-sync]
dependency_graph:
  requires: [11-03]
  provides: [desktop-reading-desk-rendering, desktop-scroll-sync]
  affects: [12, 13]
tech_stack:
  added: []
  patterns: [create-once-repopulate, targeted-signal-disconnect, stored-handler-references]
key_files:
  created: []
  modified: [genizah_app.py]
decisions:
  - id: DEC-11-11-01
    decision: "Create QScrollArea once in enter_reading_desk, repopulate container on re-render"
    reason: "deleteLater is deferred -- old widget persists in splitter causing phantom widgets"
  - id: DEC-11-11-02
    decision: "Store sync handler references for targeted disconnect instead of blanket disconnect()"
    reason: "Blanket disconnect severs QTextEdit internal scroll handling, breaking all scrolling"
metrics:
  duration: ~3 min
  completed: 2026-02-08
---

# Phase 11 Plan 11: Fix Desktop Reading Desk Rendering and Scrolling Summary

**One-liner:** Fix QScrollArea lifecycle (create once, repopulate) and scroll sync (targeted disconnect) to resolve splitter corruption and broken scrolling in desktop reading desk.

## What Was Done

### Task 1: Fix scroll area lifecycle -- create once, repopulate on re-render
**Commit:** f6ce4f8

Root cause: `_browse_rd_render_images()` destroyed and recreated the QScrollArea on every call using `deleteLater()`. Because Qt's `deleteLater()` is deferred, the old widget remained in the splitter when the new one was added, causing 4+ widgets instead of 3 and breaking layout.

Fix:
- QScrollArea created once in `_browse_enter_reading_desk()` and added to splitter there
- `_browse_rd_render_images()` now only clears/repopulates the internal container widget via `setWidget(new_container)` -- QScrollArea takes ownership and cleans up the previous container
- Removed all `deleteLater`/`setParent(None)` logic from the render method
- `_browse_rd_restore_normal_view()` properly removes scroll area from splitter on exit

### Task 2: Fix synchronized scrolling with targeted signal disconnect
**Commit:** 5844523

Root cause: `_browse_rd_setup_sync_scroll()` called `text_bar.valueChanged.disconnect()` and `image_bar.valueChanged.disconnect()` which severed ALL connected slots, including QTextEdit's internal scroll handling. After the first sync setup, basic scrolling stopped working entirely.

Fix:
- Added `_browse_rd_disconnect_sync()` helper method that disconnects only the stored handler references
- Sync handler closures stored as `self._rd_text_sync_handler` and `self._rd_image_sync_handler`
- All disconnect calls are now targeted: `.disconnect(self._rd_text_sync_handler)` instead of `.disconnect()`
- Handler attributes initialized to None in `__init__` alongside other reading desk state

## Deviations from Plan

None -- plan executed exactly as written.

## Task Commits

| Task | Name | Commit | Key Changes |
|------|------|--------|-------------|
| 1 | Fix scroll area lifecycle | f6ce4f8 | Create QScrollArea once, repopulate container on re-render |
| 2 | Fix synchronized scrolling | 5844523 | Targeted signal disconnect with stored handler references |

## Verification Results

| Check | Result |
|-------|--------|
| Import passes | PASS |
| No deleteLater in _browse_rd_render_images | PASS |
| No blanket valueChanged.disconnect() | PASS |
| Sync handler refs stored | PASS |
| _browse_rd_disconnect_sync exists | PASS |
| Scroll area created once in enter, not render | PASS |

## UAT Test Coverage

| Test | Issue | Fix |
|------|-------|-----|
| T12 | Desktop: fragments not displayed (splitter corruption) | QScrollArea created once, container repopulated |
| T13 | Desktop: synchronized scrolling broken | Targeted signal disconnect preserves internal handlers |
| T15 | Desktop: toolbar add shows only first fragment | Same root cause as T12 -- splitter corruption fixed |

## Decisions Made

1. **DEC-11-11-01:** Create QScrollArea once at reading desk entry, repopulate container widget on each render call. This avoids Qt's deferred deletion creating phantom widgets in the splitter.

2. **DEC-11-11-02:** Store sync handler references as instance attributes and use targeted `disconnect(handler)` calls. This preserves QTextEdit's internal scroll handling which was being destroyed by blanket `disconnect()`.

## Next Phase Readiness

Desktop reading desk rendering and scrolling are now structurally correct. The remaining gap closure plans (11-09, 11-10) can proceed independently -- they address different issues (toolbar fragment display and version selector).

## Self-Check: PASSED
