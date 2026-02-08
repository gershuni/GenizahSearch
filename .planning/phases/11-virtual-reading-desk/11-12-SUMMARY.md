---
phase: 11-virtual-reading-desk
plan: 12
subsystem: web-ui
tags: [bugfix, reading-desk, light-mode, language-switch, word-wrap, timer-lifecycle]
depends_on:
  requires: [11-08, 11-09, 11-02]
  provides: [RuntimeError guard for timer callbacks, Light Mode header fix, language switch guard, word wrap fix]
  affects: [11-13]
tech-stack:
  added: []
  patterns: [try/except RuntimeError for NiceGUI timer lifecycle, !important override for Quasar .q-card rule, min-width:0 for flex item shrinking]
key-files:
  created: []
  modified:
    - web/components/version_selector.py
    - web/components/notes_display.py
    - web/components/joins_panel.py
    - web/pages/browse.py
decisions: []
metrics:
  duration: ~5 min
  completed: 2026-02-08
---

# Phase 11 Plan 12: Web UAT Gap Closure (Tests 3, 8, 9, 10) Summary

**Four web reading desk regressions fixed: RuntimeError from orphaned timers, Light Mode white-on-white header, language switch state loss, and inconsistent word wrap via min-width:0 on flex items.**

## Task Commits

| Task | Name | Commit | Key Changes |
|------|------|--------|-------------|
| 1 | Fix RuntimeError from orphaned ui.timer elements | f32f7c2 | version_selector.py, notes_display.py, joins_panel.py |
| 2 | Fix Light Mode header, language switch guard, word wrap | b966942 | browse.py |

## What Was Done

### Task 1: RuntimeError Guard for Timer Callbacks (Test 3)

Three `ui.timer(0.1, callback, once=True)` calls in component files create asyncio tasks that can outlive their parent NiceGUI elements. When the parent is garbage collected (e.g., after content_container.clear() on version change or navigation), the timer's `_run_once()` tries to access a dead weakref parent_slot and raises RuntimeError.

Fix: Each timer callback is now wrapped in a `_safe_*()` function with `try/except RuntimeError: pass`. This is the safest approach since NiceGUI does not cancel timer tasks on element deletion (known issues #1500, #3187).

**Files modified:**
- `web/components/version_selector.py` - `_safe_load()` wrapping `load_and_apply_latest()`
- `web/components/notes_display.py` - `_safe_check()` wrapping `check_comments()`
- `web/components/joins_panel.py` - `_safe_load_count()` wrapping `load_count()`

### Task 2: Three Independent Browse.py Fixes (Tests 8, 9, 10)

**Fix A - Light Mode white-on-white header (Test 8):**
The global CSS `.q-card { background: var(--bg-card) !important; }` in main.py overrode the reading desk header card's inline green gradient. Added `!important` to the header card's background style at line 2422.

**Fix B - Language switch state loss (Test 9):**
`update_content()` had `if not state.current_page:` which early-returned to the welcome prompt. During language-switch restoration, `enter_joined_view()` sets `state.view_joined=True` but never sets `state.current_page`, blocking the `elif state.view_joined:` branch. Changed guard to `if not state.current_page and not state.view_joined:`.

**Fix C - Inconsistent word wrap (Test 10):**
CSS flexbox default `min-width: auto` prevents flex items from shrinking below content intrinsic width. Added `min-width: 0;` to:
- Reading desk right pane card (line 2610)
- Text content container column (line 2774)
- Single-page text panel flex in both branches (line 3235)

## Decisions Made

No new architectural decisions. All fixes are minimal, targeted patches.

## Deviations from Plan

None - plan executed exactly as written.

## Verification

1. All three component files import without errors
2. browse.py imports without errors
3. `except RuntimeError` confirmed in all three component files
4. `!important` confirmed on header card gradient
5. `not state.view_joined` confirmed in early-return guard
6. `min-width: 0` confirmed in right pane card, text container, and both text_panel_flex branches

## Next Phase Readiness

Plan 11-13 (desktop UAT gap closure) can proceed independently.

## Self-Check: PASSED
