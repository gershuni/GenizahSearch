---
phase: 11
plan: 08
subsystem: web-ui
tags: [css, light-mode, quasar, word-wrap, error-handling]
dependency_graph:
  requires: [11-06]
  provides: [light-mode-header-fix, text-wrap-fix, toolbar-error-guard]
  affects: [11-UAT]
tech_stack:
  added: []
  patterns: [quasar-text-color-prop, inline-important-override]
key_files:
  created: []
  modified: [web/pages/browse.py]
decisions:
  - id: d-1108-01
    summary: "Use inline style !important for non-button elements, Quasar text-color prop for buttons"
    context: "Quasar Light Mode overrides Tailwind text-white class; buttons need prop-level override"
metrics:
  duration: "6 min"
  completed: 2026-02-08
---

# Phase 11 Plan 08: Light Mode Header, Word Wrap, and Toolbar Error Fixes Summary

**CSS and error-handling fixes for three UAT-reported reading desk issues: Light Mode header visibility (Test 9), text pane word wrap (Test 11), and console RuntimeError on toolbar add (Test A2)**

## One-Liner
Light Mode header fixed via Quasar text-color prop + inline !important; text wrap restored by removing overflow:hidden; toolbar RuntimeError silenced

## Task Commits

| Task | Name | Commit | Key Changes |
|------|------|--------|-------------|
| 1 | Fix Light Mode header visibility (Test 9) | 65e963e | Icon/label: inline `color: white !important;`; Button: `text-color=white` prop |
| 2 | Fix text pane word wrap (Test 11) and toolbar RuntimeError (Test A2) | f5cb3e6 | Remove `overflow: hidden` from text container; catch RuntimeError in toolbar_add |

## Changes Made

### Task 1: Light Mode Header Visibility
- **Reading desk header bar**: Icon and label changed from `classes('text-white')` to `.style('color: white !important;')`. Button changed from `.style('color: white !important;')` to `.props('text-color=white')`.
- **Add from List dialog header**: Same fix applied to icon and label (line 2254-2255).
- **Document preview dialog header**: Same fix applied to icon, label, and badge (lines 2047-2050) -- deviation Rule 2.

**Why different approaches for different elements:**
- Buttons: Quasar's `text-color` prop sets color at component level, above CSS cascade. This is the official API and matches 20+ other buttons in the codebase.
- Non-button elements (icons, labels): Don't have Quasar's `text-color` prop, so inline style with `!important` is needed to override Light Mode theme.
- `classes('text-white')`: Tailwind utility class overridden by Quasar's Light Mode theme at higher specificity -- does NOT work.

### Task 2: Text Pane Word Wrap and Toolbar RuntimeError
- **Reading desk text container** (line 2728): Removed `overflow: hidden` that was clipping vertically-expanded wrapped text.
- **Single-page render_text_content** (line 3264-3267): Added `overflow-wrap: break-word; word-break: break-word;` to ensure long text wraps.
- **toolbar_add_by_shelfmark** (lines 2222-2243): Added `except RuntimeError` before the general `except Exception` to silently catch stale slot references after UI rebuilds. Also guarded the `ui.notify` in the general exception handler.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Fixed Document preview dialog header Light Mode visibility**
- **Found during:** Task 1
- **Issue:** The Document preview dialog (line 2047-2050) had the same `classes('text-white')` pattern that fails in Light Mode
- **Fix:** Applied same inline style + prop fixes as the reading desk header
- **Files modified:** web/pages/browse.py
- **Commit:** 65e963e

## Verification Results

| Check | Result |
|-------|--------|
| `python -c "from web.pages.browse import create_browse_page; print('OK')"` | PASSED |
| No `classes('text-white')` in reading desk header bar | PASSED |
| Button uses `text-color=white` prop | PASSED (line 2396) |
| Icon/label use `color: white !important;` | PASSED (lines 2380, 2385) |
| Text container has no `overflow: hidden` | PASSED |
| RuntimeError caught in toolbar_add_by_shelfmark | PASSED (lines 2237, 2243) |
| `overflow-wrap: break-word` in render_text_content | PASSED (line 3273) |

## Decisions Made

| ID | Decision | Rationale |
|----|----------|-----------|
| d-1108-01 | Inline `!important` for non-buttons, Quasar `text-color` prop for buttons | Quasar buttons need prop-level override; icons/labels don't support `text-color` prop |

## Next Phase Readiness

No blockers. Three UAT gaps (Test 9, Test 11, Test A2) are now fixed. Remaining gap closure plans (11-09, 11-10, 11-11) can proceed independently.

## Self-Check: PASSED
