---
phase: 11-virtual-reading-desk
plan: 06
subsystem: web-ui
tags: [reading-desk, bug-fix, gap-closure, browse, nicegui]
depends_on:
  requires: [11-01, 11-02, 11-05]
  provides: [web-reading-desk-gap-fixes]
  affects: []
tech-stack:
  added: []
  patterns: [inline-css-theme-override, expansion-panel-dialog, priority-state-restore]
key-files:
  created: []
  modified: [web/pages/browse.py]
decisions: []
metrics:
  duration: ~4 min
  completed: 2026-02-08
---

# Phase 11 Plan 06: Web Reading Desk Gap Closure Summary

**One-liner:** Fix 5 web reading desk visual, UX, and state persistence bugs found during human verification (W1-W5).

## Task Commits

| # | Task | Commit | Key Changes |
|---|------|--------|-------------|
| 1 | Fix visual bugs (W2, W3, W5) and list dialog (W1) | ced7d81 | Back to Page View button white CSS, badge inline styling, text word-wrap, expansion panel list dialog |
| 2 | Fix language switch state persistence (W4) | 85fdc5b | Persist selected_sources, priority restore over URL params, error logging |

## What Changed

### W1: Add from List Dialog (UX)
The dialog previously showed only list names with a click-to-add card. Now uses `ui.expansion()` panels per list with:
- List name header with item count badge
- Expandable content showing each manuscript shelfmark
- Check icon for items already in reading desk
- "Add All (N)" button per list

### W2: Back to Page View Button (Light Mode)
Changed from `.props('flat dense text-color=white')` to `.props('flat dense').style('color: white !important;')`. The Quasar `text-color` prop was overridden by NiceGUI's light theme. Inline CSS with `!important` ensures white text on the green gradient header in both themes.

### W3: Fragment Count Badge (Dark Mode)
Removed `color='white'` and `outline` props which NiceGUI interpreted differently in dark mode. Replaced with direct inline CSS: `border: 1px solid white; color: white !important; background: transparent;` to bypass theme-dependent badge rendering.

### W4: Language Switch State Persistence
Root cause: when the browse page reloads after language switch, the URL may contain `sys_id` from the previously loaded manuscript. The code path for `initial_sys_id` took priority and loaded a single manuscript, skipping reading desk restore.

Fix: reading desk restore is now checked BEFORE falling through to `load_page()` when `initial_sys_id` is present. Also persists `selected_sources` dict so version selector preferences survive the reload.

### W5: Text Pane Word Wrap
Added `overflow-wrap: break-word; word-break: break-word;` to all three text label `.style()` locations in the reading desk text pane:
1. PGP source text in version handler callback
2. V0.8 fallback text in version handler callback
3. Initial text rendering for default source selection

Also added `overflow: hidden;` to the text content container.

## Deviations from Plan

### Plan Adjustment

**1. [Deviation] W4 fix uses priority reorder instead of ui.timer**
- **Plan said:** Use `ui.timer(0.3, do_restore, once=True)` for deferred execution
- **Actual fix:** Reordered initialization so reading desk restore is checked before `load_page()` even when `initial_sys_id` is in URL params
- **Rationale:** The root cause was execution ordering, not timing. A timer would be fragile and non-deterministic. Checking reading desk state before `initial_sys_id` is the correct architectural fix.

## Verification

- `python -c "from web.pages.browse import create_browse_page; print('OK')"` passes
- All 5 gap IDs (W1-W5) addressed in browse.py
- `overflow-wrap: break-word` present in 3 text label locations
- `ui.expansion` used in list dialog with manuscript shelfmark display
- `selected_sources` persisted and restored across language switch
- Error logging added with `[ReadingDesk]` prefix

## Self-Check: PASSED
