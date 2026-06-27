# Phase 128: Search Results Space-Scroll (SEED-025) - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-27
**Phase:** 128-Search Results Space-Scroll (SEED-025)
**Areas discussed:** Suppress-scroll set, Scroll step & direction, Web scroll target, Desktop Space semantics

---

## Suppress-scroll — when Space does NOT scroll

| Option | Description | Selected |
|--------|-------------|----------|
| Any focused result control or open dialog | Space scrolls unless a result's checkbox / expand-collapse / open-detail control has focus, or a detail dialog/accordion is open | ✓ |
| Only a focused checkbox | Space toggles only when a checkbox has focus; expand/open still scroll | |
| Only when a row detail is open/expanded | Space scrolls unless a result is currently expanded/open | |

**User's choice:** Any focused result control or open dialog (Recommended)
**Notes:** Matches the original request "scroll if nothing was selected to be checked/opened/closed." The exact membership set must be enumerated + tested.

---

## Scroll step & direction

| Option | Description | Selected |
|--------|-------------|----------|
| One viewport; Shift+Space = up | Standard browser/reader convention — page down by one visible height, Shift+Space up | ✓ |
| A few rows at a time | Fixed small nudge (3-5 rows) | |
| One viewport down only | Page down, no Shift+Space up | |

**User's choice:** One viewport; Shift+Space = up (Recommended)
**Notes:** Keep native PageUp/PageDown working too.

---

## Web scroll target

| Option | Description | Selected |
|--------|-------------|----------|
| Just the results pane | Scroll only the `.results-scroll-area` container; header/filters stay fixed | ✓ |
| The whole page | Let the document body scroll | |

**User's choice:** Just the results pane (Recommended)
**Notes:** Results already live in their own `ui.scroll_area` (search.py:1763).

---

## Desktop Space semantics

| Option | Description | Selected |
|--------|-------------|----------|
| Toggle only when checkbox cell focused, else page-scroll | Mirrors web; preserves focused-checkbox toggle | ✓ |
| Always page-scroll; checkbox via Enter/click | Space never toggles in the results table | |

**User's choice:** Toggle only when checkbox cell focused, else page-scroll (Recommended)
**Notes:** Desktop results table is a QTableWidget with a checkbox column (genizah_app.py:4828/4851).

---

## Claude's Discretion

- Mechanism for detecting "actionable focus" per platform (web `activeElement` test in the keydown `js_handler`; desktop `focusWidget()`/current-cell + checkbox-column test).
- Smooth vs instant scroll animation.

## Deferred Ideas

- Space-scroll on other scrollable surfaces (browse, reading desk, catalog, Joins Lab) — future polish.
- Library filter (SEED-026) — Phase 129.
