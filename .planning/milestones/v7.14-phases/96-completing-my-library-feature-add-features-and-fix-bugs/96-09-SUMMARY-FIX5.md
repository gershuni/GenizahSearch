---
phase: 96
plan: "09"
iteration: 5
subsystem: desktop-local-browse
tags: [local-browse, result-dialog, navigation, line-numbers, ux-polish]
dependency_graph:
  requires: [96-09-SUMMARY-FIX4.md]
  provides: [item1-local-filepath-label, item2-shared-browse-nav, item3-per-page-line-numbers]
  affects: [desktop/result_dialog.py, genizah_app.py]
tech_stack:
  patterns: [PyQt6, apply_line_numbered_text per-page mode, dispatch pattern]
key_files:
  modified:
    - desktop/result_dialog.py
    - genizah_app.py
decisions:
  - "Item 2: reuse btn_b_prev/btn_b_next/btn_b_all via dispatch rather than adding show/hide logic"
  - "Item 3: use existing pages= parameter of apply_line_numbered_text (already implemented for Full Manuscript View)"
  - "Item 3: extract _get_local_pages_for_sys_id helper to avoid double index query"
metrics:
  duration: ~40min
  completed: "2026-05-24"
  tasks_completed: 3
  files_changed: 2
---

# Phase 96 Plan 09: Fix5 — Three UX Polish Items

One-liner: LOCAL filepath label in ResultDialog header + shared Browse nav buttons + per-page line-number restart in View All.

## Items Implemented

### Item 1 — Show LOCAL file path in ResultDialog (commit 3c47b95e)

**Files:** `desktop/result_dialog.py`

Added `lbl_local_file_path` QLabel to the header `meta_col` layout, positioned between `lbl_shelf` and `lbl_title`.

- Styled: italic blue, font-size 11px (`color: #2980b9; font-style: italic`)
- Hidden by default (`setVisible(False)`)
- Populated in `load_result_by_index` from `hit['display']['shelfmark']` (canonical filepath for LOCAL hits) with fallback to `_rd_local_filepath`
- Displays `folder/filename` format; full path in tooltip
- Hidden for all Genizah (non-LOCAL) hits

### Item 2 — Remove 4 duplicate LOCAL Browse nav widgets (commit 7e86f862)

**Files:** `genizah_app.py`

**Removed widgets** (4 total):
- `btn_local_browse_prev`
- `btn_local_browse_next`
- `lbl_local_browse_page`
- `btn_local_browse_view_toggle`

**Rewired to shared widgets:**
- `btn_b_prev` / `btn_b_next`: now connected to `_browse_prev_next(offset)` which dispatches to `_on_local_browse_nav` for LOCAL, `browse_navigate` for Genizah
- `btn_b_all`: `toggle_browse_view_all` dispatches to `_toggle_local_browse_view_mode` for LOCAL; in per-page mode labeled "View All", in view-all mode labeled "Per page"
- `lbl_browse_page_count`: reused for LOCAL page indicator (`Page N / total` or `Chunk N / total`)

**New helpers:**
- `_is_browsing_local()`: True when `current_browse_sid` is a LOCAL sys_id
- `_browse_prev_next(offset)`: unified dispatch prev/next handler

**`_show_local_browse_controls(visible)`** updated:
- `visible=False`: clears `lbl_browse_page_count`, disables `btn_b_prev`/`btn_b_next`, restores `btn_b_all` text to "View All"
- `visible=True`: no-op (state set by callers per mode)

### Item 3 — View All line numbers restart per page/chunk (commit 1592e28f)

**Files:** `genizah_app.py`

**New helper:** `_get_local_pages_for_sys_id(sys_id)` returns sorted `[(p_num, text), ...]`. Extracted from `_get_local_full_text_for_sys_id` to avoid double index queries; `_get_local_full_text_for_sys_id` now delegates to it.

**`_open_local_browse` (view-all path):**
- Fetches raw pages via `_get_local_pages_for_sys_id`
- Passes `pages=[text, ...]` to `apply_line_numbered_text` when `>1` pages exist
- `apply_line_numbered_text` enters per-page mode via `_mark_blocks_for_pages`:
  - Page/chunk content blocks: `userState = page_index` → numbered 1..N per page
  - Separator lines (`— page N —`): `userState = -1` → no gutter number
- Single-page or fallback-placeholder: stays in legacy continuous mode (no `pages=`)

The `pages=` parameter was already implemented for the Full Manuscript View (v7.12.0 Reading Desk). No changes to `line_number_text_edit.py` required.

## Regression

All 8 tests in `tests/test_local_nav_codex_fix4.py` pass after each commit.

## Deviations from Plan

None — all three items implemented as specified.

## Self-Check

- [x] `desktop/result_dialog.py` modified (lbl_local_file_path added)
- [x] `genizah_app.py` modified (Item 2 + 3)
- [x] Commits: 3c47b95e, 7e86f862, 1592e28f
- [x] Iteration 4 tests: 8/8 passing
- [x] Syntax OK (ast.parse)

## Self-Check: PASSED
