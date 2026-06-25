---
phase: 119-candidates-compare-visual-similarity
plan: "10"
subsystem: candidate-grid-polish
tags: [gap-closure, r2-4, r2-8, r2-9, r2-10, triage-icons, dark-mode, vs-transcription, render-smoke]
dependency_graph:
  requires: [119-09]
  provides: [icon-triage-row, vs-transcription-fetch, dark-table-css, render-smoke-r2-4-r2-9]
  affects: [119-11-compare-modal]
tech_stack:
  added: []
  patterns: [icon-glyph-buttons, css-vars-dark-mode, off-loop-io-bound, dataclasses-replace]
key_files:
  created: []
  modified:
    - web/components/candidate_grid.py
    - web/pages/joins_lab.py
    - web/static/common.css
    - tests/test_joins_lab_off_loop.py
    - tests/render_smoke/test_joins_lab_render_smoke.py
    - tests/test_candidate_grid.py
decisions:
  - "R2-4: _TRIAGE_COLORS derived from TRIAGE_ICONS (imported from shared.joins_lab) via dict comprehension — single source of truth, border-restyle logic unchanged"
  - "R2-9: browse button uses menu_book icon + js_handler navigation (both on_browse_click and normal branches use js_handler for consistency and to avoid nested link anti-pattern)"
  - "R2-8: get_browse_page returns a dict — read via page_data.get('text', ''), NOT .text attribute (HIGH-1 note in joins_executor.py confirmed)"
  - "R2-10: CSS targets .joins-candidate-table class (added to ui.table) not all .q-table to avoid app-wide styling side-effects"
  - "test_triage_btn_refs_assigned_after_button_creation: updated to accept parenthesised multi-line button assignment"
  - "compare_modal verdict buttons remain text labels (119-11 owns that file) — test_g3_compare still uses Yes/Maybe/No lookup"
metrics:
  duration: "15min"
  completed: "2026-06-19T15:36:26Z"
  tasks: 3
  files: 6
---

# Phase 119 Plan 10: R2-4/R2-8/R2-9/R2-10 Candidate Surface Polish Summary

Icon-glyph triage row (desktop parity), VS-only transcription beginning, and dark-mode-aware candidate table — all gap-closure items from the live R2 UAT.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | R2-4/R2-9: icon-glyph triage + browse/compare icon row | `79cbf8cb` | web/components/candidate_grid.py, tests/test_candidate_grid.py |
| 2 | R2-8 VS transcription + R2-10 dark-mode table CSS | `909434bb` | web/pages/joins_lab.py, web/static/common.css, tests/test_joins_lab_off_loop.py |
| 3 | Render-smoke: R2-4 + R2-9 assertions + updated G3 helper | `68b54b60` | tests/render_smoke/test_joins_lab_render_smoke.py |

## What Was Built

**Task 1 — R2-4 / R2-9 (candidate_grid.py):**

- Imported `TRIAGE_ICONS` from `shared.joins_lab`; derived `_TRIAGE_COLORS = {k: v["color"] for k, v in TRIAGE_ICONS.items()}` — single source of truth, zero color drift.
- Replaced `ui.button(tr("Yes"/"Maybe"/"No"))` with `ui.button(TRIAGE_ICONS[v]["glyph"])` (`✓`/`?`/`✗`) + `.tooltip(tr(TRIAGE_ICONS[v]["tooltip"]))` — desktop parity (R2-4).
- Active state style preserved: `background:{_TRIAGE_COLORS[verdict]}; color:#fff` on click; G3 `_triage_btn_refs` immediate-fill mechanism fully preserved.
- Browse and compare controls converted to icon-only buttons (`menu_book` / `compare_arrows`) placed in the **same row** as the triage glyphs (R2-9), with `.tooltip(tr(...))`. Both use `js_handler` for client-side navigation (no server-side `stop_propagation` — AST guard green).
- Added `joins-candidate-table` class to `ui.table` in `create_candidate_table` for R2-10 CSS targeting.

**Task 2 — R2-8 (joins_lab.py `run_vs_meta_core`):**

- Added transcription-beginning fetch inside the existing `run.io_bound(run_vs_meta_core)` worker: calls `executor.get_browse_page(c.sys_id)` (returns dict), reads `page_data.get("text", "")`, truncates to 200 chars, applies via `dataclasses.replace(c, full_text=prefix)`.
- Failure is caught with bare `except Exception: pass` — graceful: card stays blank (no crash).
- Off-loop guard `test_vs_meta_lookup_not_on_event_loop` updated to include `get_browse_page` in the guarded method list.

**Task 2 — R2-10 (common.css):**

Added under `[data-theme="dark"]`:
```css
[data-theme="dark"] .joins-candidate-table { background: var(--bg-secondary) !important; }
[data-theme="dark"] .joins-candidate-table .q-table thead tr th { background: var(--bg-tertiary) !important; }
[data-theme="dark"] .joins-candidate-table .q-table tbody td { background: var(--bg-card) !important; }
[data-theme="dark"] .joins-candidate-table .q-table tbody tr:hover td { background: var(--bg-hover) !important; }
```
All vars only — no hardcoded hex.

**Task 3 — Render-smoke (test_joins_lab_render_smoke.py):**

- Updated `_find_yes_triage_buttons` to locate `✓` glyph button (was `'Yes'`/`'כן'`).
- Added `_find_triage_glyph_buttons` helper finding all three glyphs.
- Added `test_r2_4_triage_icon_buttons_on_cards`: asserts glyph buttons present, all three glyphs covered, NO old `Yes/Maybe/No/כן/אולי/לא` text-label buttons visible.
- Added `test_r2_9_browse_compare_icon_buttons_in_triage_row`: asserts `compare_arrows` + `menu_book` icon buttons visible alongside glyph triage buttons.
- `test_g3_triage_button_fill_updates_on_click` now finds the `✓` glyph button via the updated helper and still asserts `background:#15803d + color:#fff` on click.

## Verification

- `python -m pytest tests/test_candidate_grid.py tests/test_joins_lab.py tests/test_joins_lab_off_loop.py -q` → **147 passed**
- `python -m pytest tests/render_smoke/test_joins_lab_render_smoke.py -q` → **9 passed** (7 original + 2 new R2-4/R2-9)
- `python -m pytest tests/test_no_server_side_stop_propagation.py tests/test_no_raw_storage_access.py -q` → **9 passed**
- Full combined run: **165 passed**
- CSS grep: `grep "data-theme.*dark.*joins-candidate-table\|joins-candidate-table.*var(--bg" web/static/common.css` → 7 matches

## Deferred / Human-UAT Required

The following R2 items are proven by unit/off-loop/render-smoke assertions but require **live browser verification**:

| Item | What's proven | What requires live browser |
|------|--------------|---------------------------|
| **R2-8 (VS-only transcription)** | `get_browse_page` called off-loop; `dataclasses.replace(full_text=prefix)` applied; conftest `STUB_BROWSE_PAGE` has `text='Anchor fragment text'` but VS-only stubs have their own text flows | Visual: open a fragment with VS matches + no text query → confirm card shows first 200 chars of transcription instead of blank |
| **R2-10 (dark-mode table)** | CSS rule exists with correct vars; `joins-candidate-table` class on `ui.table` | Visual: toggle dark mode → switch to Table view → confirm dark background (not white) |

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] test_triage_btn_refs_assigned_after_button_creation assertion**
- **Found during:** Task 1 test run (1 failure)
- **Issue:** The test asserted `"_btn_el = ui.button("` as a literal string; the refactored code uses a multi-line parenthesised form `_btn_el = (\n    ui.button(glyph)`.
- **Fix:** Updated assertion to accept `"_btn_el = ("` + `"ui.button("` (parenthesised chained form) in addition to the inline form.
- **Files modified:** `tests/test_candidate_grid.py`
- **Commit:** `79cbf8cb`

## Known Stubs

None. All implemented features are wired; no hardcoded placeholder data flows to the card UI. R2-8 and R2-10 live-browser items are deferred render-observable checks, not stubs in the codebase.

## Threat Flags

No new security surface introduced beyond what the plan's threat model covers:
- R2-8 transcription text flows through the existing `snippet_html()` escape path (T-119-R8).
- Browse icon button uses `json.dumps`-escaped URL literal (T-119-R9).
- R2-10 is CSS only.

## Self-Check: PASSED

- [x] `web/components/candidate_grid.py` imports `TRIAGE_ICONS`; triage row uses glyphs; browse/compare are icon buttons
- [x] `web/pages/joins_lab.py` `run_vs_meta_core` fetches `page_data.get("text","")` and applies `dataclasses.replace(full_text=...)`
- [x] `web/static/common.css` contains `[data-theme="dark"] .joins-candidate-table` with `var(--bg-*` vars
- [x] `tests/test_joins_lab_off_loop.py` names `get_browse_page` in guarded method list
- [x] `tests/render_smoke/test_joins_lab_render_smoke.py` has `test_r2_4_*` and `test_r2_9_*`
- [x] Commit `79cbf8cb` exists (Task 1)
- [x] Commit `909434bb` exists (Task 2)
- [x] Commit `68b54b60` exists (Task 3)
- [x] 165 tests pass (combined)
