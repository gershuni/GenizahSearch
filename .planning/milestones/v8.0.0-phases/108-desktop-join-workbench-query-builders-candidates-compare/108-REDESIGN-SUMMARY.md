# Phase 108 Join Lab UI Redesign Summary

**Date:** 2026-06-05
**Branch:** master-main
**Objective:** Post-UAT UI redesign of Phase 108 Join Lab — minimalist layout matching the approved HTML sketch.

---

## One-liner

Join Lab redesigned: global search options behind a dialog, per-row ⚙ gear with copy-on-OK modifiers, Filter ▾ dialog with anchor-info panel, shared bulk-action bar, icon-only card buttons + context menu, table checkbox column, 30/70 splitter, anchor+candidate puzzle includes both.

---

## Commit Table

| Step | Commit | Message |
|------|--------|---------|
| 1 | `5ff1bb57` | feat(108): builder dict-backed global opts + Search-options dialog + per-row gear + ⓘ tooltip |
| 2 | `62d034e6` | refactor(108): remove shared modifier row + active-row machinery; clean up add_or_box |
| 3 | `d7d19f60` | refactor(108): candidate pane cleanup — remove tags/self-match/include-anchor; relabel other-side |
| 4 | `5b5b090d` | feat(108): Filter ▾ dialog + Browse results ▶ + bulk bar + selection set |
| 5 | `ab07a349` | feat(108): selection set + grid checkboxes + table checkbox col + master + bulk bar wiring |
| 6 | `ded3d1ac` | feat(108): anchor icon-only bottom + 30/70 splitter + puzzle open_anchors_in_puzzle |
| 7 | `e88766c9` | refactor(108): final i18n sweep + ruff + remove orphaned eventFilter remnants |

---

## What Changed

### JoinQueryBuilder (desktop/join_workbench.py)

**Before:** Always-visible shared modifier row (6 checkboxes) + always-visible global Search-Options row (variants/JA/flex/bidir); active-row eventFilter/focus-tracking machinery; multi-checkbox Modifiers label.

**After:**
- `_global_opts` dict (variants/ja/flex_spacing/bidirectional) with defaults; survives without ever opening dialog
- "Search options ▾" button opens a QDialog that writes back to `_global_opts` on Done
- Per-row `⚙` gear button opens a line-options QDialog: negation/plene/prefix/suffix/wildcard_prefix/wildcard_suffix + starts/ends line; copy-on-OK, Cancel discards
- Wildcard-prefix disabled **and cleared** in gear dialog when row has >1 OR box (RR-13)
- ⓘ button on each row with typed-sign legend tooltip (D-04: tooltip only, no parsing)
- `_responsa_opts()` reads from `_global_opts` (RR-14 output unchanged)
- `build_side_query()` reads variants from `_global_opts` (RR-1 output unchanged)
- Removed: `eventFilter`, `_on_row_focus`, `_on_modifier_changed`, `_refresh_modifier_enabled`, `chk_negation`, `chk_plene`, `chk_wild_start`, `chk_wild_end`, `chk_prefix`, `chk_suffix`, `chk_opt_variants`, `chk_opt_ja`, `chk_opt_flex`, `chk_opt_bidir` as instance attributes
- Removed: `installEventFilter(self)` from `_make_box` (eventFilter gone)
- Removed: `QEvent` import (unused)

### JoinCandidatePane (desktop/join_workbench.py)

**Before:** `_tag("THIS SIDE…")` section label; always-on refine bar with 5 filter widgets; self-match prefix in status; `include_anchor_chk` toggle; btn_visual/btn_combined stubs; old table header with 8 columns.

**After:**
- **Section tag removed** (adapted_decision 11)
- **Include-anchor checkbox removed** — anchor excluded by default, hardcoded (adapted_decision 11)
- **Self-match prefix removed** from status text (adapted_decision 11)
- **Other-side relabeled** "search also on the other side of the leaf (p ±1)" (adapted_decision 11)
- **btn_visual / btn_combined removed** (no longer referenced)
- **Filter ▾ button** opens `_open_filter_dialog()` — QDialog with current-fragment info panel, material/dim/triage/size controls, "from anchor" shortcuts (adapted_decision 12)
- Filter state uses persistent hidden filter widgets; `apply_filters()` data path unchanged
- **Results toolbar:** view toggle + "Browse results ▶" + stretch + "Filter ▾" + count label
- **Shared bulk-action bar** (`_bulk_bar_widget`): hidden until `_selected_keys` non-empty; Browse/Join enabled only when exactly 1 selected (adapted_decision 7)
- **`_selected_keys: set`** initialized in `_build_ui`; cleared on `do_search` + on `set_anchor`; pruned to filtered universe in `apply_filters` (adapted_decision 6)
- `_candidate_key(c)` → `"{sys_id}:{page}"` stable key (adapted_decision 6)
- **Table:** checkbox column 0 added; data columns shifted to 1..8 (adapted_decision 8)
- `_table_double_clicked(row, col)`: col 0 does NOT open compare (adapted_decision 8)
- Master select-all: `horizontalHeader().sectionClicked` → `_on_table_header_clicked` (adapted_decision 8)
- `_render_table` stores `_candidate_key` in `Qt.ItemDataRole.UserRole` on col-0 item
- `_on_table_cell_changed` keeps `_selected_keys` in sync with checkbox state

### CandidateCard (desktop/join_workbench.py)

**Before:** Text-label action buttons ("Browse", "Puzzle", "List", "Add as Join"); no checkbox; no context menu.

**After:**
- Per-card selection checkbox (reads `_selected_keys` on render, blocks signals during init) (adapted_decision 6)
- Icon-only action buttons: 📖🧩☰🔗⇄⚓ with tr() tooltips (adapted_decision 9)
- Right-click context menu (same actions + Y/?/N triage) via `customContextMenuRequested` (adapted_decision 9)
- `_restyle()` shows teal outline when card is in `_selected_keys`

### JoinWorkbenchWindow (desktop/join_workbench.py)

**Before:** Anchor actions (browse/puzzle/list/join) in right pane; splitter `[420, 540]`; `open_result_in_puzzle` only added candidate.

**After:**
- Anchor pane: 4 icon-only action buttons (📖🧩☰🔗) at **bottom** of left anchor pane (adapted_decision 14)
- Splitter: `setSizes([300, 700])` = ~30% anchor / 70% right, resizable (adapted_decision 14)
- `open_result_in_puzzle(c)` now calls `open_anchors_in_puzzle(puzzle_add_targets(anchor, [c.sys_id]))` — adds anchor + candidate (adapted_decision 10)
- `_build_right_pane` simplified (cold-start row + candidate pane only)

### genizah_app.py

- Added `open_anchors_in_puzzle(sys_ids: list)` — de-dupes preserving order, loops `open_anchor_in_puzzle` (adapted_decision 10)

### genizah_translations.py

New keys registered (all wrapped in `tr()` in join_workbench.py):
- `"Search options ▾"`, `"Search options"`, `"Global search options (...)"`, `"Expand spelling variants"`
- `"Line options"`, `"Line options (modifiers, starts/ends line)"`, `"Wildcard-prefix disabled for multi-box OR lines"`
- `"⊣ ends line"`, `"The FIRST word..."`, `"The LAST word..."`, `"Typed sign legend tooltip"`
- `"search also on the other side of the leaf (p ±1)"`, `"AND narrows: ... OR widens: ..."`
- `"Browse results ▶"`, `"Open Browse results compare window"`, `"Filter ▾"`, `"Open filter dialog"`
- `"Filter candidates"`, `"Current fragment"`, `"match material"`, `"width ±2 cm of anchor"`
- `"Filter by width (cm)"`, `"min"`, `"max"`, `"Size filter note"`, `"Reset"`
- `"selected"`, `"Browse — select exactly one"`, `"Add all to Puzzle (with anchor)"`, `"Add all to list"`, `"Add as join — select exactly one"`, `"✕ clear"`
- `"Browse this fragment"`, `"Add anchor to a Puzzle"`, `"Add anchor to a list"`, `"Start a join from this anchor"`
- `"Add to Puzzle (with anchor)"`, `"Add to list"`, `"Select this candidate"`

### tests/test_join_workbench_construct.py

Updated `test_join_candidate_pane_constructs`:
- Asserts 9 columns (was 8; col 0 = checkbox)
- Asserts `"gear"` key in first row entry
- Asserts `include_anchor_chk` absent
- Asserts `_selected_keys` present
- Asserts `_bulk_bar_widget` present
- Asserts `_btn_search_opts` on builder

---

## Deviations from Plan

### Auto-fixed Issues

**[Rule 3 - Blocking] `eventFilter` / `QEvent` cleanup**
- **Found during:** Step 7 (ruff)
- **Issue:** `QEvent` remained imported after `eventFilter` was removed; `installEventFilter(self)` in `_make_box` was orphaned
- **Fix:** Removed `QEvent` import and `installEventFilter` call
- **Files modified:** `desktop/join_workbench.py`
- **Commit:** `e88766c9`

**[Rule 2 - Missing critical] `_active_row`/`_updating_modifiers` attrs removed**
- **Found during:** Step 7 cleanup
- **Issue:** Instance attrs no longer used but cluttered `__init__`
- **Fix:** Removed both attrs from `__init__`
- **Files modified:** `desktop/join_workbench.py`
- **Commit:** `e88766c9`

### Adapted decisions implemented exactly

All 17 adapted_decisions implemented as specified. No architectural changes required beyond what was planned. The `_size_widget` / `_size_row` visible container from the old inline bar was eliminated — filter state is now in the persistent hidden widgets referenced directly by `apply_filters()`.

---

## Locked Invariants Verified

| Invariant | Status |
|-----------|--------|
| RR-1/RR-13: `build_side_query` / `compose` output unchanged | PASS — 316 parser tests green |
| RR-14: ja/flex/bidir merged into ro before search | PASS — `_merge_globals` unchanged |
| RR-12: None-page guard in `_enqueue_image_for_pane` | PASS — not touched |
| D-20: zero `_vs_*` calls in `join_workbench.py` | PASS — `test_join_workbench_no_private` green |
| D-06: no dialog-level `setLayoutDirection(RightToLeft)` | PASS — new dialogs use `dlg.exec()` without RTL |
| Window must open (construction order safe) | PASS — construct tests green |
| i18n: all `tr()` keys registered | PASS — `test_join_workbench_i18n` green |

---

## Known Stubs

None — all adapted_decisions fully implemented.

---

## Threat Flags

None — no new network endpoints, auth paths, file access patterns, or schema changes introduced.

---

## Self-Check

**Files exist:**

- `C:\Genizahsearch\desktop\join_workbench.py` — FOUND
- `C:\Genizahsearch\genizah_app.py` — FOUND (open_anchors_in_puzzle added)
- `C:\Genizahsearch\genizah_translations.py` — FOUND (new keys added)
- `C:\Genizahsearch\tests\test_join_workbench_construct.py` — FOUND (updated)
- `C:\Genizahsearch\.planning\phases\108-desktop-join-workbench-query-builders-candidates-compare\108-REDESIGN-SUMMARY.md` — FOUND (this file)

**Commits exist:**
- `5ff1bb57` — FOUND
- `62d034e6` — FOUND
- `d7d19f60` — FOUND
- `5b5b090d` — FOUND
- `ab07a349` — FOUND
- `ded3d1ac` — FOUND
- `e88766c9` — FOUND

**Test results:**
- 316 non-Qt tests: PASS
- 4 construct tests: PASS
- ruff: PASS (all files clean)

## Self-Check: PASSED
