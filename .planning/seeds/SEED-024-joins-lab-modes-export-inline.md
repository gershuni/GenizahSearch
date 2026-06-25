# SEED-024 — Desktop Joins-Lab: search-mode parity + candidate XLSX export + inline options

> Source: user request 2026-06-25 ("another parity"). Folds the audit backlog item
> **SEED-019 #5** (desktop Joins-Lab candidate XLSX export — decision: XLSX via
> `shared/export_dossier.py`, not CSV).
> Branch: `audit/seed-024-joins-lab-modes-export-inline` off `master-main`.

## Problem

Three desktop/web parity + UX gaps in the desktop Joins Lab (`desktop/join_workbench.py`):

1. **Search modes** — the **web** Joins Lab anchor-side builder offers a 5-mode selector
   (Responsa-style + Exact / Variants / Fuzzy / Regex; `web/components/joins_builder.py`).
   The **desktop** builder was hardcoded to a single mode (`SearchThread(..., "exact", ...)`
   + `responsa_mode:True`) — Responsa-only, no selector.
2. **XLSX export** — the desktop Joins Lab had **no** export of any kind; the main Search tab
   has a rich 4-sheet research-grade XLSX export (`shared/export_dossier.py`).
3. **Options-in-a-dialog** — the global search options (variants / Judeo-Arabic / flex spacing /
   bidirectional) lived behind a "Search options ▾" button that opened a modal `QDialog`,
   unlike the main Search tab where they sit inline on the search row.

## Scope (delivered)

1. **Mode parity (web).** `JoinQueryBuilder(allow_modes=True)` on the anchor/main builder adds a
   `mode_combo` (5 items, index-driven via `get_mode()` — never `currentText()`). Responsa-style
   shows the structured row builder + inline options + preview; Exact/Variants/Fuzzy/Regex swap to
   a single free-text line (`_single_edit`) and hide the Responsa-only widgets
   (`_apply_mode_visibility`). `do_search()` branches: Responsa → `compose()` + `mode='exact'` +
   `responsa_options`; single-line → free text + `core_mode_for_join_mode(mode_key)` (`'literal'`/
   `'variants'`/`'fuzzy'`/`'Regex'` — the proven main-search strings) + `responsa_options=None`.
   The other-side builder stays Responsa-style only (`allow_modes=False`).

2. **Inline options (tiny tweak).** Deleted the "Search options ▾" button + `_open_search_options_dialog`;
   the 4 checkboxes are now inline on the controls row (mirrors the main-search responsa sub-row),
   shown only in Responsa mode. `_on_opts_changed` writes through to `_global_opts` (unchanged data
   flow — `_responsa_opts()`/`_merge_globals()` untouched). Persisted across sessions
   (`to_state`/`from_state` gained `mode_idx` + `single_text`); `_clear_lab` resyncs them.

3. **Candidate XLSX export (SEED-019 #5).** New "⬇ Export XLSX" button in the results toolbar.
   Scope = **selection, else all shown (filtered)** candidates (user-chosen — consistent with the
   Puzzle/List/Join selection-based bulk actions). `build_candidate_export_rows()` (pure, module-level)
   assembles candidates into the shared dossier `display`-dict shape; `_export_xlsx()` reuses the
   main-search builder `genizah_app._build_search_results_xlsx_bytes` → same 4-sheet workbook
   (Search Results / Manuscripts / Bibliography / Credits and Info). Triage counts go on the Credits
   sheet (the fixed 12-col main sheet is unchanged — keeps `test_export_xlsx_cross_parity` green);
   per-row triage is reachable via the triage filter (filter to ✓/? then export "all").

## Out of scope

- Web's cross-side **Combine (AND/OR)** is a different axis (cross-side filtering), not a search
  *mode* — the desktop already has it (`other_enable` + Narrow/Widen). Untouched.
- No per-row Triage column on the main sheet (would break the 12-col cross-app parity invariant).

## Tests

- `tests/test_joins_lab_modes_export.py` (NEW, main CI, no QApplication): `core_mode_for_join_mode`
  mapping (incl. unknown→exact), `build_candidate_export_rows` shape/defaults/empty, a **real
  workbook-build integration** test (rows → `_build_search_results_xlsx_bytes` → 4 sheets), and
  source guards (dialog removed, mode selector + inline options + export wiring present,
  `core_mode_for_join_mode` at the SearchThread call site).
- `tests/test_join_workbench_construct.py` (CI-skipped; local offscreen): updated the
  `_btn_search_opts` assertion → asserts dialog button gone + `chk_variants`/`mode_combo`/`btn_export`;
  added mode-switch visibility test + mode/single-text round-trip test.
- `tests/test_join_workbench_i18n.py`: 9 new HE translation keys added to `genizah_translations.py`.

## Verification

`test_joins_lab_modes_export.py` 16/16 + `test_join_workbench_i18n.py` + `test_join_workbench_construct.py`
8/8 (offscreen) green; full `-k join_workbench` 191 passed / 3 skipped; web `test_joins_builder.py`
31/31 in isolation (batch failures were pre-existing NiceGUI cross-test pollution, not this change);
ruff clean on changed files. Codex diff review, then PR. **Human UAT:** desktop Joins Lab — switch each
mode (single-line appears for non-Responsa), run a Fuzzy/Variants/Regex find, toggle inline options,
export candidates (selection vs all) to a 4-sheet xlsx.

## Done-when

Desktop Joins Lab offers the 5 web modes, edits options inline (no dialog), and exports candidates to
the shared 4-sheet xlsx. `OPEN_ISSUES.md` updated (SEED-019 #5 closed); Codex-reviewed; CI green; PR
squash-merged; desktop UAT passed.
