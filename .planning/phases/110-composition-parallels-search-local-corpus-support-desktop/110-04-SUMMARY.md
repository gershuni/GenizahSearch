---
phase: 110-composition-parallels-search-local-corpus-support-desktop
plan: "04"
subsystem: export
tags: [export, local, composition, xlsx, csv, txt, docx, bilingual, exp-f3, d-12]
dependency_graph:
  requires:
    - shared/export_dossier.py::build_local_document_row (Phase 103 Plan 01)
    - shared/export_dossier.py::local_documents_header_row (Phase 103 Plan 01)
    - shared/export_dossier.py::sheet_titles (local_documents key, Phase 103 Plan 01)
    - shared/docx_export.py::write_docx_result_block (Phase 103 Plan 01)
    - shared/local_sys_id.py::is_local_sys_id (Phase 95)
    - genizah_app.py::_get_meta_for_header / _comp_item_is_local / _prime_comp_local_filepath_cache (Phase 110 Plan 03)
  provides:
    - shared/export_dossier.py::_build_local_comp_row (module-level helper, C1)
    - shared/export_dossier.py::_partition_comp_export_rows (module-level helper, C1)
    - shared/export_dossier.py::filter_genizah_ids_for_metadata (Round-2 #1 / D-12)
    - genizah_app.py::export_comp_report (LOCAL-aware across xlsx/csv/txt/docx; LOCAL ids excluded from NLI prefetch)
  affects:
    - genizah_app.py
    - shared/export_dossier.py
tech_stack:
  added: []
  patterns:
    - module-level pure helpers in export_dossier (importable + offline-testable, C1)
    - LOCAL detection via is_local_sys_id / src_lbl on grouped comp items (NO display dict — Pitfall 2)
    - LOCAL ids filtered OUT of unique_ids before _fetch_metadata_with_dialog (D-12)
    - batch-primed _local_filepath_cache via my_library_tab._indexer.get_filepaths (M5 — never self.indexer)
    - LOCAL page resolved from page-level p_num/chunk_locator first, header parse fallback (C6)
    - structural Genizah parity (local_table_rows empty when no LOCAL items — C5)
key_files:
  created: []
  modified:
    - shared/export_dossier.py
    - genizah_app.py
    - genizah_core.py
    - genizah_translations.py
decisions:
  - "LOCAL row/partition logic is MODULE-LEVEL in export_dossier (C1) so the Wave-0 tests import stable symbols, not closures"
  - "genizah_ids = filter_genizah_ids_for_metadata(unique_ids, is_local_sys_id) replaces unique_ids as the source for missing/_fetch_metadata_with_dialog — a LOCAL-only export never fires an NLI lookup (D-12)"
  - "LOCAL filepath cache primed once over the full M7 coverage set (Main+Appendix+Filtered+Filtered-Appendix+Known) via the LOCAL indexer; Genizah row builders skip LOCAL items when _has_local"
  - "LOCAL emission reuses the exact Phase 103 conventions: XLSX 'Local Documents' sheet (5-col bilingual header + rich *-highlight cell); CSV labeled section after the Genizah table; TXT === filename | parent === / Path: fp (page N) block (D-09); DOCX per-result handout block via write_docx_result_block (M6 result_dict)"
metrics:
  duration: "~25 minutes"
  completed_date: "2026-06-08"
  tasks_completed: 2
  files_changed: 2
---

# Phase 110 Plan 04: LOCAL-Aware Composition Export (EXP-F3) Summary

**One-liner:** `export_comp_report` now emits LOCAL "My Library" composition hits with local-meaningful columns (filename / parent folder / full filepath / page / matched-text) across all four formats — reusing the Phase 103 Local Documents helpers and the on-screen comp display conventions — while filtering private LOCAL `97…` ids out of the NLI metadata prefetch and leaving the Genizah-only path structurally unchanged.

## What Was Built

### Task 1 — Module-level helpers + metadata-prefetch filter + LOCAL filepath prime

**`shared/export_dossier.py`** (C1 — three new pure, Qt-free, importable helpers next to `build_local_document_row`):
- `_build_local_comp_row(filename, parent_folder, full_filepath, page, matched_text_raw, sanitize_fn=None)` — thin wrapper delegating to `build_local_document_row` (returns the same 5-cell list).
- `_partition_comp_export_rows(items, is_local_fn, local_row_fn)` — splits grouped comp items into `(genizah_items, local_rows)`; objects untouched + order preserved; `(items_same_order, [])` when no LOCAL items (the C5 structural-parity guarantee).
- `filter_genizah_ids_for_metadata(unique_ids, is_local_fn)` — `[uid for uid in unique_ids if not is_local_fn(uid)]` (the Round-2 #1 / D-12 filter, surfaced so the test can import a named helper).

**`genizah_app.py::export_comp_report`:**
- **Part B (D-12 / Round-2 #1):** after `unique_ids = list(set(all_ids))`, `genizah_ids = filter_genizah_ids_for_metadata(unique_ids, is_local_sys_id)`; the `missing` / `_fetch_metadata_with_dialog` loop now reads from `genizah_ids`, NOT `unique_ids`. A LOCAL-only export yields `genizah_ids == []` → `missing == []` → the NLI dialog is never shown for private LOCAL ids.
- **Part C (M5/M7):** `local_ids = [sid for sid in unique_ids if is_local_sys_id(sid)]`; `_local_filepath_cache` primed ONCE via `my_library_tab._indexer.get_filepaths(local_ids)` (LOCAL indexer — NOT `self.indexer`, the Genizah Indexer with no `get_filepaths`); `_has_local = bool(local_ids)`. Added method-local `_resolve_item_sid`, `_is_local_item` (src_lbl=='LOCAL' fast path, else the real `97…` sys_id — Pitfall 2: grouped comp items carry no `display` dict), and `_local_row_for_page` (filepath from cache, page from page-level `p_num`/`chunk_locator` first per C6, matched-text via `_clean_and_marker(source_ctx)`).

### Task 2 — LOCAL-aware emission across all four formats (Main + Appendix)

- `_collect_local_comp_pages()` builds the ordered `(sid, page)` coverage set over Main + Appendix + Filtered + Filtered-Appendix + Known (M7); `local_table_rows` is the parallel 5-col set. Both empty when `_has_local` is False.
- Genizah row builders (`add_rows` + the Report View tree's `add_items_with_group`) `continue` past LOCAL items when `_has_local`, so LOCAL never appears on the 10-col Genizah surface.
- **XLSX:** dedicated `"Local Documents"` sheet titled `sheet_titles(CURRENT_LANG)['local_documents']`, header `local_documents_header_row(CURRENT_LANG)`, 5-col rows, matched-text cell uses the branch's `write_rich_cell` (`*`-highlight), widths `[45,25,80,10,70]` — mirroring the Phase 103 search export.
- **CSV:** LOCAL rows appended after the Genizah table under a labeled section (`sheet_titles[...]['local_documents']` separator + `local_documents_header_row`), single-file (Phase 103 convention).
- **DOCX:** per-result "research handout" blocks via `write_docx_result_block(doc, result_dict, filepath=fp, lang=CURRENT_LANG)` after the Genizah table (M7 ordering), under a `local_documents` heading; `result_dict` is the full M6 contract `{'display': {'source':'LOCAL','id':sid,'shelfmark':filename}, 'snippet':…, 'chunk_locator':…, 'p_num':…}`.
- **TXT:** `_fmt_ms_entry` gains a LOCAL branch (`=== {filename} | {parent} ===` / `Path: {fp}  (page N)` / matched-text) per D-09.

All new headers/titles come from the Phase 103 bilingual helpers — no hardcoded column labels, no new `tr` key (D-11/D-13 satisfied).

## Verification Results

```
python -m pytest tests/test_comp_export_local.py -q
4 xpassed in 0.14s   (all 4 EXP-F3/D-12 tests green: row-shape, all-formats partition,
                      genizah-only structural parity, local-only no-metadata-fetch)

python -m pytest tests/test_comp_corpus_scope.py -q
12 passed in 1.63s   (still green)

python -c "import genizah_app; import shared.export_dossier"
exit 0

python -m ruff check genizah_app.py shared/export_dossier.py
All checks passed!

python -m pytest tests/test_local_export_xlsx.py tests/test_local_export_csv_txt_docx.py \
  tests/test_local_export_non_regression.py tests/test_export_xlsx_cross_parity.py \
  tests/test_export_dossier.py tests/test_export_dossier_local.py \
  tests/test_docx_export_block.py tests/test_desktop_xlsx_multi_sheet.py -q
167 passed, 1 warning   (Phase 103 search-export regression suite unaffected)
```

Note: the EXP-F3 tests are authored as `@pytest.mark.xfail(strict=False)` (Wave-0 scaffold). With the Plan-04 helpers now present they report as **xpassed** — the intended green state; `strict=False` means xpass is not a failure.

## Commits

| Task | Commit | Message |
|------|--------|---------|
| 1 | e4e7f3a2 | feat(110-04): add module-level LOCAL comp export helpers + filter LOCAL ids from NLI prefetch |
| 2 | 131f5a42 | feat(110-04): emit LOCAL-aware comp export across xlsx/csv/txt/docx (Main+Appendix) |

## Deviations from Plan

**1. [Rule 2 - Missing critical functionality] Added `filter_genizah_ids_for_metadata` as a named module-level helper**

- **Found during:** Task 1
- **Issue:** The plan specified an inline comprehension for the D-12 metadata filter, but `tests/test_comp_export_local.py::test_local_only_export_no_metadata_fetch` prefers a named helper (`from shared.export_dossier import filter_genizah_ids_for_metadata`) and falls back to the inline comprehension only when absent.
- **Fix:** Added `filter_genizah_ids_for_metadata(unique_ids, is_local_fn)` to `shared/export_dossier.py` and used it in `export_comp_report` (`genizah_ids = filter_genizah_ids_for_metadata(unique_ids, is_local_sys_id)`). This makes the test exercise the production helper directly rather than a private re-implementation.
- **Files modified:** `shared/export_dossier.py`, `genizah_app.py`
- **Commit:** e4e7f3a2

**2. Implementation detail — `_collect_local_comp_pages` (pages) rather than only `_partition_comp_export_rows` (items)**

The DOCX branch needs the full M6 `result_dict` per LOCAL page (snippet/chunk_locator/p_num), and XLSX/CSV need 5-col rows per page, so `export_comp_report` collects ordered `(sid, page)` pairs over the M7 coverage set and derives both surfaces from them. `_partition_comp_export_rows` remains the module-level, importable contract the Wave-0 tests pin (item-level partition); the per-page collection inside the function is the page-granular realization of the same Genizah-vs-LOCAL split. No behavioral divergence from the plan — same coverage, same ordering (Genizah first, then LOCAL; Main then appendix groups by size).

## Authentication Gates

None — no auth-gated operations in this plan.

## Known Stubs

None. All four format branches emit fully-resolved LOCAL data from the primed filepath cache and the page-level fields; no hardcoded empties or placeholders introduced.

## Threat Flags

None new. T-110-01 (Information Disclosure — LOCAL ids reaching NLI) is mitigated as planned: `genizah_ids` filters LOCAL `97…` ids out of the metadata prefetch before `_fetch_metadata_with_dialog`, pinned by `test_local_only_export_no_metadata_fetch`. T-110-05 (full filepath column) is the intended, user-requested LOCAL export column (accepted, matches Phase 103). No new network endpoints, auth paths, or schema changes introduced. The three v7.14 cloud-write gates are untouched (this function calls none of them).

## Self-Check: PASSED

Files exist:
- shared/export_dossier.py — FOUND (modified; `_build_local_comp_row`, `_partition_comp_export_rows`, `filter_genizah_ids_for_metadata` present)
- genizah_app.py — FOUND (modified; `export_comp_report` references `genizah_ids`, `_has_local`, `_is_local_item`, `_local_row_for_page`, `local_documents_header_row`, `sheet_titles`, `write_docx_result_block`)

Commits exist:
- e4e7f3a2 — FOUND
- 131f5a42 — FOUND

---

## Code Review Corrections (2026-06-08, Codex review `110-CODEX-CODE-REVIEW.md`)

The Codex code review of Phase 110 surfaced 7 findings (1 BLOCKER, 2 HIGH, 3 MED, 1 LOW), all now fixed and committed. The original Plan-04 work above stands; these are corrections to the LOCAL display + LOCAL-only export shape that the first pass missed.

### Fixes applied

**FIX 1 — [BLOCKER / D-12] LOCAL `97…` ids no longer leak to NLI/FJMS during display grouping.**
- `genizah_core.py::group_composition_results` filters LOCAL ids out of `ids` (`genizah_ids = [sid for sid in ids if sid and not is_local_sys_id(sid)]`) before `batch_fetch_shelfmarks` — the prior code passed every grouped id, so a LOCAL comp run could reach the NLI network path before export. The export-time prefetch fix from the original Plan 04 did NOT cover the pre-display grouping path.
- `genizah_app.py::_collect_comp_domain_data` strips LOCAL ids from `all_sys_ids` before the FJMS domains + printed-status lookups.
- `genizah_app.py::display_comp_results::_collect_id` skips LOCAL ids so they never enter `ids_to_fetch` → `start_metadata_loading`.
- `genizah_app.py::start_metadata_loading` defensively strips LOCAL ids at the loader entry point (covers any caller).
- New pure-engine test `tests/test_comp_corpus_scope.py::test_local_comp_grouping_no_nli_fetch` proves `batch_fetch_shelfmarks` is called with `[]` for a LOCAL grouped item.

**FIX 2 — [HIGH / 5a] LOCAL-only composition export omits Genizah/MiDRASH/Stökl credits.**
- `export_comp_report` computes `_has_local` / `_local_only_comp_export` over ALL comp items (Main + Appendix + Filtered + Filtered-Appendix + Known) and threads `local_only=_local_only_comp_export` into `_get_credit_header(...)`. `_get_credit_header(local_only=True)` already suppressed the MiDRASH/Zenodo block — it was simply never called with the flag for comp exports.

**FIX 3 — [HIGH / 5b] LOCAL-only export drops empty Genizah sheets + uses "Documents" terminology.**
- XLSX: for a LOCAL-only export, the empty Genizah `Report View` / `Raw Data` sheets are removed before save (mirrors the Phase 103 search export at `genizah_app.py:2924-2934`); the workbook collapses to `[Local Documents, Query information]` with Local Documents active.
- CSV/DOCX/TXT: a LOCAL-only export does not emit the 10-column Genizah table/headers; the Local Documents surface is primary. Summary/section labels use LOCAL terminology.
- New FLAT EN→HE keys in `genizah_translations.py`: `Documents Found` / `Main Documents` / `Filtered by Text (Documents)` / `Excluded Documents` / `EXCLUDED DOCUMENTS` / `Matched Text`.
- MIXED exports keep BOTH surfaces (Genizah manuscript rows + Local Documents); Genizah-only exports are structurally unchanged.

**FIX 4 — [MED] Excluded LOCAL rows no longer duplicate into "Excluded Manuscripts".**
- XLSX excluded-sheet writer and DOCX excluded-table writer partition `c_known` into Genizah vs LOCAL; only Genizah known items go to "Excluded Manuscripts". LOCAL known items stay only in the Local Documents surface. For a LOCAL-only export the "Excluded Manuscripts" sheet/section is not created at all.

**FIX 5 — [MED] Filepath cache primed at the display entry point (covers session restore).**
- `display_comp_results` calls `_prime_comp_local_filepath_cache(...)` over the flattened main/appendix/filtered/known set at the START of the method, so session-restored comp data (rendered directly via `display_comp_results`) never falls back to per-row `indexer.get_filepath` on the UI thread. The fresh-scan prime is retained (harmless).

**FIX 6 — [MED] `_has_local` uses the item predicate, not just the 97-prefix.**
- `_has_local = any(_is_local_item(it) for it in _all_comp_items)` (so a `src_lbl='LOCAL'` item with a missing/non-97 sid is still exported to Local Documents). `local_ids` (97-prefix) is kept only for filepath-cache priming.
- New test `test_src_lbl_local_without_display_lands_on_local_surface`.

**FIX 7 — [LOW] Removed stale xfail markers + added the two reported-bug tests.**
- Removed the 4 `@pytest.mark.xfail(strict=False)` decorators in `tests/test_comp_export_local.py` (the helpers exist; tests are real passes now). Removed the now-unused `pytest` import.
- Added `test_local_only_export_omits_genizah_credits` (5a) and `test_local_only_export_uses_document_terminology` (5b) — both pure (no Qt), exercising the LOCAL-only predicate + the `credits_lines`/partition/header builders.

### Correction to original "Verification Results" note

The earlier note (line 99) said the EXP-F3 tests stay as `xfail`/`xpassed` "the intended green state". Per FIX 7 the xfail markers are now REMOVED — `tests/test_comp_export_local.py` reports plain passes, not xpasses.

### Verification (post-corrections)

```
python -m pytest tests/test_comp_export_local.py tests/test_comp_corpus_scope.py -q
20 passed              (no xfail/xpass — real passes, incl. the 4 new tests)

python -m pytest tests/test_export_dossier*.py tests/test_export_xlsx*.py tests/test_local*export*.py -q
148 passed             (Phase 103 export regression unaffected)

python -m pytest tests/test_lab_composition_chunk_hits.py tests/test_corpus_scope_routing.py -q
18 passed              (pre-existing comp suites green)

python -c "import genizah_core, genizah_app, shared.export_dossier"     # exit 0
python -c "import genizah_translations"                                 # exit 0
python -m ruff check genizah_core.py genizah_app.py shared/export_dossier.py genizah_translations.py \
  tests/test_comp_export_local.py tests/test_comp_corpus_scope.py
All checks passed!
```

### Code-review-corrections commits

| Severity | Commit | Message |
|----------|--------|---------|
| BLOCKER (FIX 1) | be8a98a2 | fix(110): stop LOCAL 97-ids leaking to NLI/FJMS in comp display grouping (D-12 BLOCKER) |
| HIGH+MED+LOW (FIX 2-7) | b26c3146 | fix(110): LOCAL-only comp export — credits, terminology, sheet shape, excluded dedup, restore prime (5a/5b/4/5/6/7) |
