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
