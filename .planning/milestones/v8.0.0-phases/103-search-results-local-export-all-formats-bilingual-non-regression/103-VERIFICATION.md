---
phase: 103-search-results-local-export-all-formats-bilingual-non-regression
verified: 2026-06-01T00:00:00Z
status: passed
score: 6/6 must-haves verified
overrides_applied: 0
---

# Phase 103: Search-Results LOCAL Export Verification Report

**Phase Goal:** Users can export a Search-results set containing LOCAL hits — in any of the four desktop formats (XLSX/CSV/TXT/DOCX) — and receive useful, locally-meaningful columns for each LOCAL row, with a dedicated "Local Documents" sheet in xlsx and full non-regression on Genizah-only exports.

**Verified:** 2026-06-01
**Status:** passed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Mixed Genizah+LOCAL xlsx has "Local Documents" sheet at position 4 with LOCAL hit filename/parent/filepath/page/matched-text; "Search Results" has only Genizah rows | ✓ VERIFIED | `_build_search_results_xlsx_bytes` adds `ws_local` between `ws_bib` and `ws_credits`; main-sheet loop skips `_is_local_row(res)`; 8 tests in `test_local_export_xlsx.py` including `test_mixed_has_local_documents_sheet`, `test_mixed_local_row_fields`, `test_mixed_search_results_excludes_local` all pass |
| 2 | "Manuscripts" / "Bibliography" sub-sheets contain no LOCAL synthetic sys_id rows | ✓ VERIFIED | `build_manuscript_row(skip_local=True)` and `build_bibliography_rows(skip_local=True)` flipped from `False`; `test_manuscripts_bibliography_exclude_local` passes; `test_export_dossier_local_handling.py` skip_local=True tests pass |
| 3 | LOCAL-only xlsx is usable workbook EXACTLY `[Local Documents, Credits and Info]` with Local Documents active; no Python error | ✓ VERIFIED | `_local_only` branch calls `wb.remove()` on the three empty Genizah sheets and sets `wb.active = wb.index(ws_local)`; `test_local_only_workbook_shape_exact` (en) and `test_local_only_workbook_shape_exact_he` (he) both assert exact sheetnames and active sheet |
| 4 | Mixed CSV/TXT/DOCX unified table: LOCAL rows carry local columns, Genizah rows carry Genizah columns; no LOCAL row of empty Genizah cells | ✓ VERIFIED | Module-level helpers `_build_export_data_row`, `_csv_extra_cols`, `_format_txt_local_block`, `write_docx_result_block` all wired; CSV LOCAL rows remap Shelfmark=filename/Library=parent/Source='LOCAL'/Snippet=matched-text + append Filepath+Page; TXT emits `=== filename | parent ===` + `Path:` line for LOCAL; DOCX uses per-result block; 11 tests in `test_local_export_csv_txt_docx.py` pass |
| 5 | Genizah-only XLSX/CSV/TXT structurally identical to pre-v7.17; `tests/test_export_xlsx_cross_parity.py` passes WITHOUT modification. (DOCX block redesign is intentional, not a regression per D-12.) | ✓ VERIFIED | `test_export_xlsx_cross_parity.py` unmodified (last commit `e01bfd14`, predates Phase 103); 4-sheet Genizah-only xlsx confirmed; `_csv_extra_cols(genizah_row) == ['','']` confirmed; Genizah TXT block byte-identical confirmed; D-12 carve-out asserted in `test_genizah_docx_is_block_layout_not_table` |
| 6 | "Local Documents" sheet title + column headers appear in Hebrew when lang='he', English when lang='en' | ✓ VERIFIED | `local_documents_header_row('he') == ['שם קובץ','תיקייה','נתיב מלא','עמוד','טקסט תואם']`; `sheet_titles('he')['local_documents'] == 'מסמכים מקומיים'`; `test_local_he_headers` and `test_local_only_workbook_shape_exact_he` both pass; inline verification one-liner passes |

**Score:** 6/6 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `shared/export_dossier.py` | Local Documents bilingual header/title + LOCAL-row builder | ✓ VERIFIED | Contains `def local_documents_header_row`, `def build_local_document_row`, `_LOCAL_HEADERS_EN`, `_LOCAL_HEADERS_HE`, `'local_documents': "Local Documents"`, `'local_documents': "מסמכים מקומיים"` |
| `shared/docx_export.py` | Module-level DOCX per-result block writer (no Qt/genizah_app dep) | ✓ VERIFIED | New file; contains `def write_docx_result_block(doc, result_dict, filepath='', lang='en')` at module level; no Qt imports; no `import genizah_app` |
| `genizah_app.py` | `_build_search_results_xlsx_bytes` with `local_filepath_map` kwarg + Local Documents sheet; CSV/TXT/DOCX LOCAL-aware branches; module-level helpers | ✓ VERIFIED | Contains `local_filepath_map=None` kwarg; `_is_local_row`, `_has_local`, `_local_only`; `ws_local`; `skip_local=True` on manuscript/bib; `wb.remove()` LOCAL-only branch; `write_docx_result_block` in DOCX branch; `_build_export_data_row`, `_csv_extra_cols`, `_format_txt_local_block`, `_format_txt_genizah_block` helpers |
| `tests/test_export_dossier_local.py` | Unit tests for bilingual header/title + row builder | ✓ VERIFIED | Exists; 9 tests; all pass |
| `tests/test_docx_export_block.py` | Unit tests for DOCX block writer | ✓ VERIFIED | Exists; 8 tests; all pass |
| `tests/test_local_export_xlsx.py` | Mixed / LOCAL-only / Genizah-only xlsx export tests | ✓ VERIFIED | Exists; 14 tests; all pass |
| `tests/test_local_export_csv_txt_docx.py` | CSV/TXT/DOCX LOCAL-aware + formula-injection + non-regression tests | ✓ VERIFIED | Exists; 11 tests; all pass |
| `tests/test_local_export_non_regression.py` | Consolidated LEXP-08 non-regression + D-12 DOCX carve-out | ✓ VERIFIED | Exists; 7 tests; all pass |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|----|--------|---------|
| `genizah_app.py::_build_search_results_xlsx_bytes` | `shared.export_dossier.build_local_document_row` + `local_documents_header_row` | lazy import + per-LOCAL-row write gated on `_has_local` | ✓ WIRED | Import confirmed at line ~2631; `ws_local` write loop uses both functions |
| `genizah_app.py::export_results` (xlsx) | `_build_search_results_xlsx_bytes(local_filepath_map=...)` | pre-primed `_local_filepath_cache` passed as kwarg | ✓ WIRED | `self._prime_local_filepath_cache(results_to_export)` called once; `local_filepath_map=dict(getattr(self, '_local_filepath_cache', {}) or {})` passed |
| `genizah_app.py::export_results` (docx) | `shared.docx_export.write_docx_result_block` | per-result call with pre-resolved LOCAL filepath | ✓ WIRED | `from shared.docx_export import write_docx_result_block`; `write_docx_result_block(doc, r, filepath=fp, lang=CURRENT_LANG)` in DOCX branch |
| `genizah_app.py::export_results` (csv) | `_csv_extra_cols` + `sanitize_text_for_excel` | `_has_local_in_export` conditional widening + per-cell formula-escape | ✓ WIRED | `_has_local_in_export` pre-check; `csv_headers = headers + [_fp_label, _pg_label]` when LOCAL present; `sanitize_text_for_excel` applied to all cells + appended `_fp`/`_pg` |
| `tests/test_local_export_non_regression.py` | `tests/test_export_xlsx_cross_parity.py` | subprocess/pytest invocation | ✓ WIRED | `test_cross_parity_invariant_still_passes` runs invariant file in subprocess; `test_cross_parity_file_assertion_intact` reads source to confirm core assertion still present |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|--------------|--------|--------------------|--------|
| `ws_local` (xlsx Local Documents sheet) | `fp` from `_local_filepath_map` | `self._local_filepath_cache` primed by `_prime_local_filepath_cache(results_to_export)` batch SQLite query | Yes — primed from `local_indexer.get_filepaths()` batch query (BUG-6 pattern) | ✓ FLOWING |
| CSV LOCAL rows | `_fp` from `_export_filepath(sid)` | `_local_fp_export_map` snapshot of `_local_filepath_cache` | Yes — same primed cache, no per-row SQLite | ✓ FLOWING |
| DOCX LOCAL blocks | `fp = _export_filepath(sid)` | Same primed cache snapshot | Yes — consistent with BUG-6 fix | ✓ FLOWING |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| All dossier primitives importable + correct values | `python -c "from shared.export_dossier import local_documents_header_row,build_local_document_row,sheet_titles; from shared.docx_export import write_docx_result_block; assert sheet_titles('he')['local_documents']=='מסמכים מקומיים'; print('OK')"` | OK | ✓ PASS |
| Full 82-test export suite | `python -m pytest [8 test files] -q` | 82 passed, 1 warning | ✓ PASS |
| Cross-parity invariant unmodified | `git log --oneline tests/test_export_xlsx_cross_parity.py` | Last commit `e01bfd14` (predates Phase 103) | ✓ PASS |
| SC4+5+6 smoke check | `python -c "..."` (row remap + Genizah non-regression + bilingual) | All spot-checks passed | ✓ PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|---------|
| LEXP-01 | 03 | LOCAL rows carry local-meaningful values in Search-results export | ✓ SATISFIED | `_build_export_data_row` remaps LOCAL rows; Shelfmark=filename, Library=parent, Source='LOCAL', Snippet=matched text; Filepath+Page appended on CSV |
| LEXP-02 | — (Phase 104) | Composition-Search report LOCAL parity | DEFERRED | Correctly assigned to Phase 104 in REQUIREMENTS.md (`| LEXP-02 | 104 | Pending |`); not orphaned |
| LEXP-03 | 02 | Mixed xlsx has dedicated "Local Documents" sheet | ✓ SATISFIED | Sheet created at position 4; LOCAL rows written to it; verified by test |
| LEXP-04 | 02 | Manuscripts/Bibliography sub-sheets have no LOCAL rows | ✓ SATISFIED | `skip_local=True` flip on both `build_manuscript_row` and `build_bibliography_rows`; test confirms absence |
| LEXP-05 | 02 | LOCAL-only xlsx usable; Genizah sub-sheets omitted/empty; no error | ✓ SATISFIED | `_local_only` branch removes `ws_main`/`ws_manu`/`ws_bib`; workbook is exactly `[Local Documents, Credits and Info]`; active = Local Documents |
| LEXP-06 | 03 | CSV/TXT/DOCX: LOCAL-aware single table (no misleading empty cells) | ✓ SATISFIED | CSV remaps LOCAL columns; TXT emits LOCAL blocks with Path line; DOCX uses per-result block writer for both Genizah and LOCAL |
| LEXP-07 | 01, 02, 03 | New LOCAL columns and sheet title bilingual (he/en) | ✓ SATISFIED | `local_documents_header_row(lang)` returns correct EN/HE lists; `sheet_titles(lang)['local_documents']` returns EN/HE; CSV appended headers use `CURRENT_LANG` |
| LEXP-08 | 04 | Genizah-only exports structurally unchanged; cross-parity test green and unmodified (DOCX block layout is intentional exception per D-12) | ✓ SATISFIED | Cross-parity file unmodified (last commit `e01bfd14`); Genizah-only xlsx 4-sheet workbook confirmed; Genizah-only CSV 7-column table confirmed; Genizah TXT byte-identical; D-12 DOCX carve-out asserted as expected contract |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| (none) | — | No stubs, placeholders, or hardcoded empty data detected | — | — |

**Stub scan notes:**

- `shared/docx_export.py`: No placeholder returns; `write_docx_result_block` fully implemented with heading/metadata/matched-text/URL/separator blocks.
- `genizah_app.py` `_build_search_results_xlsx_bytes`: No empty handlers; `ws_local` write loop uses `build_local_document_row` + `build_rich_snippet_cell` to write real data.
- `genizah_app.py` `export_results` CSV/TXT/DOCX branches: No hardcoded empty rows; `_export_filepath` reads from the primed cache.
- Return values of `_csv_extra_cols` returning `['', '']` for Genizah rows is correct non-LOCAL behavior (intentional by design), not a stub.

### Human Verification Required

None. All success criteria are verifiable programmatically. The test suite covers mixed/LOCAL-only/Genizah-only xlsx, CSV, TXT, DOCX, bilingual headers, formula injection safety, and non-regression. No visual layout, real-time behavior, or external service integration requires human testing for this phase.

### Gaps Summary

No gaps. All 6 observable truths are verified, all 8 required artifacts exist and are substantive, all key links are wired, and 82/82 tests pass including the unmodified cross-parity invariant.

**LEXP-02 deferred status confirmed:** LEXP-02 (Composition-report LOCAL export) is correctly mapped to Phase 104 in REQUIREMENTS.md — it was never in Phase 103 scope and is not orphaned.

**D-12 DOCX carve-out confirmed:** The Genizah-only DOCX block redesign is an intentional, user-approved deviation from LEXP-08's non-regression clause. It is recorded as an asserted contract in `test_genizah_docx_is_block_layout_not_table` and explicitly documented in 103-CONTEXT.md. The xlsx cross-parity invariant (`test_export_xlsx_cross_parity.py`) is xlsx-only and is not affected by the DOCX change.

---

_Verified: 2026-06-01_
_Verifier: Claude (gsd-verifier)_
