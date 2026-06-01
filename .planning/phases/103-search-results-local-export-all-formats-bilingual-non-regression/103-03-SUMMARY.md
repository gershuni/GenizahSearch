---
phase: 103-search-results-local-export-all-formats-bilingual-non-regression
plan: "03"
subsystem: export
tags: [export, local, csv, txt, docx, bilingual, non-regression, desktop, formula-injection]
dependency_graph:
  requires:
    - shared/docx_export.py::write_docx_result_block (Plan 01)
    - genizah_app.py::_prime_local_filepath_cache (Plan 02 — already added to export_results)
  provides:
    - genizah_app.py::export_results (CSV LOCAL remap + conditional Filepath/Page)
    - genizah_app.py::export_results (TXT LOCAL labeled blocks, Genizah byte-identical)
    - genizah_app.py::export_results (DOCX per-result block layout via shared writer)
    - genizah_app.py::_build_export_data_row (module-level helper)
    - genizah_app.py::_csv_extra_cols (module-level helper)
    - genizah_app.py::_format_txt_local_block (module-level helper)
    - genizah_app.py::_format_txt_genizah_block (module-level helper)
  affects:
    - genizah_app.py
    - tests/test_local_export_csv_txt_docx.py
    - tests/test_export_dossier.py (pre-existing test gap fixed)
tech_stack:
  added: []
  patterns:
    - _local_fp_export_map snapshot (no per-row SQLite during export — BUG-6 pattern)
    - _export_filepath(sid) helper (authoritative batch-primed lookup for CSV/TXT/DOCX)
    - _has_local_in_export pre-check (shared guard reused by all three branches)
    - formula-injection neutralize via sanitize_text_for_excel on ALL CSV cells (T-103-07)
    - LOCAL rows reuse display.source=='LOCAL' primary discriminator (D-14)
    - module-level helpers extracted for offline testing (same pattern as _build_search_results_xlsx_bytes)
key_files:
  created:
    - tests/test_local_export_csv_txt_docx.py
  modified:
    - genizah_app.py
    - tests/test_export_dossier.py
decisions:
  - "_export_filepath snapshot helper reads _local_filepath_cache dict once — prevents per-row SQLite fallback in _lookup_local_filepath from firing during export"
  - "Module-level helpers _build_export_data_row / _csv_extra_cols / _format_txt_local_block / _format_txt_genizah_block extracted so CSV/TXT logic is offline-testable without Qt"
  - "Genizah TXT branch is a TRUE no-op vs pre-v7.17: same snippet variable, same f-string write statement, markers preserved exactly (LEXP-08 strict content non-regression)"
  - "DOCX branch replaces the 7-col table with per-result block layout via write_docx_result_block; _set_table_rtl/_set_table_width_pct/_add_docx_highlighted_runs methods kept (used by export_comp_report)"
  - "test_sheet_titles_en/_he in test_export_dossier.py updated to include 'local_documents' key (Plan 01 added the key but did not update the test — pre-existing gap fixed)"
metrics:
  duration: "14 minutes"
  completed_date: "2026-06-01"
  tasks_completed: 4
  files_changed: 3
---

# Phase 103 Plan 03: CSV / TXT / DOCX LOCAL-Aware Export Summary

**One-liner:** CSV export gains LOCAL column remap (filename/folder/LOCAL/matched-text) + conditional Filepath/Page columns with formula-injection escaping; TXT gains LOCAL labeled blocks (`=== filename | parent ===`) while Genizah output is byte-identical; DOCX replaces the 7-column table with per-result rich-document blocks via the shared Wave-1 writer.

## What Was Built

### Task 1 — CSV branch: LOCAL column remap + conditional Filepath/Page + formula-escape (D-08, T-103-07)

Added to `genizah_app.py::export_results` after the existing `_prime_local_filepath_cache` call (Plan 02 already added that; NOT duplicated):

- **`_local_fp_export_map` snapshot** — `dict(getattr(self, '_local_filepath_cache', {}) or {})` captures the primed cache; no per-row SQLite can fire during export
- **`_export_filepath(sid)` helper** — reads from snapshot (authoritative for CSV/TXT/DOCX; NOT `self._lookup_local_filepath`)
- **`_has_local_in_export` pre-check** — `any(...source == 'LOCAL'...)` shared guard for all three branches
- **`data_rows` builder if/else** — LOCAL rows: Shelfmark=filename, Library=parent folder, Source='LOCAL', Snippet=matched text; Title blank; Image/Page=`chunk_locator` VERBATIM (D-02)
- **CSV `csv_headers`** — `headers + [_fp_label, _pg_label]` when `_has_local_in_export`; `headers` only otherwise (LEXP-08 Genizah-only non-regression)
- **Formula-injection sanitize** — `sanitize_text_for_excel(str(val).replace('*', ''))` applied to ALL row cells; appended LOCAL `_fp`/`_pg` each also wrapped in `sanitize_text_for_excel` (T-103-07)

Module-level helpers extracted for offline testing:
- `_build_export_data_row(result_dict, filepath_fn=None)` → `(row, is_local)`
- `_csv_extra_cols(result_dict, filepath_fn=None, lang='en')` → `[filepath_sanitized, page_sanitized]`

### Task 2 — TXT branch: LOCAL labeled blocks, Genizah byte-identical (D-09, LEXP-08)

Replaced the TXT `else:` branch's single-loop write with a per-result if/else:

- **LOCAL block (D-09):** `=== {filename} | {parent} ===\n` + `Path: {fp}  (page N)\n` + `{snippet_clean}\n\n` — marker-stripped snippet for the new LOCAL surface
- **Genizah `else` branch (LEXP-08):** EXACT copy of the pre-v7.17 write statement: `snippet = r.get('raw_file_hl', '').strip().replace('\n', ' ').replace('\r', '')` then `f.write(f"=== {r['display']['shelfmark']} | {r['display']['title']} ===\n{snippet}\n\n")` — `*` markers are NOT stripped (byte-identical behavior)
- **Before/after marker treatment (documented):** Pre-v7.17 Genizah TXT preserved `*` markers; this plan keeps that behavior. LOCAL blocks strip `*` markers (new surface, new convention).

Module-level helpers:
- `_format_txt_local_block(result_dict, filepath_fn=None)` → LOCAL block string (no trailing `\n\n`)
- `_format_txt_genizah_block(result_dict)` → Genizah block string (byte-identical to pre-v7.17)

### Task 3 — DOCX branch: per-result block redesign via shared writer (D-10/D-11/D-12)

Replaced the entire DOCX `elif fmt == 'docx':` body (which called `doc.add_table(...)`) with the block-layout version:

- **Imports:** `from shared.docx_export import write_docx_result_block`
- **Loop:** `for r in results_to_export:` → resolves `fp = _export_filepath(sid) if LOCAL else ''` → `write_docx_result_block(doc, r, filepath=fp, lang=CURRENT_LANG)`
- **RTL pass:** `CURRENT_LANG == 'he'` → sets Normal style + per-paragraph RTL (same as before, no table RTL needed)
- **No `doc.add_table(` in this branch** — `_set_table_rtl`/`_set_table_width_pct` methods retained for `export_comp_report`

### Task 4 — CSV/TXT/DOCX tests (11 tests)

New file `tests/test_local_export_csv_txt_docx.py`:

| Test | What it pins |
|------|-------------|
| `test_csv_mixed_appends_filepath_page_columns` | LOCAL extra cols = [filepath, page]; Genizah extra cols = ['', ''] |
| `test_csv_genizah_only_no_extra_columns` | Genizah-only header = exactly 7 columns (LEXP-08) |
| `test_csv_local_row_column_remap` | Shelfmark=filename, Library=parent folder, Source='LOCAL' (D-08) |
| `test_csv_local_filepath_formula_escaped` | =+-@ values → leading `'` via sanitize_text_for_excel (T-103-07) |
| `test_txt_local_block_format` | `=== fn \| parent ===`, `Path: fp`, `(page N)` present (D-09) |
| `test_txt_local_block_strips_markers` | LOCAL snippet has `*` stripped |
| `test_txt_genizah_block_byte_identical` | Genizah `*` markers preserved; exact header format (LEXP-08) |
| `test_txt_genizah_block_no_path_line` | Genizah TXT never contains `Path:` line |
| `test_docx_two_results_blocks` | 2 blocks, 0 tables, LOCAL filepath + Genizah URL (D-10/D-11) |
| `test_docx_local_block_parent_folder` | filename + parent in LOCAL heading |
| `test_docx_genizah_block_no_filepath` | Genizah has genizahsearch.com URL, not LOCAL filepath |

**Test strategy:** module-level helpers pattern (same as `_build_search_results_xlsx_bytes` offline tests in Plan 02) — extracted `_build_export_data_row` / `_csv_extra_cols` / `_format_txt_*` allow offline testing without Qt.

## Verification Results

```
python -m pytest tests/test_local_export_csv_txt_docx.py -x -q
11 passed in 1.46s

python -m pytest tests/test_docx_export_block.py tests/test_local_export_xlsx.py tests/test_export_xlsx_cross_parity.py tests/test_export_dossier.py tests/test_export_dossier_local.py tests/test_export_dossier_local_handling.py tests/test_desktop_xlsx_multi_sheet.py -q
165 passed in 3.62s

python -c "import ast; ast.parse(open('genizah_app.py',encoding='utf-8').read()); print('parse OK')"
parse OK

python -m ruff check genizah_app.py tests/test_local_export_csv_txt_docx.py
All checks passed!
```

Manual structural confirmation:
- EXACTLY ONE `self._prime_local_filepath_cache(results_to_export)` in `export_results` (Plan 02 added; Plan 03 did NOT duplicate)
- Genizah-only CSV header = 7 columns (no Filepath/Page appended)
- Genizah-only TXT block format unchanged vs pre-v7.17 (markers preserved)

## Commits

| Task | Commit | Message |
|------|--------|---------|
| 1+2+3 | c8ca1fb1 | feat(103-03): CSV LOCAL column remap + conditional Filepath/Page + formula-escape |
| 4 | a1e2acbb | feat(103-03): add CSV/TXT/DOCX LOCAL-aware + formula-injection + non-regression tests |
| Rule 1 fix | d4484843 | fix(103-03): update test_sheet_titles to include local_documents key (Plan 01 gap) |

## Deviations from Plan

**1. [Rule 1 - Bug] Pre-existing test gap: test_sheet_titles_en/_he in test_export_dossier.py**

- **Found during:** Task 4 (broader regression run)
- **Issue:** Plan 01 added `'local_documents'` to `sheet_titles()` dict but `test_sheet_titles_en` / `test_sheet_titles_he` checked for an exact 4-key dict — test had been failing since Plan 01 landed
- **Fix:** Updated both assertions in `tests/test_export_dossier.py::TestBilingualHeaderRows` to include the `'local_documents'` key
- **Files modified:** `tests/test_export_dossier.py`
- **Commit:** `d4484843`

**2. [Rule 2 - Missing critical functionality] Module-level helpers extracted**

The plan offered two options: (a) use a Qt/GenizahGUI harness, or (b) extract module-level helpers. Option (b) was chosen because no `qtbot` fixture exists — tests drove `QApplication` directly, not `GenizahGUI`, so the export branches were inaccessible without a full GUI instance. Four helpers extracted: `_build_export_data_row`, `_csv_extra_cols`, `_format_txt_local_block`, `_format_txt_genizah_block`. Plan explicitly permitted this path.

**3. Tasks 1, 2, 3 committed together (single genizah_app.py)**

Tasks 1 (CSV), 2 (TXT), and 3 (DOCX) all modify the same file (`genizah_app.py`). They were committed as one atomic commit `c8ca1fb1` to avoid partial-state commits. Task 4 (tests) was committed separately as `a1e2acbb`.

## Known Stubs

None. All three branches are fully implemented — no hardcoded empty values or placeholder data.

## Threat Flags

None new. T-103-07 (CSV formula injection) is mitigated as planned: `sanitize_text_for_excel` applied to all row cells and appended Filepath/Page cells, verified by `test_csv_local_filepath_formula_escaped`.

## Self-Check: PASSED

Files exist:
- genizah_app.py — FOUND (modified)
- tests/test_local_export_csv_txt_docx.py — FOUND (created)
- tests/test_export_dossier.py — FOUND (modified)

Commits exist:
- c8ca1fb1 — FOUND
- a1e2acbb — FOUND
- d4484843 — FOUND
