---
phase: 103-search-results-local-export-all-formats-bilingual-non-regression
plan: "04"
subsystem: export
tags: [export, non-regression, xlsx, csv, txt, docx, lexp-08, d-12, test-only]
dependency_graph:
  requires:
    - genizah_app._build_search_results_xlsx_bytes (Plan 02)
    - genizah_app._csv_extra_cols (Plan 03)
    - genizah_app._format_txt_genizah_block (Plan 03)
    - shared.docx_export.write_docx_result_block (Plan 01)
    - tests/test_export_xlsx_cross_parity.py (pre-existing — must stay unmodified)
  provides:
    - tests/test_local_export_non_regression.py (consolidated LEXP-08 + D-12 gate)
  affects:
    - tests/test_local_export_non_regression.py
tech_stack:
  added: []
  patterns:
    - subprocess pytest gate (cross-parity invariant tested in-process without modifying the invariant file)
    - module-level helper offline test (reuse _csv_extra_cols / _format_txt_genizah_block without Qt)
    - pytest.importorskip for python-docx skip guard
    - D-12 recorded as asserted contract (not a regression)
key_files:
  created:
    - tests/test_local_export_non_regression.py
  modified: []
decisions:
  - "Cross-parity invariant tested via subprocess (not import) so the invariant file remains 100% unmodified (LEXP-08/SC#5)"
  - "Guard test reads test_export_xlsx_cross_parity.py source to confirm the core assertion is intact — catches accidental weakening"
  - "D-12 carve-out recorded as test_genizah_docx_is_block_layout_not_table — asserts block layout as EXPECTED, not a regression; no byte-identity assertion contradicting the approved change"
metrics:
  duration: "3 minutes"
  completed_date: "2026-06-01"
  tasks_completed: 2
  files_changed: 1
---

# Phase 103 Plan 04: Non-Regression Gate + Full Export Suite Summary

**One-liner:** Consolidated LEXP-08 non-regression test module locks the xlsx cross-parity invariant (passes unmodified), Genizah-only XLSX/CSV/TXT structural guarantees, and records the intentional D-12 DOCX block-layout carve-out as an asserted contract; all 82 export tests across 8 modules pass green.

## What Was Built

### Task 1 — Consolidated Genizah-only non-regression + D-12 DOCX carve-out test module

New file `tests/test_local_export_non_regression.py` (7 tests, 1 commit):

| Test | What it pins |
|------|-------------|
| `test_cross_parity_invariant_still_passes` | Subprocess gate: `tests/test_export_xlsx_cross_parity.py` exits 0 unmodified (LEXP-08 / ROADMAP SC#5) |
| `test_cross_parity_file_assertion_intact` | Source-read guard: invariant file still contains `wb_web.sheetnames == wb_desktop.sheetnames` — catches accidental weakening |
| `test_genizah_only_xlsx_four_sheets` | Genizah-only xlsx → 4-sheet workbook, no Local Documents, Search Results active (LEXP-08) |
| `test_genizah_only_xlsx_four_sheets_he` | Same with `lang='he'` — no מסמכים מקומיים sheet (LEXP-08 bilingual) |
| `test_genizah_only_csv_seven_columns` | `_csv_extra_cols(gen_r)` yields `['', '']` → Filepath/Page NOT appended (LEXP-08) |
| `test_genizah_only_txt_block_byte_identical` | `_format_txt_genizah_block` starts with `=== T-S 12.1 | Letter ===`, preserves `*` markers, no `Path:` line (LEXP-08) |
| `test_genizah_docx_is_block_layout_not_table` | D-12 CARVE-OUT recorded: `len(doc.tables) == 0`, shelfmark + URL present; NO byte-identity-vs-pre-v7.17 assertion |

**Test design:**
- A. `test_cross_parity_invariant_still_passes` runs the existing invariant module in a subprocess so `test_export_xlsx_cross_parity.py` stays byte-for-byte unmodified.
- B. xlsx tests reuse the Plan 02 offline builder pattern (`_build_search_results_xlsx_bytes` + `_meta_resolver_fake` + `_identity_sanitize`), with a `stub_dossier` fixture.
- C. CSV/TXT tests reuse Plan 03's module-level helpers (`_csv_extra_cols`, `_format_txt_genizah_block`) — no Qt dependency.
- D. DOCX test calls `write_docx_result_block` directly from `shared.docx_export`, protected by `pytest.importorskip('docx')`.

### Task 2 — Full export-suite green run + phase verification gate

No new file content was required. All 8 export test modules passed together:

```
python -m pytest tests/test_export_dossier_local.py tests/test_docx_export_block.py \
  tests/test_local_export_xlsx.py tests/test_local_export_csv_txt_docx.py \
  tests/test_local_export_non_regression.py \
  tests/test_export_xlsx_cross_parity.py tests/test_desktop_xlsx_multi_sheet.py \
  tests/test_export_dossier_local_handling.py -q
82 passed, 1 warning in 5.88s

python -m ruff check genizah_app.py shared/export_dossier.py shared/docx_export.py \
  tests/test_export_dossier_local.py tests/test_docx_export_block.py \
  tests/test_local_export_xlsx.py tests/test_local_export_csv_txt_docx.py \
  tests/test_local_export_non_regression.py
All checks passed!
```

No pre-existing tests were modified. `tests/test_export_xlsx_cross_parity.py` is CLEAN across the entire phase (git diff confirms zero changes).

## Verification Results

```
python -m pytest tests/test_local_export_non_regression.py -x -q
7 passed, 1 warning in 3.01s

python -m pytest tests/test_export_dossier_local.py tests/test_docx_export_block.py \
  tests/test_local_export_xlsx.py tests/test_local_export_csv_txt_docx.py \
  tests/test_local_export_non_regression.py tests/test_export_xlsx_cross_parity.py \
  tests/test_desktop_xlsx_multi_sheet.py tests/test_export_dossier_local_handling.py -q
82 passed, 1 warning in 5.88s

python -m ruff check [all 9 touched files]
All checks passed!

git diff HEAD tests/test_export_xlsx_cross_parity.py
CLEAN - no changes to cross-parity file
```

## Commits

| Task | Commit | Message |
|------|--------|---------|
| 1 | b09ae3ed | test(103-04): consolidated Genizah-only non-regression + D-12 DOCX carve-out module |
| 2 | (no new content — Task 1 commit covers; verification run only) | — |

## Deviations from Plan

None — plan executed exactly as written.

- Task 1: created exactly as specified (7 tests, all required test names present)
- Task 2: all 8 export test modules passed green on first run; no production code fixes needed; ruff clean across all 9 files

## Known Stubs

None. This is a test-only plan; no production code stubs introduced.

## Threat Flags

None. This plan adds only test code. T-103-10 (the verification of the formula-injection mitigations from Plans 02 and 03) is confirmed: `test_formula_injection_filepath_escaped` (Plan 02) and `test_csv_local_filepath_formula_escaped` (Plan 03) both pass as part of the combined 82-test suite.

## Self-Check: PASSED

Files exist:
- tests/test_local_export_non_regression.py — FOUND (created)

Commits exist:
- b09ae3ed — FOUND
