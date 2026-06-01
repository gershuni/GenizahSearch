---
phase: 103-search-results-local-export-all-formats-bilingual-non-regression
plan: "02"
subsystem: export
tags: [export, local, xlsx, bilingual, non-regression, desktop]
dependency_graph:
  requires:
    - shared/export_dossier.py::local_documents_header_row (Plan 01)
    - shared/export_dossier.py::build_local_document_row (Plan 01)
    - shared/export_dossier.py::sheet_titles (local_documents key, Plan 01)
  provides:
    - genizah_app.py::_build_search_results_xlsx_bytes (local_filepath_map kwarg + Local Documents sheet)
    - genizah_app.py::export_results (prime + pass local_filepath_map)
  affects:
    - genizah_app.py
    - tests/test_local_export_xlsx.py
tech_stack:
  added: []
  patterns:
    - display.source=='LOCAL' PRIMARY / 97-prefix SECONDARY row discrimination (D-14)
    - LOCAL-only sheet removal pattern (D-05): create sheets unconditionally, remove empty Genizah sheets post-loop
    - batch-primed filepath cache passed as pre-built dict (BUG-6 pattern — no per-row SQLite)
key_files:
  created:
    - tests/test_local_export_xlsx.py
  modified:
    - genizah_app.py
decisions:
  - "_is_local_row() helper uses display.source=='LOCAL' as PRIMARY (D-14), 97-prefix as SECONDARY — single source of truth used by both main-sheet skip and Local Documents writer"
  - "ws_local created BEFORE ws_credits so sheet order is [Search Results, Manuscripts, Bibliography, Local Documents, Credits and Info] (D-04)"
  - "LOCAL-only branch removes ws_main/ws_manu/ws_bib AFTER the bibliography loop (last structural step before save) to guarantee D-05 shape without restructuring sheet creation order"
  - "_prime_local_filepath_cache called once after results_to_export finalized; xlsx builder receives pre-built dict — no per-row SQLite during export"
metrics:
  duration: "6 minutes"
  completed_date: "2026-06-01"
  tasks_completed: 3
  files_changed: 2
---

# Phase 103 Plan 02: Desktop xlsx Local Documents Sheet Summary

**One-liner:** Desktop xlsx builder gains a "Local Documents" sheet (position 4 for mixed, only sheet for LOCAL-only) with bilingual headers and rich matched-text cells; LOCAL rows excluded from Search Results / Manuscripts / Bibliography; Genizah-only export unchanged (cross-parity invariant green).

## What Was Built

### Task 1 — Local Documents sheet + skip_local flip + LOCAL-only shape in `_build_search_results_xlsx_bytes`

Added to `genizah_app.py::_build_search_results_xlsx_bytes`:

- **`local_filepath_map=None` kwarg** — pre-built sys_id → filepath dict; defaults to `None` (`{}`) so all existing test call sites (including cross-parity) work unchanged
- **Extended lazy import block** — imports `local_documents_header_row` + `build_local_document_row` from `shared.export_dossier` (Plan 01 primitives)
- **`_row_sys_id()` + `_is_local_row()` helpers** — single source of truth for LOCAL discrimination: `display.source == 'LOCAL'` PRIMARY, `is_local_sys_id()` SECONDARY (D-14)
- **`_has_local` + `_local_only` flags** — computed once from `results`
- **`ws_local` conditional sheet creation** — inserted between `ws_bib` and `ws_credits` for correct D-04 sheet order; only created when `_has_local` (D-06)
- **Main-sheet loop skip** — `if _has_local and _is_local_row(res): continue` keeps Search Results Genizah-only (D-04)
- **`skip_local` flip** — `build_manuscript_row(skip_local=True)` + `build_bibliography_rows(skip_local=True)` (D-07)
- **Local Documents sheet writer** — header row (blue bold, bilingual), per-row loop using `build_local_document_row` + `build_rich_snippet_cell`, column widths A-E [45, 25, 80, 10, 70]
- **LOCAL-only branch** — removes `ws_main`, `ws_manu`, `ws_bib` AFTER the bibliography loop; sets `wb.active = wb.index(ws_local)` → workbook is EXACTLY `[Local Documents, Credits and Info]` (D-05)

### Task 2 — Wire `export_results` xlsx branch

- **`self._prime_local_filepath_cache(results_to_export)`** added once after `results_to_export` is finalized (after the `if has_selection` / `else` block)
- **`local_filepath_map=dict(getattr(self, '_local_filepath_cache', {}) or {})`** passed to `_build_search_results_xlsx_bytes` alongside `domain_name_map=_domain_name_map_for_xlsx`

### Task 3 — xlsx export tests (`tests/test_local_export_xlsx.py`)

14 offline tests (no Qt, calls `_build_search_results_xlsx_bytes` directly):

| Test | What it pins |
|------|-------------|
| `test_local_sys_id_guard` | LOCAL_ID / GEN_ID classification guard |
| `test_mixed_has_local_documents_sheet` | Mixed → 5-sheet workbook with Local Documents at position 4 |
| `test_mixed_local_row_fields` | Local Documents row 2: filename/parent/filepath/page/matched-text cells |
| `test_mixed_search_results_excludes_local` | Search Results has exactly 1 data row (GEN_ID); LOCAL_ID absent |
| `test_mixed_active_is_search_results` | Mixed active sheet = 'Search Results' |
| `test_manuscripts_bibliography_exclude_local` | LOCAL_ID absent from col A of Manuscripts + Bibliography |
| `test_local_only_workbook_shape_exact` | LOCAL-only → EXACTLY ['Local Documents', 'Credits and Info']; Genizah sheets absent; active = 'Local Documents' |
| `test_local_only_workbook_shape_exact_he` | Same in `lang='he'` → `['מסמכים מקומיים', 'קרדיט ומידע']` |
| `test_local_only_partition_by_display_source` | GEN_ID with `display.source=='LOCAL'` treated as LOCAL (D-14 primary) |
| `test_genizah_only_unchanged` | Genizah-only → unchanged 4-sheet workbook, no Local Documents sheet |
| `test_local_he_headers` | Mixed HE: sheet name 'מסמכים מקומיים', headers `['שם קובץ', 'תיקייה', 'נתיב מלא', 'עמוד', 'טקסט תואם']` |
| `test_formula_injection_filepath_escaped` | `=cmd|calc` filepath → cell starts with `'` (sanitize_text_for_excel) |
| `test_missing_filepath_blank_no_error` | Missing filepath → blank Filepath + Parent cells, no exception |
| `test_no_local_filepath_map_kwarg_unchanged` | 4-arg call (no local_filepath_map) → unchanged 4-sheet workbook |

## Verification Results

```
python -m pytest tests/test_local_export_xlsx.py tests/test_export_xlsx_cross_parity.py tests/test_desktop_xlsx_multi_sheet.py -x -q
38 passed, 1 warning in 4.71s

python -m ruff check genizah_app.py tests/test_local_export_xlsx.py
All checks passed!

python -c "import ast; ast.parse(open('genizah_app.py',encoding='utf-8').read()); print('parse OK')"
parse OK
```

## Commits

| Task | Commit | Message |
|------|--------|---------|
| 1 + 3 (TDD) | 4bb56027 | feat(103-02): add Local Documents sheet + skip_local flip + LOCAL-only shape to xlsx builder |
| 2 | 8f3e8dd4 | feat(103-02): wire export_results xlsx branch to prime + pass local_filepath_map |

## Deviations from Plan

**1. [Rule 2 - test structure] Formula injection test calls builder directly instead of via `_build()` helper**

The plan's `test_formula_injection_filepath_escaped` test was specified to use `sanitize_fn=sanitize_text_for_excel` via `_build()`, but `_build()` already hard-codes `sanitize_fn=_identity_sanitize`. Passing a second `sanitize_fn` caused `TypeError: got multiple values for keyword argument`. Fixed by calling `_build_search_results_xlsx_bytes` directly (without the `_build()` wrapper) in that single test — all other tests use `_build()` unchanged.

## Known Stubs

None. The Local Documents sheet, row builder, and filepath map wiring are fully implemented.

## Threat Flags

None. T-103-04 (formula injection) is mitigated by `sanitize_text_for_excel` wired through `sanitize_fn` (verified by `test_formula_injection_filepath_escaped`). T-103-05 (hyperlink injection) and T-103-06 (oversized matched text) are accepted per the plan's threat register.

## Self-Check: PASSED

Files exist:
- genizah_app.py — FOUND (modified)
- tests/test_local_export_xlsx.py — FOUND (created)

Commits exist:
- 4bb56027 — FOUND
- 8f3e8dd4 — FOUND
