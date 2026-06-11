---
phase: 103-search-results-local-export-all-formats-bilingual-non-regression
plan: "01"
subsystem: export
tags: [export, local, bilingual, docx, xlsx, primitives]
dependency_graph:
  requires: []
  provides:
    - shared/export_dossier.py::local_documents_header_row
    - shared/export_dossier.py::build_local_document_row
    - shared/export_dossier.py::sheet_titles (local_documents key)
    - shared/docx_export.py::write_docx_result_block
  affects:
    - shared/export_dossier.py
tech_stack:
  added:
    - shared/docx_export.py (new module, python-docx lazy import)
  patterns:
    - bilingual header/title pattern (mirroring main_header_row)
    - module-level Qt-free DOCX block writer (Phase 104 reuse ready)
    - TDD RED/GREEN per task
key_files:
  created:
    - shared/docx_export.py
    - tests/test_export_dossier_local.py
    - tests/test_docx_export_block.py
  modified:
    - shared/export_dossier.py
decisions:
  - "build_local_document_row passes matched_text_raw through unchanged (D-14) — sanitize_fn applies only to items 0-3"
  - "chunk_locator used VERBATIM in DOCX metadata line (D-02) — p_num fallback synthesizes 'p. N' only when locator absent"
  - "write_docx_result_block placed in shared/docx_export.py (new module) for Phase 104 reuse without genizah_app import"
metrics:
  duration: "4 minutes"
  completed_date: "2026-06-01"
  tasks_completed: 3
  files_changed: 4
---

# Phase 103 Plan 01: Export Dossier Primitives + DOCX Block Writer Summary

**One-liner:** Qt-free bilingual Local Documents header/title/row-builder primitives in export_dossier + module-level DOCX per-result block writer (heading/metadata/highlight/URL/separator, Genizah + LOCAL, RTL-capable) in new shared/docx_export.py.

## What Was Built

### Task 1 — Local Documents bilingual helpers in shared/export_dossier.py

Added three new exports to `shared/export_dossier.py`:

- `_LOCAL_HEADERS_EN` / `_LOCAL_HEADERS_HE` module constants (5 columns each, per D-01/D-13)
- `'local_documents'` key in both `_SHEET_TITLES_EN` and `_SHEET_TITLES_HE` (EN: "Local Documents", HE: "מסמכים מקומיים")
- `local_documents_header_row(lang='en') -> List[str]` — bilingual, returns fresh copy
- `build_local_document_row(filename, parent_folder, full_filepath, page, matched_text_raw, sanitize_fn=None) -> List[Any]` — 5 primitives, Qt-free, raw matched_text preserved (D-14)

### Task 2 — New shared/docx_export.py module

New file `shared/docx_export.py` with:

- `_add_highlighted_runs(paragraph, text)` — inlined from `_add_docx_highlighted_runs` (module-level, no self)
- `_set_paragraph_rtl(paragraph)` — inlined from `_set_paragraph_rtl` (module-level, no self)
- `_genizah_url_for(sys_id)` — generates `https://genizahsearch.com/?sys_id=…`
- `_page_label(result_dict)` — D-02: chunk_locator VERBATIM, p_num fallback only
- `write_docx_result_block(doc, result_dict, filepath='', lang='en') -> None` — main public API

Block layout per D-10/D-11:
1. Heading paragraph (bold): `Filename — Parent` (LOCAL) or `Shelfmark — Title` (Genizah)
2. Metadata line: `filepath · page · LOCAL` (LOCAL) or `Library · img · Source` (Genizah)
3. Matched-text paragraph with bold-red `*`-highlight runs
4. URL line: full filepath (LOCAL) or `genizahsearch.com/?sys_id=…` (Genizah)
5. Separator `_` × 40

### Task 3 — Unit tests

- `tests/test_export_dossier_local.py` — 9 tests pinning bilingual headers, sheet_titles keys, row shape, None-safety, sanitize_fn skips item[4]
- `tests/test_docx_export_block.py` — 8 tests pinning LOCAL/Genizah block fields, chunk_locator verbatim (no double-prefix), p_num fallback, highlight runs, two-block separator, missing filepath graceful, RTL w:bidi on lang='he'

## Verification Results

All plan success criteria met:

```
python -m pytest tests/test_export_dossier_local.py tests/test_docx_export_block.py -x -q
17 passed in 0.49s

python -m pytest tests/test_export_dossier_local_handling.py -x -q
9 passed in 0.13s  (no regression)

python -c "from shared.export_dossier import local_documents_header_row, build_local_document_row, sheet_titles; from shared.docx_export import write_docx_result_block; print('imports OK')"
imports OK

python -m ruff check shared/export_dossier.py shared/docx_export.py tests/test_export_dossier_local.py tests/test_docx_export_block.py
All checks passed!
```

## Commits

| Task | Commit | Message |
|------|--------|---------|
| 1 | 7ddecf59 | feat(103-01): add Local Documents bilingual header/title + row builder to export_dossier |
| 2 | 60d0a1a5 | feat(103-01): add shared/docx_export.py module-level DOCX block writer |

## Deviations from Plan

None — plan executed exactly as written. The deliberate PATTERNS.md copy-paste bug (`return list(_MAIN_HEADERS_EN)` in the EN branch of `local_documents_header_row`) was noted in the plan and corrected to `return list(_LOCAL_HEADERS_EN)`.

## Known Stubs

None. Both artifacts are fully implemented — no hardcoded empty values or placeholder data.

## Threat Flags

None. No new network endpoints, auth paths, or trust-boundary surfaces introduced. The T-103-01 formula-injection vector (LOCAL filepath in xlsx cells) is intentionally deferred to Plan 02's `sanitize_fn` wiring — confirmed by the `build_local_document_row` contract that passes cells 0-3 through `sanitize_fn` (caller provides `sanitize_text_for_excel`).

## Self-Check: PASSED

Files exist:
- shared/export_dossier.py — FOUND (modified)
- shared/docx_export.py — FOUND (created)
- tests/test_export_dossier_local.py — FOUND (created)
- tests/test_docx_export_block.py — FOUND (created)

Commits exist:
- 7ddecf59 — FOUND
- 60d0a1a5 — FOUND
