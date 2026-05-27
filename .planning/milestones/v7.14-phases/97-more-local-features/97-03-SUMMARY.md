---
phase: 97-more-local-features
plan: "03"
subsystem: local-indexer
tags: [html, xlsx, csv, lxml, openpyxl, defusedxml, rtl, encoding, chunking, desktop]

# Dependency graph
requires:
  - phase: 97-more-local-features/97-01
    provides: _write_page_doc with chunk_locator kwarg, canonical Tantivy schema,
              cached_text, _rollback_partial, scan_runs lifecycle
  - phase: 97-more-local-features/97-02
    provides: _check_zip_bomb, XlsxZipBombSuspected, _MAX_CELLS_PER_SHEET,
              _MAX_CHARS_PER_CHUNK, _upsert_local_files_status, EncodingError

provides:
  - extract_html_pages (lxml.html, semantic h1/h2 + 20-para fallback, encoding chain, is_rtl flag)
  - extract_xlsx_pages (openpyxl streaming, per-sheet 500-row windows, zip-bomb pre-check, is_rtl)
  - extract_csv_pages (csv.Sniffer delimiter detection, utf-8-sig->cp1255->utf-16-le chain, 200-row windows)
  - _extract_and_write_html/xlsx/csv wrapper methods on LocalIndexer
  - _detect_html_encoding helper
  - _SUPPORTED_EXTENSIONS extended to .html / .xlsx / .csv
  - defusedxml.defuse_stdlib() called at module init (XML-bomb defense before openpyxl/lxml)
  - F-06 RTL invariant test (tests/test_format_rtl_invariant.py) — permanent CI guard
  - _XLSX_ROW_WINDOW=500, _CSV_ROW_WINDOW=200, _CSV_ENCODINGS constants

affects: [97-04, 97-05, 97-06, local_indexer, my_library_tab, desktop_search]

# Tech tracking
tech-stack:
  added:
    - defusedxml==0.7.1 (XML-bomb / XXE defense for openpyxl + lxml; requirements.txt + lock)
  patterns:
    - F-01 semantic chunking: len(headings)>=3 AND avg_inter>=5 heuristic
    - F-01 lxml.html NOT BeautifulSoup (RESEARCH Issue #2 — bs4 not installed, lxml transitive via python-docx)
    - F-02 XLSX RTL: metadata-only non-streaming open for sheetView, streaming read_only=True for data
    - F-06 invariant: AST guard test rejects any future _fix_rtl_* call in the 3 new extractors
    - Tuple-shape normalization in _extract_and_write_*: 5-tuple (HTML/XLSX) or 4-tuple (CSV)

key-files:
  created:
    - tests/test_html_extraction.py
    - tests/test_csv_extraction.py
    - tests/test_format_rtl_invariant.py
    - tests/fixtures/local_indexer/hebrew_sample.html
    - tests/fixtures/local_indexer/hebrew_sample.csv
  modified:
    - shared/local_indexer.py
    - tests/test_xlsx_extraction.py (5 Wave C tests appended to existing Wave B file)
    - requirements.txt
    - requirements-lock.txt

key-decisions:
  - "lxml.html used instead of BeautifulSoup — bs4 not installed; lxml==6.0.2 already transitive via python-docx (RESEARCH Issue #2)"
  - "XLSX RTL detection via separate metadata-only open: ReadOnlyWorksheet (read_only=True) lacks sheet_view attribute in openpyxl; metadata open is fast (no cell data loaded)"
  - "CSV trailing-flush locator uses row_num (last CSV row seen) not len(rows_in_window) to avoid counting only non-empty rows"
  - "defusedxml.defuse_stdlib() at module init before any openpyxl/lxml import is exercised — best-effort with ImportError warning"
  - "is_rtl stored in-memory only for Phase 97; logger.debug emits it per chunk; Wave F D-NEW-5 can persist as column if needed"

requirements-completed: [F-01, F-02, F-03, F-04, F-05, F-06]

# Metrics
duration: ~20 min
completed: 2026-05-25
---

# Phase 97 Plan 03: Wave C — HTML + XLSX + CSV Format Extractors Summary

**Three new file-format extractors for My Library: lxml.html semantic chunking, openpyxl streaming XLSX, csv.Sniffer CSV — all with F-06 RTL invariant locked by permanent AST guard test.**

## Performance

- **Duration:** ~20 min
- **Started:** 2026-05-25T12:11:21Z
- **Completed:** 2026-05-25T12:31:41Z
- **Tasks:** 2 (TDD: RED stubs → GREEN implementation)
- **Files modified/created:** 9

## Accomplishments

### F-01: extract_html_pages (lxml.html)

- **NOT BeautifulSoup** — `lxml==6.0.2` already ships transitively via `python-docx`; `beautifulsoup4` is not installed. Using `lxml.html.fromstring()` directly avoids a new dep and a PyInstaller `collect_all` invocation (RESEARCH Issue #2).
- `_detect_html_encoding()` helper: `<meta charset>` → UTF-8 byte-sniff → cp1255 fallback.
- Semantic chunking when `len(headings) >= 3 AND avg_inter >= 5`; 20-paragraph fallback otherwise.
- Strips `<script>` / `<style>` elements before chunking.
- `is_rtl`: True when `<html dir="rtl">` or `<body dir="rtl">`.
- Chunk locators: `§ <heading text>` (semantic) or `¶ N-M` (fallback).
- F-06: NO `_fix_rtl_line` / `_fix_rtl_page` calls — lxml returns logical-order Hebrew strings.

### F-02: extract_xlsx_pages (openpyxl streaming)

- `_check_zip_bomb(filepath)` called **before** `load_workbook` — rejects bomb at zip layer in < 1 ms (Wave B C-05 integration).
- `load_workbook(filepath, read_only=True, data_only=True)` for streaming; separate metadata-only open for `sheetView.rightToLeft` (auto-fix: `ReadOnlyWorksheet` lacks `sheet_view`).
- Per-(sheet, 500-row) window chunking. `cells_seen > _MAX_CELLS_PER_SHEET` raises `XlsxZipBombSuspected`.
- F-04 uniform row: `" | ".join(str(c) for c in row)`.
- Chunk locators: `<SheetName>!R<start>:R<end>`.

### F-03 + F-05: extract_csv_pages (csv.Sniffer)

- Encoding chain: `utf-8-sig` → `cp1255` → `utf-16-le`; `EncodingError` on total failure.
- `csv.Sniffer().sniff(sample, delimiters=",;\t")` over first 4 KB; `csv.excel` fallback.
- Per-200-row window chunks. F-04 uniform row: `" | ".join(str(c) for c in row)`.
- Chunk locators: `rows N-M`.
- `EncodingError` propagates to `_index_one_file` except clause → LD-9 dual write via `_finish_file`.

### F-06: RTL Invariant AST Guard

- `tests/test_format_rtl_invariant.py::test_format_rtl_invariant_no_fix_rtl_in_new_extractors`
- AST-walks `shared/local_indexer.py`, finds all three extractor functions, asserts ZERO calls to `_fix_rtl_line` or `_fix_rtl_page` inside them.
- Also asserts all three functions exist (RED guard in Task 1; GREEN after Task 2).
- This is a **permanent CI guard** — any future PR accidentally wiring PDF mirror-reversal into HTML/XLSX/CSV will fail here first.

### Wiring into LocalIndexer

- `_SUPPORTED_EXTENSIONS` extended: `{".docx", ".pdf", ".txt", ".html", ".xlsx", ".csv"}`.
- `_index_one_file` dispatch: 3 new `elif` branches + `XlsxZipBombSuspected` except clause above existing `EncodingError` catch.
- `_extract_and_write_html`, `_extract_and_write_xlsx`, `_extract_and_write_csv` wrapper methods.
- Each wrapper passes `chunk_locator=locator` to `_write_page_doc` (Wave A LD-1 schema field, Wave C consumers).
- `is_rtl` logged at `logger.debug` level per chunk (in-memory only; Wave F D-NEW-5 can persist).

### defusedxml Module-Init Call

- `defusedxml.defuse_stdlib()` called at top of `shared/local_indexer.py` before any `openpyxl`/`lxml` import is exercised.
- `ImportError` caught with `logger.warning` — best-effort degraded mode preserves compatibility if somehow missing.
- `defusedxml>=0.7,<1.0` added to `requirements.txt`; `requirements-lock.txt` regenerated with `defusedxml==0.7.1`.

## Task Commits

1. **Task 1: RED test stubs + static fixtures** — `dd4f40a0` (test)
2. **Task 2: GREEN — 3 extractors + wiring + defusedxml + ruff fixes** — `bc9e1fae` (feat)

## Chunk-Locator Threading (Wave A Schema Field → Wave C Consumers)

Wave A (97-01) added `chunk_locator` to the Tantivy schema and added the `chunk_locator: str = ""` kwarg to `_write_page_doc`. Wave C is the first consumer that passes real locator strings:

```python
# HTML: "§ פרק א" or "¶ 1-20"
self._write_page_doc(sys_id, chunk_num, text, title, folder_id, chunk_locator=locator)

# XLSX: "Synopsis!R1:R500"
self._write_page_doc(sys_id, chunk_num, text, title, folder_id, chunk_locator=locator)

# CSV: "rows 1-200"
self._write_page_doc(sys_id, chunk_num, text, title, folder_id, chunk_locator=locator)
```

PDF/DOCX/TXT extractors still pass `chunk_locator=""` (Wave F D-NEW-5 will fill those in).

## Tuple-Shape Normalization in _extract_and_write_* Wrappers

HTML and XLSX yield 5-tuples `(chunk_num, text, title, locator, is_rtl)`. CSV yields 4-tuples `(chunk_num, text, title, locator)`. The three wrapper methods handle the different shapes explicitly:

```python
# HTML / XLSX wrappers:
for chunk_num, text, title, locator, is_rtl in extract_html_pages(filepath):
    ...
    logger.debug("Phase 97 F-06 is_rtl=%s for sys_id=%s chunk=%d", is_rtl, sys_id, chunk_num)

# CSV wrapper:
for chunk_num, text, title, locator in extract_csv_pages(filepath):
    ...
```

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `ReadOnlyWorksheet` lacks `sheet_view` attribute in openpyxl**
- **Found during:** Task 2 (test_per_sheet_per_row_window first run)
- **Issue:** `openpyxl.worksheet.read_only.ReadOnlyWorksheet` (returned by `load_workbook(read_only=True)`) has no `sheet_view` attribute — `getattr(ws.sheet_view, "rightToLeft", False)` raises `AttributeError`.
- **Fix:** Pre-read RTL metadata via a separate non-streaming `load_workbook(read_only=False)` call (metadata-only, fast — no cell data loaded). Stream data separately with `read_only=True`. `rtl_map: dict[str, bool]` maps sheet names → is_rtl. `except Exception: pass` ensures RTL detection failure doesn't block extraction.
- **Files modified:** `shared/local_indexer.py`
- **Committed in:** `bc9e1fae` (Task 2)

**2. [Rule 1 - Bug] CSV trailing-flush locator used `len(rows_in_window)` instead of `row_num`**
- **Found during:** Task 2 (test_per_200_row_chunking — expected "rows 401-500", got "rows 401-501")
- **Issue:** `last_row = window_start + len(rows_in_window) - 1` is wrong because `rows_in_window` skips empty lines, so its length != number of CSV rows processed. The test also had a surplus header row.
- **Fix 1:** Track `row_num = 0` before the loop; trailing-flush uses `f"rows {window_start}-{row_num}"` (actual last row seen).
- **Fix 2:** Test data changed from `["a,b,c"] + 500 data rows` (501 rows total) to exactly 500 rows (no header) to get clean "rows 401-500" locator. F-04 has no header assumption anyway.
- **Files modified:** `shared/local_indexer.py`, `tests/test_csv_extraction.py`
- **Committed in:** `bc9e1fae` (Task 2)

**3. [Rule 1 - Style] Unused imports in test files flagged by ruff**
- **Found during:** Task 2 ruff check
- **Issue:** `import os` and `import pytest` unused in `test_html_extraction.py`; `from shared.local_indexer import _CSV_ENCODINGS` unused in `test_csv_extraction.py`.
- **Fix:** Removed the three unused imports.
- **Files modified:** `tests/test_html_extraction.py`, `tests/test_csv_extraction.py`
- **Committed in:** `bc9e1fae` (Task 2)

---

**Total deviations:** 3 auto-fixed (2 Rule 1 bugs, 1 Rule 1 style)
**Impact:** All fixes essential for test correctness. No scope creep.

## Known Stubs

None — all Wave C functionality fully implemented. `is_rtl` is intentionally in-memory only for Phase 97; Wave F D-NEW-5 can add a SQLite/Tantivy column if Browse panel needs per-chunk RTL rendering state persisted.

## Threat Flags

No new network endpoints, auth paths, or cross-trust-boundary surfaces introduced. All changes are local file parsing + SQLite/Tantivy writes.

Threat register mitigations (from plan's threat model):
- **T-97C-01** (Tampering — malformed HTML): mitigated — `lxml.html.fromstring` tolerates malformed HTML; `_MAX_CHARS_PER_CHUNK` caps output; cp1255 fallback handles non-utf-8.
- **T-97C-02** (XXE via lxml/openpyxl): mitigated — `defusedxml.defuse_stdlib()` at module init.
- **T-97C-03** (Billion-laughs XLSX): mitigated — defusedxml + `_check_zip_bomb` at zip layer + `_MAX_CELLS_PER_SHEET` cap.
- **T-97C-04** (Applying `_fix_rtl_*` to HTML/XLSX/CSV): mitigated — `test_format_rtl_invariant.py` AST guard.
- **T-97C-06** (CSV encoding failure): mitigated — `EncodingError` → `_finish_file` writes `extraction_status='encoding_error'` via LD-9.

## Self-Check: PASSED

All files exist and all commits are present:
- `shared/local_indexer.py` FOUND (contains extract_html_pages, extract_xlsx_pages, extract_csv_pages, defusedxml call)
- `tests/test_html_extraction.py` FOUND (4 tests)
- `tests/test_xlsx_extraction.py` FOUND (1 Wave B + 5 Wave C tests)
- `tests/test_csv_extraction.py` FOUND (5 tests)
- `tests/test_format_rtl_invariant.py` FOUND (1 AST guard test)
- `tests/fixtures/local_indexer/hebrew_sample.html` FOUND (dir=rtl, 3 h2)
- `tests/fixtures/local_indexer/hebrew_sample.csv` FOUND (UTF-8-BOM ef bb bf)
- `requirements.txt` FOUND (defusedxml>=0.7,<1.0)
- `requirements-lock.txt` FOUND (defusedxml==0.7.1)
- Task 1 commit `dd4f40a0` FOUND
- Task 2 commit `bc9e1fae` FOUND
- 16 Wave C tests: 16 passed
- 43 Phase 97 tests (all waves): 43 passed
- ruff check: clean on all modified Python files

---
*Phase: 97-more-local-features*
*Completed: 2026-05-25*
