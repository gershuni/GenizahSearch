---
phase: 103-search-results-local-export-all-formats-bilingual-non-regression
reviewed: 2026-06-01T00:00:00Z
depth: standard
files_reviewed: 9
files_reviewed_list:
  - shared/export_dossier.py
  - shared/docx_export.py
  - genizah_app.py
  - tests/test_export_dossier_local.py
  - tests/test_docx_export_block.py
  - tests/test_local_export_xlsx.py
  - tests/test_local_export_csv_txt_docx.py
  - tests/test_local_export_non_regression.py
  - tests/test_export_dossier.py
findings:
  critical: 0
  warning: 3
  issues: 3
  info: 3
  total: 6
status: issues_found
---

# Phase 103: Code Review Report

**Reviewed:** 2026-06-01
**Depth:** standard
**Files Reviewed:** 9 (genizah_app.py reviewed at diff-only scope per instruction)
**Status:** issues_found

## Summary

Phase 103 adapts the desktop result-export pipeline so LOCAL "My Library" hits export with locally-meaningful columns across XLSX, CSV, TXT, and DOCX. The architecture is clean: a new `Local Documents` worksheet in xlsx, two extra CSV columns when LOCAL hits are present, a new TXT block format, and a shared per-result DOCX block writer replacing the pre-existing table layout.

The primary security concern — spreadsheet/CSV formula-injection from filenames, folder names, and matched text — is correctly handled. The xlsx Local Documents sheet passes all four LOCAL-originated string values (filename, parent folder, filepath, page) through `sanitize_fn` inside `build_local_document_row`, and matched text through `build_rich_snippet_cell(val, sanitize_fn)`. The CSV branch applies `sanitize_text_for_excel` to every cell in the base row and to the two appended filepath/page cells. The `_csv_extra_cols` helper pre-sanitizes both extra columns before returning them.

Three warnings are raised: one is a logic gap in the xlsx sheet where LOCAL values are written to cells but the loop calls `sanitize_fn` via the helper for columns 0-3 yet writes the already-sanitized list verbatim without a re-check guard; one is a potential `KeyError` crash in `_format_txt_genizah_block` when a result dict lacks a `display` key; and one is a mismatch between `_build_export_data_row` (declared module-level, tested in isolation) and the actual `export_results` method (which does NOT call `_build_export_data_row` — it re-implements the LOCAL row construction inline). Three info items cover dead/untested code, a comment/behavior inconsistency, and a missing test edge case.

---

## Warnings

### WR-01: `_format_txt_genizah_block` crashes with `KeyError` on malformed result dict

**File:** `genizah_app.py` (in the `_format_txt_genizah_block` module-level function, diff region around `f"=== {result_dict['display']['shelfmark']} ..."`))

**Issue:** The function accesses `result_dict['display']['shelfmark']` and `result_dict['display']['title']` with hard bracket indexing (not `.get()`), exactly as the pre-v7.17 inline code did. If `result_dict` is missing the `'display'` key (e.g., a malformed history-replay result), or `'display'` is `None`, or `'shelfmark'`/`'title'` are absent, Python raises `KeyError`/`TypeError`. The pre-v7.17 code had the same exposure in-line (non-regression), but now that the code is extracted to a standalone helper that is separately called and tested, the fragility is more visible and the fix is trivial.

The function body is:
```python
def _format_txt_genizah_block(result_dict):
    snippet = result_dict.get('raw_file_hl', '').strip().replace('\n', ' ').replace('\r', '')
    return f"=== {result_dict['display']['shelfmark']} | {result_dict['display']['title']} ===\n{snippet}"
```

**Fix:**
```python
def _format_txt_genizah_block(result_dict):
    snippet = result_dict.get('raw_file_hl', '').strip().replace('\n', ' ').replace('\r', '')
    d = result_dict.get('display') or {}
    shelfmark = d.get('shelfmark', '')
    title = d.get('title', '')
    return f"=== {shelfmark} | {title} ===\n{snippet}"
```

---

### WR-02: `_build_export_data_row` is declared but never called in the actual export path

**File:** `genizah_app.py` (module-level `_build_export_data_row`, diff region; also `export_results` diff at `data_rows.append` for LOCAL branch)

**Issue:** `_build_export_data_row` is a module-level helper with its own tests in `test_local_export_csv_txt_docx.py`. However, the actual `export_results` method does **not** call it — it re-implements the LOCAL row construction inline, duplicating the logic (different variable names, same column ordering). The module-level function is essentially dead code in production.

This is a maintenance trap: a future bug fix in `_build_export_data_row` will not affect the live code path, and the tests for `_build_export_data_row` give false confidence about `export_results` behavior. The inline LOCAL block in `export_results` (around line 19860-19875 in the diff) is correct, but having a tested helper that the production code ignores is misleading.

**Fix:** Either wire `export_results` to call `_build_export_data_row` (and adjust the `_export_filepath` closure accordingly), or remove `_build_export_data_row` and update `test_local_export_csv_txt_docx.py` to test `export_results` behavior more directly. The former is cleaner. Minimal wiring:
```python
# In export_results LOCAL branch, replace the inline construction with:
row, _ = _build_export_data_row(r, filepath_fn=_export_filepath)
data_rows.append(row)
```
Note: `_build_export_data_row` does not include the `snippet` normalization (multi-space collapse via `re.sub`) that is applied inline in `export_results` before the branch, so one of the two implementations would need to be aligned.

---

### WR-03: xlsx Local Documents sheet — `build_local_document_row` sanitizes cols 0-3 but the caller then writes `val` (already-sanitized) directly for those columns without going through `sanitize_fn` again; however col 5 (matched text) written via `build_rich_snippet_cell` but the raw value passed is the pre-sanitized output from item[4]

**File:** `genizah_app.py` (xlsx Local Documents write loop, diff region `for col_idx, val in enumerate(row_vals, 1)`)

**Issue:** This is a subtle double-accounting concern. `build_local_document_row` is called with `sanitize_fn=sanitize_fn`, which sanitizes cols 0-3 inside the helper and returns already-sanitized strings in `row_vals[0:4]`. The caller then writes `val` for `col_idx != 5`, meaning the sanitized strings land in the sheet without a second sanitize pass — which is correct.

For col 5 (index 4, `col_idx == 5`), `row_vals[4]` is `matched_text_raw or ''` — raw, unsanitized. The caller passes `val` (which is `row_vals[4]`) to `build_rich_snippet_cell(val, sanitize_fn)`, and `build_rich_snippet_cell` applies `sanitize_fn` before the `*`-split per its documented contract. So matched text IS sanitized for the xlsx path.

However, there is a subtle correctness issue: the snippet assigned to `matched_text_raw` comes from `r.get('raw_file_hl', '') or r.get('snippet', '') or ''` without the multi-space normalization (`re.sub(r'\s+', ' ', snippet)`) that is applied to the CSV/TXT `snippet` variable. This means the xlsx Matched Text cell may contain runs of internal whitespace that the other formats clean up. This is a minor inconsistency rather than a security issue, but it can cause unexpected whitespace in cell content.

**Fix:** Apply the same normalization before assigning `matched_text_raw`:
```python
matched_text_raw = r.get('raw_file_hl', '') or r.get('snippet', '') or ''
matched_text_raw = str(matched_text_raw).replace('\n', ' ').replace('\r', ' ')
# multi-space collapse is optional here since build_rich_snippet_cell handles display
```

---

## Info

### IN-01: `_format_txt_local_block` and `_format_txt_genizah_block` are tested in isolation but not wired through `_format_txt_local_block` in `export_results`

**File:** `genizah_app.py` (TXT branch in `export_results`, diff; module-level `_format_txt_local_block`)

**Issue:** Same pattern as WR-02. `_format_txt_local_block` and `_format_txt_genizah_block` are standalone helpers with tests, but `export_results` TXT branch re-implements the formatting inline. The TXT inline code in `export_results` is functionally equivalent to `_format_txt_local_block`/`_format_txt_genizah_block`, but the duplication means tests of the helpers don't cover the actual production code path. Unlike WR-02 this is lower-risk since TXT is a simpler format, but the structural gap is the same.

**Fix:** Wire `export_results` TXT branch to call the extracted helpers, or document (e.g., with `# NOTE: intentionally not using _format_txt_local_block — see D-xx`) why the inline copy is preferred.

---

### IN-02: `build_local_document_row` docstring says "matched_text_raw retains its `*`-markers" but `test_build_local_document_row_sanitize_skips_matched_text` tests that `sanitize_fn` is NOT applied to item[4]

**File:** `shared/export_dossier.py` line 1225–1226; `tests/test_export_dossier_local.py` line 93–107

**Issue:** The docstring is accurate and the test is correct. However, the test description says "A custom sanitize_fn is applied to items 0-3 but NOT to item[4]" which is the desired behavior. The only gap: the test fixture name says "sanitize_skips_matched_text" but the test also verifies that items 0-3 ARE uppercased (sanitized). The test is correct and complete. This is only an observation that the test's `sanitize_fn` is an `upper()` transformer rather than the actual `sanitize_text_for_excel`; a production-realistic sanitize_fn (one that escapes `=`-prefixed strings) is only tested at the xlsx level in `test_formula_injection_filepath_escaped`. No action required, but noting for completeness.

---

### IN-03: Formula-injection test for xlsx (`test_formula_injection_filepath_escaped`) passes `sanitize_text_for_excel` as the real `sanitize_fn`, but the matched-text column is not tested for injection vectors in the xlsx path

**File:** `tests/test_local_export_xlsx.py`, `TestFormulaSafety`

**Issue:** `test_formula_injection_filepath_escaped` only tests the filepath (column 3). It does not verify that a snippet beginning with `=` (e.g., `=HYPERLINK("x","click")`) is escaped in the xlsx Matched Text column. The code path goes through `build_rich_snippet_cell(val, sanitize_fn)` which does call `sanitize_fn` first, so the actual behavior is correct — but the test gap means a future refactor that bypasses `sanitize_fn` in `build_rich_snippet_cell` would not be caught. `test_local_export_csv_txt_docx.py::test_csv_local_filepath_formula_escaped` does test the snippet filename case at the row-building level, but not the xlsx cell write.

**Fix:** Add a test in `TestFormulaSafety` with a snippet value starting with `=` and assert the xlsx Matched Text cell does not contain a literal formula prefix.

---

_Reviewed: 2026-06-01_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
