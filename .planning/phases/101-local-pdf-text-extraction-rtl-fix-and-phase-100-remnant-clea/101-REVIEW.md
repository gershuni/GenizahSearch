---
phase: 101-local-pdf-text-extraction-rtl-fix-and-phase-100-remnant-clea
reviewed: 2026-05-28T04:45:36Z
depth: standard
files_reviewed: 9
files_reviewed_list:
  - genizah_app.py
  - shared/local_indexer.py
  - tests/conftest.py
  - tests/test_format_rtl_invariant.py
  - tests/test_local_indexer.py
  - tests/test_local_pdf_extraction_fallback.py
  - tests/test_pdf_image_controller.py
  - tests/test_wr01_open_local_browse_page_ast.py
  - tests/fixtures/local_indexer/README.md
findings:
  critical: 0
  warning: 0
  info: 3
  total: 3
status: clean
---

# Phase 101: Code Review Report

**Reviewed:** 2026-05-28T04:45:36Z
**Depth:** standard
**Files Reviewed:** 9
**Status:** clean (3 info-level observations, no blocking issues)

## Summary

Phase 101 adds (a) a sort=True RTL word-order fix for PyMuPDF PDF extraction
(directional-run reversal in `_fix_sort_true_rtl_line` / `_fix_sort_true_rtl_page`),
(b) a one-shot committed-only PDF re-scan triggered by an extractor-version marker,
and (c) a WR-01 cleanup in `_open_local_browse_page` collapsing a duplicate
`self._lookup_local_filepath(sys_id)` call so the two PDF/non-PDF decision paths
can never diverge.

The diff is small (+163 / -1 in `shared/local_indexer.py`, ~14 lines net in
`genizah_app.py`), surgical, and well-protected by tests:

- **Directional-run algorithm**: correctly groups by per-token RTL/non-RTL,
  reverses the run sequence, reverses tokens within RTL runs (so pure-RTL lines
  flip), preserves order inside non-RTL runs (shelfmarks stay adjacent). The
  `_rtl_ratio > 0.4` gate has explicit boundary tests on both sides.
- **SQL extractor-version re-scan**: uses `BEGIN IMMEDIATE` to serialize
  concurrent launches, `cur.rowcount` (not a post-UPDATE `SELECT COUNT`) for the
  log count, `AND status = 'committed'` to avoid reviving error/failed/pending
  rows, `COALESCE(file_extension, '')` to defend against NULL, and writes the
  marker AFTER the SQLite commit so a crash between commit and FS write yields
  an idempotent repeat (the next launch's UPDATE matches 0 rows). Multi-process
  concurrency was verified: writes converge to the same marker.
- **WR-01 cleanup**: AST guards count exactly ONE `filepath` binding across all
  rebind forms (Assign / AugAssign / AnnAssign / NamedExpr / unpack / For /
  except-as) and exactly ONE `_lookup_local_filepath` call. The behavior change
  from `fp = ... or ""` (str) to `filepath = _resolved` (str or None) is
  correctly absorbed by the `bool(filepath) and filepath.lower().endswith('.pdf')`
  short-circuit and by the existing `if filepath else sys_id` guards downstream.
- **Test invariants**: REV-2c positive-assertion (extract_pdf_pages MUST be
  among callers of `_fix_sort_true_rtl_page`), REV-2a branch-integration via a
  `_FakePdfPage` synthesizing one-word-per-line blocks output to force the
  fallback, and `strict=True` xfail on the S-8 pathological-mixed-script case
  so an accidental fix actually fails CI. All strong.

No bugs, security issues, or correctness regressions found. Three minor
informational observations follow.

## Info

### IN-01: Non-atomic write of `.extractor_version` marker file

**File:** `shared/local_indexer.py:592-596`
**Issue:** `_write_extractor_version` writes the marker via plain
`open(..., "w")` rather than write-to-tmp-then-rename. If the OS or process
dies mid-write, the marker file can be left empty or partially written. This
is NOT a correctness bug: `_read_extractor_version` returns `None` for an
empty file (`f.read().strip() or None`), which on the next launch evaluates
`None != "2"` and re-runs the (idempotent) UPDATE and re-attempts the write.
The sibling `.schema_version` marker (`_write_schema_marker`, line 559-563)
uses the same non-atomic pattern, so this is a consistency choice rather than
a regression. Flag for future hardening only.

**Fix (optional, future):**
```python
def _write_extractor_version(index_dir: str, version: str) -> None:
    os.makedirs(index_dir, exist_ok=True)
    tmp = os.path.join(index_dir, _EXTRACTOR_VERSION_FILE + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(version)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, os.path.join(index_dir, _EXTRACTOR_VERSION_FILE))
```

### IN-02: `_fix_sort_true_rtl_page` normalizes line terminators

**File:** `shared/local_indexer.py:431-439`
**Issue:** `text.splitlines()` accepts several line-terminator characters
beyond `\n` (including `\r\n`, `\r`, `\v`, `\f`, `\x1c`, `\x1d`, `\x1e`,
`\x85`, plus the two Unicode separators U+2028 and U+2029) but the join uses
only `\n`. The docstring asserts that "PyMuPDF get_text('text', sort=True)
always emits '\n' line terminators on all platforms", which is correct for
current PyMuPDF; if a future version emits CRLF on Windows, the change would
silently normalize. This is acceptable for downstream search indexing
(whitespace is already normalized by the Tantivy tokenizer) but the trade-off
should remain documented.

**Fix (none required):** Current code is correct for the documented contract.
Consider a one-line assertion in tests pinning the PyMuPDF line-terminator
shape (e.g., `assert "\r" not in fallback_text`) if future PyMuPDF upgrades
break the assumption.

### IN-03: Test fixture `_refresh_local_indexer_for_local_indexer_tests` only rebinds 3 names

**File:** `tests/conftest.py:208-224`
**Issue:** The autouse fixture rebinds `LocalIndexer`, `EncodingError`,
`extract_txt` in the test module's namespace after reload. The module-level
import in `tests/test_local_indexer.py:16-25` brings in 8 names from
`shared.local_indexer` (`_fix_rtl_line`, `_fix_rtl_page`,
`_join_fragmented_lines`, `_rtl_ratio`, `extract_pdf_pages` are NOT rebound).
This is correct under current usage: the un-rebound names are dead-code
helpers or pure functions whose tests don't rely on class identity. But if
future tests in this file ever do `isinstance(x, SomeClassFromLocalIndexer)`
or `pytest.raises(SomeNewExceptionClass)` using one of the un-rebound names,
the same stale-alias bug the fixture documents could resurface.

**Fix (optional defensive):** Walk all attributes that appear in
`request.module.__dict__` and were originally sourced from
`shared.local_indexer`, rebinding each:
```python
_indexer_dict = vars(shared.local_indexer)
for _name in list(vars(request.module)):
    if _name in _indexer_dict and not _name.startswith('_pytest'):
        setattr(request.module, _name, _indexer_dict[_name])
```
Current targeted rebind is sufficient for the tests that exist today.

---

_Reviewed: 2026-05-28T04:45:36Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
