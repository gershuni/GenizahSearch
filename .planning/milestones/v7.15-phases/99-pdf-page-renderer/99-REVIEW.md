---
phase: 99-pdf-page-renderer
reviewed: 2026-05-27T00:00:00Z
depth: standard
files_reviewed: 3
files_reviewed_list:
  - desktop/pdf_page_renderer.py
  - scripts/generate_pdf_render_fixtures.py
  - tests/test_pdf_page_renderer.py
findings:
  critical: 0
  warning: 2
  info: 5
  total: 7
status: issues_found
---

# Phase 99: Code Review Report

**Reviewed:** 2026-05-27
**Depth:** standard
**Files Reviewed:** 3
**Status:** issues_found

## Summary

The Phase 99 PDF page renderer is a well-structured, carefully-documented module. The
single-owner thread discipline (D-09a), the single logging chokepoint (`_log_and_raise`),
the mandatory `QImage.copy()` to avoid use-after-free on the C pixmap buffer, the
bounds-check-before-render ordering, and the no-crash worker envelope are all implemented
correctly and are backed by targeted tests (call-count spies, GC-survival, loop-continuity
on a real QThread). Failure classification is exhaustive and the LRU best-effort close is
correct.

No critical bugs or security issues were found. Two warnings concern test fragility and a
cross-thread signal-type assumption that is correct in PyQt6 but undocumented/untested under
a real queued connection. The remaining items are minor robustness/consistency notes.

The module is read-only over user files (no writes), uses no `eval`/`exec`/shell, and has no
hardcoded secrets. fitz access is correctly confined to the render thread.

## Warnings

### WR-01: `render_failed` passes a non-metatype `object` slot across a queued connection — only ever tested via DirectConnection or sync calls

**File:** `desktop/pdf_page_renderer.py:379` (signal decl) and `tests/test_pdf_page_renderer.py:500-660`
**Issue:** The `render_failed` signal declares the reason slot as `object` (a raw
`PdfRenderFailure` enum, not a registered Qt metatype). The module docstring and code
comments justify `object` precisely so it can cross a *queued* cross-thread connection. However,
every test that exercises a `render_failed` emission either calls `_handle_request` synchronously
(`test_token_echoed_in_signals`, `test_worker_failure_routes_to_render_failed`) or uses
`Qt.ConnectionType.DirectConnection` (`test_worker_survives_bad_render_and_serves_next`). None
exercise the real production path: worker thread emits → UI thread receives via the *default
(AutoConnection → Queued)* connection. The Phase 100 controller will connect with the default
(queued) connection, and a queued connection must marshal the `object` argument across threads.
PyQt6 does support arbitrary Python objects across queued connections, so this is very likely
correct — but the contract that matters in production is untested here, and a regression would
only surface in Phase 100 integration.
**Fix:** Add one test that starts the real `QThread`, connects `render_failed` with the *default*
connection type (no explicit `DirectConnection`), pumps the Qt event loop
(`QCoreApplication.processEvents()` in a wait loop), and asserts the enum reason arrives intact
on the receiving side. This pins the queued-marshaling assumption the docstring relies on.

### WR-02: `test_no_disk_cache` cannot detect fitz's actual disk writes — it spies the wrong layer

**File:** `tests/test_pdf_page_renderer.py:216-253`
**Issue:** The test patches `builtins.open` and diffs `os.listdir(tmpdir)` to prove "no disk
cache". But fitz/MuPDF performs file I/O in C, which never goes through Python's `builtins.open`,
so the `write_calls` spy can never observe a fitz-originated write. The `os.listdir` diff also
only watches `tmpdir`, which is never set as the process cwd and is unrelated to where fitz would
write a cache. The test therefore passes trivially and does not actually verify the
"no-disk-cache" (PDFIMG-02 / D-06) contract it claims to. This is a false-confidence test, not a
source bug.
**Fix:** Either (a) snapshot the temp dir AND the directory containing the source PDF before/after
render and assert no new sibling files appear, or (b) downgrade the test's docstring to state it
only verifies no *Python-level* writes and add a comment that fitz C-level writes are out of scope.
Option (a) is closer to the intended guarantee.

## Info

### IN-01: `_open_doc_classified` reports a missing non-PDF path as MISSING_FILE, not NOT_PDF

**File:** `desktop/pdf_page_renderer.py:160-171`
**Issue:** Existence is checked before the extension. A non-existent `foo.txt` classifies as
MISSING_FILE even though it is also not a PDF. The behavior is internally consistent and the test
at lines 276-282 works around it by locating an existing `.txt`, but the docstring's stated
classification order means "not a PDF" can be masked by "missing". This is acceptable (a missing
file is a missing file) but worth an explicit note so Phase 100's user-facing placeholder mapping
does not assume NOT_PDF fires for every non-`.pdf` path.
**Fix:** Add one sentence to the `_open_doc_classified` docstring clarifying that for a
non-existent path the MISSING_FILE reason wins regardless of extension.

### IN-02: `_handle_request` and `render_page` use bare `# noqa: ANN001` / untyped `item` param

**File:** `desktop/pdf_page_renderer.py:417`
**Issue:** `_handle_request(self, item)` takes an untyped 4-tuple suppressed with `# noqa: ANN001`.
The tuple shape `(token, sys_id, page_num, filepath)` is documented in `enqueue` but not enforced
at the type level. A typed alias (e.g. `RenderRequest = tuple[int, str, int, str]`) would document
the contract and let static checkers catch a mis-shaped enqueue.
**Fix:** Define `RenderRequest = tuple[int, str, int, str]` and annotate both `enqueue` and
`_handle_request`.

### IN-03: `__builtins__` dict/module branch is dead in normal test execution

**File:** `tests/test_pdf_page_renderer.py:226`
**Issue:** `original_open = __builtins__["open"] if isinstance(__builtins__, dict) else open`.
In a normally-imported test module `__builtins__` is the module (not a dict), so the dict branch
never runs under pytest. Combined with WR-02 this whole spy is inert. Harmless but misleading.
**Fix:** Simplify to `original_open = open` (or remove with the WR-02 rework).

### IN-04: Module-level `_check_fixtures()` call fails collection for the whole file if any fixture is missing

**File:** `tests/test_pdf_page_renderer.py:55`
**Issue:** `_check_fixtures()` runs at import time and calls `pytest.fail(...)`, which aborts
collection of the entire module rather than failing/skipping individual tests. Some tests
(e.g. `test_missing_file_reason`, `test_enqueue_after_stop_dropped`) do not need the fixtures at
all yet would be blocked. The fail message is helpful, but a missing optional fixture takes down
unrelated tests.
**Fix:** Move the check into a `pytest` fixture (autouse on the tests that need it) or a
session-scoped fixture, so unrelated tests still run and the dependency is scoped.

### IN-05: Fixture generator does not regenerate `clean_sample.pdf` / `single_word_per_line.pdf` that the LRU test depends on

**File:** `scripts/generate_pdf_render_fixtures.py:36-49` and `tests/test_pdf_page_renderer.py:154-155`
**Issue:** `test_doc_lru_evict_and_close` and `test_lru_eviction_survives_close_error` open
`clean_sample.pdf` and `single_word_per_line.pdf`, but the Phase 99 generator only produces
`multipage_sample.pdf`, `encrypted_sample.pdf`, and `corrupt_sample.pdf`. Those two extra PDFs
exist today (from the Phase 95 local-indexer fixtures) but are not covered by this phase's
"regenerate via generate_pdf_render_fixtures.py" instruction. If they are ever cleaned, the LRU
tests break with a non-obvious error and the documented regen command will not restore them.
**Fix:** Either add `clean_sample.pdf` / `single_word_per_line.pdf` generation to the script (the
LRU test only needs any two distinct valid PDFs — it could reuse `multipage_sample.pdf` copied to
two temp paths via `tmp_path`), or update `_check_fixtures()` to include them so the failure is
explicit and the regen guidance is accurate.

---

_Reviewed: 2026-05-27_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
