---
phase: 99-pdf-page-renderer
plan: "01"
subsystem: desktop/pdf-renderer
tags: [pdf, rendering, fitz, pymupdf, lru, qimage, failure-classification, desktop-only]
dependency_graph:
  requires: []
  provides: [desktop/pdf_page_renderer.py, PdfRenderFailure enum, DocLRU, render_page, render_via_lru]
  affects: [desktop/pdf_page_renderer.py, tests/test_pdf_page_renderer.py]
tech_stack:
  added: []
  patterns:
    - OrderedDict LRU with explicit .close() on eviction
    - Single logging chokepoint (_log_and_raise) — logs exactly once before raise
    - QImage(.copy()) memory-safety pattern for fitz pixmap → Qt bridge
    - D-04 classification order (missing → extension → open → encrypted → bounds → render)
key_files:
  created:
    - desktop/pdf_page_renderer.py
    - scripts/generate_pdf_render_fixtures.py
    - tests/fixtures/local_indexer/multipage_sample.pdf
    - tests/fixtures/local_indexer/encrypted_sample.pdf
    - tests/fixtures/local_indexer/corrupt_sample.pdf
    - tests/test_pdf_page_renderer.py
  modified: []
decisions:
  - "Test uses genizah_logger.addHandler(caplog.handler) to capture logs — genizah_core.get_logger() sets propagate=False so caplog root-level capture does not work; directly attaching the handler is the correct pattern"
  - "LRU eviction test uses only valid PDFs (clean_sample + single_word + multipage); encrypted/corrupt correctly raise PdfRenderError on insert and cannot be used as valid LRU entries"
  - "width*3 comment removed from production code to satisfy acceptance criteria grep check; doc comment updated to 'never compute stride manually'"
metrics:
  duration: "~25 minutes"
  completed: "2026-05-27"
  tasks_completed: 2
  tasks_total: 2
  files_created: 6
  files_modified: 0
---

# Phase 99 Plan 01: Render Core — Wave 0 Test Scaffold + Implementation Summary

**One-liner:** PyMuPDF → QImage renderer with bounded LRU, single-log failure classification, and 16 green Wave 0 tests covering PDFIMG-01/02/06.

## What Was Built

Plan 01 delivered the synchronously-testable core of `desktop/pdf_page_renderer.py` — the non-QThread portion of the Phase 99 PDF page renderer:

**`desktop/pdf_page_renderer.py`** (303 lines, ruff clean):
- `PdfRenderFailure` enum (8 members, D-04): `missing-file`, `not-pdf`, `encrypted`, `corrupt`, `page-out-of-range`, `render-error`, `timeout`, `cancelled`
- `PdfRenderError` exception carrying `reason: PdfRenderFailure` and `detail: str`
- `_log_and_raise()` — single logging chokepoint (REVIEW item 1): logs WARNING exactly once then raises; every failure path calls this, never a bare `raise PdfRenderError`
- `_open_doc_classified()` — classifies open-time errors in D-04 order (existence → extension → fitz.open → needs_pass)
- `DocLRU(maxsize=4)` — bounded OrderedDict LRU of open `fitz.Document` handles; best-effort `.close()` on eviction and in `close_all()`
- `render_page(doc, page_num)` — bounds check BEFORE pixmap (D-04a), `get_pixmap(dpi=200, colorspace=csRGB, alpha=False)`, mandatory `img.copy()` (D-01b), `pix.stride` (not computed)
- `render_via_lru(lru, filepath, page_num)` — convenience entry point for Plan 02 worker and tests

**`scripts/generate_pdf_render_fixtures.py`**: runnable generator for three fixtures.

**Three fixtures** committed under `tests/fixtures/local_indexer/`:
- `multipage_sample.pdf` — 3 pages with "Page N" text; used by render + bounds tests
- `encrypted_sample.pdf` — AES-256 encrypted; `doc.needs_pass = True`
- `corrupt_sample.pdf` — valid header, garbage body; `fitz.open()` raises

**`tests/test_pdf_page_renderer.py`** (16 tests, all green):
All named tests from the plan's acceptance criteria, covering PDFIMG-01 (render correctness + single-page-only + memory safety), PDFIMG-02 (LRU eviction + no-disk-cache), PDFIMG-06 (all failure classifications), and D-03 token/signal enum contract.

## Verification

```
pytest tests/test_pdf_page_renderer.py -x -q   → 16 passed
pytest tests/test_pdf_page_renderer.py tests/test_local_indexer.py tests/test_local_pdf_extraction_fallback.py → 29 passed
python -m ruff check desktop/pdf_page_renderer.py scripts/generate_pdf_render_fixtures.py tests/test_pdf_page_renderer.py → All checks passed
```

## Commits

| Hash | Type | Description |
|------|------|-------------|
| `eee24e02` | test(99-01) | Wave 0 fixture generator + 16 RED tests scaffold |
| `45df8703` | feat(99-01) | Render core — PdfRenderFailure, DocLRU, render functions (RED→GREEN) |

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] caplog capture for genizah logger with propagate=False**
- **Found during:** Task 2 (test_failures_logged failing)
- **Issue:** `genizah_core.get_logger()` returns a child of the `genizah` logger which has `propagate=False`. `caplog.at_level(logger="desktop.pdf_page_renderer")` produced empty records because the logger name is actually `genizah.desktop.pdf_page_renderer` and the logger does not propagate to root.
- **Fix:** `test_failures_logged` now does `genizah_logger.addHandler(caplog.handler)` + `genizah_logger.removeHandler(caplog.handler)` in a try/finally to capture records directly from the `genizah` logger hierarchy. This is the correct pattern for non-propagating loggers.
- **Files modified:** `tests/test_pdf_page_renderer.py`
- **Commit:** `45df8703`

**2. [Rule 1 - Bug] LRU test used encrypted/corrupt paths as valid cache entries**
- **Found during:** Task 2 (test_doc_lru_evict_and_close and test_lru_eviction_survives_close_error failing)
- **Issue:** Test tried to `lru.get(ENCRYPTED_PDF)` and `lru.get(CORRUPT_PDF)` as the second/third LRU entry. Since `_open_doc_classified` correctly raises `PdfRenderError` for these, they cannot be inserted as valid cache entries. The test needed to use only valid PDF paths.
- **Fix:** Changed both LRU tests to use `multipage_sample.pdf`, `clean_sample.pdf`, and `single_word_per_line.pdf` — all three are valid openable PDFs.
- **Files modified:** `tests/test_pdf_page_renderer.py`
- **Commit:** `45df8703`

**3. [Rule 1 - Bug] width*3 in comments triggered grep acceptance check**
- **Found during:** Task 2 post-verification (acceptance criteria grep)
- **Issue:** Two comments in `desktop/pdf_page_renderer.py` contained the literal `width*3` as anti-pattern documentation ("NOT width*3"). The plan's acceptance criteria states "grep confirms `width*3` and `width * 3` are ABSENT".
- **Fix:** Replaced with "never compute stride manually" — same meaning, no forbidden literal.
- **Files modified:** `desktop/pdf_page_renderer.py`
- **Commit:** `45df8703`

## Requirements Covered

- **PDFIMG-01:** `render_page(doc, page_num)` returns non-null copied QImage for page at index `page_num-1`; verified by `test_render_single_page` + `test_only_requested_page_rendered` + `test_qimage_independent_of_pixmap`
- **PDFIMG-02 (LRU + no-disk half):** `DocLRU` evicts+closes oldest past maxsize, closes all on shutdown, no disk write during render; verified by `test_doc_lru_evict_and_close` + `test_lru_eviction_survives_close_error` + `test_no_disk_cache`
- **PDFIMG-06 (classification half):** missing/not-pdf/encrypted/corrupt/out-of-range each map to the correct `PdfRenderFailure` reason, logged exactly once; verified by 6 failure tests + `test_failures_logged`
- **D-03 (token contract stub):** `test_token_echoed_in_signals` verifies all 8 enum members by value; Plan 02 upgrades it with real QThread signal-echo assertions

## Self-Check: PASSED

| Item | Result |
|------|--------|
| `desktop/pdf_page_renderer.py` exists | FOUND |
| `scripts/generate_pdf_render_fixtures.py` exists | FOUND |
| `tests/test_pdf_page_renderer.py` exists | FOUND |
| `tests/fixtures/local_indexer/multipage_sample.pdf` exists | FOUND |
| `tests/fixtures/local_indexer/encrypted_sample.pdf` exists | FOUND |
| `tests/fixtures/local_indexer/corrupt_sample.pdf` exists | FOUND |
| Commit `eee24e02` exists | FOUND |
| Commit `45df8703` exists | FOUND |
| All 16 tests pass | 16 passed |
| ruff clean | All checks passed |
