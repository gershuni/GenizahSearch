---
phase: 96
plan: "02"
subsystem: local-indexer
tags: [phase-96, my-library, pdf-extraction, pymupdf, d-f4, detect-then-fallback]
dependency_graph:
  requires: [96-01]
  provides: [pdf-extraction-detect-fallback]
  affects: [shared/local_indexer.py, tests/test_local_pdf_extraction_fallback.py]
tech_stack:
  added: []
  patterns:
    - detect-then-fallback per-page PDF extraction (blocks primary, text+sort=True fallback)
    - monkeypatch fitz.Page.get_text spy for mode-invocation regression testing
key_files:
  created:
    - tests/test_local_pdf_extraction_fallback.py
    - tests/fixtures/local_indexer/single_word_per_line.pdf
    - tests/fixtures/local_indexer/clean_sample.pdf
    - scripts/generate_single_word_fixture.py
  modified:
    - shared/local_indexer.py
decisions:
  - "Used clean_sample.pdf (synthetic) as the 'no-fallback' control instead of hebrew_sample.pdf (which is itself 97% single-word in blocks mode)"
  - "Kept test_good_pdf_stays_blocks pointing at clean_sample.pdf for consistency with mode-spy test"
  - "Fixture generator uses reverse content-stream order (right-to-left within line groups) to make blocks mode see one-word-per-block while sort=True reconstructs visual line order"
metrics:
  duration: ~20 min
  completed: "2026-05-24T09:49:38Z"
  tasks_completed: 2
  files_created: 4
  files_modified: 1
---

# Phase 96 Plan 02: PDF Extraction Detect-Then-Fallback (D-F4) Summary

One-liner: per-page detect-then-fallback in extract_pdf_pages using _detect_single_word_per_line (0.70 threshold) + get_text("text", sort=True), with monkeypatch-spy mode regression test.

## What Was Built

### Task 1: _detect_single_word_per_line helper

Added `_detect_single_word_per_line(text: str) -> bool` to `shared/local_indexer.py` immediately after the `_join_fragmented_lines` dead code (audit trail per plan option b). Constants:

- `_SINGLE_WORD_RATIO_THRESHOLD = 0.70` — tightened from dead-code 0.60 per RESEARCH §2
- `_SINGLE_WORD_MIN_SAMPLE = 5` — prevents false positives on title pages / table cells

### Task 2: Fallback wiring + tests + fixtures

**`extract_pdf_pages` extended** with per-page detect-then-fallback:
1. Primary extraction: `page.get_text("blocks")` assembled as before
2. If `_detect_single_word_per_line(text)` returns True: re-extract via `page.get_text("text", sort=True)`
3. Fallback wrapped in `try/except` — bad PDFs degrade to blocks output rather than crashing
4. `_EMPTY_PAGE_CHAR_THRESHOLD` check runs post-fallback

**Fixtures regenerated** (prior Wave-0 fixture was inadequate per prior executor discovery):

| Fixture | Blocks ratio | sort=True ratio | Purpose |
|---------|-------------|-----------------|---------|
| `single_word_per_line.pdf` | 1.000 | 0.000 | Pathological fixture — triggers fallback |
| `clean_sample.pdf` | 0.000 | — | Clean control — fallback must NOT fire |

The fixture generation technique: words placed at the same y-coordinate (same visual line) but written in reverse content-stream order. `get_text("blocks")` respects content-stream order (one word per block, ratio 1.0). `get_text("text", sort=True)` sorts by (y, x) coordinates and reconstructs multi-word visual lines (ratio 0.0).

**4 tests in `tests/test_local_pdf_extraction_fallback.py`**, all GREEN:
1. `test_pathological_pdf_uses_fallback` — fallback fires on `single_word_per_line.pdf`, ratio drops to 0.0
2. `test_good_pdf_stays_blocks` — `clean_sample.pdf` does NOT trip detector (ratio 0.0)
3. `test_small_sample_skipped` — unit test on helper: < 5 lines returns False
4. `test_good_pdf_does_not_invoke_fallback_mode` — Codex MEDIUM #8 closure: monkeypatch spy confirms `("text", sort=True)` never invoked for `clean_sample.pdf`

## Deviations from Plan

### [Rule 2 - Critical Discovery] Fixture regenerated; new clean_sample.pdf control

**Found during:** Task 2 analysis

**Issue:** The prior executor (worktree `a9320446`) documented that `hebrew_sample.pdf` (the Phase 95 fixture) is itself 96.8-97% single-word in blocks mode. This means:
1. `test_good_pdf_stays_blocks` as originally written (checking `detect(text) is False` on hebrew_sample.pdf) would FAIL because the detector DOES fire
2. `test_good_pdf_does_not_invoke_fallback_mode` as originally written would FAIL because the fallback IS invoked on hebrew_sample.pdf

**Fix:** 
- Regenerated `single_word_per_line.pdf` with a new technique (scrambled content-stream at same y-coord) — blocks mode ratio 1.0, sort=True ratio 0.0
- Created new `clean_sample.pdf` (7 sequential `insert_text()` sentences) — blocks mode ratio 0.0
- Both `test_good_pdf_stays_blocks` and `test_good_pdf_does_not_invoke_fallback_mode` updated to use `clean_sample.pdf` as the clean control
- `scripts/generate_single_word_fixture.py` created as a reproducible generator with self-verification assertions

**Files modified:** tests/fixtures/local_indexer/single_word_per_line.pdf (regenerated), tests/fixtures/local_indexer/clean_sample.pdf (new), tests/test_local_pdf_extraction_fallback.py (clean_sample.pdf references), scripts/generate_single_word_fixture.py (new)

### Branch base note

The prior executor's `a9320446` commit (Task 1 helper only, in worktree `worktree-agent-a65491fe4e03f17b7`) was NOT on this branch (`worktree-agent-ae653e01a9c83affb`). Both tasks were implemented from scratch on this branch. The `_detect_single_word_per_line` helper implementation is identical to the plan specification.

## Known Stubs

None. All functionality is fully wired.

## Test Results

```
tests/test_local_pdf_extraction_fallback.py: 4 passed
tests/test_local_indexer.py + incremental + scale: 12 passed, 1 skipped
Full LOCAL bundle (test_local_*.py + web invariant + no-raw-storage): 166 passed, 2 skipped, 2 xfailed
Ruff: clean
```

## Self-Check: PASSED

- `shared/local_indexer.py` contains `_detect_single_word_per_line` and `get_text("text", sort=True)` — confirmed
- `tests/test_local_pdf_extraction_fallback.py` contains `test_good_pdf_does_not_invoke_fallback_mode` — confirmed (count: 1)
- `tests/fixtures/local_indexer/single_word_per_line.pdf` exists — confirmed
- `tests/fixtures/local_indexer/clean_sample.pdf` exists — confirmed
- Commits: `8727828a` (feat) and `ef34f0c6` (test) — confirmed
