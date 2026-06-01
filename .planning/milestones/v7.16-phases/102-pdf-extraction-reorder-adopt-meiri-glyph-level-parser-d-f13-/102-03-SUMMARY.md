---
phase: 102-pdf-extraction-reorder-adopt-meiri-glyph-level-parser-d-f13-
plan: "03"
subsystem: local-indexer
tags: [nikud-strip, corrupt-detection, cancel-rollback, d-06, d-07, d-08, pdf-extraction]
dependency_graph:
  requires: ["102-02"]
  provides: ["all-format-nikud-strip", "buffer-then-decide-corrupt", "cancel-rollback-complete"]
  affects: ["shared/local_indexer.py"]
tech_stack:
  added: []
  patterns:
    - "function-local lazy import for genizah_core isolation (L1)"
    - "buffer-then-decide for file-level quality decisions before any write"
    - "idempotent rollback covers local_pages + processed_files + local_files"
key_files:
  created:
    - tests/test_local_pdf_nikud_strip.py
    - tests/test_local_pdf_corrupt_status.py
  modified:
    - shared/local_indexer.py
decisions:
  - "_write_page_doc is the single shared nikud-strip site for ALL LOCAL formats (D-06 FINAL)"
  - "strip_nikud is lazy-imported function-locally (L1 — no module-top genizah_core import)"
  - "_rollback_partial extended to also delete local_files rows (Codex round-3 HIGH gap)"
  - "conservative >=50% corrupt threshold preserves 1-of-N partial corruption tolerance"
  - "encoding_error remains in indexed bucket (legacy TXT decode status — not reclassified)"
metrics:
  duration: "~30 minutes"
  completed: "2026-05-29"
  tasks_completed: 2
  files_modified: 3
---

# Phase 102 Plan 03: D-06 FINAL all-format nikud strip + buffer-then-decide corrupt flow Summary

**One-liner:** All-format nikud strip at `_write_page_doc` (content==cached_text==stripped, lazy import, version 1→2) + buffer-then-decide corrupt flow with cancel rollbacks in both buffer phase (HIGH Codex round-3) and write loop (M5), wired into 3 D-08 surfaces.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 RED | Failing tests for D-06 FINAL all-format nikud strip | `57f44a3b` | tests/test_local_pdf_nikud_strip.py |
| 1 GREEN | D-06 FINAL: strip nikud ONCE in _write_page_doc for ALL formats | `b3c6ed55` | shared/local_indexer.py |
| 2 RED | Failing tests for corrupt flow + cancel rollbacks + D-08 surfaces | `eb5df39a` | tests/test_local_pdf_corrupt_status.py |
| 2 GREEN | Buffer-then-decide corrupt + cancel rollbacks + corrupt_encoding surfaces | `61802def` | shared/local_indexer.py |

## What Was Built

### Task 1: D-06 FINAL — All-format nikud strip in `_write_page_doc` (L1)

**Changes to `shared/local_indexer.py`:**

- Added a function-local lazy import `from genizah_core import strip_nikud` inside `_write_page_doc` (L1 — keeps `shared/local_indexer.py` free of a module-top `genizah_core` import).
- Computes `stripped = strip_nikud(text)` once near the top of the function.
- `content=[stripped]`, `words = stripped.split()`, `compress_cached_text(stripped)` — content == cached_text == stripped for ALL LOCAL formats (PDF, DOCX, TXT, HTML, XLSX, CSV).
- `extraction_format_version` literal bumped from `1` to `2` in the INSERT.
- Plan 02's `extract_pdf_pages` now yields nikud-bearing text; the strip is at this single shared write site for every format (D-06 FINAL, reverts round-2 PDF-only M4 narrowing).
- `rebuild_main_index_atomic` reads the already-stripped `cached_text` per row and writes a stripped `content` — auto-consistent with no change needed.

**Tests (`tests/test_local_pdf_nikud_strip.py`):**
- `test_write_page_doc_strips_nikud_all_formats`: vocalized string → cached_text == consonantal
- `test_write_page_doc_docx_simulated_strip`: inverted M4 guard (DOCX path also stripped)
- `test_write_page_doc_extraction_format_version_is_2`: version == 2
- `test_unvocalized_query_matches_vocalized_source`: consonantal query matches vocalized-source page
- `test_no_module_top_strip_nikud_import`: L1 AST guard

### Task 2: Buffer-then-decide corrupt flow + cancel rollbacks + D-08 surfaces

**Changes to `shared/local_indexer.py`:**

**`_extract_and_write_pdf` rewritten (buffer-then-decide):**
1. BUFFER phase: accumulates all yielded pages into a list while populating `page_flags` (D-07). Checks `cancel_check()` on every buffer iteration — if cancelled, calls `self._rollback_partial(sys_id)` before returning `"cancelled"` (HIGH Codex round-3: cleans up `_index_one_file`'s pre-inserted `processed_files` + `local_files` rows that would otherwise be flipped to `committed` by `_commit_batch`).
2. File-level corrupt decision: if ≥50% of buffered pages have `page_flags[n]["corrupt"] == True`, returns `(0, "corrupt_encoding", title)` WITHOUT calling `_write_page_doc` (HIGH-2 detect-before-write).
3. Write loop: iterates buffered pages, checks `cancel_check()` before each `_write_page_doc` — on cancel calls `self._rollback_partial(sys_id)` and returns `"cancelled"` (M5).

**`_rollback_partial` extended:**
- Now also executes `DELETE FROM local_files WHERE sys_id = ?` (Codex round-3 HIGH gap). Previously only deleted `local_pages` and `processed_files`; the missing `local_files` delete meant a buffer-phase cancel left an orphan `local_files` row.

**D-08 surfaces:**
- Surface 1 (`_ERROR_STATUSES_KEPT`): added `"corrupt_encoding"`.
- Surface 2 (scan classification `~line 2293`): added comment; the indexed-bucket tuple `("ok", "no_text_layer", "encoding_error", "unsupported")` is unchanged — `corrupt_encoding` falls through to `result["errors"] += 1`. `encoding_error` (legacy TXT decode status) is preserved in the indexed bucket unchanged.
- Surface 3 (folder counter SQL `~line 3300`): added `'corrupt_encoding'` to the `error_count` subquery's `extraction_status IN (...)` list.

**Tests (`tests/test_local_pdf_corrupt_status.py`):**
- `test_corrupt_encoding_in_error_statuses_kept`: Surface 1 guard
- `test_corrupt_file_writes_zero_pages`: HIGH-2 — ≥50%-corrupt file → pages_written==0 AND `_write_page_doc` never called
- `test_cancel_during_buffering_rolls_back_pre_inserted_rows`: HIGH Codex round-3 — buffer-phase cancel → `_rollback_partial` called → zero `processed_files` AND `local_files` rows remain
- `test_cancel_during_write_loop_rolls_back_partial`: M5 — write-loop cancel → `_rollback_partial` called → zero `local_pages` rows remain
- `test_below_threshold_does_not_trigger_corrupt_encoding`: D-07 conservative threshold guard (1-of-3 corrupt does not flag file)
- `test_scan_classification_corrupt_encoding_counts_as_error`: Surface 2 — `corrupt_encoding` not in indexed-bucket tuple
- `test_scan_classification_encoding_error_still_indexed`: Surface 2 regression guard — `encoding_error` still in indexed-bucket
- `test_folder_counter_sql_includes_corrupt_encoding`: Surface 3 guard

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing functionality] `_rollback_partial` missing `local_files` delete**

- **Found during:** Task 2 implementation (Codex round-3 HIGH requirement)
- **Issue:** The plan specified that `_rollback_partial(sys_id)` "deletes local_pages AND processed_files for sys_id at :2669-2670". On inspection, the current implementation (at line ~3008) only deleted `local_pages` and `processed_files` but NOT `local_files`. Since `_index_one_file` pre-inserts a `local_files` row before calling `_extract_and_write_pdf`, a buffer-phase cancel would leave the `local_files` row behind even after calling `_rollback_partial`.
- **Fix:** Added `DELETE FROM local_files WHERE sys_id = ?` to `_rollback_partial` with an explanatory comment (Codex round-3 HIGH).
- **Files modified:** `shared/local_indexer.py`
- **Commit:** `61802def`

## Verification Results

```
python -m pytest tests/test_local_pdf_nikud_strip.py tests/test_local_pdf_corrupt_status.py -x -q
13 passed, 1 warning in 1.00s

python -m pytest tests/test_local_indexer.py -q
12 passed, 1 warning in 2.77s

python -m ruff check shared/local_indexer.py
All checks passed!
```

## Known Stubs

None — all implementations are complete and wired.

## Threat Flags

None — no new network endpoints, auth paths, file access patterns, or schema changes at trust boundaries introduced by this plan.

## Self-Check: PASSED

- `tests/test_local_pdf_nikud_strip.py` EXISTS
- `tests/test_local_pdf_corrupt_status.py` EXISTS
- `shared/local_indexer.py` modified (greps confirm all changes)
- Commits `57f44a3b`, `b3c6ed55`, `eb5df39a`, `61802def` all exist in git log
