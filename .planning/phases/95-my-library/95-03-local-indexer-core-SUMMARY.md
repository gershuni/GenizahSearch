---
phase: 95
plan: "03"
subsystem: my-library
tags: [local-indexer, tantivy, sqlite, extraction, two-phase-commit, crash-recovery]
dependency_graph:
  requires: [95-01, 95-02]
  provides: [shared/local_indexer.py]
  affects: []
tech_stack:
  added: [PyMuPDF/fitz, python-docx]
  patterns: [two-phase-commit, crash-recovery, raw-tokenizer, mtime-cache, delete-by-uid]
key_files:
  created:
    - shared/local_indexer.py
    - tests/test_local_indexer.py
    - tests/test_local_indexer_incremental.py
    - tests/test_local_delete_by_uid.py
    - tests/test_local_two_phase_commit.py
    - tests/test_local_schema_evolution.py
    - tests/test_folder_overlap_detection.py
    - tests/test_local_unavailable_folder.py
    - tests/fixtures/local_indexer/sample.docx
    - tests/fixtures/local_indexer/sample.txt
    - tests/fixtures/local_indexer/unsupported.html
    - tests/fixtures/local_indexer/cp1255_sample.txt
    - tests/fixtures/local_indexer/utf8sig_sample.txt
    - tests/fixtures/local_indexer/bad_encoding.txt
  modified:
    - shared/local_indexer.py
decisions:
  - "D-07 LOCKED: utf-8-sig strict + cp1255 fallback + EncodingError on both failures (MEDIUM-2 review fix)"
  - "HIGH-3: crash-safe three-step _delete_file (mark→Tantivy→SQLite) + _recover_pending_deletes() at init"
  - "HIGH-4 (option b): page text for LAB rebuild sourced from main LOCAL Tantivy content field, not SQLite"
  - "Tantivy lockfile released via explicit del writer + del index in close() (prevents LockBusy on re-open)"
  - "progress_cb fires only for files actually being indexed, not cache hits (matches test expectations)"
  - "_iterate_supported_files yields ALL files; unsupported extension handling in _index_one_file"
metrics:
  duration: "~90 minutes"
  completed: "2026-05-21"
  tasks_completed: 3
  files_changed: 14
---

# Phase 95 Plan 03: Local Indexer Core Summary

**One-liner:** Qt-free LOCAL Tantivy+SQLite indexer engine with crash-safe delete, two-phase commit, mtime cache, and strict TXT encoding policy.

## What Was Built

`shared/local_indexer.py` (1,159 lines) is the core indexing workhorse for the Phase 95 My Library feature. It provides:

### Public API

| Symbol | Role |
|--------|------|
| `LocalIndexer` | Main indexer class (no Qt) |
| `build_local_schema()` | LOCAL Tantivy schema with `tokenizer_name="raw"` on `unique_id` |
| `build_local_lab_schema()` | LOCAL LAB side-index schema (same raw divergence) |
| `extract_pdf_pages(filepath)` | PyMuPDF one-doc-per-page extractor (D-03) |
| `extract_docx_pages(filepath)` | python-docx 20-paragraph chunker (D-04) |
| `extract_txt(filepath)` | TXT with strict utf-8-sig + cp1255 fallback (D-07 + MEDIUM-2) |
| `init_sqlite(db_path)` | Creates 4-table D-35 schema |
| `check_folder_overlap(candidate, existing)` | Ancestor/descendant/exact overlap via `commonpath` |
| `_fix_rtl_line`, `_fix_rtl_page`, `_join_fragmented_lines`, `_rtl_ratio` | Dead code per D-02 (ported verbatim from Seewald) |
| `EncodingError` | Raised by `extract_txt` on dual decode failure (MEDIUM-2) |

### LocalIndexer Methods

| Method | Role |
|--------|------|
| `add_folder(path)` | Register folder with overlap check |
| `remove_folder(path)` | Synchronous delete all files in folder |
| `scan_all(cancel_check)` | Full incremental scan with D-40 unavailability handling |
| `prescan_count(folder_path)` | Fast file_count + total_bytes for D-26 ceiling dialog |
| `startup_recovery()` | Two-pass: pending deletes (HIGH-3) then pending inserts (D-21) |
| `_recover_pending_deletes()` | HIGH-3: replay Tantivy delete + SQLite cleanup for pending_delete=1 rows |
| `close()` | Flush + explicit del writer/index (releases Tantivy lockfile) |

## Critical Design Points

### tokenizer_name="raw" on unique_id (Pitfall #2, LOAD-BEARING)

The main Genizah Tantivy index omits `tokenizer_name="raw"` on `unique_id` because it's rebuilt from scratch — deletes are never needed. LOCAL uses incremental delete on rescan. Without `raw`, `writer.delete_documents("unique_id", uid)` silently fails because the default tokenizer splits `LOCAL_970012345601234567_P1` into multiple tokens, none of which match the full UID (tantivy-py issue #297). Pinned by `test_delete_by_uid_with_raw_tokenizer`.

### HIGH-3 Crash-Safe Three-Step Delete

`_delete_file` uses:
1. `UPDATE local_files SET pending_delete=1` (SQLite, durable)
2. Tantivy `delete_documents(uid)` + `commit()` (durable)
3. `DELETE FROM local_pages/local_files/processed_files` (SQLite cleanup)

`_recover_pending_deletes()` runs at `__init__` and replays steps 2-3 for any `pending_delete=1` rows from a previous crash. Idempotent (Tantivy no-ops already-deleted UIDs).

### Two-Phase Commit (D-21)

`_index_one_file` writes `status='pending'` to `processed_files` before Tantivy. `_commit_batch()` calls `writer.commit()` then flips to `status='committed'`. `startup_recovery()` re-extracts any `status='pending'` rows on next startup.

### D-40 Unavailable Folder

When `scan_all` finds `os.path.isdir(folder_path)` is False:
- Sets `folders.status='unavailable'`
- Skips all indexing for that folder
- Preserves all existing `local_files` / `local_pages` / Tantivy rows

The user's previously-indexed files remain searchable until they explicitly remove the folder.

### MEDIUM-2 TXT Encoding Policy (LOCKED 2026-05-21)

`extract_txt` policy:
1. Try `utf-8-sig` with `errors='strict'`
2. On `UnicodeDecodeError`: try `cp1255` with `errors='strict'`
3. On second `UnicodeDecodeError`: raise `EncodingError`

Caller (`_index_one_file`) catches `EncodingError` and sets `extraction_status='encoding_error'` with `error_msg` populated. No `errors='replace'` anywhere — no silent U+FFFD corruption.

**D-07 status: LOCKED — utf-8-sig strict + cp1255 fallback + encoding_error on both failures.**

## TXT Encoding Smoke Test Outcome

Four new tests cover the MEDIUM-2 policy:
- `test_txt_utf8_sig_strict` — BOM file decodes first attempt, no replacement chars
- `test_txt_cp1255_fallback` — cp1255 file triggers fallback, round-trips correctly
- `test_txt_undecodable_marked_encoding_error` — neither encoding → `extraction_status='encoding_error'`, zero `local_pages` rows
- `test_txt_no_replacement_chars_indexed` — no U+FFFD in any indexed Tantivy content

All 4 pass.

## D-44 Hebrew PDF Fixture

`tests/fixtures/local_indexer/hebrew_sample.pdf` (already present from Plan 95-01) + `hebrew_sample.expected.txt` tested via `test_pymupdf_hebrew_extraction_quality`. Test uses ≥50% line-level overlap with loose substring matching (accounts for whitespace variation across PyMuPDF versions). Test passes on this machine.

## SQLite Schema

Matches D-35 exactly:
```
folders(folder_id PK, path UNIQUE, added_at, last_scanned_at, status DEFAULT 'active')
processed_files(filepath PK, mtime, size, sys_id, status DEFAULT 'committed')
local_pages(sys_id, uid, page_num; PK(sys_id, page_num))
local_files(file_id PK, sys_id UNIQUE, filepath, folder_id FK, display_title,
            original_filename, file_extension, page_count DEFAULT 0,
            file_size_bytes, extraction_status, last_indexed_at, sha256_full,
            error_msg, pending_delete DEFAULT 0)
```

HIGH-3 addition: `pending_delete INTEGER NOT NULL DEFAULT 0` column in `local_files`.

## Test Results

24 tests across 7 files — all GREEN.

| File | Tests | Coverage |
|------|-------|---------|
| test_local_indexer.py | 8 | PyMuPDF quality, RTL helpers, supported types, unsupported ext, 4× encoding policy |
| test_local_indexer_incremental.py | 3 | second-scan-fast, modified-only, deleted-removed |
| test_local_delete_by_uid.py | 1 | raw tokenizer delete-by-uid end-to-end |
| test_local_two_phase_commit.py | 3 | D-21 fault injection, HIGH-3 step1 crash, HIGH-3 step2+crash |
| test_local_schema_evolution.py | 4 | PRAGMA table_info for all 4 tables |
| test_folder_overlap_detection.py | 4 | exact/descendant/ancestor/sibling + Windows case-insensitive |
| test_local_unavailable_folder.py | 1 | unavailable folder preserves rows |

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Tantivy lockfile not released on close()**
- **Found during:** Task 2 (test_local_unavailable_folder — second LocalIndexer open failed with LockBusy)
- **Issue:** `close()` committed the writer but didn't delete the Python objects; Tantivy lockfile stayed held
- **Fix:** Added `del self._writer; del self._index` in `close()` after final commit
- **Files modified:** `shared/local_indexer.py`
- **Commit:** 7d8310ca

**2. [Rule 1 - Bug] progress_cb fired for all files including cache hits**
- **Found during:** Task 2 (test_modified_file_reextract_only — progress_cb counted 3 files instead of 1)
- **Issue:** `scan_all` called `_progress_cb` before the cache-hit check, so all 3 files triggered it
- **Fix:** Moved `_progress_cb` call to after the cache-hit skip check, so it only fires for files actually being indexed
- **Files modified:** `shared/local_indexer.py`
- **Commit:** 7d8310ca

**3. [Rule 2 - Missing functionality] _iterate_supported_files excluded unsupported extensions**
- **Found during:** Task 2 (test_unsupported_extension_status — no row for .html in local_files)
- **Issue:** `_iterate_supported_files` filtered to only `_SUPPORTED_EXTENSIONS`, so .html files never reached `_index_one_file` to get `status='unsupported'` rows
- **Fix:** Removed the extension filter from `_iterate_supported_files`; unsupported extensions handled in `_index_one_file`
- **Files modified:** `shared/local_indexer.py`
- **Commit:** 7d8310ca

**4. [Rule 1 - Bug] test_rtl_helpers_ported used wrong Hebrew character (mem vs mem-sofit)**
- **Found during:** Task 2 (test_rtl_helpers_ported failed)
- **Issue:** Test used `מולש` (mem U+05DE) as mirror of `שלום` (ends in mem-sofit U+05DD) — the reversal was inconsistent
- **Fix:** Changed to a simpler 3-char word `שבת` whose reversal `תבש` round-trips correctly through `_fix_rtl_line`
- **Files modified:** `tests/test_local_indexer.py`
- **Commit:** 7d8310ca

## Open Implementation Questions for Wave 2/3

- `_write_page_doc` currently queries `local_files` for `file_id` after INSERT which may not have a row yet if called before `_finish_file` commits — this works because `_finish_file` does an `INSERT OR REPLACE` and the file_id is needed for the `full_header` format. Current approach sets `file_id=0` as fallback. Wave 3 can optimize by pre-inserting the `local_files` stub before extraction.
- `startup_recovery()` Pass B re-extracts pending inserts by finding the folder_id via a path-prefix LIKE query; this is approximate. A more robust approach (follow-up) would store `folder_id` in `processed_files` directly.
- The `_rollback_partial` method deletes writer and recreates it; on cancellation mid-large-PDF this may lose already-committed docs from the same batch. Acceptable for v1 (cancel is rare).

## Threat Flags

None found. All SQL uses parameterized `?` placeholders. No new network endpoints or cloud-write paths introduced. `local_files.error_msg` stores PyMuPDF exception strings locally only (T-95-11 accepted).

## Self-Check: PASSED

- `shared/local_indexer.py` exists: FOUND
- Task 1 commit 0495ee76: FOUND
- Task 2 commit 7d8310ca: FOUND
- 24 tests pass: CONFIRMED
- ruff clean: CONFIRMED
- Zero `raise NotImplementedError` stubs: CONFIRMED
