---
phase: 97-more-local-features
plan: "02"
subsystem: database
tags: [sqlite, tantivy, zipfile, commit-triggers, zip-bomb, mtime-ns, local-indexer, desktop]

# Dependency graph
requires:
  - phase: 97-more-local-features/97-01
    provides: SQLite migration ladder (user_version=2 with mtime_ns column), scan_runs lifecycle,
              _commit_batch durability bracket, _begin_scan_run/_end_scan_run, _write_page_doc
              with cached_text, atomic Tantivy rebuild

provides:
  - _CommitTriggers class (bytes/count/time thresholds) replacing fixed 25-file batch
  - _check_zip_bomb() pre-check for .docx/.xlsx zip-container size before openpyxl/python-docx
  - XlsxZipBombSuspected exception for Wave C XLSX extractor
  - _MAX_FILE_SIZE / _MAX_UNCOMPRESSED_BYTES / _MAX_CELLS_PER_SHEET / _MAX_CHARS_PER_CHUNK constants
  - _upsert_local_files_status() LD-9 dual-write helper (processed_files + local_files)
  - mtime_ns integer cache-hit comparison (legacy NULL forces remiss)
  - oversized + zip_bomb_suspected status codes in both UI tables
  - desktop/my_library_tab.py status renderers for oversized + zip_bomb_suspected (EN+HE, orange)

affects: [97-03, 97-04, 97-05, 97-06, local_indexer, my_library_tab]

# Tech tracking
tech-stack:
  added: []  # zipfile is stdlib; no new deps required for Wave B
  patterns:
    - _CommitTriggers byte/count/time trigger pattern (no heap-sampling — tantivy-py 0.25.1 has no get_memory_usage)
    - LD-9 dual-write pattern: every skip-status writes to BOTH processed_files AND local_files
    - mtime_ns integer exact comparison replacing float mtime with 0.01s tolerance
    - _check_zip_bomb ZipInfo.file_size sum BEFORE openpyxl/python-docx (T-97B-01)
    - Monkeypatched infolist test fixture (Codex MEDIUM #1 corrected approach)

key-files:
  created:
    - tests/test_commit_triggers.py
    - tests/test_xlsx_extraction.py
    - tests/test_mtime_ns.py
  modified:
    - shared/local_indexer.py
    - desktop/my_library_tab.py

key-decisions:
  - "Heap-sampling branch DROPPED per RESEARCH Issue #1: tantivy-py 0.25.1 has no writer.get_memory_usage(). Commit policy is byte/count/time only. TODO(tantivy >= 0.26) comment preserves the deferred re-add path."
  - "LD-9 dual-write: oversized/zip_bomb_suspected status written to BOTH processed_files.status AND local_files.extraction_status so UI tree (which reads local_files) and Wave D folder counters (which aggregate from local_files) both see the status."
  - "mtime_ns NULL from Phase 95 legacy rows forces cache miss via (cached_row['mtime_ns'] or 0) == mtime_ns — evaluates 0 == current_mtime_ns which is always False for real files."
  - "Zip-bomb test fixture uses monkeypatched zipfile.ZipFile.infolist (Codex MEDIUM #1): ZipInfo.file_size=600MB before writestr() is overwritten by Python's writer. Monkeypatch approach is stable and tests the actual central-directory-sum logic."
  - "mtime_ns increment for test uses +1000ns (1 microsecond) with filesystem-skip guard: NTFS rounds to 100ns intervals so +1ns gets rounded to same slot; +1000ns is safely above the rounding threshold. Test skips if filesystem cannot represent sub-second precision (FAT32)."

patterns-established:
  - "Dual-table status write: any pre-extraction skip status uses _upsert_local_files_status() + processed_files INSERT in same transaction before commit()"
  - "_CommitTriggers: instantiate in __init__, call record_file(fsize) after each file, check should_commit() and reset() in same if block"
  - "mtime_ns cache query: always SELECT pf.mtime_ns alongside pf.mtime; use (cached_row['mtime_ns'] or 0) for NULL-safe comparison"

requirements-completed: [C-02, C-05, D-NEW-8]

# Metrics
duration: ~45min
completed: 2026-05-25
---

# Phase 97 Plan 02: Wave B — Commit Triggers + Zip-Bomb Defense + mtime_ns Summary

**byte/count/time _CommitTriggers replaces fixed 25-file batch; zip-bomb pre-check on .docx/.xlsx via ZipInfo sum before openpyxl; integer mtime_ns cache-hit replacing float tolerance; all skip-statuses dual-written to both processed_files and local_files (LD-9)**

## Performance

- **Duration:** ~45 min
- **Started:** 2026-05-25
- **Completed:** 2026-05-25
- **Tasks:** 2 (TDD: RED stubs -> GREEN implementation)
- **Files modified:** 5

## Accomplishments

- `_CommitTriggers` class with BYTES_THRESHOLD=200MB / FILES_THRESHOLD=100 / SECONDS_THRESHOLD=60s. Replaces Phase 95's fixed 25-file batch in `scan_all`. Heap-sampling branch absent per RESEARCH Issue #1 (tantivy-py 0.25.1 has no `writer.get_memory_usage()`). `_COMMIT_BATCH_SIZE=25` kept as dead-code constant per D-02.
- `_check_zip_bomb()` iterates `zipfile.ZipInfo.file_size` sum BEFORE handing file to openpyxl/python-docx. Rejects files claiming > 500 MB uncompressed in < 1 ms without allocating decompression memory. Applied only to `.docx` / `.xlsx` (zip-container formats).
- `_upsert_local_files_status()` LD-9 dual-write helper: `oversized` and `zip_bomb_suspected` statuses written to BOTH `processed_files.status` AND `local_files.extraction_status`. Wave D folder counters aggregate from `local_files.extraction_status`; writing only `processed_files` would leave UI blind.
- `mtime_ns INTEGER` cache-hit comparison: `(cached_row["mtime_ns"] or 0) == mtime_ns` replaces `abs(float_mtime - mtime) < 0.01`. Legacy Phase 95 rows with `mtime_ns=NULL` force a cache miss on first Phase 97 scan, then get backfilled.
- `desktop/my_library_tab.py` status renderers: `oversized` -> "Too large (>100 MB)" / "גדול מדי (>100 מ\"ב)", `zip_bomb_suspected` -> "Suspicious archive" / "ארכיון חשוד" — both with orange foreground (#e67e22). Handles both tree-populate path and live `update_file_status` callback path.

## Task Commits

Each task was committed atomically:

1. **Task 1: Wave 0 RED test stubs (3 test files)** - `f3856119` (test)
2. **Task 2: GREEN — _CommitTriggers + C-05 constants + zip-bomb + mtime_ns + LD-9** - `419d8413` (feat)

## Files Created/Modified

- `shared/local_indexer.py` — Added _CommitTriggers class, _check_zip_bomb(), XlsxZipBombSuspected, C-05 constants, _upsert_local_files_status(), mtime_ns in stat block + cache comparison + INSERT, _commit_triggers instance in __init__, oversized/zip_bomb_suspected status writes (LD-9 dual)
- `desktop/my_library_tab.py` — Status renderers for 'oversized' and 'zip_bomb_suspected' in both tree-populate path and update_file_status callback
- `tests/test_commit_triggers.py` — 5 tests: bytes threshold, file count, seconds elapsed, reset, AST guard for heap-sampling absence
- `tests/test_xlsx_extraction.py` — 1 test: zip-bomb defense via monkeypatched infolist (Codex MEDIUM #1 corrected fixture)
- `tests/test_mtime_ns.py` — 2 tests: mtime_ns cache-hit comparison, legacy NULL mtime_ns forced remiss

## Decisions Made

1. **Heap-sampling branch dropped entirely**: RESEARCH Issue #1 confirmed tantivy-py 0.25.1 has no `writer.get_memory_usage()`. The `heap_size=` argument to `index.writer()` is a memory ceiling but not a commit trigger. Commit policy is byte/count/time only. `TODO(tantivy >= 0.26)` comment preserves deferred re-add path when the API exists.

2. **Zip-bomb test fixture — monkeypatched infolist**: Codex MEDIUM #1 flagged the prior approach (`ZipInfo.file_size = 600MB` before `writestr()`) as broken — Python's zip writer overwrites `file_size` with actual byte length at write time. Monkeypatching `zipfile.ZipFile.infolist` to return a synthesized `SimpleNamespace(file_size=600MB)` correctly tests the central-directory sum logic without binary fixture files.

3. **mtime_ns test uses +1000ns with filesystem skip guard**: NTFS rounds timestamps to 100ns intervals — requesting `mtime_ns + 1` yields the same value. Test uses `+1_000ns` (1 microsecond) and skips if `stat_after.st_mtime_ns == stat_before.st_mtime_ns` (FAT32 or other low-precision filesystems).

4. **_upsert_local_files_status uses ON CONFLICT DO UPDATE**: The `local_files` table has a `UNIQUE` constraint on `sys_id`. For oversized/zip_bomb_suspected files (where `_index_one_file` is never called and thus the pre-INSERT never runs), we UPSERT so existing rows are updated rather than inserting a duplicate.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] mtime_ns +1ns test rounded to same NTFS slot**
- **Found during:** Task 2 (test_cache_hit_uses_mtime_ns)
- **Issue:** NTFS timestamps have 100ns precision — requesting `mtime_ns + 1` yields the same rounded value; the assertion `stat_after.st_mtime_ns == new_mtime_ns` failed.
- **Fix:** Changed increment from +1ns to +1_000ns (1 microsecond), added filesystem skip guard that calls `pytest.skip()` when the filesystem cannot represent the new value (FAT32). Changed final assertion to use `stat_after.st_mtime_ns` (actual filesystem value) instead of the requested `new_mtime_ns`.
- **Files modified:** `tests/test_mtime_ns.py`
- **Committed in:** `419d8413` (Task 2)

---

**Total deviations:** 1 auto-fixed (Rule 1 bug — filesystem precision)
**Impact on plan:** Fix essential for test correctness on Windows/NTFS. The D-NEW-8 logic itself is correct — the test just needed to respect NTFS precision granularity.

## Issues Encountered

- NTFS filesystem timestamp rounding discovered during test run. The +1000ns solution is robust across NTFS (100ns precision), ext4 (1ns precision), and APFS (1ns precision). FAT32 (2s precision) silently skips via the guard.

## Known Stubs

None. All Wave B functionality is fully implemented. Wave C constants (`_MAX_CELLS_PER_SHEET`, `_MAX_CHARS_PER_CHUNK`) are defined here as forward-declarations for the XLSX extractor that lands in plan 97-03; they are not stubs — they are design constraints consumed by the next plan.

## Threat Flags

No new network endpoints, auth paths, or cross-trust-boundary surfaces introduced. All changes are local filesystem + SQLite + zipfile stdlib. T-97B-01 through T-97B-06 mitigations all implemented as specified in the plan threat register:
- T-97B-01 (zip-bomb DoS): mitigated — `_check_zip_bomb()` gates BEFORE openpyxl
- T-97B-02 (uncapped batch DoS): mitigated — `_CommitTriggers` fires on bytes/files/time
- T-97B-03 (mtime_ns cache tamper): mitigated — exact int comparison; NULL forces remiss
- T-97B-06 (UI blind to statuses): mitigated — LD-9 dual-write to both tables

## Self-Check: PASSED

- `tests/test_commit_triggers.py` FOUND
- `tests/test_xlsx_extraction.py` FOUND
- `tests/test_mtime_ns.py` FOUND
- `shared/local_indexer.py` FOUND (contains _CommitTriggers, _check_zip_bomb, _upsert_local_files_status, mtime_ns)
- `desktop/my_library_tab.py` FOUND (contains zip_bomb_suspected, oversized status renderers)
- Task commits FOUND: `f3856119` (RED stubs), `419d8413` (GREEN implementation)
- 8 plan-specific tests: 8 passed
- ruff check: clean

---
*Phase: 97-more-local-features*
*Completed: 2026-05-25*
