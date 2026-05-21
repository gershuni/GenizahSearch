---
phase: 95
plan: 03
type: execute
wave: 1
depends_on: [01, 02]
files_modified:
  - shared/local_indexer.py
  - tests/test_local_indexer.py
  - tests/test_local_indexer_incremental.py
  - tests/test_local_delete_by_uid.py
  - tests/test_local_two_phase_commit.py
  - tests/test_local_schema_evolution.py
  - tests/test_folder_overlap_detection.py
  - tests/test_local_unavailable_folder.py
autonomous: true
requirements: [REQ-1, REQ-3, REQ-4, REQ-5]
must_haves:
  truths:
    - "PyMuPDF extracts Hebrew PDF text via get_text('blocks') and the D-44 fixture asserts expected reading order"
    - "Indexer handles .docx, .pdf, .txt and yields status='unsupported_extension' for other extensions"
    - "Second scan of unchanged 100-file folder completes in ≤ 5% of first scan wall time"
    - "Modifying one file's mtime re-extracts only that file"
    - "Deleting a file removes its rows from both SQLite cache AND Tantivy side-index"
    - "Tantivy delete-by-uid works because unique_id field uses tokenizer_name='raw' (LOCAL schema divergence from main)"
    - "Two-phase commit recovers from crash between Tantivy commit and SQLite UPDATE"
    - "LOCAL Tantivy schema mirrors main index field set EXCEPT unique_id uses raw tokenizer"
    - "RTL helpers (_fix_rtl_line, _fix_rtl_page, _join_fragmented_lines) ported VERBATIM as dead code (D-02)"
    - "Folder overlap detection uses os.path.commonpath after _canonical_filepath normalization"
    - "Unavailable folder marked status='unavailable' on auto-rescan; rows preserved"
    - "HIGH-3 review fix: _delete_file uses crash-safe THREE-STEP protocol (mark pending_delete=1 in SQLite → Tantivy delete + commit → SQLite final DELETE). The old 'SQLite DELETE then Tantivy' ordering is REMOVED."
    - "HIGH-3 review fix: _recover_pending_deletes() runs at LocalIndexer init; replays steps 2-3 for any rows left with pending_delete=1 from a previous crash."
    - "HIGH-3 review fix: local_files table has a `pending_delete INTEGER NOT NULL DEFAULT 0` column (D-21 commitment now actually implemented per HIGH-3 review)."
    - "HIGH-4 review fix (option b LOCKED): page text for LAB rebuild is read from main LOCAL Tantivy `content` stored field by UID — NOT from SQLite (this plan's schema stores metadata only) and NOT by re-extracting from source files. See Plan 06 for the reader-side implementation."
    - "MEDIUM-2 review fix: TXT extraction tries utf-8-sig with errors=strict; on UnicodeDecodeError, attempts cp1255 (legacy Windows Hebrew); on both failures, the file is marked extraction_status='encoding_error' and NO replacement characters are indexed."
  artifacts:
    - path: "shared/local_indexer.py"
      provides: "LocalIndexer class, build_local_schema(), extract_pdf_pages, extract_docx_pages, extract_txt, _fix_rtl_line (dead code), folder enumeration, mtime cache, two-phase commit, delete-by-uid"
      contains: "build_local_schema"
      min_lines: 400
  key_links:
    - from: "shared/local_indexer.py"
      to: "shared/local_sys_id.py"
      via: "imports generate_local_sys_id, _canonical_filepath"
      pattern: "from shared.local_sys_id import"
    - from: "shared/local_indexer.py:build_local_schema()"
      to: "Tantivy delete-by-term"
      via: "tokenizer_name='raw' on unique_id field"
      pattern: 'tokenizer_name="raw"'
    - from: "shared/local_indexer.py:LocalIndexer.commit_batch"
      to: "SQLite processed_files + local_pages + local_files tables"
      via: "two-phase commit per D-21"
      pattern: "status.*pending"
---

<objective>

**Content source for LAB rebuild (HIGH-4 review fix, option b LOCKED):** Page text for the LAB side-index (Plan 06) is sourced by reading the `content` stored field from the main LOCAL Tantivy side-index, keyed by `unique_id`. This plan's SQLite schema (`local_files` / `local_pages`) stores metadata + UID tracking only — NOT page text. Rationale documented in this objective and mirrored in Plan 06's objective. Choice rationale: option (b) survives source-file deletion + D-40 folder-unavailability (the LAB index stays consistent with the main LOCAL index, which is the right semantic per D-40); option (a) breaks on missing source files; option (c) doubles storage cost. The main LOCAL schema already has `content` with `stored=True` (95-RESEARCH.md line 439 + this plan's interfaces block), so this option requires zero schema additions.

Build the Qt-free indexer engine in `shared/local_indexer.py`: PyMuPDF page extraction, python-docx 20-paragraph chunking, TXT reading, RTL helpers as dead code, LOCAL Tantivy + LAB Tantivy schema builders (with `tokenizer_name="raw"` on `unique_id`), SQLite cache (`folders`, `processed_files`, `local_pages`, `local_files` per D-35), folder enumeration with overlap detection (D-17 + D-42), incremental re-extract, crash-safe delete with `pending_delete` two-phase protocol (HIGH-3 review fix — D-21 commitment now implemented), two-phase commit (D-21), and the cancellation hooks (cooperative flag — Qt-side wrapper lives in Plan 07).

Purpose: This is the workhorse. Without it, nothing indexes; without correct `tokenizer_name="raw"`, every modify silently doubles row count (Pitfall #2 — load-bearing); without two-phase commit, a crash mid-batch corrupts state (Pitfall #6). Wave 2 (search merger) reads this module's output; Wave 3 (MyLibraryTab) wraps it in a QThread.

Output: One module + 7 GREEN test files. Module is library-only (no CLI, no Qt). All RTL helpers ported verbatim but NEVER invoked in v1 (D-02 contract).
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/PROJECT.md
@.planning/phases/95-my-library/95-CONTEXT.md
@.planning/phases/95-my-library/95-PATTERNS.md
@.planning/phases/95-my-library/95-VALIDATION.md
@seewald_addition/genizah_local_indexer.py
@seewald_addition/genizah_make_index.py
@shared/local_sys_id.py
@genizah_core.py

<interfaces>
<!-- Schema templates the indexer must mirror -->

From genizah_core.py:5124-5136 (main Tantivy schema — LOCAL mirrors this):
```python
builder = tantivy.SchemaBuilder()
builder.add_text_field("unique_id", stored=True)  # ← LOCAL DIVERGES: uses tokenizer_name="raw"
builder.add_text_field("content", stored=True, tokenizer_name="whitespace")
builder.add_text_field("content_head", stored=False, tokenizer_name="whitespace")
builder.add_text_field("content_tail", stored=False, tokenizer_name="whitespace")
builder.add_text_field("line_starts", stored=False, tokenizer_name="whitespace")
builder.add_text_field("line_ends", stored=False, tokenizer_name="whitespace")
builder.add_text_field("source", stored=True)
builder.add_text_field("full_header", stored=True)
builder.add_text_field("shelfmark", stored=True)
builder.add_text_field("scope", stored=True)
builder.add_text_field("boundaries", stored=True)
schema = builder.build()
```

From genizah_core.py:763-777 (LAB schema — LOCAL LAB mirrors this):
```python
builder = tantivy.SchemaBuilder()
builder.add_text_field("unique_id", stored=True)  # ← LOCAL LAB DIVERGES: tokenizer_name="raw"
builder.add_text_field("text_normalized", stored=True, tokenizer_name="simple")
builder.add_text_field("text_ngram", stored=False, tokenizer_name="whitespace")
builder.add_text_field(self.LAB_FINGERPRINT_FIELD, stored=False, tokenizer_name="simple")
builder.add_text_field("fingerprint_dyn", stored=False, tokenizer_name="simple")
builder.add_text_field("full_header", stored=True)
builder.add_text_field("shelfmark", stored=True)
builder.add_text_field("source", stored=True)
builder.add_text_field("content", stored=True, tokenizer_name="simple")
schema = builder.build()
```

From seewald_addition/genizah_make_index.py:67-105 (RTL helpers — port VERBATIM as dead code per D-02):
- `_rtl_ratio(text)` — fraction of RTL chars among alpha chars (uses `unicodedata.bidirectional`)
- `_fix_rtl_line(line)` — reverse mirror-reversed RTL line if ratio > 0.4
- `_fix_rtl_page(text)` — apply per-line + re-glue punctuation
- `_join_fragmented_lines(text)` — join when single-word-per-line ratio > 0.60

D-34 LOCAL formats (from CONTEXT):
- `unique_id` = `f"LOCAL_{sys_id}_P{page_num}"` (length ≤ 40)
- `full_header` = `f"{sys_id}_LOCAL_P{page_num}_F{file_id:04d}"`

D-35 SQLite schema:
```sql
CREATE TABLE folders(folder_id INTEGER PRIMARY KEY AUTOINCREMENT, path TEXT UNIQUE NOT NULL,
                     added_at REAL, last_scanned_at REAL, status TEXT);
CREATE TABLE processed_files(filepath TEXT PRIMARY KEY, mtime REAL, size INTEGER, sys_id TEXT,
                             status TEXT NOT NULL DEFAULT 'committed');
CREATE TABLE local_pages(sys_id TEXT, uid TEXT, page_num INTEGER, PRIMARY KEY(sys_id, page_num));
CREATE TABLE local_files(
    file_id INTEGER PRIMARY KEY AUTOINCREMENT,
    sys_id TEXT NOT NULL UNIQUE,
    filepath TEXT NOT NULL,
    folder_id INTEGER NOT NULL REFERENCES folders(folder_id),
    display_title TEXT,
    original_filename TEXT NOT NULL,
    file_extension TEXT NOT NULL,
    page_count INTEGER NOT NULL DEFAULT 0,
    file_size_bytes INTEGER NOT NULL,
    extraction_status TEXT NOT NULL,
    last_indexed_at REAL NOT NULL,
    sha256_full TEXT,
    error_msg TEXT,
    -- HIGH-3 review fix: crash-safe delete via pending_delete two-phase protocol.
    -- 0 = healthy row; 1 = marked for deletion (Tantivy delete pending or not yet confirmed).
    -- Startup recovery scans rows WHERE pending_delete=1 and replays the Tantivy delete + SQLite DELETE.
    pending_delete INTEGER NOT NULL DEFAULT 0
);
```
</interfaces>
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: Implement shared/local_indexer.py core — schema, extraction, SQLite (REQ-1, REQ-3, REQ-4)</name>
  <read_first>
    - seewald_addition/genizah_local_indexer.py (port source for SQLite cache pattern — ~1424 lines)
    - seewald_addition/genizah_make_index.py:60-105 (RTL helpers — port verbatim as DEAD CODE)
    - genizah_core.py:5118-5189 (main Tantivy schema — mirror, with divergence on unique_id)
    - genizah_core.py:742-790 (LAB schema — mirror for LOCAL LAB)
    - .planning/phases/95-my-library/95-PATTERNS.md ("shared/local_indexer.py" sections — "Mirror this" + 9 "Divergences" + "shared/local_indexer.py — LAB side-index builder (D-09)")
    - .planning/phases/95-my-library/95-CONTEXT.md (D-01..D-07, D-34..D-36, D-42)
    - shared/local_sys_id.py (helper imports)
  </read_first>
  <behavior>
    Module must expose (public API):
    - `class LocalIndexer` — owns the indexing pipeline; constructor takes `index_dir, lab_index_dir, db_path, progress_cb=None, file_finished_cb=None`.
    - `LocalIndexer.add_folder(path: str) -> bool` — normalizes, checks overlap, INSERT into `folders`, returns True if added.
    - `LocalIndexer.remove_folder(path: str) -> int` — synchronous delete; returns # of files removed.
    - `LocalIndexer.scan_all(cancel_check=lambda: False) -> dict` — runs through all registered folders; returns `{"indexed": N, "skipped": N, "errors": N, "cancelled": bool}`.
    - `LocalIndexer.prescan_count(folder_path: str) -> (file_count, total_bytes)` — fast count for D-26 ceiling dialog.
    - `LocalIndexer.startup_recovery() -> dict` — at app start: HIGH-3 review fix — TWO-PASS recovery. Pass A: call `_recover_pending_deletes()` first (finishes any crash-interrupted deletes). Pass B: find `processed_files.status='pending'` rows, re-extract. Returns `{'pending_deletes_recovered': N, 'pending_inserts_recovered': M}`.
    - `LocalIndexer._recover_pending_deletes() -> int` — HIGH-3 review fix — NEW startup recovery for crash-interrupted deletes. SELECTs `local_files` rows WHERE `pending_delete=1`; for each: replays Tantivy delete-by-uid + SQLite final cleanup (idempotent). Returns count of completed deletes. Called from `__init__` AND from `startup_recovery()` Pass A.
    - `LocalIndexer.close() -> None` — flush + close writer + close SQLite connection.

    Pure helpers (module level, importable by tests):
    - `build_local_schema() -> tantivy.Schema` — main side-index schema.
    - `build_local_lab_schema() -> tantivy.Schema` — LAB side-index schema (still has the raw tokenizer divergence).
    - `extract_pdf_pages(filepath: str) -> Iterator[(page_num, text, title)]` — PyMuPDF `get_text("blocks")` per D-03.
    - `extract_docx_pages(filepath: str) -> Iterator[(chunk_num, text, title)]` — 20-paragraph chunks per D-04.
    - `extract_txt(filepath: str) -> Iterator[(1, text, basename)]` — single-page; `utf-8-sig` encoding per D-07.
    - `_fix_rtl_line(line)`, `_fix_rtl_page(text)`, `_join_fragmented_lines(text)`, `_rtl_ratio(text)` — DEAD CODE per D-02; ported verbatim from seewald_addition, but NEVER called in this module's runtime path.
    - `init_sqlite(db_path: str) -> sqlite3.Connection` — creates `folders`, `processed_files`, `local_pages`, `local_files` tables per D-35.
    - `check_folder_overlap(candidate: str, existing_paths: list[str]) -> str | None` — returns the conflicting path if overlap, else None. Uses `_canonical_filepath` + `os.path.commonpath`.

    Test behaviors:
    - `test_pymupdf_hebrew_extraction_quality` (D-44) — opens `tests/fixtures/local_indexer/hebrew_sample.pdf`, iterates pages, joins text. Compares against `hebrew_sample.expected.txt`. PASS condition: ≥ 80% line-level overlap with expected (loose match accounts for whitespace differences).
    - `test_rtl_helpers_ported` — imports `_fix_rtl_line` and applies to a known mirror-reversed Hebrew string; asserts the corrected output matches a hand-fixed reference. (Tests the dead code — D-02 contract.)
    - `test_supported_file_types_docx_pdf_txt` — runs indexer on fixtures dir, asserts 3 files indexed, 1 (.html) gets `status="unsupported_extension"` row.
    - `test_unsupported_extension_status` — calls indexer on a single `.html` file; asserts `local_files.extraction_status == "unsupported"`.
    - `test_second_scan_fast` — 100-file fixture; first scan timed; second scan must complete in ≤ 5% of first time (or ≤ 0.5s, whichever is greater — CI variance).
    - `test_modified_file_reextract_only` — touch one file (`os.utime`); rescan; assert only that one file's extractor was called (via `progress_cb` mock).
    - `test_deleted_file_removed` — delete one file; rescan; assert `local_files` row gone AND `writer.delete_documents` was called for each of that file's `local_pages` UIDs.
    - `test_delete_by_uid_with_raw_tokenizer` — insert doc with `unique_id="LOCAL_970012345601234567_P1"`; call `writer.delete_documents(Term("unique_id", uid))`; commit; searcher returns 0 hits. (Without raw tokenizer this would silently fail per Pitfall #2 / tantivy-py #297.)
    - `test_crash_between_tantivy_and_sqlite_recovers` — Fault inject: kill process between Tantivy commit + SQLite UPDATE. Restart indexer. Assert `startup_recovery()` re-extracts the pending files. (Two-phase commit per D-21.)
    - `test_crash_between_pending_delete_and_tantivy_commit_recovers` — **HIGH-3 review fix — NEW test.** Set up an indexed file (its sys_id has rows in `local_files`, `local_pages`, and Tantivy docs). Manually simulate step 1 of the new `_delete_file`: `UPDATE local_files SET pending_delete=1 WHERE sys_id=?` (commit). Do NOT proceed to step 2 (Tantivy delete). Close + reopen the LocalIndexer. Assert: (a) the searcher returns ZERO hits for that sys_id's UIDs (recovery completed step 2). (b) The `local_files` / `local_pages` / `processed_files` rows for that sys_id are GONE (recovery completed step 3). (c) `_recover_pending_deletes` logged a count of 1.
    - `test_crash_between_tantivy_commit_and_sqlite_final_delete_recovers` — **HIGH-3 review fix — NEW test.** Similar setup, but simulate steps 1+2 completed (`pending_delete=1` flag set AND Tantivy delete-by-uid committed); only step 3 missing. Reopen LocalIndexer. Recovery re-runs step 2 (idempotent — Tantivy ignores already-deleted UIDs) and completes step 3. Final state: no Tantivy hits, no SQLite rows.
    - `test_folders_table_schema`, `test_local_files_table_schema`, `test_local_pages_table_schema`, `test_processed_files_table_schema` — introspect via `PRAGMA table_info(...)`; assert exact column names and types per D-35.
    - `test_overlap_via_commonpath` — register `/a/b`; try `/a/b/c` (descendant — REJECT), `/a` (ancestor — REJECT), `/a/b` (exact — REJECT), `/a/b2` (sibling — ACCEPT).
    - `test_unavailable_folder_marked_status_unavailable` — register folder; delete the actual filesystem folder; trigger `scan_all`; assert folder's `status` column is `unavailable` AND existing `local_files` rows are PRESERVED (not deleted).
  </behavior>
  <action>
    Create `shared/local_indexer.py` per PATTERNS.md "Mirror this" + "Divergences" guidance. Key implementation details:

    **1. Imports + module docstring:**
    ```python
    # -*- coding: utf-8 -*-
    """Phase 95 LOCAL indexer (REQ-1, REQ-3, REQ-4, REQ-5, D-01..D-44).

    Qt-free. Owns: PyMuPDF/python-docx/TXT extraction, LOCAL Tantivy side-index
    + LOCAL LAB side-index builders, SQLite cache (folders / processed_files /
    local_pages / local_files), incremental re-extract, delete-by-uid, two-phase
    commit with crash recovery.

    Critical divergences from main Tantivy index (CONTEXT D-08 / D-13 / D-20 / D-21
    + RESEARCH Pitfall #2):
      - unique_id field uses tokenizer_name="raw" (main index omits this kwarg).
        Required because LOCAL deletes-by-term on rescan; main never deletes.
      - One Tantivy doc per PDF page (D-03), 20-paragraph chunk for DOCX (D-04).
      - LOCAL hits never write Transcriptions.txt / libraries.csv / browse_map.pkl
        (Seewald's prototype patched the shared corpus — we don't; SPEC Constraint #6).

    Per CONTEXT D-02: _fix_rtl_line, _fix_rtl_page, _join_fragmented_lines are
    PORTED VERBATIM from seewald_addition/genizah_make_index.py:67-105 but NEVER
    invoked at runtime. They exist as a regression-prevention contract for the
    future fallback path (pdfplumber/pypdf). Tests in tests/test_local_indexer.py
    exercise the helpers in isolation.
    """
    from __future__ import annotations

    import hashlib
    import logging
    import os
    import re
    import sqlite3
    import time
    import unicodedata
    from contextlib import contextmanager
    from pathlib import Path
    from typing import Callable, Iterator, Optional

    import fitz  # PyMuPDF — D-01
    from docx import Document as _DocxDoc  # python-docx — D-04
    import tantivy

    from shared.local_sys_id import (
        is_local_sys_id,
        generate_local_sys_id,
        _canonical_filepath,
    )

    logger = logging.getLogger(__name__)

    # Constants
    _SUPPORTED_EXTENSIONS = {".docx", ".pdf", ".txt"}
    _DOCX_CHUNK_PARAGRAPHS = 20            # D-04
    _SCANNED_PDF_CHAR_THRESHOLD = 50       # D-05
    _EMPTY_PAGE_CHAR_THRESHOLD = 10        # D-06
    _COMMIT_BATCH_SIZE = 25                # D-21
    _MAX_COLLISION_RETRIES = 4             # D-19
    _UNIQUE_ID_PREFIX = "LOCAL"            # D-34
    _DEFAULT_TXT_ENCODING = "utf-8-sig"    # D-07 starting policy
    ```

    **2. RTL helpers (port verbatim from seewald_addition/genizah_make_index.py:67-105):**
    Copy `_rtl_ratio`, `_fix_rtl_line`, `_fix_rtl_page`, `_join_fragmented_lines` exactly. Add docstring noting they are DEAD CODE per D-02 and `# noqa: F841` (or simply unused) — confirm via `# pragma: no cover` marker if appropriate.

    **3. Schema builders:**
    ```python
    def build_local_schema():
        """LOCAL Tantivy side-index schema (REQ-3).

        CRITICAL DIVERGENCE: unique_id uses tokenizer_name="raw" (main index at
        genizah_core.py:5125 omits this kwarg). Required for delete_documents()
        to work on rescan — RESEARCH Pitfall #2 / tantivy-py #297.
        """
        builder = tantivy.SchemaBuilder()
        # ⚠ tokenizer_name="raw" is LOAD-BEARING — see Pitfall #2.
        builder.add_text_field("unique_id", stored=True, tokenizer_name="raw")
        builder.add_text_field("content", stored=True, tokenizer_name="whitespace")
        builder.add_text_field("content_head", stored=False, tokenizer_name="whitespace")
        builder.add_text_field("content_tail", stored=False, tokenizer_name="whitespace")
        builder.add_text_field("line_starts", stored=False, tokenizer_name="whitespace")
        builder.add_text_field("line_ends", stored=False, tokenizer_name="whitespace")
        builder.add_text_field("source", stored=True)
        builder.add_text_field("full_header", stored=True)
        builder.add_text_field("shelfmark", stored=True)
        builder.add_text_field("scope", stored=True)
        builder.add_text_field("boundaries", stored=True)
        return builder.build()


    def build_local_lab_schema():
        """LOCAL LAB side-index schema (D-09).

        Same raw-tokenizer divergence on unique_id. fingerprint_dyn computed at
        build time using current LAB dynamic_rank_map.
        """
        builder = tantivy.SchemaBuilder()
        builder.add_text_field("unique_id", stored=True, tokenizer_name="raw")  # ⚠ Pitfall #2
        builder.add_text_field("text_normalized", stored=True, tokenizer_name="simple")
        builder.add_text_field("text_ngram", stored=False, tokenizer_name="whitespace")
        # NOTE: main LAB uses self.LAB_FINGERPRINT_FIELD; we mirror as "fingerprint".
        builder.add_text_field("fingerprint", stored=False, tokenizer_name="simple")
        builder.add_text_field("fingerprint_dyn", stored=False, tokenizer_name="simple")
        builder.add_text_field("full_header", stored=True)
        builder.add_text_field("shelfmark", stored=True)
        builder.add_text_field("source", stored=True)
        builder.add_text_field("content", stored=True, tokenizer_name="simple")
        return builder.build()
    ```

    **4. Per-format extractors (RESEARCH.md Code Examples + D-03/D-04/D-07):**
    - `extract_pdf_pages(filepath)` — `fitz.open` + iterate pages + `page.get_text("blocks")` + filter `b[6] == 0` text blocks. Yield `(page_num, text, title)` per page. D-06: skip pages with < 10 chars. D-05: if total chars across all pages < 50, raise/signal "no_text_layer".
    - `extract_docx_pages(filepath)` — `_DocxDoc(filepath)` + iterate `doc.paragraphs` + chunk every 20. Yield `(chunk_num, chunk_text, doc.core_properties.title or basename)`.
    - `extract_txt(filepath)` — read with `utf-8-sig` (fall back to `cp1255` if decode fails AND log; D-07 open decision). Yield single `(1, text, basename)`.

    **5. SQLite schema init (D-35):**
    ```python
    def init_sqlite(db_path: str) -> sqlite3.Connection:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA journal_mode=WAL")  # Pitfall #6 mitigation
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS folders (
                folder_id        INTEGER PRIMARY KEY AUTOINCREMENT,
                path             TEXT    UNIQUE NOT NULL,
                added_at         REAL,
                last_scanned_at  REAL,
                status           TEXT    NOT NULL DEFAULT 'active'
            );
            CREATE TABLE IF NOT EXISTS processed_files (
                filepath  TEXT    PRIMARY KEY,
                mtime     REAL,
                size      INTEGER,
                sys_id    TEXT,
                status    TEXT    NOT NULL DEFAULT 'committed'
            );
            CREATE TABLE IF NOT EXISTS local_pages (
                sys_id    TEXT    NOT NULL,
                uid       TEXT    NOT NULL,
                page_num  INTEGER NOT NULL,
                PRIMARY KEY (sys_id, page_num)
            );
            CREATE TABLE IF NOT EXISTS local_files (
                file_id           INTEGER PRIMARY KEY AUTOINCREMENT,
                sys_id            TEXT    NOT NULL UNIQUE,
                filepath          TEXT    NOT NULL,
                folder_id         INTEGER NOT NULL REFERENCES folders(folder_id),
                display_title     TEXT,
                original_filename TEXT    NOT NULL,
                file_extension    TEXT    NOT NULL,
                page_count        INTEGER NOT NULL DEFAULT 0,
                file_size_bytes   INTEGER NOT NULL,
                extraction_status TEXT    NOT NULL,
                last_indexed_at   REAL    NOT NULL,
                sha256_full       TEXT,
                error_msg         TEXT,
                -- HIGH-3 review fix: crash-safe delete via pending_delete two-phase protocol.
                pending_delete    INTEGER NOT NULL DEFAULT 0
            );
        """)
        conn.commit()
        return conn
    ```

    **6. Folder overlap detection (D-17 + D-42):**
    ```python
    def check_folder_overlap(candidate: str, existing_paths: list[str]) -> Optional[str]:
        """Return the conflicting existing path if candidate overlaps, else None."""
        cand = _canonical_filepath(candidate)
        for existing in existing_paths:
            exist = _canonical_filepath(existing)
            if cand == exist:
                return existing
            try:
                common = os.path.normcase(os.path.commonpath([cand, exist]))
            except ValueError:
                continue  # different drives
            if common == cand or common == exist:
                return existing
        return None
    ```

    **7. LocalIndexer class — core methods:**
    - `add_folder(path)` — normalize, overlap check (raise/return False on conflict with explanatory error), INSERT into `folders`.
    - `_iterate_supported_files(folder_path, cancel_check)` — `os.walk(followlinks=False)` per D-26 hardening; try/except OSError per directory; yield `(filepath, file_size_bytes)`; check cancel between directories.
    - `_index_one_file(filepath, folder_id, cancel_check)` — compute canonical, generate sys_id (with collision retry up to 4 slots), extract, write docs to Tantivy writer, INSERT/UPDATE `local_files` + `local_pages` + `processed_files` with `status='pending'`, NO commit yet (commit happens at batch boundary).
    - `_commit_batch()` — Two-phase commit per D-21: (1) Tantivy `writer.commit()`, (2) SQLite `UPDATE processed_files SET status='committed' WHERE filepath IN (...)`.
    - `_delete_file(sys_id, filepath)` — **HIGH-3 review fix: crash-safe three-step protocol replaces the old "delete SQLite then Tantivy" ordering.** The new sequence:
        1. **Mark**: BEGIN TRANSACTION → `UPDATE local_files SET pending_delete=1 WHERE sys_id=?` → COMMIT (SQLite is now durably aware that this sys_id is being deleted; if we crash here, `_recover_pending_deletes()` finishes the job at next startup).
        2. **Tantivy delete**: SELECT all uids FROM `local_pages` WHERE `sys_id=?`; for each uid call `writer.delete_documents(Term("unique_id", uid))`; `writer.commit()` (durable on disk).
        3. **Final SQLite cleanup**: BEGIN TRANSACTION → DELETE FROM `local_pages` WHERE sys_id=?; DELETE FROM `local_files` WHERE sys_id=?; DELETE FROM `processed_files` WHERE sys_id=? → COMMIT. (Only runs after Tantivy commit succeeded.)
      If a crash occurs between step 1 and step 2: `_recover_pending_deletes()` replays steps 2 and 3.
      If a crash occurs between step 2 and step 3: `_recover_pending_deletes()` re-runs step 2 (idempotent — deleting an already-deleted UID is a Tantivy no-op) and then step 3.
      The OLD ordering (SQLite DELETE before Tantivy commit, per the original Plan 03 lines 377-378 the reviewer flagged) is REMOVED.
    - `_recover_pending_deletes()` — **HIGH-3 review fix: NEW startup recovery method.** Called from `LocalIndexer.__init__` (or via explicit `startup_recovery()` per D-21). Body:
        1. SELECT `sys_id` FROM `local_files` WHERE `pending_delete=1`.
        2. For each pending sys_id: run step 2 (Tantivy delete-by-uid loop + commit) and step 3 (SQLite final cleanup) from `_delete_file`. Idempotent.
        3. Log the count: `logger.info("HIGH-3 recovery: completed %d pending deletes", n)`.
      Pin via `tests/test_local_two_phase_commit.py::test_crash_between_pending_delete_and_tantivy_commit_recovers`.
    - `remove_folder(folder_path)` — synchronous: SELECT sys_ids in folder, loop `_delete_file`, commit, DELETE folder row.
    - `scan_all(cancel_check)` — for each `folders` row with `status='active'`: check `os.path.isdir(path)`; if False, UPDATE `status='unavailable'` and continue; if True, iterate files, diff against `processed_files`, route to `_index_one_file` / `_delete_file` accordingly.
    - `startup_recovery()` — Two-pass recovery on app startup (D-21 + HIGH-3 review fix):
        - Pass A (HIGH-3 — NEW): call `_recover_pending_deletes()` first. This finishes any deletes interrupted by a crash before they could complete; the LOCAL Tantivy index is reconciled with `local_files`/`local_pages`.
        - Pass B (D-21 existing): SELECT `filepath FROM processed_files WHERE status='pending'`; for each, call `_delete_file` then re-extract (idempotent — uses the new crash-safe `_delete_file` above).
      Order matters: pending deletes are resolved BEFORE pending inserts so the writer doesn't fight itself.
    - `prescan_count(folder_path)` — return `(file_count, total_bytes)` via `os.walk(followlinks=False)` with try/except OSError; per D-26.

    **8. Cancellation hooks:**
    - `cancel_check: Callable[[], bool]` passed through `scan_all`. Checked:
      - Between files (D-24 base).
      - Between PDF pages inside `extract_pdf_pages` — yield-and-check.
      - Between DOCX chunks.
    - On cancellation mid-file: call `writer.rollback()` (then re-open writer; per D-24 Codex revision).

    **9. UID + full_header builders (D-34):**
    ```python
    def _make_uid(sys_id: str, page_num: int) -> str:
        return f"{_UNIQUE_ID_PREFIX}_{sys_id}_P{page_num}"

    def _make_full_header(sys_id: str, page_num: int, file_id: int) -> str:
        return f"{sys_id}_LOCAL_P{page_num}_F{file_id:04d}"
    ```
  </action>
  <verify>
    <automated>python -m pytest tests/test_local_indexer.py tests/test_local_schema_evolution.py tests/test_folder_overlap_detection.py -x -q</automated>
  </verify>
  <acceptance_criteria>
    - `shared/local_indexer.py` exists, ≥ 400 lines.
    - `grep -c 'tokenizer_name="raw"' shared/local_indexer.py` returns ≥ 2 (main + LAB schemas).
    - Public API discoverable: `python -c "from shared.local_indexer import LocalIndexer, build_local_schema, build_local_lab_schema, extract_pdf_pages, extract_docx_pages, extract_txt, init_sqlite, check_folder_overlap, _fix_rtl_line; print('OK')"` exits 0.
    - `grep -c "def _fix_rtl_line\\|def _fix_rtl_page\\|def _join_fragmented_lines\\|def _rtl_ratio" shared/local_indexer.py` returns 4 (all four dead-code helpers ported).
    - `grep -c "from shared.local_sys_id" shared/local_indexer.py` returns ≥ 1.
    - Module is Qt-free: `grep -c "PyQt6\\|QtCore\\|QtWidgets\\|QThread" shared/local_indexer.py` returns 0.
    - Module is CLI-free: `grep -c "if __name__ == .__main__.\\|argparse" shared/local_indexer.py` returns 0.
    - Module does NOT write Transcriptions.txt / libraries.csv / browse_map.pkl: `grep -cE "(Transcriptions\\.txt|libraries\\.csv|browse_map\\.pkl|metadata_cache\\.pkl)" shared/local_indexer.py` returns 0.
    - `python -m ruff check shared/local_indexer.py` exits 0.
  </acceptance_criteria>
  <done>Module shipped with public API; RTL helpers ported as dead code; Tantivy schemas use raw tokenizer on unique_id; no Qt/CLI/shared-file writes.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Green tests for extraction, incremental, delete-by-uid, two-phase commit, schema, overlap, unavailable folder</name>
  <read_first>
    - shared/local_indexer.py (created in Task 1)
    - tests/fixtures/local_indexer/ (Plan 01 Task 1 fixtures — at least hebrew_sample.pdf + sample.docx + sample.txt + unsupported.html)
    - .planning/phases/95-my-library/95-PATTERNS.md ("Per-Test Pattern Assignments" — tests with research patterns)
    - .planning/phases/95-my-library/95-CONTEXT.md (D-02, D-19, D-20, D-21, D-35, D-44)
  </read_first>
  <behavior>
    See behaviors enumerated in Task 1 (the same 7 test files). Implement each test body. Use `temp_local_index_dir` and `local_indexer_fixtures_dir` fixtures from `tests/conftest.py`.

    For `test_crash_between_tantivy_and_sqlite_recovers`:
    - Patch `LocalIndexer._commit_batch` to raise `OSError("simulated crash")` AFTER `writer.commit()` but BEFORE SQLite UPDATE.
    - Verify `processed_files` has rows with `status='pending'`.
    - Create a fresh `LocalIndexer` instance; call `startup_recovery()`.
    - Assert recovery re-extracted (the pending rows are now `committed` and `local_pages` rows are consistent).

    For `test_modified_file_reextract_only`:
    - First scan, record extracted files via `progress_cb` mock.
    - Touch ONE file (`os.utime(path, (now+1, now+1))`).
    - Second scan; assert `progress_cb` was called ONLY for that one file (not all files).

    For `test_deleted_file_removed`:
    - First scan a 3-file folder.
    - `os.remove` one file.
    - Second scan; assert `local_files`, `local_pages`, `processed_files` no longer have rows for that file. Assert `writer.delete_documents` was called (mock + spy).

    For `test_unavailable_folder_marked_status_unavailable`:
    - `add_folder(temp_path)` where temp_path contains 2 files.
    - First scan: 2 files indexed.
    - `shutil.rmtree(temp_path)` (folder unavailable now).
    - `scan_all()` again — assert `folders.status == 'unavailable'` for that row AND `local_files` rows STILL EXIST (not purged).
  </behavior>
  <action>
    Replace Wave-0 stub bodies in each of these test files with the real test bodies described above:
    1. `tests/test_local_indexer.py` — 4 tests (PyMuPDF Hebrew, RTL helpers, supported types, unsupported extension).
    2. `tests/test_local_indexer_incremental.py` — 3 tests (second scan fast, modified-only, deleted-removed).
    3. `tests/test_local_delete_by_uid.py` — 1 test (delete-by-uid with raw tokenizer).
    4. `tests/test_local_two_phase_commit.py` — 1 test (crash recovery via fault injection).
    5. `tests/test_local_schema_evolution.py` — 4 tests (one per SQLite table introspection).
    6. `tests/test_folder_overlap_detection.py` — 1 parametrized test (commonpath overlap cases).
    7. `tests/test_local_unavailable_folder.py` — 1 test.

    Use `temp_local_index_dir` fixture and `local_indexer_fixtures_dir` fixture. For the slow test (`test_second_scan_fast`), include `pytest.mark.slow` marker — `@pytest.mark.slow def test_second_scan_fast(...)` — so it can be excluded from the default LOCAL-quick run if needed (though Wave-0 sampling at 12s should still include it).

    For Windows-specific tests (the test_folder_overlap_detection junction cases and test_canonical_filepath UNC), use `pytest.mark.skipif(sys.platform != "win32", ...)`.
  </action>
  <verify>
    <automated>python -m pytest tests/test_local_indexer.py tests/test_local_indexer_incremental.py tests/test_local_delete_by_uid.py tests/test_local_two_phase_commit.py tests/test_local_schema_evolution.py tests/test_folder_overlap_detection.py tests/test_local_unavailable_folder.py -x -q</automated>
  </verify>
  <acceptance_criteria>
    - All 7 test files pass: `python -m pytest tests/test_local_indexer.py tests/test_local_indexer_incremental.py tests/test_local_delete_by_uid.py tests/test_local_two_phase_commit.py tests/test_local_schema_evolution.py tests/test_folder_overlap_detection.py tests/test_local_unavailable_folder.py -x -q` exits 0.
    - `grep -c "raise NotImplementedError" tests/test_local_indexer.py tests/test_local_indexer_incremental.py tests/test_local_delete_by_uid.py tests/test_local_two_phase_commit.py tests/test_local_schema_evolution.py tests/test_folder_overlap_detection.py tests/test_local_unavailable_folder.py` returns 0 (all stubs replaced).
    - PyMuPDF Hebrew test runs and passes (D-44 fixture or xfail-deferred per Plan 01 Task 1 user decision).
    - `python -m ruff check tests/test_local_indexer.py tests/test_local_indexer_incremental.py tests/test_local_delete_by_uid.py tests/test_local_two_phase_commit.py tests/test_local_schema_evolution.py tests/test_folder_overlap_detection.py tests/test_local_unavailable_folder.py` exits 0.
  </acceptance_criteria>
  <done>7 stub files green; ruff clean.</done>
</task>

<task type="auto">
  <name>Task 3: TXT encoding policy - strict utf-8-sig + cp1255 fallback + encoding_error on dual failure (D-07 + MEDIUM-2 review fix)</name>
  <read_first>
    - shared/local_indexer.py extract_txt implementation (Task 1)
    - .planning/phases/95-my-library/95-CONTEXT.md (D-07: starting policy utf-8-sig only; planner picks)
  </read_first>
  <action>
    **MEDIUM-2 review fix (2026-05-21):** the previous policy locked at `utf-8-sig` with `errors="replace"`, which silently corrupts non-UTF-8 input (cp1255 or damaged) by emitting U+FFFD replacement characters that then get indexed as garbage. The reviewer flagged this as a MEDIUM concern. Policy revised: try utf-8-sig with `errors="strict"`; on `UnicodeDecodeError`, fall back to `cp1255` (legacy Windows Hebrew, commonly produced by older Hebrew text editors); on a second `UnicodeDecodeError`, mark the file `extraction_status="encoding_error"` and emit NO Tantivy docs. The user sees the failure in the per-file status panel rather than getting silently-corrupt indexed content.

    Implementation in `extract_txt` (MEDIUM-2 strict + cp1255 fallback):
    ```python
    def extract_txt(filepath: str) -> Iterator[tuple[int, str, str]]:
        """TXT extraction (D-07 + MEDIUM-2 review fix: strict utf-8-sig + cp1255 fallback;
        encoding_error on both failures - no silent replacement-character indexing).

        Encoding policy:
          1. Try utf-8-sig with errors='strict'.
          2. On UnicodeDecodeError, try cp1255 with errors='strict' (legacy Windows Hebrew).
          3. On second UnicodeDecodeError, raise EncodingError - caller (LocalIndexer._index_one_file)
             catches and sets local_files.extraction_status = 'encoding_error'. No docs emitted.

        DO NOT use errors='replace'. The replacement character (U+FFFD) silently corrupts
        the index and breaks search for the affected files (MEDIUM-2 review fix).
        """
        try:
            with open(filepath, "r", encoding="utf-8-sig", errors="strict") as f:
                text = f.read()
        except UnicodeDecodeError as utf8_err:
            try:
                with open(filepath, "r", encoding="cp1255", errors="strict") as f:
                    text = f.read()
                logger.info(
                    "extract_txt fallback to cp1255 for %s (utf-8-sig failed: %s)",
                    filepath, utf8_err,
                )
            except UnicodeDecodeError as cp1255_err:
                raise EncodingError(
                    f"Cannot decode {filepath} as utf-8-sig or cp1255: "
                    f"utf-8 error: {utf8_err}; cp1255 error: {cp1255_err}"
                )
        yield (1, text, os.path.basename(filepath))


    class EncodingError(Exception):
        """Raised by extract_txt when both utf-8-sig and cp1255 fail (MEDIUM-2)."""
        pass
    ```

    The caller `_index_one_file` MUST wrap `extract_txt` in `try/except EncodingError` and set `local_files.extraction_status = "encoding_error"` + `error_msg = str(e)`. No Tantivy docs are emitted for the file. The UI status panel surfaces this row to the user.

    Behavior tests to ADD in `tests/test_local_indexer.py`:
    - `test_txt_utf8_sig_strict` - UTF-8-sig file with Hebrew BOM decodes successfully on first attempt; NO `replace` characters in indexed content.
    - `test_txt_cp1255_fallback` - cp1255-encoded Hebrew file triggers second-attempt fallback that succeeds; indexed content matches the round-tripped expected text; NO `replace` characters.
    - `test_txt_undecodable_marked_encoding_error` - A file whose bytes are NEITHER valid utf-8-sig NOR valid cp1255 (e.g., random bytes 0x80-0xFF). Indexer marks `local_files.extraction_status == "encoding_error"`. NO `local_pages` rows emitted for that sys_id. NO Tantivy docs added. The error_msg column contains both UnicodeDecodeError messages.
    - `test_txt_no_replacement_chars_indexed` - Negative regression: the character U+FFFD does NOT appear in any indexed content. Scan stored `content` field across all LOCAL Tantivy docs and assert `�` substring is absent.

    Record the policy decision in `.planning/phases/95-my-library/95-03-SUMMARY.md`. Update CONTEXT D-07 status to: "LOCKED - utf-8-sig strict + cp1255 fallback + encoding_error on both failures (MEDIUM-2 review fix, 2026-05-21)".
  </action>
  <verify>
    <automated>python -m pytest tests/test_local_indexer.py -x -q -k "txt_utf8_sig_strict or txt_cp1255_fallback or txt_undecodable_marked_encoding_error or txt_no_replacement_chars_indexed"</automated>
  </verify>
  <acceptance_criteria>
    - `extract_txt` uses `errors="strict"` (MEDIUM-2): `grep "errors=.strict." shared/local_indexer.py` returns >= 2 (one per encoding attempt).
    - `extract_txt` does NOT use `errors="replace"`: `grep -c "errors=.replace." shared/local_indexer.py` returns 0.
    - `EncodingError` class defined: `grep -c "class EncodingError" shared/local_indexer.py` returns 1.
    - cp1255 fallback present: `grep -c "cp1255" shared/local_indexer.py` returns >= 2 (the open call + the error message).
    - All 4 new behavior tests pass: `python -m pytest tests/test_local_indexer.py -x -q -k "txt_utf8_sig_strict or txt_cp1255_fallback or txt_undecodable_marked_encoding_error or txt_no_replacement_chars_indexed"` exits 0.
    - SUMMARY.md records the locked policy and the MEDIUM-2 review-fix attribution.
  </acceptance_criteria>
  <done>TXT extraction uses strict decoding with cp1255 fallback; encoding_error surfaced; no silent replacement characters in indexed content (MEDIUM-2 review fix).</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| User .pdf/.docx/.txt files → PyMuPDF/python-docx parsers | Untrusted file content — malformed PDFs trigger fitz exceptions |
| Filesystem walk → SQLite cache | Filepath input flows from `os.walk` into SQL via parameterized queries |
| Tantivy writer state → on-disk index files | Crash between writer.commit() and SQLite UPDATE leaves split-state; D-21 two-phase commit recovers |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-95-07 | Tampering | Malicious PDF parses into hostile content / triggers PyMuPDF crash | mitigate | `_index_one_file` wraps extraction in try/except; on exception, sets `extraction_status='error'` + `error_msg=str(e)`; UI shows the row, file_id_id_stays_logged; no crash propagates to caller (REQ-8 acceptance) |
| T-95-08 | Tampering | Filename injection via shell metacharacters in `local_files.filepath` | mitigate | All SQLite calls use parameterized `?` placeholders; filepath never passed to shell; `os.startfile()` (in Plan 07) only operates on already-canonicalized paths |
| T-95-09 | Denial of service | Tantivy index file lock leaves stale state after crash | mitigate | D-37 fallback (in Plan 05); D-21 two-phase commit + `startup_recovery()` re-extracts pending files at next launch |
| T-95-10 | Tampering | LOCAL LAB index becomes stale after main LAB rebuild → silent fingerprint corruption | mitigate (delegated) | D-09 / D-38 weights_hash invalidation is implemented in Plan 06 (LAB merge plan); this plan emits the metadata file at build time only |
| T-95-11 | Information disclosure | `error_msg` in `local_files` table may leak filepath info from PyMuPDF exception strings | accept (low risk) | Stored locally only; never serialized to cloud (Plan 04 gates); fits "personal data on personal disk" trust model |
| T-95-12 | Tampering | Folder path traversal via overlapping registration | mitigate | D-17 + D-42: `_canonical_filepath` + `os.path.commonpath`; tests pin Windows-specific UNC / junction / casing cases |
</threat_model>

<verification>
- `python -m pytest tests/test_local_indexer.py tests/test_local_indexer_incremental.py tests/test_local_delete_by_uid.py tests/test_local_two_phase_commit.py tests/test_local_schema_evolution.py tests/test_folder_overlap_detection.py tests/test_local_unavailable_folder.py -x -q` exits 0.
- `python -m pytest tests/ -q` exits 0 (full suite — no regressions).
- `python -m ruff check shared/local_indexer.py` exits 0.
- `python -c "import shared.local_indexer; print('imports clean')"` exits 0.
</verification>

<success_criteria>
- `shared/local_indexer.py` provides Qt-free indexer with the 7 public API entries listed under behavior.
- Tantivy `unique_id` field uses `tokenizer_name="raw"` in BOTH main and LAB LOCAL schemas (Pitfall #2 P0 fix).
- RTL helpers ported verbatim as DEAD CODE (D-02 contract).
- SQLite schema matches D-35 exactly (4 tables — `folders`, `processed_files`, `local_pages`, `local_files`).
- Two-phase commit per D-21 with `startup_recovery()` re-extracting pending rows.
- Folder overlap detection per D-17 + D-42 (commonpath after canonical normalization).
- TXT policy: utf-8-sig strict → cp1255 fallback → EncodingError on dual failure (D-07 + MEDIUM-2 review fold). No errors="replace" anywhere in the codepath.
- 7 Wave-0 stub files turned GREEN.
- No writes to Transcriptions.txt / libraries.csv / browse_map.pkl / metadata_cache.pkl (SPEC Constraint #6).
</success_criteria>

<output>
After completion, create `.planning/phases/95-my-library/95-03-SUMMARY.md` documenting:
- Public API of `shared/local_indexer.py`
- TXT encoding smoke test outcome (D-07 lock decision)
- Whether D-44 Hebrew fixture passes or xfail (link back to Plan 01 Task 1 decision)
- Any open implementation questions for Wave 2/3
</output>
