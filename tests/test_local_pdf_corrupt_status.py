# -*- coding: utf-8 -*-
"""Phase 102 Plan 03 Task 2 — buffer-then-decide corrupt flow + cancel rollbacks + D-08 surfaces.

Tests the following correctness invariants:

HIGH-2 (detect-before-write):
  - A file whose >=50% of pages are flagged corrupt returns 'corrupt_encoding' with
    pages_written == 0 AND _write_page_doc is NEVER called (no garbage indexed).

HIGH (Codex round-3 — buffer-phase cancel):
  - Cancellation DURING buffering (before any page is written) returns 'cancelled',
    calls _rollback_partial, and leaves ZERO rows in both processed_files AND local_files
    for sys_id (the pre-inserted rows from _index_one_file are cleaned up).

M5 (write-loop cancel):
  - Cancellation DURING the write loop (after >=1 page written) returns 'cancelled',
    calls _rollback_partial, and leaves no partial rows.

D-07 conservative threshold:
  - A file with only 1 of 3 pages flagged corrupt (< 50%) does NOT return 'corrupt_encoding'.

D-08 surfaces:
  - 'corrupt_encoding' is in _ERROR_STATUSES_KEPT (surface 1).
  - Scan classification counts 'corrupt_encoding' as errors (not indexed) (surface 2).
  - The pre-existing 'encoding_error' is still classified as indexed (regression guard).
  - Folder counter SQL includes 'corrupt_encoding' in error_count subquery (surface 3).

These tests FAIL until the implementation is updated (TDD RED).
"""
import os
import sqlite3
import time
from unittest.mock import patch


from shared.local_indexer import (
    LocalIndexer,
    _ERROR_STATUSES_KEPT,
)
from shared.local_sys_id import _canonical_filepath


def _make_indexer(tmp_path):
    """Create a LocalIndexer instance in a fresh tmp directory."""
    index_dir = str(tmp_path / "idx")
    lab_dir = str(tmp_path / "lab")
    db_path = str(tmp_path / "test.sqlite3")
    os.makedirs(index_dir)
    os.makedirs(lab_dir)
    return LocalIndexer(index_dir, lab_dir, db_path), db_path


def _pre_insert_rows(indexer, sys_id, folder_path, filename="test.pdf"):
    """Pre-insert processed_files + local_files rows, simulating _index_one_file.

    Returns (folder_id, canonical_fpath).
    """
    fpath = os.path.join(folder_path, filename)
    if not os.path.exists(fpath):
        with open(fpath, "w", encoding="utf-8") as f:
            f.write("dummy")

    indexer.add_folder(folder_path)
    canonical_folder = _canonical_filepath(folder_path)
    row = indexer._conn.execute(
        "SELECT folder_id FROM folders WHERE path = ?", (canonical_folder,)
    ).fetchone()
    assert row is not None, f"Folder not found: {canonical_folder}"
    folder_id = row["folder_id"]

    canonical_fpath = _canonical_filepath(fpath)
    now = time.time()
    fsize = os.path.getsize(canonical_fpath) if os.path.exists(canonical_fpath) else 10
    fmtime = os.path.getmtime(canonical_fpath) if os.path.exists(canonical_fpath) else now

    # processed_files: filepath PK, mtime, size, sys_id, status
    indexer._conn.execute(
        "INSERT OR REPLACE INTO processed_files (filepath, mtime, size, sys_id, status) "
        "VALUES (?, ?, ?, ?, 'pending')",
        (canonical_fpath, fmtime, fsize, sys_id),
    )
    indexer._conn.commit()

    # local_files: sys_id UNIQUE, filepath, folder_id, ...
    ext = os.path.splitext(filename)[1].lower()
    indexer._conn.execute(
        "INSERT OR REPLACE INTO local_files "
        "(sys_id, filepath, folder_id, display_title, original_filename, file_extension, "
        " page_count, file_size_bytes, extraction_status, last_indexed_at) "
        "VALUES (?, ?, ?, ?, ?, ?, 0, ?, 'pending', ?)",
        (sys_id, canonical_fpath, folder_id, "Test PDF", filename, ext, fsize, now),
    )
    indexer._conn.commit()
    return folder_id, canonical_fpath


# ---------------------------------------------------------------------------
# Surface 1: _ERROR_STATUSES_KEPT
# ---------------------------------------------------------------------------

def test_corrupt_encoding_in_error_statuses_kept():
    """Surface 1 — 'corrupt_encoding' must be a member of _ERROR_STATUSES_KEPT."""
    assert "corrupt_encoding" in _ERROR_STATUSES_KEPT, (
        f"'corrupt_encoding' not found in _ERROR_STATUSES_KEPT: {_ERROR_STATUSES_KEPT}"
    )


# ---------------------------------------------------------------------------
# HIGH-2: detect-before-write — corrupt file writes ZERO pages
# ---------------------------------------------------------------------------

def test_corrupt_file_writes_zero_pages(tmp_path):
    """HIGH-2 detect-before-write: a >=50%-corrupt file returns 'corrupt_encoding',
    pages_written==0, AND _write_page_doc is NEVER called (no garbage indexed).
    """
    indexer, db_path = _make_indexer(tmp_path)
    sys_id = "TEST-CORRUPT-001"
    folder = str(tmp_path / "docs")
    os.makedirs(folder, exist_ok=True)
    try:
        folder_id, canonical_fpath = _pre_insert_rows(indexer, sys_id, folder, "test.pdf")

        # A stubbed extractor that yields 3 pages, all marked corrupt in page_flags
        def mock_extract_pdf(filepath, page_flags=None):
            pages = [
                (1, "garbage text page 1 " * 10, "Test"),
                (2, "garbage text page 2 " * 10, "Test"),
                (3, "garbage text page 3 " * 10, "Test"),
            ]
            for page_num, text, title in pages:
                if page_flags is not None:
                    page_flags[page_num] = {"corrupt": True, "multicolumn": False}
                yield page_num, text, title

        write_page_doc_calls = []

        original_write_page_doc = indexer._write_page_doc
        def tracking_write_page_doc(*args, **kwargs):
            write_page_doc_calls.append(args)
            return original_write_page_doc(*args, **kwargs)
        indexer._write_page_doc = tracking_write_page_doc

        with patch("shared.local_indexer.extract_pdf_pages", side_effect=mock_extract_pdf):
            pages_written, status, title = indexer._extract_and_write_pdf(
                sys_id, str(canonical_fpath), folder_id, cancel_check=lambda: False
            )

        assert status == "corrupt_encoding", (
            f"Expected status='corrupt_encoding' for >=50%-corrupt file, got '{status}'"
        )
        assert pages_written == 0, (
            f"Expected pages_written==0 for corrupt file, got {pages_written}"
        )
        assert len(write_page_doc_calls) == 0, (
            f"_write_page_doc was called {len(write_page_doc_calls)} times for a corrupt file "
            f"— HIGH-2 detect-before-write: NO garbage must reach the index"
        )

        # Verify no local_pages rows were written
        conn = sqlite3.connect(db_path)
        page_count = conn.execute(
            "SELECT COUNT(*) FROM local_pages WHERE sys_id = ?", (sys_id,)
        ).fetchone()[0]
        conn.close()
        assert page_count == 0, (
            f"Expected 0 local_pages rows for corrupt file, got {page_count}"
        )
    finally:
        indexer.close()


# ---------------------------------------------------------------------------
# HIGH (Codex round-3): cancel DURING buffering rolls back pre-inserted rows
# ---------------------------------------------------------------------------

def test_cancel_during_buffering_rolls_back_pre_inserted_rows(tmp_path):
    """HIGH (Codex round-3) — cancel during the BUFFER phase (before any write,
    before the corrupt decision) calls _rollback_partial and leaves ZERO rows in
    both processed_files AND local_files for sys_id.

    _index_one_file pre-inserts these rows BEFORE calling _extract_and_write_pdf.
    Without the buffer-phase rollback, _commit_batch would later flip the pending
    processed_files row to committed → a committed-but-empty cancelled PDF.
    """
    indexer, db_path = _make_indexer(tmp_path)
    sys_id = "TEST-CANCEL-BUFFER-001"
    folder = str(tmp_path / "docs")
    os.makedirs(folder, exist_ok=True)
    try:
        folder_id, canonical_fpath = _pre_insert_rows(indexer, sys_id, folder, "test.pdf")

        # Verify rows exist before the call
        conn = sqlite3.connect(db_path)
        pf_before = conn.execute(
            "SELECT COUNT(*) FROM processed_files WHERE sys_id = ?", (sys_id,)
        ).fetchone()[0]
        lf_before = conn.execute(
            "SELECT COUNT(*) FROM local_files WHERE sys_id = ?", (sys_id,)
        ).fetchone()[0]
        conn.close()
        assert pf_before >= 1, f"processed_files row should exist before cancel, got {pf_before}"
        assert lf_before >= 1, f"local_files row should exist before cancel, got {lf_before}"

        # A cancel_check that fires on the FIRST buffer-loop iteration
        call_count = [0]
        def cancel_on_first_call():
            call_count[0] += 1
            return call_count[0] >= 1  # True on first call (during buffering)

        rollback_called = [False]
        original_rollback = indexer._rollback_partial
        def tracking_rollback(sid):
            rollback_called[0] = True
            return original_rollback(sid)
        indexer._rollback_partial = tracking_rollback

        # Stubbed extractor that would yield pages (cancel happens before any page is processed)
        def mock_extract_pdf(filepath, page_flags=None):
            for page_num in range(1, 4):
                if page_flags is not None:
                    page_flags[page_num] = {"corrupt": False, "multicolumn": False}
                yield page_num, "שלום עולם text on page", "Test"

        with patch("shared.local_indexer.extract_pdf_pages", side_effect=mock_extract_pdf):
            pages_written, status, title = indexer._extract_and_write_pdf(
                sys_id, str(canonical_fpath), folder_id, cancel_check=cancel_on_first_call
            )

        assert status == "cancelled", (
            f"Expected status='cancelled' after buffer-phase cancel, got '{status}'"
        )
        assert rollback_called[0], (
            "HIGH (Codex round-3): _rollback_partial was NOT called after buffer-phase cancel. "
            "The caller's pre-inserted rows will persist and _commit_batch will flip the "
            "pending processed_files row to committed → committed-but-empty cancelled PDF."
        )

        # CRITICAL: pre-inserted rows must be gone after rollback
        conn = sqlite3.connect(db_path)
        pf_after = conn.execute(
            "SELECT COUNT(*) FROM processed_files WHERE sys_id = ?", (sys_id,)
        ).fetchone()[0]
        lf_after = conn.execute(
            "SELECT COUNT(*) FROM local_files WHERE sys_id = ?", (sys_id,)
        ).fetchone()[0]
        conn.close()

        assert pf_after == 0, (
            f"HIGH (Codex round-3) row leak: {pf_after} processed_files rows remain for "
            f"sys_id='{sys_id}' after buffer-phase cancel + rollback. Expected 0. "
            f"_rollback_partial must delete processed_files rows."
        )
        assert lf_after == 0, (
            f"HIGH (Codex round-3) row leak: {lf_after} local_files rows remain for "
            f"sys_id='{sys_id}' after buffer-phase cancel + rollback. Expected 0. "
            f"_rollback_partial must delete local_files rows."
        )
    finally:
        indexer.close()


# ---------------------------------------------------------------------------
# M5: cancel DURING the write loop rolls back partial pages
# ---------------------------------------------------------------------------

def test_cancel_during_write_loop_rolls_back_partial(tmp_path):
    """M5 — cancel during the write loop (after >=1 page already written) calls
    _rollback_partial and leaves no partial rows.
    """
    indexer, db_path = _make_indexer(tmp_path)
    sys_id = "TEST-CANCEL-WRITE-001"
    folder = str(tmp_path / "docs")
    os.makedirs(folder, exist_ok=True)
    try:
        folder_id, canonical_fpath = _pre_insert_rows(indexer, sys_id, folder, "test.pdf")

        # Buffer-phase cancel_check returns False (buffering completes successfully)
        # Write-loop cancel_check returns True on second write call (after first page written)
        buffer_done = [False]
        write_loop_calls = [0]

        def cancel_check():
            if not buffer_done[0]:
                # Still in buffer phase — don't cancel
                return False
            # In write loop — cancel on 2nd call (after first page written)
            write_loop_calls[0] += 1
            return write_loop_calls[0] >= 2

        rollback_called = [False]
        original_rollback = indexer._rollback_partial
        def tracking_rollback(sid):
            rollback_called[0] = True
            return original_rollback(sid)
        indexer._rollback_partial = tracking_rollback

        # Stubbed extractor: 3 non-corrupt pages
        def mock_extract_pdf(filepath, page_flags=None):
            for page_num in range(1, 4):
                if page_flags is not None:
                    page_flags[page_num] = {"corrupt": False, "multicolumn": False}
                yield page_num, "שלום עולם " * 10, "Test"
            buffer_done[0] = True

        with patch("shared.local_indexer.extract_pdf_pages", side_effect=mock_extract_pdf):
            pages_written, status, title = indexer._extract_and_write_pdf(
                sys_id, str(canonical_fpath), folder_id, cancel_check=cancel_check
            )

        assert status == "cancelled", (
            f"Expected status='cancelled' after write-loop cancel, got '{status}'"
        )
        assert rollback_called[0], (
            "M5: _rollback_partial was NOT called after write-loop cancel. "
            "Partial pages remain in local_pages + Tantivy."
        )

        # Verify no partial local_pages rows remain
        conn = sqlite3.connect(db_path)
        page_count = conn.execute(
            "SELECT COUNT(*) FROM local_pages WHERE sys_id = ?", (sys_id,)
        ).fetchone()[0]
        conn.close()
        assert page_count == 0, (
            f"M5: {page_count} partial local_pages rows remain after write-loop cancel + rollback"
        )
    finally:
        indexer.close()


# ---------------------------------------------------------------------------
# D-07 conservative threshold: 1 of 3 corrupt pages must NOT trigger corrupt_encoding
# ---------------------------------------------------------------------------

def test_below_threshold_does_not_trigger_corrupt_encoding(tmp_path):
    """D-07 conservative threshold guard: a file with only 1 of 3 pages corrupt
    (< 50%) must NOT return 'corrupt_encoding' — it writes normally.
    """
    indexer, db_path = _make_indexer(tmp_path)
    sys_id = "TEST-THRESHOLD-001"
    folder = str(tmp_path / "docs")
    os.makedirs(folder, exist_ok=True)
    try:
        folder_id, canonical_fpath = _pre_insert_rows(indexer, sys_id, folder, "test.pdf")

        # 1 of 3 pages corrupt (33% < 50% threshold)
        def mock_extract_pdf(filepath, page_flags=None):
            pages = [
                (1, "שלום עולם " * 10, "Test"),       # clean
                (2, "garbage " * 10, "Test"),           # corrupt
                (3, "שלום עולם " * 10, "Test"),         # clean
            ]
            for page_num, text, title in pages:
                if page_flags is not None:
                    page_flags[page_num] = {
                        "corrupt": page_num == 2,  # only page 2 is corrupt
                        "multicolumn": False,
                    }
                yield page_num, text, title

        with patch("shared.local_indexer.extract_pdf_pages", side_effect=mock_extract_pdf):
            pages_written, status, title = indexer._extract_and_write_pdf(
                sys_id, str(canonical_fpath), folder_id, cancel_check=lambda: False
            )

        assert status != "corrupt_encoding", (
            f"1-of-3-corrupt file should NOT return 'corrupt_encoding' (< 50% threshold), "
            f"got '{status}'"
        )
        # Should write all 3 pages (conservative: only flag if >=50% are corrupt)
        assert pages_written == 3, (
            f"Expected 3 pages written for 1-of-3-corrupt file, got {pages_written}"
        )
    finally:
        indexer.close()


# ---------------------------------------------------------------------------
# Surface 2: scan classification — corrupt_encoding → errors, encoding_error → indexed
# ---------------------------------------------------------------------------

def test_scan_classification_corrupt_encoding_counts_as_error():
    """Surface 2 — scan classification routes 'corrupt_encoding' to the error bucket
    (not the indexed bucket). It is an unfixable-without-OCR error.
    """
    import inspect
    import shared.local_indexer as mod

    source_path = inspect.getfile(mod)
    with open(source_path, "r", encoding="utf-8") as f:
        source = f.read()

    # The scan classification tuple is ("ok", "no_text_layer", "encoding_error", "unsupported")
    # 'corrupt_encoding' must NOT be in that tuple (it falls through to the else/errors branch)
    # Find the classification tuple region
    assert '"corrupt_encoding"' not in source or source.find('"corrupt_encoding"') >= 0, (
        "Unexpected: 'corrupt_encoding' not found in source"
    )
    # The indexed-bucket tuple must NOT contain corrupt_encoding
    # (we check via functional behavior — drive a simulated classification)
    # We test via a minimal LocalIndexer integration via the class method logic:
    # The surface-2 code lives in _index_one_file's status branch.
    # We check the source directly: the indexed tuple must be ("ok", "no_text_layer", "encoding_error", "unsupported")
    assert '"encoding_error"' in source, (
        "'encoding_error' was removed from source — regression"
    )
    # The tuple on the classification line must contain encoding_error but NOT corrupt_encoding
    import re as _re
    match = _re.search(
        r'if status in \(([^)]+)\):\s*result\["indexed"\]',
        source,
    )
    assert match is not None, (
        "Cannot find scan-classification indexed-bucket branch in source. "
        "The pattern 'if status in (...): result[\"indexed\"]' was not found."
    )
    tuple_content = match.group(1)
    assert "encoding_error" in tuple_content, (
        f"'encoding_error' missing from indexed-bucket tuple: {tuple_content!r} — regression guard"
    )
    assert "corrupt_encoding" not in tuple_content, (
        f"'corrupt_encoding' found in indexed-bucket tuple: {tuple_content!r} — it must be "
        f"in the error bucket (not indexed)"
    )


def test_scan_classification_encoding_error_still_indexed():
    """Surface 2 regression guard: 'encoding_error' must still be classified as indexed
    (pre-existing legacy status — must NOT be reclassified to errors by D-08 changes).
    """
    import inspect
    import re as _re
    import shared.local_indexer as mod

    source_path = inspect.getfile(mod)
    with open(source_path, "r", encoding="utf-8") as f:
        source = f.read()

    match = _re.search(
        r'if status in \(([^)]+)\):\s*result\["indexed"\]',
        source,
    )
    assert match is not None, "Cannot find indexed-bucket branch in source"
    tuple_content = match.group(1)
    assert "encoding_error" in tuple_content, (
        f"REGRESSION: 'encoding_error' removed from indexed-bucket tuple: {tuple_content!r}. "
        f"Legacy status 'encoding_error' must remain indexed — it is NOT the same as "
        f"the new 'corrupt_encoding' status."
    )


# ---------------------------------------------------------------------------
# Surface 3: folder counter SQL includes 'corrupt_encoding'
# ---------------------------------------------------------------------------

def test_folder_counter_sql_includes_corrupt_encoding():
    """Surface 3 — the folder error_count SQL subquery must include 'corrupt_encoding'."""
    import inspect
    import shared.local_indexer as mod

    source_path = inspect.getfile(mod)
    with open(source_path, "r", encoding="utf-8") as f:
        source = f.read()

    # Find the error_count subquery region
    # Pattern: extraction_status IN (...) in a context that updates error_count
    import re as _re
    match = _re.search(
        r"error_count\s*=\s*\(\s*SELECT COUNT\(\*\) FROM local_files.*?"
        r"extraction_status IN \(([^)]+)\)",
        source, _re.DOTALL,
    )
    assert match is not None, (
        "Cannot find error_count SQL subquery with extraction_status IN (...) in source"
    )
    in_clause = match.group(1)
    assert "corrupt_encoding" in in_clause, (
        f"'corrupt_encoding' not found in folder error_count SQL IN-clause: {in_clause!r}"
    )
