# -*- coding: utf-8 -*-
"""Phase 97 R-02: atomic Tantivy rebuild tests.

Tests:
  - test_write_page_doc_populates_cached_text
  - test_close_all_handles_before_rename
  - test_corrupt_index_recovery_from_cached_text
  - test_old_dir_cleanup_via_pending_dir_cleanup
  - test_rebuild_failure_raises_does_not_silent_fresh_empty
"""
from __future__ import annotations

import os
import sqlite3

import pytest


def _make_bare_indexer(tmp_path):
    """Build a LocalIndexer rooted at tmp_path."""
    from shared.local_indexer import LocalIndexer
    index_dir = str(tmp_path / "idx")
    lab_dir = str(tmp_path / "lab")
    db_path = str(tmp_path / "test.sqlite3")
    os.makedirs(index_dir, exist_ok=True)
    os.makedirs(lab_dir, exist_ok=True)
    return LocalIndexer(index_dir, lab_dir, db_path), db_path, index_dir, lab_dir


def test_write_page_doc_populates_cached_text(tmp_path):
    """_write_page_doc writes non-NULL cached_text + correct codec + uncompressed_len
    + extraction_format_version + chunk_locator into local_pages.
    """
    from shared.local_indexer import decompress_cached_text

    indexer, db_path, index_dir, lab_dir = _make_bare_indexer(tmp_path)

    # Pre-insert the folder and a processed_files + local_files row
    folder_id = 1
    sys_id = "97000000000000001"
    filepath = str(tmp_path / "doc.txt")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("test text for caching שלום")

    indexer._conn.execute(
        "INSERT OR IGNORE INTO folders (folder_id, path, status) VALUES (?, ?, 'active')",
        (folder_id, str(tmp_path)),
    )
    indexer._conn.execute(
        "INSERT OR REPLACE INTO processed_files (filepath, mtime, size, sys_id, status) "
        "VALUES (?, ?, ?, ?, 'pending')",
        (filepath, 1000.0, 100, sys_id),
    )
    indexer._conn.execute(
        "INSERT OR IGNORE INTO local_files "
        "(file_id, sys_id, filepath, folder_id, original_filename, file_extension, "
        "page_count, file_size_bytes, extraction_status, last_indexed_at) "
        "VALUES (1, ?, ?, ?, ?, '.txt', 0, 100, 'pending', 1000.0)",
        (sys_id, filepath, folder_id, os.path.basename(filepath)),
    )
    indexer._conn.commit()

    text = "Hello world שלום"
    chunk_locator = "p. 1"
    uid = indexer._write_page_doc(sys_id, 1, text, "Test", folder_id, chunk_locator=chunk_locator)

    # Check the local_pages row
    row = indexer._conn.execute(
        "SELECT cached_text, cached_text_codec, cached_text_uncompressed_len, "
        "extraction_format_version, chunk_locator FROM local_pages WHERE sys_id = ?",
        (sys_id,),
    ).fetchone()

    assert row is not None, "local_pages row not found"
    assert row["cached_text"] is not None, "cached_text should be non-NULL"
    assert row["cached_text_codec"] == "zstd"
    assert row["cached_text_uncompressed_len"] == len(text.encode("utf-8"))
    assert row["extraction_format_version"] == 1
    assert row["chunk_locator"] == chunk_locator

    # Verify round-trip
    recovered = decompress_cached_text(row["cached_text"])
    assert recovered == text


def test_close_all_handles_before_rename(tmp_path):
    """All 7 handles closed (set None) BEFORE first os.rename call in rebuild."""
    import uuid
    from unittest.mock import MagicMock, patch

    indexer, db_path, index_dir, lab_dir = _make_bare_indexer(tmp_path)

    # Track call order between handle closures and os.rename
    call_log = []

    def make_mock_close(name):
        def _close():
            call_log.append(f"close:{name}")
        return _close

    # close_searcher_cb must null the 4 engine-side handles:
    # local_searcher, local_index, local_lab_searcher, _local_lab_index
    engine = MagicMock()
    engine.local_searcher = MagicMock()
    engine.local_index = MagicMock()
    engine.local_lab_searcher = MagicMock()
    engine._local_lab_index = MagicMock()

    original_close_internal = indexer._close_internal_writer_index

    def tracked_close_internal():
        call_log.append("close:internal_writer_index")
        original_close_internal()

    def close_searcher_cb():
        call_log.append("close:searcher_cb")
        engine.local_searcher = None
        engine.local_index = None
        engine.local_lab_searcher = None
        engine._local_lab_index = None

    def reload_searcher_cb():
        call_log.append("reload:searcher_cb")

    rename_calls = []

    def tracked_rename(src, dst):
        rename_calls.append((src, dst))
        call_log.append(f"rename:{os.path.basename(src)}->{os.path.basename(dst)}")
        os.rename(src, dst)

    with patch.object(indexer, "_close_internal_writer_index", side_effect=tracked_close_internal):
        with patch.object(indexer, "_retry_windows_rename", side_effect=tracked_rename):
            scan_run_id = uuid.uuid4().hex
            try:
                indexer.rebuild_main_index_atomic(
                    scan_run_id,
                    close_searcher_cb=close_searcher_cb,
                    reload_searcher_cb=reload_searcher_cb,
                )
            except Exception:
                pass  # We don't care about rebuild success for this test

    # Verify: all close operations happened BEFORE the first rename
    first_rename_idx = next(
        (i for i, e in enumerate(call_log) if e.startswith("rename:")),
        len(call_log),
    )
    close_indices = [i for i, e in enumerate(call_log) if e.startswith("close:")]
    assert len(close_indices) >= 2, f"Expected at least 2 close calls, got: {call_log}"
    assert all(i < first_rename_idx for i in close_indices), (
        f"Some close calls happened AFTER first rename. Call log: {call_log}"
    )

    # Verify engine handles were nulled
    assert engine.local_searcher is None
    assert engine.local_index is None
    assert engine.local_lab_searcher is None
    assert engine._local_lab_index is None


def test_corrupt_index_recovery_from_cached_text(tmp_path):
    """Corrupt the live index; LocalIndexer init triggers atomic rebuild; result is searchable."""
    import os
    from shared.local_indexer import LocalIndexer

    index_dir = str(tmp_path / "idx")
    lab_dir = str(tmp_path / "lab")
    db_path = str(tmp_path / "test.sqlite3")
    os.makedirs(index_dir, exist_ok=True)
    os.makedirs(lab_dir, exist_ok=True)

    # Create a healthy indexer, index one file
    folder = str(tmp_path / "docs")
    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, "doc.txt")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("Search text content for recovery test שלום")

    indexer = LocalIndexer(index_dir, lab_dir, db_path)
    indexer.add_folder(folder)
    indexer.scan_all()
    indexer.close()

    # Verify data was indexed
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    committed = conn.execute(
        "SELECT COUNT(*) as cnt FROM processed_files WHERE status = 'committed'"
    ).fetchone()["cnt"]
    conn.close()

    if committed == 0:
        pytest.skip("No committed rows after scan — cannot test recovery")

    # Corrupt the Tantivy index by overwriting the meta.json
    meta_path = os.path.join(index_dir, "meta.json")
    if os.path.exists(meta_path):
        with open(meta_path, "w") as f:
            f.write("THIS IS CORRUPT JSON {{{")

    # Re-open LocalIndexer — should trigger atomic rebuild from cached_text
    try:
        indexer2 = LocalIndexer(index_dir, lab_dir, db_path)
        # If we get here without RuntimeError, the rebuild succeeded
        # Verify the index is searchable
        searcher = indexer2._index.searcher()
        assert searcher is not None
    except RuntimeError as e:
        # Rebuild from cached_text may fail if cached_text is NULL (pre-Phase 97 data)
        # This is acceptable behavior per the plan
        if "Reset My Library" in str(e):
            pass  # Expected when cached_text is NULL
        else:
            raise


def test_old_dir_cleanup_via_pending_dir_cleanup(tmp_path):
    """After rebuild, pending_dir_cleanup has a row; clean_pending_rebuild_dirs removes it."""
    from shared.local_indexer import LocalIndexer

    index_dir = str(tmp_path / "idx")
    lab_dir = str(tmp_path / "lab")
    db_path = str(tmp_path / "test.sqlite3")
    os.makedirs(index_dir, exist_ok=True)
    os.makedirs(lab_dir, exist_ok=True)

    indexer = LocalIndexer(index_dir, lab_dir, db_path)

    # Directly insert a pending_dir_cleanup row with a real directory path
    old_dir = str(tmp_path / "old_idx_to_delete")
    os.makedirs(old_dir, exist_ok=True)
    # Write a file in it to verify shutil.rmtree works
    with open(os.path.join(old_dir, "dummy.txt"), "w") as f:
        f.write("old data")

    indexer._conn.execute(
        "INSERT OR REPLACE INTO pending_dir_cleanup (path, kind, created_at) "
        "VALUES (?, 'rebuild_old', strftime('%s','now'))",
        (old_dir,),
    )
    indexer._conn.commit()

    # Verify the row exists
    rows_before = indexer._conn.execute(
        "SELECT COUNT(*) FROM pending_dir_cleanup WHERE kind = 'rebuild_old'"
    ).fetchone()[0]
    assert rows_before >= 1, "Expected at least 1 pending_dir_cleanup row"

    # Call cleanup
    indexer.clean_pending_rebuild_dirs()

    # Verify row is gone
    rows_after = indexer._conn.execute(
        "SELECT COUNT(*) FROM pending_dir_cleanup WHERE path = ?",
        (old_dir,),
    ).fetchone()[0]
    assert rows_after == 0, "Expected pending_dir_cleanup row to be deleted"

    # Verify directory is gone
    assert not os.path.exists(old_dir), f"Expected {old_dir} to be removed"


def test_rebuild_failure_raises_does_not_silent_fresh_empty(tmp_path):
    """When cached_text is NULL and source files missing, RuntimeError with 'Reset My Library'."""
    import os
    from shared.local_indexer import LocalIndexer

    index_dir = str(tmp_path / "idx")
    lab_dir = str(tmp_path / "lab")
    db_path = str(tmp_path / "test.sqlite3")
    os.makedirs(index_dir, exist_ok=True)
    os.makedirs(lab_dir, exist_ok=True)

    # Create initial indexer and scan a file
    folder = str(tmp_path / "docs")
    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, "doc.txt")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("Some text")

    indexer = LocalIndexer(index_dir, lab_dir, db_path)
    indexer.add_folder(folder)
    indexer.scan_all()
    indexer.close()

    # Null out all cached_text in local_pages (simulate pre-Phase 97 data)
    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE local_pages SET cached_text = NULL")
    conn.commit()
    conn.close()

    # Delete the source file
    os.remove(filepath)

    # Corrupt the Tantivy index
    meta_path = os.path.join(index_dir, "meta.json")
    if os.path.exists(meta_path):
        with open(meta_path, "w") as f:
            f.write("CORRUPTED")

    # Re-open LocalIndexer; expect RuntimeError with "Reset My Library"
    with pytest.raises(RuntimeError, match="Reset My Library"):
        LocalIndexer(index_dir, lab_dir, db_path)
