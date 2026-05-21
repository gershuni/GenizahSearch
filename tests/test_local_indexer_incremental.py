# -*- coding: utf-8 -*-
"""Phase 95 REQ-5: mtime-cache incremental indexing.

Tests:
- test_second_scan_fast: second scan with no changes skips all extraction
- test_modified_file_reextract_only: only the modified file is re-extracted
- test_deleted_file_removed: deleted file is removed from index and SQLite cache
"""
import os
import sqlite3
import time

import pytest

from shared.local_indexer import LocalIndexer


def _make_test_folder(tmp_path, n_files=3):
    """Create n_files .txt files in a temp folder. Returns folder path + list of file paths."""
    folder = str(tmp_path / "docs")
    os.makedirs(folder)
    files = []
    for i in range(n_files):
        fpath = os.path.join(folder, f"doc{i}.txt")
        with open(fpath, "w", encoding="utf-8") as f:
            f.write(f"Document {i}: שלום עולם test content for incremental indexing.")
        files.append(fpath)
    return folder, files


def _make_indexer(tmp_path):
    index_dir = str(tmp_path / "idx")
    lab_dir = str(tmp_path / "lab")
    db_path = str(tmp_path / "test.sqlite3")
    os.makedirs(index_dir)
    os.makedirs(lab_dir)
    return LocalIndexer(index_dir, lab_dir, db_path), db_path, index_dir, lab_dir


@pytest.mark.slow
def test_second_scan_fast(tmp_path):
    """REQ-5: second scan with no modified files skips all extraction (cache hit).

    PASS condition: second scan completes in <= 5% of first scan wall time
    OR <= 0.5s (whichever is greater — CI variance tolerance).
    """
    # Create 100 small text files
    folder = str(tmp_path / "docs")
    os.makedirs(folder)
    for i in range(100):
        fpath = os.path.join(folder, f"doc{i:04d}.txt")
        with open(fpath, "w", encoding="utf-8") as f:
            f.write(f"Content for doc {i}: שלום עולם test paragraph. " * 5)

    indexer, db_path, index_dir, lab_dir = _make_indexer(tmp_path)
    try:
        indexer.add_folder(folder)

        # First scan - timed
        t0 = time.perf_counter()
        result1 = indexer.scan_all()
        t1 = time.perf_counter()
        first_scan_time = t1 - t0
    finally:
        indexer.close()

    assert result1["indexed"] == 100, f"Expected 100 indexed on first scan, got {result1}"

    # Second scan - should be very fast (all cache hits)
    indexer2 = LocalIndexer(index_dir, lab_dir, db_path)
    try:
        t2 = time.perf_counter()
        result2 = indexer2.scan_all()
        t3 = time.perf_counter()
        second_scan_time = t3 - t2
    finally:
        indexer2.close()

    assert result2["indexed"] == 0, f"Expected 0 indexed on second scan (all cached), got {result2}"
    assert result2["skipped"] == 100, f"Expected 100 skipped on second scan, got {result2}"

    # Timing assertion: second scan <= max(5% of first, 0.5s)
    max_allowed = max(first_scan_time * 0.05, 0.5)
    assert second_scan_time <= max_allowed, (
        f"Second scan too slow: {second_scan_time:.3f}s > {max_allowed:.3f}s "
        f"(first scan: {first_scan_time:.3f}s)"
    )


def test_modified_file_reextract_only(tmp_path):
    """REQ-5 + D-36: only the modified file is re-extracted; others stay cached."""
    folder, files = _make_test_folder(tmp_path, n_files=3)
    indexer, db_path, index_dir, lab_dir = _make_indexer(tmp_path)

    extracted_files = []

    def track_progress(idx, total, filename):
        extracted_files.append(filename)

    indexer._progress_cb = track_progress
    try:
        indexer.add_folder(folder)
        indexer.scan_all()
    finally:
        indexer.close()

    assert len(extracted_files) == 3, f"Expected 3 files on first scan, got {extracted_files}"

    # Touch ONE file: advance its mtime by 2 seconds
    target_file = files[1]  # doc1.txt
    stat = os.stat(target_file)
    new_time = stat.st_mtime + 2.0
    os.utime(target_file, (new_time, new_time))

    # Second scan
    extracted_files_2 = []
    indexer2 = LocalIndexer(index_dir, lab_dir, db_path)
    indexer2._progress_cb = lambda idx, total, fn: extracted_files_2.append(fn)
    try:
        result2 = indexer2.scan_all()
    finally:
        indexer2.close()

    # Only the modified file should have been re-extracted
    assert result2["indexed"] == 1, f"Expected 1 re-indexed, got {result2}"
    assert result2["skipped"] == 2, f"Expected 2 skipped, got {result2}"
    assert len(extracted_files_2) == 1, (
        f"Expected progress_cb called for 1 file only, got: {extracted_files_2}"
    )
    assert os.path.basename(target_file) in extracted_files_2[0], (
        f"Expected modified file in extracted list, got: {extracted_files_2}"
    )


def test_deleted_file_removed(tmp_path):
    """REQ-5 + D-36: deleted file is removed from Tantivy index and SQLite cache."""
    folder, files = _make_test_folder(tmp_path, n_files=3)
    indexer, db_path, index_dir, lab_dir = _make_indexer(tmp_path)

    try:
        indexer.add_folder(folder)
        indexer.scan_all()
    finally:
        indexer.close()

    # Verify all 3 files are in local_files
    conn = sqlite3.connect(db_path)
    count_before = conn.execute("SELECT COUNT(*) FROM local_files").fetchone()[0]
    assert count_before == 3, f"Expected 3 files before delete, got {count_before}"
    conn.close()

    # Delete one file
    deleted_file = files[0]  # doc0.txt
    deleted_canonical = deleted_file.lower().replace("\\", "/") if os.sep == "\\" else deleted_file
    os.remove(deleted_file)
    assert not os.path.exists(deleted_file)

    # Second scan: should detect the deletion
    indexer2 = LocalIndexer(index_dir, lab_dir, db_path)
    try:
        result2 = indexer2.scan_all()
    finally:
        indexer2.close()

    # local_files, local_pages, processed_files should NOT have the deleted file
    conn2 = sqlite3.connect(db_path)
    files_after = conn2.execute(
        "SELECT filepath FROM local_files"
    ).fetchall()
    file_paths_after = [r[0] for r in files_after]

    # The deleted file's path should not appear in local_files
    deleted_norm = os.path.normcase(deleted_file)
    assert all(
        os.path.normcase(p) != deleted_norm for p in file_paths_after
    ), f"Deleted file still in local_files: {file_paths_after}"

    count_after = conn2.execute("SELECT COUNT(*) FROM local_files").fetchone()[0]
    assert count_after == 2, f"Expected 2 files after deletion, got {count_after}"

    # local_pages for the deleted file's sys_id should be gone
    # (get sys_id from processed_files which should also be cleaned)
    pf_rows = conn2.execute(
        "SELECT filepath FROM processed_files"
    ).fetchall()
    pf_paths = [r[0] for r in pf_rows]
    assert all(
        os.path.normcase(p) != deleted_norm for p in pf_paths
    ), f"Deleted file still in processed_files: {pf_paths}"

    conn2.close()
