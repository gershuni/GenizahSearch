# -*- coding: utf-8 -*-
"""Phase 97 Wave B — D-NEW-8 mtime_ns incremental cache tests.

Tests:
- test_cache_hit_uses_mtime_ns: mtime_ns change forces re-extraction (no longer
  a cache hit after os.utime(path, ns=(atime_ns, mtime_ns+1))).
- test_legacy_null_mtime_ns_forces_remiss: processed_files row with mtime_ns=NULL
  (Phase 95 legacy) is NOT treated as a cache hit; forced re-extraction occurs and
  mtime_ns is populated after the scan.
"""
from __future__ import annotations

import os
import sqlite3
import time

import pytest

from shared.local_indexer import LocalIndexer


def _make_indexer(tmp_path):
    """Build a LocalIndexer with isolated index/lab/db paths."""
    index_dir = str(tmp_path / "idx")
    lab_dir = str(tmp_path / "lab")
    db_path = str(tmp_path / "test.sqlite3")
    os.makedirs(index_dir, exist_ok=True)
    os.makedirs(lab_dir, exist_ok=True)
    return LocalIndexer(index_dir, lab_dir, db_path), db_path, index_dir, lab_dir


def test_cache_hit_uses_mtime_ns(tmp_path):
    """D-NEW-8: changing mtime by 1 nanosecond forces re-extraction (not a cache hit).

    This tests that the cache-hit check uses st_mtime_ns (integer) instead of
    the old float mtime with 0.01s tolerance. At sub-microsecond precision
    (common on network drives and modern filesystems), the float check could
    silently treat a modified file as unchanged.

    Flow:
    1. Create a .txt file, first scan -> indexed.
    2. Verify the file is skipped on second scan (cache hit).
    3. Increment mtime_ns by 1 via os.utime(path, ns=(atime_ns, mtime_ns + 1)).
    4. Third scan -> file must be re-extracted (NOT skipped).
    5. Verify mtime_ns in processed_files is updated.
    """
    folder = str(tmp_path / "docs")
    os.makedirs(folder)
    fpath = os.path.join(folder, "test_doc.txt")
    with open(fpath, "w", encoding="utf-8") as f:
        f.write("שלום עולם. This is a test document for mtime_ns cache testing.")

    indexer, db_path, index_dir, lab_dir = _make_indexer(tmp_path)
    try:
        indexer.add_folder(folder)
        # First scan — indexes the file
        result1 = indexer.scan_all()
    finally:
        indexer.close()

    assert result1["indexed"] >= 1, f"Expected at least 1 indexed, got {result1}"

    # Second scan — should be a cache hit
    indexer2 = LocalIndexer(index_dir, lab_dir, db_path)
    try:
        result2 = indexer2.scan_all()
    finally:
        indexer2.close()

    assert result2["skipped"] >= 1, f"Expected cache hit on second scan, got {result2}"
    assert result2["indexed"] == 0, f"Expected 0 re-indexed on second scan, got {result2}"

    # Advance mtime_ns by exactly 1 nanosecond
    stat_before = os.stat(fpath)
    new_atime_ns = stat_before.st_atime_ns
    new_mtime_ns = stat_before.st_mtime_ns + 1
    os.utime(fpath, ns=(new_atime_ns, new_mtime_ns))

    stat_after = os.stat(fpath)
    assert stat_after.st_mtime_ns == new_mtime_ns, (
        "os.utime ns precision not honored by filesystem — test may be unreliable on FAT32"
    )

    # Third scan — mtime_ns changed, so the file must be re-extracted
    indexer3 = LocalIndexer(index_dir, lab_dir, db_path)
    try:
        result3 = indexer3.scan_all()
    finally:
        indexer3.close()

    assert result3["indexed"] >= 1, (
        f"Expected re-extraction after mtime_ns+1, but got: {result3}. "
        "Cache-hit check must use integer mtime_ns, not float mtime with 0.01s tolerance."
    )
    assert result3["skipped"] == 0, (
        f"Expected 0 skipped (mtime_ns changed by 1 ns), but got: {result3}"
    )

    # Verify mtime_ns is now populated in processed_files
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT mtime_ns FROM processed_files WHERE filepath LIKE ?",
        (f"%{os.path.basename(fpath)}%",),
    ).fetchone()
    conn.close()
    assert row is not None, "processed_files row not found after third scan"
    assert row["mtime_ns"] == new_mtime_ns, (
        f"mtime_ns in processed_files should be {new_mtime_ns}, got {row['mtime_ns']}"
    )


def test_legacy_null_mtime_ns_forces_remiss(tmp_path):
    """D-NEW-8: processed_files row with mtime_ns=NULL (Phase 95 legacy) forces cache miss.

    Phase 95 rows have mtime_ns=NULL. On first Phase 97 scan, those rows must
    NOT be treated as cache hits — the NULL means we have no nanosecond baseline.
    The scan should re-extract and populate mtime_ns.

    Flow:
    1. Create a .txt file and first-scan it (establishes committed row).
    2. Manually NULL-out the mtime_ns in processed_files (simulating Phase 95 legacy row).
    3. Second scan -> file must NOT be skipped (NULL != current mtime_ns).
    4. Verify mtime_ns is now populated after the scan.
    """
    folder = str(tmp_path / "docs")
    os.makedirs(folder)
    fpath = os.path.join(folder, "legacy_doc.txt")
    with open(fpath, "w", encoding="utf-8") as f:
        f.write("Legacy Phase 95 document. שלום עולם. Used to simulate null mtime_ns scenario.")

    indexer, db_path, index_dir, lab_dir = _make_indexer(tmp_path)
    try:
        indexer.add_folder(folder)
        result1 = indexer.scan_all()
    finally:
        indexer.close()

    assert result1["indexed"] >= 1, f"Expected at least 1 indexed on first scan, got {result1}"

    # Simulate Phase 95 legacy: NULL out mtime_ns (the column exists from the
    # Wave A 1->2 migration, but Phase 95 runs never populated it).
    conn = sqlite3.connect(db_path)
    conn.execute(
        "UPDATE processed_files SET mtime_ns = NULL WHERE filepath LIKE ?",
        (f"%{os.path.basename(fpath)}%",),
    )
    conn.commit()

    # Verify the row is committed (not just pending)
    row = conn.execute(
        "SELECT status, mtime_ns FROM processed_files WHERE filepath LIKE ?",
        (f"%{os.path.basename(fpath)}%",),
    ).fetchone()
    conn.close()
    assert row is not None, "processed_files row should exist after first scan"
    assert row[0] == "committed", f"Expected status='committed', got {row[0]!r}"
    assert row[1] is None, "mtime_ns should be NULL after manual reset"

    # Second scan — NULL mtime_ns MUST force a cache miss (re-extraction)
    indexer2 = LocalIndexer(index_dir, lab_dir, db_path)
    try:
        result2 = indexer2.scan_all()
    finally:
        indexer2.close()

    assert result2["indexed"] >= 1, (
        f"Expected re-extraction when mtime_ns=NULL (legacy row), but got: {result2}. "
        "NULL mtime_ns must NOT be treated as a cache hit — "
        "the comparison (NULL or 0) == current_mtime_ns is always False for any real file."
    )
    assert result2["skipped"] == 0, (
        f"Expected 0 skipped when mtime_ns=NULL (forced miss), but got: {result2}"
    )

    # Verify mtime_ns is now populated
    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT mtime_ns FROM processed_files WHERE filepath LIKE ?",
        (f"%{os.path.basename(fpath)}%",),
    ).fetchone()
    conn.close()
    assert row is not None, "processed_files row not found after second scan"
    assert row[0] is not None, (
        "mtime_ns should be populated after Phase 97 scan fills in the legacy NULL"
    )
    assert isinstance(row[0], int), f"mtime_ns should be an integer, got {type(row[0])}"
