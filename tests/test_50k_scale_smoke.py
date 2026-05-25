# -*- coding: utf-8 -*-
"""Phase 97 Wave E scale smoke test — 50K synthetic files.

Marked @pytest.mark.scale — excluded from the default test run.
Enable with:  pytest tests/test_50k_scale_smoke.py --run-scale -x

This test synthesizes 50K tiny text files in a temporary directory and
indexes them via scan_all(), asserting no crash and all files reach
status='committed' in processed_files.

Memory check: RSS stays under 4 GB (generous — real limit depends on host).
"""
import os
import sys
import time

import pytest


@pytest.mark.scale
def test_50k_scale_smoke(tmp_path):
    """50K synthetic tiny files index without crash; all rows reach status='committed'.

    Requires --run-scale flag to execute (excluded from default CI).
    """
    try:
        from shared.local_indexer import LocalIndexer
    except ImportError as exc:
        pytest.skip(f"LocalIndexer not importable: {exc}")

    idx_dir = str(tmp_path / "idx")
    lab_dir = str(tmp_path / "lab")
    db_path = str(tmp_path / "db.sqlite3")
    os.makedirs(idx_dir)
    os.makedirs(lab_dir)

    # Create 50K tiny text files (1-5 lines each) in a flat folder
    folder = str(tmp_path / "corpus50k")
    os.makedirs(folder)
    print(f"\n[scale] Creating 50K synthetic files in {folder}...", flush=True)
    t0 = time.monotonic()
    for i in range(50_000):
        p = os.path.join(folder, f"doc_{i:06d}.txt")
        with open(p, "w", encoding="utf-8") as f:
            f.write(f"Document {i}\nTest content line 1\nTest content line 2\n")
    create_time = time.monotonic() - t0
    print(f"[scale] Created 50K files in {create_time:.1f}s", flush=True)

    # Index all 50K files
    indexer = LocalIndexer(idx_dir, lab_dir, db_path)
    try:
        indexer.add_folder(folder)
        t1 = time.monotonic()
        print("[scale] Starting scan_all on 50K files...", flush=True)
        result = indexer.scan_all()
        scan_time = time.monotonic() - t1
        print(f"[scale] scan_all completed in {scan_time:.1f}s", flush=True)
        print(f"[scale] Result: {result}", flush=True)
    finally:
        indexer.close()

    # Memory check (optional — psutil may not be available)
    try:
        import psutil
        proc = psutil.Process(os.getpid())
        rss_gb = proc.memory_info().rss / (1024 ** 3)
        print(f"[scale] RSS after scan: {rss_gb:.2f} GB", flush=True)
        assert rss_gb < 4.0, f"RSS too high: {rss_gb:.2f} GB (limit: 4 GB)"
    except ImportError:
        pass  # psutil not available; skip memory check

    # Verify all files indexed (or at least most — oversized is OK for test)
    import sqlite3
    conn = sqlite3.connect(db_path)
    committed = conn.execute(
        "SELECT COUNT(*) FROM processed_files WHERE status = 'committed'"
    ).fetchone()[0]
    total = conn.execute(
        "SELECT COUNT(*) FROM processed_files"
    ).fetchone()[0]
    conn.close()

    print(f"[scale] committed={committed}/{total}", flush=True)
    assert total >= 50_000, f"Expected >= 50K processed_files rows, got {total}"
    assert committed == total, (
        f"Not all files committed: {committed}/{total} committed. "
        "Check for errors in scan_all result."
    )
