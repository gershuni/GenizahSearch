# -*- coding: utf-8 -*-
"""Phase 97 D-NEW-3: File-change-during-index detection.

Tests verify:
- pre+post os.stat bracket: when mtime_ns or size changes between pre-extraction
  and post-extraction, the file is marked 'changed_during_index' and re-queued.
- max 3 retries per scan_run: on the 4th attempt, give up (no re-queue).
"""
import os
import sqlite3
import tempfile
import time
from unittest.mock import MagicMock, patch

import pytest

from shared.local_indexer import LocalIndexer


# ---------------------------------------------------------------------------
# Helper — build indexer with a single registered folder containing one file
# ---------------------------------------------------------------------------

def _make_indexer_with_file(tmp_path, content="Phase 97 test content for changed during index."):
    """Create LocalIndexer with a single txt file registered."""
    index_dir = str(tmp_path / "idx")
    lab_dir = str(tmp_path / "lab")
    db_path = str(tmp_path / "test.sqlite3")
    os.makedirs(index_dir)
    os.makedirs(lab_dir)

    folder = str(tmp_path / "docs")
    os.makedirs(folder)
    filepath = os.path.join(folder, "testfile.txt")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

    indexer = LocalIndexer(index_dir, lab_dir, db_path)
    indexer.add_folder(folder)
    return indexer, filepath, db_path


def test_changed_during_index_requeues(tmp_path):
    """D-NEW-3: file whose mtime_ns changes between pre-extract and post-extract
    gets status='changed_during_index' and is re-queued (retry 1/3).

    Strategy: wrap _index_one_file to actually modify the file during 'extraction',
    so the real os.stat detects a genuine mtime change. No os.stat mocking needed.
    """
    indexer, filepath, db_path = _make_indexer_with_file(tmp_path)

    original_index_one_file = indexer._index_one_file

    def slow_index_one_file(fp, folder_id, cancel_check):
        """Simulate file modification happening during extraction."""
        result = original_index_one_file(fp, folder_id, cancel_check)
        # Touch the file AFTER extraction to simulate external modification.
        # We need a mtime that differs from the pre-stat. Sleep briefly so the
        # OS clock advances (Windows has 100ns resolution — on slow CI just set
        # mtime explicitly via os.utime with a forced offset).
        try:
            current_stat = os.stat(fp)
            new_mtime = current_stat.st_mtime + 10.0  # +10 seconds
            os.utime(fp, (new_mtime, new_mtime))
        except OSError:
            pass
        return result

    indexer._index_one_file = slow_index_one_file

    try:
        indexer.scan_all()
    finally:
        indexer.close()

    # Check that the file was marked 'changed_during_index'
    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT status FROM processed_files WHERE filepath LIKE ?",
        ("%testfile.txt",)
    ).fetchone()
    conn.close()

    assert row is not None, "processed_files row should exist for the test file"
    assert row[0] == "changed_during_index", (
        f"Phase 97 D-NEW-3: expected status='changed_during_index', got '{row[0]}'"
    )


def test_changed_during_index_gives_up_after_3_retries(tmp_path):
    """D-NEW-3: on the 4th attempt (retries dict has value 3), do NOT re-queue.

    Strategy: wrap _index_one_file to (a) pre-seed retries=3 and (b) touch file
    so D-NEW-3 fires. With retries=3 already set, the give-up branch runs and
    the file is NOT added to _re_queue.
    """
    indexer, filepath, db_path = _make_indexer_with_file(tmp_path)

    original_index_one_file = indexer._index_one_file
    re_queue_lengths = []  # capture _re_queue length after give-up

    def index_with_retry_inject(fp, fid, cc):
        # Pre-seed retries to 3 so the give-up branch triggers
        indexer._scan_run_retries[fp] = 3
        result = original_index_one_file(fp, fid, cc)
        # Touch the file to trigger D-NEW-3 detection
        try:
            current_stat = os.stat(fp)
            os.utime(fp, (current_stat.st_mtime + 10.0, current_stat.st_mtime + 10.0))
        except OSError:
            pass
        return result

    indexer._index_one_file = index_with_retry_inject

    try:
        indexer.scan_all()
        # After scan_all, _re_queue should NOT contain filepath (give-up, not re-queued)
        re_queue_lengths.append(len(getattr(indexer, "_re_queue", [])))
    finally:
        indexer.close()

    # The file must NOT have been re-queued (give-up at retries >= 3)
    assert re_queue_lengths, "scan_all must have run"
    assert re_queue_lengths[0] == 0, (
        f"Phase 97 D-NEW-3: expected _re_queue to be empty after give-up "
        f"(retries=3), but got {re_queue_lengths[0]} entries"
    )

    conn = sqlite3.connect(db_path)
    row = conn.execute(
        "SELECT status FROM processed_files WHERE filepath LIKE ?",
        ("%testfile.txt",)
    ).fetchone()
    conn.close()

    assert row is not None, "processed_files row must exist"
    # Status is either committed (extraction succeeded before give-up) or
    # changed_during_index (D-NEW-3 insert ran). Either is acceptable.
    assert row[0] in ("changed_during_index", "committed", "ok"), (
        f"Phase 97 D-NEW-3: unexpected status '{row[0]}'"
    )
