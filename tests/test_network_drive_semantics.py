# -*- coding: utf-8 -*-
"""Phase 97 D-NEW-2: Network drive semantics — errno-discriminated retry + skip set.

Tests verify:
- ENOENT (folder vanished): non-retryable 'unreachable'
- ETIMEDOUT (transient): 3-retry × 2s backoff then 'timeout'
- EACCES (permission denied): non-retryable 'unreachable' (no retry)
- scan_all skip-set membership: folders with status 'unreachable' OR 'timeout' are
  skipped the same way 'unavailable' is skipped (LD-9 propagation).
"""
import errno
import os
import sqlite3
from unittest.mock import patch


from shared.local_indexer import LocalIndexer, _check_folder_reachable


# ---------------------------------------------------------------------------
# _check_folder_reachable unit tests (D-NEW-2)
# ---------------------------------------------------------------------------

def test_enoent():
    """ENOENT (folder does not exist) -> non-retryable 'unreachable', no sleep."""
    with patch("shared.local_indexer.os.path.isdir", return_value=False) as mock_isdir, \
         patch("shared.local_indexer.time.sleep") as mock_sleep:
        reachable, status = _check_folder_reachable("/nonexistent/path")
    assert reachable is False
    assert status == "unreachable"
    mock_sleep.assert_not_called()


def test_etimedout_retry(tmp_path):
    """ETIMEDOUT (transient network error) -> retry 3× with 2s backoff, then 'timeout'."""
    call_count = [0]

    def raise_etimedout(path):
        call_count[0] += 1
        raise OSError(errno.ETIMEDOUT, "Connection timed out", path)

    with patch("shared.local_indexer.os.path.isdir", side_effect=raise_etimedout), \
         patch("shared.local_indexer.time.sleep") as mock_sleep:
        reachable, status = _check_folder_reachable("/slow/network/share", max_retries=3)

    assert reachable is False
    assert status == "timeout"
    # 3 attempts; sleep called BETWEEN attempts 0->1 and 1->2 (2 sleeps)
    assert mock_sleep.call_count == 2
    assert all(call.args[0] == 2.0 for call in mock_sleep.call_args_list)
    assert call_count[0] == 3


def test_eaccess_no_retry():
    """EACCES (permission denied) -> non-retryable 'unreachable', no sleep."""
    def raise_eacces(path):
        raise OSError(errno.EACCES, "Permission denied", path)

    with patch("shared.local_indexer.os.path.isdir", side_effect=raise_eacces), \
         patch("shared.local_indexer.time.sleep") as mock_sleep:
        reachable, status = _check_folder_reachable("/protected/path")

    assert reachable is False
    assert status == "unreachable"
    mock_sleep.assert_not_called()


def test_skip_set_membership_in_scan_all(tmp_path):
    """LD-9: folders with status 'unreachable' or 'timeout' are skipped in scan_all,
    the same way 'unavailable' folders are skipped.
    """
    index_dir = str(tmp_path / "idx")
    lab_dir = str(tmp_path / "lab")
    db_path = str(tmp_path / "test.sqlite3")
    os.makedirs(index_dir)
    os.makedirs(lab_dir)

    # Create a folder and index it
    folder = str(tmp_path / "my_docs")
    os.makedirs(folder)
    with open(os.path.join(folder, "doc.txt"), "w", encoding="utf-8") as f:
        f.write("Test document content")

    indexer = LocalIndexer(index_dir, lab_dir, db_path)
    try:
        indexer.add_folder(folder)
        indexer.scan_all()
    finally:
        indexer.close()

    # Manually set folder status to 'unreachable'
    conn = sqlite3.connect(db_path)
    conn.execute("UPDATE folders SET status = 'unreachable' WHERE path LIKE ?",
                 (f"%{os.path.basename(folder)}%",))
    conn.commit()
    folder_id = conn.execute("SELECT folder_id FROM folders WHERE path LIKE ?",
                             (f"%{os.path.basename(folder)}%",)).fetchone()[0]
    conn.close()

    # Patch _check_folder_reachable to always return unreachable (folder exists on disk though)
    # scan_all should SKIP this folder (not re-walk it) because status='unreachable'
    walk_calls = []
    original_walk = os.walk

    def tracking_walk(path, *args, **kwargs):
        walk_calls.append(path)
        return original_walk(path, *args, **kwargs)

    indexer2 = LocalIndexer(index_dir, lab_dir, db_path)
    try:
        with patch("shared.local_indexer.os.walk", side_effect=tracking_walk):
            # scan_all should skip the unreachable folder
            indexer2.scan_all()
    finally:
        indexer2.close()

    # The folder with status='unreachable' should NOT have been walked
    # (LD-9 skip-set membership check)
    assert not any(folder in str(p) for p in walk_calls), (
        f"Phase 97 D-NEW-2 LD-9: folder with status='unreachable' should be skipped "
        f"in scan_all, but os.walk was called on it. walk_calls={walk_calls}"
    )
