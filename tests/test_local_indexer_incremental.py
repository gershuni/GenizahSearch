# -*- coding: utf-8 -*-
"""Wave 0 stub — Phase 95 REQ-5: mtime-cache incremental indexing.

Real implementation: shared/local_indexer.py (Wave 1, Plan 95-03).
All tests raise NotImplementedError until Plan 95-03 ships.
"""


def test_second_scan_fast():
    """REQ-5: second scan with no modified files skips all extraction (cache hit)."""
    raise NotImplementedError(
        "Wave 0 stub for REQ-5 mtime cache second-scan — implemented in Wave 1 plan 95-03"
    )


def test_modified_file_reextract_only():
    """REQ-5 + D-36: only the modified file is re-extracted; others stay cached."""
    raise NotImplementedError(
        "Wave 0 stub for REQ-5 / D-36 modified-file update algorithm — implemented in Wave 1 plan 95-03"
    )


def test_deleted_file_removed():
    """REQ-5 + D-36: deleted file is removed from Tantivy index and SQLite cache."""
    raise NotImplementedError(
        "Wave 0 stub for REQ-5 deleted-file removal — implemented in Wave 1 plan 95-03"
    )
