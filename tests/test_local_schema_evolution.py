# -*- coding: utf-8 -*-
"""Wave 0 stub — Phase 95 D-35: SQLite schema for local_index.sqlite3.

Real implementation: shared/local_indexer.py (Wave 1, Plan 95-03).
"""


def test_folders_table_schema():
    """D-35: folders table has columns: folder_id, path, added_at, last_scanned_at, status."""
    raise NotImplementedError(
        "Wave 0 stub for D-35 folders table schema — implemented in Wave 1 plan 95-03"
    )


def test_local_files_table_schema():
    """D-35: local_files table has all 12 required columns including sys_id UNIQUE,
    filepath, folder_id FK, display_title, extraction_status, last_indexed_at, etc.
    """
    raise NotImplementedError(
        "Wave 0 stub for D-35 local_files table schema — implemented in Wave 1 plan 95-03"
    )


def test_local_pages_table_schema():
    """D-20: local_pages table has columns: sys_id, uid, page_num (PK composite).
    Used for delete-by-uid tracking.
    """
    raise NotImplementedError(
        "Wave 0 stub for D-20/D-35 local_pages table schema — implemented in Wave 1 plan 95-03"
    )


def test_processed_files_table_schema():
    """D-35 / REQ-5: processed_files table retains its narrow mtime-cache role.
    Columns: filepath, mtime, size, last_indexed_at, status.
    """
    raise NotImplementedError(
        "Wave 0 stub for D-35 processed_files table schema — implemented in Wave 1 plan 95-03"
    )
