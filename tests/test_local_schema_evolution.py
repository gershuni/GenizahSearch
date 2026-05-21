# -*- coding: utf-8 -*-
"""Phase 95 D-35: SQLite schema for local_index.sqlite3.

Tests introspect PRAGMA table_info to verify exact column names and types.
"""

import pytest

from shared.local_indexer import init_sqlite


def _table_columns(conn, table_name):
    """Return dict of {column_name: (type, notnull, dflt_value, pk)} via PRAGMA table_info."""
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row[1]: row for row in rows}


@pytest.fixture
def db_conn():
    """Fresh in-memory SQLite connection with all tables initialized."""
    conn = init_sqlite(":memory:")
    yield conn
    conn.close()


def test_folders_table_schema(db_conn):
    """D-35: folders table has columns: folder_id, path, added_at, last_scanned_at, status."""
    cols = _table_columns(db_conn, "folders")
    assert "folder_id" in cols, "Missing column: folder_id"
    assert "path" in cols, "Missing column: path"
    assert "added_at" in cols, "Missing column: added_at"
    assert "last_scanned_at" in cols, "Missing column: last_scanned_at"
    assert "status" in cols, "Missing column: status"

    # folder_id is INTEGER PRIMARY KEY
    assert cols["folder_id"][5] == 1, "folder_id should be primary key (pk=1)"
    # path is NOT NULL
    assert cols["path"][3] == 1, "path should be NOT NULL"
    # status has default 'active'
    assert cols["status"][4] is not None, "status should have a default value"


def test_local_files_table_schema(db_conn):
    """D-35: local_files table has all required columns including sys_id UNIQUE,
    filepath, folder_id FK, display_title, extraction_status, last_indexed_at,
    pending_delete (HIGH-3 review fix).
    """
    cols = _table_columns(db_conn, "local_files")

    required = [
        "file_id",
        "sys_id",
        "filepath",
        "folder_id",
        "display_title",
        "original_filename",
        "file_extension",
        "page_count",
        "file_size_bytes",
        "extraction_status",
        "last_indexed_at",
        "sha256_full",
        "error_msg",
        "pending_delete",  # HIGH-3 review fix
    ]
    for col in required:
        assert col in cols, f"Missing column: {col}"

    # file_id is INTEGER PRIMARY KEY
    assert cols["file_id"][5] == 1, "file_id should be primary key (pk=1)"

    # pending_delete has DEFAULT 0 (HIGH-3 review fix)
    assert cols["pending_delete"][4] is not None, "pending_delete should have a default"
    assert "0" in str(cols["pending_delete"][4]), "pending_delete default should be 0"

    # page_count has DEFAULT 0
    assert cols["page_count"][4] is not None, "page_count should have a default"

    # extraction_status is NOT NULL
    assert cols["extraction_status"][3] == 1, "extraction_status should be NOT NULL"

    # last_indexed_at is NOT NULL
    assert cols["last_indexed_at"][3] == 1, "last_indexed_at should be NOT NULL"

    # Verify sys_id UNIQUE constraint via index
    indexes = db_conn.execute("PRAGMA index_list(local_files)").fetchall()
    unique_indexes = [idx for idx in indexes if idx[2] == 1]  # unique=1
    # sys_id should have a unique constraint (either via UNIQUE keyword or unique index)
    has_sys_id_unique = False
    for idx in unique_indexes:
        idx_cols = db_conn.execute(f"PRAGMA index_info({idx[1]})").fetchall()
        col_names = [c[2] for c in idx_cols]
        if "sys_id" in col_names:
            has_sys_id_unique = True
            break
    assert has_sys_id_unique, "sys_id should have a UNIQUE constraint"


def test_local_pages_table_schema(db_conn):
    """D-20: local_pages table has columns: sys_id, uid, page_num (PK composite).
    Used for delete-by-uid tracking.
    """
    cols = _table_columns(db_conn, "local_pages")

    assert "sys_id" in cols, "Missing column: sys_id"
    assert "uid" in cols, "Missing column: uid"
    assert "page_num" in cols, "Missing column: page_num"

    # sys_id is NOT NULL
    assert cols["sys_id"][3] == 1, "sys_id should be NOT NULL"
    # uid is NOT NULL
    assert cols["uid"][3] == 1, "uid should be NOT NULL"
    # page_num is NOT NULL
    assert cols["page_num"][3] == 1, "page_num should be NOT NULL"

    # Composite PRIMARY KEY on (sys_id, page_num)
    # Both should have pk > 0
    assert cols["sys_id"][5] > 0, "sys_id should be part of primary key"
    assert cols["page_num"][5] > 0, "page_num should be part of primary key"


def test_processed_files_table_schema(db_conn):
    """D-35 / REQ-5: processed_files table retains its narrow mtime-cache role.
    Columns: filepath (PK), mtime, size, sys_id, status.
    """
    cols = _table_columns(db_conn, "processed_files")

    assert "filepath" in cols, "Missing column: filepath"
    assert "mtime" in cols, "Missing column: mtime"
    assert "size" in cols, "Missing column: size"
    assert "sys_id" in cols, "Missing column: sys_id"
    assert "status" in cols, "Missing column: status"

    # filepath is PRIMARY KEY
    assert cols["filepath"][5] == 1, "filepath should be primary key (pk=1)"

    # status has default 'committed'
    assert cols["status"][4] is not None, "status should have a default value"
    assert "committed" in str(cols["status"][4]), "status default should be 'committed'"
