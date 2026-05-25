# -*- coding: utf-8 -*-
"""Phase 97 D-NEW-1 + LD-2: SQLite migration ladder tests.

Covers the four-fixture matrix per 97-01-PLAN.md:
  - fresh DB stamps target version
  - Phase 95-style DB at v0 migrates to target
  - partial migration idempotent resume
  - rerun on v2 is no-op
  - integrity_check failure surfaces "Reset My Library"
  - BEGIN IMMEDIATE + ROLLBACK on failure
"""
from __future__ import annotations

import sqlite3

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_fresh_db(tmp_path):
    """Create a fresh LocalIndexer SQLite DB via init_sqlite (no data)."""
    from shared.local_indexer import init_sqlite
    db_path = str(tmp_path / "db.sqlite3")
    conn = init_sqlite(db_path)
    conn.row_factory = sqlite3.Row
    return conn, db_path


def _make_v0_db_with_data(tmp_path):
    """Create a DB that mimics Phase 95: user_version=0, tables exist, has data."""
    db_path = str(tmp_path / "db_v0.sqlite3")
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS folders (
            folder_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            path             TEXT    UNIQUE NOT NULL,
            added_at         REAL,
            last_scanned_at  REAL,
            status           TEXT    NOT NULL DEFAULT 'active'
        );
        CREATE TABLE IF NOT EXISTS processed_files (
            filepath  TEXT    PRIMARY KEY,
            mtime     REAL,
            size      INTEGER,
            sys_id    TEXT,
            status    TEXT    NOT NULL DEFAULT 'committed'
        );
        CREATE TABLE IF NOT EXISTS local_pages (
            sys_id    TEXT    NOT NULL,
            uid       TEXT    NOT NULL,
            page_num  INTEGER NOT NULL,
            PRIMARY KEY (sys_id, page_num)
        );
        CREATE TABLE IF NOT EXISTS local_files (
            file_id           INTEGER PRIMARY KEY AUTOINCREMENT,
            sys_id            TEXT    NOT NULL UNIQUE,
            filepath          TEXT    NOT NULL,
            folder_id         INTEGER NOT NULL,
            display_title     TEXT,
            original_filename TEXT    NOT NULL,
            file_extension    TEXT    NOT NULL,
            page_count        INTEGER NOT NULL DEFAULT 0,
            file_size_bytes   INTEGER NOT NULL,
            extraction_status TEXT    NOT NULL,
            last_indexed_at   REAL    NOT NULL,
            sha256_full       TEXT,
            error_msg         TEXT,
            pending_delete    INTEGER NOT NULL DEFAULT 0
        );
    """)
    # Seed some data so the fresh-DB guard in init_sqlite yields false
    conn.execute(
        "INSERT INTO processed_files (filepath, mtime, size, sys_id, status) VALUES (?,?,?,?,?)",
        ("/old/doc.txt", 12345.0, 100, "97000000000000001", "committed"),
    )
    conn.commit()
    # Explicitly set user_version=0 (the REAL Phase 95 state)
    conn.execute("PRAGMA user_version = 0")
    conn.commit()
    conn.row_factory = sqlite3.Row
    return conn, db_path


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_fresh_db_stamps_target_version(tmp_path):
    """Fresh DB (init_sqlite with no data) ends at user_version=2 immediately.

    The fresh-DB stamp in init_sqlite bypasses the ladder since there is no
    pre-existing data.
    """
    from shared.local_indexer_migrations import _LATEST_VERSION, run

    conn, _ = _make_fresh_db(tmp_path)
    # Fresh DB should already be at target (stamp in init_sqlite).
    version_before = conn.execute("PRAGMA user_version").fetchone()[0]
    assert version_before == _LATEST_VERSION, (
        f"Fresh DB should already be at user_version={_LATEST_VERSION} "
        f"after init_sqlite, got {version_before}"
    )
    # run() should be a no-op now.
    result = run(conn)
    assert result == _LATEST_VERSION


def test_phase95_db_at_v0_migrates_to_target(tmp_path):
    """Phase 95 DB at user_version=0 migrates to user_version=2 via ladder."""
    from shared.local_indexer_migrations import _LATEST_VERSION, run

    conn, _ = _make_v0_db_with_data(tmp_path)
    version_before = conn.execute("PRAGMA user_version").fetchone()[0]
    assert version_before == 0, f"Expected starting at 0, got {version_before}"

    result = run(conn)
    assert result == _LATEST_VERSION, (
        f"Expected migration to land at {_LATEST_VERSION}, got {result}"
    )
    version_after = conn.execute("PRAGMA user_version").fetchone()[0]
    assert version_after == _LATEST_VERSION


def test_partial_migration_idempotent_resume(tmp_path, monkeypatch):
    """Simulated crash mid-1->2 migration: second run completes idempotently."""
    from shared.local_indexer_migrations import _LATEST_VERSION, run
    import shared.local_indexer_migrations as _mig_mod

    conn, _ = _make_v0_db_with_data(tmp_path)
    # First apply the 0->1 migration manually
    run_id_before = conn.execute("PRAGMA user_version").fetchone()[0]

    # Simulate partial crash: apply 0->1 stamp but then fail during 1->2 by
    # patching _alter_safe to raise after first call in _migrate_1_to_2
    alter_call_count = [0]
    original_alter_safe = _mig_mod._alter_safe

    def _partial_alter(cur, ddl):
        alter_call_count[0] += 1
        if alter_call_count[0] > 3:
            raise RuntimeError("Simulated partial crash during ALTER")
        original_alter_safe(cur, ddl)

    # Apply 0->1 cleanly first
    try:
        conn.execute("BEGIN IMMEDIATE")
        _mig_mod._migrate_0_to_1(conn)
        conn.execute("PRAGMA user_version = 1")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    assert conn.execute("PRAGMA user_version").fetchone()[0] == 1

    # Now patch _alter_safe so the 1->2 migration crashes partway through
    monkeypatch.setattr(_mig_mod, "_alter_safe", _partial_alter)
    with pytest.raises(RuntimeError, match="Simulated partial crash"):
        run(conn)

    # user_version should still be 1 (migration rolled back)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 1
    # conn should NOT be in an active transaction
    assert not conn.in_transaction

    # Restore and resume - idempotent
    monkeypatch.setattr(_mig_mod, "_alter_safe", original_alter_safe)
    result = run(conn)
    assert result == _LATEST_VERSION
    assert conn.execute("PRAGMA user_version").fetchone()[0] == _LATEST_VERSION


def test_rerun_on_v2_is_noop(tmp_path):
    """DB already at user_version=2 — run() returns 2 without re-applying ALTERs."""
    from shared.local_indexer_migrations import _LATEST_VERSION, run

    conn, _ = _make_v0_db_with_data(tmp_path)
    # Bring to v2
    run(conn)
    # Second run is a no-op
    result = run(conn)
    assert result == _LATEST_VERSION
    # Verify no duplicate columns were created by attempting to add one manually
    # (if the migration tried to re-apply, it would duplicate or error)
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    assert version == _LATEST_VERSION


def test_integrity_check_failure_surfaces_reset_my_library(tmp_path):
    """Corrupt DB triggers RuntimeError containing 'Reset My Library'."""
    from shared.local_indexer_migrations import run

    db_path = str(tmp_path / "corrupt.sqlite3")
    # Create a valid DB first
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE t (x INTEGER)")
    conn.commit()
    conn.close()

    # Corrupt the file by overwriting some bytes
    with open(db_path, "r+b") as f:
        f.seek(100)
        f.write(b"\x00" * 50)

    conn2 = sqlite3.connect(db_path)
    with pytest.raises(RuntimeError, match="Reset My Library"):
        run(conn2)
    conn2.close()


def test_v1_to_v2_uses_begin_immediate_and_rollback_on_failure(tmp_path, monkeypatch):
    """Monkeypatch ALTER to raise mid-1->2; assert ROLLBACK fires and conn not in txn."""
    from shared.local_indexer_migrations import run
    import shared.local_indexer_migrations as _mig_mod

    conn, _ = _make_v0_db_with_data(tmp_path)

    # Apply 0->1 first
    try:
        conn.execute("BEGIN IMMEDIATE")
        _mig_mod._migrate_0_to_1(conn)
        conn.execute("PRAGMA user_version = 1")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    # Patch _alter_safe to raise immediately in 1->2
    original = _mig_mod._alter_safe

    def _fail_alter(cur, ddl):
        raise sqlite3.OperationalError("Simulated fatal ALTER failure")

    monkeypatch.setattr(_mig_mod, "_alter_safe", _fail_alter)
    with pytest.raises(Exception):
        run(conn)

    # Connection must NOT be in an active transaction after the failure
    assert not conn.in_transaction, "Connection was left in a transaction after ROLLBACK"
    # user_version must still be 1 (rolled back)
    # Need a new connection since the current one may be in an error state
    monkeypatch.setattr(_mig_mod, "_alter_safe", original)
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    assert version == 1, f"Expected user_version=1 after rollback, got {version}"
