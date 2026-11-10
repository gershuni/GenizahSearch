# -*- coding: utf-8 -*-
"""Phase 102 — Migration 2→3 + corrupt_encoding in _KEPT_STATUSES + fresh-DB stamp.

Tests:
  1. Fresh DB via init_sqlite stamps user_version == _LATEST_VERSION (== 3) [MED-7]
  2. Migration ladder from user_version=1 advances to user_version=3
  3. A corrupt_encoding row seeded BEFORE the 1→2 prune survives the prune [MED-8]
     (proves _KEPT_STATUSES protects it DURING the real prune, not a post-prune re-run)
  4. "corrupt_encoding" is a member of _KEPT_STATUSES (direct assertion)
  5. run() is idempotent (second call on already-v3 DB returns 3, no exception)
  6. No row-status flip in _migrate_2_to_3 (D-10: no UPDATE processed_files)
"""
from __future__ import annotations

import sqlite3
import inspect



# ---------------------------------------------------------------------------
# Helpers (mirrors the established pattern in test_local_indexer_migrations.py)
# ---------------------------------------------------------------------------

def _make_fresh_db(tmp_path):
    """Create a fresh LocalIndexer SQLite DB via init_sqlite (no data)."""
    from shared.local_indexer import init_sqlite
    db_path = str(tmp_path / "fresh_db.sqlite3")
    conn = init_sqlite(db_path)
    conn.row_factory = sqlite3.Row
    return conn, db_path


def _make_v1_db_with_data(tmp_path):
    """Create a DB at user_version=1 (post-0→1 stamp, pre-1→2 Phase 97 columns).

    Mimics the state after the 0→1 no-op migration ran — tables exist but the
    Phase 97 columns (cached_text, scan_run_id, etc.) have NOT been added yet.
    Includes a processed_files row so the fresh-DB guard in init_sqlite does
    NOT fire (this DB has data → it came from a prior version, not a fresh install).
    """
    db_path = str(tmp_path / "db_v1.sqlite3")
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
    # Seed a committed row so the fresh-DB guard yields False
    conn.execute(
        "INSERT INTO processed_files (filepath, mtime, size, sys_id, status) VALUES (?,?,?,?,?)",
        ("/old/doc.txt", 12345.0, 100, "97000000000000001", "committed"),
    )
    conn.commit()
    # Set user_version=1 (post-0→1, pre-1→2)
    conn.execute("PRAGMA user_version = 1")
    conn.commit()
    conn.row_factory = sqlite3.Row
    return conn, db_path


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_fresh_db_stamps_v3(tmp_path):
    """MED-7: init_sqlite stamps fresh empty DB at user_version == _LATEST_VERSION (3).

    A fresh DB must skip the migration ladder — it is already at the latest version.
    """
    from shared.local_indexer_migrations import _LATEST_VERSION

    conn, _ = _make_fresh_db(tmp_path)
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    assert version == _LATEST_VERSION, (
        f"Fresh DB should be stamped at _LATEST_VERSION={_LATEST_VERSION}, got {version}"
    )
    assert _LATEST_VERSION == 3, (
        f"_LATEST_VERSION should be 3 (Phase 102 bump), got {_LATEST_VERSION}"
    )
    conn.close()


def test_migration_ladder_from_v1_reaches_v3(tmp_path):
    """Migration ladder starting from user_version=1 advances to user_version=3."""
    from shared.local_indexer_migrations import _LATEST_VERSION, run

    conn, _ = _make_v1_db_with_data(tmp_path)
    version_before = conn.execute("PRAGMA user_version").fetchone()[0]
    assert version_before == 1, f"Expected starting at 1, got {version_before}"

    result = run(conn)
    assert result == _LATEST_VERSION, (
        f"Expected migration to land at _LATEST_VERSION={_LATEST_VERSION}, got {result}"
    )
    version_after = conn.execute("PRAGMA user_version").fetchone()[0]
    assert version_after == 3
    conn.close()


def test_corrupt_encoding_row_survives_prune_seeded_before_1to2(tmp_path):
    """MED-8: corrupt_encoding row seeded at user_version=1 survives the 1→2 D-NEW-4 prune.

    CRITICAL: the row is seeded BEFORE running migrations.run(conn). This proves
    _KEPT_STATUSES protects the row DURING the actual 1→2 prune step, not as a
    post-prune no-op. An unsupported extension (.xyz) is used to ensure the prune
    WOULD delete the row if status='corrupt_encoding' were not in _KEPT_STATUSES.
    """
    from shared.local_indexer_migrations import run

    conn, _ = _make_v1_db_with_data(tmp_path)
    # Seed a corrupt_encoding row with an unsupported extension (.xyz)
    # BEFORE migrations run. The 1→2 D-NEW-4 prune targets unsupported-extension
    # rows with NULL/unset status — corrupt_encoding must be in _KEPT_STATUSES.
    conn.execute(
        "INSERT INTO processed_files (filepath, mtime, size, sys_id, status) VALUES (?,?,?,?,?)",
        ("/docs/vilna_shabbat.xyz", 99999.0, 512, "corrupt-test-id-001", "corrupt_encoding"),
    )
    conn.commit()

    # Confirm the row is there before migration
    before = conn.execute(
        "SELECT COUNT(*) FROM processed_files WHERE filepath = ?",
        ("/docs/vilna_shabbat.xyz",),
    ).fetchone()[0]
    assert before == 1, f"Corrupt_encoding row should exist before migration, got {before}"

    # Run the FULL migration ladder (1→2 prune runs, then 2→3 no-op stamp)
    run(conn)

    # Assert the corrupt_encoding row SURVIVED the prune
    after = conn.execute(
        "SELECT COUNT(*) FROM processed_files WHERE filepath = ?",
        ("/docs/vilna_shabbat.xyz",),
    ).fetchone()[0]
    assert after == 1, (
        f"corrupt_encoding row was PRUNED by 1→2 migration — "
        f"'corrupt_encoding' must be in _KEPT_STATUSES to protect it. "
        f"Row count after migration: {after}"
    )
    conn.close()


def test_corrupt_encoding_in_kept_statuses():
    """Direct assertion: 'corrupt_encoding' is a member of _KEPT_STATUSES."""
    from shared.local_indexer_migrations import _KEPT_STATUSES

    assert "corrupt_encoding" in _KEPT_STATUSES, (
        f"'corrupt_encoding' must be in _KEPT_STATUSES. Current tuple: {_KEPT_STATUSES}"
    )


def test_run_is_idempotent(tmp_path):
    """run() on an already-v3 DB is a no-op (returns 3, no exception)."""
    from shared.local_indexer_migrations import _LATEST_VERSION, run

    conn, _ = _make_v1_db_with_data(tmp_path)
    # First run: bring to v3
    run(conn)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 3

    # Second run: no-op
    result = run(conn)
    assert result == _LATEST_VERSION
    assert conn.execute("PRAGMA user_version").fetchone()[0] == 3
    conn.close()


def test_migrate_2_to_3_has_no_row_flip(tmp_path):
    """D-10 guard: _migrate_2_to_3 source code contains no UPDATE processed_files statement.

    This prevents a regression to the Phase 101 D-04 mass-reindex-on-startup bug
    where a migration flipped 12K rows to pending on the UI thread.
    """
    import shared.local_indexer_migrations as _mig_mod

    assert hasattr(_mig_mod, "_migrate_2_to_3"), (
        "_migrate_2_to_3 function must exist in shared/local_indexer_migrations.py"
    )
    src = inspect.getsource(_mig_mod._migrate_2_to_3)
    # Must NOT contain any status flip/update statement
    assert "UPDATE processed_files" not in src.upper(), (
        "_migrate_2_to_3 must NOT contain 'UPDATE processed_files' — "
        "no auto-flip/mass-reindex (D-10). Found in source:\n" + src
    )
    assert "SET status" not in src.upper(), (
        "_migrate_2_to_3 must NOT contain 'SET status' — "
        "no auto-flip/mass-reindex (D-10). Found in source:\n" + src
    )


def test_latest_version_is_3():
    """_LATEST_VERSION must equal 3 after the Phase 102 bump."""
    from shared.local_indexer_migrations import _LATEST_VERSION

    assert _LATEST_VERSION == 3, (
        f"_LATEST_VERSION must be 3 (Phase 102 bump), got {_LATEST_VERSION}"
    )


def test_migrate_2_to_3_registered():
    """_migrate_2_to_3 must be registered at key 2 in the _MIGRATIONS dict."""
    import shared.local_indexer_migrations as _mig_mod

    assert hasattr(_mig_mod, "_MIGRATIONS"), "_MIGRATIONS dict must exist"
    assert 2 in _mig_mod._MIGRATIONS, (
        "2: _migrate_2_to_3 must be registered in _MIGRATIONS"
    )
    assert _mig_mod._MIGRATIONS[2].__name__ == "_migrate_2_to_3", (
        f"_MIGRATIONS[2] should be _migrate_2_to_3, got {_mig_mod._MIGRATIONS[2].__name__}"
    )
