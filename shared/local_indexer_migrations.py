# -*- coding: utf-8 -*-
"""Phase 97 D-NEW-1 + Phase 102: SQLite migration ladder for the LOCAL indexer database.

Migrates local_index.sqlite3 from user_version=0 (Phase 95 baseline + fresh
installs) to user_version=3 (Phase 102 target schema) via an idempotent ladder.

Migration summary:
  0 -> 1: Baseline stamp — Phase 95 tables already exist via init_sqlite;
           this step formally records them under the versioning scheme.
  1 -> 2: Column additions (per 97-CONTEXT.md LD-1 SQL block):
           - local_pages: cached_text, cached_text_codec, cached_text_uncompressed_len,
             extraction_format_version, chunk_locator
           - processed_files: scan_run_id, mtime_ns
           - folders: indexed_count, error_count, pending_count, oversized_count,
             last_aggregate_at
           - NEW TABLE: scan_runs (replaces _pending_cleanup sentinel anti-pattern)
           - NEW TABLE: pending_dir_cleanup (GC for .old-<ts> rebuild dirs)
           - D-NEW-4 prune: delete unsupported-extension rows with NULL/unset status
  2 -> 3: Phase 102 stamp — no DDL change; registers corrupt_encoding as a kept
           status (already in _KEPT_STATUSES) and bumps user_version so pre-Phase-102
           rows (extraction_format_version=1) stay identifiable for the manual
           'Re-index All' recovery (D-10 — NO auto-flip / mass re-index).

THREAT:
  T-97A-01 — PRAGMA integrity_check BEFORE migration; raises RuntimeError on != "ok"
  surfacing "Reset My Library" (D-NEW-1). Each migration runs in its own
  BEGIN IMMEDIATE / COMMIT / ROLLBACK transaction so partial migrations don't
  advance user_version.
"""
from __future__ import annotations

import logging
import sqlite3

logger = logging.getLogger(__name__)

_LATEST_VERSION = 3

# Supported file extensions — D-NEW-4: rows for other extensions with NULL/unset
# status are pruned in the 1->2 migration to prevent SQLite bloat at 100K+ trees.
_SUPPORTED_EXTENSIONS = (
    ".pdf",
    ".docx",
    ".txt",
    ".html",
    ".xlsx",
    ".csv",
)

# Status codes that ALWAYS keep a row regardless of extension (D-NEW-4).
_KEPT_STATUSES = (
    "oversized",
    "error",
    "encoding_error",
    "corrupt_encoding",
    "changed_during_index",
    "zip_bomb_suspected",
    "unreachable",
    "timeout",
    "committed",
    "pending",
    "pending_delete",
)


def _alter_safe(cur: sqlite3.Cursor, ddl: str) -> None:
    """Execute an ALTER TABLE ADD COLUMN, swallowing 'duplicate column name' errors only."""
    try:
        cur.execute(ddl)
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e).lower():
            pass  # Column already exists — idempotent
        else:
            raise


def _migrate_0_to_1(conn: sqlite3.Connection) -> None:
    """0 -> 1: No-op baseline stamp.

    Phase 95 tables already exist via init_sqlite(). This migration only
    formalises that they constitute 'version 1' in our versioning scheme.
    No DDL changes are needed.
    """
    logger.info(
        "Phase 97 migration 0->1: stamping Phase 95 baseline schema as v1 (no-op column changes)"
    )
    # Intentionally empty — tables exist; stamp in run() sets user_version=1.


def _migrate_1_to_2(conn: sqlite3.Connection) -> None:
    """1 -> 2: Phase 97 column additions + new tables + D-NEW-4 prune.

    All ALTER TABLE ADD COLUMN calls are wrapped via _alter_safe so a second
    run after a partial migration crash is fully idempotent.
    """
    cur = conn.cursor()

    # --- local_pages: Phase 97 R-03 + D-NEW-5 ---
    _alter_safe(cur, "ALTER TABLE local_pages ADD COLUMN cached_text BLOB")
    _alter_safe(cur, "ALTER TABLE local_pages ADD COLUMN cached_text_codec TEXT NOT NULL DEFAULT 'zstd'")
    _alter_safe(cur, "ALTER TABLE local_pages ADD COLUMN cached_text_uncompressed_len INTEGER")
    _alter_safe(cur, "ALTER TABLE local_pages ADD COLUMN extraction_format_version INTEGER NOT NULL DEFAULT 1")
    _alter_safe(cur, "ALTER TABLE local_pages ADD COLUMN chunk_locator TEXT")

    # --- processed_files: Phase 97 U-02 + D-NEW-8 ---
    _alter_safe(cur, "ALTER TABLE processed_files ADD COLUMN scan_run_id TEXT")
    _alter_safe(cur, "ALTER TABLE processed_files ADD COLUMN mtime_ns INTEGER")

    # --- folders: Phase 97 C-04 persisted counters ---
    _alter_safe(cur, "ALTER TABLE folders ADD COLUMN indexed_count INTEGER NOT NULL DEFAULT 0")
    _alter_safe(cur, "ALTER TABLE folders ADD COLUMN error_count INTEGER NOT NULL DEFAULT 0")
    _alter_safe(cur, "ALTER TABLE folders ADD COLUMN pending_count INTEGER NOT NULL DEFAULT 0")
    _alter_safe(cur, "ALTER TABLE folders ADD COLUMN oversized_count INTEGER NOT NULL DEFAULT 0")
    _alter_safe(cur, "ALTER TABLE folders ADD COLUMN last_aggregate_at REAL")

    # --- NEW TABLE: scan_runs (replaces _pending_cleanup sentinel — D-NEW-1 / LD-6) ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scan_runs (
            scan_run_id   TEXT PRIMARY KEY,
            started_at    REAL NOT NULL,
            ended_at      REAL,
            status        TEXT NOT NULL CHECK (status IN ('running', 'completed', 'canceled', 'discarded'))
        )
    """)

    # --- NEW TABLE: pending_dir_cleanup (.old-<ts> GC for atomic rebuild) ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pending_dir_cleanup (
            path        TEXT PRIMARY KEY,
            kind        TEXT NOT NULL,
            created_at  REAL NOT NULL DEFAULT (strftime('%s','now'))
        )
    """)

    # --- D-NEW-4: prune Phase 95 rows for unsupported extensions with no status ---
    # Build a LIKE chain for supported extensions (SQLite has no REGEXP by default).
    ext_conditions = " AND ".join(
        f"LOWER(filepath) NOT LIKE '%{ext}'" for ext in _SUPPORTED_EXTENSIONS
    )
    kept_status_placeholders = ",".join("?" * len(_KEPT_STATUSES))
    prune_sql = (
        f"DELETE FROM processed_files WHERE ({ext_conditions}) "  # noqa: S608
        f"AND (status IS NULL OR status NOT IN ({kept_status_placeholders}))"
    )
    cur.execute(prune_sql, list(_KEPT_STATUSES))
    pruned = cur.rowcount
    if pruned > 0:
        logger.info(
            "Phase 97 D-NEW-4: pruned %d unsupported-extension rows from processed_files",
            pruned,
        )


def _migrate_2_to_3(conn: sqlite3.Connection) -> None:
    """2 -> 3: Phase 102 stamp. No DDL change — registers corrupt_encoding as a
    kept status (already in _KEPT_STATUSES) and bumps user_version so pre-Phase-102
    rows (extraction_format_version=1) stay identifiable for the manual
    'Re-index All' recovery (D-10 — NO auto-flip / mass re-index)."""
    logger.info("Phase 102 migration 2->3: stamping rawdict-extractor schema (no-op DDL)")
    # Intentionally no DDL: the existing extraction_format_version column carries the
    # per-row 1 vs 2 distinction written by _write_page_doc; no auto re-flip here.


# Registry: {current_version: migration_function}
_MIGRATIONS: dict[int, object] = {
    0: _migrate_0_to_1,
    1: _migrate_1_to_2,
    2: _migrate_2_to_3,
}


def run(conn: sqlite3.Connection) -> int:
    """Run all pending migrations on the given connection.

    Returns the final user_version (= _LATEST_VERSION on success).

    Raises:
        RuntimeError: if PRAGMA integrity_check fails (surfacing "Reset My Library")
        RuntimeError: if no migrator is registered for the current version
        Any sqlite3 exception from a migration that rolls back cleanly
    """
    # T-97A-01: integrity_check BEFORE any migration
    # A severely corrupt file raises sqlite3.DatabaseError on the PRAGMA itself;
    # we catch that and convert it to the same RuntimeError with "Reset My Library".
    try:
        row = conn.execute("PRAGMA integrity_check").fetchone()
    except sqlite3.DatabaseError as _db_exc:
        raise RuntimeError(
            f"SQLite integrity_check raised DatabaseError: {_db_exc}. "
            "To recover, use the 'Reset My Library' button in Advanced settings "
            "(this DELETES your indexed cache and forces a fresh scan)."
        ) from _db_exc
    if row is None or row[0] != "ok":
        raise RuntimeError(
            f"SQLite integrity_check failed: {row[0] if row else None}. "
            "To recover, use the 'Reset My Library' button in Advanced settings "
            "(this DELETES your indexed cache and forces a fresh scan)."
        )

    current = conn.execute("PRAGMA user_version").fetchone()[0]

    while current < _LATEST_VERSION:
        migrator = _MIGRATIONS.get(current)
        if migrator is None:
            raise RuntimeError(
                f"No migrator registered for user_version={current}. "
                f"Cannot advance to {_LATEST_VERSION}."
            )

        logger.info(
            "Phase 97 migration: upgrading user_version %d -> %d",
            current, current + 1,
        )

        try:
            conn.execute("BEGIN IMMEDIATE")
            migrator(conn)  # type: ignore[operator]
            conn.execute(f"PRAGMA user_version = {current + 1}")
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.OperationalError:
                pass  # Already rolled back — no active transaction
            raise

        current += 1

    return current
