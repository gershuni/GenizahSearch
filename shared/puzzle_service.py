# -*- coding: utf-8 -*-
"""
Puzzle Service for joins.db sidecar persistence.

Provides CRUD operations for PuzzleDocument objects stored in a local SQLite
sidecar database (joins.db). Includes a fragment index table for reverse
lookups (find which documents contain a given fragment).

Used by both web and desktop apps. Write operations are protected by a
threading.Lock for concurrency safety. The database uses WAL journal mode
for concurrent read access.

Follows the singleton pattern established by nli_crossref_service.py.

Owners (schema v3). Every row has a nullable ``owner_key``. The web app keeps
each visitor's saved joins apart by passing ``owner_key=`` on every call
(``'u:<user id>'`` when signed in, ``'b:<session uuid>'`` when signed out; see
``web/saved_joins.py``, the only web module that talks to this service). When
``owner_key`` is given, reads and deletes only see that owner's rows and a save
refuses to touch a row that belongs to anyone else (including a row with no
owner). The desktop app never passes an owner: with ``owner_key=None`` every
method behaves exactly as before, over the per-install joins.db.
"""

import json
import logging
import os
import sqlite3
import threading
from pathlib import Path
from typing import Dict, List, Optional

from shared.puzzle_model import PuzzleDocument, PuzzleFragment

logger = logging.getLogger(__name__)

_SIDECAR_FILENAME = "joins.db"
_SIDECAR_DIR = "joins_data"


def _find_project_root() -> Optional[Path]:
    """Find the project root by looking for libraries.csv up from this file."""
    current = Path(__file__).resolve().parent
    for _ in range(5):
        if (current / "libraries.csv").exists():
            return current
        current = current.parent
    return None


def _user_data_db_path() -> str:
    """Writable per-user joins.db path.

    Used when the auto-detected project root is read-only — notably a frozen
    PyInstaller build where `_find_project_root()` resolves to the install dir
    (e.g. under Program Files, not writable for a standard user). Mirrors the
    ``%LOCALAPPDATA%\\GenizahSearchPro`` convention used by genizah_core.Config so
    joins.db lands alongside the app's other per-user data.
    """
    base = os.environ.get("LOCALAPPDATA") or os.environ.get("APPDATA")
    if base:
        base = os.path.join(base, "GenizahSearchPro")
    else:
        base = os.path.join(os.path.expanduser("~"), ".genizahsearch")
    return os.path.join(base, _SIDECAR_DIR, _SIDECAR_FILENAME)


class PuzzleService:
    """Service for persisting puzzle documents to joins.db sidecar."""

    def __init__(self, db_path: str = None, thread_safe: bool = False):
        """
        Initialize PuzzleService.

        Args:
            db_path: Path to joins.db. If None, auto-detect from project root.
            thread_safe: If True, use check_same_thread=False for web app.
        """
        self._conn: Optional[sqlite3.Connection] = None
        self._db_path: Optional[str] = None
        self._write_lock = threading.Lock()

        # Resolve db_path
        auto_detected = False
        if db_path is None:
            root = _find_project_root()
            # Source/web: the (writable) repo root. Frozen app: the root resolves
            # to the read-only install dir, or no marker is found — use a writable
            # per-user location instead.
            db_path = str(root / _SIDECAR_DIR / _SIDECAR_FILENAME) if root else _user_data_db_path()
            auto_detected = True

        if db_path is None:
            logger.warning("PuzzleService: no db_path and project root not found")
            return

        if auto_detected:
            # Create the joins_data dir. If it is NOT writable (e.g. a frozen app
            # installed under Program Files, where the project root is the read-only
            # install dir), fall back to a writable per-user dir rather than raising.
            # Regression: this mkdir was previously unguarded and raised an uncaught
            # PermissionError on the main thread when the puzzle opened (the
            # _refresh_docs_list -> get_puzzle_service() call) — a desktop crash.
            parent = Path(db_path).parent
            try:
                parent.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                fallback = _user_data_db_path()
                if Path(fallback).parent != parent:
                    logger.warning(
                        "PuzzleService: %s not writable (%s); falling back to %s",
                        parent, e, fallback,
                    )
                    db_path = fallback
                    try:
                        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
                    except OSError as e2:
                        logger.error("PuzzleService: fallback joins_data dir not writable: %s", e2)
                        return
                else:
                    logger.error("PuzzleService: joins_data dir not writable: %s", e)
                    return
        else:
            # Explicit path: require the parent to already exist (unchanged contract).
            if not Path(db_path).parent.exists():
                logger.warning("PuzzleService: parent directory does not exist: %s", Path(db_path).parent)
                return

        try:
            self._conn = sqlite3.connect(
                db_path,
                check_same_thread=not thread_safe
            )
            self._conn.row_factory = sqlite3.Row
            self._db_path = db_path

            # Pragmas
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._conn.execute("PRAGMA foreign_keys=ON")

            self._init_schema()
            logger.info("PuzzleService: opened %s", db_path)
        except Exception as e:
            logger.error("PuzzleService: failed to open %s: %s", db_path, e)
            self._conn = None

    def _init_schema(self):
        """Create tables if they don't exist."""
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            INSERT OR REPLACE INTO meta (key, value) VALUES ('schema_version', '3');

            CREATE TABLE IF NOT EXISTS join_documents (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL DEFAULT '',
                notes TEXT NOT NULL DEFAULT '',
                join_type TEXT NOT NULL DEFAULT 'uncertain'
                    CHECK (join_type IN ('physical', 'content', 'uncertain')),
                fragments_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_join_documents_updated
                ON join_documents(updated_at DESC);

            CREATE TABLE IF NOT EXISTS join_document_fragments (
                doc_id TEXT NOT NULL,
                fl_id TEXT NOT NULL,
                sys_id TEXT NOT NULL,
                FOREIGN KEY (doc_id) REFERENCES join_documents(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_jdf_fl_id ON join_document_fragments(fl_id);
            CREATE INDEX IF NOT EXISTS idx_jdf_sys_id ON join_document_fragments(sys_id);
        """)

        # Schema migration to v2: add thumbnail_b64 column
        if self._add_column_if_missing('thumbnail_b64', "TEXT DEFAULT ''"):
            logger.info("PuzzleService: migrated schema to v2 (added thumbnail_b64)")

        # Schema migration to v3: add owner_key column. Existing rows keep
        # owner_key = NULL, which the web app never matches (so they stay in
        # the file but are not shown to any web visitor) and which the desktop
        # app ignores (it never filters by owner).
        if self._add_column_if_missing('owner_key', 'TEXT'):
            logger.info("PuzzleService: migrated schema to v3 (added owner_key)")
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_join_documents_owner "
            "ON join_documents(owner_key, updated_at DESC)"
        )
        self._conn.commit()

    def _has_column(self, column: str) -> bool:
        return any(r[1] == column for r in self._conn.execute("PRAGMA table_info(join_documents)"))

    def _add_column_if_missing(self, column: str, decl: str) -> bool:
        """Add ``column`` to join_documents unless it is already there.

        Several processes (or several service objects) can open the same file
        for the first time at once. The check and the ALTER run in one
        IMMEDIATE transaction, so only one opener adds the column; the others
        wait (busy_timeout) and then find it. A "duplicate column" error from
        a writer that does not take this path is also read as "already added".

        Returns True when this call added the column.
        """
        if self._has_column(column):
            return False
        try:
            self._conn.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError:
            if self._has_column(column):
                return False
            raise
        try:
            if self._has_column(column):
                self._conn.rollback()
                return False
            self._conn.execute(f"ALTER TABLE join_documents ADD COLUMN {column} {decl}")
            self._conn.commit()
            return True
        except sqlite3.OperationalError as e:
            self._conn.rollback()
            if 'duplicate column' in str(e).lower() and self._has_column(column):
                return False
            raise

    def is_available(self) -> bool:
        """Check if the service has a valid database connection."""
        return self._conn is not None

    def save_document(self, doc: PuzzleDocument, thumbnail_b64: str = None,
                      *, owner_key: Optional[str] = None) -> Optional[str]:
        """
        Save or update a PuzzleDocument.

        Args:
            doc: The PuzzleDocument to save.
            thumbnail_b64: Optional base64-encoded thumbnail PNG. If None,
                preserves existing thumbnail (avoids overwrite on metadata-only saves).
            owner_key: When given, the row is written with this owner, and an
                existing row with the same id is only replaced if it already
                belongs to this owner (a row with another owner, or with no
                owner, is left untouched and None is returned). When None
                (desktop), the existing row's owner is kept as it is.

        Returns:
            The document ID on success, None on failure.
        """
        if not self.is_available():
            return None

        fragments_json = json.dumps(
            [{'sys_id': f.sys_id, 'folio_label': f.folio_label, 'fl_id': f.fl_id,
              'shelfmark': f.shelfmark,
              'x': f.x, 'y': f.y, 'rotation': f.rotation, 'scale': f.scale,
              'flip_h': f.flip_h, 'flip_v': f.flip_v,
              'bg_removal_threshold': f.bg_removal_threshold,
              'crop_top': f.crop_top, 'crop_bottom': f.crop_bottom,
              'crop_left': f.crop_left, 'crop_right': f.crop_right,
              'processed': f.processed,
              'image_url': getattr(f, 'image_url', ''),
              'external_provider': getattr(f, 'external_provider', ''),
              'page_index': getattr(f, 'page_index', -1)}
             for f in doc.fragments],
            ensure_ascii=False
        )

        with self._write_lock:
            try:
                # Read the existing row inside the lock, so the owner check and
                # the write below see the same state (no other save can slip in
                # between them on this shared connection).
                existing = self._conn.execute(
                    "SELECT thumbnail_b64, owner_key FROM join_documents WHERE id = ?",
                    (doc.id,)
                ).fetchone()

                if owner_key is not None:
                    if existing is not None and existing['owner_key'] != owner_key:
                        # The id is taken by another owner's row (or by a row
                        # with no owner): never replace it.
                        logger.info("PuzzleService.save_document: id belongs to another owner; not saved")
                        return None
                    row_owner = owner_key
                else:
                    # Desktop: keep whatever owner the row already has.
                    row_owner = existing['owner_key'] if existing is not None else None

                # Preserve existing thumbnail when not explicitly provided.
                if thumbnail_b64 is None:
                    thumbnail_b64 = (existing['thumbnail_b64'] or '') if existing is not None else ''

                # INSERT OR REPLACE rewrites the whole row, so owner_key must
                # be in the column list on every write.
                self._conn.execute(
                    """INSERT OR REPLACE INTO join_documents
                       (id, title, notes, join_type, fragments_json, thumbnail_b64,
                        created_at, updated_at, owner_key)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (doc.id, doc.title, doc.notes, doc.join_type,
                     fragments_json, thumbnail_b64, doc.created_at, doc.updated_at,
                     row_owner)
                )
                # Rebuild fragment index
                self._conn.execute(
                    "DELETE FROM join_document_fragments WHERE doc_id = ?",
                    (doc.id,)
                )
                for frag in doc.fragments:
                    self._conn.execute(
                        "INSERT INTO join_document_fragments (doc_id, fl_id, sys_id) VALUES (?, ?, ?)",
                        (doc.id, frag.fl_id, frag.sys_id)
                    )
                self._conn.commit()
                return doc.id
            except Exception as e:
                # Roll back the partial transaction. Without this the dirty
                # writes (doc row + DELETE + partial fragment inserts) would sit
                # pending on the long-lived connection and get flushed by the
                # NEXT successful commit, desyncing join_document_fragments from
                # fragments_json (audit 2026-05-29).
                try:
                    self._conn.rollback()
                except Exception:
                    pass
                logger.error("PuzzleService.save_document failed: %s", e)
                return None

    def load_document(self, doc_id: str, *,
                      owner_key: Optional[str] = None) -> Optional[PuzzleDocument]:
        """
        Load a PuzzleDocument by ID.

        Args:
            doc_id: The document ID.
            owner_key: When given, only a row that belongs to this owner is
                returned.

        Returns:
            The PuzzleDocument, or None if not found or unavailable.
        """
        if not self.is_available():
            return None

        try:
            # Serialize on the shared connection: all web requests share one
            # sqlite3.Connection (check_same_thread=False), so reads must not
            # run concurrently with a write+commit on another thread.
            with self._write_lock:
                if owner_key is not None:
                    row = self._conn.execute(
                        "SELECT * FROM join_documents WHERE id = ? AND owner_key = ?",
                        (doc_id, owner_key)
                    ).fetchone()
                else:
                    row = self._conn.execute(
                        "SELECT * FROM join_documents WHERE id = ?", (doc_id,)
                    ).fetchone()
            if row is None:
                return None

            fragments_data = json.loads(row['fragments_json'])
            fragments = [PuzzleFragment(**f) for f in fragments_data]
            return PuzzleDocument(
                id=row['id'],
                title=row['title'],
                notes=row['notes'],
                join_type=row['join_type'],
                fragments=fragments,
                created_at=row['created_at'],
                updated_at=row['updated_at']
            )
        except Exception as e:
            logger.error("PuzzleService.load_document failed: %s", e)
            return None

    def list_documents(self, *, owner_key: Optional[str] = None) -> List[Dict]:
        """
        List puzzle documents, sorted by updated_at DESC.

        Args:
            owner_key: When given, only this owner's documents are listed.
                When None (desktop), every document in the file is listed.

        Returns:
            List of dicts with id, title, join_type, fragments_json,
            thumbnail_b64, updated_at, shelfmarks_summary.
        """
        if not self.is_available():
            return []

        try:
            with self._write_lock:  # serialize on the shared connection
                if owner_key is not None:
                    rows = self._conn.execute(
                        "SELECT id, title, join_type, fragments_json, thumbnail_b64, updated_at "
                        "FROM join_documents WHERE owner_key = ? ORDER BY updated_at DESC",
                        (owner_key,)
                    ).fetchall()
                else:
                    rows = self._conn.execute(
                        "SELECT id, title, join_type, fragments_json, thumbnail_b64, updated_at "
                        "FROM join_documents ORDER BY updated_at DESC"
                    ).fetchall()
            results = []
            for r in rows:
                d = dict(r)
                # Extract shelfmarks summary from fragments_json
                try:
                    frags = json.loads(d.get('fragments_json', '[]'))
                    seen = []
                    for f in frags:
                        sm = f.get('shelfmark', '')
                        if sm and sm not in seen:
                            seen.append(sm)
                    d['shelfmarks_summary'] = ' + '.join(seen) if seen else ''
                except Exception:
                    d['shelfmarks_summary'] = ''  # Shelfmark lookup failed; use raw identifier
                results.append(d)
            return results
        except Exception as e:
            logger.error("PuzzleService.list_documents failed: %s", e)
            return []

    def delete_document(self, doc_id: str, *, owner_key: Optional[str] = None) -> bool:
        """
        Delete a puzzle document by ID (CASCADE deletes fragment index entries).

        Args:
            doc_id: The document ID.
            owner_key: When given, only a row that belongs to this owner is
                deleted.

        Returns:
            True if a row was deleted, False otherwise.
        """
        if not self.is_available():
            return False

        with self._write_lock:
            try:
                if owner_key is not None:
                    cursor = self._conn.execute(
                        "DELETE FROM join_documents WHERE id = ? AND owner_key = ?",
                        (doc_id, owner_key)
                    )
                else:
                    cursor = self._conn.execute(
                        "DELETE FROM join_documents WHERE id = ?", (doc_id,)
                    )
                self._conn.commit()
                return cursor.rowcount > 0
            except Exception as e:
                try:
                    self._conn.rollback()
                except Exception:
                    pass
                logger.error("PuzzleService.delete_document failed: %s", e)
                return False

    def list_documents_for_fragment(self, fl_id: str = None, sys_id: str = None,
                                    *, owner_key: Optional[str] = None) -> List[str]:
        """
        Find document IDs containing a given fragment.

        Args:
            fl_id: Look up by NLI FL ID.
            sys_id: Look up by system ID.
            owner_key: When given, only this owner's documents are returned.

        Returns:
            List of document ID strings.
        """
        if not self.is_available():
            return []

        if fl_id is not None:
            column, value = 'fl_id', fl_id
        elif sys_id is not None:
            column, value = 'sys_id', sys_id
        else:
            return []

        try:
            with self._write_lock:  # serialize on the shared connection
                if owner_key is not None:
                    rows = self._conn.execute(
                        f"SELECT DISTINCT f.doc_id FROM join_document_fragments f "
                        f"JOIN join_documents d ON d.id = f.doc_id "
                        f"WHERE f.{column} = ? AND d.owner_key = ?",
                        (value, owner_key)
                    ).fetchall()
                else:
                    rows = self._conn.execute(
                        f"SELECT DISTINCT doc_id FROM join_document_fragments WHERE {column} = ?",
                        (value,)
                    ).fetchall()
            return [r[0] for r in rows]
        except Exception as e:
            logger.error("PuzzleService.list_documents_for_fragment failed: %s", e)
            return []


# ── Singleton ────────────────────────────────────────────────────────

_service_instance: Optional[PuzzleService] = None


def get_puzzle_service(thread_safe: bool = False) -> PuzzleService:
    """Get or create the singleton PuzzleService instance."""
    global _service_instance
    if _service_instance is None:
        _service_instance = PuzzleService(thread_safe=thread_safe)
    return _service_instance


def reset_puzzle_service():
    """Reset the singleton instance (for testing)."""
    global _service_instance
    _service_instance = None
