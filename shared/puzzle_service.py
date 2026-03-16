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
"""

import json
import logging
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
            if root:
                sidecar_dir = root / _SIDECAR_DIR
                sidecar_dir.mkdir(exist_ok=True)
                db_path = str(sidecar_dir / _SIDECAR_FILENAME)
                auto_detected = True

        if db_path is None:
            logger.warning("PuzzleService: no db_path and project root not found")
            return

        # Only auto-create parent dirs for auto-detected paths
        # For explicit paths, require parent dir to exist
        parent = Path(db_path).parent
        if not parent.exists():
            if auto_detected:
                parent.mkdir(parents=True, exist_ok=True)
            else:
                logger.warning("PuzzleService: parent directory does not exist: %s", parent)
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
            INSERT OR REPLACE INTO meta (key, value) VALUES ('schema_version', '2');

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
        try:
            self._conn.execute("SELECT thumbnail_b64 FROM join_documents LIMIT 0")
        except sqlite3.OperationalError:
            self._conn.execute("ALTER TABLE join_documents ADD COLUMN thumbnail_b64 TEXT DEFAULT ''")
            self._conn.commit()
            logger.info("PuzzleService: migrated schema to v2 (added thumbnail_b64)")

    def is_available(self) -> bool:
        """Check if the service has a valid database connection."""
        return self._conn is not None

    def save_document(self, doc: PuzzleDocument, thumbnail_b64: str = None) -> Optional[str]:
        """
        Save or update a PuzzleDocument.

        Args:
            doc: The PuzzleDocument to save.
            thumbnail_b64: Optional base64-encoded thumbnail PNG. If None,
                preserves existing thumbnail (avoids overwrite on metadata-only saves).

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
              'processed': f.processed}
             for f in doc.fragments],
            ensure_ascii=False
        )

        # Preserve existing thumbnail when not explicitly provided
        if thumbnail_b64 is None:
            try:
                existing = self._conn.execute(
                    "SELECT thumbnail_b64 FROM join_documents WHERE id = ?", (doc.id,)
                ).fetchone()
                thumbnail_b64 = existing['thumbnail_b64'] if existing else ''
            except Exception:
                thumbnail_b64 = ''

        with self._write_lock:
            try:
                self._conn.execute(
                    """INSERT OR REPLACE INTO join_documents
                       (id, title, notes, join_type, fragments_json, thumbnail_b64, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (doc.id, doc.title, doc.notes, doc.join_type,
                     fragments_json, thumbnail_b64, doc.created_at, doc.updated_at)
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
                logger.error("PuzzleService.save_document failed: %s", e)
                return None

    def load_document(self, doc_id: str) -> Optional[PuzzleDocument]:
        """
        Load a PuzzleDocument by ID.

        Returns:
            The PuzzleDocument, or None if not found or unavailable.
        """
        if not self.is_available():
            return None

        try:
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

    def list_documents(self) -> List[Dict]:
        """
        List all puzzle documents, sorted by updated_at DESC.

        Returns:
            List of dicts with id, title, join_type, fragments_json,
            thumbnail_b64, updated_at, shelfmarks_summary.
        """
        if not self.is_available():
            return []

        try:
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
                    d['shelfmarks_summary'] = ''
                results.append(d)
            return results
        except Exception as e:
            logger.error("PuzzleService.list_documents failed: %s", e)
            return []

    def delete_document(self, doc_id: str) -> bool:
        """
        Delete a puzzle document by ID (CASCADE deletes fragment index entries).

        Returns:
            True if a row was deleted, False otherwise.
        """
        if not self.is_available():
            return False

        with self._write_lock:
            try:
                cursor = self._conn.execute(
                    "DELETE FROM join_documents WHERE id = ?", (doc_id,)
                )
                self._conn.commit()
                return cursor.rowcount > 0
            except Exception as e:
                logger.error("PuzzleService.delete_document failed: %s", e)
                return False

    def list_documents_for_fragment(self, fl_id: str = None, sys_id: str = None) -> List[str]:
        """
        Find document IDs containing a given fragment.

        Args:
            fl_id: Look up by NLI FL ID.
            sys_id: Look up by system ID.

        Returns:
            List of document ID strings.
        """
        if not self.is_available():
            return []

        try:
            if fl_id is not None:
                rows = self._conn.execute(
                    "SELECT DISTINCT doc_id FROM join_document_fragments WHERE fl_id = ?",
                    (fl_id,)
                ).fetchall()
            elif sys_id is not None:
                rows = self._conn.execute(
                    "SELECT DISTINCT doc_id FROM join_document_fragments WHERE sys_id = ?",
                    (sys_id,)
                ).fetchall()
            else:
                return []
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
