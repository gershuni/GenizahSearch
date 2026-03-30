# -*- coding: utf-8 -*-
"""
Service for accessing visual similarity suggestions from the visual_similarity.db sidecar.

This sidecar contains SVM-scored visual similarity pairs from FJMS's image analysis
pipeline (Image_BestMarkForJoin table in FIST.db). The data is server-only; desktop
fetches per-manuscript via the /api/visual_suggestions/{sys_id} endpoint.

Usage:
    from shared.visual_similarity_service import get_vs_service
    svc = get_vs_service()
    suggestions = svc.get_suggestions("990001234500205171")
"""

import logging
import os
import sqlite3
import threading
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_SIDECAR_DIR = "fist_data"
_SIDECAR_FILENAME = "visual_similarity.db"


def _find_project_root() -> Optional[Path]:
    """Find the project root by looking for libraries.csv up from this file."""
    current = Path(__file__).resolve().parent
    for _ in range(5):
        if (current / "libraries.csv").exists():
            return current
        current = current.parent
    return None


class VisualSimilarityService:
    """Service for querying visual similarity suggestions from the SQLite sidecar."""

    def __init__(self, db_path: str = None, thread_safe: bool = True):
        """
        Initialize VisualSimilarityService.

        Args:
            db_path: Path to visual_similarity.db. If None, auto-detect.
            thread_safe: If True, use per-thread connections via ThreadLocalConnection.
        """
        self._conn = None
        self._db_path: Optional[str] = None

        # Resolve db_path
        if db_path is None:
            user_path = os.path.join(
                os.environ.get('LOCALAPPDATA', ''),
                'GenizahSearchPro', 'data', _SIDECAR_DIR, _SIDECAR_FILENAME
            )
            if os.path.isfile(user_path):
                db_path = user_path
            else:
                root = _find_project_root()
                if root:
                    db_path = str(root / _SIDECAR_DIR / _SIDECAR_FILENAME)

        if db_path is None:
            logger.warning("VisualSimilarityService: No db_path provided and project root not found")
            return

        self._db_path = db_path
        db_file = Path(db_path)

        if not db_file.exists():
            logger.warning(f"VisualSimilarityService: Sidecar not found at {db_path}")
            return

        try:
            uri = f"file:{db_path}?mode=ro"
            if thread_safe:
                from shared.thread_local_db import ThreadLocalConnection
                self._conn = ThreadLocalConnection(
                    uri, row_factory=sqlite3.Row, timeout=10.0
                )
            else:
                self._conn = sqlite3.connect(
                    uri, uri=True, check_same_thread=True, timeout=10.0
                )
                self._conn.row_factory = sqlite3.Row
            logger.info(f"VisualSimilarityService: Connected to {db_path}")
        except Exception as e:
            logger.error(f"VisualSimilarityService: Failed to connect: {e}")
            self._conn = None

    def is_available(self) -> bool:
        """Return True if the sidecar database is connected."""
        return self._conn is not None

    def get_suggestions(self, sys_id: str, limit: int = 200) -> list:
        """Return ranked visual similarity suggestions for a manuscript.

        Args:
            sys_id: System number (AlmaId) of the manuscript.
            limit: Maximum number of suggestions to return.

        Returns:
            List of dicts: {'alma_id': str, 'svm_score': float, 'rank': int}
            Ordered by svm_score descending, rank 1-indexed.
        """
        if not self._conn:
            return []
        try:
            alma_id = int(sys_id)
        except (ValueError, TypeError):
            return []

        try:
            cursor = self._conn.execute(
                'SELECT alma_id_b, svm_score FROM visual_suggestions '
                'WHERE alma_id_a = ? ORDER BY svm_score DESC LIMIT ?',
                (alma_id, limit)
            )
            rows = cursor.fetchall()
            return [
                {'alma_id': str(row[0]), 'svm_score': row[1], 'rank': i + 1}
                for i, row in enumerate(rows)
            ]
        except Exception as e:
            logger.error(f"get_suggestions error for {sys_id}: {e}")
            return []

    def has_suggestions(self, sys_id: str) -> bool:
        """Check if a manuscript has any visual similarity suggestions."""
        if not self._conn:
            return False
        try:
            alma_id = int(sys_id)
        except (ValueError, TypeError):
            return False
        try:
            cursor = self._conn.execute(
                'SELECT 1 FROM visual_suggestions WHERE alma_id_a = ? LIMIT 1',
                (alma_id,)
            )
            return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"has_suggestions error for {sys_id}: {e}")
            return False

    def get_suggestion_count(self, sys_id: str) -> int:
        """Return the number of visual similarity suggestions for a manuscript."""
        if not self._conn:
            return 0
        try:
            alma_id = int(sys_id)
        except (ValueError, TypeError):
            return 0
        try:
            cursor = self._conn.execute(
                'SELECT COUNT(*) FROM visual_suggestions WHERE alma_id_a = ?',
                (alma_id,)
            )
            row = cursor.fetchone()
            return row[0] if row else 0
        except Exception as e:
            logger.error(f"get_suggestion_count error for {sys_id}: {e}")
            return 0

    def batch_has_suggestions(self, sys_ids: list) -> dict:
        """Check which sys_ids have visual suggestions.

        Args:
            sys_ids: List of sys_id strings. Limited to 500 entries.

        Returns:
            Dict mapping sys_id -> bool.
        """
        if not self._conn:
            return {sid: False for sid in sys_ids}

        # Enforce 500 limit
        if len(sys_ids) > 500:
            logger.warning(f"batch_has_suggestions: truncating {len(sys_ids)} IDs to 500")
            sys_ids = sys_ids[:500]

        # Convert to ints, tracking valid ones
        id_map = {}  # int_id -> str_id
        for sid in sys_ids:
            try:
                id_map[int(sid)] = sid
            except (ValueError, TypeError):
                pass

        result = {sid: False for sid in sys_ids}
        if not id_map:
            return result

        try:
            placeholders = ','.join('?' * len(id_map))
            cursor = self._conn.execute(
                f'SELECT DISTINCT alma_id_a FROM visual_suggestions '
                f'WHERE alma_id_a IN ({placeholders})',
                list(id_map.keys())
            )
            found = {row[0] for row in cursor.fetchall()}
            for int_id, str_id in id_map.items():
                result[str_id] = int_id in found
        except Exception as e:
            logger.error(f"batch_has_suggestions error: {e}")

        return result

    def get_suggestion_partners(self, sys_ids: list, mode: str = 'union') -> set:
        """Get partner sys_ids from visual suggestions for given manuscripts.

        Args:
            sys_ids: List of source manuscript sys_ids.
            mode: 'union' to combine all partners, 'intersection' for common partners only.

        Returns:
            Set of partner sys_id strings.
        """
        if not self._conn or not sys_ids:
            return set()

        # Convert to ints, filtering invalid
        int_ids = []
        id_map = {}  # int -> str
        for sid in sys_ids:
            try:
                alma_id = int(sid)
                int_ids.append(alma_id)
                id_map[alma_id] = sid
            except (ValueError, TypeError):
                continue

        if not int_ids:
            return set()

        # Batch query with IN (...) for union mode; per-source for intersection
        partner_sets = []
        if mode == 'union' and len(int_ids) > 1:
            # Single batched query for union
            try:
                placeholders = ','.join('?' * len(int_ids))
                cursor = self._conn.execute(
                    f'SELECT DISTINCT alma_id_b FROM visual_suggestions '
                    f'WHERE alma_id_a IN ({placeholders})',
                    int_ids
                )
                return {str(row[0]) for row in cursor.fetchall()}
            except Exception as e:
                logger.error(f"get_suggestion_partners batch error: {e}")
                return set()
        else:
            # Per-source queries needed for intersection
            for alma_id in int_ids:
                try:
                    cursor = self._conn.execute(
                        'SELECT alma_id_b FROM visual_suggestions WHERE alma_id_a = ?',
                        (alma_id,)
                    )
                    partners = {str(row[0]) for row in cursor.fetchall()}
                    partner_sets.append(partners)
                except Exception as e:
                    logger.error(f"get_suggestion_partners error for {alma_id}: {e}")

        if not partner_sets:
            return set()

        if mode == 'intersection':
            result = partner_sets[0]
            for ps in partner_sets[1:]:
                result = result & ps
            return result
        else:  # union
            result = set()
            for ps in partner_sets:
                result |= ps
            return result

    def get_db_version(self) -> dict:
        """Return database version metadata for cache staleness detection.

        Returns:
            Dict with keys: version, import_date, pair_count, manuscript_count.
            Empty dict if unavailable.
        """
        if not self._conn:
            return {}
        try:
            cursor = self._conn.execute('SELECT key, value FROM vs_metadata')
            return {row[0]: row[1] for row in cursor.fetchall()}
        except Exception as e:
            logger.error(f"get_db_version error: {e}")
            return {}

    def close(self):
        """Close the database connection."""
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None


# ── Singleton ─────────────────────────────────────────────────────

_vs_instance = None
_vs_lock = threading.Lock()


def get_vs_service(thread_safe: bool = True) -> VisualSimilarityService:
    """Get or create the default VisualSimilarityService singleton."""
    global _vs_instance
    with _vs_lock:
        if _vs_instance is None:
            _vs_instance = VisualSimilarityService(thread_safe=thread_safe)
        return _vs_instance


def reset_vs_service():
    """Reset the singleton VisualSimilarityService instance."""
    global _vs_instance
    with _vs_lock:
        if _vs_instance is not None:
            _vs_instance.close()
        _vs_instance = None
