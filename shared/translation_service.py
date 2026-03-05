# -*- coding: utf-8 -*-
"""
Translation Service for accessing pre-computed translations from sidecar databases.

This module provides the TranslationService class for querying translated metadata
from pgp.db (pgp_translations table) and fjms_enrichment.db (fjms_translations table).
Used by both the web app and desktop app at runtime for displaying translations.

All translations are pre-computed by batch scripts and stored in sidecar databases.
This service is read-only -- it never calls the Dicta API.

Follows the same pattern as FjmsService and PgpService:
- Constructor auto-finds sidecars or accepts explicit paths
- Graceful degradation when databases/tables are missing
- Thread-safe mode for NiceGUI web app (check_same_thread=False)

Schema creation helpers (ensure_pgp_translations_table, ensure_fjms_translations_table)
are provided for use by batch translation scripts.
"""

import logging
import os
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# =============================================================================
# Schema Definitions
# =============================================================================

PGP_TRANSLATIONS_SCHEMA = """
CREATE TABLE IF NOT EXISTS pgp_translations (
    pgpid INTEGER PRIMARY KEY,
    description_he TEXT,
    document_type_he TEXT,
    translated_at TEXT,
    model_version TEXT DEFAULT 'dictalm2.0'
)
"""

FJMS_TRANSLATIONS_SCHEMA = """
CREATE TABLE IF NOT EXISTS fjms_translations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    alma_id TEXT NOT NULL,
    field_name TEXT NOT NULL,
    signature_id INTEGER,
    original_text TEXT NOT NULL,
    translated_text TEXT NOT NULL,
    direction TEXT NOT NULL,
    translated_at TEXT,
    model_version TEXT DEFAULT 'dictalm2.0'
)
"""

FJMS_TRANSLATIONS_INDEX_ALMA = (
    "CREATE INDEX IF NOT EXISTS idx_fjms_trans_alma "
    "ON fjms_translations(alma_id)"
)

FJMS_TRANSLATIONS_INDEX_FIELD = (
    "CREATE INDEX IF NOT EXISTS idx_fjms_trans_field "
    "ON fjms_translations(alma_id, field_name)"
)


# =============================================================================
# Schema Creation Helpers
# =============================================================================


def ensure_pgp_translations_table(conn: sqlite3.Connection) -> None:
    """Create the pgp_translations table if it does not exist.

    Args:
        conn: An open sqlite3 connection to the PGP sidecar database.
    """
    conn.execute(PGP_TRANSLATIONS_SCHEMA)
    conn.commit()


def ensure_fjms_translations_table(conn: sqlite3.Connection) -> None:
    """Create the fjms_translations table and indexes if they do not exist.

    Args:
        conn: An open sqlite3 connection to the FJMS sidecar database.
    """
    conn.execute(FJMS_TRANSLATIONS_SCHEMA)
    conn.execute(FJMS_TRANSLATIONS_INDEX_ALMA)
    conn.execute(FJMS_TRANSLATIONS_INDEX_FIELD)
    conn.commit()


# =============================================================================
# Helper: Find project root
# =============================================================================


def _find_project_root() -> Optional[Path]:
    """Find the project root by looking for libraries.csv up from this file."""
    current = Path(__file__).resolve().parent
    for _ in range(5):
        if (current / "libraries.csv").exists():
            return current
        current = current.parent
    return None


# =============================================================================
# TranslationService
# =============================================================================


class TranslationService:
    """Read-only service for accessing pre-computed translations from sidecar databases.

    Connects to pgp.db and fjms_enrichment.db sidecar databases and queries
    translation tables for Hebrew/English translations of scholarly metadata.

    Usage:
        svc = TranslationService(thread_safe=True)  # For web app
        if svc.pgp_available():
            he = svc.get_pgp_description_he(1001)
        svc.close()
    """

    def __init__(
        self,
        pgp_db_path: Optional[str] = None,
        fjms_db_path: Optional[str] = None,
        thread_safe: bool = False,
    ):
        """Initialize TranslationService.

        Args:
            pgp_db_path: Explicit path to pgp.db. If None, auto-detect.
            fjms_db_path: Explicit path to fjms_enrichment.db. If None, auto-detect.
            thread_safe: If True, use check_same_thread=False for NiceGUI web app.
        """
        self._pgp_conn: Optional[sqlite3.Connection] = None
        self._fjms_conn: Optional[sqlite3.Connection] = None
        self._titles_conn: Optional[sqlite3.Connection] = None
        self._pgp_has_translations = False
        self._fjms_has_translations = False
        self._titles_has_translations = False

        # Resolve PGP database path
        if pgp_db_path is None:
            pgp_db_path = self._find_pgp_db()
        if pgp_db_path and os.path.isfile(pgp_db_path):
            try:
                self._pgp_conn = sqlite3.connect(
                    pgp_db_path, check_same_thread=not thread_safe
                )
                self._pgp_conn.row_factory = sqlite3.Row
                self._pgp_has_translations = self._table_exists(
                    self._pgp_conn, "pgp_translations"
                )
            except Exception as e:
                logger.warning("Failed to connect to PGP sidecar: %s", e)

        # Resolve FJMS database path
        if fjms_db_path is None:
            fjms_db_path = self._find_fjms_db()
        if fjms_db_path and os.path.isfile(fjms_db_path):
            try:
                self._fjms_conn = sqlite3.connect(
                    fjms_db_path, check_same_thread=not thread_safe
                )
                self._fjms_conn.row_factory = sqlite3.Row
                self._fjms_has_translations = self._table_exists(
                    self._fjms_conn, "fjms_translations"
                )
            except Exception as e:
                logger.warning("Failed to connect to FJMS sidecar: %s", e)

        # Resolve libraries_translations.db path
        titles_db_path = self._find_titles_db()
        if titles_db_path and os.path.isfile(titles_db_path):
            try:
                self._titles_conn = sqlite3.connect(
                    titles_db_path, check_same_thread=not thread_safe
                )
                self._titles_conn.row_factory = sqlite3.Row
                self._titles_has_translations = self._table_exists(
                    self._titles_conn, "title_translations"
                )
            except Exception as e:
                logger.warning("Failed to connect to titles sidecar: %s", e)

    # -------------------------------------------------------------------------
    # Availability Checks
    # -------------------------------------------------------------------------

    def is_available(self) -> bool:
        """True if at least one sidecar has translation tables."""
        return self._pgp_has_translations or self._fjms_has_translations

    def pgp_available(self) -> bool:
        """True if pgp_translations table exists in the PGP sidecar."""
        return self._pgp_has_translations

    def fjms_available(self) -> bool:
        """True if fjms_translations table exists in the FJMS sidecar."""
        return self._fjms_has_translations

    # -------------------------------------------------------------------------
    # PGP Translation Methods
    # -------------------------------------------------------------------------

    def get_pgp_description_he(self, pgpid: int) -> Optional[str]:
        """Get Hebrew translation of a PGP document description.

        Args:
            pgpid: PGP document ID.

        Returns:
            Hebrew description text, or None if not found.
        """
        if not self._pgp_has_translations or not self._pgp_conn:
            return None
        try:
            row = self._pgp_conn.execute(
                "SELECT description_he FROM pgp_translations WHERE pgpid = ?",
                (pgpid,),
            ).fetchone()
            return row[0] if row and row[0] else None
        except Exception as e:
            logger.warning("Error reading PGP translation for pgpid=%s: %s", pgpid, e)
            return None

    def get_pgp_document_type_he(self, pgpid: int) -> Optional[str]:
        """Get Hebrew translation of a PGP document type.

        Args:
            pgpid: PGP document ID.

        Returns:
            Hebrew document type text, or None if not found.
        """
        if not self._pgp_has_translations or not self._pgp_conn:
            return None
        try:
            row = self._pgp_conn.execute(
                "SELECT document_type_he FROM pgp_translations WHERE pgpid = ?",
                (pgpid,),
            ).fetchone()
            return row[0] if row and row[0] else None
        except Exception as e:
            logger.warning(
                "Error reading PGP doc type translation for pgpid=%s: %s", pgpid, e
            )
            return None

    def get_pgp_translations_batch(self, pgpids: List[int]) -> Dict[int, dict]:
        """Batch lookup of PGP translations.

        Args:
            pgpids: List of PGP document IDs.

        Returns:
            Dict of {pgpid: {"description_he": str, "document_type_he": str}}
            for found entries only.
        """
        if not self._pgp_has_translations or not self._pgp_conn or not pgpids:
            return {}
        try:
            placeholders = ",".join("?" * len(pgpids))
            rows = self._pgp_conn.execute(
                f"SELECT pgpid, description_he, document_type_he "
                f"FROM pgp_translations WHERE pgpid IN ({placeholders})",
                pgpids,
            ).fetchall()
            return {
                row[0]: {
                    "description_he": row[1],
                    "document_type_he": row[2],
                }
                for row in rows
            }
        except Exception as e:
            logger.warning("Error in PGP batch translation lookup: %s", e)
            return {}

    # -------------------------------------------------------------------------
    # FJMS Translation Methods
    # -------------------------------------------------------------------------

    def get_fjms_translation(
        self, alma_id: str, field_name: str
    ) -> Optional[str]:
        """Get translated text for a specific FJMS catalog field.

        Args:
            alma_id: FJMS AlmaId identifier.
            field_name: Field name (e.g., 'Title', 'TitleHeb', 'AuthorText').

        Returns:
            Translated text, or None if not found.
        """
        if not self._fjms_has_translations or not self._fjms_conn:
            return None
        try:
            row = self._fjms_conn.execute(
                "SELECT translated_text FROM fjms_translations "
                "WHERE alma_id = ? AND field_name = ?",
                (alma_id, field_name),
            ).fetchone()
            return row[0] if row and row[0] else None
        except Exception as e:
            logger.warning(
                "Error reading FJMS translation for %s/%s: %s",
                alma_id,
                field_name,
                e,
            )
            return None

    def get_fjms_free_desc_en(
        self, alma_id: str, signature_id: int
    ) -> Optional[str]:
        """Get English translation of an FJMS free description.

        Args:
            alma_id: FJMS AlmaId identifier.
            signature_id: Signature ID for the free description.

        Returns:
            Translated free description text, or None if not found.
        """
        if not self._fjms_has_translations or not self._fjms_conn:
            return None
        try:
            row = self._fjms_conn.execute(
                "SELECT translated_text FROM fjms_translations "
                "WHERE alma_id = ? AND field_name = 'FreeDesc' AND signature_id = ?",
                (alma_id, signature_id),
            ).fetchone()
            return row[0] if row and row[0] else None
        except Exception as e:
            logger.warning(
                "Error reading FJMS free desc translation for %s/%s: %s",
                alma_id,
                signature_id,
                e,
            )
            return None

    def get_fjms_translations_batch(
        self, alma_ids: List[str]
    ) -> Dict[str, dict]:
        """Batch lookup of FJMS translations.

        Args:
            alma_ids: List of FJMS AlmaId identifiers.

        Returns:
            Dict of {alma_id: {field_name: translated_text}} for found entries.
        """
        if not self._fjms_has_translations or not self._fjms_conn or not alma_ids:
            return {}
        try:
            placeholders = ",".join("?" * len(alma_ids))
            rows = self._fjms_conn.execute(
                f"SELECT alma_id, field_name, translated_text "
                f"FROM fjms_translations WHERE alma_id IN ({placeholders})",
                alma_ids,
            ).fetchall()

            result: Dict[str, dict] = {}
            for row in rows:
                aid = row[0]
                if aid not in result:
                    result[aid] = {}
                result[aid][row[1]] = row[2]
            return result
        except Exception as e:
            logger.warning("Error in FJMS batch translation lookup: %s", e)
            return {}

    # -------------------------------------------------------------------------
    # Translation Search Methods (SQLite metadata search, NOT Tantivy)
    # -------------------------------------------------------------------------

    def search_pgp_by_translation(
        self, query: str, lang: str
    ) -> set:
        """Search PGP translations for descriptions matching query.

        Simple LIKE search on pgp_translations table. This is an additional
        metadata lookup, NOT a replacement for Tantivy full-text search.

        Args:
            query: Search query string.
            lang: Target language ('he' for Hebrew descriptions, 'en' for English).

        Returns:
            Set of pgpid integers matching the query.
        """
        if not self._pgp_has_translations or not self._pgp_conn:
            return set()
        if lang == 'en':
            # PGP descriptions are already in English, no translation search needed
            return set()
        if not query or not query.strip():
            return set()
        try:
            pattern = f"%{query.strip()}%"
            rows = self._pgp_conn.execute(
                "SELECT pgpid FROM pgp_translations WHERE description_he LIKE ?",
                (pattern,),
            ).fetchall()
            return {row[0] for row in rows}
        except Exception as e:
            logger.warning("Error searching PGP translations: %s", e)
            return set()

    def search_fjms_by_translation(
        self, query: str, lang: str
    ) -> set:
        """Search FJMS translations for matching translated_text.

        Simple LIKE search on fjms_translations table.

        Args:
            query: Search query string.
            lang: Target language ('he' or 'en').

        Returns:
            Set of alma_id strings matching the query.
        """
        if not self._fjms_has_translations or not self._fjms_conn:
            return set()
        if not query or not query.strip():
            return set()
        try:
            pattern = f"%{query.strip()}%"
            rows = self._fjms_conn.execute(
                "SELECT DISTINCT alma_id FROM fjms_translations WHERE translated_text LIKE ?",
                (pattern,),
            ).fetchall()
            return {row[0] for row in rows}
        except Exception as e:
            logger.warning("Error searching FJMS translations: %s", e)
            return set()

    def get_pgp_translations_by_sys_ids(
        self, sys_ids: List[str]
    ) -> Dict[str, dict]:
        """Get PGP translations keyed by sys_id (for search result display).

        Joins document_fragments with pgp_translations to map sys_id -> translation data.

        Args:
            sys_ids: List of sys_ids to look up.

        Returns:
            Dict of {sys_id: {"description_he": str, "document_type_he": str}}
        """
        if not self._pgp_has_translations or not self._pgp_conn or not sys_ids:
            return {}
        try:
            result = {}
            batch_size = 400
            for i in range(0, len(sys_ids), batch_size):
                batch = sys_ids[i:i + batch_size]
                placeholders = ",".join("?" * len(batch))
                rows = self._pgp_conn.execute(
                    f"SELECT df.sys_id, pt.description_he, pt.document_type_he "
                    f"FROM document_fragments df "
                    f"JOIN pgp_translations pt ON df.document_id = pt.pgpid "
                    f"WHERE df.sys_id IN ({placeholders})",
                    batch,
                ).fetchall()
                for row in rows:
                    result[row[0]] = {
                        "description_he": row[1],
                        "document_type_he": row[2],
                    }
            return result
        except Exception as e:
            logger.warning("Error in PGP translations by sys_id lookup: %s", e)
            return {}

    def get_translated_match_sys_ids(
        self, query: str, sys_ids: List[str]
    ) -> set:
        """Find sys_ids whose PGP translations match the query.

        Joins document_fragments with pgp_translations to map sys_id -> pgpid,
        then checks if the translated description matches the query via LIKE.

        Args:
            query: Search query string.
            sys_ids: List of sys_ids to check (from search results).

        Returns:
            Set of sys_id strings that have a translated match.
        """
        if not self._pgp_has_translations or not self._pgp_conn:
            return set()
        if not query or not query.strip() or not sys_ids:
            return set()
        try:
            pattern = f"%{query.strip()}%"
            result_set = set()
            batch_size = 400
            for i in range(0, len(sys_ids), batch_size):
                batch = sys_ids[i:i + batch_size]
                placeholders = ",".join("?" * len(batch))
                rows = self._pgp_conn.execute(
                    f"SELECT DISTINCT df.sys_id "
                    f"FROM document_fragments df "
                    f"JOIN pgp_translations pt ON df.document_id = pt.pgpid "
                    f"WHERE df.sys_id IN ({placeholders}) "
                    f"AND pt.description_he LIKE ?",
                    batch + [pattern],
                ).fetchall()
                result_set.update(row[0] for row in rows)
            return result_set
        except Exception as e:
            logger.warning("Error finding translated match sys_ids: %s", e)
            return set()

    # -------------------------------------------------------------------------
    # Title Translation Methods (from libraries_translations.db)
    # -------------------------------------------------------------------------

    def titles_available(self) -> bool:
        """True if libraries_translations.db has title_translations table."""
        return self._titles_has_translations

    def get_title_translations_batch(
        self, sys_ids: List[str]
    ) -> Dict[str, dict]:
        """Batch lookup of title translations by system_number.

        Args:
            sys_ids: List of system_number strings to look up.

        Returns:
            Dict of {system_number: {"original_title": str, "english_title": str,
            "hebrew_title": str, "source": str}} for found entries only.
        """
        if not self._titles_has_translations or not self._titles_conn or not sys_ids:
            return {}
        try:
            result = {}
            batch_size = 400
            for i in range(0, len(sys_ids), batch_size):
                batch = sys_ids[i:i + batch_size]
                placeholders = ",".join("?" * len(batch))
                rows = self._titles_conn.execute(
                    f"SELECT system_number, original_title, english_title, "
                    f"hebrew_title, source "
                    f"FROM title_translations WHERE system_number IN ({placeholders})",
                    batch,
                ).fetchall()
                for row in rows:
                    result[row[0]] = {
                        "original_title": row[1],
                        "english_title": row[2],
                        "hebrew_title": row[3],
                        "source": row[4],
                    }
            return result
        except Exception as e:
            logger.warning("Error in title translations batch lookup: %s", e)
            return {}

    def get_title_translation(self, sys_id: str) -> Optional[dict]:
        """Get title translation for a single system_number.

        Returns:
            Dict with original_title, english_title, hebrew_title, source
            or None if not found.
        """
        if not self._titles_has_translations or not self._titles_conn:
            return None
        try:
            row = self._titles_conn.execute(
                "SELECT original_title, english_title, hebrew_title, source "
                "FROM title_translations WHERE system_number = ?",
                (sys_id,),
            ).fetchone()
            if not row:
                return None
            return {
                "original_title": row[0],
                "english_title": row[1],
                "hebrew_title": row[2],
                "source": row[3],
            }
        except Exception as e:
            logger.warning("Error reading title translation for %s: %s", sys_id, e)
            return None

    # -------------------------------------------------------------------------
    # No-Overwrite Safety Check
    # -------------------------------------------------------------------------

    def has_existing_translation(
        self, pgpid: int, field: str
    ) -> bool:
        """Check if a PGP translation already exists for a given field.

        Used by batch scripts to avoid overwriting existing translations.

        Args:
            pgpid: PGP document ID.
            field: Column name in pgp_translations (e.g., 'description_he').

        Returns:
            True if translation exists and is non-empty, False otherwise.
        """
        if not self._pgp_has_translations or not self._pgp_conn:
            return False
        try:
            row = self._pgp_conn.execute(
                f"SELECT {field} FROM pgp_translations WHERE pgpid = ?",
                (pgpid,),
            ).fetchone()
            return bool(row and row[0])
        except Exception as e:
            logger.warning(
                "Error checking existing translation for pgpid=%s: %s", pgpid, e
            )
            return False

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    def close(self) -> None:
        """Close database connections."""
        if self._pgp_conn:
            try:
                self._pgp_conn.close()
            except Exception:
                pass
            self._pgp_conn = None
        if self._fjms_conn:
            try:
                self._fjms_conn.close()
            except Exception:
                pass
            self._fjms_conn = None
        if self._titles_conn:
            try:
                self._titles_conn.close()
            except Exception:
                pass
            self._titles_conn = None

    # -------------------------------------------------------------------------
    # Internal Helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
        """Check if a table exists in the database."""
        try:
            row = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,),
            ).fetchone()
            return row is not None
        except Exception:
            return False

    @staticmethod
    def _find_pgp_db() -> Optional[str]:
        """Auto-detect pgp.db location."""
        # Check LOCALAPPDATA first (user-updated sidecar)
        user_path = os.path.join(
            os.environ.get("LOCALAPPDATA", ""),
            "GenizahSearchPro",
            "data",
            "pgp_data",
            "pgp.db",
        )
        if os.path.isfile(user_path):
            return user_path

        # Fall back to project root
        root = _find_project_root()
        if root:
            candidate = root / "pgp_data" / "pgp.db"
            if candidate.is_file():
                return str(candidate)
        return None

    @staticmethod
    def _find_titles_db() -> Optional[str]:
        """Auto-detect libraries_translations.db location."""
        # Check LOCALAPPDATA first
        user_path = os.path.join(
            os.environ.get("LOCALAPPDATA", ""),
            "GenizahSearchPro",
            "data",
            "libraries_translations.db",
        )
        if os.path.isfile(user_path):
            return user_path

        # Fall back to project root
        root = _find_project_root()
        if root:
            candidate = root / "libraries_translations.db"
            if candidate.is_file():
                return str(candidate)
        return None

    @staticmethod
    def _find_fjms_db() -> Optional[str]:
        """Auto-detect fjms_enrichment.db location."""
        # Check LOCALAPPDATA first
        user_path = os.path.join(
            os.environ.get("LOCALAPPDATA", ""),
            "GenizahSearchPro",
            "data",
            "fist_data",
            "fjms_enrichment.db",
        )
        if os.path.isfile(user_path):
            return user_path

        # Fall back to project root
        root = _find_project_root()
        if root:
            candidate = root / "fist_data" / "fjms_enrichment.db"
            if candidate.is_file():
                return str(candidate)
        return None
