# -*- coding: utf-8 -*-
"""
FJMS Enrichment Service for accessing FIST/FJMS data from the SQLite sidecar.

This module provides the FjmsService class for querying domain classifications,
scholar join groups, and catalog metadata from the fjms_enrichment.db sidecar
database. Used by both the web app and desktop app.

All methods handle errors gracefully, returning empty results rather than
raising exceptions. When the sidecar database is missing, the service
degrades gracefully (is_available() returns False, all queries return empty).

Thread-safe mode (check_same_thread=False) is available for the NiceGUI
web app which serves concurrent requests from multiple threads.
"""

import logging
import sqlite3
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Default sidecar filename
_SIDECAR_FILENAME = "fjms_enrichment.db"
_SIDECAR_DIR = "fist_data"


def _find_project_root() -> Optional[Path]:
    """Find the project root by looking for libraries.csv up from this file."""
    current = Path(__file__).resolve().parent
    for _ in range(5):  # Up to 5 levels
        if (current / "libraries.csv").exists():
            return current
        current = current.parent
    return None


class FjmsService:
    """Service for accessing FJMS enrichment data from the SQLite sidecar."""

    def __init__(self, db_path: str = None, thread_safe: bool = False):
        """
        Initialize FjmsService.

        Args:
            db_path: Path to fjms_enrichment.db. If None, auto-detect from project root.
            thread_safe: If True, use check_same_thread=False for NiceGUI web app.
                        Desktop app should leave this False (single-threaded).
        """
        self._conn: Optional[sqlite3.Connection] = None
        self._db_path: Optional[str] = None

        # Resolve db_path
        if db_path is None:
            root = _find_project_root()
            if root:
                db_path = str(root / _SIDECAR_DIR / _SIDECAR_FILENAME)

        if db_path is None:
            logger.warning("FjmsService: No db_path provided and project root not found")
            return

        self._db_path = db_path
        db_file = Path(db_path)

        if not db_file.exists():
            logger.warning(f"FjmsService: Sidecar database not found at {db_path}")
            return

        try:
            # Open read-only connection using URI mode
            uri = f"file:{db_path}?mode=ro"
            self._conn = sqlite3.connect(
                uri,
                uri=True,
                check_same_thread=not thread_safe,
                timeout=10.0,
            )
            self._conn.row_factory = sqlite3.Row
            logger.info(f"FjmsService: Connected to {db_path}")
        except Exception as e:
            logger.error(f"FjmsService: Failed to connect to {db_path}: {e}")
            self._conn = None

    def is_available(self) -> bool:
        """Returns True if the sidecar database connection is active."""
        return self._conn is not None

    def get_version(self) -> Optional[str]:
        """
        Get the sidecar database version.

        Returns:
            Version string (e.g., '1.0.0') or None if unavailable.
        """
        if self._conn is None:
            return None
        try:
            cursor = self._conn.execute(
                "SELECT value FROM meta WHERE key = 'version'"
            )
            row = cursor.fetchone()
            return row["value"] if row else None
        except Exception as e:
            logger.error(f"FjmsService.get_version error: {e}")
            return None

    def get_domains(self, sys_id: str) -> list[dict]:
        """
        Get domain classifications for a manuscript.

        Args:
            sys_id: The Alma/system ID for the manuscript.

        Returns:
            List of dicts with keys: domain, domain_heb, parent_domain, parent_domain_heb.
            Returns [] if conn is None or sys_id not found.
        """
        if self._conn is None:
            return []
        try:
            cursor = self._conn.execute(
                "SELECT Domain, DomainHeb, ParentDomain, ParentDomainHeb "
                "FROM domains WHERE AlmaId = ?",
                (sys_id,),
            )
            return [
                {
                    "domain": row["Domain"],
                    "domain_heb": row["DomainHeb"],
                    "parent_domain": row["ParentDomain"],
                    "parent_domain_heb": row["ParentDomainHeb"],
                }
                for row in cursor
            ]
        except Exception as e:
            logger.error(f"FjmsService.get_domains error for {sys_id}: {e}")
            return []

    def get_manuscripts_by_domain(self, domain: str) -> set[str]:
        """
        Get all manuscript IDs classified under a domain.

        Matches both direct domain assignments and parent domain references,
        enabling domain-based search filtering via set intersection.

        Args:
            domain: Domain name in English (e.g., 'Piyyut', 'Letters').

        Returns:
            Set of AlmaId strings. Returns set() if conn is None.
        """
        if self._conn is None:
            return set()
        try:
            cursor = self._conn.execute(
                "SELECT DISTINCT AlmaId FROM domains "
                "WHERE Domain = ? OR ParentDomain = ?",
                (domain, domain),
            )
            return {row["AlmaId"] for row in cursor}
        except Exception as e:
            logger.error(f"FjmsService.get_manuscripts_by_domain error for {domain}: {e}")
            return set()

    def get_all_domains(self) -> list[dict]:
        """
        Get all unique domain names with manuscript counts.

        Useful for populating domain filter dropdowns in the UI.

        Returns:
            List of dicts with keys: domain, domain_heb, count.
            Sorted by count descending. Returns [] if conn is None.
        """
        if self._conn is None:
            return []
        try:
            cursor = self._conn.execute(
                "SELECT Domain, DomainHeb, COUNT(DISTINCT AlmaId) as count "
                "FROM domains GROUP BY Domain ORDER BY count DESC"
            )
            return [
                {
                    "domain": row["Domain"],
                    "domain_heb": row["DomainHeb"],
                    "count": row["count"],
                }
                for row in cursor
            ]
        except Exception as e:
            logger.error(f"FjmsService.get_all_domains error: {e}")
            return []

    def get_join_group(self, sys_id: str) -> list[dict]:
        """
        Get other manuscripts in the same join group(s) as the given manuscript.

        A manuscript may belong to multiple join groups. This returns all other
        members across all groups, excluding the queried manuscript itself.

        Args:
            sys_id: The Alma/system ID for the manuscript.

        Returns:
            List of dicts with keys: alma_id, join_group_id, scholar_name, comment, join_type.
            Returns [] if conn is None or no joins found.
        """
        if self._conn is None:
            return []
        try:
            cursor = self._conn.execute(
                "SELECT AlmaId, JoinGroupId, ScholarName, Comment, JoinType "
                "FROM joins "
                "WHERE JoinGroupId IN (SELECT JoinGroupId FROM joins WHERE AlmaId = ?) "
                "AND AlmaId != ?",
                (sys_id, sys_id),
            )
            return [
                {
                    "alma_id": row["AlmaId"],
                    "join_group_id": row["JoinGroupId"],
                    "scholar_name": row["ScholarName"],
                    "comment": row["Comment"],
                    "join_type": row["JoinType"],
                }
                for row in cursor
            ]
        except Exception as e:
            logger.error(f"FjmsService.get_join_group error for {sys_id}: {e}")
            return []

    def get_catalog(self, sys_id: str) -> Optional[dict]:
        """
        Get catalog metadata for a manuscript.

        Args:
            sys_id: The Alma/system ID for the manuscript.

        Returns:
            Dict with keys: title, title_heb, author_text, copy_date, copy_place,
            description_eng, description_heb, textual_frame_heb, textual_frame_eng.
            Returns None if conn is None or not found.
        """
        if self._conn is None:
            return None
        try:
            cursor = self._conn.execute(
                "SELECT * FROM catalog WHERE AlmaId = ?",
                (sys_id,),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return {
                "title": row["Title"],
                "title_heb": row["TitleHeb"],
                "author_text": row["AuthorText"],
                "copy_date": row["CopyDate"],
                "copy_place": row["CopyPlace"],
                "description_eng": row["DescriptionEng"],
                "description_heb": row["DescriptionHeb"],
                "textual_frame_heb": row["TextualFrameHeb"],
                "textual_frame_eng": row["TextualFrameEng"],
            }
        except Exception as e:
            logger.error(f"FjmsService.get_catalog error for {sys_id}: {e}")
            return None

    def close(self):
        """Close the database connection if open."""
        if self._conn is not None:
            try:
                self._conn.close()
                logger.info("FjmsService: Connection closed")
            except Exception as e:
                logger.error(f"FjmsService.close error: {e}")
            finally:
                self._conn = None


# Module-level singleton pattern
_default_service: Optional[FjmsService] = None


def get_fjms_service(thread_safe: bool = False) -> FjmsService:
    """Get or create the default FjmsService singleton."""
    global _default_service
    if _default_service is None:
        _default_service = FjmsService(thread_safe=thread_safe)
    return _default_service
