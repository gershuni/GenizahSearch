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
import re
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

    def get_domains_for_sys_ids(self, sys_ids: list[str]) -> dict:
        """
        Get domain classifications for multiple manuscripts in batch.

        More efficient than calling get_domains() per sys_id when processing
        search results. Uses batched IN queries to stay within SQLite limits.

        Args:
            sys_ids: List of Alma/system IDs.

        Returns:
            Dict mapping sys_id -> list of domain dicts.
            Each domain dict has keys: domain, domain_heb, parent_domain, parent_domain_heb.
        """
        if not self._conn or not sys_ids:
            return {}
        try:
            result = {}
            # Batch to stay under SQLite variable limit (999)
            batch_size = 500
            for i in range(0, len(sys_ids), batch_size):
                batch = sys_ids[i:i + batch_size]
                placeholders = ','.join('?' * len(batch))
                cursor = self._conn.execute(
                    f"SELECT AlmaId, Domain, DomainHeb, ParentDomain, ParentDomainHeb "
                    f"FROM domains WHERE AlmaId IN ({placeholders})",
                    batch,
                )
                for row in cursor:
                    sid = row["AlmaId"]
                    if sid not in result:
                        result[sid] = []
                    result[sid].append({
                        "domain": row["Domain"],
                        "domain_heb": row["DomainHeb"],
                        "parent_domain": row["ParentDomain"],
                        "parent_domain_heb": row["ParentDomainHeb"],
                    })
            return result
        except Exception as e:
            logger.error(f"FjmsService.get_domains_for_sys_ids error: {e}")
            return {}

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

    def get_domain_hierarchy(self) -> dict:
        """
        Get domain hierarchy with counts, grouped by parent domain.

        Returns:
            Dict mapping parent_domain -> {
                'parent_domain_heb': str,
                'count': int,  # total manuscripts under this parent (including children)
                'children': [{'domain': str, 'domain_heb': str, 'count': int}, ...]
            }
            Sorted by parent count descending, children by count descending within each parent.
            Returns {} if conn is None.
        """
        if self._conn is None:
            return {}
        try:
            # Query all domain entries with counts
            cursor = self._conn.execute(
                "SELECT Domain, DomainHeb, ParentDomain, ParentDomainHeb, "
                "COUNT(DISTINCT AlmaId) as count "
                "FROM domains GROUP BY Domain, ParentDomain ORDER BY count DESC"
            )
            rows = cursor.fetchall()

            # Build hierarchy: map parent -> {parent_domain_heb, count, children[]}
            hierarchy = {}
            parent_counts = {}  # Track total counts per parent

            for row in rows:
                domain = row["Domain"]
                domain_heb = row["DomainHeb"]
                parent = row["ParentDomain"]
                parent_heb = row["ParentDomainHeb"]
                count = row["count"]

                # If this domain HAS a parent (not a root domain)
                if parent and parent != domain:
                    if parent not in hierarchy:
                        hierarchy[parent] = {
                            'parent_domain_heb': parent_heb,
                            'count': 0,
                            'children': []
                        }
                    hierarchy[parent]['children'].append({
                        'domain': domain,
                        'domain_heb': domain_heb,
                        'count': count
                    })
                    hierarchy[parent]['count'] += count
                    parent_counts[parent] = parent_counts.get(parent, 0) + count
                else:
                    # Root-level domain (Domain == ParentDomain or no parent)
                    if domain not in hierarchy:
                        hierarchy[domain] = {
                            'parent_domain_heb': domain_heb,
                            'count': count,
                            'children': []
                        }
                    else:
                        # Already exists as parent, just update count
                        hierarchy[domain]['count'] += count
                    parent_counts[domain] = parent_counts.get(domain, 0) + count

            # Deduplicate: if a domain appears as both a child and a standalone
            # root (e.g., "Piyyut" with ParentDomain=NULL AND ParentDomain="Piyut and its Interpretation"),
            # merge the root count into the child entry and remove the standalone root.
            child_domains = set()
            for info in hierarchy.values():
                for child in info.get('children', []):
                    child_domains.add(child['domain'])
            for child_name in child_domains:
                if child_name in hierarchy:
                    # Merge root count into child entry
                    root_count = hierarchy[child_name].get('count', 0)
                    # Find and update the child entry in its parent
                    for info in hierarchy.values():
                        for child in info.get('children', []):
                            if child['domain'] == child_name:
                                child['count'] += root_count
                                break
                    # Also move any children of the standalone root under the parent
                    orphan_children = hierarchy[child_name].get('children', [])
                    if orphan_children:
                        for info in hierarchy.values():
                            for child in info.get('children', []):
                                if child['domain'] == child_name:
                                    # Can't nest deeper, so promote orphans to same parent level
                                    info['children'].extend(orphan_children)
                                    break
                    del hierarchy[child_name]

            # Sort children within each parent by count descending
            for parent in hierarchy:
                hierarchy[parent]['children'].sort(key=lambda x: x['count'], reverse=True)

            # Recalculate parent counts after dedup
            for parent_name, info in hierarchy.items():
                child_total = sum(c['count'] for c in info.get('children', []))
                if child_total > info.get('count', 0):
                    info['count'] = child_total
                    parent_counts[parent_name] = child_total

            # Return sorted by parent count
            return dict(sorted(hierarchy.items(), key=lambda x: parent_counts.get(x[0], 0), reverse=True))
        except Exception as e:
            logger.error(f"FjmsService.get_domain_hierarchy error: {e}")
            return {}

    @staticmethod
    def _split_concat(val):
        """Split GROUP_CONCAT result into list of non-empty strings."""
        if not val:
            return []
        return [v.strip() for v in val.split(',') if v.strip()]

    def get_join_group(self, sys_id: str) -> list[dict]:
        """
        Get other manuscripts in the same join group(s) as the given manuscript.

        A manuscript may belong to multiple join groups. If the same partner
        appears in multiple groups, it is returned once with all distinct
        scholar names and join types aggregated into lists.

        Args:
            sys_id: The Alma/system ID for the manuscript.

        Returns:
            List of dicts with keys:
                - alma_id (str): Partner manuscript ID
                - join_group_ids (list[int]): All join group IDs containing this partner
                - scholar_names (list[str]): All distinct scholars who identified this join
                - join_types (list[str]): All distinct non-NULL join types across groups
                - comment (str or None): Comments joined with '; ' if multiple
            Returns [] if conn is None or no joins found.
        """
        if self._conn is None:
            return []
        try:
            cursor = self._conn.execute(
                "SELECT AlmaId, "
                "       GROUP_CONCAT(DISTINCT JoinGroupId) as JoinGroupIds, "
                "       GROUP_CONCAT(DISTINCT ScholarName) as ScholarNames, "
                "       GROUP_CONCAT(DISTINCT Comment) as Comments, "
                "       GROUP_CONCAT(DISTINCT JoinType) as JoinTypes "
                "FROM joins "
                "WHERE JoinGroupId IN (SELECT JoinGroupId FROM joins WHERE AlmaId = ?) "
                "  AND AlmaId != ? "
                "GROUP BY AlmaId",
                (sys_id, sys_id),
            )
            results = []
            for row in cursor:
                group_ids = self._split_concat(row["JoinGroupIds"])
                comments = self._split_concat(row["Comments"])
                results.append({
                    "alma_id": row["AlmaId"],
                    "join_group_ids": [int(g) for g in group_ids],
                    "scholar_names": self._split_concat(row["ScholarNames"]),
                    "join_types": self._split_concat(row["JoinTypes"]),
                    "comment": '; '.join(comments) if comments else None,
                })
            return results
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

    # Sentinel CopyDate values that should be treated as None
    _SENTINEL_DATES = frozenset(('0', '-99', '-1', '0.0', '-99.0', '-1.0', ''))

    def get_catalog_records(self, sys_id: str) -> list[dict]:
        """Get all non-empty catalog records for a manuscript.

        Returns list of dicts. Filters out completely empty records and
        deduplicates by (textual_frame_eng, copy_date, title) tuple.
        Sentinel CopyDate values (0, -99, -1) are normalized to None.
        """
        if self._conn is None:
            return []
        try:
            cursor = self._conn.execute(
                "SELECT * FROM catalog WHERE AlmaId = ?",
                (sys_id,),
            )
            results = []
            seen = set()
            col_names = None

            for row in cursor:
                if col_names is None:
                    col_names = row.keys()

                # Normalize CopyDate sentinel values
                copy_date = row["CopyDate"]
                if copy_date is not None and str(copy_date).strip() in self._SENTINEL_DATES:
                    copy_date = None

                # Handle SourceName columns gracefully (may not exist in old sidecars)
                has_source = "SourceName" in col_names
                source_name = row["SourceName"] if has_source else None
                source_name_heb = row["SourceNameHeb"] if has_source else None

                record = {
                    "title": row["Title"],
                    "title_heb": row["TitleHeb"],
                    "author_text": row["AuthorText"],
                    "copy_date": copy_date,
                    "copy_place": row["CopyPlace"],
                    "textual_frame_heb": row["TextualFrameHeb"],
                    "textual_frame_eng": row["TextualFrameEng"],
                    "source_name": source_name,
                    "source_name_heb": source_name_heb,
                }

                # Filter completely empty records (source fields don't count)
                content_fields = (
                    record["title"], record["title_heb"], record["author_text"],
                    record["copy_date"], record["copy_place"],
                    record["textual_frame_heb"], record["textual_frame_eng"],
                )
                if not any(v and str(v).strip() for v in content_fields):
                    continue

                # Deduplicate by key tuple
                key = (
                    record["textual_frame_eng"] or '',
                    record["copy_date"] or '',
                    record["title"] or '',
                )
                if key in seen:
                    continue
                seen.add(key)

                results.append(record)

            return results
        except Exception as e:
            logger.error(f"FjmsService.get_catalog_records error for {sys_id}: {e}")
            return []

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


def merge_catalog_records(records: list[dict]) -> dict:
    """Merge multiple catalog records into a display-ready structure.

    Metadata fields (title, author, date, place) are merged by taking
    the first non-empty value. TextualFrame entries are collected as
    a list of distinct values with their source attribution.

    Args:
        records: List of catalog record dicts from get_catalog_records().

    Returns:
        Dict with keys: title, title_heb, author_text, copy_date, copy_place,
        textual_frames (list of dicts), record_count (int).
    """
    if not records:
        return {
            "title": None, "title_heb": None, "author_text": None,
            "copy_date": None, "copy_place": None,
            "textual_frames": [], "record_count": 0,
        }

    result = {
        "title": None,
        "title_heb": None,
        "author_text": None,
        "copy_date": None,
        "copy_place": None,
    }

    # Take first non-empty value for each metadata field
    for rec in records:
        for key in ("title", "title_heb", "author_text", "copy_date", "copy_place"):
            if result[key] is None and rec.get(key) and str(rec[key]).strip():
                result[key] = rec[key]

    # Collect distinct TextualFrame entries with source attribution
    frames = []
    seen_frames = set()
    for rec in records:
        eng = rec.get("textual_frame_eng") or ''
        heb = rec.get("textual_frame_heb") or ''
        if not eng.strip() and not heb.strip():
            continue
        frame_key = (eng.strip(), heb.strip())
        if frame_key in seen_frames:
            continue
        seen_frames.add(frame_key)
        frames.append({
            "eng": eng.strip() if eng.strip() else None,
            "heb": heb.strip() if heb.strip() else None,
            "source_name": rec.get("source_name"),
            "source_name_heb": rec.get("source_name_heb"),
        })

    result["textual_frames"] = frames
    result["record_count"] = len(records)

    return result


def parse_textual_frame(text: str) -> tuple[str, str]:
    """Parse '[$Category$]: Content' notation into (category, content).

    Strips optional @ prefix. Returns ('', full_text) if no pattern match.

    Args:
        text: The textual frame string (e.g., '[$Bible$]: Leviticus 23:40 - 41').

    Returns:
        Tuple of (category, content). Category is '' if no pattern match.
    """
    if not text:
        return ('', '')
    text = text.strip().lstrip('@')
    match = re.match(r'\[\$(.+?)\$\]\s*:\s*(.*)', text, re.DOTALL)
    if match:
        return (match.group(1).strip(), match.group(2).strip())
    return ('', text)


# Module-level singleton pattern
_default_service: Optional[FjmsService] = None


def get_fjms_service(thread_safe: bool = False) -> FjmsService:
    """Get or create the default FjmsService singleton."""
    global _default_service
    if _default_service is None:
        _default_service = FjmsService(thread_safe=thread_safe)
    return _default_service
