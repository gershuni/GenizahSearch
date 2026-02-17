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
        Get catalog metadata for a manuscript (first record only).

        Args:
            sys_id: The Alma/system ID for the manuscript.

        Returns:
            Dict with keys: title, title_heb, author_text, copy_date, copy_place,
            textual_frame_heb, textual_frame_eng, unit_catalog_rec_id,
            num_folio, num_column, num_row, genizah_title_org, genizah_title_eng.
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
            col_names = row.keys()
            return {
                "title": row["Title"],
                "title_heb": row["TitleHeb"],
                "author_text": row["AuthorText"],
                "copy_date": row["CopyDate"],
                "copy_place": row["CopyPlace"],
                "textual_frame_heb": row["TextualFrameHeb"],
                "textual_frame_eng": row["TextualFrameEng"],
                "unit_catalog_rec_id": row["UnitCatalogRecId"] if "UnitCatalogRecId" in col_names else None,
                "num_folio": row["NumFolio"] if "NumFolio" in col_names else None,
                "num_column": row["NumColumn"] if "NumColumn" in col_names else None,
                "num_row": row["NumRow"] if "NumRow" in col_names else None,
                "genizah_title_org": row["GenizahTitleOrgTitle"] if "GenizahTitleOrgTitle" in col_names else None,
                "genizah_title_eng": row["GenizahTitleEngTitle"] if "GenizahTitleEngTitle" in col_names else None,
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

                # Handle new v3.0.0 columns gracefully (may not exist in old sidecars)
                has_rec_id = "UnitCatalogRecId" in col_names
                has_num_folio = "NumFolio" in col_names
                has_num_column = "NumColumn" in col_names
                has_num_row = "NumRow" in col_names
                has_genizah_org = "GenizahTitleOrgTitle" in col_names
                has_genizah_eng = "GenizahTitleEngTitle" in col_names

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
                    "unit_catalog_rec_id": row["UnitCatalogRecId"] if has_rec_id else None,
                    "num_folio": row["NumFolio"] if has_num_folio else None,
                    "num_column": row["NumColumn"] if has_num_column else None,
                    "num_row": row["NumRow"] if has_num_row else None,
                    "genizah_title_org": row["GenizahTitleOrgTitle"] if has_genizah_org else None,
                    "genizah_title_eng": row["GenizahTitleEngTitle"] if has_genizah_eng else None,
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

    # ── Bibliography & Catalog Refs (Phase 33: META-03) ─────────────

    # Generic source names to filter out from get_source_names()
    _GENERIC_SOURCE_NAMES = frozenset({'Catalogs', 'Institution', 'Collection', 'Other'})

    def get_bibliography(self, sys_id: str) -> list[dict]:
        """
        Get bibliography entries for a manuscript.

        Returns denormalized bibliography rows with resolved author/title/mention
        information, ordered with Discussion entries first, then Mentioned, then others.

        Args:
            sys_id: The Alma/system ID for the manuscript.

        Returns:
            List of dicts with keys: running_title, title_year, title_acronym,
            mention_page, from_page, to_page, volume, mention_type,
            transcription_type, translation_type, article_name,
            article_author_eng, article_author_heb, catalog_acronym.
            Returns [] if conn is None, sys_id not found, or table missing.
        """
        if self._conn is None:
            return []
        try:
            cursor = self._conn.execute(
                "SELECT * FROM bibliography WHERE AlmaId = ? "
                "ORDER BY CASE MentionType "
                "WHEN 'Discussion' THEN 0 "
                "WHEN 'Mentioned' THEN 1 "
                "ELSE 2 END, RunningTitle",
                (sys_id,),
            )
            return [
                {
                    "running_title": row["RunningTitle"],
                    "title_year": row["TitleYear"],
                    "title_acronym": row["TitleAcronym"],
                    "mention_page": row["MentionPage"],
                    "from_page": row["FromPage"],
                    "to_page": row["ToPage"],
                    "volume": row["Volume"],
                    "mention_type": row["MentionType"],
                    "transcription_type": row["TranscriptionType"],
                    "translation_type": row["TranslationType"],
                    "article_name": row["ArticleName"],
                    "article_author_eng": row["ArticleAuthorEng"],
                    "article_author_heb": row["ArticleAuthorHeb"],
                    "catalog_acronym": row["CatalogAcronym"],
                }
                for row in cursor
            ]
        except Exception as e:
            logger.error(f"FjmsService.get_bibliography error for {sys_id}: {e}")
            return []

    def get_catalog_refs(self, sys_id: str) -> list[dict]:
        """
        Get catalog cross-references for a manuscript.

        Returns entries linking the manuscript to scholarly catalogs
        (e.g., Goitein Med Soc, Gil Palestine).

        Args:
            sys_id: The Alma/system ID for the manuscript.

        Returns:
            List of dicts with keys: cat_acronym, catalog_author,
            catalog_title, catalog_entry, is_source.
            Returns [] if conn is None, sys_id not found, or table missing.
        """
        if self._conn is None:
            return []
        try:
            cursor = self._conn.execute(
                "SELECT * FROM catalog_refs WHERE AlmaId = ? "
                "ORDER BY CatAcronym, CatalogEntry",
                (sys_id,),
            )
            return [
                {
                    "cat_acronym": row["CatAcronym"],
                    "catalog_author": row["CatalogAuthor"],
                    "catalog_title": row["CatalogTitle"],
                    "catalog_entry": row["CatalogEntry"],
                    "is_source": row["IsSource"],
                }
                for row in cursor
            ]
        except Exception as e:
            logger.error(f"FjmsService.get_catalog_refs error for {sys_id}: {e}")
            return []

    def get_source_names(self, sys_id: str) -> list[str]:
        """
        Get distinct scholarly source names for a manuscript.

        Queries the catalog table for SourceName values, filtering out
        generic labels like 'Catalogs', 'Institution', 'Collection', 'Other'.

        Args:
            sys_id: The Alma/system ID for the manuscript.

        Returns:
            List of non-generic SourceName strings.
            Returns [] if conn is None, sys_id not found, or table missing.
        """
        if self._conn is None:
            return []
        try:
            cursor = self._conn.execute(
                "SELECT DISTINCT SourceName FROM catalog "
                "WHERE AlmaId = ? AND SourceName IS NOT NULL AND SourceName != ''",
                (sys_id,),
            )
            return [
                row["SourceName"]
                for row in cursor
                if row["SourceName"] not in self._GENERIC_SOURCE_NAMES
            ]
        except Exception as e:
            logger.error(f"FjmsService.get_source_names error for {sys_id}: {e}")
            return []

    def get_catalog_source_counts(self, sys_ids: list[str]) -> dict[str, int]:
        """
        Get distinct catalog source counts for multiple manuscripts in batch.

        Used for search card button labels: "Catalog Records (N)".
        Excludes generic source names (Catalogs, Institution, Collection, Other).

        Args:
            sys_ids: List of Alma/system IDs.

        Returns:
            Dict mapping sys_id -> count of distinct non-generic SourceName values.
            IDs with no catalog data are omitted (not present in result).
        """
        if not self._conn or not sys_ids:
            return {}
        try:
            result = {}
            batch_size = 500
            for i in range(0, len(sys_ids), batch_size):
                batch = sys_ids[i:i + batch_size]
                placeholders = ','.join('?' * len(batch))
                cursor = self._conn.execute(
                    f"SELECT AlmaId, COUNT(DISTINCT SourceName) as cnt FROM catalog "
                    f"WHERE AlmaId IN ({placeholders}) "
                    f"AND SourceName IS NOT NULL AND SourceName != '' "
                    f"AND SourceName NOT IN ('Catalogs','Institution','Collection','Other') "
                    f"GROUP BY AlmaId",
                    batch,
                )
                for row in cursor:
                    result[row["AlmaId"]] = row["cnt"]
            return result
        except Exception as e:
            logger.error(f"FjmsService.get_catalog_source_counts error: {e}")
            return {}

    def get_catalog_detail(self, sys_id: str) -> dict:
        """
        Get structured catalog detail for the dialog display.

        Returns all catalog data for a manuscript grouped by child table:
        records, running titles, sizes, fields, and free descriptions.

        Args:
            sys_id: The Alma/system ID for the manuscript.

        Returns:
            Dict with keys:
                - records: list of catalog record dicts (from get_catalog_records)
                - running_titles: dict mapping UnitCatalogRecId -> list of
                    {"running_title": str, "comment": str}
                - sizes: dict mapping UnitCatalogRecId -> list of
                    {"size_x": float, "size_y": float, "inner_size_x": float, "inner_size_y": float}
                - fields: dict mapping UnitCatalogRecId -> {FieldCategory: [{"value": str, "value_heb": str}]}
                - free_descriptions: list of {"text": str, "signature_id": int}
        """
        empty = {
            "records": [],
            "running_titles": {},
            "sizes": {},
            "fields": {},
            "free_descriptions": [],
        }
        if self._conn is None:
            return empty

        # 1. Catalog records
        records = self.get_catalog_records(sys_id)

        # 2. Running titles
        running_titles = {}
        try:
            cursor = self._conn.execute(
                "SELECT UnitCatalogRecId, RunningTitle, Comment "
                "FROM catalog_running_titles WHERE AlmaId = ?",
                (sys_id,),
            )
            for row in cursor:
                rec_id = row["UnitCatalogRecId"]
                if rec_id not in running_titles:
                    running_titles[rec_id] = []
                running_titles[rec_id].append({
                    "running_title": row["RunningTitle"],
                    "comment": row["Comment"],
                })
        except Exception as e:
            logger.debug(f"FjmsService.get_catalog_detail running_titles error for {sys_id}: {e}")

        # 3. Sizes
        sizes = {}
        try:
            cursor = self._conn.execute(
                "SELECT UnitCatalogRecId, SizeX, SizeY, InnerSizeX, InnerSizeY "
                "FROM catalog_sizes WHERE AlmaId = ?",
                (sys_id,),
            )
            for row in cursor:
                rec_id = row["UnitCatalogRecId"]
                if rec_id not in sizes:
                    sizes[rec_id] = []
                sizes[rec_id].append({
                    "size_x": row["SizeX"],
                    "size_y": row["SizeY"],
                    "inner_size_x": row["InnerSizeX"],
                    "inner_size_y": row["InnerSizeY"],
                })
        except Exception as e:
            logger.debug(f"FjmsService.get_catalog_detail sizes error for {sys_id}: {e}")

        # 4. Fields (grouped by UnitCatalogRecId then FieldCategory)
        fields = {}
        try:
            cursor = self._conn.execute(
                "SELECT UnitCatalogRecId, FieldCategory, FieldValue, FieldValueHeb "
                "FROM catalog_fields WHERE AlmaId = ?",
                (sys_id,),
            )
            for row in cursor:
                rec_id = row["UnitCatalogRecId"]
                category = row["FieldCategory"]
                if rec_id not in fields:
                    fields[rec_id] = {}
                if category not in fields[rec_id]:
                    fields[rec_id][category] = []
                fields[rec_id][category].append({
                    "value": row["FieldValue"],
                    "value_heb": row["FieldValueHeb"],
                })
        except Exception as e:
            logger.debug(f"FjmsService.get_catalog_detail fields error for {sys_id}: {e}")

        # 5. Free descriptions
        free_descriptions = []
        try:
            cursor = self._conn.execute(
                "SELECT SignatureId, FreeDesc "
                "FROM catalog_free_desc WHERE AlmaId = ?",
                (sys_id,),
            )
            for row in cursor:
                free_descriptions.append({
                    "text": row["FreeDesc"],
                    "signature_id": row["SignatureId"],
                })
        except Exception as e:
            logger.debug(f"FjmsService.get_catalog_detail free_desc error for {sys_id}: {e}")

        return {
            "records": records,
            "running_titles": running_titles,
            "sizes": sizes,
            "fields": fields,
            "free_descriptions": free_descriptions,
        }

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


def format_page_ref(entry: dict) -> str:
    """Format page reference from FJMS bibliography entry fields.

    Handles mention_page, from_page/to_page ranges, and volume prefixes.

    Args:
        entry: Dict with keys mention_page, from_page, to_page, volume.

    Returns:
        Formatted page reference string (e.g., 'vol. 2, pp. 15-20') or ''.
    """
    parts = []
    vol = entry.get('volume', '')
    if vol and str(vol).strip():
        parts.append(f'vol. {vol}')
    mention_page = entry.get('mention_page', '')
    from_page = entry.get('from_page', '')
    to_page = entry.get('to_page', '')
    if mention_page and str(mention_page).strip():
        parts.append(f'p. {mention_page}')
    elif from_page and str(from_page).strip():
        if to_page and str(to_page).strip() and str(to_page) != str(from_page):
            parts.append(f'pp. {from_page}-{to_page}')
        else:
            parts.append(f'p. {from_page}')
    return ', '.join(parts)


def _parse_marc_annotations(marc_str: str) -> dict:
    """Parse Hebrew annotations from end of NLI MARC 581 string.

    NLI MARC strings end with parenthetical Hebrew annotations derived from
    FJMS data, e.g.: '(דיון, יש תמונה, יש העתקה (מלא), יש תרגום (מלא)).'

    Args:
        marc_str: Raw MARC bibliography string.

    Returns:
        Dict with keys: mention_type, has_image, transcription, translation.
    """
    result = {'mention_type': '', 'has_image': False, 'transcription': '', 'translation': ''}
    if not marc_str:
        return result

    # Find the last parenthetical block containing Hebrew annotations
    # Pattern: content in parens that contains Hebrew chars, possibly nested parens
    match = re.search(r'\(([^()]*(?:\([^()]*\)[^()]*)*)\)\s*\.?\s*$', marc_str)
    if not match:
        return result

    annotation = match.group(1)

    # Mention type
    if 'דיון' in annotation:
        result['mention_type'] = 'Discussion'
    elif 'איזכור' in annotation:
        result['mention_type'] = 'Mentioned'
    elif 'מפתח' in annotation:
        result['mention_type'] = 'Index'

    # Has image
    if 'יש תמונה' in annotation or 'תמונה' in annotation:
        result['has_image'] = True

    # Transcription
    if 'יש העתקה' in annotation:
        if 'העתקה (מלא)' in annotation:
            result['transcription'] = 'Full'
        elif 'העתקה (חלקי)' in annotation:
            result['transcription'] = 'Partial'
        else:
            result['transcription'] = 'Exists'

    # Translation
    if 'יש תרגום' in annotation:
        if 'תרגום (מלא)' in annotation:
            result['translation'] = 'Full'
        elif 'תרגום (חלקי)' in annotation:
            result['translation'] = 'Partial'
        else:
            result['translation'] = 'Exists'

    return result


def strip_marc_annotation_suffix(marc_str: str) -> str:
    """Strip trailing Hebrew annotation parenthetical from MARC 581 string.

    NLI MARC strings end with '(דיון, יש תמונה, ...).' — this returns
    the clean reference text for display in the NLI bibliography table.

    Args:
        marc_str: Raw MARC bibliography string.

    Returns:
        Reference text with trailing annotation removed, or original string.
    """
    if not marc_str:
        return ''
    s = marc_str.strip()
    # Remove trailing period
    if s.endswith('.'):
        s = s[:-1].rstrip()
    # Remove last parenthetical block that contains Hebrew chars
    cleaned = re.sub(r'\s*\(([^()]*(?:\([^()]*\)[^()]*)*)\)\s*$', '', s)
    # Only strip if the removed part actually contained Hebrew
    if cleaned != s:
        removed = s[len(cleaned):]
        if re.search(r'[\u0590-\u05FF]', removed):
            return cleaned.rstrip(' .,;-')
    return s


def _ts_symbol(value) -> str:
    """Map transcription/translation value to FJMS-style symbol.

    Full → '✓+', Partial → '✓−', truthy/Exists → '✓', None/empty → ''.
    """
    if not value or str(value).strip() in ('', 'None', 'Unknown'):
        return ''
    v = str(value).strip()
    if v == 'Full':
        return '\u2713+'
    if v == 'Partial':
        return '\u2713\u2212'
    return '\u2713'


def _parse_marc_bib_string(marc_str: str) -> dict:
    """Parse an NLI MARC 581 bibliography string into structured fields.

    Extracts author (text before first comma), 4-digit year, page numbers,
    title, and Hebrew annotations from the raw MARC string.

    Args:
        marc_str: Raw bibliography string from MARC tag 581.

    Returns:
        Dict with keys: author, year, pages, title, plus annotation fields.
    """
    result = {'author': '', 'year': '', 'pages': '', 'title': ''}
    if not marc_str or not marc_str.strip():
        return result

    s = marc_str.strip()

    # Extract author: text before first comma (if not too long)
    comma_idx = s.find(',')
    if 0 < comma_idx <= 60:
        result['author'] = s[:comma_idx].strip()

    # Extract title: text between author section and year/page section
    # Pattern: "Author, Article Title. Book/Journal Title, Year, ..."
    # Try to find text after author+article that looks like a title
    title_match = re.search(r'(?:,\s+[^,]+)?\.\s+([^,.]+?)(?:\.\s|\,\s*\d{4}|\,\s*\d+\s*עמ)', s)
    if title_match:
        candidate = title_match.group(1).strip()
        # Only use if it's not too short and not just a year
        if len(candidate) > 3 and not re.match(r'^\d{4}$', candidate):
            result['title'] = candidate

    # Extract 4-digit year
    year_match = re.search(r'\b(1[4-9]\d{2}|20[0-2]\d)\b', s)
    if year_match:
        result['year'] = year_match.group(1)

    # Extract page references - Hebrew patterns first (more common in MARC)
    heb_match = re.search(r"עמ(?:וד|['\u2019])\s*([\w\d,/ -–]+?)(?:\s*\(|$)", s)
    if heb_match:
        result['pages'] = heb_match.group(1).strip().rstrip('.')
    else:
        # English patterns
        page_match = re.search(r'(?:pp?\.\s*|pages?\s+)(\d+(?:\s*[-–]\s*\d+)?)', s)
        if page_match:
            result['pages'] = page_match.group(1).strip()

    # Parse Hebrew annotations
    annotations = _parse_marc_annotations(s)
    result.update(annotations)

    return result


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
    # TextualFrame fields can contain multiple entries separated by '; @[$'
    frames = []
    seen_frames = set()
    for rec in records:
        eng_text = rec.get("textual_frame_eng") or ''
        heb_text = rec.get("textual_frame_heb") or ''
        if not eng_text.strip() and not heb_text.strip():
            continue
        eng_parts = split_textual_frames(eng_text)
        heb_parts = split_textual_frames(heb_text)
        max_len = max(len(eng_parts), len(heb_parts), 1)
        # If no parts from split (plain text without [$...$]), use original
        if not eng_parts and not heb_parts:
            eng_parts = [eng_text.strip()] if eng_text.strip() else []
            heb_parts = [heb_text.strip()] if heb_text.strip() else []
            max_len = max(len(eng_parts), len(heb_parts))
        for i in range(max_len):
            eng = eng_parts[i].strip() if i < len(eng_parts) else None
            heb = heb_parts[i].strip() if i < len(heb_parts) else None
            if not (eng or heb):
                continue
            frame_key = (eng or '', heb or '')
            if frame_key in seen_frames:
                continue
            seen_frames.add(frame_key)
            frames.append({
                "eng": eng if eng else None,
                "heb": heb if heb else None,
                "source_name": rec.get("source_name"),
                "source_name_heb": rec.get("source_name_heb"),
            })

    result["textual_frames"] = frames
    result["record_count"] = len(records)

    return result


def split_textual_frames(text: str) -> list[str]:
    """Split a compound TextualFrame string into individual entries.

    FJMS TextualFrame fields can contain multiple entries separated by '; '
    where each entry starts with @[$Category$] or [$Category$] notation.
    E.g.: '@[$Piyyut$]: "poem1"; @[$Piyyut$] (Yotzer): "poem2"'
    """
    if not text or not text.strip():
        return []
    # Split on '; ' followed by optional @ then [$
    parts = re.split(r';\s*(?=@?\[\$)', text.strip())
    return [p.strip() for p in parts if p.strip()]


def parse_textual_frame(text: str) -> tuple[str, str]:
    """Parse '[$Category$]: Content' notation into (category, content).

    Strips optional @ prefix. Captures parenthetical sub-type as part of
    category (e.g., '[$Piyyut$] (Yotzer)' -> category='Piyyut (Yotzer)').
    Returns ('', full_text) if no pattern match.

    Args:
        text: A single textual frame entry (use split_textual_frames first
              to split compound strings).

    Returns:
        Tuple of (category, content). Category is '' if no pattern match.
    """
    if not text:
        return ('', '')
    text = text.strip().lstrip('@')
    match = re.match(r'\[\$(.+?)\$\]\s*(\([^)]+\))?\s*:?\s*(.*)', text, re.DOTALL)
    if match:
        category = match.group(1).strip()
        sub_type = match.group(2)
        content = match.group(3).strip()
        if sub_type:
            category = f"{category} {sub_type}"
        return (category, content)
    return ('', text)


# Module-level singleton pattern
_default_service: Optional[FjmsService] = None


def get_fjms_service(thread_safe: bool = False) -> FjmsService:
    """Get or create the default FjmsService singleton."""
    global _default_service
    if _default_service is None:
        _default_service = FjmsService(thread_safe=thread_safe)
    return _default_service
