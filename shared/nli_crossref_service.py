# -*- coding: utf-8 -*-
"""
NLI Crossref Service for accessing NLI image data and Cambridge IIIF manifests.

This module provides the NliCrossrefService class for querying NLI image records,
Cambridge IIIF manifest URLs, physical metadata, and relationship data from the
nli_crossref.db sidecar database. Used by both the web app and desktop app.

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


# ── Folio Label Parsing (Phase 31: IMG-04) ───────────────────────

# Pattern: L{leaf}F{folio}B{bifolio}S{side} within NLI ImageName values
_FOLIO_PATTERN = re.compile(r'L(\d+)F\d+B\d+S(\d+)')


def parse_folio_label(image_name: str) -> str:
    """
    Extract folio notation from an NLI ImageName value.

    The ImageName pattern is: {shelfmark_prefix}__L{leaf}F{folio}B{bifolio}S{side}
    where S1=recto (r), S2=verso (v).

    Examples:
        - 'T_S_12_1__L1F0B0S1' -> '1r'
        - 'T_S_12_1__L1F0B0S2' -> '1v'
        - 'I_C_71__L3F0B0S1'   -> '3r'
        - 'Yevr_III_B_1093__L7F0B0S1' -> '7r'

    Args:
        image_name: The NLI ImageName string.

    Returns:
        Folio label string (e.g., '1r', '3v') or empty string if pattern not found.
    """
    if not image_name:
        return ''
    match = _FOLIO_PATTERN.search(image_name)
    if not match:
        return ''
    leaf = match.group(1)
    side = match.group(2)
    side_letter = 'r' if side == '1' else 'v' if side == '2' else ''
    return f"{leaf}{side_letter}"

# Default sidecar filename
_SIDECAR_FILENAME = "nli_crossref.db"
_SIDECAR_DIR = "nli_data"


def _find_project_root() -> Optional[Path]:
    """Find the project root by looking for libraries.csv up from this file."""
    current = Path(__file__).resolve().parent
    for _ in range(5):  # Up to 5 levels
        if (current / "libraries.csv").exists():
            return current
        current = current.parent
    return None


class NliCrossrefService:
    """Service for accessing NLI crossref and Cambridge IIIF data from the SQLite sidecar."""

    def __init__(self, db_path: str = None, thread_safe: bool = False):
        """
        Initialize NliCrossrefService.

        Args:
            db_path: Path to nli_crossref.db. If None, auto-detect from project root.
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
            logger.warning("NliCrossrefService: No db_path provided and project root not found")
            return

        self._db_path = db_path
        db_file = Path(db_path)

        if not db_file.exists():
            logger.warning(f"NliCrossrefService: Sidecar database not found at {db_path}")
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
            logger.info(f"NliCrossrefService: Connected to {db_path}")
        except Exception as e:
            logger.error(f"NliCrossrefService: Failed to connect to {db_path}: {e}")
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
            logger.error(f"NliCrossrefService.get_version error: {e}")
            return None

    # ── Image Lookup (Phase 30: IMG-01) ─────────────────────────────

    def get_images(self, sys_id: str) -> list[dict]:
        """
        Get all image rows for a manuscript by NLI_AlmaId.

        Args:
            sys_id: The Alma/system ID for the manuscript.

        Returns:
            List of dicts with keys: fgp_image_number_id, fgp_number,
            image_name, image_source_name, shelfmark.
            Ordered by ImageName (natural page sequence).
            Returns [] if conn is None or sys_id not found.
        """
        if self._conn is None:
            return []
        try:
            cursor = self._conn.execute(
                "SELECT FGPImageNumberId, FGPNumber, ImageName, "
                "ImageSourceName, Shelfmark "
                "FROM nli_images WHERE NLI_AlmaId = ? "
                "ORDER BY ImageName",
                (sys_id,),
            )
            return [
                {
                    "fgp_image_number_id": row["FGPImageNumberId"],
                    "fgp_number": row["FGPNumber"],
                    "image_name": row["ImageName"],
                    "image_source_name": row["ImageSourceName"],
                    "shelfmark": row["Shelfmark"],
                }
                for row in cursor
            ]
        except Exception as e:
            logger.error(f"NliCrossrefService.get_images error for {sys_id}: {e}")
            return []

    def get_images_batch(self, sys_ids: list[str]) -> dict[str, list[dict]]:
        """
        Get image records for multiple manuscripts in batch.

        More efficient than calling get_images() per sys_id when processing
        search results. Uses batched IN queries to stay within SQLite limits.

        Args:
            sys_ids: List of Alma/system IDs.

        Returns:
            Dict mapping sys_id -> list of image dicts.
            Each image dict has keys: fgp_image_number_id, fgp_number,
            image_name, image_source_name, shelfmark.
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
                    f"SELECT NLI_AlmaId, FGPImageNumberId, FGPNumber, "
                    f"ImageName, ImageSourceName, Shelfmark "
                    f"FROM nli_images WHERE NLI_AlmaId IN ({placeholders}) "
                    f"ORDER BY NLI_AlmaId, ImageName",
                    batch,
                )
                for row in cursor:
                    sid = row["NLI_AlmaId"]
                    if sid not in result:
                        result[sid] = []
                    result[sid].append({
                        "fgp_image_number_id": row["FGPImageNumberId"],
                        "fgp_number": row["FGPNumber"],
                        "image_name": row["ImageName"],
                        "image_source_name": row["ImageSourceName"],
                        "shelfmark": row["Shelfmark"],
                    })
            return result
        except Exception as e:
            logger.error(f"NliCrossrefService.get_images_batch error: {e}")
            return {}

    # ── Folio Image Lookup (Phase 31: IMG-04) ──────────────────────

    def get_folio_images(self, sys_id: str) -> list[dict]:
        """
        Get all image rows for a manuscript, enriched with folio labels.

        Returns the same data as get_images() but with an additional
        'folio_label' key in each dict, generated by parse_folio_label().
        For images where parse_folio_label returns empty string, assigns
        a sequential fallback label (e.g., '1', '2', '3').

        Args:
            sys_id: The Alma/system ID for the manuscript.

        Returns:
            List of dicts with keys: fgp_image_number_id, fgp_number,
            image_name, image_source_name, shelfmark, folio_label.
            Ordered by ImageName (natural page sequence).
        """
        images = self.get_images(sys_id)
        fallback_counter = 0
        for img in images:
            label = parse_folio_label(img.get('image_name', ''))
            if not label:
                fallback_counter += 1
                label = str(fallback_counter)
            img['folio_label'] = label
        return images

    # ── Cambridge IIIF (Phase 30: IMG-02) ───────────────────────────

    def get_cambridge_manifest(self, normalized_shelfmark: str) -> Optional[str]:
        """
        Get Cambridge IIIF manifest URL for a normalized shelfmark.

        Args:
            normalized_shelfmark: Normalized shelfmark string.

        Returns:
            Manifest URL string or None if not found.
        """
        if self._conn is None:
            return None
        try:
            cursor = self._conn.execute(
                "SELECT manifest_url FROM cambridge_manifests "
                "WHERE normalized_shelfmark = ?",
                (normalized_shelfmark,),
            )
            row = cursor.fetchone()
            return row["manifest_url"] if row else None
        except Exception as e:
            logger.error(f"NliCrossrefService.get_cambridge_manifest error for {normalized_shelfmark}: {e}")
            return None

    def get_cambridge_manifest_by_label(self, label: str) -> Optional[str]:
        """
        Get Cambridge IIIF manifest URL by CUDL label.

        Args:
            label: CUDL label string (e.g., 'MS-TS-00006-F-00001').

        Returns:
            Manifest URL string or None if not found.
        """
        if self._conn is None:
            return None
        try:
            cursor = self._conn.execute(
                "SELECT manifest_url FROM cambridge_manifests "
                "WHERE label = ?",
                (label,),
            )
            row = cursor.fetchone()
            return row["manifest_url"] if row else None
        except Exception as e:
            logger.error(f"NliCrossrefService.get_cambridge_manifest_by_label error for {label}: {e}")
            return None

    # ── Metadata (Phase 32: META-01, META-02) ───────────────────────

    def get_physical_metadata(self, sys_id: str) -> Optional[dict]:
        """
        Get physical metadata (material, folio counts, size) for a manuscript.

        Args:
            sys_id: The Alma/system ID for the manuscript.

        Returns:
            Dict with keys: material, num_folio, num_bifolio, size.
            Returns None if no non-empty metadata found.
        """
        if self._conn is None:
            return None
        try:
            cursor = self._conn.execute(
                "SELECT DISTINCT Material, NumFolio, NumBifolio, Size "
                "FROM nli_images WHERE NLI_AlmaId = ? "
                "AND (Material != '' OR NumFolio != '' OR NumBifolio != '' OR Size != '')",
                (sys_id,),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return {
                "material": row["Material"],
                "num_folio": row["NumFolio"],
                "num_bifolio": row["NumBifolio"],
                "size": row["Size"],
            }
        except Exception as e:
            logger.error(f"NliCrossrefService.get_physical_metadata error for {sys_id}: {e}")
            return None

    # ── Relationships (Phase 33: REL-01, REL-02) ────────────────────

    def get_part_of(self, sys_id: str) -> list[str]:
        """
        Get PartOf shelfmark references for this manuscript.

        Args:
            sys_id: The Alma/system ID for the manuscript.

        Returns:
            List of shelfmark strings. Returns [] if none found.
        """
        if self._conn is None:
            return []
        try:
            cursor = self._conn.execute(
                "SELECT DISTINCT PartOf FROM nli_images "
                "WHERE NLI_AlmaId = ? AND PartOf != ''",
                (sys_id,),
            )
            return [row["PartOf"] for row in cursor]
        except Exception as e:
            logger.error(f"NliCrossrefService.get_part_of error for {sys_id}: {e}")
            return []

    def get_see_references(self, sys_id: str) -> list[str]:
        """
        Get See cross-references for this manuscript.

        Note: Data investigation showed 0 records with See values,
        but this method is kept for future data updates.

        Args:
            sys_id: The Alma/system ID for the manuscript.

        Returns:
            List of reference strings. Returns [] if none found.
        """
        if self._conn is None:
            return []
        try:
            cursor = self._conn.execute(
                "SELECT DISTINCT See FROM nli_images "
                "WHERE NLI_AlmaId = ? AND See != ''",
                (sys_id,),
            )
            return [row["See"] for row in cursor]
        except Exception as e:
            logger.error(f"NliCrossrefService.get_see_references error for {sys_id}: {e}")
            return []

    def get_bifolio_partners(self, sys_id: str) -> list[dict]:
        """
        Get BifolioWith references for this manuscript.

        Args:
            sys_id: The Alma/system ID for the manuscript.

        Returns:
            List of dicts with keys: bifolio_with, image_name.
            Returns [] if none found.
        """
        if self._conn is None:
            return []
        try:
            cursor = self._conn.execute(
                "SELECT DISTINCT BifolioWith, ImageName FROM nli_images "
                "WHERE NLI_AlmaId = ? AND BifolioWith != ''",
                (sys_id,),
            )
            return [
                {
                    "bifolio_with": row["BifolioWith"],
                    "image_name": row["ImageName"],
                }
                for row in cursor
            ]
        except Exception as e:
            logger.error(f"NliCrossrefService.get_bifolio_partners error for {sys_id}: {e}")
            return []

    # ── Image Availability Indicator (Phase 31: IMG-03) ─────────────

    def get_image_sources(self, sys_id: str, normalized_shelfmark: str = None) -> dict:
        """
        Quick check of which image sources exist for a manuscript.

        Args:
            sys_id: The Alma/system ID for the manuscript.
            normalized_shelfmark: Optional normalized shelfmark for Cambridge lookup.

        Returns:
            Dict with keys:
                - nli_fgp (bool): True if any FGPImageNumberId is non-empty
                - cambridge (bool): True if normalized_shelfmark has a Cambridge manifest
                - image_count (int): Count of rows with non-empty FGPImageNumberId
        """
        result = {"nli_fgp": False, "cambridge": False, "image_count": 0}

        if self._conn is None:
            return result

        try:
            # Check NLI FGP images
            cursor = self._conn.execute(
                "SELECT COUNT(*) as cnt FROM nli_images "
                "WHERE NLI_AlmaId = ? AND FGPImageNumberId != ''",
                (sys_id,),
            )
            row = cursor.fetchone()
            if row and row["cnt"] > 0:
                result["nli_fgp"] = True
                result["image_count"] = row["cnt"]

            # Check Cambridge manifests
            if normalized_shelfmark:
                cam_cursor = self._conn.execute(
                    "SELECT COUNT(*) as cnt FROM cambridge_manifests "
                    "WHERE normalized_shelfmark = ?",
                    (normalized_shelfmark,),
                )
                cam_row = cam_cursor.fetchone()
                if cam_row and cam_row["cnt"] > 0:
                    result["cambridge"] = True

        except Exception as e:
            logger.error(f"NliCrossrefService.get_image_sources error for {sys_id}: {e}")

        return result

    def close(self):
        """Close the database connection if open."""
        if self._conn is not None:
            try:
                self._conn.close()
                logger.info("NliCrossrefService: Connection closed")
            except Exception as e:
                logger.error(f"NliCrossrefService.close error: {e}")
            finally:
                self._conn = None


# Module-level singleton pattern
_default_service: Optional[NliCrossrefService] = None


def get_nli_crossref_service(thread_safe: bool = False) -> NliCrossrefService:
    """Get or create the default NliCrossrefService singleton."""
    global _default_service
    if _default_service is None:
        _default_service = NliCrossrefService(thread_safe=thread_safe)
    return _default_service
