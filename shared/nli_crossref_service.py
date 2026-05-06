# -*- coding: utf-8 -*-
"""
NLI Crossref Service for accessing NLI image data and Cambridge IIIF manifests.

This module provides the NliCrossrefService class for querying NLI image records,
Cambridge IIIF manifest URLs, physical metadata, and relationship data from the
nli_crossref.db sidecar database. Used by both the web app and desktop app.

All methods handle errors gracefully, returning empty results rather than
raising exceptions. When the sidecar database is missing, the service
degrades gracefully (is_available() returns False, all queries return empty).

Thread-safe: uses per-thread SQLite connections via ThreadLocalConnection
so concurrent NiceGUI run.io_bound() calls each get their own connection.
"""

import logging
import re
import sqlite3
from pathlib import Path
from typing import Optional
from urllib.parse import quote as url_quote

from shared.thread_local_db import ThreadLocalConnection

logger = logging.getLogger(__name__)

# Phase 84: WARNING-once flag for shelfmark_bridge import failures (Gemini LOW).
_BRIDGE_IMPORT_WARNED = False


# ── Folio Label Parsing (Phase 31: IMG-04) ───────────────────────

# Pattern: L{leaf}(optionally _{second_leaf} for bifolio/paired-leaf)F{folio}B{bifolio}S{side}
# Paired-leaf form (e.g. 'L1_12F0B0S1') indicates a bifolio of two conjoint
# leaves; the first number is treated as the primary leaf for folio-label
# and sort-order purposes (bug 260419-nwv).
_FOLIO_PATTERN = re.compile(r'L(\d+)(?:_\d+)?F\d+B\d+S(\d+)')


def parse_folio_label(image_name: str) -> str:
    """
    Extract folio notation from an NLI ImageName value.

    The ImageName pattern is: {shelfmark_prefix}__L{leaf}F{folio}B{bifolio}S{side}
    where S1=recto (r), S2=verso (v). A paired-leaf / bifolio variant
    ``L{first}_{second}F...`` is also accepted; the primary leaf is the
    first number.

    Examples:
        - 'T_S_12_1__L1F0B0S1' -> '1r'
        - 'T_S_12_1__L1F0B0S2' -> '1v'
        - 'I_C_71__L3F0B0S1'   -> '3r'
        - 'Yevr_III_B_1093__L7F0B0S1' -> '7r'
        - 'T_S_NS_158_112__L1_12F0B0S1' -> '1r' (paired-leaf / bifolio; primary leaf = 1)

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
            thread_safe: If True, use per-thread connections for NiceGUI web app.
                        Desktop app should leave this False (single-threaded).
        """
        self._conn = None  # ThreadLocalConnection or sqlite3.Connection
        self._db_path: Optional[str] = None

        # Resolve db_path
        if db_path is None:
            # Check user-updated sidecar location first (LOCALAPPDATA)
            import os
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
            logger.warning("NliCrossrefService: No db_path provided and project root not found")
            return

        self._db_path = db_path
        db_file = Path(db_path)

        if not db_file.exists():
            logger.warning(f"NliCrossrefService: Sidecar database not found at {db_path}")
            return

        try:
            uri = f"file:{db_path}?mode=ro"
            if thread_safe:
                self._conn = ThreadLocalConnection(
                    uri, row_factory=sqlite3.Row, timeout=10.0
                )
            else:
                self._conn = sqlite3.connect(
                    uri, uri=True, check_same_thread=True, timeout=10.0
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

        The result is sorted numerically by (leaf_number, side) so that
        ``1r, 1v, 2r, 2v, ...`` is the natural page order.  The
        underlying ``get_images()`` query orders by ImageName which is
        alphabetical (``L10`` before ``L1``); this method corrects that.

        Args:
            sys_id: The Alma/system ID for the manuscript.

        Returns:
            List of dicts with keys: fgp_image_number_id, fgp_number,
            image_name, image_source_name, shelfmark, folio_label.
            Ordered by leaf number then side (recto before verso).
        """
        images = self.get_images(sys_id)
        fallback_counter = 0
        for img in images:
            label = parse_folio_label(img.get('image_name', ''))
            if not label:
                fallback_counter += 1
                label = str(fallback_counter)
            img['folio_label'] = label

        # Sort by (leaf_number, side) for natural page order.
        # parse_folio_label already extracted leaf+side; re-extract the
        # numeric components from ImageName for a stable numeric sort.
        def _sort_key(img):
            name = img.get('image_name', '')
            m = _FOLIO_PATTERN.search(name)
            if m:
                return (int(m.group(1)), int(m.group(2)))
            # Fall back to keeping alphabetical order for non-matching names
            return (999999, 0)

        images.sort(key=_sort_key)
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

    def get_cambridge_manifest_with_bridge(self, shelfmark: str) -> Optional[str]:
        """Phase 84: Try canonical normalized lookup, then CUDL-bridge fallbacks.

        cambridge_manifests.normalized_shelfmark is stored in CUDL classmark form,
        so the bridge's cudl_normalize() is the appropriate normalizer for queries
        against this table — different from the rest of the codebase.

        The wrapper takes a RAW shelfmark and performs canonical normalization
        internally — callers should NOT pre-normalize. This is the contract
        genizah_core.py relies on after the option-(b) migration (Plan 04).
        """
        if not shelfmark or self._conn is None:
            return None
        global _BRIDGE_IMPORT_WARNED
        try:
            from genizah_core import normalize_shelfmark
            from shared.shelfmark_bridge import cudl_normalize, shelfmark_to_cudl_label
        except ImportError as _e:
            # Round 3 Codex MEDIUM — degraded path must STILL normalize before lookup.
            # Pre-phase callers passed normalize_shelfmark(shelfmark) to
            # get_cambridge_manifest; after option-(b) migration callers pass raw,
            # so the wrapper must normalize internally even in the import-failure branch.
            if not _BRIDGE_IMPORT_WARNED:
                logger.warning("shelfmark_bridge unavailable in nli_crossref (degrading): %s", _e)
                _BRIDGE_IMPORT_WARNED = True
            try:
                from genizah_core import normalize_shelfmark as _ns
                return self.get_cambridge_manifest(_ns(shelfmark))
            except Exception:
                # Last-resort: hand the raw shelfmark in. Worse than canonical but
                # preserves at least exact-match behavior on already-canonical inputs.
                return self.get_cambridge_manifest(shelfmark)

        # 1. Existing canonical path (preserves pre-phase-84 behavior).
        url = self.get_cambridge_manifest(normalize_shelfmark(shelfmark))
        if url:
            return url
        # 2. cudl_normalize fallback (cambridge_manifests stores CUDL form).
        url = self.get_cambridge_manifest(cudl_normalize(shelfmark))
        if url:
            return url
        # 3. Mosseri-specific forward-label fallback:
        #    construct_mosseri_cudl_label() returns the actual CUDL `label` form,
        #    e.g. 'MS-MOSSERI-III-00027-O', which is what `cambridge_manifests.label`
        #    stores. Using slug.upper() would NOT match.
        try:
            from genizah_core import construct_mosseri_cudl_label
            mosseri_label = construct_mosseri_cudl_label(shelfmark)
            if mosseri_label:
                url = self.get_cambridge_manifest_by_label(mosseri_label)
                if url:
                    return url
        except ImportError:
            pass  # already warned above

        # 4. Generic forward-label fallback via shelfmark_to_cudl_label (T-S / Add. / Or.).
        slug = shelfmark_to_cudl_label(shelfmark)
        if slug:
            url = self.get_cambridge_manifest(slug)
            if url:
                return url
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

    def get_library_viewer_url(self, sys_id: str) -> Optional[dict]:
        """
        Get holding library digital collection URL for a manuscript.

        Constructs a URL to the library's digital collection based on the
        LibraryAbbrev from the NLI crossref data. Supports CUL (Cambridge),
        JTS/Princeton, Manchester, and BL (British Library).

        Args:
            sys_id: The Alma/system ID for the manuscript.

        Returns:
            Dict with keys: url, label, library_abbrev, library_name_eng.
            Returns None if no URL can be constructed (unknown library,
            missing data, or Oxford which uses a separate path).
        """
        if self._conn is None:
            return None
        try:
            cursor = self._conn.execute(
                "SELECT DISTINCT LibraryAbbrev, LibraryNameEng, Shelfmark "
                "FROM nli_images WHERE NLI_AlmaId = ? LIMIT 1",
                (sys_id,),
            )
            row = cursor.fetchone()
            if row is None:
                return None

            abbrev = (row["LibraryAbbrev"] or "").strip()
            name_eng = (row["LibraryNameEng"] or "").strip()
            shelfmark = (row["Shelfmark"] or "").strip()

            if not abbrev or not shelfmark:
                return None

            url = None
            label = None

            if abbrev == "CUL":
                # Cambridge: search-based fallback URL
                url = f"https://cudl.lib.cam.ac.uk/search?keyword={url_quote(shelfmark)}"
                label = "Cambridge Digital Library"
            elif abbrev == "JTS":
                # JTS / Princeton: try detail catalog page from sidecar, fall back to search
                dpul_url = self.get_jts_dpul_url(shelfmark)
                if dpul_url:
                    url = dpul_url
                else:
                    url = (
                        f"https://dpul.princeton.edu/cairo_geniza/catalog"
                        f"?search_field=all_fields&q={url_quote(shelfmark)}"
                    )
                label = "Princeton Digital Library"
            elif abbrev == "Manchester":
                # Manchester LUNA: try detail page from sidecar, fall back to search
                # Get first image's ImageSourceName to look up luna_id
                try:
                    img_cursor = self._conn.execute(
                        "SELECT ImageSourceName FROM nli_images "
                        "WHERE NLI_AlmaId = ? AND ImageSourceName != '' LIMIT 1",
                        (sys_id,),
                    )
                    img_row = img_cursor.fetchone()
                    img_source = (img_row["ImageSourceName"] or "").lower() if img_row else ""
                except Exception:
                    img_source = ""  # Image source lookup failed; use empty string

                luna_id = self.get_manchester_luna_id(img_source) if img_source else None
                if luna_id:
                    url = f"https://luna.manchester.ac.uk/luna/servlet/detail/{luna_id}"
                else:
                    url = (
                        f"https://luna.manchester.ac.uk/luna/servlet/view/search"
                        f"?q={url_quote(shelfmark)}&search=Go&QuickSearchA=QuickSearchA"
                    )
                label = "Manchester LUNA"
            elif abbrev == "BL":
                # Strip leaf suffix: "OR 10110.1" -> "OR 10110", "GASTER 1201.5" -> "GASTER 1201"
                # NOTE: Do NOT convert spaces to underscores -- searcharchives.bl.uk search
                # requires URL-encoded spaces (verified: underscores return zero results).
                bl_shelfmark = re.sub(r'\.\d+$', '', shelfmark)
                url = f"https://searcharchives.bl.uk/?q={url_quote(bl_shelfmark)}"
                label = "British Library"
            else:
                # Oxford and others: no known URL pattern
                return None

            return {
                "url": url,
                "label": label,
                "library_abbrev": abbrev,
                "library_name_eng": name_eng,
            }
        except Exception as e:
            logger.error(f"NliCrossrefService.get_library_viewer_url error for {sys_id}: {e}")
            return None

    # ── Manchester LUNA (Phase 34: IMG-05) ──────────────────────────

    def get_manchester_luna_id(self, image_source_name: str) -> Optional[str]:
        """
        Get Manchester LUNA internal ID for an image source name (JRL filename).

        Args:
            image_source_name: Lowercased JRL filename (e.g., 'rylands_jrl1379735').

        Returns:
            LUNA ID string or None if not found / table missing.
        """
        if self._conn is None or not image_source_name:
            return None
        try:
            cursor = self._conn.execute(
                "SELECT luna_id FROM manchester_luna WHERE image_source_name = ?",
                (image_source_name,),
            )
            row = cursor.fetchone()
            return row["luna_id"] if row else None
        except Exception as e:
            logger.debug(f"NliCrossrefService.get_manchester_luna_id error: {e}")
            return None

    def get_manchester_manifest_url(self, image_source_name: str) -> Optional[str]:
        """
        Get Manchester LUNA IIIF manifest URL for an image source name.

        Args:
            image_source_name: Lowercased JRL filename (e.g., 'rylands_jrl1379735').

        Returns:
            IIIF manifest URL string or None if not found / table missing.
        """
        luna_id = self.get_manchester_luna_id(image_source_name)
        if luna_id:
            return f"https://luna.manchester.ac.uk/luna/servlet/iiif/m/{luna_id}/manifest"
        return None

    def get_manchester_canvases(self, sys_id: str) -> list[dict]:
        """
        Build canvas entries for ALL Manchester crossref images (each has its own luna_id).

        Instead of fetching a single IIIF manifest (which only contains 1 canvas per luna_id),
        this resolves each crossref image directly to its IIIF image service URL.

        Args:
            sys_id: The Alma/system ID for the manuscript.

        Returns:
            List of canvas dicts: {'label': str, 'url': str, 'folio_num': int|None}
            Only includes images whose luna_id was found. Preserves ImageName sort order.
        """
        images = self.get_images(sys_id)
        if not images:
            return []
        canvases = []
        for img in images:
            img_source = (img.get('image_source_name', '') or '').lower()
            if not img_source:
                continue
            luna_id = self.get_manchester_luna_id(img_source)
            if not luna_id:
                continue
            label = parse_folio_label(img.get('image_name', ''))
            url = f"https://luna.manchester.ac.uk/luna/servlet/iiif/{luna_id}"
            # Extract leading integer from label for folio_num
            folio_num = None
            if label:
                m = re.match(r'(\d+)', label)
                if m:
                    folio_num = int(m.group(1))
            canvases.append({'label': label, 'url': url, 'folio_num': folio_num})
        return canvases

    # ── JTS/Princeton DPUL (Phase 34: IMG-05) ────────────────────────

    def get_jts_manifest_url(self, shelfmark: str) -> Optional[str]:
        """
        Get JTS Figgy IIIF manifest URL for a shelfmark.

        Tries each variant in turn: the full shelfmark as given, the
        shelfmark without a leading ``Ms. `` / ``MS. `` prefix (NLI MARC
        942$z uses this prefix for JTS holdings, e.g. ``Ms. ENA 1052.1``,
        while ``jts_dpul.shelfmark`` stores the bare form ``ENA 1052.1``),
        and finally each of the above with any trailing ``.N`` leaf
        suffix stripped.

        Args:
            shelfmark: JTS shelfmark (e.g., 'ENA 2573.1', 'ENA 2573',
                'Ms. ENA 1052.1').

        Returns:
            Figgy manifest URL string or None if not found / table missing.
        """
        if self._conn is None or not shelfmark:
            return None
        try:
            for variant in _jts_shelfmark_variants(shelfmark):
                cursor = self._conn.execute(
                    "SELECT manifest_url FROM jts_dpul WHERE shelfmark = ?",
                    (variant,),
                )
                row = cursor.fetchone()
                if row:
                    return row["manifest_url"]
            return None
        except Exception as e:
            logger.debug(f"NliCrossrefService.get_jts_manifest_url error: {e}")
            return None

    def get_jts_urls_for_sys_id(self, sys_id: str) -> Optional[dict]:
        """Find JTS manifest + DPUL URLs using the canonical
        ``nli_images.Shelfmark`` for ``sys_id``, not the user-facing
        shelfmark string.

        ``jts_dpul.shelfmark`` stores the bare JTS form (e.g.
        ``"ENA 1052.1"``), which is also what ``nli_images.Shelfmark``
        carries. csv_bank-derived shelfmarks pass through abbreviation
        prefixes (``"Ms. 1052.1"``) or catalog-wrapper strings
        (``"The Jewish Theological Seminary... Ms. ENA 1052.1"``) that
        won't match; iterating every call_number variant is wasteful.
        One JOIN keyed on sys_id is enough.

        Returns ``{"shelfmark", "manifest_url", "dpul_url"}`` for the
        first matching nli_images row, or None when no match exists.
        """
        if self._conn is None or not sys_id:
            return None
        try:
            row = self._conn.execute(
                "SELECT j.shelfmark, j.manifest_url, j.dpul_url "
                "FROM nli_images n JOIN jts_dpul j "
                "ON n.Shelfmark = j.shelfmark "
                "WHERE n.NLI_AlmaId = ? LIMIT 1",
                (sys_id,),
            ).fetchone()
            if row:
                return {
                    'shelfmark': row['shelfmark'],
                    'manifest_url': row['manifest_url'],
                    'dpul_url': row['dpul_url'],
                }
            return None
        except Exception as e:
            logger.debug(f"NliCrossrefService.get_jts_urls_for_sys_id error: {e}")
            return None

    def get_jts_dpul_url(self, shelfmark: str) -> Optional[str]:
        """
        Get JTS/Princeton DPUL catalog page URL for a shelfmark.

        Same variant-matching logic as get_jts_manifest_url: tries the
        shelfmark as given, with any leading ``Ms. ``/``MS. `` stripped,
        and with trailing ``.N`` leaf suffixes stripped.

        Args:
            shelfmark: JTS shelfmark (e.g., 'ENA 2573.1', 'ENA 2573',
                'Ms. ENA 1052.1').

        Returns:
            DPUL catalog URL string or None if not found / table missing.
        """
        if self._conn is None or not shelfmark:
            return None
        try:
            for variant in _jts_shelfmark_variants(shelfmark):
                cursor = self._conn.execute(
                    "SELECT dpul_url FROM jts_dpul WHERE shelfmark = ?",
                    (variant,),
                )
                row = cursor.fetchone()
                if row:
                    return row["dpul_url"]
            return None
        except Exception as e:
            logger.debug(f"NliCrossrefService.get_jts_dpul_url error: {e}")
            return None

    # ── Metadata Enrichment (Phase 33: META-03) ─────────────────────

    def get_catalog_entry(self, sys_id: str) -> Optional[str]:
        """
        Get catalog entry reference (e.g., Neubauer-Cowley) for a manuscript.

        All values are Oxford Neubauer-Cowley references from the NLI crossref data.

        Args:
            sys_id: The Alma/system ID for the manuscript.

        Returns:
            Catalog entry string (e.g., 'Neubauer - Cowley 2603.1') or None.
        """
        if self._conn is None:
            return None
        try:
            cursor = self._conn.execute(
                "SELECT DISTINCT CatalogEntry FROM nli_images "
                "WHERE NLI_AlmaId = ? AND CatalogEntry != '' LIMIT 1",
                (sys_id,),
            )
            row = cursor.fetchone()
            return row["CatalogEntry"] if row else None
        except Exception as e:
            logger.error(f"NliCrossrefService.get_catalog_entry error for {sys_id}: {e}")
            return None

    def get_collection_storage(self, sys_id: str) -> Optional[dict]:
        """
        Get collection and physical storage references for a manuscript.

        Returns collection name and original box/volume/folio storage information
        from the NLI crossref data.

        Args:
            sys_id: The Alma/system ID for the manuscript.

        Returns:
            Dict with keys: collection_name, ob_box, ob_volume, ob_folio.
            Returns None if no non-empty data found.
        """
        if self._conn is None:
            return None
        try:
            cursor = self._conn.execute(
                "SELECT DISTINCT CollectionName, OBBox, OBVolume, OBFolio "
                "FROM nli_images WHERE NLI_AlmaId = ? "
                "AND (CollectionName != '' OR OBBox != '' OR OBVolume != '' OR OBFolio != '') "
                "LIMIT 1",
                (sys_id,),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return {
                "collection_name": row["CollectionName"],
                "ob_box": row["OBBox"],
                "ob_volume": row["OBVolume"],
                "ob_folio": row["OBFolio"],
            }
        except Exception as e:
            logger.error(f"NliCrossrefService.get_collection_storage error for {sys_id}: {e}")
            return None

    def get_crossref_metadata(self, sys_id: str) -> dict:
        """Fetch all browse-relevant crossref metadata for a sys_id in one call.

        Consolidates individual crossref queries used by the browse page into
        a single method call for use in parallel enrichment.

        Args:
            sys_id: The Alma/system ID for the manuscript.

        Returns:
            Dict with keys: catalog_entry, collection_storage,
            physical_metadata. Returns empty dict if service unavailable.
        """
        if self._conn is None or not sys_id:
            return {}
        return {
            'catalog_entry': self.get_catalog_entry(sys_id),
            'collection_storage': self.get_collection_storage(sys_id),
            'physical_metadata': self.get_physical_metadata(sys_id),
        }

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
        result = {
            "nli_fgp": False, "cambridge": False,
            "manchester": False, "jts": False,
            "image_count": 0,
        }

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

        # Check Manchester LUNA (table may not exist)
        try:
            man_cursor = self._conn.execute(
                "SELECT COUNT(*) as cnt FROM manchester_luna m "
                "JOIN nli_images i ON LOWER(i.ImageSourceName) = m.image_source_name "
                "WHERE i.NLI_AlmaId = ?",
                (sys_id,),
            )
            man_row = man_cursor.fetchone()
            if man_row and man_row["cnt"] > 0:
                result["manchester"] = True
        except Exception:
            pass  # table may not exist

        # Check JTS DPUL (table may not exist)
        try:
            if normalized_shelfmark:
                # Use the original shelfmark from nli_images for JTS lookup
                sm_cursor = self._conn.execute(
                    "SELECT DISTINCT Shelfmark FROM nli_images WHERE NLI_AlmaId = ? LIMIT 1",
                    (sys_id,),
                )
                sm_row = sm_cursor.fetchone()
                if sm_row and sm_row["Shelfmark"]:
                    jts_shelfmark = sm_row["Shelfmark"]
                    jts_cursor = self._conn.execute(
                        "SELECT COUNT(*) as cnt FROM jts_dpul WHERE shelfmark = ?",
                        (jts_shelfmark,),
                    )
                    jts_row = jts_cursor.fetchone()
                    if jts_row and jts_row["cnt"] > 0:
                        result["jts"] = True
        except Exception:
            pass  # table may not exist

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


def reset_nli_crossref_service():
    """Reset the singleton NliCrossrefService instance.

    Call this after replacing the nli_crossref.db sidecar file to force
    re-initialization on next access. Closes the existing connection
    before clearing the singleton.
    """
    global _default_service
    if _default_service is not None:
        _default_service.close()
        _default_service = None


# ── Folio+side canvas resolver (260419-cfx) ────────────────────────────
#
# CUDL IIIF manifests sometimes expose fewer canvases than a manuscript
# has transcription pages (e.g. T-S NS 158.112: 12 CUDL canvases vs 14
# transcription pages). Positional indexing `images_ext[page]` therefore
# returns the wrong canvas past the shortfall, and paired-leaf bifolios
# shift the ordering earlier. The resolver below maps a transcription
# page index → the exact CUDL canvas that matches the (folio_num, side)
# of the N-th nli_images row; callers serve an NLI fallback when no
# CUDL canvas matches.
#
# NEVER construct NLI IIIF URLs from NliCrossrefService.get_images()'s
# `fgp_image_number_id` column — that is a Friedberg photo number, not
# an NLI IIIF FL id; they are different numbering systems (see
# .planning/research/PITFALLS.md Pitfall 6). FL ids come from the NLI
# IIIF manifest's canvas_map (web: web.api.fetch_fl_ids_from_nli;
# desktop: GenizahSearchEngine.fetch_iiif_manifest[canvas_map]).

_DEGRADED: dict = {'degraded': True}
_SIDE_FROM_LABEL_RE = re.compile(r'^(\d+)([rv])?$', re.IGNORECASE)

# NLI MARC 942$z returns JTS shelfmarks with a leading ``Ms. `` / ``MS. ``
# abbreviation (e.g. ``Ms. ENA 1052.1``), but jts_dpul.shelfmark stores
# the bare form (``ENA 1052.1``). Strip these prefixes so the exact-match
# lookup in get_jts_manifest_url / get_jts_dpul_url does not miss.
_MS_PREFIX_RE = re.compile(r'^\s*ms\.?\s+', re.IGNORECASE)


def _jts_shelfmark_variants(shelfmark: str) -> list[str]:
    """Yield lookup variants for a JTS shelfmark, most-specific first.

    For ``"Ms. ENA 1052.1"`` returns::

        ["Ms. ENA 1052.1", "ENA 1052.1", "Ms. ENA 1052", "ENA 1052"]

    For ``"ENA 1052.1"`` returns::

        ["ENA 1052.1", "ENA 1052"]

    Duplicates are removed while preserving order. Callers iterate and
    query each variant against jts_dpul.shelfmark in turn.
    """
    if not shelfmark:
        return []
    variants: list[str] = []

    def _add(s: str) -> None:
        if s and s not in variants:
            variants.append(s)

    raw = shelfmark.strip()
    _add(raw)

    # Strip leading "Ms. " / "MS. " / "Ms " / "MS "
    no_ms = _MS_PREFIX_RE.sub('', raw).strip()
    _add(no_ms)

    # For each form so far, add the base (trailing ".N" leaf stripped)
    for v in list(variants):
        base = re.sub(r'\.\d+$', '', v)
        if base != v:
            _add(base)

    return variants


def _extract_side_from_nli_label(folio_label: str) -> Optional[str]:
    """Return 'r' or 'v' from a label like '1r' or '8v'; None otherwise.

    This is a lightweight last-character check; the resolver uses the
    stricter _SIDE_FROM_LABEL_RE for full parsing.
    """
    if not folio_label:
        return None
    last = folio_label[-1].lower()
    if last in ('r', 'v'):
        return last
    return None


def resolve_cambridge_canvas_for_page(
    sys_id: str,
    page: int,
    images_ext: list,
    *,
    svc: Optional['NliCrossrefService'] = None,
) -> Optional[dict]:
    """Map a transcription page index → CUDL canvas index using (folio, side).

    Args:
        sys_id: Manuscript Alma/system ID.
        page: 0-based transcription page index.
        images_ext: The CUDL canvas list for this manuscript (each entry
            carries 'folio_num' and 'folio_side' — produced by
            GenizahSearchEngine.fetch_external_iiif_data).
        svc: Optional NliCrossrefService. When None, the module-level
            singleton is used.

    Returns:
        - {'canvas_index': int, 'folio_num': int, 'side': 'r'|'v'} on a
          successful (folio, side) match. Caller uses images_ext[canvas_index].
        - None when the resolver identified a target (folio, side) but no
          CUDL canvas matches. Caller should serve the NLI image for
          this page (e.g. via fetch_fl_ids_from_nli(sys_id)[page]).
        - {'degraded': True} when the sidecar is unavailable OR the
          sys_id has no nli_images rows. Caller should fall back to
          legacy positional behavior (images_ext[page]) and log WARN
          once per sys_id.

    The resolver NEVER constructs NLI IIIF URLs from FGPImageNumberId.
    It only consults the sorted (leaf, side) order of the NLI ImageNames
    for this sys_id; the caller is responsible for producing the NLI
    image bytes when the resolver returns None (and MUST source FL ids
    from the NLI IIIF manifest canvas_map, not from nli_crossref).
    """
    if svc is None:
        svc = get_nli_crossref_service(thread_safe=True)
    if svc is None or not svc.is_available():
        return dict(_DEGRADED)
    try:
        folio_rows = svc.get_folio_images(sys_id)
    except Exception as e:
        logger.warning(f"resolve_cambridge_canvas_for_page: get_folio_images error for {sys_id}: {e}")
        return dict(_DEGRADED)
    if not folio_rows:
        return dict(_DEGRADED)
    if page < 0 or page >= len(folio_rows):
        # Out of nli_images range — caller may still attempt NLI fallback
        # (which will also fail for pages past manifest length, and that
        # is fine: the endpoint returns 404 in that case).
        return None

    target = folio_rows[page]
    target_folio_label = target.get('folio_label', '') or ''
    m = _SIDE_FROM_LABEL_RE.match(target_folio_label)
    if not m:
        return None
    try:
        target_folio = int(m.group(1))
    except (TypeError, ValueError):
        return None
    side_raw = m.group(2)
    # Convention: bare numeric target folio label defaults to recto.
    target_side = side_raw.lower() if side_raw else 'r'

    # 1. Exact (folio_num, side) match.
    for idx, c in enumerate(images_ext or []):
        if c.get('folio_num') == target_folio and c.get('folio_side') == target_side:
            return {
                'canvas_index': idx,
                'folio_num': target_folio,
                'side': target_side,
            }

    # 2. Side-less canvas (bare-numeric label like '1', folio_side=None):
    #    only matches when target is recto. Verso targets fall through
    #    to NLI fallback.
    if target_side == 'r':
        for idx, c in enumerate(images_ext or []):
            if c.get('folio_num') == target_folio and not c.get('folio_side'):
                return {
                    'canvas_index': idx,
                    'folio_num': target_folio,
                    'side': 'r',
                }

    return None


def classify_cambridge_alignment(
    sys_id: str,
    images_ext: list,
    *,
    svc: Optional['NliCrossrefService'] = None,
) -> dict:
    """Classify whether a CUDL canvas list aligns with NLI folio order.

    The 260419-cfx fix only flipped the default image source to NLI when
    ``len(images_ext) != len(images_nli)``. That count heuristic misses
    manuscripts where CUDL and NLI have the same length but different
    canvas order (e.g. CUDL prepends a binding canvas and drops a folio,
    or CUDL uses a different foliation model entirely — see Or.2245,
    sys_id 990001332980205171). In those cases positional indexing still
    serves a wrong-leaf image silently.

    This helper is a single source of truth for the per-position
    alignment check. Callers (``enrich_metadata`` and each UI default-
    source site) should consume the returned verdict instead of
    recomputing their own length check.

    Args:
        sys_id: Manuscript Alma/system ID.
        images_ext: CUDL canvas list (each entry expected to carry
            ``folio_num`` and ``folio_side`` — produced by
            ``GenizahSearchEngine.fetch_external_iiif_data`` via
            ``_parse_cudl_label``).
        svc: Optional NliCrossrefService; the module singleton is used
            when None.

    Returns:
        A dict with keys:

        - ``verdict``: ``'aligned'`` (safe to keep CUDL as default),
          ``'misaligned'`` (caller should default to NLI), or
          ``'unknown'`` (insufficient info — caller should keep legacy
          default behavior).
        - ``reason``: ``'ok'``, ``'count_mismatch'``,
          ``'position_mismatch'``, ``'no_sidecar'``, ``'no_ext'``.
        - ``ext_count`` / ``nli_count``: integers for logging.
        - ``first_mismatch_index``: int when ``reason == 'position_mismatch'``,
          else None.

    Scope: callers are responsible for gating on
    ``external_provider == 'cambridge' and lib_code == 'CUL'``. The
    helper itself does not inspect the library code; it only examines
    the canvas ordering.

    Per Codex 2026-04-21 review caveats:
    - Unparseable CUDL canvases at position i (``folio_num is None``
      or ``folio_side is None``) are skipped rather than forced into a
      mismatch. Binding/cover canvases in an otherwise aligned list
      should not flip the whole manuscript to NLI.
    - Unparseable NLI folio labels at position i are likewise skipped.
    - A concrete ``(folio_num, side)`` disagreement at any comparable
      position still triggers ``misaligned / position_mismatch``.
    """
    if not images_ext:
        return {
            'verdict': 'unknown',
            'reason': 'no_ext',
            'ext_count': 0,
            'nli_count': 0,
            'first_mismatch_index': None,
        }

    if svc is None:
        svc = get_nli_crossref_service(thread_safe=True)
    if svc is None or not svc.is_available():
        return {
            'verdict': 'unknown',
            'reason': 'no_sidecar',
            'ext_count': len(images_ext),
            'nli_count': 0,
            'first_mismatch_index': None,
        }

    try:
        folio_rows = svc.get_folio_images(sys_id)
    except Exception as e:
        logger.warning(
            f"classify_cambridge_alignment: get_folio_images error for {sys_id}: {e}"
        )
        return {
            'verdict': 'unknown',
            'reason': 'no_sidecar',
            'ext_count': len(images_ext),
            'nli_count': 0,
            'first_mismatch_index': None,
        }

    ext_count = len(images_ext)
    nli_count = len(folio_rows or [])

    if nli_count == 0:
        return {
            'verdict': 'unknown',
            'reason': 'no_sidecar',
            'ext_count': ext_count,
            'nli_count': 0,
            'first_mismatch_index': None,
        }

    if ext_count != nli_count:
        return {
            'verdict': 'misaligned',
            'reason': 'count_mismatch',
            'ext_count': ext_count,
            'nli_count': nli_count,
            'first_mismatch_index': None,
        }

    for i, (ext_c, nli_r) in enumerate(zip(images_ext, folio_rows)):
        ext_folio = ext_c.get('folio_num')
        ext_side = ext_c.get('folio_side')
        if ext_folio is None or ext_side is None:
            continue

        nli_label = nli_r.get('folio_label', '') or ''
        m = _SIDE_FROM_LABEL_RE.match(nli_label)
        if not m:
            continue
        try:
            nli_folio = int(m.group(1))
        except (TypeError, ValueError):
            continue
        side_raw = m.group(2)
        nli_side = side_raw.lower() if side_raw else 'r'

        if ext_folio != nli_folio or ext_side != nli_side:
            return {
                'verdict': 'misaligned',
                'reason': 'position_mismatch',
                'ext_count': ext_count,
                'nli_count': nli_count,
                'first_mismatch_index': i,
            }

    return {
        'verdict': 'aligned',
        'reason': 'ok',
        'ext_count': ext_count,
        'nli_count': nli_count,
        'first_mismatch_index': None,
    }
