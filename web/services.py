"""
GenizahService - Thread-safe wrapper for genizah_core.
Updated to proxy web.state.state for backward compatibility.
"""

import logging
import sys
import os
import re
from typing import Optional, List, Dict, Tuple
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# Add parent directory to path for importing genizah_core
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from genizah_core import (
    get_library_display,
)
from shared.synthetic_sys_id import is_synthetic_sys_id  # noqa: F401  Phase 85 D-06/D-08/D-14: imported as defensive marker for Phase 86 AUDIT-03. Synthetic-aware page-count plumbing flows through web/pages/browse_enrichment.py:250 (Phase B cambridge_images population) — NOT through this file. The plan's pseudo-code expected a dict-with-canvases shape from get_cambridge_manifest_with_bridge, but that function returns a single manifest URL string. See .planning/phases/85-synthetic-fjms-inventory-rows/85-04-AUDIT.md "web/services.py" section for details.
from web.state import state

# Library-specific attribution: (english, hebrew) tuples.
# None = attribution comes from IIIF manifest (don't override).
# Missing key = NLI default.
_NLI_EN = 'National Library of Israel'
_NLI_HE = '\u05d4\u05e1\u05e4\u05e8\u05d9\u05d9\u05d4 \u05d4\u05dc\u05d0\u05d5\u05de\u05d9\u05ea'

ATTRIBUTION_BY_LIBRARY = {
    'CUL': None,        # Cambridge IIIF manifest provides attribution
    'JTS': None,        # JTS/Princeton Figgy manifest provides attribution
    'Manchester': ('The University of Manchester Library \u00b7 CC BY-NC-SA 4.0',
                   'The University of Manchester Library \u00b7 CC BY-NC-SA 4.0'),
    'Oxford': ('Bodleian Libraries, University of Oxford \u00b7 CC BY-NC 4.0',
               'Bodleian Libraries, University of Oxford \u00b7 CC BY-NC 4.0'),
    'BL': (f'British Library \u00b7 image: {_NLI_EN}',
           f'\u05d4\u05e1\u05e4\u05e8\u05d9\u05d9\u05d4 \u05d4\u05d1\u05e8\u05d9\u05d8\u05d9\u05ea \u00b7 image: {_NLI_HE}'),
    'RNL': (f'National Library of Russia \u00b7 image: {_NLI_EN}',
            f'\u05d4\u05e1\u05e4\u05e8\u05d9\u05d9\u05d4 \u05d4\u05dc\u05d0\u05d5\u05de\u05d9\u05ea \u05e9\u05dc \u05e8\u05d5\u05e1\u05d9\u05d4 \u00b7 image: {_NLI_HE}'),
    'AIU': (f'Alliance Isra\u00e9lite Universelle \u00b7 image: {_NLI_EN}',
            f'\u05d0\u05dc\u05d9\u05d0\u05e0\u05e1 \u05d9\u05e9\u05e8\u05d0\u05dc\u05d9\u05ea \u05d0\u05d5\u05e0\u05d9\u05d1\u05e8\u05e1\u05dc\u05d9\u05ea \u00b7 image: {_NLI_HE}'),
    'Mosseri': (f'Mosseri Collection \u00b7 image: {_NLI_EN}',
                f'\u05d0\u05d5\u05e1\u05e3 \u05de\u05d5\u05e1\u05e8\u05d9 \u00b7 image: {_NLI_HE}'),
    'Gaster': (f'Gaster Collection \u00b7 image: {_NLI_EN}',
               f'\u05d0\u05d5\u05e1\u05e3 \u05d2\u05e1\u05d8\u05e8 \u00b7 image: {_NLI_HE}'),
    'Halper': (f'Halper Collection \u00b7 image: {_NLI_EN}',
               f'\u05d0\u05d5\u05e1\u05e3 \u05d4\u05dc\u05e4\u05e8 \u00b7 image: {_NLI_HE}'),
    'Westminster': (f'Westminster College \u00b7 image: {_NLI_EN}',
                    f'\u05de\u05db\u05dc\u05dc\u05ea \u05d5\u05e1\u05d8\u05de\u05d9\u05e0\u05e1\u05d8\u05e8 \u00b7 image: {_NLI_HE}'),
    'Freer': (f'Freer Gallery of Art \u00b7 image: {_NLI_EN}',
              f'Freer Gallery of Art \u00b7 image: {_NLI_HE}'),
    'HUC': (f'Hebrew Union College \u00b7 image: {_NLI_EN}',
            f'\u05d4\u05d9\u05d1\u05e8\u05d5 \u05d9\u05d5\u05e0\u05d9\u05d5\u05df \u05e7\u05d5\u05dc\u05d2\u05f3 \u00b7 image: {_NLI_HE}'),
}
from web.translations import get_language


def _get_library_attribution(library_code: str) -> Optional[str]:
    """Get language-aware attribution for a library code, or None to keep IIIF manifest."""
    entry = ATTRIBUTION_BY_LIBRARY.get(library_code)
    if entry is None:
        return None  # None in dict = keep IIIF manifest; missing key handled by caller
    if isinstance(entry, tuple):
        return entry[1] if get_language() == 'he' else entry[0]
    return entry

# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class SearchResult:
    """Structured search result for the web UI."""
    uid: str
    sys_id: str
    display: Dict[str, str]
    snippet: str
    raw_header: str
    source: str
    full_text: str = ''
    highlight_pattern: Optional[str] = None
    cross_page: bool = False
    page_highlights: List[Dict] = field(default_factory=list)
    scope: str = 'page'
    library_code: str = ''  # Library code (e.g., 'CUL', 'JTS')

@dataclass
class BrowsePage:
    """Page data with navigation info."""
    uid: str
    p_num: int
    text: str
    full_header: str
    total_pages: int
    current_idx: int
    sys_id: str
    fl_id: Optional[str] = None
    shelfmark: str = ''
    title: str = ''
    thumb_url: Optional[str] = None
    image_url: Optional[str] = None
    internal_index: int = 0
    attribution: str = ''  # Image credit/attribution
    is_oxford: bool = False  # Whether this is an Oxford manuscript
    is_cambridge: bool = False # Whether this is a Cambridge manuscript
    external_url: Optional[str] = None # URL to external viewer (Bodleian/CUDL)
    oxford_part_id: Optional[str] = None # Oxford Part ID (e.g. "MS. Heb. d. 29/2")
    oxford_part_display: str = '' # Display name for part (e.g. "heb. d. 29 part 2")
    oxford_part_metadata: Dict[str, str] = field(default_factory=dict) # Oxford Part metadata
    library_code: str = ''  # Library code (e.g., 'CUL', 'JTS')
    library_name: str = ''  # Full library name for display
    folio_label: str = ''  # Current folio label (e.g., '1r', '2v')
    image_source_info: Dict = field(default_factory=dict)  # {nli_fgp: bool, cambridge: bool, image_count: int}
    folio_images: List[Dict] = field(default_factory=list)  # Folio sequence from NliCrossrefService
    cambridge_images: List[Dict] = field(default_factory=list)  # Cambridge IIIF canvas URLs from nli_cache images_ext
    external_provider: str = ''  # Which library provided images_ext: 'manchester', 'jts', or '' (Cambridge)
    cambridge_alignment: Optional[Dict] = None  # 260421-aln: CUDL↔NLI alignment verdict (see shared.nli_crossref_service.classify_cambridge_alignment)
    physical_metadata: Optional[Dict] = None  # {material, num_folio, num_bifolio, size} from NLI crossref
    library_viewer_url: Optional[Dict] = None  # {url, label, library_abbrev} for holding library link
    # Volume-aware browse (multi-IE manuscripts)
    volume_ie: Optional[str] = None  # Active IE identifier (e.g. 'IE89040977')
    volume_suffix: int = 1  # IIIF manifest suffix for active IE (1=primary)
    volume_count: int = 1  # Total number of volumes/IEs for this manuscript
    volumes: List[Dict] = field(default_factory=list)  # [{ie_id, suffix, page_count}, ...]

@dataclass
class DocumentPage:
    """Single page data for document viewer."""
    uid: str
    p_num: int
    text: str
    full_header: str
    fl_id: Optional[str] = None
    sys_id: str = ''

@dataclass
class ManuscriptInfo:
    sys_id: str
    shelfmark: str
    title: str
    page_count: int = 0
    thumb_url: Optional[str] = None
    has_external_images: bool = False
    oxford_part_id: Optional[str] = None
    attribution: str = ''
    library_code: str = ''  # Library code (e.g., 'CUL', 'JTS')

@dataclass
class ImageInfo:
    label: str
    url: str
    thumb_url: str
    source: str
    fl_id: Optional[str] = None
    folio_num: Optional[int] = None

# ============================================================================
# Image URL Helpers
# ============================================================================

NLI_IIIF_BASE = "https://iiif.nli.org.il/IIIFv21"


def get_thumbnail_url(fl_id: str, size: int = 400) -> str:
    if not fl_id: return ''
    digits = re.sub(r"\D", "", str(fl_id))
    if not digits: return ''
    return f"{NLI_IIIF_BASE}/FL{digits}/full/{size},/0/default.jpg"

def get_full_image_url(fl_id: str) -> str:
    if not fl_id: return ''
    digits = re.sub(r"\D", "", str(fl_id))
    if not digits: return ''
    return f"{NLI_IIIF_BASE}/FL{digits}/full/2000,/0/default.jpg"

def build_iiif_image_url(base_url: str, size: str = 'full') -> str:
    if not base_url: return ''
    if size == 'thumb': size_param = '400,'
    elif size == 'full': size_param = '2000,'
    else: size_param = size
    return f"{base_url}/full/{size_param}/0/default.jpg"


def is_oxford_manuscript(shelfmark: str = '', library_code: str = '') -> bool:
    """Classify Oxford manuscripts using library code first, shelfmark fallback."""
    if (library_code or '').strip().lower() == 'oxford':
        return True
    shelfmark_lower = (shelfmark or '').strip().lower()
    return shelfmark_lower.startswith('ms heb') or shelfmark_lower.startswith('ms. heb')


def get_oxford_direct_image_url(shelfmark: str = '', page_idx: int = 0, folio_offset: int = 0) -> str:
    """Build direct Bodleian image URL from Oxford shelfmark (MS heb. e.93/58).

    Args:
        folio_offset: For multi-IE manuscripts, offset the folio number by this amount
                      (e.g., Volume 2 of d.50/19 → folio 20 = offset 1).
    """
    if not shelfmark:
        return ''
    match = re.match(
        r'^(?:MS\.?\s*)?Heb\.?\s*([a-z])\.?\s*(\d+)[./](\d+)',
        shelfmark.strip(),
        re.IGNORECASE,
    )
    if not match:
        return ''
    letter, volume, folio = match.groups()
    folio_num = int(folio) + folio_offset
    side = 'b' if int(page_idx or 0) % 2 == 1 else 'a'
    return f"https://hebrew.bodleian.ox.ac.uk/fragments/full/MS_HEB_{letter}_{volume}_{folio_num}{side}.jpg"

# ============================================================================
# Service Proxy
# ============================================================================

class GenizahService:
    """Proxy class that maps Service calls to the global State objects."""

    @property
    def is_ready(self) -> bool:
        return state.is_ready()

    @property
    def index_exists(self) -> bool:
        return state.searcher and state.searcher.index is not None

    @property
    def init_error(self) -> Optional[str]:
        return None

    def initialize(self) -> bool:
        # Initialization is handled by main.py startup thread
        return True

    def search_by_shelfmark(self, shelfmark_query: str, limit: int = 100) -> Tuple[List[ManuscriptInfo], bool]:
        """
        Search for manuscripts by shelfmark using MetadataManager.resolve_system_by_shelfmark().

        This uses the same logic as the desktop app:
        - Normalizes query and all shelfmarks for comparison
        - Finds exact matches first, then partial matches
        - Sorts using natural_sort_key (100.1, 100.2, 100.10 not 100.1, 100.10, 100.2)

        Returns:
            Tuple of (list of ManuscriptInfo, exact_match_found boolean)
            If single exact match is found, returns that match with exact_match=True.
            Otherwise returns sorted suggestions with exact_match=False.
        """
        if not self.is_ready or not state.meta_mgr:
            return [], False

        try:
            # Use the same resolution logic as desktop app
            result = state.meta_mgr.resolve_system_by_shelfmark(shelfmark_query, limit=limit)

            # Single exact match found
            if result.get('sys_id'):
                sys_id = result['sys_id']
                library_code = state.meta_mgr.get_library_for_id(sys_id) if state.meta_mgr else ''
                return [ManuscriptInfo(
                    sys_id=sys_id,
                    shelfmark=result.get('selected_shelfmark', ''),
                    title='',  # Title fetched later if needed
                    library_code=library_code
                )], True

            # Multiple options - already sorted by natural_sort_key in genizah_core
            options = result.get('options', [])
            if not options:
                return [], False

            manuscripts = []
            for opt in options:
                sys_id = opt['sys_id']
                library_code = state.meta_mgr.get_library_for_id(sys_id) if state.meta_mgr else ''
                manuscripts.append(ManuscriptInfo(
                    sys_id=sys_id,
                    shelfmark=opt['shelfmark'],
                    title=opt.get('title', ''),
                    library_code=library_code
                ))

            # If there's only one option, treat it as exact match
            if len(manuscripts) == 1:
                return manuscripts, True

            return manuscripts, False

        except Exception as e:
            logger.error("Search by shelfmark error: %s", e)
            return [], False

    def get_browse_page(self, sys_id: str, p_num: Optional[int] = None, direction: int = 0, absolute_index: Optional[int] = None, allow_cross: bool = False, volume_ie: Optional[str] = None) -> Optional[BrowsePage]:
        """Phase A (hot path): Tantivy + csv_bank only. No SQLite/crossref calls.

        Args:
            volume_ie: If set, navigate within this IE's pages only (for multi-IE manuscripts).
        """
        if not self.is_ready: return None
        try:
            result = state.searcher.get_browse_page(
                sys_id, p_num=p_num, next_prev=direction, absolute_index=absolute_index, allow_cross=allow_cross, volume_ie=volume_ie
            )
            if not result: return None

            actual_sys_id = result.get('sys_id', sys_id)
            shelfmark, title = state.meta_mgr.get_meta_for_id(actual_sys_id)
            library_code = state.meta_mgr.get_library_for_id(actual_sys_id)
            library_name = get_library_display(library_code, short=False, lang=get_language()) if library_code else ''

            fl_id = None
            try:
                parsed = state.meta_mgr.parse_full_id_components(result.get('full_header', ''))
                fl_id = parsed.get('fl_id')
            except Exception:
                pass  # PGP enrichment failed for this result; continue without

            thumb_url = get_thumbnail_url(fl_id) if fl_id else None
            image_url = get_full_image_url(fl_id) if fl_id else None

            # Pure string check -- no I/O
            is_oxford = is_oxford_manuscript(shelfmark, library_code)

            # Default NLI attribution (Phase B will refine if needed)
            attribution = _NLI_HE if get_language() == 'he' else _NLI_EN

            # Volume info for multi-IE manuscripts
            from genizah_core import get_volumes_for_sys_id, _extract_ie_from_header
            volumes = get_volumes_for_sys_id(actual_sys_id)
            active_ie = result.get('volume_ie') or _extract_ie_from_header(result.get('full_header', ''))
            volume_suffix = 1
            if volumes and active_ie:
                for v in volumes:
                    if v['ie_id'] == active_ie:
                        volume_suffix = v['suffix']
                        break

            return BrowsePage(
                uid=result.get('uid', ''),
                p_num=result.get('p_num', 0),
                text=result.get('text', ''),
                full_header=result.get('full_header', ''),
                total_pages=result.get('total_pages', 0),
                current_idx=result.get('current_idx', 0),
                sys_id=actual_sys_id,
                fl_id=fl_id,
                shelfmark=shelfmark or '',
                title=title or '',
                thumb_url=thumb_url,
                image_url=image_url,
                internal_index=result.get('internal_index', 0),
                attribution=attribution,
                is_oxford=is_oxford,
                library_code=library_code,
                library_name=library_name,
                volume_ie=active_ie,
                volume_suffix=volume_suffix,
                volume_count=len(volumes) if volumes else 1,
                volumes=volumes,
            )
        except Exception as e:
            logger.error("Browse page error: %s", e)
            return None

    def get_metadata_only_browse_page(self, sys_id: str) -> Optional[BrowsePage]:
        """Phase A (hot path): csv_bank only for metadata-only records. No SQLite/crossref.

        Returns None if sys_id is not in csv_bank.
        fl_id, images, and enrichment data are populated by Phase B.
        """
        if not self.is_ready or not sys_id:
            return None
        try:
            shelfmark, title = state.meta_mgr.get_meta_for_id(sys_id)
            if shelfmark == 'Unknown':
                return None  # Not in csv_bank

            library_code = state.meta_mgr.get_library_for_id(sys_id)
            library_name = get_library_display(library_code, short=False, lang=get_language()) if library_code else ''

            # Pure string check -- no I/O
            is_oxford = is_oxford_manuscript(shelfmark, library_code)

            # Default NLI attribution (Phase B will refine if needed)
            attribution = _NLI_HE if get_language() == 'he' else _NLI_EN

            return BrowsePage(
                uid='',
                p_num=0,
                text='',
                full_header='',
                total_pages=0,
                current_idx=0,
                sys_id=sys_id,
                shelfmark=shelfmark or '',
                title=title or '',
                internal_index=0,
                attribution=attribution,
                is_oxford=is_oxford,
                library_code=library_code,
                library_name=library_name,
            )
        except Exception as e:
            logger.error("Metadata-only browse page error: %s", e)
            return None

    def get_browse_page_by_fl(self, fl_id: str, sys_id: Optional[str] = None) -> Optional[BrowsePage]:
        """Phase A (hot path): Tantivy + csv_bank only. No SQLite/crossref calls."""
        if not self.is_ready: return None
        try:
            result = state.searcher.get_browse_page_by_fl(fl_id, sys_id=sys_id)
            if not result: return None

            actual_sys_id = result.get('sys_id', '')
            shelfmark, title = state.meta_mgr.get_meta_for_id(actual_sys_id)
            library_code = state.meta_mgr.get_library_for_id(actual_sys_id)
            library_name = get_library_display(library_code, short=False, lang=get_language()) if library_code else ''

            fl_id_parsed = None
            try:
                parsed = state.meta_mgr.parse_full_id_components(result.get('full_header', ''))
                fl_id_parsed = parsed.get('fl_id')
            except Exception:
                pass  # IE volume enrichment failed; use default volume

            thumb_url = get_thumbnail_url(fl_id_parsed) if fl_id_parsed else None
            image_url = get_full_image_url(fl_id_parsed) if fl_id_parsed else None

            # Pure string check -- no I/O
            is_oxford = is_oxford_manuscript(shelfmark, library_code)

            # Default NLI attribution (Phase B will refine if needed)
            attribution = _NLI_HE if get_language() == 'he' else _NLI_EN

            # Volume info for multi-IE manuscripts
            from genizah_core import get_volumes_for_sys_id, _extract_ie_from_header
            volumes = get_volumes_for_sys_id(actual_sys_id)
            active_ie = result.get('volume_ie') or _extract_ie_from_header(result.get('full_header', ''))
            volume_suffix = 1
            if volumes and active_ie:
                for v in volumes:
                    if v['ie_id'] == active_ie:
                        volume_suffix = v['suffix']
                        break

            return BrowsePage(
                uid=result.get('uid', ''),
                p_num=result.get('p_num', 0),
                text=result.get('text', ''),
                full_header=result.get('full_header', ''),
                total_pages=result.get('total_pages', 0),
                current_idx=result.get('current_idx', 0),
                sys_id=actual_sys_id,
                fl_id=fl_id_parsed,
                shelfmark=shelfmark or '',
                title=title or '',
                thumb_url=thumb_url,
                image_url=image_url,
                internal_index=result.get('internal_index', 0),
                attribution=attribution,
                is_oxford=is_oxford,
                library_code=library_code,
                library_name=library_name,
                volume_ie=active_ie,
                volume_suffix=volume_suffix,
                volume_count=len(volumes) if volumes else 1,
                volumes=volumes,
            )
        except Exception as e:
            logger.error("Browse page by FL error: %s", e)
            return None

    def get_full_manuscript(self, sys_id: str) -> List[DocumentPage]:
        """Get all pages for a manuscript."""
        if not self.is_ready: return []
        try:
            pages = state.searcher.get_full_manuscript(sys_id)
            result = []
            for p in pages:
                fl_id = None
                try:
                    parsed = state.meta_mgr.parse_full_id_components(p.get('full_header', ''))
                    fl_id = parsed.get('fl_id')
                except Exception:
                    pass  # Search enrichment failed; continue with available results

                result.append(DocumentPage(
                    uid=p.get('uid', ''),
                    p_num=p.get('p_num', 0),
                    text=p.get('text', ''),
                    full_header=p.get('full_header', ''),
                    fl_id=fl_id,
                    sys_id=sys_id
                ))
            return result
        except Exception as e:
            logger.error("Get full manuscript error: %s", e)
            return []

    def get_adjacent_shelfmark(self, sys_id: str, direction: int) -> Optional[str]:
        """Get next/prev shelfmark based on file order in Transcriptions.txt.

        Args:
            sys_id: Current system ID
            direction: 1 for next, -1 for previous

        Returns:
            Adjacent system ID or None if at boundary
        """
        if not self.is_ready: return None
        try:
            return state.searcher.get_adjacent_sys_id_by_file_order(sys_id, direction)
        except Exception as e:
            logger.error("Get adjacent shelfmark error: %s", e)
            return None

_service_instance = GenizahService()

def get_service():
    return _service_instance

def init_service():
    return True
