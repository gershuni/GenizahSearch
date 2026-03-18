"""
GenizahService - Thread-safe wrapper for genizah_core.
Updated to proxy web.state.state for backward compatibility.
"""

import logging
import sys
import os
import re
import threading
import time
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# Add parent directory to path for importing genizah_core
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from genizah_core import (
    Config,
    MetadataManager,
    VariantManager,
    SearchEngine,
    LabEngine,
    LabSettings,
    LIBRARY_CODES,
    get_library_display,
)
from web.state import state

# Library-specific attribution text for image credit lines.
# None = attribution comes from IIIF manifest (don't override).
# Missing key = NLI default (manuscript digitized by NLI, no other source).
ATTRIBUTION_BY_LIBRARY = {
    'CUL': None,        # Cambridge IIIF manifest provides attribution
    'JTS': None,        # JTS/Princeton Figgy manifest provides attribution
    'Manchester': 'The University of Manchester Library \u00b7 CC BY-NC-SA 4.0',
    'Oxford': 'Bodleian Libraries, University of Oxford \u00b7 CC BY-NC 4.0',
    'BL': 'British Library \u00b7 image: \u05d4\u05e1\u05e4\u05e8\u05d9\u05d9\u05d4 \u05d4\u05dc\u05d0\u05d5\u05de\u05d9\u05ea',
    'RNL': 'National Library of Russia \u00b7 image: \u05d4\u05e1\u05e4\u05e8\u05d9\u05d9\u05d4 \u05d4\u05dc\u05d0\u05d5\u05de\u05d9\u05ea',
    'AIU': 'Alliance Isra\u00e9lite Universelle \u00b7 image: \u05d4\u05e1\u05e4\u05e8\u05d9\u05d9\u05d4 \u05d4\u05dc\u05d0\u05d5\u05de\u05d9\u05ea',
    'Mosseri': 'Mosseri Collection \u00b7 image: \u05d4\u05e1\u05e4\u05e8\u05d9\u05d9\u05d4 \u05d4\u05dc\u05d0\u05d5\u05de\u05d9\u05ea',
    'Gaster': 'Gaster Collection \u00b7 image: \u05d4\u05e1\u05e4\u05e8\u05d9\u05d9\u05d4 \u05d4\u05dc\u05d0\u05d5\u05de\u05d9\u05ea',
    'Halper': 'Halper Collection \u00b7 image: \u05d4\u05e1\u05e4\u05e8\u05d9\u05d9\u05d4 \u05d4\u05dc\u05d0\u05d5\u05de\u05d9\u05ea',
    'Westminster': 'Westminster College \u00b7 image: \u05d4\u05e1\u05e4\u05e8\u05d9\u05d9\u05d4 \u05d4\u05dc\u05d0\u05d5\u05de\u05d9\u05ea',
    'Freer': 'Freer Gallery of Art \u00b7 image: \u05d4\u05e1\u05e4\u05e8\u05d9\u05d9\u05d4 \u05d4\u05dc\u05d0\u05d5\u05de\u05d9\u05ea',
    'HUC': 'Hebrew Union College \u00b7 image: \u05d4\u05e1\u05e4\u05e8\u05d9\u05d9\u05d4 \u05d4\u05dc\u05d0\u05d5\u05de\u05d9\u05ea',
}
from web.translations import get_language

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
    physical_metadata: Optional[Dict] = None  # {material, num_folio, num_bifolio, size} from NLI crossref
    library_viewer_url: Optional[Dict] = None  # {url, label, library_abbrev} for holding library link

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


def get_oxford_direct_image_url(shelfmark: str = '', page_idx: int = 0) -> str:
    """Build direct Bodleian image URL from Oxford shelfmark (MS heb. e.93/58)."""
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
    side = 'b' if int(page_idx or 0) % 2 == 1 else 'a'
    return f"https://hebrew.bodleian.ox.ac.uk/fragments/full/MS_HEB_{letter}_{volume}_{folio}{side}.jpg"

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

    def get_browse_page(self, sys_id: str, p_num: Optional[int] = None, direction: int = 0, absolute_index: Optional[int] = None, allow_cross: bool = False) -> Optional[BrowsePage]:
        if not self.is_ready: return None
        try:
            result = state.searcher.get_browse_page(
                sys_id, p_num=p_num, next_prev=direction, absolute_index=absolute_index, allow_cross=allow_cross
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
                pass

            thumb_url = get_thumbnail_url(fl_id) if fl_id else None
            image_url = get_full_image_url(fl_id) if fl_id else None

            # Determine attribution and source classification
            attribution = ''
            is_oxford = is_oxford_manuscript(shelfmark, library_code)

            # 1. Try IIIF manifest attribution from cache
            if actual_sys_id and hasattr(state.meta_mgr, 'nli_cache'):
                cached_meta = state.meta_mgr.nli_cache.get(actual_sys_id, {})
                attribution = cached_meta.get('attribution', '')

            # 2. Library-specific override (hardcoded text, or keep IIIF manifest)
            if library_code in ATTRIBUTION_BY_LIBRARY:
                lib_attr = ATTRIBUTION_BY_LIBRARY[library_code]
                if lib_attr is not None:  # None = keep IIIF manifest attribution
                    attribution = lib_attr
            elif is_oxford:
                attribution = 'Bodleian Libraries, University of Oxford \u00b7 CC BY-NC 4.0'

            # 3. Default: NLI
            if not attribution:
                attribution = '\u05d4\u05e1\u05e4\u05e8\u05d9\u05d9\u05d4 \u05d4\u05dc\u05d0\u05d5\u05de\u05d9\u05ea / National Library of Israel'

            # Logic for External Links (Oxford/Cambridge)
            external_url = None
            is_cambridge = False
            oxford_part_id = None
            oxford_part_display = ''
            oxford_part_metadata = {}

            # 1. Oxford
            if is_oxford and actual_sys_id:
                # Use CodicologicalManager to find the Part for this folio
                # accessing via state.meta_mgr.codico_mgr ideally, or if exposed on meta_mgr
                if hasattr(state.meta_mgr, 'get_part_for_folio'):
                    part_id = state.meta_mgr.get_part_for_folio(actual_sys_id)
                    if part_id:
                        oxford_part_id = part_id
                        # Get display name
                        if hasattr(state.meta_mgr.codico_mgr, 'get_part_display_name'):
                            oxford_part_display = state.meta_mgr.codico_mgr.get_part_display_name(part_id)
                        
                        # Get metadata
                        part_meta = state.meta_mgr.get_part_metadata(part_id)
                        if part_meta:
                            # Filter only what we need for display
                            for key in ['title', 'contents', 'provenance']:
                                if part_meta.get(key):
                                    oxford_part_metadata[key] = part_meta[key]
                            
                            # Use direct_link as external_url
                            if part_meta.get('direct_link'):
                                external_url = part_meta.get('direct_link')

            # 2. Cambridge
            # Check MARC data for CUDL link if not already handled
            if not external_url:
                # We need to fetch MARC to check for 856 link if not cached
                # The meta_mgr.nli_cache might have it if it was fetched previously
                # But for now, let's rely on what we might have or fetch it if crucial
                # For basic browsing, we might skip heavy fetching unless necessary.
                # However, get_browse_page usually relies on search index data.
                # Let's see if we can get it from 'result' or re-fetch.
                # state.meta_mgr.get_meta_for_id returns (shelfmark, title).
                # We need more. Let's try to get it from cache or fetch.
                marc_data = {}
                if hasattr(state.meta_mgr, 'nli_cache') and actual_sys_id in state.meta_mgr.nli_cache:
                     marc_data = state.meta_mgr.nli_cache[actual_sys_id].get('marc', {})

                # If we didn't mock/cache it fully, we might miss it.
                # In GenizahSearch desktop, it lazily fetches.
                # Here, let's check if we can get `external_iiif_link` from cached meta.

                # If we don't have it, we might simply check if shelfmark implies Cambridge (T-S ...)
                # AND we want to call the API.
                # But cleaner is:
                ext_link = marc_data.get('external_iiif_link')
                if ext_link and "cudl.lib.cam.ac.uk" in ext_link:
                    is_cambridge = True
                    # Transform IIIF manifest URL to viewer URL
                    external_url = ext_link.replace("/iiif/", "/view/")

            # 3. NLI crossref: folio images, source indicators, physical metadata
            folio_label = ''
            image_source_info = {}
            folio_images = []
            physical_metadata = None
            library_viewer_url = None
            try:
                from shared.nli_crossref_service import get_nli_crossref_service
                crossref_svc = get_nli_crossref_service(thread_safe=True)
                if crossref_svc.is_available() and actual_sys_id:
                    # Get normalized shelfmark for Cambridge lookup
                    from genizah_core import normalize_shelfmark
                    norm_shelf = normalize_shelfmark(shelfmark) if shelfmark else None

                    # Image source indicators
                    image_source_info = crossref_svc.get_image_sources(
                        actual_sys_id, normalized_shelfmark=norm_shelf
                    )

                    # Update is_cambridge from sidecar if not already set
                    if not is_cambridge and image_source_info.get('cambridge'):
                        is_cambridge = True

                    # Folio images with labels
                    folio_images = crossref_svc.get_folio_images(actual_sys_id)

                    # Extract current folio label from page number.
                    # Only use crossref labels when the image count matches the
                    # page count from the search index -- otherwise the labels
                    # would map to the wrong pages (e.g. crossref starts at leaf 4
                    # while search-index pages start at 1).
                    total_pages = result.get('total_pages', 0)
                    current_p = result.get('p_num', 0)
                    if (folio_images
                            and len(folio_images) == total_pages
                            and 0 < current_p <= len(folio_images)):
                        folio_label = folio_images[current_p - 1].get('folio_label', '')

                    # Physical metadata (material, folio counts)
                    physical_metadata = crossref_svc.get_physical_metadata(actual_sys_id)

                    # Library digital collection URL
                    library_viewer_url = crossref_svc.get_library_viewer_url(actual_sys_id)
            except Exception as crossref_err:
                logger.error("NLI crossref enrichment error: %s", crossref_err)

            # External images from nli_cache (populated by enrich_metadata)
            # images_ext may come from Cambridge, Manchester LUNA, or JTS Figgy
            cambridge_images = []
            external_provider = ''
            if actual_sys_id and hasattr(state.meta_mgr, 'nli_cache'):
                cached = state.meta_mgr.nli_cache.get(actual_sys_id, {})
                cambridge_images = cached.get('images_ext', [])
                external_provider = cached.get('external_provider', '')

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
                is_cambridge=is_cambridge,
                external_url=external_url,
                oxford_part_id=oxford_part_id,
                oxford_part_display=oxford_part_display,
                oxford_part_metadata=oxford_part_metadata,
                library_code=library_code,
                library_name=library_name,
                folio_label=folio_label,
                image_source_info=image_source_info,
                folio_images=folio_images,
                cambridge_images=cambridge_images,
                external_provider=external_provider,
                physical_metadata=physical_metadata,
                library_viewer_url=library_viewer_url,
            )
        except Exception as e:
            logger.error("Browse page error: %s", e)
            return None

    def get_browse_page_by_fl(self, fl_id: str, sys_id: Optional[str] = None) -> Optional[BrowsePage]:
        """Get a browse page by FL ID."""
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
                pass

            thumb_url = get_thumbnail_url(fl_id_parsed) if fl_id_parsed else None
            image_url = get_full_image_url(fl_id_parsed) if fl_id_parsed else None

            # Determine attribution and source classification
            attribution = ''
            is_oxford = is_oxford_manuscript(shelfmark, library_code)

            # 1. Try IIIF manifest attribution from cache
            if actual_sys_id and hasattr(state.meta_mgr, 'nli_cache'):
                cached_meta = state.meta_mgr.nli_cache.get(actual_sys_id, {})
                attribution = cached_meta.get('attribution', '')

            # 2. Library-specific override (hardcoded text, or keep IIIF manifest)
            if library_code in ATTRIBUTION_BY_LIBRARY:
                lib_attr = ATTRIBUTION_BY_LIBRARY[library_code]
                if lib_attr is not None:  # None = keep IIIF manifest attribution
                    attribution = lib_attr
            elif is_oxford:
                attribution = 'Bodleian Libraries, University of Oxford \u00b7 CC BY-NC 4.0'

            # 3. Default: NLI
            if not attribution:
                attribution = '\u05d4\u05e1\u05e4\u05e8\u05d9\u05d9\u05d4 \u05d4\u05dc\u05d0\u05d5\u05de\u05d9\u05ea / National Library of Israel'

            # Logic for External Links (Oxford/Cambridge) - Duplicate logic for by_fl
            external_url = None
            is_cambridge = False
            oxford_part_id = None
            oxford_part_display = ''
            oxford_part_metadata = {}

            # 1. Oxford
            if is_oxford and actual_sys_id:
                if hasattr(state.meta_mgr, 'get_part_for_folio'):
                    part_id = state.meta_mgr.get_part_for_folio(actual_sys_id)
                    if part_id:
                        oxford_part_id = part_id
                        if hasattr(state.meta_mgr.codico_mgr, 'get_part_display_name'):
                            oxford_part_display = state.meta_mgr.codico_mgr.get_part_display_name(part_id)
                        
                        part_meta = state.meta_mgr.get_part_metadata(part_id)
                        if part_meta:
                            for key in ['title', 'contents', 'provenance']:
                                if part_meta.get(key):
                                    oxford_part_metadata[key] = part_meta[key]
                            if part_meta.get('direct_link'):
                                external_url = part_meta.get('direct_link')

            # 2. Cambridge
            if not external_url:
                marc_data = {}
                if hasattr(state.meta_mgr, 'nli_cache') and actual_sys_id in state.meta_mgr.nli_cache:
                     marc_data = state.meta_mgr.nli_cache[actual_sys_id].get('marc', {})

                ext_link = marc_data.get('external_iiif_link')
                if ext_link and "cudl.lib.cam.ac.uk" in ext_link:
                    is_cambridge = True
                    # Transform IIIF manifest URL to viewer URL
                    external_url = ext_link.replace("/iiif/", "/view/")

            # 3. NLI crossref: folio images, source indicators, physical metadata
            folio_label = ''
            image_source_info = {}
            folio_images = []
            physical_metadata = None
            library_viewer_url = None
            try:
                from shared.nli_crossref_service import get_nli_crossref_service
                crossref_svc = get_nli_crossref_service(thread_safe=True)
                if crossref_svc.is_available() and actual_sys_id:
                    from genizah_core import normalize_shelfmark
                    norm_shelf = normalize_shelfmark(shelfmark) if shelfmark else None

                    image_source_info = crossref_svc.get_image_sources(
                        actual_sys_id, normalized_shelfmark=norm_shelf
                    )

                    if not is_cambridge and image_source_info.get('cambridge'):
                        is_cambridge = True

                    folio_images = crossref_svc.get_folio_images(actual_sys_id)

                    # Extract current folio label from page number.
                    # Only use crossref labels when the image count matches the
                    # page count from the search index -- otherwise the labels
                    # would map to the wrong pages (e.g. crossref starts at leaf 4
                    # while search-index pages start at 1).
                    total_pages = result.get('total_pages', 0)
                    current_p = result.get('p_num', 0)
                    if (folio_images
                            and len(folio_images) == total_pages
                            and 0 < current_p <= len(folio_images)):
                        folio_label = folio_images[current_p - 1].get('folio_label', '')

                    # Physical metadata (material, folio counts)
                    physical_metadata = crossref_svc.get_physical_metadata(actual_sys_id)

                    # Library digital collection URL
                    library_viewer_url = crossref_svc.get_library_viewer_url(actual_sys_id)
            except Exception as crossref_err:
                logger.error("NLI crossref enrichment error (by_fl): %s", crossref_err)

            # External images from nli_cache (populated by enrich_metadata)
            cambridge_images = []
            external_provider = ''
            if actual_sys_id and hasattr(state.meta_mgr, 'nli_cache'):
                cached = state.meta_mgr.nli_cache.get(actual_sys_id, {})
                cambridge_images = cached.get('images_ext', [])
                external_provider = cached.get('external_provider', '')

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
                is_cambridge=is_cambridge,
                external_url=external_url,
                oxford_part_id=oxford_part_id,
                oxford_part_display=oxford_part_display,
                oxford_part_metadata=oxford_part_metadata,
                library_code=library_code,
                library_name=library_name,
                folio_label=folio_label,
                image_source_info=image_source_info,
                folio_images=folio_images,
                cambridge_images=cambridge_images,
                external_provider=external_provider,
                physical_metadata=physical_metadata,
                library_viewer_url=library_viewer_url,
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
                    pass

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
