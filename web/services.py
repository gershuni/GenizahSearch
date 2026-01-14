"""
GenizahService - Thread-safe wrapper for genizah_core.

This service layer isolates the web application from the core,
providing a clean API and handling thread safety for concurrent requests.
"""

import sys
import os
import re
import threading
import time
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field
from functools import lru_cache

# Add parent directory to path for importing genizah_core
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from genizah_core import (
    Config,
    MetadataManager,
    VariantManager,
    SearchEngine,
    LabEngine,
    LabSettings,
)


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class SearchResult:
    """Structured search result for the web UI."""
    uid: str
    sys_id: str
    display: Dict[str, str]  # shelfmark, title, img, source, id
    snippet: str
    raw_header: str
    source: str
    full_text: str = ''
    highlight_pattern: Optional[str] = None
    cross_page: bool = False
    page_highlights: List[Dict] = field(default_factory=list)
    scope: str = 'page'


@dataclass
class CompositionResult:
    """Result from composition/parallel search."""
    score: float
    sys_id: str
    raw_header: str
    ms_snippet: str  # Manuscript matching text
    src_snippet: str  # Source text context
    pattern: str
    display: Dict[str, str] = field(default_factory=dict)
    uid: str = ''


@dataclass
class DocumentPage:
    """A single page of a manuscript."""
    uid: str
    p_num: int
    text: str
    full_header: str
    fl_id: Optional[str] = None
    sys_id: Optional[str] = None
    image_url: Optional[str] = None
    thumb_url: Optional[str] = None


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


@dataclass
class ManuscriptInfo:
    """Full manuscript information for browse/display."""
    sys_id: str
    shelfmark: str
    title: str
    page_count: int = 0
    thumb_url: Optional[str] = None
    has_external_images: bool = False
    oxford_part_id: Optional[str] = None
    attribution: str = ''


@dataclass
class ImageInfo:
    """Image information for IIIF/viewer."""
    label: str
    url: str
    thumb_url: str
    source: str  # 'NLI', 'Cambridge', 'Oxford', etc.
    fl_id: Optional[str] = None
    folio_num: Optional[int] = None


# ============================================================================
# Image URL Helpers
# ============================================================================

NLI_IIIF_BASE = "https://iiif.nli.org.il/IIIFv21"


def get_thumbnail_url(fl_id: str, size: int = 400) -> str:
    """Build IIIF thumbnail URL from FL ID."""
    if not fl_id:
        return ''
    digits = re.sub(r"\D", "", str(fl_id))
    if not digits:
        return ''
    return f"{NLI_IIIF_BASE}/FL{digits}/full/{size},/0/default.jpg"


def get_full_image_url(fl_id: str) -> str:
    """Build IIIF full image URL from FL ID."""
    if not fl_id:
        return ''
    digits = re.sub(r"\D", "", str(fl_id))
    if not digits:
        return ''
    return f"{NLI_IIIF_BASE}/FL{digits}/full/max/0/default.jpg"


def get_rosetta_fallback_url(fl_id: str) -> str:
    """Build Rosetta fallback thumbnail URL."""
    if not fl_id:
        return ''
    digits = re.sub(r"\D", "", str(fl_id))
    if not digits:
        return ''
    return f"https://rosetta.nli.org.il/delivery/DeliveryManagerServlet?dps_func=thumbnail&dps_pid=FL{digits}"


def build_iiif_image_url(base_url: str, size: str = 'full') -> str:
    """Build IIIF image URL with specified size from base service URL."""
    if not base_url:
        return ''
    # Handle size parameter: 'full', 'max', '400,', ',400', '400,400'
    if size == 'thumb':
        size_param = '400,'
    elif size == 'full':
        size_param = 'max'
    else:
        size_param = size
    return f"{base_url}/full/{size_param}/0/default.jpg"


# ============================================================================
# Main Service Class
# ============================================================================

class GenizahService:
    """
    Thread-safe service layer for Genizah search operations.

    Provides:
    - Singleton pattern for shared resource management
    - Thread-safe search operations via lock
    - Clean API for web handlers
    - Support for search, composition search, and browse
    - Variant generation and management
    - IIIF/image handling
    - Metadata caching
    """

    _instance: Optional['GenizahService'] = None
    _lock = threading.Lock()
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if GenizahService._initialized:
            return

        with GenizahService._lock:
            if GenizahService._initialized:
                return

            self._search_lock = threading.Lock()
            self._meta_mgr: Optional[MetadataManager] = None
            self._var_mgr: Optional[VariantManager] = None
            self._search_engine: Optional[SearchEngine] = None
            self._lab_engine: Optional[LabEngine] = None
            self._lab_settings: Optional[LabSettings] = None
            self._ready = False
            self._init_error: Optional[str] = None

            # Metadata cache for web requests (TTL-based)
            self._metadata_cache: Dict[str, Tuple[Dict, float]] = {}
            self._cache_ttl = 300  # 5 minutes
            self._uid_sys_id_cache: Dict[str, Tuple[str, float]] = {}
            self._uid_cache_ttl = 3600  # 1 hour

            GenizahService._initialized = True

    def initialize(self) -> bool:
        """Initialize core components. Call this once at startup."""
        try:
            self._lab_settings = LabSettings()
            self._meta_mgr = MetadataManager()
            self._var_mgr = VariantManager(settings=self._lab_settings)
            self._search_engine = SearchEngine(self._meta_mgr, self._var_mgr)

            # Initialize Lab Engine for composition search
            try:
                self._lab_engine = LabEngine(self._meta_mgr, self._var_mgr)
            except Exception as e:
                print(f"Lab engine init warning (composition search may be unavailable): {e}")

            # Start background loading of heavy resources
            self._meta_mgr.start_background_loading()

            self._ready = True
            return True

        except Exception as e:
            self._init_error = str(e)
            return False

    @property
    def is_ready(self) -> bool:
        return self._ready and self._search_engine is not None

    @property
    def has_lab_engine(self) -> bool:
        return self._lab_engine is not None

    @property
    def init_error(self) -> Optional[str]:
        return self._init_error

    @property
    def index_exists(self) -> bool:
        db_path = os.path.join(Config.INDEX_DIR, "tantivy_db")
        return os.path.exists(db_path)

    # ========================================================================
    # Variant Management
    # ========================================================================

    def get_variants(
        self,
        term: str,
        mode: str = "variants",
        limit: Optional[int] = None
    ) -> List[str]:
        """
        Generate spelling variants for a Hebrew search term.

        Args:
            term: The Hebrew term to generate variants for
            mode: One of 'exact', 'variants', 'variants_extended',
                  'variants_maximum', 'fuzzy'
            limit: Maximum number of variants to return

        Returns:
            List of variant strings, with original term first
        """
        if not self.is_ready:
            return [term]

        # Map mode aliases
        mode_map = {
            'exact': None,  # No variants for exact mode
            'variants': 'variants',
            'extended': 'variants_extended',
            'variants_extended': 'variants_extended',
            'maximum': 'variants_maximum',
            'variants_maximum': 'variants_maximum',
            'fuzzy': 'variants_maximum',  # Fuzzy uses maximum variants
        }

        variant_mode = mode_map.get(mode)
        if variant_mode is None:
            return [term]

        try:
            return self._var_mgr.get_variants(term, variant_mode, limit=limit)
        except Exception as e:
            print(f"Variant generation error: {e}")
            return [term]

    def add_custom_variant(self, pair: str) -> bool:
        """
        Add a custom variant pair (e.g., 'ק=א' or 'כו=מ').

        Args:
            pair: The variant pair in 'a=b' format

        Returns:
            True if added successfully
        """
        if not self.is_ready or not self._lab_settings:
            return False

        try:
            if '=' not in pair:
                return False

            self._lab_settings.custom_variants[pair] = True
            self._lab_settings.save()
            self._var_mgr.set_settings(self._lab_settings)
            return True
        except Exception as e:
            print(f"Add custom variant error: {e}")
            return False

    def remove_custom_variant(self, pair: str) -> bool:
        """Remove a custom variant pair."""
        if not self.is_ready or not self._lab_settings:
            return False

        try:
            if pair in self._lab_settings.custom_variants:
                del self._lab_settings.custom_variants[pair]
                self._lab_settings.save()
                self._var_mgr.set_settings(self._lab_settings)
                return True
            return False
        except Exception as e:
            print(f"Remove custom variant error: {e}")
            return False

    def get_custom_variants(self) -> Dict[str, bool]:
        """Get all custom variant pairs."""
        if not self._lab_settings:
            return {}
        return dict(self._lab_settings.custom_variants)

    # ========================================================================
    # Search Operations
    # ========================================================================

    def search(
        self,
        query: str,
        mode: str = "variants",
        gap: int = 0,
        limit: int = 100
    ) -> List[SearchResult]:
        """
        Execute a search query.

        Args:
            query: The search query string
            mode: Search mode - 'exact', 'variants', 'extended', 'maximum',
                  'fuzzy', 'regex', 'title', 'shelfmark'
            gap: Maximum word gap allowed between search terms
            limit: Maximum results to return

        Returns:
            List of SearchResult objects
        """
        if not self.is_ready:
            return []

        mode_map = {
            'exact': 'exact',
            'variants': 'variants',
            'extended': 'variants_extended',
            'variants_extended': 'variants_extended',
            'maximum': 'variants_maximum',
            'variants_maximum': 'variants_maximum',
            'fuzzy': 'fuzzy',
            'regex': 'Regex',
            'Regex': 'Regex',
            'title': 'Title',
            'Title': 'Title',
            'shelfmark': 'Shelfmark',
            'Shelfmark': 'Shelfmark',
        }
        search_mode = mode_map.get(mode, 'variants')

        with self._search_lock:
            try:
                raw_results = self._search_engine.execute_search(
                    query, search_mode, gap
                )

                results = []
                for r in raw_results[:limit]:
                    display = r.get('display', {})
                    if isinstance(display, str):
                        display = {'shelfmark': display, 'title': '', 'img': '', 'source': '', 'id': ''}

                    sys_id = display.get('id', '') or self.extract_sys_id(
                        r.get('raw_header', '') or r.get('uid', '')
                    )
                    self.cache_uid_sys_id(r.get('uid', ''), sys_id)

                    # Extract page highlights for cross-page results
                    page_highlights = r.get('page_highlights', [])

                    results.append(SearchResult(
                        uid=r.get('uid', ''),
                        sys_id=sys_id,
                        display=display,
                        snippet=r.get('snippet', ''),
                        raw_header=r.get('raw_header', ''),
                        source=r.get('source', 'V0.8'),
                        full_text=r.get('full_text', ''),
                        highlight_pattern=r.get('highlight_pattern'),
                        cross_page=r.get('cross_page', False),
                        page_highlights=page_highlights,
                        scope=r.get('scope', 'page')
                    ))

                return results

            except Exception as e:
                print(f"Search error: {e}")
                return []

    # ========================================================================
    # Composition (Parallel) Search
    # ========================================================================

    def composition_search(
        self,
        full_text: str,
        mode: str = "variants",
        chunk_size: int = 4,
        max_freq: int = 100,
        filter_text: Optional[str] = None,
        limit: int = 100
    ) -> List[CompositionResult]:
        """
        Search for parallel texts in the Genizah corpus.

        Args:
            full_text: The source text to find parallels for
            mode: Search mode (variants, extended, etc.)
            chunk_size: Words per chunk (default 4)
            max_freq: Maximum frequency threshold (default 100)
            filter_text: Text to exclude matches from
            limit: Maximum results to return

        Returns:
            List of CompositionResult objects
        """
        if not self.is_ready:
            return []

        with self._search_lock:
            try:
                raw_result = self._search_engine.search_composition_logic(
                    full_text,
                    chunk_size=chunk_size,
                    max_freq=max_freq,
                    mode=mode,
                    filter_text=filter_text
                )

                # search_composition_logic returns dict with 'main', 'filtered'
                if raw_result is None:
                    return []

                main_results = raw_result.get('main', []) if isinstance(raw_result, dict) else raw_result

                results = []
                for r in main_results[:limit]:
                    sys_id = self.extract_sys_id(r.get('raw_header', '') or r.get('uid', ''))
                    display = r.get('display', {})
                    if isinstance(display, str):
                        display = {'shelfmark': display}

                    # Get metadata if not in display
                    if not display.get('shelfmark') and sys_id:
                        meta = self.get_metadata(sys_id)
                        display = {
                            'shelfmark': meta.get('shelfmark', ''),
                            'title': meta.get('title', ''),
                            'id': sys_id
                        }

                    results.append(CompositionResult(
                        score=r.get('score', r.get('sort_score', 0)),
                        sys_id=sys_id,
                        raw_header=r.get('raw_header', ''),
                        ms_snippet=r.get('text', r.get('snippet', '')),
                        src_snippet=r.get('source_ctx', ''),
                        pattern=r.get('highlight_pattern', ''),
                        display=display,
                        uid=r.get('uid', '')
                    ))
                    self.cache_uid_sys_id(r.get('uid', ''), sys_id)

                return results

        except Exception as e:
            print(f"Composition search error: {e}")
            import traceback
            traceback.print_exc()
            return []

    def search_composition_logic(
        self,
        full_text: str,
        chunk_size: int = 4,
        max_freq: int = 100,
        mode: str = "variants",
        filter_text: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Raw composition search returning full structure with main/filtered results.

        Returns:
            Dict with 'main' and 'filtered' lists of results
        """
        if not self.is_ready:
            return {'main': [], 'filtered': []}

        with self._search_lock:
            try:
                result = self._search_engine.search_composition_logic(
                    full_text,
                    chunk_size=chunk_size,
                    max_freq=max_freq,
                    mode=mode,
                    filter_text=filter_text
                )
                return result if result else {'main': [], 'filtered': []}
            except Exception as e:
                print(f"Composition search logic error: {e}")
                return {'main': [], 'filtered': []}

    def group_composition_results(
        self,
        items: List[Dict],
        threshold: int = 5
    ) -> Tuple[List[Dict], Dict[str, List], Dict[str, List]]:
        """
        Group composition results by title/work.

        Args:
            items: List of composition result items
            threshold: Minimum count to form a group

        Returns:
            Tuple of (main_list, appendix_dict, summary_dict)
        """
        if not self.is_ready:
            return [], {}, {}

        with self._search_lock:
            try:
                return self._search_engine.group_composition_results(items, threshold)
            except Exception as e:
                print(f"Group composition results error: {e}")
                return items, {}, {}

    # ========================================================================
    # Document & Browse Operations
    # ========================================================================

    def get_document(self, sys_id: str) -> List[DocumentPage]:
        """Get all pages of a manuscript by system ID."""
        if not self.is_ready:
            return []

        with self._search_lock:
            try:
                pages_data = self._search_engine.get_full_manuscript(sys_id)

                pages = []
                for p in pages_data:
                    fl_id = p.get('fl_id')
                    pages.append(DocumentPage(
                        uid=p.get('uid', ''),
                        p_num=p.get('p_num', 0),
                        text=p.get('text', ''),
                        full_header=p.get('full_header', ''),
                        fl_id=fl_id,
                        sys_id=sys_id,
                        image_url=get_full_image_url(fl_id) if fl_id else None,
                        thumb_url=get_thumbnail_url(fl_id) if fl_id else None
                    ))

                return pages

            except Exception as e:
                print(f"Get document error: {e}")
                return []

    def get_browse_page(
        self,
        sys_id: str,
        p_num: Optional[int] = None,
        direction: int = 0,
        absolute_index: Optional[int] = None,
        allow_cross: bool = False
    ) -> Optional[BrowsePage]:
        """
        Get a page for browsing with navigation info.

        Args:
            sys_id: Manuscript system ID
            p_num: Target page number
            direction: Navigation direction (-1=prev, 0=current, 1=next)
            absolute_index: Direct array index (overrides p_num)
            allow_cross: Allow navigation to adjacent manuscripts

        Returns:
            BrowsePage object with text and image URLs
        """
        if not self.is_ready:
            return None

        with self._search_lock:
            try:
                result = self._search_engine.get_browse_page(
                    sys_id,
                    p_num=p_num,
                    next_prev=direction,
                    absolute_index=absolute_index,
                    allow_cross=allow_cross
                )

                if not result:
                    return None

                # Get metadata
                shelfmark, title = '', ''
                try:
                    shelfmark, title = self._meta_mgr.get_meta_for_id(result.get('sys_id', sys_id))
                except:
                    pass

                # Get FL ID for image
                fl_id = None
                try:
                    parsed = self._meta_mgr.parse_full_id_components(result.get('full_header', ''))
                    fl_id = parsed.get('fl_id')
                except:
                    pass

                # Build image URLs
                thumb_url = get_thumbnail_url(fl_id) if fl_id else None
                image_url = get_full_image_url(fl_id) if fl_id else None

                return BrowsePage(
                    uid=result.get('uid', ''),
                    p_num=result.get('p_num', 0),
                    text=result.get('text', ''),
                    full_header=result.get('full_header', ''),
                    total_pages=result.get('total_pages', 0),
                    current_idx=result.get('current_idx', 0),
                    sys_id=result.get('sys_id', sys_id),
                    fl_id=fl_id,
                    shelfmark=shelfmark or '',
                    title=title or '',
                    thumb_url=thumb_url,
                    image_url=image_url,
                    internal_index=result.get('internal_index', 0)
                )

            except Exception as e:
                print(f"Browse page error: {e}")
                return None

    def get_browse_page_by_fl(
        self,
        fl_id: str,
        sys_id: Optional[str] = None
    ) -> Optional[BrowsePage]:
        """
        Get a browse page by FL ID.

        Args:
            fl_id: The FL (image) ID
            sys_id: Optional system ID to narrow search

        Returns:
            BrowsePage object if found
        """
        if not self.is_ready:
            return None

        with self._search_lock:
            try:
                result = self._search_engine.get_browse_page_by_fl(fl_id, sys_id)
                if not result:
                    return None

                shelfmark, title = '', ''
                try:
                    shelfmark, title = self._meta_mgr.get_meta_for_id(result.get('sys_id', ''))
                except:
                    pass

                return BrowsePage(
                    uid=result.get('uid', ''),
                    p_num=result.get('p_num', 0),
                    text=result.get('text', ''),
                    full_header=result.get('full_header', ''),
                    total_pages=result.get('total_pages', 0),
                    current_idx=result.get('current_idx', 0),
                    sys_id=result.get('sys_id', ''),
                    fl_id=result.get('fl_id'),
                    shelfmark=shelfmark or '',
                    title=title or '',
                    thumb_url=get_thumbnail_url(result.get('fl_id')),
                    image_url=get_full_image_url(result.get('fl_id')),
                    internal_index=result.get('current_idx', 1) - 1
                )
            except Exception as e:
                print(f"Browse page by FL error: {e}")
                return None

    def get_manuscript_info(self, sys_id: str) -> Optional[ManuscriptInfo]:
        """
        Get full manuscript information.

        Args:
            sys_id: The system ID of the manuscript

        Returns:
            ManuscriptInfo with metadata, page count, and image info
        """
        if not self.is_ready:
            return None

        try:
            # Get basic metadata with Part info
            meta = self._meta_mgr.get_meta_with_part(sys_id)

            # Get page count
            page_count = self.get_page_count(sys_id)

            # Get enriched data for images
            enriched = self._meta_mgr.enrich_metadata(sys_id)

            # Determine if external images exist
            has_external = bool(enriched.get('images_ext', []))

            # Get thumbnail
            thumb_url = enriched.get('thumb_url')
            if not thumb_url and enriched.get('images_nli'):
                first_img = enriched['images_nli'][0]
                thumb_url = get_thumbnail_url(first_img.get('fl_id', ''))

            return ManuscriptInfo(
                sys_id=sys_id,
                shelfmark=meta.get('shelfmark', 'Unknown'),
                title=meta.get('title', ''),
                page_count=page_count,
                thumb_url=thumb_url,
                has_external_images=has_external,
                oxford_part_id=meta.get('oxford_part_id'),
                attribution=enriched.get('attribution', '')
            )
        except Exception as e:
            print(f"Get manuscript info error: {e}")
            return None

    def get_page_count(self, sys_id: str) -> int:
        """
        Get the number of pages in a manuscript.

        Args:
            sys_id: The system ID of the manuscript

        Returns:
            Number of pages, or 0 if unknown
        """
        if not self.is_ready:
            return 0

        try:
            # Get first page to get total count
            page = self._search_engine.get_browse_page(sys_id, p_num=1)
            if page:
                return page.get('total_pages', 0)
            return 0
        except Exception as e:
            print(f"Get page count error: {e}")
            return 0

    def search_by_shelfmark(self, shelfmark_query: str, limit: int = 50) -> List[ManuscriptInfo]:
        """Search manuscripts by shelfmark for browse feature."""
        if not self.is_ready:
            return []

        try:
            # Use the Title/Shelfmark search mode
            results = self.search(shelfmark_query, mode='shelfmark', limit=limit)

            manuscripts = []
            seen_ids = set()

            for r in results:
                sys_id = r.sys_id
                if sys_id and sys_id not in seen_ids:
                    seen_ids.add(sys_id)
                    manuscripts.append(ManuscriptInfo(
                        sys_id=sys_id,
                        shelfmark=r.display.get('shelfmark', ''),
                        title=r.display.get('title', '')
                    ))

            return manuscripts

        except Exception as e:
            print(f"Search by shelfmark error: {e}")
            return []

    def resolve_shelfmark(self, query: str, limit: int = 20) -> Dict[str, Any]:
        """
        Resolve a shelfmark query to system IDs (for autocomplete).

        Args:
            query: Partial or full shelfmark query
            limit: Maximum suggestions to return

        Returns:
            Dict with 'sys_id' (if single match), 'options' list,
            and 'selected_shelfmark'
        """
        if not self.is_ready:
            return {'sys_id': None, 'options': [], 'selected_shelfmark': None}

        try:
            return self._meta_mgr.resolve_system_by_shelfmark(query, limit=limit)
        except Exception as e:
            print(f"Resolve shelfmark error: {e}")
            return {'sys_id': None, 'options': [], 'selected_shelfmark': None}

    # ========================================================================
    # IIIF / Image Support
    # ========================================================================

    def get_iiif_manifest(self, sys_id: str) -> Dict[str, Any]:
        """
        Fetch IIIF manifest data for a manuscript.

        Args:
            sys_id: The system ID of the manuscript

        Returns:
            Dict with 'physical_desc', 'canvas_map', 'attribution'
        """
        if not self.is_ready:
            return {}

        try:
            return self._meta_mgr.fetch_iiif_manifest(sys_id)
        except Exception as e:
            print(f"Get IIIF manifest error: {e}")
            return {}

    def get_image_list(self, sys_id: str, include_external: bool = True) -> List[ImageInfo]:
        """
        Get list of all images for a manuscript.

        Args:
            sys_id: The system ID of the manuscript
            include_external: Whether to include external (Cambridge, Oxford) images

        Returns:
            List of ImageInfo objects with URLs and labels
        """
        if not self.is_ready:
            return []

        try:
            enriched = self._meta_mgr.enrich_metadata(sys_id)
            images = []

            # NLI images
            for img in enriched.get('images_nli', []):
                fl_id = img.get('fl_id', '')
                images.append(ImageInfo(
                    label=img.get('label', ''),
                    url=build_iiif_image_url(img.get('url', ''), 'full'),
                    thumb_url=get_thumbnail_url(fl_id),
                    source='NLI',
                    fl_id=fl_id,
                    folio_num=None
                ))

            # External images (Cambridge, Oxford)
            if include_external:
                for img in enriched.get('images_ext', []):
                    base_url = img.get('url', '')
                    images.append(ImageInfo(
                        label=img.get('label', ''),
                        url=build_iiif_image_url(base_url, 'full') if base_url else '',
                        thumb_url=img.get('thumb_url', '') or build_iiif_image_url(base_url, 'thumb'),
                        source='External',
                        fl_id=None,
                        folio_num=img.get('folio_num')
                    ))

            return images

        except Exception as e:
            print(f"Get image list error: {e}")
            return []

    def get_images(self, sys_id: str) -> List[Dict[str, str]]:
        """
        Get list of images for a manuscript (simple dict format).

        Args:
            sys_id: The system ID of the manuscript

        Returns:
            List of dicts with 'label', 'url', 'thumb_url', 'source'
        """
        if not self.is_ready:
            return []

        try:
            enriched = self.get_enriched_metadata(sys_id)
            images = []

            # NLI images
            for img in enriched.get('images_nli', []):
                images.append({
                    'label': img.get('label', ''),
                    'url': img.get('url', ''),
                    'thumb_url': get_thumbnail_url(img.get('fl_id', '')),
                    'source': 'NLI'
                })

            # External images
            for img in enriched.get('images_ext', []):
                images.append({
                    'label': img.get('label', ''),
                    'url': img.get('url', ''),
                    'thumb_url': img.get('thumb_url', ''),
                    'source': 'External'
                })

            return images

        except Exception as e:
            print(f"Get images error: {e}")
            return []

    # ========================================================================
    # Metadata Operations
    # ========================================================================

    def get_metadata(self, sys_id: str) -> Dict[str, Any]:
        """Get basic metadata for a manuscript."""
        if not self.is_ready:
            return {}

        try:
            shelfmark, title = self._meta_mgr.get_meta_for_id(sys_id)
            return {
                'sys_id': sys_id,
                'shelfmark': shelfmark or '',
                'title': title or ''
            }
        except Exception as e:
            print(f"Get metadata error: {e}")
            return {}

    def get_enriched_metadata(self, sys_id: str) -> Dict[str, Any]:
        """
        Get enriched metadata including images, MARC data, and external info.
        Uses caching to avoid repeated fetches.

        Args:
            sys_id: The system ID of the manuscript

        Returns:
            Dict with full metadata including images, bibliography, etc.
        """
        if not self.is_ready:
            return {}

        # Check cache
        cache_entry = self._metadata_cache.get(sys_id)
        if cache_entry:
            data, timestamp = cache_entry
            if time.time() - timestamp < self._cache_ttl:
                return data

        try:
            data = self._meta_mgr.enrich_metadata(sys_id)
            # Cache the result
            self._metadata_cache[sys_id] = (data or {}, time.time())
            return data or {}
        except Exception as e:
            print(f"Get enriched metadata error: {e}")
            return {}

    def fetch_nli_metadata(self, sys_id: str) -> Dict[str, Any]:
        """
        Fetch NLI metadata for a manuscript (basic info from NLI API).

        Args:
            sys_id: The system ID of the manuscript

        Returns:
            Dict with shelfmark, title, desc, fl_ids, thumb_url
        """
        if not self.is_ready:
            return {}

        try:
            return self._meta_mgr.fetch_nli_data(sys_id)
        except Exception as e:
            print(f"Fetch NLI metadata error: {e}")
            return {}

    def get_display_data(self, sys_id: str) -> Dict[str, Any]:
        """
        Get display-ready metadata for UI.

        Args:
            sys_id: The system ID of the manuscript

        Returns:
            Dict with shelfmark, title, img (page), source, id
        """
        if not self.is_ready:
            return {}

        try:
            # Get basic metadata
            meta = self.get_metadata(sys_id)

            # Get enriched data for thumb
            enriched = self.get_enriched_metadata(sys_id)

            return {
                'shelfmark': meta.get('shelfmark', f"ID: {sys_id}"),
                'title': meta.get('title', ''),
                'id': sys_id,
                'thumb_url': enriched.get('thumb_url', ''),
                'has_images': bool(enriched.get('images', [])),
                'attribution': enriched.get('attribution', ''),
                'oxford_part_id': enriched.get('oxford_part_id'),
            }
        except Exception as e:
            print(f"Get display data error: {e}")
            return {}

    def get_marc_data(self, sys_id: str) -> Dict[str, Any]:
        """
        Fetch MARC bibliographic data for a manuscript.

        Returns:
            Dict with bibliography, notes, english_title, dimensions, etc.
        """
        if not self.is_ready:
            return {}

        try:
            return self._meta_mgr.fetch_marc_data(sys_id)
        except Exception as e:
            print(f"Get MARC data error: {e}")
            return {}

    def batch_fetch_metadata(
        self,
        sys_ids: List[str],
        use_network: bool = False
    ) -> None:
        """
        Pre-fetch metadata for multiple system IDs.

        Args:
            sys_ids: List of system IDs to fetch
            use_network: Whether to fetch from NLI API (slow) or just local
        """
        if not self.is_ready:
            return

        try:
            self._meta_mgr.batch_fetch_shelfmarks(sys_ids, use_network=use_network)
        except Exception as e:
            print(f"Batch fetch metadata error: {e}")

    # ========================================================================
    # Utility Methods
    # ========================================================================

    def extract_sys_id(self, uid_or_header: str) -> str:
        """Extract system ID from UID or header.

        Handles multiple formats:
        - 99XXXXXXXX (NLI format)
        - IEXXXXXXXX (IE prefix format)
        - Plain numbers
        """
        if not uid_or_header:
            return ''

        # First try the standard 99... format
        match = re.search(r'(99\d{8,})', uid_or_header)
        if match:
            return match.group(1)

        # Try IE prefix format (e.g., IE37931387)
        match = re.search(r'IE(\d{7,})', uid_or_header)
        if match:
            return match.group(1)  # Return just the numbers

        # Try any long number sequence (at least 7 digits)
        match = re.search(r'(\d{7,})', uid_or_header)
        if match:
            return match.group(1)

        return ''

    def cache_uid_sys_id(self, uid: str, sys_id: str) -> None:
        """Cache UID to system ID mapping for later resolution."""
        if not uid or not sys_id:
            return
        self._uid_sys_id_cache[uid] = (sys_id, time.time())

    def resolve_sys_id(self, uid_or_header: str) -> str:
        """Resolve sys_id using cache first, then fallback extraction."""
        if not uid_or_header:
            return ''
        cached = self._uid_sys_id_cache.get(uid_or_header)
        if cached:
            sys_id, cached_at = cached
            if time.time() - cached_at < self._uid_cache_ttl:
                return sys_id
            self._uid_sys_id_cache.pop(uid_or_header, None)
        return self.extract_sys_id(uid_or_header)

    def parse_header(self, full_header: str) -> Dict[str, str]:
        """Parse a full header into components."""
        if not self.is_ready:
            return {}

        try:
            return self._meta_mgr.parse_full_id_components(full_header)
        except:
            return {}

    def clear_metadata_cache(self) -> None:
        """Clear the metadata cache."""
        self._metadata_cache.clear()

    def get_adjacent_manuscript(
        self,
        sys_id: str,
        direction: int
    ) -> Optional[str]:
        """
        Get the next or previous manuscript in file order.

        Args:
            sys_id: Current system ID
            direction: 1 for next, -1 for previous

        Returns:
            System ID of adjacent manuscript, or None
        """
        if not self.is_ready:
            return None

        try:
            return self._search_engine.get_adjacent_sys_id_by_file_order(sys_id, direction)
        except Exception as e:
            print(f"Get adjacent manuscript error: {e}")
            return None


# ============================================================================
# Global Service Functions
# ============================================================================

_service: Optional[GenizahService] = None


def get_service() -> GenizahService:
    """Get the global GenizahService instance."""
    global _service
    if _service is None:
        _service = GenizahService()
    return _service


def init_service() -> bool:
    """Initialize the global service. Call once at startup."""
    service = get_service()
    return service.initialize()
