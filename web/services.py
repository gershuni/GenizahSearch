"""
GenizahService - Thread-safe wrapper for genizah_core.

This service layer isolates the web application from the core,
providing a clean API and handling thread safety for concurrent requests.
"""

import sys
import os
import re
import threading
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field

# Add parent directory to path for importing genizah_core
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from genizah_core import (
    Config,
    MetadataManager,
    VariantManager,
    SearchEngine,
    LabEngine,
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


@dataclass
class DocumentPage:
    """A single page of a manuscript."""
    uid: str
    p_num: int
    text: str
    full_header: str
    fl_id: Optional[str] = None
    sys_id: Optional[str] = None


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


@dataclass
class ManuscriptInfo:
    """Basic manuscript information for browse list."""
    sys_id: str
    shelfmark: str
    title: str


# ============================================================================
# Image URL Helpers
# ============================================================================

NLI_IIIF_BASE = "https://iiif.nli.org.il/IIIFv21"


def get_thumbnail_url(fl_id: str, size: int = 400) -> str:
    """Build IIIF thumbnail URL from FL ID."""
    digits = re.sub(r"\D", "", str(fl_id))
    if not digits:
        return ''
    return f"{NLI_IIIF_BASE}/FL{digits}/full/{size},/0/default.jpg"


def get_full_image_url(fl_id: str) -> str:
    """Build IIIF full image URL from FL ID."""
    digits = re.sub(r"\D", "", str(fl_id))
    if not digits:
        return ''
    return f"{NLI_IIIF_BASE}/FL{digits}/full/max/0/default.jpg"


def get_rosetta_fallback_url(fl_id: str) -> str:
    """Build Rosetta fallback thumbnail URL."""
    digits = re.sub(r"\D", "", str(fl_id))
    if not digits:
        return ''
    return f"https://rosetta.nli.org.il/delivery/DeliveryManagerServlet?dps_func=thumbnail&dps_pid=FL{digits}"


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
            self._ready = False
            self._init_error: Optional[str] = None

            GenizahService._initialized = True

    def initialize(self) -> bool:
        """Initialize core components. Call this once at startup."""
        try:
            self._meta_mgr = MetadataManager()
            self._var_mgr = VariantManager()
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
    # Search Operations
    # ========================================================================

    def search(
        self,
        query: str,
        mode: str = "variants",
        gap: int = 0,
        limit: int = 100
    ) -> List[SearchResult]:
        """Execute a search query."""
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
            'shelfmark': 'Shelfmark',
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

                    sys_id = display.get('id', '') or self.extract_sys_id(r.get('uid', ''))

                    results.append(SearchResult(
                        uid=r.get('uid', ''),
                        sys_id=sys_id,
                        display=display,
                        snippet=r.get('snippet', ''),
                        raw_header=r.get('raw_header', ''),
                        source=r.get('source', 'V0.8'),
                        full_text=r.get('full_text', ''),
                        highlight_pattern=r.get('highlight_pattern'),
                        cross_page=r.get('cross_page', False)
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

                # lab_composition_search returns dict with 'main', 'filtered', 'known'
                main_results = raw_result.get('main', []) if isinstance(raw_result, dict) else raw_result

                results = []
                for r in main_results[:limit]:
                    sys_id = self.extract_sys_id(r.get('raw_header', '') or r.get('uid', ''))
                    display = r.get('display', {})
                    if isinstance(display, str):
                        display = {'shelfmark': display}

                    results.append(CompositionResult(
                        score=r.get('sort_score', r.get('score', 0)),
                        sys_id=sys_id,
                        raw_header=r.get('raw_header', ''),
                        ms_snippet=r.get('ms_snippet', r.get('snippet', '')),
                        src_snippet=r.get('src_snippet', ''),
                        pattern=r.get('pattern', r.get('highlight_pattern', '')),
                        display=display
                    ))

                return results

            except Exception as e:
                print(f"Composition search error: {e}")
                import traceback
                traceback.print_exc()
                return []

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
                    pages.append(DocumentPage(
                        uid=p.get('uid', ''),
                        p_num=p.get('p_num', 0),
                        text=p.get('text', ''),
                        full_header=p.get('full_header', ''),
                        fl_id=p.get('fl_id'),
                        sys_id=sys_id
                    ))

                return pages

            except Exception as e:
                print(f"Get document error: {e}")
                return []

    def browse_page(
        self,
        sys_id: str,
        p_num: Optional[int] = None,
        direction: int = 0,
        absolute_index: Optional[int] = None
    ) -> Optional[BrowsePage]:
        """
        Get a page for browsing with navigation info.

        Args:
            sys_id: Manuscript system ID
            p_num: Target page number
            direction: Navigation direction (-1=prev, 0=current, 1=next)
            absolute_index: Direct array index (overrides p_num)
        """
        if not self.is_ready:
            return None

        with self._search_lock:
            try:
                result = self._search_engine.get_browse_page(
                    sys_id,
                    p_num=p_num,
                    next_prev=direction,
                    absolute_index=absolute_index
                )

                if not result:
                    return None

                # Get metadata
                shelfmark, title = '', ''
                try:
                    shelfmark, title = self._meta_mgr.get_meta_for_id(sys_id)
                except:
                    pass

                # Get FL ID for image
                fl_id = None
                try:
                    parsed = self._meta_mgr.parse_full_id_components(result.get('full_header', ''))
                    fl_id = parsed.get('fl_id')
                except:
                    pass

                # Build thumbnail URL
                thumb_url = get_thumbnail_url(fl_id) if fl_id else None

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
                    thumb_url=thumb_url
                )

            except Exception as e:
                print(f"Browse page error: {e}")
                return None

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

    # ========================================================================
    # Metadata & Images
    # ========================================================================

    def get_metadata(self, sys_id: str) -> Dict[str, Any]:
        """Get metadata for a manuscript."""
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
        """Get enriched metadata including images."""
        if not self.is_ready:
            return {}

        try:
            data = self._meta_mgr.enrich_metadata(sys_id)
            return data or {}
        except Exception as e:
            print(f"Get enriched metadata error: {e}")
            return {}

    def get_images(self, sys_id: str) -> List[Dict[str, str]]:
        """Get list of images for a manuscript."""
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
    # Utility Methods
    # ========================================================================

    def extract_sys_id(self, uid_or_header: str) -> str:
        """Extract system ID (99...) from UID or header."""
        if not uid_or_header:
            return ''

        match = re.search(r'(99\d{8,})', uid_or_header)
        if match:
            return match.group(1)
        return ''

    def parse_header(self, full_header: str) -> Dict[str, str]:
        """Parse a full header into components."""
        if not self.is_ready:
            return {}

        try:
            return self._meta_mgr.parse_full_id_components(full_header)
        except:
            return {}


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
