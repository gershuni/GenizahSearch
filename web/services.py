"""
GenizahService - Thread-safe wrapper for genizah_core.
Updated to proxy web.state.state for backward compatibility.
"""

import sys
import os
import re
import threading
import time
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
    LabSettings,
)
from web.state import state

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
    return f"{NLI_IIIF_BASE}/FL{digits}/full/max/0/default.jpg"

def build_iiif_image_url(base_url: str, size: str = 'full') -> str:
    if not base_url: return ''
    if size == 'thumb': size_param = '400,'
    elif size == 'full': size_param = 'max'
    else: size_param = size
    return f"{base_url}/full/{size_param}/0/default.jpg"

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

    def search_by_shelfmark(self, shelfmark_query: str, limit: int = 50) -> List[ManuscriptInfo]:
        if not self.is_ready: return []
        try:
            results = state.searcher.execute_search(shelfmark_query, 'Shelfmark', 0)
            manuscripts = []
            seen_ids = set()
            for r in results[:limit]:
                sys_id = r['display']['id']
                if sys_id and sys_id not in seen_ids:
                    seen_ids.add(sys_id)
                    manuscripts.append(ManuscriptInfo(
                        sys_id=sys_id,
                        shelfmark=r['display'].get('shelfmark', ''),
                        title=r['display'].get('title', '')
                    ))
            return manuscripts
        except Exception as e:
            print(f"Search by shelfmark error: {e}")
            return []

    def get_browse_page(self, sys_id: str, p_num: Optional[int] = None, direction: int = 0, absolute_index: Optional[int] = None, allow_cross: bool = False) -> Optional[BrowsePage]:
        if not self.is_ready: return None
        try:
            result = state.searcher.get_browse_page(
                sys_id, p_num=p_num, next_prev=direction, absolute_index=absolute_index, allow_cross=allow_cross
            )
            if not result: return None

            shelfmark, title = state.meta_mgr.get_meta_for_id(result.get('sys_id', sys_id))

            fl_id = None
            try:
                parsed = state.meta_mgr.parse_full_id_components(result.get('full_header', ''))
                fl_id = parsed.get('fl_id')
            except: pass

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

_service_instance = GenizahService()

def get_service():
    return _service_instance

def init_service():
    return True
