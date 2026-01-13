"""
GenizahService - Thread-safe wrapper for genizah_core.

This service layer isolates the web application from the core,
providing a clean API and handling thread safety for concurrent requests.
"""

import sys
import os
import threading
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

# Add parent directory to path for importing genizah_core
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from genizah_core import (
    Config,
    MetadataManager,
    VariantManager,
    SearchEngine,
)


@dataclass
class SearchResult:
    """Structured search result for the web UI."""
    uid: str
    display: str
    snippet: str
    raw_header: str
    source: str
    highlight_pattern: Optional[str] = None
    cross_page: bool = False


@dataclass
class DocumentPage:
    """A single page of a manuscript."""
    uid: str
    p_num: int
    text: str
    full_header: str
    fl_id: Optional[str] = None


class GenizahService:
    """
    Thread-safe service layer for Genizah search operations.

    Provides:
    - Singleton pattern for shared resource management
    - Thread-safe search operations via lock
    - Clean API for web handlers
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
        # Only initialize once
        if GenizahService._initialized:
            return

        with GenizahService._lock:
            if GenizahService._initialized:
                return

            self._search_lock = threading.Lock()
            self._meta_mgr: Optional[MetadataManager] = None
            self._var_mgr: Optional[VariantManager] = None
            self._search_engine: Optional[SearchEngine] = None
            self._ready = False
            self._init_error: Optional[str] = None

            GenizahService._initialized = True

    def initialize(self) -> bool:
        """
        Initialize core components. Call this once at startup.
        Returns True if successful.
        """
        try:
            self._meta_mgr = MetadataManager()
            self._var_mgr = VariantManager()
            self._search_engine = SearchEngine(self._meta_mgr, self._var_mgr)

            # Start background loading of heavy resources
            self._meta_mgr.start_background_loading()

            self._ready = True
            return True

        except Exception as e:
            self._init_error = str(e)
            return False

    @property
    def is_ready(self) -> bool:
        """Check if the service is initialized and ready."""
        return self._ready and self._search_engine is not None

    @property
    def init_error(self) -> Optional[str]:
        """Get initialization error message if any."""
        return self._init_error

    @property
    def index_exists(self) -> bool:
        """Check if the search index exists."""
        db_path = os.path.join(Config.INDEX_DIR, "tantivy_db")
        return os.path.exists(db_path)

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
            query: Search terms
            mode: Search mode - 'exact', 'variants', 'variants_extended', 'variants_maximum', 'fuzzy', 'Regex'
            gap: Maximum gap between terms for proximity search
            limit: Maximum results to return

        Returns:
            List of SearchResult objects
        """
        if not self.is_ready:
            return []

        # Normalize mode names
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
        }
        search_mode = mode_map.get(mode, 'variants')

        with self._search_lock:
            try:
                raw_results = self._search_engine.execute_search(
                    query,
                    search_mode,
                    gap
                )

                results = []
                for r in raw_results[:limit]:
                    results.append(SearchResult(
                        uid=r.get('uid', ''),
                        display=r.get('display', ''),
                        snippet=r.get('snippet', ''),
                        raw_header=r.get('raw_header', ''),
                        source=r.get('source', 'V0.8'),
                        highlight_pattern=r.get('highlight_pattern'),
                        cross_page=r.get('cross_page', False)
                    ))

                return results

            except Exception as e:
                print(f"Search error: {e}")
                return []

    def get_document(self, sys_id: str) -> List[DocumentPage]:
        """
        Get all pages of a manuscript by system ID.

        Args:
            sys_id: The manuscript system identifier

        Returns:
            List of DocumentPage objects, sorted by page number
        """
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
                        fl_id=p.get('fl_id')
                    ))

                return pages

            except Exception as e:
                print(f"Get document error: {e}")
                return []

    def get_page(
        self,
        sys_id: str,
        p_num: Optional[int] = None,
        uid: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get a specific page of a manuscript.

        Args:
            sys_id: The manuscript system identifier
            p_num: Page number (optional)
            uid: Unique page ID (optional, alternative to p_num)

        Returns:
            Dict with page data or None if not found
        """
        if not self.is_ready:
            return None

        with self._search_lock:
            try:
                result = self._search_engine.get_browse_page(sys_id, p_num=p_num)
                return result
            except Exception as e:
                print(f"Get page error: {e}")
                return None

    def get_metadata(self, sys_id: str) -> Dict[str, Any]:
        """
        Get metadata for a manuscript.

        Args:
            sys_id: The manuscript system identifier

        Returns:
            Dict with metadata fields
        """
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

    def extract_sys_id(self, uid: str) -> str:
        """
        Extract system ID from a unique page ID.

        Args:
            uid: The unique page identifier (e.g., "99123456789_IE12345_1")

        Returns:
            System ID - the 99... number (e.g., "99123456789")
        """
        if not uid:
            return ''

        # sys_id is a number starting with 99
        import re
        match = re.search(r'(99\d{8,})', uid)
        if match:
            return match.group(1)
        return uid


# Global service instance
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
