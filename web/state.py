from typing import Optional, List, Dict, Any, Union
from genizah_core import MetadataManager, VariantManager, SearchEngine, LabEngine, Indexer, AIManager, ListsManager


class AppState:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(AppState, cls).__new__(cls)
            cls._instance.init()
        return cls._instance

    def init(self):
        self.meta_mgr: Optional[MetadataManager] = None
        self.var_mgr: Optional[VariantManager] = None
        self.searcher: Optional[SearchEngine] = None
        self.lab_engine: Optional[LabEngine] = None
        self.indexer: Optional[Indexer] = None
        self.ai_mgr: Optional[AIManager] = None

        # Local lists manager (for per-device storage / anonymous users)
        self._local_lists_mgr: Optional[ListsManager] = None

        # User lists manager (auth-aware wrapper)
        self._user_lists_mgr = None

        self.last_results: List[Dict[str, Any]] = []
        self.current_search_query: str = ""

        # Parallels results (for export functionality)
        self.parallels_results: List[Dict[str, Any]] = []
        self.parallels_filtered: List[Dict[str, Any]] = []

    @property
    def lists_mgr(self):
        """
        Get the appropriate lists manager based on auth state.

        Returns UserListsManager when available (for logged-in users),
        falls back to local ListsManager for anonymous users.
        """
        # Try to use the auth-aware manager
        if self._user_lists_mgr is not None:
            return self._user_lists_mgr

        # Fall back to local manager
        return self._local_lists_mgr

    @lists_mgr.setter
    def lists_mgr(self, value):
        """Set the local lists manager."""
        self._local_lists_mgr = value
        # Update user lists manager if it exists
        if self._user_lists_mgr is not None:
            self._user_lists_mgr.local_mgr = value
            self._user_lists_mgr.meta_mgr = self.meta_mgr

    def init_user_lists_mgr(self):
        """
        Initialize the user lists manager.
        Should be called after app is ready and auth state is available.
        """
        try:
            from web.user_lists import UserListsManager
            self._user_lists_mgr = UserListsManager(
                local_mgr=self._local_lists_mgr,
                meta_mgr=self.meta_mgr
            )
        except ImportError:
            # user_lists module not available, use local manager
            pass

    def get_local_lists_mgr(self) -> Optional[ListsManager]:
        """Get the local lists manager directly (for migration)."""
        return self._local_lists_mgr

    def is_ready(self):
        return self.searcher is not None and self.meta_mgr is not None


# Global instance
state = AppState()
