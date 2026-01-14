from typing import Optional, List, Dict, Any
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
        self.lists_mgr: Optional[ListsManager] = None

        self.last_results: List[Dict[str, Any]] = []
        self.current_search_query: str = ""

    def is_ready(self):
        return self.searcher is not None and self.meta_mgr is not None

# Global instance
state = AppState()
