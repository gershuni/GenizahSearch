from typing import Optional
from genizah_core import MetadataManager, VariantManager, SearchEngine, LabEngine, Indexer, ListsManager


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
        # Local lists manager (for per-device storage / anonymous users)
        self._local_lists_mgr: Optional[ListsManager] = None

        # Per-user export state migrated to web.export_state (Phase 88, 2026-05-13).
        # User lists per-access factory migrated to lists_mgr @property (Phase 89, 2026-05-15).
        # See .planning/phases/ for migration history.

    @property
    def lists_mgr(self):
        """Per-access factory: return a fresh UserListsManager wrapping the
        local mgr + meta mgr, or None if bootstrap has not yet wired
        ``_local_lists_mgr`` (load-bearing — call sites at web/api.py:2114,
        web/components/comment_dialog.py:93, web/pages/lists.py:218 use
        ``if not state.lists_mgr:`` to detect the pre-bootstrap window).

        Phase 89 (D-01, D-02): every access constructs a new UserListsManager.
        Safe because UserListsManager is stateless post-Phase 89 (no per-instance
        cache fields — see web/user_lists.py docstring). The per-ACCESS lifecycle
        satisfies LISTS-02 in effect: no state crosses request boundaries because
        no state exists. Plan 89-02 (2026-05-15) atomically deleted the
        dead-code AppState field, the bootstrap helper method, and the caller
        at web/main.py:1508 alongside the Phase 88 survivor-test update at
        tests/test_no_appstate_export_fields.py:67 (D-09 plan-boundary
        discipline; R10 5-file atomic-commit verification).
        """
        if self._local_lists_mgr is None:
            return None  # Pre-bootstrap None-guard contract — DO NOT remove.
        # Local import: UserListsManager imports from web.user_lists at module
        # load time would create a cycle (user_lists imports from web.auth_state
        # which imports from web.supabase_client which depends on nicegui app
        # state). Lazy import preserves the existing import-time graph.
        from web.user_lists import UserListsManager
        return UserListsManager(self._local_lists_mgr, self.meta_mgr)

    @lists_mgr.setter
    def lists_mgr(self, value):
        """Set the local lists manager (per-device anonymous store).

        The factory property at ``lists_mgr`` wraps ``_local_lists_mgr``
        + ``meta_mgr`` into a fresh ``UserListsManager`` on each access
        (Phase 89 D-01). Setting ``state.lists_mgr = ListsManager(...)``
        from web/main.py:1505 stores the local manager; the factory then
        produces the wrapper on demand.
        """
        self._local_lists_mgr = value

    def get_local_lists_mgr(self) -> Optional[ListsManager]:
        """Get the local lists manager directly (for migration)."""
        return self._local_lists_mgr

    def is_ready(self):
        return self.searcher is not None and self.meta_mgr is not None


# Global instance
state = AppState()
