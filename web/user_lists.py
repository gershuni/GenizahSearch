# -*- coding: utf-8 -*-
"""
User Lists Manager - Supabase-backed lists management for the web interface.

This module provides a lists manager that:
- Uses Supabase when user is logged in (cloud storage)
- Gives anonymous visitors no lists at all: empty reads, refused writes

Anonymous visitors used to fall back to the process-wide local ListsManager
(the server's lists.pkl), which every visitor shared (improvement sweep C2).
That fallback is gone; the shared store is never read or written from here.

Usage:
    from web.user_lists import get_lists_manager

    lists_mgr = get_lists_manager()
    lists = lists_mgr.get_all_lists()
"""

from typing import Optional, Dict, List

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

from nicegui import run

from web.auth_state import GlobalAuthState
from web.supabase_client import (
    get_user_client, get_user_lists, create_list as sb_create_list,
    update_list as sb_update_list, delete_list as sb_delete_list,
    get_list_items, add_list_item, add_list_items, update_list_item, delete_list_item,
    get_recent_items, add_recent_item, get_projects, create_project as sb_create_project,
    update_project as sb_update_project, delete_project as sb_delete_project
)
from genizah_core import ListsManager

import logging
LOGGER = logging.getLogger(__name__)


def localize_list_name(list_data: Dict) -> str:
    """Return the display name for a list, localized for app-managed lists.

    System lists (e.g. "Recently Viewed") AND the default list ("General") are
    stored verbatim in the DB with their canonical English name; their names have
    translation keys in the shared TRANSLATIONS dict and must be localized at
    render time. User-created list names are NEVER translated (a user list named
    "General" would otherwise be mistranslated).

    This mirrors the desktop's GenizahGUI._get_list_display_name, which translates
    when `is_system OR is_default`. W4 (2026-06-23): the web render sites only
    translated `is_system`, so the default "General" list leaked English under a
    Hebrew UI. Use this helper at EVERY web render site so the rule stays generic
    (all app-managed names), not hardcoded per string.
    """
    from web.translations import tr as _tr
    name = list_data.get('name') or list_data.get('name_en') or ''
    if list_data.get('is_system') or list_data.get('is_default'):
        return _tr(name)
    return name


# Project color palette (same as desktop app)
PROJECT_COLORS = [
    '#4CAF50',  # Green
    '#2196F3',  # Blue
    '#9C27B0',  # Purple
    '#FF5722',  # Deep Orange
    '#00BCD4',  # Cyan
    '#E91E63',  # Pink
    '#795548',  # Brown
    '#607D8B',  # Blue Gray
    '#FF9800',  # Orange
    '#009688',  # Teal
]


class UserListsManager:
    """
    Auth-aware, Supabase-only lists manager.

    When a user is logged in, all operations go through Supabase.
    When not logged in, reads return empty/default data and writes return a
    falsy result. The local ListsManager (pkl file) is never consulted.
    """

    def __init__(self, local_mgr: Optional[ListsManager] = None, meta_mgr=None):
        """
        Initialize the user lists manager.

        Args:
            local_mgr: Accepted for signature compatibility only and IGNORED.
                It is the process-wide store shared by every web visitor, so
                reading it would show one visitor's lists to another (sweep C2).
            meta_mgr: Metadata manager for enriching items

        Phase 89 (2026-05-14): The 10s TTL cache was removed. Each .data /
        ._get_cached_data() call hits Supabase fresh. The factory-property
        at web/state.py:AppState.lists_mgr returns a new instance per access,
        so per-instance memoization would still be unsafe under UI callback
        capture (web/components/add_to_list_dialog.py, project_tree.py); see
        Phase 89 CONTEXT.md D-03/D-04.
        """
        # C2 containment: never keep the shared store. No method reads
        # self.local_mgr any more; the attribute stays None for callers that
        # still inspect it.
        self.local_mgr = None
        self.meta_mgr = meta_mgr

    @property
    def is_authenticated(self) -> bool:
        """Check if user is logged in."""
        return GlobalAuthState.is_logged_in()

    @property
    def user_id(self) -> Optional[str]:
        """Get current user ID if logged in."""
        return GlobalAuthState.get_user_id()

    @property
    def data(self) -> Dict:
        """
        Get lists data - for compatibility with existing code.
        """
        if self.is_authenticated:
            return self._get_cached_data()
        return self._get_default_data()

    def _get_default_data(self) -> Dict:
        """Return default data structure."""
        return {
            'lists': {
                'default': {
                    'name': 'General',
                    'name_en': 'General',
                    'color': '#FFD700',
                    'is_default': True,
                    'is_system': False,
                    'project_id': None
                }
            },
            'projects': {},
            'lists_order': ['default'],
            'projects_order': [],
            'items': {},
            'recent_items': [],
            'all_tags': []
        }

    def _get_cached_data(self, *, client=None, user_id: Optional[str] = None,
                         is_authenticated: Optional[bool] = None) -> Dict:
        """Fetch lists+projects data fresh from Supabase on every call (Phase 89).

        Previously this method memoized the result in a (user_id, timestamp, data)
        tuple for 10 seconds. Phase 89 removed that cache because (a) the
        factory-property at AppState.lists_mgr returns a new instance per
        access, so per-instance state cannot be trusted to survive between
        accesses, and (b) UI dialog callbacks capture managers into closures
        that outlive the request that constructed them — per-instance cache
        in those captured managers would serve stale data indefinitely. See
        Phase 89 CONTEXT.md D-03/D-04 for the full analysis.

        Defensive: if is_logged_in() returns True but get_user_id() returns
        None (edge case during token rotation / partial auth state), return
        default data rather than calling Supabase with user_id=None which
        would behave like an unfiltered query. See REVIEWS.md R2.

        The three keyword-only parameters (2026-10-21) make this read safe in a
        `run.io_bound` worker, exactly as on `get_items_in_list_sync`: a worker
        cannot read the visitor's storage, so the auth decision, the user id and
        the client are resolved on the loop (`_worker_context`) and passed in.
        All three default to None = unchanged behaviour; `client` is forwarded
        to the two readers only when given, so a caller (or a test fake) that
        takes a user id alone still works.
        """
        authed = self.is_authenticated if is_authenticated is None else is_authenticated
        if not authed:
            return self._get_default_data()

        # An explicit `is_authenticated` means the caller resolved auth on the
        # loop and may be calling from a worker, where `self.user_id` reads the
        # visitor's storage and silently answers None: never re-derive it here.
        if user_id is None and is_authenticated is None:
            user_id = self.user_id
        if not user_id:
            # R2 defensive guard: never call get_user_lists(None) / get_projects(None).
            return self._get_default_data()

        if client is None:
            lists = get_user_lists(user_id)
            projects = get_projects(user_id)
        else:
            lists = get_user_lists(user_id, client=client)
            projects = get_projects(user_id, client=client)

        data = self._get_default_data()
        data['lists'] = {}
        data['projects'] = {}

        for lst in lists:
            list_id = str(lst['id'])
            data['lists'][list_id] = {
                'name': lst.get('name', ''),
                'name_en': lst.get('name_en', ''),
                'color': lst.get('color', '#FFD700'),
                'is_default': lst.get('is_default', False),
                'is_system': lst.get('is_system', False),
                'project_id': lst.get('project_id'),
                'created': lst.get('created_at'),
            }

        for proj in projects:
            proj_id = str(proj['id'])
            data['projects'][proj_id] = {
                'name': proj.get('name', ''),
                'color': proj.get('color', '#4CAF50'),
                'created': proj.get('created_at'),
            }

        data['lists_order'] = list(data['lists'].keys())
        data['projects_order'] = list(data['projects'].keys())
        return data

    def invalidate_cache(self):
        """Compatibility no-op — Phase 89 removed the cache.

        Internal mutation paths (create_list, update_list, add_item, etc.)
        still call this; the call is harmless. Public callers: none. The
        method is preserved (not deleted) to keep ~10 internal call sites
        byte-unchanged at the Phase 89 boundary. See CONTEXT.md D-05.
        """
        pass

    def _worker_context(self):
        """(authed, user_id, client) for a write that will run in a worker thread.

        Resolved HERE, on the event loop, before the offload. In a
        `run.io_bound` worker the visitor's storage is unreadable:
        `self.is_authenticated` reads as logged-out and `get_user_client()`
        hands back the anonymous client, so the write would be refused by RLS
        with nothing saying so (see get_items_in_list_sync). The client is
        built only for a signed-in visitor; anonymous callers get
        ``(False, None, None)`` and never reach Supabase.
        """
        if not self.is_authenticated:
            return False, None, None
        return True, self.user_id, get_user_client()

    def _resolve_list_id(self, list_id: str, user_id: str, *, client) -> Optional[int]:
        """``'default'`` -> the default list's row id (one Supabase read); else ``int(list_id)``.

        Runs in the worker: resolving ``'default'`` is a read. None when the id
        is not numeric (or no default list exists).
        """
        actual_list_id = list_id
        if list_id == 'default':
            data = self._get_cached_data(client=client, user_id=user_id, is_authenticated=True)
            for lid, ldata in data.get('lists', {}).items():
                if ldata.get('is_default'):
                    actual_list_id = lid
                    break
        try:
            return int(actual_list_id)
        except ValueError:
            return None

    def _meta_for(self, sys_id: str):
        """(shelfmark, title) for an item, or (None, None) without a metadata manager."""
        if self.meta_mgr:
            return self.meta_mgr.get_meta_for_id(sys_id)
        return None, None

    # === List Operations ===

    def _get_list_item_count(self, list_id: str) -> int:
        """Get item count for a list."""
        if self.is_authenticated:
            # Recent system list lives in recent_items, not list_items (see
            # _is_recent_list); counting via get_list_items(id) would return 0.
            if self._is_recent_list(list_id):
                return len(get_recent_items(self.user_id))
            try:
                list_id_int = int(list_id)
                items = get_list_items(list_id_int)
                return len(items)
            except (ValueError, Exception):
                return 0
        return 0

    def get_all_lists(self, include_recent: bool = True) -> List[Dict]:
        """Get all lists sorted by order."""
        if self.is_authenticated:
            data = self._get_cached_data()
            lists = []
            for list_id, lst in data.get('lists', {}).items():
                if not include_recent and lst.get('is_system'):
                    continue
                lists.append({
                    'id': list_id,
                    **lst,
                    'count': 0  # Count loaded on demand
                })
            return lists
        return []

    def _create_list_blocking(self, user_id: str, name: str, color: Optional[str],
                              project_id: Optional[str], project_id_int: Optional[int],
                              *, client) -> Dict:
        """The Supabase part of create_list; runs in a worker thread."""
        # If in a project, get project color
        if project_id:
            data = self._get_cached_data(client=client, user_id=user_id, is_authenticated=True)
            project = data.get('projects', {}).get(str(project_id), {})
            color = project.get('color', '#FFD700')
        return sb_create_list(
            user_id, name,
            name_en=name,
            color=color or '#FFD700',
            project_id=project_id_int,
            client=client,
        )

    async def create_list(self, name: str, color: str = None, project_id: str = None) -> Optional[str]:
        """
        Create a new list, optionally inside a project.

        Args:
            name: List name
            color: Color (ignored if project_id is set - will inherit from project)
            project_id: Optional project to add list to

        The Supabase round trips (the project colour read, the insert) run off
        the event loop (2026-10-21); auth and the client are resolved here, on it.
        """
        authed, user_id, client = self._worker_context()
        if authed:
            project_id_int = int(project_id) if project_id else None
            result = await run.io_bound(self._create_list_blocking, user_id, name, color,
                                        project_id, project_id_int, client=client)
            if result and result.get('success'):
                self.invalidate_cache()
                return str(result['list']['id'])
            LOGGER.error(f"Failed to create list: {result.get('error') if result else 'no answer'}")
            return None
        return None

    def create_list_sync(self, name: str, color: str = None, project_id: str = None) -> Optional[str]:
        """Synchronous version of create_list."""
        if self.is_authenticated:
            if project_id:
                data = self._get_cached_data()
                project = data.get('projects', {}).get(str(project_id), {})
                color = project.get('color', '#FFD700')

            result = sb_create_list(
                self.user_id, name,
                name_en=name,
                color=color or '#FFD700',
                project_id=int(project_id) if project_id else None
            )
            if result.get('success'):
                self.invalidate_cache()
                return str(result['list']['id'])
            return None
        return None

    async def update_list(self, list_id: str, name: str = None, color: str = None) -> bool:
        """Update a list's properties. The write runs off the event loop."""
        authed, _user_id, client = self._worker_context()
        if authed:
            data = {}
            if name is not None:
                data['name'] = name
                data['name_en'] = name
            if color is not None:
                data['color'] = color

            try:
                list_id_int = int(list_id)
            except ValueError:
                return False

            result = await run.io_bound(sb_update_list, list_id_int, data, client=client)
            if result and result.get('success'):
                self.invalidate_cache()
                return True
            return False
        return False

    async def update_list_project(self, list_id: str, project_id: Optional[str]) -> bool:
        """Move a list to a project (or remove from project if project_id is None)."""
        authed, _user_id, client = self._worker_context()
        if authed:
            try:
                list_id_int = int(list_id)
                project_id_int = int(project_id) if project_id else None
            except (ValueError, TypeError):
                return False

            result = await run.io_bound(sb_update_list, list_id_int, {'project_id': project_id_int},
                                        client=client)
            if result and result.get('success'):
                self.invalidate_cache()
                return True
            return False
        return False

    async def delete_list(self, list_id: str) -> bool:
        """Soft-delete a list (move to trash)."""
        authed, _user_id, client = self._worker_context()
        if authed:
            try:
                list_id_int = int(list_id)
            except ValueError:
                return False

            result = await run.io_bound(sb_delete_list, list_id_int, client=client)  # soft delete
            if result and result.get('success'):
                self.invalidate_cache()
                return True
            return False
        return False

    def get_deleted_lists(self) -> List[Dict]:
        """Get soft-deleted lists (trash view)."""
        if self.is_authenticated:
            from web.supabase_client import get_deleted_lists as sb_get_deleted_lists
            return sb_get_deleted_lists(self.user_id)
        return []

    async def restore_list(self, list_id: str) -> bool:
        """Restore a soft-deleted list from trash."""
        authed, _user_id, client = self._worker_context()
        if authed:
            try:
                list_id_int = int(list_id)
            except ValueError:
                return False

            from web.supabase_client import restore_list as sb_restore_list
            result = await run.io_bound(sb_restore_list, list_id_int, client=client)
            if result and result.get('success'):
                self.invalidate_cache()
                return True
            return False
        return False

    async def permanently_delete_list(self, list_id: str) -> bool:
        """Permanently delete a list (no recovery)."""
        authed, _user_id, client = self._worker_context()
        if authed:
            try:
                list_id_int = int(list_id)
            except ValueError:
                return False

            result = await run.io_bound(sb_delete_list, list_id_int, permanent=True, client=client)
            if result and result.get('success'):
                self.invalidate_cache()
                return True
            return False
        return False

    async def empty_trash(self) -> Optional[int]:
        """Permanently delete all soft-deleted lists. Returns the count deleted, or None on failure.

        None (not 0) for a failure or a signed-out caller, so the page can tell
        a failed Empty Trash from an empty one.
        """
        authed, user_id, client = self._worker_context()
        if authed:
            from web.supabase_client import empty_trash as sb_empty_trash
            result = await run.io_bound(sb_empty_trash, user_id, client=client)
            if result is None:
                return None
            if result.get('deleted_count'):
                self.invalidate_cache()
            if result.get('success'):
                return result.get('deleted_count', 0)
            return None
        return None

    # === Item Operations ===

    def _add_item_blocking(self, user_id: str, sys_id: str, list_id: str, note: str,
                           tags: Optional[List[str]], fl_id: Optional[str], *, client) -> Dict:
        """The Supabase part of add_item; runs in a worker thread."""
        list_id_int = self._resolve_list_id(list_id, user_id, client=client)
        if list_id_int is None:
            return {'error': 'Invalid list id'}
        shelfmark, title = self._meta_for(sys_id)
        return add_list_item(
            list_id_int, sys_id,
            shelfmark=shelfmark,
            title=title,
            fl_id=fl_id,
            note=note,
            tags=tags,
            client=client,
        )

    async def add_item(self, sys_id: str, list_id: str = 'default',
                       note: str = '', tags: List[str] = None,
                       source: str = '', fl_id: str = None, img: str = None) -> bool:
        """Add an item to a list. The write (and the 'default' lookup) runs off the loop."""
        authed, user_id, client = self._worker_context()
        if authed:
            if list_id != 'default':
                try:
                    int(list_id)
                except ValueError:
                    return False

            result = await run.io_bound(self._add_item_blocking, user_id, sys_id, list_id, note,
                                        tags, fl_id, client=client)
            if result and result.get('success'):
                self.invalidate_cache()
                return True
            return False
        return False

    def add_item_sync(self, sys_id: str, list_id: str = 'default',
                      note: str = '', tags: List[str] = None,
                      source: str = '', fl_id: str = None, img: str = None) -> bool:
        """Synchronous version of add_item."""
        if self.is_authenticated:
            actual_list_id = list_id
            if list_id == 'default':
                data = self._get_cached_data()
                for lid, ldata in data.get('lists', {}).items():
                    if ldata.get('is_default'):
                        actual_list_id = lid
                        break

            try:
                list_id_int = int(actual_list_id)
            except ValueError:
                return False

            shelfmark = None
            title = None
            if self.meta_mgr:
                shelfmark, title = self.meta_mgr.get_meta_for_id(sys_id)

            result = add_list_item(
                list_id_int, sys_id,
                shelfmark=shelfmark,
                title=title,
                fl_id=fl_id,
                note=note,
                tags=tags
            )
            return result.get('success', False)
        return False

    def _add_items_blocking(self, user_id: str, sys_ids: List[str], list_id: str, note: str,
                            tags: Optional[List[str]], fl_id: Optional[str], *, client) -> Dict:
        """The Supabase part of add_items; runs in a worker thread."""
        list_id_int = self._resolve_list_id(list_id, user_id, client=client)
        if list_id_int is None:
            return {'error': 'Invalid list id'}
        rows = []
        for sys_id in sys_ids:
            shelfmark, title = self._meta_for(sys_id)
            rows.append({'sys_id': sys_id, 'shelfmark': shelfmark, 'title': title,
                         'fl_id': fl_id, 'note': note, 'tags': tags})
        return add_list_items(list_id_int, rows, client=client)

    async def add_items(self, sys_ids, list_id: str = 'default', *, note: str = '',
                        tags: List[str] = None, fl_id: str = None) -> int:
        """Add several items to one list in ONE insert; returns the number added.

        The search page's bulk add used to run one `add_item_sync` per item on
        the event loop (one transatlantic round trip each). Anonymous visitors
        get 0 with no Supabase call; empty sys_ids are skipped. The insert is
        all or nothing (`add_list_items`), so the answer is 0 or the number
        wanted; no answer from the worker (run.io_bound returned None) is 0.
        """
        authed, user_id, client = self._worker_context()
        if not authed:
            return 0
        wanted = [s for s in sys_ids if s]
        if not wanted:
            return 0
        if list_id != 'default':
            try:
                int(list_id)
            except ValueError:
                return 0
        result = await run.io_bound(self._add_items_blocking, user_id, wanted, list_id, note,
                                    tags, fl_id, client=client)
        if result and result.get('success'):
            self.invalidate_cache()
            return int(result.get('added') or 0)
        return 0

    async def remove_item_from_list(self, item_id: str, list_id: str) -> bool:
        """Remove an item from a list."""
        authed, _user_id, client = self._worker_context()
        if authed:
            try:
                item_id_int = int(item_id)
            except ValueError:
                return False

            result = await run.io_bound(delete_list_item, item_id_int, client=client)
            if result and result.get('success'):
                self.invalidate_cache()
                return True
            return False
        return False

    def remove_item_from_list_sync(self, item_id: str, list_id: str) -> bool:
        """Synchronous version of remove_item_from_list."""
        if self.is_authenticated:
            try:
                item_id_int = int(item_id)
            except ValueError:
                return False

            result = delete_list_item(item_id_int)
            if result.get('success'):
                self.invalidate_cache()
                return True
            return False
        return False

    async def update_item_note(self, item_id: str, note: str, list_id: str = None) -> bool:
        """Update an item's note."""
        authed, _user_id, client = self._worker_context()
        if authed:
            try:
                item_id_int = int(item_id)
            except ValueError:
                return False

            result = await run.io_bound(update_list_item, item_id_int, {'note': note}, client=client)
            if result and result.get('success'):
                self.invalidate_cache()
                return True
            return False
        return False

    async def update_item_tags(self, item_id: str, tags: List[str], list_id: str = None) -> bool:
        """Update an item's tags."""
        authed, _user_id, client = self._worker_context()
        if authed:
            try:
                item_id_int = int(item_id)
            except ValueError:
                return False

            result = await run.io_bound(update_list_item, item_id_int, {'tags': tags}, client=client)
            if result and result.get('success'):
                self.invalidate_cache()
                return True
            return False
        return False

    def _is_recent_list(self, list_id: str, *, is_authenticated: Optional[bool] = None) -> bool:
        """True when list_id refers to the "Recently Viewed" system list.

        The recent list can be addressed two ways:
          - The literal sentinel 'recent' (used by the add-to-joins / comment
            "Recent activity" tabs, which always worked), and
          - Its numeric Supabase user_lists row id, which is how the /lists page
            sidebar and the Join-Labs picker select it (they key lists by
            str(lst['id'])). That numeric path used to fall through to
            get_list_items(id) on the empty list_items table -> empty list.

        Both must resolve to the recent_items source. We detect the numeric form
        by consulting the cached lists data for a row that is_system AND named the
        canonical 'Recently Viewed' (the DB-stored name_en is the stable key; the
        localized name may differ).
        """
        if list_id == 'recent':
            # Short-circuits BEFORE any storage read, which is why the literal
            # sentinel is the only form safe to resolve off the event loop.
            return True
        if not (self.is_authenticated if is_authenticated is None else is_authenticated):
            return False
        # NOTE: `_get_cached_data()` may read per-user state, so the numeric-id
        # form is NOT safe to resolve in a worker thread. Callers offloading this
        # should pass the literal 'recent' sentinel.
        data = self._get_cached_data()
        lst = data.get('lists', {}).get(str(list_id))
        if not lst or not lst.get('is_system'):
            return False
        return 'Recently Viewed' in (lst.get('name_en'), lst.get('name'))

    async def get_items_in_list(self, list_id: str) -> List[Dict]:
        """Get all items in a list."""
        if self.is_authenticated:
            if self._is_recent_list(list_id):
                return self._format_recent_items(get_recent_items(self.user_id))
            try:
                list_id_int = int(list_id)
            except ValueError:
                return []
            return get_list_items(list_id_int)
        return []

    def get_items_in_list_sync(self, list_id: str, *, client=None,
                               is_authenticated: Optional[bool] = None,
                               user_id: Optional[str] = None) -> List[Dict]:
        """Synchronous version of get_items_in_list.

        All three keyword-only parameters were added 2026-07-30 and default to
        None = unchanged behaviour. Together they make this method safe to run in
        a `run.io_bound` worker thread; passing only `client` is NOT enough.

        `client`: an already-built Supabase client. Both underlying tables
        (`recent_items`, `list_items`) are `TO authenticated` and user-scoped, so
        a worker that fell back to the anonymous client returns ZERO rows -- an
        empty list for a logged-in user rather than an error.

        `is_authenticated` / `user_id`: the caller's auth decision, resolved on
        the event loop. WITHOUT these the method still re-derives auth itself via
        `self.is_authenticated` -> `GlobalAuthState.is_logged_in()` ->
        `safe_user_get(USER_KEY)`, which in a worker thread has no NiceGUI UI
        context and degrades to None (web/safe_storage.py). `is_authenticated`
        then reads False and a signed-in user silently gets empty results while
        the carefully-supplied authenticated `client` is never touched at all.
        """
        authed = self.is_authenticated if is_authenticated is None else is_authenticated
        uid = self.user_id if user_id is None else user_id
        if authed:
            if self._is_recent_list(list_id, is_authenticated=authed):
                return self._format_recent_items(get_recent_items(uid, client=client))
            try:
                list_id_int = int(list_id)
            except ValueError:
                return []
            return get_list_items(list_id_int, client=client)
        return []

    @staticmethod
    def _format_recent_items(rows: List[Dict]) -> List[Dict]:
        """Convert Supabase recent_items rows to the item dict format expected by callers."""
        return [
            {
                'item_id': row.get('sys_id', ''),
                'sys_id': row.get('sys_id', ''),
                'shelfmark': row.get('shelfmark', ''),
                'title': row.get('title', ''),
                'fl_id': row.get('fl_id', ''),
            }
            for row in rows
        ]

    def is_item_in_any_list(self, item_id: str) -> bool:
        """Check if item is in any list.

        Always False for now. This used to read the shared local store with no
        auth check, so every star icon (for signed-in users too) reflected
        what anonymous visitors had saved (sweep C2). A per-page Supabase
        membership set for signed-in users is a follow-up; a per-row Supabase
        call here would run up to 200 blocking reads per results page.
        """
        return False

    def get_item_lists(self, item_id: str) -> List[str]:
        """Get lists containing an item. Always [] (see is_item_in_any_list)."""
        return []

    # === Recent Items ===

    async def add_to_recent(self, sys_id: str, fl_id: str = None, img: str = None):
        """Add item to recently viewed."""
        if self.is_authenticated:
            shelfmark = None
            title = None
            if self.meta_mgr:
                shelfmark, title = self.meta_mgr.get_meta_for_id(sys_id)

            add_recent_item(self.user_id, sys_id, shelfmark, title, fl_id)

    def add_to_recent_sync(self, sys_id: str, fl_id: str = None, img: str = None):
        """Synchronous version of add_to_recent."""
        if self.is_authenticated:
            shelfmark = None
            title = None
            if self.meta_mgr:
                shelfmark, title = self.meta_mgr.get_meta_for_id(sys_id)

            add_recent_item(self.user_id, sys_id, shelfmark, title, fl_id)

    # === Tags ===

    def get_all_tags(self) -> List[str]:
        """Get all tags. Always []: the only source was the shared local store (sweep C2)."""
        return []

    # === Projects ===

    def get_projects(self) -> List[Dict]:
        """Get all projects."""
        if self.is_authenticated:
            data = self._get_cached_data()
            projects = []
            for pid, pdata in data.get('projects', {}).items():
                projects.append({'id': pid, **pdata})
            return projects
        return []

    @staticmethod
    def _next_project_color(projects: List[Dict]) -> str:
        """The first palette colour no project in ``projects`` uses (cycling when all are used)."""
        used_colors = {p.get('color') for p in projects}

        for color in PROJECT_COLORS:
            if color not in used_colors:
                return color

        # Cycle if all colors used
        return PROJECT_COLORS[len(projects) % len(PROJECT_COLORS)]

    def get_next_project_color(self) -> str:
        """Get the next available project color from the palette."""
        return self._next_project_color(self.get_projects())

    def _create_project_blocking(self, user_id: str, name: str, color: Optional[str], *, client) -> Dict:
        """The Supabase part of create_project; runs in a worker thread."""
        if color is None:
            data = self._get_cached_data(client=client, user_id=user_id, is_authenticated=True)
            color = self._next_project_color(list(data.get('projects', {}).values()))
        return sb_create_project(user_id, name, color, client=client)

    async def create_project(self, name: str, color: str = None) -> Optional[str]:
        """Create a new project with auto-assigned color if not specified.

        The colour lookup (a projects read) and the insert run off the loop.
        """
        authed, user_id, client = self._worker_context()
        if authed:
            result = await run.io_bound(self._create_project_blocking, user_id, name, color,
                                        client=client)
            if result and result.get('success'):
                self.invalidate_cache()
                return str(result['project']['id'])
            return None
        return None

    def create_project_sync(self, name: str, color: str = None) -> Optional[str]:
        """Synchronous version of create_project."""
        if color is None:
            color = self.get_next_project_color()

        if self.is_authenticated:
            result = sb_create_project(self.user_id, name, color)
            if result.get('success'):
                self.invalidate_cache()
                return str(result['project']['id'])
            return None
        return None

    async def update_project(self, project_id: str, name: str = None) -> bool:
        """Update a project's name. Color is auto-assigned and not editable."""
        authed, _user_id, client = self._worker_context()
        if authed:
            data = {}
            if name is not None:
                data['name'] = name

            if not data:
                return True  # Nothing to update

            try:
                project_id_int = int(project_id)
            except ValueError:
                return False

            result = await run.io_bound(sb_update_project, project_id_int, data, client=client)
            if result and result.get('success'):
                self.invalidate_cache()
                return True
            return False
        return False

    def _delete_project_blocking(self, user_id: str, project_id: str, project_id_int: int,
                                 delete_lists: bool, *, client) -> Dict:
        """The Supabase part of delete_project (unlink loop included); runs in a worker thread."""
        # If not deleting lists, first unlink them from the project
        if not delete_lists:
            data = self._get_cached_data(client=client, user_id=user_id, is_authenticated=True)
            for list_id, list_data in data.get('lists', {}).items():
                if str(list_data.get('project_id')) == project_id:
                    try:
                        sb_update_list(int(list_id), {'project_id': None}, client=client)
                    except Exception:
                        pass  # Cache operation failed; continue without cached data
        return sb_delete_project(project_id_int, client=client)

    async def delete_project(self, project_id: str, delete_lists: bool = False) -> bool:
        """
        Delete a project.

        Args:
            project_id: Project to delete
            delete_lists: If True, delete lists in project. If False, lists become standalone.
        """
        authed, user_id, client = self._worker_context()
        if authed:
            try:
                project_id_int = int(project_id)
            except ValueError:
                return False

            result = await run.io_bound(self._delete_project_blocking, user_id, project_id,
                                        project_id_int, delete_lists, client=client)
            if result and result.get('success'):
                self.invalidate_cache()
                return True
            return False
        return False

    async def move_list_to_project(self, list_id: str, project_id: Optional[str]) -> bool:
        """
        Move a list to a project (or remove from project if project_id is None).
        The list's display color will change to match the project.
        """
        authed, _user_id, client = self._worker_context()
        if authed:
            try:
                list_id_int = int(list_id)
                project_id_int = int(project_id) if project_id else None
            except ValueError:
                return False

            result = await run.io_bound(sb_update_list, list_id_int, {'project_id': project_id_int},
                                        client=client)
            if result and result.get('success'):
                self.invalidate_cache()
                return True
            return False
        return False

    def get_list_display_color(self, list_id: str, data: Optional[Dict] = None) -> str:
        """
        Get the display color for a list, considering project inheritance.

        Color priority:
        1. System lists (Recently Viewed) -> gray
        2. Lists in projects -> project's color
        3. Standalone lists -> gold

        Args:
            data: Optional pre-fetched data dict (Reviews Round-2 HIGH-2 Path A /
                  CONTEXT.md D-FANOUT-01). When None, falls back to _get_cached_data().
        """
        if data is None:
            data = self._get_cached_data() if self.is_authenticated else self._get_default_data()

        list_data = data.get('lists', {}).get(str(list_id), {})

        # System lists use gray
        if list_data.get('is_system'):
            return '#9E9E9E'

        # Lists in projects inherit project color
        project_id = list_data.get('project_id')
        if project_id:
            project = data.get('projects', {}).get(str(project_id), {})
            if project:
                return project.get('color', '#FFD700')

        # Standalone lists use gold
        return '#FFD700'

    def get_lists_by_project(self, data: Optional[Dict] = None) -> Dict[Optional[str], List[Dict]]:
        """
        Get lists organized by project.

        Args:
            data: Optional pre-fetched data dict (Reviews Round-2 HIGH-2 Path A /
                  CONTEXT.md D-FANOUT-01). When None, falls back to _get_cached_data().

        Returns:
            Dict mapping project_id (or None for standalone) to list of lists
        """
        if data is None:
            data = self._get_cached_data() if self.is_authenticated else self._get_default_data()

        by_project: Dict[Optional[str], List[Dict]] = {None: []}

        # Initialize project groups
        for pid in data.get('projects', {}).keys():
            by_project[pid] = []

        # Assign lists to projects
        for list_id, list_data in data.get('lists', {}).items():
            project_id = list_data.get('project_id')
            if project_id:
                project_id = str(project_id)
                if project_id not in by_project:
                    by_project[project_id] = []
                by_project[project_id].append({'id': list_id, **list_data})
            else:
                by_project[None].append({'id': list_id, **list_data})

        return by_project

    # === Migration ===

    async def migrate_local_to_user(self) -> Dict:
        """Retired: always returns an error dict and never touches any store.

        "Move to account" copied the WHOLE process-wide local store (every
        anonymous visitor's lists, not the caller's) into the caller's
        account and then called clear_all(), which rewrote lists.pkl (sweep
        C2). Kept only so a stale caller gets a harmless error, not an
        AttributeError. It must never read the local store or clear it.
        """
        return {"error": "Moving browser lists to an account is no longer available"}

    def has_local_lists(self) -> bool:
        """Retired with migrate_local_to_user: there are no per-browser lists."""
        return False

    # === Export ===

    def export_list(self, list_id: str, include_metadata: bool = True) -> Optional[Dict]:
        """Export a list to dictionary.

        Always None: the only implementation read the shared local store with
        no auth check (sweep C2). The Excel export at
        /api/export/list/{list_id}/excel builds its own export from Supabase.
        """
        return None

    # === Compatibility Methods ===

    def save(self):
        """No-op. Saving would rewrite the shared lists.pkl (sweep C2)."""
        return None

    def load(self):
        """No-op. Supabase data is fetched fresh on every read."""
        return None

    async def refresh_data(self) -> Dict:
        """Refresh and return data from the backing store.

        Phase 92.2 Reviews Codex-HIGH 2: returns the fetched dict so callers
        (notably async_refresh_ui in web/pages/lists.py) can thread it
        forward without re-fetching. Previously `-> None` and the caller
        had to do a separate `lists_mgr.data` access -- that triggered a
        second fetch on the warm path. Return-type drift is backwards-
        compatible because Python lets `None`-returning callers ignore
        the return value; the only caller that consumes the new return
        is the threaded async_refresh_ui added in Phase 92.2 Task 6 Part D.
        Anonymous callers get the default skeleton (sweep C2). The two reads
        run off the event loop (2026-10-21); no answer from the worker is the
        default skeleton too, as it is for a failed read.
        """
        self.invalidate_cache()
        authed, user_id, client = self._worker_context()
        if authed:
            data = await run.io_bound(self._get_cached_data, client=client, user_id=user_id,
                                      is_authenticated=True)
            if data is not None:
                return data
        return self._get_default_data()


def get_lists_manager(local_mgr: Optional[ListsManager] = None,
                      meta_mgr=None) -> UserListsManager:
    """
    Get the appropriate lists manager based on auth state.

    Args:
        local_mgr: Ignored (signature compatibility; see UserListsManager)
        meta_mgr: Metadata manager for enriching items

    Returns:
        UserListsManager instance
    """
    return UserListsManager(local_mgr, meta_mgr)
