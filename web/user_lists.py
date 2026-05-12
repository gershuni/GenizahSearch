# -*- coding: utf-8 -*-
"""
User Lists Manager - Supabase-backed lists management for the web interface.

This module provides a lists manager that:
- Uses Supabase when user is logged in (cloud storage)
- Falls back to local ListsManager when user is not logged in (per-device storage)
- Handles migration of local lists to user account on login

Usage:
    from web.user_lists import get_lists_manager

    lists_mgr = get_lists_manager()
    lists = lists_mgr.get_all_lists()
"""

import time
from typing import Optional, Dict, List

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

from web.auth_state import GlobalAuthState
from web.supabase_client import (
    get_user_lists, create_list as sb_create_list,
    update_list as sb_update_list, delete_list as sb_delete_list,
    get_list_items, add_list_item, update_list_item, delete_list_item,
    get_recent_items, add_recent_item, get_projects, create_project as sb_create_project,
    update_project as sb_update_project, delete_project as sb_delete_project
)
from genizah_core import ListsManager

import logging
LOGGER = logging.getLogger(__name__)

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
    Auth-aware lists manager that wraps both Supabase and local storage.

    When a user is logged in, all operations go through Supabase.
    When not logged in, operations use the local ListsManager (pkl file).
    """

    def __init__(self, local_mgr: Optional[ListsManager] = None, meta_mgr=None):
        """
        Initialize the user lists manager.

        Args:
            local_mgr: Optional local ListsManager for fallback
            meta_mgr: Metadata manager for enriching items
        """
        self.local_mgr = local_mgr
        self.meta_mgr = meta_mgr
        self._cache = None
        self._cache_time = 0
        self._cache_user_id: Optional[str] = None  # 2026-05-12 cross-user fix
        self._cache_ttl = 10  # Cache for 10 seconds

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
        elif self.local_mgr:
            return self.local_mgr.data
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

    def _get_cached_data(self) -> Dict:
        """Get data from cache or fetch from Supabase.

        2026-05-12 cross-user fix: `UserListsManager` is a singleton on
        `AppState` (web/state.py), so multi-tenant requests share the same
        instance. The TTL cache must therefore be keyed by `user_id`
        too — otherwise User B's request within the 10s window returns
        User A's lists.
        """
        now = time.time()
        current_user_id = self.user_id
        if (
            self._cache
            and self._cache_user_id == current_user_id
            and (now - self._cache_time) < self._cache_ttl
        ):
            return self._cache

        # Fetch fresh data
        if self.is_authenticated:
            user_id = current_user_id
            lists = get_user_lists(user_id)
            projects = get_projects(user_id)

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
                    'created': lst.get('created_at')
                }

            for proj in projects:
                proj_id = str(proj['id'])
                data['projects'][proj_id] = {
                    'name': proj.get('name', ''),
                    'color': proj.get('color', '#4CAF50'),
                    'created': proj.get('created_at')
                }

            data['lists_order'] = list(data['lists'].keys())
            data['projects_order'] = list(data['projects'].keys())

            self._cache = data
            self._cache_time = now
            self._cache_user_id = current_user_id  # 2026-05-12 cross-user fix
            return data

        return self._get_default_data()

    def invalidate_cache(self):
        """Invalidate the cache to force refresh."""
        self._cache = None
        self._cache_time = 0
        self._cache_user_id = None  # 2026-05-12 cross-user fix

    # === List Operations ===

    def _get_list_item_count(self, list_id: str) -> int:
        """Get item count for a list."""
        if self.is_authenticated:
            try:
                list_id_int = int(list_id)
                items = get_list_items(list_id_int)
                return len(items)
            except (ValueError, Exception):
                return 0
        elif self.local_mgr:
            return len(self.local_mgr.get_items_in_list(list_id))
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
        elif self.local_mgr:
            return self.local_mgr.get_all_lists(include_recent)
        return []

    async def create_list(self, name: str, color: str = None, project_id: str = None) -> Optional[str]:
        """
        Create a new list, optionally inside a project.

        Args:
            name: List name
            color: Color (ignored if project_id is set - will inherit from project)
            project_id: Optional project to add list to
        """
        if self.is_authenticated:
            # If in a project, get project color
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
            LOGGER.error(f"Failed to create list: {result.get('error')}")
            return None
        elif self.local_mgr:
            list_id = self.local_mgr.create_list(name, color)
            if list_id and project_id:
                self.local_mgr.update_list_project(list_id, project_id)
            return list_id
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
        elif self.local_mgr:
            list_id = self.local_mgr.create_list(name, color)
            if list_id and project_id:
                self.local_mgr.update_list_project(list_id, project_id)
            return list_id
        return None

    async def update_list(self, list_id: str, name: str = None, color: str = None) -> bool:
        """Update a list's properties."""
        if self.is_authenticated:
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

            result = sb_update_list(list_id_int, data)
            if result.get('success'):
                self.invalidate_cache()
                return True
            return False
        elif self.local_mgr:
            return self.local_mgr.update_list(list_id, name, color)
        return False

    async def update_list_project(self, list_id: str, project_id: Optional[str]) -> bool:
        """Move a list to a project (or remove from project if project_id is None)."""
        if self.is_authenticated:
            try:
                list_id_int = int(list_id)
                project_id_int = int(project_id) if project_id else None
            except (ValueError, TypeError):
                return False

            result = sb_update_list(list_id_int, {'project_id': project_id_int})
            if result.get('success'):
                self.invalidate_cache()
                return True
            return False
        elif self.local_mgr:
            return self.local_mgr.update_list_project(list_id, project_id)
        return False

    async def delete_list(self, list_id: str) -> bool:
        """Soft-delete a list (move to trash)."""
        if self.is_authenticated:
            try:
                list_id_int = int(list_id)
            except ValueError:
                return False

            result = sb_delete_list(list_id_int)  # Now does soft delete
            if result.get('success'):
                self.invalidate_cache()
                return True
            return False
        elif self.local_mgr:
            return self.local_mgr.delete_list(list_id)
        return False

    def get_deleted_lists(self) -> List[Dict]:
        """Get soft-deleted lists (trash view)."""
        if self.is_authenticated:
            from web.supabase_client import get_deleted_lists as sb_get_deleted_lists
            return sb_get_deleted_lists(self.user_id)
        elif self.local_mgr:
            return self.local_mgr.get_deleted_lists()
        return []

    async def restore_list(self, list_id: str) -> bool:
        """Restore a soft-deleted list from trash."""
        if self.is_authenticated:
            try:
                list_id_int = int(list_id)
            except ValueError:
                return False

            from web.supabase_client import restore_list as sb_restore_list
            result = sb_restore_list(list_id_int)
            if result.get('success'):
                self.invalidate_cache()
                return True
            return False
        elif self.local_mgr:
            return self.local_mgr.restore_list(list_id)
        return False

    async def permanently_delete_list(self, list_id: str) -> bool:
        """Permanently delete a list (no recovery)."""
        if self.is_authenticated:
            try:
                list_id_int = int(list_id)
            except ValueError:
                return False

            result = sb_delete_list(list_id_int, permanent=True)
            if result.get('success'):
                self.invalidate_cache()
                return True
            return False
        elif self.local_mgr:
            return self.local_mgr.permanently_delete_list(list_id)
        return False

    async def empty_trash(self) -> int:
        """Permanently delete all soft-deleted lists. Returns count deleted."""
        if self.is_authenticated:
            from web.supabase_client import empty_trash as sb_empty_trash
            result = sb_empty_trash(self.user_id)
            if result.get('success'):
                self.invalidate_cache()
                return result.get('deleted_count', 0)
            return 0
        elif self.local_mgr:
            return self.local_mgr.empty_trash()
        return 0

    # === Item Operations ===

    async def add_item(self, sys_id: str, list_id: str = 'default',
                       note: str = '', tags: List[str] = None,
                       source: str = '', fl_id: str = None, img: str = None) -> bool:
        """Add an item to a list."""
        if self.is_authenticated:
            # Find the actual list ID
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

            # Get metadata for the item
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
            if result.get('success'):
                self.invalidate_cache()
                return True
            return False
        elif self.local_mgr:
            return self.local_mgr.add_item(sys_id, list_id, note, tags, source, fl_id, img)
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
        elif self.local_mgr:
            return self.local_mgr.add_item(sys_id, list_id, note, tags, source, fl_id, img)
        return False

    async def remove_item_from_list(self, item_id: str, list_id: str) -> bool:
        """Remove an item from a list."""
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
        elif self.local_mgr:
            return self.local_mgr.remove_item_from_list(item_id, list_id)
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
        elif self.local_mgr:
            return self.local_mgr.remove_item_from_list(item_id, list_id)
        return False

    async def update_item_note(self, item_id: str, note: str, list_id: str = None) -> bool:
        """Update an item's note."""
        if self.is_authenticated:
            try:
                item_id_int = int(item_id)
            except ValueError:
                return False

            result = update_list_item(item_id_int, {'note': note})
            if result.get('success'):
                self.invalidate_cache()
                return True
            return False
        elif self.local_mgr:
            return self.local_mgr.update_item(item_id, note=note)
        return False

    async def update_item_tags(self, item_id: str, tags: List[str], list_id: str = None) -> bool:
        """Update an item's tags."""
        if self.is_authenticated:
            try:
                item_id_int = int(item_id)
            except ValueError:
                return False

            result = update_list_item(item_id_int, {'tags': tags})
            if result.get('success'):
                self.invalidate_cache()
                return True
            return False
        elif self.local_mgr:
            return self.local_mgr.update_item(item_id, tags=tags)
        return False

    async def get_items_in_list(self, list_id: str) -> List[Dict]:
        """Get all items in a list."""
        if self.is_authenticated:
            if list_id == 'recent':
                return self._format_recent_items(get_recent_items(self.user_id))
            try:
                list_id_int = int(list_id)
            except ValueError:
                return []
            return get_list_items(list_id_int)
        elif self.local_mgr:
            return self.local_mgr.get_items_in_list(list_id)
        return []

    def get_items_in_list_sync(self, list_id: str) -> List[Dict]:
        """Synchronous version of get_items_in_list."""
        if self.is_authenticated:
            if list_id == 'recent':
                return self._format_recent_items(get_recent_items(self.user_id))
            try:
                list_id_int = int(list_id)
            except ValueError:
                return []
            return get_list_items(list_id_int)
        elif self.local_mgr:
            return self.local_mgr.get_items_in_list(list_id)
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
        """Check if item is in any list."""
        if self.local_mgr:
            return self.local_mgr.is_item_in_any_list(item_id)
        return False

    def get_item_lists(self, item_id: str) -> List[str]:
        """Get lists containing an item."""
        if self.local_mgr:
            return self.local_mgr.get_item_lists(item_id)
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
        elif self.local_mgr:
            self.local_mgr.add_to_recent(sys_id, fl_id, img)

    def add_to_recent_sync(self, sys_id: str, fl_id: str = None, img: str = None):
        """Synchronous version of add_to_recent."""
        if self.is_authenticated:
            shelfmark = None
            title = None
            if self.meta_mgr:
                shelfmark, title = self.meta_mgr.get_meta_for_id(sys_id)

            add_recent_item(self.user_id, sys_id, shelfmark, title, fl_id)
        elif self.local_mgr:
            self.local_mgr.add_to_recent(sys_id, fl_id, img)

    # === Tags ===

    def get_all_tags(self) -> List[str]:
        """Get all tags."""
        if self.local_mgr:
            return self.local_mgr.get_all_tags()
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
        elif self.local_mgr:
            return self.local_mgr.get_projects()
        return []

    def get_next_project_color(self) -> str:
        """Get the next available project color from the palette."""
        projects = self.get_projects()
        used_colors = {p.get('color') for p in projects}

        for color in PROJECT_COLORS:
            if color not in used_colors:
                return color

        # Cycle if all colors used
        return PROJECT_COLORS[len(projects) % len(PROJECT_COLORS)]

    async def create_project(self, name: str, color: str = None) -> Optional[str]:
        """Create a new project with auto-assigned color if not specified."""
        if color is None:
            color = self.get_next_project_color()

        if self.is_authenticated:
            result = sb_create_project(self.user_id, name, color)
            if result.get('success'):
                self.invalidate_cache()
                return str(result['project']['id'])
            return None
        elif self.local_mgr:
            return self.local_mgr.create_project(name, color)
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
        elif self.local_mgr:
            return self.local_mgr.create_project(name, color)
        return None

    async def update_project(self, project_id: str, name: str = None) -> bool:
        """Update a project's name. Color is auto-assigned and not editable."""
        if self.is_authenticated:
            data = {}
            if name is not None:
                data['name'] = name

            if not data:
                return True  # Nothing to update

            try:
                project_id_int = int(project_id)
            except ValueError:
                return False

            result = sb_update_project(project_id_int, data)
            if result.get('success'):
                self.invalidate_cache()
                return True
            return False
        elif self.local_mgr:
            return self.local_mgr.update_project(project_id, name)
        return False

    async def delete_project(self, project_id: str, delete_lists: bool = False) -> bool:
        """
        Delete a project.

        Args:
            project_id: Project to delete
            delete_lists: If True, delete lists in project. If False, lists become standalone.
        """
        if self.is_authenticated:
            try:
                project_id_int = int(project_id)
            except ValueError:
                return False

            # If not deleting lists, first unlink them from the project
            if not delete_lists:
                data = self._get_cached_data()
                for list_id, list_data in data.get('lists', {}).items():
                    if str(list_data.get('project_id')) == project_id:
                        try:
                            sb_update_list(int(list_id), {'project_id': None})
                        except Exception:
                            pass  # Cache operation failed; continue without cached data

            result = sb_delete_project(project_id_int)
            if result.get('success'):
                self.invalidate_cache()
                return True
            return False
        elif self.local_mgr:
            return self.local_mgr.delete_project(project_id, delete_lists)
        return False

    async def move_list_to_project(self, list_id: str, project_id: Optional[str]) -> bool:
        """
        Move a list to a project (or remove from project if project_id is None).
        The list's display color will change to match the project.
        """
        if self.is_authenticated:
            try:
                list_id_int = int(list_id)
                project_id_int = int(project_id) if project_id else None
            except ValueError:
                return False

            result = sb_update_list(list_id_int, {'project_id': project_id_int})
            if result.get('success'):
                self.invalidate_cache()
                return True
            return False
        elif self.local_mgr:
            return self.local_mgr.update_list_project(list_id, project_id)
        return False

    def get_list_display_color(self, list_id: str) -> str:
        """
        Get the display color for a list, considering project inheritance.

        Color priority:
        1. System lists (Recently Viewed) -> gray
        2. Lists in projects -> project's color
        3. Standalone lists -> gold
        """
        data = self._get_cached_data() if self.is_authenticated else (
            self.local_mgr.data if self.local_mgr else self._get_default_data()
        )

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

    def get_lists_by_project(self) -> Dict[Optional[str], List[Dict]]:
        """
        Get lists organized by project.

        Returns:
            Dict mapping project_id (or None for standalone) to list of lists
        """
        data = self._get_cached_data() if self.is_authenticated else (
            self.local_mgr.data if self.local_mgr else self._get_default_data()
        )

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
        """
        Migrate local lists to user account.
        Should be called after user logs in if they have local lists.
        """
        if not self.is_authenticated or not self.local_mgr:
            return {"error": "Not authenticated or no local lists"}

        local_data = self.local_mgr.data
        migrated_lists = 0
        migrated_items = 0

        # Migrate each list
        for list_id, list_data in local_data.get('lists', {}).items():
            if list_data.get('is_system'):
                continue

            # Create the list in Supabase
            result = sb_create_list(
                self.user_id,
                list_data.get('name', 'Imported List'),
                name_en=list_data.get('name_en'),
                color=list_data.get('color', '#FFD700'),
                is_default=list_data.get('is_default', False)
            )

            if result.get('success'):
                new_list_id = result['list']['id']
                migrated_lists += 1

                # Migrate items in this list
                items = self.local_mgr.get_items_in_list(list_id)
                for item in items:
                    item_result = add_list_item(
                        new_list_id,
                        item.get('sys_id'),
                        shelfmark=item.get('shelfmark'),
                        title=item.get('title'),
                        fl_id=item.get('fl_id'),
                        note=item.get('note', ''),
                        tags=item.get('tags', [])
                    )
                    if item_result.get('success'):
                        migrated_items += 1

        # Clear local lists after successful migration
        if migrated_lists > 0:
            self.local_mgr.clear_all()
            self.invalidate_cache()

        return {
            "success": True,
            "migrated_lists": migrated_lists,
            "migrated_items": migrated_items
        }

    def has_local_lists(self) -> bool:
        """Check if there are local lists that could be migrated."""
        if not self.local_mgr:
            return False

        data = self.local_mgr.data

        # Check for user-created lists
        for list_id, list_data in data.get('lists', {}).items():
            if not list_data.get('is_system') and not list_data.get('is_default'):
                return True

        # Check for items
        if data.get('items'):
            return True

        return False

    # === Export ===

    def export_list(self, list_id: str, include_metadata: bool = True) -> Optional[Dict]:
        """Export a list to dictionary."""
        if self.local_mgr:
            return self.local_mgr.export_list(list_id, include_metadata)
        return None

    # === Compatibility Methods ===

    def save(self):
        """Save data - for local manager compatibility."""
        if self.local_mgr:
            self.local_mgr.save()

    def load(self):
        """Load data - for local manager compatibility."""
        if self.local_mgr:
            self.local_mgr.load()

    async def refresh_data(self):
        """Refresh data from Supabase."""
        self.invalidate_cache()
        if self.is_authenticated:
            self._get_cached_data()


def get_lists_manager(local_mgr: Optional[ListsManager] = None,
                      meta_mgr=None) -> UserListsManager:
    """
    Get the appropriate lists manager based on auth state.

    Args:
        local_mgr: Local ListsManager instance
        meta_mgr: Metadata manager for enriching items

    Returns:
        UserListsManager instance
    """
    return UserListsManager(local_mgr, meta_mgr)
