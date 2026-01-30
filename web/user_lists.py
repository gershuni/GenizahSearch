# -*- coding: utf-8 -*-
"""
User Lists Manager - Auth-aware lists management for the web interface.

This module provides a lists manager that:
- Uses the backend API when user is logged in (per-user storage)
- Falls back to local ListsManager when user is not logged in (per-device storage)
- Handles migration of local lists to user account on login

Usage:
    from web.user_lists import get_lists_manager

    lists_mgr = get_lists_manager()
    lists = lists_mgr.get_all_lists()
"""

import time
import httpx
from typing import Optional, Dict, List, Any
from nicegui import app
from web.auth_state import GlobalAuthState, api_call, get_api_base
from genizah_core import ListsManager

import logging
LOGGER = logging.getLogger(__name__)


class UserListsManager:
    """
    Auth-aware lists manager that wraps both API calls and local storage.

    When a user is logged in, all operations go through the backend API.
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
        self._api_data_cache = None
        self._cache_time = 0
        self._cache_ttl = 5  # Cache for 5 seconds

    @property
    def is_authenticated(self) -> bool:
        """Check if user is logged in."""
        return GlobalAuthState.is_logged_in()

    @property
    def user_id(self) -> Optional[int]:
        """Get current user ID if logged in."""
        user = GlobalAuthState.get_user()
        return user.get('id') if user else None

    @property
    def data(self) -> Dict:
        """
        Get lists data - for compatibility with existing code.
        This property provides direct access to data structure.
        """
        if self.is_authenticated:
            return self._get_api_data_sync()
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
                },
                'recent': {
                    'name': 'Recently Viewed',
                    'name_en': 'Recently Viewed',
                    'color': '#9E9E9E',
                    'is_system': True,
                    'max_items': 50,
                    'project_id': None
                }
            },
            'projects': {},
            'lists_order': ['default', 'recent'],
            'projects_order': [],
            'items': {},
            'recent_items': [],
            'all_tags': []
        }

    def _get_api_data_sync(self) -> Dict:
        """
        Get data from API cache.
        Uses cached data if available and fresh.
        """
        now = time.time()
        cache_age = now - self._cache_time if self._cache_time else float('inf')
        print(f"[DEBUG] _get_api_data_sync: cache exists={self._api_data_cache is not None}, cache_age={cache_age:.1f}s, ttl={self._cache_ttl}s")
        if self._api_data_cache and cache_age < self._cache_ttl:
            print(f"[DEBUG] _get_api_data_sync: returning cached data with {len(self._api_data_cache.get('lists', {}))} lists")
            return self._api_data_cache

        # Return cached data or default - actual refresh happens asynchronously
        result = self._api_data_cache or self._get_default_data()
        print(f"[DEBUG] _get_api_data_sync: returning {'cached' if self._api_data_cache else 'default'} data with {len(result.get('lists', {}))} lists")
        return result

    async def refresh_data(self):
        """Refresh data from API."""
        if not self.is_authenticated:
            return

        result = await api_call("GET", "/lists/")
        if "error" not in result:
            self._api_data_cache = self._transform_api_response(result)
            self._cache_time = time.time()

    def _transform_api_response(self, response: Dict) -> Dict:
        """Transform API response to match local data structure."""
        data = self._get_default_data()

        # Transform lists
        data['lists'] = {}
        for lst in response.get('lists', []):
            list_id = lst.get('id', 'default')
            data['lists'][list_id] = {
                'name': lst.get('name', ''),
                'name_en': lst.get('name_en', ''),
                'color': lst.get('color', '#FFD700'),
                'is_default': lst.get('is_default', False),
                'is_system': lst.get('is_system', False),
                'project_id': lst.get('project_id'),
                'created': lst.get('created')
            }

        # Ensure default lists exist
        if not any(lst.get('is_default') for lst in data['lists'].values()):
            data['lists']['default'] = {
                'name': 'General',
                'name_en': 'General',
                'color': '#FFD700',
                'is_default': True,
                'is_system': False,
                'project_id': None
            }

        # Transform projects
        data['projects'] = {}
        for proj in response.get('projects', []):
            data['projects'][str(proj['id'])] = {
                'name': proj.get('name', ''),
                'color': proj.get('color', '#4CAF50'),
                'created': proj.get('created')
            }

        data['lists_order'] = response.get('lists_order', list(data['lists'].keys()))
        data['projects_order'] = [str(p) for p in response.get('projects_order', [])]
        data['all_tags'] = response.get('all_tags', [])

        return data

    def invalidate_cache(self):
        """Invalidate the API cache to force refresh."""
        self._cache_time = 0

    # === List Operations ===

    def get_all_lists(self, include_recent: bool = True) -> List[Dict]:
        """Get all lists sorted by order."""
        if self.is_authenticated:
            data = self._get_api_data_sync()
            lists = []
            for list_id in data.get('lists_order', []):
                if list_id not in data.get('lists', {}):
                    continue
                if list_id == 'recent' and not include_recent:
                    continue
                lst = data['lists'][list_id]
                lists.append({
                    'id': list_id,
                    **lst,
                    'count': self._get_list_item_count(list_id)
                })
            return lists
        elif self.local_mgr:
            return self.local_mgr.get_all_lists(include_recent)
        return []

    def _get_list_item_count(self, list_id: str) -> int:
        """Get item count for a list."""
        # For now return 0 - actual counts come from API
        # This is a performance optimization to avoid loading items
        return 0

    async def create_list(self, name: str, color: str = None) -> Optional[str]:
        """Create a new list (async for authenticated users)."""
        print(f"[DEBUG] UserListsManager.create_list: name={name}, color={color}, is_authenticated={self.is_authenticated}")
        if self.is_authenticated:
            result = await api_call("POST", "/lists/", {
                "name": name,
                "color": color
            })
            print(f"[DEBUG] API response: {result}")
            if "error" not in result:
                self.invalidate_cache()
                list_id = result.get('id')
                print(f"[DEBUG] Returning list_id={list_id}")
                return list_id
            print(f"[DEBUG] API returned error: {result.get('error')}")
            return None
        elif self.local_mgr:
            result = self.local_mgr.create_list(name, color)
            print(f"[DEBUG] Local manager returned: {result}")
            return result
        print(f"[DEBUG] No auth and no local_mgr, returning None")
        return None

    def create_list_sync(self, name: str, color: str = None) -> Optional[str]:
        """Synchronous version of create_list - uses local manager only."""
        if self.local_mgr:
            return self.local_mgr.create_list(name, color)
        return None

    async def update_list(self, list_id: str, name: str = None, color: str = None) -> bool:
        """Update a list's properties."""
        if self.is_authenticated:
            data = {}
            if name is not None:
                data['name'] = name
            if color is not None:
                data['color'] = color

            try:
                list_id_int = int(list_id)
            except ValueError:
                return False

            result = await api_call("PUT", f"/lists/{list_id_int}", data)
            if "error" not in result:
                self.invalidate_cache()
                return True
            return False
        elif self.local_mgr:
            return self.local_mgr.update_list(list_id, name, color)
        return False

    async def delete_list(self, list_id: str) -> bool:
        """Delete a list."""
        if self.is_authenticated:
            try:
                list_id_int = int(list_id)
            except ValueError:
                return False

            result = await api_call("DELETE", f"/lists/{list_id_int}")
            if "error" not in result:
                self.invalidate_cache()
                return True
            return False
        elif self.local_mgr:
            return self.local_mgr.delete_list(list_id)
        return False

    # === Item Operations ===

    async def add_item(self, sys_id: str, list_id: str = 'default',
                       note: str = '', tags: List[str] = None,
                       source: str = '', fl_id: str = None, img: str = None) -> bool:
        """Add an item to a list."""
        if self.is_authenticated:
            try:
                list_id_int = int(list_id)
            except ValueError:
                # For 'default' or other string IDs, we need to find the actual ID
                if list_id == 'default':
                    data = self._get_api_data_sync()
                    for lid, ldata in data.get('lists', {}).items():
                        if ldata.get('is_default'):
                            try:
                                list_id_int = int(lid)
                                break
                            except ValueError:
                                continue
                    else:
                        return False
                else:
                    return False

            # Get metadata for the item
            shelfmark = None
            title = None
            if self.meta_mgr:
                shelfmark, title = self.meta_mgr.get_meta_for_id(sys_id)

            result = await api_call("POST", f"/lists/{list_id_int}/items", {
                "sys_id": sys_id,
                "shelfmark": shelfmark,
                "title": title,
                "fl_id": fl_id,
                "note": note,
                "tags": tags or []
            })
            if "error" not in result:
                self.invalidate_cache()
                return True
            return False
        elif self.local_mgr:
            return self.local_mgr.add_item(sys_id, list_id, note, tags, source, fl_id, img)
        return False

    def add_item_sync(self, sys_id: str, list_id: str = 'default',
                      note: str = '', tags: List[str] = None,
                      source: str = '', fl_id: str = None, img: str = None) -> bool:
        """Synchronous version of add_item - uses local manager only."""
        if self.local_mgr:
            return self.local_mgr.add_item(sys_id, list_id, note, tags, source, fl_id, img)
        return False

    async def remove_item_from_list(self, item_id: str, list_id: str) -> bool:
        """Remove an item from a list."""
        if self.is_authenticated:
            try:
                list_id_int = int(list_id)
                item_id_int = int(item_id)
            except ValueError:
                return False

            result = await api_call("DELETE", f"/lists/{list_id_int}/items/{item_id_int}")
            if "error" not in result:
                self.invalidate_cache()
                return True
            return False
        elif self.local_mgr:
            return self.local_mgr.remove_item_from_list(item_id, list_id)
        return False

    def remove_item_from_list_sync(self, item_id: str, list_id: str) -> bool:
        """Synchronous version of remove_item_from_list."""
        if self.is_authenticated:
            LOGGER.warning("remove_item_from_list_sync called in authenticated mode")
            return False
        elif self.local_mgr:
            return self.local_mgr.remove_item_from_list(item_id, list_id)
        return False

    async def update_item_note(self, item_id: str, note: str, list_id: str = None) -> bool:
        """Update an item's note."""
        if self.is_authenticated:
            if not list_id:
                # Need to find which list the item is in
                return False
            try:
                list_id_int = int(list_id)
                item_id_int = int(item_id)
            except ValueError:
                return False

            result = await api_call("PUT", f"/lists/{list_id_int}/items/{item_id_int}", {
                "note": note
            })
            if "error" not in result:
                self.invalidate_cache()
                return True
            return False
        elif self.local_mgr:
            return self.local_mgr.update_item(item_id, note=note)
        return False

    async def update_item_tags(self, item_id: str, tags: List[str], list_id: str = None) -> bool:
        """Update an item's tags."""
        if self.is_authenticated:
            if not list_id:
                return False
            try:
                list_id_int = int(list_id)
                item_id_int = int(item_id)
            except ValueError:
                return False

            result = await api_call("PUT", f"/lists/{list_id_int}/items/{item_id_int}", {
                "tags": tags
            })
            if "error" not in result:
                self.invalidate_cache()
                return True
            return False
        elif self.local_mgr:
            return self.local_mgr.update_item(item_id, tags=tags)
        return False

    async def get_items_in_list(self, list_id: str) -> List[Dict]:
        """Get all items in a list."""
        if self.is_authenticated:
            try:
                list_id_int = int(list_id)
            except ValueError:
                return []

            result = await api_call("GET", f"/lists/{list_id_int}/items")
            if "error" not in result:
                return result
            return []
        elif self.local_mgr:
            return self.local_mgr.get_items_in_list(list_id)
        return []

    def get_items_in_list_sync(self, list_id: str) -> List[Dict]:
        """Synchronous version - uses sync API call for authenticated users, local manager otherwise."""
        if self.is_authenticated:
            try:
                list_id_int = int(list_id)
            except ValueError:
                return []

            # Make synchronous API call
            try:
                base_url = get_api_base()
                headers = GlobalAuthState.get_headers()
                with httpx.Client(timeout=10.0) as client:
                    response = client.get(
                        f"{base_url}/lists/{list_id_int}/items",
                        headers=headers
                    )
                    if response.status_code == 200:
                        return response.json()
            except Exception as e:
                LOGGER.warning(f"Sync API call failed for list {list_id}: {e}")
            return []
        elif self.local_mgr:
            return self.local_mgr.get_items_in_list(list_id)
        return []

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

            await api_call("POST", "/lists/recent/items", {
                "sys_id": sys_id,
                "shelfmark": shelfmark,
                "title": title,
                "fl_id": fl_id
            })
        elif self.local_mgr:
            self.local_mgr.add_to_recent(sys_id, fl_id, img)

    def add_to_recent_sync(self, sys_id: str, fl_id: str = None, img: str = None):
        """Synchronous version of add_to_recent."""
        if self.local_mgr:
            self.local_mgr.add_to_recent(sys_id, fl_id, img)

    # === Tags ===

    def get_all_tags(self) -> List[str]:
        """Get all tags."""
        if self.is_authenticated:
            data = self._get_api_data_sync()
            return data.get('all_tags', [])
        elif self.local_mgr:
            return self.local_mgr.get_all_tags()
        return []

    # === Projects ===

    def get_projects(self) -> List[Dict]:
        """Get all projects."""
        if self.is_authenticated:
            data = self._get_api_data_sync()
            projects = []
            for pid in data.get('projects_order', []):
                if pid in data.get('projects', {}):
                    projects.append({'id': pid, **data['projects'][pid]})
            return projects
        elif self.local_mgr:
            return self.local_mgr.get_projects()
        return []

    # === Migration ===

    async def migrate_local_to_user(self) -> Dict:
        """
        Migrate local lists to user account.
        Should be called after user logs in if they have local lists.

        Returns migration result with counts.
        """
        if not self.is_authenticated or not self.local_mgr:
            return {"error": "Not authenticated or no local lists"}

        local_data = self.local_mgr.data

        # Prepare migration data
        lists_to_migrate = []
        for list_id, list_data in local_data.get('lists', {}).items():
            if list_data.get('is_system'):
                continue  # Skip system lists

            items = self.local_mgr.get_items_in_list(list_id)
            items_data = []
            for item in items:
                items_data.append({
                    "sys_id": item.get('sys_id'),
                    "shelfmark": item.get('shelfmark'),
                    "title": item.get('title'),
                    "fl_id": item.get('fl_id'),
                    "note": item.get('note', ''),
                    "tags": item.get('tags', [])
                })

            lists_to_migrate.append({
                "name": list_data.get('name', 'Imported List'),
                "name_en": list_data.get('name_en'),
                "color": list_data.get('color', '#FFD700'),
                "is_default": list_data.get('is_default', False),
                "project_id": list_data.get('project_id'),
                "items": items_data
            })

        # Prepare projects
        projects_to_migrate = []
        for proj_id, proj_data in local_data.get('projects', {}).items():
            projects_to_migrate.append({
                "name": proj_data.get('name', 'Project'),
                "color": proj_data.get('color', '#4CAF50')
            })

        # Prepare recent items
        recent_to_migrate = []
        for sys_id in local_data.get('recent_items', [])[:50]:
            item = local_data.get('items', {}).get(sys_id, {})
            recent_to_migrate.append({
                "sys_id": item.get('sys_id', sys_id),
                "shelfmark": item.get('shelfmark'),
                "title": item.get('title'),
                "fl_id": item.get('fl_id')
            })

        # Call migration API
        result = await api_call("POST", "/lists/migrate", {
            "lists": lists_to_migrate,
            "projects": projects_to_migrate,
            "recent_items": recent_to_migrate
        })

        if "error" not in result:
            print(f"[DEBUG] Migration successful, invalidating cache")
            self.invalidate_cache()
            # Clear local lists after successful migration
            if self.local_mgr:
                print(f"[DEBUG] Calling local_mgr.clear_all()")
                self.local_mgr.clear_all()
                print(f"[DEBUG] After clear_all, local lists: {list(self.local_mgr.data.get('lists', {}).keys())}")
                print(f"[DEBUG] After clear_all, items count: {len(self.local_mgr.data.get('items', {}))}")
        else:
            print(f"[DEBUG] Migration returned error: {result}")

        return result

    def has_local_lists(self) -> bool:
        """Check if there are local lists that could be migrated."""
        print(f"[DEBUG] has_local_lists called, local_mgr={self.local_mgr}")
        if not self.local_mgr:
            return False

        data = self.local_mgr.data
        print(f"[DEBUG] has_local_lists: lists={list(data.get('lists', {}).keys())}")
        print(f"[DEBUG] has_local_lists: items count={len(data.get('items', {}))}")

        # Check for user-created lists (not just default/system)
        for list_id, list_data in data.get('lists', {}).items():
            if not list_data.get('is_system') and not list_data.get('is_default'):
                print(f"[DEBUG] has_local_lists: found user list '{list_id}' -> returning True")
                return True

        # Check for items in any list
        items = data.get('items', {})
        if items:
            print(f"[DEBUG] has_local_lists: checking {len(items)} items...")
            for item_id, item_data in items.items():
                print(f"[DEBUG]   item '{item_id}': {item_data}")
                if item_data.get('lists'):
                    print(f"[DEBUG] has_local_lists: found item '{item_id}' in lists -> returning True")
                    return True
            # If there are items but none have 'lists' attribute, still consider them as local data
            print(f"[DEBUG] has_local_lists: {len(items)} items exist -> returning True")
            return True

        print(f"[DEBUG] has_local_lists: no user lists or items -> returning False")
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
