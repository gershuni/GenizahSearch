# -*- coding: utf-8 -*-
"""User list management with offline-first caching.

Phase 123: Extracted from genizah_core.py (v8.3.0 God-File Decomposition).
genizah_core.py retains a permanent same-object re-export shim so all
existing ``from genizah_core import ListsManager`` callers continue working.

Note: Config is imported at module level BEFORE the class body because
ListsManager uses Config.INDEX_DIR (bound at class-definition time).
"""

import logging
import os
import pickle
import re
import time

from genizah_translations import TRANSLATIONS

from shared.config import Config

LOGGER = logging.getLogger("genizah." + __name__)


def _tr(text: str) -> str:
    """Translate text if current language is Hebrew.

    Mirrors genizah_core.tr() — lazy import of CURRENT_LANG inside the
    function body so we always see the live value (Pitfall 2 of Phase 123).
    GUARD-01-safe: the import is function-body-only, not module-level.
    """
    from genizah_core import CURRENT_LANG  # noqa: PLC0415 — intentional lazy; GUARD-01 safe
    if CURRENT_LANG == 'he':
        return TRANSLATIONS.get(text, text)
    return text



class ListsManager:
    """
    Manages personal lists (starred/saved manuscripts) with tags and notes.

    Features:
    - Multiple named lists with colors
    - Built-in "General" default list
    - Built-in "Recently Viewed" auto-populated list
    - Tags and notes per item
    - Export/import functionality
    """

    LISTS_FILE = os.path.join(Config.INDEX_DIR, "lists.pkl")
    MAX_RECENT_ITEMS = 50

    # Default colors for lists
    DEFAULT_COLORS = [
        '#FFD700',  # Gold (default)
        '#4CAF50',  # Green
        '#2196F3',  # Blue
        '#9C27B0',  # Purple
        '#FF5722',  # Deep Orange
        '#00BCD4',  # Cyan
        '#E91E63',  # Pink
        '#795548',  # Brown
        '#607D8B',  # Blue Grey
        '#F44336',  # Red
    ]

    def __init__(self, meta_mgr=None):
        """Initialize the lists manager."""
        self.meta_mgr = meta_mgr
        self.data = self._get_default_data()
        self.load()

    def _get_default_data(self):
        """Return the default data structure."""
        import time
        return {
            'lists': {
                'default': {
                    'name': 'General',
                    'name_en': 'General',
                    'color': '#FFD700',
                    'created': time.time(),
                    'is_default': True,
                    'is_system': False,
                    'project_id': None
                },
                'recent': {
                    'name': 'Recently Viewed',
                    'name_en': 'Recently Viewed',
                    'color': '#9E9E9E',
                    'is_system': True,
                    'max_items': self.MAX_RECENT_ITEMS,
                    'project_id': None
                }
            },
            'projects': {},
            'lists_order': [],
            'projects_order': [],
            'items': {},  # sys_id -> item data
            'recent_items': [],  # ordered list of sys_ids (most recent first)
            'all_tags': []  # for autocomplete
        }

    def load(self):
        """Load lists from file."""
        if os.path.exists(self.LISTS_FILE):
            try:
                with open(self.LISTS_FILE, 'rb') as f:
                    loaded = pickle.load(f)
                    # Merge with defaults to handle new fields
                    defaults = self._get_default_data()
                    for key in defaults:
                        if key not in loaded:
                            loaded[key] = defaults[key]
                    # Ensure system lists exist
                    if 'default' not in loaded['lists']:
                        loaded['lists']['default'] = defaults['lists']['default']
                    if 'recent' not in loaded['lists']:
                        loaded['lists']['recent'] = defaults['lists']['recent']
                    if 'projects' not in loaded:
                        loaded['projects'] = defaults['projects']
                    if 'lists_order' not in loaded:
                        loaded['lists_order'] = defaults['lists_order']
                    if 'projects_order' not in loaded:
                        loaded['projects_order'] = defaults['projects_order']
                    if not loaded.get('lists_order'):
                        loaded['lists_order'] = list(loaded.get('lists', {}).keys())
                    if not loaded.get('projects_order'):
                        loaded['projects_order'] = list(loaded.get('projects', {}).keys())
                    for project_data in loaded.get('projects', {}).values():
                        if 'color' not in project_data:
                            project_data['color'] = self._get_next_project_color(loaded.get('projects', {}))
                    for list_data in loaded.get('lists', {}).values():
                        if 'project_id' not in list_data:
                            list_data['project_id'] = None
                    # Backfill sys_id on stored items (older format used key only)
                    for item_id, item_data in loaded.get('items', {}).items():
                        if isinstance(item_data, dict) and 'sys_id' not in item_data:
                            item_data['sys_id'] = item_id
                    self.data = loaded
            except Exception as e:
                LOGGER.warning(f"Failed to load lists: {e}")
                self.data = self._get_default_data()
        else:
            self.data = self._get_default_data()

    def save(self):
        """Save lists to file."""
        try:
            os.makedirs(Config.INDEX_DIR, exist_ok=True)
            # Create backup before saving (keep last 3 backups)
            if os.path.exists(self.LISTS_FILE):
                import shutil
                for i in range(2, 0, -1):
                    old_backup = f"{self.LISTS_FILE}.bak{i}"
                    new_backup = f"{self.LISTS_FILE}.bak{i+1}"
                    if os.path.exists(old_backup):
                        if os.path.exists(new_backup):
                            os.remove(new_backup)
                        shutil.move(old_backup, new_backup)
                backup_file = f"{self.LISTS_FILE}.bak1"
                shutil.copy2(self.LISTS_FILE, backup_file)
            with open(self.LISTS_FILE, 'wb') as f:
                pickle.dump(self.data, f)
        except Exception as e:
            LOGGER.error(f"Failed to save lists: {e}")

    def clear_all(self):
        """Clear all lists and reset to default state. Used after migration."""
        self.data = self._get_default_data()
        self.save()

    # --- Cloud Sync Integration ---

    def enable_cloud_sync(self, user_id: str, supabase_client=None):
        """Enable cloud sync for the given user (call after login).

        Args:
            user_id: The user's UUID from Supabase auth
            supabase_client: Optional authenticated Supabase client for RLS
        """
        try:
            from lists_sync import get_lists_sync
            sync = get_lists_sync(self)
            sync.set_user(user_id)
            if supabase_client:
                sync.set_client(supabase_client)
            LOGGER.info(f"Cloud sync enabled for user")
        except ImportError:
            LOGGER.debug("lists_sync module not available")
        except Exception as e:
            LOGGER.warning(f"Failed to enable cloud sync: {e}")

    def disable_cloud_sync(self):
        """Disable cloud sync (call on logout)."""
        try:
            from lists_sync import get_lists_sync
            sync = get_lists_sync(self)
            sync.clear_user()
        except Exception:
            pass  # Cloud sync best-effort; offline mode continues

    def sync_from_cloud(self):
        """Pull lists from cloud and merge with local data."""
        try:
            from lists_sync import get_lists_sync
            sync = get_lists_sync(self)
            return sync.sync_from_cloud()
        except ImportError:
            return {'success': False, 'error': 'Cloud sync not available'}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def is_sync_available(self):
        """Check if cloud sync is available (user logged in, network ok)."""
        try:
            from lists_sync import get_lists_sync
            sync = get_lists_sync(self)
            return sync.is_sync_available()
        except ImportError:
            return False
        except Exception:
            return False  # Cannot determine sync state; assume not synced

    @property
    def _last_sync(self):
        """Get timestamp of last sync (for debouncing)."""
        try:
            from lists_sync import get_lists_sync
            sync = get_lists_sync(self)
            return getattr(sync, '_last_sync', 0)
        except Exception:
            return 0  # Count unavailable; return zero

    def sync_to_cloud(self):
        """Push local lists to cloud."""
        try:
            from lists_sync import get_lists_sync
            sync = get_lists_sync(self)
            return sync.sync_to_cloud()
        except ImportError:
            return {'success': False, 'error': 'Cloud sync not available'}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def get_cloud_lists_preview(self):
        """Get preview of cloud lists without syncing (for dialog display)."""
        try:
            from lists_sync import get_lists_sync
            sync = get_lists_sync(self)
            return sync.get_cloud_lists_preview()
        except ImportError:
            return {'success': False, 'lists': [], 'error': 'Cloud sync not available'}
        except Exception as e:
            return {'success': False, 'lists': [], 'error': str(e)}

    def get_local_lists_summary(self):
        """Get summary of local lists for dialog display."""
        lists = []
        for list_id, list_data in self.data.get('lists', {}).items():
            if list_data.get('is_system'):
                continue
            lists.append({
                'id': list_id,
                'name': list_data.get('name', 'Unnamed'),
                'color': list_data.get('color', '#FFD700'),
                'item_count': self._get_list_item_count(list_id)
            })
        return lists

    # --- List Management ---

    def get_all_lists(self, include_recent=True, include_deleted=False):
        """Get all lists sorted alphabetically (system lists have special handling).

        Args:
            include_recent: Include the "Recently Viewed" system list
            include_deleted: Include soft-deleted lists (for trash view)
        """
        lists = []
        list_ids = []
        ordered = [list_id for list_id in self.data.get('lists_order', []) if list_id in self.data['lists']]
        fallback = [list_id for list_id in self.data['lists'] if list_id not in ordered]
        list_ids.extend(ordered)
        list_ids.extend(sorted(fallback, key=lambda list_id: self.data['lists'][list_id].get('name', '')))

        for list_id in list_ids:
            if list_id == 'recent' and not include_recent:
                continue
            list_data = self.data['lists'][list_id]
            # Skip deleted lists unless explicitly requested
            if list_data.get('deleted_at') and not include_deleted:
                continue
            lists.append({
                'id': list_id,
                **list_data,
                'count': self._get_list_item_count(list_id)
            })

        return lists

    def get_deleted_lists(self):
        """Get soft-deleted lists (trash view)."""
        deleted = []
        for list_id, list_data in self.data['lists'].items():
            if list_data.get('deleted_at'):
                deleted.append({
                    'id': list_id,
                    **list_data,
                    'count': self._get_list_item_count(list_id)
                })
        # Sort by deletion time, most recent first
        deleted.sort(key=lambda x: x.get('deleted_at', 0), reverse=True)
        return deleted

    def _get_list_item_count(self, list_id):
        """Get the number of items in a list."""
        if list_id == 'recent':
            return len(self.data.get('recent_items', []))

        count = 0
        for item in self.data['items'].values():
            if list_id in item.get('lists', []):
                count += 1
        return count

    def create_list(self, name, color=None):
        """Create a new list. Returns the list ID."""
        import time
        import uuid

        list_id = f"list_{uuid.uuid4().hex[:8]}"

        if color is None:
            # Pick next available color
            used_colors = {lst.get('color') for lst in self.data['lists'].values()}
            for c in self.DEFAULT_COLORS:
                if c not in used_colors:
                    color = c
                    break
            if color is None:
                color = self.DEFAULT_COLORS[0]

        self.data['lists'][list_id] = {
            'name': name,
            'color': color,
            'created': time.time(),
            'project_id': None
        }
        self.data.setdefault('lists_order', []).append(list_id)
        self.save()
        return list_id

    def update_list(self, list_id, name=None, color=None):
        """Update list properties."""
        if list_id not in self.data['lists']:
            return False

        lst = self.data['lists'][list_id]
        if lst.get('is_system'):
            return False  # Cannot edit system lists

        if name is not None:
            lst['name'] = name
        if color is not None:
            lst['color'] = color

        self.save()
        return True

    def update_list_project(self, list_id, project_id=None):
        """Assign a list to a project (or clear project)."""
        if list_id not in self.data['lists']:
            return False

        lst = self.data['lists'][list_id]
        if lst.get('is_system') or lst.get('is_default'):
            return False

        if project_id and project_id not in self.data.get('projects', {}):
            return False

        lst['project_id'] = project_id
        self.save()
        return True

    def create_project(self, name, color=None):
        """Create a new project. Returns the project ID.

        Args:
            name: Project name.
            color: Optional hex color (e.g. '#4CAF50'). When None, auto-assigned
                from the palette via _get_next_project_color().

        Phase 89 (D-06): the optional ``color`` parameter was added for parity
        with web/user_lists.py:UserListsManager.create_project, which passes
        ``self.local_mgr.create_project(name, color)`` and previously raised
        TypeError when local_mgr was a stock ListsManager. Desktop callers
        (genizah_app.py:12237, 12996) pass ``name`` only and are unaffected.
        """
        import time
        import uuid

        project_id = f"project_{uuid.uuid4().hex[:8]}"
        self.data.setdefault('projects', {})[project_id] = {
            'name': name,
            'created': time.time(),
            'color': color or self._get_next_project_color(),
        }
        self.data.setdefault('projects_order', []).append(project_id)
        self.save()
        return project_id

    def get_projects(self):
        """Get projects sorted by name."""
        projects = []
        project_ids = []
        ordered = [project_id for project_id in self.data.get('projects_order', []) if project_id in self.data.get('projects', {})]
        fallback = [project_id for project_id in self.data.get('projects', {}) if project_id not in ordered]
        project_ids.extend(ordered)
        project_ids.extend(sorted(fallback, key=lambda project_id: self.data['projects'][project_id].get('name', '').lower()))

        for project_id in project_ids:
            data = self.data['projects'][project_id]
            projects.append({'id': project_id, **data})
        return projects

    def update_project(self, project_id, name=None):
        """Update a project's properties."""
        project = self.data.get('projects', {}).get(project_id)
        if not project:
            return False
        if name is not None:
            project['name'] = name
        self.save()
        return True

    def delete_project(self, project_id, delete_lists=False):
        """Delete a project and optionally its lists."""
        if project_id not in self.data.get('projects', {}):
            return False

        if delete_lists:
            list_ids = [
                list_id for list_id, list_data in self.data.get('lists', {}).items()
                if list_data.get('project_id') == project_id
            ]
            for list_id in list_ids:
                self.delete_list(list_id)
        else:
            for list_data in self.data.get('lists', {}).values():
                if list_data.get('project_id') == project_id:
                    list_data['project_id'] = None

        del self.data['projects'][project_id]
        if project_id in self.data.get('projects_order', []):
            self.data['projects_order'].remove(project_id)
        self.save()
        return True

    def _get_next_project_color(self, projects=None):
        source = projects if projects is not None else self.data.get('projects', {})
        used_colors = {
            project.get('color')
            for project in source.values()
            if project.get('color')
        }
        for color in self.DEFAULT_COLORS[1:]:
            if color not in used_colors:
                return color
        return self.DEFAULT_COLORS[0]

    def apply_list_layout(self, list_project_map, list_order, project_order):
        """Apply list ordering and project assignments in one save."""
        for list_id, project_id in list_project_map.items():
            lst = self.data['lists'].get(list_id)
            if not lst or lst.get('is_system') or lst.get('is_default'):
                continue
            if project_id and project_id not in self.data.get('projects', {}):
                continue
            lst['project_id'] = project_id
            if project_id:
                project = self.data.get('projects', {}).get(project_id, {})
                if project.get('color'):
                    lst['color'] = project['color']
            else:
                default_color = self.data.get('lists', {}).get('default', {}).get('color')
                if default_color:
                    lst['color'] = default_color

        self.data['lists_order'] = [list_id for list_id in list_order if list_id in self.data['lists']]
        self.data['projects_order'] = [
            project_id for project_id in project_order if project_id in self.data.get('projects', {})
        ]
        self.save()

    def delete_list(self, list_id, permanent=False):
        """Soft-delete a list (move to trash).

        Args:
            list_id: The list ID to delete
            permanent: If True, permanently delete. Default is soft delete.
        """
        if list_id not in self.data['lists']:
            return False

        lst = self.data['lists'][list_id]
        if lst.get('is_default') or lst.get('is_system'):
            return False  # Cannot delete system lists

        if permanent:
            # Permanent delete - remove list and orphaned items
            items_to_remove = []
            for sys_id, item in self.data['items'].items():
                if list_id in item.get('lists', []):
                    item['lists'].remove(list_id)
                    if not item['lists']:
                        items_to_remove.append(sys_id)
            for sys_id in items_to_remove:
                del self.data['items'][sys_id]
            del self.data['lists'][list_id]
            if list_id in self.data.get('lists_order', []):
                self.data['lists_order'].remove(list_id)
        else:
            # Soft delete - set deleted_at timestamp
            import time
            lst['deleted_at'] = time.time()

        self.save()
        return True

    def restore_list(self, list_id):
        """Restore a soft-deleted list from trash."""
        if list_id not in self.data['lists']:
            return False

        lst = self.data['lists'][list_id]
        if not lst.get('deleted_at'):
            return False  # Not deleted

        del lst['deleted_at']
        self.save()
        return True

    def permanently_delete_list(self, list_id):
        """Permanently delete a list (no recovery)."""
        return self.delete_list(list_id, permanent=True)

    def empty_trash(self):
        """Permanently delete all soft-deleted lists."""
        deleted_lists = [lid for lid, data in self.data['lists'].items() if data.get('deleted_at')]
        count = 0
        for list_id in deleted_lists:
            if self.delete_list(list_id, permanent=True):
                count += 1
        return count

    def duplicate_list(self, list_id, new_name=None):
        """Duplicate a list with all its items."""
        if list_id not in self.data['lists']:
            return None

        original = self.data['lists'][list_id]
        if new_name is None:
            new_name = f"{original.get('name', _tr('List'))} ({_tr('Copy')})"

        new_list_id = self.create_list(new_name, original.get('color'))
        if original.get('project_id'):
            self.update_list_project(new_list_id, original.get('project_id'))

        # Copy items
        for sys_id, item in self.data['items'].items():
            if list_id in item.get('lists', []):
                if new_list_id not in item['lists']:
                    item['lists'].append(new_list_id)

        self.save()
        return new_list_id

    def merge_lists(self, source_list_id, target_list_id, delete_source=True):
        """Merge source list into target list."""
        if source_list_id not in self.data['lists'] or target_list_id not in self.data['lists']:
            return False

        source = self.data['lists'][source_list_id]
        if source.get('is_system'):
            return False  # Cannot merge system lists

        # Move items from source to target
        for sys_id, item in self.data['items'].items():
            if source_list_id in item.get('lists', []):
                if target_list_id not in item['lists']:
                    item['lists'].append(target_list_id)

        if delete_source:
            self.delete_list(source_list_id)
        else:
            self.save()

        return True

    def find_duplicate_lists(self):
        """
        Find all duplicate lists (same name) and return info for resolution.

        Returns:
            List of duplicate groups, each containing:
            {
                'name': str,
                'has_conflict': bool,  # True if different projects
                'lists': [{'id', 'project_id', 'project_name', 'item_count', 'created', 'has_cloud_id'}, ...]
            }
        """
        from collections import defaultdict

        lists_by_name = defaultdict(list)
        for list_id, list_data in self.data.get('lists', {}).items():
            list_name = list_data.get('name', '')
            lists_by_name[list_name].append((list_id, list_data))

        projects = self.data.get('projects', {})
        duplicate_groups = []

        for list_name, lists in lists_by_name.items():
            if len(lists) <= 1:
                continue

            group_info = {'name': list_name, 'lists': []}
            project_ids = set()

            for list_id, list_data in lists:
                project_id = list_data.get('project_id')
                project_ids.add(project_id)

                if list_id == 'recent':
                    item_count = len(self.data.get('recent_items', []))
                else:
                    item_count = sum(1 for item in self.data.get('items', {}).values()
                                    if list_id in item.get('lists', []))

                group_info['lists'].append({
                    'id': list_id,
                    'project_id': project_id,
                    'project_name': projects.get(project_id, {}).get('name') if project_id else None,
                    'item_count': item_count,
                    'created': list_data.get('created', 0),
                    'has_cloud_id': bool(list_data.get('cloud_id'))
                })

            group_info['has_conflict'] = len(project_ids) > 1
            duplicate_groups.append(group_info)

        return duplicate_groups

    def merge_duplicate_group(self, keep_id, duplicate_ids, target_project_id=None):
        """Merge a group of duplicate lists into one."""
        result = {'merged_items': 0, 'deleted_count': 0}

        keeper_data = self.data['lists'].get(keep_id)
        if not keeper_data:
            return result

        list_name = keeper_data.get('name', '')

        if target_project_id is not None:
            keeper_data['project_id'] = target_project_id if target_project_id else None

        for dup_id in duplicate_ids:
            if dup_id == keep_id:
                continue

            dup_data = self.data['lists'].get(dup_id)
            if not dup_data:
                continue

            if list_name == 'Recently Viewed':
                recent_items = self.data.setdefault('recent_items', [])
                for item_id, item in self.data.get('items', {}).items():
                    if dup_id in item.get('lists', []):
                        item['lists'].remove(dup_id)
                        sys_id = item.get('sys_id', item_id)
                        if sys_id and sys_id not in recent_items:
                            recent_items.insert(0, sys_id)
                            result['merged_items'] += 1
                if len(recent_items) > self.MAX_RECENT_ITEMS:
                    self.data['recent_items'] = recent_items[:self.MAX_RECENT_ITEMS]
            else:
                for item_id, item in self.data.get('items', {}).items():
                    if dup_id in item.get('lists', []):
                        item['lists'].remove(dup_id)
                        if keep_id not in item['lists']:
                            item['lists'].append(keep_id)
                            result['merged_items'] += 1

            if dup_id in self.data['lists']:
                del self.data['lists'][dup_id]
                result['deleted_count'] += 1
            if dup_id in self.data.get('lists_order', []):
                self.data['lists_order'].remove(dup_id)

            LOGGER.info(f"Merged duplicate list '{list_name}' ({dup_id}) into {keep_id}")

        self.save()
        return result

    def auto_merge_duplicate_group(self, group):
        """Automatically merge a duplicate group using heuristics."""
        list_name = group['name']
        lists = group['lists']

        if list_name == 'General':
            keep_id = 'default'
        elif list_name == 'Recently Viewed':
            keep_id = 'recent'
        else:
            sorted_lists = sorted(lists, key=lambda x: (
                0 if x['project_id'] else 1,
                0 if not x['has_cloud_id'] else 1,
                x['created']
            ))
            keep_id = sorted_lists[0]['id']

        duplicate_ids = [l['id'] for l in lists if l['id'] != keep_id]
        result = self.merge_duplicate_group(keep_id, duplicate_ids)
        result['keep_id'] = keep_id
        return result

    def restore_project_hierarchy(self):
        """Restore project hierarchy for orphaned lists by color matching."""
        result = {'restored_count': 0}

        projects = self.data.get('projects', {})
        if not projects:
            return result

        color_to_project = {proj.get('color'): pid for pid, proj in projects.items() if proj.get('color')}

        for list_id, list_data in self.data.get('lists', {}).items():
            if list_id in ['default', 'recent'] or list_data.get('is_system'):
                continue
            if list_data.get('project_id') and list_data['project_id'] in projects:
                continue

            list_color = list_data.get('color')
            if list_color and list_color in color_to_project:
                list_data['project_id'] = color_to_project[list_color]
                result['restored_count'] += 1
                LOGGER.info(f"Restored project for list '{list_data.get('name')}' by color match")

        if result['restored_count'] > 0:
            self.save()

        return result

    # --- Item Management ---

    def _build_item_id(self, sys_id, img=None, fl_id=None):
        if img not in (None, ""):
            return f"{sys_id}::img::{img}"
        if fl_id not in (None, ""):
            return f"{sys_id}::fl::{fl_id}"
        return sys_id

    def add_item(self, sys_id, list_id='default', note='', tags=None, source='', fl_id=None, img=None):
        """Add an item to a list. Returns True if added, False if already exists."""
        import time

        if list_id not in self.data['lists']:
            return False

        item_id = self._build_item_id(sys_id, img=img, fl_id=fl_id)

        if item_id in self.data['items']:
            # Item exists, add to list if not already
            item = self.data['items'][item_id]
            if list_id in item.get('lists', []):
                return False  # Already in this list
            item['lists'].append(list_id)
            if fl_id:
                item['fl_id'] = fl_id
            if img not in (None, ""):
                item['img'] = img
            item['modified'] = time.time()
        else:
            # New item
            self.data['items'][item_id] = {
                'sys_id': sys_id,
                'lists': [list_id],
                'tags': tags or [],
                'note': note,
                'source': source,
                'added': time.time(),
                'modified': time.time(),
                'shelfmark_override': None,  # For unidentified items
                'fl_id': fl_id,
                'img': img
            }

        # Update all_tags
        if tags:
            for tag in tags:
                if tag not in self.data['all_tags']:
                    self.data['all_tags'].append(tag)

        self.save()
        return True

    def add_items_bulk(self, items, list_id='default', source='', fl_id_map=None):
        """Add multiple items to a list at once."""
        import time

        if list_id not in self.data['lists']:
            return 0

        added = 0
        for entry in items:
            if isinstance(entry, dict):
                sys_id = entry.get('sys_id')
                fl_id = entry.get('fl_id')
                img = entry.get('img')
            else:
                sys_id = entry
                fl_id = fl_id_map.get(sys_id) if fl_id_map else None
                img = None

            if not sys_id:
                continue

            item_id = self._build_item_id(sys_id, img=img, fl_id=fl_id)

            if item_id in self.data['items']:
                item = self.data['items'][item_id]
                if list_id not in item.get('lists', []):
                    item['lists'].append(list_id)
                    item['modified'] = time.time()
                    added += 1
                if fl_id and item.get('fl_id') != fl_id:
                    item['fl_id'] = fl_id
                    item['modified'] = time.time()
                if img not in (None, "") and item.get('img') != img:
                    item['img'] = img
                    item['modified'] = time.time()
            else:
                self.data['items'][item_id] = {
                    'sys_id': sys_id,
                    'lists': [list_id],
                    'tags': [],
                    'note': '',
                    'source': source,
                    'added': time.time(),
                    'modified': time.time(),
                    'shelfmark_override': None,
                    'fl_id': fl_id,
                    'img': img
                }
                added += 1

        self.save()
        return added

    def update_item(self, item_id, note=None, tags=None, shelfmark_override=None, fl_id=None, img=None):
        """Update an item's properties."""
        import time

        if item_id not in self.data['items']:
            return False

        item = self.data['items'][item_id]

        if note is not None:
            item['note'] = note
        if tags is not None:
            item['tags'] = tags
            # Update all_tags
            for tag in tags:
                if tag not in self.data['all_tags']:
                    self.data['all_tags'].append(tag)
        if shelfmark_override is not None:
            item['shelfmark_override'] = shelfmark_override
        if fl_id is not None:
            item['fl_id'] = fl_id
        if img is not None:
            item['img'] = img

        item['modified'] = time.time()
        self.save()
        return True

    def remove_item_from_list(self, item_id, list_id):
        """Remove an item from a specific list."""
        if item_id not in self.data['items']:
            return False

        item = self.data['items'][item_id]
        if list_id not in item.get('lists', []):
            return False

        item['lists'].remove(list_id)

        # If item has no more lists, remove it entirely
        if not item['lists']:
            del self.data['items'][item_id]

        self.save()
        return True

    def move_items_to_list(self, sys_ids, from_list_id, to_list_id):
        """Move items from one list to another."""
        import time

        for item_id in sys_ids:
            if item_id in self.data['items']:
                item = self.data['items'][item_id]
                if from_list_id in item.get('lists', []):
                    item['lists'].remove(from_list_id)
                if to_list_id not in item.get('lists', []):
                    item['lists'].append(to_list_id)
                item['modified'] = time.time()

        self.save()

    def get_items_in_list(self, list_id):
        """Get all items in a list with their metadata."""
        if list_id == 'recent':
            items = []
            for item_id in self.data.get('recent_items', []):
                item_data = self.data['items'].get(item_id, {})
                items.append({
                    **item_data,
                    'item_id': item_id
                })
                if 'sys_id' not in items[-1] and item_id:
                    items[-1]['sys_id'] = item_id
            return items

        items = []
        for item_id, item_data in self.data['items'].items():
            if list_id in item_data.get('lists', []):
                items.append({
                    **item_data,
                    'item_id': item_id
                })
                if 'sys_id' not in items[-1] and item_id:
                    items[-1]['sys_id'] = item_id
        return items

    def get_item(self, item_id):
        """Get a single item's data."""
        if item_id in self.data['items']:
            item = dict(self.data['items'][item_id])
            if 'sys_id' not in item:
                item['sys_id'] = item_id
            item['item_id'] = item_id
            return item
        return None

    def is_item_in_any_list(self, item_id):
        """Check if an item is in any list (excluding recent)."""
        return item_id in self.data['items']

    def get_item_lists(self, item_id):
        """Get list of lists an item belongs to."""
        if item_id not in self.data['items']:
            return []
        return self.data['items'][item_id].get('lists', [])

    # --- Recently Viewed ---

    def add_to_recent(self, sys_id, fl_id=None, img=None):
        """Add an item to the recently viewed list."""
        import time

        recent = self.data.get('recent_items', [])

        # Remove if already present (we'll add to front)
        item_id = self._build_item_id(sys_id, img=img, fl_id=fl_id)
        if item_id in recent:
            recent.remove(item_id)

        # Add to front
        recent.insert(0, item_id)

        # Trim to max size
        if len(recent) > self.MAX_RECENT_ITEMS:
            recent = recent[:self.MAX_RECENT_ITEMS]

        self.data['recent_items'] = recent

        # Also ensure item exists in items dict for metadata
        if item_id not in self.data['items']:
            self.data['items'][item_id] = {
                'sys_id': sys_id,
                'lists': [],  # Not in any regular list, just recent
                'tags': [],
                'note': '',
                'source': '',
                'added': time.time(),
                'modified': time.time(),
                'shelfmark_override': None,
                'fl_id': fl_id,
                'img': img
            }
        else:
            if fl_id:
                self.data['items'][item_id]['fl_id'] = fl_id
            if img is not None:
                self.data['items'][item_id]['img'] = img

        self.save()

    # --- Tags ---

    def get_all_tags(self):
        """Get all tags for autocomplete."""
        return sorted(self.data.get('all_tags', []))

    def add_tag_to_items(self, sys_ids, tag):
        """Add a tag to multiple items."""
        import time

        for sys_id in sys_ids:
            if sys_id in self.data['items']:
                item = self.data['items'][sys_id]
                if tag not in item.get('tags', []):
                    if 'tags' not in item:
                        item['tags'] = []
                    item['tags'].append(tag)
                    item['modified'] = time.time()

        if tag not in self.data['all_tags']:
            self.data['all_tags'].append(tag)

        self.save()

    # --- Export/Import ---

    def export_list(self, list_id, include_metadata=True, include_snippets=False):
        """Export a list to a dictionary suitable for JSON serialization."""
        if list_id not in self.data['lists']:
            return None

        list_info = self.data['lists'][list_id]
        items = self.get_items_in_list(list_id)

        export_data = {
            'version': 1,
            'list_name': list_info.get('name', ''),
            'list_color': list_info.get('color', ''),
            'exported_at': time.time(),
            'items': []
        }

        for item in items:
            item_export = {
                'sys_id': item.get('sys_id'),
                'fl_id': item.get('fl_id'),
                'img': item.get('img'),
                'tags': item.get('tags', []),
                'note': item.get('note', ''),
                'source': item.get('source', ''),
                'shelfmark_override': item.get('shelfmark_override')
            }

            if include_metadata and self.meta_mgr:
                shelfmark, title = self.meta_mgr.get_meta_for_id(item['sys_id'])
                item_export['shelfmark'] = shelfmark
                item_export['title'] = title

            # Snippets would require access to the search engine - skip for now

            export_data['items'].append(item_export)

        return export_data

    def import_list(self, import_data, list_name_override=None):
        """Import a list from exported data. Returns (list_id, imported_count, unidentified_count)."""
        if not import_data or 'items' not in import_data:
            return None, 0, 0

        list_name = list_name_override or import_data.get('list_name', _tr("Imported List"))
        list_color = import_data.get('list_color')

        list_id = self.create_list(list_name, list_color)

        imported = 0
        unidentified = 0

        for item in import_data['items']:
            sys_id = item.get('sys_id')
            if not sys_id:
                continue

            # Check if this sys_id exists in our database
            is_identified = True
            if self.meta_mgr:
                shelfmark, title = self.meta_mgr.get_meta_for_id(sys_id)
                if not shelfmark or shelfmark == 'Unknown':
                    is_identified = False
                    unidentified += 1

            self.add_item(
                sys_id=sys_id,
                list_id=list_id,
                note=item.get('note', ''),
                tags=item.get('tags', []),
                source=item.get('source', ''),
                fl_id=item.get('fl_id'),
                img=item.get('img')
            )

            # Set shelfmark override for unidentified items
            if not is_identified and item.get('shelfmark'):
                item_id = self._build_item_id(sys_id, img=item.get('img'), fl_id=item.get('fl_id'))
                self.update_item(item_id, shelfmark_override=item.get('shelfmark'))

            imported += 1

        return list_id, imported, unidentified

    # --- Sorting ---

    @staticmethod
    def shelfmark_sort_key(shelfmark):
        """
        Sort key for shelfmarks that handles dots correctly.
        E.g., T-S K1.2 < T-S K1.10 (not lexicographic)
        """
        if not shelfmark:
            return ('', [])

        # Split into parts
        parts = re.split(r'(\d+)', shelfmark)
        result = []
        for part in parts:
            if part.isdigit():
                result.append((0, int(part)))  # Numbers sort first by value
            else:
                result.append((1, part.lower()))  # Strings sort lexicographically
        return result

    def get_items_sorted(self, list_id, sort_by='shelfmark', reverse=False):
        """Get items in a list, sorted by the specified field."""
        items = self.get_items_in_list(list_id)

        # Enrich with metadata
        if self.meta_mgr:
            for item in items:
                sys_id = item['sys_id']
                shelfmark, title = self.meta_mgr.get_meta_for_id(sys_id)
                item['shelfmark'] = item.get('shelfmark_override') or shelfmark or 'Unknown'
                item['title'] = title or ''

        if sort_by == 'shelfmark':
            items.sort(key=lambda x: self.shelfmark_sort_key(x.get('shelfmark', '')), reverse=reverse)
        elif sort_by == 'title':
            items.sort(key=lambda x: x.get('title', '').lower(), reverse=reverse)
        elif sort_by == 'added':
            items.sort(key=lambda x: x.get('added', 0), reverse=not reverse)  # Default newest first
        elif sort_by == 'modified':
            items.sort(key=lambda x: x.get('modified', 0), reverse=not reverse)

        return items

    # --- Copy Info ---

    def get_item_copy_text(self, item_id, format_type='compact'):
        """
        Generate text for copying item info.
        format_type: 'compact', 'detailed', 'with_link'
        """
        if item_id not in self.data['items']:
            return ''

        item = self.data['items'][item_id]
        sys_id = item.get('sys_id', item_id)

        shelfmark = 'Unknown'
        title = ''

        if item.get('shelfmark_override'):
            shelfmark = item['shelfmark_override']
        elif self.meta_mgr:
            shelfmark, title = self.meta_mgr.get_meta_for_id(sys_id)

        if format_type == 'compact':
            if title:
                return f"{shelfmark} - {title}"
            return shelfmark

        elif format_type == 'detailed':
            lines = [f"{_tr('Shelfmark:')} {shelfmark}"]
            if title:
                lines.append(f"{_tr('Title:')} {title}")
            lines.append(f"{_tr('System ID:')} {sys_id}")
            if item.get('note'):
                lines.append(f"{_tr('Note:')} {item['note']}")
            return '\n'.join(lines)

        elif format_type == 'with_link':
            from shared.synthetic_sys_id import is_synthetic_sys_id
            lines = [f"{_tr('Shelfmark:')} {shelfmark}"]
            if title:
                lines.append(f"{_tr('Title:')} {title}")
            lines.append(f"{_tr('System ID:')} {sys_id}")
            # Add Ktiv link — Phase 85 D-06: skip for synthetic sys_ids
            if not is_synthetic_sys_id(sys_id):
                ktiv_url = f"https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/itempage?vid=KTIV&scope=KTIV&docId=PNX_MANUSCRIPTS{sys_id}"
                lines.append(f"{_tr('Link:')} {ktiv_url}")
            return '\n'.join(lines)

        return shelfmark

