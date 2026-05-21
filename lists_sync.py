# -*- coding: utf-8 -*-
"""
Lists Cloud Sync Module

Provides bidirectional sync between local ListsManager (pickle file)
and Supabase cloud storage. Enables cross-device sync of user lists
between the desktop app and web app.

Part of Phase 5: Desktop App Supabase Migration
"""
import logging
import time
from typing import Optional, Dict, Any

from shared.local_sys_id import is_local_sys_id

logger = logging.getLogger(__name__)

# Try to import Supabase
try:
    from supabase import create_client, Client
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False
    Client = None

# Configuration - centralized via provider
from shared.supabase_provider import get_url, get_anon_key
SUPABASE_URL = get_url()
SUPABASE_ANON_KEY = get_anon_key()


class ListsCloudSync:
    """
    Handles synchronization between local ListsManager and Supabase.

    Sync strategy:
    - On login: Pull cloud lists and merge with local
    - On list/item changes: Push to cloud if logged in
    - Conflict resolution: Last-modified wins

    Usage:
        sync = ListsCloudSync(lists_manager)
        sync.set_user(user_id)  # Call after login
        sync.sync_from_cloud()  # Pull cloud data
        sync.sync_to_cloud()    # Push local data
    """

    def __init__(self, lists_manager=None):
        """Initialize the sync manager."""
        self.lists_manager = lists_manager
        self._client: Optional[Client] = None
        self._user_id: Optional[str] = None
        self._last_sync: float = 0
        self._sync_in_progress = False
        self._external_client = None  # Can be set to use an authenticated client

    def set_client(self, client: Client):
        """Set an external authenticated client (from corrections system)."""
        self._external_client = client
        logger.debug("External authenticated client set for lists sync")

    def _get_client(self) -> Optional[Client]:
        """Get Supabase client - preferring the authenticated external client."""
        # Prefer external authenticated client
        if self._external_client:
            return self._external_client

        if not SUPABASE_AVAILABLE or not SUPABASE_ANON_KEY:
            return None

        if self._client is None:
            try:
                self._client = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
            except Exception as e:
                logger.error(f"Failed to create Supabase client: {e}")
                return None
        return self._client

    def set_user(self, user_id: str):
        """Set the current user ID (UUID from Supabase auth)."""
        self._user_id = user_id

    def clear_user(self):
        """Clear user ID (on logout)."""
        self._user_id = None

    def is_sync_available(self) -> bool:
        """Check if cloud sync is available."""
        available = (
            SUPABASE_AVAILABLE and
            bool(SUPABASE_ANON_KEY) and
            bool(self._user_id) and
            self.lists_manager is not None
        )
        if not available:
            logger.debug(f"Sync not available: SUPABASE_AVAILABLE={SUPABASE_AVAILABLE}, "
                        f"ANON_KEY_SET={bool(SUPABASE_ANON_KEY)}, "
                        f"user_id={self._user_id is not None}, "
                        f"lists_manager={self.lists_manager is not None}")
        return available

    def get_cloud_lists_preview(self) -> Dict[str, Any]:
        """
        Get a preview of cloud lists without syncing.
        Used to show user what will be synced before they decide.

        Returns:
            Dict with 'success', 'lists' (list of {name, color, item_count}), 'error'
        """
        logger.debug(f"get_cloud_lists_preview called, user_id={self._user_id}")
        if not self.is_sync_available():
            logger.warning("Cloud lists preview: sync not available")
            return {'success': False, 'lists': [], 'error': 'Sync not available'}

        try:
            client = self._get_client()
            if not client:
                return {'success': False, 'lists': [], 'error': 'No Supabase client'}

            # Fetch user's lists from cloud
            logger.info(f"Querying user_lists for user_id={self._user_id}")
            lists_response = client.table('user_lists').select('*').eq(
                'user_id', self._user_id
            ).execute()
            logger.info(f"Got {len(lists_response.data or [])} lists from Supabase")

            cloud_lists = []
            for lst in lists_response.data or []:
                # Get item count for this list
                items_response = client.table('list_items').select('id').eq(
                    'list_id', lst['id']
                ).execute()
                item_count = len(items_response.data or [])

                cloud_lists.append({
                    'id': lst['id'],
                    'name': lst.get('name', 'Unnamed'),
                    'color': lst.get('color', '#FFD700'),
                    'item_count': item_count,
                    'is_system': lst.get('is_system', False)
                })

            return {'success': True, 'lists': cloud_lists, 'error': None}

        except Exception as e:
            logger.error(f"Error getting cloud lists preview: {e}")
            return {'success': False, 'lists': [], 'error': str(e)}

    def _backup_local_data(self) -> bool:
        """Create a backup of local data before sync. Returns True if backup succeeded."""
        if not self.lists_manager:
            return False
        try:
            # Force a save with backup (the save() method creates rotating backups)
            self.lists_manager.save()
            logger.info("Created backup before sync")
            return True
        except Exception as e:
            logger.error(f"Failed to create backup: {e}")
            return False

    def _dedupe_and_index_local_items(self):
        """
        Merge local items that share a cloud_id (pre-existing duplicates from
        earlier sync bugs) and return lookup maps for matching cloud rows to
        local item keys.

        Why duplicates can exist: desktop search adds items keyed as
        ``{sys_id}::img::{img}`` while cloud only stores ``sys_id`` + ``fl_id``.
        sync_from_cloud used to compute ``{sys_id}::fl::{fl_id}`` and insert a
        second local item alongside the original ``::img::`` one.
        """
        items = self.lists_manager.data.get('items', {})

        cloud_id_to_keys: Dict[str, list] = {}
        for k, v in items.items():
            cid = v.get('cloud_id')
            if cid:
                cloud_id_to_keys.setdefault(cid, []).append(k)

        # Merge any pre-existing duplicates: keep most-recently-modified key,
        # transfer list memberships into it, drop the rest.
        for cid, keys in cloud_id_to_keys.items():
            if len(keys) < 2:
                continue
            keys_sorted = sorted(
                keys,
                key=lambda k: items[k].get('modified', items[k].get('added', 0)),
                reverse=True,
            )
            canonical = keys_sorted[0]
            for other in keys_sorted[1:]:
                for lst in items[other].get('lists', []):
                    if lst not in items[canonical]['lists']:
                        items[canonical]['lists'].append(lst)
                del items[other]
            logger.info(
                f"Merged {len(keys) - 1} duplicate local item(s) sharing cloud_id {cid}"
            )

        cloud_id_to_item_id: Dict[str, str] = {}
        sys_id_fl_id_to_item_id: Dict[tuple, str] = {}
        for k, v in items.items():
            cid = v.get('cloud_id')
            if cid:
                cloud_id_to_item_id[cid] = k
            sys_id = v.get('sys_id')
            if sys_id:
                sys_id_fl_id_to_item_id[(sys_id, v.get('fl_id'))] = k

        return cloud_id_to_item_id, sys_id_fl_id_to_item_id

    def sync_from_cloud(self) -> Dict[str, Any]:
        """
        Pull lists and items from Supabase and merge with local data.

        IMPORTANT: This only ADDS data from cloud, never removes local data.
        If cloud is empty, local data is preserved unchanged.

        Returns:
            Dict with 'success', 'lists_added', 'items_added', 'error'
        """
        if not self.is_sync_available():
            return {'success': False, 'error': 'Sync not available'}

        if self._sync_in_progress:
            return {'success': False, 'error': 'Sync already in progress'}

        # SAFETY: Always backup before sync
        if not self._backup_local_data():
            logger.warning("Proceeding with sync despite backup failure")

        self._sync_in_progress = True
        result = {
            'success': False,
            'lists_added': 0,
            'lists_updated': 0,
            'items_added': 0,
            'error': None
        }

        try:
            client = self._get_client()
            if not client:
                result['error'] = 'No Supabase client'
                return result

            # Fetch user's projects from cloud first
            projects_response = client.table('projects').select('*').eq(
                'user_id', self._user_id
            ).execute()
            cloud_projects = projects_response.data or []

            # Sync projects
            cloud_project_to_local = {}
            local_projects = self.lists_manager.data.setdefault('projects', {})

            for cloud_proj in cloud_projects:
                cloud_proj_id = cloud_proj['id']
                cloud_proj_name = cloud_proj.get('name', '')

                # Find matching local project by name
                local_proj_id = None
                for pid, pdata in local_projects.items():
                    if pdata.get('name') == cloud_proj_name:
                        local_proj_id = pid
                        break

                if local_proj_id:
                    # Update existing project
                    cloud_project_to_local[cloud_proj_id] = local_proj_id
                    local_projects[local_proj_id]['cloud_id'] = cloud_proj_id
                    if cloud_proj.get('color'):
                        local_projects[local_proj_id]['color'] = cloud_proj['color']
                else:
                    # Create new local project
                    import uuid
                    new_proj_id = f"proj_{uuid.uuid4().hex[:8]}"
                    local_projects[new_proj_id] = {
                        'name': cloud_proj_name,
                        'color': cloud_proj.get('color', '#4CAF50'),
                        'created': time.time(),
                        'cloud_id': cloud_proj_id
                    }
                    cloud_project_to_local[cloud_proj_id] = new_proj_id
                    self.lists_manager.data.setdefault('projects_order', []).append(new_proj_id)

            # Fetch user's lists from cloud
            lists_response = client.table('user_lists').select('*').eq(
                'user_id', self._user_id
            ).execute()

            cloud_lists = lists_response.data or []

            # If cloud has no lists AND no projects, preserve local data
            if not cloud_lists and not cloud_projects:
                logger.info("Cloud has no lists or projects - preserving local data unchanged")
                result['success'] = True
                result['error'] = None
                return result

            # Build mapping of cloud list IDs to local list IDs
            cloud_to_local = {}
            local_lists = self.lists_manager.data.get('lists', {})

            # Map of cloud names to local system list IDs (for special handling)
            SYSTEM_LIST_NAME_MAP = {
                'General': 'default',
                'Recently Viewed': 'recent',
            }

            for cloud_list in cloud_lists:
                cloud_id = cloud_list['id']
                cloud_name = cloud_list.get('name', '')

                # First, check if this is a system list by name
                local_id = SYSTEM_LIST_NAME_MAP.get(cloud_name)

                # Skip syncing "Recently Viewed" - it's auto-generated locally
                if local_id == 'recent':
                    logger.debug(f"Skipping cloud list '{cloud_name}' - Recently Viewed is local-only")
                    continue

                # If not a system list, find matching local list by name
                if not local_id:
                    for lid, ldata in local_lists.items():
                        if ldata.get('name') == cloud_name:
                            local_id = lid
                            break

                # Map cloud project_id to local project_id
                cloud_project_id = cloud_list.get('project_id')
                local_project_id = cloud_project_to_local.get(cloud_project_id) if cloud_project_id else None

                # Handle soft delete - convert cloud timestamp to local format
                cloud_deleted_at = cloud_list.get('deleted_at')
                local_deleted_at = None
                if cloud_deleted_at:
                    # Parse ISO timestamp from cloud to Unix timestamp
                    try:
                        from datetime import datetime
                        if isinstance(cloud_deleted_at, str):
                            dt = datetime.fromisoformat(cloud_deleted_at.replace('Z', '+00:00'))
                            local_deleted_at = dt.timestamp()
                        else:
                            local_deleted_at = cloud_deleted_at
                    except Exception:
                        local_deleted_at = time.time()  # Fallback to now

                if local_id:
                    # Update existing list
                    cloud_to_local[cloud_id] = local_id
                    local_lists[local_id]['cloud_id'] = cloud_id
                    if cloud_list.get('color'):
                        local_lists[local_id]['color'] = cloud_list['color']
                    # Update project assignment if changed
                    if local_project_id:
                        local_lists[local_id]['project_id'] = local_project_id
                    # Sync deleted_at status
                    if local_deleted_at:
                        local_lists[local_id]['deleted_at'] = local_deleted_at
                    elif 'deleted_at' in local_lists[local_id]:
                        # Cloud restored the list - remove local deleted_at
                        del local_lists[local_id]['deleted_at']
                    result['lists_updated'] += 1
                else:
                    # Create new local list
                    import uuid
                    new_id = f"list_{uuid.uuid4().hex[:8]}"
                    new_list_data = {
                        'name': cloud_name,
                        'name_en': cloud_list.get('name_en', cloud_name),
                        'color': cloud_list.get('color', '#FFD700'),
                        'created': time.time(),
                        'is_default': cloud_list.get('is_default', False),
                        'is_system': cloud_list.get('is_system', False),
                        'project_id': local_project_id,
                        'cloud_id': cloud_id
                    }
                    if local_deleted_at:
                        new_list_data['deleted_at'] = local_deleted_at
                    local_lists[new_id] = new_list_data
                    cloud_to_local[cloud_id] = new_id
                    self.lists_manager.data.setdefault('lists_order', []).append(new_id)
                    result['lists_added'] += 1

            # Build lookup maps and clean up any pre-existing local duplicates
            # (caused by past syncs that keyed items by `img` while cloud keyed by
            # `fl_id`). Matching priority below: cloud_id → (sys_id, fl_id) → computed key.
            cloud_id_to_item_id, sys_id_fl_id_to_item_id = self._dedupe_and_index_local_items()

            # Fetch items for each cloud list
            for cloud_id, local_id in cloud_to_local.items():
                # Skip syncing items for deleted lists
                if local_lists.get(local_id, {}).get('deleted_at'):
                    logger.debug(f"Skipping items sync for deleted list '{local_id}'")
                    continue

                items_response = client.table('list_items').select('*').eq(
                    'list_id', cloud_id
                ).execute()

                for cloud_item in items_response.data or []:
                    sys_id = cloud_item.get('sys_id')
                    if not sys_id:
                        continue

                    fl_id = cloud_item.get('fl_id')
                    cloud_row_id = cloud_item['id']
                    item_id = (
                        cloud_id_to_item_id.get(cloud_row_id)
                        or sys_id_fl_id_to_item_id.get((sys_id, fl_id))
                        or self.lists_manager._build_item_id(sys_id, fl_id=fl_id)
                    )

                    items = self.lists_manager.data.get('items', {})
                    if item_id in items:
                        # Item exists, add to list if not already
                        if local_id not in items[item_id].get('lists', []):
                            items[item_id]['lists'].append(local_id)
                            result['items_added'] += 1
                        # Update note/tags if cloud has newer data
                        if cloud_item.get('note'):
                            items[item_id]['note'] = cloud_item['note']
                        if cloud_item.get('tags'):
                            items[item_id]['tags'] = cloud_item['tags']
                        items[item_id]['cloud_id'] = cloud_row_id
                        cloud_id_to_item_id[cloud_row_id] = item_id
                        sys_id_fl_id_to_item_id[(sys_id, fl_id)] = item_id
                    else:
                        # New item
                        items[item_id] = {
                            'sys_id': sys_id,
                            'lists': [local_id],
                            'tags': cloud_item.get('tags', []) or [],
                            'note': cloud_item.get('note', ''),
                            'source': 'cloud_sync',
                            'added': time.time(),
                            'modified': time.time(),
                            'shelfmark_override': None,
                            'fl_id': fl_id,
                            'cloud_id': cloud_row_id
                        }
                        self.lists_manager.data['items'] = items
                        cloud_id_to_item_id[cloud_row_id] = item_id
                        sys_id_fl_id_to_item_id[(sys_id, fl_id)] = item_id
                        result['items_added'] += 1

            self.lists_manager.save()
            self._last_sync = time.time()
            result['success'] = True

        except Exception as e:
            logger.error(f"Error syncing from cloud: {e}")
            result['error'] = str(e)

        finally:
            self._sync_in_progress = False

        return result

    def sync_to_cloud(self) -> Dict[str, Any]:
        """
        Push local lists and items to Supabase.

        Returns:
            Dict with 'success', 'lists_pushed', 'items_pushed', 'error'
        """
        if not self.is_sync_available():
            return {'success': False, 'error': 'Sync not available'}

        if self._sync_in_progress:
            return {'success': False, 'error': 'Sync already in progress'}

        # SAFETY: Always backup before sync
        if not self._backup_local_data():
            logger.warning("Proceeding with sync despite backup failure")

        self._sync_in_progress = True
        result = {
            'success': False,
            'lists_pushed': 0,
            'items_pushed': 0,
            'error': None
        }

        try:
            client = self._get_client()
            if not client:
                result['error'] = 'No Supabase client'
                return result

            # Fetch existing cloud projects to prevent duplicates
            existing_projects_response = client.table('projects').select('id, name').eq(
                'user_id', self._user_id
            ).execute()
            existing_cloud_projects = {proj['name']: proj['id'] for proj in (existing_projects_response.data or [])}
            valid_project_ids = {proj['id'] for proj in (existing_projects_response.data or [])}

            # Push projects first (so we have cloud IDs for list references)
            local_projects = self.lists_manager.data.get('projects', {})
            local_project_to_cloud = {}

            for proj_id, proj_data in local_projects.items():
                cloud_proj_id = proj_data.get('cloud_id')
                # Validate cloud_id still exists
                if cloud_proj_id and cloud_proj_id not in valid_project_ids:
                    logger.debug(f"Clearing stale cloud_id {cloud_proj_id} for project '{proj_data.get('name')}'")
                    cloud_proj_id = None
                    proj_data.pop('cloud_id', None)
                proj_name = proj_data.get('name', 'Unnamed')

                proj_payload = {
                    'user_id': self._user_id,
                    'name': proj_name,
                    'color': proj_data.get('color', '#4CAF50')
                }

                if cloud_proj_id:
                    # Update existing cloud project
                    client.table('projects').update(proj_payload).eq(
                        'id', cloud_proj_id
                    ).execute()
                    local_project_to_cloud[proj_id] = cloud_proj_id
                elif proj_name in existing_cloud_projects:
                    # Project with same name exists - use it
                    cloud_proj_id = existing_cloud_projects[proj_name]
                    proj_data['cloud_id'] = cloud_proj_id
                    local_project_to_cloud[proj_id] = cloud_proj_id
                    client.table('projects').update(proj_payload).eq(
                        'id', cloud_proj_id
                    ).execute()
                else:
                    # Create new cloud project
                    response = client.table('projects').insert(proj_payload).execute()
                    if response.data:
                        cloud_proj_id = response.data[0]['id']
                        proj_data['cloud_id'] = cloud_proj_id
                        local_project_to_cloud[proj_id] = cloud_proj_id
                        existing_cloud_projects[proj_name] = cloud_proj_id

            # Fetch existing cloud lists to prevent duplicates
            existing_lists_response = client.table('user_lists').select('id, name').eq(
                'user_id', self._user_id
            ).execute()
            existing_cloud_lists = {lst['name']: lst['id'] for lst in (existing_lists_response.data or [])}
            # Also build reverse map: id -> name (for validating cloud_ids)
            valid_cloud_ids = {lst['id'] for lst in (existing_lists_response.data or [])}
            logger.debug(f"Found {len(existing_cloud_lists)} existing cloud lists")

            # Push lists
            local_lists = self.lists_manager.data.get('lists', {})

            for list_id, list_data in local_lists.items():
                # Skip "Recently Viewed" - it's auto-generated locally and not synced
                # But DO sync "default" (General) list
                if list_id == 'recent' or list_data.get('is_system'):
                    continue

                cloud_id = list_data.get('cloud_id')
                # Validate cloud_id still exists (might be stale after cleanup)
                if cloud_id and cloud_id not in valid_cloud_ids:
                    logger.debug(f"Clearing stale cloud_id {cloud_id} for list '{list_data.get('name')}'")
                    cloud_id = None
                    list_data.pop('cloud_id', None)
                list_name = list_data.get('name', 'Unnamed')

                # Map local project_id to cloud project_id
                local_proj_id = list_data.get('project_id')
                cloud_proj_id = local_project_to_cloud.get(local_proj_id) if local_proj_id else None

                # Handle soft delete - convert local timestamp to ISO format for cloud
                local_deleted_at = list_data.get('deleted_at')
                cloud_deleted_at = None
                if local_deleted_at:
                    from datetime import datetime, timezone
                    cloud_deleted_at = datetime.fromtimestamp(local_deleted_at, tz=timezone.utc).isoformat()

                list_payload = {
                    'user_id': self._user_id,
                    'name': list_name,
                    'name_en': list_data.get('name_en', list_name),
                    'color': list_data.get('color', '#FFD700'),
                    'is_default': list_data.get('is_default', False),
                    'is_system': False,
                    'project_id': cloud_proj_id,
                    'deleted_at': cloud_deleted_at
                }

                if cloud_id:
                    # Update existing cloud list by stored cloud_id
                    client.table('user_lists').update(list_payload).eq(
                        'id', cloud_id
                    ).execute()
                elif list_name in existing_cloud_lists:
                    # List with same name exists in cloud - use it instead of creating duplicate
                    cloud_id = existing_cloud_lists[list_name]
                    list_data['cloud_id'] = cloud_id
                    logger.debug(f"Found existing cloud list '{list_name}' with ID {cloud_id}")
                    # Update it with local data
                    client.table('user_lists').update(list_payload).eq(
                        'id', cloud_id
                    ).execute()
                else:
                    # Create new cloud list (no duplicate exists)
                    response = client.table('user_lists').insert(list_payload).execute()
                    if response.data:
                        cloud_id = response.data[0]['id']
                        list_data['cloud_id'] = cloud_id
                        existing_cloud_lists[list_name] = cloud_id  # Track to prevent dups in same sync

                result['lists_pushed'] += 1

                # Skip syncing items for deleted lists
                if local_deleted_at:
                    logger.debug(f"Skipping items sync for deleted list '{list_name}'")
                    continue

                # Push items in this list - BATCH approach for performance
                items = self.lists_manager.data.get('items', {})

                # 1. Get all existing cloud items for this list in ONE call
                existing_items_response = client.table('list_items').select('id, sys_id').eq(
                    'list_id', cloud_id
                ).execute()
                existing_items_map = {item['sys_id']: item['id'] for item in (existing_items_response.data or [])}

                # 2. Collect items to insert (new) vs update (existing)
                items_to_insert = []
                items_to_update = []

                for item_id, item_data in items.items():
                    if list_id not in item_data.get('lists', []):
                        continue

                    sys_id = item_data.get('sys_id', item_id)
                    item_cloud_id = item_data.get('cloud_id') or existing_items_map.get(sys_id)

                    item_payload = {
                        'list_id': cloud_id,
                        'sys_id': sys_id,
                        'shelfmark': item_data.get('shelfmark_override'),
                        'title': None,
                        'fl_id': item_data.get('fl_id'),
                        'note': item_data.get('note', ''),
                        'tags': item_data.get('tags', [])
                    }

                    if item_cloud_id:
                        # Update existing
                        items_to_update.append((item_cloud_id, item_payload, item_data))
                    elif sys_id not in existing_items_map:
                        # New item - insert
                        items_to_insert.append((item_payload, item_data))

                # 3. Batch insert new items
                if items_to_insert:
                    insert_payloads = [p for p, _ in items_to_insert]
                    try:
                        response = client.table('list_items').insert(insert_payloads).execute()
                        if response.data:
                            for i, row in enumerate(response.data):
                                if i < len(items_to_insert):
                                    items_to_insert[i][1]['cloud_id'] = row['id']
                    except Exception as e:
                        logger.warning(f"Batch insert failed, falling back to individual: {e}")
                        for payload, item_data in items_to_insert:
                            try:
                                response = client.table('list_items').insert(payload).execute()
                                if response.data:
                                    item_data['cloud_id'] = response.data[0]['id']
                            except Exception:
                                pass

                # 4. Update existing items (still individual but fewer calls)
                for item_cloud_id, payload, item_data in items_to_update:
                    try:
                        client.table('list_items').update(payload).eq('id', item_cloud_id).execute()
                        item_data['cloud_id'] = item_cloud_id
                    except Exception:
                        pass  # Ignore update errors

                result['items_pushed'] += len(items_to_insert) + len(items_to_update)

            self.lists_manager.save()
            self._last_sync = time.time()
            result['success'] = True

        except Exception as e:
            logger.error(f"Error syncing to cloud: {e}")
            result['error'] = str(e)

        finally:
            self._sync_in_progress = False

        return result

    def sync_list_to_cloud(self, list_id: str) -> bool:
        """Push a specific list and its items to cloud."""
        # ===== Phase 95 LOCAL gate (D-30 Codex P0, REQ-9) =====
        # Abort entire list sync if any item belonging to this list has a LOCAL sys_id.
        # B2 — field names pinned from sync_to_cloud:619-635 canonical pattern.
        # Items are stored as a flat dict at self.lists_manager.data['items'].
        # Each item dict has a 'lists' list field holding the list_ids it belongs to.
        # The sys_id is in 'sys_id' (fallback item_id).
        items_map = self.lists_manager.data.get('items', {})
        for iid, item_data in items_map.items():
            if list_id not in (item_data.get('lists') or []):
                continue  # item not in this list
            if is_local_sys_id(item_data.get('sys_id', iid)):
                logger.info("[list contains LOCAL items, not synced] list_id=%s", list_id)
                return False
        # ======================================================
        if not self.is_sync_available():
            return False

        try:
            client = self._get_client()
            if not client:
                return False

            list_data = self.lists_manager.data.get('lists', {}).get(list_id)
            if not list_data or list_data.get('is_system'):
                return False

            cloud_id = list_data.get('cloud_id')

            list_payload = {
                'user_id': self._user_id,
                'name': list_data.get('name', 'Unnamed'),
                'name_en': list_data.get('name_en', list_data.get('name', '')),
                'color': list_data.get('color', '#FFD700'),
                'is_default': list_data.get('is_default', False),
                'is_system': False
            }

            if cloud_id:
                client.table('user_lists').update(list_payload).eq('id', cloud_id).execute()
            else:
                response = client.table('user_lists').insert(list_payload).execute()
                if response.data:
                    list_data['cloud_id'] = response.data[0]['id']
                    self.lists_manager.save()

            return True

        except Exception as e:
            logger.error(f"Error syncing list to cloud: {e}")
            return False

    def sync_item_to_cloud(self, item_id: str, list_id: str) -> bool:
        """Push a specific item to cloud."""
        # ===== Phase 95 LOCAL gate (D-30 Codex P0 + HIGH-2 review fix, REQ-9) =====
        # MUST run BEFORE _get_client() and sync_list_to_cloud() — both leak
        # cloud activity even though the natural sys_id lookup is at line ~762.
        # HIGH-2: derive sys_id BEFORE the `if item_data:` branch so a LOCAL
        # item_id with missing item_data is ALSO gated (the previous draft
        # nested the derivation INSIDE the `if item_data:` body which let this
        # case slip through).
        # Lookup from in-memory self.lists_manager.data only (no network).
        item_data = self.lists_manager.data.get('items', {}).get(item_id)
        sys_id = item_data.get('sys_id', item_id) if item_data else item_id
        if is_local_sys_id(sys_id):
            logger.info("[local-only item, not synced] item_id=%s sys_id=%s", item_id, sys_id)
            return False
        # ===========================================================================
        if not self.is_sync_available():
            return False

        try:
            client = self._get_client()
            if not client:
                return False

            list_data = self.lists_manager.data.get('lists', {}).get(list_id)
            if not list_data:
                return False

            cloud_list_id = list_data.get('cloud_id')
            if not cloud_list_id:
                # Need to sync list first
                self.sync_list_to_cloud(list_id)
                cloud_list_id = list_data.get('cloud_id')
                if not cloud_list_id:
                    return False

            item_data = self.lists_manager.data.get('items', {}).get(item_id)
            if not item_data:
                return False

            sys_id = item_data.get('sys_id', item_id)
            item_payload = {
                'list_id': cloud_list_id,
                'sys_id': sys_id,
                'shelfmark': item_data.get('shelfmark_override'),
                'fl_id': item_data.get('fl_id'),
                'note': item_data.get('note', ''),
                'tags': item_data.get('tags', [])
            }

            # Check if exists
            existing = client.table('list_items').select('id').eq(
                'list_id', cloud_list_id
            ).eq('sys_id', sys_id).execute()

            if existing.data:
                client.table('list_items').update(item_payload).eq(
                    'id', existing.data[0]['id']
                ).execute()
                item_data['cloud_id'] = existing.data[0]['id']
            else:
                response = client.table('list_items').insert(item_payload).execute()
                if response.data:
                    item_data['cloud_id'] = response.data[0]['id']

            self.lists_manager.save()
            return True

        except Exception as e:
            logger.error(f"Error syncing item to cloud: {e}")
            return False

    def delete_list_from_cloud(self, list_id: str) -> bool:
        """Delete a list from cloud (cascade deletes items)."""
        if not self.is_sync_available():
            return False

        try:
            client = self._get_client()
            if not client:
                return False

            list_data = self.lists_manager.data.get('lists', {}).get(list_id)
            if not list_data:
                return True  # Already deleted locally

            cloud_id = list_data.get('cloud_id')
            if cloud_id:
                client.table('user_lists').delete().eq('id', cloud_id).execute()

            return True

        except Exception as e:
            logger.error(f"Error deleting list from cloud: {e}")
            return False

    def delete_item_from_cloud(self, item_id: str, list_id: str) -> bool:
        """Delete an item from a cloud list."""
        if not self.is_sync_available():
            return False

        try:
            client = self._get_client()
            if not client:
                return False

            list_data = self.lists_manager.data.get('lists', {}).get(list_id)
            if not list_data:
                return True

            cloud_list_id = list_data.get('cloud_id')
            if not cloud_list_id:
                return True

            item_data = self.lists_manager.data.get('items', {}).get(item_id)
            sys_id = item_data.get('sys_id', item_id) if item_data else item_id

            client.table('list_items').delete().eq(
                'list_id', cloud_list_id
            ).eq('sys_id', sys_id).execute()

            return True

        except Exception as e:
            logger.error(f"Error deleting item from cloud: {e}")
            return False


# Singleton instance
_sync_instance: Optional[ListsCloudSync] = None


def get_lists_sync(lists_manager=None) -> ListsCloudSync:
    """Get or create the lists sync singleton."""
    global _sync_instance
    if _sync_instance is None:
        _sync_instance = ListsCloudSync(lists_manager)
    elif lists_manager is not None and _sync_instance.lists_manager is None:
        _sync_instance.lists_manager = lists_manager
    return _sync_instance
