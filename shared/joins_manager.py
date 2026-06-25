# -*- coding: utf-8 -*-
"""Fragment joins management with offline-first caching.

Phase 123: Extracted from genizah_core.py (v8.3.0 God-File Decomposition).
genizah_core.py retains a permanent same-object re-export shim so all
existing ``from genizah_core import JoinsManager`` callers continue working.

Note: Config is imported at module level BEFORE the class body because
JoinsManager.JOINS_FILE = os.path.join(Config.INDEX_DIR, ...) evaluates at
class-definition time (Pitfall 3 of Phase 123).
"""

import logging
import os
import pickle
import threading
import time

from shared.browse_map_utils import normalize_shelfmark
from shared.config import Config

LOGGER = logging.getLogger(__name__)


class JoinsManager:
    """
    Manages fragment joins with offline-first caching.

    Features:
    - Local pickle cache for offline access
    - Background sync with server when connected
    - Efficient lookup by shelfmark (normalized)
    - Queue for pending operations when offline
    """

    JOINS_FILE = os.path.join(Config.INDEX_DIR, "joins_cache.pkl")
    SYNC_INTERVAL = 300  # 5 minutes between background syncs

    def __init__(self, corrections_client=None):
        """Initialize the joins manager."""
        self.client = corrections_client
        self.data = self._get_default_data()
        self._sync_thread = None
        self._stop_sync = False
        self._last_sync = 0
        self._sync_lock = threading.Lock()
        self.load()

    def _get_default_data(self):
        """Return the default data structure."""
        return {
            'joins': {},  # join_id -> join data
            'by_normalized': {},  # normalized_shelfmark -> set of join_ids
            'pending_creates': [],  # joins to create when back online
            'pending_deletes': [],  # join_ids to delete when back online
            'last_server_sync': 0,  # timestamp of last successful sync
            'version': 1
        }

    def _normalize_shelfmark(self, shelfmark: str) -> str:
        """Normalize shelfmarks using the canonical module-level function."""
        return normalize_shelfmark(shelfmark)

    def load(self):
        """Load joins from local cache file."""
        if os.path.exists(self.JOINS_FILE):
            try:
                with open(self.JOINS_FILE, 'rb') as f:
                    loaded = pickle.load(f)
                    # Merge with defaults for any new fields
                    defaults = self._get_default_data()
                    for key in defaults:
                        if key not in loaded:
                            loaded[key] = defaults[key]
                    self.data = loaded
                    LOGGER.info(f"Loaded {len(self.data.get('joins', {}))} joins from cache")
            except Exception as e:
                LOGGER.warning(f"Failed to load joins cache: {e}")
                self.data = self._get_default_data()
        else:
            self.data = self._get_default_data()

    def save(self):
        """Save joins to local cache file."""
        try:
            os.makedirs(Config.INDEX_DIR, exist_ok=True)
            with open(self.JOINS_FILE, 'wb') as f:
                pickle.dump(self.data, f)
        except Exception as e:
            LOGGER.error(f"Failed to save joins cache: {e}")

    def _index_join(self, join_data: dict):
        """Add a join to the normalized index (by shelfmark and document_id)."""
        join_id = join_data.get('id')
        if not join_id:
            return

        # Index by both fragments (shelfmark)
        for key in ['fragment_a', 'fragment_b']:
            shelfmark = join_data.get(key, '')
            if shelfmark:
                normalized = self._normalize_shelfmark(shelfmark)
                if normalized not in self.data['by_normalized']:
                    self.data['by_normalized'][normalized] = set()
                self.data['by_normalized'][normalized].add(join_id)

        # Also index by document_id (sys_id) - this is the reliable key
        if 'by_document_id' not in self.data:
            self.data['by_document_id'] = {}
        for key in ['document_id_a', 'document_id_b']:
            doc_id = join_data.get(key, '')
            if doc_id:
                if doc_id not in self.data['by_document_id']:
                    self.data['by_document_id'][doc_id] = set()
                self.data['by_document_id'][doc_id].add(join_id)

    def _unindex_join(self, join_data: dict):
        """Remove a join from the normalized index."""
        join_id = join_data.get('id')
        if not join_id:
            return

        for key in ['fragment_a', 'fragment_b']:
            shelfmark = join_data.get(key, '')
            if shelfmark:
                normalized = self._normalize_shelfmark(shelfmark)
                if normalized in self.data['by_normalized']:
                    self.data['by_normalized'][normalized].discard(join_id)

        # Also remove from document_id index
        if 'by_document_id' in self.data:
            for key in ['document_id_a', 'document_id_b']:
                doc_id = join_data.get(key, '')
                if doc_id and doc_id in self.data['by_document_id']:
                    self.data['by_document_id'][doc_id].discard(join_id)

    def get_joins_for_shelfmark(self, shelfmark: str) -> list:
        """
        Get all joins involving a shelfmark from local cache.
        Returns list of join dictionaries.
        """
        normalized = self._normalize_shelfmark(shelfmark)
        join_ids = self.data['by_normalized'].get(normalized, set())

        joins = []
        for join_id in join_ids:
            join = self.data['joins'].get(join_id)
            if join:
                joins.append(join)

        return joins

    def get_connected_fragments(self, shelfmark: str) -> dict:
        """
        Get all fragments connected to this shelfmark (BFS through joins).
        Returns dict with 'fragments' list and 'joins' list.
        """
        normalized = self._normalize_shelfmark(shelfmark)
        LOGGER.debug(f"get_connected_fragments: input='{shelfmark}', normalized='{normalized}'")
        LOGGER.debug(f"  Available normalized keys: {list(self.data['by_normalized'].keys())[:10]}...")  # First 10

        # BFS to find all connected fragments
        visited_fragments = set()
        visited_joins = set()
        queue = [normalized]

        while queue:
            current = queue.pop(0)
            if current in visited_fragments:
                continue
            visited_fragments.add(current)

            # Find all joins for this fragment
            join_ids = self.data['by_normalized'].get(current, set())
            for join_id in join_ids:
                if join_id in visited_joins:
                    continue
                visited_joins.add(join_id)

                join = self.data['joins'].get(join_id)
                if not join:
                    continue

                # Add the other fragment to queue
                for key in ['fragment_a', 'fragment_b']:
                    other = self._normalize_shelfmark(join.get(key, ''))
                    if other and other not in visited_fragments:
                        queue.append(other)

        # Get original shelfmarks for connected fragments
        fragments = []
        joins = []
        fragment_map = {}  # normalized -> original shelfmark

        for join_id in visited_joins:
            join = self.data['joins'].get(join_id)
            if join:
                joins.append(join)
                for key in ['fragment_a', 'fragment_b']:
                    orig = join.get(key, '')
                    if orig:
                        norm = self._normalize_shelfmark(orig)
                        if norm not in fragment_map:
                            fragment_map[norm] = orig

        fragments = list(fragment_map.values())

        return {
            'shelfmark': shelfmark,
            'fragments': fragments,
            'joins': joins,
            'total_fragments': len(fragments),
            'total_joins': len(joins)
        }

    def get_connected_fragments_by_id(self, document_id: str) -> dict:
        """
        Get all fragments connected to this document_id (sys_id).
        Uses the by_document_id index for reliable lookup regardless of shelfmark format.
        Returns dict with 'fragments' list and 'joins' list.
        """
        if not document_id:
            return {
                'document_id': document_id,
                'fragments': [],
                'joins': [],
                'total_fragments': 0,
                'total_joins': 0
            }

        # Ensure by_document_id index exists
        if 'by_document_id' not in self.data:
            self.data['by_document_id'] = {}

        LOGGER.debug(f"get_connected_fragments_by_id: document_id='{document_id}'")

        # BFS to find all connected fragments via document_id
        visited_doc_ids = set()
        visited_joins = set()
        queue = [document_id]

        while queue:
            current_doc_id = queue.pop(0)
            if current_doc_id in visited_doc_ids:
                continue
            visited_doc_ids.add(current_doc_id)

            # Find all joins for this document_id
            join_ids = self.data['by_document_id'].get(current_doc_id, set())
            for join_id in join_ids:
                if join_id in visited_joins:
                    continue
                visited_joins.add(join_id)

                join = self.data['joins'].get(join_id)
                if not join:
                    continue

                # Add the other document_id to queue
                for key in ['document_id_a', 'document_id_b']:
                    other_doc_id = join.get(key, '')
                    if other_doc_id and other_doc_id not in visited_doc_ids:
                        queue.append(other_doc_id)

        # Get original shelfmarks for connected fragments
        joins = []
        fragment_map = {}  # document_id -> shelfmark

        for join_id in visited_joins:
            join = self.data['joins'].get(join_id)
            if join:
                joins.append(join)
                # Map document_id to shelfmark
                for doc_key, shelf_key in [('document_id_a', 'fragment_a'), ('document_id_b', 'fragment_b')]:
                    doc_id = join.get(doc_key, '')
                    shelfmark = join.get(shelf_key, '')
                    if doc_id and shelfmark and doc_id not in fragment_map:
                        fragment_map[doc_id] = shelfmark

        fragments = list(fragment_map.values())

        return {
            'document_id': document_id,
            'fragments': fragments,
            'joins': joins,
            'total_fragments': len(fragments),
            'total_joins': len(joins)
        }

    def has_joins_by_id(self, document_id: str) -> bool:
        """Quick check if a document_id has any joins."""
        if 'by_document_id' not in self.data:
            return False
        return bool(self.data['by_document_id'].get(document_id))

    def has_joins(self, shelfmark: str) -> bool:
        """Quick check if a shelfmark has any joins."""
        normalized = self._normalize_shelfmark(shelfmark)
        return bool(self.data['by_normalized'].get(normalized))

    def get_join_count(self, shelfmark: str) -> int:
        """Get count of joins for a shelfmark."""
        normalized = self._normalize_shelfmark(shelfmark)
        return len(self.data['by_normalized'].get(normalized, set()))

    # --- Sync Operations ---

    def start_background_sync(self):
        """Start background sync thread."""
        if self._sync_thread and self._sync_thread.is_alive():
            return

        self._stop_sync = False
        self._sync_thread = threading.Thread(target=self._sync_loop, daemon=True)
        self._sync_thread.start()
        LOGGER.info("Started joins background sync")

    def stop_background_sync(self):
        """Stop background sync thread."""
        self._stop_sync = True
        if self._sync_thread:
            self._sync_thread.join(timeout=2)

    def _sync_loop(self):
        """Background sync — runs once at startup only."""
        try:
            LOGGER.info("Starting joins sync on startup...")
            self.sync_with_server()
            self._last_sync = time.time()
            LOGGER.info("Startup joins sync completed")
        except Exception as e:
            LOGGER.warning(f"Startup joins sync error: {e}")

    def sync_with_server(self, force: bool = False):
        """
        Sync joins with server. Called in background.

        Args:
            force: If True, fetch all joins even if recently synced
        """
        if not self.client:
            return False

        # Quick server availability check to avoid blocking
        if not self.client.is_server_available():
            LOGGER.debug("Server unavailable, skipping joins sync")
            return False

        with self._sync_lock:
            try:
                # First, process any pending operations
                self._process_pending_operations()

                # Then fetch all joins from server
                all_joins = self._fetch_all_joins()
                if all_joins is None:
                    return False  # Network error

                # Rebuild local cache
                self.data['joins'] = {}
                self.data['by_normalized'] = {}
                self.data['by_document_id'] = {}

                for join in all_joins:
                    join_id = join.get('id')
                    if join_id:
                        self.data['joins'][join_id] = join
                        self._index_join(join)

                self.data['last_server_sync'] = time.time()
                self.save()

                LOGGER.info(f"Synced {len(all_joins)} joins from server")
                return True

            except Exception as e:
                LOGGER.warning(f"Failed to sync joins: {e}")
                return False

    def _fetch_all_joins(self) -> list:
        """Fetch all joins from server with pagination."""
        if not self.client:
            return None

        all_joins = []
        offset = 0
        limit = 200  # Max allowed by API

        try:
            while True:
                # Use search_joins to get paginated results (returns tuple)
                joins, total = self.client.search_joins(limit=limit, offset=offset)
                if not joins:
                    break

                for join in joins:
                    all_joins.append({
                        'id': join.id,
                        'fragment_a': join.fragment_a,
                        'fragment_b': join.fragment_b,
                        'document_id_a': getattr(join, 'document_id_a', None),
                        'document_id_b': getattr(join, 'document_id_b', None),
                        'relationship_type': join.relationship_type,
                        'notes': join.notes,
                        'source': join.source,
                        'source_url': join.source_url,
                        'created_by_username': join.created_by_username,
                        'created_at': join.created_at
                    })

                if len(joins) < limit:
                    break
                offset += limit

            return all_joins
        except Exception as e:
            LOGGER.warning(f"Failed to fetch joins: {e}")
            return None

    def _process_pending_operations(self):
        """Process any pending create/delete operations."""
        if not self.client:
            return

        # Process pending creates
        pending = self.data.get('pending_creates', [])
        successful_creates = []
        for create_data in pending:
            try:
                join, msg = self.client.create_join(
                    fragment_a=create_data['fragment_a'],
                    fragment_b=create_data['fragment_b'],
                    relationship_type=create_data.get('relationship_type'),
                    notes=create_data.get('notes'),
                    document_id_a=create_data.get('document_id_a'),
                    document_id_b=create_data.get('document_id_b')
                )
                if join:
                    successful_creates.append(create_data)
            except Exception as e:
                LOGGER.warning(f"Failed to create pending join: {e}")

        # Remove successful creates from pending
        for c in successful_creates:
            if c in self.data['pending_creates']:
                self.data['pending_creates'].remove(c)

        # Process pending deletes
        pending_deletes = self.data.get('pending_deletes', [])
        successful_deletes = []
        for join_id in pending_deletes:
            try:
                success, msg = self.client.delete_join(join_id)
                if success:
                    successful_deletes.append(join_id)
            except Exception as e:
                LOGGER.warning(f"Failed to delete pending join {join_id}: {e}")

        # Remove successful deletes from pending
        for d in successful_deletes:
            if d in self.data['pending_deletes']:
                self.data['pending_deletes'].remove(d)

        if successful_creates or successful_deletes:
            self.save()

    # --- Local Operations (with queuing for offline) ---

    def create_join_local(self, fragment_a: str, fragment_b: str,
                          relationship_type: str = None, notes: str = None,
                          document_id_a: str = None, document_id_b: str = None) -> dict:
        """
        Create a join locally and queue for server sync.
        Returns the local join data.
        """
        # Generate temporary local ID
        import uuid
        local_id = f"local_{uuid.uuid4().hex[:8]}"

        join_data = {
            'id': local_id,
            'fragment_a': fragment_a,
            'fragment_b': fragment_b,
            'document_id_a': document_id_a,
            'document_id_b': document_id_b,
            'relationship_type': relationship_type,
            'notes': notes,
            'source': 'user',
            'is_local': True,
            'created_at': time.time()
        }

        # Add to local cache
        self.data['joins'][local_id] = join_data
        self._index_join(join_data)

        # Queue for server sync
        self.data['pending_creates'].append({
            'fragment_a': fragment_a,
            'fragment_b': fragment_b,
            'document_id_a': document_id_a,
            'document_id_b': document_id_b,
            'relationship_type': relationship_type,
            'notes': notes,
            'local_id': local_id
        })

        self.save()
        return join_data

    def delete_join_local(self, join_id) -> bool:
        """
        Delete a join locally and queue for server sync.
        """
        join_id_str = str(join_id)

        # Handle local-only joins
        if join_id_str.startswith('local_'):
            # Remove from pending creates
            self.data['pending_creates'] = [
                c for c in self.data['pending_creates']
                if c.get('local_id') != join_id_str
            ]
        else:
            # Queue server delete
            if join_id not in self.data['pending_deletes']:
                self.data['pending_deletes'].append(join_id)

        # Remove from local cache
        if join_id_str in self.data['joins']:
            join_data = self.data['joins'][join_id_str]
            self._unindex_join(join_data)
            del self.data['joins'][join_id_str]
        elif isinstance(join_id, int) and join_id in self.data['joins']:
            join_data = self.data['joins'][join_id]
            self._unindex_join(join_data)
            del self.data['joins'][join_id]

        self.save()
        return True

    def get_all_shelfmarks_with_joins(self) -> list:
        """Get list of all shelfmarks that have joins (for autocomplete)."""
        shelfmarks = set()
        for join in self.data['joins'].values():
            if join.get('fragment_a'):
                shelfmarks.add(join['fragment_a'])
            if join.get('fragment_b'):
                shelfmarks.add(join['fragment_b'])
        return sorted(shelfmarks)


# ==============================================================================
#  PERSONAL LISTS MANAGER
# ==============================================================================

