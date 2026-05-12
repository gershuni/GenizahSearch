"""CRITICAL regression test for cross-user lists cache contamination.

Codex review of v7.11.0 post-release commits flagged a second cross-user
data leak alongside the export-filename one we just fixed:

  UserListsManager is a singleton on AppState (web/state.py). Its 10-second
  TTL ``_cache`` was returned without re-checking ``self.user_id``, so a
  request from User B within the TTL window of User A's fetch would
  receive User A's lists. ``/api/export/list/{list_id}/excel`` reads
  through the same path.

Fix: cache now stores ``_cache_user_id`` alongside the data and lookups
only return the cached value when the user_id matches the current
``GlobalAuthState.get_user_id()`` (which IS per-session, backed by
``app.storage.user``).
"""
from unittest.mock import patch, MagicMock


def _build_lists(user_id, count=2):
    """Fake Supabase response for a user."""
    return [
        {
            'id': f'list-{user_id}-{i}',
            'name': f'{user_id}-list-{i}',
            'name_en': f'{user_id}-list-{i}',
            'color': '#FFD700',
            'is_default': False,
            'is_system': False,
            'project_id': None,
            'created_at': '2026-05-12T00:00:00Z',
        }
        for i in range(count)
    ]


def test_cache_keyed_by_user_id_blocks_cross_user_read():
    """User A's fetch caches user A's data; a subsequent fetch as User B
    within the TTL window MUST refetch instead of returning A's cache.
    """
    from web.user_lists import UserListsManager

    mgr = UserListsManager()

    auth_state = {'user_id': 'user-A'}

    def fake_is_logged_in():
        return True

    def fake_get_user_id():
        return auth_state['user_id']

    def fake_get_user_lists(user_id):
        return _build_lists(user_id)

    def fake_get_projects(user_id):
        return []

    with patch('web.user_lists.GlobalAuthState.is_logged_in', fake_is_logged_in), \
         patch('web.user_lists.GlobalAuthState.get_user_id', fake_get_user_id), \
         patch('web.user_lists.get_user_lists', side_effect=fake_get_user_lists) as patched_lists, \
         patch('web.user_lists.get_projects', side_effect=fake_get_projects):

        # --- User A's first fetch ---
        data_a1 = mgr._get_cached_data()
        a_list_names = {v['name'] for v in data_a1['lists'].values()}
        assert a_list_names == {'user-A-list-0', 'user-A-list-1'}
        assert patched_lists.call_count == 1

        # --- Repeat as User A within TTL — cache hit (no extra call) ---
        data_a2 = mgr._get_cached_data()
        assert data_a2 is data_a1  # Same object — cache returned
        assert patched_lists.call_count == 1

        # --- User B fetches via the SAME singleton manager ---
        auth_state['user_id'] = 'user-B'
        data_b = mgr._get_cached_data()
        b_list_names = {v['name'] for v in data_b['lists'].values()}
        assert b_list_names == {'user-B-list-0', 'user-B-list-1'}, (
            f"CROSS-USER CACHE LEAK: User B received {b_list_names!r} but expected "
            f"user-B-list-0/1. Pre-fix bug returned User A's cache (was "
            f"{a_list_names!r})."
        )
        assert patched_lists.call_count == 2, (
            "User B's fetch should bypass User A's cache and hit Supabase."
        )

        # --- Back to User A — should refetch (because B clobbered the cache slot) ---
        auth_state['user_id'] = 'user-A'
        data_a3 = mgr._get_cached_data()
        a3_names = {v['name'] for v in data_a3['lists'].values()}
        assert a3_names == {'user-A-list-0', 'user-A-list-1'}
        assert patched_lists.call_count == 3


def test_invalidate_cache_clears_user_id_too():
    """invalidate_cache() must clear the user_id stamp; otherwise a subsequent
    fetch as the same user could short-circuit using stale ``_cache=None`` plus
    matching user_id.
    """
    from web.user_lists import UserListsManager

    mgr = UserListsManager()
    mgr._cache = {'lists': {}}
    mgr._cache_time = 9_999_999.0
    mgr._cache_user_id = 'user-A'

    mgr.invalidate_cache()

    assert mgr._cache is None
    assert mgr._cache_time == 0
    assert mgr._cache_user_id is None
