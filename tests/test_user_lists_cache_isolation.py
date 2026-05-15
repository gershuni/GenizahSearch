"""Phase 89 — Lists Cache Per-Request behavior tests (LISTS-04).

Replaces the Phase 87-era atomic-tuple cache tests with behavior assertions
against the new design:

  1. ``state.lists_mgr`` is a per-access factory (D-01, D-02): every read
     returns a fresh ``UserListsManager`` instance.
  2. ``UserListsManager._get_cached_data`` is stateless (D-03, D-04): each
     authenticated call hits Supabase fresh — no user-id-keyed cache lookup
     possible, no TTL window during which a cross-user leak could occur.
     This holds for BOTH factory-fresh managers AND captured-into-closure
     managers (the R1 review addition proves the latter case directly).
  3. ``UserListsManager.invalidate_cache`` is a compatibility no-op (D-05):
     ~10 internal callers in mutation paths (create_list, update_list,
     add_item, etc.) keep calling it byte-unchanged.

The cross-user leak class that Phase 87's test guarded against (singleton
cache returning User A's data to User B within the TTL window) is now
structurally impossible: there is no cache to leak from. The companion
static AST guard in Plan 89-02 (tests/test_no_deleted_lists_state_references.py)
is the permanent CI lint against re-introducing the cache fields.

See .planning/phases/89-lists-cache-per-request/89-CONTEXT.md for full
decision rationale (D-01..D-11) and 89-REVIEWS.md R1 for the captured-
manager test rationale.
"""
from unittest.mock import patch

import pytest

from genizah_core import ListsManager
from web.state import AppState
from web.user_lists import UserListsManager


@pytest.fixture
def bootstrapped_state():
    """Bootstrap an AppState with a real ListsManager wired into
    _local_lists_mgr so the factory property in state.lists_mgr
    constructs a UserListsManager on each access. meta_mgr is set to
    None — the wrapper passes it through untouched and the tests do not
    exercise the metadata path.

    Note: the factory property (web/state.py post-Plan-89-01) reads only
    _local_lists_mgr when deciding whether to return None vs construct
    a wrapper, so we do NOT need to mutate _user_lists_mgr here.
    Plan 89-02 deletes that field entirely; touching it here would also
    cause Plan 89-02's static AST scanner to flag this test file as a
    violation (D-10), which we avoid by simply not referencing the field.
    """
    state = AppState()
    # Save and restore so other tests in the suite aren't perturbed.
    saved_local = state._local_lists_mgr
    saved_meta = state.meta_mgr
    state._local_lists_mgr = ListsManager(None)
    state.meta_mgr = None
    yield state
    state._local_lists_mgr = saved_local
    state.meta_mgr = saved_meta


def _fake_get_user_lists(user_id):
    """Shared fake — returns one list per user_id, labeled with the user_id
    so cross-user contamination is visible in assertions."""
    return [{
        'id': f'list-{user_id}-0',
        'name': f'{user_id}-list-0',
        'name_en': f'{user_id}-list-0',
        'color': '#FFD700',
        'is_default': False,
        'is_system': False,
        'project_id': None,
        'created_at': '2026-05-14T00:00:00Z',
    }]


def test_two_accesses_get_distinct_managers(bootstrapped_state):
    """Per-ACCESS factory contract (D-01, D-02): each read of
    ``state.lists_mgr`` constructs a new ``UserListsManager``.

    Two consecutive reads must be distinct Python objects. This is the
    structural property that makes per-instance caching unsafe — and is
    the reason _get_cached_data() became stateless.
    """
    mgr_a = bootstrapped_state.lists_mgr
    mgr_b = bootstrapped_state.lists_mgr
    assert mgr_a is not mgr_b, (
        "state.lists_mgr returned the same object across two accesses. "
        "Phase 89 D-01/D-02 require a fresh UserListsManager per access."
    )
    assert isinstance(mgr_a, UserListsManager)
    assert isinstance(mgr_b, UserListsManager)


def test_authenticated_fetch_does_not_leak_across_users(bootstrapped_state):
    """Cross-user fetch isolation — FACTORY-ACCESS case (LISTS-04).

    Two fresh ``state.lists_mgr`` accesses, one per user. With the cache
    deleted, each fresh manager's first ``_get_cached_data`` call invokes
    ``get_user_lists(user_id)`` directly. Pre-Phase-89 the 10s TTL cache
    on the shared singleton could return User A's data to User B within
    the TTL window; post-Phase-89 each fresh manager has no state to
    return.

    This test proves the FACTORY behavior across the user switch. The
    companion test ``test_captured_manager_does_not_serve_stale_data_after_user_switch``
    proves the CAPTURED-MANAGER case (D-03 — the actual review-flagged bug).
    """
    auth_state = {'user_id': 'user-A'}

    def fake_is_logged_in():
        return True

    def fake_get_user_id():
        return auth_state['user_id']

    with patch('web.user_lists.GlobalAuthState.is_logged_in', fake_is_logged_in), \
         patch('web.user_lists.GlobalAuthState.get_user_id', fake_get_user_id), \
         patch('web.user_lists.get_user_lists', side_effect=_fake_get_user_lists) as patched_lists, \
         patch('web.user_lists.get_projects', return_value=[]):

        # User A — freshly-accessed manager.
        mgr_a = bootstrapped_state.lists_mgr
        data_a = mgr_a.get_all_lists()
        a_names = {lst['name'] for lst in data_a}
        assert a_names == {'user-A-list-0'}, f"User A got {a_names}"

        # User B — freshly-accessed manager (different from mgr_a per D-01).
        auth_state['user_id'] = 'user-B'
        mgr_b = bootstrapped_state.lists_mgr
        assert mgr_b is not mgr_a, "factory contract broken"
        data_b = mgr_b.get_all_lists()
        b_names = {lst['name'] for lst in data_b}
        assert b_names == {'user-B-list-0'}, (
            f"CROSS-USER LEAK (factory case): User B received {b_names!r} "
            f"— expected only user-B-list-0. Pre-Phase-89 the cache could "
            f"return A's data here; post-Phase-89 must not."
        )

        # Both fetches hit Supabase — no cache short-circuit.
        assert patched_lists.call_count >= 2, (
            f"Expected at least 2 get_user_lists calls (one per user), "
            f"got {patched_lists.call_count}. A cache would short-circuit."
        )
        # Confirm distinct user_ids were used.
        called_user_ids = {call.args[0] for call in patched_lists.call_args_list}
        assert called_user_ids == {'user-A', 'user-B'}, (
            f"Supabase was called with {called_user_ids!r} — expected "
            f"both 'user-A' and 'user-B'."
        )


def test_captured_manager_does_not_serve_stale_data_after_user_switch():
    """Cross-user fetch isolation — CAPTURED-MANAGER case (D-03 / R1).

    This is the test that proves the actual D-03 review-flagged bug class:
    UI dialog callbacks capture ``UserListsManager`` references into closures
    (see web/components/add_to_list_dialog.py:84-243). The captured manager
    outlives the request that constructed it. Pre-Phase-89, per-instance
    ``_cache_entry`` memoization in such a captured manager would serve
    stale data indefinitely. Post-Phase-89, the manager is stateless and
    must produce fresh-per-call results even on the SAME instance.

    Test shape (per REVIEWS.md R1):
      1. Construct ONE ``mgr = UserListsManager(ListsManager(None), None)``
         directly (simulating capture into a closure — no factory re-access).
      2. Patch logged-in=True, user_id='user-A'. Call ``mgr.get_all_lists()``.
      3. Switch the user_id patch to 'user-B' WITHOUT touching ``mgr``.
      4. Call ``mgr.get_all_lists()`` again on the SAME ``mgr`` instance.
      5. Assert Supabase was called twice with distinct user_ids.

    Pre-Phase-89 this would have failed: step 4 would return cached
    user-A data because the (user_id='user-A', timestamp, data) cache
    tuple was still valid in mgr._cache_entry within the 10s TTL window.
    Post-Phase-89 there is no cache to memoize from.
    """
    mgr = UserListsManager(ListsManager(None), None)
    auth_state = {'user_id': 'user-A'}

    def fake_is_logged_in():
        return True

    def fake_get_user_id():
        return auth_state['user_id']

    with patch('web.user_lists.GlobalAuthState.is_logged_in', fake_is_logged_in), \
         patch('web.user_lists.GlobalAuthState.get_user_id', fake_get_user_id), \
         patch('web.user_lists.get_user_lists', side_effect=_fake_get_user_lists) as patched_lists, \
         patch('web.user_lists.get_projects', return_value=[]):

        # Call 1 — captured manager, User A.
        data_a = mgr.get_all_lists()
        a_names = {lst['name'] for lst in data_a}
        assert a_names == {'user-A-list-0'}, f"User A got {a_names}"

        # Switch user WITHOUT re-accessing state.lists_mgr — mgr is captured.
        auth_state['user_id'] = 'user-B'

        # Call 2 — SAME captured manager, User B.
        data_b = mgr.get_all_lists()
        b_names = {lst['name'] for lst in data_b}
        assert b_names == {'user-B-list-0'}, (
            f"CROSS-USER LEAK (captured-manager case): A captured "
            f"UserListsManager served {b_names!r} for User B — expected "
            f"only user-B-list-0. Pre-Phase-89 this was the D-03 bug class "
            f"(per-instance _cache_entry on captured dialog managers). "
            f"Post-Phase-89 the manager is stateless and must always fetch fresh."
        )

        # Both calls hit Supabase — no cache short-circuit on the captured mgr.
        assert patched_lists.call_count >= 2, (
            f"Expected at least 2 get_user_lists calls on the captured manager, "
            f"got {patched_lists.call_count}. A per-instance cache would "
            f"short-circuit the second call."
        )
        called_user_ids = [call.args[0] for call in patched_lists.call_args_list]
        assert called_user_ids[0] == 'user-A' and called_user_ids[-1] == 'user-B', (
            f"Supabase call order was {called_user_ids!r} — expected "
            f"first 'user-A', last 'user-B'."
        )


def test_invalidate_cache_is_compatibility_no_op(bootstrapped_state):
    """invalidate_cache() must remain callable as a no-op (D-05).

    ~10 internal mutation paths in UserListsManager (create_list, update_list,
    add_item, delete_list, etc.) call ``self.invalidate_cache()`` on success.
    Phase 89 reduced the method to ``pass`` so those call sites stay
    byte-unchanged; the method must still exist and must not raise.
    """
    mgr = bootstrapped_state.lists_mgr
    # Calling invalidate_cache() does not raise.
    mgr.invalidate_cache()
    # Subsequent .data access still works (returns a dict — default data
    # because the fixture's _local_lists_mgr is an empty ListsManager).
    data = mgr.data
    assert isinstance(data, dict)
    assert 'lists' in data
