# -*- coding: utf-8 -*-
"""Phase 92.2 D-FANOUT-01 behavioral tests for data=None kwarg on
UserListsManager.get_list_display_color() and get_lists_by_project().

6 tests covering:
- Test 1: get_list_display_color with data kwarg skips _get_cached_data()
- Test 2: get_list_display_color without data kwarg calls _get_cached_data()
- Test 3: get_lists_by_project with data kwarg skips _get_cached_data()
- Test 4: get_lists_by_project without data kwarg calls _get_cached_data()
- Test 5: Semantic equivalence (same output threaded vs fetched)
- Test 6: anonymous callers never read a passed-in local_mgr (sweep C2)
"""

import pytest
from web.user_lists import UserListsManager


def _make_mgr(monkeypatch, is_authenticated=True, local_mgr=None):
    """Build a UserListsManager with mocked internals."""
    mgr = UserListsManager(local_mgr=local_mgr)
    # is_authenticated is a property delegating to GlobalAuthState.is_logged_in
    monkeypatch.setattr('web.auth_state.GlobalAuthState.is_logged_in', staticmethod(lambda: is_authenticated))
    return mgr


FAKE_DATA = {
    'lists': {
        '10': {'name': 'List A', 'project_id': '20', 'is_system': False},
        '11': {'name': 'List B', 'project_id': None, 'is_system': False},
        '12': {'name': 'Recently Viewed', 'is_system': True},
    },
    'projects': {
        '20': {'name': 'Project Alpha', 'color': '#4CAF50'},
    },
    'items': {},
    'recent': [],
}


def test_get_list_display_color_with_data_skips_fetch(monkeypatch):
    """Test 1: get_list_display_color(list_id, data=...) does NOT call _get_cached_data."""
    mgr = _make_mgr(monkeypatch, is_authenticated=True)
    monkeypatch.setattr(mgr, '_get_cached_data', lambda: pytest.fail('_get_cached_data must NOT be called when data= is provided'))

    result = mgr.get_list_display_color('10', data=FAKE_DATA)
    assert result == '#4CAF50', f"expected project color, got {result}"


def test_get_list_display_color_without_data_fetches(monkeypatch):
    """Test 2: get_list_display_color(list_id) (no data kwarg) calls _get_cached_data."""
    mgr = _make_mgr(monkeypatch, is_authenticated=True)
    called = [False]

    def fake_cached_data():
        called[0] = True
        return FAKE_DATA

    monkeypatch.setattr(mgr, '_get_cached_data', fake_cached_data)
    mgr.get_list_display_color('10')
    assert called[0], "_get_cached_data must be called when data=None (fallback path)"


def test_get_lists_by_project_with_data_skips_fetch(monkeypatch):
    """Test 3: get_lists_by_project(data=...) does NOT call _get_cached_data."""
    mgr = _make_mgr(monkeypatch, is_authenticated=True)
    monkeypatch.setattr(mgr, '_get_cached_data', lambda: pytest.fail('_get_cached_data must NOT be called when data= is provided'))

    result = mgr.get_lists_by_project(data=FAKE_DATA)
    # Project '20' should be present
    assert '20' in result, f"project '20' should be in result, got keys: {list(result.keys())}"


def test_get_lists_by_project_without_data_fetches(monkeypatch):
    """Test 4: get_lists_by_project() (no data kwarg) calls _get_cached_data."""
    mgr = _make_mgr(monkeypatch, is_authenticated=True)
    called = [False]

    def fake_cached_data():
        called[0] = True
        return FAKE_DATA

    monkeypatch.setattr(mgr, '_get_cached_data', fake_cached_data)
    mgr.get_lists_by_project()
    assert called[0], "_get_cached_data must be called when data=None (fallback path)"


def test_semantic_equivalence(monkeypatch):
    """Test 5: threaded data= produces identical output to fetched (fallback) path."""
    mgr = _make_mgr(monkeypatch, is_authenticated=True)
    monkeypatch.setattr(mgr, '_get_cached_data', lambda: FAKE_DATA)

    # With data=
    color_threaded = mgr.get_list_display_color('11', data=FAKE_DATA)
    by_proj_threaded = mgr.get_lists_by_project(data=FAKE_DATA)

    # Without data= (falls back to _get_cached_data which returns same data)
    color_fetched = mgr.get_list_display_color('11')
    by_proj_fetched = mgr.get_lists_by_project()

    assert color_threaded == color_fetched, "color must be identical threaded vs fetched"
    assert by_proj_threaded == by_proj_fetched, "by_project must be identical threaded vs fetched"


def test_anonymous_ignores_local_mgr_data(monkeypatch):
    """Test 6: anonymous callers get the default skeleton, never local_mgr.data.

    Formerly ``test_local_mgr_fallback_preserved``, which pinned the fallback
    to the shared local store (improvement sweep C2). The local mgr's .data
    must now never be read, even when one is passed in.
    """
    class _LocalMgr:
        def __init__(self):
            self.reads = 0

        @property
        def data(self):
            self.reads += 1
            return FAKE_DATA

    local_mgr = _LocalMgr()
    mgr = _make_mgr(monkeypatch, is_authenticated=False, local_mgr=local_mgr)

    color = mgr.get_list_display_color('11')
    # The default skeleton has no list '11', so it is treated as standalone.
    assert color == '#FFD700', f"standalone list should be gold, got {color}"

    by_proj = mgr.get_lists_by_project()
    assert None in by_proj, "standalone lists must appear under None key"
    assert [lst['id'] for lst in by_proj[None]] == ['default']
    assert '20' not in by_proj, "anonymous result carries the local store's project"
    assert local_mgr.reads == 0, "anonymous caller read local_mgr.data"
