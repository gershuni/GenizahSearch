# -*- coding: utf-8 -*-
"""Phase 92.2 Reviews Codex-HIGH 2 behavioral tests for
UserListsManager.refresh_data() return-type change (None -> Dict).

3 tests:
- Test 1: authenticated branch returns fetched data dict
- Test 2: anonymous refresh ignores a passed-in local_mgr (sweep C2)
- Test 3: default branch returns default data dict

All 3 assert await mgr.refresh_data() is a dict (NOT None).

Uses asyncio.run() to avoid pytest-asyncio dependency (mirrors Phase 91 pattern).
"""

import asyncio
from unittest.mock import MagicMock

from web.user_lists import UserListsManager


def test_refresh_data_authenticated_returns_dict(monkeypatch):
    """Test 1: authenticated branch returns _get_cached_data() result."""
    monkeypatch.setattr('web.auth_state.GlobalAuthState.is_logged_in', staticmethod(lambda: True))

    mgr = UserListsManager()
    fake_data = {'lists': {'1': {'name': 'Test'}}, 'projects': {}, 'items': {}, 'recent': []}
    monkeypatch.setattr(mgr, '_get_cached_data', lambda: fake_data)
    monkeypatch.setattr(mgr, 'invalidate_cache', lambda: None)

    result = asyncio.run(mgr.refresh_data())

    assert isinstance(result, dict), f"expected dict, got {type(result)}"
    assert result is fake_data, "authenticated branch must return _get_cached_data() result"


def test_refresh_data_anonymous_ignores_local_mgr(monkeypatch):
    """Test 2: anonymous refresh returns the default skeleton, never local data.

    This test used to assert ``result is local_mgr.data``, which pinned the
    server-wide anonymous store leak (improvement sweep C2). The local mgr is
    now ignored entirely, even when one is passed in.
    """
    monkeypatch.setattr('web.auth_state.GlobalAuthState.is_logged_in', staticmethod(lambda: False))

    local_mgr_mock = MagicMock()
    local_data = {'lists': {'leak': {'name': 'Leak'}}, 'projects': {}, 'items': {}, 'recent': []}
    local_mgr_mock.data = local_data
    mgr = UserListsManager(local_mgr=local_mgr_mock)
    monkeypatch.setattr(mgr, 'invalidate_cache', lambda: None)

    result = asyncio.run(mgr.refresh_data())

    assert isinstance(result, dict), f"expected dict, got {type(result)}"
    assert result is not local_data, "anonymous refresh returned the shared local store"
    assert 'leak' not in result.get('lists', {})
    assert result == mgr._get_default_data()
    assert local_mgr_mock.mock_calls == [], local_mgr_mock.mock_calls


def test_refresh_data_default_branch_returns_dict(monkeypatch):
    """Test 3: default branch (no auth, no local_mgr) returns _get_default_data()."""
    monkeypatch.setattr('web.auth_state.GlobalAuthState.is_logged_in', staticmethod(lambda: False))

    mgr = UserListsManager(local_mgr=None)
    monkeypatch.setattr(mgr, 'invalidate_cache', lambda: None)

    result = asyncio.run(mgr.refresh_data())

    assert isinstance(result, dict), f"expected dict, got {type(result)}"
    # Default data has lists + projects keys
    assert 'lists' in result, "default data must have 'lists' key"
    assert 'projects' in result, "default data must have 'projects' key"
