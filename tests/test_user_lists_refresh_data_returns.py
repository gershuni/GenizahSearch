# -*- coding: utf-8 -*-
"""Phase 92.2 Reviews Codex-HIGH 2 behavioral tests for
UserListsManager.refresh_data() return-type change (None -> Dict).

3 tests:
- Test 1: authenticated branch returns fetched data dict
- Test 2: local_mgr branch returns local_mgr.data
- Test 3: default branch returns default data dict

All 3 assert await mgr.refresh_data() is a dict (NOT None).

Uses asyncio.run() to avoid pytest-asyncio dependency (mirrors Phase 91 pattern).
"""

import asyncio
from unittest.mock import MagicMock, patch

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


def test_refresh_data_local_mgr_returns_local_data(monkeypatch):
    """Test 2: local_mgr branch returns local_mgr.data."""
    monkeypatch.setattr('web.auth_state.GlobalAuthState.is_logged_in', staticmethod(lambda: False))

    local_mgr_mock = MagicMock()
    local_mgr_mock.data = {'lists': {}, 'projects': {}, 'items': {}, 'recent': []}
    mgr = UserListsManager(local_mgr=local_mgr_mock)
    monkeypatch.setattr(mgr, 'invalidate_cache', lambda: None)

    result = asyncio.run(mgr.refresh_data())

    assert isinstance(result, dict), f"expected dict, got {type(result)}"
    assert result is local_mgr_mock.data, "local_mgr branch must return local_mgr.data"


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
