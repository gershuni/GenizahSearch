# -*- coding: utf-8 -*-
"""Phase 92.2 D-FANOUT-02 behavioral tests for the zero-arg get_list_item_counts()
RPC reader in web/supabase_client.py.

6 tests:
- Test 1: authenticated call invokes _apply_user_auth_to_client + rpc('get_list_item_counts_for_user', {})
- Test 2: return shape {list_id: count} from rpc rows
- Test 3: empty/None response returns {} (valid batched empty result, not None)
- Test 4: RE-RAISES on non-JWT exception (Reviews MUST-FIX 1)
- Test 5: JWT-expired retry: refresh_session + retry; RE-RAISES on non-JWT after retry
- Test 6: anonymous fallback negative control: RE-RAISES permission-denied
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import web.supabase_client as mod


def _seed_logged_in_storage(monkeypatch):
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user={
            '_session_uuid': 'abcd1234efab5678abcd1234efab5678',
            'auth_session': {
                'access_token': 'good.future.jwt',
                'refresh_token': 'good-refresh-token',
            },
        })),
    )


def _seed_anonymous_storage(monkeypatch):
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user={})),
    )


def _install_common_stubs(m, monkeypatch):
    """Stub supabase plumbing so the RPC reader runs without network."""
    monkeypatch.setattr(m, '_access_token_near_expiry', lambda _t: False)

    apply_mock = MagicMock(name='apply_user_auth')
    monkeypatch.setattr(m, '_apply_user_auth_to_client', apply_mock)

    fake_client = MagicMock(name='fake_client')
    fake_rpc_response = SimpleNamespace(data=[
        {'list_id': 1, 'item_count': 3},
        {'list_id': 7, 'item_count': 0},
    ])
    fake_client.rpc.return_value.execute.return_value = fake_rpc_response

    monkeypatch.setattr(m, 'create_client', lambda *a, **kw: fake_client)

    anon_sentinel = MagicMock(name='anonymous_singleton')
    get_client_mock = MagicMock(return_value=anon_sentinel)
    monkeypatch.setattr(m, 'get_client', get_client_mock)

    return apply_mock, anon_sentinel, fake_client, get_client_mock


def test_get_list_item_counts_authenticated_invokes_rpc(monkeypatch):
    """Test 1: authenticated call invokes _apply_user_auth_to_client + correct rpc call."""
    _seed_logged_in_storage(monkeypatch)
    apply_mock, _, fake_client, _ = _install_common_stubs(mod, monkeypatch)

    result = mod.get_list_item_counts()

    apply_mock.assert_called_once()
    fake_client.rpc.assert_called_once_with('get_list_item_counts_for_user', {})
    assert result == {1: 3, 7: 0}


def test_get_list_item_counts_return_shape(monkeypatch):
    """Test 2: return shape is Dict[int, int] from rpc rows."""
    _seed_logged_in_storage(monkeypatch)
    _, _, fake_client, _ = _install_common_stubs(mod, monkeypatch)

    result = mod.get_list_item_counts()

    assert isinstance(result, dict)
    assert result[1] == 3
    assert result[7] == 0


def test_get_list_item_counts_empty_response_returns_empty_dict(monkeypatch):
    """Test 3: empty/None data returns {} (valid batched empty result per Reviews MUST-FIX 1).

    {} means 'RPC succeeded but user has no items' — NOT an error.
    The legacy fallback is triggered only by None (which signals RPC failure/anon).
    """
    _seed_logged_in_storage(monkeypatch)
    _, _, fake_client, _ = _install_common_stubs(mod, monkeypatch)

    # Set empty data
    fake_client.rpc.return_value.execute.return_value = SimpleNamespace(data=[])

    result = mod.get_list_item_counts()
    assert result == {}, f"empty response must return {{}} not None, got {result!r}"


def test_get_list_item_counts_raises_on_non_jwt_error(monkeypatch):
    """Test 4: RE-RAISES on non-JWT exception (Reviews MUST-FIX 1).

    This is the pivot point: the helper must NOT swallow to {} — the caller
    (_load_list_item_counts in lists.py) catches and sets counts=None which
    triggers the per-list legacy fallback. If helper returned {} instead,
    the caller would see valid batched empty result and never fallback.
    """
    _seed_logged_in_storage(monkeypatch)
    _, _, fake_client, _ = _install_common_stubs(mod, monkeypatch)

    fake_client.rpc.return_value.execute.side_effect = Exception("postgrest 500")
    monkeypatch.setattr(mod, '_is_jwt_expired', lambda e: False)

    with pytest.raises(Exception, match="postgrest 500"):
        mod.get_list_item_counts()


def test_get_list_item_counts_jwt_expired_retry(monkeypatch):
    """Test 5: JWT-expired error triggers refresh + retry."""
    _seed_logged_in_storage(monkeypatch)
    _, _, fake_client, _ = _install_common_stubs(mod, monkeypatch)

    call_count = [0]
    def execute_side_effect():
        call_count[0] += 1
        if call_count[0] == 1:
            raise Exception("JWT expired")
        return SimpleNamespace(data=[{'list_id': 5, 'item_count': 2}])

    fake_client.rpc.return_value.execute.side_effect = execute_side_effect

    # Make first call trigger JWT-expired detection, second succeed
    is_jwt_calls = [0]
    def mock_is_jwt_expired(e):
        is_jwt_calls[0] += 1
        return is_jwt_calls[0] == 1  # True only on first call

    monkeypatch.setattr(mod, '_is_jwt_expired', mock_is_jwt_expired)
    refresh_mock = MagicMock(return_value=True)
    monkeypatch.setattr(mod, '_refresh_user_session', refresh_mock)

    result = mod.get_list_item_counts()

    refresh_mock.assert_called_once()
    assert call_count[0] == 2, f"expected 2 execute() calls (initial + retry), got {call_count[0]}"
    assert result == {5: 2}


def test_get_list_item_counts_anonymous_raises(monkeypatch):
    """Test 6: anonymous fallback negative control — RE-RAISES permission-denied.

    The anonymous role has NO EXECUTE grant (Reviews MUST-FIX 2 — REVOKE ALL FROM PUBLIC, anon).
    When called without auth, the helper RE-RAISES so the caller sets counts=None
    and triggers per-list legacy fetch.
    """
    _seed_anonymous_storage(monkeypatch)
    _, _, _, get_client_mock = _install_common_stubs(mod, monkeypatch)

    anon_client = MagicMock(name='anon_client')
    anon_client.rpc.return_value.execute.side_effect = Exception(
        "permission denied for function get_list_item_counts_for_user"
    )
    # In anonymous path, get_user_client() falls back to get_client() anon singleton
    get_client_mock.return_value = anon_client

    monkeypatch.setattr(mod, '_is_jwt_expired', lambda e: False)

    with pytest.raises(Exception, match="permission denied"):
        mod.get_list_item_counts()
