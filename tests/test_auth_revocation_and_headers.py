"""Mocked unit tests for Plan 90-01 auth boundary behaviors.

Codex review round 1 M3: three behaviors that lacked direct test
coverage in the original plan:
  - admin.sign_out(access_token, "global") is the actual revocation path
  - change_password sends all four required headers on the PUT
  - _apply_user_auth_to_client applies the access token to ALL THREE
    sub-clients (PostgREST + functions + storage)

All tests are pure-mock -- no network, no NiceGUI context required.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock, call


def test_sign_out_calls_admin_global_revocation(monkeypatch):
    """sign_out("jwt") must call throwaway.auth.admin.sign_out(jwt, "global")."""
    import web.supabase_client as mod

    mock_client = MagicMock()
    mock_client.auth.admin.sign_out = MagicMock(return_value=None)

    def fake_create_client(url, key, options=None):
        return mock_client

    monkeypatch.setattr(mod, 'create_client', fake_create_client)

    result = mod.sign_out(access_token='test-access-jwt')

    # Body succeeded
    assert result.get('success') is True, result
    # The revocation call landed with the correct args
    assert mock_client.auth.admin.sign_out.call_args == call('test-access-jwt', 'global'), (
        f"Expected admin.sign_out('test-access-jwt', 'global'); "
        f"got {mock_client.auth.admin.sign_out.call_args}"
    )


def test_sign_out_with_none_token_returns_noop_success(monkeypatch):
    """sign_out(None) with empty storage returns {'success': True, 'note': ...} without calling admin.sign_out."""
    import web.supabase_client as mod
    # Stub safe_storage so safe_user_get returns no auth_session
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user={})),
    )
    mock_client = MagicMock()

    def fake_create_client(url, key, options=None):
        return mock_client

    monkeypatch.setattr(mod, 'create_client', fake_create_client)

    result = mod.sign_out(access_token=None)
    assert result == {'success': True, 'note': 'no active session to revoke'}, result
    # admin.sign_out should NOT have been called
    assert mock_client.auth.admin.sign_out.call_count == 0


def test_change_password_sends_four_headers(monkeypatch):
    """change_password must include all four headers: apikey, Authorization, Content-Type, Accept."""
    import web.supabase_client as mod
    from web import supabase_client as sc

    # Seed safe_storage stub with an auth_session
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user={
            'auth_session': {
                'access_token': 'user-access-jwt',
                'refresh_token': 'user-refresh-token',
            },
        })),
    )

    captured = {}

    def fake_put(url, headers=None, json=None, timeout=None):
        captured['url'] = url
        captured['headers'] = headers
        captured['json'] = json
        response = MagicMock()
        response.status_code = 200
        response.json = lambda: {'id': 'user-id-1'}
        return response

    # change_password imports httpx INSIDE the function body, so patch the
    # module-level httpx.put attribute (importing httpx is unconditional).
    import httpx
    monkeypatch.setattr(httpx, 'put', fake_put)

    result = mod.change_password('new-secret-pw')

    assert result.get('success') is True, result
    # URL is the GoTrue user endpoint
    assert captured['url'].endswith('/auth/v1/user'), captured['url']
    # All FOUR headers present with expected values
    headers = captured['headers']
    assert headers is not None
    assert set(headers.keys()) >= {'apikey', 'Authorization', 'Content-Type', 'Accept'}, (
        f"Expected at least 4 keys (apikey, Authorization, Content-Type, Accept); "
        f"got {sorted(headers.keys())}"
    )
    assert headers['apikey'] == sc.SUPABASE_ANON_KEY
    assert headers['Authorization'] == 'Bearer user-access-jwt'
    assert headers['Content-Type'] == 'application/json'
    assert headers['Accept'] == 'application/json'
    # JSON body carries the new password
    assert captured['json'] == {'password': 'new-secret-pw'}


def test_change_password_returns_error_when_not_logged_in(monkeypatch):
    """change_password with no auth_session returns {'error': 'Not logged in'}."""
    import web.supabase_client as mod
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user={})),
    )
    result = mod.change_password('new-pw')
    assert result == {'error': 'Not logged in'}, result


def test_apply_user_auth_sets_storage_header():
    """_apply_user_auth_to_client must apply token to PostgREST, functions, AND storage."""
    from web.supabase_client import _apply_user_auth_to_client

    mock_client = MagicMock()
    # storage.session.headers must be a dict-like that records __setitem__
    mock_client.storage.session.headers = MagicMock()

    _apply_user_auth_to_client(mock_client, 'test-jwt')

    # All three sub-clients received the token
    assert mock_client.postgrest.auth.call_args == call('test-jwt'), (
        f"Expected postgrest.auth('test-jwt'); got {mock_client.postgrest.auth.call_args}"
    )
    assert mock_client.functions.set_auth.call_args == call('test-jwt'), (
        f"Expected functions.set_auth('test-jwt'); got {mock_client.functions.set_auth.call_args}"
    )
    # Storage header is the load-bearing one Codex F1 caught
    assert mock_client.storage.session.headers.__setitem__.call_args == call(
        'Authorization', 'Bearer test-jwt',
    ), (
        f"Expected storage.session.headers['Authorization'] = 'Bearer test-jwt'; "
        f"got {mock_client.storage.session.headers.__setitem__.call_args}"
    )


def test_get_user_client_returns_anonymous_when_refresh_fails(monkeypatch):
    """R3-M1: get_user_client() must short-circuit to the anonymous singleton when
    _refresh_user_session returns False (terminal refresh failure or missing
    persisted UUID). It must NOT build an authenticated client with the stale
    near-expiry token.
    """
    import web.supabase_client as mod

    # Seed storage with a near-expiry access token so the proactive-refresh
    # branch fires. We use a synthetic JWT whose `exp` claim is already in
    # the past -- _access_token_near_expiry will return True (treat any
    # malformed/expired JWT as expired per Helper 2's defensive fallback).
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user={
            'auth_session': {
                'access_token': 'stale.expired.jwt',
                'refresh_token': 'stale-refresh-token',
            },
        })),
    )

    # Mock _refresh_user_session to return False (refresh failed / skipped).
    refresh_mock = MagicMock(return_value=False)
    monkeypatch.setattr(mod, '_refresh_user_session', refresh_mock)

    # Mock _apply_user_auth_to_client so we can assert it was NEVER called.
    apply_mock = MagicMock()
    monkeypatch.setattr(mod, '_apply_user_auth_to_client', apply_mock)

    # Mock get_client to return a sentinel anonymous client.
    anon_sentinel = MagicMock(name='anonymous_singleton')
    monkeypatch.setattr(mod, 'get_client', lambda: anon_sentinel)

    client = mod.get_user_client()

    # The function returned the anonymous singleton, not an authenticated
    # client built from the stale token.
    assert client is anon_sentinel, (
        f"Expected anonymous singleton; got {client!r} "
        f"(R3-M1: refresh returned False, must short-circuit)"
    )
    # Refresh was attempted exactly once with the stale token snapshot.
    assert refresh_mock.call_count == 1, refresh_mock.call_count
    assert refresh_mock.call_args == call(stale_refresh_token='stale-refresh-token'), (
        refresh_mock.call_args
    )
    # CRITICAL: _apply_user_auth_to_client was NEVER called -- we did NOT
    # build an authenticated client with the stale token. This is the
    # load-bearing invariant of R3-M1.
    assert apply_mock.call_count == 0, (
        f"Expected _apply_user_auth_to_client to NEVER be called when "
        f"refresh returns False; got {apply_mock.call_count} calls. "
        f"R3-M1 invariant violated."
    )
