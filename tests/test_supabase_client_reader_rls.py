# -*- coding: utf-8 -*-
"""Phase 92.1 READER-05 behavioral regression tests.

Each migrated reader, when called with an auth_session present in storage,
MUST build an authenticated client via the same code path get_user_client()
uses -- i.e. _apply_user_auth_to_client(client, access_token) fires once
with the access_token from storage. When auth_session is absent, the reader
falls back to the anonymous singleton (get_client) and never invokes
_apply_user_auth_to_client.

Pattern mirrors tests/test_auth_revocation_and_headers.py:149-203.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest


def _seed_logged_in_storage(monkeypatch):
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user={
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


def _install_common_stubs(mod, monkeypatch):
    """Stub the supabase plumbing so reads return empty data without network."""
    monkeypatch.setattr(mod, '_access_token_near_expiry', lambda _t: False)

    apply_mock = MagicMock()
    monkeypatch.setattr(mod, '_apply_user_auth_to_client', apply_mock)

    # create_client returns a MagicMock; reader's chained `.table(...).select(...).eq(...).execute()`
    # returns SimpleNamespace(data=[], count=0).
    fake_client = MagicMock(name='fake_client')
    fake_response = SimpleNamespace(data=[], count=0)
    # The chain is variadic per reader, so we configure the final .execute() return only.
    # MagicMock auto-creates intermediate attribute calls; just pin .execute on the deepest leaf.
    fake_client.table.return_value.select.return_value.eq.return_value.is_.return_value.order.return_value.execute.return_value = fake_response
    fake_client.table.return_value.select.return_value.eq.return_value.order.return_value.execute.return_value = fake_response
    fake_client.table.return_value.select.return_value.eq.return_value.not_.is_.return_value.order.return_value.execute.return_value = fake_response
    fake_client.table.return_value.select.return_value.eq.return_value.limit.return_value.execute.return_value = fake_response
    monkeypatch.setattr(mod, 'create_client', lambda *args, **kwargs: fake_client)

    # Reviews L1 (2026-05-17): get_client must be a MagicMock(return_value=anon_sentinel)
    # so the negative control can assert it WAS called (proving anon fallback fired).
    anon_sentinel = MagicMock(name='anonymous_singleton')
    # Configure the anon sentinel to also support the same chain so anonymous reads
    # don't crash before the assertion.
    anon_sentinel.table.return_value.select.return_value.eq.return_value.is_.return_value.order.return_value.execute.return_value = fake_response
    anon_sentinel.table.return_value.select.return_value.eq.return_value.order.return_value.execute.return_value = fake_response
    anon_sentinel.table.return_value.select.return_value.eq.return_value.not_.is_.return_value.order.return_value.execute.return_value = fake_response
    anon_sentinel.table.return_value.select.return_value.eq.return_value.limit.return_value.execute.return_value = fake_response
    get_client_mock = MagicMock(name='get_client', return_value=anon_sentinel)
    monkeypatch.setattr(mod, 'get_client', get_client_mock)

    return apply_mock, anon_sentinel, fake_client, get_client_mock


USER_ID = '00000000-0000-0000-0000-000000000001'


@pytest.mark.parametrize('reader_name,reader_args', [
    ('get_user_lists', {'user_id': USER_ID}),
    ('get_deleted_lists', {'user_id': USER_ID}),
    ('get_list_items', {'list_id': 1}),
    ('get_recent_items', {'user_id': USER_ID}),
    ('get_projects', {'user_id': USER_ID}),
])
def test_reader_builds_authenticated_client_when_logged_in(reader_name, reader_args, monkeypatch):
    import web.supabase_client as mod
    _seed_logged_in_storage(monkeypatch)
    apply_mock, anon_sentinel, fake_client, get_client_mock = _install_common_stubs(mod, monkeypatch)

    reader = getattr(mod, reader_name)
    reader(**reader_args)

    assert apply_mock.call_count == 1, (
        "Reader %r did NOT invoke _apply_user_auth_to_client -- it is still "
        "using the anonymous singleton. Phase 92.1 READER-01 regression: "
        "RLS-`TO authenticated` SELECT policy will return 0 rows for logged-in user. "
        "calls=%r" % (reader_name, apply_mock.mock_calls)
    )
    # Second positional arg is the access_token
    assert apply_mock.call_args.args[1] == 'good.future.jwt', (
        "Reader %r built an authenticated client but with the wrong access_token: %r"
        % (reader_name, apply_mock.call_args)
    )


def test_reader_falls_back_to_anonymous_when_no_auth_session(monkeypatch):
    """Negative control: anonymous browser path. get_user_client falls back to
    anon singleton at supabase_client.py:285; _apply_user_auth_to_client never fires,
    AND get_client IS called (the actual fallback path)."""
    import web.supabase_client as mod
    _seed_anonymous_storage(monkeypatch)
    apply_mock, anon_sentinel, fake_client, get_client_mock = _install_common_stubs(mod, monkeypatch)

    # Use get_user_lists as representative; same anonymous fallback path applies to all 5.
    mod.get_user_lists(user_id=USER_ID)

    assert apply_mock.call_count == 0, (
        "Anonymous fallback path invoked _apply_user_auth_to_client. "
        "get_user_client() must short-circuit to get_client() when no auth_session. "
        "calls=%r" % apply_mock.mock_calls
    )
    # Reviews L1 (2026-05-17): assert the anon fallback was ACTUALLY taken, not merely
    # that auth wasn't applied. A reader that crashed before the fallback would also have
    # apply_mock.call_count == 0; this stronger assertion proves get_client() was invoked.
    assert get_client_mock.called is True, (
        "Anonymous fallback path did NOT call get_client(). The reader may have crashed "
        "or short-circuited before reaching the anon fallback in get_user_client. "
        "get_client.call_args_list=%r" % get_client_mock.call_args_list
    )
