# -*- coding: utf-8 -*-
"""The fragment-joins read uses an anonymous client with a short PostgREST timeout.

``get_fragment_joins`` used ``get_user_client()``: a token read from per-user
storage (which silently degrades to anonymous in a worker thread), a possible
networked token refresh, and the PostgREST default timeout of 120 s. Every
caller of the joins lookup now runs in a worker, so during a Supabase outage
each one could hold a thread of NiceGUI's shared pool for two minutes per
query.

fragment_joins and profiles are publicly readable (``SELECT USING (true)``,
supabase_setup.sql), so the read is made explicitly anonymous -- the same
result on the loop and in a worker -- through ``get_public_read_client()``, a
separate singleton built with ``SyncClientOptions(postgrest_client_timeout=5)``.
The ordinary ``get_client()`` keeps its default timeout.

No network: a REAL client is built from dummy credentials only to read its
timeout; the queries go to a MagicMock.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import web.supabase_client as sb


@pytest.fixture
def read_client_env(monkeypatch):
    monkeypatch.setattr(sb, 'SUPABASE_URL', 'https://example.supabase.co')
    monkeypatch.setattr(sb, 'SUPABASE_ANON_KEY', 'dummy-anon-key')
    if hasattr(sb, '_public_read_client'):
        monkeypatch.setattr(sb, '_public_read_client', None)
    # Never build or reuse the real anonymous singleton here.
    monkeypatch.setattr(sb, '_client', None)

    built = []
    real_create_client = sb.create_client
    response = SimpleNamespace(data=[{'id': 1, 'user_id': None,
                                      'fragment_a_sys_id': '99001', 'fragment_b_sys_id': '99002'}])
    fake = MagicMock(name='queries')
    fake.table.return_value.select.return_value.or_.return_value.order.return_value.execute.return_value = response

    def _create_client(url, key, *args, **kwargs):
        real = real_create_client(url, key, *args, **kwargs)
        built.append(real)
        return fake

    monkeypatch.setattr(sb, 'create_client', _create_client)
    user_client = MagicMock(name='get_user_client')
    monkeypatch.setattr(sb, 'get_user_client', user_client)
    # monkeypatch restores _client / _public_read_client / create_client afterwards.
    return built, fake, user_client


def test_get_fragment_joins_uses_a_short_timeout_anonymous_read_client(read_client_env):
    built, fake, user_client = read_client_env
    rows = sb.get_fragment_joins(fragment_sys_id='99001')
    assert not user_client.called, 'get_fragment_joins still builds a per-user client'
    assert rows and rows[0]['id'] == 1
    fake.table.assert_any_call('fragment_joins')
    assert len(built) == 1, 'expected exactly one client to be built: %r' % built
    timeout = built[0].postgrest.session.timeout
    assert timeout.read is not None and timeout.read <= 10, timeout


def test_public_read_client_is_a_reused_singleton(read_client_env):
    built, fake, user_client = read_client_env
    a = sb.get_public_read_client()
    b = sb.get_public_read_client()
    assert a is b
    assert len(built) == 1


def test_default_anonymous_client_timeout_is_unchanged(read_client_env):
    """Regression guard: only the joins read client is shortened, not get_client()."""
    built, fake, user_client = read_client_env
    sb.get_client()
    assert built[0].postgrest.session.timeout.read == 120
