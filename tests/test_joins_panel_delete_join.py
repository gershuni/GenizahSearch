# -*- coding: utf-8 -*-
"""Admin join delete (web/components/joins_panel.py::delete_join).

It used to delete through the anonymous ``get_client()`` and return True
regardless of the result, so RLS filtered the delete to 0 rows and the admin
saw 'Join deleted'. It now goes through ``delete_fragment_join`` (the
authenticated client plus a row check).

Isolation: BOTH ``web.components.joins_panel.get_client`` (bound by name at
import) and ``web.supabase_client.get_client`` are replaced by a sentinel, and
``create_client`` / ``_apply_user_auth_to_client`` are stubbed, so this test
can never reach a real Supabase project -- and never populates the real
``_client`` singleton.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

joins_panel = pytest.importorskip('web.components.joins_panel')


def _stub(monkeypatch, rows):
    import web.supabase_client as sb
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user={
            'auth_session': {'access_token': 'good.future.jwt', 'refresh_token': 'r'},
        })),
    )
    monkeypatch.setattr(sb, '_access_token_near_expiry', lambda _t: False)
    apply_mock = MagicMock(name='apply_user_auth')
    monkeypatch.setattr(sb, '_apply_user_auth_to_client', apply_mock)

    response = SimpleNamespace(data=rows)
    fake_client = MagicMock(name='fake_user_client')
    fake_client.table.return_value.delete.return_value.eq.return_value.execute.return_value = response
    monkeypatch.setattr(sb, 'create_client', lambda *a, **k: fake_client)

    anon = MagicMock(name='anonymous_singleton')
    anon.table.return_value.delete.return_value.eq.return_value.execute.return_value = response
    get_client_mock = MagicMock(name='get_client', return_value=anon)
    monkeypatch.setattr(sb, 'get_client', get_client_mock)
    monkeypatch.setattr(joins_panel, 'get_client', get_client_mock)
    return apply_mock, fake_client, anon


def test_delete_join_uses_user_client_and_reports_zero_rows(monkeypatch):
    apply_mock, fake_client, anon = _stub(monkeypatch, [])
    assert joins_panel.delete_join(3) is False
    assert not anon.table.called, "delete_join wrote through the anonymous singleton"
    assert apply_mock.call_count == 1
    fake_client.table.assert_called_with('fragment_joins')


def test_delete_join_with_a_deleted_row_returns_true(monkeypatch):
    apply_mock, fake_client, anon = _stub(monkeypatch, [{'id': 3}])
    assert joins_panel.delete_join(3) is True
    assert not anon.table.called
    assert apply_mock.call_count == 1
