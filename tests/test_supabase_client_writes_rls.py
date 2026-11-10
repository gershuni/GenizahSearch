# -*- coding: utf-8 -*-
"""Write helpers in web/supabase_client.py: authenticated client, and 0 rows is NOT success.

PostgREST answers an update/delete that RLS filtered to nothing with 200 and
``[]`` (postgrest 2.x defaults to ``returning=representation``, so
``response.data`` is exactly the changed rows). A helper that returns
``{'success': True}`` without looking at ``response.data`` therefore reports a
change that never happened. These tests pin both halves of the contract:

  * the write is built through the authenticated path (the same one
    ``get_user_client()`` uses: ``_apply_user_auth_to_client`` fires once with
    the stored access token) and never touches the anonymous singleton;
  * ``data=[]`` gives ``'error'`` (and ``no_rows``), ``data=[row]`` gives
    ``'success'``.

No network: ``create_client`` and ``get_client`` are replaced, and the storage
read goes to a SimpleNamespace, following tests/test_supabase_client_reader_rls.py.
The execute() leaves are configured explicitly -- a bare MagicMock's ``.data``
is truthy and would turn the zero-row tests falsely green.
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


def _stub(monkeypatch, rows):
    import web.supabase_client as mod
    _seed_logged_in_storage(monkeypatch)
    monkeypatch.setattr(mod, '_access_token_near_expiry', lambda _t: False)
    apply_mock = MagicMock(name='apply_user_auth')
    monkeypatch.setattr(mod, '_apply_user_auth_to_client', apply_mock)

    response = SimpleNamespace(data=rows)
    fake_client = MagicMock(name='fake_user_client')
    fake_client.table.return_value.delete.return_value.eq.return_value.execute.return_value = response
    fake_client.table.return_value.update.return_value.eq.return_value.execute.return_value = response
    monkeypatch.setattr(mod, 'create_client', lambda *a, **k: fake_client)

    anon = MagicMock(name='anonymous_singleton')
    anon.table.return_value.delete.return_value.eq.return_value.execute.return_value = response
    anon.table.return_value.update.return_value.eq.return_value.execute.return_value = response
    monkeypatch.setattr(mod, 'get_client', MagicMock(name='get_client', return_value=anon))
    return mod, apply_mock, fake_client, anon


def _assert_authenticated(apply_mock, anon):
    assert apply_mock.call_count == 1, apply_mock.mock_calls
    assert apply_mock.call_args.args[1] == 'good.future.jwt'
    assert not anon.table.called, "the anonymous singleton was used for a write"


def _check_zero_rows_is_an_error(monkeypatch, helper, table, arg):
    mod, apply_mock, fake_client, anon = _stub(monkeypatch, [])
    result = getattr(mod, helper)(arg)
    assert 'error' in result and 'success' not in result, result
    assert result.get('no_rows') is True, result
    _assert_authenticated(apply_mock, anon)
    fake_client.table.assert_called_with(table)
    fake_client.table.return_value.delete.return_value.eq.assert_called_with('id', arg)


@pytest.mark.parametrize('helper,arg', [
    ('delete_correction', 5),
    ('delete_comment', 7),
    ('delete_fragment_join', 3),
])
def test_delete_helper_with_a_changed_row_is_success(helper, arg, monkeypatch):
    """Positive control (green before and after): a returned row means it was deleted."""
    mod, apply_mock, fake_client, anon = _stub(monkeypatch, [{'id': arg}])
    result = getattr(mod, helper)(arg)
    assert result.get('success') is True and 'error' not in result, result
    _assert_authenticated(apply_mock, anon)


def test_delete_correction_zero_rows_is_an_error(monkeypatch):
    _check_zero_rows_is_an_error(monkeypatch, 'delete_correction', 'corrections', 5)


def test_delete_comment_zero_rows_is_an_error(monkeypatch):
    _check_zero_rows_is_an_error(monkeypatch, 'delete_comment', 'comments', 7)


def test_delete_fragment_join_zero_rows_is_an_error(monkeypatch):
    _check_zero_rows_is_an_error(monkeypatch, 'delete_fragment_join', 'fragment_joins', 3)


def test_update_comment_uses_user_client_and_checks_rows(monkeypatch):
    mod, apply_mock, fake_client, anon = _stub(monkeypatch, [])
    result = mod.update_comment(7, 'new text')
    assert 'error' in result and 'success' not in result, result
    assert result.get('no_rows') is True, result
    _assert_authenticated(apply_mock, anon)
    fake_client.table.assert_called_with('comments')
    fake_client.table.return_value.update.assert_called_with({'content': 'new text'})
    fake_client.table.return_value.update.return_value.eq.assert_called_with('id', 7)

    mod, apply_mock, fake_client, anon = _stub(monkeypatch, [{'id': 7, 'content': 'new text'}])
    result = mod.update_comment(7, 'new text')
    assert result.get('success') is True, result
    assert result.get('comment') == {'id': 7, 'content': 'new text'}
    _assert_authenticated(apply_mock, anon)


def test_write_helper_exception_is_an_error_without_no_rows(monkeypatch):
    """A raised client error is still an error, but it is not reported as '0 rows'."""
    mod, apply_mock, fake_client, anon = _stub(monkeypatch, [])
    fake_client.table.return_value.delete.return_value.eq.return_value.execute.side_effect = RuntimeError('boom')
    result = mod.delete_comment(7)
    assert 'error' in result and not result.get('no_rows'), result
