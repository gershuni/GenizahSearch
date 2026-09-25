# -*- coding: utf-8 -*-
"""List write helpers report "nothing changed" and trash errors honestly.

``web/pages/lists.py`` now shows a success message only when the manager says
the write worked (``_run_lists_write``). That is only as honest as the helpers
underneath:

* ``delete_list`` (soft and permanent) and ``delete_list_item`` discarded the
  PostgREST response and always returned ``{'success': True}``; a list deleted
  in another tab, or a row RLS filtered away, still read as deleted.
* ``empty_trash`` listed the trash through ``get_deleted_lists``, which turns a
  read error into ``[]``, and ``UserListsManager.empty_trash`` turned any
  error into ``0`` -- so a failed Empty Trash read as "Deleted 0 lists".

Now 0 changed rows is ``{'error': ..., 'no_rows': True}``, Empty Trash counts
only the rows it really deleted and reports a read or partial failure as an
error, and ``UserListsManager.empty_trash`` returns ``None`` (not ``0``) for a
failure. No network: the client is a MagicMock whose ``execute()`` leaves are
configured explicitly (a bare MagicMock's ``.data`` is truthy).
"""
from types import SimpleNamespace
from unittest.mock import MagicMock

import asyncio

import pytest


def _stub(monkeypatch):
    import web.supabase_client as mod
    monkeypatch.setattr(
        'web.safe_storage.app',
        SimpleNamespace(storage=SimpleNamespace(user={
            'auth_session': {'access_token': 'good.future.jwt', 'refresh_token': 'r'},
        })),
    )
    monkeypatch.setattr(mod, '_access_token_near_expiry', lambda _t: False)
    monkeypatch.setattr(mod, '_apply_user_auth_to_client', MagicMock(name='apply_user_auth'))
    client = MagicMock(name='fake_user_client')
    monkeypatch.setattr(mod, 'create_client', lambda *a, **k: client)
    monkeypatch.setattr(mod, 'get_client', MagicMock(name='get_client'))
    return mod, client


def _rows(client, *, update=None, delete=None):
    tbl = client.table.return_value
    if update is not None:
        tbl.update.return_value.eq.return_value.execute.return_value = SimpleNamespace(data=update)
    if delete is not None:
        tbl.delete.return_value.eq.return_value.execute.return_value = SimpleNamespace(data=delete)


def _trash(client, rows=None, error=None):
    chain = client.table.return_value.select.return_value.eq.return_value.not_.is_.return_value
    if error is not None:
        chain.execute.side_effect = error
        chain.order.return_value.execute.side_effect = error
    else:
        chain.execute.return_value = SimpleNamespace(data=rows)
        chain.order.return_value.execute.return_value = SimpleNamespace(data=rows)


@pytest.mark.parametrize('rows,ok', [([], False), ([{'id': 5}], True)])
def test_soft_delete_list_reports_no_rows(monkeypatch, rows, ok):
    mod, client = _stub(monkeypatch)
    _rows(client, update=rows)
    result = mod.delete_list(5)
    assert ('success' in result) is ok, result
    if not ok:
        assert result.get('no_rows') is True


@pytest.mark.parametrize('rows,ok', [([], False), ([{'id': 5}], True)])
def test_permanent_delete_list_reports_no_rows(monkeypatch, rows, ok):
    mod, client = _stub(monkeypatch)
    _rows(client, delete=rows)
    result = mod.delete_list(5, permanent=True)
    assert ('success' in result) is ok, result
    if not ok:
        assert result.get('no_rows') is True


@pytest.mark.parametrize('rows,ok', [([], False), ([{'id': 9}], True)])
def test_delete_list_item_reports_no_rows(monkeypatch, rows, ok):
    mod, client = _stub(monkeypatch)
    _rows(client, delete=rows)
    result = mod.delete_list_item(9)
    assert ('success' in result) is ok, result
    if not ok:
        assert result.get('no_rows') is True


def test_empty_trash_read_failure_is_an_error_not_zero(monkeypatch):
    mod, client = _stub(monkeypatch)
    _trash(client, error=RuntimeError('connection reset'))
    result = mod.empty_trash('u1')
    assert 'error' in result and 'success' not in result, result


def test_empty_trash_counts_the_rows_it_really_deleted(monkeypatch):
    mod, client = _stub(monkeypatch)
    _trash(client, rows=[{'id': 1}, {'id': 2}])
    _rows(client, delete=[{'id': 1}])
    result = mod.empty_trash('u1')
    assert result == {'success': True, 'deleted_count': 2}, result


def test_empty_trash_with_undeleted_rows_is_an_error(monkeypatch):
    mod, client = _stub(monkeypatch)
    _trash(client, rows=[{'id': 1}, {'id': 2}])
    _rows(client, delete=[])
    result = mod.empty_trash('u1')
    assert 'error' in result and 'success' not in result, result
    assert result.get('deleted_count') == 0


def test_an_empty_trash_is_a_success_with_zero(monkeypatch):
    mod, client = _stub(monkeypatch)
    _trash(client, rows=[])
    assert mod.empty_trash('u1') == {'success': True, 'deleted_count': 0}


@pytest.fixture
def signed_in_manager(monkeypatch):
    from web.user_lists import UserListsManager
    monkeypatch.setattr(UserListsManager, 'is_authenticated', property(lambda self: True))
    monkeypatch.setattr(UserListsManager, 'user_id', property(lambda self: 'u1'))
    mgr = UserListsManager(None, None)
    monkeypatch.setattr(mgr, 'invalidate_cache', lambda: None, raising=False)
    return mgr


def test_manager_empty_trash_failure_is_none_not_zero(monkeypatch, signed_in_manager):
    monkeypatch.setattr('web.supabase_client.empty_trash', lambda uid: {'error': 'boom'})
    assert asyncio.run(signed_in_manager.empty_trash()) is None


def test_manager_empty_trash_success_returns_the_count(monkeypatch, signed_in_manager):
    monkeypatch.setattr('web.supabase_client.empty_trash',
                        lambda uid: {'success': True, 'deleted_count': 3})
    assert asyncio.run(signed_in_manager.empty_trash()) == 3


def test_manager_empty_trash_signed_out_is_none(monkeypatch):
    from web.user_lists import UserListsManager
    monkeypatch.setattr(UserListsManager, 'is_authenticated', property(lambda self: False))
    assert asyncio.run(UserListsManager(None, None).empty_trash()) is None
