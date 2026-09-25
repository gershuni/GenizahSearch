# -*- coding: utf-8 -*-
"""Search page bulk "Add to List": re-checks sign-in and reports the real outcome.

The sign-in check ran only when the dialog opened. If the session ended while
it was open, every UserListsManager.add_item_sync() returned False, yet the
callbacks still toasted "0 items added to list" as a success and closed the
dialog. ``_bulk_add_items`` now re-checks sign-in, counts the writes that
succeeded, and toasts a failure (nothing added), a warning (some added) or a
success (all added); the dialog closes only when something was added.
"""
from __future__ import annotations

import ast
from pathlib import Path
from unittest.mock import MagicMock

import pytest

SEARCH_PY = Path(__file__).resolve().parent.parent / 'web' / 'pages' / 'search.py'


@pytest.fixture
def search_page(monkeypatch):
    mod = pytest.importorskip('web.pages.search')
    monkeypatch.setattr(mod, 'ui', MagicMock())
    return mod


def _signed_in(monkeypatch, value):
    from web.auth_state import GlobalAuthState
    monkeypatch.setattr(GlobalAuthState, 'is_logged_in', staticmethod(lambda: value))


def _toast_types(mod):
    return [c.kwargs.get('type') for c in mod.ui.notify.call_args_list]


class _Mgr:
    def __init__(self, ok_ids):
        self.ok_ids = set(ok_ids)
        self.calls = []

    def add_item_sync(self, sys_id, list_id, **kw):
        self.calls.append((sys_id, list_id))
        return sys_id in self.ok_ids


def test_signed_out_bulk_add_writes_nothing(monkeypatch, search_page):
    _signed_in(monkeypatch, False)
    mgr = _Mgr(ok_ids={'1', '2'})
    assert search_page._bulk_add_items(mgr, ['1', '2'], 'L') is None
    assert mgr.calls == []
    assert _toast_types(search_page) == ['warning']


def test_nothing_added_is_a_failure(monkeypatch, search_page):
    _signed_in(monkeypatch, True)
    mgr = _Mgr(ok_ids=set())
    assert search_page._bulk_add_items(mgr, ['1', '2'], 'L') == 0
    assert _toast_types(search_page) == ['negative']


def test_some_added_is_a_warning(monkeypatch, search_page):
    _signed_in(monkeypatch, True)
    mgr = _Mgr(ok_ids={'1'})
    assert search_page._bulk_add_items(mgr, ['1', '2', None, ''], 'L') == 1
    assert mgr.calls == [('1', 'L'), ('2', 'L')], 'empty sys_ids must be skipped'
    assert _toast_types(search_page) == ['warning']


def test_all_added_is_a_success(monkeypatch, search_page):
    _signed_in(monkeypatch, True)
    mgr = _Mgr(ok_ids={'1', '2'})
    assert search_page._bulk_add_items(mgr, ['1', '2'], 'L') == 2
    assert _toast_types(search_page) == ['positive']


def _bulk_add_closures():
    tree = ast.parse(SEARCH_PY.read_text(encoding='utf-8'))
    outer = next(n for n in ast.walk(tree)
                 if isinstance(n, ast.FunctionDef) and n.name == 'bulk_add_to_list')
    return {n.name: n for n in ast.walk(outer) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}


def _calls(fn, name):
    return [n for n in ast.walk(fn) if isinstance(n, ast.Call)
            and ((isinstance(n.func, ast.Name) and n.func.id == name)
                 or (isinstance(n.func, ast.Attribute) and n.func.attr == name))]


@pytest.mark.parametrize('closure', ['add_all', 'create_and_add_all'])
def test_bulk_add_callbacks_route_through_the_counted_helper(closure):
    fn = _bulk_add_closures()[closure]
    assert not _calls(fn, 'add_item_sync'), f'{closure} still calls add_item_sync directly'
    assert _calls(fn, '_bulk_add_items'), f'{closure} does not use _bulk_add_items'
    closes = _calls(fn, 'close')
    assert closes, f'{closure} never closes the dialog'
    # Every dialog.close() sits under an `if` (only close when something was added).
    tree_parents = {}
    for node in ast.walk(fn):
        for child in ast.iter_child_nodes(node):
            tree_parents[child] = node
    for call in closes:
        node, guarded = call, False
        while node in tree_parents:
            node = tree_parents[node]
            if isinstance(node, ast.If):
                guarded = True
                break
        assert guarded, f'{closure}: dialog.close() at line {call.lineno} runs unconditionally'


def test_create_and_add_rechecks_sign_in_before_creating_the_list():
    fn = _bulk_add_closures()['create_and_add_all']
    login = [c.lineno for c in _calls(fn, 'is_logged_in')]
    create = [c.lineno for c in _calls(fn, 'create_list_sync')]
    assert create, 'create_and_add_all no longer creates the list'
    assert login and min(login) < min(create), 'sign-in is not re-checked before create_list_sync'
