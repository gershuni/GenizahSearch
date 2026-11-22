# -*- coding: utf-8 -*-
"""/lists: "Add from the shelf" (D8; plan §10.13), driven through the real page callbacks.

The page is built over a MagicMock ``ui`` with a real ``UserListsManager`` and a faked
Supabase (the harness of tests/test_lists_page_item_page.py); the shelf picker is a stand-in
that records how it was opened, and the test confirms a choice as its confirm button does.

* The list shown gets what is chosen, in one ``add_items`` through ``run_lists_write``; what it
  already holds counts as taken and is not inserted again (``list_items`` has no unique key).
* All already there: nothing is written, all are taken.
* A failed insert takes nothing new (the choice stays); a failed read of the list takes nothing.
* At most LIST_ADD_MAX for each confirm; a refresh that fails after the insert still takes them.
* Not on the recent list; absent while the shelf tree is not served.
* The list's manuscripts are read page by page, and a failed page is a failed read.
"""
from __future__ import annotations

import asyncio
import types
from unittest.mock import AsyncMock

import pytest

import web.pages.lists as lists_page
from tests.test_lists_page_item_page import (  # noqa: F401  (fixtures)
    ORDINARY_LIST, RECENT_LIST, lang, page, signed_in,
)
from web.components import shelf_pick

TREE = object()
IN_LIST = '990000000000001'       # what the list already holds (sys_ids_in_list is stubbed)


class _Picker:
    def __init__(self):
        self.opened = []

    def __call__(self, container, service, on_pick=None, start=None, title=None, **multi):
        self.opened.append(multi)


@pytest.fixture
def shelf(monkeypatch, page):  # noqa: F811  (the imported fixture)
    stand_in = _Picker()
    monkeypatch.setattr(shelf_pick, 'ui', page.ui)
    monkeypatch.setattr(shelf_pick, 'open_shelf_picker', stand_in)
    monkeypatch.setattr(shelf_pick, 'shelf_tree_service', lambda: TREE)
    monkeypatch.setattr(shelf_pick, 'client_gone', lambda _element: False)
    mgr = lists_page.state.lists_mgr
    monkeypatch.setattr(mgr, 'sys_ids_in_list', AsyncMock(return_value={IN_LIST}))
    monkeypatch.setattr(mgr, 'add_items', AsyncMock(side_effect=lambda ids, list_id: len(ids)))
    return types.SimpleNamespace(picker=stand_in, mgr=mgr, ui=page.ui, select=page.select)


def _shelf_button_click(fake_ui):
    return [c.kwargs['on_click'] for c in fake_ui.button.call_args_list
            if c.args and c.args[0] == 'Add from the shelf']


def _open_and_confirm(shelf, sys_ids):
    shelf.select(ORDINARY_LIST)
    (click,) = _shelf_button_click(shelf.ui)
    click()
    opened = shelf.picker.opened[-1]
    assert (opened['mode'], opened['purpose'], opened['confirm_label']) == ('multi', 'lists', 'Add them to this list')
    entries = [{'sys_id': s, 'shelfmark': s, 'placement_id': f'inv:1:{s}'} for s in sys_ids]
    return asyncio.run(opened['on_confirm'](entries, 'chosen'))


def test_the_list_shown_gets_what_is_new(shelf):
    took = _open_and_confirm(shelf, [IN_LIST, '990000000000003', '990000000000004'])
    assert took == [IN_LIST, '990000000000003', '990000000000004']
    shelf.mgr.add_items.assert_awaited_once_with(['990000000000003', '990000000000004'], ORDINARY_LIST)
    shelf.mgr.sys_ids_in_list.assert_awaited_once_with(ORDINARY_LIST)


def test_all_already_there_writes_nothing_and_takes_them(shelf):
    assert _open_and_confirm(shelf, [IN_LIST]) == [IN_LIST]
    shelf.mgr.add_items.assert_not_awaited()


def test_a_failed_insert_takes_nothing_new(shelf):
    shelf.mgr.add_items.side_effect = lambda ids, list_id: 0
    assert _open_and_confirm(shelf, [IN_LIST, '990000000000003']) == [IN_LIST]
    assert 'negative' in [c.kwargs.get('type') for c in shelf.ui.notify.call_args_list]


def test_a_failed_read_of_the_list_takes_nothing(shelf):
    shelf.mgr.sys_ids_in_list.return_value = None
    assert _open_and_confirm(shelf, ['990000000000003']) == []
    shelf.mgr.add_items.assert_not_awaited()


def test_at_most_list_add_max_at_a_time(shelf, monkeypatch):
    monkeypatch.setattr(lists_page, 'LIST_ADD_MAX', 2)
    assert _open_and_confirm(shelf, ['990000000000003', '990000000000004', '990000000000005']) == ['990000000000003', '990000000000004']
    shelf.mgr.add_items.assert_awaited_once_with(['990000000000003', '990000000000004'], ORDINARY_LIST)


def test_a_refresh_that_fails_after_the_insert_still_takes_them(shelf, monkeypatch):
    def broken():
        raise RuntimeError('refresh failed')

    shelf.select(ORDINARY_LIST)                       # drawn before the refresh breaks
    monkeypatch.setattr(lists_page, '_load_list_item_counts', broken)   # async_refresh_ui raises
    (click,) = _shelf_button_click(shelf.ui)[-1:]
    click()
    entries = [{'sys_id': '990000000000003', 'shelfmark': 'x', 'placement_id': None}]
    assert asyncio.run(shelf.picker.opened[-1]['on_confirm'](entries, 'chosen')) == ['990000000000003']
    shelf.mgr.add_items.assert_awaited_once()


def test_not_on_the_recent_list(shelf):
    shelf.select(RECENT_LIST)
    assert _shelf_button_click(shelf.ui) == []


def test_absent_while_the_tree_is_not_served(shelf, monkeypatch):
    monkeypatch.setattr(shelf_pick, 'shelf_tree_service', lambda: None)
    shelf.select(ORDINARY_LIST)
    assert _shelf_button_click(shelf.ui) == []


# -- the read of a list's manuscripts ---------------------------------------------------------

class _PagedClient:
    def __init__(self, rows, fail_at=None):
        self.rows, self.fail_at, self.ranges = rows, fail_at, []

    def table(self, _name):
        client = self

        class _Q:
            def select(self, *_a):
                return self

            def eq(self, *_a):
                return self

            def order(self, *_a, **_k):
                return self

            def range(self, start, end):
                self.span = (start, end)
                return self

            def execute(self):
                client.ranges.append(self.span)
                if client.fail_at is not None and self.span[0] >= client.fail_at:
                    raise RuntimeError('network')
                return types.SimpleNamespace(data=client.rows[self.span[0]:self.span[1] + 1])

        return _Q()


def test_a_list_is_read_page_by_page_and_a_failed_page_is_a_failed_read():
    from web.supabase_client import get_list_item_sys_ids
    rows = [{'id': n, 'sys_id': f'99{n:08d}'} for n in range(5)]
    client = _PagedClient(rows)
    assert get_list_item_sys_ids(7, client=client, page_size=2) == {r['sys_id'] for r in rows}
    assert client.ranges == [(0, 1), (2, 3), (4, 5)]
    assert get_list_item_sys_ids(7, client=_PagedClient(rows, fail_at=2), page_size=2) is None
