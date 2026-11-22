# -*- coding: utf-8 -*-
"""Where the website keeps a pending choice on the shelf (web/shelf_selection_store.py, D12) and
how it is saved as a list (web/components/shelf_selection_lists.py); picker PR 4.

The store keys a choice by purpose and by the account it was made under, so one account never
reads another's; it keeps chips only, clears an empty selection and reads a broken value as
empty. Saving as a list goes through ``run_lists_write`` for both writes, and a list made whose
items were not added is reported so a retry fills it instead of making another.
"""
from __future__ import annotations

import asyncio

import pytest

import web.components.shelf_selection_lists as lists_mod
import web.shelf_selection_store as store
from shared.shelfmark_tree_selection import Chip, Selection


@pytest.fixture
def storage(monkeypatch):
    data: dict = {}
    user = {'id': None}
    monkeypatch.setattr(store, 'safe_user_get', lambda key, default=None: data.get(key, default))

    def _set(key, value):
        data[key] = value
        return True

    monkeypatch.setattr(store, 'safe_user_set', _set)
    def _remove(key):
        data.pop(key, None)
        return True

    monkeypatch.setattr(store, 'safe_user_remove', _remove)
    monkeypatch.setattr(store.GlobalAuthState, 'get_user_id', classmethod(lambda cls: user['id']))
    return data, user


def _one(ref='vol:100'):
    return Selection([Chip('node', ref, ref)])


def test_a_choice_is_kept_per_purpose_and_per_account(storage):
    data, user = storage
    assert store.current_owner() == 'anon'
    store.save_selection('search_exclude', 'anon', _one())
    user['id'] = 'u1'
    assert store.current_owner() == 'u1'
    assert len(store.load_selection('search_exclude', 'u1')) == 0          # another account's is not read
    store.save_selection('search_exclude', 'u1', _one('vol:101'))
    store.save_selection('reading_desk', 'u1', _one('vol:110'))
    assert [c.ref for c in store.load_selection('search_exclude', 'anon').chips] == ['vol:100']
    assert [c.ref for c in store.load_selection('search_exclude', 'u1').chips] == ['vol:101']
    assert [c.ref for c in store.load_selection('reading_desk', 'u1').chips] == ['vol:110']
    assert set(data) == {'shelf_selection:search_exclude:anon', 'shelf_selection:search_exclude:u1',
                         'shelf_selection:reading_desk:u1'}
    # chips only: a node is kept by its id, never by its manuscripts
    assert data['shelf_selection:search_exclude:u1']['chips'] == [
        {'kind': 'node', 'ref': 'vol:101', 'label': 'vol:101', 'scope': 'all', 'removed': []}]


def test_an_empty_selection_is_cleared_and_a_broken_value_reads_empty(storage):
    data, _user = storage
    store.save_selection('search_exclude', 'anon', _one())
    store.save_selection('search_exclude', 'anon', Selection())
    assert data == {}
    data['shelf_selection:search_exclude:anon'] = {'format': 'something else'}
    assert len(store.load_selection('search_exclude', 'anon')) == 0


@pytest.mark.parametrize('purpose', ['', 'Exclude', 'a:b', 'x' * 40, '../x'])
def test_a_purpose_is_a_plain_name(storage, purpose):
    with pytest.raises(ValueError):
        store.load_selection(purpose, 'anon')


class _Lists:
    """A lists manager over one list whose rows are kept (``rows``: one per insert, as
    ``list_items`` has no unique key)."""

    def __init__(self, added=None):
        self.calls = []
        self._added = added
        self.rows = []
        self.lose_answer = False      # the insert is made, but its answer never arrives

    async def create_list(self, name):
        self.calls.append(('create', name))
        return 42

    async def add_items(self, ids, list_id):
        self.calls.append(('add', tuple(ids), list_id))
        if self._added == 0:
            return 0
        self.rows += list(ids)
        if self.lose_answer:
            raise ConnectionError('the answer was lost')
        return len(ids) if self._added is None else self._added

    async def sys_ids_in_list(self, list_id):
        self.calls.append(('read', list_id))
        return set(self.rows)


@pytest.fixture
def writes(monkeypatch):
    seen = []

    async def _run(write, **_kw):
        seen.append(write)
        result = await write()
        return result or None

    monkeypatch.setattr(lists_mod, 'run_lists_write', _run)
    return seen


def test_saved_as_a_list_through_run_lists_write(writes):
    mgr = _Lists()
    saved = asyncio.run(lists_mod.save_selection_as_list(mgr, ' T-S AS 2 ', ['a', 'b', 'a', '']))
    assert saved.list_id == '42' and saved.complete and saved.added == 2
    assert mgr.calls == [('create', 'T-S AS 2'), ('add', ('a', 'b'), '42')]
    assert len(writes) == 2                                  # both writes went through the one wrapper


def test_a_list_made_but_not_filled_is_reported_and_filled_on_the_retry(writes):
    mgr = _Lists(added=0)
    saved = asyncio.run(lists_mod.save_selection_as_list(mgr, 'X', ['a', 'b']))
    assert saved.list_id == '42' and not saved.complete
    mgr._added = None
    again = asyncio.run(lists_mod.save_selection_as_list(mgr, 'X', ['a', 'b'], list_id=saved.list_id))
    assert again.complete
    assert [c[0] for c in mgr.calls] == ['create', 'add', 'read', 'add']  # one list, not two
    assert sorted(mgr.rows) == ['a', 'b']


def test_a_retry_after_a_lost_answer_puts_each_manuscript_in_once(monkeypatch):
    # Codex on #73: the insert was made, its answer lost; the retry into the same list must not
    # insert them again. Through the real run_lists_write, which takes the lost answer for a failure.
    import web.components.lists_write as runner
    from unittest.mock import MagicMock
    monkeypatch.setattr(runner.GlobalAuthState, 'is_logged_in', classmethod(lambda cls: True))
    monkeypatch.setattr(runner, 'ui', MagicMock())
    mgr = _Lists()
    mgr.lose_answer = True
    saved = asyncio.run(lists_mod.save_selection_as_list(mgr, 'X', ['a', 'b', 'c']))
    assert saved.list_id == '42' and not saved.complete                    # reported as not added
    mgr.lose_answer = False
    again = asyncio.run(lists_mod.save_selection_as_list(mgr, 'X', ['a', 'b', 'c'], list_id=saved.list_id))
    assert again.complete
    assert sorted(mgr.rows) == ['a', 'b', 'c']                             # each once
    assert [c for c in mgr.calls if c[0] == 'add'] == [('add', ('a', 'b', 'c'), '42')]


def test_a_retry_adds_only_what_the_list_lacks_and_takes_nothing_unread(monkeypatch, writes):
    mgr = _Lists()
    mgr.rows = ['a']
    again = asyncio.run(lists_mod.save_selection_as_list(mgr, 'X', ['a', 'b'], list_id='42'))
    assert again.complete and again.added == 2 and sorted(mgr.rows) == ['a', 'b']
    assert ('add', ('b',), '42') in mgr.calls

    async def unreadable(_list_id):
        return None

    from unittest.mock import MagicMock
    monkeypatch.setattr(lists_mod, 'ui', MagicMock())
    mgr.sys_ids_in_list = unreadable
    before = list(mgr.calls)
    failed = asyncio.run(lists_mod.save_selection_as_list(mgr, 'X', ['c'], list_id='42'))
    assert not failed.complete and mgr.calls == before                     # nothing inserted
    lists_mod.ui.notify.assert_called_once()


def test_nothing_to_save_writes_nothing(writes):
    mgr = _Lists()
    assert asyncio.run(lists_mod.save_selection_as_list(mgr, 'X', [])) is None
    assert asyncio.run(lists_mod.save_selection_as_list(None, 'X', ['a'])) is None
    assert asyncio.run(lists_mod.save_selection_as_list(mgr, '  ', ['a'])) is None
    assert mgr.calls == [] and writes == []


@pytest.mark.parametrize('chips', [5, True, 'x', {'kind': 'node', 'ref': 'vol:1'}])
def test_a_broken_kept_value_of_the_right_version_reads_empty(storage, chips):
    # Codex, PR #70: a kept value whose chips are not a list raised out of load_selection
    data, _user = storage
    data['shelf_selection:search_exclude:anon'] = {'format': 'shelf-selection', 'version': 1, 'chips': chips}
    assert len(store.load_selection('search_exclude', 'anon')) == 0

def test_a_clear_the_storage_refused_is_reported(storage, monkeypatch):
    # Codex, PR #70 round 3: an empty selection was "kept" (True) even when the storage refused to
    # remove the old one, which then came back on reopen with no warning.
    data, _user = storage
    store.save_selection('search_exclude', 'anon', _one())
    monkeypatch.setattr(store, 'safe_user_remove', lambda key: False)
    assert store.save_selection('search_exclude', 'anon', Selection()) is False
    assert store.clear_selection('search_exclude', 'anon') is False


def test_safe_user_remove_tells_a_refusal_from_an_absent_key(monkeypatch):
    import web.safe_storage as safe_storage

    class _Refusing(dict):
        def pop(self, *_a):
            raise AssertionError('user storage for x should be created before accessing it')

    class _Broken(dict):
        def pop(self, *_a):
            raise RuntimeError('disk full')

    class _App:
        class storage:
            user = {}

    monkeypatch.setattr(safe_storage, 'app', _App)
    _App.storage.user = {'k': 1}
    assert safe_storage.safe_user_remove('k') is True and 'k' not in _App.storage.user
    assert safe_storage.safe_user_remove('absent') is True
    _App.storage.user = _Refusing()
    assert safe_storage.safe_user_remove('k') is False
    assert safe_storage.safe_user_pop('k', 'default') == 'default'      # what could not tell them apart
    _App.storage.user = _Broken()
    assert safe_storage.safe_user_remove('k') is False