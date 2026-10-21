# -*- coding: utf-8 -*-
"""UserListsManager's writes run in a worker thread with the visitor's own client.

Every async write of the web lists manager (create / rename / delete / restore
a list, empty the trash, add / remove / edit items, create / rename / delete a
project, move a list between projects) called its Supabase helper
synchronously on the event loop: one or more Oregon->Frankfurt round trips
(~150 ms each) during which uvicorn's single worker served nobody. They now
resolve the signed-in user and the client ON the loop (``_worker_context``)
and run the helper through ``run.io_bound(..., client=client)``; the search
page's bulk add is one ``add_items`` call (one insert, ``add_list_items``)
instead of one round trip per item.

Passing only the client is not enough (see ``get_items_in_list_sync``): in the
worker the visitor's storage is unreadable, so a method that re-derived auth
there would read as signed out. The blocking bodies therefore take
``user_id`` and ``is_authenticated=True`` as plain values.

No network: every helper is a recorder that notes the thread it ran on and
the client it was handed.
"""
from __future__ import annotations

import ast
import asyncio
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

import web.supabase_client as sc
import web.user_lists as user_lists
from web.user_lists import UserListsManager

USER_LISTS_PY = Path(user_lists.__file__)
SENTINEL = object()
USER_ID = 'user-1'

# What the two readers answer: list 7 is the default list, list 8 sits in project 3.
LISTS = [
    {'id': 7, 'name': 'General', 'name_en': 'General', 'color': '#FFD700', 'is_default': True,
     'is_system': False, 'project_id': None, 'created_at': 't'},
    {'id': 8, 'name': 'Mine', 'name_en': 'Mine', 'color': '#FFD700', 'is_default': False,
     'is_system': False, 'project_id': 3, 'created_at': 't'},
]
PROJECTS = [{'id': 3, 'name': 'P', 'color': '#4CAF50', 'created_at': 't'}]

# helper name -> (module it is looked up in, its success answer). restore_list
# and empty_trash are imported inside their methods, so they are patched where
# that import finds them.
HELPERS = {
    'get_user_lists': (user_lists, LISTS),
    'get_projects': (user_lists, PROJECTS),
    'sb_create_list': (user_lists, {'success': True, 'list': {'id': 11}}),
    'sb_update_list': (user_lists, {'success': True, 'list': {'id': 8}}),
    'sb_delete_list': (user_lists, {'success': True}),
    'add_list_item': (user_lists, {'success': True, 'item': {'id': 5}}),
    'add_list_items': (user_lists, None),  # answered per call: added = len(rows)
    'update_list_item': (user_lists, {'success': True, 'item': {'id': 5}}),
    'delete_list_item': (user_lists, {'success': True}),
    'sb_create_project': (user_lists, {'success': True, 'project': {'id': 12}}),
    'sb_update_project': (user_lists, {'success': True, 'project': {'id': 3}}),
    'sb_delete_project': (user_lists, {'success': True}),
    'restore_list': (sc, {'success': True, 'list': {'id': 8}}),
    'empty_trash': (sc, {'success': True, 'deleted_count': 2}),
}


@pytest.fixture
def harness(monkeypatch):
    """Every helper recorded (thread, client, args); the client a sentinel built on demand."""
    h = SimpleNamespace(loop_thread=None, calls=[], client_threads=[], sleep_s=0.0, signed_in=True)

    def make(name, answer):
        def recorder(*args, client=None, **kwargs):
            if h.sleep_s:
                time.sleep(h.sleep_s)
            h.calls.append({'helper': name, 'thread': threading.get_ident(), 'client': client,
                            'args': args, 'kwargs': kwargs})
            if name == 'add_list_items':
                return {'success': True, 'added': len(args[1])}
            return answer
        recorder.__name__ = name
        return recorder

    for name, (module, answer) in HELPERS.items():
        monkeypatch.setattr(module, name, make(name, answer))

    def build_client():
        h.client_threads.append(threading.get_ident())
        return SENTINEL

    monkeypatch.setattr(user_lists, 'get_user_client', build_client)
    monkeypatch.setattr(user_lists.GlobalAuthState, 'is_logged_in', classmethod(lambda cls: h.signed_in))
    monkeypatch.setattr(user_lists.GlobalAuthState, 'get_user_id',
                        classmethod(lambda cls: USER_ID if h.signed_in else None))
    return h


def _call(harness, method, *args, **kwargs):
    async def _run():
        harness.loop_thread = threading.get_ident()
        return await getattr(UserListsManager(None, None), method)(*args, **kwargs)
    return asyncio.run(_run())


def _helper_call(harness, name):
    found = [c for c in harness.calls if c['helper'] == name]
    assert len(found) == 1, f'{name} was called {len(found)} times: {harness.calls}'
    return found[0]


# (method, args, kwargs, helpers it must reach, success return, failure return)
WRITES = [
    pytest.param('create_list', ('New',), {}, {'sb_create_list'}, '11', None, id='create_list'),
    pytest.param('create_list', ('New',), {'project_id': '3'},
                 {'get_user_lists', 'get_projects', 'sb_create_list'}, '11', None, id='create_list_in_project'),
    pytest.param('update_list', ('8',), {'name': 'Renamed'}, {'sb_update_list'}, True, False, id='update_list'),
    pytest.param('update_list_project', ('8', '3'), {}, {'sb_update_list'}, True, False, id='update_list_project'),
    pytest.param('delete_list', ('8',), {}, {'sb_delete_list'}, True, False, id='delete_list'),
    pytest.param('restore_list', ('8',), {}, {'restore_list'}, True, False, id='restore_list'),
    pytest.param('permanently_delete_list', ('8',), {}, {'sb_delete_list'}, True, False,
                 id='permanently_delete_list'),
    pytest.param('empty_trash', (), {}, {'empty_trash'}, 2, None, id='empty_trash'),
    pytest.param('add_item', ('990001', '8'), {'note': 'n'}, {'add_list_item'}, True, False, id='add_item'),
    pytest.param('add_item', ('990001',), {}, {'get_user_lists', 'get_projects', 'add_list_item'}, True, False,
                 id='add_item_to_default'),
    pytest.param('remove_item_from_list', ('5', '8'), {}, {'delete_list_item'}, True, False,
                 id='remove_item_from_list'),
    pytest.param('update_item_note', ('5', 'note'), {}, {'update_list_item'}, True, False, id='update_item_note'),
    pytest.param('update_item_tags', ('5', ['t']), {}, {'update_list_item'}, True, False, id='update_item_tags'),
    pytest.param('create_project', ('Proj',), {}, {'get_user_lists', 'get_projects', 'sb_create_project'},
                 '12', None, id='create_project'),
    pytest.param('create_project', ('Proj',), {'color': '#000000'}, {'sb_create_project'}, '12', None,
                 id='create_project_with_colour'),
    pytest.param('update_project', ('3',), {'name': 'Q'}, {'sb_update_project'}, True, False, id='update_project'),
    pytest.param('delete_project', ('3',), {},
                 {'get_user_lists', 'get_projects', 'sb_update_list', 'sb_delete_project'}, True, False,
                 id='delete_project_unlinks_its_lists'),
    pytest.param('delete_project', ('3',), {'delete_lists': True}, {'sb_delete_project'}, True, False,
                 id='delete_project_with_lists'),
    pytest.param('move_list_to_project', ('8', '3'), {}, {'sb_update_list'}, True, False,
                 id='move_list_to_project'),
    pytest.param('add_items', (['990001', '990002'], '8'), {}, {'add_list_items'}, 2, 0, id='add_items'),
    pytest.param('add_items', (['990001', '', None],), {}, {'get_user_lists', 'get_projects', 'add_list_items'},
                 1, 0, id='add_items_to_default'),
]


@pytest.mark.parametrize('method,args,kwargs,helpers,ok,_failed', WRITES)
def test_write_runs_in_a_worker_with_the_client_built_on_the_loop(harness, method, args, kwargs,
                                                                   helpers, ok, _failed):
    result = _call(harness, method, *args, **kwargs)

    assert result == ok and type(result) is type(ok), f'{method} returned {result!r}'
    assert {c['helper'] for c in harness.calls} == helpers, harness.calls
    on_loop = [c['helper'] for c in harness.calls if c['thread'] == harness.loop_thread]
    assert on_loop == [], f'{method}: these Supabase calls ran on the event loop: {on_loop}'
    without_client = [c['helper'] for c in harness.calls if c['client'] is not SENTINEL]
    assert without_client == [], (
        f"{method}: these calls did not get the visitor's client (the worker would write anonymously): "
        f'{without_client}')
    assert harness.client_threads == [harness.loop_thread], (
        f'{method}: get_user_client must be called exactly once, on the loop; got {harness.client_threads}')


@pytest.mark.parametrize('method,args,kwargs,_helpers,_ok,failed', WRITES)
def test_no_answer_from_the_worker_is_the_failure_return(harness, monkeypatch, method, args, kwargs,
                                                          _helpers, _ok, failed):
    """NiceGUI's run.io_bound returns None when the task is cancelled or the app is
    stopping (tests/test_web_io_bound_none.py). That is the method's failure return,
    never a crash and never a success."""
    async def _no_answer(callback, *a, **k):
        return None

    monkeypatch.setattr(user_lists, 'run', SimpleNamespace(io_bound=_no_answer))
    result = _call(harness, method, *args, **kwargs)
    assert result == failed and type(result) is type(failed), f'{method} returned {result!r}, not {failed!r}'


def test_refresh_data_reads_in_a_worker_and_falls_back_to_the_skeleton(harness, monkeypatch):
    data = _call(harness, 'refresh_data')
    assert set(data['lists']) == {'7', '8'} and set(data['projects']) == {'3'}
    assert {c['helper'] for c in harness.calls} == {'get_user_lists', 'get_projects'}
    assert all(c['thread'] != harness.loop_thread and c['client'] is SENTINEL for c in harness.calls)

    async def _no_answer(callback, *a, **k):
        return None

    monkeypatch.setattr(user_lists, 'run', SimpleNamespace(io_bound=_no_answer))
    mgr = UserListsManager(None, None)
    assert _call(harness, 'refresh_data') == mgr._get_default_data()


def test_anonymous_add_items_is_zero_without_a_supabase_call(harness):
    harness.signed_in = False
    assert _call(harness, 'add_items', ['990001', '990002'], '8') == 0
    assert harness.calls == [] and harness.client_threads == []


def test_add_items_skips_empty_ids_and_passes_the_rows_in_one_batch(harness):
    class _Meta:
        def get_meta_for_id(self, sys_id):
            return (f'T-S {sys_id[-1]}', f'title {sys_id[-1]}')

    async def _run():
        return await UserListsManager(None, _Meta()).add_items(['990001', None, '990002', ''], '8',
                                                               note='n', tags=['t'], fl_id='FL1')

    assert asyncio.run(_run()) == 2
    call = _helper_call(harness, 'add_list_items')
    assert call['args'][0] == 8
    assert call['args'][1] == [
        {'sys_id': '990001', 'shelfmark': 'T-S 1', 'title': 'title 1', 'fl_id': 'FL1', 'note': 'n', 'tags': ['t']},
        {'sys_id': '990002', 'shelfmark': 'T-S 2', 'title': 'title 2', 'fl_id': 'FL1', 'note': 'n', 'tags': ['t']},
    ]


def test_create_list_in_a_project_takes_the_project_colour_read_in_the_worker(harness):
    assert _call(harness, 'create_list', 'New', color='#123456', project_id='3') == '11'
    call = _helper_call(harness, 'sb_create_list')
    assert call['args'] == (USER_ID, 'New')
    assert call['kwargs']['color'] == '#4CAF50' and call['kwargs']['project_id'] == 3


def test_create_project_picks_the_next_colour_from_a_worker_read(harness):
    assert _call(harness, 'create_project', 'Proj') == '12'
    call = _helper_call(harness, 'sb_create_project')
    assert call['args'] == (USER_ID, 'Proj', '#2196F3'), 'the first palette colour (#4CAF50) is taken by project 3'


def test_delete_project_unlinks_the_lists_it_held_in_the_same_worker_body(harness):
    assert _call(harness, 'delete_project', '3') is True
    assert _helper_call(harness, 'sb_update_list')['args'] == (8, {'project_id': None})
    assert _helper_call(harness, 'sb_delete_project')['args'] == (3,)


def test_add_item_to_default_resolves_the_default_list_in_the_worker(harness):
    assert _call(harness, 'add_item', '990001', fl_id='FL1') is True
    call = _helper_call(harness, 'add_list_item')
    assert call['args'] == (7, '990001') and call['kwargs']['fl_id'] == 'FL1'


def test_a_bad_list_id_is_refused_on_the_loop_without_a_write(harness):
    assert _call(harness, 'update_list', 'not-a-number', name='x') is False
    assert _call(harness, 'add_items', ['990001'], 'not-a-number') == 0
    assert [c['helper'] for c in harness.calls] == []


def test_cached_data_forwards_the_client_only_when_given(harness, monkeypatch):
    mgr = UserListsManager(None, None)
    data = mgr._get_cached_data(client=SENTINEL, user_id=USER_ID, is_authenticated=True)
    assert set(data['lists']) == {'7', '8'} and set(data['projects']) == {'3'}
    assert [(c['helper'], c['client'], c['args']) for c in harness.calls] == [
        ('get_user_lists', SENTINEL, (USER_ID,)), ('get_projects', SENTINEL, (USER_ID,))]

    # Without a client the readers are called as before: a fake that takes the
    # user id alone (tests/test_user_lists_cache_isolation.py) must still work.
    monkeypatch.setattr(user_lists, 'get_user_lists', lambda user_id: LISTS)
    monkeypatch.setattr(user_lists, 'get_projects', lambda user_id: PROJECTS)
    assert set(mgr._get_cached_data()['lists']) == {'7', '8'}


def test_a_slow_write_does_not_stall_the_event_loop(harness):
    """The behavioural contract (shape: tests/test_offloaded_readers.py): a ticker
    keeps ticking while a 0.3 s write runs. On the loop it would get ~0 ticks."""
    harness.sleep_s = 0.3
    ticks = 0

    async def ticker(stop):
        nonlocal ticks
        while not stop.is_set():
            await asyncio.sleep(0.01)
            ticks += 1

    async def main():
        stop = asyncio.Event()
        t = asyncio.create_task(ticker(stop))
        ok = await UserListsManager(None, None).update_list('8', name='Renamed')
        stop.set()
        await t
        return ok

    assert asyncio.run(main()) is True
    assert ticks >= 5, f'the event loop was starved during the write (ticks={ticks})'


# ---------------------------------------------------------------------------
# The helper: one insert, never its own client when given one
# ---------------------------------------------------------------------------

class _Table:
    def __init__(self, log, name):
        self.log, self.name = log, name

    def insert(self, data):
        self.log.append((self.name, 'insert', data))
        return self

    def execute(self):
        return SimpleNamespace(data=list(self.log[-1][2]))


class _Client:
    def __init__(self):
        self.log = []

    def table(self, name):
        return _Table(self.log, name)


def test_add_list_items_is_one_insert_of_n_rows_with_the_given_client(monkeypatch):
    monkeypatch.setattr(sc, 'get_user_client',
                        lambda: pytest.fail('add_list_items built its own client although one was passed'))
    client = _Client()
    rows = [{'sys_id': '990001', 'shelfmark': 'T-S 1.1', 'title': 'a'},
            {'sys_id': '990002', 'note': 'n', 'tags': ['t'], 'fl_id': 'FL2'}]

    assert sc.add_list_items(8, rows, client=client) == {'success': True, 'added': 2}

    assert len(client.log) == 1, f'expected ONE insert, got {client.log}'
    table, op, data = client.log[0]
    assert (table, op) == ('list_items', 'insert')
    assert data == [
        {'list_id': 8, 'sys_id': '990001', 'shelfmark': 'T-S 1.1', 'title': 'a', 'fl_id': None,
         'note': '', 'tags': []},
        {'list_id': 8, 'sys_id': '990002', 'shelfmark': None, 'title': None, 'fl_id': 'FL2',
         'note': 'n', 'tags': ['t']},
    ]


def test_add_list_items_empty_batch_adds_nothing_without_a_round_trip():
    client = _Client()
    assert sc.add_list_items(8, [], client=client) == {'success': True, 'added': 0}
    assert client.log == []


def test_add_list_items_reports_a_failed_insert():
    class _Failing(_Client):
        def table(self, name):
            raise RuntimeError('connection reset')

    assert sc.add_list_items(8, [{'sys_id': '990001'}], client=_Failing()) == {'error': 'connection reset'}


# ---------------------------------------------------------------------------
# Structural guard: every async write awaits an offload
# ---------------------------------------------------------------------------

# migrate_local_to_user and add_to_recent do no Supabase work of their own
# (retired; browse records recents through _record_recent). get_items_in_list's
# numeric-id form resolves the recent list through _is_recent_list, which reads
# per-user state and has no client parameter, so it stays on the loop for now.
EXEMPT = {'migrate_local_to_user', 'add_to_recent', 'get_items_in_list'}


def _awaits_io_bound(fn) -> bool:
    for node in ast.walk(fn):
        if not (isinstance(node, ast.Await) and isinstance(node.value, ast.Call)):
            continue
        func = node.value.func
        if (isinstance(func, ast.Attribute) and func.attr == 'io_bound'
                and isinstance(func.value, ast.Name) and func.value.id in {'run', 'io_bound_result'}):
            return True
    return False


def test_every_async_method_of_the_manager_awaits_an_offload():
    tree = ast.parse(USER_LISTS_PY.read_text(encoding='utf-8'))
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'UserListsManager')
    checked = [n for n in cls.body if isinstance(n, ast.AsyncFunctionDef) and n.name not in EXEMPT]
    assert len(checked) >= 17, 'the 15 writes, add_items and refresh_data must all be async methods here'
    offenders = [n.name for n in checked if not _awaits_io_bound(n)]
    assert offenders == [], f'these async methods still call Supabase on the event loop: {offenders}'


def test_offload_guard_can_fail():
    good = ast.parse('async def f(self):\n    return await run.io_bound(x, client=c)\n').body[0]
    also_good = ast.parse('async def f(self):\n    return await io_bound_result.io_bound(x)\n').body[0]
    bad = ast.parse('async def f(self):\n    return sb_update_list(1, {})\n').body[0]
    wrong_module = ast.parse('async def f(self):\n    return await other.io_bound(x)\n').body[0]
    assert _awaits_io_bound(good) and _awaits_io_bound(also_good)
    assert not _awaits_io_bound(bad) and not _awaits_io_bound(wrong_module)
