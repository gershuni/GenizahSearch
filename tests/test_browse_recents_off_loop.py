# -*- coding: utf-8 -*-
"""A signed-in /browse page turn records Recently Viewed after the page shows, off the loop.

The write (delete + insert + select + maybe delete: 3-4 Supabase round trips,
transatlantic from the Oregon server) ran synchronously in ``load_page`` before
Phase A rendered, so every signed-in page turn waited for it and every other
visitor's request stalled for as long. It must run in a worker thread, after
Phase A, with the visitor's own client (resolved on the loop: in a worker the
visitor's storage is unreadable and the write would silently go out anonymous),
and two writes for one account must not interleave, from one tab or several
(Codex 2026-10-21: two tabs left duplicate rows). A write that waited is
dropped if the browser has switched account meanwhile (Codex 2026-10-21: it
landed in the new account's history).

The tests wait on events, not on fixed sleeps: a write scheduled (the page's
Phase A is done), a write started, a write held open until the test lets it
finish. Fixed sleeps passed locally and failed on a slow Windows CI runner.
"""

from __future__ import annotations

import asyncio
import itertools
import threading
import time

from nicegui import background_tasks, core, ui
from nicegui.client import Client
import pytest
from nicegui.testing.general import prepare_simulation

_paths = itertools.count()


@pytest.fixture
def supabase_offline(monkeypatch):
    """Any Supabase client the page builds points at a refused local port: no network."""
    import shared.supabase_provider as provider
    import web.supabase_client as supabase_client

    for module in (supabase_client, provider):
        monkeypatch.setattr(module, 'SUPABASE_URL', 'http://127.0.0.1:9', raising=False)
        monkeypatch.setattr(module, '_client', None, raising=False)
    monkeypatch.setattr(supabase_client, '_public_read_client', None)


@pytest.fixture
def recents(monkeypatch, supabase_offline):
    """A /browse page on a fake service; every Recently Viewed write is recorded.

    ``signed_in['id']`` is the account the visitor is signed in as; ``write_s``
    how long one write takes (a slow Supabase); a write whose fl_id is in
    ``hold`` waits for that threading.Event; ``scheduled`` counts the writes the
    page has handed to background_tasks (one per Phase A).
    """
    from web.services import BrowsePage
    import web.auth_state as auth_state
    import web.pages.browse as browse
    import web.supabase_client as supabase_client
    from web.state import state as app_state

    def page(p):
        return BrowsePage(uid=f'u{p}', p_num=p, text='שלום', full_header='hdr',
                          total_pages=3, current_idx=p, sys_id='990000000000017',
                          shelfmark='T-S 1.1', fl_id=f'FL{p}')

    class _Service:
        is_ready = True

        def get_browse_page(self, sys_id, p_num=1, direction=0, volume_ie=None, **_kwargs):
            return page(max(1, min(3, (p_num or 1) + (direction or 0))))

        def __getattr__(self, _name):
            return lambda *a, **k: None

    class _Meta:
        def get_meta_for_id(self, _sys_id):
            return ('T-S 1.1', 'title')

    class _ListsMgr:
        meta_mgr = _Meta()

        def add_to_recent_sync(self, *_a, **_k):
            raise AssertionError('the recents write ran on the event loop')

    h = {'calls': [], 'active': {'n': 0, 'max': 0}, 'pages': [], 'loop_thread': None,
         'signed_in': {'id': 'user-1'}, 'write_s': 0.3, 'client': object(), 'hold': {},
         'scheduled': []}

    def recorder(user_id, sys_id, shelfmark, title, fl_id, *, client=None):
        active = h['active']
        active['n'] += 1
        active['max'] = max(active['max'], active['n'])
        h['calls'].append({'thread': threading.get_ident(), 'client': client, 'user': user_id,
                           'fl_id': fl_id})
        held = h['hold'].get(fl_id)
        if held is not None:
            held.wait(20)
        time.sleep(h['write_s'])
        active['n'] -= 1
        return {'success': True}

    real_create = background_tasks.create

    def counting_create(coroutine, *args, **kwargs):
        if kwargs.get('name') == 'browse-recents':
            # Judged when the write is handed over: by the time a queued write
            # runs, the next page's load may already have cleared this one.
            h['scheduled'].append({'phase_a_shown': any(
                isinstance(e, ui.button) and str(e._props.get('data-action', '')).strip('"') == 'next'
                for e in h['pages'][-1].elements.values())})
        return real_create(coroutine, *args, **kwargs)

    monkeypatch.setattr(browse, 'get_service', lambda: _Service())
    monkeypatch.setattr(background_tasks, 'create', counting_create)
    monkeypatch.setattr(auth_state.GlobalAuthState, 'is_logged_in', staticmethod(lambda: True))
    monkeypatch.setattr(auth_state.GlobalAuthState, 'get_user_id', staticmethod(lambda: h['signed_in']['id']))
    monkeypatch.setattr(supabase_client, 'add_recent_item', recorder)
    monkeypatch.setattr(supabase_client, 'get_user_client', lambda: h['client'])
    monkeypatch.setattr(app_state, 'lists_mgr', _ListsMgr(), raising=False)
    monkeypatch.setenv('GENIZAH_STORAGE_SECRET', 'x' * 40)
    prepare_simulation()

    def open_page():
        with Client(ui.page(f'/_recents_probe_{next(_paths)}')) as client:
            h['pages'].append(client)
            browse.create_browse_page(initial_sys_id='990000000000017')
        return client

    h['open_page'] = open_page
    yield h
    for held in h['hold'].values():
        held.set()  # never leave a worker thread waiting


async def _wait(pred, timeout=20.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not pred():
        if asyncio.get_running_loop().time() > deadline:
            return False
        await asyncio.sleep(0.02)
    return True


def _next_buttons(client):
    # The live Next button: every full render rebuilds it (the old one stays,
    # dimmed and inert, while a page loads).
    return [e for e in client.elements.values() if isinstance(e, ui.button)
            and str(e._props.get('data-action', '')).strip('"') == 'next']


def _load_page_of(client):
    """The page's own ``load_page``, reached through the live Next button
    (``on_click=lambda: load_page(direction=1)``). While a load runs, the
    previous page's buttons stay under the spinner, dimmed and inert; the
    newest one is taken."""
    import functools

    stack = [listener.handler for listener in _next_buttons(client)[-1]._event_listeners.values()
             if listener.type == 'click']
    seen = set()
    while stack:
        fn = stack.pop()
        if id(fn) in seen:
            continue
        seen.add(id(fn))
        if getattr(fn, '__name__', '') == 'load_page':
            return fn
        if isinstance(fn, functools.partial):
            stack.append(fn.func)
        for cell in getattr(fn, '__closure__', None) or ():
            try:
                if callable(cell.cell_contents):
                    stack.append(cell.cell_contents)
            except ValueError:
                pass
    raise AssertionError('load_page is not reachable from the Next button')


async def _click_next(client):
    assert await _wait(lambda: _next_buttons(client)), 'the page rendered no Next button'
    for listener in _next_buttons(client)[-1]._event_listeners.values():
        if listener.type == 'click':
            listener.handler(None)


def test_recents_are_written_after_phase_a_off_the_loop_one_at_a_time(recents):
    async def _run():
        core.loop = asyncio.get_running_loop()
        recents['loop_thread'] = threading.get_ident()
        client = recents['open_page']()
        assert await _wait(lambda: len(recents['scheduled']) == 1), 'the load scheduled no write'
        # Two page turns, each clicked as soon as the previous page has shown:
        # the writes (0.3 s each) would overlap without the lock.
        for n in (2, 3):
            await _click_next(client)
            assert await _wait(lambda: len(recents['scheduled']) == n), f'page turn {n} scheduled no write'
        assert await _wait(lambda: len(recents['calls']) == 3 and recents['active']['n'] == 0)

    asyncio.run(_run())
    calls = recents['calls']
    assert len(calls) == 3, calls
    assert all(c['thread'] != recents['loop_thread'] for c in calls), 'recents written on the event loop'
    assert all(c['client'] is recents['client'] and c['user'] == 'user-1' for c in calls), (
        'the write did not get the visitor\'s own client')
    assert all(s['phase_a_shown'] for s in recents['scheduled']), 'recents written before the page was shown'
    assert recents['active']['max'] == 1, 'two recents writes overlapped'


def test_two_tabs_of_one_account_write_one_at_a_time(recents):
    async def _run():
        core.loop = asyncio.get_running_loop()
        recents['open_page']()
        recents['open_page']()  # the same account in a second tab, at once
        assert await _wait(lambda: len(recents['calls']) == 2 and recents['active']['n'] == 0)

    asyncio.run(_run())
    assert len(recents['calls']) == 2, recents['calls']
    assert recents['active']['max'] == 1, (
        'two tabs of one account wrote Recently Viewed at once; interleaved '
        'delete/insert pairs leave duplicate rows')


def test_a_waiting_write_is_dropped_after_an_account_switch(recents):
    release = threading.Event()
    recents['hold']['FL2'] = release  # page 2's write holds the account's lock until released

    async def _run():
        core.loop = asyncio.get_running_loop()
        client = recents['open_page']()
        assert await _wait(lambda: len(recents['calls']) == 1 and recents['active']['n'] == 0)
        await _click_next(client)  # page 2: its write starts and is held
        assert await _wait(lambda: len(recents['calls']) == 2), 'page 2 wrote nothing'
        await _click_next(client)  # page 3: its write is scheduled and waits for the lock
        assert await _wait(lambda: len(recents['scheduled']) == 3), 'page 3 scheduled no write'
        recents['signed_in']['id'] = 'user-2'  # signed out and in as someone else, in another tab
        release.set()
        assert await _wait(lambda: recents['active']['n'] == 0)
        await asyncio.sleep(0.5)  # room for the waiting write to run, if it were going to

    asyncio.run(_run())
    calls = recents['calls']
    assert [c['fl_id'] for c in calls] == ['FL1', 'FL2'], (
        f'the write that waited ran after the account switch: {calls}')
    assert all(c['user'] == 'user-1' for c in calls), calls


def test_a_load_overtaken_by_a_newer_one_records_nothing(recents, monkeypatch):
    """Codex on #423: a load that passed its last generation check and was then
    overtaken while awaiting the title lookup still scheduled its page, after
    the newer load's -- so the superseded folio became the most recent."""
    import shared.translation_service as translation_service

    entered, release = threading.Event(), threading.Event()
    slow = {'next': False}

    class _Titles:  # the title lookup, made slow for one load
        def __init__(self, *_a, **_k):
            pass

        def titles_available(self):
            if slow['next']:
                slow['next'] = False
                entered.set()
                release.wait(20)
            return False

        def close(self):
            pass

    monkeypatch.setattr(translation_service, 'TranslationService', _Titles)

    async def _run():
        core.loop = asyncio.get_running_loop()
        client = recents['open_page']()
        assert await _wait(lambda: len(recents['calls']) == 1 and recents['active']['n'] == 0)
        load_page = _load_page_of(client)
        slow['next'] = True
        with client:
            older = background_tasks.create(load_page(direction=1), name='older-load')  # page 2, held in its lookup
        assert await _wait(entered.is_set), 'the older load never reached its title lookup'
        with client:
            newer = background_tasks.create(load_page(direction=1), name='newer-load')  # page 3, runs through
        await newer
        assert await _wait(lambda: len(recents['scheduled']) == 2), 'the newer load scheduled no write'
        release.set()
        await older
        assert await _wait(lambda: recents['active']['n'] == 0)
        await asyncio.sleep(0.5)  # room for a stale write to run, if one were scheduled

    asyncio.run(_run())
    assert [c['fl_id'] for c in recents['calls']] == ['FL1', 'FL3'], recents['calls']
