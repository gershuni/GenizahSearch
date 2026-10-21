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
"""

from __future__ import annotations

import asyncio
import itertools
import threading
import time

from nicegui import core, ui
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
    how long one write takes (a slow Supabase).
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
         'signed_in': {'id': 'user-1'}, 'write_s': 0.3, 'client': object()}

    def recorder(user_id, sys_id, shelfmark, title, fl_id, *, client=None):
        active = h['active']
        active['n'] += 1
        active['max'] = max(active['max'], active['n'])
        h['calls'].append({'thread': threading.get_ident(), 'client': client, 'user': user_id,
                           'fl_id': fl_id, 'phase_a_shown': any(
                               isinstance(e, ui.button) and str(e._props.get('data-action', '')).strip('"') == 'next'
                               for e in h['pages'][0].elements.values())})
        time.sleep(h['write_s'])
        active['n'] -= 1
        return {'success': True}

    monkeypatch.setattr(browse, 'get_service', lambda: _Service())
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
    return h


def _click_next(client):
    # The live Next button: every render rebuilds it.
    nexts = [e for e in client.elements.values() if isinstance(e, ui.button)
             and str(e._props.get('data-action', '')).strip('"') == 'next']
    for listener in nexts[-1]._event_listeners.values():
        if listener.type == 'click':
            listener.handler(None)


def test_recents_are_written_after_phase_a_off_the_loop_one_at_a_time(recents):
    async def _run():
        core.loop = asyncio.get_running_loop()
        recents['loop_thread'] = threading.get_ident()
        client = recents['open_page']()
        await asyncio.sleep(1.0)
        for _ in range(2):  # two quick page turns: each shows, their writes would overlap
            _click_next(client)
            await asyncio.sleep(0.15)
        await asyncio.sleep(1.5)

    asyncio.run(_run())
    calls = recents['calls']
    assert len(calls) == 3, calls
    assert all(c['thread'] != recents['loop_thread'] for c in calls), 'recents written on the event loop'
    assert all(c['client'] is recents['client'] and c['user'] == 'user-1' for c in calls), (
        'the write did not get the visitor\'s own client')
    assert all(c['phase_a_shown'] for c in calls), 'recents written before the page was shown'
    assert recents['active']['max'] == 1, 'two recents writes overlapped'


def test_two_tabs_of_one_account_write_one_at_a_time(recents):
    async def _run():
        core.loop = asyncio.get_running_loop()
        recents['open_page']()
        recents['open_page']()  # the same account in a second tab, at once
        await asyncio.sleep(2.0)

    asyncio.run(_run())
    assert len(recents['calls']) == 2, recents['calls']
    assert recents['active']['max'] == 1, (
        'two tabs of one account wrote Recently Viewed at once; interleaved '
        'delete/insert pairs leave duplicate rows')


def test_a_waiting_write_is_dropped_after_an_account_switch(recents):
    recents['write_s'] = 1.0

    async def _run():
        core.loop = asyncio.get_running_loop()
        client = recents['open_page']()
        await asyncio.sleep(1.5)  # the load's own write is done
        _click_next(client)       # its write starts and holds the account's lock
        await asyncio.sleep(0.15)
        _click_next(client)       # this one waits for the lock
        await asyncio.sleep(0.15)
        recents['signed_in']['id'] = 'user-2'  # signed out and in as someone else, in another tab
        await asyncio.sleep(2.5)

    asyncio.run(_run())
    calls = recents['calls']
    assert [c['fl_id'] for c in calls] == ['FL1', 'FL2'], (
        f'the write that waited ran after the account switch: {calls}')
    assert all(c['user'] == 'user-1' for c in calls), calls
