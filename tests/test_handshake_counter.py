# -*- coding: utf-8 -*-
"""The handshakes NiceGUI accepts and refuses are counted, and nothing else changes.

After a restart every open tab reconnects with a client id the new process
never had; ``nicegui.nicegui._on_handshake`` refuses it and the tab reloads
(web/reconnect_backoff.py spreads those reloads). Nothing recorded how many, so
a reload storm was visible only as load. ``web.framework_patches
._patch_handshake_counter`` wraps the handler -- the module global for the
implicit path, ``sio.on('handshake')`` for the explicit one -- and must be
transparent: ``web.main`` applies it in every test process, so a change in
behaviour would show up in tests/test_citation_chip_reconnect.py.
"""

from __future__ import annotations

import asyncio
import itertools
import logging

import nicegui.nicegui as nicegui_server
import pytest
from nicegui import core, ui
from nicegui.client import Client
from nicegui.testing.general import prepare_simulation
from starlette.requests import Request

import web.framework_patches as fp

_paths = itertools.count()


def _request(path: str) -> Request:
    return Request({'type': 'http', 'method': 'GET', 'path': path, 'headers': [],
                    'query_string': b'',
                    'session': {'id': '11111111-1111-4111-8111-111111111111'}})


def _data(client_id: str, sid_hint: str = 'tab-1') -> dict:
    return {'client_id': client_id, 'tab_id': sid_hint, 'document_id': 'doc-1', 'next_message_id': '0'}


def _reset_counts() -> None:
    for name in fp._HANDSHAKE_CATEGORIES:
        fp._HANDSHAKE_STATS[name].update(count=0, first=None, last=None)
    fp._HANDSHAKE_STATS['burst_warnings'] = 0
    fp._UNKNOWN_CLIENT_RECENT.clear()
    fp._handshake_burst_warned_at = 0.0


@pytest.fixture
def counted():
    fp.apply_all_patches()
    _reset_counts()
    yield fp._handshake_stats
    _reset_counts()


def test_an_unknown_client_is_counted_and_still_refused(counted):
    assert asyncio.run(nicegui_server._on_handshake('test-1', _data('gone'))) is False
    snapshot = counted()
    assert snapshot['unknown_client']['count'] == 1
    assert snapshot['unknown_client']['first'] == snapshot['unknown_client']['last'] > 0
    assert snapshot['accepted']['count'] == 0 and snapshot['refused_other']['count'] == 0
    assert snapshot['unknown_client_last_minute'] == 1


def test_a_real_client_is_accepted_and_a_replayed_socket_is_refused_other(counted):
    prepare_simulation()
    path = f'/_handshake_counter_probe_{next(_paths)}'

    async def _run():
        core.loop = asyncio.get_running_loop()
        client = Client(ui.page(path), request=_request(path))
        with client:
            ui.label('x')
        try:
            data = _data(client.id)
            data['next_message_id'] = str(client.outbox.next_message_id)
            assert await nicegui_server._on_handshake('test-sid-1', data) is True
            # The same socket handshaking the same client again is a replayed frame:
            # Client.accept_handshake says no, and the client is known.
            assert await nicegui_server._on_handshake('test-sid-1', data) is False
        finally:
            client.delete()

    asyncio.run(_run())
    snapshot = counted()
    assert snapshot['accepted']['count'] == 1
    assert snapshot['refused_other']['count'] == 1
    assert snapshot['unknown_client']['count'] == 0


def test_the_wrapper_raises_what_nicegui_raises(counted):
    """Same exceptions: a handshake without a client id was a KeyError and stays one."""
    with pytest.raises(KeyError):
        asyncio.run(fp._nicegui_on_handshake('test-2', {}))
    with pytest.raises(KeyError):
        asyncio.run(nicegui_server._on_handshake('test-2', {}))
    assert counted()['refused_other']['count'] == 1


def test_applying_twice_is_idempotent(counted):
    wrapper = nicegui_server._on_handshake
    original = fp._nicegui_on_handshake
    fp.apply_all_patches()
    fp._patch_handshake_counter()
    assert nicegui_server._on_handshake is wrapper
    assert fp._nicegui_on_handshake is original
    assert getattr(wrapper, '_genizah_handshake_counter', False) is True
    assert not getattr(original, '_genizah_handshake_counter', False), 'the kept original is a wrapper'
    assert asyncio.iscoroutinefunction(wrapper) and asyncio.iscoroutinefunction(original)
    # The explicit path ('handshake' event) runs the same wrapper.
    assert core.sio.handlers['/']['handshake'] is wrapper


def test_the_snapshot_is_registered_as_handshakes(counted):
    from web.api import get_runtime_cache_stats

    stats = get_runtime_cache_stats()
    assert 'handshakes' in stats, sorted(stats)
    assert set(stats['handshakes']) >= {'accepted', 'unknown_client', 'refused_other',
                                        'burst_warnings', 'unknown_client_last_minute'}
    assert stats['handshakes'] == counted()


def test_a_reload_burst_is_warned_about_once(counted, caplog):
    caplog.set_level(logging.WARNING, logger='web.framework_patches')

    async def _refuse(n):
        for i in range(n):
            assert await nicegui_server._on_handshake(f'test-b{i}', _data(f'gone-{i}')) is False

    asyncio.run(_refuse(30))
    assert not [r for r in caplog.records if 'reload burst' in r.getMessage()], 'warned at 30, not above 30'
    asyncio.run(_refuse(21))
    bursts = [r for r in caplog.records if 'reload burst' in r.getMessage()]
    assert len(bursts) == 1 and bursts[0].levelno == logging.WARNING, [r.getMessage() for r in caplog.records]
    assert '31 handshakes' in bursts[0].getMessage()
    assert counted()['burst_warnings'] == 1
    assert counted()['unknown_client']['count'] == 51
