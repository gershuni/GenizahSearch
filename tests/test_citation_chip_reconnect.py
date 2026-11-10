# -*- coding: utf-8 -*-
"""The "How to cite" chip keeps following the page across a websocket reconnect.

The chip's registry entry was dropped on ``client.on_disconnect``, which NiceGUI
also fires when a socket drops and the SAME client reconnects (a network blip,
a laptop waking up). On NiceGUI 3.18 the page survives that reconnect instead of
reloading, so ``set_page_citation`` found no entry and the chip silently kept the
previous page's citation. The entry must go only when the client is really
deleted (``on_delete``), which still stops the per-visit leak the hook was added
for.
"""

from __future__ import annotations

import asyncio
import itertools

import nicegui.nicegui as nicegui_server
from nicegui import core, ui
from nicegui.client import Client
from nicegui.testing.general import prepare_simulation
from starlette.requests import Request

import web.citation_chip as chip
from shared.transcription_credits import PageCitation

_paths = itertools.count()


def _request(path: str) -> Request:
    return Request({'type': 'http', 'method': 'GET', 'path': path, 'headers': [],
                    'query_string': b'',
                    'session': {'id': '11111111-1111-4111-8111-111111111111'}})


async def _handshake(client: Client, sid: str) -> bool:
    return await nicegui_server._on_handshake(sid, {
        'client_id': client.id, 'tab_id': 'tab-1', 'document_id': 'doc-1',
        'next_message_id': str(client.outbox.next_message_id)})


def _new_page_with_chip() -> Client:
    path = f'/_chip_reconnect_probe_{next(_paths)}'
    client = Client(ui.page(path), request=_request(path))
    with client:
        chip.render_citation_chip(lang='en')
    return client


def test_the_chip_follows_the_page_after_a_reconnect_and_is_dropped_on_delete():
    prepare_simulation()
    seen = []

    async def _run():
        core.loop = asyncio.get_running_loop()
        client = _new_page_with_chip()
        assert client.id in chip._UPDATERS
        assert await _handshake(client, 'test-sid-1')

        client.handle_disconnect('test-sid-1')  # the socket drops ...
        await asyncio.sleep(0.05)
        assert await _handshake(client, 'test-sid-2')  # ... and the same page reconnects
        assert client.id in Client.instances
        assert client.id in chip._UPDATERS, (
            'a reconnect forgot the chip: it now shows the previous page\'s citation')

        chip._UPDATERS[client.id] = seen.append
        with client:
            chip.set_page_citation(PageCitation(heading='Cite this page', text='T-S 1.1, fol. 1r'))

        client.delete()
        assert client.id not in chip._UPDATERS, 'a deleted client kept its registry entry (leak)'

    asyncio.run(_run())
    assert len(seen) == 1


def test_a_client_that_never_comes_back_is_still_forgotten():
    prepare_simulation()

    async def _run():
        core.loop = asyncio.get_running_loop()
        client = _new_page_with_chip()
        assert await _handshake(client, 'test-sid-3')
        client.page.reconnect_timeout = 0.1
        client.handle_disconnect('test-sid-3')
        await asyncio.sleep(0.5)
        assert client.id not in Client.instances
        assert client.id not in chip._UPDATERS

    asyncio.run(_run())
