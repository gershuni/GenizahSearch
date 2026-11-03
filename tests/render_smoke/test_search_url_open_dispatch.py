# -*- coding: utf-8 -*-
"""A search opened from its address reaches the engine before the browser connects.

/search?q=... builds the page and starts the search in the same request. Anything
the search awaits that needs the page's socket -- an awaited ``ui.run_javascript``
waits, with no limit, for ``client.connected()`` in NiceGUI 3 -- holds the search
until the browser has loaded the page and connected: ~1.6 s of server time on top
of every link-opened search, measured in production as ux_timing ``nav`` 2.9 s
against 1.2 s typed (2026-10-24..29, fixed by bde98fb7 in v9.11.1). The typed
search waited one browser round trip at the same await.

Both are pinned here on the real page, in a NiceGUI User simulation whose socket
handshake is withheld: the engine must be called before the handshake (URL open),
and within a fraction of a second of the click (typed). Against the old code the
first wait expires with the engine never called.

In tests/render_smoke/ because it enters and tears down the app lifespan: the main
lane deselects this directory and a fresh-process job runs it (tests/conftest.py).
"""
from __future__ import annotations

import asyncio
import os
import re
import time
from contextlib import AsyncExitStack, asynccontextmanager
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

import httpx
from nicegui import core, ui
from nicegui.client import Client
from nicegui.context import context as nicegui_context
from nicegui.nicegui import _on_handshake
from nicegui.testing.general import prepare_simulation
from nicegui.testing.user import User
from nicegui.testing.user_interaction import UserInteraction
from nicegui.ui_run import set_storage_secret
from tests.render_smoke._bounded_run import run_bounded

import web.main  # noqa: E402,F401  (registers /search)
from web import research_jobs  # noqa: E402
from web.state import state as _web_state  # noqa: E402

QUERY = 'שבת קודש'
# The engine must be reached this soon after the page response (URL open) or the
# click (typed). A browser needs more than this to load the page and connect.
DISPATCH_WITHIN = 1.5


class _Engine:
    """Records when each search reached it."""

    def __init__(self):
        self.called_at = []

    def parse_query_syntax(self, query, responsa_mode=False):
        return None, query

    def execute_search(self, *args, **kwargs):
        self.called_at.append(time.perf_counter())
        return []


@asynccontextmanager
async def _app(engine):
    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()
    kept = (_web_state.is_ready, _web_state.searcher, _web_state.lab_engine, _web_state.meta_mgr)
    _web_state.is_ready = lambda: True
    _web_state.searcher = engine
    _web_state.lab_engine = SimpleNamespace(settings=None)
    _web_state.meta_mgr = SimpleNamespace(csv_bank={})
    saved_executor = research_jobs._wait_executor
    research_jobs._wait_executor = None
    try:
        prepare_simulation()
        set_storage_secret('search-url-open-dispatch-secret', {})
        async with AsyncExitStack() as stack:
            stack.enter_context(patch('web.main._resolve_ui_language', return_value='en'))
            os.environ['NICEGUI_USER_SIMULATION'] = 'true'
            try:
                await stack.enter_async_context(core.app.router.lifespan_context(core.app))
                client = await stack.enter_async_context(httpx.AsyncClient(
                    transport=httpx.ASGITransport(core.app), base_url='http://test'))
                yield User(client)
            finally:
                os.environ.pop('NICEGUI_USER_SIMULATION', None)
    finally:
        core.app._startup_handlers.clear()
        core.app._startup_handlers.extend(saved_handlers)
        (_web_state.is_ready, _web_state.searcher, _web_state.lab_engine, _web_state.meta_mgr) = kept
        research_jobs._wait_executor = saved_executor


async def _get_page(user, path):
    """The page response alone: the socket is NOT connected yet."""
    response = await user.http_client.get(path, follow_redirects=True)
    assert response.status_code == 200, response.status_code
    client_id = re.search(r"'client_id': '([0-9a-f-]+)'", response.text).group(1)
    user.client = Client.instances[client_id]


async def _connect(user):
    """The browser's socket handshake, as User.open does it."""
    user.sio.on('connect')
    await _on_handshake(f'test-{uuid4()}', {
        'client_id': user.client.id, 'tab_id': user.tab_id, 'document_id': str(uuid4()),
    })
    user._patch_outbox_emit_function()


async def _wait_for_calls(engine, n, timeout):
    deadline = time.perf_counter() + timeout
    while len(engine.called_at) < n and time.perf_counter() < deadline:
        await asyncio.sleep(0.01)
    return len(engine.called_at) >= n


def _run(coro_fn):
    saved_slots = list(nicegui_context.slot_stack)
    try:
        run_bounded(coro_fn())
    finally:
        nicegui_context.slot_stack.clear()
        nicegui_context.slot_stack.extend(saved_slots)


def test_a_url_opened_search_reaches_the_engine_before_the_browser_connects():
    engine = _Engine()
    seen = {}

    async def go():
        async with _app(engine) as user:
            t0 = time.perf_counter()
            await _get_page(user, f'/search?q={QUERY}')
            seen['dispatched'] = await _wait_for_calls(engine, 1, DISPATCH_WITHIN)
            seen['after_s'] = (engine.called_at[0] - t0) if engine.called_at else None
            await _connect(user)   # let the page finish normally
            await _wait_for_calls(engine, 1, 5.0)
            seen['calls'] = len(engine.called_at)

    _run(go)
    assert seen['calls'] == 1, seen
    assert seen['dispatched'], (
        f'the search waited for the browser to connect: the engine was not called within '
        f'{DISPATCH_WITHIN} s of the page response ({seen})')


def test_a_typed_search_reaches_the_engine_without_a_browser_round_trip():
    engine = _Engine()
    seen = {}

    async def go():
        async with _app(engine) as user:
            await _get_page(user, '/search')
            await _connect(user)
            await asyncio.sleep(0.3)   # the page's deferred loaders
            query = next(e for e in _elements(user, ui.input)
                         if e.props.get('placeholder') == 'Enter Hebrew text to search')
            with user.client:
                query.value = QUERY
            button = next(b for b in _elements(user, ui.button)
                          if b.props.get('label') == 'Search' and 'px-8' in b.classes)
            t0 = time.perf_counter()
            UserInteraction(user, {button}, None).click()
            # The simulated browser answers no window.innerWidth: an awaited request
            # for it holds the search until its 1 s timeout.
            seen['dispatched'] = await _wait_for_calls(engine, 1, 0.5)
            seen['after_s'] = (engine.called_at[0] - t0) if engine.called_at else None
            await _wait_for_calls(engine, 1, 5.0)

    _run(go)
    assert seen['dispatched'], f'the typed search waited for a browser round trip ({seen})'


def _elements(user, kind):
    with user.client:
        return [e for e in user.client.elements.values() if isinstance(e, kind)]
