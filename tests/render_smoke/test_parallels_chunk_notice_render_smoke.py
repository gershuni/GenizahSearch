# -*- coding: utf-8 -*-
"""Render-smoke: a chunk search's composition notice reaches the /parallels page.

tests/test_parallels_page_chunk_notice.py pins, on the source, where the page
reads ``composition_notices``. This drives the page itself: a three-word text
at the default chunk size goes through the real Run button and the real
``execute_parallels`` handler, the engine (a stand-in, no index) answers the
way search_composition_logic does for a text shorter than the chunk size, and
the visitor must see the sentence that says the text was searched as one chunk.
"""
from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import patch

import httpx
from nicegui import core, ui
from nicegui.context import context as _nicegui_context
from nicegui.testing.general import prepare_simulation
from nicegui.testing.user import User
from nicegui.ui_run import set_storage_secret

import web.main  # noqa: F401  -- registers /parallels on core.app at import
from shared.composition_windows import chunk_notice_message
from web import research_jobs
from web.state import state as _web_state

NOTICE = {'code': 'text_shorter_than_chunk_size', 'words': 3,
          'chunk_size': 5, 'effective_chunk_size': 3}


class _ChunkEngine:
    """Answers a composition search the way the engine does for a short text."""

    def __init__(self):
        self.calls = []

    def search_composition_logic(self, text, **kwargs):
        self.calls.append((text, kwargs))
        return {'main': [], 'filtered': [], 'partial': False, 'boundary_stats': None,
                'corpus_scope': 'genizah', 'effective_chunk_size': 3,
                'composition_notices': [dict(NOTICE)]}


@asynccontextmanager
async def _parallels_user(engine):
    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()
    saved = (_web_state.is_ready, _web_state.searcher, _web_state.lab_engine, _web_state.meta_mgr)
    _web_state.is_ready = lambda: True
    _web_state.searcher = engine
    _web_state.lab_engine = SimpleNamespace(settings=None)
    _web_state.meta_mgr = SimpleNamespace(csv_bank={})
    saved_executor = research_jobs._wait_executor
    research_jobs._wait_executor = None
    try:
        prepare_simulation()
        set_storage_secret('render-smoke-test-secret', {})
        # Chunk search: the letter-level index is not available.
        with patch('web.pages.parallels.passage_available', return_value=False),                 patch('web.main._resolve_ui_language', return_value='en'):
            os.environ['NICEGUI_USER_SIMULATION'] = 'true'
            try:
                async with core.app.router.lifespan_context(core.app):
                    async with httpx.AsyncClient(
                        transport=httpx.ASGITransport(core.app),
                        base_url='http://test',
                    ) as client:
                        yield User(client)
            finally:
                os.environ.pop('NICEGUI_USER_SIMULATION', None)
    finally:
        core.app._startup_handlers.clear()
        core.app._startup_handlers.extend(saved_handlers)
        (_web_state.is_ready, _web_state.searcher, _web_state.lab_engine,
         _web_state.meta_mgr) = saved
        research_jobs._wait_executor = saved_executor


def test_short_text_chunk_search_tells_the_visitor_it_ran_as_one_chunk():
    engine = _ChunkEngine()
    message = chunk_notice_message(NOTICE, lambda english: english)   # the page renders in English
    saved_slot_stack = list(_nicegui_context.slot_stack)

    async def _go():
        async with _parallels_user(engine) as user:
            await user.open('/parallels')
            box = user.find(kind=ui.textarea, marker=None,
                            content=None).elements
            text_box = next(e for e in box
                            if e.props.get('placeholder') == 'Paste your Hebrew text here...')
            with user.client:
                text_box.value = 'ברוך אתה יי'
            user.find(kind=ui.button, content='Find Parallels').click()
            await user.should_see(message, retries=100)

    try:
        asyncio.run(_go())
    finally:
        _nicegui_context.slot_stack.clear()
        _nicegui_context.slot_stack.extend(saved_slot_stack)
    assert engine.calls, 'the page never ran the chunk search'
    assert engine.calls[0][1].get('chunk_size') == 5
