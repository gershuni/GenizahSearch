# -*- coding: utf-8 -*-
"""Render-smoke: the Puzzle's toolbars, its canvas's width and its way back (owner, 2026-11-23).

* In a short window a toolbar shrank to its 32px floor and its wrapped lines lay over the next row
  (measured at 1200x600: 32px for 88px of buttons): the toolbars and the extension's notice keep their
  height; the canvas gives way, to 240px.
* The canvas kept the width it was drawn at when its room changed without the window (the side menu, the
  notice dismissed): a ResizeObserver on its room draws it again, and goes with the canvas.
* From the Reading Desk, or any other way in, only the browser's back went back: a Back button first on
  the toolbar, shown once the browser says the reader came from another page of the site.
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from unittest.mock import patch

import httpx

import web.main as web_main  # noqa: F401  (registers /puzzle)
from nicegui import core, ui
from nicegui.context import context as nicegui_context
from nicegui.testing.general import prepare_simulation
from nicegui.testing.user import User
from nicegui.ui_run import set_storage_secret
from tests.render_smoke._bounded_run import run_user_driver
from web.pages import puzzle as puzzle_page


@asynccontextmanager
async def _puzzle_user():
    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()
    try:
        prepare_simulation()
        set_storage_secret('puzzle-layout-render-smoke-secret', {})
        with patch('web.main._resolve_ui_language', return_value='en'):
            os.environ['NICEGUI_USER_SIMULATION'] = 'true'
            try:
                async with core.app.router.lifespan_context(core.app):
                    async with httpx.AsyncClient(transport=httpx.ASGITransport(core.app),
                                                 base_url='http://test') as client:
                        yield User(client)
            finally:
                os.environ.pop('NICEGUI_USER_SIMULATION', None)
    finally:
        core.app._startup_handlers.clear()
        core.app._startup_handlers.extend(saved_handlers)


def _run(driver):
    saved_slots = list(nicegui_context.slot_stack)
    try:
        run_user_driver(_puzzle_user, driver)
    finally:
        nicegui_context.slot_stack.clear()
        nicegui_context.slot_stack.extend(saved_slots)


def test_the_puzzle_has_a_way_back_first_on_its_toolbar():
    seen = {}

    async def driver(user):
        await user.open('/puzzle')
        with user._client:
            buttons = [e for e in user._client.elements.values()
                       if isinstance(e, ui.button) and 'puzzle-back' in e.classes]
            toolbar = next(e for e in user._client.elements.values() if 'puzzle-toolbar' in e.classes)
            first = [c for c in toolbar.default_slot.children if isinstance(c, ui.button)][0]
            seen['back'] = [(b.props.get('icon'), b.props.get('aria-label'),
                             [ls.js_handler for ls in b._event_listeners.values() if ls.type == 'click'])
                            for b in buttons]
            seen['first'] = first in buttons

    _run(driver)
    assert seen['back'] == [('arrow_back', 'Back', ['() => history.back()'])]
    assert seen['first'] is True
    js = puzzle_page._BACK_SHOWN_JS
    assert 'from.origin !== location.origin' in js and "setAttribute('data-back', '')" in js
    assert '.puzzle-back:not([data-back]) {\n    display: none !important;\n}' in puzzle_page.PUZZLE_STYLES


def test_the_toolbars_keep_their_height_and_the_canvas_follows_its_room():
    css = puzzle_page.PUZZLE_STYLES
    assert '.puzzle-toolbar, #extBannerContainer {\n    flex-shrink: 0;\n}' in css
    assert 'min-height: 240px;' in css and 'min-height: 400px;' not in css
    js = puzzle_page.PUZZLE_CANVAS_JS
    assert 'this._resizeObserver = new ResizeObserver(' in js
    # the same 240px floor when the canvas is first made as when it is drawn again (Codex on #113)
    assert 'Math.max(container.clientHeight, 240)' in js and 'Math.max(p.clientHeight, 240)' in js
    assert 'clientHeight, 400)' not in js
    assert js.count('this._resizeObserver.disconnect();') == 2           # where the window's listener goes
