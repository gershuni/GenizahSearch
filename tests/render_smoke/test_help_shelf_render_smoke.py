"""Render-smoke coverage for the Pick from the shelf card on /help (v11.0.0).

The card and its contents entry are drawn only where the shelf tree is served, in both
languages, and its "Look before you open" lines render as a list (markdown needs
``cuddled-lists`` for a list straight after a line of text).
"""

from __future__ import annotations

import os
from contextlib import ExitStack, asynccontextmanager
from unittest.mock import AsyncMock, patch

import httpx

import web.main as _web_main  # noqa: F401  -- registers /help on core.app
from nicegui import core
from nicegui.context import context as nicegui_context
from nicegui.testing.general import prepare_simulation
from nicegui.testing.user import User
from nicegui.ui_run import set_storage_secret

from tests.render_smoke._bounded_run import run_bounded

_TITLES = {'en': 'Pick from the shelf', 'he': 'בחירה מהמדף'}
# How the list's first line starts, up to its first bold word (the HTML has a tag there).
_LIST_LINE = {'en': 'its image, with ',
              'he': 'התמונה, עם '}


@asynccontextmanager
async def _help_user(lang: str, *, shelf_on: bool):
    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()
    try:
        prepare_simulation()
        set_storage_secret('help-shelf-render-smoke-secret', {})
        with ExitStack() as stack:
            stack.enter_context(patch('web.pages.help._shelf_served', return_value=shelf_on))
            stack.enter_context(patch('web.main.discovery_methods_noindex', return_value=False))
            stack.enter_context(
                patch('web.main.get_all_band_precision', new=AsyncMock(return_value={})))
            stack.enter_context(
                patch('web.main.get_band_claim_counts', new=AsyncMock(return_value={})))
            stack.enter_context(patch('web.main._resolve_ui_language', return_value=lang))
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


def _run(lang, driver, *, shelf_on):
    saved_slots = list(nicegui_context.slot_stack)

    async def invoke():
        async with _help_user(lang, shelf_on=shelf_on) as user:
            await driver(user)

    try:
        run_bounded(invoke())
    finally:
        nicegui_context.slot_stack.clear()
        nicegui_context.slot_stack.extend(saved_slots)


def _seen(lang, *, shelf_on):
    """(anchor drawn, contents link drawn, title shown, the list line inside an <li>)."""
    found = {}

    async def driver(user):
        await user.open('/help')
        with user._client:
            elements = list(user._client.elements.values())
        props = [getattr(e, '_props', None) or {} for e in elements]
        found['anchor'] = any(p.get('name') == 'help-shelf' for p in props)
        found['toc'] = any(p.get('href') == '#help-shelf' for p in props)
        # The card's heading is a SemanticHeading (ui.html): its <h2> is in its content.
        contents = [str(getattr(e, 'content', '') or '') for e in elements]
        found['title'] = any(f'>{_TITLES[lang]}</h2>' in c for c in contents)
        markdown = [p.get('innerHTML', '') for e, p in zip(elements, props)
                    if _LIST_LINE[lang] in (getattr(e, 'content', '') or '')]
        found['listed'] = any(f'<li>{_LIST_LINE[lang]}' in h for h in markdown)

    _run(lang, driver, shelf_on=shelf_on)
    return found


def test_the_shelf_card_is_drawn_where_the_shelf_is_served():
    for lang in ('en', 'he'):
        assert _seen(lang, shelf_on=True) == {
            'anchor': True, 'toc': True, 'title': True, 'listed': True}, lang


def test_no_shelf_card_where_the_shelf_is_not_served():
    for lang in ('en', 'he'):
        assert _seen(lang, shelf_on=False) == {
            'anchor': False, 'toc': False, 'title': False, 'listed': False}, lang
