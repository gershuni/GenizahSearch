"""Server-render smoke coverage for the v9.0.0 "What's New" toast.

The toast lives in `create_layout`, so it renders on EVERY page — which is
exactly why it needs its own guard. Three behaviours are pinned here:

1. It advertises only surfaces that are actually reachable, gated on the same
   availability predicates as their routes. Advertising a clean-hidden surface
   would send the reader to a 404.
2. It suppresses itself on `/`, `/start` and `/help`, which already feature both
   surfaces. Without this, the toast added a second `/atlas` link to `/start` and
   broke that page's "exactly one atlas link" guard — the guard was right.
3. With neither surface reachable it does not render at all, rather than printing
   a lead-in sentence followed by no links.
"""

from __future__ import annotations

import asyncio
import os
from contextlib import AsyncExitStack, asynccontextmanager
from unittest.mock import patch

import httpx
from nicegui import core
from nicegui.context import context as nicegui_context
from nicegui.testing.general import prepare_simulation
from nicegui.testing.user import User
from nicegui.ui_run import set_storage_secret


@asynccontextmanager
async def _user(lang: str, *, discovery_on: bool, atlas_on: bool):
    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()
    try:
        prepare_simulation()
        set_storage_secret('whats-new-render-smoke-secret', {})
        async with AsyncExitStack() as stack:
            # `web.main` binds both predicates at import (`from ... import f`),
            # so the toast reads THESE names, not the source module's.
            stack.enter_context(
                patch('web.main.discovery_available', return_value=discovery_on))
            stack.enter_context(
                patch('web.main.atlas_preview_available', return_value=atlas_on))
            stack.enter_context(
                patch('web.main._resolve_ui_language', return_value=lang))
            os.environ['NICEGUI_USER_SIMULATION'] = 'true'
            try:
                await stack.enter_async_context(
                    core.app.router.lifespan_context(core.app))
                client = await stack.enter_async_context(httpx.AsyncClient(
                    transport=httpx.ASGITransport(core.app),
                    base_url='http://test',
                ))
                yield User(client)
            finally:
                os.environ.pop('NICEGUI_USER_SIMULATION', None)
    finally:
        core.app._startup_handlers.clear()
        core.app._startup_handlers.extend(saved_handlers)


def _run(driver, *, lang='en', discovery_on=True, atlas_on=True):
    saved_slots = list(nicegui_context.slot_stack)

    async def invoke():
        async with _user(lang, discovery_on=discovery_on, atlas_on=atlas_on) as user:
            await driver(user)

    try:
        asyncio.run(invoke())
    finally:
        nicegui_context.slot_stack.clear()
        nicegui_context.slot_stack.extend(saved_slots)


def _elements(user):
    with user._client:
        return list(user._client.elements.values())


def _hrefs(elements):
    return {
        (getattr(element, '_props', None) or {}).get('href')
        for element in elements
    } - {None}


def _texts(elements):
    return ' '.join(
        str((getattr(element, '_props', None) or {}).get('label') or '')
        + ' ' + str(getattr(element, 'text', '') or '')
        for element in elements
    )


def test_toast_advertises_both_surfaces_on_a_page_that_does_not_feature_them():
    async def driver(user):
        await user.open('/search')
        elements = _elements(user)
        hrefs = _hrefs(elements)
        assert '/computed-identifications' in hrefs
        assert '/atlas' in hrefs
        assert 'Two new research surfaces' in _texts(elements)

    _run(driver)


def test_toast_names_only_the_reachable_surface():
    """A clean-hidden surface is never advertised, and the lead-in agrees."""
    async def only_discovery(user):
        await user.open('/search')
        elements = _elements(user)
        hrefs = _hrefs(elements)
        assert '/computed-identifications' in hrefs
        assert '/atlas' not in hrefs
        text = _texts(elements)
        assert 'New in this release' in text
        # The two-surface lead-in must not survive when one surface is down.
        assert 'Two new research surfaces' not in text

    _run(only_discovery, discovery_on=True, atlas_on=False)

    async def only_atlas(user):
        await user.open('/search')
        hrefs = _hrefs(_elements(user))
        assert '/atlas' in hrefs
        assert '/computed-identifications' not in hrefs

    _run(only_atlas, discovery_on=False, atlas_on=True)


def test_toast_does_not_render_when_neither_surface_is_reachable():
    """No lead-in sentence stranded above an empty link list."""
    async def driver(user):
        await user.open('/search')
        text = _texts(_elements(user))
        assert 'New in this release' not in text
        assert 'Two new research surfaces' not in text

    _run(driver, discovery_on=False, atlas_on=False)


def test_toast_suppresses_itself_where_the_page_already_features_the_surfaces():
    """`/start` already links both; a repeat is duplication, not an announcement."""
    async def driver(user):
        await user.open('/start')
        text = _texts(_elements(user))
        assert 'Two new research surfaces' not in text
        assert 'New in this release' not in text

    _run(driver)


def test_toast_is_translated_rather_than_leaking_english_into_the_hebrew_ui():
    """Hebrew is the DEFAULT language, so an unregistered string leaks to most readers."""
    async def driver(user):
        await user.open('/search')
        text = _texts(_elements(user))
        assert 'שני ממשקי מחקר חדשים' in text
        assert 'Two new research surfaces' not in text

    _run(driver, lang='he')
