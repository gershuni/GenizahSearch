"""Server-render smoke coverage for the v9.0.0 "What's New" toast.

The toast lives in `create_layout`, so it renders on nearly every page — which is
exactly why it needs its own guard. Four behaviours are pinned here:

1. It advertises three things in the owner's order: the guided introduction, the
   atlas, then computed identifications.
2. The two research surfaces are gated on the same availability predicates as
   their routes; advertising a clean-hidden surface would send a reader to a 404.
   The introduction carries no flag and is therefore always present.
3. It suppresses itself on `/start` and `/help`, which already feature these
   surfaces. It is NOT suppressed on `/`: the toast exists for the reader arriving
   at the site, and that arrival is normally the homepage.
4. Its strings are translated. Hebrew is the DEFAULT language here, so an
   unregistered string leaks English to most of the site's readers.

Absence is asserted against the container's own `whats-new-banner` mark rather
than against body text. An earlier revision checked for a lead-in sentence; when
that sentence was removed from the implementation those assertions kept passing
while testing nothing.
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


def _banner(elements):
    """The toast container, or None. Keyed to its mark, never to body text."""
    for element in elements:
        if 'whats-new-banner' in (getattr(element, '_markers', None) or []):
            return element
    return None


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


def test_toast_advertises_all_three_in_the_owners_order():
    async def driver(user):
        elements = _elements(user)
        assert _banner(elements) is not None
        hrefs = _hrefs(elements)
        assert {'/start', '/atlas', '/computed-identifications'} <= hrefs

        # Order is a product decision, not an accident: the introduction comes
        # first because a reader who does not know what the Genizah is needs it
        # before either research surface.
        links = [
            (getattr(e, '_props', None) or {}).get('href')
            for e in elements
            if ((getattr(e, '_props', None) or {}).get('href')
                in {'/start', '/atlas', '/computed-identifications'})
        ]
        assert links == ['/start', '/atlas', '/computed-identifications']

    async def on_search(user):
        await user.open('/search')
        await driver(user)

    _run(on_search)


def test_toast_shows_on_the_homepage_because_that_is_where_readers_arrive():
    async def driver(user):
        await user.open('/')
        assert _banner(_elements(user)) is not None

    _run(driver)


def test_toast_names_only_the_reachable_surfaces():
    """A clean-hidden surface is never advertised; the introduction always is."""
    async def only_discovery(user):
        await user.open('/search')
        hrefs = _hrefs(_elements(user))
        assert '/start' in hrefs
        assert '/computed-identifications' in hrefs
        assert '/atlas' not in hrefs

    _run(only_discovery, discovery_on=True, atlas_on=False)

    async def only_atlas(user):
        await user.open('/search')
        hrefs = _hrefs(_elements(user))
        assert '/start' in hrefs
        assert '/atlas' in hrefs
        assert '/computed-identifications' not in hrefs

    _run(only_atlas, discovery_on=False, atlas_on=True)


def test_toast_still_renders_with_only_the_introduction_when_both_surfaces_are_down():
    """`/start` carries no flag, so the toast never degrades to an empty shell."""
    async def driver(user):
        await user.open('/search')
        elements = _elements(user)
        assert _banner(elements) is not None
        hrefs = _hrefs(elements)
        assert '/start' in hrefs
        assert '/atlas' not in hrefs
        assert '/computed-identifications' not in hrefs

    _run(driver, discovery_on=False, atlas_on=False)


def test_toast_suppresses_itself_where_the_page_already_features_the_surfaces():
    """`/start` and `/help` already carry these; a repeat is duplication."""
    async def on_start(user):
        await user.open('/start')
        assert _banner(_elements(user)) is None

    _run(on_start)

    async def on_help(user):
        await user.open('/help')
        assert _banner(_elements(user)) is None

    _run(on_help)


def test_toast_is_translated_rather_than_leaking_english_into_the_hebrew_ui():
    """Hebrew is the DEFAULT language, so an unregistered string reaches most readers."""
    async def driver(user):
        await user.open('/search')
        text = _texts(_elements(user))
        assert 'תכונות חדשות!' in text
        assert 'דף היכרות עם הגניזה ואתר הגניזה' in text
        assert 'An introduction to the Genizah and this site' not in text
        assert 'New Features!' not in text

    _run(driver, lang='he')
