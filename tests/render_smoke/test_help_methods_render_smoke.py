"""Render-smoke coverage for the practical Atlas and computed-ID help."""

from __future__ import annotations

import asyncio
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


@asynccontextmanager
async def _help_user(
    lang: str,
    *,
    discovery_on: bool,
    atlas_on: bool,
):
    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()
    try:
        prepare_simulation()
        set_storage_secret('help-tools-render-smoke-secret', {})
        with ExitStack() as stack:
            stack.enter_context(
                patch('web.pages.help.discovery_available', return_value=discovery_on)
            )
            stack.enter_context(
                patch('web.pages.help.atlas_preview_available', return_value=atlas_on)
            )
            # The route still accepts the legacy report inputs while the old
            # public confidence-band report is retired.
            stack.enter_context(
                patch('web.main.discovery_methods_noindex', return_value=False)
            )
            stack.enter_context(
                patch('web.main.get_all_band_precision', new=AsyncMock(return_value={}))
            )
            stack.enter_context(
                patch('web.main.get_band_claim_counts', new=AsyncMock(return_value={}))
            )
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


def _run(lang, driver, *, discovery_on=True, atlas_on=True):
    saved_slots = list(nicegui_context.slot_stack)

    async def invoke():
        async with _help_user(
            lang,
            discovery_on=discovery_on,
            atlas_on=atlas_on,
        ) as user:
            await driver(user)

    try:
        asyncio.run(invoke())
    finally:
        nicegui_context.slot_stack.clear()
        nicegui_context.slot_stack.extend(saved_slots)


def _elements(user):
    with user._client:
        return list(user._client.elements.values())


def _marked(elements, marker):
    return [element for element in elements if marker in getattr(element, '_markers', [])]


def _hrefs(elements):
    return {
        str((getattr(element, '_props', None) or {}).get('href'))
        for element in elements
        if (getattr(element, '_props', None) or {}).get('href')
    }


def _anchor_names(elements):
    return {
        str((getattr(element, '_props', None) or {}).get('name'))
        for element in elements
        if (getattr(element, '_props', None) or {}).get('name')
    }


def test_help_explains_current_computed_matches_pane_in_english():
    async def driver(user):
        await user.open('/help')
        elements = _elements(user)
        anchors = _anchor_names(elements)
        hrefs = _hrefs(elements)

        assert 'help-computed-identifications' in anchors
        assert len(_marked(elements, 'help-computed-identifications')) == 1
        assert '/computed-identifications' in hrefs
        await user.should_see('Computed Identifications (Beta)')
        await user.should_see('On this page')
        await user.should_see('Show more possible matches')
        await user.should_see('In this manuscript')
        await user.should_see('includes this page')
        await user.should_see('View text match')
        await user.should_see('Does not correspond to the catalogue — not adjudicated')
        await user.should_see('algorithmic suggestions, not identifications reviewed by a scholar')
        await user.should_not_see('Confidence Bands and Methods')

    _run('en', driver)


def test_help_explains_computed_matches_in_hebrew_rtl():
    async def driver(user):
        await user.open('/help')
        elements = _elements(user)
        assert 'help-computed-identifications' in _anchor_names(elements)
        await user.should_see('זיהויים מחושבים (בטא)')
        await user.should_see('בדף זה')
        await user.should_see('הצג התאמות אפשריות נוספות')
        await user.should_see('בכתב־יד זה')
        await user.should_see('כולל דף זה')
        await user.should_see('הצגת התאמת הטקסט')
        await user.should_see('אינו מתאים לקטלוג — לא הוכרע')
        await user.should_not_see('דרגות ודאות ושיטות')

    _run('he', driver)


def test_help_explains_the_visual_atlas_in_both_languages():
    async def english_driver(user):
        await user.open('/help')
        elements = _elements(user)
        assert 'help-visual-atlas' in _anchor_names(elements)
        assert len(_marked(elements, 'help-visual-atlas')) == 1
        assert '/atlas' in _hrefs(elements)
        await user.should_see('The Visual Genizah Atlas')
        await user.should_see('proximity reflects algorithmically calculated textual similarity')
        await user.should_see('not shared physical provenance')

    _run('en', english_driver)

    async def hebrew_driver(user):
        await user.open('/help')
        await user.should_see('אטלס הגניזה החזותי')
        await user.should_see('הקרבה במפה משקפת דמיון טקסטואלי')
        await user.should_see('לא מוצא פיזי משותף')

    _run('he', hebrew_driver)


def test_help_tools_follow_their_independent_readiness_gates():
    async def only_atlas(user):
        await user.open('/help')
        elements = _elements(user)
        anchors = _anchor_names(elements)
        assert 'help-computed-identifications' not in anchors
        assert 'help-visual-atlas' in anchors
        assert '/computed-identifications' not in _hrefs(elements)
        assert '/atlas' in _hrefs(elements)

    _run('en', only_atlas, discovery_on=False, atlas_on=True)

    async def only_computed(user):
        await user.open('/help')
        elements = _elements(user)
        anchors = _anchor_names(elements)
        assert 'help-computed-identifications' in anchors
        assert 'help-visual-atlas' not in anchors
        assert '/computed-identifications' in _hrefs(elements)
        assert '/atlas' not in _hrefs(elements)

    _run('en', only_computed, discovery_on=True, atlas_on=False)

    async def neither(user):
        await user.open('/help')
        anchors = _anchor_names(_elements(user))
        assert 'help-computed-identifications' not in anchors
        assert 'help-visual-atlas' not in anchors
        await user.should_see('Public API & AI Tools')

    _run('en', neither, discovery_on=False, atlas_on=False)
