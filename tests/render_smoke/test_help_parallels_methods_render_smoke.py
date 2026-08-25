# -*- coding: utf-8 -*-
"""Render-smoke: the Parallels help section describes the method that is on.

The section documented only chunk search while letter-level had been the
DEFAULT since 2026-08-23, so a reader following the help would tune a chunk
size the default method does not have (owner-reported, 2026-08-25). The fix
adds two gated blocks per language, and gating is the part that can silently
go wrong in both directions:

* gated OFF when the index IS loaded -- the default method stays undocumented,
  which is the bug we just fixed, restored;
* gated ON when the index is NOT loaded -- help for a radio button the page
  hides entirely, which is worse than no help.

Headless tests cannot see either: the blocks are `if` statements inside a page
builder that no unit test imports. This drives the real /help route through
NiceGUI's in-process User with the two predicates patched, in both languages.
"""
from __future__ import annotations

import asyncio
import os
from contextlib import ExitStack, asynccontextmanager
from unittest.mock import AsyncMock, patch

import httpx
import pytest

import web.main as _web_main  # noqa: F401  -- registers /help on core.app
from nicegui import core
from nicegui.context import context as nicegui_context
from nicegui.testing.general import prepare_simulation
from nicegui.testing.user import User
from nicegui.ui_run import set_storage_secret


@asynccontextmanager
async def _help_user(lang: str, *, passage_on: bool, multi_on: bool):
    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()
    try:
        prepare_simulation()
        set_storage_secret('help-parallels-methods-secret', {})
        with ExitStack() as stack:
            stack.enter_context(
                patch('web.pages.help.passage_available', return_value=passage_on))
            stack.enter_context(
                patch('web.pages.help.passage_multi_witness_available',
                      return_value=multi_on))
            # Unrelated surfaces off, so this test fails for its own reasons.
            stack.enter_context(
                patch('web.pages.help.discovery_available', return_value=False))
            stack.enter_context(
                patch('web.pages.help.atlas_preview_available', return_value=False))
            stack.enter_context(
                patch('web.main.discovery_methods_noindex', return_value=False))
            stack.enter_context(
                patch('web.main.get_all_band_precision', new=AsyncMock(return_value={})))
            stack.enter_context(
                patch('web.main.get_band_claim_counts', new=AsyncMock(return_value={})))
            stack.enter_context(
                patch('web.main._resolve_ui_language', return_value=lang))
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


def _page_text(lang: str, *, passage_on: bool, multi_on: bool) -> str:
    """Every string rendered on /help, as one blob."""
    saved_slots = list(nicegui_context.slot_stack)
    collected: list[str] = []

    async def invoke():
        async with _help_user(lang, passage_on=passage_on,
                              multi_on=multi_on) as user:
            await user.open('/help')
            with user._client:
                for element in user._client.elements.values():
                    for attr in ('text', 'content'):
                        value = getattr(element, attr, None)
                        if isinstance(value, str):
                            collected.append(value)
                    props = getattr(element, '_props', None) or {}
                    for value in props.values():
                        if isinstance(value, str):
                            collected.append(value)

    try:
        asyncio.run(invoke())
    finally:
        nicegui_context.slot_stack.clear()
        nicegui_context.slot_stack.extend(saved_slots)
    return '\n'.join(collected)


# Distinctive phrases, one per block, chosen so no other Help copy contains
# them -- a marker that also appears elsewhere would make these vacuous.
EN_METHOD = 'Letter-level search is the default'
EN_HOWTO = 'stream of Hebrew base letters'
EN_WITNESS = 'Witnesses** panel lets you'
EN_CHUNK = 'How Chunk Search Works'

HE_METHOD = 'חיפוש ברמת האות הוא ברירת המחדל'
HE_HOWTO = 'רצף של אותיות עבריות בלבד'
HE_WITNESS = 'עדי נוסח** מאפשר לחפש בכולם'
HE_CHUNK = 'איך עובד חיפוש מקטעים?'


@pytest.mark.render_smoke
@pytest.mark.parametrize('lang,method,howto,chunk', [
    ('en', EN_METHOD, EN_HOWTO, EN_CHUNK),
    ('he', HE_METHOD, HE_HOWTO, HE_CHUNK),
])
def test_the_default_method_is_documented_when_the_index_is_loaded(
    lang, method, howto, chunk,
):
    text = _page_text(lang, passage_on=True, multi_on=False)
    assert method in text, 'the default method is undocumented'
    assert howto in text, 'no explanation of how the default method works'
    # The older method keeps its documentation -- it is still selectable.
    assert chunk in text


@pytest.mark.render_smoke
@pytest.mark.parametrize('lang,method,howto,chunk', [
    ('en', EN_METHOD, EN_HOWTO, EN_CHUNK),
    ('he', HE_METHOD, HE_HOWTO, HE_CHUNK),
])
def test_no_letter_level_help_when_the_index_is_not_loaded(
    lang, method, howto, chunk,
):
    """A box without the index hides the method selector entirely. Help for a
    control that is not on screen is worse than no help."""
    text = _page_text(lang, passage_on=False, multi_on=False)
    assert method not in text
    assert howto not in text
    # ...and chunk search, which IS what that box runs, is still described.
    assert chunk in text


@pytest.mark.render_smoke
@pytest.mark.parametrize('lang,witness', [('en', EN_WITNESS), ('he', HE_WITNESS)])
def test_multi_witness_help_follows_its_own_flag(lang, witness):
    """`passage_multi_witness_available()` is a NARROWER gate than
    `passage_available()`: single-witness letter-level search can be broadly
    on while the fan-out is still being validated."""
    assert witness not in _page_text(lang, passage_on=True, multi_on=False)
    assert witness in _page_text(lang, passage_on=True, multi_on=True)
