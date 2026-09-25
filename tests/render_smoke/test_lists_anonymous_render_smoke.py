"""Server-render smoke: /lists for an anonymous visitor (improvement sweep C2).

An anonymous visitor must get a sign-in empty state and nothing else: no
Create List button, no "Lists are stored locally." text, and no read of the
process-wide shared store. The shared store is replaced by a tripwire that
records every attribute access; the page must not touch it at all.
"""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from unittest.mock import MagicMock, patch

import httpx
import web.main as web_main  # noqa: F401  (side-effect import: registers /lists)
from nicegui import core, ui
from nicegui.context import context as nicegui_context
from nicegui.testing.general import prepare_simulation
from nicegui.testing.user import User
from nicegui.ui_run import set_storage_secret
from shared.genizah_translations import TRANSLATIONS
from web.state import state


class _Tripwire:
    def __init__(self):
        object.__setattr__(self, 'accessed', [])

    def __getattr__(self, name):
        self.accessed.append(name)
        return MagicMock(name=f'tripwire.{name}')


def _t(text, lang):
    return TRANSLATIONS.get(text, text) if lang == 'he' else text


@asynccontextmanager
async def _start_user(lang: str):
    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()
    try:
        prepare_simulation()
        set_storage_secret('lists-anonymous-render-smoke-secret', {})
        with patch('web.main._resolve_ui_language', return_value=lang):
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


def _run(lang, driver):
    saved_slots = list(nicegui_context.slot_stack)

    async def invoke():
        async with _start_user(lang) as user:
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


def _descendants(element):
    out = []
    for slot in element.slots.values():
        for child in slot.children:
            out.append(child)
            out.extend(_descendants(child))
    return out


def _check_anonymous_lists_page(lang):
    trip = _Tripwire()
    saved = state._local_lists_mgr
    state._local_lists_mgr = trip
    try:
        async def driver(user):
            await user.open('/lists')
            await user.should_see(_t('Sign in to access your saved research lists.', lang))
            await user.should_not_see('Lists are stored locally.')
            await user.should_not_see(_t('Lists are stored locally.', lang))
            await user.should_not_see(_t('Create List', lang))
            await user.should_not_see(_t('Move to account', lang))

            elements = _elements(user)
            gates = _marked(elements, 'lists-anonymous-gate')
            assert len(gates) == 1, 'anonymous sign-in state not rendered exactly once'
            inside = _descendants(gates[0])
            buttons = [e for e in inside if isinstance(e, ui.button)]
            assert [b.text for b in buttons] == [_t('Sign in', lang)]
            h1s = [e for e in elements if getattr(e, 'tag_name', None) == 'h1']
            assert any(_t('Personal Lists', lang) in (getattr(h, 'content', '') or '') for h in h1s)

        _run(lang, driver)
    finally:
        state._local_lists_mgr = saved
    assert trip.accessed == [], f'/lists read the shared store anonymously: {trip.accessed}'


def test_anonymous_lists_page_shows_sign_in_and_reads_nothing_en():
    _check_anonymous_lists_page('en')


def test_anonymous_lists_page_shows_sign_in_and_reads_nothing_he():
    _check_anonymous_lists_page('he')
