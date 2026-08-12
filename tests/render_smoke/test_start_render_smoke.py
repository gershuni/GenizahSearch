"""Server-render smoke coverage for the bilingual /start launchpad."""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from unittest.mock import patch

import httpx
import web.discovery_assets as discovery_assets
import web.main as web_main
import web.pages.start as start_page
from nicegui import core
from nicegui.context import context as nicegui_context
from nicegui.testing.general import prepare_simulation
from nicegui.testing.user import User
from nicegui.ui_run import set_storage_secret


@asynccontextmanager
async def _start_user(lang: str):
    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()
    try:
        prepare_simulation()
        set_storage_secret('start-render-smoke-secret', {})
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


def _hrefs(elements):
    return {
        str((getattr(element, '_props', None) or {}).get('href'))
        for element in elements
        if (getattr(element, '_props', None) or {}).get('href')
    }


def _set_gates(monkeypatch, *, enabled: bool):
    launch_hash = '53725098ece6cf152a72425587dc2fe9119261427fc82e008a5b953dcbd2bce7'
    monkeypatch.setattr(start_page, 'atlas_preview_available', lambda: enabled)
    monkeypatch.setattr(start_page, 'discovery_available', lambda: enabled)
    monkeypatch.setattr(start_page, 'WEB_PUZZLE_ENABLED', enabled)
    monkeypatch.setattr(web_main, 'atlas_preview_available', lambda: enabled)
    monkeypatch.setattr(web_main, 'discovery_available', lambda: enabled)
    monkeypatch.setattr(web_main, 'WEB_PUZZLE_ENABLED', enabled)
    monkeypatch.setattr(discovery_assets, 'discovery_available', lambda: enabled)
    monkeypatch.setattr(discovery_assets, 'discovery_meta', lambda key: launch_hash if enabled else None)


def test_start_renders_three_routes_one_h1_and_native_links_en(monkeypatch):
    _set_gates(monkeypatch, enabled=False)

    async def driver(user):
        await user.open('/start')
        elements = _elements(user)
        assert len(_marked(elements, 'start-page')) == 1
        assert len(_marked(elements, 'start-route-explore')) == 1
        assert len(_marked(elements, 'start-route-search')) == 1
        assert len(_marked(elements, 'start-route-research')) == 1
        revolutions = _marked(elements, 'start-revolutions')
        assert len(revolutions) == 1
        assert getattr(revolutions[0], 'tag', None) == 'details'
        assert 'open' not in (getattr(revolutions[0], '_props', None) or {})
        assert len(_marked(elements, 'start-revolutions-summary')) == 1
        assert len(_marked(elements, 'start-revolution-card')) == 4
        assert sum(getattr(element, 'tag_name', None) == 'h1' for element in elements) == 1
        root = _marked(elements, 'start-page')[0]
        assert (getattr(root, '_style', {}) or {}).get('direction') == 'ltr'

        hrefs = _hrefs(elements)
        assert '#start-explore' in hrefs
        assert any(href.startswith('/search?q=') for href in hrefs)
        assert any(href.startswith('/browse?sys_id=') for href in hrefs)
        assert any(href.startswith('/catalog-browse?work=') for href in hrefs)
        assert 'https://www.midrash.eu/' in hrefs
        assert 'https://fjms.genizah.org/' in hrefs

    _run('en', driver)


def test_start_renders_hebrew_rtl_without_english_title_leak(monkeypatch):
    _set_gates(monkeypatch, enabled=False)

    async def driver(user):
        await user.open('/start')
        elements = _elements(user)
        root = _marked(elements, 'start-page')[0]
        assert (getattr(root, '_style', {}) or {}).get('direction') == 'rtl'
        h1s = [element for element in elements if getattr(element, 'tag_name', None) == 'h1']
        assert len(h1s) == 1
        assert 'לא יודעים מה לחפש? התחילו כאן.' in (getattr(h1s[0], 'content', '') or '')
        assert any(
            'ארבע המהפכות בחקר הגניזה' in (getattr(element, 'content', '') or '')
            for element in elements
        )

    _run('he', driver)


def test_optional_tools_and_frame_bound_candidates_hide_and_show_cleanly(monkeypatch):
    _set_gates(monkeypatch, enabled=False)

    async def hidden_driver(user):
        await user.open('/start')
        elements = _elements(user)
        hrefs = _hrefs(elements)
        assert '/atlas' not in hrefs
        assert '/computed-identifications' not in hrefs
        assert '/puzzle' not in hrefs
        assert not any('computed=1' in href for href in hrefs)
        assert not _marked(elements, 'start-computed-feature')

    _run('en', hidden_driver)

    _set_gates(monkeypatch, enabled=True)

    async def visible_driver(user):
        await user.open('/start')
        hrefs = _hrefs(_elements(user))
        assert '/atlas' in hrefs
        assert '/computed-identifications' in hrefs
        assert '/puzzle' in hrefs  # pending saved ID intentionally degrades to the generic tool
        assert sum('computed=1' in href for href in hrefs) == 3
        assert len(_marked(_elements(user), 'start-computed-feature')) == 1

        # The heavy Atlas renderer is absent from first paint, then mounts
        # inline without introducing a second page heading.
        assert not any(
            (getattr(element, '_props', {}) or {}).get('id') == 'start-atlas-canvas'
            for element in _elements(user)
        )
        user.find('Open the interactive Atlas here').click()
        elements = _elements(user)
        assert any(
            (getattr(element, '_props', {}) or {}).get('id') == 'start-atlas-canvas'
            for element in elements
        )
        assert sum(getattr(element, 'tag_name', None) == 'h1' for element in elements) == 1

    _run('en', visible_driver)
