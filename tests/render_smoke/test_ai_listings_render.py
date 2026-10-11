"""Server-render coverage for the AI listings launch (docs/plans/AI_LISTINGS_LAUNCH_PLAN.md).

The /ai page, the homepage AI strip, the header's What's New entry and /chatgpt follow production
settings (web/ai_listings.py). Pinned here, by rendering the real pages:

1. With no setting the site is unchanged: the GPT is offered, with the "MCP · Planned" badge.
2. Each live listing gets its own "Add to ..." button; the ChatGPT plugin replaces the GPT
   everywhere, including /chatgpt.
3. During the announcement the homepage strip says "New: GenizahSearch in ...", naming only what
   is live, and the What's New entry names the live listings, in Hebrew too.
4. From the GPT's retirement it is never offered, listings or not.
"""
from __future__ import annotations

import asyncio
import os
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import date
from unittest.mock import patch

import httpx
import pytest
from nicegui import core
from nicegui.context import context as nicegui_context
from nicegui.testing.general import prepare_simulation
from nicegui.testing.user import User
from nicegui.ui_run import set_storage_secret

import web.ai_listings as listings

PLUGIN = 'https://chatgpt.com/apps/genizahsearch'
CONNECTOR = 'https://claude.ai/directory/genizahsearch'
BEFORE, AFTER = date(2026, 11, 1), listings.GPT_RETIRES


@pytest.fixture
def settings(monkeypatch):
    for name in (listings.CHATGPT_SETTING, listings.CLAUDE_SETTING, listings.ANNOUNCE_SETTING):
        monkeypatch.delenv(name, raising=False)

    def apply(today=BEFORE, **values):
        monkeypatch.setattr(listings, '_today', lambda: today)
        for name, value in values.items():
            monkeypatch.setenv(name, value)
    return apply


@asynccontextmanager
async def _user(lang):
    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()
    try:
        prepare_simulation()
        set_storage_secret('ai-listings-render-smoke-secret', {})
        async with AsyncExitStack() as stack:
            stack.enter_context(patch('web.main._resolve_ui_language', return_value=lang))
            os.environ['NICEGUI_USER_SIMULATION'] = 'true'
            try:
                await stack.enter_async_context(core.app.router.lifespan_context(core.app))
                client = await stack.enter_async_context(httpx.AsyncClient(
                    transport=httpx.ASGITransport(core.app), base_url='http://test'))
                yield User(client), client
            finally:
                os.environ.pop('NICEGUI_USER_SIMULATION', None)
    finally:
        core.app._startup_handlers.clear()
        core.app._startup_handlers.extend(saved_handlers)


def _run(driver, lang='en'):
    saved_slots = list(nicegui_context.slot_stack)

    async def invoke():
        async with _user(lang) as (user, client):
            return await driver(user, client)

    try:
        return asyncio.run(invoke())
    finally:
        nicegui_context.slot_stack.clear()
        nicegui_context.slot_stack.extend(saved_slots)


def _page(path, lang='en'):
    """(hrefs, text) of a rendered page."""
    return _render(path, lang)[:2]


def _render(path, lang='en'):
    """(hrefs, text, classes) of a rendered page."""
    async def driver(user, client):
        await user.open(path)
        with user._client:
            elements = list(user._client.elements.values())
        hrefs = {(getattr(e, '_props', None) or {}).get('href') for e in elements} - {None}
        text = ' '.join(str((getattr(e, '_props', None) or {}).get('label') or '') + ' '
                        + str(getattr(e, 'text', '') or '') for e in elements)
        classes = {c for e in elements for c in (getattr(e, '_classes', None) or [])}
        return hrefs, text, classes
    return _run(driver, lang)


def _strip_title(path='/', lang='en'):
    """The homepage AI strip's own title (not the header's What's New, which also says "New")."""
    async def driver(user, client):
        await user.open(path)
        with user._client:
            titles = [e.text for e in user._client.elements.values()
                      if 'gs-ai-strip-title' in (getattr(e, '_classes', None) or [])]
        assert len(titles) == 1, titles
        return titles[0]
    return _run(driver, lang)


def _has_unread_dot(dismissed):
    """Open '/', store a dismissed What's New key as an earlier visit would, reopen."""
    async def driver(user, client):
        from nicegui import app
        await user.open('/')
        if dismissed is not None:
            with user._client:
                app.storage.user['whats_new_dismissed'] = dismissed
        await user.open('/')
        with user._client:
            return any('whats-new-dot' in (getattr(e, '_classes', None) or [])
                       or (getattr(e, '_props', None) or {}).get('id') == 'whats-new-dot'
                       for e in user._client.elements.values())
    return _run(driver)


def _redirect():
    async def driver(user, client):
        response = await client.get('/chatgpt', follow_redirects=False)
        return response.status_code, response.headers.get('location')
    return _run(driver)


def test_with_no_setting_the_ai_page_is_unchanged(settings):
    settings()
    hrefs, text = _page('/ai')
    assert '/chatgpt' in hrefs and 'Open in ChatGPT' in text
    assert 'MCP · Planned' in text and '/api/chatgpt/privacy' in hrefs
    assert 'Add to' not in text and listings.MCP_URL not in text
    assert _redirect() == (307, listings.GPT_URL)


def test_the_homepage_strip_offers_the_gpt_with_no_setting(settings):
    settings()
    hrefs, text = _page('/')
    assert '/chatgpt' in hrefs and 'Open in ChatGPT' in text and 'New: GenizahSearch' not in text
    assert 'the GenizahSearch GPT' in text  # the What's New entry, unchanged


def test_the_chatgpt_plugin_replaces_the_gpt_everywhere(settings):
    settings(AI_CHATGPT_PLUGIN_URL=PLUGIN)
    hrefs, text = _page('/ai')
    assert PLUGIN in hrefs and 'Add to ChatGPT' in text
    assert '/chatgpt' not in hrefs and 'Open in ChatGPT' not in text and 'Planned' not in text
    assert listings.MCP_URL in text and listings.GEMINI_INSTALL in text
    assert {'/ai/privacy', '/terms', '/support'} <= hrefs and '/api/chatgpt/privacy' not in hrefs
    home_hrefs, home_text = _page('/')
    assert PLUGIN in home_hrefs and '/chatgpt' not in home_hrefs
    assert 'the GenizahSearch GPT' not in home_text  # the GPT's What's New entry is gone
    assert 'New: GenizahSearch in ChatGPT - ask about the manuscripts' in home_text
    assert _redirect() == (307, PLUGIN)


def test_claude_first_keeps_the_gpt_beside_it(settings):
    """If Claude is listed before the ChatGPT plugin, ChatGPT readers keep the GPT."""
    settings(AI_CLAUDE_CONNECTOR_URL=CONNECTOR)
    for path in ('/ai', '/'):
        hrefs, text = _page(path)
        assert {CONNECTOR, '/chatgpt'} <= hrefs and 'Add to Claude' in text and 'Open in ChatGPT' in text
        assert 'Add to ChatGPT' not in text and 'Planned' not in text
    assert _redirect() == (307, listings.GPT_URL)


def test_the_announcement_names_only_what_is_live(settings):
    settings(AI_CHATGPT_PLUGIN_URL=PLUGIN, AI_LAUNCH_ANNOUNCE_UNTIL='2026-11-30')
    assert _strip_title() == 'New: GenizahSearch in ChatGPT'
    settings(AI_CHATGPT_PLUGIN_URL=PLUGIN, AI_CLAUDE_CONNECTOR_URL=CONNECTOR,
             AI_LAUNCH_ANNOUNCE_UNTIL='2026-11-30')
    assert _strip_title() == 'New: GenizahSearch in ChatGPT and Claude'
    hrefs, text = _page('/')
    assert {PLUGIN, CONNECTOR} <= hrefs
    assert 'New: GenizahSearch in ChatGPT and Claude - ask about the manuscripts' in text  # What's New


def test_after_the_announcement_the_strip_is_quiet_but_keeps_the_buttons(settings):
    settings(today=date(2026, 12, 1), AI_CHATGPT_PLUGIN_URL=PLUGIN, AI_LAUNCH_ANNOUNCE_UNTIL='2026-11-30')
    hrefs, text, classes = _render('/')
    assert 'gs-ai-new' not in classes and _strip_title() == 'Explore the Genizah with AI'
    assert PLUGIN in hrefs
    # The header's What's New entry stays until the next announcement wave.
    assert 'New: GenizahSearch in ChatGPT - ask about the manuscripts' in text
    settings(AI_CHATGPT_PLUGIN_URL=PLUGIN, AI_LAUNCH_ANNOUNCE_UNTIL='2026-11-30')
    assert 'gs-ai-new' in _render('/')[2]


def test_the_announcement_is_hebrew_in_the_hebrew_ui(settings):
    settings(AI_CHATGPT_PLUGIN_URL=PLUGIN, AI_CLAUDE_CONNECTOR_URL=CONNECTOR,
             AI_LAUNCH_ANNOUNCE_UNTIL='2026-11-30')
    assert _strip_title(lang='he') == 'חדש: GenizahSearch ב־ChatGPT וב־Claude'
    _, text = _page('/', lang='he')
    assert 'שאלו על כתבי היד במילים שלכם' in text  # the What's New entry
    assert 'New: GenizahSearch' not in text and 'Add to' not in text


def test_after_retirement_the_gpt_is_never_offered(settings):
    settings(today=AFTER)
    hrefs, text = _page('/ai')
    assert '/chatgpt' not in hrefs and 'Open in ChatGPT' not in text and 'Planned' not in text
    assert listings.MCP_URL in text
    home_hrefs, _ = _page('/')
    assert '/chatgpt' not in home_hrefs
    assert _redirect() == (307, '/ai')


def test_after_retirement_claude_alone_and_no_gpt(settings):
    settings(today=AFTER, AI_CLAUDE_CONNECTOR_URL=CONNECTOR)
    for path in ('/ai', '/'):
        hrefs, text = _page(path)
        assert CONNECTOR in hrefs and '/chatgpt' not in hrefs and 'Open in ChatGPT' not in text
    assert _redirect() == (307, '/ai')


def test_each_launch_lights_the_unread_dot_again(settings):
    """A reader who dismissed the 9.6.0 announcement sees the dot again when a listing goes
    live; with the new key stored the dot stays off; the second listing lights it once more."""
    from web.main import WHATS_NEW_VERSION
    settings()
    assert _has_unread_dot(WHATS_NEW_VERSION) is False          # control: dismissed, nothing new
    settings(AI_CHATGPT_PLUGIN_URL=PLUGIN)
    assert _has_unread_dot(WHATS_NEW_VERSION) is True
    assert _has_unread_dot(WHATS_NEW_VERSION + '+ai-chatgpt') is False
    settings(AI_CHATGPT_PLUGIN_URL=PLUGIN, AI_CLAUDE_CONNECTOR_URL=CONNECTOR)
    assert _has_unread_dot(WHATS_NEW_VERSION + '+ai-chatgpt') is True
