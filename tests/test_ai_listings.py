"""Which AI listings the site offers, from production settings.

docs/plans/AI_LISTINGS_LAUNCH_PLAN.md: with no setting the site offers the GPT until OpenAI retires
it; each listing URL switches its own surfaces on; the What's New key changes with each launch. The
pages themselves are rendered in tests/render_smoke/test_ai_listings_render.py.
"""
from __future__ import annotations

from datetime import date

import pytest

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


def test_with_no_setting_the_gpt_is_offered_until_it_retires(settings):
    settings()
    now = listings.current()
    assert (now.live, now.gpt_available, now.announcing) == ([], True, False)
    assert listings.chatgpt_target() == listings.GPT_URL
    assert listings.whats_new_suffix() == ''
    settings(today=AFTER)
    assert listings.current().gpt_available is False
    assert listings.chatgpt_target() == '/ai'


def test_the_chatgpt_plugin_replaces_the_gpt(settings):
    settings(AI_CHATGPT_PLUGIN_URL=PLUGIN)
    now = listings.current()
    assert now.live == [('ChatGPT', PLUGIN)] and now.gpt_available is False
    assert listings.chatgpt_target() == PLUGIN
    assert listings.whats_new_suffix() == '+ai-chatgpt'


def test_claude_alone_leaves_the_gpt_running(settings):
    settings(AI_CLAUDE_CONNECTOR_URL=CONNECTOR)
    now = listings.current()
    assert now.live == [('Claude', CONNECTOR)] and now.gpt_available is True
    assert listings.chatgpt_target() == listings.GPT_URL
    assert listings.whats_new_suffix() == '+ai-claude'


def test_both_listings_chatgpt_first_and_a_new_key_for_each_launch(settings):
    settings(AI_CHATGPT_PLUGIN_URL=PLUGIN, AI_CLAUDE_CONNECTOR_URL=CONNECTOR)
    assert listings.current().live == [('ChatGPT', PLUGIN), ('Claude', CONNECTOR)]
    assert listings.whats_new_suffix() == '+ai-chatgpt-claude'


@pytest.mark.parametrize('value', [
    'http://chatgpt.com/x', 'chatgpt.com/x', ' ', 'javascript:alert(1)', 'https://', 'https://chatgpt.com',
    'https://chatgpt.com/', 'https://evil.example/app', 'https://chatgpt.com@evil.example/app',
    'https://user:pw@chatgpt.com/app', 'https://chatgpt.com.evil.example/app', 'https://[::1/app',
    listings.GPT_URL,                                   # the old GPT is not a plugin listing
    CONNECTOR,                                          # the other provider's listing
])
def test_only_a_well_formed_provider_link_is_published(settings, value):
    """Codex (launch review): a bare https://, another site, or the GPT itself would have been
    published site-wide, hiding a still-working GPT."""
    settings(AI_CHATGPT_PLUGIN_URL=value)
    assert listings.current().live == []
    assert listings.chatgpt_target() == listings.GPT_URL


@pytest.mark.parametrize('value', ['https://chatgpt.com/apps/x', 'https://claude.ai', 'https://evil.example/x'])
def test_the_claude_setting_takes_only_claude_links(settings, value):
    settings(AI_CLAUDE_CONNECTOR_URL=value)
    assert listings.current().live == []


def test_after_retirement_each_combination(settings):
    settings(today=AFTER, AI_CLAUDE_CONNECTOR_URL=CONNECTOR)
    now = listings.current()
    assert now.live == [('Claude', CONNECTOR)] and now.gpt_available is False
    assert listings.chatgpt_target() == '/ai'
    settings(today=AFTER, AI_CHATGPT_PLUGIN_URL=PLUGIN)
    assert listings.chatgpt_target() == PLUGIN


@pytest.mark.parametrize('today,values,expected', [
    (BEFORE, {}, 'with ChatGPT, a research skill, or the public API.'),
    (AFTER, {}, 'with any MCP client, a research skill, or the public API.'),
    (BEFORE, {'AI_CLAUDE_CONNECTOR_URL': CONNECTOR}, 'with ChatGPT and Claude, any MCP client'),  # GPT + Claude
    (AFTER, {'AI_CLAUDE_CONNECTOR_URL': CONNECTOR}, 'with Claude, any MCP client, a research skill'),
    (BEFORE, {'AI_CHATGPT_PLUGIN_URL': PLUGIN, 'AI_CLAUDE_CONNECTOR_URL': CONNECTOR},
     'with ChatGPT and Claude, any MCP client'),
])
def test_the_page_description_names_only_what_is_offered(settings, today, values, expected):
    """Codex (launch review): after retirement with no listing it still advertised ChatGPT."""
    settings(today=today, **values)
    assert expected in listings.page_description()


@pytest.mark.parametrize('until,today,expected', [
    ('2026-11-30', date(2026, 11, 30), True),     # through the last day
    ('2026-11-30', date(2026, 12, 1), False),
    ('not-a-date', date(2026, 11, 1), False),
    ('', date(2026, 11, 1), False),
])
def test_the_announcement_runs_until_its_date(settings, until, today, expected):
    settings(today=today, AI_CHATGPT_PLUGIN_URL=PLUGIN, AI_LAUNCH_ANNOUNCE_UNTIL=until)
    assert listings.current().announcing is expected


def test_no_announcement_without_a_live_listing(settings):
    settings(AI_LAUNCH_ANNOUNCE_UNTIL='2026-12-31')
    assert listings.current().announcing is False


def test_every_whats_new_entry_is_translated():
    from shared.genizah_translations import TRANSLATIONS
    from web.main import AI_LAUNCH_WHATS_NEW
    assert set(AI_LAUNCH_WHATS_NEW) == {('ChatGPT', 'Claude'), ('ChatGPT',), ('Claude',)}
    for english in AI_LAUNCH_WHATS_NEW.values():
        assert TRANSLATIONS.get(english, english) != english, english
