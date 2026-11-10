"""Which AI listings are live, from production settings.

docs/plans/AI_LISTINGS_LAUNCH_PLAN.md. Each listing (the ChatGPT plugin, the Claude connector) is
switched on by putting its public listing URL in production ``.env`` and restarting; with no
setting the site shows what it showed before the listings existed. Settings are read per call, so
a test can render every combination without a restart. No NiceGUI import: ``web/main.py`` reads
this for the ``/chatgpt`` redirect and the What's New entry.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from urllib.parse import urlsplit

GPT_URL = 'https://chatgpt.com/g/g-6aa8230eaf888191bb44c5cb1163a39e-genizahsearch'
# OpenAI retires custom GPTs; from this day the GPT link is dead and never offered.
GPT_RETIRES = date(2026, 12, 11)
MCP_URL = 'https://genizahsearch.com/mcp'
GEMINI_INSTALL = 'gemini extensions install https://github.com/gershuni/genizahsearch-ai-plugins'

CHATGPT_SETTING = 'AI_CHATGPT_PLUGIN_URL'
CLAUDE_SETTING = 'AI_CLAUDE_CONNECTOR_URL'
ANNOUNCE_SETTING = 'AI_LAUNCH_ANNOUNCE_UNTIL'


def _today() -> date:
    return date.today()


# The hosts a listing may live on. A setting is an operator's paste, so this guards against a
# typo or a wrong link (a bare "https://", another site, the old GPT) being published site-wide.
LISTING_HOSTS = {
    CHATGPT_SETTING: frozenset({'chatgpt.com', 'www.chatgpt.com', 'openai.com', 'www.openai.com'}),
    CLAUDE_SETTING: frozenset({'claude.ai', 'www.claude.ai', 'claude.com', 'www.claude.com'}),
}


def _listing_url(setting: str) -> str | None:
    """The setting's URL when it is a well-formed https link to its provider, else None."""
    value = os.environ.get(setting, '').strip()
    try:
        parts = urlsplit(value)
        host = parts.hostname
    except ValueError:
        return None
    if (parts.scheme != 'https' or host not in LISTING_HOSTS[setting] or parts.username is not None
            or parts.password is not None or parts.path in ('', '/') or parts.path.startswith('/g/')):
        return None   # /g/ is a custom GPT, never a plugin listing
    return value


def _announce_until() -> date | None:
    try:
        return date.fromisoformat(os.environ.get(ANNOUNCE_SETTING, '').strip())
    except ValueError:
        return None


@dataclass(frozen=True)
class Listings:
    chatgpt_url: str | None
    claude_url: str | None
    gpt_available: bool   # the old GPT is still offered: no ChatGPT plugin yet, before retirement
    announcing: bool      # a listing is live and the launch announcement has not ended

    @property
    def live(self) -> list[tuple[str, str]]:
        """(assistant, listing URL) for each live listing, ChatGPT first."""
        return [(name, url) for name, url in (('ChatGPT', self.chatgpt_url), ('Claude', self.claude_url)) if url]


def current() -> Listings:
    today = _today()
    chatgpt, claude = _listing_url(CHATGPT_SETTING), _listing_url(CLAUDE_SETTING)
    until = _announce_until()
    return Listings(
        chatgpt_url=chatgpt,
        claude_url=claude,
        gpt_available=chatgpt is None and today < GPT_RETIRES,
        announcing=bool(chatgpt or claude) and until is not None and today <= until,
    )


def chatgpt_target() -> str:
    """Where /chatgpt leads: the plugin listing, else the GPT while it lives, else the AI page."""
    listings = current()
    if listings.chatgpt_url:
        return listings.chatgpt_url
    return GPT_URL if listings.gpt_available else '/ai'


def page_description() -> str:
    """The /ai page's meta description, naming only what is offered."""
    listings = current()
    names = [name for name, _ in listings.live]
    if not names and listings.gpt_available:
        return 'Research Cairo Genizah manuscripts with ChatGPT, a research skill, or the public API.'
    if listings.gpt_available:          # Claude listed first: the GPT is still offered beside it
        names = ['ChatGPT'] + names
    lead = ' and '.join(names) + ', any MCP client' if names else 'any MCP client'
    return f'Research Cairo Genizah manuscripts with {lead}, a research skill, or the public API.'


def whats_new_suffix() -> str:
    """Appended to the What's New key, so each launch lights the unread dot again."""
    names = [name.lower() for name, _ in current().live]
    return '+ai-' + '-'.join(names) if names else ''
