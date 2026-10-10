"""Public pages the AI-directory listings point to (docs/plans/MCP_SERVER_PLAN.md, build 7).

OpenAI's plugin submission needs a privacy policy, terms of use and a support page
from the same publisher, plus a domain-verification file; Anthropic's listing asks
for the privacy policy. Static HTML, like the existing GPT privacy page, so a
reviewer can read them without the app's scripts.
"""
from __future__ import annotations

import os
from pathlib import Path

from fastapi.responses import HTMLResponse, PlainTextResponse, Response

ROOT = Path(__file__).resolve().parents[1]
PAGES = {
    '/terms': ROOT / 'integrations' / 'mcp' / 'terms.html',
    '/support': ROOT / 'integrations' / 'mcp' / 'support.html',
    # The same file the GPT action links to at /api/chatgpt/privacy, which stays.
    '/ai/privacy': ROOT / 'integrations' / 'chatgpt' / 'privacy.html',
}
# OpenAI's domain-verification challenge. Its value comes from the submission form,
# so it is configuration, not code: unset (the default) answers 404.
OPENAI_CHALLENGE_PATH = '/.well-known/openai-apps-challenge'
OPENAI_CHALLENGE_ENV = 'OPENAI_APPS_CHALLENGE'


def _page(source: Path):
    async def page():
        return HTMLResponse(source.read_text(encoding='utf-8'))
    return page


async def openai_challenge():
    value = os.environ.get(OPENAI_CHALLENGE_ENV, '').strip()
    if not value:
        return Response(status_code=404)
    return PlainTextResponse(value)


def register_public_pages(app) -> None:
    for path, source in PAGES.items():
        app.add_api_route(path, _page(source), methods=['GET'], include_in_schema=False)
    app.add_api_route(OPENAI_CHALLENGE_PATH, openai_challenge, methods=['GET'], include_in_schema=False)
