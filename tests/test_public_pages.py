"""Public pages for the AI-directory listings (docs/plans/MCP_SERVER_PLAN.md, build 7)."""
from __future__ import annotations

import html
import re
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from PIL import Image

from web.public_pages import OPENAI_CHALLENGE_ENV, OPENAI_CHALLENGE_PATH, register_public_pages

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def client():
    app = FastAPI()
    register_public_pages(app)
    return TestClient(app)


@pytest.mark.parametrize('path', ['/terms', '/support', '/ai/privacy'])
def test_each_page_is_public_html_from_the_same_publisher(client, path):
    response = client.get(path)
    assert response.status_code == 200 and response.headers['content-type'].startswith('text/html')
    assert 'Hillel Gershuni' in response.text and 'mailto:gershuni@gmail.com' in response.text


def test_the_pages_link_to_each_other(client):
    assert 'genizahsearch.com/support' in client.get('/terms').text
    support = client.get('/support').text
    assert 'genizahsearch.com/terms' in support and 'genizahsearch.com/ai/privacy' in support
    privacy = client.get('/ai/privacy').text
    assert 'genizahsearch.com/terms' in privacy and 'genizahsearch.com/support' in privacy


def _words_from_markdown(text):
    text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)
    text = re.sub(r'^\s*(#+|-)\s*', '', text, flags=re.M)
    return re.sub(r'[*`]', '', text).split()


def _words_from_html(text):
    text = re.sub(r'</?(a|strong|em|code)\b[^>]*>', '', text)  # inline: no word break
    return html.unescape(re.sub(r'<[^>]+>', ' ', text)).split()


def test_the_published_terms_are_exactly_the_owner_approved_text(client):
    # The owner edited integrations/mcp/TERMS_DRAFT.md on 2026-10-10; the page must carry
    # that text, without the draft banner above the title or the review notes below '---'.
    draft = (ROOT / 'integrations' / 'mcp' / 'TERMS_DRAFT.md').read_text(encoding='utf-8')
    approved = draft[draft.index('# Terms of Use'):draft.index('\n---')]
    published = re.search(r'<main>(.*)</main>', client.get('/terms').text, re.S).group(1)
    assert _words_from_html(published) == _words_from_markdown(approved)
    assert 'DRAFT' not in published and 'Sources checked' not in published


def test_the_privacy_policy_discloses_the_mcp_usage_record(client):
    text = client.get('/ai/privacy').text
    for phrase in ('https://genizahsearch.com/mcp', 'the tool name', 'MCP protocol version',
                   'the name the client gives itself', 'never include tool arguments, search text, or results',
                   'keeps no conversation state', 'Anthropic', 'Google'):
        assert phrase in text, phrase


def test_the_openai_challenge_is_served_only_when_configured(client, monkeypatch):
    monkeypatch.delenv(OPENAI_CHALLENGE_ENV, raising=False)
    assert client.get(OPENAI_CHALLENGE_PATH).status_code == 404
    monkeypatch.setenv(OPENAI_CHALLENGE_ENV, '  token-from-openai-123  ')
    response = client.get(OPENAI_CHALLENGE_PATH)
    assert response.status_code == 200 and response.text == 'token-from-openai-123'
    assert response.headers['content-type'].startswith('text/plain')


def test_the_directory_icon_is_a_512_pixel_square_png():
    with Image.open(ROOT / 'web' / 'static' / 'genizahsearch-icon-512.png') as icon:
        assert (icon.format, icon.size) == ('PNG', (512, 512))


def test_the_web_app_registers_the_pages():
    source = (ROOT / 'web' / 'main.py').read_text(encoding='utf-8')
    assert 'register_public_pages(app)' in source
