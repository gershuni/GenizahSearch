"""Page citations for MCP browse results (build commit 5).

docs/plans/MCP_SERVER_PLAN.md section 5.3 and section 8: how_to_cite comes from
shared/transcription_credits.py, decided by what text was actually served
(text_source), on real browse shapes from the public API's own serializer; the
helper's new no-transcription option leaves every existing citation byte-identical.
"""
from __future__ import annotations

import asyncio
import datetime
import hashlib
import json
import queue
from contextlib import asynccontextmanager

import httpx2
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from mcp.client import Client
from mcp.client.streamable_http import streamable_http_client

import web.api_hardening as hardening
import web.mcp_server as mcp_server
from shared.export_utils import MIDRASH_INLINE_CITATION
from shared.search_serializer import serialize_browse_payload
from shared.transcription_credits import KIND_NONE, page_citation
from web.services import BrowsePage

GOITEIN = 'S. D. Goitein, unpublished editions. (T-S 8J)'
PGP_URL = 'https://geniza.princeton.edu/en/documents/1234/'
TODAY = '2026-10-09'


# --- the shared helper ---------------------------------------------------------------

# Recorded from page_citation at 4fa217fd, before transcription_shown existed.
CASES = [None, {'source': 'original'},
         {'source': 'pgp', 'attribution': GOITEIN, 'pgp_url': 'https://geniza.princeton.edu/en/documents/1/'},
         {'source': 'pgp', 'attribution': 'PGP'},
         {'source': 'fgp', 'source_credit': 'Friedberg Genizah Project, ed. X'},
         {'source': 'fgp', 'is_translation': True, 'language': 'English', 'source_credit': 'FGP translation'},
         {'source': 'translation', 'language': 'Hebrew', 'attribution': 'Translator Y'},
         {'source': 'user', 'author': 'Reader Z'}, {'source': 'pending'}, {'source': 'unknown-thing'}]
REFERENCE_DIGEST = 'b48148bb230d91066b801526fb1cd13eb6dfec23f07c0d9eca6be1efe0d7c26a'


def test_existing_page_citations_are_byte_identical():
    out = []
    for lang in ('en', 'he'):
        for version_info in CASES:
            for kwargs in ({}, {'library': 'Cambridge University Library', 'shelfmark': 'T-S 8J1.1',
                                'folio': '1r', 'page_url': 'https://genizahsearch.com/browse?sys_id=990001&page=1',
                                'retrieved_on': datetime.date(2026, 10, 9)}):
                citation = page_citation(version_info, lang=lang, **kwargs)
                out.append([citation.heading, citation.text, citation.credits_midrash, citation.kind])
    assert len(out) == 40
    assert hashlib.sha256(json.dumps(out, ensure_ascii=False).encode()).hexdigest() == REFERENCE_DIGEST


@pytest.mark.parametrize('lang', ['en', 'he'])
def test_no_transcription_credits_nobody(lang):
    citation = page_citation(None, lang=lang, library='Cambridge University Library', shelfmark='T-S 8J1.1',
                             page_url='https://genizahsearch.com/browse?sys_id=1&page=1',
                             retrieved_on=TODAY, transcription_shown=False)
    assert citation.kind == KIND_NONE and citation.credits_midrash is False
    assert MIDRASH_INLINE_CITATION not in citation.text and 'MiDRASH' not in citation.text
    assert 'T-S 8J1.1' in citation.text and 'genizahsearch.com/browse?sys_id=1' in citation.text


def test_no_transcription_and_a_version_are_refused_together():
    with pytest.raises(ValueError):
        page_citation({'source': 'pgp', 'attribution': GOITEIN}, transcription_shown=False)


# --- real browse shapes ---------------------------------------------------------------------

def browse_payload(*, text='', pgp=None, shelfmark='T-S 8J1.1', volume_ie='IE12345', p_num=2):
    page = BrowsePage(uid='IE12345_P2_FL3', p_num=p_num, text=text, full_header='', total_pages=4,
                      current_idx=1, sys_id='990001234', fl_id='FL3', shelfmark=shelfmark,
                      library_code='CUL')
    page.volume_ie = volume_ie
    return serialize_browse_payload(page=page, pgp=pgp, fjms=None, nli=None)


PGP_WITH_TEXT = {'page_section_text': 'נוסח פרינסטון', 'transcription_source': GOITEIN, 'pgp_url': PGP_URL}
PGP_METADATA_ONLY = {'transcription_source': GOITEIN, 'pgp_url': PGP_URL}


def test_a_pgp_page_transcription_credits_pgp_not_midrash():
    payload = browse_payload(text='טקסט אוטומטי', pgp=PGP_WITH_TEXT)
    assert payload['text_source'] == 'pgp_transcription'
    cite = mcp_server.how_to_cite(payload, TODAY)
    assert cite['kind'] == 'pgp' and cite['credits_midrash'] is False
    assert GOITEIN in cite['en'] and PGP_URL in cite['en'] and 'Princeton Geniza Project' in cite['en']
    assert 'MiDRASH' not in cite['en'] and 'MiDRASH' not in cite['he']


def test_a_snippet_credits_midrash_even_with_pgp_metadata_on_the_page():
    payload = browse_payload(text='טקסט אוטומטי', pgp=PGP_METADATA_ONLY)
    assert payload['text_source'] == 'snippet'
    cite = mcp_server.how_to_cite(payload, TODAY)
    assert cite['kind'] == 'htr' and cite['credits_midrash'] is True
    assert MIDRASH_INLINE_CITATION in cite['en']
    assert GOITEIN not in cite['en'] and 'Princeton' not in cite['en']


def test_a_page_with_no_text_credits_no_transcription():
    payload = browse_payload(text='', pgp=None)
    assert payload['text_source'] == 'none'
    cite = mcp_server.how_to_cite(payload, TODAY)
    assert cite['kind'] == KIND_NONE and cite['credits_midrash'] is False
    assert 'MiDRASH' not in cite['en'] and 'Princeton' not in cite['en']


def test_missing_pgp_attribution_invents_no_name():
    payload = browse_payload(text='', pgp={'page_section_text': 'נוסח'})
    cite = mcp_server.how_to_cite(payload, TODAY)
    assert cite['kind'] == 'pgp' and 'None' not in cite['en'] and 'MiDRASH' not in cite['en']


def test_citation_names_the_page_it_was_served():
    cite = mcp_server.how_to_cite(browse_payload(text='x'), TODAY)
    assert 'Cambridge University Library, T-S 8J1.1' in cite['en']
    assert 'https://genizahsearch.com/browse?sys_id=990001234&page=2&volume_ie=IE12345' in cite['en']
    assert 'Oct. 9, 2026' in cite['en'] and 'כיצד' not in cite['en']
    assert cite['he'] != cite['en'] and '2026' in cite['he']


def test_a_page_without_a_shelfmark_is_cited_by_its_id():
    cite = mcp_server.how_to_cite(browse_payload(text='x', shelfmark=''), TODAY)
    assert 'ID: 990001234' in cite['en']


# --- through the tool ------------------------------------------------------------------------

@pytest.fixture
def host(monkeypatch):
    monkeypatch.setattr(hardening, '_event_queue', queue.Queue())
    monkeypatch.setattr(hardening, '_start_drain_thread_once', lambda: None)
    monkeypatch.setattr(mcp_server, 'MCP_ENABLED', True)
    facade = FastAPI()
    facade.state.chatgpt_jobs = {}
    facade.state.queries = []
    facade.state.payload = browse_payload(text='טקסט אוטומטי', pgp=PGP_METADATA_ONLY)

    @facade.get('/chatgpt/browse')
    async def browse(request: Request):
        facade.state.queries.append(dict(request.query_params))
        return JSONResponse(facade.state.payload)

    @asynccontextmanager
    async def lifespan(app):
        yield

    app = FastAPI(lifespan=lifespan)
    app.mount('/api', facade)
    assert mcp_server.register_mcp_server(app, facade)
    return app, facade


def call_browse(app, arguments, mode='legacy'):
    async def go():
        async with app.router.lifespan_context(app):
            http = httpx2.AsyncClient(transport=httpx2.ASGITransport(app=app), base_url='http://127.0.0.1:8081')
            async with Client(streamable_http_client('http://127.0.0.1:8081/mcp', http_client=http),
                              mode=mode) as client:
                return await client.call_tool('browse_manuscript_page', arguments)
    return asyncio.run(asyncio.wait_for(go(), timeout=30))


@pytest.mark.parametrize('mode', ['legacy', '2026-07-28'])
def test_browse_tool_forwards_the_locator_and_adds_how_to_cite(host, mode):
    app, facade = host
    result = call_browse(app, {'sys_id': '990001234', 'p_num': 2, 'volume_ie': 'IE12345'}, mode)
    assert result.is_error is False
    page = json.loads(result.content[0].text)
    assert page['text_source'] == 'snippet' and page['locator']['sys_id'] == '990001234'
    assert page['how_to_cite']['credits_midrash'] is True and MIDRASH_INLINE_CITATION in page['how_to_cite']['en']
    assert facade.state.queries == [{'sys_id': '990001234', 'p_num': '2', 'volume_ie': 'IE12345',
                                     'text_cap': '4000'}]


@pytest.mark.parametrize('arguments', [
    {'sys_id': '990001234', 'p_num': 0},
    {'sys_id': '990001234', 'p_num': 1, 'text_cap': 50},
    {'sys_id': '990001234', 'p_num': 1, 'text_cap': 20000},
    {'p_num': 1},
])
def test_browse_tool_refuses_invalid_locators_without_calling_the_facade(host, arguments):
    app, facade = host
    assert call_browse(app, arguments).is_error is True
    assert facade.state.queries == []
