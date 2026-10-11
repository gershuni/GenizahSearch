"""MCP size budgets (docs/plans/MCP_SERVER_PLAN.md D9), build commit 4.

The GPT's pages stay byte-identical; MCP pages are sized by their escaped form;
job pages and details pages follow the caller's channel (details from a shared
snapshot); a record too large for MCP fails explicitly; and the final guard keeps
every real HTTP body under the bound for both protocol generations.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import queue
from contextlib import asynccontextmanager

import httpx
import httpx2
import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response
from mcp.client import Client
from mcp.client.streamable_http import streamable_http_client

import web.api_hardening as hardening
import web.chatgpt_jobs as chatgpt_jobs
import web.mcp_server as mcp_server
from shared.manuscript_details import SECTIONS
from web.chatgpt_details import MCP_DETAILS_RECORD_BYTES, register_manuscript_details
from web.chatgpt_jobs import register_chatgpt_jobs
from web.chatgpt_pagination import (
    MCP_PAGE_BYTES, MCP_RESPONSE_BYTES, PAGE_BYTES, build_pages, mcp_escaped_size, page_budget)

MODERN = '2026-07-28'


def hebrew_payload(n=60, chars=2600):
    text = ('בראשית ברא אלהים "את" השמים ' * (chars // 20 + 1))[:chars]
    return {'schema_version': 1, 'source': 'search', 'generated_at': '2026-10-09T00:00:00Z',
            'request': {'search_mode': 'exact', 'limit': 100}, 'count': n, 'total': n,
            'warnings': [{'code': 'engine_cap'}],
            'results': [{'uid': str(i), 'locator': {'sys_id': f'99{i:010d}', 'p_num': 1},
                         'shelfmark': f'T-S {i}.1', 'snippet': text,
                         'matches': [{'text': text[:400]}] * 5} for i in range(n)],
            'filtered': [{'uid': f'f{i}', 'shelfmark': f'F {i}', 'snippet': text[:900]} for i in range(7)]}


def quote_payload(n=40):
    text = '"\\"' * 3000
    return {'schema_version': 1, 'source': 'search', 'generated_at': '2026-10-09T00:00:00Z',
            'request': {}, 'count': n, 'total': n, 'warnings': [],
            'results': [{'uid': str(i), 'snippet': text} for i in range(n)], 'filtered': []}


def digest(pages):
    return hashlib.sha256(json.dumps({k: [p.hex() for p in v] for k, v in pages.items()}).encode()).hexdigest()


def uids(pages, collection):
    return [row['uid'] for page in pages[collection] for row in json.loads(page)[collection]]


# --- pagination ---------------------------------------------------------------------

# Recorded from build_pages at b40aeea7, before the budget arguments existed.
GPT_REFERENCE = {
    'hebrew': ({'results': 6, 'filtered': 1}, 440970,
               'ecf3756314ee8ebf3b1c4d99291f9e46f19a98dbafa1950f61823c8f33e39e97'),
    'quotes': ({'results': 10, 'filtered': 1}, 726566,
               '35618fca76f754f154e0e4d4912b0295f11d1869e8569645e7bc234e2b48a76e'),
}


@pytest.mark.parametrize('name,payload', [('hebrew', hebrew_payload()), ('quotes', quote_payload())])
def test_gpt_pages_are_byte_identical(name, payload):
    assert page_budget(None) == page_budget('chatgpt') == (PAGE_BYTES, len)
    pages, stored = build_pages(payload, 'job-token')
    counts, reference_stored, reference_digest = GPT_REFERENCE[name]
    assert ({k: len(v) for k, v in pages.items()}, stored, digest(pages)) == (counts, reference_stored, reference_digest)


@pytest.mark.parametrize('payload', [hebrew_payload(), quote_payload()])
def test_mcp_pages_fit_their_escaped_budget_and_keep_every_candidate(payload):
    page_bytes, measure = page_budget('mcp')
    pages, stored = build_pages(payload, 'job-token', page_bytes=page_bytes, measure=measure)
    for collection in ('results', 'filtered'):
        assert uids(pages, collection) == [row['uid'] for row in payload[collection]]
        for page in pages[collection]:
            assert mcp_escaped_size(page) <= MCP_PAGE_BYTES
    assert stored == sum(len(page) for collection in pages.values() for page in collection)
    gpt_pages, _ = build_pages(payload, 'job-token')
    assert len(pages['results']) > len(gpt_pages['results'])


def test_a_row_too_large_for_mcp_drops_its_snippet_not_the_job():
    payload = hebrew_payload(n=1, chars=20000)
    pages, _ = build_pages(payload, 'job-token')  # fits the GPT's raw-byte page whole
    assert 'snippet_omitted' not in json.loads(pages['results'][0])['results'][0]
    pages, _ = build_pages(payload, 'job-token', page_bytes=MCP_PAGE_BYTES, measure=mcp_escaped_size)
    row = json.loads(pages['results'][0])['results'][0]
    assert row['snippet_omitted'] is True and 'snippet' not in row
    assert row['locator'] == payload['results'][0]['locator'] and len(row['matches']) == 3


def test_a_row_too_large_even_without_its_snippet_fails_explicitly():
    payload = hebrew_payload(n=1, chars=20000)
    payload['results'][0]['matches'] = [{'text': payload['results'][0]['snippet']}]
    build_pages(payload, 'job-token')  # fits the GPT's raw-byte page
    with pytest.raises(ValueError):
        build_pages(payload, 'job-token', page_bytes=MCP_PAGE_BYTES, measure=mcp_escaped_size)


# --- jobs -------------------------------------------------------------------------------

def test_jobs_size_pages_by_the_callers_channel(monkeypatch):
    monkeypatch.setattr(chatgpt_jobs, 'POLL_SECONDS', 0.01)
    app = FastAPI()

    async def handler(request):
        return JSONResponse(hebrew_payload())

    async def prepare(kind, request):
        return request

    register_chatgpt_jobs(app, {'/search': handler, '/parallels': handler}, prepare,
                          lambda response: response, lambda request: request.client.host)

    async def first_page(target):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=target), base_url='http://t') as c:
            job = (await c.post('/chatgpt/search/jobs', json={})).json()['job_id']
            for _ in range(500):
                page = await c.get(f'/chatgpt/jobs/{job}')
                if page.status_code == 200:
                    return page.content
            raise AssertionError('job did not finish')

    gpt = asyncio.run(first_page(app))
    mcp = asyncio.run(first_page(mcp_server._with_mcp_channel(app)))
    assert len(gpt) <= PAGE_BYTES and mcp_escaped_size(gpt) > MCP_PAGE_BYTES
    assert mcp_escaped_size(mcp) <= MCP_PAGE_BYTES
    assert len(json.loads(mcp)['results']) < len(json.loads(gpt)['results'])


# --- details ---------------------------------------------------------------------------

def details_app(rows):
    app = FastAPI()

    def loader(sys_id, section):
        return rows, 'available'

    register_manuscript_details(app, lambda request: request.client.host, loader=loader)
    return app


def get_details(target, **params):
    async def go():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=target), base_url='http://t') as c:
            return await c.get('/chatgpt/manuscript-details', params=params)
    return asyncio.run(go())


def test_details_pages_follow_the_callers_channel_from_one_snapshot():
    rows = [{'n': i, 'text': 'שלום "x" ' * 750} for i in range(10)]
    app = details_app(rows)
    section = sorted(SECTIONS)[0]
    gpt = get_details(app, sys_id='990001', section=section).json()
    mcp = get_details(mcp_server._with_mcp_channel(app), sys_id='990001', section=section,
                      offset=0, snapshot=gpt['snapshot'])
    assert mcp.status_code == 200
    mcp = mcp.json()
    assert mcp['snapshot'] == gpt['snapshot']
    assert 0 < mcp['returned'] < gpt['returned']
    assert sum(mcp_escaped_size(json.dumps(r, ensure_ascii=False).encode()) for r in mcp['records']) \
        <= MCP_DETAILS_RECORD_BYTES
    assert mcp['next_offset'] == mcp['returned']


def test_a_record_too_large_for_mcp_is_an_explicit_error():
    app = details_app([{'text': 'שלום ' * 6000}])  # ~60 KB raw, fits the GPT; ~180 KB escaped
    section = sorted(SECTIONS)[0]
    assert get_details(app, sys_id='990002', section=section).json()['returned'] == 1
    refused = get_details(mcp_server._with_mcp_channel(app), sys_id='990002', section=section)
    assert refused.status_code == 503 and refused.json()['error']['code'] == 'details_too_large'


# --- the final guard, on the wire -------------------------------------------------------------

@pytest.fixture
def wire_host(monkeypatch):
    monkeypatch.setattr(hardening, '_event_queue', queue.Queue())
    monkeypatch.setattr(hardening, '_start_drain_thread_once', lambda: None)
    monkeypatch.setattr(mcp_server, 'MCP_ENABLED', True)
    facade = FastAPI()
    facade.state.chatgpt_jobs = {}
    facade.state.body = b'{}'
    facade.state.status = 200

    @facade.get('/chatgpt/capabilities')
    async def capabilities():
        return Response(facade.state.body, status_code=facade.state.status, media_type='application/json')

    @asynccontextmanager
    async def lifespan(app):
        yield

    host = FastAPI(lifespan=lifespan)
    host.mount('/api', facade)
    assert mcp_server.register_mcp_server(host, facade)
    return host, facade


def call_on_the_wire(host, mode):
    sizes = []

    async def hook(response):
        if b'"tools/call"' in (response.request.content or b''):
            await response.aread()
            sizes.append(len(response.content))

    async def go():
        async with host.router.lifespan_context(host):
            http = httpx2.AsyncClient(transport=httpx2.ASGITransport(app=host), base_url='http://127.0.0.1:8081',
                                      event_hooks={'response': [hook]})
            async with Client(streamable_http_client('http://127.0.0.1:8081/mcp', http_client=http),
                              mode=mode) as client:
                return await client.call_tool('get_capabilities', {})
    result = asyncio.run(asyncio.wait_for(go(), timeout=30))
    [size] = sizes
    return result, size


HEBREW_45K = json.dumps({'t': 'שלום "x" ' * 3000}, ensure_ascii=False).encode()
ASCII_96K = json.dumps({'t': 'abc ' * 24000}).encode()
ASCII_99K = json.dumps({'t': 'abc ' * 24800}).encode()


@pytest.mark.parametrize('body,mode,fits', [
    (HEBREW_45K, 'legacy', True),
    (HEBREW_45K, MODERN, False),   # 45 KB of Hebrew is ~105 KB once ASCII-escaped
    (ASCII_96K, 'legacy', True),
    (ASCII_96K, MODERN, True),
    (ASCII_99K, 'legacy', False),
    (ASCII_99K, MODERN, False),
], ids=['hebrew45k-legacy', 'hebrew45k-modern', 'ascii96k-legacy', 'ascii96k-modern',
        'ascii99k-legacy', 'ascii99k-modern'])
def test_every_real_response_stays_under_the_bound(wire_host, body, mode, fits):
    host, facade = wire_host
    facade.state.body = body
    result, size = call_on_the_wire(host, mode)
    assert size <= MCP_RESPONSE_BYTES
    if fits:
        assert result.is_error is False and result.content[0].text == body.decode()
    else:
        assert result.is_error is True
        error = json.loads(result.content[0].text)['error']
        assert error['code'] == 'mcp_response_too_large' and error['limit_bytes'] == MCP_RESPONSE_BYTES


# --- exact GPT boundaries (Codex build review: the digests above never reach them) -----------
# Literal numbers on purpose: importing PAGE_BYTES would let a changed limit pass.

def _one_row_payload(filler, field='shelfmark'):
    # The size sits in a field that is never dropped, so the page limit itself is
    # what decides; a snippet would be omitted instead (test below).
    return {'schema_version': 1, 'source': 'search', 'generated_at': '2026-10-09T00:00:00Z',
            'request': {}, 'count': 1, 'total': 1, 'warnings': [],
            'results': [{'uid': '0', field: filler}], 'filtered': []}


def _sized_gpt_page(target_bytes, field='shelfmark'):
    overhead = len(build_pages(_one_row_payload('', field), 'job-token')[0]['results'][0])
    return _one_row_payload('a' * (target_bytes - overhead), field)


def test_a_gpt_page_of_exactly_80000_bytes_still_fits():
    pages, _ = build_pages(_sized_gpt_page(80_000), 'job-token')
    assert [len(page) for page in pages['results']] == [80_000]


def test_a_gpt_page_of_80001_bytes_is_refused():
    with pytest.raises(ValueError):
        build_pages(_sized_gpt_page(80_001), 'job-token')


def test_an_80001_byte_snippet_row_is_served_without_its_snippet():
    pages, _ = build_pages(_sized_gpt_page(80_000, 'snippet'), 'job-token')
    assert [len(page) for page in pages['results']] == [80_000]
    pages, _ = build_pages(_sized_gpt_page(80_001, 'snippet'), 'job-token')
    row = json.loads(pages['results'][0])['results'][0]
    assert row == {'uid': '0', 'snippet_omitted': True}


def test_a_gpt_details_record_of_exactly_70000_bytes_is_served_and_70001_is_not():
    section = sorted(SECTIONS)[0]
    fits = details_app([{'t': 'a' * (70_000 - len('{"t":""}'))}])
    assert get_details(fits, sys_id='990003', section=section).json()['returned'] == 1
    too_big = details_app([{'t': 'a' * (70_001 - len('{"t":""}'))}])
    refused = get_details(too_big, sys_id='990004', section=section)
    assert refused.status_code == 503 and refused.json()['error']['code'] == 'details_too_large'


# --- error paths and refusals on the wire ----------------------------------------------------------

@pytest.mark.parametrize('mode,code', [('legacy', 'rate_limited'), (MODERN, 'mcp_response_too_large')])
def test_an_oversized_error_message_is_bounded_too(wire_host, mode, code):
    host, facade = wire_host
    # 40,000 Hebrew letters: about 80 KB as UTF-8, about 240 KB once ASCII-escaped.
    facade.state.body = json.dumps({'error': {'code': 'rate_limited', 'message': 'א' * 40_000}},
                                   ensure_ascii=False).encode()
    facade.state.status = 429
    result, size = call_on_the_wire(host, mode)
    assert size <= MCP_RESPONSE_BYTES and result.is_error is True
    assert json.loads(result.content[0].text)['error']['code'] == code


def raw_post(host, body, protocol='2025-11-25'):
    async def go():
        async with host.router.lifespan_context(host):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=host),
                                         base_url='http://127.0.0.1:8081') as client:
                return await client.post('/mcp', content=body, headers={
                    'accept': 'application/json, text/event-stream', 'content-type': 'application/json',
                    'mcp-protocol-version': protocol})
    return asyncio.run(asyncio.wait_for(go(), timeout=30))


# Long enough to break the id/name limits, short enough to stay under the 64 KiB body limit,
# so these reach the id and name checks themselves.
MEDIUM = 'x' * 5_000


@pytest.mark.parametrize('protocol', ['2025-11-25', MODERN])
@pytest.mark.parametrize('message,reason', [
    ({'jsonrpc': '2.0', 'id': MEDIUM, 'method': 'tools/list'}, 'request_id_too_long'),
    ({'jsonrpc': '2.0', 'id': 1, 'method': 'tools/call', 'params': {'name': MEDIUM, 'arguments': {}}},
     'tool_name_too_long'),
    ({'jsonrpc': '2.0', 'id': 1, 'method': MEDIUM}, 'method_too_long'),
], ids=['long-id', 'long-tool-name', 'long-method'])
def test_oversized_ids_and_names_are_refused_without_echo(wire_host, protocol, message, reason):
    host, _ = wire_host
    response = raw_post(host, json.dumps(message).encode(), protocol)
    assert response.status_code == 400
    assert response.json()['error']['message'] == f'Request refused: {reason}.'
    assert len(response.content) < 500 and MEDIUM[:200] not in response.text


@pytest.mark.parametrize('protocol', ['2025-11-25', MODERN])
def test_a_body_over_64_kib_is_refused_without_echo(wire_host, protocol):
    # Codex's case: a 100,001-character id made the reply exceed 100,000 bytes.
    host, _ = wire_host
    response = raw_post(host, json.dumps({'jsonrpc': '2.0', 'id': 'x' * 100_001, 'method': 'tools/list'}).encode(),
                        protocol)
    assert response.status_code == 413
    assert response.json()['error']['message'] == 'Request refused: request_too_large.'
    assert len(response.content) < 500


def test_limits_leave_ordinary_requests_alone(wire_host):
    host, _ = wire_host
    ok = raw_post(host, json.dumps({'jsonrpc': '2.0', 'id': 'a' * 128, 'method': 'initialize', 'params': {
        'protocolVersion': '2025-11-25', 'capabilities': {}, 'clientInfo': {'name': 't', 'version': '0'}}}).encode())
    assert ok.status_code == 200 and ok.json()['id'] == 'a' * 128
