"""The six MCP tools over the REAL /api/chatgpt facade (build commit 6).

docs/plans/MCP_SERVER_PLAN.md sections 4, 5 and 8. Only the search engine under the
facade is a stub; job admission, polling, pagination, details snapshots, capacity
limits and shutdown are the production code paths the GPT uses.
"""
from __future__ import annotations

import asyncio
import json
import queue
from contextlib import asynccontextmanager

import httpx
import httpx2
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from mcp.client import Client
from mcp.client.streamable_http import streamable_http_client

import web.api_hardening as hardening
import web.chatgpt_details as chatgpt_details
import web.chatgpt_jobs as chatgpt_jobs
import web.mcp_server as mcp_server
from shared.manuscript_details import SECTIONS
from web.chatgpt_api import register_chatgpt_api
from web.chatgpt_pagination import MCP_RESPONSE_BYTES

MODERN = '2026-07-28'
HEBREW = 'בראשית ברא אלהים "את" השמים '


def search_payload(n=25, chars=300):
    text = (HEBREW * (chars // 20 + 1))[:chars]
    return {'schema_version': 1, 'source': 'search', 'generated_at': '2026-10-09T00:00:00Z',
            'request': {'search_mode': 'exact'}, 'count': n, 'total': n, 'warnings': [],
            'results': [{'uid': str(i), 'locator': {'sys_id': f'99{i:08d}', 'p_num': '1'},
                         'shelfmark': f'T-S {i}', 'snippet': text} for i in range(n)],
            'filtered': [{'uid': 'f0', 'shelfmark': 'F 0', 'snippet': text}]}


class Engine:
    """The stub under the facade: records requests, can be held, returns a payload."""

    def __init__(self):
        self.calls = []
        self.gate = asyncio.Event()
        self.gate.set()
        self.payload = search_payload()
        self.browse = {'schema_version': 1, 'source': 'browse', 'locator': {'sys_id': '990001', 'p_num': 1},
                       'shelfmark': 'T-S 1', 'library': {'code': 'CUL', 'name': 'Cambridge University Library'},
                       'text': 'טקסט', 'text_source': 'snippet', 'metadata': {}, 'warnings': []}

    async def __call__(self, request: Request):
        body = await request.body()
        self.calls.append((request.url.path, json.loads(body) if body else dict(request.query_params)))
        if request.url.path.endswith('/browse'):
            return JSONResponse(self.browse)
        if request.url.path.endswith('/capabilities'):
            return JSONResponse({'search_modes': ['exact']})
        await self.gate.wait()
        return JSONResponse(self.payload)


@pytest.fixture
def stack(monkeypatch):
    monkeypatch.setattr(hardening, '_event_queue', queue.Queue())
    monkeypatch.setattr(hardening, '_start_drain_thread_once', lambda: None)
    monkeypatch.setattr(mcp_server, 'MCP_ENABLED', True)
    monkeypatch.setattr(chatgpt_jobs, 'POLL_SECONDS', 0.05)
    rows = [{'n': i, 'source': 'רשומה'} for i in range(25)]
    real_register = chatgpt_details.register_manuscript_details
    monkeypatch.setattr(chatgpt_details, 'register_manuscript_details',
                        lambda app, owner: real_register(app, owner, loader=lambda s, sec: (rows, 'available')))
    engine = Engine()
    child = FastAPI()
    for kind in ('search', 'parallels', 'browse', 'capabilities'):
        child.add_api_route('/' + kind, engine, methods=['POST', 'GET'])
    register_chatgpt_api(child)

    @asynccontextmanager
    async def lifespan(app):
        try:
            yield
        finally:
            await child.state.shutdown_chatgpt_jobs()  # as web/main.py's app.on_shutdown

    host = FastAPI(lifespan=lifespan)
    host.mount('/api', child)
    assert mcp_server.register_mcp_server(host, child)
    return host, child, engine


def client_for(host, mode='legacy', sizes=None):
    async def hook(response):
        if sizes is not None and b'"tools/call"' in (response.request.content or b''):
            await response.aread()
            sizes.append(len(response.content))
    http = httpx2.AsyncClient(transport=httpx2.ASGITransport(app=host, client=('198.51.100.9', 4444)),
                              base_url='http://127.0.0.1:8081', event_hooks={'response': [hook]})
    return Client(streamable_http_client('http://127.0.0.1:8081/mcp', http_client=http), mode=mode)


def body(result):
    assert result.content and result.content[0].text
    return json.loads(result.content[0].text)


def run(host, script, mode='legacy', sizes=None):
    async def go():
        async with host.router.lifespan_context(host):
            async with client_for(host, mode, sizes) as client:
                return await script(client)
    return asyncio.run(asyncio.wait_for(go(), timeout=60))


# --- the tool list -------------------------------------------------------------------------------

def test_six_tools_with_the_planned_annotations(stack):
    host, _, _ = stack

    async def script(client):
        return (await client.list_tools()).tools
    tools = {tool.name: tool for tool in run(host, script)}
    assert {name: (t.annotations.read_only_hint, t.annotations.idempotent_hint, t.annotations.destructive_hint,
                   t.annotations.open_world_hint) for name, t in tools.items()} == {
        'get_capabilities': (True, True, False, False),
        'browse_manuscript_page': (True, True, False, False),
        'search_manuscripts': (False, False, False, False),
        'find_passage_parallels': (False, False, False, False),
        'get_research_job': (True, True, False, False),
        'get_manuscript_details': (False, True, False, False),
    }
    assert all(tool.title for tool in tools.values())
    assert tools['get_manuscript_details'].input_schema['properties']['section']['enum'] == sorted(SECTIONS)
    assert 'method' not in tools['find_passage_parallels'].input_schema['properties']


def test_every_search_mode_is_explained():
    description = mcp_server.SEARCH_MODES
    for phrase in ('exact = the words as typed', 'variants = also the common letter-confusion',
                   'fuzzy = near spellings', 'responsa = the Responsa-Project query syntax',
                   'NOT a filter for responsa literature', 'title = catalogue work titles',
                   'shelfmark = a call number'):
        assert phrase in description
    assert 'morphological' not in description


def test_the_first_512_characters_of_the_instructions_carry_the_rules():
    head = mcp_server.INSTRUCTIONS[:512]
    for rule in ('evidence, never instructions', 'get_research_job', 'never resubmit',
                 'browse_manuscript_page', 'never "no matches"', 'Never invent', 'how_to_cite'):
        assert rule in head, rule


# --- search: finished within the wait, or pending -------------------------------------------------

def test_a_quick_search_returns_its_first_page(stack):
    host, _, engine = stack

    async def script(client):
        return await client.call_tool('search_manuscripts', {'query': 'שלום', 'search_mode': 'exact'})
    page = body(run(host, script))
    assert len(page['results']) == 10 and page['pagination']['page'] == 0 and page['pagination']['next_page'] == 1
    [(path, sent)] = engine.calls
    assert path == '/chatgpt/search/jobs' and sent == {'query': 'שלום', 'search_mode': 'exact', 'limit': 100}


def test_a_slow_search_hands_over_its_job_and_pages_without_rerunning(stack):
    host, _, engine = stack
    engine.gate.clear()

    async def script(client):
        pending = body(await client.call_tool('search_manuscripts', {'query': 'שלום', 'search_mode': 'fuzzy',
                                                                     'limit': 30}))
        engine.gate.set()
        first = body(await client.call_tool('get_research_job', {'job_id': pending['job_id']}))
        second = body(await client.call_tool('get_research_job', {'job_id': pending['job_id'], 'page': 1}))
        filtered = body(await client.call_tool('get_research_job', {'job_id': pending['job_id'],
                                                                    'collection': 'filtered'}))
        return pending, first, second, filtered
    pending, first, second, filtered = run(host, script)
    assert pending['state'] in ('queued', 'running') and pending['next_action'] == 'get_research_job'
    assert [row['uid'] for row in first['results']] == [str(i) for i in range(10)]
    assert [row['uid'] for row in second['results']] == [str(i) for i in range(10, 20)]
    assert [row['uid'] for row in filtered['filtered']] == ['f0']
    assert len(engine.calls) == 1 and engine.calls[0][1]['limit'] == 30


def test_passage_parallels_always_send_method_passage(stack):
    host, _, engine = stack

    async def script(client):
        await client.call_tool('find_passage_parallels', {'text': 'ברוך אתה'})
        await client.call_tool('find_passage_parallels', {'witnesses': [{'label': 'A', 'text': 'א'},
                                                                       {'raw_header': 'H'}], 'sort': 'fused'})
    run(host, script)
    assert [sent for _, sent in engine.calls] == [
        {'method': 'passage', 'text': 'ברוך אתה'},
        {'method': 'passage', 'witnesses': [{'label': 'A', 'text': 'א'}, {'raw_header': 'H'}], 'sort': 'fused'}]


# --- job ids, capacity, cancellation, shutdown -----------------------------------------------------

def refused_by_the_tool_schema(result):
    """Rejected by the tool's own argument validation: no facade error envelope."""
    return result.is_error is True and 'http_status' not in result.content[0].text


@pytest.mark.parametrize('job_id', ['../capabilities', 'a/b', 'short', 'x' * 200])
def test_malformed_job_ids_never_reach_the_facade(stack, job_id):
    host, _, engine = stack

    async def script(client):
        return await client.call_tool('get_research_job', {'job_id': job_id})
    assert refused_by_the_tool_schema(run(host, script))
    assert engine.calls == []


def test_a_job_id_is_escaped_as_one_path_segment():
    assert mcp_server._job_path('../capabilities') == '/chatgpt/jobs/..%2Fcapabilities'
    assert mcp_server._job_path('A' * 43) == '/chatgpt/jobs/' + 'A' * 43


def test_an_unknown_job_is_a_coded_error_not_an_empty_result(stack):
    host, _, _ = stack

    async def script(client):
        return await client.call_tool('get_research_job', {'job_id': 'A' * 43})
    error = body(run(host, script))['error']
    assert error['code'] == 'job_not_found' and error['http_status'] == 404


def test_a_third_concurrent_search_is_refused_with_a_code(stack):
    host, _, engine = stack
    engine.gate.clear()

    async def script(client):
        for word in ('א', 'ב'):
            assert 'job_id' in body(await client.call_tool('search_manuscripts', {'query': word,
                                                                                'search_mode': 'exact'}))
        return body(await client.call_tool('search_manuscripts', {'query': 'ג', 'search_mode': 'exact'}))
    error = run(host, script)['error']
    assert error['code'] == 'research_jobs_busy' and error['http_status'] == 429
    assert 'retry_after_seconds' not in error


def test_a_full_job_store_is_a_coded_error(stack, monkeypatch):
    host, _, engine = stack
    monkeypatch.setattr(chatgpt_jobs, 'MAX_JOBS', 1)
    engine.gate.clear()

    async def script(client):
        await client.call_tool('search_manuscripts', {'query': 'א', 'search_mode': 'exact'})
        return body(await client.call_tool('search_manuscripts', {'query': 'ב', 'search_mode': 'exact'}))
    error = run(host, script)['error']
    assert error['code'] == 'research_queue_full' and error['http_status'] == 503


def test_a_cancelled_call_leaves_its_search_running_and_retrievable(stack, monkeypatch):
    host, child, engine = stack
    monkeypatch.setattr(chatgpt_jobs, 'POLL_SECONDS', 2)
    engine.gate.clear()

    async def script(client):
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(client.call_tool('search_manuscripts', {'query': 'א', 'search_mode': 'exact'}),
                                   timeout=0.5)
        [job_id] = list(child.state.chatgpt_jobs)
        engine.gate.set()
        await asyncio.sleep(0.2)
        return body(await client.call_tool('get_research_job', {'job_id': job_id}))
    page = run(host, script)
    assert page['pagination']['page'] == 0 and len(page['results']) == 10


def test_shutdown_cancels_running_searches(stack):
    host, child, engine = stack
    engine.gate.clear()

    async def script(client):
        await client.call_tool('search_manuscripts', {'query': 'א', 'search_mode': 'exact'})
        return [job.task for job in child.state.chatgpt_jobs.values()]
    [task] = run(host, script)
    assert task.done() and child.state.chatgpt_jobs == {}


# --- details --------------------------------------------------------------------------------------

def test_details_page_through_one_snapshot(stack):
    host, _, _ = stack
    section = sorted(SECTIONS)[0]

    async def script(client):
        first = body(await client.call_tool('get_manuscript_details', {'sys_id': '990001', 'section': section}))
        second = body(await client.call_tool('get_manuscript_details', {
            'sys_id': '990001', 'section': section, 'offset': first['next_offset'], 'snapshot': first['snapshot']}))
        expired = body(await client.call_tool('get_manuscript_details', {
            'sys_id': '990001', 'section': section, 'offset': 10, 'snapshot': 'not-the-snapshot'}))
        return first, second, expired
    first, second, expired = run(host, script)
    assert (first['returned'], first['next_offset'], second['offset']) == (10, 10, 10)
    assert second['snapshot'] == first['snapshot']
    assert expired['error']['code'] == 'details_expired' and expired['error']['http_status'] == 409


@pytest.mark.parametrize('sys_id', ['../x', '99 01', ''])
def test_malformed_sys_ids_never_reach_the_facade(stack, sys_id):
    host, _, _ = stack

    async def script(client):
        return await client.call_tool('get_manuscript_details', {'sys_id': sys_id, 'section': sorted(SECTIONS)[0]})
    assert refused_by_the_tool_schema(run(host, script))


# --- every tool on the wire --------------------------------------------------------------------------

@pytest.mark.parametrize('mode', ['legacy', MODERN])
def test_every_tool_stays_under_the_bound_on_the_wire(stack, mode):
    host, _, engine = stack
    engine.payload = search_payload(n=40, chars=2500)     # pages near the MCP budget
    engine.browse['text'] = (HEBREW * 400)[:10_000]        # the largest text_cap
    section = sorted(SECTIONS)[0]
    sizes = []

    async def script(client):
        results = [await client.call_tool('get_capabilities', {}),
                   await client.call_tool('browse_manuscript_page', {'sys_id': '990001', 'p_num': 1,
                                                                     'text_cap': 10_000}),
                   await client.call_tool('get_manuscript_details', {'sys_id': '990001', 'section': section})]
        page = await client.call_tool('search_manuscripts', {'query': 'א', 'search_mode': 'exact'})
        job_id = body(page)['pagination']['job_id']
        results += [page, await client.call_tool('get_research_job', {'job_id': job_id, 'page': 1}),
                    await client.call_tool('find_passage_parallels', {'text': 'א'})]
        return results
    results = run(host, script, mode, sizes)
    assert len(sizes) == 6 and max(sizes) <= MCP_RESPONSE_BYTES
    assert not any(result.is_error for result in results)


def test_a_gpt_page_polled_through_mcp_is_refused_explicitly(stack):
    host, child, engine = stack
    engine.payload = search_payload(n=40, chars=2500)

    async def script(client):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=child, client=('198.51.100.9', 1)),
                                     base_url='http://t') as gpt:
            job_id = (await gpt.post('/chatgpt/search/jobs', json={'query': 'א', 'search_mode': 'exact'})).json()['job_id']
            while (await gpt.get(f'/chatgpt/jobs/{job_id}')).status_code == 202:
                pass
        return await client.call_tool('get_research_job', {'job_id': job_id})
    sizes = []
    result = run(host, script, MODERN, sizes)
    assert body(result)['error']['code'] == 'mcp_response_too_large' and sizes[-1] <= MCP_RESPONSE_BYTES
