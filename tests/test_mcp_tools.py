"""The six MCP tools over the REAL /api/chatgpt facade (build commit 6).

docs/plans/MCP_SERVER_PLAN.md sections 4, 5 and 8. Only the search engine under the
facade is a stub; job admission, polling, pagination, details snapshots, capacity
limits and shutdown are the production code paths the GPT uses.
"""
from __future__ import annotations

import asyncio
import json
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
from tests.mcp_events import CapturedEvents

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
        self.failure = None  # (status, body, headers): the engine's own failure
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
        if self.failure:
            status, failure, headers = self.failure
            return JSONResponse(failure, status_code=status, headers=headers)
        return JSONResponse(self.payload)


@pytest.fixture
def stack(monkeypatch):
    # Not a real queue: the real drain thread, if an earlier test started it, took
    # the events drain_events() reads (tests/mcp_events.py).
    monkeypatch.setattr(hardening, '_event_queue', CapturedEvents())
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


def client_for(host, mode='legacy', sizes=None, ip='198.51.100.9'):
    async def hook(response):
        if sizes is not None and b'"tools/call"' in (response.request.content or b''):
            await response.aread()
            sizes.append(len(response.content))
    http = httpx2.AsyncClient(transport=httpx2.ASGITransport(app=host, client=(ip, 4444)),
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

def test_seven_tools_with_the_planned_annotations(stack):
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
        'find_chunk_parallels': (False, False, False, False),
        'get_research_job': (True, True, False, False),
        'get_manuscript_details': (False, True, False, False),
    }
    assert all(tool.title for tool in tools.values())
    assert tools['get_manuscript_details'].input_schema['properties']['section']['enum'] == sorted(SECTIONS)
    assert 'method' not in tools['find_passage_parallels'].input_schema['properties']
    chunk = tools['find_chunk_parallels'].input_schema
    assert set(chunk['properties']) == {'text', 'chunk_size', 'mode', 'filters'}
    assert chunk['required'] == ['text']
    assert chunk['properties']['mode']['enum'] == ['exact', 'variants', 'fuzzy']
    assert (chunk['properties']['chunk_size']['minimum'], chunk['properties']['chunk_size']['maximum']) == (2, 20)
    search_modes = set(tools['search_manuscripts'].input_schema['properties']['search_mode']['enum'])
    assert 'variants_extended' in search_modes and 'variants_maximum' not in search_modes


def test_every_search_mode_is_explained():
    description = mcp_server.SEARCH_MODES
    for phrase in ('exact = the words as typed', 'variants = also the common letter-confusion',
                   'fuzzy = near spellings', 'only the word as typed for one or two letters',
                   'responsa = the Responsa-Project query syntax',
                   'NOT a filter for responsa literature', 'title = words in the catalogue titles',
                   'shelfmark = a call number', 'variants_extended = 70 pairs, up to two changes',
                   'The Maximum level is not offered'):
        assert phrase in description
    assert 'morphological' not in description
    # Chunk search: Basic only (ONE change per word); the higher levels are held and said so.
    assert 'ONE change per word' in mcp_server.CHUNK_MODES and 'two changes' not in mcp_server.CHUNK_MODES
    assert 'Extended and Maximum levels are not offered for chunk search' in mcp_server.CHUNK_MODES


def test_the_instructions_send_chunk_requests_to_the_chunk_tool():
    """The first real ChatGPT test asked for 'חיפוש מקטעים, מקטע=3, וריאנטים מורחב' and got a refusal."""
    text = mcp_server.INSTRUCTIONS
    assert 'חיפוש מקטעים' in text and 'means find_chunk_parallels' in text
    assert 'then variants_extended' in text and 'the Maximum level is not offered' in text
    assert 'Extended or Maximum level there, say it is not available' in text
    assert 'research_jobs_busy' in text and 'snippet_omitted' in text and 'results_cut_off' in text


def test_the_smoke_script_expects_exactly_the_served_tools():
    import importlib.util
    from pathlib import Path
    path = Path(__file__).resolve().parents[1] / 'integrations' / 'mcp' / 'smoke_test.py'
    spec = importlib.util.spec_from_file_location('mcp_smoke_tools', path)
    smoke = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(smoke)
    served = {tool.name for tool in mcp_server.build_server(FastAPI())._tool_manager.list_tools()}
    assert smoke.EXPECTED_TOOLS == served


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


def test_chunk_parallels_always_send_method_chunk_with_their_options(stack):
    host, _, engine = stack

    async def script(client):
        await client.call_tool('find_chunk_parallels', {'text': 'ברוך אתה ה'})
        await client.call_tool('find_chunk_parallels', {'text': 'ברוך אתה ה', 'chunk_size': 3,
                                                        'mode': 'variants',
                                                        'filters': {'library': ['CUL']}})
    run(host, script)
    assert all(path.endswith('/chatgpt/parallels/jobs') for path, _ in engine.calls)
    assert [sent for _, sent in engine.calls] == [
        {'method': 'chunk', 'text': 'ברוך אתה ה', 'chunk_size': 5, 'mode': 'exact'},
        {'method': 'chunk', 'text': 'ברוך אתה ה', 'chunk_size': 3, 'mode': 'variants',
         'filters': {'library': ['CUL']}}]


@pytest.mark.parametrize('arguments', [
    {'text': 'א ב', 'chunk_size': 1}, {'text': 'א ב', 'chunk_size': 21}, {'text': 'א ב', 'chunk_size': True},
    {'text': 'א ב', 'mode': 'variants_ultra'}, {'text': 'א ב', 'mode': 'variants_extended'},
    {'text': 'א ב', 'mode': 'variants_maximum'}, {'text': 'א' * 5001}, {'text': ''}, {}])
def test_chunk_arguments_are_checked_before_anything_runs(stack, arguments):
    host, _, engine = stack

    async def script(client):
        return await client.call_tool('find_chunk_parallels', arguments)
    assert refused_by_the_tool_schema(run(host, script))
    assert engine.calls == []


@pytest.mark.parametrize('extra', [{'witnesses': [{'text': 'א'}]}, {'sort': 'fused'}, {'method': 'passage'}])
def test_passage_options_are_not_chunk_arguments(stack, extra):
    host, _, engine = stack
    response = raw_tool_call(host, 'find_chunk_parallels', {'text': 'א ב', **extra})
    assert response.status_code == 400 and 'unknown_argument' in response.text
    assert engine.calls == []


async def finish(client, job_id):
    """Poll get_research_job until the job is no longer pending; returns its last body."""
    while True:
        result = body(await client.call_tool('get_research_job', {'job_id': job_id}))
        if result.get('state') not in ('queued', 'running'):
            return result
        await asyncio.sleep(0.01)


def test_only_one_chunk_search_runs_at_a_time(stack):
    """Chunk jobs share the research workers with the website and bypass the API's
    heavy budget: a second one is refused with a retry delay, other searches are not,
    and the next chunk search is admitted once the first has finished."""
    host, _, engine = stack
    engine.gate.clear()

    async def script(client):
        first = body(await client.call_tool('find_chunk_parallels', {'text': 'א ב ג'}))
        second = body(await client.call_tool('find_chunk_parallels', {'text': 'ד ה ו'}))
        passage = body(await client.call_tool('find_passage_parallels', {'text': 'א'}))
        engine.gate.set()
        await finish(client, first['job_id'])
        await finish(client, passage['job_id'])
        third = body(await client.call_tool('find_chunk_parallels', {'text': 'ז ח ט'}))
        return first, second, passage, third
    first, second, passage, third = run(host, script)
    assert first['state'] in ('queued', 'running') and 'job_id' in first
    assert second['error']['code'] == 'research_jobs_busy' and second['error']['http_status'] == 429
    assert second['error']['retry_after_seconds'] == chatgpt_jobs.CHUNK_BUSY_RETRY_SECONDS
    assert 'job_id' in passage
    assert 'error' not in third
    assert [sent['text'] for _, sent in engine.calls] == ['א ב ג', 'א', 'ז ח ט']


def test_one_overlong_chunk_row_keeps_the_job_and_says_its_text_was_omitted(stack):
    """A chunk parallel's snippet joins every matched fragment and has no length limit.
    One such row no longer fails the job: it loses its snippet and excerpt (not its
    locator or matches) and says so; every other row arrives whole."""
    host, _, engine = stack
    payload = search_payload(n=3, chars=300)
    payload['source'] = 'parallels'
    payload['results'][1].update(snippet=HEBREW * 6000, excerpt=HEBREW * 10,
                                 matches=[{'chunk_index': 0, 'source_chunk_text': 'א ב', 'manuscript_snippet': 'א ב'}])
    engine.payload = payload

    async def script(client):
        first = body(await client.call_tool('find_chunk_parallels', {'text': 'א ב ג', 'chunk_size': 2}))
        job_id = first['pagination']['job_id']
        pages = [first]
        while pages[-1]['pagination']['next_page'] is not None:
            pages.append(body(await client.call_tool('get_research_job', {
                'job_id': job_id, 'page': pages[-1]['pagination']['next_page']})))
        return pages
    pages = run(host, script, MODERN)
    rows = [row for page in pages for row in page['results']]
    assert [row['uid'] for row in rows] == ['0', '1', '2']
    long_row = rows[1]
    assert long_row['snippet_omitted'] is True
    assert 'snippet' not in long_row and 'excerpt' not in long_row
    assert long_row['locator'] == payload['results'][1]['locator']
    assert long_row['matches'] == payload['results'][1]['matches']  # every span, each whole
    assert all(row['snippet'] == payload['results'][0]['snippet'] and 'snippet_omitted' not in row
               for row in (rows[0], rows[2]))


def test_a_second_caller_waits_for_the_running_chunk_search(stack):
    """The guard counts every caller's chunk jobs, not only one caller's (Codex build review)."""
    host, _, engine = stack
    engine.gate.clear()

    async def go():
        async with host.router.lifespan_context(host):
            async with client_for(host) as first, client_for(host, ip='203.0.113.7') as second:
                running = body(await first.call_tool('find_chunk_parallels', {'text': 'א ב ג'}))
                refused = body(await second.call_tool('find_chunk_parallels', {'text': 'ד ה ו'}))
                searching = body(await second.call_tool('search_manuscripts', {'query': 'א',
                                                                               'search_mode': 'exact'}))
                engine.gate.set()
                return running, refused, searching
    running, refused, searching = asyncio.run(asyncio.wait_for(go(), timeout=60))
    assert 'job_id' in running
    assert refused['error']['code'] == 'research_jobs_busy'
    assert refused['error']['retry_after_seconds'] == chatgpt_jobs.CHUNK_BUSY_RETRY_SECONDS
    assert 'error' not in searching


def test_a_failed_chunk_search_frees_the_next_one(stack):
    host, _, engine = stack
    engine.failure = (500, {'error': {'code': 'internal_error', 'message': 'boom'}}, {})

    async def script(client):
        failed = body(await client.call_tool('find_chunk_parallels', {'text': 'א ב ג'}))
        engine.failure = None
        return failed, body(await client.call_tool('find_chunk_parallels', {'text': 'ד ה ו'}))
    failed, second = run(host, script)
    assert failed['error']['code'] == 'internal_error'
    assert 'error' not in second


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


def test_cancelling_the_in_tool_wait_leaves_the_search_running_and_retrievable(stack, monkeypatch):
    # Codex (build 5-6 review): the old version never cancelled the server side. This
    # cancels the tool's own wait mid-poll and checks the job outlives it.
    _, child, engine = stack
    monkeypatch.setattr(chatgpt_jobs, 'POLL_SECONDS', 2)
    engine.gate.clear()
    target = mcp_server._with_mcp_channel(child)

    async def go():
        wait = asyncio.create_task(mcp_server._start_and_wait(
            target, '198.51.100.9', '/chatgpt/search/jobs', {'query': 'א', 'search_mode': 'exact'}))
        while not child.state.chatgpt_jobs:
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.2)  # inside a facade poll now
        wait.cancel()
        with pytest.raises(asyncio.CancelledError):
            await wait
        [(job_id, job)] = list(child.state.chatgpt_jobs.items())
        assert job.finished is None and not job.task.done()
        engine.gate.set()
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=target, client=('198.51.100.9', 1)),
                                     base_url='http://t') as client:
            for _ in range(200):
                page = await client.get(f'/chatgpt/jobs/{job_id}')
                if page.status_code == 200:
                    return page.json()
        raise AssertionError('the job did not finish')
    page = asyncio.run(asyncio.wait_for(go(), timeout=30))
    assert page['pagination']['page'] == 0 and len(page['results']) == 10


def test_a_cancelled_tool_call_records_one_cancelled_event(stack):
    from types import SimpleNamespace
    ctx = SimpleNamespace(request_context=SimpleNamespace(request=None, session=None),
                          protocol_version='2025-11-25', request_id='1')

    async def go():
        async def never(client_ip):
            await asyncio.Event().wait()
        task = asyncio.create_task(mcp_server._run_tool(ctx, 'search_manuscripts', never))
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    asyncio.run(go())
    [event] = drain_events()
    assert (event['properties']['status_code'], event['properties']['error_code']) == (499, 'cancelled')


def drain_events():
    return hardening._event_queue.drain()


def test_a_search_waits_exactly_three_polls_before_handing_over(stack, monkeypatch):
    host, _, engine = stack
    engine.gate.clear()
    calls, original = [], mcp_server._facade

    async def recording(target, client_ip, method, path, **kwargs):
        calls.append((method, path.split('/jobs/')[0] + ('/jobs/<id>' if '/jobs/' in path else '')))
        return await original(target, client_ip, method, path, **kwargs)
    monkeypatch.setattr(mcp_server, '_facade', recording)

    async def script(client):
        return body(await client.call_tool('search_manuscripts', {'query': 'א', 'search_mode': 'exact'}))
    assert 'job_id' in run(host, script)
    assert calls == [('POST', '/chatgpt/search/jobs')] + [('GET', '/chatgpt/jobs/<id>')] * 3


def test_a_failed_search_is_an_error_with_its_job_id_not_pending(stack):
    host, _, engine = stack
    engine.failure = (429, {'error': {'code': 'rate_limited', 'message': 'Too many requests'}}, {'Retry-After': '60'})

    async def script(client):
        return body(await client.call_tool('search_manuscripts', {'query': 'א', 'search_mode': 'exact'}))
    error = run(host, script)['error']
    assert (error['code'], error['http_status'], error['retry_after_seconds']) == ('rate_limited', 429, 60)
    assert len(error['job_id']) >= 16


def test_every_argument_reaches_the_facade_unchanged(stack):
    host, _, engine = stack
    responsa = {'variants': True, 'ja': False, 'flex_spacing': True, 'bidirectional': True}
    filters = {'library': ['CUL'], 'library_filter_mode': 'exclude', 'materials': ['paper'],
               'domains': ['d'], 'authors': ['a'], 'works': ['w'], 'date_from': 1000, 'date_to': 1200}

    async def script(client):
        await client.call_tool('search_manuscripts', {'query': 'א', 'search_mode': 'responsa', 'limit': 7,
                                                      'gap': 2, 'responsa_options': responsa, 'filters': filters})
        await client.call_tool('find_passage_parallels', {'text': 'א', 'filters': {'library': ['JTS']}})
    run(host, script)
    assert [sent for _, sent in engine.calls] == [
        {'query': 'א', 'search_mode': 'responsa', 'limit': 7, 'gap': 2, 'responsa_options': responsa,
         'filters': filters},
        {'method': 'passage', 'text': 'א', 'filters': {'library': ['JTS']}}]


def raw_tool_call(host, name, arguments, protocol='2025-11-25'):
    params = {'name': name, 'arguments': arguments}
    if protocol == MODERN:  # 2026-07-28 carries version and capabilities on every request
        params['_meta'] = {'io.modelcontextprotocol/protocolVersion': MODERN,
                           'io.modelcontextprotocol/clientCapabilities': {}}

    async def go():
        async with host.router.lifespan_context(host):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=host),
                                         base_url='http://127.0.0.1:8081') as client:
                return await client.post('/mcp', json={
                    'jsonrpc': '2.0', 'id': 1, 'method': 'tools/call', 'params': params}, headers={
                    'accept': 'application/json, text/event-stream', 'mcp-protocol-version': protocol,
                    'mcp-method': 'tools/call', 'mcp-name': name})
    return asyncio.run(asyncio.wait_for(go(), timeout=30))


def test_an_undeclared_argument_is_refused_not_dropped(stack):
    host, _, engine = stack
    # 'filter' for 'filters': the SDK would drop it and run an unfiltered search.
    response = raw_tool_call(host, 'search_manuscripts',
                             {'query': 'א', 'search_mode': 'exact', 'filter': {'library': ['CUL']}})
    assert response.status_code == 400 and 'unknown_argument' in response.text
    assert engine.calls == []


@pytest.mark.parametrize('arguments', [
    {'query': 'א', 'search_mode': 'exact', 'limit': True},
    {'query': 'א', 'search_mode': 'exact', 'limit': '5'},
    {'query': 'א', 'search_mode': 'exact', 'filters': {'date_from': '1000'}},
])
def test_numbers_must_be_numbers(stack, arguments):
    host, _, engine = stack

    async def script(client):
        return await client.call_tool('search_manuscripts', arguments)
    assert refused_by_the_tool_schema(run(host, script))
    assert engine.calls == []


@pytest.mark.parametrize('protocol', ['2025-11-25', MODERN])
def test_a_huge_validation_error_is_refused_not_sent(stack, protocol):
    # Codex: 1,000 unknown filter fields made a 188 KB validation reply.
    host, _, engine = stack
    junk = {f'field_{i:04d}': 'x' * 40 for i in range(1000)}
    response = raw_tool_call(host, 'search_manuscripts', {'query': 'א', 'search_mode': 'exact', 'filters': junk},
                             protocol)
    assert len(response.content) <= MCP_RESPONSE_BYTES
    assert 'mcp_response_too_large' in response.text and response.json()['id'] == 1
    assert engine.calls == []


def test_the_guard_knows_exactly_the_advertised_arguments(stack):
    host, _, _ = stack
    [guard] = [route.app for route in host.router.routes if getattr(route, 'path', '') == '/mcp']

    async def script(client):
        return (await client.list_tools()).tools
    advertised = {tool.name: frozenset(tool.input_schema.get('properties', {})) for tool in run(host, script)}
    assert guard.declared == advertised


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
                    await client.call_tool('find_passage_parallels', {'text': 'א'}),
                    await client.call_tool('find_chunk_parallels', {'text': 'א ב ג', 'chunk_size': 2})]
        return results
    results = run(host, script, mode, sizes)
    assert len(sizes) == 7 and max(sizes) <= MCP_RESPONSE_BYTES
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
