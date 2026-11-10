"""Tool contract of the MCP adapter (web/mcp_server.py), build commit 3.

docs/plans/MCP_SERVER_PLAN.md section 4 and section 8: facade failures come back as
one JSON text block with the facade's code and message, the original HTTP status and
Retry-After only when sent, flagged isError and never as an empty result; MCP traffic
is labelled 'mcp' through a path no caller can reach, and the GPT's label is kept;
every tool call records exactly one mcp_tool_call event with the negotiated protocol.
"""
from __future__ import annotations

import asyncio
import json
import queue
import time
from contextlib import asynccontextmanager

import httpx
import httpx2
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from mcp.client import Client
from mcp.client.streamable_http import streamable_http_client

import web.api_hardening as hardening
import web.mcp_server as mcp_server
from web.api_hardening import API_CHANNEL_SCOPE_KEY, api_channel
from web.chatgpt_api import _mark_chatgpt
from tests.mcp_events import CapturedEvents

LEGACY, MODERN = '2025-11-25', '2026-07-28'


@pytest.fixture
def events(monkeypatch):
    # Not a real queue: the real drain thread, if an earlier test started it, took
    # these events (tests/mcp_events.py).
    captured = CapturedEvents()
    monkeypatch.setattr(hardening, '_event_queue', captured)
    monkeypatch.setattr(hardening, '_start_drain_thread_once', lambda: None)
    monkeypatch.delenv('SEARCH_API_POSTHOG_SAMPLE_N', raising=False)
    return captured.drain


@pytest.fixture
def facade():
    """Stub facade that marks requests the way the real /api/chatgpt routes do."""
    app = FastAPI()
    app.state.chatgpt_jobs = {}
    app.state.channels = []
    app.state.reply = (200, {'search_modes': ['exact']}, {})

    @app.get('/chatgpt/capabilities')
    async def capabilities(request: Request):
        _mark_chatgpt(request)
        app.state.channels.append(api_channel(request))
        status, body, headers = app.state.reply
        if isinstance(body, Exception):
            raise body
        if isinstance(body, str):
            return PlainTextResponse(body, status_code=status, headers=headers)
        return JSONResponse(body, status_code=status, headers=headers)
    return app


@pytest.fixture
def host(facade, monkeypatch):
    monkeypatch.setattr(mcp_server, 'MCP_ENABLED', True)

    @asynccontextmanager
    async def lifespan(app):
        yield

    app = FastAPI(lifespan=lifespan)
    app.mount('/api', facade)
    assert mcp_server.register_mcp_server(app, facade)
    return app


def call(host, mode=LEGACY):
    async def go():
        async with host.router.lifespan_context(host):
            http = httpx2.AsyncClient(transport=httpx2.ASGITransport(app=host, client=('198.51.100.9', 4444)),
                                      base_url='http://127.0.0.1:8081')
            async with Client(streamable_http_client('http://127.0.0.1:8081/mcp', http_client=http),
                              mode='legacy' if mode == LEGACY else mode) as client:
                return await client.call_tool('get_capabilities', {})
    return asyncio.run(asyncio.wait_for(go(), timeout=30))


def error_of(result):
    assert result.is_error is True
    assert len(result.content) == 1 and result.content[0].type == 'text' and result.content[0].text
    return json.loads(result.content[0].text)['error']


# --- results and errors --------------------------------------------------------

@pytest.mark.parametrize('mode', [LEGACY, MODERN])
def test_success_is_the_facade_body(host, events, mode):
    result = call(host, mode)
    assert result.is_error is False
    assert json.loads(result.content[0].text) == {'search_modes': ['exact']}


@pytest.mark.parametrize('mode', [LEGACY, MODERN])
def test_rate_limit_keeps_code_message_status_and_retry_after(host, facade, events, mode):
    facade.state.reply = (429, {'error': {'code': 'rate_limited', 'message': 'Slow down'}}, {'Retry-After': '60'})
    assert error_of(call(host, mode)) == {
        'code': 'rate_limited', 'message': 'Slow down', 'http_status': 429, 'retry_after_seconds': 60}


def test_retry_after_only_when_the_facade_sent_it(host, facade, events):
    facade.state.reply = (429, {'error': {'code': 'research_jobs_busy', 'message': 'Two searches'}}, {})
    error = error_of(call(host))
    assert error == {'code': 'research_jobs_busy', 'message': 'Two searches', 'http_status': 429}


def test_extra_facade_error_fields_are_kept(host, facade, events):
    facade.state.reply = (503, {'error': {'code': 'chatgpt_storage_limit', 'message': 'Full',
                                          'upstream_body_truncated': True}}, {})
    assert error_of(call(host))['upstream_body_truncated'] is True


def test_a_non_json_failure_is_still_a_coded_error(host, facade, events):
    facade.state.reply = (502, 'Bad gateway', {})
    assert error_of(call(host)) == {'code': 'upstream_error', 'message': 'Bad gateway', 'http_status': 502}


# --- channel ------------------------------------------------------------------------

@pytest.mark.parametrize('mode', [LEGACY, MODERN])
def test_mcp_calls_are_labelled_mcp(host, facade, events, mode):
    call(host, mode)
    assert facade.state.channels == ['mcp']


def test_gpt_calls_keep_their_label_and_callers_cannot_claim_mcp(facade):
    async def go():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=facade),
                                     base_url='http://127.0.0.1') as client:
            await client.get('/chatgpt/capabilities')
            await client.get('/chatgpt/capabilities',
                             headers={API_CHANNEL_SCOPE_KEY: 'mcp', 'x-genizah-api-channel': 'mcp'})
            await client.get('/chatgpt/capabilities', params={API_CHANNEL_SCOPE_KEY: 'mcp'})
    asyncio.run(go())
    assert facade.state.channels == ['chatgpt', 'chatgpt', 'chatgpt']


class _Request:
    def __init__(self, scope):
        self.scope = scope


@pytest.mark.parametrize('scope,expected', [
    ({}, 'api'),
    ({'research_job': object()}, 'api_job'),
    ({API_CHANNEL_SCOPE_KEY: 'chatgpt'}, 'chatgpt'),
    ({API_CHANNEL_SCOPE_KEY: 'chatgpt', 'research_job': object()}, 'chatgpt_job'),
    ({API_CHANNEL_SCOPE_KEY: 'mcp'}, 'mcp'),
    ({API_CHANNEL_SCOPE_KEY: 'mcp', 'research_job': object()}, 'mcp_job'),
    ({API_CHANNEL_SCOPE_KEY: 'something-else'}, 'api'),
])
def test_api_channel_values(scope, expected):
    assert api_channel(_Request(scope)) == expected


def test_mark_chatgpt_keeps_an_existing_mcp_label():
    marked, unmarked = _Request({API_CHANNEL_SCOPE_KEY: 'mcp'}), _Request({})
    _mark_chatgpt(marked)
    _mark_chatgpt(unmarked)
    assert marked.scope[API_CHANNEL_SCOPE_KEY] == 'mcp'
    assert unmarked.scope[API_CHANNEL_SCOPE_KEY] == 'chatgpt'


# --- telemetry ------------------------------------------------------------------------

def test_a_live_drain_thread_cannot_take_the_events_under_test(host, events):
    """Regression for the full-suite flake: a drain thread that reads the installed queue,
    as production's does, must not see the events these tests count."""
    import threading
    stolen, stop = [], threading.Event()

    def drain_like_production():
        while not stop.is_set():
            try:
                stolen.append(hardening._event_queue.get(timeout=0.01))
            except queue.Empty:
                continue
    worker = threading.Thread(target=drain_like_production, daemon=True)
    worker.start()
    try:
        call(host)
        time.sleep(0.05)  # give the thread its chance
        recorded = events()
    finally:
        stop.set()
        worker.join(timeout=2)
    assert [event['properties']['tool'] for event in recorded] == ['get_capabilities'] and stolen == []


@pytest.mark.parametrize('mode,label', [(LEGACY, None), (MODERN, 'mcp')])
def test_one_event_per_successful_call(host, events, mode, label):
    call(host, mode)
    [event] = events()
    assert event['event'] == 'mcp_tool_call'
    assert event['properties'] == {
        'tool': 'get_capabilities', 'status_code': 200, 'error_code': None,
        'latency_bucket': event['properties']['latency_bucket'],
        'protocol_version': mode, 'client_label': label}
    assert event['distinct_id'] == hardening.hash_ip('198.51.100.9')


def test_one_event_per_refused_call(host, facade, events):
    facade.state.reply = (429, {'error': {'code': 'research_jobs_busy', 'message': 'Busy'}}, {})
    call(host)
    [event] = events()
    assert (event['properties']['status_code'], event['properties']['error_code']) == (429, 'research_jobs_busy')


def test_one_event_when_the_facade_raises(host, facade, events):
    facade.state.reply = (200, RuntimeError('facade crashed'), {})
    assert error_of(call(host)) == {
        'code': 'adapter_exception', 'http_status': 500,
        'message': 'The tool failed inside the server; no result is available. This is an error, not an empty result.'}
    [event] = events()
    assert (event['properties']['status_code'], event['properties']['error_code']) == (500, 'adapter_exception')


def test_events_never_carry_arguments_or_results(host, facade, events):
    facade.state.reply = (200, {'secret_result': 'בדיקה'}, {})
    call(host)
    [event] = events()
    assert 'בדיקה' not in json.dumps(event, ensure_ascii=False)
    assert set(event['properties']) == {'tool', 'status_code', 'error_code', 'latency_bucket',
                                        'protocol_version', 'client_label'}
