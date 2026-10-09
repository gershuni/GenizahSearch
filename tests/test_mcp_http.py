"""HTTP-level gates for the public MCP server (web/mcp_server.py).

docs/plans/MCP_SERVER_PLAN.md section 8 and build commit 1: routing, Host/Origin
checks before the slash redirect, method refusals, subscriptions, lifespan,
both protocol generations, the caller IP the facade sees, and per-app isolation.

The host app here is a plain FastAPI app with a stub facade sub-app, not the
NiceGUI app; the NiceGUI middleware gates (sessions) live in their own tests.
"""
from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager

import httpx
import httpx2
import pytest
from fastapi import FastAPI, Request
from mcp.client import Client
from mcp.client.streamable_http import streamable_http_client
from mcp.shared.exceptions import MCPError

import web.mcp_server as mcp_server

LEGACY = '2025-11-25'
MODERN = '2026-07-28'
MCP_HEADERS = {'accept': 'application/json, text/event-stream',
               'content-type': 'application/json',
               'mcp-protocol-version': LEGACY}
INITIALIZE = json.dumps({'jsonrpc': '2.0', 'id': 1, 'method': 'initialize', 'params': {
    'protocolVersion': LEGACY, 'capabilities': {}, 'clientInfo': {'name': 'test', 'version': '0'}}})


def make_facade(marker='one'):
    facade = FastAPI()
    facade.state.chatgpt_jobs = {}
    facade.state.seen = []

    @facade.get('/chatgpt/capabilities')
    async def capabilities(request: Request):
        facade.state.seen.append(request.client.host if request.client else None)
        return {'marker': marker, 'note': 'בדיקה "quoted"'}

    return facade


def make_host(facade, monkeypatch, enabled=True):
    monkeypatch.setattr(mcp_server, 'MCP_ENABLED', enabled)

    @asynccontextmanager
    async def lifespan(app):
        yield

    host = FastAPI(lifespan=lifespan)
    host.mount('/api', facade)
    added = mcp_server.register_mcp_server(host, facade)

    @host.get('/after')
    async def after():
        return {'ok': True}

    return host, added


def raw_client(host, base_url='http://127.0.0.1:8081', client=('203.0.113.7', 5555)):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=host, client=client),
                             base_url=base_url, follow_redirects=False)


def sdk_client(host, mode, client=('198.51.100.9', 4444)):
    http = httpx2.AsyncClient(transport=httpx2.ASGITransport(app=host, client=client),
                              base_url='http://127.0.0.1:8081')
    return Client(streamable_http_client('http://127.0.0.1:8081/mcp', http_client=http), mode=mode)


def run(coro):
    return asyncio.run(asyncio.wait_for(coro, timeout=30))


# --- registration -----------------------------------------------------------

def test_flag_off_registers_nothing(monkeypatch):
    host, added = make_host(make_facade(), monkeypatch, enabled=False)
    assert added is False

    async def go():
        async with raw_client(host) as c:
            assert (await c.post('/mcp', headers=MCP_HEADERS, content=INITIALIZE)).status_code == 404
            assert (await c.post('/mcp/', headers=MCP_HEADERS)).status_code == 404
    run(go())


def test_missing_facade_registers_nothing(monkeypatch):
    facade = make_facade()
    del facade.state.chatgpt_jobs
    host, added = make_host(facade, monkeypatch)
    assert added is False

    async def go():
        async with raw_client(host) as c:
            assert (await c.post('/mcp', headers=MCP_HEADERS, content=INITIALIZE)).status_code == 404
    run(go())


# --- routing ------------------------------------------------------------------

def test_exact_route_redirect_and_refusals(monkeypatch):
    host, added = make_host(make_facade(), monkeypatch)
    assert added is True

    async def go():
        async with host.router.lifespan_context(host):
            async with raw_client(host) as c:
                ok = await c.post('/mcp', headers=MCP_HEADERS, content=INITIALIZE)
                assert ok.status_code == 200 and ok.headers['content-type'].startswith('application/json')
                slash = await c.post('/mcp/', headers=MCP_HEADERS, content=INITIALIZE)
                assert slash.status_code == 307
                assert slash.headers['location'] == 'http://127.0.0.1:8081/mcp'
                assert (await c.post('/mcp/mcp', headers=MCP_HEADERS, content=INITIALIZE)).status_code == 404
                assert (await c.get('/mcp')).status_code == 405
                assert (await c.delete('/mcp')).status_code == 405
                assert (await c.get('/after')).json() == {'ok': True}
    run(go())


def test_slash_redirect_checks_host_before_redirecting(monkeypatch):
    host, _ = make_host(make_facade(), monkeypatch)

    async def go():
        async with raw_client(host) as c:
            for method in ('POST', 'GET'):
                r = await c.request(method, '/mcp/', headers={'host': 'evil.invalid'})
                assert r.status_code == 421, method
                assert 'location' not in r.headers
            r = await c.post('/mcp/', headers={'origin': 'https://evil.invalid'})
            assert r.status_code == 403 and 'location' not in r.headers
    run(go())


def test_slash_redirect_keeps_https(monkeypatch):
    host, _ = make_host(make_facade(), monkeypatch)

    async def go():
        async with raw_client(host, base_url='https://genizahsearch.com') as c:
            r = await c.post('/mcp/')
            assert r.status_code == 307
            assert r.headers['location'] == 'https://genizahsearch.com/mcp'
    run(go())


@pytest.mark.parametrize('host_header', [
    'genizahsearch.com', 'genizahsearch.com:443', 'www.genizahsearch.com', 'www.genizahsearch.com:443',
    'localhost', 'localhost:8084', '127.0.0.1', '127.0.0.1:8081', '[::1]', '[::1]:8084',
])
def test_allowed_hosts(monkeypatch, host_header):
    host, _ = make_host(make_facade(), monkeypatch)

    async def go():
        async with host.router.lifespan_context(host):
            async with raw_client(host) as c:
                r = await c.post('/mcp', headers={**MCP_HEADERS, 'host': host_header}, content=INITIALIZE)
                assert r.status_code == 200, (host_header, r.status_code, r.text)
    run(go())


@pytest.mark.parametrize('host_header', [
    'evil.invalid', 'genizahsearch.com.evil.invalid', 'evilgenizahsearch.com', 'genizahsearch.com:8443',
])
def test_rejected_hosts(monkeypatch, host_header):
    host, _ = make_host(make_facade(), monkeypatch)

    async def go():
        async with host.router.lifespan_context(host):
            async with raw_client(host) as c:
                r = await c.post('/mcp', headers={**MCP_HEADERS, 'host': host_header}, content=INITIALIZE)
                assert r.status_code == 421, (host_header, r.status_code)
    run(go())


@pytest.mark.parametrize('origin,expected', [
    (None, 200), ('https://genizahsearch.com', 200), ('http://localhost:8084', 200),
    ('https://evil.invalid', 403), ('http://genizahsearch.com', 403),
])
def test_origin_check(monkeypatch, origin, expected):
    host, _ = make_host(make_facade(), monkeypatch)
    headers = dict(MCP_HEADERS)
    if origin:
        headers['origin'] = origin

    async def go():
        async with host.router.lifespan_context(host):
            async with raw_client(host) as c:
                assert (await c.post('/mcp', headers=headers, content=INITIALIZE)).status_code == expected
    run(go())


# --- lifespan -------------------------------------------------------------------

def test_requests_need_the_host_lifespan(monkeypatch):
    host, _ = make_host(make_facade(), monkeypatch)

    async def outside():
        async with raw_client(host) as c:
            await c.post('/mcp', headers=MCP_HEADERS, content=INITIALIZE)

    with pytest.raises(RuntimeError, match='run'):
        run(outside())

    async def inside():
        async with host.router.lifespan_context(host):
            async with raw_client(host) as c:
                return (await c.post('/mcp', headers=MCP_HEADERS, content=INITIALIZE)).status_code
    assert run(inside()) == 200


# --- protocol generations, caller IP, isolation ------------------------------------

@pytest.mark.parametrize('mode', ['legacy', MODERN])
def test_both_protocol_generations_reach_the_facade_as_the_caller(monkeypatch, mode):
    facade = make_facade()
    host, _ = make_host(facade, monkeypatch)

    async def go():
        async with host.router.lifespan_context(host):
            async with sdk_client(host, mode) as client:
                tools = await client.list_tools()
                result = await client.call_tool('get_capabilities', {})
                return client.protocol_version, tools, result
    version, tools, result = run(go())
    assert version == (LEGACY if mode == 'legacy' else MODERN)
    tool = {t.name: t for t in tools.tools}['get_capabilities']
    assert tool.annotations.read_only_hint is True and tool.annotations.destructive_hint is False
    assert tool.title
    assert result.is_error is False
    assert json.loads(result.content[0].text)['note'] == 'בדיקה "quoted"'
    assert facade.state.seen == ['198.51.100.9']


def test_two_registered_apps_keep_their_own_facade(monkeypatch):
    facade_one, facade_two = make_facade('one'), make_facade('two')
    host_one, _ = make_host(facade_one, monkeypatch)
    host_two, _ = make_host(facade_two, monkeypatch)

    async def marker(host):
        async with host.router.lifespan_context(host):
            async with sdk_client(host, 'legacy') as client:
                result = await client.call_tool('get_capabilities', {})
                return json.loads(result.content[0].text)['marker']
    assert run(marker(host_one)) == 'one'
    assert run(marker(host_two)) == 'two'


def test_subscriptions_are_refused(monkeypatch):
    host, _ = make_host(make_facade(), monkeypatch)

    refusals = []

    async def go():
        # The SDK client raises the refusal from inside its task group.
        try:
            async with host.router.lifespan_context(host):
                async with sdk_client(host, MODERN) as client:
                    async with asyncio.timeout(10):
                        async with client.listen(tools_list_changed=True):
                            refusals.append('listening')
        except* MCPError as group:
            refusals.extend(error.message for error in _leaves(group))
    run(go())
    assert refusals == ['Method not found']


def _leaves(group):
    for error in group.exceptions:
        yield from _leaves(error) if isinstance(error, BaseExceptionGroup) else (error,)
