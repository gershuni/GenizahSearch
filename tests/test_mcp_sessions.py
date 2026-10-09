"""The public MCP endpoint never touches browser sessions (build commit 2).

docs/plans/MCP_SERVER_PLAN.md section 8: no Set-Cookie and no NiceGUI user storage
on /mcp -- success, error or redirect, with or without an existing cookie -- while
ordinary pages keep their sessions, and /mcp is never classed as a cacheable asset.
Runs the real MCP route inside the two patched NiceGUI middleware classes.
"""
from __future__ import annotations

import json
from contextlib import asynccontextmanager

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

import web.framework_patches as patches
import web.mcp_server as mcp_server
from web.framework_patches import (
    _CacheSafeRequestTrackingMiddleware,
    _CacheSafeSessionMiddleware,
    _is_public_cacheable_asset_path,
    _is_session_free_path,
)

MCP_HEADERS = {'accept': 'application/json, text/event-stream',
               'content-type': 'application/json',
               'mcp-protocol-version': '2025-11-25'}
INITIALIZE = json.dumps({'jsonrpc': '2.0', 'id': 1, 'method': 'initialize', 'params': {
    'protocolVersion': '2025-11-25', 'capabilities': {}, 'clientInfo': {'name': 't', 'version': '0'}}})
EXISTING_COOKIE = {'Cookie': 'session=an-existing-cookie-must-not-be-read-or-reissued'}


def test_mcp_paths_are_session_free_but_never_cacheable():
    assert {mcp_server.MCP_PATH, mcp_server.MCP_PATH + '/'} == patches._SESSION_FREE_PATHS
    for path in ('/mcp', '/mcp/'):
        assert _is_session_free_path(path)
        assert not _is_public_cacheable_asset_path(path)
    for path in ('/mcp/mcp', '/mcpx', '/api/mcp', '/', '/search', '/static/common.css'):
        assert not _is_session_free_path(path)


@pytest.fixture
def stack(monkeypatch):
    """Host app with the real /mcp routes, wrapped like NiceGUI wraps the site."""
    monkeypatch.setattr(mcp_server, 'MCP_ENABLED', True)
    facade = FastAPI()
    facade.state.chatgpt_jobs = {}

    @asynccontextmanager
    async def lifespan(app):
        yield

    host = FastAPI(lifespan=lifespan)
    host.mount('/api', facade)
    assert mcp_server.register_mcp_server(host, facade)

    @host.get('/search')
    async def page():
        return {'page': True}

    seen = {'session_in_scope': [], 'tracking_ran': []}

    async def recorder(scope, receive, send):
        if scope['type'] == 'http':
            seen['session_in_scope'].append((scope['path'], 'session' in scope))
        await host(scope, receive, send)

    original_dispatch = _CacheSafeRequestTrackingMiddleware.__mro__[1].dispatch

    async def spy_dispatch(self, request, call_next):
        seen['tracking_ran'].append(request.url.path)
        return await original_dispatch(self, request, call_next)

    monkeypatch.setattr(_CacheSafeRequestTrackingMiddleware.__mro__[1], 'dispatch', spy_dispatch)
    app = _CacheSafeSessionMiddleware(_CacheSafeRequestTrackingMiddleware(recorder),
                                      secret_key='test-secret')
    return app, seen


@pytest.mark.parametrize('cookie', [{}, EXISTING_COOKIE])
def test_mcp_success_error_and_redirect_set_no_cookie_and_skip_user_storage(stack, cookie):
    app, seen = stack
    with TestClient(app, base_url='http://127.0.0.1:8081') as client:
        ok = client.post('/mcp', headers={**MCP_HEADERS, **cookie}, content=INITIALIZE)
        bad = client.post('/mcp', headers={**MCP_HEADERS, **cookie}, content=b'not json')
        refused = client.get('/mcp', headers=cookie)
        redirect = client.post('/mcp/', headers={**MCP_HEADERS, **cookie}, follow_redirects=False)
    assert ok.status_code == 200
    assert bad.status_code >= 400
    assert refused.status_code == 405
    assert redirect.status_code == 307
    for response in (ok, bad, refused, redirect):
        assert 'set-cookie' not in response.headers, response.request.url
    assert seen['tracking_ran'] == []
    assert seen['session_in_scope'] and not any(has for _, has in seen['session_in_scope'])


def test_ordinary_pages_keep_their_sessions(stack):
    app, seen = stack
    with TestClient(app, base_url='http://127.0.0.1:8081') as client:
        response = client.get('/search')
    assert response.status_code == 200
    assert response.headers['set-cookie'].startswith('session=')
    assert seen['tracking_ran'] == ['/search']
    assert seen['session_in_scope'] == [('/search', True)]
