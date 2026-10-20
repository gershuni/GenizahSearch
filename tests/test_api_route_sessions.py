"""/api/browse and /api/chatgpt/browse set no session cookie and say how long to keep a complete answer.

docs/OPEN_ISSUES.md, "Cloudflare caches no page or API response": the browse route answers
from its query string alone (D-22 statelessness), so it bypasses the session middleware
(web/framework_patches.py ``_is_public_api_read_route``) and Cloudflare and API clients can
keep it. A complete answer is kept ``API_BROWSE_HTTP_MAX_AGE`` seconds; an answer with an
enrichment timeout or failure, an error, a 429 and a SEARCH_API_MODE refusal are
``no-store``. Every other /api/* route keeps its session cookie.

Runs the real /api sub-app (``init_search_api`` + ``register_chatgpt_api``) mounted at /api
inside the two patched NiceGUI middleware classes, as tests/test_image_route_sessions.py
does for the image proxies. No corpus: the browse page and the sidecars are fakes.
"""
from __future__ import annotations

import asyncio

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

import web.search_api as search_api
from tests.test_browse_api import _make_browse_page
from web.chatgpt_api import register_chatgpt_api
from web.framework_patches import (
    _CacheSafeRequestTrackingMiddleware,
    _CacheSafeSessionMiddleware,
    _is_public_api_read_route,
    _is_public_image_route,
    _is_session_free_path,
    _skips_session,
)
from web.services import get_service

EXISTING_COOKIE = {'Cookie': 'session=an-existing-cookie-must-not-be-read-or-reissued'}
KEEP = 'public, max-age=3600'
BROWSE_ROUTES = ['/api/browse', '/api/chatgpt/browse']
QUERY = {'sys_id': '99001', 'p_num': 3}


def test_the_browse_routes_and_only_they_are_cacheable_api_reads():
    for path in BROWSE_ROUTES:
        assert _is_public_api_read_route(path), path
        assert _skips_session(path), path
        assert not _is_public_image_route(path)
        assert not _is_session_free_path(path)
    for path in (
        '/api/search', '/api/parallels', '/api/capabilities', '/api/chatgpt/capabilities',
        '/api/chatgpt/search', '/api/chatgpt/manuscript-details', '/api/chatgpt/jobs/abc',
        '/api/research/jobs/abc', '/api/browse/', '/api/browsex', '/api/browse/x',
        '/API/browse', '/x/api/browse', '/api/api/browse', '/browse', '/api/openapi.json',
        '/api/export/json', '/api/visual_suggestions/version',
    ):
        assert not _is_public_api_read_route(path), path
        assert not _skips_session(path), path


@pytest.fixture
def stack(monkeypatch):
    """The real /api sub-app mounted at /api, wrapped like NiceGUI wraps the site."""
    monkeypatch.setenv('SEARCH_API_MODE', 'open')
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '30')
    monkeypatch.setenv('SEARCH_API_POSTHOG_SAMPLE_N', '999999')
    monkeypatch.setenv('SEARCH_API_BROWSE_TIMEOUT', '1.0')
    search_api._browse_rate_limiter.reset_for_tests()
    search_api._capabilities_rate_limiter.reset_for_tests()
    svc = get_service()
    page = _make_browse_page()
    monkeypatch.setattr(svc, 'get_browse_page', lambda *a, **k: page)
    monkeypatch.setattr(svc, 'get_browse_page_by_fl', lambda *a, **k: page)
    for sidecar in ('_pgp_sync', '_fjms_sync', '_nli_sync'):
        monkeypatch.setattr(f'shared.browse_service.{sidecar}', lambda *a, **k: None)

    sub = FastAPI()
    search_api.init_search_api(app_override=sub, path_prefix='')
    register_chatgpt_api(sub)
    host = FastAPI()
    host.mount('/api', sub)

    seen = {'session_in_scope': []}

    async def recorder(scope, receive, send):
        if scope['type'] == 'http':
            seen['session_in_scope'].append((scope['path'], 'session' in scope))
        await host(scope, receive, send)

    app = _CacheSafeSessionMiddleware(_CacheSafeRequestTrackingMiddleware(recorder),
                                      secret_key='test-secret')
    yield TestClient(app, base_url='http://127.0.0.1:8081'), seen
    asyncio.run(sub.state.shutdown_chatgpt_jobs())


@pytest.mark.parametrize('cookie', [{}, EXISTING_COOKIE])
@pytest.mark.parametrize('path', BROWSE_ROUTES)
def test_a_complete_browse_answer_carries_no_cookie_and_a_lifetime(stack, path, cookie):
    client, seen = stack
    response = client.get(path, params=QUERY, headers=cookie)
    assert response.status_code == 200, response.text
    assert response.json()['locator']['sys_id'] == '99001'
    assert 'set-cookie' not in response.headers
    assert response.headers['cache-control'] == KEEP
    assert seen['session_in_scope'] == [(path, False)]


@pytest.mark.parametrize('path', BROWSE_ROUTES)
def test_an_enrichment_failure_is_not_kept(stack, monkeypatch, path):
    client, _ = stack

    def _boom(*a, **k):
        raise RuntimeError('sidecar down')

    monkeypatch.setattr('shared.browse_service._pgp_sync', _boom)
    response = client.get(path, params=QUERY)
    assert response.status_code == 200
    assert any(w['code'] == 'enrichment_failed' for w in response.json()['warnings'])
    assert response.headers['cache-control'] == 'no-store'
    assert 'set-cookie' not in response.headers


@pytest.mark.parametrize('path', BROWSE_ROUTES)
def test_a_bad_request_is_not_kept(stack, path):
    client, _ = stack
    response = client.get(path, params={'p_num': 'x'}, headers=EXISTING_COOKIE)
    assert response.status_code == 400
    assert response.headers['cache-control'] == 'no-store'
    assert 'set-cookie' not in response.headers


@pytest.mark.parametrize('path', BROWSE_ROUTES)
def test_a_rate_limited_answer_is_not_kept(stack, monkeypatch, path):
    client, _ = stack
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '1')
    assert client.get(path, params=QUERY).status_code == 200
    response = client.get(path, params=QUERY)
    assert response.status_code == 429
    assert response.headers['cache-control'] == 'no-store'


@pytest.mark.parametrize('mode,status', [('disabled', 503), ('localhost-only', 403)])
@pytest.mark.parametrize('path', BROWSE_ROUTES)
def test_a_search_api_mode_refusal_is_not_kept(stack, monkeypatch, path, mode, status):
    client, _ = stack
    monkeypatch.setenv('SEARCH_API_MODE', mode)
    monkeypatch.setattr('web.api_hardening._is_loopback_request', lambda request: False)
    response = client.get(path, params=QUERY)
    assert response.status_code == status
    assert response.headers['cache-control'] == 'no-store'


def test_a_zero_lifetime_keeps_nothing(stack, monkeypatch):
    client, _ = stack
    monkeypatch.setattr(search_api, 'API_BROWSE_HTTP_MAX_AGE', 0)
    response = client.get('/api/browse', params=QUERY)
    assert response.status_code == 200
    assert response.headers['cache-control'] == 'no-store'


@pytest.mark.parametrize('path', ['/api/capabilities', '/api/chatgpt/capabilities'])
def test_other_api_routes_keep_their_sessions(stack, path):
    client, seen = stack
    response = client.get(path)
    assert response.headers['set-cookie'].startswith('session=')
    assert seen['session_in_scope'] == [(path, True)]


def test_the_default_lifetime_is_an_hour():
    assert search_api.API_BROWSE_HTTP_MAX_AGE == 3600
