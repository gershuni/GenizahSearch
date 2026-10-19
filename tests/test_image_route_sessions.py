"""The library image proxies set no session cookie and say how long to keep their bytes.

PR 6 (a) of docs/plans/JOINS_LAB_VS_IMPROVEMENTS_PLAN.md: /api/nli_image,
/api/nli_image_by_sysid, /api/cambridge_image, /api/manchester_image, /api/jts_image and
/api/oxford_image answer from their path and query alone, so they bypass the session
middleware (web/framework_patches.py ``_is_public_image_route``) and Cloudflare and
browsers can keep them. Image bytes are kept ``IMAGE_HTTP_MAX_AGE`` seconds; a 404, error
or redirect is ``no-store``; the synthetic sys_id 204 is unchanged. Pages, every other
/api/* route and the puzzle routes keep their session cookie.

Runs the real routes (``init_api_routes`` on a bare app) inside the two patched NiceGUI
middleware classes, as tests/test_mcp_sessions.py does for /mcp. No network: the library
fetch is a fake.
"""
from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from starlette.responses import Response
from starlette.testclient import TestClient

import shared.puzzle_image_service as puzzle_image_service
import web.api as api
from web.framework_patches import (
    _CacheSafeRequestTrackingMiddleware,
    _CacheSafeSessionMiddleware,
    _is_public_cacheable_asset_path,
    _is_public_image_route,
    _is_session_free_path,
    _skips_session,
)

SYS = '990012345670205171'
FL = '1234567'
JPEG = b'\xff\xd8\xff\xe0' + b'\0' * 6000
EXISTING_COOKIE = {'Cookie': 'session=an-existing-cookie-must-not-be-read-or-reissued'}
LONG = 'public, max-age=604800'

IMAGE_ROUTES = [
    f'/api/nli_image/{FL}',
    f'/api/nli_image_by_sysid/{SYS}',
    f'/api/cambridge_image/{SYS}',
    f'/api/manchester_image/{SYS}',
    f'/api/jts_image/{SYS}',
    f'/api/oxford_image/{SYS}',
]


def test_the_image_routes_and_only_they_are_session_free():
    for path in IMAGE_ROUTES:
        assert _is_public_image_route(path), path
        assert _skips_session(path), path
        # A separate predicate: not an asset, not one of the literal session-free paths.
        assert not _is_public_cacheable_asset_path(path)
        assert not _is_session_free_path(path)
    for path in (
        '/api/puzzle_ext_image', '/api/puzzle_image', '/api/puzzle_process',
        '/api/puzzle_export', '/api/proxy_image', f'/api/oxford_image_url/{SYS}',
        f'/api/nli_image_by_sysid_evil/{SYS}', f'/api/nli_imagex/{FL}',
        '/api/nli_image', '/api/nli_image/', f'/api/jts_image/{SYS}/',
        f'/api/jts_image/{SYS}/extra', f'/api/api/jts_image/{SYS}',
        f'/x/api/jts_image/{SYS}', f'/API/jts_image/{SYS}', f'/api/jts_image/{SYS}\n/x',
        f'/api/nli_fl_ids/{SYS}', '/api/search', '/', '/browse', '/robots.txt',
    ):
        assert not _is_public_image_route(path), path
        assert not _skips_session(path), path


class _Upstream:
    def __init__(self, status=200, content=JPEG, content_type='image/jpeg'):
        self.status_code = status
        self.content = content
        self.headers = {'Content-Type': content_type}


class _MetaMgr:
    codico_mgr = None

    def __init__(self, images_ext):
        self.nli_cache = {SYS: {'images_ext': images_ext}}

    def enrich_metadata(self, sys_id):
        return None


@pytest.fixture
def stack(monkeypatch, tmp_path):
    """The real API routes on a bare app, wrapped like NiceGUI wraps the site."""
    monkeypatch.setattr(api, '_save_nli_persistent_cache', lambda *a, **kw: None)
    monkeypatch.setattr(api.state, 'meta_mgr', _MetaMgr([{'url': 'https://iiif.example/c1'}]))
    upstream = {'response': _Upstream()}
    monkeypatch.setattr(puzzle_image_service, 'get_with_checked_redirects',
                        lambda *a, **kw: upstream['response'])
    host = FastAPI()
    api.init_api_routes(app_override=host)

    # The FL-id lookup is a closure over its own cache; seed it so no manifest is fetched.
    fetch = api._api_test_seam['fetch_fl_ids_from_nli']
    cells = dict(zip(fetch.__code__.co_freevars, fetch.__closure__))
    import time
    cells['_nli_cache'].cell_contents[SYS] = [FL]
    cells['_nli_cache_time'].cell_contents[SYS] = time.time()

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
    yield TestClient(app, base_url='http://127.0.0.1:8081'), seen, upstream
    cells['_nli_cache'].cell_contents.pop(SYS, None)
    cells['_nli_cache_time'].cell_contents.pop(SYS, None)


@pytest.mark.parametrize('cookie', [{}, EXISTING_COOKIE])
@pytest.mark.parametrize('path', [p for p in IMAGE_ROUTES if 'oxford' not in p])
def test_image_bytes_carry_no_cookie_and_a_long_lifetime(stack, path, cookie):
    client, seen, _ = stack
    response = client.get(path, params={'width': 300}, headers=cookie)
    assert response.status_code == 200, response.text
    assert response.content == JPEG
    assert 'set-cookie' not in response.headers
    assert response.headers['cache-control'] == LONG
    assert seen['tracking_ran'] == []
    assert seen['session_in_scope'] == [(path, False)]


@pytest.mark.parametrize('path', [p for p in IMAGE_ROUTES if 'oxford' not in p])
def test_a_failed_fetch_is_not_kept(stack, path):
    client, _, upstream = stack
    upstream['response'] = _Upstream(status=503, content=b'down', content_type='text/plain')
    response = client.get(path, params={'width': 300, 'page': 5})
    assert response.status_code >= 400
    assert 'set-cookie' not in response.headers
    assert response.headers['cache-control'] == 'no-store'


def test_the_rosetta_no_image_placeholder_is_a_404_that_is_not_kept(stack):
    client, _, upstream = stack
    upstream['response'] = _Upstream(content=b'\x89PNG' + b'\0' * 1600, content_type='image/png')
    response = client.get(f'/api/nli_image/{FL}')
    assert response.status_code == 404
    assert response.headers['cache-control'] == 'no-store'


def test_no_images_known_is_a_404_that_is_not_kept(stack, monkeypatch):
    client, _, _ = stack
    monkeypatch.setattr(api.state, 'meta_mgr', _MetaMgr([]))
    response = client.get(f'/api/manchester_image/{SYS}')
    assert response.status_code == 404
    assert 'set-cookie' not in response.headers
    assert response.headers['cache-control'] == 'no-store'


def test_the_synthetic_sys_id_204_is_unchanged(stack):
    client, _, _ = stack
    response = client.get('/api/nli_image_by_sysid/990001234560000000')
    assert response.status_code == 204
    assert 'set-cookie' not in response.headers
    assert 'cache-control' not in response.headers


def test_the_oxford_redirect_to_the_library_is_not_kept(monkeypatch):
    # The Oxford fallback hands the browser the library's own URL when the server
    # fetch fails; a passing failure must not be kept as the answer.
    monkeypatch.setattr(api, 'IMAGE_HTTP_MAX_AGE', 604800)
    route = api._image_cache_headers(lambda: RedirectResponse('https://example.org/i.jpg', 307))
    assert route().headers['cache-control'] == 'no-store'
    image = api._image_cache_headers(lambda: Response(JPEG, media_type='image/jpeg'))
    assert image().headers['cache-control'] == LONG
    monkeypatch.setattr(api, 'IMAGE_HTTP_MAX_AGE', 0)
    assert image().headers['cache-control'] == 'no-store'


@pytest.mark.parametrize('path', [
    '/robots.txt',
    f'/api/oxford_image_url/{SYS}',
    '/api/puzzle_ext_image',
    f'/api/nli_image_by_sysid_evil/{SYS}',
])
def test_pages_other_apis_and_the_puzzle_routes_keep_their_sessions(stack, path):
    client, seen, _ = stack
    response = client.get(path)
    assert response.headers['set-cookie'].startswith('session=')
    assert seen['tracking_ran'] == [path]
    assert seen['session_in_scope'] == [(path, True)]


def test_the_default_lifetime_is_a_week():
    assert api.IMAGE_HTTP_MAX_AGE == 604800
