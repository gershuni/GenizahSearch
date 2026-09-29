# -*- coding: utf-8 -*-
"""Fragment Puzzle: images a browser uploads are kept for that browser.

* signed out: the upload is kept for the uploading browser only and served to
  it with ``Cache-Control: private``; another browser still gets a miss;
* signed in: the upload goes to the shared cache and the account id and UTC
  time are recorded next to the cache;
* exports use the requesting browser's own images, and nobody else's;
* an image fetched from a direct URL is filed under that URL's name, never
  under a fragment's, and only known library image hosts are fetched;
* a request without a session cookie keeps nothing;
* only NLI image ids name a fragment image upload.

Real routes (``init_api_routes``) on a bare FastAPI app with a session cookie
per TestClient; a temp cache directory; the account is set per request.
"""
from __future__ import annotations

import io
import json
import uuid

import pytest
from fastapi import FastAPI
from PIL import Image
from starlette.middleware.sessions import SessionMiddleware
from starlette.testclient import TestClient

FL = '23456789'


def _jpeg(color) -> bytes:
    buf = io.BytesIO()
    Image.new('RGB', (64, 48), color=color).save(buf, format='JPEG')
    return buf.getvalue()


class _SessionIdMiddleware:
    """Gives every browser a session id, as NiceGUI's own middleware does."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope['type'] == 'http':
            session = scope.get('session')
            if session is not None and 'id' not in session:
                session['id'] = str(uuid.uuid4())
        await self.app(scope, receive, send)


@pytest.fixture
def puzzle_app(tmp_path, monkeypatch):
    import shared.puzzle_image_service as pis
    from web.auth_state import GlobalAuthState
    pis.reset_puzzle_image_service()
    service = pis.get_puzzle_image_service(cache_dir=tmp_path / 'puzzle')
    monkeypatch.setattr(service, '_fetch_iiif_image', lambda fl_id, size: None)
    signed_in = {'user_id': None}
    monkeypatch.setattr(GlobalAuthState, 'get_user_id',
                        classmethod(lambda cls: signed_in['user_id']))
    from web.api import init_api_routes
    app = FastAPI()
    init_api_routes(app_override=app)
    app.add_middleware(_SessionIdMiddleware)
    app.add_middleware(SessionMiddleware, secret_key='test-session-secret-' + 'y' * 24)
    yield app, service, signed_in, tmp_path / 'puzzle'
    pis.reset_puzzle_image_service()


def _params(processed):
    return {'fl_id': FL, 'size': 800, 'threshold': 30,
            'processed': str(processed).lower(), 'is_cul': 'false'}


def _get(client, processed=False):
    return client.get('/api/puzzle_image', params=_params(processed))


def _miss_and_upload(client, body, processed=False):
    miss = _get(client, processed)
    assert miss.status_code == 404
    token = miss.headers['X-Puzzle-Upload-Token']
    resp = client.post('/api/puzzle_process', params=_params(processed),
                       headers={'X-Puzzle-Upload-Token': token, 'Content-Type': 'image/jpeg'},
                       content=body)
    assert resp.status_code == 200
    return resp


@pytest.mark.parametrize('processed', [False, True])
def test_a_signed_out_upload_is_served_only_to_its_browser(puzzle_app, processed):
    app, service, _signed_in, cache_dir = puzzle_app
    browser_a, browser_b = TestClient(app), TestClient(app)
    uploaded = _miss_and_upload(browser_a, _jpeg((200, 30, 30)), processed)

    again_a = _get(browser_a, processed)
    seen_by_b = _get(browser_b, processed)

    assert again_a.status_code == 200
    assert again_a.content == uploaded.content
    assert again_a.headers['Cache-Control'].startswith('private')
    assert seen_by_b.status_code == 404
    assert not service.get_cache_path(FL, 800, 30.0, processed, False).exists()
    assert not (cache_dir / '_uploads.jsonl').exists()


def test_a_signed_in_upload_is_shared_and_its_uploader_recorded(puzzle_app):
    app, service, signed_in, cache_dir = puzzle_app
    browser_a, browser_b = TestClient(app), TestClient(app)
    body = _jpeg((30, 200, 30))

    signed_in['user_id'] = 'account-a'
    _miss_and_upload(browser_a, body)
    signed_in['user_id'] = None
    seen_by_b = _get(browser_b)

    assert seen_by_b.status_code == 200
    assert seen_by_b.content == body
    assert seen_by_b.headers['Cache-Control'].startswith('public')
    lines = (cache_dir / '_uploads.jsonl').read_text(encoding='utf-8').splitlines()
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record['user_id'] == 'account-a'
    assert record['file'] == service.get_cache_path(FL, 800, 30.0, False, False).name
    assert record['uploaded_at'].endswith('+00:00')


def test_an_export_uses_the_requesting_browsers_own_images_only(puzzle_app):
    app, _service, _signed_in, _cache_dir = puzzle_app
    browser_a, browser_b = TestClient(app), TestClient(app)
    _miss_and_upload(browser_a, _jpeg((30, 30, 200)), processed=True)
    fragments = {'fragments': [{
        'sys_id': '990000000000001', 'folio_label': '1r', 'fl_id': FL,
        'shelfmark': 'ENA 1.1', 'processed': True, 'bg_removal_threshold': 30.0,
    }]}

    export_a = browser_a.post('/api/puzzle_export', json=fragments)
    export_b = browser_b.post('/api/puzzle_export', json=fragments)

    assert export_a.status_code == 200
    assert export_a.content[:4] == b'\x89PNG'
    assert export_b.status_code == 500  # nothing to draw for browser B


def test_a_browser_view_finds_its_own_upload_and_the_plain_service_does_not(tmp_path):
    from shared.puzzle_image_service import (
        PuzzleImageService, STORED_BROWSER, STORED_NOWHERE,
    )
    service = PuzzleImageService(cache_dir=tmp_path / 'puzzle')
    service._fetch_iiif_image = lambda fl_id, size: None
    own = b'image-from-browser-a'

    assert service.store_upload(FL, 800, 30.0, False, False, own,
                                browser_key='b:browser-a') == STORED_BROWSER
    assert service.store_upload(FL, 800, 30.0, False, False, b'image-with-no-owner') == STORED_NOWHERE

    assert service.for_browser('b:browser-a').resolve_fragment_image(
        FL, size=800, processed=False) == own
    assert service.for_browser('b:browser-b').resolve_fragment_image(
        FL, size=800, processed=False) is None
    assert service.resolve_fragment_image(FL, size=800, processed=False) is None


def test_a_shared_upload_never_replaces_a_cached_file(tmp_path):
    from shared.puzzle_image_service import PuzzleImageService, STORED_SHARED
    service = PuzzleImageService(cache_dir=tmp_path / 'puzzle')
    path = service.get_cache_path(FL, 800, 30.0, False, False)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'image-already-cached')

    assert service.store_upload(FL, 800, 30.0, False, False, b'image-from-account-b',
                                user_id='account-b') == STORED_SHARED

    assert path.read_bytes() == b'image-already-cached'
    assert not (tmp_path / 'puzzle' / '_uploads.jsonl').exists()


class _FakeResponse:
    def __init__(self, content):
        self.status_code = 200
        self.content = content


def test_an_export_never_files_a_url_image_under_a_fragment_name(puzzle_app, monkeypatch):
    import shared.puzzle_image_service as pis
    app, service, _signed_in, _cache_dir = puzzle_app
    fetched = []

    def _fake_get(url, **kwargs):
        fetched.append(url)
        return _FakeResponse(_jpeg((120, 60, 20)))

    monkeypatch.setattr(pis.requests, 'get', _fake_get)
    browser_a, browser_b = TestClient(app), TestClient(app)
    fragments = {'fragments': [{
        'sys_id': '990000000000001', 'folio_label': '1r', 'fl_id': FL,
        'shelfmark': 'ENA 1.1', 'processed': True, 'bg_removal_threshold': 30.0,
        'image_url': 'https://images.example.org/iiif/item',
    }]}

    browser_a.post('/api/puzzle_export', json=fragments)
    seen_by_b = _get(browser_b, processed=True)

    assert not any('example.org' in url for url in fetched)
    assert not service.get_cache_path(FL, 800, 30.0, True, False).exists()
    assert seen_by_b.status_code == 404


def test_a_url_image_is_cached_under_its_url_name(tmp_path, monkeypatch):
    import shared.puzzle_image_service as pis
    service = pis.PuzzleImageService(cache_dir=tmp_path / 'puzzle')
    body = _jpeg((20, 120, 60))
    monkeypatch.setattr(pis.requests, 'get', lambda url, **kwargs: _FakeResponse(body))
    url = 'https://images.lib.cam.ac.uk/iiif/MS-TS-00001-00001-000-00001.jp2'

    got = service.resolve_fragment_image(FL, size=800, processed=False, image_url=url, web=True)

    assert got == body
    assert not service.get_cache_path(FL, 800, 30.0, False, False).exists()
    url_name = pis._safe_filename(url[:120])
    assert service.read_cached(url_name, 800, 30.0, False, False) == (body, False)


@pytest.mark.parametrize('url', [
    'https://images.example.org/iiif/item',
    'http://127.0.0.1/iiif/item',
    'https://images.lib.cam.ac.uk.example.org/iiif/item',
    'https://someone@images.lib.cam.ac.uk/iiif/item',
    'https://images.lib.cam.ac.uk:8080/iiif/item',
    'file:///tmp/item',
])
def test_only_known_library_image_hosts_are_fetched(tmp_path, monkeypatch, url):
    import shared.puzzle_image_service as pis
    service = pis.PuzzleImageService(cache_dir=tmp_path / 'puzzle')
    fetched = []

    def _fake_get(u, **kwargs):
        fetched.append(u)
        return _FakeResponse(_jpeg((50, 50, 50)))

    monkeypatch.setattr(pis.requests, 'get', _fake_get)

    assert service.resolve_fragment_image('', size=800, processed=False, image_url=url,
                                          web=True) is None
    assert fetched == []


def test_a_request_without_a_session_cookie_keeps_nothing(puzzle_app):
    app, service, _signed_in, cache_dir = puzzle_app
    miss = _get(TestClient(app))
    token = miss.headers['X-Puzzle-Upload-Token']
    no_cookie = TestClient(app)

    resp = no_cookie.post('/api/puzzle_process', params=_params(False),
                          headers={'X-Puzzle-Upload-Token': token, 'Content-Type': 'image/jpeg'},
                          content=_jpeg((70, 70, 70)))

    assert resp.status_code == 200
    assert resp.headers['Cache-Control'].startswith('private')
    assert not service.get_cache_path(FL, 800, 30.0, False, False).exists()
    browser_dir = cache_dir / '_browser'
    assert not browser_dir.exists() or not any(p.is_file() for p in browser_dir.rglob('*'))


@pytest.mark.parametrize('name', ['cambridge_990000000000001_page0', '12a34', 'FL', '12_34'])
def test_only_nli_image_ids_name_a_fragment_image(puzzle_app, name):
    app, _service, _signed_in, _cache_dir = puzzle_app
    client = TestClient(app)
    params = dict(_params(False), fl_id=name)

    got = client.get('/api/puzzle_image', params=params)
    posted = client.post('/api/puzzle_process', params=params,
                         headers={'X-Puzzle-Upload-Token': 'x|y', 'Content-Type': 'image/jpeg'},
                         content=_jpeg((50, 50, 50)))

    assert got.status_code == 400
    assert 'X-Puzzle-Upload-Token' not in got.headers
    assert posted.status_code == 400


def test_an_external_page_image_request_uses_the_preset_size(puzzle_app):
    app, service, _signed_in, _cache_dir = puzzle_app
    cached = b'image-already-cached-for-page-0'
    path = service.get_cache_path('cambridge_990000000000001_page0', 800, 30.0, True, False)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(cached)

    got = TestClient(app).get('/api/puzzle_ext_image', params={
        'sys_id': '990000000000001', 'page': 0, 'provider': 'cambridge',
        'size': 801, 'threshold': 30.04,
    })

    assert got.status_code == 200
    assert got.content == cached
