# -*- coding: utf-8 -*-
"""Fragment Puzzle image cache: how a file gets into the shared cache.

* a signed-in upload: the uploader's record line is on disk before the file
  has its name, and when the line cannot be written the file is not shared;
* web writes use a create-only atomic step; where hard links are not
  available the result is left uncached (never written under its final name);
* web fetches follow redirects one hop at a time and only to the known
  library image hosts: the direct-URL fetch, the NLI fetch and the provider
  image fetch behind /api/puzzle_ext_image;
* the desktop keeps its v9.4.0 behaviour: a fragment with both an fl_id and
  an image URL is found under its fl_id, and a URL on any host is fetched.

A temp cache directory; ``requests.get`` is replaced by a fake that behaves as
requests does (it follows redirects itself unless ``allow_redirects=False``),
so no network is used.
"""
from __future__ import annotations

import errno
import io
import json
import os
import uuid
from urllib.parse import urljoin

import pytest
import requests
from PIL import Image

FL = '23456789'
CAM_URL = 'https://images.lib.cam.ac.uk/iiif/MS-TS-00012-00034-000-00001.jp2'
CAM_FULL = CAM_URL + '/full/800,/0/default.jpg'
OX_FULL = 'https://iiif.bodleian.ox.ac.uk/iiif/image/abc/full/800,/0/default.jpg'
OTHER_FULL = 'https://images.example.org/iiif/other/full/800,/0/default.jpg'
NLI_FULL = f'https://iiif.nli.org.il/IIIFv21/FL{FL}/full/800,/0/default.jpg'


def _jpeg(color) -> bytes:
    buf = io.BytesIO()
    Image.new('RGB', (64, 48), color=color).save(buf, format='JPEG')
    return buf.getvalue()


class _Resp:
    def __init__(self, status, content=b'', location=None):
        self.status_code = status
        self.content = content
        self.body = content
        self.headers = {'Content-Type': 'image/jpeg'}
        if location:
            self.headers['Location'] = location

    def close(self):
        pass


def _fake_web(routes, calls):
    """A requests.get stand-in over ``routes`` (url -> _Resp)."""
    def get(url, *args, allow_redirects=True, **kwargs):
        calls.append((url, allow_redirects))
        resp = routes[url]
        hops = 0
        while allow_redirects and resp.status_code in (301, 302, 303, 307, 308):
            hops += 1
            if hops > 30:
                raise requests.exceptions.TooManyRedirects('Exceeded 30 redirects.')
            url = urljoin(url, resp.headers['Location'])
            calls.append((url, 'followed by requests'))
            resp = routes[url]
        return resp
    return get


@pytest.fixture
def pis(monkeypatch):
    import shared.puzzle_image_service as module
    from shared import nli_circuit_breaker
    nli_circuit_breaker._reset_for_tests()
    module.reset_puzzle_image_service()
    yield module
    module.reset_puzzle_image_service()
    nli_circuit_breaker._reset_for_tests()


def _files(cache_dir):
    return sorted(p.relative_to(cache_dir).as_posix() for p in cache_dir.rglob('*') if p.is_file())


# ── Q2: the uploader is recorded before the file is shared ─────────────────

def test_the_uploader_record_is_on_disk_before_the_shared_file_has_its_name(tmp_path, pis,
                                                                           monkeypatch):
    service = pis.PuzzleImageService(cache_dir=tmp_path / 'puzzle')
    manifest = tmp_path / 'puzzle' / pis.UPLOAD_MANIFEST_NAME
    body = _jpeg((30, 200, 30))
    real_link = os.link
    record_seen_at_link = []

    def _link(src, dst, *args, **kwargs):
        lines = manifest.read_text(encoding='utf-8').splitlines() if manifest.exists() else []
        record_seen_at_link.append(any(json.loads(line)['file'] == os.path.basename(str(dst))
                                       for line in lines))
        return real_link(src, dst, *args, **kwargs)

    monkeypatch.setattr(os, 'link', _link)

    stored = service.store_upload(FL, 800, 30.0, False, False, body, user_id='account-a')

    assert stored == pis.STORED_SHARED
    assert record_seen_at_link == [True]
    record = json.loads(manifest.read_text(encoding='utf-8').splitlines()[0])
    path = service.get_cache_path(FL, 800, 30.0, False, False)
    assert record['file'] == path.name
    assert record['user_id'] == 'account-a'
    assert record['sha256'] == __import__('hashlib').sha256(body).hexdigest()
    assert path.read_bytes() == body


@pytest.mark.parametrize('browser_key', ['b:browser-a', None])
def test_a_signed_in_upload_whose_record_cannot_be_written_is_not_shared(tmp_path, pis,
                                                                        browser_key):
    service = pis.PuzzleImageService(cache_dir=tmp_path / 'puzzle')
    # A directory where the record file belongs: the line cannot be appended.
    (tmp_path / 'puzzle' / pis.UPLOAD_MANIFEST_NAME).mkdir()
    body = _jpeg((200, 200, 30))

    stored = service.store_upload(FL, 800, 30.0, False, False, body,
                                  user_id='account-a', browser_key=browser_key)

    assert not service.get_cache_path(FL, 800, 30.0, False, False).exists()
    assert service.read_cached(FL, 800, 30.0, False, False) is None
    if browser_key:
        assert stored == pis.STORED_BROWSER
        assert service.read_cached(FL, 800, 30.0, False, False,
                                   browser_key=browser_key) == (body, True)
    else:
        assert stored == pis.STORED_NOWHERE
    assert not [name for name in _files(tmp_path / 'puzzle') if name.startswith('.partial-')]


def test_the_upload_route_keeps_an_unrecorded_upload_for_its_browser_only(tmp_path, pis,
                                                                         monkeypatch):
    from fastapi import FastAPI
    from starlette.middleware.sessions import SessionMiddleware
    from starlette.testclient import TestClient
    from web.auth_state import GlobalAuthState
    from web.api import init_api_routes

    service = pis.get_puzzle_image_service(cache_dir=tmp_path / 'puzzle')
    monkeypatch.setattr(service, '_fetch_iiif_image', lambda fl_id, size: None)
    monkeypatch.setattr(GlobalAuthState, 'get_user_id', classmethod(lambda cls: 'account-a'))
    (tmp_path / 'puzzle' / pis.UPLOAD_MANIFEST_NAME).mkdir()

    class _SessionId:
        def __init__(self, app):
            self.app = app

        async def __call__(self, scope, receive, send):
            if scope['type'] == 'http' and scope.get('session') is not None:
                scope['session'].setdefault('id', str(uuid.uuid4()))
            await self.app(scope, receive, send)

    app = FastAPI()
    init_api_routes(app_override=app)
    app.add_middleware(_SessionId)
    app.add_middleware(SessionMiddleware, secret_key='test-session-secret-' + 'z' * 24)
    params = {'fl_id': FL, 'size': 800, 'threshold': 30, 'processed': 'false', 'is_cul': 'false'}
    browser_a, browser_b = TestClient(app), TestClient(app)
    body = _jpeg((10, 90, 160))

    miss = browser_a.get('/api/puzzle_image', params=params)
    posted = browser_a.post('/api/puzzle_process', params=params, content=body, headers={
        'X-Puzzle-Upload-Token': miss.headers['X-Puzzle-Upload-Token'],
        'Content-Type': 'image/jpeg'})
    again_a = browser_a.get('/api/puzzle_image', params=params)
    seen_by_b = browser_b.get('/api/puzzle_image', params=params)

    assert posted.status_code == 200
    assert posted.headers['Cache-Control'].startswith('private')
    assert again_a.status_code == 200 and again_a.content == body
    assert again_a.headers['Cache-Control'].startswith('private')
    assert seen_by_b.status_code == 404
    assert not service.get_cache_path(FL, 800, 30.0, False, False).exists()


# ── Q2: a web write is atomic, or it is not made ───────────────────────────

def test_without_hard_links_a_web_result_is_left_uncached(tmp_path, pis, monkeypatch):
    service = pis.PuzzleImageService(cache_dir=tmp_path / 'puzzle')
    body = _jpeg((90, 20, 20))
    calls = []
    monkeypatch.setattr(pis.requests, 'get', _fake_web({CAM_FULL: _Resp(200, body)}, calls))

    def _no_links(src, dst, *args, **kwargs):
        raise OSError(errno.EPERM, 'hard links not supported here')

    monkeypatch.setattr(os, 'link', _no_links)

    got = service.for_browser(None).resolve_fragment_image(
        '', size=800, processed=False, image_url=CAM_URL)
    stored = service.store_upload(FL, 800, 30.0, False, False, body, user_id='account-a')

    assert got == body
    assert stored == pis.STORED_NOWHERE
    assert not service.get_cache_path(FL, 800, 30.0, False, False).exists()
    assert not service.get_cache_path(pis._safe_filename(CAM_URL[:120]), 800, 30.0,
                                      False, False).exists()
    assert [n for n in _files(tmp_path / 'puzzle') if n != pis.UPLOAD_MANIFEST_NAME] == []


# ── Q4: redirects are checked hop by hop ───────────────────────────────────

def test_a_direct_url_redirect_off_the_library_hosts_is_not_followed(tmp_path, pis,
                                                                     monkeypatch):
    service = pis.PuzzleImageService(cache_dir=tmp_path / 'puzzle')
    calls = []
    monkeypatch.setattr(pis.requests, 'get', _fake_web({
        CAM_FULL: _Resp(302, location=OTHER_FULL),
        OTHER_FULL: _Resp(200, _jpeg((1, 2, 3))),
    }, calls))

    got = service.for_browser(None).resolve_fragment_image(
        '', size=800, processed=False, image_url=CAM_URL)

    assert got is None
    assert not any('example.org' in url for url, _ in calls)
    assert _files(tmp_path / 'puzzle') == []


def test_redirects_between_library_hosts_are_followed_one_checked_hop_at_a_time(
        tmp_path, pis, monkeypatch):
    service = pis.PuzzleImageService(cache_dir=tmp_path / 'puzzle')
    body = _jpeg((4, 5, 6))
    calls = []
    monkeypatch.setattr(pis.requests, 'get', _fake_web({
        CAM_FULL: _Resp(302, location=OX_FULL),
        OX_FULL: _Resp(200, body),
    }, calls))

    got = service.for_browser(None).resolve_fragment_image(
        '', size=800, processed=False, image_url=CAM_URL)

    assert got == body
    assert calls == [(CAM_FULL, False), (OX_FULL, False)]


def test_a_redirect_loop_between_library_hosts_stops_after_the_hop_limit(tmp_path, pis,
                                                                         monkeypatch):
    service = pis.PuzzleImageService(cache_dir=tmp_path / 'puzzle')
    calls = []
    monkeypatch.setattr(pis.requests, 'get', _fake_web({
        CAM_FULL: _Resp(302, location=OX_FULL),
        OX_FULL: _Resp(302, location=CAM_FULL),
    }, calls))

    got = service.for_browser(None).resolve_fragment_image(
        '', size=800, processed=False, image_url=CAM_URL)

    assert got is None
    assert all(flag is False for _, flag in calls)
    assert len(calls) == pis.MAX_REDIRECT_HOPS + 1


def _api_app(tmp_path, pis, monkeypatch):
    from fastapi import FastAPI
    from starlette.testclient import TestClient
    from web.api import init_api_routes
    pis.get_puzzle_image_service(cache_dir=tmp_path / 'puzzle')
    app = FastAPI()
    init_api_routes(app_override=app)
    return TestClient(app)


def test_the_puzzle_image_route_does_not_follow_an_nli_redirect_off_the_library_hosts(
        tmp_path, pis, monkeypatch):
    calls = []
    monkeypatch.setattr(pis.requests, 'get', _fake_web({
        NLI_FULL: _Resp(302, location=OTHER_FULL),
        OTHER_FULL: _Resp(200, _jpeg((7, 8, 9))),
    }, calls))
    client = _api_app(tmp_path, pis, monkeypatch)

    got = client.get('/api/puzzle_image', params={
        'fl_id': FL, 'size': 800, 'threshold': 30, 'processed': 'false', 'is_cul': 'false'})

    assert got.status_code == 404
    assert calls == [(NLI_FULL, False)]
    assert _files(tmp_path / 'puzzle') == []


def test_a_provider_image_redirect_off_the_library_hosts_is_not_cached(tmp_path, pis,
                                                                       monkeypatch):
    from web.state import state

    sys_id = '990000000000077'
    canvas = 'https://luna.manchester.ac.uk/iiif/item-77'
    start = canvas + '/full/2000,/0/default.jpg'

    class _Meta:
        nli_cache = {sys_id: {'images_ext': [{'url': canvas}]}}

        def enrich_metadata(self, _sys_id):
            return {}

    monkeypatch.setattr(state, 'meta_mgr', _Meta())
    calls = []
    monkeypatch.setattr(pis.requests, 'get', _fake_web({
        start: _Resp(302, location=OTHER_FULL),
        OTHER_FULL: _Resp(200, _jpeg((11, 12, 13))),
    }, calls))
    client = _api_app(tmp_path, pis, monkeypatch)

    got = client.get('/api/puzzle_ext_image', params={
        'sys_id': sys_id, 'page': 0, 'provider': 'manchester', 'processed': 'false'})

    assert got.status_code != 200
    assert calls == [(start, False)]
    assert _files(tmp_path / 'puzzle') == []


# ── Q5: the desktop keeps its v9.4.0 lookup and fetch ──────────────────────

def _run_desktop_loader(fl_id, image_url):
    from desktop.gui_threads import PuzzleImageLoaderThread
    loader = PuzzleImageLoaderThread(fl_id, threshold=30.0, size=800, processed=True,
                                     is_cul=False, image_url=image_url)
    ready, failed = [], []
    loader.image_ready.connect(lambda key, data: ready.append((key, bytes(data))))
    loader.load_failed.connect(lambda key, msg: failed.append((key, msg)))
    loader.run()  # the thread body, run in this thread
    return ready, failed


def test_the_desktop_finds_a_fragment_with_an_image_url_under_its_fl_id_offline(
        tmp_path, pis, monkeypatch):
    service = pis.get_puzzle_image_service(cache_dir=tmp_path / 'puzzle')
    cached = b'\x89PNG' + b'image-cached-by-an-earlier-desktop-session'
    path = service.get_cache_path(FL, 800, 30.0, True, False)
    path.write_bytes(cached)
    calls = []

    def _offline(url, *args, **kwargs):
        calls.append(url)
        raise requests.exceptions.ConnectionError('offline')

    monkeypatch.setattr(pis.requests, 'get', _offline)

    ready, failed = _run_desktop_loader(FL, CAM_URL)

    assert failed == []
    assert ready == [(FL, cached)]
    assert calls == []


def test_the_desktop_still_fetches_an_image_url_on_any_host(tmp_path, pis, monkeypatch):
    pis.get_puzzle_image_service(cache_dir=tmp_path / 'puzzle')
    canvas = 'https://images.example.org/iiif/item-9'
    body = _jpeg((100, 110, 120))
    calls = []
    monkeypatch.setattr(pis.requests, 'get', _fake_web({
        canvas + '/full/800,/0/default.jpg': _Resp(200, body)}, calls))

    ready, failed = _run_desktop_loader('', canvas)

    assert failed == []
    assert len(ready) == 1 and ready[0][0] == canvas
    assert calls == [(canvas + '/full/800,/0/default.jpg', True)]
