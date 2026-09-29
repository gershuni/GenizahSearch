# -*- coding: utf-8 -*-
"""Fragment Puzzle image cache: how a file gets into the shared cache.

* a signed-in upload: the uploader's record line is on disk before the file
  has its name, and when the line cannot be written the file is not shared;
* web writes use a create-only atomic step; where hard links are not
  available the result is left uncached (never written under its final name);
* web fetches follow redirects one hop at a time and only to the known
  library image hosts, at every site that can fill the shared cache: the
  direct-URL fetch, the NLI fetch (/api/puzzle_image and the threshold
  pre-fetch), each provider fetch behind /api/puzzle_ext_image and the
  Cambridge route's NLI fallback (IIIF and Rosetta); a refused hop does not
  count toward the NLI breaker; /api/proxy_image checks its hops too;
* the real image URLs of every host (live probe, 2026-09-29: no redirects)
  are still served to the /browse viewer;
* the record line and the image bytes are fsynced before the link, and a
  record whose file did not get its name is followed by a not-published line;
* every web call site uses the web rules (a source-level guard);
* the desktop keeps its v9.4.0 behaviour: a fragment with both an fl_id and
  an image URL is found under its fl_id, a URL on any host is fetched, and
  the result is cached even where hard links are not available.

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
    def __init__(self, status, content=b'', location=None, content_type='image/jpeg'):
        self.status_code = status
        self.content = content
        self.body = content
        self.headers = {'Content-Type': content_type}
        if location:
            self.headers['Location'] = location
        # What requests sets on a response it did not follow (nli_image_get reads these).
        self.is_redirect = bool(location) and status in (301, 302, 303, 307, 308)
        self.next = None

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
        if resp.is_redirect:
            from types import SimpleNamespace
            resp.next = SimpleNamespace(url=urljoin(url, resp.headers['Location']))
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


# Real canvas / image URLs of each provider (from nli_cache.pkl and oxford_full_db.json);
# a live probe on 2026-09-29 got each one with no redirect (see the "real chains" tests).
REAL_CUDL_CANVAS = 'https://images.lib.cam.ac.uk/iiif/MS-TS-NS-00075-00015-000-00001.jp2'
REAL_LUNA_CANVAS = ('https://luna.manchester.ac.uk/luna/servlet/iiif/'
                    'ManchesterDev~95~2~136876~127421')
REAL_FIGGY_CANVAS = ('https://iiif-cloud.princeton.edu/iiif/2/fe%2F07%2Ffb%2F'
                     'fe07fbf1fc9549b6a59d21056814cc9b%2Fintermediate_file')
REAL_OX_FULL = 'https://hebrew.bodleian.ox.ac.uk/fragments/full/MS_HEB_f_107_41a.jpg'
REAL_OX_THUMB = 'https://hebrew.bodleian.ox.ac.uk/fragments/thumbs/MS_HEB_f_107_41a.jpg'


class _Meta:
    """The metadata manager the provider routes read, for one sys_id."""

    def __init__(self, sys_id, images_ext=(), oxford_images=()):
        self.nli_cache = {sys_id: {'images_ext': list(images_ext)}}
        self.codico_mgr = None
        if oxford_images:
            images = list(oxford_images)

            class _Codico:
                _loaded = True
                part_metadata = {}

                def get_part_for_folio(self, _sys_id):
                    return 'MS. Heb. f. 107/23'

                def get_part_images(self, _part_id):
                    return images

            self.codico_mgr = _Codico()

    def enrich_metadata(self, _sys_id):
        return {}

    def get_meta_for_id(self, _sys_id):
        return ('MS heb. f.107/41',)


def _provider_setup(provider, sys_id, monkeypatch):
    """Point ``provider``'s route at one real canvas; return the first URL it requests."""
    from web.state import state
    if provider == 'oxford':
        meta = _Meta(sys_id, oxford_images=[
            {'folio_num': 41, 'full_url': REAL_OX_FULL, 'thumb_url': REAL_OX_THUMB}])
        start = REAL_OX_FULL
    else:
        canvas = {'manchester': REAL_LUNA_CANVAS, 'cambridge': REAL_CUDL_CANVAS,
                  'jts': REAL_FIGGY_CANVAS}[provider]
        meta = _Meta(sys_id, images_ext=[{'url': canvas}])
        start = canvas + '/full/2000,/0/default.jpg'
    if provider == 'cambridge':
        # No nli_images rows: the route takes images_ext[page] (the CUDL fetch).
        import shared.nli_crossref_service as crossref
        monkeypatch.setattr(crossref, 'resolve_cambridge_canvas_for_page',
                            lambda *a, **kw: {'degraded': True})
    monkeypatch.setattr(state, 'meta_mgr', meta)
    return start


@pytest.mark.parametrize('provider', ['manchester', 'cambridge', 'jts', 'oxford'])
def test_a_provider_image_redirect_off_the_library_hosts_is_not_cached(tmp_path, pis,
                                                                       monkeypatch, provider):
    sys_id = f'9900000000{len(provider):02d}77'
    start = _provider_setup(provider, sys_id, monkeypatch)
    calls = []
    monkeypatch.setattr(pis.requests, 'get', _fake_web({
        start: _Resp(302, location=OTHER_FULL),
        OTHER_FULL: _Resp(200, _jpeg((11, 12, 13))),
    }, calls))
    client = _api_app(tmp_path, pis, monkeypatch)

    got = client.get('/api/puzzle_ext_image', params={
        'sys_id': sys_id, 'page': 0, 'provider': provider, 'processed': 'false'},
        follow_redirects=False)

    assert got.status_code != 200
    assert calls == [(start, False)]
    assert _files(tmp_path / 'puzzle') == []


# ── the Cambridge route's NLI fallback: IIIF, then the Rosetta thumbnail ───

NLI_2000 = f'https://iiif.nli.org.il/IIIFv21/FL{FL}/full/2000,/0/default.jpg'
ROSETTA_THUMB = ('https://rosetta.nli.org.il/delivery/DeliveryManagerServlet'
                 f'?dps_func=thumbnail&dps_pid=FL{FL}')
BIG_IMAGE = _jpeg((40, 50, 60)) + b'\0' * 6000  # over both size floors


class _Manifest:
    status_code = 200
    headers = {'Content-Type': 'application/json'}

    def json(self):
        return {'sequences': [{'canvases': [{'images': [{'resource': {
            'service': {'@id': f'https://iiif.nli.org.il/IIIFv21/FL{FL}'}}}]}]}]}


def _nli_manifest_for(sys_id, monkeypatch):
    """Serve the NLI manifest (one FL) for ``sys_id``; never write the FL-id cache file."""
    import web.api as api_mod
    manifest_url = f'https://iiif.nli.org.il/IIIFv21/DOCID/PNX_MANUSCRIPTS{sys_id}-1/manifest'

    def _session_get(url, *args, **kwargs):
        assert url == manifest_url, url
        return _Manifest()

    monkeypatch.setattr(api_mod._nli_session, 'get', _session_get)
    monkeypatch.setattr(api_mod, '_save_nli_persistent_cache', lambda *a, **kw: None)


@pytest.mark.parametrize('redirected', ['iiif', 'rosetta'])
def test_the_cambridge_nli_fallback_does_not_follow_a_redirect_off_the_library_hosts(
        tmp_path, pis, monkeypatch, redirected):
    from shared import nli_circuit_breaker
    sys_id = '990000000000' + {'iiif': '881', 'rosetta': '882'}[redirected]
    monkeypatch.setattr(__import__('web.state', fromlist=['state']).state, 'meta_mgr',
                        _Meta(sys_id, images_ext=[]))
    _nli_manifest_for(sys_id, monkeypatch)
    other_png = 'https://images.example.org/thumb.png'
    routes = {
        OTHER_FULL: _Resp(200, BIG_IMAGE),
        other_png: _Resp(200, BIG_IMAGE, content_type='image/png'),
    }
    if redirected == 'iiif':
        routes[NLI_2000] = _Resp(302, location=OTHER_FULL)
        routes[ROSETTA_THUMB] = _Resp(404, b'', content_type='text/html')
    else:
        routes[NLI_2000] = _Resp(404, b'', content_type='text/html')
        routes[ROSETTA_THUMB] = _Resp(302, location=other_png)
    calls = []
    monkeypatch.setattr(pis.requests, 'get', _fake_web(routes, calls))
    client = _api_app(tmp_path, pis, monkeypatch)

    got = client.get('/api/puzzle_ext_image', params={
        'sys_id': sys_id, 'page': 0, 'provider': 'cambridge', 'processed': 'false'})

    assert got.status_code != 200
    assert calls and all(flag is False for _, flag in calls)
    assert not any('example.org' in url for url, _ in calls)
    assert _files(tmp_path / 'puzzle') == []
    # A refused hop is not an NLI outage: it must not count toward the breaker.
    assert nli_circuit_breaker._state_snapshot()['consecutive_failures'] == 0


def test_the_threshold_prefetch_does_not_follow_an_nli_redirect_off_the_library_hosts(
        tmp_path, pis, monkeypatch):
    from web.pages.puzzle import _invalidate_and_refetch
    pis.get_puzzle_image_service(cache_dir=tmp_path / 'puzzle')
    calls = []
    monkeypatch.setattr(pis.requests, 'get', _fake_web({
        NLI_FULL: _Resp(302, location=OTHER_FULL),
        OTHER_FULL: _Resp(200, _jpeg((14, 15, 16))),
    }, calls))

    _invalidate_and_refetch(FL, 42.0)

    assert calls == [(NLI_FULL, False)]
    assert _files(tmp_path / 'puzzle') == []


def test_the_image_proxy_does_not_follow_a_redirect_off_the_allowed_domains(tmp_path, pis,
                                                                           monkeypatch):
    start = f'https://iiif.nli.org.il/IIIFv21/FL{FL}/full/400,/0/default.jpg'
    lookalike = 'https://iiif.nli.org.il.example.org/x.jpg'
    calls = []
    monkeypatch.setattr(pis.requests, 'get', _fake_web({
        start: _Resp(302, location=lookalike),
        lookalike: _Resp(200, _jpeg((17, 18, 19))),
    }, calls))
    client = _api_app(tmp_path, pis, monkeypatch)

    got = client.get('/api/proxy_image', params={'url': start})

    assert got.status_code != 200
    assert calls == [(start, False)]


# ── /browse: the real chains of every image host still work ────────────────
# Live probe, 2026-09-29, one GET per host with allow_redirects=False: every
# host answered the first request itself (no redirect), so the hop checks never
# refuse a real image. The Bodleian answered 200 text/html (a bot-check page).

@pytest.mark.parametrize('provider, real_url', [
    ('cambridge', REAL_CUDL_CANVAS + '/full/2000,/0/default.jpg'),
    ('manchester', REAL_LUNA_CANVAS + '/full/2000,/0/default.jpg'),
    ('jts', REAL_FIGGY_CANVAS + '/full/2000,/0/default.jpg'),
])
def test_the_browse_viewer_gets_each_real_provider_image(tmp_path, pis, monkeypatch,
                                                        provider, real_url):
    sys_id = f'9900000000{len(provider):02d}55'
    assert _provider_setup(provider, sys_id, monkeypatch) == real_url
    body = _jpeg((21, 22, 23))
    calls = []
    monkeypatch.setattr(pis.requests, 'get', _fake_web({real_url: _Resp(200, body)}, calls))
    client = _api_app(tmp_path, pis, monkeypatch)

    got = client.get(f'/api/{provider}_image/{sys_id}', params={'page': 0})

    assert got.status_code == 200
    assert got.content == body
    assert calls == [(real_url, False)]


def test_the_browse_viewer_sends_the_real_bodleian_answer_to_the_browser(tmp_path, pis,
                                                                        monkeypatch):
    sys_id = '990000000000655'
    _provider_setup('oxford', sys_id, monkeypatch)
    calls = []
    monkeypatch.setattr(pis.requests, 'get', _fake_web({
        REAL_OX_FULL: _Resp(200, b'<!doctype html>' * 20, content_type='text/html; charset=utf-8'),
    }, calls))
    client = _api_app(tmp_path, pis, monkeypatch)

    got = client.get(f'/api/oxford_image/{sys_id}', params={'page': 0}, follow_redirects=False)

    assert got.status_code == 307
    assert got.headers['location'] == REAL_OX_FULL
    assert calls == [(REAL_OX_FULL, False)]


@pytest.mark.parametrize('source', ['iiif', 'rosetta'])
def test_the_browse_viewer_gets_the_real_nli_image(tmp_path, pis, monkeypatch, source):
    sys_id = '990000000000' + {'iiif': '771', 'rosetta': '772'}[source]
    _nli_manifest_for(sys_id, monkeypatch)
    png = b'\x89PNG' + b'\0' * 3000
    routes = {NLI_2000: _Resp(200, BIG_IMAGE),
              ROSETTA_THUMB: _Resp(200, png, content_type='image/png;charset=UTF-8')}
    if source == 'rosetta':
        routes[NLI_2000] = _Resp(404, b'', content_type='text/html')
    calls = []
    monkeypatch.setattr(pis.requests, 'get', _fake_web(routes, calls))
    client = _api_app(tmp_path, pis, monkeypatch)

    got = client.get(f'/api/nli_image_by_sysid/{sys_id}', params={'page': 0})

    assert got.status_code == 200
    assert got.content == (BIG_IMAGE if source == 'iiif' else png)
    expected = [(NLI_2000, False)] + ([(ROSETTA_THUMB, False)] if source == 'rosetta' else [])
    assert calls == expected


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


def test_the_desktop_still_caches_where_hard_links_are_not_available(tmp_path, pis,
                                                                     monkeypatch):
    service = pis.get_puzzle_image_service(cache_dir=tmp_path / 'puzzle')
    canvas = 'https://images.example.org/iiif/item-10'
    calls = []
    monkeypatch.setattr(pis.requests, 'get', _fake_web({
        canvas + '/full/800,/0/default.jpg': _Resp(200, _jpeg((120, 130, 140)))}, calls))

    def _no_links(src, dst, *args, **kwargs):
        raise OSError(errno.EPERM, 'hard links not supported here')

    monkeypatch.setattr(os, 'link', _no_links)

    ready, failed = _run_desktop_loader('', canvas)

    assert failed == []
    assert len(ready) == 1
    path = service.get_cache_path(pis._safe_filename(canvas[:120]), 800, 30.0, True, False)
    assert path.read_bytes() == ready[0][1]
    assert not [n for n in _files(tmp_path / 'puzzle') if n.startswith('.partial-')]


# ── the record line and the image bytes are flushed before the link ────────

def test_the_record_line_and_the_image_are_fsynced_before_the_shared_file_has_its_name(
        tmp_path, pis, monkeypatch):
    service = pis.PuzzleImageService(cache_dir=tmp_path / 'puzzle')
    manifest = tmp_path / 'puzzle' / pis.UPLOAD_MANIFEST_NAME
    body = _jpeg((50, 60, 70))
    real_fsync, real_link = os.fsync, os.link
    fsynced = []    # (st_dev, st_ino) of every file fsynced
    at_link = []

    def _fsync(fd):
        st = os.fstat(fd)
        fsynced.append((st.st_dev, st.st_ino))
        return real_fsync(fd)

    def _link(src, dst, *args, **kwargs):
        src_st, manifest_st = os.stat(src), os.stat(manifest)
        at_link.append({
            'temp file fsynced': (src_st.st_dev, src_st.st_ino) in fsynced,
            'record fsynced': (manifest_st.st_dev, manifest_st.st_ino) in fsynced,
        })
        return real_link(src, dst, *args, **kwargs)

    monkeypatch.setattr(os, 'fsync', _fsync)
    monkeypatch.setattr(os, 'link', _link)

    stored = service.store_upload(FL, 800, 30.0, False, False, body, user_id='account-a')

    assert stored == pis.STORED_SHARED
    assert os.stat(manifest).st_ino != 0  # file ids are real on this file system
    assert at_link == [{'temp file fsynced': True, 'record fsynced': True}]


# ── a record whose file was not published is withdrawn ─────────────────────

def _record_lines(tmp_path, pis):
    manifest = tmp_path / 'puzzle' / pis.UPLOAD_MANIFEST_NAME
    return [json.loads(line) for line in manifest.read_text(encoding='utf-8').splitlines()]


def test_a_record_whose_link_then_fails_is_followed_by_a_not_published_line(tmp_path, pis,
                                                                            monkeypatch):
    service = pis.PuzzleImageService(cache_dir=tmp_path / 'puzzle')
    body = _jpeg((80, 90, 100))

    def _no_links(src, dst, *args, **kwargs):
        raise OSError(errno.EPERM, 'hard links not supported here')

    monkeypatch.setattr(os, 'link', _no_links)

    stored = service.store_upload(FL, 800, 30.0, False, False, body, user_id='account-a')

    assert stored == pis.STORED_NOWHERE
    assert not service.get_cache_path(FL, 800, 30.0, False, False).exists()
    lines = _record_lines(tmp_path, pis)
    assert len(lines) == 2, lines
    first, second = lines
    assert 'published' not in first
    assert second['published'] is False
    for key in ('file', 'user_id', 'sha256'):
        assert second[key] == first[key]


def test_a_record_that_loses_the_name_to_another_copy_is_followed_by_a_not_published_line(
        tmp_path, pis, monkeypatch):
    service = pis.PuzzleImageService(cache_dir=tmp_path / 'puzzle')
    ours, theirs = _jpeg((1, 90, 1)), _jpeg((90, 1, 1))
    path = service.get_cache_path(FL, 800, 30.0, False, False)

    def _link(src, dst, *args, **kwargs):
        path.write_bytes(theirs)  # another copy is linked first
        raise FileExistsError(errno.EEXIST, 'exists', str(dst))

    monkeypatch.setattr(os, 'link', _link)

    stored = service.store_upload(FL, 800, 30.0, False, False, ours, user_id='account-a')

    assert stored == pis.STORED_SHARED
    assert path.read_bytes() == theirs
    lines = _record_lines(tmp_path, pis)
    assert len(lines) == 2, lines
    first, second = lines
    assert second['published'] is False
    assert second['sha256'] == first['sha256'] == __import__('hashlib').sha256(ours).hexdigest()


# ── every web call site uses the web rules ─────────────────────────────────

_WEB_ROOT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'web')
# name -> index of the image-service argument (keyword ``image_service``)
_TAKES_AN_IMAGE_SERVICE = {'compose_puzzle_export': 1, 'generate_thumbnail': 1,
                           'publish_join': 3}


def _is_for_browser_call(node):
    return (isinstance(node, __import__('ast').Call)
            and isinstance(node.func, __import__('ast').Attribute)
            and node.func.attr == 'for_browser')


def _web_rule_violations(source, filename):
    """Calls under web/ that could reach the image service without the web rules."""
    import ast
    tree = ast.parse(source, filename)
    parents = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            parents[child] = parent

    def _scope(node):
        while node in parents and not isinstance(
                node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Module)):
            node = parents[node]
        return node

    def _from_for_browser(expr):
        if _is_for_browser_call(expr):
            return True
        if not isinstance(expr, ast.Name):
            return False
        # Every assignment to that name in the same function is a for_browser(...) call.
        values = [n.value for n in ast.walk(_scope(expr))
                  if isinstance(n, ast.Assign)
                  and any(isinstance(t, ast.Name) and t.id == expr.id for t in n.targets)]
        return bool(values) and all(_is_for_browser_call(v) for v in values)

    def _name(expr):
        if isinstance(expr, ast.Name):
            return expr.id
        if isinstance(expr, ast.Attribute):
            return expr.attr
        return None

    problems, accounted = [], set()
    for call in [n for n in ast.walk(tree) if isinstance(n, ast.Call)]:
        where = f'{filename}:{call.lineno}'
        if _name(call.func) == 'resolve_fragment_image':
            accounted.add(id(call.func))
            web_true = any(k.arg == 'web' and isinstance(k.value, ast.Constant)
                           and k.value.value is True for k in call.keywords)
            receiver = call.func.value if isinstance(call.func, ast.Attribute) else None
            if not web_true and not (receiver is not None and _from_for_browser(receiver)):
                problems.append(f'{where}: resolve_fragment_image without web=True')
            continue
        # f(svc_arg...) directly, or run.io_bound(f, ...) / executor(f, ...).
        target, args = _name(call.func), list(call.args)
        if target not in _TAKES_AN_IMAGE_SERVICE:
            target = None
            for i, arg in enumerate(call.args):
                if _name(arg) in _TAKES_AN_IMAGE_SERVICE:
                    target, args = _name(arg), list(call.args[i + 1:])
                    accounted.add(id(arg))
                    break
        else:
            accounted.add(id(call.func))
        if target is None:
            continue
        index = _TAKES_AN_IMAGE_SERVICE[target]
        svc = args[index] if len(args) > index else next(
            (k.value for k in call.keywords if k.arg == 'image_service'), None)
        if svc is None or not _from_for_browser(svc):
            problems.append(f'{where}: {target} given an image service not from for_browser()')
    # Any other use of these names (a callback, a partial) is not checked: refuse it.
    for node in ast.walk(tree):
        if (isinstance(node, (ast.Name, ast.Attribute)) and isinstance(node.ctx, ast.Load)
                and _name(node) in set(_TAKES_AN_IMAGE_SERVICE) | {'resolve_fragment_image'}
                and id(node) not in accounted):
            problems.append(f'{filename}:{node.lineno}: unchecked use of {_name(node)}')
    return problems


def test_every_web_call_of_the_image_service_uses_the_web_rules():
    problems, sites = [], 0
    for root, _dirs, names in os.walk(_WEB_ROOT):
        for name in names:
            if not name.endswith('.py'):
                continue
            path = os.path.join(root, name)
            with open(path, encoding='utf-8') as fh:
                source = fh.read()
            if not any(n in source for n in (*_TAKES_AN_IMAGE_SERVICE, 'resolve_fragment_image')):
                continue
            sites += 1
            problems += _web_rule_violations(source, os.path.relpath(path, _WEB_ROOT))
    assert sites >= 2  # web/api.py and web/pages/puzzle.py at least
    assert problems == []


@pytest.mark.parametrize('snippet, ok', [
    ('svc.resolve_fragment_image(fl_id=f, web=True)', True),
    ('svc.for_browser(k).resolve_fragment_image(f)', True),
    ('svc.resolve_fragment_image(fl_id=f)', False),
    ('svc.resolve_fragment_image(fl_id=f, web=False)', False),
    ('def f():\n    s = get().for_browser(k)\n    compose_puzzle_export(frags, s)', True),
    ('def f():\n    s = get()\n    compose_puzzle_export(frags, s)', False),
    ('def f():\n    s = get()\n    generate_thumbnail(frags, image_service=s)', False),
    ('async def f():\n    s = get().for_browser(k)\n    await run.io_bound(publish_join, c, u, d, s)',
     True),
    ('async def f():\n    s = get()\n    await run.io_bound(publish_join, c, u, d, s)', False),
    ('cb = functools.partial(compose_puzzle_export, frags)', False),
])
def test_the_web_rule_guard_tells_the_cases_apart(snippet, ok):
    assert (_web_rule_violations(snippet, 'snippet.py') == []) is ok
