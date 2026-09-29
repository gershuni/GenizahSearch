# -*- coding: utf-8 -*-
"""Library fetches on the web: manifests and MARC records check every redirect hop.

The web's ``MetadataManager`` is built with ``checked_library_fetches=True``
(web/main.py, and the research worker the web starts). Its five library
fetches -- the NLI manifest, the NLI MARC record, the first MARC lookup, the
FL-id lookup and the CUDL/Figgy manifest -- then follow redirects one hop at a
time, only to the library hosts; so does the puzzle page's folio list. What
they return fills ``nli_cache`` (saved to disk) and, through
``/api/puzzle_ext_image``, the shared Fragment Puzzle image cache.

* through ``/api/puzzle_ext_image``: a redirect off the library hosts at any
  of the fetches is not followed, and nothing it would have supplied reaches
  the response, ``nli_cache`` or the shared image cache;
* a redirect that is not followed is handled as a 404: no NLI breaker
  failure, and the same result and per-sys_id negative cache a 404 leaves;
* the puzzle page's folio list does not follow such a redirect;
* the web builds its managers with the flag (a source-level pin);
* the desktop (the class default) calls ``session.get`` with the same
  arguments as at v9.4.0 and follows redirects as before.

A temp cache directory; the sessions and ``requests.get`` are fakes that
behave as requests does (they follow redirects themselves unless
``allow_redirects=False``), so no network is used.
"""
from __future__ import annotations

import ast
import io
import pathlib
from urllib.parse import urljoin

import pytest
import requests
from PIL import Image

REPO = pathlib.Path(__file__).resolve().parent.parent

SYS = '990012345670205171'
MARC_URL = f'https://iiif.nli.org.il/IIIFv21/marc/bib/{SYS}'
MANIFEST_URL = f'https://iiif.nli.org.il/IIIFv21/DOCID/PNX_MANUSCRIPTS{SYS}-1/manifest'
CUDL_VIEW = 'https://cudl.lib.cam.ac.uk/view/MS-TS-00001-00001'
CUDL_MANIFEST = 'https://cudl.lib.cam.ac.uk/iiif/MS-TS-00001-00001'
CANVAS = 'https://images.lib.cam.ac.uk/iiif/MS-TS-00001-00001-000-00001.jp2'
IMAGE = CANVAS + '/full/2000,/0/default.jpg'
OTHER_MARC = 'https://records.example.org/marc/bib/1'
OTHER_MANIFEST = 'https://records.example.org/iiif/manifest'
OTHER_FL = '7654321'


def _jpeg() -> bytes:
    buf = io.BytesIO()
    Image.new('RGB', (64, 48), color=(20, 30, 40)).save(buf, format='JPEG')
    return buf.getvalue() + b'\0' * 400


def _marc(shelfmark='T-S 1.1', link=CUDL_VIEW, fl='FL1234567') -> bytes:
    return (
        '<marc:record xmlns:marc="http://www.loc.gov/MARC21/slim">'
        f'<marc:datafield tag="942"><marc:subfield code="z">{shelfmark}</marc:subfield></marc:datafield>'
        f'<marc:datafield tag="856"><marc:subfield code="u">{link}</marc:subfield></marc:datafield>'
        f'<marc:datafield tag="907"><marc:subfield code="d">{fl}</marc:subfield></marc:datafield>'
        '</marc:record>'
    ).encode('utf-8')


def _cudl_manifest(canvas=CANVAS):
    return {'sequences': [{'canvases': [{'label': '1r', 'images': [{'resource': {
        'service': {'@id': canvas}}}]}]}]}


def _nli_manifest(fl=OTHER_FL):
    return {'sequences': [{'canvases': [{'label': '1r', 'images': [{'resource': {
        'service': {'@id': f'https://iiif.nli.org.il/IIIFv21/FL{fl}'}}}]}]}]}


class _Resp:
    def __init__(self, status, body=b'', *, json_body=None, location=None,
                 content_type='application/xml'):
        self.status_code = status
        self.content = body
        self.text = body.decode('utf-8', 'replace') if isinstance(body, bytes) else str(body)
        self._json = json_body
        self.headers = {'Content-Type': content_type}
        if location:
            self.headers['Location'] = location

    def json(self):
        if self._json is None:
            raise ValueError('no JSON body')
        return self._json

    def close(self):
        pass


class _Web:
    """``requests.get`` and ``Session.get`` over ``routes``; unknown URLs answer 404."""

    def __init__(self, routes):
        self.routes = routes
        self.calls = []  # (url, the keyword arguments exactly as passed)

    def _answer(self, url):
        resp = self.routes.get(url)
        return resp if resp is not None else _Resp(404)

    def get(self, url, *args, **kwargs):
        assert not args, args
        self.calls.append((url, dict(kwargs)))
        resp = self._answer(url)
        while kwargs.get('allow_redirects', True) and resp.status_code in (301, 302, 303, 307, 308):
            url = urljoin(url, resp.headers['Location'])
            self.calls.append((url, {'followed by requests': True}))
            resp = self._answer(url)
        return resp

    def all_unfollowed(self):
        return bool(self.calls) and all(kw.get('allow_redirects') is False for _, kw in self.calls)

    def urls(self):
        return [call[0] for call in self.calls]


def _normal_routes():
    return {
        MARC_URL: _Resp(200, _marc()),
        CUDL_MANIFEST: _Resp(200, json_body=_cudl_manifest(), content_type='application/json'),
        IMAGE: _Resp(200, _jpeg(), content_type='image/jpeg'),
    }


@pytest.fixture
def env(tmp_path, monkeypatch):
    """Config paths in tmp_path, no crossref/FJMS sidecars, breaker reset, failures recorded."""
    import shared.metadata_manager as mm_mod
    import shared.puzzle_image_service as pis
    from shared import nli_circuit_breaker
    from shared.config import Config

    monkeypatch.setattr(Config, 'INDEX_DIR', str(tmp_path / 'index'))
    monkeypatch.setattr(Config, 'CACHE_NLI', str(tmp_path / 'index' / 'nli_cache.pkl'))
    monkeypatch.setattr(Config, 'CACHE_META', str(tmp_path / 'index' / 'metadata_cache.pkl'))
    monkeypatch.setattr(mm_mod, '_get_crossref_service', lambda: None)
    monkeypatch.setattr(mm_mod, '_get_fjms_service', lambda: None)
    for cache in (mm_mod.MetadataManager._iiif_manifest_cache,
                  mm_mod.MetadataManager._iiif_manifest_fail_cache,
                  mm_mod.MetadataManager._marc_fail_cache):
        cache.clear()
    failures = []
    monkeypatch.setattr(mm_mod, '_nli_record_failure',
                        lambda failure_type, path: failures.append((failure_type, path)))
    nli_circuit_breaker._reset_for_tests()
    pis.reset_puzzle_image_service()
    yield {'mm_mod': mm_mod, 'pis': pis, 'failures': failures, 'tmp': tmp_path}
    pis.reset_puzzle_image_service()
    nli_circuit_breaker._reset_for_tests()
    for cache in (mm_mod.MetadataManager._iiif_manifest_cache,
                  mm_mod.MetadataManager._iiif_manifest_fail_cache,
                  mm_mod.MetadataManager._marc_fail_cache):
        cache.clear()


def _manager(env, web, monkeypatch, *, checked):
    mm_mod = env['mm_mod']
    mm = (mm_mod.MetadataManager(checked_library_fetches=True) if checked
          else mm_mod.MetadataManager())
    monkeypatch.setattr(mm, '_make_session', lambda: web)
    monkeypatch.setattr(mm, 'get_part_for_folio', lambda _sys_id: None)
    monkeypatch.setattr(requests, 'get', web.get)
    import time
    monkeypatch.setattr(time, 'sleep', lambda _s: None)  # the first lookup's retry pauses
    return mm


def _puzzle_files(env):
    root = env['tmp'] / 'puzzle'
    return sorted(p.name for p in root.rglob('*') if p.is_file()) if root.exists() else []


def _puzzle_ext_image(env, mm, monkeypatch):
    from fastapi import FastAPI
    from starlette.testclient import TestClient
    from web.api import init_api_routes
    from web.state import state

    env['pis'].get_puzzle_image_service(cache_dir=env['tmp'] / 'puzzle')
    monkeypatch.setattr(state, 'meta_mgr', mm)
    app = FastAPI()
    init_api_routes(app_override=app)
    return TestClient(app).get('/api/puzzle_ext_image', params={
        'sys_id': SYS, 'page': 0, 'provider': 'jts', 'processed': 'false'},
        follow_redirects=False)


# ── the route works with the checked fetches (no redirects) ────────────────

def test_the_route_serves_and_caches_a_library_image_with_the_checked_fetches(env, monkeypatch):
    web = _Web(_normal_routes())
    mm = _manager(env, web, monkeypatch, checked=True)

    got = _puzzle_ext_image(env, mm, monkeypatch)

    assert got.status_code == 200
    assert len(_puzzle_files(env)) == 1
    assert web.all_unfollowed()
    assert env['failures'] == []


# ── one redirect off the library hosts, at each fetch ──────────────────────

def test_an_external_manifest_redirect_off_the_library_hosts_is_not_followed(env, monkeypatch):
    routes = _normal_routes()
    routes[CUDL_MANIFEST] = _Resp(302, location=OTHER_MANIFEST)
    routes[OTHER_MANIFEST] = _Resp(200, json_body=_cudl_manifest(), content_type='application/json')
    web = _Web(routes)
    mm = _manager(env, web, monkeypatch, checked=True)

    got = _puzzle_ext_image(env, mm, monkeypatch)

    assert got.status_code != 200
    assert OTHER_MANIFEST not in web.urls()
    assert _puzzle_files(env) == []
    assert not mm.nli_cache[SYS].get('images_ext')
    assert env['failures'] == []


def test_a_marc_record_redirect_off_the_library_hosts_is_not_followed(env, monkeypatch):
    """fetch_marc_data: the basic record is already cached, so only it reads MARC_URL."""
    routes = _normal_routes()
    routes[MARC_URL] = _Resp(302, location=OTHER_MARC)
    routes[OTHER_MARC] = _Resp(200, _marc())
    web = _Web(routes)
    mm = _manager(env, web, monkeypatch, checked=True)
    mm.nli_cache[SYS] = {'shelfmark': 'T-S 1.1', 'title': '', 'fl_ids': []}

    got = _puzzle_ext_image(env, mm, monkeypatch)

    assert got.status_code != 200
    assert OTHER_MARC not in web.urls()
    assert CUDL_MANIFEST not in web.urls()
    assert _puzzle_files(env) == []
    assert mm.nli_cache[SYS]['marc'].get('external_iiif_link') is None
    assert env['failures'] == []
    assert SYS in mm._marc_fail_cache


def test_the_first_marc_lookup_does_not_follow_a_redirect_off_the_library_hosts(env, monkeypatch):
    """_fetch_single_worker (cold cache) and fetch_marc_data both read MARC_URL."""
    routes = _normal_routes()
    routes[MARC_URL] = _Resp(302, location=OTHER_MARC)
    routes[OTHER_MARC] = _Resp(200, _marc(shelfmark='Other shelfmark', fl='FL9999999'))
    web = _Web(routes)
    mm = _manager(env, web, monkeypatch, checked=True)

    got = _puzzle_ext_image(env, mm, monkeypatch)

    assert got.status_code != 200
    assert OTHER_MARC not in web.urls()
    assert _puzzle_files(env) == []
    assert mm.nli_cache[SYS].get('shelfmark') != 'Other shelfmark'
    assert 'FL9999999' not in (mm.nli_cache[SYS].get('fl_ids') or [])
    assert env['failures'] == []
    # A refused hop ends the retry loop as a 404 does: one request, no second attempt.
    assert web.urls().count(MARC_URL) == 2  # one per fetch (first lookup + MARC record)


def test_an_nli_manifest_redirect_off_the_library_hosts_is_not_followed(env, monkeypatch):
    routes = _normal_routes()
    routes[MANIFEST_URL] = _Resp(302, location=OTHER_MANIFEST)
    routes[OTHER_MANIFEST] = _Resp(200, json_body=_nli_manifest(), content_type='application/json')
    web = _Web(routes)
    mm = _manager(env, web, monkeypatch, checked=True)

    got = _puzzle_ext_image(env, mm, monkeypatch)

    assert got.status_code == 200  # the CUDL image, unaffected
    assert OTHER_MANIFEST not in web.urls()
    assert not mm.nli_cache[SYS].get('images_nli')
    assert OTHER_FL not in (mm.nli_cache[SYS].get('canvas_map') or {})
    assert env['failures'] == []
    assert (SYS, 1) in mm._iiif_manifest_fail_cache


def test_the_fl_id_lookup_does_not_follow_a_redirect_off_the_library_hosts(env, monkeypatch):
    """_fetch_fl_ids: the web never calls get_thumbnail today; the web's instance checks it anyway."""
    routes = {MARC_URL: _Resp(302, location=OTHER_MARC),
              OTHER_MARC: _Resp(200, _marc(fl=f'FL{OTHER_FL}'))}
    web = _Web(routes)
    mm = _manager(env, web, monkeypatch, checked=True)

    thumb = mm.get_thumbnail(SYS)

    assert thumb is None
    assert OTHER_MARC not in web.urls()
    assert mm.nli_cache[SYS]['fl_ids'] == []
    assert env['failures'] == []


# ── a refused hop leaves what a 404 leaves ─────────────────────────────────

def _run_site(mm, site):
    if site == 'manifest':
        return mm.fetch_iiif_manifest(SYS, 1)
    if site == 'marc':
        return mm.fetch_marc_data(SYS)
    if site == 'external':
        return mm.fetch_external_iiif_data(CUDL_VIEW)
    if site == 'first_lookup':
        return mm._fetch_single_worker(SYS)
    return mm._fetch_fl_ids(SYS)


_SITE_URL = {'manifest': MANIFEST_URL, 'marc': MARC_URL, 'external': CUDL_MANIFEST,
             'first_lookup': MARC_URL, 'fl_ids': MARC_URL}


def _state_after(env, monkeypatch, site, first):
    mm_mod = env['mm_mod']
    for cache in (mm_mod.MetadataManager._iiif_manifest_cache,
                  mm_mod.MetadataManager._iiif_manifest_fail_cache,
                  mm_mod.MetadataManager._marc_fail_cache):
        cache.clear()
    env['failures'].clear()
    web = _Web({_SITE_URL[site]: first, OTHER_MARC: _Resp(200, _marc()),
                OTHER_MANIFEST: _Resp(200, json_body=_nli_manifest(),
                                      content_type='application/json')})
    mm = _manager(env, web, monkeypatch, checked=True)
    result = _run_site(mm, site)
    return (result, dict(mm._iiif_manifest_fail_cache).keys(),
            dict(mm._marc_fail_cache).keys(), list(env['failures']),
            [u for u in web.urls() if 'example.org' in u])


@pytest.mark.parametrize('site', ['manifest', 'marc', 'external', 'first_lookup', 'fl_ids'])
def test_a_refused_hop_leaves_what_a_404_leaves(env, monkeypatch, site):
    other = OTHER_MANIFEST if site in ('manifest', 'external') else OTHER_MARC
    refused = _state_after(env, monkeypatch, site, _Resp(302, location=other))
    not_found = _state_after(env, monkeypatch, site, _Resp(404))

    assert refused[:4] == not_found[:4]
    assert refused[3] == []  # no breaker failure
    assert refused[4] == []  # the other host was never asked


# ── the puzzle page's folio list ───────────────────────────────────────────

def test_the_puzzle_folio_list_does_not_follow_a_manifest_redirect_off_the_library_hosts(
        env, monkeypatch):
    import web.pages.puzzle as puzzle_page
    from web.state import state

    web = _Web({MANIFEST_URL: _Resp(302, location=OTHER_MANIFEST),
                OTHER_MANIFEST: _Resp(200, json_body=_nli_manifest(),
                                      content_type='application/json')})
    monkeypatch.setattr(requests, 'get', web.get)
    monkeypatch.setattr(state, 'meta_mgr', None)
    page_failures = []
    monkeypatch.setattr(puzzle_page, '_nli_record_failure',
                        lambda failure_type, path: page_failures.append(failure_type))

    folios = puzzle_page._resolve_folios(SYS)

    assert folios == []
    assert web.urls() == [MANIFEST_URL]
    assert web.all_unfollowed()
    assert page_failures == []


def test_the_puzzle_folio_list_still_reads_an_nli_manifest(env, monkeypatch):
    import web.pages.puzzle as puzzle_page
    from web.state import state

    web = _Web({MANIFEST_URL: _Resp(200, json_body=_nli_manifest('1112223'),
                                    content_type='application/json')})
    monkeypatch.setattr(requests, 'get', web.get)
    monkeypatch.setattr(state, 'meta_mgr', None)

    folios = puzzle_page._resolve_folios(SYS)

    assert [f['fl_id'] for f in folios] == ['1112223']
    assert web.urls() == [MANIFEST_URL]
    assert web.all_unfollowed()


# ── the web builds its managers with the flag ──────────────────────────────

@pytest.mark.parametrize('path', ['web/main.py', 'shared/research_worker.py'])
def test_the_web_builds_its_metadata_manager_with_checked_fetches(path):
    tree = ast.parse((REPO / path).read_text(encoding='utf-8'))
    calls = [node for node in ast.walk(tree)
             if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
             and node.func.id == 'MetadataManager']
    assert calls, f'{path} builds no MetadataManager'
    for call in calls:
        flags = [kw.value for kw in call.keywords if kw.arg == 'checked_library_fetches']
        assert flags and isinstance(flags[0], ast.Constant) and flags[0].value is True, (
            f'{path}:{call.lineno} builds a MetadataManager without checked_library_fetches=True')


def test_the_class_default_is_the_desktop_behaviour():
    from shared.metadata_manager import MetadataManager
    assert MetadataManager.checked_library_fetches is False
    bare = MetadataManager.__new__(MetadataManager)  # tests and tools that skip __init__
    assert bare.checked_library_fetches is False


# ── the desktop: plain session.get, same arguments, redirects followed ─────

def test_the_desktop_fetches_call_session_get_with_the_v940_arguments(env, monkeypatch):
    import shared.puzzle_image_service as pis
    from shared.config import Config
    from shared.metadata_manager import EXTERNAL_IIIF_HTTP_TIMEOUT
    from shared.nli_circuit_breaker import (
        NLI_CONNECT_TIMEOUT, NLI_IIIF_READ_TIMEOUT, NLI_MARC_READ_TIMEOUT)

    def _no_checked(*a, **kw):
        raise AssertionError('the desktop must not use the checked-hop helper')

    monkeypatch.setattr(pis, 'get_with_checked_redirects', _no_checked)
    marc_other = _Resp(200, _marc(shelfmark='Moved shelfmark'))
    web = _Web({MARC_URL: _Resp(302, location=OTHER_MARC), OTHER_MARC: marc_other,
                MANIFEST_URL: _Resp(302, location=OTHER_MANIFEST),
                OTHER_MANIFEST: _Resp(200, json_body=_nli_manifest(),
                                      content_type='application/json'),
                CUDL_MANIFEST: _Resp(200, json_body=_cudl_manifest(),
                                     content_type='application/json')})
    mm = _manager(env, web, monkeypatch, checked=False)

    def _no_plain_get(*a, **kw):
        raise AssertionError('the desktop metadata fetches use the session, not requests.get')

    monkeypatch.setattr(requests, 'get', _no_plain_get)
    headers = Config.HTTP_HEADERS
    expected = [
        ('manifest', MANIFEST_URL, {'headers': headers,
                                    'timeout': (NLI_CONNECT_TIMEOUT, NLI_IIIF_READ_TIMEOUT),
                                    'verify': True}),
        ('marc', MARC_URL, {'headers': headers,
                            'timeout': (NLI_CONNECT_TIMEOUT, NLI_MARC_READ_TIMEOUT)}),
        ('external', CUDL_MANIFEST, {'timeout': EXTERNAL_IIIF_HTTP_TIMEOUT}),
        ('first_lookup', MARC_URL, {'headers': headers,
                                    'timeout': (NLI_CONNECT_TIMEOUT, NLI_MARC_READ_TIMEOUT)}),
    ]
    for site, url, kwargs in expected:
        web.calls.clear()
        _run_site(mm, site)
        assert web.calls[0] == (url, kwargs), site

    # _fetch_fl_ids passes allow_redirects=True itself, as it always did.
    web.calls.clear()
    mm._fetch_fl_ids(SYS)
    assert web.calls[0] == (MARC_URL, {
        'headers': headers, 'timeout': (NLI_CONNECT_TIMEOUT, NLI_MARC_READ_TIMEOUT),
        'allow_redirects': True})

    # ...and the desktop still follows a redirect, as at v9.4.0.
    web.calls.clear()
    assert mm.fetch_marc_data(SYS)['shelfmark_alt'] == 'Moved shelfmark'
    assert OTHER_MARC in web.urls()
