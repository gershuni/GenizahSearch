# -*- coding: utf-8 -*-
"""Phase 79 Plan 04 -- Test surface for GET /api/browse.

Mirrors Phase 78's tests/test_search_api.py fixture pattern (TestClient against
a bare FastAPI app, per-test fresh idempotency marker, mock searcher + state for
fast tests).

Coverage scope (D-24 + cross-AI review reflected updates):
  - Locator validation (8 tests): missing, conflicts, malformed uid, p_num bounds
  - Happy paths (4 tests): uid-only, p_num+volume_ie, p_num alone, fl_id
  - Enrichment failure modes (3 tests): timeout, exception, truncation
  - Rate-limit topology (2 tests): 429 envelope + independence from /api/search bucket
  - Mode gate (2 tests): disabled / localhost-only loopback success
  - Envelope shape (3 tests): error envelope shape, image picker per library, image.sources[] shape
  - Cross-phase integrity (2 tests): real-HTTP round-trip [R-PR-06] + serializer-direct unit
  - Real-core smoke (1 test, skipif) [R-PR-07]
  - Unit (3 tests): _parse_uid, _validate_locator/NormalizedLocator, decorator contract

R-PR-01 (D-14 reopened): NO test asserts image.url == null on proxy failure or
warnings: ['image_unavailable']. The replacement asserts image.url is emitted
unconditionally even when the proxy is unreachable.

R-PR-06: PRIMARY round-trip test does a real HTTP POST /api/search ->
GET /api/browse flow against TestClient. The serializer-direct shortcut is a
SEPARATE secondary unit test, NOT a fallback inside the round-trip test.

R-PR-07: at least one test exercises the REAL WebDataService core fetch shape
(post-R-PR-02 fix path), not the fully-mocked enriched-dict shape.
"""

from __future__ import annotations

import inspect
import json
import time

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.search_api import (
    init_search_api,
    BrowseRequest,
    NormalizedLocator,
    _rate_limiter,
    _browse_rate_limiter,
    _parse_uid,
    _validate_locator,
)
from web.api_hardening import RateLimiter  # noqa: F401 -- imported for type checks
from web.services import BrowsePage, get_service
from shared.api_errors import APIError, ERROR_CODES  # noqa: F401


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def bare_app():
    """Per-test bare app -- fresh idempotency marker mirrors test_search_api.py."""
    bare = FastAPI()
    init_search_api(app_override=bare)
    return bare


@pytest.fixture
def client(bare_app):
    return TestClient(bare_app)


@pytest.fixture
def clean_env(monkeypatch):
    monkeypatch.setenv('SEARCH_API_MODE', 'open')
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '30')
    monkeypatch.setenv('SEARCH_API_POSTHOG_SAMPLE_N', '999999')
    monkeypatch.setenv('SEARCH_API_BROWSE_TIMEOUT', '1.0')
    monkeypatch.setenv('SEARCH_API_BROWSE_CORE_TIMEOUT', '2.0')
    monkeypatch.setenv('SEARCH_API_BROWSE_TEXT_CAP', '4000')
    # Reset both buckets so prior-test state cannot pollute counters.
    _rate_limiter.reset_for_tests()
    _browse_rate_limiter.reset_for_tests()


def _make_browse_page(**overrides) -> BrowsePage:
    """Build a hydrated BrowsePage instance (post-R-PR-02 canonical shape)."""
    base = dict(
        uid='IE99_P3_FL12345',
        p_num=3,
        text='Synthetic Hebrew transcription text...',
        full_header='h_99001_IE99_P3_FL12345',
        total_pages=50,
        current_idx=2,
        sys_id='99001',
        fl_id='FL12345',
        shelfmark='T-S 99.99',
        title='Synthetic Title',
        library_code='CUL',
        library_name='Cambridge University Library',
        volume_ie='IE99',
        volumes=[{'ie_id': 'IE99', 'suffix': 1, 'page_count': 50}],
        folio_label='1r',
        folio_images=[],
        cambridge_images=[],
        physical_metadata=None,
        library_viewer_url=None,
        external_provider='',
    )
    base.update(overrides)
    return BrowsePage(**base)


@pytest.fixture
def mock_browse_page(monkeypatch):
    """Default: WebDataService returns a single multi-IE-but-1-volume page.
    R-PR-02 / R-PR-07: real BrowsePage instances, not synthetic dicts.
    """
    svc = get_service()
    page = _make_browse_page()
    monkeypatch.setattr(svc, 'get_browse_page', lambda *a, **k: page)
    monkeypatch.setattr(svc, 'get_browse_page_by_fl', lambda *a, **k: page)
    return page


@pytest.fixture
def silent_sidecars(monkeypatch):
    """Force PGP/FJMS/NLI sidecars to return None -- keeps bundles lean."""
    monkeypatch.setattr('shared.browse_service._pgp_sync', lambda *a, **k: None)
    monkeypatch.setattr('shared.browse_service._fjms_sync', lambda *a, **k: None)
    monkeypatch.setattr('shared.browse_service._nli_sync', lambda *a, **k: None)


def _has_test_fixture_data():
    """Detect whether the dev/CI environment has the full Tantivy index +
    csv_bank loaded for real-shape integration tests. R-PR-07 smoke test
    skips when this returns False so CI without the full data set still passes.
    """
    try:
        from web.state import state
        if not state or not getattr(state, 'searcher', None):
            return False
        return bool(getattr(state, 'meta_mgr', None))
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Route registration + decorator contract (R-PR-03)
# ---------------------------------------------------------------------------

def test_init_search_api_registers_browse_endpoint(bare_app):
    """Plan 03 Block C: GET /api/browse is registered on init_search_api."""
    paths = [getattr(r, 'path', None) for r in bare_app.routes]
    assert '/api/browse' in paths, f'expected /api/browse in routes; got {paths!r}'
    # Confirm GET method registered for it.
    browse_routes = [r for r in bare_app.routes if getattr(r, 'path', None) == '/api/browse']
    assert browse_routes, 'no /api/browse route found'
    methods = browse_routes[0].methods if hasattr(browse_routes[0], 'methods') else set()
    assert 'GET' in methods, f'expected GET method on /api/browse; got {methods!r}'


def test_browse_endpoint_uses_wrap_endpoint_decorator():
    """R-PR-03 contract test: @wrap_endpoint(endpoint_name='browse') applied."""
    import web.search_api as sapi
    src = inspect.getsource(sapi.init_search_api)
    # The decorator must appear adjacent to async def browse_endpoint.
    assert "@wrap_endpoint(endpoint_name='browse')" in src or \
           '@wrap_endpoint(endpoint_name="browse")' in src, (
        'browse_endpoint must be decorated with '
        "@wrap_endpoint(endpoint_name='browse'); decorator string not found in "
        'init_search_api source. R-PR-03 regression.'
    )
    # And make sure the decorator string sits before the def line.
    decorator_idx = src.find("@wrap_endpoint(endpoint_name='browse')")
    if decorator_idx < 0:
        decorator_idx = src.find('@wrap_endpoint(endpoint_name="browse")')
    def_idx = src.find('async def browse_endpoint')
    assert 0 <= decorator_idx < def_idx, (
        '@wrap_endpoint(endpoint_name=\'browse\') must precede async def browse_endpoint'
    )


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------

def test_browse_happy_path_uid(client, mock_browse_page, silent_sidecars, clean_env):
    """GET /api/browse?sys_id=...&uid=... returns 200 with browse envelope."""
    r = client.get('/api/browse?sys_id=99001&uid=IE99_P3_FL12345')
    assert r.status_code == 200, r.text
    body = r.json()
    assert body['source'] == 'browse'
    assert body['schema_version'] == 1
    assert body['page_indexing'] == '1-based'
    assert body['locator']['fl_id'] == 'FL12345'  # R-04 round-trip
    assert body['locator']['sys_id'] == '99001'
    assert body['locator']['p_num'] == 3


def test_browse_uid_only_path_resolves(client, monkeypatch, silent_sidecars, clean_env):
    """R-PR-04 contract test: uid-only path normalizes to {p_num, volume_ie, fl_id}
    and passes the parsed components (NOT uid) to WebDataService.

    When uid carries an FL component, _fetch_core routes via
    get_browse_page_by_fl (fl_id is the most specific pin). The R-PR-04
    contract is satisfied as long as the handler forwarded the parsed
    fl_id from the uid (rather than handing the raw uid string down)."""
    svc = get_service()
    captured_args = []

    def _fake_get_browse_page(sys_id, **kwargs):
        captured_args.append({'fn': 'get_browse_page', 'sys_id': sys_id, **kwargs})
        return _make_browse_page()

    def _fake_get_browse_page_by_fl(fl_id, **kwargs):
        captured_args.append({'fn': 'get_browse_page_by_fl', 'fl_id': fl_id, **kwargs})
        return _make_browse_page()

    monkeypatch.setattr(svc, 'get_browse_page', _fake_get_browse_page)
    monkeypatch.setattr(svc, 'get_browse_page_by_fl', _fake_get_browse_page_by_fl)

    r = client.get('/api/browse?sys_id=99001&uid=IE99_P3_FL12345')
    assert r.status_code == 200, r.text
    assert captured_args, 'no WebDataService browse method was called'
    call = captured_args[0]
    # R-PR-04: handler must pass parsed components (fl_id='FL12345') derived
    # from uid, NOT the raw uid string.
    if call['fn'] == 'get_browse_page_by_fl':
        assert call.get('fl_id') == 'FL12345', (
            f"expected fl_id='FL12345' parsed from uid, got {call!r}. R-PR-04 regression."
        )
        assert call.get('sys_id') == '99001'
    else:
        assert call.get('p_num') == 3, (
            f'expected p_num=3 normalized from uid, got {call!r}. R-PR-04 regression.'
        )
        assert call.get('volume_ie') == 'IE99', (
            f"expected volume_ie='IE99' normalized from uid, got {call!r}"
        )


def test_browse_happy_path_p_num_and_volume_ie(
    client, mock_browse_page, silent_sidecars, clean_env,
):
    r = client.get('/api/browse?sys_id=99001&p_num=3&volume_ie=IE99')
    assert r.status_code == 200, r.text
    body = r.json()
    assert body['locator']['sys_id'] == '99001'
    assert body['locator']['p_num'] == 3
    assert body['locator']['volume_ie'] == 'IE99'


def test_browse_happy_path_p_num_alone(
    client, mock_browse_page, silent_sidecars, clean_env,
):
    """Single-IE manuscripts can be browsed via p_num alone (no volume_ie)."""
    r = client.get('/api/browse?sys_id=99001&p_num=3')
    assert r.status_code == 200, r.text
    body = r.json()
    # Single volume in mock_browse_page -> no volume_ie_defaulted warning.
    warnings = body.get('warnings') or []
    assert not any(
        (isinstance(w, dict) and w.get('code') == 'volume_ie_defaulted')
        or (isinstance(w, str) and 'volume_ie_defaulted' in w)
        for w in warnings
    ), f'unexpected volume_ie_defaulted warning on single-volume page: {warnings!r}'


def test_browse_happy_path_fl_id(client, monkeypatch, silent_sidecars, clean_env):
    """fl_id-only path routes through WebDataService.get_browse_page_by_fl."""
    svc = get_service()
    by_fl_calls = []
    by_p_calls = []

    def _fake_by_fl(fl_id, sys_id=None):
        by_fl_calls.append({'fl_id': fl_id, 'sys_id': sys_id})
        return _make_browse_page()

    def _fake_get_browse_page(sys_id, **kwargs):
        by_p_calls.append({'sys_id': sys_id, **kwargs})
        return _make_browse_page()

    monkeypatch.setattr(svc, 'get_browse_page_by_fl', _fake_by_fl)
    monkeypatch.setattr(svc, 'get_browse_page', _fake_get_browse_page)

    r = client.get('/api/browse?sys_id=99001&fl_id=FL12345')
    assert r.status_code == 200, r.text
    assert by_fl_calls, 'expected get_browse_page_by_fl to be called for fl_id locator'
    assert not by_p_calls, (
        f'get_browse_page should NOT have been called when fl_id present; got {by_p_calls!r}'
    )


# ---------------------------------------------------------------------------
# Locator validation
# ---------------------------------------------------------------------------

def test_browse_missing_locator(client, mock_browse_page, clean_env):
    """No uid/p_num/fl_id -> 400 invalid_request."""
    r = client.get('/api/browse?sys_id=99001')
    assert r.status_code == 400, r.text
    assert r.json()['error']['code'] == 'invalid_request'


def test_browse_missing_sys_id(client, clean_env):
    r = client.get('/api/browse?uid=IE99_P3_FL12345')
    assert r.status_code == 400, r.text
    assert r.json()['error']['code'] == 'invalid_request'


def test_browse_unknown_query_param(client, clean_env):
    """Pydantic extra='forbid' -> invalid_request."""
    r = client.get('/api/browse?sys_id=99001&p_num=1&unknown_field=x')
    assert r.status_code == 400, r.text
    assert r.json()['error']['code'] == 'invalid_request'


def test_browse_locator_conflict_uid_volume_ie(
    client, mock_browse_page, clean_env,
):
    r = client.get('/api/browse?sys_id=99001&uid=IE99_P3_FL12345&volume_ie=IE100')
    assert r.status_code == 400, r.text
    body = r.json()
    assert body['error']['code'] == 'locator_conflict'
    assert 'volume_ie' in body['error']['message']


def test_browse_locator_conflict_uid_p_num(client, mock_browse_page, clean_env):
    r = client.get('/api/browse?sys_id=99001&uid=IE99_P3_FL12345&p_num=99')
    assert r.status_code == 400, r.text
    assert r.json()['error']['code'] == 'locator_conflict'


def test_browse_locator_conflict_uid_fl_id(client, mock_browse_page, clean_env):
    r = client.get('/api/browse?sys_id=99001&uid=IE99_P3_FL12345&fl_id=FL999')
    assert r.status_code == 400, r.text
    assert r.json()['error']['code'] == 'locator_conflict'


def test_browse_locator_conflict_malformed_uid(client, mock_browse_page, clean_env):
    r = client.get('/api/browse?sys_id=99001&uid=garbage')
    assert r.status_code == 400, r.text
    assert r.json()['error']['code'] == 'locator_conflict'


def test_browse_p_num_must_be_positive(client, mock_browse_page, clean_env):
    r = client.get('/api/browse?sys_id=99001&p_num=0')
    assert r.status_code == 400, r.text
    assert r.json()['error']['code'] == 'invalid_request'

    r = client.get('/api/browse?sys_id=99001&p_num=-1')
    assert r.status_code == 400, r.text
    assert r.json()['error']['code'] == 'invalid_request'


def test_browse_p_num_must_be_int(client, mock_browse_page, clean_env):
    r = client.get('/api/browse?sys_id=99001&p_num=abc')
    assert r.status_code == 400, r.text
    body = r.json()
    assert body['error']['code'] == 'invalid_request'
    assert 'p_num' in body['error']['message']


def test_browse_text_cap_bounds(
    client, mock_browse_page, silent_sidecars, clean_env,
):
    # too low
    r = client.get('/api/browse?sys_id=99001&p_num=1&text_cap=50')
    assert r.status_code == 400, r.text
    # too high
    r = client.get('/api/browse?sys_id=99001&p_num=1&text_cap=99999')
    assert r.status_code == 400, r.text
    # valid
    r = client.get('/api/browse?sys_id=99001&p_num=1&text_cap=4000')
    assert r.status_code == 200, r.text


# ---------------------------------------------------------------------------
# Multi-IE default warning + manuscript-not-found + post-resolution mismatch
# ---------------------------------------------------------------------------

def test_browse_multi_ie_default_warning(
    client, monkeypatch, silent_sidecars, clean_env,
):
    """sys_id+p_num on a multi-IE manuscript -> warnings: volume_ie_defaulted."""
    svc = get_service()
    multi_page = _make_browse_page(
        volume_ie='IE99',
        volumes=[
            {'ie_id': 'IE99', 'suffix': 1, 'page_count': 50},
            {'ie_id': 'IE100', 'suffix': 2, 'page_count': 30},
        ],
    )
    monkeypatch.setattr(svc, 'get_browse_page', lambda *a, **k: multi_page)
    monkeypatch.setattr(svc, 'get_browse_page_by_fl', lambda *a, **k: multi_page)

    r = client.get('/api/browse?sys_id=99001&p_num=3')
    assert r.status_code == 200, r.text
    body = r.json()
    warnings = body.get('warnings') or []
    found = any(
        (isinstance(w, dict) and w.get('code') == 'volume_ie_defaulted')
        or (isinstance(w, str) and 'volume_ie_defaulted' in w)
        for w in warnings
    )
    assert found, f'expected volume_ie_defaulted warning; got {warnings!r}'


def test_browse_manuscript_not_found_returns_404(
    client, monkeypatch, clean_env,
):
    """WebDataService returns None -> 404 manuscript_page_not_found."""
    svc = get_service()
    monkeypatch.setattr(svc, 'get_browse_page', lambda *a, **k: None)
    monkeypatch.setattr(svc, 'get_browse_page_by_fl', lambda *a, **k: None)

    r = client.get('/api/browse?sys_id=000000&p_num=1')
    assert r.status_code == 404, r.text
    assert r.json()['error']['code'] == 'manuscript_page_not_found'


def test_browse_uid_post_resolution_mismatch_returns_404(
    client, monkeypatch, silent_sidecars, clean_env,
):
    """D-03b/R-03: resolved page.uid != requested uid -> 404."""
    svc = get_service()
    page_with_different_uid = _make_browse_page(uid='IE100_P3_FL999')
    monkeypatch.setattr(svc, 'get_browse_page', lambda *a, **k: page_with_different_uid)
    monkeypatch.setattr(svc, 'get_browse_page_by_fl', lambda *a, **k: page_with_different_uid)

    # Use uid-only request so _validate_locator accepts (no other locator components
    # to conflict-check); the post-resolution check is the only line of defense.
    r = client.get('/api/browse?sys_id=99001&uid=IE99_P3_FL12345')
    assert r.status_code == 404, r.text
    body = r.json()
    assert body['error']['code'] == 'manuscript_page_not_found'
    assert 'resolved to different page' in body['error']['message'].lower() or \
           'different' in body['error']['message'].lower()


# ---------------------------------------------------------------------------
# Image picker (D-12, D-13) -- R-PR-01 reflected
# ---------------------------------------------------------------------------

def test_browse_image_url_picker_per_library(
    client, monkeypatch, silent_sidecars, clean_env,
):
    """D-12 library-aware image.url picker."""
    svc = get_service()
    cases = [
        ('CUL',        '/api/cambridge_image/'),
        ('Manchester', '/api/manchester_image/'),
        ('JTS',        '/api/jts_image/'),
        ('Oxford',     '/api/oxford_image/'),
        ('BL',         '/api/nli_image_by_sysid/'),  # default fallback
    ]
    for library_code, expected_prefix in cases:
        page = _make_browse_page(library_code=library_code)
        monkeypatch.setattr(svc, 'get_browse_page', lambda *a, p=page, **k: p)
        monkeypatch.setattr(svc, 'get_browse_page_by_fl', lambda *a, p=page, **k: p)
        r = client.get('/api/browse?sys_id=99001&p_num=3')
        assert r.status_code == 200, f'{library_code}: {r.text}'
        body = r.json()
        url = body['image']['url']
        assert url and url.startswith(expected_prefix), (
            f'{library_code}: expected prefix {expected_prefix!r}, got {url!r}'
        )


def test_browse_image_emitted_unconditionally(
    client, mock_browse_page, silent_sidecars, clean_env,
):
    """R-PR-01 / D-14 reopened: image.url is emitted unconditionally.

    Even when a real upstream proxy would have failed, the response MUST emit
    image.url (non-null) and MUST NOT emit an image_unavailable warning.

    This is the REPLACEMENT for the previous
    test_browse_image_unavailable_returns_null_url_with_warning test (which
    asserted the opposite -- the old contract is gone with D-14 reopened).
    """
    r = client.get('/api/browse?sys_id=99001&p_num=3')
    assert r.status_code == 200, r.text
    body = r.json()
    url = body['image']['url']
    assert url, 'image.url must be emitted unconditionally per R-PR-01'
    assert isinstance(url, str) and url.startswith('/api/cambridge_image/'), (
        f'expected /api/cambridge_image/* URL for CUL library_code; got {url!r}'
    )
    # Negative assertion: image_unavailable warning is NOT emitted.
    warnings = body.get('warnings') or []
    for w in warnings:
        s = str(w) if not isinstance(w, dict) else json.dumps(w)
        assert 'image_unavailable' not in s, (
            f'image_unavailable warning leaked despite R-PR-01: {warnings!r}'
        )
    # image.sources must be a list (may be empty per R-06; with a CUL page
    # that has a valid library-aware URL, sources contains the iiif_proxy entry).
    assert isinstance(body['image']['sources'], list)


def test_browse_image_sources_shape(
    client, monkeypatch, silent_sidecars, clean_env,
):
    """Each entry in image.sources[] has keys per D-13 / R-05."""
    svc = get_service()
    page = _make_browse_page(
        cambridge_images=[
            {'url': 'https://example.com/img1.jpg', 'fl_id': 'FL12345', 'folio_label': '1r'},
        ],
        library_viewer_url={
            'url': 'https://cudl.example.com/manuscript/1',
            'library_abbrev': 'CUDL',
            'label': 'CUDL',
        },
    )
    monkeypatch.setattr(svc, 'get_browse_page', lambda *a, **k: page)
    monkeypatch.setattr(svc, 'get_browse_page_by_fl', lambda *a, **k: page)

    r = client.get('/api/browse?sys_id=99001&p_num=3')
    assert r.status_code == 200, r.text
    sources = r.json()['image']['sources']
    assert len(sources) >= 2, f'expected >=2 entries (companion + viewer); got {sources!r}'
    expected_keys = {'url', 'provider', 'role', 'kind', 'fl_id', 'folio_label'}
    roles = set()
    kinds_per_role = {}
    for entry in sources:
        assert expected_keys.issubset(set(entry.keys())), (
            f'entry missing required keys: {entry!r}'
        )
        roles.add(entry['role'])
        kinds_per_role.setdefault(entry['role'], set()).add(entry['kind'])
    # iiif_proxy + companion_folio + external_viewer (or at least 2 of these).
    assert 'companion_folio' in roles, f'expected companion_folio role; got {roles!r}'
    assert 'external_viewer' in roles, f'expected external_viewer role; got {roles!r}'
    # external_viewer entry has kind='viewer'; companion_folio has kind='image'.
    if 'external_viewer' in kinds_per_role:
        assert 'viewer' in kinds_per_role['external_viewer']
    if 'companion_folio' in kinds_per_role:
        assert 'image' in kinds_per_role['companion_folio']


# ---------------------------------------------------------------------------
# Enrichment failure modes
# ---------------------------------------------------------------------------

def test_browse_transcription_truncation_warning(
    client, monkeypatch, mock_browse_page, clean_env,
):
    """PGP page-section text > text_cap -> text_truncated + warning."""
    long_text = 'word ' * 2000
    monkeypatch.setattr(
        'shared.browse_service._pgp_sync',
        lambda *a, **k: {
            'description': 'x', 'tags': [], 'document_type': None,
            'languages_primary': [], 'languages_secondary': [],
            'doc_date_original': None, 'doc_date_standard': None,
            'inferred_date_display': None, 'pgpid': None, 'pgp_url': None,
            'page_section_text': long_text,
        },
    )
    monkeypatch.setattr('shared.browse_service._fjms_sync', lambda *a, **k: None)
    monkeypatch.setattr('shared.browse_service._nli_sync', lambda *a, **k: None)

    r = client.get('/api/browse?sys_id=99001&p_num=3&text_cap=200')
    assert r.status_code == 200, r.text
    body = r.json()
    assert body['text_truncated'] is True
    assert body['text'].endswith('…'), f"expected ellipsis tail; got {body['text'][-10:]!r}"
    warnings = body.get('warnings') or []
    found = any(
        (isinstance(w, dict) and w.get('code') == 'transcription_truncated')
        or (isinstance(w, str) and 'transcription_truncated' in w)
        for w in warnings
    )
    assert found, f'expected transcription_truncated warning; got {warnings!r}'


def test_browse_enrichment_timeout_warning(
    client, monkeypatch, mock_browse_page, clean_env,
):
    """PGP sync sleeps longer than SEARCH_API_BROWSE_TIMEOUT -> enrichment_timeout."""
    monkeypatch.setenv('SEARCH_API_BROWSE_TIMEOUT', '0.05')

    def _slow(*a, **k):
        time.sleep(2)
        return None

    monkeypatch.setattr('shared.browse_service._pgp_sync', _slow)
    monkeypatch.setattr('shared.browse_service._fjms_sync', lambda *a, **k: None)
    monkeypatch.setattr('shared.browse_service._nli_sync', lambda *a, **k: None)

    r = client.get('/api/browse?sys_id=99001&p_num=3')
    assert r.status_code == 200, r.text
    body = r.json()
    assert body['metadata']['pgp'] is None
    warnings = body.get('warnings') or []
    found = any(
        isinstance(w, dict)
        and w.get('code') == 'enrichment_timeout'
        and w.get('source') == 'pgp'
        for w in warnings
    )
    assert found, f'expected enrichment_timeout/pgp warning; got {warnings!r}'


def test_browse_enrichment_exception_warning(
    client, monkeypatch, mock_browse_page, clean_env,
):
    """R-PR-05 contract: inner sync helper raises -> enrichment_failed warning."""

    def _boom(*a, **k):
        raise RuntimeError('boom')

    monkeypatch.setattr('shared.browse_service._pgp_sync', lambda *a, **k: None)
    monkeypatch.setattr('shared.browse_service._fjms_sync', _boom)
    monkeypatch.setattr('shared.browse_service._nli_sync', lambda *a, **k: None)

    r = client.get('/api/browse?sys_id=99001&p_num=3')
    assert r.status_code == 200, r.text
    body = r.json()
    assert body['metadata']['fjms'] is None
    warnings = body.get('warnings') or []
    found = any(
        isinstance(w, dict)
        and w.get('code') == 'enrichment_failed'
        and w.get('source') == 'fjms'
        for w in warnings
    )
    assert found, f'expected enrichment_failed/fjms warning; got {warnings!r}'


def test_browse_core_timeout_returns_504(
    client, monkeypatch, clean_env,
):
    """WebDataService.get_browse_page sleeps -> 504 core_timeout."""
    monkeypatch.setenv('SEARCH_API_BROWSE_CORE_TIMEOUT', '0.05')

    def _slow(*a, **k):
        time.sleep(2)
        return _make_browse_page()

    svc = get_service()
    monkeypatch.setattr(svc, 'get_browse_page', _slow)
    monkeypatch.setattr(svc, 'get_browse_page_by_fl', _slow)

    r = client.get('/api/browse?sys_id=99001&p_num=1')
    assert r.status_code == 504, r.text
    assert r.json()['error']['code'] == 'core_timeout'


# ---------------------------------------------------------------------------
# Statelessness
# ---------------------------------------------------------------------------

def test_browse_statelessness_repeat_request(
    client, mock_browse_page, silent_sidecars, clean_env,
):
    """Two identical requests -> bodies differ ONLY in generated_at."""
    r1 = client.get('/api/browse?sys_id=99001&p_num=3')
    r2 = client.get('/api/browse?sys_id=99001&p_num=3')
    assert r1.status_code == 200 and r2.status_code == 200
    j1, j2 = r1.json(), r2.json()
    j1.pop('generated_at', None)
    j2.pop('generated_at', None)
    assert json.dumps(j1, sort_keys=True) == json.dumps(j2, sort_keys=True), (
        'identical browse requests must produce byte-identical responses '
        '(modulo generated_at)'
    )


# ---------------------------------------------------------------------------
# Rate limit + mode gate
# ---------------------------------------------------------------------------

def test_browse_rate_limit_returns_429_with_retry_after(
    client, mock_browse_page, silent_sidecars, monkeypatch,
):
    """Burst beyond SEARCH_API_RATE_LIMIT returns 429 with Retry-After."""
    monkeypatch.setenv('SEARCH_API_MODE', 'open')
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '3')
    _rate_limiter.reset_for_tests()
    _browse_rate_limiter.reset_for_tests()

    statuses = []
    for _ in range(10):
        statuses.append(client.get('/api/browse?sys_id=99001&p_num=3'))
    rate_limited = [r for r in statuses if r.status_code == 429]
    assert rate_limited, f'expected at least one 429; got {[r.status_code for r in statuses]!r}'
    r = rate_limited[0]
    assert r.json()['error']['code'] == 'rate_limited'
    ra = r.headers.get('Retry-After') or r.headers.get('retry-after')
    assert ra is not None and int(ra) >= 1, f'Retry-After missing/invalid: {ra!r}'


def test_browse_rate_limit_independent_from_search(
    bare_app, mock_browse_page, silent_sidecars, monkeypatch,
):
    """D-18: separate per-IP buckets per endpoint.

    Burst 31 requests on /api/browse exhausts that bucket; a follow-up
    POST /api/search MUST succeed (separate bucket).
    """
    monkeypatch.setenv('SEARCH_API_MODE', 'open')
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '3')
    monkeypatch.setenv('SEARCH_API_POSTHOG_SAMPLE_N', '999999')
    _rate_limiter.reset_for_tests()
    _browse_rate_limiter.reset_for_tests()

    # Stub the searcher so /api/search returns 200 with empty results.
    from web.state import state
    saved_searcher = state.searcher
    saved_meta = state.meta_mgr
    from unittest.mock import MagicMock
    fake_searcher = MagicMock()
    fake_searcher.execute_search.return_value = []
    fake_meta = MagicMock()
    fake_meta.get_meta_for_id.return_value = ('T-S 99.99', 'Synthetic')
    fake_meta.get_library_for_id.return_value = 'CUL'
    fake_meta.parse_full_id_components.return_value = {
        'sys_id': '99001', 'ie_id': 'IE99', 'p_num': '3', 'fl_id': None,
    }
    state.searcher = fake_searcher
    state.meta_mgr = fake_meta
    try:
        with TestClient(bare_app) as c:
            # Exhaust the BROWSE bucket.
            statuses = [c.get('/api/browse?sys_id=99001&p_num=3') for _ in range(6)]
            rate_limited = [r for r in statuses if r.status_code == 429]
            assert rate_limited, (
                f'browse bucket should have been exhausted; got '
                f'{[r.status_code for r in statuses]!r}'
            )
            # SEARCH bucket is independent -- this MUST succeed (NOT 429).
            r = c.post('/api/search', json={'query': 'x', 'mode': 'text'})
        assert r.status_code != 429, (
            f'search bucket was incorrectly exhausted by browse traffic '
            f'(D-18 regression); got status {r.status_code} body={r.text[:200]!r}'
        )
        # Should be a successful 200 (empty results from mocked searcher).
        assert r.status_code == 200, r.text
    finally:
        state.searcher = saved_searcher
        state.meta_mgr = saved_meta


def test_browse_disabled_mode_returns_503(
    client, mock_browse_page, monkeypatch,
):
    monkeypatch.setenv('SEARCH_API_MODE', 'disabled')
    _browse_rate_limiter.reset_for_tests()
    r = client.get('/api/browse?sys_id=99001&p_num=3')
    assert r.status_code == 503, r.text
    assert r.json()['error']['code'] == 'disabled'


def test_browse_localhost_only_mode_with_loopback_succeeds(
    client, mock_browse_page, silent_sidecars, monkeypatch,
):
    monkeypatch.setenv('SEARCH_API_MODE', 'localhost-only')
    monkeypatch.setattr('web.api_hardening._is_loopback_request', lambda req: True)
    _browse_rate_limiter.reset_for_tests()
    r = client.get('/api/browse?sys_id=99001&p_num=3')
    assert r.status_code == 200, r.text


# ---------------------------------------------------------------------------
# Error envelope shape
# ---------------------------------------------------------------------------

def test_browse_error_envelope_shape(client, mock_browse_page, clean_env):
    """Every non-2xx response is {error:{code,message}}."""
    r = client.get('/api/browse?sys_id=99001')  # missing locator
    assert r.status_code == 400, r.text
    body = r.json()
    assert 'error' in body
    assert isinstance(body['error'], dict)
    assert 'code' in body['error']
    assert 'message' in body['error']
    # Must NOT be the FastAPI raw 422 envelope at the top level (legacy shape).
    assert 'detail' not in body or 'error' in body


# ---------------------------------------------------------------------------
# Cross-phase integrity: locator round-trip search -> browse
# ---------------------------------------------------------------------------

def test_browse_locator_round_trip_real_http(
    client, monkeypatch, silent_sidecars, clean_env,
):
    """R-PR-06 PRIMARY round-trip test: real HTTP POST /api/search ->
    GET /api/browse flow against TestClient.

    No serializer-direct fallback inside this test. If the Phase 78 search
    pipeline cannot be driven through TestClient with the synthetic result,
    the test FAILS -- the round-trip is the contract.
    """
    from web.state import state
    saved_searcher = state.searcher
    saved_meta = state.meta_mgr
    from unittest.mock import MagicMock

    # Synthetic search result row, shape matching what serialize_search_payload expects.
    fake_searcher = MagicMock()
    fake_searcher.execute_search.return_value = [{
        'uid': 'IE99_P3_FL12345',
        'display': {
            'shelfmark': 'T-S 99.99', 'title': 'Synthetic Title',
            'id': '99001', 'library_code': 'CUL',
        },
        'raw_header': 'h_99001_IE99_P3_FL12345',
        'snippet': 'a *match* here',
        'full_text': 'lorem ipsum',
        'sort_score': 0.5,
    }]
    fake_meta = MagicMock()
    fake_meta.get_meta_for_id.return_value = ('T-S 99.99', 'Synthetic Title')
    fake_meta.get_library_for_id.return_value = 'CUL'
    fake_meta.parse_full_id_components.return_value = {
        'sys_id': '99001', 'ie_id': 'IE99', 'p_num': '3', 'fl_id': None,
    }
    state.searcher = fake_searcher
    state.meta_mgr = fake_meta

    # WebDataService also returns a hydrated BrowsePage matching the locator.
    svc = get_service()
    monkeypatch.setattr(svc, 'get_browse_page', lambda *a, **k: _make_browse_page())
    monkeypatch.setattr(svc, 'get_browse_page_by_fl', lambda *a, **k: _make_browse_page())

    try:
        # Step 1: real HTTP POST /api/search.
        r = client.post('/api/search', json={'query': 'foo', 'mode': 'text'})
        assert r.status_code == 200, r.text
        body = r.json()
        items = body.get('results', [])
        assert items, 'POST /api/search returned no items; round-trip cannot proceed'
        item = items[0]
        locator = item.get('locator') or {}
        assert locator.get('sys_id'), f'item.locator missing sys_id: {item!r}'
        # Search-side locator carries sys_id+volume_ie+p_num; uid is at item['uid'].
        item_uid = item.get('uid')
        assert item_uid or locator.get('p_num'), 'no uid or p_num to drive browse'

        # Step 2: build /api/browse query string from the locator.
        sys_id = locator['sys_id']
        if item_uid:
            url = f'/api/browse?sys_id={sys_id}&uid={item_uid}'
        elif locator.get('volume_ie'):
            url = (
                f"/api/browse?sys_id={sys_id}"
                f"&p_num={locator['p_num']}&volume_ie={locator['volume_ie']}"
            )
        else:
            url = f"/api/browse?sys_id={sys_id}&p_num={locator['p_num']}"

        # Step 3: real HTTP GET /api/browse.
        r2 = client.get(url)
        assert r2.status_code == 200, r2.text
        body2 = r2.json()
        assert body2.get('shelfmark'), f'browse missing shelfmark: {body2!r}'
        assert body2['locator']['sys_id'], 'browse locator missing sys_id'
        assert body2['locator']['fl_id'] is not None, (
            'R-04: browse locator MUST echo fl_id (round-trip preservation)'
        )

        # Step 4: multi-IE round-trip.
        multi_page = _make_browse_page(
            volume_ie='IE100',
            uid='IE100_P3_FL55555',
            volumes=[
                {'ie_id': 'IE99', 'suffix': 1, 'page_count': 50},
                {'ie_id': 'IE100', 'suffix': 2, 'page_count': 30},
            ],
        )
        monkeypatch.setattr(svc, 'get_browse_page', lambda *a, **k: multi_page)
        monkeypatch.setattr(svc, 'get_browse_page_by_fl', lambda *a, **k: multi_page)
        r3 = client.get('/api/browse?sys_id=99001&uid=IE100_P3_FL55555')
        assert r3.status_code == 200, r3.text
        body3 = r3.json()
        assert body3['locator']['volume_ie'] == 'IE100'
    finally:
        state.searcher = saved_searcher
        state.meta_mgr = saved_meta


def test_browse_locator_round_trip_serializer_unit(
    client, mock_browse_page, silent_sidecars, clean_env,
):
    """R-PR-06 SECONDARY unit test: serializer-direct locator extraction.

    Constructs a synthetic search-serializer envelope via direct call;
    extracts a locator; uses it to GET /api/browse. This is preserved as a
    separate unit test for the locator-extraction path -- it does NOT
    replace or fall back from the round-trip test above.
    """
    from shared.search_serializer import serialize_search_payload
    from unittest.mock import MagicMock

    fake_meta = MagicMock()
    fake_meta.parse_full_id_components.return_value = {
        'sys_id': '99001', 'ie_id': 'IE99', 'p_num': '3', 'fl_id': None,
    }
    results = [{
        'uid': 'IE99_P3_FL12345',
        'display': {
            'shelfmark': 'T-S 99.99', 'title': 'Synthetic',
            'id': '99001', 'library_code': 'CUL',
        },
        'raw_header': 'h_99001_IE99_P3_FL12345',
        'snippet': 's', 'full_text': 't', 'sort_score': 0.1,
    }]
    envelope = serialize_search_payload(
        results, meta_mgr=fake_meta, query='x', mode='text',
    )
    assert envelope['source'] == 'search'
    assert envelope['results'], 'serializer produced no results'
    item = envelope['results'][0]
    locator = item['locator']
    sys_id = locator['sys_id']
    assert sys_id

    # Drive /api/browse from the extracted locator (uid is at item['uid']).
    item_uid = item.get('uid')
    url = f'/api/browse?sys_id={sys_id}&uid={item_uid}' if item_uid \
          else f'/api/browse?sys_id={sys_id}&p_num={locator["p_num"]}'
    r = client.get(url)
    assert r.status_code == 200, r.text


@pytest.mark.skipif(
    not _has_test_fixture_data(),
    reason='requires real csv_bank/Tantivy index (R-PR-07 smoke test)',
)
def test_browse_real_core_shape_smoke():
    """R-PR-07 integration smoke test.

    Constructs a fresh FastAPI app + init_search_api. Does NOT mock
    WebDataService at all. Picks a known fixture sys_id from production
    csv_bank. This is the test that fails if R-PR-02 regresses (someone
    reverts _fetch_core to call the raw core resolver and the resulting
    minimal-dict shape breaks serializer attribute access).

    Skipif when fixture data unavailable -- runs on dev machines, skipped
    on minimal CI.
    """
    bare = FastAPI()
    init_search_api(app_override=bare)
    with TestClient(bare) as c:
        # Use a known sys_id from production csv_bank. '1' is a safe choice
        # because libraries.csv contains 217K records starting at sys_id=1.
        # If '1' doesn't resolve, pick the first sys_id from state.meta_mgr.
        from web.state import state
        candidate_sys_id = '1'
        try:
            mgr = state.meta_mgr
            if mgr and hasattr(mgr, 'csv_bank'):
                rows = list(mgr.csv_bank.values())[:5] if mgr.csv_bank else []
                if rows:
                    first = rows[0]
                    cand = first.get('id') if isinstance(first, dict) else None
                    if cand:
                        candidate_sys_id = str(cand)
        except Exception:
            pass

        r = c.get(f'/api/browse?sys_id={candidate_sys_id}&p_num=1')
        # Either 200 (resolved) or 404 (manuscript_page_not_found).
        # 504 (core_timeout) is also acceptable on a slow dev machine.
        # The test FAILS only on 500 (which is the R-PR-02 regression: serializer
        # tried to read attributes off the raw core resolver's minimal dict).
        assert r.status_code in (200, 404, 504), r.text
        if r.status_code == 200:
            body = r.json()
            # Verify hydrated shape is preserved.
            assert isinstance(body.get('shelfmark'), str)
            assert isinstance(body.get('library', {}).get('code'), str)


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------

def test_parse_uid_unit():
    """Pure unit test for _parse_uid -- valid, malformed, empty cases."""
    # Valid.
    parsed = _parse_uid('IE99_P3_FL12345')
    assert parsed is not None
    assert parsed == {'volume_ie': 'IE99', 'p_num': 3, 'fl_id': 'FL12345'}

    # Malformed.
    assert _parse_uid('garbage') is None
    assert _parse_uid('IE99_FL12345') is None  # missing P
    assert _parse_uid('IE99_P0_FL12345') is None  # p_num must be >=1

    # Empty / None.
    assert _parse_uid('') is None
    assert _parse_uid(None) is None  # type: ignore[arg-type]


def test_validate_locator_returns_normalized_locator(clean_env):
    """R-PR-04 unit test: _validate_locator returns NormalizedLocator with
    effective_* fields normalized from uid (when present)."""
    # uid path -- effective fields derived from parsing.
    req = BrowseRequest(sys_id='99001', uid='IE99_P3_FL12345')
    loc = _validate_locator(req)
    assert isinstance(loc, NormalizedLocator)
    assert loc.effective_p_num == 3
    assert loc.effective_volume_ie == 'IE99'
    assert loc.effective_fl_id == 'FL12345'
    assert loc.requested_uid == 'IE99_P3_FL12345'

    # No-uid path -- effective fields mirror request.
    req2 = BrowseRequest(sys_id='99001', p_num=5, volume_ie='IE100')
    loc2 = _validate_locator(req2)
    assert isinstance(loc2, NormalizedLocator)
    assert loc2.effective_p_num == 5
    assert loc2.effective_volume_ie == 'IE100'
    assert loc2.requested_uid is None
