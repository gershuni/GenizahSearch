# -*- coding: utf-8 -*-
"""Tests for GET /api/capabilities (D3/D4, 2026-09-08 external MCP-server incident fix).

Mirrors the fixture style of tests/test_browse_api.py and tests/test_parallels_api.py:
a bare FastAPI app + TestClient via init_search_api(app_override=...), per-test
idempotency marker, rate-limiter reset, and the same monkeypatch-the-module-attribute
pattern tests/test_parallels_api.py uses to flip `web.passage_assets.passage_available`
(the passage gate reads its flag ONCE at import per CLAUDE.md, but the capabilities
handler -- like the parallels handler -- imports `passage_available` fresh INSIDE the
request, so monkeypatching the module attribute is sufficient; no importlib.reload
needed).

Pinned contract (see the orchestrator task): exact top-level keys, D4 fail-closed
posture (a closed passage gate must remove 'passage' from parallels.methods entirely,
not merely flag it unavailable), and every limit/timeout value must be read from the
live resolver, never a hardcoded literal (this is what "the header said 300s" (P1) was
in miniature -- the whole point of this endpoint is to never drift like that again).
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.search_api import (
    init_search_api,
    DEFAULT_FUZZY_TIMEOUT,
    DEFAULT_PARALLELS_TIMEOUT,
    _rate_limiter,
    _browse_rate_limiter,
    _parallels_rate_limiter,
    _capabilities_rate_limiter,
    _HeavySemaphoreState,
    _PassageSemaphoreState,
    DEFAULT_HEAVY_CONCURRENCY,
    DEFAULT_PASSAGE_CONCURRENCY,
)
import version as version_module

PINNED_TOP_KEYS = {
    'schema_version', 'request', 'api_version', 'endpoints', 'search_modes',
    'features', 'parallels', 'limits', 'timeouts',
}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def bare_app():
    bare = FastAPI()
    init_search_api(app_override=bare)
    return bare


@pytest.fixture
def client(bare_app):
    return TestClient(bare_app)


@pytest.fixture
def clean_env(monkeypatch):
    monkeypatch.setenv('SEARCH_API_MODE', 'open')
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '9999')
    monkeypatch.setenv('SEARCH_API_POSTHOG_SAMPLE_N', '999999')
    # Clear any per-test timeout overrides so defaults are observed unless a
    # test explicitly sets one.
    for var in (
        'SEARCH_API_FUZZY_TIMEOUT', 'SEARCH_API_PARALLELS_TIMEOUT',
        'SEARCH_API_PASSAGE_MAX_WITNESSES',
    ):
        monkeypatch.delenv(var, raising=False)
    _rate_limiter.reset_for_tests()
    _browse_rate_limiter.reset_for_tests()
    _parallels_rate_limiter.reset_for_tests()
    _capabilities_rate_limiter.reset_for_tests()


@pytest.fixture(autouse=True)
def _reset_heavy_semaphore():
    _HeavySemaphoreState.reset(DEFAULT_HEAVY_CONCURRENCY)
    yield
    _HeavySemaphoreState.reset(DEFAULT_HEAVY_CONCURRENCY)


@pytest.fixture(autouse=True)
def _reset_passage_semaphore():
    _PassageSemaphoreState.reset(DEFAULT_PASSAGE_CONCURRENCY)
    yield
    _PassageSemaphoreState.reset(DEFAULT_PASSAGE_CONCURRENCY)


def _get(client):
    return client.get('/api/capabilities')


# ---------------------------------------------------------------------------
# Shape / envelope
# ---------------------------------------------------------------------------

def test_capabilities_200_with_exact_top_level_keys(client, clean_env):
    r = _get(client)
    assert r.status_code == 200, r.text
    body = r.json()
    assert set(body.keys()) == PINNED_TOP_KEYS, (
        f'unexpected top-level keys: {set(body.keys())} (expected {PINNED_TOP_KEYS})'
    )
    assert body['schema_version'] == 1
    assert body['request'] == {}


def test_capabilities_api_version_matches_import_not_a_literal(client, clean_env):
    """Assert against the version.APP_VERSION import, never a hardcoded string,
    so a future version bump cannot silently redden (or silently pass a stale
    value in) this test."""
    r = _get(client)
    assert r.status_code == 200, r.text
    assert r.json()['api_version'] == version_module.APP_VERSION


def test_capabilities_endpoints_list(client, clean_env):
    r = _get(client)
    body = r.json()
    assert body['endpoints'] == [
        '/api/search', '/api/browse', '/api/parallels', '/api/capabilities',
    ]


def test_capabilities_search_modes_derived_from_real_enum(client, clean_env):
    from web.search_api import _SEARCH_MODE_TO_INTERNAL
    r = _get(client)
    body = r.json()
    assert body['search_modes'] == list(_SEARCH_MODE_TO_INTERNAL.keys())
    assert body['search_modes'] == [
        'exact', 'variants', 'responsa', 'title', 'shelfmark', 'fuzzy',
    ]


# ---------------------------------------------------------------------------
# Timeouts follow the live resolvers (D1) — the test that would have caught
# hardcoding the 110.0 default back into a literal.
# ---------------------------------------------------------------------------

def test_capabilities_timeouts_default_110(client, clean_env):
    r = _get(client)
    body = r.json()
    assert body['timeouts']['fuzzy'] == 110.0 == DEFAULT_FUZZY_TIMEOUT
    assert body['timeouts']['parallels'] == 110.0 == DEFAULT_PARALLELS_TIMEOUT


def test_capabilities_timeouts_follow_env_override(client, clean_env, monkeypatch):
    """Setting SEARCH_API_FUZZY_TIMEOUT / SEARCH_API_PARALLELS_TIMEOUT must move
    the reported value — proves the handler re-reads the live resolver per
    request rather than embedding a literal 110.0."""
    monkeypatch.setenv('SEARCH_API_FUZZY_TIMEOUT', '77.5')
    monkeypatch.setenv('SEARCH_API_PARALLELS_TIMEOUT', '88.5')
    r = _get(client)
    body = r.json()
    assert body['timeouts']['fuzzy'] == 77.5
    assert body['timeouts']['parallels'] == 88.5


def test_capabilities_timeouts_full_set(client, clean_env):
    r = _get(client)
    timeouts = r.json()['timeouts']
    assert timeouts['search_core'] == 30.0
    assert timeouts['variants'] == 60.0
    assert timeouts['passage'] == 30.0
    assert timeouts['browse'] == 1.0
    assert timeouts['browse_core'] == 2.0
    for key in (
        'search_core', 'variants', 'fuzzy', 'parallels', 'passage',
        'browse', 'browse_core',
    ):
        assert isinstance(timeouts[key], float), f'{key} is not a float: {timeouts[key]!r}'


def test_capabilities_limits_full_set(client, clean_env):
    """Every `limits.*` value must equal the SAME live resolver the other
    endpoints use (D1's own philosophy: never hardcode).

    NOTE (finding, not asserted here as a failure): the orchestrator task's
    illustrative sample JSON shows `"passage_concurrency": 2`, but the actual
    default (`DEFAULT_PASSAGE_CONCURRENCY` in web/search_api.py, and
    `SEARCH_API_PASSAGE_CONCURRENCY`'s documented default in docs/SEARCH_API.md,
    independently, in three places) is 4 -- `heavy_concurrency` is the one
    that defaults to 2. This assertion is written against the live resolver
    (`_PassageSemaphoreState._capacity`) rather than either hardcoded number,
    so it is correct regardless of which figure was a drafting slip; see the
    structured report for the divergence writeup.
    """
    from web.search_api import _HeavySemaphoreState, _PassageSemaphoreState
    r = _get(client)
    limits = r.json()['limits']
    assert limits['search_requests_per_minute'] == _rate_limiter._current_limit()
    assert limits['browse_requests_per_minute'] == _browse_rate_limiter._current_limit()
    assert limits['parallels_requests_per_minute'] == _parallels_rate_limiter._current_limit()
    assert limits['capabilities_requests_per_minute'] == _capabilities_rate_limiter._current_limit()
    assert limits['heavy_concurrency'] == _HeavySemaphoreState._capacity == 2
    assert limits['passage_concurrency'] == _PassageSemaphoreState._capacity == 4


# ---------------------------------------------------------------------------
# D4 — fail-closed posture. THE most important test in this file.
# ---------------------------------------------------------------------------

def test_capabilities_passage_gate_closed_by_default(client, clean_env):
    """Default test environment (this worktree carries no real passage_index/,
    and PASSAGE_PARALLELS_ENABLED defaults off) — passage_available() is False
    with no monkeypatching, mirroring
    tests/test_parallels_api.py::test_parallels_method_passage_unavailable_returns_503.
    """
    r = _get(client)
    body = r.json()
    assert body['features']['passage'] is False
    assert body['features']['passage_multi_witness'] is False
    assert 'passage' not in body['parallels']['methods'], (
        "D4 violation: 'passage' must be ABSENT from parallels.methods "
        "(not merely flagged unavailable) when the gate is closed"
    )
    assert body['parallels']['methods'] == ['chunk']
    assert body['parallels']['multi_witness'] is False
    assert body['parallels']['sorts'] == [], (
        "D4 violation: sorts must be [] when multi_witness is off — advertising "
        "them would contradict the 400 'sort_requires_multi_witness' the real "
        "/api/parallels endpoint raises for sort without witnesses[]"
    )


def test_capabilities_passage_gate_open(client, clean_env, monkeypatch):
    """Gate OPEN case — same monkeypatch pattern as
    tests/test_parallels_api.py (module-attribute patch, picked up because the
    handler imports the symbol fresh inside the request, same convention as
    the passage gate at ~line 2124 of web/search_api.py)."""
    monkeypatch.setattr('web.passage_assets.passage_available', lambda: True)
    monkeypatch.setattr(
        'web.passage_assets.passage_multi_witness_available', lambda: True,
    )
    r = _get(client)
    body = r.json()
    assert body['features']['passage'] is True
    assert body['features']['passage_multi_witness'] is True
    assert 'passage' in body['parallels']['methods']
    assert body['parallels']['methods'] == ['chunk', 'passage']
    assert body['parallels']['multi_witness'] is True
    assert body['parallels']['sorts'] == ['fused', 'best_match', 'witness_count']


def test_capabilities_passage_on_but_multi_witness_off(client, clean_env, monkeypatch):
    """Single-witness passage can be live while multi-witness fan-out stays
    gated separately (mirrors web/passage_assets.py's own docstring: multi-
    witness ANDs its flag with passage_available() rather than replacing it)."""
    monkeypatch.setattr('web.passage_assets.passage_available', lambda: True)
    monkeypatch.setattr(
        'web.passage_assets.passage_multi_witness_available', lambda: False,
    )
    r = _get(client)
    body = r.json()
    assert body['features']['passage'] is True
    assert body['features']['passage_multi_witness'] is False
    assert body['parallels']['methods'] == ['chunk', 'passage']
    assert body['parallels']['multi_witness'] is False
    assert body['parallels']['sorts'] == []


def test_capabilities_max_witnesses_follows_env(client, clean_env, monkeypatch):
    r = _get(client)
    assert r.json()['parallels']['max_witnesses'] == 25  # default

    monkeypatch.setenv('SEARCH_API_PASSAGE_MAX_WITNESSES', '12')
    r = _get(client)
    assert r.json()['parallels']['max_witnesses'] == 12


# ---------------------------------------------------------------------------
# Mode gate — identical contract to every other endpoint.
# ---------------------------------------------------------------------------

def test_capabilities_mode_disabled_returns_503(client, clean_env, monkeypatch):
    monkeypatch.setenv('SEARCH_API_MODE', 'disabled')
    r = _get(client)
    assert r.status_code == 503, r.text
    assert r.json()['error']['code'] == 'disabled'


def test_capabilities_mode_localhost_only_non_loopback_returns_403(
    client, clean_env, monkeypatch,
):
    monkeypatch.setenv('SEARCH_API_MODE', 'localhost-only')
    monkeypatch.setattr('web.api_hardening._is_loopback_request', lambda req: False)
    r = _get(client)
    assert r.status_code == 403, r.text
    assert r.json()['error']['code'] == 'localhost_only'


def test_capabilities_mode_localhost_only_loopback_succeeds(
    client, clean_env, monkeypatch,
):
    monkeypatch.setenv('SEARCH_API_MODE', 'localhost-only')
    monkeypatch.setattr('web.api_hardening._is_loopback_request', lambda req: True)
    r = _get(client)
    assert r.status_code == 200, r.text


# ---------------------------------------------------------------------------
# Its own rate-limit bucket (per the pinned contract).
# ---------------------------------------------------------------------------

def test_capabilities_has_its_own_rate_limit_bucket(client, clean_env, monkeypatch):
    """Burst on /api/capabilities must not exhaust the search/browse/parallels
    buckets, and vice versa (D-05-style three/four-bucket independence)."""
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '1')
    _capabilities_rate_limiter.reset_for_tests()
    r1 = _get(client)
    assert r1.status_code == 200, r1.text
    r2 = _get(client)
    assert r2.status_code == 429, r2.text
    assert r2.json()['error']['code'] == 'rate_limited'
    # The search bucket is untouched.
    assert _rate_limiter is not _capabilities_rate_limiter


# ---------------------------------------------------------------------------
# OpenAPI presence — sub-mount-aware client (test_openapi_scope.py's own
# docstring explains why importing nicegui.app directly would falsely pass).
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('noop', [None])
def test_capabilities_appears_in_openapi(noop):
    try:
        import web.main  # noqa: F401 -- import-for-side-effect (triggers sub-mount)
        from nicegui import app as nicegui_app
    except Exception as exc:  # pragma: no cover
        pytest.skip(f'web.main not importable in this env: {exc}')
    real_client = TestClient(nicegui_app)
    resp = real_client.get('/api/openapi.json')
    assert resp.status_code == 200, f'GET /api/openapi.json returned {resp.status_code}'
    paths = set(resp.json().get('paths', {}).keys())
    has_capabilities = '/capabilities' in paths or '/api/capabilities' in paths
    assert has_capabilities, f'/api/capabilities not in OpenAPI spec paths: {paths}'


# ---------------------------------------------------------------------------
# Cheapness — no index load, no database, no search.
# ---------------------------------------------------------------------------

def test_capabilities_never_touches_the_searcher(client, clean_env, monkeypatch):
    """Strongest available proof of cheapness without instrumenting the whole
    call graph: replace state.searcher with an object whose execute_search
    raises, and the two real search entry points the handler COULD reach
    (fetch_parallels_results for chunk-mode parallels, get_passage_searcher
    for passage-mode) with stubs that raise, then call /api/capabilities and
    assert it still returns 200 with none of them invoked.

    This proves the handler does not call SearchEngine.execute_search,
    shared.parallels_service.fetch_parallels_results, or
    web.passage_assets.get_passage_searcher — the three concrete entry points
    a search/parallels/passage request goes through in this codebase. It does
    NOT prove the handler touches no I/O whatsoever (e.g. it still reads env
    vars and a couple of module-level class attributes) — that weaker claim
    is not what D3 requires ("no index load, no database, no search").
    """
    from web.state import state

    class _BoomSearcher:
        def execute_search(self, *a, **k):
            raise AssertionError('capabilities endpoint touched execute_search')

    saved_searcher = state.searcher
    state.searcher = _BoomSearcher()

    def _boom_parallels(*a, **k):
        raise AssertionError('capabilities endpoint touched fetch_parallels_results')

    def _boom_passage_searcher(*a, **k):
        raise AssertionError('capabilities endpoint touched get_passage_searcher')

    monkeypatch.setattr(
        'web.search_api.fetch_parallels_results', _boom_parallels,
    )
    monkeypatch.setattr(
        'web.passage_assets.get_passage_searcher', _boom_passage_searcher,
    )
    # Also flip the passage gate open, so if the handler DID try to construct
    # a passage searcher for some reason, it would not be short-circuited by
    # the gate being closed — the boom function is the only thing standing
    # in the way.
    monkeypatch.setattr('web.passage_assets.passage_available', lambda: True)
    monkeypatch.setattr(
        'web.passage_assets.passage_multi_witness_available', lambda: True,
    )

    try:
        r = _get(client)
        assert r.status_code == 200, r.text
    finally:
        state.searcher = saved_searcher
