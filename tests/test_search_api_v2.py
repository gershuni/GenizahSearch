"""81A — search_mode x responsa_options x invalid-combination matrix.

Per D-12, this NEW file owns the matrix tests for the v7.10 API contract.
The existing tests/test_search_api.py is being rewritten in-place by Plan 04
to migrate Phase 78 hardening tests from the old `mode` field to `search_mode`;
this file owns the new contract surface tests (Sections 1-7 below).

Regex is intentionally absent from the v7.10 enum (D-09 — deferred to v7.11).
The 5 valid search_mode values are: exact, variants, responsa, title, shelfmark.

Sections:
    1. search_mode value coverage (AC2; two layers — stub + real-index)
    2. responsa_options flag effect (AC3)
    3. Invalid combination matrix (AC4)
    4. Bounds — query/limit (AC5)
    5. Hard cutover for old `mode` field (AC1, D-13)
    6. Request-echo correctness (AC6)
    7. PostHog properties (D-08 / Codex MEDIUM-3)

All Pydantic constraint failures (e.g. limit out of range, Literal enum
violation, extra='forbid' rejection) return HTTP 400 with
`code='invalid_request'` per the Phase 78 envelope wrapper at
web/api_hardening.py:326. NOT 422.
"""

import os
import json
import inspect
from typing import Any, Optional
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Module imports — these symbols MUST exist after Plans 01-03.
# ---------------------------------------------------------------------------

from web.search_api import (  # noqa: E402
    init_search_api,
    SearchRequest,
    ResponsaOptions,
    MAX_LIMIT,
    _SEARCH_MODE_TO_INTERNAL,
)
from shared.api_errors import APIError, ERROR_CODES  # noqa: E402


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SEARCH_MODES = ['exact', 'variants', 'responsa', 'title', 'shelfmark']

# Concrete fixture queries per AC2 — picked so a real Tantivy index would
# return ≥1 result. Layer-2 tests are skipped in environments without an
# index (most CI runs).
QUERIES_PER_MODE = {
    'exact':     'ברכת המזון',
    'variants':  'ברכת המזון',
    'responsa':  'שאלה',
    'title':     'ברכת המזון',
    'shelfmark': 'T-S 12.123',
}

# Internal `mode` argument to SearchEngine.execute_search per
# _SEARCH_MODE_TO_INTERNAL (Plan 01 Blocker-2 fix).
EXPECTED_INTERNAL_MODE = {
    'exact': 'exact',
    'variants': 'variants',
    'responsa': 'Responsa',
    'title': 'Title',
    'shelfmark': 'Shelfmark',
}

REQUEST_ECHO_KEYS = {
    'search_mode',
    'responsa_options',
    'responsa_options_effective',
    'gap',
    'limit',
    'limit_effective',
    'filters',
}


def _has_index() -> bool:
    """Return True only if a real Tantivy index appears to be loadable."""
    return os.path.isdir('Genizah_Index')


# ---------------------------------------------------------------------------
# Fixtures (mirroring tests/test_search_api.py patterns)
# ---------------------------------------------------------------------------

class StubSearcher:
    """Records execute_search args; returns a fixed result list.

    Set `cascade_meta`/`cascade_message` to simulate a Responsa cascade —
    after the stub is invoked it sets the legacy + structured thread-local
    channels so the handler picks them up.
    """

    def __init__(self):
        self.calls: list[dict] = []
        self.results: list[dict] = []
        self.cascade_meta: Optional[dict] = None
        self.cascade_message: Optional[str] = None

    def execute_search(self, **kwargs) -> list:
        # Record what the handler passed. Critical args for AC2/AC3:
        # `mode` (translated internal mode) and `responsa_options` dict.
        self.calls.append(kwargs)
        if self.cascade_message is not None:
            from genizah_core import _set_last_responsa_downgrade
            _set_last_responsa_downgrade(self.cascade_message)
        if self.cascade_meta is not None:
            from genizah_core import _set_last_responsa_downgrade_meta
            _set_last_responsa_downgrade_meta(dict(self.cascade_meta))
        return list(self.results)


@pytest.fixture(autouse=True)
def _drain_thread_locals():
    """T-81A05-01: drain both thread-local cascade channels before AND after
    each test so monkeypatched stubs don't leak across tests on the same
    worker thread."""
    from genizah_core import (
        _consume_last_responsa_downgrade,
        _consume_last_responsa_downgrade_meta,
    )
    _consume_last_responsa_downgrade()
    _consume_last_responsa_downgrade_meta()
    yield
    _consume_last_responsa_downgrade()
    _consume_last_responsa_downgrade_meta()


@pytest.fixture(autouse=True)
def _silence_posthog(monkeypatch):
    """Default: disable PostHog sampling. Individual tests that need event
    capture override SEARCH_API_POSTHOG_SAMPLE_N=1."""
    monkeypatch.setenv('SEARCH_API_POSTHOG_SAMPLE_N', '999999')
    monkeypatch.setenv('SEARCH_API_MODE', 'open')
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '9999')


@pytest.fixture(autouse=True)
def _reset_rate_limiter():
    """Clean rate-limiter state per test (R2-#2 pattern)."""
    from web.search_api import _rate_limiter
    _rate_limiter.reset_for_tests()
    yield
    _rate_limiter.reset_for_tests()


@pytest.fixture
def stub_searcher():
    """Replace state.searcher with a fresh StubSearcher; restore after."""
    from web.state import state
    saved = state.searcher
    fake = StubSearcher()
    fake.results = [{
        'uid': 'uid_001',
        'display': {
            'shelfmark': 'T-S 12.345',
            'title': 'Test',
            'id': '9912345678901234',
            'library_code': 'CUL',
        },
        'raw_header': 'header_9912345678901234_IE99_P7',
        'snippet': 'a *match* here',
        'full_text': 'lorem ipsum',
        'sort_score': 0.5,
    }]
    state.searcher = fake
    yield fake
    state.searcher = saved


@pytest.fixture
def stub_meta_mgr():
    from web.state import state
    saved = state.meta_mgr
    mgr = MagicMock()
    mgr.get_meta_for_id.return_value = ('T-S 12.345', 'Test Title')
    mgr.get_library_for_id.return_value = 'CUL'
    mgr.parse_full_id_components.return_value = {
        'sys_id': '9912345678901234',
        'ie_id': 'IE99',
        'p_num': '7',
        'fl_id': None,
    }
    state.meta_mgr = mgr
    yield mgr
    state.meta_mgr = saved


@pytest.fixture
def client(stub_searcher, stub_meta_mgr):
    bare = FastAPI()
    init_search_api(app_override=bare)
    return TestClient(bare)


@pytest.fixture
def captured_events(monkeypatch):
    """Capture every event handed to capture_api_event via the FakeQueue
    pattern from tests/test_api_hardening.py."""
    monkeypatch.setenv('SEARCH_API_POSTHOG_SAMPLE_N', '1')
    captured: list[dict] = []

    class FakeQueue:
        def put_nowait(self, item):
            captured.append(item)

    monkeypatch.setattr('web.api_hardening._event_queue', FakeQueue())
    return captured


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _assert_search_envelope_shape(envelope, *, expected_search_mode):
    assert envelope.get('schema_version') == 1, envelope
    assert envelope.get('source') == 'search', envelope
    assert 'request' in envelope, f'response missing `request` echo: {envelope}'
    echo = envelope['request']
    assert set(echo.keys()) == REQUEST_ECHO_KEYS, (
        f'unexpected echo keys: {set(echo.keys())} (expected {REQUEST_ECHO_KEYS})'
    )
    assert echo['search_mode'] == expected_search_mode


def _post_search(client, **body):
    """Convenience wrapper for POST /api/search."""
    return client.post('/api/search', json=body)


# ---------------------------------------------------------------------------
# Section 1 — search_mode value coverage (AC2)
#   Layer 1: deterministic stub-searcher tests (always run).
#   Layer 2: real-index integration tests (skipif no index).
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('search_mode', SEARCH_MODES)
def test_search_mode_engine_receives_translated_internal_mode(client, stub_searcher, search_mode):
    """LAYER 1: each search_mode translates to its expected internal `mode`."""
    body = {'query': QUERIES_PER_MODE[search_mode], 'search_mode': search_mode}
    r = _post_search(client, **body)
    assert r.status_code == 200, r.text
    assert len(stub_searcher.calls) == 1
    call = stub_searcher.calls[0]
    assert call['mode'] == EXPECTED_INTERNAL_MODE[search_mode], call


def test_search_mode_exact_vs_variants_behavioral_difference(client, stub_searcher):
    """LAYER 1 / Blocker-2: exact and variants produce DIFFERENT internal
    `mode` args (NOT both collapsed to 'text'). The two API values exercise
    distinct variant tiers in var_mgr.get_variants(term, mode)."""
    r1 = _post_search(client, query='ברכת המזון', search_mode='exact')
    r2 = _post_search(client, query='ברכת המזון', search_mode='variants')
    assert r1.status_code == 200 and r2.status_code == 200
    assert len(stub_searcher.calls) == 2
    assert stub_searcher.calls[0]['mode'] == 'exact'
    assert stub_searcher.calls[1]['mode'] == 'variants'
    assert stub_searcher.calls[0]['mode'] != stub_searcher.calls[1]['mode']


@pytest.mark.parametrize('search_mode', SEARCH_MODES)
def test_search_mode_returns_envelope_via_stub(client, stub_searcher, search_mode):
    """LAYER 1 (stub): envelope shape is correct for every search_mode value."""
    body = {'query': QUERIES_PER_MODE[search_mode], 'search_mode': search_mode}
    r = _post_search(client, **body)
    assert r.status_code == 200, r.text
    env = r.json()
    _assert_search_envelope_shape(env, expected_search_mode=search_mode)
    if search_mode == 'responsa':
        # Default ResponsaOptions echo: no cascade fired by stub.
        opts = env['request']['responsa_options']
        assert opts == {
            'variants': False, 'ja': False,
            'flex_spacing': False, 'bidirectional': False,
        }
        assert env['request']['responsa_options_effective'] == opts
    else:
        assert env['request']['responsa_options'] is None
        assert env['request']['responsa_options_effective'] is None


@pytest.mark.skipif(not _has_index(), reason='no Tantivy index in test env')
@pytest.mark.parametrize('search_mode', SEARCH_MODES)
def test_search_mode_real_index_returns_at_least_one_result(search_mode):
    """LAYER 2: against a real Tantivy index every fixture query returns ≥1
    result. Skipped in CI environments without the index. The stub-based
    tests above cover the same envelope-shape assertion deterministically."""
    bare = FastAPI()
    init_search_api(app_override=bare)
    with TestClient(bare) as cli:
        r = cli.post('/api/search', json={
            'query': QUERIES_PER_MODE[search_mode],
            'search_mode': search_mode,
        })
        assert r.status_code == 200, r.text
        env = r.json()
        _assert_search_envelope_shape(env, expected_search_mode=search_mode)
        assert env['count'] >= 1, env


# ---------------------------------------------------------------------------
# Section 2 — responsa_options flag effect (AC3)
# ---------------------------------------------------------------------------

def test_responsa_options_variants_passed_to_engine(client, stub_searcher):
    """variants=True → engine receives responsa_options['variants']=True
    AND responsa_options['variant_mode']=='variants'."""
    r = _post_search(
        client, query='שאלה', search_mode='responsa',
        responsa_options={'variants': True, 'ja': False,
                          'flex_spacing': False, 'bidirectional': False},
    )
    assert r.status_code == 200, r.text
    opts_received = stub_searcher.calls[0]['responsa_options']
    assert opts_received['variants'] is True
    assert opts_received['variant_mode'] == 'variants'


def test_responsa_options_variants_toggle_flips_variant_mode(client, stub_searcher):
    """variants:True vs variants:False on the SAME query → recorded
    `responsa_options['variant_mode']` flips 'variants' → 'exact'."""
    _post_search(client, query='שאלה', search_mode='responsa',
                 responsa_options={'variants': True})
    _post_search(client, query='שאלה', search_mode='responsa',
                 responsa_options={'variants': False})
    assert len(stub_searcher.calls) == 2
    assert stub_searcher.calls[0]['responsa_options']['variant_mode'] == 'variants'
    assert stub_searcher.calls[1]['responsa_options']['variant_mode'] == 'exact'


def test_responsa_options_ja_passed_to_engine(client, stub_searcher):
    r = _post_search(
        client, query='שאלה', search_mode='responsa',
        responsa_options={'ja': True},
    )
    assert r.status_code == 200, r.text
    assert stub_searcher.calls[0]['responsa_options']['ja'] is True


def test_responsa_options_ja_toggle(client, stub_searcher):
    """ja:True vs ja:False → recorded responsa_options['ja'] flips."""
    _post_search(client, query='שאלה', search_mode='responsa',
                 responsa_options={'ja': True})
    _post_search(client, query='שאלה', search_mode='responsa',
                 responsa_options={'ja': False})
    assert stub_searcher.calls[0]['responsa_options']['ja'] is True
    assert stub_searcher.calls[1]['responsa_options']['ja'] is False


def test_responsa_options_flex_spacing_passed_to_engine(client, stub_searcher):
    r = _post_search(
        client, query='שאלה', search_mode='responsa',
        responsa_options={'flex_spacing': True},
    )
    assert r.status_code == 200, r.text
    assert stub_searcher.calls[0]['responsa_options']['flex_spacing'] is True


def test_responsa_options_bidirectional_passed_to_engine(client, stub_searcher):
    r = _post_search(
        client, query='שאלה', search_mode='responsa',
        responsa_options={'bidirectional': True},
    )
    assert r.status_code == 200, r.text
    assert stub_searcher.calls[0]['responsa_options']['bidirectional'] is True


# ---------------------------------------------------------------------------
# Section 3 — Invalid combination matrix (AC4)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('mode', ['exact', 'variants', 'title', 'shelfmark'])
def test_responsa_options_with_non_responsa_mode_rejected(client, mode):
    r = _post_search(
        client, query='x', search_mode=mode,
        responsa_options={'variants': True},
    )
    assert r.status_code == 400, r.text
    body = r.json()
    err = body.get('error', {})
    assert err.get('code') == 'invalid_combination', body
    msg = (err.get('message') or '').lower()
    assert 'responsa_options' in msg, msg
    assert 'search_mode' in msg or mode in msg, msg


@pytest.mark.parametrize('mode', ['title', 'shelfmark'])
@pytest.mark.parametrize('gap', [1, 2, 5, 10])
def test_gap_with_metadata_mode_rejected(client, mode, gap):
    r = _post_search(client, query='x', search_mode=mode, gap=gap)
    assert r.status_code == 400, r.text
    body = r.json()
    err = body.get('error', {})
    assert err.get('code') == 'invalid_combination', body
    msg = (err.get('message') or '').lower()
    assert 'gap' in msg, msg
    assert mode in msg, msg


@pytest.mark.parametrize('mode', ['title', 'shelfmark'])
def test_gap_zero_with_metadata_mode_legal(client, stub_searcher, mode):
    r = _post_search(client, query='x', search_mode=mode, gap=0)
    assert r.status_code == 200, r.text


# ---------------------------------------------------------------------------
# Section 4 — Bounds (AC5)
#   All Pydantic constraint failures route through web/api_hardening.py:326
#   and return HTTP 400 + code='invalid_request'.
# ---------------------------------------------------------------------------

def test_query_empty_after_strip_rejected(client):
    r = _post_search(client, query='   ', search_mode='exact')
    assert r.status_code == 400
    assert r.json()['error']['code'] == 'query_required'


def test_query_too_long_rejected(client):
    r = _post_search(client, query='x' * 1001, search_mode='exact')
    assert r.status_code == 400
    assert r.json()['error']['code'] == 'query_too_long'


def test_query_at_cap_legal(client, stub_searcher):
    r = _post_search(client, query='x' * 1000, search_mode='exact')
    assert r.status_code == 200, r.text


@pytest.mark.parametrize('limit', [101, 200, 500, 9999])
def test_limit_above_max_rejected(client, limit):
    r = _post_search(client, query='x', search_mode='exact', limit=limit)
    assert r.status_code == 400, r.text
    assert r.json()['error']['code'] == 'invalid_request'


@pytest.mark.parametrize('limit', [0, -1, -5, -100])
def test_limit_below_min_rejected(client, limit):
    r = _post_search(client, query='x', search_mode='exact', limit=limit)
    assert r.status_code == 400, r.text
    assert r.json()['error']['code'] == 'invalid_request'


def test_limit_at_max_legal(client, stub_searcher):
    r = _post_search(client, query='x', search_mode='exact', limit=100)
    assert r.status_code == 200, r.text


def test_limit_at_min_legal(client, stub_searcher):
    r = _post_search(client, query='x', search_mode='exact', limit=1)
    assert r.status_code == 200, r.text


# ---------------------------------------------------------------------------
# Section 5 — Hard cutover for old `mode` field (AC1, D-13)
# ---------------------------------------------------------------------------

def test_old_mode_field_rejected_with_helpful_message(client):
    """D-13: payload using old `mode` returns 400 invalid_request with a
    cutover hint that names BOTH the old and the new field."""
    r = client.post('/api/search', json={'query': 'x', 'mode': 'text'})
    assert r.status_code == 400, r.text
    body = r.json()
    err = body.get('error', {})
    assert err.get('code') == 'invalid_request', body
    msg = (err.get('message') or '').lower()
    assert 'mode' in msg
    assert 'search_mode' in msg


def test_extra_unknown_field_rejected(client):
    """extra='forbid' on SearchRequest rejects arbitrary extra keys."""
    r = client.post('/api/search',
                    json={'query': 'x', 'search_mode': 'exact', 'foo': 'bar'})
    assert r.status_code == 400, r.text
    assert r.json()['error']['code'] == 'invalid_request'


def test_responsa_options_extra_field_rejected(client):
    """ResponsaOptions has extra='forbid' too: variant_mode is server-derived
    and MUST NOT be accepted in the request shape."""
    r = _post_search(
        client, query='x', search_mode='responsa',
        responsa_options={'variants': True, 'variant_mode': 'variants'},
    )
    assert r.status_code == 400, r.text
    assert r.json()['error']['code'] == 'invalid_request'


def test_responsa_options_variants_extended_rejected(client):
    """variants_extended is deferred to v7.11 (RESCOPE §3.2). v7.10
    ResponsaOptions rejects it via extra='forbid'."""
    r = _post_search(
        client, query='x', search_mode='responsa',
        responsa_options={'variants_extended': True},
    )
    assert r.status_code == 400, r.text
    assert r.json()['error']['code'] == 'invalid_request'


def test_search_mode_regex_rejected(client):
    """D-09: regex is NOT in the v7.10 enum. Pydantic Literal rejects it."""
    r = _post_search(client, query='x', search_mode='regex')
    assert r.status_code == 400, r.text
    assert r.json()['error']['code'] == 'invalid_request'


# ---------------------------------------------------------------------------
# Section 6 — request-echo correctness (AC6)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('mode', SEARCH_MODES)
def test_request_echo_present_on_all_5_modes(client, stub_searcher, mode):
    r = _post_search(client, query=QUERIES_PER_MODE[mode], search_mode=mode)
    assert r.status_code == 200, r.text
    env = r.json()
    _assert_search_envelope_shape(env, expected_search_mode=mode)


def test_request_echo_responsa_no_cascade_options_equal_effective(client, stub_searcher):
    """Responsa with no cascade → responsa_options == responsa_options_effective."""
    r = _post_search(
        client, query='שאלה', search_mode='responsa',
        responsa_options={'variants': True, 'ja': True},
    )
    assert r.status_code == 200
    env = r.json()
    assert env['request']['responsa_options'] == env['request']['responsa_options_effective']


def test_request_echo_search_mode_never_downgraded(client, stub_searcher):
    """D-04: even when the cascade fires, request.search_mode stays
    'responsa' verbatim (never silently downgraded)."""
    stub_searcher.cascade_message = 'Judeo-Arabic expansion disabled'
    stub_searcher.cascade_meta = {
        'variants': True, 'ja': False,
        'flex_spacing': False, 'bidirectional': False,
    }
    r = _post_search(
        client, query='שאלה', search_mode='responsa',
        responsa_options={'variants': True, 'ja': True},
    )
    assert r.status_code == 200
    env = r.json()
    assert env['request']['search_mode'] == 'responsa'


def test_request_echo_responsa_cascade_diverges(client, stub_searcher):
    """Cascade firing → request.responsa_options.ja=True AND
    request.responsa_options_effective.ja=False AND warnings[] mentions JA."""
    stub_searcher.cascade_message = 'Judeo-Arabic expansion disabled'
    stub_searcher.cascade_meta = {
        'variants': True, 'ja': False,
        'flex_spacing': False, 'bidirectional': False,
    }
    r = _post_search(
        client, query='שאלה', search_mode='responsa',
        responsa_options={'variants': True, 'ja': True,
                          'flex_spacing': False, 'bidirectional': False},
    )
    assert r.status_code == 200, r.text
    env = r.json()
    echo = env['request']
    assert echo['responsa_options']['ja'] is True
    assert echo['responsa_options_effective']['ja'] is False
    warnings = env.get('warnings') or []
    assert any('judeo-arabic' in (w or '').lower() or 'ja' in (w or '').lower()
               for w in warnings), warnings


def test_request_echo_limit_effective_reflects_cap(client, stub_searcher):
    r = _post_search(client, query='x', search_mode='exact', limit=50)
    assert r.status_code == 200
    assert r.json()['request']['limit_effective'] == 50

    r2 = _post_search(client, query='x', search_mode='exact', limit=10)
    assert r2.status_code == 200
    assert r2.json()['request']['limit_effective'] == 10


def test_request_echo_filters_passthrough(client, stub_searcher, monkeypatch):
    """filters dict round-trips into request.filters."""
    # Stub fjms_service so an arbitrary filter validates and short-circuits
    # to a non-empty restricted set.
    from shared import fjms_service as fjms_module
    monkeypatch.setattr(fjms_module, 'validate_filter_values', lambda d: None)
    monkeypatch.setattr(
        fjms_module, 'get_filter_sys_ids',
        lambda **kw: {'9912345678901234'},
    )
    r = _post_search(
        client, query='x', search_mode='exact',
        filters={'domains': ['liturgy']},
    )
    assert r.status_code == 200, r.text
    assert r.json()['request']['filters'] == {'domains': ['liturgy']}


# ---------------------------------------------------------------------------
# Section 7 — PostHog properties (D-08 / Codex MEDIUM-3)
# ---------------------------------------------------------------------------

def _props(event: dict) -> dict:
    assert isinstance(event, dict)
    return event.get('properties') or {}


def test_posthog_event_carries_search_mode_value_for_exact(
    client, stub_searcher, captured_events,
):
    r = _post_search(client, query='x', search_mode='exact')
    assert r.status_code == 200
    assert len(captured_events) >= 1
    props = _props(captured_events[-1])
    assert props.get('search_mode_value') == 'exact'
    assert props.get('responsa_options_count') == 0


def test_posthog_event_carries_responsa_options_count_zero_for_non_responsa(
    client, stub_searcher, captured_events,
):
    r = _post_search(client, query='x', search_mode='title')
    assert r.status_code == 200
    props = _props(captured_events[-1])
    assert props.get('responsa_options_count') == 0
    assert props.get('search_mode_value') == 'title'


def test_posthog_event_carries_responsa_options_count_three_for_three_flags(
    client, stub_searcher, captured_events,
):
    r = _post_search(
        client, query='שאלה', search_mode='responsa',
        responsa_options={'variants': True, 'ja': True,
                          'flex_spacing': True, 'bidirectional': False},
    )
    assert r.status_code == 200, r.text
    props = _props(captured_events[-1])
    assert props.get('search_mode_value') == 'responsa'
    assert props.get('responsa_options_count') == 3


def test_posthog_search_mode_value_present_on_invalid_combination(
    client, captured_events,
):
    """Codex MEDIUM-3: cross-field rejection (raised by @model_validator)
    happens AFTER raw-body provisional capture, so search_mode_value is
    preserved in telemetry. The rejection itself surfaces as
    code='invalid_combination'."""
    r = _post_search(
        client, query='x', search_mode='exact',
        responsa_options={'variants': True},
    )
    assert r.status_code == 400, r.text
    assert r.json()['error']['code'] == 'invalid_combination'
    assert len(captured_events) >= 1
    props = _props(captured_events[-1])
    assert props.get('search_mode_value') == 'exact'
    assert props.get('error_code') == 'invalid_combination'


def test_posthog_search_mode_value_null_on_invalid_request_unknown_field(
    client, captured_events,
):
    """Old `mode` field rejection is structural (extra='forbid') — provisional
    capture keys off `search_mode`, which is absent → search_mode_value=None."""
    r = client.post('/api/search', json={'query': 'x', 'mode': 'text'})
    assert r.status_code == 400, r.text
    assert r.json()['error']['code'] == 'invalid_request'
    assert len(captured_events) >= 1
    props = _props(captured_events[-1])
    assert props.get('search_mode_value') is None
    assert props.get('error_code') == 'invalid_request'
    assert props.get('responsa_options_count') == 0


def test_posthog_event_search_mode_value_null_on_pydantic_rejection(
    client, captured_events,
):
    """Structural rejection where search_mode is absent from the body →
    no provisional capture → search_mode_value is None."""
    r = client.post('/api/search', json={'query': 'x'})  # missing search_mode
    assert r.status_code == 400, r.text
    assert len(captured_events) >= 1
    props = _props(captured_events[-1])
    assert props.get('search_mode_value') is None
    assert props.get('responsa_options_count') == 0


# ---------------------------------------------------------------------------
# Misc sanity — internal mapping + ERROR_CODES contract
# ---------------------------------------------------------------------------

def test_search_mode_to_internal_mapping_complete():
    """Every UI search_mode maps to a non-None internal mode."""
    for m in SEARCH_MODES:
        assert m in _SEARCH_MODE_TO_INTERNAL
        assert _SEARCH_MODE_TO_INTERNAL[m] == EXPECTED_INTERNAL_MODE[m]


def test_invalid_combination_code_registered():
    """81A D-03: invalid_combination is in the canonical taxonomy."""
    assert 'invalid_combination' in ERROR_CODES


def test_max_limit_lowered_to_100():
    """81A D-06: ceiling lowered from 200 (Phase 78) to 100."""
    assert MAX_LIMIT == 100
