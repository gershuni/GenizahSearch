"""
Phase 78 Plan 01 (Wave 0 RED) — handler-level test scaffold for /api/search.

Tests fail at IMPORT time today because `web.search_api`, `web.api_hardening`,
and `shared.api_errors` do not yet exist. Plans 02 and 03 must produce these
modules with the exact symbol names imported below — that is the contract this
RED scaffold locks.

Coverage: D-21 + D-23 + the review-driven additions from 78-REVIEWS.md
(Concerns #1, #2, #3, #4, #5, #6, #9, #10, #12) + round-2 additions
(R2-#1, R2-#2, R2-#3).
"""

import json
import pytest
from unittest.mock import MagicMock
from fastapi import FastAPI
from fastapi.testclient import TestClient

# These imports fail with ModuleNotFoundError/AttributeError until Plans 02+03 land.
# That is the intended RED state — Plans 02+03 must produce these exact symbols.
from web.search_api import init_search_api, FiltersModel, SearchRequest  # noqa: F401
from web.api_hardening import (  # noqa: F401
    RateLimiter,
    capture_api_event,
    wrap_endpoint,
    _build_envelope_response,
    _resolve_rate_limit_key,
    _is_loopback_request,
    get_dropped_event_count,  # Concern #9 — Plan 02 must export this
)
from shared.api_errors import APIError  # Concern #3 — neutral location, NOT web.api_hardening


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def bare_app():
    """Per-test bare app so Concern #10 idempotency test gets a clean slate."""
    bare = FastAPI()
    init_search_api(app_override=bare)
    return bare


@pytest.fixture
def client(bare_app):
    return TestClient(bare_app)


@pytest.fixture
def mock_searcher():
    """Replace state.searcher with a MagicMock returning a single synthetic result."""
    from web.state import state
    saved = state.searcher
    fake = MagicMock()
    fake.execute_search.return_value = [{
        'uid': 'uid_001',
        'display': {'shelfmark': 'T-S 12.345', 'title': 'Test',
                    'id': '9912345678901234', 'library_code': 'CUL'},
        'raw_header': 'header_9912345678901234_IE99_P7',
        'snippet': 'a *match* here',
        'full_text': 'lorem ipsum',
        'sort_score': 0.5,
    }]
    state.searcher = fake
    yield fake
    state.searcher = saved


@pytest.fixture
def mock_meta_mgr():
    from web.state import state
    saved = state.meta_mgr
    mgr = MagicMock()
    mgr.get_meta_for_id.return_value = ("T-S 12.345", "Test Title")
    mgr.parse_full_id_components.return_value = {
        'sys_id': '9912345678901234', 'ie_id': 'IE99', 'p_num': '7', 'fl_id': None,
    }
    state.meta_mgr = mgr
    yield mgr
    state.meta_mgr = saved


@pytest.fixture
def populated_state(mock_searcher, mock_meta_mgr):
    """Convenience fixture combining searcher + meta_mgr swaps."""
    return (mock_searcher, mock_meta_mgr)


@pytest.fixture
def clean_env(monkeypatch):
    monkeypatch.setenv('SEARCH_API_MODE', 'open')
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '30')
    monkeypatch.setenv('SEARCH_API_POSTHOG_SAMPLE_N', '999999')  # silence PostHog by default


@pytest.fixture
def captured_posthog_events(monkeypatch):
    """Capture every event passed to capture_api_event without going to network."""
    captured = []

    def fake_capture(**kwargs):
        captured.append(kwargs)

    monkeypatch.setattr('web.search_api.capture_api_event', fake_capture)
    return captured


# ---------------------------------------------------------------------------
# Singleton immutability + idempotency (Concerns #10, #2)
# ---------------------------------------------------------------------------

def test_init_search_api_does_not_mutate_nicegui_singleton():
    """Calling init_search_api(bare_app) does NOT mutate nicegui.app."""
    from nicegui import app as nicegui_app
    before = len(nicegui_app.routes) if hasattr(nicegui_app, 'routes') else 0
    bare = FastAPI()
    init_search_api(app_override=bare)
    after = len(nicegui_app.routes) if hasattr(nicegui_app, 'routes') else 0
    assert after == before, (
        f"NiceGUI singleton was mutated: routes {before} -> {after}."
    )
    assert len(bare.routes) > 0, "Bare app got no routes -- app_override dispatch broken."


def test_init_search_api_idempotent():
    """Concern #10: re-calling init_search_api on the same app must NOT duplicate routes."""
    bare = FastAPI()
    init_search_api(app_override=bare)
    init_search_api(app_override=bare)  # second call should be no-op
    matches = [r for r in bare.routes if getattr(r, 'path', None) == '/api/search']
    assert len(matches) == 1, (
        f"init_search_api is not idempotent: /api/search registered {len(matches)} times"
    )


def test_apierror_imported_from_shared_api_errors_module():
    """Concern #3 dependency-inversion fix: APIError lives in shared/api_errors.py
    so both web (web/api_hardening.py) and shared (shared/fjms_service.py) can import
    it without creating a shared→web back-reference."""
    from shared.api_errors import APIError as SharedAPIError
    from web.api_hardening import APIError as WebReexportedAPIError
    assert SharedAPIError is WebReexportedAPIError, (
        "web.api_hardening.APIError must be a re-export of "
        "shared.api_errors.APIError, not a separate class"
    )


# ---------------------------------------------------------------------------
# Happy-path per mode
# ---------------------------------------------------------------------------

def test_happy_path_text_mode(client, populated_state, clean_env):
    r = client.post('/api/search', json={'query': 'foo', 'search_mode': 'exact'})
    assert r.status_code == 200, r.json()
    body = r.json()
    assert body['source'] == 'search'
    # 81A: top-level `mode` echo now reflects internal mode value (search_mode='exact' -> 'exact').
    assert body['mode'] == 'exact'
    assert body['request']['search_mode'] == 'exact'
    assert body['schema_version'] == 1
    assert isinstance(body['results'], list)
    assert isinstance(body.get('warnings'), list)


def test_happy_path_title_mode(client, populated_state, clean_env):
    r = client.post('/api/search', json={'query': 'foo', 'search_mode': 'title'})
    assert r.status_code == 200, r.json()
    assert r.json()['mode'] == 'Title'


def test_happy_path_shelfmark_mode(client, populated_state, clean_env):
    r = client.post('/api/search', json={'query': 'T-S 12.345', 'search_mode': 'shelfmark'})
    assert r.status_code == 200, r.json()
    assert r.json()['mode'] == 'Shelfmark'


def test_happy_path_responsa_mode(client, populated_state, clean_env):
    r = client.post('/api/search', json={'query': 'דרבי', 'search_mode': 'responsa'})
    assert r.status_code == 200, r.json()
    assert r.json()['mode'] == 'Responsa'


# ---------------------------------------------------------------------------
# Locator
# ---------------------------------------------------------------------------

def test_locator_present_on_every_item(client, populated_state, clean_env):
    """D-21 + Phase 77 D-04: every result item has uid AND locator."""
    r = client.post('/api/search', json={'query': 'foo', 'search_mode': 'exact'})
    assert r.status_code == 200
    for item in r.json().get('results', []):
        assert 'uid' in item
        assert 'locator' in item
        loc = item['locator']
        assert 'sys_id' in loc
        assert 'volume_ie' in loc
        assert 'p_num' in loc


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def test_query_required(client, populated_state, clean_env):
    """Empty query (after .strip()) → query_required."""
    r = client.post('/api/search', json={'query': '   ', 'search_mode': 'exact'})
    assert r.status_code == 400, r.json()
    assert r.json()['error']['code'] == 'query_required'


def test_query_too_long(client, populated_state, clean_env):
    """Query > 1000 chars → query_too_long."""
    long_q = 'x' * 1001
    r = client.post('/api/search', json={'query': long_q, 'search_mode': 'exact'})
    assert r.status_code == 400, r.json()
    assert r.json()['error']['code'] == 'query_too_long'


def test_unknown_mode_returns_invalid_request(client, populated_state, clean_env):
    r = client.post('/api/search', json={'query': 'x', 'search_mode': 'NOT_A_MODE'})
    assert r.status_code == 400, r.json()
    assert r.json()['error']['code'] == 'invalid_request'


def test_unknown_filter_key_returns_invalid_request(client, populated_state, clean_env):
    """Pydantic extra='forbid' on FiltersModel → invalid_request."""
    r = client.post('/api/search', json={
        'query': 'x', 'search_mode': 'exact',
        'filters': {'__bogus_key__': ['anything']},
    })
    assert r.status_code == 400, r.json()
    assert r.json()['error']['code'] == 'invalid_request'


def test_extra_top_level_key_rejected(client, populated_state, clean_env):
    r = client.post('/api/search', json={
        'query': 'x', 'search_mode': 'exact', '__bogus_top_key__': 1,
    })
    assert r.status_code == 400, r.json()
    assert r.json()['error']['code'] == 'invalid_request'


def test_limit_too_high(client, populated_state, clean_env):
    """81A D-06: MAX_LIMIT lowered 200 -> 100; Pydantic Field(le=100) rejects via
    envelope wrapper as HTTP 400 invalid_request (NOT 422, NOT limit_too_high)."""
    r = client.post('/api/search', json={'query': 'x', 'search_mode': 'exact', 'limit': 101})
    assert r.status_code == 400, r.json()
    assert r.json()['error']['code'] == 'invalid_request'


def test_limit_zero_returns_invalid_request(client, populated_state, clean_env):
    r = client.post('/api/search', json={'query': 'x', 'search_mode': 'exact', 'limit': 0})
    assert r.status_code == 400, r.json()
    assert r.json()['error']['code'] == 'invalid_request'


# ---------------------------------------------------------------------------
# Error envelope shape
# ---------------------------------------------------------------------------

def test_error_envelope_shape(client, populated_state, clean_env):
    """Every non-2xx response is {error:{code,message,...}}, never raw 422 dump."""
    r = client.post('/api/search', json={'search_mode': 'exact'})  # missing 'query'
    assert r.status_code == 400, r.json()
    body = r.json()
    assert 'error' in body
    assert isinstance(body['error'], dict)
    assert 'code' in body['error']
    assert 'message' in body['error']
    # Must NOT be the raw FastAPI 422 envelope.
    assert 'detail' not in body or 'error' in body


# ---------------------------------------------------------------------------
# Filter resolution
# ---------------------------------------------------------------------------

def test_filter_resolution_known_good(client, populated_state, clean_env, monkeypatch):
    """Known-good filter values resolve and search executes normally."""
    monkeypatch.setattr(
        'shared.fjms_service._domain_vocabulary_is_loadable', lambda: True, raising=False,
    )
    monkeypatch.setattr(
        'shared.fjms_service.is_valid_domain_token',
        lambda v: v in ('Piyyut', 'Liturgy'), raising=False,
    )
    r = client.post('/api/search', json={
        'query': 'x', 'search_mode': 'exact',
        'filters': {'domains': ['Piyyut']},
    })
    assert r.status_code == 200, r.json()


def test_filter_resolution_bogus_value(client, populated_state, clean_env, monkeypatch):
    monkeypatch.setattr(
        'shared.fjms_service._domain_vocabulary_is_loadable', lambda: True, raising=False,
    )
    monkeypatch.setattr(
        'shared.fjms_service.is_valid_domain_token',
        lambda v: False, raising=False,
    )
    r = client.post('/api/search', json={
        'query': 'x', 'search_mode': 'exact',
        'filters': {'domains': ['__bogus_domain_xyz__']},
    })
    assert r.status_code == 400, r.json()
    assert r.json()['error']['code'] == 'unresolvable_filter_value'


def test_filter_resolution_yields_empty_intersection_returns_empty_results_without_executing_search(
    monkeypatch, mock_meta_mgr, clean_env,
):
    """Empty intersection → 200 with count=0, results=[]; execute_search NOT called.

    Distinct from 'unresolvable_filter_value' (which is a 400 error). All filter
    values resolve, but their AND yields zero manuscripts. This is the only
    honest reading of API-07.
    """
    from web.state import state
    saved = state.searcher
    fake = MagicMock()
    state.searcher = fake
    try:
        monkeypatch.setattr(
            'shared.fjms_service._domain_vocabulary_is_loadable', lambda: True, raising=False,
        )
        monkeypatch.setattr(
            'shared.fjms_service.is_valid_domain_token', lambda v: True, raising=False,
        )
        # Make the resolver return an explicit empty set (intersection of valid filters).
        monkeypatch.setattr(
            'shared.fjms_service.get_filter_sys_ids',
            lambda **_: set(), raising=False,
        )
        bare = FastAPI()
        init_search_api(app_override=bare)
        with TestClient(bare) as c:
            r = c.post('/api/search', json={
                'query': 'x', 'search_mode': 'exact',
                'filters': {'domains': ['Piyyut']},
            })
        assert r.status_code == 200, r.json()
        body = r.json()
        assert body['count'] == 0
        assert body['results'] == []
        assert fake.execute_search.call_count == 0, (
            'execute_search should be short-circuited on empty intersection'
        )
    finally:
        state.searcher = saved


# ---------------------------------------------------------------------------
# Mode gate (Concerns #1, #4)
# ---------------------------------------------------------------------------

def test_mode_gate_disabled(monkeypatch, populated_state):
    monkeypatch.setenv('SEARCH_API_MODE', 'disabled')
    bare = FastAPI()
    init_search_api(app_override=bare)
    with TestClient(bare) as c:
        r = c.post('/api/search', json={'query': 'x', 'search_mode': 'exact'})
    assert r.status_code == 503, r.json()
    assert r.json()['error']['code'] == 'disabled'


def test_mode_gate_localhost_only_loopback_direct(monkeypatch, populated_state):
    """peer=127.0.0.1, no XFF → 200."""
    monkeypatch.setenv('SEARCH_API_MODE', 'localhost-only')
    bare = FastAPI()
    init_search_api(app_override=bare)
    monkeypatch.setattr('web.api_hardening._is_loopback_request', lambda req: True)
    with TestClient(bare) as c:
        r = c.post('/api/search', json={'query': 'x', 'search_mode': 'exact'})
    assert r.status_code == 200, r.json()


def test_mode_gate_localhost_only_non_loopback(monkeypatch, populated_state):
    """peer=192.0.2.1, no XFF → 403, code='localhost_only'."""
    monkeypatch.setenv('SEARCH_API_MODE', 'localhost-only')
    bare = FastAPI()
    init_search_api(app_override=bare)
    monkeypatch.setattr('web.api_hardening._is_loopback_request', lambda req: False)
    with TestClient(bare) as c:
        r = c.post('/api/search', json={'query': 'x', 'search_mode': 'exact'})
    assert r.status_code == 403, r.json()
    assert r.json()['error']['code'] == 'localhost_only'


def test_mode_gate_localhost_only_xff_spoof_rejected(monkeypatch, populated_state):
    """Concern #4: peer=127.0.0.1 with XFF='127.0.0.1, 203.0.113.5' MUST be rejected.
    The right-most untrusted entry is 203.0.113.5 (real client behind nginx)."""
    monkeypatch.setenv('SEARCH_API_MODE', 'localhost-only')
    bare = FastAPI()
    init_search_api(app_override=bare)
    # Use the REAL helper — do not patch. We are testing the helper's own logic
    # by passing the headers TestClient sends.
    with TestClient(bare) as c:
        r = c.post(
            '/api/search',
            json={'query': 'x', 'search_mode': 'exact'},
            headers={'X-Forwarded-For': '127.0.0.1, 203.0.113.5'},
        )
    # _is_loopback_request must reject because not EVERY XFF entry is loopback.
    assert r.status_code == 403, r.json()
    assert r.json()['error']['code'] == 'localhost_only'


def test_mode_gate_localhost_only_clean_xff_chain(monkeypatch, populated_state):
    """Every entry in XFF must be loopback to grant access (Concern #1 strict semantics).

    R2-#7 determinism fix: patch `_is_loopback_request` to True so this test
    deterministically exercises the post-gate code path (200) regardless of
    TestClient's pseudo-peer. The XFF-strictness contract itself is already
    covered by helper-level tests (test_is_loopback_request_*); this route-level
    test only verifies the mode gate routes to the endpoint when the helper says
    the peer is loopback-equivalent.
    """
    monkeypatch.setenv('SEARCH_API_MODE', 'localhost-only')
    monkeypatch.setattr('web.api_hardening._is_loopback_request', lambda req: True)
    bare = FastAPI()
    init_search_api(app_override=bare)
    with TestClient(bare) as c:
        r = c.post(
            '/api/search',
            json={'query': 'x', 'search_mode': 'exact'},
            headers={'X-Forwarded-For': '127.0.0.1, ::1'},
        )
    assert r.status_code == 200, r.json()


# ---------------------------------------------------------------------------
# Statelessness (D-20)
# ---------------------------------------------------------------------------

def test_identical_requests_byte_identical_modulo_timestamp(client, populated_state, clean_env):
    """Two identical requests produce identical bodies (modulo generated_at)."""
    body = {'query': 'foo', 'search_mode': 'exact'}
    r1 = client.post('/api/search', json=body)
    r2 = client.post('/api/search', json=body)
    assert r1.status_code == 200 and r2.status_code == 200
    j1, j2 = r1.json(), r2.json()
    j1.pop('generated_at', None)
    j2.pop('generated_at', None)
    assert json.dumps(j1, sort_keys=True) == json.dumps(j2, sort_keys=True), (
        'identical requests must produce byte-identical responses (modulo timestamp)'
    )


# ---------------------------------------------------------------------------
# Warnings
# ---------------------------------------------------------------------------

def test_warnings_array_always_present(client, populated_state, clean_env):
    """Top-level 'warnings' key must always be present (even if empty)."""
    r = client.post('/api/search', json={'query': 'x', 'search_mode': 'exact'})
    assert r.status_code == 200
    assert 'warnings' in r.json()
    assert isinstance(r.json()['warnings'], list)


def test_warnings_surfaced_at_top_level(monkeypatch, mock_meta_mgr, clean_env):
    """If a downgrade is signaled, it appears in top-level warnings, not in results[0]."""
    from web.state import state
    saved = state.searcher
    fake = MagicMock()
    fake.execute_search.return_value = [{
        'uid': 'uid_001',
        'display': {'shelfmark': 'X', 'title': 'Y',
                    'id': '9912345678901234', 'library_code': 'CUL'},
        'raw_header': 'header_9912345678901234_IE99_P7',
        'snippet': 's', 'full_text': 't', 'sort_score': 0.5,
    }]
    state.searcher = fake
    monkeypatch.setattr(
        'web.search_api._consume_last_responsa_downgrade',
        lambda: 'query_downgraded: variant cascade triggered',
        raising=False,
    )
    try:
        bare = FastAPI()
        init_search_api(app_override=bare)
        with TestClient(bare) as c:
            r = c.post('/api/search', json={'query': 'x', 'search_mode': 'responsa'})
        assert r.status_code == 200
        body = r.json()
        warnings = body.get('warnings') or []
        assert len(warnings) >= 1, warnings
        assert any('query_downgraded' in str(w) for w in warnings), warnings
        # MUST NOT be embedded in the first result item.
        for item in body.get('results', []):
            assert 'query_downgraded' not in json.dumps(item), (
                'warnings must NOT live inside result items'
            )
    finally:
        state.searcher = saved


def test_zero_result_responsa_downgrade_warning_still_surfaced(monkeypatch, mock_meta_mgr, clean_env):
    """Concern #6: surface query_downgraded warnings even when results == [].
    The warning cannot live on results[0] because there is no results[0]."""
    from web.state import state
    saved_searcher = state.searcher
    fake = MagicMock()
    fake.execute_search.return_value = []
    monkeypatch.setattr(
        'web.search_api._consume_last_responsa_downgrade',
        lambda: 'Variant mode downgraded to basic to fit MAX_EXPANDED_TERMS=500',
        raising=False,
    )
    state.searcher = fake
    try:
        bare = FastAPI()
        init_search_api(app_override=bare)
        with TestClient(bare) as c:
            r = c.post('/api/search', json={'query': 'דרבי', 'search_mode': 'responsa'})
        assert r.status_code == 200, r.json()
        body = r.json()
        assert body['results'] == [], body
        assert body['count'] == 0
        warnings = body.get('warnings') or []
        assert len(warnings) >= 1, (
            f"expected downgrade warning even with zero results, got {warnings!r}"
        )
        assert any('query_downgraded' in str(w) or 'downgrad' in str(w).lower() for w in warnings), warnings
    finally:
        state.searcher = saved_searcher


# ---------------------------------------------------------------------------
# Rate limiting envelope
# ---------------------------------------------------------------------------

def test_rate_limited_envelope_code(monkeypatch, populated_state):
    """When the rate limiter trips, body shape is {error:{code:'rate_limited',...}}
    and Retry-After header is present + an integer >= 1."""
    monkeypatch.setenv('SEARCH_API_MODE', 'open')
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '1')
    bare = FastAPI()
    init_search_api(app_override=bare)
    with TestClient(bare) as c:
        # First few hits exhaust the limit.
        last = None
        for _ in range(5):
            last = c.post('/api/search', json={'query': 'x', 'search_mode': 'exact'})
        # At least one of those hits must have been throttled.
    assert last is not None
    # Find a 429 across the burst.
    statuses = []
    with TestClient(bare) as c:
        for _ in range(10):
            r = c.post('/api/search', json={'query': 'x', 'search_mode': 'exact'})
            statuses.append(r)
    rate_limited = [r for r in statuses if r.status_code == 429]
    assert len(rate_limited) >= 1, [r.status_code for r in statuses]
    r = rate_limited[0]
    assert r.json()['error']['code'] == 'rate_limited'
    assert 'Retry-After' in r.headers or 'retry-after' in r.headers
    ra = r.headers.get('Retry-After') or r.headers.get('retry-after')
    assert int(ra) >= 1


# ---------------------------------------------------------------------------
# PostHog observability (Concern #12)
# ---------------------------------------------------------------------------

def test_capture_api_event_called_with_correct_status_and_error_code_on_apierror(
    monkeypatch, captured_posthog_events, clean_env, populated_state,
):
    """Errors raised as APIError must surface in capture_api_event with the
    correct status_code + error_code."""
    bare = FastAPI()
    init_search_api(app_override=bare)
    with TestClient(bare) as c:
        # Empty query → APIError(query_required, 400)
        r = c.post('/api/search', json={'query': '   ', 'search_mode': 'exact'})
    assert r.status_code == 400
    matching = [e for e in captured_posthog_events
                if e.get('error_code') == 'query_required']
    assert len(matching) >= 1, (
        f"expected query_required capture, got events={captured_posthog_events!r}"
    )
    assert matching[0].get('status_code') == 400


def test_pydantic_structural_error_captures_posthog_invalid_request_event(
    monkeypatch, captured_posthog_events, clean_env, populated_state,
):
    """Concern #12: the endpoint's wrap_endpoint helper must capture
    invalid_request events in PostHog when Pydantic validation fails. Per
    Concern #2 the handler is route-scoped, so the wrap helper IS the place
    to capture."""
    bare = FastAPI()
    init_search_api(app_override=bare)
    with TestClient(bare) as c:
        r = c.post('/api/search', json={'query': 'x', 'search_mode': 'NOT_A_MODE'})
    assert r.status_code == 400, r.json()
    assert r.json()['error']['code'] == 'invalid_request', r.json()
    matching = [e for e in captured_posthog_events
                if e.get('error_code') == 'invalid_request']
    assert len(matching) >= 1, (
        f"expected invalid_request capture, got events={captured_posthog_events!r}"
    )


# ====================================================================
# Round-2 revision additions (78-REVIEWS.md round 2: R2-#1, R2-#2, R2-#3)
# ====================================================================

# R2-#3 — fail-closed filter validation -------------------------------

def test_validate_filter_values_qualified_domain_accepted(monkeypatch):
    """R2-#3: qualified domain forms (e.g., 'Other (Bible)') that
    unqualify_domain_name + get_filter_sys_ids accept MUST also be accepted by
    validate_filter_values. The helper is_valid_domain_token must canonicalize
    through the same logic, NOT a bare get_all_domains() membership check."""
    from shared.fjms_service import validate_filter_values
    monkeypatch.setattr(
        'shared.fjms_service._domain_vocabulary_is_loadable', lambda: True, raising=False,
    )
    monkeypatch.setattr(
        'shared.fjms_service.is_valid_domain_token',
        lambda v: v == 'Other (Bible)' or v == 'Piyyut',
    )
    # Should not raise.
    validate_filter_values({'domains': ['Other (Bible)']})
    validate_filter_values({'domains': ['Piyyut']})


def test_validate_filter_values_parent_domain_accepted(monkeypatch):
    """R2-#3: parent-domain tokens that get_filter_sys_ids resolves via the
    UNION on ParentDomain (shared/fjms_service.py:976) MUST be accepted."""
    from shared.fjms_service import validate_filter_values
    monkeypatch.setattr(
        'shared.fjms_service._domain_vocabulary_is_loadable', lambda: True, raising=False,
    )
    monkeypatch.setattr(
        'shared.fjms_service.is_valid_domain_token',
        lambda v: v == 'Liturgy',  # parent-domain token
    )
    validate_filter_values({'domains': ['Liturgy']})


def test_validate_filter_values_unknown_domain_rejected(monkeypatch):
    """R2-#3: bogus tokens MUST raise APIError(http_status=400)."""
    from shared.fjms_service import validate_filter_values
    monkeypatch.setattr(
        'shared.fjms_service._domain_vocabulary_is_loadable', lambda: True, raising=False,
    )
    monkeypatch.setattr('shared.fjms_service.is_valid_domain_token', lambda v: False)
    with pytest.raises(APIError) as exc_info:
        validate_filter_values({'domains': ['__bogus_domain_xyz__']})
    err = exc_info.value
    assert err.http_status == 400, err.http_status
    # locked to ERROR_CODES vocabulary; 'invalid_filter_value' is not registered
    assert err.code == 'unresolvable_filter_value', err.code


def test_validate_filter_values_domain_vocabulary_unavailable_fails_closed(monkeypatch):
    """R2-#3 fail-closed: when is_valid_domain_token (or its loader) raises,
    validate_filter_values MUST raise APIError(http_status=503), NOT silently
    allow the request."""
    from shared.fjms_service import validate_filter_values

    def boom(_v):
        raise RuntimeError('FJMS sidecar unreachable')

    monkeypatch.setattr('shared.fjms_service.is_valid_domain_token', boom)
    with pytest.raises(APIError) as exc_info:
        validate_filter_values({'domains': ['Piyyut']})
    err = exc_info.value
    assert err.http_status == 503, f'expected 503 fail-closed, got {err.http_status}'
    assert err.code in ('filter_vocabulary_unavailable', 'unresolvable_filter_value'), err.code


def test_validate_filter_values_empty_domain_vocabulary_fails_closed(monkeypatch):
    """R2-#3: empty domain vocabulary (e.g., _conn is None and get_all_domains
    returns []) MUST also fail closed. The validator MUST detect that the
    vocabulary loader returned an empty set and reject — NOT silently allow-all
    via 'if valid_domains and v not in valid_domains' degradation."""
    from shared.fjms_service import validate_filter_values
    monkeypatch.setattr('shared.fjms_service._domain_vocabulary_is_loadable', lambda: False, raising=False)
    monkeypatch.setattr('shared.fjms_service.is_valid_domain_token', lambda v: False)
    with pytest.raises(APIError) as exc_info:
        validate_filter_values({'domains': ['Piyyut']})
    err = exc_info.value
    assert err.http_status == 503, f'expected 503, got {err.http_status}'


def test_validate_filter_values_materials_vocabulary_unavailable_fails_closed(monkeypatch):
    """R2-#3: materials loader exception → APIError(503). NEVER allow-all."""
    from shared.fjms_service import validate_filter_values, get_fjms_service
    fjms = get_fjms_service(thread_safe=True)

    def boom(self):
        raise RuntimeError('catalog_fields query failed')

    monkeypatch.setattr(type(fjms), '_discover_valid_materials', boom, raising=False)
    with pytest.raises(APIError) as exc_info:
        validate_filter_values({'materials': ['Parchment']})
    err = exc_info.value
    assert err.http_status == 503, f'expected 503, got {err.http_status}'


def test_validate_filter_values_empty_materials_vocabulary_fails_closed(monkeypatch):
    """R2-#3: empty materials vocabulary MUST reject, NOT allow-all.
    This is the headline regression Codex flagged: current pseudocode
    `if valid_materials and v not in valid_materials` becomes a no-op when
    valid_materials is empty (e.g., _conn is None branch)."""
    from shared.fjms_service import validate_filter_values, get_fjms_service
    fjms = get_fjms_service(thread_safe=True)
    monkeypatch.setattr(type(fjms), '_discover_valid_materials',
                        lambda self: set(), raising=False)
    with pytest.raises(APIError) as exc_info:
        validate_filter_values({'materials': ['Parchment']})
    err = exc_info.value
    assert err.http_status == 503, (
        f'expected 503 fail-closed for empty materials vocabulary, got {err.http_status}; '
        'this is the API-07 regression — empty vocabulary MUST NOT degrade to allow-all'
    )


# R2-#1 — thread-local lifecycle ---------------------------------------

def test_responsa_downgrade_threadlocal_cleared_on_exception(monkeypatch, mock_meta_mgr, clean_env):
    """R2-#1: if execute_search raises AFTER the cascade signal was set, the
    next request on the same worker thread MUST NOT inherit the stale warning.
    Plan 03's execute_search must consume-on-entry; the handler must consume
    in a try/finally."""
    from web.state import state
    from genizah_core import _set_last_responsa_downgrade  # noqa: F401 — must exist after Plan 02

    # Stage a stale signal in the thread-local AS IF a prior request crashed
    # before consuming.
    _set_last_responsa_downgrade('STALE_FROM_PRIOR_REQUEST')

    saved = state.searcher
    try:
        # Successful execute_search; if Plan 03 consumes on entry as required,
        # the stale signal will NOT leak into this response.
        fake = MagicMock()
        fake.execute_search.return_value = []
        state.searcher = fake

        bare = FastAPI()
        init_search_api(app_override=bare)
        with TestClient(bare) as c:
            r = c.post('/api/search', json={'query': 'x', 'search_mode': 'exact'})
        assert r.status_code == 200, r.json()
        warnings = r.json().get('warnings') or []
        leaked = [w for w in warnings if 'STALE_FROM_PRIOR_REQUEST' in str(w)]
        assert not leaked, (
            f'thread-local downgrade signal leaked across requests: {warnings!r}. '
            'Plan 03 must clear-on-entry inside SearchEngine.execute_search.'
        )
    finally:
        state.searcher = saved


# R2-#2 — app-state idempotency ----------------------------------------

def test_init_search_api_uses_app_state_not_module_global():
    """R2-#2: init_search_api MUST mark idempotency on target_app.state.
    Module-global _INITIALIZED_APPS: set[int] is brittle (id(app) GC reuse)
    and pollutes test isolation."""
    bare1 = FastAPI()
    init_search_api(app_override=bare1)
    assert getattr(bare1.state, 'search_api_initialized', False) is True, (
        'expected target_app.state.search_api_initialized = True after init'
    )
    # A SECOND independent app must NOT inherit the flag.
    bare2 = FastAPI()
    assert getattr(bare2.state, 'search_api_initialized', False) is False, (
        'second app inherited flag — implementation likely uses a module-global '
        'set instead of target_app.state (R2-#2 regression)'
    )
    init_search_api(app_override=bare2)
    assert getattr(bare2.state, 'search_api_initialized', False) is True
    # Both apps now have routes — verify each has /api/search exactly once.
    for bare in (bare1, bare2):
        matches = [r for r in bare.routes if getattr(r, 'path', None) == '/api/search']
        assert len(matches) == 1, f'{bare}: /api/search registered {len(matches)} times'


# ====================================================================
# Phase 81A — old `mode` field rejection (D-13)
# ====================================================================

def test_old_mode_field_rejected_with_helpful_message(client, populated_state, clean_env):
    """81A D-13 -- sending the old `mode` field returns 400 invalid_request
    with a message that names both the old and new field names so skill authors
    can find the migration path."""
    resp = client.post('/api/search', json={'query': 'foo', 'mode': 'text'})
    assert resp.status_code == 400, resp.json()
    body = resp.json()
    assert body['error']['code'] == 'invalid_request', body
    msg = body['error']['message']
    # Both `mode` and `search_mode` must appear so skill authors can find the migration path.
    assert 'mode' in msg and 'search_mode' in msg, msg
    # Specifically, the cutover string from the handler:
    assert "unknown field 'mode'" in msg, msg
