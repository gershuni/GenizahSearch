"""
Phase 78 Plan 01 (Wave 0 RED) — helper-level test scaffold for web/api_hardening.py.

Tests fail at IMPORT time today because `web.api_hardening` and `shared.api_errors`
do not yet exist. Plan 02 must produce these modules with the exact symbol names
imported below — that is the contract this RED scaffold locks.

Coverage:
- Concern #1: _resolve_rate_limit_key vs _is_loopback_request distinct helpers
- Concern #3: APIError imported from shared.api_errors (re-exported by web layer)
- Concern #4: XFF spoof rejection — every XFF entry must be loopback
- Concern #5: RateLimiter bucket TTL eviction
- Concern #9: PostHog dropped event counter
- D-01..D-14 surface: sliding-window rate limiter, retry-after, mode gate,
  IP-hash, latency/result-count buckets, capture_api_event sampling/non-blocking
- R2-#2: RateLimiter.reset_for_tests() for test isolation
"""

import os
import time
import pytest
from unittest.mock import MagicMock
from fastapi import FastAPI  # noqa: F401  — used in wrap_endpoint tests
from fastapi.testclient import TestClient  # noqa: F401

from web.api_hardening import (  # noqa: F401
    RateLimiter,
    enforce_mode_gate,
    wrap_endpoint,
    _build_envelope_response,
    _resolve_rate_limit_key,
    _is_loopback_request,
    hash_ip,
    latency_bucket,
    result_count_bucket,
    capture_api_event,
    get_dropped_event_count,
    LOOPBACK_IPS,
    ERROR_CODES,
    RATE_LIMIT_BUCKET_TTL,  # Concern #5
)
from shared.api_errors import APIError  # Concern #3


# ---------------------------------------------------------------------------
# Concern #1 — _resolve_rate_limit_key (rate-limit IP source)
# ---------------------------------------------------------------------------

def test_resolve_rate_limit_key_direct_peer_non_loopback():
    """Peer is not a trusted proxy → XFF ignored; key = peer."""
    req = MagicMock()
    req.client.host = '8.8.8.8'
    req.headers = {'x-forwarded-for': '203.0.113.99'}  # attacker-supplied
    assert _resolve_rate_limit_key(req) == '8.8.8.8'


def test_resolve_rate_limit_key_trusted_proxy_uses_rightmost_xff():
    """Peer in trusted set → use right-most untrusted XFF entry."""
    req = MagicMock()
    req.client.host = '127.0.0.1'
    req.headers = {'x-forwarded-for': '8.8.8.8, 1.2.3.4'}
    # Right-most entry = '1.2.3.4' (the entry nginx APPENDED, the real client).
    assert _resolve_rate_limit_key(req) == '1.2.3.4'


def test_resolve_rate_limit_key_trusted_proxy_empty_xff_returns_peer():
    req = MagicMock()
    req.client.host = '127.0.0.1'
    req.headers = {}
    assert _resolve_rate_limit_key(req) == '127.0.0.1'


def test_resolve_rate_limit_key_trusted_proxy_multi_hop_xff():
    """Multi-hop XFF: walk right-to-left, return right-most untrusted entry.
    With the default trusted set being only loopback, every XFF entry is
    untrusted, so the right-most entry wins."""
    req = MagicMock()
    req.client.host = '127.0.0.1'
    req.headers = {'x-forwarded-for': 'client.ip, hop1.ip, hop2.ip'}
    assert _resolve_rate_limit_key(req) == 'hop2.ip'


def test_resolve_rate_limit_key_custom_trusted_proxies_env(monkeypatch):
    """Add an extra trusted proxy via _TRUSTED_PROXIES; right-most untrusted wins."""
    import web.api_hardening as ah
    saved = getattr(ah, '_TRUSTED_PROXIES', None)
    try:
        # Inject 10.0.0.5 into the trusted set.
        new_trusted = set(saved) if saved is not None else set()
        new_trusted |= {'127.0.0.1', '::1', '10.0.0.5'}
        monkeypatch.setattr(ah, '_TRUSTED_PROXIES', new_trusted, raising=False)
        req = MagicMock()
        req.client.host = '10.0.0.5'
        req.headers = {'x-forwarded-for': 'real.ip'}
        assert _resolve_rate_limit_key(req) == 'real.ip'
    finally:
        if saved is not None:
            monkeypatch.setattr(ah, '_TRUSTED_PROXIES', saved, raising=False)


# ---------------------------------------------------------------------------
# Concern #1 + #4 — _is_loopback_request (mode-gate trust decision)
# ---------------------------------------------------------------------------

def test_is_loopback_request_direct_loopback_no_xff():
    req = MagicMock()
    req.client.host = '127.0.0.1'
    req.headers = {}
    assert _is_loopback_request(req) is True


def test_is_loopback_request_direct_loopback_all_loopback_xff():
    req = MagicMock()
    req.client.host = '127.0.0.1'
    req.headers = {'x-forwarded-for': '127.0.0.1, ::1'}
    assert _is_loopback_request(req) is True


def test_is_loopback_request_xff_spoof_127_then_external_rejected():
    """Concern #4: classic spoof — peer is loopback, attacker prepends 127.0.0.1
    to XFF. Plan 02 explicitly drops the .split(',')[0] rule and requires EVERY
    XFF entry to be loopback."""
    req = MagicMock()
    req.client.host = '127.0.0.1'
    req.headers = {'x-forwarded-for': '127.0.0.1, 203.0.113.5'}
    assert _is_loopback_request(req) is False, (
        "must reject when ANY XFF entry is non-loopback (concern #4)"
    )


def test_is_loopback_request_external_peer_rejected():
    req = MagicMock()
    req.client.host = '8.8.8.8'
    req.headers = {}
    assert _is_loopback_request(req) is False


def test_is_loopback_request_rfc1918_peer_rejected():
    """D-03: RFC1918 ranges (10.*, 192.168.*, 172.16-31.*) are NOT loopback."""
    for peer in ('10.0.0.5', '192.168.1.10', '172.20.5.5'):
        req = MagicMock()
        req.client.host = peer
        req.headers = {}
        assert _is_loopback_request(req) is False, f"peer={peer} should reject"


def test_is_loopback_request_xff_with_whitespace():
    """Trim whitespace per XFF entry."""
    req = MagicMock()
    req.client.host = '127.0.0.1'
    req.headers = {'x-forwarded-for': '127.0.0.1 , ::1'}
    assert _is_loopback_request(req) is True


def test_is_loopback_request_ipv6_loopback():
    """::1 is loopback."""
    req = MagicMock()
    req.client.host = '::1'
    req.headers = {}
    assert _is_loopback_request(req) is True


# ---------------------------------------------------------------------------
# Sliding-window RateLimiter — happy path + blocking + Retry-After
# ---------------------------------------------------------------------------

def test_rate_limiter_allows_under_limit():
    """check() returns the bucket state for IPs under the limit, no APIError raised."""
    rl = RateLimiter(default_limit=10)
    for _ in range(5):
        rl.check('1.2.3.4')  # must not raise


def test_rate_limiter_blocks_at_limit(monkeypatch):
    """Limit reached → APIError(rate_limited, http_status=429)."""
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '3')
    rl = RateLimiter(default_limit=3)
    for _ in range(3):
        rl.check('1.2.3.4')
    with pytest.raises(APIError) as exc_info:
        rl.check('1.2.3.4')
    err = exc_info.value
    assert err.code == 'rate_limited'
    assert err.http_status == 429


def test_rate_limiter_retry_after_meaningful(monkeypatch):
    """Retry-After header is propagated and is integer >= 1."""
    fake_now = [1000.0]
    monkeypatch.setattr('web.api_hardening.time.time', lambda: fake_now[0])
    rl = RateLimiter(default_limit=2)
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '2')
    rl.check('1.2.3.4')
    fake_now[0] = 1010.0
    rl.check('1.2.3.4')
    fake_now[0] = 1015.0
    with pytest.raises(APIError) as exc_info:
        rl.check('1.2.3.4')
    err = exc_info.value
    headers = getattr(err, 'headers', {}) or {}
    ra = headers.get('Retry-After') or headers.get('retry-after')
    assert ra is not None, headers
    assert int(ra) >= 1


def test_rate_limiter_per_ip_isolation():
    """Limit on one IP does NOT spill to another."""
    rl = RateLimiter(default_limit=2)
    rl.check('1.1.1.1')
    rl.check('1.1.1.1')
    # Different IP — must still pass.
    rl.check('2.2.2.2')


def test_rate_limiter_env_reread(monkeypatch):
    """SEARCH_API_RATE_LIMIT is re-read per call (D-02)."""
    rl = RateLimiter(default_limit=2)
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '2')
    rl.check('1.1.1.1')
    rl.check('1.1.1.1')
    # Bump limit at runtime — the next call should NOT raise.
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '10')
    rl.check('1.1.1.1')  # must not raise


# ---------------------------------------------------------------------------
# Concern #5 — RateLimiter bucket eviction
# ---------------------------------------------------------------------------

def test_rate_limiter_evicts_stale_buckets(monkeypatch):
    """RateLimiter._buckets must shed empty, time-out-of-band entries.

    Concern #5: without eviction, every unique IP that ever hit the endpoint
    would accumulate a deque entry forever — a memory growth vector under scans."""
    fake_now = [1000.0]
    monkeypatch.setattr('web.api_hardening.time.time', lambda: fake_now[0])

    rl = RateLimiter(default_limit=10)
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '10')

    for i in range(100):
        rl.check(f'10.0.0.{i}')
    assert len(rl._buckets) == 100

    fake_now[0] = 1000.0 + RATE_LIMIT_BUCKET_TTL + 60.0  # +TTL +1 minute

    rl.check('new_ip')

    # All 100 prior buckets had deques with single entries at t=1000; those
    # entries are >60s old so the deques drained. Then the bucket itself is
    # evicted because deque is empty AND last_seen > TTL.
    assert len(rl._buckets) <= 2, (
        f"eviction failed: {len(rl._buckets)} buckets remain after TTL"
    )


def test_rate_limiter_eviction_does_not_kick_active_buckets(monkeypatch):
    """Active buckets (non-empty deque) must NOT be evicted even if they crossed TTL."""
    fake_now = [1000.0]
    monkeypatch.setattr('web.api_hardening.time.time', lambda: fake_now[0])
    rl = RateLimiter(default_limit=10)
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '10')

    for _ in range(5):
        rl.check('busy')

    fake_now[0] = 1030.0  # 30s later — entries still within window.
    rl.check('trigger')  # new IP triggers eviction sweep.

    assert 'busy' in rl._buckets, "active bucket evicted prematurely"


# ---------------------------------------------------------------------------
# Mode gate — enforce_mode_gate
# ---------------------------------------------------------------------------

def test_enforce_mode_gate_disabled_raises_apierror(monkeypatch):
    monkeypatch.setenv('SEARCH_API_MODE', 'disabled')
    req = MagicMock()
    req.client.host = '127.0.0.1'
    req.headers = {}
    with pytest.raises(APIError) as exc_info:
        enforce_mode_gate(req)
    assert exc_info.value.code == 'disabled'
    assert exc_info.value.http_status == 503


def test_enforce_mode_gate_localhost_only_pass_loopback(monkeypatch):
    monkeypatch.setenv('SEARCH_API_MODE', 'localhost-only')
    req = MagicMock()
    req.client.host = '127.0.0.1'
    req.headers = {}
    # Should NOT raise.
    enforce_mode_gate(req)


def test_enforce_mode_gate_localhost_only_fail_external(monkeypatch):
    monkeypatch.setenv('SEARCH_API_MODE', 'localhost-only')
    req = MagicMock()
    req.client.host = '8.8.8.8'
    req.headers = {}
    with pytest.raises(APIError) as exc_info:
        enforce_mode_gate(req)
    assert exc_info.value.code == 'localhost_only'
    assert exc_info.value.http_status == 403


def test_enforce_mode_gate_open_default(monkeypatch):
    monkeypatch.setenv('SEARCH_API_MODE', 'open')
    req = MagicMock()
    req.client.host = '8.8.8.8'
    req.headers = {}
    enforce_mode_gate(req)  # must not raise


def test_enforce_mode_gate_unset_defaults_to_open(monkeypatch):
    monkeypatch.delenv('SEARCH_API_MODE', raising=False)
    req = MagicMock()
    req.client.host = '8.8.8.8'
    req.headers = {}
    enforce_mode_gate(req)  # must not raise


# ---------------------------------------------------------------------------
# wrap_endpoint / _build_envelope_response (Concern #2 — per-endpoint envelope)
# ---------------------------------------------------------------------------

def test_build_envelope_response_apierror():
    """_build_envelope_response wraps an APIError into the {error:{code,message}} envelope."""
    e = APIError('rate_limited', 'Too many requests', http_status=429,
                 headers={'Retry-After': '5'})
    resp = _build_envelope_response(e)
    # Status code reflects the APIError.
    assert resp.status_code == 429
    # Body is the envelope.
    import json as _json
    body = _json.loads(resp.body.decode('utf-8'))
    assert body['error']['code'] == 'rate_limited'
    assert body['error']['message'] == 'Too many requests'
    # Headers propagated.
    assert resp.headers.get('Retry-After') == '5' or resp.headers.get('retry-after') == '5'


def test_apierror_has_headers_attribute():
    """APIError must accept a `headers` kwarg for Retry-After propagation."""
    e = APIError('rate_limited', 'slow down', http_status=429,
                 headers={'Retry-After': '7'})
    assert e.code == 'rate_limited'
    assert e.http_status == 429
    assert e.headers == {'Retry-After': '7'}


# ---------------------------------------------------------------------------
# IP-hash determinism (D-11)
# ---------------------------------------------------------------------------

def test_hash_ip_deterministic():
    """Same input → same hash."""
    a = hash_ip('1.2.3.4')
    b = hash_ip('1.2.3.4')
    assert a == b
    assert isinstance(a, str)
    assert len(a) == 16  # D-11: hexdigest()[:16]


def test_hash_ip_distinct_for_distinct_ips():
    a = hash_ip('1.2.3.4')
    b = hash_ip('5.6.7.8')
    assert a != b


# ---------------------------------------------------------------------------
# Bucket helpers (D-12)
# ---------------------------------------------------------------------------

def test_latency_bucket_boundaries():
    """D-12: lt_100ms < 100ms; lt_500ms covers [100ms,500ms); etc."""
    assert latency_bucket(0.05) == 'lt_100ms'
    assert latency_bucket(0.099) == 'lt_100ms'
    assert latency_bucket(0.1) == 'lt_500ms'
    assert latency_bucket(0.4999) == 'lt_500ms'
    assert latency_bucket(0.5) == 'lt_2s'
    assert latency_bucket(1.999) == 'lt_2s'
    assert latency_bucket(2.0) == 'lt_10s'
    assert latency_bucket(9.999) == 'lt_10s'
    assert latency_bucket(10.0) == 'gte_10s'
    assert latency_bucket(60.0) == 'gte_10s'


def test_result_count_bucket_boundaries():
    """D-12: zero / count_1_10 / count_11_50 / count_51_200."""
    assert result_count_bucket(0) == 'zero'
    assert result_count_bucket(1) == 'count_1_10'
    assert result_count_bucket(10) == 'count_1_10'
    assert result_count_bucket(11) == 'count_11_50'
    assert result_count_bucket(50) == 'count_11_50'
    assert result_count_bucket(51) == 'count_51_200'
    assert result_count_bucket(200) == 'count_51_200'


# ---------------------------------------------------------------------------
# Concern #9 — PostHog dropped event counter
# ---------------------------------------------------------------------------

def test_posthog_dropped_event_counter_increments(monkeypatch):
    """When the event queue is full, drops MUST be counted."""
    import queue as _q
    full_q = _q.Queue(maxsize=2)
    full_q.put_nowait({'event': 'pre1'})
    full_q.put_nowait({'event': 'pre2'})
    monkeypatch.setattr('web.api_hardening._event_queue', full_q)
    monkeypatch.setenv('SEARCH_API_POSTHOG_SAMPLE_N', '1')

    starting = get_dropped_event_count()
    for _ in range(5):
        capture_api_event(
            endpoint='search', mode='text', latency_seconds=0.05,
            result_count=0, status_code=200, error_code=None,
            client_ip='127.0.0.1',
        )
    ending = get_dropped_event_count()
    assert ending - starting >= 3, (
        f"dropped counter did not increment: started at {starting}, ended at {ending}"
    )


def test_get_dropped_event_count_returns_non_negative_integer():
    n = get_dropped_event_count()
    assert isinstance(n, int) and n >= 0


# ---------------------------------------------------------------------------
# capture_api_event sampling + non-blocking + no-payload
# ---------------------------------------------------------------------------

def test_capture_api_event_non_blocking(monkeypatch):
    """capture_api_event must not block (returns quickly) — D-10."""
    monkeypatch.setenv('SEARCH_API_POSTHOG_SAMPLE_N', '1')
    t0 = time.monotonic()
    capture_api_event(
        endpoint='search', mode='text', latency_seconds=0.05,
        result_count=0, status_code=200, error_code=None,
        client_ip='127.0.0.1',
    )
    elapsed = time.monotonic() - t0
    assert elapsed < 0.5, f"capture_api_event blocked for {elapsed}s — must be fire-and-forget"


def test_capture_api_event_sampling(monkeypatch):
    """SEARCH_API_POSTHOG_SAMPLE_N=999999 → almost no events captured."""
    monkeypatch.setenv('SEARCH_API_POSTHOG_SAMPLE_N', '999999')
    captured = []

    class FakeQueue:
        def put_nowait(self, item):
            captured.append(item)

    monkeypatch.setattr('web.api_hardening._event_queue', FakeQueue())
    for _ in range(100):
        capture_api_event(
            endpoint='search', mode='text', latency_seconds=0.05,
            result_count=0, status_code=200, error_code=None,
            client_ip='127.0.0.1',
        )
    # With sample N very high, almost no events should pass.
    assert len(captured) <= 1, f"expected <=1 captured, got {len(captured)}"


def test_capture_api_event_does_not_log_query_or_filters(monkeypatch):
    """HARDEN-05: the event payload MUST NOT include query, filters, gap, snippets, full_text."""
    monkeypatch.setenv('SEARCH_API_POSTHOG_SAMPLE_N', '1')
    captured = []

    class FakeQueue:
        def put_nowait(self, item):
            captured.append(item)

    monkeypatch.setattr('web.api_hardening._event_queue', FakeQueue())
    capture_api_event(
        endpoint='search', mode='text', latency_seconds=0.05,
        result_count=42, status_code=200, error_code=None,
        client_ip='127.0.0.1',
    )
    assert len(captured) >= 1
    item = captured[0]
    # Allowed keys only — no query/filters/gap/snippet/full_text.
    serialized = str(item).lower()
    for forbidden in ('query', 'filters', 'gap', 'snippet', 'full_text'):
        # The forbidden strings must not appear as keys/values in the payload.
        # We allow them in property KEY names like 'mode' (not 'query'), so
        # check for actual leak: if the payload contains a key `query`, fail.
        if isinstance(item, dict):
            properties = item.get('properties', item)
            assert 'query' not in (properties or {}), f"query leaked: {item}"
            assert 'filters' not in (properties or {}), f"filters leaked: {item}"
            assert 'gap' not in (properties or {}), f"gap leaked: {item}"
            assert 'snippet' not in (properties or {}), f"snippet leaked: {item}"
            assert 'full_text' not in (properties or {}), f"full_text leaked: {item}"


# ---------------------------------------------------------------------------
# Constants exported (sanity)
# ---------------------------------------------------------------------------

def test_loopback_ips_constant_is_set_with_127_and_ipv6():
    assert isinstance(LOOPBACK_IPS, (set, frozenset))
    assert '127.0.0.1' in LOOPBACK_IPS
    assert '::1' in LOOPBACK_IPS


def test_error_codes_taxonomy_includes_locked_codes():
    """D-07: stable error codes for the Phase 81 skill consumer."""
    locked = {
        'invalid_request', 'invalid_mode', 'query_required', 'query_too_long',
        'limit_too_high', 'unknown_filter_key', 'unresolvable_filter_value',
        'rate_limited', 'disabled', 'localhost_only', 'internal_error',
    }
    if isinstance(ERROR_CODES, dict):
        present = set(ERROR_CODES.keys())
    else:
        present = set(ERROR_CODES)
    missing = locked - present
    assert not missing, f"ERROR_CODES missing locked codes: {missing}"


# ---------------------------------------------------------------------------
# Concern #3 — APIError dependency-inversion lock
# ---------------------------------------------------------------------------

def test_apierror_imported_from_shared_api_errors_not_web():
    """Plan 02 must move APIError to shared/api_errors.py (Concern #3 — fix shared→web inversion).
    web/api_hardening.py re-exports it for legacy import paths, but the class object is the same."""
    from shared.api_errors import APIError as A
    from web.api_hardening import APIError as B
    assert A is B, "web.api_hardening.APIError must re-export shared.api_errors.APIError, not redefine it"
    e = A('rate_limited', 'msg', http_status=429, headers={'Retry-After': '5'})
    assert e.code == 'rate_limited'
    assert e.http_status == 429
    assert e.headers == {'Retry-After': '5'}


# ====================================================================
# Round-2 revision: rate_limiter test-isolation reset (R2-#2)
# ====================================================================

def test_rate_limiter_reset_for_tests_clears_buckets():
    """R2-#2: module-global _rate_limiter accumulates state across tests.
    Plan 02 must expose _rate_limiter.reset_for_tests() (or equivalent)
    so test fixtures can clear bucket state between requests."""
    from web.api_hardening import RateLimiter
    rl = RateLimiter(default_limit=5)
    for ip in ('a.b.c.d', 'e.f.g.h'):
        for _ in range(3):
            rl.check(ip)
    assert len(rl._buckets) == 2, rl._buckets
    assert hasattr(rl, 'reset_for_tests'), (
        'RateLimiter.reset_for_tests() is required by Plan 02 for test isolation '
        '(R2-#2). Without it, the module-global _rate_limiter pollutes test runs.'
    )
    rl.reset_for_tests()
    assert len(rl._buckets) == 0, f'after reset, buckets should be empty, got {rl._buckets!r}'
