"""Phase 78 D-22 form 1: @pytest.mark.slow soak test for the sliding-window
rate limiter. Demonstrates honest Retry-After.

Run explicitly with `pytest -m slow tests/test_search_api_soak.py`.

Concern #7 (78-REVIEWS.md): pyproject.toml registers the `slow` marker for
--strict-markers tooling, but does NOT add a repo-wide addopts default-exclude.
Slow tests are run via explicit `-m slow` opt-in. See tests/README.md for
the documented invocation.
"""

import pytest
from unittest.mock import MagicMock
from fastapi import FastAPI
from fastapi.testclient import TestClient


@pytest.mark.slow
def test_rate_limit_soak(monkeypatch):
    """50 quick requests with SEARCH_API_RATE_LIMIT=30 must produce >=15 429s,
    each carrying a parseable Retry-After header and the rate_limited envelope code."""
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '30')
    monkeypatch.setenv('SEARCH_API_MODE', 'open')
    monkeypatch.setenv('SEARCH_API_POSTHOG_SAMPLE_N', '999999')

    # R2-#2 — reset module-global rate-limiter state so prior tests don't pollute.
    from web.search_api import _rate_limiter
    _rate_limiter.reset_for_tests()

    from web.state import state
    saved_searcher, saved_meta = state.searcher, state.meta_mgr
    fake = MagicMock()
    fake.execute_search.return_value = []
    state.searcher = fake
    state.meta_mgr = MagicMock()
    state.meta_mgr.parse_full_id_components.return_value = {
        'sys_id': 'X', 'ie_id': 'X', 'p_num': '1', 'fl_id': None,
    }

    try:
        from web.search_api import init_search_api
        bare = FastAPI()
        init_search_api(app_override=bare)
        with TestClient(bare) as client:
            responses = [
                client.post('/api/search', json={'query': 'soak', 'mode': 'text'})
                for _ in range(50)
            ]
        rate_limited = [r for r in responses if r.status_code == 429]
        assert len(rate_limited) >= 15, (
            f"expected >=15 rate-limited responses with cap=30 over 50 requests; got "
            f"{len(rate_limited)} (statuses: {[r.status_code for r in responses]})"
        )
        for r in rate_limited:
            retry_after = r.headers.get('Retry-After')
            assert retry_after is not None, "Retry-After header missing on 429"
            assert int(retry_after) >= 1
            body = r.json()
            assert body.get('error', {}).get('code') == 'rate_limited', body
    finally:
        state.searcher = saved_searcher
        state.meta_mgr = saved_meta


@pytest.mark.slow
def test_rate_limit_recovers_after_window(monkeypatch):
    """After 60s the sliding window drains; subsequent request succeeds."""
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '5')
    monkeypatch.setenv('SEARCH_API_MODE', 'open')
    monkeypatch.setenv('SEARCH_API_POSTHOG_SAMPLE_N', '999999')

    fake_time = [1000.0]
    monkeypatch.setattr('web.api_hardening.time.time', lambda: fake_time[0])

    # R2-#2 — reset module-global rate-limiter state so prior tests don't pollute.
    from web.search_api import _rate_limiter
    _rate_limiter.reset_for_tests()

    from web.state import state
    saved_searcher, saved_meta = state.searcher, state.meta_mgr
    state.searcher = MagicMock()
    state.searcher.execute_search.return_value = []
    state.meta_mgr = MagicMock()
    state.meta_mgr.parse_full_id_components.return_value = {
        'sys_id': 'X', 'ie_id': 'X', 'p_num': '1', 'fl_id': None,
    }

    try:
        from web.search_api import init_search_api
        bare = FastAPI()
        init_search_api(app_override=bare)
        with TestClient(bare) as client:
            for _ in range(6):
                client.post('/api/search', json={'query': 's', 'mode': 'text'})
            r = client.post('/api/search', json={'query': 's', 'mode': 'text'})
            assert r.status_code == 429, r.json()
            fake_time[0] = 1061.0
            r2 = client.post('/api/search', json={'query': 's', 'mode': 'text'})
            assert r2.status_code == 200, r2.json()
    finally:
        state.searcher = saved_searcher
        state.meta_mgr = saved_meta


@pytest.mark.slow
def test_retry_after_honest_in_sliding_window(monkeypatch):
    """Retry-After at t=0 should be ~60; at t=30 should be ~30; at t=59 should be ~1."""
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '2')
    monkeypatch.setenv('SEARCH_API_MODE', 'open')
    monkeypatch.setenv('SEARCH_API_POSTHOG_SAMPLE_N', '999999')

    fake_time = [2000.0]
    monkeypatch.setattr('web.api_hardening.time.time', lambda: fake_time[0])

    # R2-#2 — reset module-global rate-limiter state so prior tests don't pollute.
    from web.search_api import _rate_limiter
    _rate_limiter.reset_for_tests()

    from web.state import state
    saved_searcher, saved_meta = state.searcher, state.meta_mgr
    state.searcher = MagicMock()
    state.searcher.execute_search.return_value = []
    state.meta_mgr = MagicMock()
    state.meta_mgr.parse_full_id_components.return_value = {
        'sys_id': 'X', 'ie_id': 'X', 'p_num': '1', 'fl_id': None,
    }

    try:
        from web.search_api import init_search_api
        bare = FastAPI()
        init_search_api(app_override=bare)
        with TestClient(bare) as client:
            client.post('/api/search', json={'query': 's', 'mode': 'text'})
            client.post('/api/search', json={'query': 's', 'mode': 'text'})
            r0 = client.post('/api/search', json={'query': 's', 'mode': 'text'})
            assert r0.status_code == 429
            ra0 = int(r0.headers['Retry-After'])
            assert 58 <= ra0 <= 60, f"at t=0, Retry-After should be ~60, got {ra0}"

            fake_time[0] = 2030.0
            r30 = client.post('/api/search', json={'query': 's', 'mode': 'text'})
            assert r30.status_code == 429
            ra30 = int(r30.headers['Retry-After'])
            assert 28 <= ra30 <= 32

            fake_time[0] = 2059.0
            r59 = client.post('/api/search', json={'query': 's', 'mode': 'text'})
            assert r59.status_code == 429
            ra59 = int(r59.headers['Retry-After'])
            assert ra59 == 1
    finally:
        state.searcher = saved_searcher
        state.meta_mgr = saved_meta
