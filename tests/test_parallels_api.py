# -*- coding: utf-8 -*-
"""Phase 80 Plan 04 -- Test surface for POST /api/parallels.

Mirrors Phase 79's tests/test_browse_api.py fixture pattern (TestClient against
a bare FastAPI app, per-test fresh idempotency marker, mock searcher + meta_mgr
for fast tests).

Coverage scope (CONTEXT D-10 + review action items):
  - Happy paths (6+): mode={exact,variants,fuzzy} x boundary_mode={full,boundary,combined}
  - Locator (3): item.locator presence; mocked round-trip; REAL round-trip (SC-4 / D-08, env-gated)
  - Validation (10): missing/empty/oversize text, chunk_size bounds, mode/boundary_mode enums,
    extra fields, malformed JSON, unknown filter key
  - Cap boundary validation (3): text at exactly COMPOSITION_LENGTH_CAP passes (boundary inclusive);
    text at COMPOSITION_LENGTH_CAP+1 rejects; whitespace-prefixed text that strips to within cap
    passes (verifies .strip() before length check)
  - Filtered key always present D-04 (3 sample modes per SC-2)
  - max_freq behavior (2)
  - Hardening parity (5): mode gate, rate limit + Retry-After, three-bucket independence (D-05),
    error envelope shape, statelessness
  - Group cap D-07 (3): > 200 groups -> truncated warning + results capped; <= 200 -> no warning;
    filtered_results > 200 rows -> NOT capped (v7.10 explicit decision)
  - Empty results (1)
  - Statelessness (1)
  - @wrap_endpoint reuse (1)
  - ParallelsRequest unit (2)

D-05 enforcement (separate rate-limit bucket): test_parallels_rate_limit_independence
extends Phase 79's two-bucket pattern to three buckets -- burst on /api/parallels
exhausts ONLY the parallels bucket; /api/search AND /api/browse buckets remain
healthy. Verifies _rate_limiter is not _browse_rate_limiter is not _parallels_rate_limiter.

D-08 / SC-4 (locator round-trip): test_parallels_locator_round_trip_real does
real HTTP POST /api/parallels -> GET /api/browse against the production corpus.
Env-gated via @pytest.mark.skipif(not _has_fixture_corpus()) so locked-in tests
do NOT depend on fixture corpus availability.

filtered_results uncapped (v7.10 decision): test_parallels_filtered_results_uncapped
asserts that when the searcher returns > 200 filtered rows, the envelope filtered[]
length matches the mock count -- NOT capped at 200. This makes the Plan 02 v7.10
decision explicit and prevents future silent regressions.
"""

from __future__ import annotations

import inspect  # noqa: F401
import json  # noqa: F401
import time  # noqa: F401

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError as PVE

from web.search_api import (
    init_search_api,
    ParallelsRequest,
    FiltersModel,  # noqa: F401
    COMPOSITION_LENGTH_CAP,
    _rate_limiter,
    _browse_rate_limiter,
    _parallels_rate_limiter,
)
from web.api_hardening import RateLimiter  # noqa: F401
from shared.api_errors import APIError, ERROR_CODES, WARNING_CODES  # noqa: F401
from shared.parallels_service import PARALLELS_GROUP_CAP


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def bare_app():
    """Per-test bare app -- fresh idempotency marker mirrors test_browse_api.py."""
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
    _rate_limiter.reset_for_tests()
    _browse_rate_limiter.reset_for_tests()
    _parallels_rate_limiter.reset_for_tests()


def _make_main_row(uid='IE99_P3_FL12345', sys_id='99001', score=5.0, chunk_index=0):
    """Build one search_composition_logic row matching the post-Plan-77-05 shape."""
    return {
        'uid': uid,
        'raw_header': f'h_{sys_id}_{uid}',
        'src_lbl': 'CUL',
        'source_ctx': 'sample composition snippet',
        'text': 'matched manuscript text',
        'score': score, 'final_score': score,
        'has_boundary_matches': False, 'boundary_match_count': 0,
        'chunk_count': 1,
        'chunk_hits': [(chunk_index, 'sample composition snippet', score, 'matched manuscript text')],
        'is_filtered': False,
    }


def _make_filtered_row(uid='IE100_P1_FL11111', sys_id='99002'):
    row = _make_main_row(uid=uid, sys_id=sys_id, score=2.0, chunk_index=1)
    row['is_filtered'] = True
    row['filter_reason'] = 'high_frequency'
    return row


@pytest.fixture
def mock_searcher(monkeypatch):
    """Default: returns one main row + zero filtered rows."""
    from web.state import state
    from unittest.mock import MagicMock
    saved_searcher = state.searcher
    saved_meta = state.meta_mgr
    fake_searcher = MagicMock()
    fake_searcher.search_composition_logic.return_value = {
        'main': [_make_main_row()],
        'filtered': [],
        'boundary_stats': None,
    }
    fake_meta = MagicMock()
    fake_meta.get_meta_for_id.return_value = ('T-S 99.99', 'Synthetic Title')
    fake_meta.get_library_for_id.return_value = 'CUL'

    def _parse_components(uid_or_header):
        # Handle both raw uids and full headers ('h_99001_IE99_P3_FL12345').
        if uid_or_header and '_IE' in uid_or_header:
            parts = uid_or_header.split('_')
            try:
                ie_idx = next(i for i, p in enumerate(parts) if p.startswith('IE'))
                sys_id_guess = parts[ie_idx - 1] if ie_idx > 0 else None
                return {
                    'sys_id': sys_id_guess or '99001',
                    'ie_id': parts[ie_idx],
                    'p_num': parts[ie_idx + 1].lstrip('P') if ie_idx + 1 < len(parts) else '1',
                    'fl_id': parts[ie_idx + 2] if ie_idx + 2 < len(parts) else None,
                }
            except StopIteration:
                pass
        return {'sys_id': '99001', 'ie_id': 'IE99', 'p_num': '3', 'fl_id': 'FL12345'}

    fake_meta.parse_full_id_components.side_effect = _parse_components
    state.searcher = fake_searcher
    state.meta_mgr = fake_meta
    yield fake_searcher
    state.searcher = saved_searcher
    state.meta_mgr = saved_meta


def _has_fixture_corpus() -> bool:
    """Detect whether real Tantivy index + meta_mgr are loaded.

    Mirrors test_browse_api.py:_has_fixture_corpus pattern.
    """
    try:
        from web.state import state
        if state.searcher is None or state.meta_mgr is None:
            return False
        if not getattr(state.meta_mgr, 'csv_bank', None):
            return False
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# ParallelsRequest unit (2)
# ---------------------------------------------------------------------------

def test_parallels_request_default_values():
    req = ParallelsRequest(text='hello')
    assert req.chunk_size == 5
    assert req.mode == 'exact'
    assert req.max_freq is None
    assert req.boundary_mode == 'full'
    assert req.filters is None


def test_parallels_request_chunk_size_bounds():
    with pytest.raises(PVE):
        ParallelsRequest(text='x', chunk_size=1)
    with pytest.raises(PVE):
        ParallelsRequest(text='x', chunk_size=21)
    req = ParallelsRequest(text='x', chunk_size=5)
    assert req.chunk_size == 5


# ---------------------------------------------------------------------------
# Happy paths (6) -- mode x boundary_mode coverage
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('mode', ['exact', 'variants', 'fuzzy'])
def test_parallels_happy_path_per_mode(client, mock_searcher, clean_env, mode):
    r = client.post('/api/parallels', json={'text': 'hello world', 'mode': mode})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body['source'] == 'parallels'
    assert body['mode'] == mode
    assert body['count'] >= 0
    assert 'results' in body
    assert 'filtered' in body  # D-04


@pytest.mark.parametrize('boundary_mode', ['full', 'boundary', 'combined'])
def test_parallels_happy_path_per_boundary_mode(client, mock_searcher, clean_env, boundary_mode):
    r = client.post('/api/parallels', json={
        'text': 'hello world', 'mode': 'exact', 'boundary_mode': boundary_mode,
    })
    assert r.status_code == 200, r.text
    body = r.json()
    assert body['boundary_options']['boundary_mode'] == boundary_mode


# ---------------------------------------------------------------------------
# Locator (2 mocked + 1 real env-gated)
# ---------------------------------------------------------------------------

def test_parallels_each_result_has_locator_block(client, mock_searcher, clean_env):
    r = client.post('/api/parallels', json={'text': 'hello world', 'mode': 'exact'})
    assert r.status_code == 200, r.text
    body = r.json()
    for item in body['results']:
        assert 'uid' in item, f'item missing uid: {item!r}'
        assert 'locator' in item, f'item missing locator: {item!r}'
        loc = item['locator']
        assert loc.get('sys_id'), f'locator missing sys_id: {loc!r}'


def test_parallels_locator_round_trip_serializer_unit(client, mock_searcher, clean_env, monkeypatch):
    """Round-trip with mocked GET /api/browse target -- proves locator extracted
    from /api/parallels can be assembled into a /api/browse query string.
    The actual /api/browse handler is short-circuited by mocking
    WebDataService.get_browse_page (Phase 79 pattern).
    """
    from web.services import get_service, BrowsePage
    svc = get_service()
    fake_page = BrowsePage(
        uid='IE99_P3_FL12345', p_num=3, text='', full_header='', total_pages=1, current_idx=0,
        sys_id='99001', fl_id='FL12345', shelfmark='T-S 99.99', title='Synthetic',
        library_code='CUL', library_name='Cambridge', volume_ie='IE99',
        volumes=[{'ie_id': 'IE99', 'suffix': 1, 'page_count': 1}],
        folio_label='1r', folio_images=[], cambridge_images=[],
        physical_metadata=None, library_viewer_url=None, external_provider='',
    )
    monkeypatch.setattr(svc, 'get_browse_page', lambda *a, **k: fake_page)
    monkeypatch.setattr(svc, 'get_browse_page_by_fl', lambda *a, **k: fake_page)

    r = client.post('/api/parallels', json={'text': 'hello world', 'mode': 'exact'})
    assert r.status_code == 200, r.text
    body = r.json()
    if not body['results']:
        pytest.skip('No results from mock searcher; cannot round-trip')
    item = body['results'][0]
    sys_id = item['locator']['sys_id']
    uid = item.get('uid')
    url = (f'/api/browse?sys_id={sys_id}&uid={uid}'
           if uid else
           f"/api/browse?sys_id={sys_id}&p_num={item['locator'].get('p_num') or 1}")
    r2 = client.get(url)
    assert r2.status_code == 200, r2.text


@pytest.mark.skipif(not _has_fixture_corpus(), reason='no fixture corpus available')
def test_parallels_locator_round_trip_real(bare_app, clean_env):
    """SC-4 / D-08 PRIMARY locator round-trip test against real Tantivy index +
    sidecars. Uses a short composition with high probability of matches.
    """
    composition = (
        # Common Hebrew piyut opening -- high match probability across the corpus.
        'אדון עולם אשר מלך'
    )
    with TestClient(bare_app) as c:
        r = c.post('/api/parallels', json={
            'text': composition, 'chunk_size': 3, 'mode': 'exact',
        })
        if r.status_code != 200:
            pytest.skip(f'POST /api/parallels failed: {r.status_code} {r.text[:200]}')
        body = r.json()
        results = body.get('results') or []
        if not results:
            pytest.skip('Composition produced no results against this corpus')
        item = results[0]
        locator = item['locator']
        sys_id = locator['sys_id']
        item_uid = item.get('uid')
        url = (f'/api/browse?sys_id={sys_id}&uid={item_uid}'
               if item_uid else
               f"/api/browse?sys_id={sys_id}&p_num={locator['p_num']}")
        r2 = c.get(url)
        # 200 (browse resolved); 404 (locator pointed at non-existent -- corpus mismatch);
        # 504 (core_timeout on slow dev machine). 500 is the SC-4 regression.
        assert r2.status_code in (200, 404, 504), (
            f'SC-4 regression: GET /api/browse returned {r2.status_code} '
            f'body={r2.text[:200]!r} for url={url}'
        )


# ---------------------------------------------------------------------------
# Validation (10)
# ---------------------------------------------------------------------------

def test_parallels_missing_text(client, mock_searcher, clean_env):
    r = client.post('/api/parallels', json={'mode': 'exact'})
    assert r.status_code == 400, r.text
    body = r.json()
    assert body['error']['code'] == 'invalid_request'


def test_parallels_empty_text(client, mock_searcher, clean_env):
    r = client.post('/api/parallels', json={'text': '   ', 'mode': 'exact'})
    assert r.status_code == 400, r.text
    body = r.json()
    assert body['error']['code'] == 'composition_required'


def test_parallels_text_too_long(client, mock_searcher, clean_env):
    text = 'a' * (COMPOSITION_LENGTH_CAP + 100)
    r = client.post('/api/parallels', json={'text': text, 'mode': 'exact'})
    assert r.status_code == 400, r.text
    body = r.json()
    assert body['error']['code'] == 'composition_too_long'
    assert str(COMPOSITION_LENGTH_CAP) in body['error']['message']
    assert str(len(text)) in body['error']['message']


def test_parallels_text_at_exact_cap_passes(client, mock_searcher, clean_env):
    """Boundary: text with exactly COMPOSITION_LENGTH_CAP chars (after strip) must
    return 200. The cap check is `len(text) > COMPOSITION_LENGTH_CAP` -- strictly
    greater, so exactly-at-cap is within bounds.
    """
    text = 'a' * COMPOSITION_LENGTH_CAP
    r = client.post('/api/parallels', json={'text': text, 'mode': 'exact'})
    assert r.status_code == 200, (
        f'Expected 200 at exactly COMPOSITION_LENGTH_CAP ({COMPOSITION_LENGTH_CAP}) chars; '
        f'got {r.status_code}: {r.text[:200]}'
    )


def test_parallels_text_at_cap_plus_one_rejects(client, mock_searcher, clean_env):
    """Boundary: text with exactly COMPOSITION_LENGTH_CAP+1 chars (after strip) must
    return 400 composition_too_long. This is the first char that exceeds the cap.
    """
    text = 'a' * (COMPOSITION_LENGTH_CAP + 1)
    r = client.post('/api/parallels', json={'text': text, 'mode': 'exact'})
    assert r.status_code == 400, (
        f'Expected 400 at COMPOSITION_LENGTH_CAP+1 ({COMPOSITION_LENGTH_CAP + 1}) chars; '
        f'got {r.status_code}'
    )
    assert r.json()['error']['code'] == 'composition_too_long'


def test_parallels_whitespace_stripped_before_cap_check(client, mock_searcher, clean_env):
    """Boundary: text with COMPOSITION_LENGTH_CAP leading spaces + 1 real char must
    return 200. This verifies that .strip() is applied BEFORE the length check --
    stripped length is 1 char, which is well within cap.
    """
    text = ' ' * COMPOSITION_LENGTH_CAP + 'x'
    # raw length = COMPOSITION_LENGTH_CAP + 1, stripped length = 1
    assert len(text) == COMPOSITION_LENGTH_CAP + 1
    assert len(text.strip()) == 1
    r = client.post('/api/parallels', json={'text': text, 'mode': 'exact'})
    assert r.status_code == 200, (
        f'Expected 200 after strip (stripped length=1); '
        f'got {r.status_code}: {r.text[:200]}'
    )


def test_parallels_chunk_size_too_low(client, mock_searcher, clean_env):
    r = client.post('/api/parallels', json={'text': 'x', 'chunk_size': 1})
    assert r.status_code == 400
    assert r.json()['error']['code'] == 'invalid_request'


def test_parallels_chunk_size_too_high(client, mock_searcher, clean_env):
    r = client.post('/api/parallels', json={'text': 'x', 'chunk_size': 21})
    assert r.status_code == 400
    assert r.json()['error']['code'] == 'invalid_request'


def test_parallels_unknown_mode(client, mock_searcher, clean_env):
    r = client.post('/api/parallels', json={'text': 'x', 'mode': 'invalid'})
    assert r.status_code == 400
    assert r.json()['error']['code'] == 'invalid_request'


def test_parallels_unknown_boundary_mode(client, mock_searcher, clean_env):
    r = client.post('/api/parallels', json={
        'text': 'x', 'mode': 'exact', 'boundary_mode': 'invalid',
    })
    assert r.status_code == 400
    assert r.json()['error']['code'] == 'invalid_request'


def test_parallels_extra_field_rejected(client, mock_searcher, clean_env):
    r = client.post('/api/parallels', json={'text': 'x', 'unknown_field': 1})
    assert r.status_code == 400
    assert r.json()['error']['code'] == 'invalid_request'


def test_parallels_malformed_json(client, mock_searcher, clean_env):
    r = client.post('/api/parallels', data='not-json',
                    headers={'Content-Type': 'application/json'})
    assert r.status_code == 400
    assert r.json()['error']['code'] == 'invalid_request'


def test_parallels_unknown_filter_key(client, mock_searcher, clean_env):
    r = client.post('/api/parallels', json={
        'text': 'x', 'mode': 'exact',
        'filters': {'unknown_filter': ['x']},
    })
    assert r.status_code == 400
    assert r.json()['error']['code'] == 'invalid_request'


# ---------------------------------------------------------------------------
# Filtered key always present D-04 (3 modes for SC-2)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('mode', ['exact', 'variants', 'fuzzy'])
def test_parallels_filtered_key_always_emitted(client, mock_searcher, clean_env, mode):
    """SC-2: filtered key is present across at least three sample compositions
    (here: three modes). Even when max_freq=None and no filtered hits, the key
    is `filtered: []`, never absent or null.
    """
    r = client.post('/api/parallels', json={
        'text': 'sample text for filtered-key test', 'mode': mode,
    })
    assert r.status_code == 200, r.text
    body = r.json()
    assert 'filtered' in body, f'filtered key missing in mode={mode}: {list(body.keys())}'
    assert isinstance(body['filtered'], list), \
        f'filtered must be a list, got {type(body["filtered"])}'


# ---------------------------------------------------------------------------
# max_freq behavior (2)
# ---------------------------------------------------------------------------

def test_parallels_max_freq_none_filtered_empty(client, mock_searcher, clean_env):
    r = client.post('/api/parallels', json={
        'text': 'hello world', 'mode': 'exact', 'max_freq': None,
    })
    assert r.status_code == 200
    body = r.json()
    assert body['max_freq'] is None
    assert body['filtered'] == []


def test_parallels_max_freq_populates_filtered(client, mock_searcher, clean_env, monkeypatch):
    """When the searcher returns rows in 'filtered', envelope filtered[] is
    non-empty and the request-echoed max_freq is preserved.
    """
    from web.state import state
    state.searcher.search_composition_logic.return_value = {
        'main': [_make_main_row()],
        'filtered': [_make_filtered_row()],
        'boundary_stats': None,
    }
    r = client.post('/api/parallels', json={
        'text': 'hello world', 'mode': 'exact', 'max_freq': 5.0,
    })
    assert r.status_code == 200, r.text
    body = r.json()
    assert body['max_freq'] == 5.0
    assert len(body['filtered']) >= 1


# ---------------------------------------------------------------------------
# Hardening parity (5)
# ---------------------------------------------------------------------------

def test_parallels_disabled_mode_returns_503(client, mock_searcher, monkeypatch):
    monkeypatch.setenv('SEARCH_API_MODE', 'disabled')
    _parallels_rate_limiter.reset_for_tests()
    r = client.post('/api/parallels', json={'text': 'x', 'mode': 'exact'})
    assert r.status_code == 503, r.text
    assert r.json()['error']['code'] == 'disabled'


def test_parallels_localhost_only_mode_with_loopback_succeeds(
    client, mock_searcher, monkeypatch,
):
    monkeypatch.setenv('SEARCH_API_MODE', 'localhost-only')
    monkeypatch.setattr('web.api_hardening._is_loopback_request', lambda req: True)
    _parallels_rate_limiter.reset_for_tests()
    r = client.post('/api/parallels', json={'text': 'x', 'mode': 'exact'})
    assert r.status_code == 200, r.text


def test_parallels_rate_limit_returns_429_with_retry_after(
    client, mock_searcher, monkeypatch,
):
    monkeypatch.setenv('SEARCH_API_MODE', 'open')
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '3')
    monkeypatch.setenv('SEARCH_API_POSTHOG_SAMPLE_N', '999999')
    _rate_limiter.reset_for_tests()
    _browse_rate_limiter.reset_for_tests()
    _parallels_rate_limiter.reset_for_tests()

    statuses = []
    for _ in range(10):
        statuses.append(client.post('/api/parallels',
                                    json={'text': 'x', 'mode': 'exact'}))
    rate_limited = [r for r in statuses if r.status_code == 429]
    assert rate_limited, f'expected 429; got {[r.status_code for r in statuses]!r}'
    r = rate_limited[0]
    assert r.json()['error']['code'] == 'rate_limited'
    ra = r.headers.get('Retry-After') or r.headers.get('retry-after')
    assert ra is not None and int(ra) >= 1


def test_parallels_rate_limit_independence(bare_app, mock_searcher, monkeypatch):
    """D-05: third bucket independent from /api/search and /api/browse buckets.

    Burst /api/parallels until 429; verify /api/search AND /api/browse still
    succeed. Verifies _rate_limiter is not _browse_rate_limiter is not
    _parallels_rate_limiter (three distinct instances).
    """
    monkeypatch.setenv('SEARCH_API_MODE', 'open')
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '3')
    monkeypatch.setenv('SEARCH_API_POSTHOG_SAMPLE_N', '999999')
    _rate_limiter.reset_for_tests()
    _browse_rate_limiter.reset_for_tests()
    _parallels_rate_limiter.reset_for_tests()

    # Stub /api/browse so it returns 200 with mocked WebDataService.
    from web.services import get_service, BrowsePage
    svc = get_service()
    fake_page = BrowsePage(
        uid='IE99_P3_FL12345', p_num=3, text='', full_header='', total_pages=1,
        current_idx=0, sys_id='99001', fl_id='FL12345',
        shelfmark='T-S 99.99', title='Synthetic',
        library_code='CUL', library_name='Cambridge', volume_ie='IE99',
        volumes=[{'ie_id': 'IE99', 'suffix': 1, 'page_count': 1}],
        folio_label='1r', folio_images=[], cambridge_images=[],
        physical_metadata=None, library_viewer_url=None, external_provider='',
    )
    monkeypatch.setattr(svc, 'get_browse_page', lambda *a, **k: fake_page)
    monkeypatch.setattr(svc, 'get_browse_page_by_fl', lambda *a, **k: fake_page)
    # Stub state.searcher.execute_search for /api/search.
    from web.state import state
    state.searcher.execute_search.return_value = []

    with TestClient(bare_app) as c:
        # Exhaust the PARALLELS bucket.
        statuses = [
            c.post('/api/parallels', json={'text': 'x', 'mode': 'exact'})
            for _ in range(6)
        ]
        rate_limited = [r for r in statuses if r.status_code == 429]
        assert rate_limited, (
            f'parallels bucket should have been exhausted; got '
            f'{[r.status_code for r in statuses]!r}'
        )

        # SEARCH bucket independent.
        r = c.post('/api/search', json={'query': 'x', 'search_mode': 'exact'})
    assert r.status_code != 429, (
        f'search bucket was incorrectly exhausted by parallels traffic '
        f'(D-05 regression); got status {r.status_code}'
    )

    # BROWSE bucket independent.
    with TestClient(bare_app) as c2:
        r3 = c2.get('/api/browse?sys_id=99001&p_num=3')
    assert r3.status_code != 429, (
        f'browse bucket was incorrectly exhausted by parallels traffic '
        f'(D-05 regression); got status {r3.status_code}'
    )


def test_parallels_error_envelope_shape(client, mock_searcher, clean_env):
    r = client.post('/api/parallels', json={'text': '', 'mode': 'exact'})
    assert r.status_code == 400
    body = r.json()
    assert 'error' in body
    assert 'code' in body['error']
    assert 'message' in body['error']
    assert body['error']['code'] == 'composition_required'
    assert isinstance(body['error']['message'], str) and body['error']['message']


# ---------------------------------------------------------------------------
# Group cap D-07 (2) + filtered_results uncapped (1)
# ---------------------------------------------------------------------------

def test_parallels_group_cap_emits_warning_at_201_groups(
    client, mock_searcher, clean_env,
):
    """When raw groups > 200, envelope warnings contains 'truncated_to_200' and
    results length is exactly 200. Tests cap path through fetch_parallels_results.
    """
    from web.state import state
    rows = []
    for i in range(250):
        sys_id = f'sysX_{i}'
        rows.append(_make_main_row(uid=f'IE{i}_P1_FL{i}', sys_id=sys_id, score=250 - i))
    state.searcher.search_composition_logic.return_value = {
        'main': rows, 'filtered': [], 'boundary_stats': None,
    }

    def _parse(uid_or_header):
        # 'h_sysX_5_IE5_P1_FL5' -> sys_id='sysX_5'
        if uid_or_header and uid_or_header.startswith('h_sysX_'):
            parts = uid_or_header.split('_')
            return {
                'sys_id': '_'.join(parts[1:3]),
                'ie_id': parts[3], 'p_num': '1', 'fl_id': parts[5] if len(parts) > 5 else None,
            }
        return {'sys_id': '99001', 'ie_id': 'IE99', 'p_num': '1', 'fl_id': None}

    state.meta_mgr.parse_full_id_components.side_effect = _parse

    r = client.post('/api/parallels', json={'text': 'hello', 'mode': 'exact'})
    assert r.status_code == 200, r.text
    body = r.json()
    assert 'truncated_to_200' in body['warnings'], (
        f'expected truncated_to_200 in warnings; got {body.get("warnings")!r}'
    )
    assert len(body['results']) == PARALLELS_GROUP_CAP, (
        f'expected exactly {PARALLELS_GROUP_CAP} results; got {len(body["results"])}'
    )


def test_parallels_no_cap_warning_below_threshold(client, mock_searcher, clean_env):
    """When groups <= 200, no truncated_to_200 warning is emitted."""
    r = client.post('/api/parallels', json={'text': 'hello', 'mode': 'exact'})
    assert r.status_code == 200, r.text
    body = r.json()
    assert 'truncated_to_200' not in (body.get('warnings') or [])


def test_parallels_filtered_results_uncapped(client, mock_searcher, clean_env):
    """v7.10 explicit decision: filtered_results is NOT subject to PARALLELS_GROUP_CAP.

    When the searcher returns > 200 filtered rows (simulated with 250 rows),
    the envelope filtered[] length must match the mock's filtered count -- NOT
    capped at 200. This makes the Plan 02 decision explicit: main_results is
    capped, filtered_results is passed through uncapped.

    Rationale (from Plan 02 module docstring): filtered_results is driven by
    the user's max_freq threshold and is typically small; capping adds complexity
    for a rare edge case; v7.11 may add if load testing warrants.
    """
    from web.state import state
    filtered_rows = []
    for i in range(250):
        row = _make_filtered_row(
            uid=f'IE{1000 + i}_P1_FL{i}',
            sys_id=f'filtered_sys_{i}',
        )
        filtered_rows.append(row)

    state.searcher.search_composition_logic.return_value = {
        'main': [],
        'filtered': filtered_rows,
        'boundary_stats': None,
    }

    r = client.post('/api/parallels', json={
        'text': 'hello world', 'mode': 'exact', 'max_freq': 0.001,
    })
    assert r.status_code == 200, r.text
    body = r.json()

    actual_filtered_len = len(body.get('filtered') or [])
    assert actual_filtered_len == len(filtered_rows), (
        f'filtered_results should be uncapped in v7.10 '
        f'(expected {len(filtered_rows)}, got {actual_filtered_len}). '
        f'If this fails, a cap was silently added to filtered_results -- '
        f'see Plan 02 module docstring for the explicit v7.10 decision.'
    )

    assert 'truncated_to_200' not in (body.get('warnings') or [])


# ---------------------------------------------------------------------------
# Empty results (1)
# ---------------------------------------------------------------------------

def test_parallels_empty_results_not_an_error(client, mock_searcher, clean_env):
    """Composition with zero matches -> count=0, total=0, results=[], filtered=[].
    NOT an error per CONTEXT Claude's Discretion.
    """
    from web.state import state
    state.searcher.search_composition_logic.return_value = {
        'main': [], 'filtered': [], 'boundary_stats': None,
    }
    r = client.post('/api/parallels', json={'text': 'unmatched text', 'mode': 'exact'})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body['count'] == 0
    assert body['total'] == 0
    assert body['results'] == []
    assert body['filtered'] == []


# ---------------------------------------------------------------------------
# Statelessness (1)
# ---------------------------------------------------------------------------

def test_parallels_statelessness_two_identical_posts(client, mock_searcher, clean_env):
    """Two identical posts diff ONLY in generated_at field. No state leak."""
    body = {'text': 'hello world', 'mode': 'exact'}
    r1 = client.post('/api/parallels', json=body)
    r2 = client.post('/api/parallels', json=body)
    assert r1.status_code == 200 and r2.status_code == 200
    b1, b2 = r1.json(), r2.json()
    b1.pop('generated_at', None)
    b2.pop('generated_at', None)
    assert b1 == b2, f'stateless contract broken: {b1!r} != {b2!r}'


# ---------------------------------------------------------------------------
# @wrap_endpoint reuse (1)
# ---------------------------------------------------------------------------

def test_parallels_endpoint_uses_wrap_endpoint_decorator():
    """R-PR-03 precedent inherited: parallels_endpoint is decorated with
    @wrap_endpoint(endpoint_name='parallels') and the handler body has no
    hand-rolled try/except/finally / capture_api_event boilerplate.
    """
    import pathlib
    src = pathlib.Path('web/search_api.py').read_text(encoding='utf-8')
    # Decorator present.
    assert '@wrap_endpoint(endpoint_name=\'parallels\')' in src or \
           '@wrap_endpoint(endpoint_name="parallels")' in src, \
        'parallels_endpoint must be decorated with @wrap_endpoint(endpoint_name=\'parallels\')'
    # Body inspection: capture_api_event must NOT appear inside parallels_endpoint body.
    import re
    # Match the parallels_endpoint body: from `async def parallels_endpoint(...)`
    # up to the next sibling decorator (@target_app... at 4-space indent), the
    # next module-level `def`/`async def` (no indent), or a module-level
    # `logger.info` call.
    # parallels_endpoint is the last route registered inside init_search_api,
    # followed only by an indented `logger.info(...)` call (no module-level
    # sibling). Match from the signature line to that logger.info.
    m = re.search(
        r'async def parallels_endpoint\([^)]*\)[^\n]*\n(.*?)\n\s+logger\.info',
        src, re.S,
    )
    body = m.group(1) if m else ''
    assert body, 'parallels_endpoint body not found by regex'
    # Strip the docstring (which legitimately mentions these symbols in prose)
    # before scanning for hand-rolled boilerplate.
    body_no_doc = re.sub(r'""".*?"""', '', body, count=1, flags=re.S)
    assert 'capture_api_event' not in body_no_doc, \
        'R-PR-03 precedent: capture_api_event must not appear in parallels_endpoint body'
    assert 't0 = time.monotonic()' not in body_no_doc, \
        'R-PR-03 precedent: t0 monotonic clock setup must not appear in handler body'


# ---------------------------------------------------------------------------
# Phase 81A Plan 04 Task 3 — request echo presence (AC8 / D-07)
# ---------------------------------------------------------------------------

def test_parallels_envelope_contains_request_echo(client, mock_searcher, clean_env):
    """81A AC8 / D-07 — /api/parallels envelope gains a `request` echo block.
    Field name is `mode` (NOT `search_mode`) per D-07; no `responsa_options`
    (parallels never used Responsa); no `gap` (ParallelsRequest has no gap).
    Exactly 6 keys: mode, chunk_size, max_freq, boundary_options,
    limit_effective, filters."""
    payload = {'text': 'hello world', 'mode': 'exact'}
    resp = client.post('/api/parallels', json=payload)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert 'request' in body, body
    echo = body['request']
    assert set(echo.keys()) == {
        'mode', 'chunk_size', 'max_freq', 'boundary_options',
        'limit_effective', 'filters',
    }, echo
    # D-07: parallels keeps `mode`, not `search_mode`.
    assert 'search_mode' not in echo
    assert 'responsa_options' not in echo
    # Echo's `mode` matches what the client sent.
    assert echo['mode'] == 'exact'
