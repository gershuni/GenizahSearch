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
import pathlib
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


@pytest.fixture(autouse=True)
def _reset_heavy_semaphore():
    """P9X: reset the heavy-mode concurrency semaphore to its default size
    before and after each test so tests do not share state through the
    module-level singleton."""
    from web.search_api import _HeavySemaphoreState, DEFAULT_HEAVY_CONCURRENCY
    _HeavySemaphoreState.reset(DEFAULT_HEAVY_CONCURRENCY)
    yield
    _HeavySemaphoreState.reset(DEFAULT_HEAVY_CONCURRENCY)


@pytest.fixture(autouse=True)
def _reset_passage_semaphore():
    """Phase 145: same hygiene as _reset_heavy_semaphore, for the passage
    budget's semaphore + its own ThreadPoolExecutor."""
    from web.search_api import _PassageSemaphoreState, DEFAULT_PASSAGE_CONCURRENCY
    _PassageSemaphoreState.reset(DEFAULT_PASSAGE_CONCURRENCY)
    yield
    _PassageSemaphoreState.reset(DEFAULT_PASSAGE_CONCURRENCY)


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
    # Phase 145: default 'chunk' keeps every existing caller byte-compatible.
    assert req.method == 'chunk'


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


@pytest.mark.parametrize('bad', [0.05, 0.5, 0.999, 0])
def test_parallels_max_freq_below_one_is_rejected(client, mock_searcher,
                                                  clean_env, bad):
    """max_freq is a DOCUMENT COUNT, and a fractional one is a silent disaster.

    The engine tests `len(hits) > max_freq`
    (shared/search_engine.py::search_composition_logic), so any value under 1
    is true for every chunk that matches anything: the whole result set is
    diverted out of `results` and the caller gets an empty envelope with a
    200. Until 2026-08-24 the field's own description and docs/SEARCH_API.md
    called it a "0.0-1.0 ratio" and the documented example was 0.05 -- i.e.
    following the documentation produced no results, silently. The `ge=1`
    bound turns that into a loud 400.

    This cannot break a working integration: a caller sending 0.05 today is
    already getting nothing back.
    """
    r = client.post('/api/parallels', json={
        'text': 'hello world', 'mode': 'exact', 'max_freq': bad,
    })
    assert r.status_code == 400, r.text
    err = r.json()['error']
    assert err['code'] == 'invalid_request'
    assert 'max_freq' in err.get('fields', [])


def test_max_freq_effective_ceiling_is_still_fifty():
    """The documented effective range of max_freq is [1, 50). Guard the literal.

    `shared/search_engine.py::search_composition_logic` retrieves at most 50
    hits per chunk and then tests `len(hits) > max_freq`, so no value at or
    above 50 can ever fire -- it is identical to None. That ceiling is not a
    tuning detail: it is now stated in this endpoint's field description and
    in docs/SEARCH_API.md, and a measurement on 2026-08-24 confirmed it
    (identical results at max_freq 50 / 100 / 1000 / 100000, `filtered`
    empty at every one).

    If someone raises the retrieval cap, that documentation goes silently
    wrong -- the API would start filtering where it says it cannot. This
    test fails first so the docs get updated in the same change.
    """
    import re as _re
    src = pathlib.Path(__file__).resolve().parents[1] / 'shared' / 'search_engine.py'
    text = src.read_text(encoding='utf-8')
    caps = _re.findall(r'\.search\(\s*\w+\s*,\s*(\d+)\s*\)\.hits', text)
    assert caps, 'per-chunk retrieval call not found -- did the call shape change?'
    assert set(caps) == {'50'}, (
        f'per-chunk retrieval cap changed to {sorted(set(caps))}; max_freq\'s '
        f'effective ceiling moved with it. Update the field description in '
        f'web/search_api.py and the max_freq row in docs/SEARCH_API.md.')


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
        # 1 = the strictest LEGAL value: max_freq is a document count, so
        # this means "a chunk matching more than one document is too common".
        # Was 0.001 until 2026-08-24, written against a docstring that wrongly
        # called the field a 0.0-1.0 ratio; the searcher is mocked here, so
        # the value only has to be well-formed.
        'text': 'hello world', 'mode': 'exact', 'max_freq': 1,
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
# P9X Task 1 — Parallels timeout + heavy concurrency (2)
# ---------------------------------------------------------------------------

def test_parallels_timeout_uses_parallels_knob(client, clean_env, monkeypatch):
    """SEARCH_API_PARALLELS_TIMEOUT=0.2 + slow composition stub → 504 core_timeout."""
    import asyncio

    monkeypatch.setenv('SEARCH_API_PARALLELS_TIMEOUT', '0.2')

    # Patch fetch_parallels_results to be a slow coroutine
    async def _slow_parallels(**kwargs):
        await asyncio.sleep(1.0)
        from shared.parallels_service import ParallelsResultBundle
        return ParallelsResultBundle(
            main_results=[], filtered_results=[],
            boundary_options={'boundary_mode': 'full', 'boundary_delimiter': '\n',
                              'boundary_boost': 1.5, 'min_boundary_matches': 0,
                              'min_delimiter_distance': 3},
            truncated_to_200=False,
        )

    monkeypatch.setattr('web.search_api.fetch_parallels_results', _slow_parallels)

    r = client.post('/api/parallels', json={'text': 'hello world', 'mode': 'exact'})
    assert r.status_code == 504, r.text
    assert r.json()['error']['code'] == 'core_timeout'


def test_parallels_heavy_concurrency_fast_fail(monkeypatch):
    """Second concurrent parallels request → 503 heavy_search_busy.
    Tests _acquire_heavy_slot directly (avoids threading races in TestClient)."""
    import asyncio
    import pytest

    monkeypatch.setenv('SEARCH_API_HEAVY_CONCURRENCY', '1')

    from web.search_api import _acquire_heavy_slot, _HeavySemaphoreState

    async def _test():
        _HeavySemaphoreState.reset(1)
        sem = _HeavySemaphoreState.sem
        # Hold the single slot by decrementing _value directly
        # (same mechanism as _acquire_heavy_slot uses)
        assert sem._value == 1
        sem._value -= 1
        assert sem._value == 0
        try:
            with pytest.raises(APIError) as exc_info:
                await _acquire_heavy_slot()
            err = exc_info.value
            assert err.code == 'heavy_search_busy'
            assert err.http_status == 503
            assert 'Retry-After' in err.headers
        finally:
            sem.release()

    asyncio.run(_test())


def test_parallels_heavy_slot_held_until_task_completes(bare_app, mock_searcher, clean_env, monkeypatch):
    """REGRESSION (Codex HIGH, parallels parity): a /api/parallels request that
    times out must hold its heavy slot until the composition task ACTUALLY
    finishes — not release it when asyncio.wait returns. A concurrent parallels
    request in that window must get 503; the slot frees only after the task
    completes."""
    import asyncio
    import httpx
    from shared.parallels_service import ParallelsResultBundle
    from web.search_api import _HeavySemaphoreState, DEFAULT_HEAVY_CONCURRENCY

    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '9999')
    monkeypatch.setenv('SEARCH_API_HEAVY_CONCURRENCY', '1')
    monkeypatch.setenv('SEARCH_API_PARALLELS_TIMEOUT', '0.3')
    _parallels_rate_limiter.reset_for_tests()
    _HeavySemaphoreState.reset(1)

    def _empty_bundle():
        return ParallelsResultBundle(
            main_results=[], filtered_results=[],
            boundary_options={'boundary_mode': 'full', 'boundary_delimiter': '\n',
                              'boundary_boost': 1.5, 'min_boundary_matches': 0,
                              'min_delimiter_distance': 3},
            truncated_to_200=False,
        )

    async def _run():
        blocker = asyncio.Event()

        async def _blocking_parallels(**kwargs):
            # Blocks the composition task (awaiting on the loop) until released.
            await blocker.wait()
            return _empty_bundle()

        monkeypatch.setattr('web.search_api.fetch_parallels_results', _blocking_parallels)

        transport = httpx.ASGITransport(app=bare_app)
        async with httpx.AsyncClient(transport=transport, base_url='http://t') as ac:
            # A: acquires the only slot, blocks in the task, 504s at 0.3s.
            task_a = asyncio.create_task(
                ac.post('/api/parallels', json={'text': 'hello world', 'mode': 'exact'})
            )
            await asyncio.sleep(0.8)  # A trips its 0.3s timeout; task still blocked
            # B: second parallels while A's task still holds the slot → 503.
            resp_b = await ac.post(
                '/api/parallels', json={'text': 'other text', 'mode': 'exact'}
            )
            assert resp_b.status_code == 503, resp_b.text
            assert resp_b.json()['error']['code'] == 'heavy_search_busy'
            # Unblock A's task → it completes → done-callback releases the slot.
            blocker.set()
            resp_a = await task_a
            assert resp_a.status_code == 504, resp_a.text
            assert resp_a.json()['error']['code'] == 'core_timeout'
            await asyncio.sleep(0.1)
            resp_c = await ac.post(
                '/api/parallels', json={'text': 'third text', 'mode': 'exact'}
            )
            assert resp_c.status_code == 200, resp_c.text

    try:
        asyncio.run(_run())
    finally:
        _HeavySemaphoreState.reset(DEFAULT_HEAVY_CONCURRENCY)


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
    Exactly 7 keys: mode, chunk_size, max_freq, boundary_options,
    limit_effective, filters, method (method added Phase 145)."""
    payload = {'text': 'hello world', 'mode': 'exact'}
    resp = client.post('/api/parallels', json=payload)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert 'request' in body, body
    echo = body['request']
    assert set(echo.keys()) == {
        'mode', 'chunk_size', 'max_freq', 'boundary_options',
        'limit_effective', 'filters', 'method',
    }, echo
    # D-07: parallels keeps `mode`, not `search_mode`.
    assert 'search_mode' not in echo
    assert 'responsa_options' not in echo
    # Echo's `mode` matches what the client sent.
    assert echo['mode'] == 'exact'
    # Phase 145: `method` defaults to 'chunk' when the client omits it --
    # the pre-Phase-145 request shape stays byte-compatible.
    assert echo['method'] == 'chunk'


# ---------------------------------------------------------------------------
# Phase 145 — method='passage' validation (scope restriction + availability)
# ---------------------------------------------------------------------------

def test_parallels_method_passage_unavailable_returns_503(client, mock_searcher, clean_env):
    """method='passage' with no loaded index (the default test environment --
    this worktree carries no real passage_index/) is a clean 503, never a
    silent fallback to the chunk engine. passage_available() is False here
    for TWO independent reasons (PASSAGE_PARALLELS_ENABLED defaults off, AND
    no index was ever loaded), either of which must produce this result."""
    r = client.post('/api/parallels', json={'text': 'hello world', 'method': 'passage'})
    assert r.status_code == 503, r.text
    assert r.json()['error']['code'] == 'passage_unavailable'


def test_parallels_method_passage_scope_unsupported_returns_400(client, mock_searcher, clean_env, monkeypatch):
    """method='passage' + filters.library=['LOCAL'] (include mode, the
    default) is rejected as a structural scope mismatch -- the passage index
    holds no Local-corpus records by construction -- rather than silently
    degrading to an empty result indistinguishable from "no matches found".
    Availability is mocked True here so this test isolates the SCOPE check
    from the (separately tested) availability check above."""
    monkeypatch.setattr('web.passage_assets.passage_available', lambda: True)
    r = client.post('/api/parallels', json={
        'text': 'hello world', 'method': 'passage',
        'filters': {'library': ['LOCAL']},
    })
    assert r.status_code == 400, r.text
    assert r.json()['error']['code'] == 'passage_scope_unsupported'


@pytest.mark.parametrize('bad_boundary_mode', ['boundary', 'combined'])
def test_parallels_method_passage_boundary_mode_unsupported_returns_400(
    client, mock_searcher, clean_env, monkeypatch, bad_boundary_mode,
):
    """Adversarial review finding #2: method='passage' + boundary_mode !=
    'full' is a structural rejection (400 'passage_option_unsupported'),
    never a silent degradation to 'full'. Rejected at request-validation
    time (step 4b) BEFORE any concurrency slot is acquired -- the mock
    searcher's search_composition_logic must never even be called."""
    monkeypatch.setattr('web.passage_assets.passage_available', lambda: True)
    r = client.post('/api/parallels', json={
        'text': 'hello world', 'method': 'passage',
        'boundary_mode': bad_boundary_mode,
    })
    assert r.status_code == 400, r.text
    assert r.json()['error']['code'] == 'passage_option_unsupported'
    mock_searcher.search_composition_logic.assert_not_called()


def test_parallels_method_passage_boundary_mode_full_is_not_rejected(
    client, mock_searcher, clean_env, monkeypatch,
):
    monkeypatch.setattr('web.passage_assets.passage_available', lambda: True)
    monkeypatch.setattr(
        'web.passage_assets.get_passage_searcher',
        lambda text_fetcher: mock_searcher,
    )
    r = client.post('/api/parallels', json={
        'text': 'hello world', 'method': 'passage', 'boundary_mode': 'full',
    })
    assert r.status_code == 200, r.text


def test_parallels_method_passage_scope_exclude_local_is_not_rejected(
    client, mock_searcher, clean_env, monkeypatch,
):
    """Excluding 'LOCAL' (library_filter_mode='exclude') is a no-op for
    passage -- it never had Local records to exclude -- and must NOT be
    rejected; the request proceeds and returns 200."""
    monkeypatch.setattr('web.passage_assets.passage_available', lambda: True)
    monkeypatch.setattr(
        'web.passage_assets.get_passage_searcher',
        lambda text_fetcher: mock_searcher,
    )
    r = client.post('/api/parallels', json={
        'text': 'hello world', 'method': 'passage',
        'filters': {'library': ['LOCAL'], 'library_filter_mode': 'exclude'},
    })
    assert r.status_code == 200, r.text


def test_parallels_method_passage_happy_path_routes_to_passage_searcher(
    client, mock_searcher, clean_env, monkeypatch,
):
    """method='passage', available, no scope conflict -> 200, and the
    envelope's request echo names 'passage' (not silently 'chunk')."""
    monkeypatch.setattr('web.passage_assets.passage_available', lambda: True)
    monkeypatch.setattr(
        'web.passage_assets.get_passage_searcher',
        lambda text_fetcher: mock_searcher,
    )
    r = client.post('/api/parallels', json={'text': 'hello world', 'method': 'passage'})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body['request']['method'] == 'passage'
    mock_searcher.search_composition_logic.assert_called_once()


# ---------------------------------------------------------------------------
# Codex review finding #13 -- passage mode must not silently ignore
# chunk_size/mode/max_freq, and the envelope must echo the EFFECTIVE
# (passage) policy instead of the ignored chunk knobs.
# ---------------------------------------------------------------------------

def _fake_passage_policy() -> dict:
    """A realistic PassagePolicy.as_dict() shape (shared/passage_policy.py)
    for envelope-echo assertions -- NOT a bare MagicMock, which
    FastAPI's jsonable_encoder silently degrades to {} rather than raising
    (verified: json.dumps(MagicMock()) raises, but jsonable_encoder does
    not -- a real dict here is what makes these tests actually exercise the
    echo content, not merely "the response was 200")."""
    return {
        'name': 'standard-40', 'min_span': 40, 'regime': 'one_sided',
        'density_scale': 1.0, 'budget_policy': 'band',
        'posting_budget': 500_000, 'candidate_cap': 200_000,
        'verify_cap': 3_000, 'min_anchors': 2, 'schema_version': 1,
        'policy_id': 'pp1-0000000000000000',
    }


@pytest.mark.parametrize('chunk_size', [2, 4, 6, 20])
def test_parallels_method_passage_nondefault_chunk_size_returns_400(
    client, mock_searcher, clean_env, monkeypatch, chunk_size,
):
    """Finding #13(a): chunk_size has no passage-matching equivalent (no
    sliding-window chunk) -- a non-default value must be REJECTED, never
    silently ignored while the client believes it was applied."""
    monkeypatch.setattr('web.passage_assets.passage_available', lambda: True)
    r = client.post('/api/parallels', json={
        'text': 'hello world', 'method': 'passage', 'chunk_size': chunk_size,
    })
    assert r.status_code == 400, r.text
    assert r.json()['error']['code'] == 'passage_option_unsupported'
    mock_searcher.search_composition_logic.assert_not_called()


@pytest.mark.parametrize('field', ['chunk_size', 'mode', 'boundary_mode'])
def test_passage_accepts_each_chunk_knob_at_its_own_model_default(
    client, mock_searcher, clean_env, monkeypatch, field,
):
    """A caller who sends a knob at its declared default is never rejected.

    `method='passage'` rejects non-default values of the chunk-only knobs, and
    that check compared against bare literals with no link to the Field
    defaults. Nothing stopped the two drifting apart: move a default and every
    passage caller who omitted the field starts getting 400
    `passage_option_unsupported`, told to omit a field they already omitted.

    This reads each default off the model itself, so it fails the moment the
    declared default and the validator's threshold stop agreeing -- whichever
    side moved.
    """
    monkeypatch.setattr('web.passage_assets.passage_available', lambda: True)
    default = ParallelsRequest.model_fields[field].default
    r = client.post('/api/parallels', json={
        'text': 'hello world', 'method': 'passage', field: default,
    })
    assert r.status_code != 400, (
        f'{field}={default!r} is this model\'s OWN default but the passage '
        f'validator rejected it: {r.text}')


def test_passage_accepts_a_request_that_omits_every_chunk_knob(
    client, mock_searcher, clean_env, monkeypatch,
):
    """The plain passage request -- no chunk knobs at all -- must always pass."""
    monkeypatch.setattr('web.passage_assets.passage_available', lambda: True)
    r = client.post('/api/parallels', json={
        'text': 'hello world', 'method': 'passage',
    })
    assert r.status_code != 400, r.text


@pytest.mark.parametrize('mode', ['variants', 'fuzzy'])
def test_parallels_method_passage_nondefault_mode_returns_400(
    client, mock_searcher, clean_env, monkeypatch, mode,
):
    """Finding #13(a): mode (variants/fuzzy) has no passage-matching
    equivalent (character-level Levenshtein has no morphological-variant
    concept) -- rejected, not silently ignored."""
    monkeypatch.setattr('web.passage_assets.passage_available', lambda: True)
    r = client.post('/api/parallels', json={
        'text': 'hello world', 'method': 'passage', 'mode': mode,
    })
    assert r.status_code == 400, r.text
    assert r.json()['error']['code'] == 'passage_option_unsupported'
    mock_searcher.search_composition_logic.assert_not_called()


def test_parallels_method_passage_nondefault_max_freq_returns_400(
    client, mock_searcher, clean_env, monkeypatch,
):
    """Finding #13(a): max_freq has no passage-matching equivalent (no
    per-chunk frequency signal) -- rejected, not silently ignored."""
    monkeypatch.setattr('web.passage_assets.passage_available', lambda: True)
    r = client.post('/api/parallels', json={
        # 50 = a legal non-default count (was 0.05, which the ge=1 guard now
        # rejects as invalid_request BEFORE the passage block is reached, so
        # the test could no longer prove what it claims to).
        'text': 'hello world', 'method': 'passage', 'max_freq': 50,
    })
    assert r.status_code == 400, r.text
    assert r.json()['error']['code'] == 'passage_option_unsupported'
    mock_searcher.search_composition_logic.assert_not_called()


def test_parallels_method_passage_default_chunk_knobs_are_not_rejected(
    client, mock_searcher, clean_env, monkeypatch,
):
    """The request's OWN declared defaults (chunk_size=5, mode='exact',
    max_freq=None) must still pass for method='passage' -- only a
    NON-default value is rejected."""
    monkeypatch.setattr('web.passage_assets.passage_available', lambda: True)
    monkeypatch.setattr(
        'web.passage_assets.get_passage_searcher',
        lambda text_fetcher: mock_searcher,
    )
    mock_searcher.policy.as_dict.return_value = _fake_passage_policy()
    r = client.post('/api/parallels', json={
        'text': 'hello world', 'method': 'passage',
        'chunk_size': 5, 'mode': 'exact', 'max_freq': None,
    })
    assert r.status_code == 200, r.text


def test_parallels_method_passage_echo_nulls_ignored_chunk_knobs(
    client, mock_searcher, clean_env, monkeypatch,
):
    """Finding #13(b): the response envelope must not echo chunk_size/mode/
    max_freq/boundary_options as though they were applied -- they are
    nulled for method='passage', both at the top level (Phase 77 fields)
    and inside the `request` echo block (Phase 81A)."""
    monkeypatch.setattr('web.passage_assets.passage_available', lambda: True)
    monkeypatch.setattr(
        'web.passage_assets.get_passage_searcher',
        lambda text_fetcher: mock_searcher,
    )
    mock_searcher.policy.as_dict.return_value = _fake_passage_policy()
    r = client.post('/api/parallels', json={'text': 'hello world', 'method': 'passage'})
    assert r.status_code == 200, r.text
    body = r.json()

    # Top-level Phase 77 fields.
    assert body['chunk_size'] is None
    # shared/search_serializer.py::serialize_parallels_payload has its OWN
    # pre-existing `mode or 'exact'` fallback (predates Phase 145 and is out
    # of scope to change here -- web/api.py's export path also depends on
    # it), so a null mode floors to 'exact' at the TOP level specifically.
    # This happens to coincide with the only mode value method='passage' can
    # ever reach this point with (a non-default mode is already rejected by
    # step 4b's 400), so it never actually echoes an ignored CHOICE -- but
    # the authoritative place a consumer should read "was mode applied" is
    # the `request` echo block below, where None means exactly that.
    assert body['mode'] == 'exact'
    assert body['max_freq'] is None
    assert body['boundary_options'] is None

    # The `request` echo block -- the authoritative Phase 81A source, with
    # no such legacy fallback.
    echo = body['request']
    assert echo['chunk_size'] is None
    assert echo['mode'] is None
    assert echo['max_freq'] is None
    assert echo['boundary_options'] is None
    assert echo['method'] == 'passage'

    # The EFFECTIVE policy that actually drove the search.
    assert echo['passage_policy'] == _fake_passage_policy()


def test_parallels_method_chunk_echo_has_no_passage_policy_key(
    client, mock_searcher, clean_env,
):
    """Regression pin: method='chunk' (the default) must not gain a
    passage_policy key at all -- not even set to None -- so the
    pre-existing 7-key echo shape stays byte-for-byte identical."""
    r = client.post('/api/parallels', json={'text': 'hello world'})
    assert r.status_code == 200, r.text
    echo = r.json()['request']
    assert 'passage_policy' not in echo
    assert set(echo.keys()) == {
        'mode', 'chunk_size', 'max_freq', 'boundary_options',
        'limit_effective', 'filters', 'method',
    }
    assert echo['mode'] == 'exact'
    assert echo['chunk_size'] == 5
    assert echo['boundary_options'] is not None


# ---------------------------------------------------------------------------
# Codex review finding #15 -- web/pages/parallels.py must route passage
# searches through the SAME bounded execution budget POST /api/parallels
# uses (run_through_passage_budget / run_passage_search), never NiceGUI's
# generic, unbounded run.io_bound.
# ---------------------------------------------------------------------------

def test_run_passage_search_dispatches_on_the_dedicated_executor():
    """run_passage_search (the page's entry point into the shared budget)
    must run the sync callable on the SAME dedicated executor
    (_PassageSemaphoreState.executor()) the API's own passage branch uses --
    verified by thread name, a real executor, no mocking."""
    import asyncio
    import threading

    from web.search_api import run_passage_search

    names = {}

    def _capture():
        names['thread'] = threading.current_thread().name
        return 'ok'

    result = asyncio.run(run_passage_search(_capture))
    assert result == 'ok'
    assert 'passage-search' in names['thread']


def test_run_through_passage_budget_busy_when_semaphore_exhausted(monkeypatch):
    """The core primitive both surfaces share: exhausting the semaphore via
    ANY caller must make the NEXT caller see passage_search_busy -- proving
    the budget is genuinely ONE shared resource, not one per call site.

    Sets SEARCH_API_PASSAGE_CONCURRENCY=1 via the env (not merely
    _PassageSemaphoreState.reset(1)) -- acquire() re-reads this env var on
    every call and auto-corrects the capacity back to its class default
    whenever the semaphore is fully idle, which would silently undo a bare
    .reset(1) the instant the FIRST acquire() ran, defeating the "exhausted"
    setup before the test's own second acquire ever happened."""
    import asyncio

    from web.search_api import (
        run_through_passage_budget, _PassageSemaphoreState,
        DEFAULT_PASSAGE_CONCURRENCY,
    )

    monkeypatch.setenv('SEARCH_API_PASSAGE_CONCURRENCY', '1')

    async def _run():
        release = await _PassageSemaphoreState.acquire()  # takes the only slot
        try:
            assert _PassageSemaphoreState._capacity == 1
            made = []

            def _factory():
                made.append(1)
                return asyncio.get_event_loop().create_future()

            with pytest.raises(APIError) as exc_info:
                await run_through_passage_budget(_factory)
            assert exc_info.value.code == 'passage_search_busy'
            assert exc_info.value.http_status == 503
            # Adversarial review round 2: a rejected caller must not have
            # created -- let alone dispatched -- any work. Before the factory
            # signature, run_passage_search called run_in_executor BEFORE
            # admission, so a 503'd request still occupied a pool worker and
            # the rejection shed no load at all.
            assert made == [], (
                'the awaitable was constructed despite a 503 -- admission '
                'control must run BEFORE the work exists'
            )
        finally:
            release()

    asyncio.run(_run())
    _PassageSemaphoreState.reset(DEFAULT_PASSAGE_CONCURRENCY)


def test_a_busy_rejection_never_reaches_the_worker(monkeypatch):
    """THE regression test for the adversarial-review-round-2 defect.

    `run_passage_search` used to call `loop.run_in_executor(...)` BEFORE
    `run_through_passage_budget` acquired the semaphore. `run_in_executor`
    submits immediately, so a caller answered `passage_search_busy` had
    already handed its search to the pool: the 503 removed the waiting
    client but not the load, and under a burst every surplus job stayed
    queued on a 4-worker executor. Same shape as the discovery-service
    backlog CLAUDE.md records.

    Note WHERE this asserts. The two tests around it exercise
    `run_through_passage_budget` directly, which never had the bug -- it
    always acquired before `ensure_future`. The defect lived in the page's
    entry point, so only a test that goes through `run_passage_search` can
    fail on it. Mutation-checked: restoring the submit-then-acquire order in
    web/search_api.py::run_passage_search turns this red and leaves every
    other test in this file green.
    """
    import asyncio

    from web.search_api import (
        run_passage_search, _PassageSemaphoreState, DEFAULT_PASSAGE_CONCURRENCY,
    )

    monkeypatch.setenv('SEARCH_API_PASSAGE_CONCURRENCY', '1')

    ran = []

    def _worker():
        ran.append(1)
        return 'should never happen'

    async def _run():
        release = await _PassageSemaphoreState.acquire()  # the only slot
        try:
            with pytest.raises(APIError) as exc_info:
                await run_passage_search(_worker)
            assert exc_info.value.code == 'passage_search_busy'
        finally:
            release()
        # Give any (wrongly) submitted job a chance to land on a pool thread
        # before asserting -- a same-tick assertion could pass merely because
        # the executor had not scheduled it yet, which is exactly the kind of
        # vacuous green this project treats as a defect.
        await asyncio.sleep(0.25)
        assert ran == [], (
            'the worker ran despite a 503 -- work was dispatched to the '
            'executor before admission control'
        )

    asyncio.run(_run())
    _PassageSemaphoreState.reset(DEFAULT_PASSAGE_CONCURRENCY)


def test_budget_refuses_an_already_built_awaitable(monkeypatch):
    """The factory contract is enforced, not merely documented: passing the
    pre-round-2 shape (an awaitable) must fail loudly BEFORE a slot is
    acquired, so it can never strand a permit or silently re-open the hole.
    """
    import asyncio

    from web.search_api import run_through_passage_budget, _PassageSemaphoreState

    async def _run():
        coro = asyncio.sleep(0)
        try:
            with pytest.raises(TypeError):
                await run_through_passage_budget(coro)
        finally:
            coro.close()  # never awaited; close it so pytest sees no warning
        assert not _PassageSemaphoreState.sem.locked(), 'a permit was stranded'

    asyncio.run(_run())


def test_run_through_passage_budget_times_out(monkeypatch):
    """A slow awaitable that exceeds SEARCH_API_PASSAGE_TIMEOUT surfaces as
    APIError('core_timeout', ..., 504) -- the slot is still held/released
    correctly (no leak) via the done-callback, same discipline as the
    chunk-path heavy budget."""
    import asyncio

    from web.search_api import run_through_passage_budget, _PassageSemaphoreState

    monkeypatch.setenv('SEARCH_API_PASSAGE_TIMEOUT', '0.05')

    async def _run():
        async def _slow():
            await asyncio.sleep(2.0)
            return 'too late'

        with pytest.raises(APIError) as exc_info:
            await run_through_passage_budget(_slow)
        assert exc_info.value.code == 'core_timeout'
        assert exc_info.value.http_status == 504
        # The slot must be free again once the slow task's done-callback has
        # had a chance to run (it releases on eventual completion, not on
        # the timeout itself -- run_in_executor/asyncio tasks cannot be
        # cancelled out from under a real thread).
        await asyncio.sleep(2.2)
        assert not _PassageSemaphoreState.sem.locked()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# PR #324 round 3: the QueryReport and hygiene counts must reach the envelope.
# ---------------------------------------------------------------------------

def test_passage_truncation_and_dup_counts_reach_the_envelope(
    client, mock_searcher, clean_env, monkeypatch,
):
    """A capped passage search, and one that demoted duplicate photography,
    must SAY so in warnings -- and the full report must ride the request echo
    for evaluation consumers. Until round 3 the searcher discarded the report
    entirely, so a truncated search looked complete."""
    monkeypatch.setattr('web.passage_assets.passage_available', lambda: True)
    monkeypatch.setattr(
        'web.passage_assets.get_passage_searcher',
        lambda text_fetcher: mock_searcher,
    )
    mock_searcher.search_composition_logic.return_value = {
        'main': [_make_main_row()],
        'filtered': [],
        'dropped_text_lookup_failures': 0,
        'duplicate_photography_demoted': 2,
        'query_report': {
            'policy_id': 'p-test', 'candidates_truncated': False,
            'verify_truncated': True, 'postings_excluded': 5,
        },
    }
    r = client.post('/api/parallels',
                    json={'text': 'hello world', 'method': 'passage'})
    assert r.status_code == 200, r.text
    body = r.json()
    codes = [w.get('code') if isinstance(w, dict) else w
             for w in body.get('warnings', [])]
    assert 'passage_results_truncated' in codes, codes
    assert 'duplicate_photography_demoted' in codes, codes
    dup = next(w for w in body['warnings']
               if isinstance(w, dict)
               and w.get('code') == 'duplicate_photography_demoted')
    assert dup['count'] == 2
    assert body['request']['passage_report']['verify_truncated'] is True


def test_an_untruncated_passage_search_warns_nothing_extra(
    client, mock_searcher, clean_env, monkeypatch,
):
    """postings_excluded alone is ROUTINE budget behaviour on long queries --
    it must NOT produce the truncation warning (it lives in the echo's full
    report instead), or the warning fires on nearly every request and stops
    meaning anything."""
    monkeypatch.setattr('web.passage_assets.passage_available', lambda: True)
    monkeypatch.setattr(
        'web.passage_assets.get_passage_searcher',
        lambda text_fetcher: mock_searcher,
    )
    mock_searcher.search_composition_logic.return_value = {
        'main': [_make_main_row()],
        'filtered': [],
        'query_report': {
            'policy_id': 'p-test', 'candidates_truncated': False,
            'verify_truncated': False, 'postings_excluded': 12345,
        },
    }
    r = client.post('/api/parallels',
                    json={'text': 'hello world', 'method': 'passage'})
    assert r.status_code == 200, r.text
    codes = [w.get('code') if isinstance(w, dict) else w
             for w in r.json().get('warnings', [])]
    assert 'passage_results_truncated' not in codes, codes
    assert 'duplicate_photography_demoted' not in codes, codes


# ---------------------------------------------------------------------------
# Multi-witness passage search (witnesses[] + sort).
#
# One work survives in many manuscripts and no single witness of it retrieves
# every other: 17 Birkat Hamazon witnesses searched separately and merged
# reach 85% of the reachable census, against 50-69% for any one of them.
# ---------------------------------------------------------------------------

W1 = {'text': 'the first witness text'}
W2 = {'text': 'the second witness text'}


def _make_fused_row(uid, sys_id, score, fusion, witness_ids):
    row = _make_main_row(uid=uid, sys_id=sys_id, score=score)
    ids = witness_ids.split(',')
    row.update({
        'fusion_score': fusion,
        'witness_count': len(ids),
        'witness_ids': witness_ids,
        'witness_id': ids[0],
        'witness_label': 'Witness ' + ids[0],
        # Deliberately HIGHER than `score`: the fused row renders one
        # witness's evidence and reports that witness's matched letters, so
        # the strongest match any witness made is a separate number.
        'best_witness_score': score * 2,
    })
    return row


def _witness_report(requested=2, searched=2, unresolved=()):
    resolved = [
        {'id': 'w%d' % (i + 1), 'label': 'Witness w%d' % (i + 1),
         'kind': 'pasted', 'resolved': True, 'reason': None, 'letters': 100}
        for i in range(searched)
    ]
    return {
        'requested': requested,
        'searched': searched,
        'witnesses': resolved + list(unresolved),
        'unresolved': list(unresolved),
    }


@pytest.fixture
def multi_witness(monkeypatch, mock_searcher):
    """Passage available, multi-witness enabled, and the searcher returning a
    two-witness fused result."""
    monkeypatch.setattr('web.passage_assets.passage_available', lambda: True)
    monkeypatch.setattr(
        'web.passage_assets.passage_multi_witness_available', lambda: True)
    monkeypatch.setattr(
        'web.passage_assets.get_passage_searcher',
        lambda text_fetcher: mock_searcher,
    )
    mock_searcher.policy.as_dict.return_value = _fake_passage_policy()
    mock_searcher.search_composition_logic.return_value = {
        'main': [
            _make_fused_row('IE1_P1_FL1', '99001', 100.0, 2 / 61, 'w1,w2'),
            _make_fused_row('IE2_P1_FL2', '99002', 900.0, 1 / 61, 'w1'),
        ],
        'filtered': [],
        'truncated_to_200': False,
        'dropped_text_lookup_failures': 0,
        'duplicate_photography_demoted': 0,
        'query_report': {'candidates': 10, 'verify_truncated': False},
        'witness_report': _witness_report(),
        'per_witness_query_reports': [
            {'witness_id': 'w1', 'witness_label': 'Witness w1', 'report': {}},
            {'witness_id': 'w2', 'witness_label': 'Witness w2', 'report': {}},
        ],
    }
    return mock_searcher


def test_witnesses_with_chunk_method_is_rejected(client, mock_searcher,
                                                 clean_env):
    """The chunk engine decomposes a query into independent per-chunk lookups
    with no shared budget, so joining witnesses into `text` there returns the
    IDENTICAL manuscript set (measured: 392 both ways, empty difference in
    both directions) and costs less. Rejecting keeps that finding legible."""
    r = client.post('/api/parallels', json={'witnesses': [W1, W2]})
    assert r.status_code == 400, r.text
    assert r.json()['error']['code'] == 'witnesses_require_passage_method'
    mock_searcher.search_composition_logic.assert_not_called()


def test_text_and_witnesses_together_is_rejected(client, mock_searcher,
                                                 clean_env):
    """Never silently pick one: the query the caller believes ran would
    differ from the one that did."""
    r = client.post('/api/parallels', json={
        'text': 'hello world', 'method': 'passage', 'witnesses': [W1]})
    assert r.status_code == 400, r.text
    assert r.json()['error']['code'] == 'witnesses_and_text_conflict'
    mock_searcher.search_composition_logic.assert_not_called()


def test_witnesses_without_the_flag_is_503(client, mock_searcher, clean_env,
                                           monkeypatch):
    """Gated separately from PASSAGE_PARALLELS_ENABLED so single-witness
    passage can stay broadly on while the costlier fan-out is validated."""
    monkeypatch.setattr('web.passage_assets.passage_available', lambda: True)
    monkeypatch.setattr(
        'web.passage_assets.passage_multi_witness_available', lambda: False)
    r = client.post('/api/parallels', json={
        'method': 'passage', 'witnesses': [W1, W2]})
    assert r.status_code == 503, r.text
    assert r.json()['error']['code'] == 'passage_multi_witness_unavailable'


def test_too_many_witnesses_is_rejected(client, multi_witness, clean_env,
                                        monkeypatch):
    monkeypatch.setenv('SEARCH_API_PASSAGE_MAX_WITNESSES', '3')
    r = client.post('/api/parallels', json={
        'method': 'passage', 'witnesses': [W1, W2, W1, W2]})
    assert r.status_code == 400, r.text
    assert r.json()['error']['code'] == 'too_many_witnesses'
    multi_witness.search_composition_logic.assert_not_called()


def test_seventeen_witnesses_fit_the_default_cap(client, multi_witness,
                                                 clean_env):
    """The flagship case is a 17-witness Birkat Hamazon set pasted from a
    file. A cap of twelve would have rejected the very workflow the feature
    was built for; the default is 25."""
    r = client.post('/api/parallels', json={
        'method': 'passage', 'witnesses': [dict(W1) for _ in range(17)]})
    assert r.status_code == 200, r.text


def test_a_witness_list_that_cannot_fit_the_ceiling_is_refused_before_the_slot(
    client, multi_witness, clean_env, monkeypatch,
):
    """The witness CAP, not the timeout ceiling, is the control on cost --
    because on timeout the permit is held until the executor thread really
    finishes (run_in_executor cannot cancel a thread), so a raised ceiling
    would let timed-out requests occupy every slot while their clients retry.
    This keeps the two numbers provably consistent: raise the cap past what
    the ceiling can serve and requests are refused UP FRONT."""
    monkeypatch.setenv('SEARCH_API_PASSAGE_MAX_WITNESSES', '100')
    monkeypatch.setenv('SEARCH_API_PASSAGE_TIMEOUT', '5')
    r = client.post('/api/parallels', json={
        'method': 'passage', 'witnesses': [dict(W1) for _ in range(20)]})
    assert r.status_code == 400, r.text
    assert r.json()['error']['code'] == 'too_many_witnesses'
    # Refused BEFORE the work was ever dispatched.
    multi_witness.search_composition_logic.assert_not_called()


@pytest.mark.parametrize('witness', [
    {'text': 'a witness', 'raw_header': '99001_IE1_P1_FL1'},   # both
    {'label': 'no source at all'},                             # neither
    {'text': '   '},                                           # blank text
])
def test_a_witness_needs_exactly_one_source(client, mock_searcher, clean_env,
                                            monkeypatch, witness):
    monkeypatch.setattr('web.passage_assets.passage_available', lambda: True)
    monkeypatch.setattr(
        'web.passage_assets.passage_multi_witness_available', lambda: True)
    r = client.post('/api/parallels', json={
        'method': 'passage', 'witnesses': [witness]})
    assert r.status_code == 400, r.text
    assert r.json()['error']['code'] == 'invalid_request'


def test_an_oversize_pasted_witness_is_rejected(client, multi_witness,
                                                clean_env):
    r = client.post('/api/parallels', json={
        'method': 'passage', 'witnesses': [W1, {'text': 'x' * 20001}]})
    assert r.status_code == 400, r.text
    assert r.json()['error']['code'] == 'witness_too_long'


def test_sort_without_witnesses_is_rejected(client, mock_searcher, clean_env):
    """sort orders groups by facts only a multi-witness search produces."""
    r = client.post('/api/parallels', json={
        'text': 'hello world', 'sort': 'witness_count'})
    assert r.status_code == 400, r.text
    assert r.json()['error']['code'] == 'sort_requires_multi_witness'


def test_no_witness_resolving_is_a_400_naming_the_reason(client, multi_witness,
                                                         clean_env):
    """An empty result set would be indistinguishable from an honest
    "no matches" -- the one thing a search must never be ambiguous about."""
    from shared.passage_parallels import NoWitnessesResolved
    multi_witness.search_composition_logic.side_effect = NoWitnessesResolved({
        'requested': 2, 'searched': 0, 'witnesses': [],
        'unresolved': [{'id': 'w1', 'reason': 'not_found'},
                       {'id': 'w2', 'reason': 'bad_ref'}],
    })
    r = client.post('/api/parallels', json={
        'method': 'passage',
        'witnesses': [{'raw_header': 'NOPE_A'}, {'raw_header': 'NOPE_B'}]})
    assert r.status_code == 400, r.text
    body = r.json()
    assert body['error']['code'] == 'witnesses_required'
    # It must say WHICH failed and why, not merely that something did.
    assert 'not_found' in body['error']['message']
    assert 'bad_ref' in body['error']['message']


def test_an_unresolved_witness_warns_and_the_rest_still_run(client,
                                                            multi_witness,
                                                            clean_env):
    """Skip-and-report, never fatal: rejecting a 17-witness request over one
    stale reference wastes the sixteen the caller can still have -- and
    skipping WITHOUT saying so is silent content loss."""
    result = dict(multi_witness.search_composition_logic.return_value)
    result['witness_report'] = _witness_report(
        requested=3, searched=2,
        unresolved=[{'id': 'w3', 'label': 'Stale', 'kind': 'manuscript',
                     'resolved': False, 'reason': 'not_found', 'letters': 0}])
    multi_witness.search_composition_logic.return_value = result

    r = client.post('/api/parallels', json={
        'method': 'passage',
        'witnesses': [W1, W2, {'raw_header': 'GONE_1'}]})
    assert r.status_code == 200, r.text
    body = r.json()
    warn = [w for w in body['warnings']
            if isinstance(w, dict)
            and w.get('code') == 'witness_ref_unresolved']
    assert len(warn) == 1
    assert warn[0]['count'] == 1
    assert warn[0]['witnesses'][0]['reason'] == 'not_found'
    assert body['request']['witnesses']['requested'] == 3
    assert body['request']['witnesses']['searched'] == 2


def test_multi_witness_happy_path_echo_and_envelope(client, multi_witness,
                                                    clean_env):
    r = client.post('/api/parallels', json={
        'method': 'passage', 'witnesses': [W1, W2]})
    assert r.status_code == 200, r.text
    body = r.json()

    echo = body['request']
    assert echo['witnesses']['requested'] == 2
    assert echo['witnesses']['searched'] == 2
    assert [w['id'] for w in echo['witnesses']['labels']] == ['w1', 'w2']
    assert echo['sort'] == 'fused'

    # Fusion facts are NESTED under one conditional key, never bare item
    # keys: _serialize_item emits a fixed key set shared with /api/search,
    # and test_search_and_parallels_share_item_shape pins the difference
    # between the two shapes at exactly {'matches'}.
    top = body['results'][0]
    assert top['witness_fusion']['witness_count'] == 2
    assert top['witness_fusion']['witness_ids'] == ['w1', 'w2']
    # The row found by BOTH witnesses outranks the one with 9x the raw
    # matched letters. That IS rank fusion -- and if either the group cap or
    # the serializer had ranked by `score`, this order would be reversed.
    other = body['results'][1].get('witness_fusion') or {}
    assert top['witness_fusion']['fusion_score'] > (other.get('fusion_score') or 0)


def test_witness_texts_are_never_echoed_back(client, multi_witness, clean_env):
    """A witness can be 20,000 characters, the caller already has it, and 25
    of them would dominate the response."""
    secret = 'DISTINCTIVE-WITNESS-BODY-TEXT'
    r = client.post('/api/parallels', json={
        'method': 'passage', 'witnesses': [{'text': secret + ' more words'}]})
    assert r.status_code == 200, r.text
    assert secret not in json.dumps(r.json()['request'])


def test_all_witnesses_run_inside_one_request_and_one_slot(client,
                                                           multi_witness,
                                                           clean_env):
    """One HTTP request is one budget slot, with the witnesses sequential
    inside it. Releasing and re-acquiring mid-request would let a request 503
    halfway through; fanning witnesses across the executor would add a second
    concurrency dimension on top of SEARCH_API_PASSAGE_CONCURRENCY -- the
    two-budgets lesson."""
    r = client.post('/api/parallels', json={
        'method': 'passage', 'witnesses': [W1, W2, W1, W2, W1]})
    assert r.status_code == 200, r.text
    assert multi_witness.search_composition_logic.call_count == 1
    kwargs = multi_witness.search_composition_logic.call_args.kwargs
    assert len(kwargs['witnesses']) == 5
    # The engine, not the API, does the fan-out -- and it receives the
    # witnesses as a LIST, never as one joined string.
    assert kwargs['full_text'] == ''


@pytest.mark.parametrize('sort', ['fused', 'best_match', 'witness_count'])
def test_every_sort_value_is_accepted_with_witnesses(client, multi_witness,
                                                     clean_env, sort):
    r = client.post('/api/parallels', json={
        'method': 'passage', 'witnesses': [W1, W2], 'sort': sort})
    assert r.status_code == 200, r.text
    assert r.json()['request']['sort'] == sort


def test_a_request_without_witnesses_is_untouched_by_the_feature(
    client, mock_searcher, clean_env, monkeypatch,
):
    """The 8-key passage echo and the 7-key chunk echo must both be unchanged
    for any request that does not use witnesses."""
    monkeypatch.setattr('web.passage_assets.passage_available', lambda: True)
    monkeypatch.setattr(
        'web.passage_assets.get_passage_searcher',
        lambda text_fetcher: mock_searcher)
    mock_searcher.policy.as_dict.return_value = _fake_passage_policy()
    r = client.post('/api/parallels', json={
        'text': 'hello world', 'method': 'passage'})
    assert r.status_code == 200, r.text
    echo = r.json()['request']
    assert 'witnesses' not in echo and 'sort' not in echo

    r2 = client.post('/api/parallels', json={'text': 'hello world'})
    assert r2.status_code == 200, r2.text
    echo2 = r2.json()['request']
    assert 'witnesses' not in echo2 and 'sort' not in echo2
    assert 'witness_fusion' not in r2.json()['results'][0]


def test_multi_witness_score_is_still_matched_letters(client, multi_witness,
                                                      clean_env):
    """THE contract: `score` means matched letters on every method and every
    response. It was ~0.03 on a multi-witness response, because the group's
    fusion sum became `aggregate_score` -> `sort_score` -> the item's
    top-level `score` (`_serialize_item` prefers `sort_score`). A documented
    field silently changed meaning, and the UI badges and export columns read
    it. Found by review, not by any test here.
    """
    r = client.post('/api/parallels', json={
        'method': 'passage', 'witnesses': [W1, W2]})
    assert r.status_code == 200, r.text
    body = r.json()

    for item in body['results']:
        # The fixture rows carry scores of 100 and 900 matched letters. An RRF
        # sum is ~0.016-0.05, so any value below 1 means fusion leaked into
        # the field.
        assert item['score'] >= 1, (
            f"score={item['score']} is an RRF sum, not matched letters"
        )
    # The two fixture groups sum to exactly their rows' letters.
    assert sorted(i['score'] for i in body['results']) == [100.0, 900.0]


def test_multi_witness_groups_are_ordered_by_fusion_not_by_score(
    client, multi_witness, clean_env,
):
    """The order must still be the ranking that produced the rows. The fixture
    is built so the two disagree: the shared row has 100 matched letters and
    twice the RRF, the singleton has 900 letters and half."""
    r = client.post('/api/parallels', json={
        'method': 'passage', 'witnesses': [W1, W2]})
    assert r.status_code == 200, r.text
    results = r.json()['results']

    assert results[0]['witness_fusion']['witness_count'] == 2
    assert results[0]['score'] == 100.0, (
        'the fusion-ranked group must come first even though it has fewer '
        'matched letters'
    )
    assert results[1]['score'] == 900.0
    # ... so a consumer sorting by `score` gets a DIFFERENT order than the
    # array's. That is the documented consequence of keeping one scale.
    assert [i['score'] for i in results] != sorted(
        (i['score'] for i in results), reverse=True)


def test_a_single_witness_response_score_is_unchanged(client, mock_searcher,
                                                      clean_env, monkeypatch):
    """The other half of the same contract: nothing about the ordinary
    response moved."""
    monkeypatch.setattr('web.passage_assets.passage_available', lambda: True)
    monkeypatch.setattr(
        'web.passage_assets.get_passage_searcher',
        lambda text_fetcher: mock_searcher)
    mock_searcher.policy.as_dict.return_value = _fake_passage_policy()
    r = client.post('/api/parallels', json={
        'text': 'hello world', 'method': 'passage'})
    assert r.status_code == 200, r.text
    assert r.json()['results'][0]['score'] == 5.0


def test_the_service_orders_the_group_cap_by_fusion(client, multi_witness,
                                                    clean_env, monkeypatch):
    """The cap must SELECT by the fusion key on a multi-witness run.

    Asserted at the call, because the cap is a no-op below 200 groups and no
    fixture here reaches that -- a mutation removing `order_key` from this
    call stayed green against every other test in the suite.
    """
    import shared.parallels_service as svc

    seen = {}
    real = svc._cap_main_results_by_group

    def spy(rows, meta_mgr, cap=None, order_key=None):
        seen['order_key'] = order_key
        kw = {} if cap is None else {'cap': cap}
        return real(rows, meta_mgr, order_key=order_key, **kw)

    monkeypatch.setattr(svc, '_cap_main_results_by_group', spy)
    r = client.post('/api/parallels', json={
        'method': 'passage', 'witnesses': [W1, W2]})
    assert r.status_code == 200, r.text
    assert seen.get('order_key') == 'fusion_score'


def test_the_service_leaves_the_chunk_cap_alone(client, mock_searcher,
                                                clean_env, monkeypatch):
    """...and the chunk path must still pass nothing, or its 200-group cap
    starts selecting on a field it does not have."""
    import shared.parallels_service as svc

    seen = {}
    real = svc._cap_main_results_by_group

    def spy(rows, meta_mgr, cap=None, order_key=None):
        seen['order_key'] = order_key
        kw = {} if cap is None else {'cap': cap}
        return real(rows, meta_mgr, order_key=order_key, **kw)

    monkeypatch.setattr(svc, '_cap_main_results_by_group', spy)
    r = client.post('/api/parallels', json={'text': 'hello world'})
    assert r.status_code == 200, r.text
    assert seen.get('order_key') is None


def test_the_best_witness_score_reaches_the_envelope(client, multi_witness,
                                                     clean_env):
    """The fused row keeps `score` on the witness it actually renders, so the
    strongest single match any witness made would be lost unless the envelope
    reports it. It goes in `witness_fusion`, beside the other facts about the
    fusion, where it cannot be mistaken for the row's own score."""
    r = client.post('/api/parallels', json={
        'method': 'passage', 'witnesses': [W1, W2]})
    assert r.status_code == 200, r.text
    results = r.json()['results']

    for item in results:
        wf = item['witness_fusion']
        assert 'best_witness_score' in wf, (
            'the strongest match on this manuscript is not reported anywhere'
        )
        assert wf['best_witness_score'] == item['score'] * 2, (
            'it must be the fusion figure, not a copy of the score'
        )


def test_a_single_witness_response_has_no_best_witness_score(
    client, mock_searcher, clean_env, monkeypatch,
):
    """`witness_fusion` is emitted only for genuinely fused groups -- the
    single-witness path short-circuits before fusing, and a bare key here
    would break the shape shared with /api/search."""
    monkeypatch.setattr('web.passage_assets.passage_available', lambda: True)
    monkeypatch.setattr(
        'web.passage_assets.get_passage_searcher',
        lambda text_fetcher: mock_searcher)
    mock_searcher.policy.as_dict.return_value = _fake_passage_policy()
    r = client.post('/api/parallels', json={
        'text': 'hello world', 'method': 'passage'})
    assert r.status_code == 200, r.text
    assert 'witness_fusion' not in r.json()['results'][0]


def test_a_sort_that_could_not_be_applied_is_reported(client, mock_searcher,
                                                      clean_env, monkeypatch):
    """Send witnesses, have fewer than two resolve, and the response echoes
    the requested sort over an array ordered by score: the single-witness path
    short-circuits before fusing, so `fused` and `witness_count` are facts
    nothing produced. The echo stays -- that is what an echo is -- and a
    warning says the sort was dropped."""
    monkeypatch.setattr('web.passage_assets.passage_available', lambda: True)
    monkeypatch.setattr(
        'web.passage_assets.passage_multi_witness_available', lambda: True)
    monkeypatch.setattr(
        'web.passage_assets.get_passage_searcher',
        lambda text_fetcher: mock_searcher)
    mock_searcher.policy.as_dict.return_value = _fake_passage_policy()
    # One witness resolved out of two requested -> no fusion happened.
    mock_searcher.search_composition_logic.return_value = {
        'main': [_make_main_row(uid='IE1_P1_FL1', sys_id='99001', score=5.0)],
        'filtered': [],
        'truncated_to_200': False,
        'dropped_text_lookup_failures': 0,
        'duplicate_photography_demoted': 0,
        'query_report': {'candidates': 10, 'verify_truncated': False},
        'witness_report': _witness_report(requested=2, searched=1),
        'per_witness_query_reports': [],
    }
    r = client.post('/api/parallels', json={
        'method': 'passage', 'witnesses': [W1, W2],
        'sort': 'witness_count'})
    assert r.status_code == 200, r.text
    body = r.json()

    codes = {w['code'] for w in body.get('warnings') or []}
    assert 'sort_not_applied' in codes, (
        'the response claims an ordering it does not have'
    )
    # The echo still reports what was asked for.
    assert body['request']['sort'] == 'witness_count'


def test_an_applied_sort_is_not_warned_about(client, multi_witness, clean_env):
    r = client.post('/api/parallels', json={
        'method': 'passage', 'witnesses': [W1, W2], 'sort': 'witness_count'})
    assert r.status_code == 200, r.text
    codes = {w['code'] for w in r.json().get('warnings') or []}
    assert 'sort_not_applied' not in codes
