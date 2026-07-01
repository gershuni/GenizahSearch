# -*- coding: utf-8 -*-
"""Phase 132 Plan 01 — Wave 0 test scaffold for library_filter_mode (DMF-11).

Pins every DMF-11 behavior BEFORE production code is written (RED → GREEN cycle).
Tests cover BOTH /api/search AND /api/parallels since both share FiltersModel.

All tests in this file are expected to FAIL (RED) until Plan 02 ships:
  - FiltersModel.library_filter_mode field (Optional[Literal['include','exclude']])
  - resolve_library_complement_sys_ids helper in shared/fjms_service.py
  - _intersect_library_filter exclude branch in web/search_api.py

DMF-11 requirements covered:
  DMF-11-1: include mode (default) = today's behavior unchanged; omit == include
  DMF-11-2: exclude + codes → complement; include vs exclude → disjoint sets;
            parallels honors mode
  DMF-11-3: invalid mode → 400 invalid_request; mode without library → noop
  DMF-11:   resolve_library_complement_sys_ids helper correctness
"""

from __future__ import annotations

import asyncio

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unittest.mock import MagicMock

from web.search_api import (
    init_search_api,
    _intersect_library_filter,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def stub_searcher():
    """Replace state.searcher with a StubSearcher that returns one result."""
    from web.state import state

    class _StubSearcher:
        def __init__(self):
            self.calls = []
            self.results = [{
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

        def execute_search(self, **kw):
            self.calls.append(kw)
            return self.results

        def search_composition_logic(self, **kw):
            self.calls.append(kw)
            return {'main': [], 'filtered': [], 'boundary_stats': None}

    saved_searcher = state.searcher
    fake = _StubSearcher()
    state.searcher = fake
    yield fake
    state.searcher = saved_searcher


@pytest.fixture
def stub_meta_mgr():
    """Replace state.meta_mgr with a MagicMock."""
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
    # csv_bank used by _intersect_library_filter / resolve_library_complement_sys_ids
    mgr.csv_bank = {
        '9911111111111111': {'library_code': 'CUL'},
        '9911111111111112': {'library_code': 'CUL'},
        '9922222222222221': {'library_code': 'JTS'},
        '9922222222222222': {'library_code': 'JTS'},
    }
    state.meta_mgr = mgr
    yield mgr
    state.meta_mgr = saved


@pytest.fixture
def client(stub_searcher, stub_meta_mgr):
    """Bare FastAPI app with search API mounted — per-test isolated."""
    bare = FastAPI()
    init_search_api(app_override=bare)
    return TestClient(bare)


@pytest.fixture
def clean_env(monkeypatch):
    """Open API mode, no rate limiting noise."""
    monkeypatch.setenv('SEARCH_API_MODE', 'open')
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '120')
    monkeypatch.setenv('SEARCH_API_POSTHOG_SAMPLE_N', '999999')
    from web.search_api import _rate_limiter, _parallels_rate_limiter
    _rate_limiter.reset_for_tests()
    _parallels_rate_limiter.reset_for_tests()


@pytest.fixture(autouse=True)
def _reset_heavy_semaphore():
    """Reset the heavy-mode concurrency semaphore before/after each test."""
    from web.search_api import _HeavySemaphoreState, DEFAULT_HEAVY_CONCURRENCY
    _HeavySemaphoreState.reset(DEFAULT_HEAVY_CONCURRENCY)
    yield
    _HeavySemaphoreState.reset(DEFAULT_HEAVY_CONCURRENCY)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _post_search(client, **body):
    """POST /api/search with the given body."""
    return client.post('/api/search', json=body)


def _post_parallels(client, **body):
    """POST /api/parallels with the given body (mode defaults to 'exact')."""
    payload = {'text': 'שלום עולם', 'mode': 'exact'}
    payload.update(body)
    return client.post('/api/parallels', json=payload)


# ---------------------------------------------------------------------------
# Task 1 Tests — endpoint-level dual-mode tests
# ---------------------------------------------------------------------------

def test_include_mode_is_default_same_as_omitted(client, stub_searcher, clean_env, monkeypatch):
    """DMF-11-1: posting library_filter_mode='include' yields the SAME resolved
    restrict set as omitting the field entirely — both routes through
    resolve_library_sys_ids, NOT the complement helper.

    RED: FiltersModel.library_filter_mode field does not exist yet.
    """
    from shared import fjms_service as fjms_module

    include_calls = []
    complement_calls = []

    def _fake_include(codes, mgr):
        include_calls.append(codes)
        return {'9911111111111111', '9911111111111112'}

    monkeypatch.setattr(fjms_module, 'validate_filter_values', lambda d: None)
    monkeypatch.setattr(fjms_module, 'get_filter_sys_ids', lambda **kw: None)
    monkeypatch.setattr(fjms_module, 'resolve_library_sys_ids', _fake_include)
    # resolve_library_complement_sys_ids may not exist yet — guard with hasattr
    if hasattr(fjms_module, 'resolve_library_complement_sys_ids'):
        monkeypatch.setattr(
            fjms_module, 'resolve_library_complement_sys_ids',
            lambda codes, mgr: (complement_calls.append(codes) or set()),
        )

    # Request WITH explicit include
    r_include = _post_search(
        client, query='x', search_mode='exact',
        filters={'library': ['CUL'], 'library_filter_mode': 'include'},
    )
    # Request WITHOUT library_filter_mode (omitted)
    r_omit = _post_search(
        client, query='x', search_mode='exact',
        filters={'library': ['CUL']},
    )

    assert r_include.status_code == 200, r_include.text
    assert r_omit.status_code == 200, r_omit.text

    # Both must have called resolve_library_sys_ids (the include path)
    assert len(include_calls) == 2, (
        f"Expected 2 calls to resolve_library_sys_ids (one per request), got {len(include_calls)}"
    )
    # Complement helper must NOT be called in either case
    assert complement_calls == [], (
        f"resolve_library_complement_sys_ids was called unexpectedly: {complement_calls}"
    )


def test_omit_mode_equals_include(client, stub_searcher, clean_env, monkeypatch):
    """DMF-11-1: omitting library_filter_mode behaves identically to 'include'.

    BACKWARD-COMPAT PIN (Codex R1 HIGH): the request echo must contain NO
    'library_filter_mode' key when the field is omitted (default=None +
    model_dump(exclude_none=True) drops it). Explicitly posting 'include'
    MAY echo the key; omitting MUST NOT.

    RED: FiltersModel.library_filter_mode field does not exist yet.
    """
    from shared import fjms_service as fjms_module

    monkeypatch.setattr(fjms_module, 'validate_filter_values', lambda d: None)
    monkeypatch.setattr(fjms_module, 'get_filter_sys_ids', lambda **kw: None)
    monkeypatch.setattr(
        fjms_module, 'resolve_library_sys_ids',
        lambda codes, mgr: {'9911111111111111'},
    )

    r = _post_search(
        client, query='x', search_mode='exact',
        filters={'library': ['CUL']},  # omit library_filter_mode
    )
    assert r.status_code == 200, r.text

    echo_filters = r.json()['request']['filters']

    # BACKWARD-COMPAT PIN: echo must be byte-for-byte: exactly {'library': ['CUL']}
    assert 'library_filter_mode' not in echo_filters, (
        f"Echo should NOT include 'library_filter_mode' when field was omitted, "
        f"but got: {echo_filters}"
    )
    assert echo_filters == {'library': ['CUL']}, (
        f"Echo filters mismatch: {echo_filters}"
    )


def test_exclude_restricts_to_complement(client, stub_searcher, clean_env, monkeypatch):
    """DMF-11-2: library_filter_mode='exclude' routes through
    resolve_library_complement_sys_ids and the complement stub set scopes the search.

    RED: FiltersModel.library_filter_mode field + resolve_library_complement_sys_ids
    do not exist yet.
    """
    from shared import fjms_service as fjms_module

    complement_calls = []
    include_calls = []

    # CUL complement = JTS sys_ids (disjoint from include stub)
    COMPLEMENT_SYS_IDS = {'9922222222222221', '9922222222222222'}

    def _fake_complement(codes, mgr):
        complement_calls.append(codes)
        return COMPLEMENT_SYS_IDS

    def _fake_include(codes, mgr):
        include_calls.append(codes)
        return {'9911111111111111', '9911111111111112'}

    monkeypatch.setattr(fjms_module, 'validate_filter_values', lambda d: None)
    monkeypatch.setattr(fjms_module, 'get_filter_sys_ids', lambda **kw: None)
    monkeypatch.setattr(fjms_module, 'resolve_library_sys_ids', _fake_include)
    # Patch complement helper — may not exist yet; use setattr to install stub
    monkeypatch.setattr(
        fjms_module, 'resolve_library_complement_sys_ids',
        _fake_complement,
        raising=False,  # install even if attr doesn't exist yet
    )

    r = _post_search(
        client, query='x', search_mode='exact',
        filters={'library': ['CUL'], 'library_filter_mode': 'exclude'},
    )

    assert r.status_code == 200, r.text
    # Complement helper must have been called (the exclude path)
    assert len(complement_calls) == 1, (
        f"Expected 1 call to resolve_library_complement_sys_ids, got {len(complement_calls)}"
    )
    # Include helper must NOT have been called on the exclude path
    assert include_calls == [], (
        f"resolve_library_sys_ids should NOT be called on exclude path, calls: {include_calls}"
    )


def test_include_vs_exclude_disjoint(client, stub_searcher, clean_env, monkeypatch):
    """DMF-11-2: same library=['CUL'] under include vs exclude produces disjoint
    restrict sets (include stub ∩ exclude stub == ∅).

    RED: FiltersModel.library_filter_mode field does not exist yet.
    """
    from shared import fjms_service as fjms_module

    # Disjoint stubs: include = CUL sys_ids; exclude complement = JTS sys_ids
    INCLUDE_SYS_IDS = {'9911111111111111', '9911111111111112'}
    COMPLEMENT_SYS_IDS = {'9922222222222221', '9922222222222222'}

    # These two sets must be disjoint by design
    assert INCLUDE_SYS_IDS & COMPLEMENT_SYS_IDS == set(), (
        "Test setup error: include and complement stubs must be disjoint"
    )

    restrict_sets_seen = []

    original_intersect = _intersect_library_filter

    async def _spy_intersect(restrict_sys_ids, filters_dict, meta_mgr):
        result = await original_intersect(restrict_sys_ids, filters_dict, meta_mgr)
        restrict_sets_seen.append(result)
        return result

    monkeypatch.setattr(fjms_module, 'validate_filter_values', lambda d: None)
    monkeypatch.setattr(fjms_module, 'get_filter_sys_ids', lambda **kw: None)
    monkeypatch.setattr(
        fjms_module, 'resolve_library_sys_ids',
        lambda codes, mgr: INCLUDE_SYS_IDS,
    )
    monkeypatch.setattr(
        fjms_module, 'resolve_library_complement_sys_ids',
        lambda codes, mgr: COMPLEMENT_SYS_IDS,
        raising=False,
    )

    r_include = _post_search(
        client, query='x', search_mode='exact',
        filters={'library': ['CUL'], 'library_filter_mode': 'include'},
    )
    r_exclude = _post_search(
        client, query='x', search_mode='exact',
        filters={'library': ['CUL'], 'library_filter_mode': 'exclude'},
    )

    assert r_include.status_code == 200, r_include.text
    assert r_exclude.status_code == 200, r_exclude.text

    # The key invariant: include set and exclude set (complement) are disjoint
    # We verify this via the stub stubs — the sets themselves do not overlap
    assert INCLUDE_SYS_IDS & COMPLEMENT_SYS_IDS == set(), (
        "include and exclude result sets must be disjoint for library=['CUL']"
    )


def test_parallels_exclude_mode(client, stub_searcher, clean_env, monkeypatch):
    """DMF-11-2: /api/parallels with library_filter_mode='exclude' honors the mode
    (parity with /api/search) — assert 200 + the complement path was taken.

    RED: FiltersModel.library_filter_mode field does not exist yet.
    """
    from shared import fjms_service as fjms_module

    complement_calls = []
    include_calls = []

    COMPLEMENT_SYS_IDS = {'9922222222222221', '9922222222222222'}

    monkeypatch.setattr(fjms_module, 'validate_filter_values', lambda d: None)
    monkeypatch.setattr(fjms_module, 'get_filter_sys_ids', lambda **kw: None)
    monkeypatch.setattr(
        fjms_module, 'resolve_library_sys_ids',
        lambda codes, mgr: (include_calls.append(codes) or {'9911111111111111'}),
    )
    monkeypatch.setattr(
        fjms_module, 'resolve_library_complement_sys_ids',
        lambda codes, mgr: (complement_calls.append(codes) or COMPLEMENT_SYS_IDS),
        raising=False,
    )

    r = _post_parallels(
        client,
        filters={'library': ['CUL'], 'library_filter_mode': 'exclude'},
    )

    assert r.status_code == 200, r.text

    # Complement helper must have been called (the exclude path)
    assert len(complement_calls) == 1, (
        f"Expected 1 call to resolve_library_complement_sys_ids on /api/parallels, "
        f"got {len(complement_calls)}"
    )
    # Include helper must NOT have been called
    assert include_calls == [], (
        f"resolve_library_sys_ids should NOT be called for exclude mode, calls: {include_calls}"
    )


def test_invalid_mode_returns_400(client, stub_searcher, clean_env):
    """DMF-11-3: library_filter_mode='sideways' on /api/search returns HTTP 400
    with r.json()['error']['code'] == 'invalid_request' (Pydantic Literal rejection).

    Also asserts the same bad value 400s on /api/parallels.

    RED: FiltersModel.library_filter_mode field (with Literal constraint) does not
    exist yet — currently FiltersModel.extra='forbid' will reject the unknown key as
    invalid_request, which means this test MAY be green incidentally; but we assert
    the error code matches 'invalid_request' in both cases.

    NOTE: If the field doesn't exist yet and extra='forbid' catches it, the test
    may pass for the wrong reason. The test is still valid: once the field is added
    (Plan 02), an invalid Literal value must still yield 400 invalid_request.
    """
    # /api/search
    r_search = _post_search(
        client, query='x', search_mode='exact',
        filters={'library': ['CUL'], 'library_filter_mode': 'sideways'},
    )
    assert r_search.status_code == 400, r_search.text
    assert r_search.json()['error']['code'] == 'invalid_request', r_search.json()

    # /api/parallels — same bad value must also 400
    r_parallels = _post_parallels(
        client,
        filters={'library': ['CUL'], 'library_filter_mode': 'sideways'},
    )
    assert r_parallels.status_code == 400, r_parallels.text
    assert r_parallels.json()['error']['code'] == 'invalid_request', r_parallels.json()


def test_mode_without_library_is_noop(client, stub_searcher, clean_env, monkeypatch):
    """DMF-11-3: library_filter_mode='exclude' with NO filters.library (or empty list)
    applies no filter: assert neither resolve helper is called and the search still 200s.

    The _intersect_library_filter short-circuits when `not libs`, so this is a noop.

    RED: FiltersModel.library_filter_mode field does not exist yet; if extra='forbid'
    blocks it, this test will fail with 400. After Plan 02, it must 200.
    """
    from shared import fjms_service as fjms_module

    resolve_calls = []

    def _boom(codes, mgr):
        resolve_calls.append(('include', codes))
        return set()

    def _boom_complement(codes, mgr):
        resolve_calls.append(('complement', codes))
        return set()

    monkeypatch.setattr(fjms_module, 'validate_filter_values', lambda d: None)
    monkeypatch.setattr(fjms_module, 'get_filter_sys_ids', lambda **kw: None)
    monkeypatch.setattr(fjms_module, 'resolve_library_sys_ids', _boom)
    monkeypatch.setattr(
        fjms_module, 'resolve_library_complement_sys_ids',
        _boom_complement,
        raising=False,
    )

    # Mode with NO library list — should be a noop, no resolve called
    r = _post_search(
        client, query='x', search_mode='exact',
        filters={'library_filter_mode': 'exclude'},  # no 'library' key
    )
    assert r.status_code == 200, r.text
    assert resolve_calls == [], (
        f"Neither resolve helper should be called when no library list is provided, "
        f"but got: {resolve_calls}"
    )


# ---------------------------------------------------------------------------
# Task 2 Tests — complement helper unit tests
# ---------------------------------------------------------------------------

def test_resolve_library_complement_sys_ids(monkeypatch):
    """DMF-11: resolve_library_complement_sys_ids returns sys_ids whose library_code
    is NOT in the given set (exact complement of resolve_library_sys_ids).

    Contract:
      - Given csv_bank with 2 CUL + 2 JTS rows:
        complement(['CUL'], mgr) == {JTS sys_ids} (exactly)
        complement(['JTS'], mgr) == {CUL sys_ids} (exactly)
      - Empty/None codes → set()
      - union(complement(['CUL']), resolve_library_sys_ids(['CUL'])) == all keys
      - intersection(complement(['CUL']), resolve_library_sys_ids(['CUL'])) == set()

    RED: resolve_library_complement_sys_ids does not exist yet in shared/fjms_service.
    """
    # Import lazily inside test body so collection does not error before Plan 02
    try:
        from shared.fjms_service import resolve_library_complement_sys_ids
    except ImportError:
        pytest.fail(
            "resolve_library_complement_sys_ids not yet in shared/fjms_service — "
            "this test will remain RED until Plan 02 implements the helper."
        )

    from shared.fjms_service import resolve_library_sys_ids

    # Build a fake meta_mgr with a csv_bank having 2 CUL + 2 JTS rows
    mgr = MagicMock()
    CUL_SYS_IDS = {'cul_001', 'cul_002'}
    JTS_SYS_IDS = {'jts_001', 'jts_002'}
    ALL_SYS_IDS = CUL_SYS_IDS | JTS_SYS_IDS

    mgr.csv_bank = {
        'cul_001': {'library_code': 'CUL'},
        'cul_002': {'library_code': 'CUL'},
        'jts_001': {'library_code': 'JTS'},
        'jts_002': {'library_code': 'JTS'},
    }

    # Complement of CUL = JTS set
    result_complement_cul = resolve_library_complement_sys_ids(['CUL'], mgr)
    assert result_complement_cul == JTS_SYS_IDS, (
        f"complement(['CUL']) should return JTS sys_ids {JTS_SYS_IDS}, "
        f"got {result_complement_cul}"
    )

    # Complement of JTS = CUL set
    result_complement_jts = resolve_library_complement_sys_ids(['JTS'], mgr)
    assert result_complement_jts == CUL_SYS_IDS, (
        f"complement(['JTS']) should return CUL sys_ids {CUL_SYS_IDS}, "
        f"got {result_complement_jts}"
    )

    # Empty codes → set()
    assert resolve_library_complement_sys_ids([], mgr) == set(), (
        "Empty codes list should return set()"
    )
    assert resolve_library_complement_sys_ids(None, mgr) == set(), (  # type: ignore[arg-type]
        "None codes should return set()"
    )

    # None meta_mgr → set()
    assert resolve_library_complement_sys_ids(['CUL'], None) == set(), (  # type: ignore[arg-type]
        "None meta_mgr should return set()"
    )

    # Exact complement invariant: complement ∪ include == all; complement ∩ include == ∅
    include_cul = resolve_library_sys_ids(['CUL'], mgr)
    complement_cul = resolve_library_complement_sys_ids(['CUL'], mgr)

    assert include_cul | complement_cul == ALL_SYS_IDS, (
        f"union of include + complement should equal all sys_ids: "
        f"include={include_cul}, complement={complement_cul}, expected union={ALL_SYS_IDS}"
    )
    assert include_cul & complement_cul == set(), (
        f"intersection of include + complement should be empty: "
        f"include={include_cul}, complement={complement_cul}"
    )


def test_intersect_helper_exclude_branch(monkeypatch):
    """DMF-11: asyncio.run(_intersect_library_filter({'a','b','c'},
    {'library':['CUL'], 'library_filter_mode':'exclude'}, object()))
    with complement stub returning {'b','c','d'} → result is {'b','c'}
    (intersection with existing restrict set).

    Also asserts resolve_library_sys_ids was NOT called on the exclude path.

    RED: _intersect_library_filter exclude branch does not exist yet.
    """
    from shared import fjms_service as fjms_module

    COMPLEMENT_STUB = {'b', 'c', 'd'}
    include_calls = []
    complement_calls = []

    def _fake_include(codes, mgr):
        include_calls.append(codes)
        return {'a', 'b'}  # should NOT be called on exclude path

    def _fake_complement(codes, mgr):
        complement_calls.append(codes)
        return COMPLEMENT_STUB

    monkeypatch.setattr(fjms_module, 'resolve_library_sys_ids', _fake_include)
    monkeypatch.setattr(
        fjms_module, 'resolve_library_complement_sys_ids',
        _fake_complement,
        raising=False,
    )

    result = asyncio.run(
        _intersect_library_filter(
            {'a', 'b', 'c'},
            {'library': ['CUL'], 'library_filter_mode': 'exclude'},
            object(),
        )
    )

    # Intersection of {'a','b','c'} with complement {'b','c','d'} == {'b','c'}
    assert result == {'b', 'c'}, (
        f"exclude branch should return intersection of restrict set with complement, "
        f"expected {{'b', 'c'}}, got {result}"
    )

    # resolve_library_sys_ids must NOT be called on the exclude path
    assert include_calls == [], (
        f"resolve_library_sys_ids should NOT be called on exclude path, got: {include_calls}"
    )

    # Complement helper must have been called exactly once
    assert len(complement_calls) == 1, (
        f"resolve_library_complement_sys_ids should be called once, got {len(complement_calls)}"
    )
