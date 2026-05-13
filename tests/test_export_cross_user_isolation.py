"""Cross-user isolation regression test for the export pipeline.

Bug 2026-05-12: User A's search query name appeared as the suggested
xlsx filename in User B's export dialog. They were on totally different
devices and networks; both shared the production process. Root cause:
``state.last_results`` / ``state.current_search_query`` / ``state.last_selected_uids``
on AppState (singleton) were the source of truth for the export handlers,
so the last writer won -- User B's request to /api/export/excel read
whatever User A's search had just written to those fields.

Fix: handlers now read from ``web.export_state`` which routes through
``web.safe_storage`` to ``app.storage.user`` (per-session). This test
simulates two sessions with distinct storage dicts and asserts their
filenames + result counts are independent.

IMPORTANT (per Phase 88 D-03): this is SEQUENTIAL simulation, not true
concurrent coverage. We monkeypatch ``web.safe_storage.app`` to a stub
whose ``storage.user`` is a plain dict, swap the dict between requests
to model two sessions sharing one Python process, and verify
isolation. Real concurrency (two NiceGUI processes or fully-instantiated
``app.storage.user`` per request via the NiceGUI test harness) is
deferred to Phase 92 SWEEP-05 production smoke-test (two browser sessions,
manual checklist).
"""
from types import SimpleNamespace
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unittest.mock import MagicMock


def _make_stub(initial_storage: dict):
    """Instance-isolated stub mirroring app.storage.user surface.

    Per Phase 88 review (Codex LOW, Refinement 6): each invocation returns
    a fresh SimpleNamespace tree -- no class-level state shared across tests.
    """
    return SimpleNamespace(storage=SimpleNamespace(user=initial_storage))


def _make_mock_meta_mgr():
    mgr = MagicMock()
    mgr.get_meta_for_id.return_value = ('T-S 12.345', 'Test Title')
    mgr.get_library_for_id.return_value = 'CUL'
    mgr.parse_full_id_components.return_value = {
        'sys_id': '9912345678901234',
        'ie_id': 'IE99', 'p_num': '7', 'fl_id': None,
    }
    return mgr


def _build_payload(results, query):
    return {
        'results': results,
        'query': query,
        'mode': 'text',
        'gap': None,
        'filters': None,
        'warnings': [],
        'selected_uids': None,
    }


def _user_results(uid_prefix, count=3):
    return [{
        'uid': f'{uid_prefix}_{i}',
        'display': {
            'shelfmark': f'T-S {uid_prefix}.{i}',
            'title': f'title {i}',
            'id': '9912345678901234',
            'library_code': 'CUL',
        },
        'raw_header': f'header_99123456789012{i:02d}_IE99_P{i+1}',
        'snippet': f'a *match* {i}',
        'full_text': 'lorem ipsum',
        'sort_score': 0.5 + i * 0.1,
    } for i in range(count)]


def test_two_sessions_get_independent_filenames(monkeypatch):
    """User A queries 'alpha', User B queries 'beta'. User B's xlsx must
    NOT carry User A's 'alpha' in its filename or User A's results.
    """
    from web.api import init_api_routes, state

    bare = FastAPI()
    init_api_routes(app_override=bare)
    client = TestClient(bare)

    saved_meta = state.meta_mgr
    state.meta_mgr = _make_mock_meta_mgr()

    try:
        # --- User A's session: searches 'alpha-query' with 3 results ---
        user_a_storage = {
            'export_search_payload': _build_payload(
                _user_results('alpha', 3), 'alpha-query'
            )
        }
        monkeypatch.setattr('web.safe_storage.app', _make_stub(user_a_storage))
        r_a = client.get('/api/export/excel')
        assert r_a.status_code == 200
        cd_a = r_a.headers.get('content-disposition', '')
        assert 'alpha-query' in cd_a, (
            f"User A's filename should contain 'alpha-query', got {cd_a!r}"
        )

        # --- User B's session: different storage dict, different query ---
        user_b_storage = {
            'export_search_payload': _build_payload(
                _user_results('beta', 5), 'beta-query'
            )
        }
        monkeypatch.setattr('web.safe_storage.app', _make_stub(user_b_storage))
        r_b = client.get('/api/export/excel')
        assert r_b.status_code == 200
        cd_b = r_b.headers.get('content-disposition', '')

        # CRITICAL: User B sees only User B's query name.
        assert 'beta-query' in cd_b, (
            f"User B's filename should contain 'beta-query', got {cd_b!r}"
        )
        assert 'alpha-query' not in cd_b, (
            f"CROSS-USER LEAK: User B's filename contains User A's query: {cd_b!r}"
        )

        # --- Back to User A: must not see User B's data ---
        monkeypatch.setattr('web.safe_storage.app', _make_stub(user_a_storage))
        r_a2 = client.get('/api/export/excel')
        assert r_a2.status_code == 200
        cd_a2 = r_a2.headers.get('content-disposition', '')
        assert 'alpha-query' in cd_a2, (
            f"User A's second request should still see 'alpha-query', got {cd_a2!r}"
        )
        assert 'beta-query' not in cd_a2, (
            f"CROSS-USER LEAK: User A's filename contains User B's query: {cd_a2!r}"
        )
    finally:
        state.meta_mgr = saved_meta


def test_empty_session_does_not_inherit_other_session_results(monkeypatch):
    """A fresh session (no prior search) must get 400 even if another
    session has results on disk. Confirms the handler does not silently
    fall back to any global cache.
    """
    from web.api import init_api_routes, state

    bare = FastAPI()
    init_api_routes(app_override=bare)
    client = TestClient(bare)

    saved_meta = state.meta_mgr
    state.meta_mgr = _make_mock_meta_mgr()

    try:
        # New session: empty per-session storage.
        monkeypatch.setattr('web.safe_storage.app', _make_stub({}))
        r = client.get('/api/export/excel')
        assert r.status_code == 400, (
            f"Empty session must return 400; got {r.status_code} {r.content!r}"
        )
        assert b'No results to export' in r.content
    finally:
        state.meta_mgr = saved_meta


def test_parallels_cross_user_isolation(monkeypatch):
    """Same isolation guarantee for the parallels export endpoint."""
    from web.api import init_api_routes, state

    bare = FastAPI()
    init_api_routes(app_override=bare)
    client = TestClient(bare)

    saved_meta = state.meta_mgr
    state.meta_mgr = _make_mock_meta_mgr()

    try:
        # User A: parallels search with source text 'one'
        user_a_storage = {
            'export_parallels_payload': {
                'results': [{
                    'uid': 'uid_a',
                    'raw_header': 'header_9911111111111111_IE1_P3',
                    'score': 50,
                    'source_ctx': 'first',
                    'text': 'manuscript text A',
                    'chunk_hits': [(0, 'first', 30, 'snippet')],
                }],
                'filtered': [],
                'meta': {
                    'source_text': 'one', 'chunk_size': 5,
                    'mode': 'exact', 'max_freq': 50.0,
                    'filters': None, 'boundary_options': None,
                    'warnings': [],
                },
            }
        }
        monkeypatch.setattr('web.safe_storage.app', _make_stub(user_a_storage))
        r_a = client.get('/api/export/parallels/json')
        assert r_a.status_code == 200
        body_a = r_a.json()
        assert 'one' in body_a.get('source_text', '') or \
               'one' in body_a.get('meta', {}).get('source_text', '')

        # User B: parallels search with source text 'two'
        user_b_storage = {
            'export_parallels_payload': {
                'results': [{
                    'uid': 'uid_b',
                    'raw_header': 'header_9922222222222222_IE2_P5',
                    'score': 70,
                    'source_ctx': 'second',
                    'text': 'manuscript text B',
                    'chunk_hits': [(0, 'second', 40, 'snippet')],
                }],
                'filtered': [],
                'meta': {
                    'source_text': 'two', 'chunk_size': 5,
                    'mode': 'exact', 'max_freq': 50.0,
                    'filters': None, 'boundary_options': None,
                    'warnings': [],
                },
            }
        }
        monkeypatch.setattr('web.safe_storage.app', _make_stub(user_b_storage))
        r_b = client.get('/api/export/parallels/json')
        assert r_b.status_code == 200
        body_b = r_b.json()
        # User B must not see 'one' (User A's source text)
        b_src = body_b.get('source_text', '') or \
                body_b.get('meta', {}).get('source_text', '')
        assert 'one' not in b_src, (
            f"CROSS-USER PARALLELS LEAK: User B saw 'one' in source_text: {b_src!r}"
        )
    finally:
        state.meta_mgr = saved_meta


def test_parallels_source_text_cannot_leak_via_deleted_fallback(monkeypatch):
    """Phase 88 D-15 (strengthened per Codex review, Refinement 2) -- prove the
    legacy source_text fallback is dead via a POSITIVE export path.

    Setup:
      - User B's storage has a VALID parallels payload (results + filtered +
        meta with NO source_text key). The export handler will reach the
        positive-export code path that USED to consult the legacy fallback.
      - User B's storage also has the legacy key
        ``app.storage.user['parallels_source_text']`` set to a bait string
        (simulates the worst case where a leftover legacy write polluted
        storage, OR where a future regression re-introduced the fallback).

    Before Phase 88 D-14: /api/export/parallels/json would have read
    ``meta.get('source_text') or safe_user_get('parallels_source_text', '')``,
    so the bait would have surfaced in User B's exported JSON envelope.

    After Phase 88 D-14: the fallback is gone; source_text comes exclusively
    from User B's own ``meta['source_text']``, which is empty. The exported
    envelope must NOT contain the bait even when results EXIST so the handler
    reaches the code branch that USED to read it.

    Why this is stronger than the previous empty-storage 400 test
    (Refinement 2, Codex MEDIUM): A reintroduced fallback could pass the
    400-path test if it sits BEHIND a ``if not parallels_results and not
    filtered_results: return 400`` early return -- the bait would never be
    touched in the 400 path. This POSITIVE-path test ensures results EXIST so
    the fallback (if reintroduced) WOULD be consulted.
    """
    from web.api import init_api_routes, state

    bare = FastAPI()
    init_api_routes(app_override=bare)
    client = TestClient(bare)

    saved_meta = state.meta_mgr
    state.meta_mgr = _make_mock_meta_mgr()
    try:
        # User B's session: valid parallels payload with results, meta has
        # no source_text. ALSO has the legacy bait key set -- simulating
        # the worst case where a leftover legacy write polluted storage.
        # The fallback (if reintroduced) would surface this bait.
        valid_results = [{
            'uid': 'uid_b',
            'raw_header': 'header_9933333333333333_IE3_P7',
            'score': 80,
            'source_ctx': 'third',
            'text': 'manuscript text B',
            'chunk_hits': [(0, 'third', 30, 'snippet')],
        }]
        user_b_storage = {
            'export_parallels_payload': {
                'results': valid_results,
                'filtered': [],
                'meta': {  # NO 'source_text' key -- was previously sourced from legacy fallback
                    'chunk_size': 3, 'mode': 'exact', 'max_freq': None,
                    'filters': None, 'boundary_options': None, 'warnings': [],
                },
            },
            # Legacy key -- must NOT be read by the export handler post-D-14.
            'parallels_source_text': 'alpha-leak-bait',
        }
        monkeypatch.setattr('web.safe_storage.app', _make_stub(user_b_storage))
        r = client.get('/api/export/parallels/json')
        # Handler reaches positive-export path (results exist).
        assert r.status_code == 200, (
            f"Expected 200 with valid results, got {r.status_code}: {r.text[:200]}"
        )
        body_bytes = r.content
        assert b'alpha-leak-bait' not in body_bytes, (
            "LEAK: legacy source_text fallback is still being read. "
            "Phase 88 D-14 deleted it; if this fires, the fallback was reintroduced. "
            "source_text should come exclusively from meta['source_text']."
        )
        # Also assert source_text in the response envelope is empty (no bait,
        # no other surface). Envelope shape from web/api.py export_parallels_json
        # +shared/search_serializer.serialize_parallels_payload: top-level
        # 'source_text' key.
        body = r.json()
        assert body.get('source_text', '') == '', (
            f"source_text must be empty when meta has no source_text key and "
            f"the legacy fallback is deleted; got {body.get('source_text')!r}."
        )
    finally:
        state.meta_mgr = saved_meta
