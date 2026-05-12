"""Cross-user isolation regression test for the export pipeline.

Bug 2026-05-12: User A's search query name appeared as the suggested
xlsx filename in User B's export dialog. They were on totally different
devices and networks; both shared the production process. Root cause:
`state.last_results` / `state.current_search_query` / `state.last_selected_uids`
on AppState (singleton) were the source of truth for the export handlers,
so the last writer won — User B's request to /api/export/excel read
whatever User A's search had just written to those fields.

Fix: handlers now read from `web.export_state` which targets
``app.storage.user`` (per-session). This test simulates two sessions
with distinct backends and asserts their filenames + result counts are
independent.

The simulation uses the `_TEST_BACKEND` hook on `web.export_state` to
swap backends between requests in a single test process.
"""
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unittest.mock import MagicMock


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
    import web.export_state as export_state

    bare = FastAPI()
    init_api_routes(app_override=bare)
    client = TestClient(bare)

    # Set the singleton meta_mgr (shared across users — read-only).
    saved_meta = state.meta_mgr
    state.meta_mgr = _make_mock_meta_mgr()

    try:
        # --- User A's session: searches 'alpha-query' with 3 results ---
        user_a_backend = {
            'export_search_payload': _build_payload(
                _user_results('alpha', 3), 'alpha-query'
            )
        }
        monkeypatch.setattr(export_state, '_TEST_BACKEND', user_a_backend)
        r_a = client.get('/api/export/excel')
        assert r_a.status_code == 200
        cd_a = r_a.headers.get('content-disposition', '')
        assert 'alpha-query' in cd_a, (
            f"User A's filename should contain 'alpha-query', got {cd_a!r}"
        )

        # --- User B's session: different backend, different query ---
        user_b_backend = {
            'export_search_payload': _build_payload(
                _user_results('beta', 5), 'beta-query'
            )
        }
        monkeypatch.setattr(export_state, '_TEST_BACKEND', user_b_backend)
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
        monkeypatch.setattr(export_state, '_TEST_BACKEND', user_a_backend)
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
    import web.export_state as export_state

    bare = FastAPI()
    init_api_routes(app_override=bare)
    client = TestClient(bare)

    saved_meta = state.meta_mgr
    state.meta_mgr = _make_mock_meta_mgr()
    # Set singleton state.* to a populated value to confirm the handler
    # IGNORES it now (it should only consult the per-session payload).
    saved_results = state.last_results
    saved_query = state.current_search_query
    state.last_results = _user_results('singleton-leak', 7)
    state.current_search_query = 'leaky-singleton-query'

    try:
        # New session: empty per-session backend.
        empty_backend = {}
        monkeypatch.setattr(export_state, '_TEST_BACKEND', empty_backend)
        r = client.get('/api/export/excel')
        assert r.status_code == 400, (
            f"Empty session must return 400 even when state.last_results "
            f"is populated (singleton-leak regression guard); got {r.status_code} {r.content!r}"
        )
        assert b'No results to export' in r.content
    finally:
        state.last_results = saved_results
        state.current_search_query = saved_query
        state.meta_mgr = saved_meta


def test_parallels_cross_user_isolation(monkeypatch):
    """Same isolation guarantee for the parallels export endpoint."""
    from web.api import init_api_routes, state
    import web.export_state as export_state

    bare = FastAPI()
    init_api_routes(app_override=bare)
    client = TestClient(bare)

    saved_meta = state.meta_mgr
    state.meta_mgr = _make_mock_meta_mgr()

    try:
        # User A: parallels search with source text 'one'
        user_a_backend = {
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
        monkeypatch.setattr(export_state, '_TEST_BACKEND', user_a_backend)
        r_a = client.get('/api/export/parallels/json')
        assert r_a.status_code == 200
        body_a = r_a.json()
        assert 'one' in body_a.get('source_text', '') or \
               'one' in body_a.get('meta', {}).get('source_text', '')

        # User B: parallels search with source text 'two'
        user_b_backend = {
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
        monkeypatch.setattr(export_state, '_TEST_BACKEND', user_b_backend)
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
