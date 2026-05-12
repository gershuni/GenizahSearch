"""
Phase 77 Plan 06 -- gap-closure regression tests.

Covers:
  Gap #1 (UAT test 8): _reset_search clears the export payload so
                       post-reset exports return 400.
  Gap #2 (UAT test 9): exports honor session_payload['selected_uids']:
                       - None  -> full set
                       - list  -> uid-filtered subset
                       - []    -> defensive -- treated as None
                       Filename gets '-selected-N' suffix when filtered.

Updated 2026-05-12: exports read from the per-session export_state payload
(web.export_state.get_search_export) instead of the cross-user-leaky
state.* singleton fields. Tests inject a stub backend dict via the
``_TEST_BACKEND`` hook on web.export_state. The singleton state.* fields
are still cleared by ``_reset_search`` for legacy callers, but the
export handlers themselves only consult the per-session payload.

Builds a bare FastAPI app per fixture (mirrors test_api_export_json.py
HIGH-08 pattern) so handler logic can be exercised without NiceGUI.
"""
import pytest
from unittest.mock import MagicMock
from fastapi import FastAPI
from fastapi.testclient import TestClient


@pytest.fixture(scope='module')
def bare_app_with_routes():
    """Build a bare FastAPI app and register Phase 77 routes onto it.

    Per HIGH-08 commentary in tests/test_api_export_json.py:
    init_api_routes(app_override=bare) is idempotent and safe to call
    multiple times across the test session because each call targets
    the bare-app passed in (not nicegui.app).
    """
    from web.api import init_api_routes
    bare = FastAPI()
    init_api_routes(app_override=bare)
    return bare


@pytest.fixture
def client(bare_app_with_routes):
    return TestClient(bare_app_with_routes)


@pytest.fixture
def mock_meta_mgr():
    mgr = MagicMock()
    mgr.get_meta_for_id.return_value = ('T-S 12.345', 'Test Title')
    mgr.get_library_for_id.return_value = 'CUL'
    mgr.parse_full_id_components.return_value = {
        'sys_id': '9912345678901234',
        'ie_id': 'IE99', 'p_num': '7', 'fl_id': None,
    }
    return mgr


class _StateProxy:
    """Backwards-compat wrapper exposing ``last_selected_uids`` writes that
    propagate into the per-session export payload.

    Pre-2026-05-12 tests set ``state.last_selected_uids = ...`` directly to
    drive the singleton export path. The handlers now read from
    ``web.export_state`` instead. This proxy mirrors the assignment into
    the stub backend's selected_uids field so the same test ergonomics
    keep working.
    """
    def __init__(self, state, backend):
        self._state = state
        self._backend = backend

    def __setattr__(self, name, value):
        if name in ('_state', '_backend'):
            super().__setattr__(name, value)
            return
        if name == 'last_selected_uids':
            payload = self._backend.get('export_search_payload')
            if payload is not None:
                payload['selected_uids'] = value
                self._backend['export_search_payload'] = payload
        setattr(self._state, name, value)

    def __getattr__(self, name):
        return getattr(self._state, name)


@pytest.fixture
def state_with_5_results(mock_meta_mgr, monkeypatch):
    """Populate per-session export payload (and legacy state.*) with 5 results."""
    from web.api import state
    import web.export_state as export_state

    saved = {
        'last_results': state.last_results,
        'current_search_query': state.current_search_query,
        'current_search_mode': getattr(state, 'current_search_mode', 'text'),
        'current_search_gap': getattr(state, 'current_search_gap', None),
        'last_filters_applied': getattr(state, 'last_filters_applied', None),
        'last_search_warnings': getattr(state, 'last_search_warnings', []),
        'last_selected_uids': getattr(state, 'last_selected_uids', None),
        'meta_mgr': state.meta_mgr,
    }
    state.meta_mgr = mock_meta_mgr
    results = [{
        'uid': f'u{i}',
        'display': {
            'shelfmark': f'T-S 12.34{i}',
            'title': f'title {i}',
            'id': '9912345678901234',
            'library_code': 'CUL',
        },
        'raw_header': f'header_99123456789012{i:02d}_IE99_P{i+1}',
        'snippet': f'a *match* {i}',
        'full_text': 'lorem ipsum',
        'sort_score': 0.5 + i * 0.1,
    } for i in range(5)]
    state.last_results = results
    state.current_search_query = 'foo'
    state.current_search_mode = 'text'
    state.current_search_gap = None
    state.last_filters_applied = None
    state.last_search_warnings = []
    state.last_selected_uids = None

    # Inject stub backend so export_state functions route through a local
    # dict instead of NiceGUI's app.storage.user (unavailable in TestClient).
    fake_backend = {
        'export_search_payload': {
            'results': results,
            'query': 'foo',
            'mode': 'text',
            'gap': None,
            'filters': None,
            'warnings': [],
            'selected_uids': None,
        }
    }
    monkeypatch.setattr(export_state, '_TEST_BACKEND', fake_backend)

    yield _StateProxy(state, fake_backend)
    for k, v in saved.items():
        setattr(state, k, v)


# --- Gap #2 tests: selection filtering ----------------------------------

def test_export_json_no_selection_returns_full_set(client, state_with_5_results):
    state_with_5_results.last_selected_uids = None
    r = client.get('/api/export/json')
    assert r.status_code == 200
    body = r.json()
    assert body['count'] == 5
    assert len(body['results']) == 5


def test_export_json_with_selection_filters_by_uid(client, state_with_5_results):
    state_with_5_results.last_selected_uids = ['u1', 'u3']
    r = client.get('/api/export/json')
    assert r.status_code == 200
    body = r.json()
    assert body['count'] == 2
    returned_uids = {item.get('uid') for item in body['results']}
    assert returned_uids == {'u1', 'u3'}


def test_export_json_empty_selection_treated_as_none(client, state_with_5_results):
    state_with_5_results.last_selected_uids = []
    r = client.get('/api/export/json')
    assert r.status_code == 200
    body = r.json()
    assert body['count'] == 5


def test_export_excel_with_selection_filters_filename(client, state_with_5_results):
    state_with_5_results.last_selected_uids = ['u1', 'u3']
    r = client.get('/api/export/excel')
    assert r.status_code == 200
    cd = r.headers.get('content-disposition', '')
    assert '-selected-2.xlsx' in cd, f"Expected filename suffix in {cd}"


def test_export_word_with_selection_filters_filename(client, state_with_5_results):
    state_with_5_results.last_selected_uids = ['u1', 'u3']
    r = client.get('/api/export/word')
    assert r.status_code == 200
    cd = r.headers.get('content-disposition', '')
    assert '-selected-2.docx' in cd, f"Expected filename suffix in {cd}"


def test_export_json_filename_no_suffix_when_no_selection(client, state_with_5_results):
    state_with_5_results.last_selected_uids = None
    r = client.get('/api/export/json')
    cd = r.headers.get('content-disposition', '')
    assert '-selected-' not in cd, f"Unexpected suffix when no selection: {cd}"


def test_export_json_filename_no_suffix_when_full_set_selected(client, state_with_5_results):
    state_with_5_results.last_selected_uids = ['u0', 'u1', 'u2', 'u3', 'u4']
    r = client.get('/api/export/json')
    cd = r.headers.get('content-disposition', '')
    assert '-selected-' not in cd, f"Suffix should only appear when len(filtered) < len(full): {cd}"


# --- Gap #1 test: reset clears global state -----------------------------

def test_reset_clears_global_state_then_export_returns_400(client, state_with_5_results):
    """Manually performs the exact assignments _reset_search does at
    web/pages/search.py:_reset_search end-block (Plan 06 Gap #1 fix +
    2026-05-12 cross-user fix).

    Asserts the legacy state.* fields are cleared, the per-session
    export payload is cleared, and a follow-up export returns 400.
    """
    # Pre-condition: state populated.
    assert len(state_with_5_results.last_results) == 5
    assert state_with_5_results.current_search_query == 'foo'

    # The exact block _reset_search now executes:
    state_with_5_results.last_results = []
    state_with_5_results.current_search_query = ''
    state_with_5_results.current_search_mode = 'exact'
    state_with_5_results.current_search_gap = None
    state_with_5_results.last_filters_applied = None
    state_with_5_results.last_search_warnings = []
    state_with_5_results.last_selected_uids = None
    # 2026-05-12: per-session clear is what gates the export handler now.
    from web.export_state import clear_search_export
    clear_search_export()

    # Each field at its documented default:
    assert state_with_5_results.last_results == []
    assert state_with_5_results.current_search_query == ''
    assert state_with_5_results.current_search_mode == 'exact'
    assert state_with_5_results.current_search_gap is None
    assert state_with_5_results.last_filters_applied is None
    assert state_with_5_results.last_search_warnings == []
    assert state_with_5_results.last_selected_uids is None

    # And the export handler now returns 400 cleanly:
    r = client.get('/api/export/json')
    assert r.status_code == 400
    assert b'No results to export' in r.content
