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

Updated 2026-05-13 (Phase 88 D-02): tests now monkeypatch
``web.safe_storage.app`` to a stub whose ``storage.user`` is a plain
dict. The pre-Phase-88 state-proxy wrapper and the in-process test
backend shim are deleted; tests call
``export_state.set_search_export(...)`` and
``export_state.update_search_export_selection(...)`` directly.

Per Phase 88 Refinement 6 (Codex review): the stub uses
``SimpleNamespace(storage=SimpleNamespace(user=...))`` for
instance-isolated state -- no class-level ``_StubApp.storage.user``
shared across tests.

Builds a bare FastAPI app per fixture (mirrors test_api_export_json.py
HIGH-08 pattern) so handler logic can be exercised without NiceGUI.
"""
import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock
from fastapi import FastAPI
from fastapi.testclient import TestClient


def _make_stub(initial_storage: dict):
    """Instance-isolated stub mirroring app.storage.user surface.

    Per Phase 88 review (Codex LOW, Refinement 6): each invocation returns
    a fresh SimpleNamespace tree -- no class-level state shared across tests.
    """
    return SimpleNamespace(storage=SimpleNamespace(user=initial_storage))


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


@pytest.fixture
def session_with_5_results(mock_meta_mgr, monkeypatch):
    """Populate per-session export payload with 5 results via export_state helpers.

    No state.* setup; no in-process backend shim. The fixture monkeypatches
    ``web.safe_storage.app`` to a SimpleNamespace stub whose
    ``storage.user`` is a fresh dict, then drives the helper to round-trip
    the payload through ``safe_user_set``. Tests mutate selection by calling
    ``export_state.update_search_export_selection(...)``.
    """
    from web.api import state
    from web import export_state

    saved_meta = state.meta_mgr
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

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    # Populate via the helper itself -- proves the helper round-trips correctly.
    export_state.set_search_export(
        results=results,
        query='foo',
        mode='text',
        gap=None,
        filters=None,
        warnings=[],
        selected_uids=None,
    )

    yield storage  # tests mutate selected_uids via export_state.update_search_export_selection

    state.meta_mgr = saved_meta


# --- Gap #2 tests: selection filtering ----------------------------------

def test_export_json_no_selection_returns_full_set(client, session_with_5_results):
    # Already None from fixture; no extra setup needed.
    r = client.get('/api/export/json')
    assert r.status_code == 200
    body = r.json()
    assert body['count'] == 5
    assert len(body['results']) == 5


def test_export_json_with_selection_filters_by_uid(client, session_with_5_results):
    from web import export_state
    export_state.update_search_export_selection(['u1', 'u3'])
    r = client.get('/api/export/json')
    assert r.status_code == 200
    body = r.json()
    assert body['count'] == 2
    returned_uids = {item.get('uid') for item in body['results']}
    assert returned_uids == {'u1', 'u3'}


def test_export_json_empty_selection_treated_as_none(client, session_with_5_results):
    from web import export_state
    export_state.update_search_export_selection([])
    r = client.get('/api/export/json')
    assert r.status_code == 200
    body = r.json()
    assert body['count'] == 5


def test_export_excel_with_selection_filters_filename(client, session_with_5_results):
    from web import export_state
    export_state.update_search_export_selection(['u1', 'u3'])
    r = client.get('/api/export/excel')
    assert r.status_code == 200
    cd = r.headers.get('content-disposition', '')
    assert '-selected-2.xlsx' in cd, f"Expected filename suffix in {cd}"


def test_export_word_with_selection_filters_filename(client, session_with_5_results):
    from web import export_state
    export_state.update_search_export_selection(['u1', 'u3'])
    r = client.get('/api/export/word')
    assert r.status_code == 200
    cd = r.headers.get('content-disposition', '')
    assert '-selected-2.docx' in cd, f"Expected filename suffix in {cd}"


def test_export_json_filename_no_suffix_when_no_selection(client, session_with_5_results):
    from web import export_state
    export_state.update_search_export_selection(None)
    r = client.get('/api/export/json')
    cd = r.headers.get('content-disposition', '')
    assert '-selected-' not in cd, f"Unexpected suffix when no selection: {cd}"


def test_export_json_filename_no_suffix_when_full_set_selected(client, session_with_5_results):
    from web import export_state
    export_state.update_search_export_selection(['u0', 'u1', 'u2', 'u3', 'u4'])
    r = client.get('/api/export/json')
    cd = r.headers.get('content-disposition', '')
    assert '-selected-' not in cd, f"Suffix should only appear when len(filtered) < len(full): {cd}"


# --- Gap #1 test: reset clears per-session payload ----------------------

def test_reset_clears_per_session_payload_then_export_returns_400(client, session_with_5_results):
    """Per-session payload clear (via clear_search_export) gates the export
    handler. Plan 06 Gap #1 fix + 2026-05-12 cross-user fix.

    Pre-Phase-88 this test also asserted legacy ``state.*`` field clears
    propagated; Phase 88 deletes those fields (Plan 88-03). The handler
    only reads through ``web.export_state``, so the per-session clear is
    the only thing that matters.
    """
    from web import export_state
    # Pre-condition: payload is populated by the fixture.
    payload = export_state.get_search_export()
    assert payload is not None and len(payload['results']) == 5

    # The clear the post-Plan-88-01 _reset_search executes:
    export_state.clear_search_export()

    # And the export handler now returns 400 cleanly:
    r = client.get('/api/export/json')
    assert r.status_code == 400
    assert b'No results to export' in r.content


# --- Refinement 4 (Codex review): getter isinstance guard --------------

def test_getters_return_none_on_poisoned_payload(monkeypatch):
    """Phase 88 D-11 extension (Refinement 4 -- Codex review): get_search_export
    and get_parallels_export must return None when storage holds a non-dict at
    the key. Callers in web/api.py do ``payload.get('results')`` which would
    crash TypeError on a non-dict payload; the isinstance guard makes the
    contract explicit.
    """
    from web import export_state

    # Poison search payload with a string.
    storage = {'export_search_payload': 'not-a-dict'}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))
    assert export_state.get_search_export() is None, (
        "isinstance guard missing on get_search_export -- a non-dict payload "
        "would crash callers doing payload.get('results')"
    )

    # Poison parallels payload with a list.
    storage = {'export_parallels_payload': ['not', 'a', 'dict']}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))
    assert export_state.get_parallels_export() is None, (
        "isinstance guard missing on get_parallels_export"
    )

    # Poison with None (existing well-formed missing case).
    storage = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))
    assert export_state.get_search_export() is None
    assert export_state.get_parallels_export() is None
