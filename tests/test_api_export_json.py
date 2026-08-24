"""
Phase 77 Plan 04 - handler behavior tests for the new JSON download routes.

Covers VALIDATION.md rows EXPORT-01 (handler-empty) and EXPORT-02 (handler-empty)
and the populated-state 200 paths for both /api/export/json and /api/export/parallels/json.

HIGH-08 (codex review): builds a bare FastAPI app per test fixture and registers
routes onto it via init_api_routes(bare_app). The NiceGUI global app is not
mutated -- calling init_api_routes() multiple times in a test session is safe
because each call targets the bare-app passed in. Routes registered onto a
test-scoped FastAPI instance are GC'd at fixture teardown.

Updated 2026-05-13 (Phase 88 D-02/D-04): payload now read from per-session
``web.export_state`` (formerly state.* singleton). Tests monkeypatch
``web.safe_storage.app`` to a SimpleNamespace stub (Refinement 6) and use
``export_state.set_search_export(...)`` / ``set_parallels_export(...)`` to
populate the per-session payload -- no state.X = ... fixture setup, no
in-process backend shim.
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

    HIGH-08: this fixture does NOT touch nicegui.app. Tests that depend on
    NiceGUI session state are out of scope for this file; pure handler
    behavior is exercised through the same code path init_api_routes
    registers in production, just routed onto a disposable FastAPI app.
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
    mgr.get_meta_for_id.return_value = ("T-S 12.345", "Test Title")
    mgr.get_library_for_id.return_value = "CUL"
    mgr.parse_full_id_components.return_value = {
        'sys_id': '9912345678901234',
        'ie_id': 'IE99',
        'p_num': '7',
        'fl_id': None,
    }
    return mgr


@pytest.fixture
def populated_search_state(mock_meta_mgr, monkeypatch):
    """Populate per-session search export payload via export_state helper.

    Phase 88 D-02/D-04: monkeypatches web.safe_storage.app with a
    SimpleNamespace stub (Refinement 6), then calls
    ``export_state.set_search_export(...)`` to round-trip the payload
    through ``safe_user_set``. No state.X = ... fixture setup.
    """
    from web.api import state
    from web import export_state
    saved_meta = state.meta_mgr
    state.meta_mgr = mock_meta_mgr

    results = [{
        'uid': 'uid_001',
        'display': {
            'shelfmark': 'T-S 12.345',
            'title': 'test',
            'id': '9912345678901234',
            'library_code': 'CUL',
        },
        'raw_header': 'header_9912345678901234_IE99_P7',
        'snippet': 'a *match* here',
        'full_text': 'lorem ipsum',
        'sort_score': 0.5,
    }]

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))
    export_state.set_search_export(
        results=results,
        query='foo',
        mode='text',
        gap=None,
        filters=None,
        warnings=[],
        selected_uids=None,
    )
    yield state
    state.meta_mgr = saved_meta


@pytest.fixture
def empty_search_state(monkeypatch):
    """Empty per-session storage -- export handler must return 400."""
    monkeypatch.setattr('web.safe_storage.app', _make_stub({}))
    from web.api import state
    yield state


@pytest.fixture
def populated_parallels_state(mock_meta_mgr, monkeypatch):
    """Populate per-session parallels export payload via export_state helper."""
    from web.api import state
    from web import export_state
    saved_meta = state.meta_mgr
    state.meta_mgr = mock_meta_mgr

    parallels_results = [{
        'uid': 'uid_a',
        'raw_header': 'header_9911111111111111_IE1_P3',
        'score': 50,
        'source_ctx': 'first chunk',
        'text': 'manuscript text',
        'chunk_hits': [(0, 'first chunk', 30, 'manuscript snippet')],
    }]
    meta = {
        'source_text': 'hello world',
        'chunk_size': 5,
        'mode': 'exact',
        'max_freq': 50.0,
        'filters': None,
        'boundary_options': None,
        'warnings': [],
    }

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))
    export_state.set_parallels_export(
        results=parallels_results,
        filtered=[],
        meta=meta,
    )
    yield state
    state.meta_mgr = saved_meta


@pytest.fixture
def empty_parallels_state(monkeypatch):
    """Empty per-session storage -- parallels export handler must return 400."""
    monkeypatch.setattr('web.safe_storage.app', _make_stub({}))
    from web.api import state
    yield state


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_export_json_handler_empty(client, empty_search_state):
    """EXPORT-01: empty per-session storage -> 400 + body 'No results to export'."""
    r = client.get('/api/export/json')
    assert r.status_code == 400
    assert b'No results to export' in r.content


def test_export_json_handler_populated(client, populated_search_state):
    """EXPORT-01: populated state -> 200 + Content-Disposition + JSON body shape."""
    r = client.get('/api/export/json')
    assert r.status_code == 200, r.content
    assert r.headers.get('content-type', '').startswith('application/json')
    cd = r.headers.get('content-disposition', '')
    assert 'genizah-search-' in cd
    assert '.json' in cd
    body = r.json()
    assert isinstance(body, dict)
    assert 'results' in body and isinstance(body['results'], list)


def test_export_parallels_json_handler_empty(client, empty_parallels_state):
    """EXPORT-02: empty parallels state -> 400 + body 'No parallels results to export'."""
    r = client.get('/api/export/parallels/json')
    assert r.status_code == 400
    assert b'No parallels results to export' in r.content


def test_export_parallels_json_handler_populated(client, populated_parallels_state):
    """EXPORT-02: populated parallels state -> 200 + Content-Disposition + JSON body shape."""
    r = client.get('/api/export/parallels/json')
    assert r.status_code == 200, r.content
    assert r.headers.get('content-type', '').startswith('application/json')
    cd = r.headers.get('content-disposition', '')
    assert 'genizah-parallels-' in cd
    assert '.json' in cd
    body = r.json()
    assert isinstance(body, dict)
    assert 'results' in body and isinstance(body['results'], list)
    assert 'filtered' in body and isinstance(body['filtered'], list)


def test_init_api_routes_does_not_mutate_nicegui_singleton():
    """HIGH-08: calling init_api_routes(bare_app) does NOT mutate nicegui.app.

    Sanity check that the app_override path actually targets the passed app,
    not the global. Construct a fresh bare app, register, and confirm the
    NiceGUI app's route count is unchanged.
    """
    from web.api import init_api_routes
    from nicegui import app as nicegui_app

    # Snapshot NiceGUI app's route count before
    before = len(nicegui_app.routes) if hasattr(nicegui_app, 'routes') else 0
    bare = FastAPI()
    init_api_routes(app_override=bare)
    after = len(nicegui_app.routes) if hasattr(nicegui_app, 'routes') else 0
    assert after == before, \
        f"NiceGUI singleton was mutated: routes {before} -> {after}. HIGH-08 regression."
    # And the bare app got the routes
    assert len(bare.routes) > 0, "Bare app got no routes -- app_override dispatch broken."


# ---------------------------------------------------------------------------
# A multi-witness JSON export must present the fused ranking.
# ---------------------------------------------------------------------------

@pytest.fixture
def multi_witness_parallels_state(mock_meta_mgr, monkeypatch):
    """A FUSED two-row payload where fusion and raw score disagree on order.

    The high-score row has the LOWER fusion score, so an export that re-ranks
    by summed matched letters returns them in the opposite order to the page.
    """
    from web.api import state
    from web import export_state
    saved_meta = state.meta_mgr
    state.meta_mgr = mock_meta_mgr
    # The shared mock returns ONE fixed sys_id for every header, which would
    # group both rows into a single manuscript and leave no order to test.
    mock_meta_mgr.parse_full_id_components.side_effect = lambda h: {
        'sys_id': (h or '').split('_')[1], 'ie_id': 'IE1', 'p_num': '3',
        'fl_id': None,
    }

    def _row(uid, sys_id, score, fusion, ids):
        return {
            'uid': uid,
            'raw_header': f'header_{sys_id}_IE1_P3',
            'score': score,
            'final_score': score,
            'source_ctx': 'chunk',
            'text': 'manuscript text',
            'chunk_hits': [(0, 'chunk', 30, 'snippet')],
            'fusion_score': fusion,
            'witness_count': len(ids.split(',')),
            'witness_ids': ids,
            'witness_id': ids.split(',')[0],
            'witness_label': 'Witness ' + ids.split(',')[0],
            'best_witness_score': score * 2,
        }

    results = [
        _row('uid_a', '9911111111111111', 50, 0.01, 'w1'),
        _row('uid_b', '9922222222222222', 10, 0.09, 'w1,w2'),
    ]
    meta = {
        'source_text': 'hello world', 'chunk_size': 5, 'mode': 'exact',
        'max_freq': 50.0, 'filters': None, 'boundary_options': None,
        'warnings': [],
        'witnesses': [{'label': 'W2', 'kind': 'pasted', 'sys_id': None}],
        'multi_witness': True,
    }
    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))
    export_state.set_parallels_export(results=results, filtered=[], meta=meta)
    yield state
    state.meta_mgr = saved_meta


def test_the_parallels_json_export_keeps_the_fused_order(
    client, multi_witness_parallels_state,
):
    """The export called `serialize_parallels_payload` with no `order_key`, so
    a downloaded file listed the same manuscripts in a different order than
    the page that produced it, with nothing to explain the difference."""
    r = client.get('/api/export/parallels/json')
    assert r.status_code == 200, r.content
    results = r.json()['results']
    assert len(results) == 2
    # The 10-letter, high-fusion group must come first.
    assert results[0]['score'] == 10.0, (
        'the export re-ranked by matched letters instead of by fusion'
    )
    assert results[1]['score'] == 50.0


def test_the_parallels_json_export_carries_the_witness_facts(
    client, multi_witness_parallels_state,
):
    """Every exported row already carried them; the serializer was simply
    never told to emit them, so a downloaded file could not say which
    witnesses found what."""
    r = client.get('/api/export/parallels/json')
    assert r.status_code == 200, r.content
    fused = [g for g in r.json()['results'] if g['score'] == 10.0][0]
    assert fused['witness_fusion']['witness_count'] == 2
    assert fused['witness_fusion']['witness_ids'] == ['w1', 'w2']
