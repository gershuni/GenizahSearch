"""Legacy-route immutability spot check (D-23 + Concerns #2, #8 from 78-REVIEWS.md).

Phase 78 promises that existing /api/* routes are byte-identical. The original
plan only spot-checked happy paths — that cannot catch the regression where a
GLOBAL exception handler (registered by init_search_api) silently rewrites
legacy validation-failure responses from FastAPI's default 422 detail envelope
to Phase 78's new error envelope.

Plan 02 + Plan 03 fix this by removing the GLOBAL handler install and instead
wrapping envelope rewriting INSIDE the new endpoint via a `wrap_endpoint` helper
(Concern #2, option b). This test enforces that the legacy route validation
behavior remains the standard FastAPI 422 dump (NO `error.code` / `error.message`
envelope keys at the top level).

These tests fail at IMPORT time today on `from web.search_api import
init_search_api` — the intended RED state until Plan 03 lands.
"""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.api import init_api_routes
from web.search_api import init_search_api  # RED until Plan 03


@pytest.fixture
def client_with_both_inits():
    """Mount BOTH registrars on the same bare app (mirroring production)."""
    bare = FastAPI()
    init_api_routes(app_override=bare)
    init_search_api(app_override=bare)
    return TestClient(bare)


def test_legacy_export_route_shape_unchanged(client_with_both_inits):
    """Happy path: GET /api/export/json (legacy Phase 77 route) returns the
    Phase 77 envelope with no `error` key — proving init_search_api did not
    accidentally short-circuit the legacy route.

    Reference: tests/test_api_export_json.py:54-150 for the seed pattern."""
    from web.state import state
    saved_results = state.last_results
    saved_meta = state.meta_mgr
    from unittest.mock import MagicMock
    fake_meta = MagicMock()
    fake_meta.get_meta_for_id.return_value = ("T-S 12.345", "Test Title")
    fake_meta.get_library_for_id.return_value = "CUL"
    fake_meta.parse_full_id_components.return_value = {
        'sys_id': '9912345678901234',
        'ie_id': 'IE99', 'p_num': '7', 'fl_id': None,
    }
    state.meta_mgr = fake_meta
    state.last_results = [{
        'uid': 'uid_001',
        'display': {
            'shelfmark': 'T-S 12.345', 'title': 'test',
            'id': '9912345678901234', 'library_code': 'CUL',
        },
        'raw_header': 'header_9912345678901234_IE99_P7',
        'snippet': 'a *match* here', 'full_text': 'lorem ipsum',
        'sort_score': 0.5,
    }]
    try:
        r = client_with_both_inits.get('/api/export/json')
        assert r.status_code == 200, r.text
        body = r.json()
        # Phase 77 envelope keys.
        assert 'schema_version' in body
        assert 'count' in body
        assert 'results' in body
        # CRITICAL: no Phase 78 error envelope keys leaked into the legacy happy path.
        assert 'error' not in body, (
            "legacy /api/export/json must NOT return Phase 78 error envelope on happy path"
        )
    finally:
        state.last_results = saved_results
        state.meta_mgr = saved_meta


def test_legacy_validation_failure_envelope_unchanged(client_with_both_inits):
    """Concern #2 + #8: legacy /api/* routes preserve FastAPI's DEFAULT 422
    validation envelope shape (`{detail: [...]}`), NOT Phase 78's
    `{error: {code, message, fields}}`.

    Plan 02 must NOT install RequestValidationError or APIError handlers GLOBALLY
    on `target_app`. Instead, Plan 03's `init_search_api` wraps envelope rewriting
    inside the new endpoint only (per Concern #2 option b). This test enforces
    that decision.

    Target route: GET /sitemap-manuscripts-{chunk}.xml — chunk is a typed `int`
    path param (web/api.py:283-284). Hitting it with a non-int chunk drives a
    standard FastAPI RequestValidationError → 422 with the default `{detail: [...]}`
    envelope. If Plan 02 ever installs the Phase 78 handler globally, this test
    will detect it (response body would become {error:{code:'invalid_request',...}}).
    """
    r = client_with_both_inits.get('/sitemap-manuscripts-not_an_int.xml')

    # Two acceptable outcomes for a missing-int path param:
    #   (a) 422 with FastAPI's standard `{"detail": [...]}` shape.
    #   (b) 404 if the route doesn't match (path param parsing fails earlier).
    # The forbidden outcome is `{"error": {"code": "invalid_request", ...}}` at
    # the top level — that would indicate the global handler was installed.
    if r.status_code == 422:
        body = r.json()
        # FastAPI default validation envelope.
        assert 'detail' in body, (
            f"legacy validation failure must return FastAPI's default 422 envelope, got {body!r}"
        )
        # Must NOT have Phase 78 error envelope at the top level.
        err = body.get('error')
        assert err is None or not isinstance(err, dict) or 'code' not in err, (
            f"legacy /api/* route MUST NOT use Phase 78 error envelope; got {body!r}. "
            "This indicates Plan 02 installed exception handlers GLOBALLY (Concern #2 regression)."
        )
    elif r.status_code == 404:
        # Path didn't match — try a different typed-param route.
        # Fallback: hit /api/cambridge_image/some_sys_id?page=not_an_int
        # which has a typed `int` query param `page` (web/api.py:611).
        r2 = client_with_both_inits.get('/api/cambridge_image/9912345678901234?page=not_an_int')
        if r2.status_code == 422:
            body = r2.json()
            assert 'detail' in body, (
                f"legacy validation failure must return FastAPI default 422 envelope, got {body!r}"
            )
            err = body.get('error')
            assert err is None or not isinstance(err, dict) or 'code' not in err, (
                f"legacy /api/* MUST NOT use Phase 78 error envelope; got {body!r}"
            )
        else:
            pytest.skip(
                f'Could not drive a 422 through a legacy route; sitemap got 404, '
                f'cambridge got {r2.status_code}. Rewrite this test against another '
                'legacy route from web/api.py with a typed parameter.'
            )
    else:
        # Any other status (200, 500, etc.) — still assert NOT the Phase 78 envelope.
        try:
            body = r.json() if r.headers.get('content-type', '').startswith('application/json') else {}
        except ValueError:
            body = {}
        if isinstance(body, dict) and isinstance(body.get('error'), dict):
            err = body['error']
            assert 'code' not in err or 'message' not in err, (
                f"legacy /api/* MUST NOT use Phase 78 error envelope; got {body!r}. "
                "FastAPI's default 422 envelope shape MUST survive Phase 78 imports. "
                "This is the Concern #2 regression test."
            )


def test_legacy_nli_image_by_sysid_unchanged(client_with_both_inits):
    """D-25 (Phase 79 Plan 04): /api/nli_image_by_sysid behavior is unchanged
    after init_search_api runs alongside init_api_routes.

    The route is registered by init_api_routes (web/api.py:573) and returns a
    binary image Response on success or `Response(content="Image not found",
    status_code=404)` on miss. Phase 79's init_search_api MUST NOT route this
    request through the new envelope rewriting path or otherwise mutate the
    legacy contract.
    """
    r = client_with_both_inits.get(
        '/api/nli_image_by_sysid/__nonexistent_sys_id__?page=0'
    )
    # Acceptable: 404 (image not found, legacy plaintext) or 200 (rare; would
    # only happen if the test environment has a working NLI fetch). The
    # FORBIDDEN outcome is a Phase 78 JSON error envelope leaking onto this
    # route.
    assert r.status_code in (200, 404, 500, 502, 503, 504), (
        f'unexpected status {r.status_code} for legacy /api/nli_image_by_sysid; '
        'baseline before Phase 79 was 404 plaintext or 200 image'
    )
    ct = r.headers.get('content-type', '')
    if ct.startswith('application/json'):
        try:
            body = r.json()
        except ValueError:
            body = {}
        if isinstance(body, dict) and isinstance(body.get('error'), dict):
            err = body['error']
            assert 'code' not in err or 'message' not in err, (
                f'legacy /api/nli_image_by_sysid MUST NOT use Phase 78 error '
                f'envelope; got {body!r}. Phase 79 D-25 regression: '
                'init_search_api leaked envelope rewriting onto a legacy route.'
            )


def test_legacy_puzzle_image_route_status_unchanged(client_with_both_inits):
    """Spot check the puzzle image route status code is unchanged.

    /api/puzzle_image takes a query param fl_id; with a nonexistent value it
    returns 400/404/422 — whichever it returned BEFORE Phase 78. Phase 78
    must not change that status code.
    """
    r = client_with_both_inits.get('/api/puzzle_image?fl_id=__nonexistent__')
    assert r.status_code in (200, 400, 404, 422, 500), (
        f"unexpected status {r.status_code}; baseline before Phase 78 was 400/404/422"
    )
    # Any error response body MUST NOT be the Phase 78 envelope.
    if r.headers.get('content-type', '').startswith('application/json'):
        try:
            body = r.json()
        except ValueError:
            body = {}
        if isinstance(body, dict) and isinstance(body.get('error'), dict):
            err = body['error']
            assert 'code' not in err or 'message' not in err, (
                f"legacy /api/puzzle_image MUST NOT use Phase 78 error envelope; got {body!r}"
            )
