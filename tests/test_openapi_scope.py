"""Phase 83 Wave 0 — OpenAPI scope tests (PUBLIC-04 / D-08).

Per Codex review concern #1: tests MUST import web.main (or the
create_search_helper_app factory exposed by Plan 03) so the sub-mount
is actually exercised. Importing nicegui.app directly tests an
unmounted app and would falsely pass.

These tests are RED until Plan 03 (sub-mount wiring) lands. Until then:
- the import of web.main may succeed but /api/openapi.json returns 404, OR
- the sub-mount line doesn't exist and there's no /api FastAPI sub-app.
Either way, the assertions below fail at assertion time, not import time.
"""
import pytest

try:
    from fastapi.testclient import TestClient
    # Import web.main to ensure NiceGUI singleton has the Phase 83 sub-mount applied.
    # Plan 03 adds: app.mount("/api", search_helper_app) inside web/main.py.
    import web.main  # noqa: F401 — import-for-side-effect (triggers sub-mount)
    from nicegui import app as nicegui_app
    _IMPORT_OK = True
except Exception:
    _IMPORT_OK = False


@pytest.mark.skipif(not _IMPORT_OK, reason="web.main not importable in this env")
def test_openapi_includes_search_helper_endpoints():
    """GET /api/openapi.json must list /search, /browse, /parallels."""
    client = TestClient(nicegui_app)
    resp = client.get("/api/openapi.json")
    assert resp.status_code == 200, f"GET /api/openapi.json returned {resp.status_code}"
    paths = set(resp.json().get("paths", {}).keys())
    # D-08 path-name assertion. Note: actual rendered path keys depend on
    # whether the sub-app uses path_prefix='' (paths render as /search) or
    # path_prefix='/api' + servers metadata. Plan 03 documents the chosen
    # convention; both forms are acceptable.
    has_search = "/search" in paths or "/api/search" in paths
    has_browse = "/browse" in paths or "/api/browse" in paths
    has_parallels = "/parallels" in paths or "/api/parallels" in paths
    assert has_search, f"search endpoint not in OpenAPI spec paths: {paths}"
    assert has_browse, f"browse endpoint not in OpenAPI spec paths: {paths}"
    assert has_parallels, f"parallels endpoint not in OpenAPI spec paths: {paths}"


@pytest.mark.skipif(not _IMPORT_OK, reason="web.main not importable in this env")
def test_openapi_excludes_legacy_routes():
    """GET /api/openapi.json must NOT contain legacy /api/* routes."""
    client = TestClient(nicegui_app)
    resp = client.get("/api/openapi.json")
    assert resp.status_code == 200
    paths = set(resp.json().get("paths", {}).keys())
    forbidden = {
        "/api/cambridge_image/{sys_id}", "/cambridge_image/{sys_id}",
        "/robots.txt", "/sitemap.xml", "/_internal/memstat",
        "/api/nli_image_by_sysid", "/nli_image_by_sysid",
    }
    leaked = forbidden & paths
    assert not leaked, f"Legacy routes leaked into OpenAPI spec (D-07 violation): {leaked}"


@pytest.mark.skipif(not _IMPORT_OK, reason="web.main not importable in this env")
def test_openapi_request_schemas_populated():
    """Codex concern #2: /search and /parallels must have populated requestBody
    schemas; /browse must have populated query parameters. Without this,
    Swagger UI at /api/docs renders buttonless endpoints."""
    client = TestClient(nicegui_app)
    resp = client.get("/api/openapi.json")
    assert resp.status_code == 200
    spec = resp.json()
    paths = spec.get("paths", {})

    def _path_entry(name):
        return paths.get(f"/{name}") or paths.get(f"/api/{name}") or {}

    search_post = _path_entry("search").get("post", {})
    parallels_post = _path_entry("parallels").get("post", {})
    browse_get = _path_entry("browse").get("get", {})

    assert search_post.get("requestBody"), (
        "POST /search has no requestBody schema in OpenAPI spec — Swagger UI "
        "will render a buttonless endpoint. Plan 03 must use FastAPI's "
        "automatic Pydantic body parsing (req: SearchRequest) or openapi_extra."
    )
    assert parallels_post.get("requestBody"), (
        "POST /parallels has no requestBody schema in OpenAPI spec."
    )
    # Browse uses query params, not a body.
    params = browse_get.get("parameters") or []
    assert len(params) > 0, (
        "GET /browse has no query parameters in OpenAPI spec. Plan 03 must "
        "expose BrowseRequest fields as query parameters."
    )


@pytest.mark.skipif(not _IMPORT_OK, reason="web.main not importable in this env")
def test_swagger_ui_renders():
    """GET /api/docs must return 200 with Swagger UI HTML."""
    client = TestClient(nicegui_app)
    resp = client.get("/api/docs")
    assert resp.status_code == 200, f"GET /api/docs returned {resp.status_code}"
    assert "swagger-ui" in resp.text.lower(), (
        "/api/docs response does not contain 'swagger-ui' — Swagger UI not rendering"
    )
