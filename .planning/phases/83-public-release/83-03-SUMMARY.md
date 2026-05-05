---
phase: 83-public-release
plan: 03
subsystem: web/api
tags: [openapi, swagger, fastapi, public-api, codex-fix]
requires:
  - 83-01 (RED tests landed at tests/test_openapi_scope.py)
provides:
  - "/api/openapi.json — OpenAPI 3.x spec with populated requestBody/parameters/responses"
  - "/api/docs — Swagger UI rendering all 3 endpoints"
  - "/api/redoc — ReDoc alternative UI"
  - "init_search_api(path_prefix=...) — parametric prefix for sub-app mounting"
affects:
  - web/search_api.py (path_prefix kwarg, Field descriptions, openapi_extra metadata)
  - web/main.py (sub-app wiring, APP_VERSION, servers metadata)
tech-stack:
  added: []
  patterns:
    - "FastAPI sub-app + app.mount('/api', sub) for scoped OpenAPI exposure"
    - "openapi_extra= on route decorators (Option B) preserves Concern #2 envelope routing"
    - "model_json_schema() -> requestBody/parameters helper functions"
key-files:
  created: []
  modified:
    - web/search_api.py
    - web/main.py
decisions:
  - "Option B (openapi_extra= on decorators) chosen over typed-signature handler refactor (Option A) -- lowest-risk; handler bodies/signatures byte-identical so Phase 78/79/80/81A behavior preserved"
  - "path_prefix='/api' default (not '') for backward compat with all existing tests using init_search_api(app_override=bare)"
  - "version=APP_VERSION imported from version.py (not hard-coded) so bump_version.py keeps spec current"
  - "servers=[{url:'/api'}] on sub-app so spec consumers see /api/search even though sub-app's path keys are /search"
metrics:
  duration: ~25 minutes
  tasks: 3
  commits: 3
  files_modified: 2
  completed: 2026-05-05
---

# Phase 83 Plan 03: OpenAPI Spec Wiring Summary

**One-liner:** Wired `/api/openapi.json` + `/api/docs` Swagger UI via FastAPI sub-app mount, with populated request/response schemas via `openapi_extra=` on each route decorator (Codex HIGH concern #2 fix), `APP_VERSION` from `version.py` (Codex MEDIUM), `servers=[{url:'/api'}]` for consumer path rendering, and ASCII-only `Field(description=...)` kwargs (Codex LOW).

## Tasks Completed

| Task | Name                                                     | Commit   | Files                       |
| ---- | -------------------------------------------------------- | -------- | --------------------------- |
| 1    | path_prefix kwarg + ASCII-only Field descriptions        | 237322d5 | web/search_api.py           |
| 2    | openapi_extra requestBody/parameters/responses on routes | 628a9ca7 | web/search_api.py           |
| 3    | search_helper sub-app mount in web/main.py               | b135f47b | web/main.py                 |

## Verification

- `pytest tests/test_openapi_scope.py -v` -- 4/4 GREEN
  - test_openapi_includes_search_helper_endpoints PASSED
  - test_openapi_excludes_legacy_routes PASSED
  - test_openapi_request_schemas_populated PASSED (Codex HIGH fix verified)
  - test_swagger_ui_renders PASSED
- Smoke test (sub-app + TestClient): `/search` and `/parallels` expose populated `requestBody`; `/browse` exposes 6 query parameters
- Regression suites: `test_search_api.py`, `test_api_hardening.py`, `test_api_legacy_unchanged.py`, `test_browse_api.py`, `test_parallels_api.py` -- 6 pre-existing failures (filter_vocabulary_unavailable, FJMS sidecar env-dependent), no new failures
- ASCII guard: 28 description= strings, all ASCII (Codex LOW fix verified)

## Codex Concern Resolution

| Concern        | Severity | Fix                                                                                  |
| -------------- | -------- | ------------------------------------------------------------------------------------ |
| #2 (HIGH)      | HIGH     | openapi_extra= on each route decorator with model_json_schema()-derived schemas      |
| version stale  | MEDIUM   | `from version import APP_VERSION as _APP_VERSION` -- no hard-coded "7.10.0" string   |
| path-rendering | MEDIUM   | `servers=[{"url": "/api"}]` declared on sub-app                                      |
| ASCII-only     | LOW      | All 28 Field description= strings ASCII; `->` instead of `→`                         |

## Deviations from Plan

None - plan executed exactly as written. Pre-existing 6 test failures in `test_search_api.py` (filter_vocabulary_unavailable / FJMS sidecar env) verified against baseline (pre-edit) and confirmed not regressions.

## Self-Check: PASSED

- web/search_api.py: modified (path_prefix kwarg present, 28 ASCII descriptions, 3 openapi_extra= sites, helper functions defined)
- web/main.py: modified (_search_helper_app built, APP_VERSION imported, servers metadata declared, app.mount('/api', _search_helper_app) wired)
- Commit 237322d5: present in git log
- Commit 628a9ca7: present in git log
- Commit b135f47b: present in git log
- 4/4 OpenAPI scope tests GREEN
