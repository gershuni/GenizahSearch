---
phase: 77-serializer-json-export
plan: 04
subsystem: search-api
tags: [search-api, http-handlers, toolbar-buttons, lifecycle-wiring, i18n, http-isolation]

# Dependency graph
requires:
  - phase: 77 plan 01
    provides: state.last_results + envelope-echo fields (current_search_query, current_search_mode, current_search_gap, last_filters_applied, last_search_warnings, parallels_search_meta) populated at execute-time -- the HTTP handlers consume them directly
  - phase: 77 plan 03
    provides: shared/search_serializer.py (5 public exports -- SCHEMA_VERSION, serialize_search_payload, serialize_parallels_payload, build_search_filename, build_parallels_filename) -- the HTTP handlers call these
provides:
  - GET /api/export/json -- stateful download returning Claude-friendly JSON for /search results; reads state.last_results + envelope-echo fields; 400 on empty state; Content-Disposition filename "genizah-search-{ts}.json"
  - GET /api/export/parallels/json -- stateful download for /parallels results with results[] + filtered[] arrays; reads state.parallels_results + state.parallels_filtered + state.parallels_search_meta; 400 on both empty
  - init_api_routes(app_override=None) refactor (HIGH-08) -- production callers passing zero args unchanged; tests pass a bare FastAPI() instance to register routes onto it instead of mutating the NiceGUI singleton
  - tests/test_api_export_json.py (5 tests) -- empty + populated x 2 routes + HIGH-08 isolation sanity check
  - Toolbar JSON button on /search (always-enabled, line 1447) and /parallels (line 1236, full lifecycle disable/enable wiring at lines 1942 / 2659 / 2667)
  - Hebrew translation pairs in genizah_translations.py (lines 1589-1590) for "Export JSON" / "Download JSON" (LOW-01)
  - Rule 1 fix: export_parallels_json handler now checks empty state BEFORE touching app.storage.user; storage access wrapped in try/except so the handler works in non-NiceGUI request contexts (tests, future stateless callers)
affects: [78-* (POST /api/search inherits init_api_routes app_override pattern + serializer call shape), 80-* (POST /api/parallels inherits same pattern)]

# Tech tracking
tech-stack:
  added: []  # No new libraries -- only consumes existing FastAPI + Plan 03 serializer
  patterns:
    - "app_override dispatch in init_api_routes: target_app = app_override if app_override is not None else app; tests pass bare FastAPI(), production passes nothing"
    - "Empty-state-first guard in stateful download handlers: avoids touching session storage when there's nothing to export -- handler now graceful in non-NiceGUI contexts"
    - "Storage access guarded by try/except so handlers degrade gracefully outside NiceGUI request contexts"
    - "TestClient against bare FastAPI app: each test fixture builds its own bare app, registers routes onto it, NiceGUI singleton is structurally untouched"
    - "JSON download handler skeleton: empty-check -> serializer call -> JSONResponse with Content-Disposition; mirrors existing Excel/Word skeleton verbatim except media_type and serializer call differ"

key-files:
  created:
    - tests/test_api_export_json.py
  modified:
    - web/api.py
    - web/pages/search.py
    - web/pages/parallels.py
    - genizah_translations.py

key-decisions:
  - "init_api_routes(app_override=None) signature is the surgical fix for HIGH-08: backward-compatible (no production caller change) AND eliminates singleton mutation in tests. target_app local variable controls registration; every @app.get/post/delete inside the function renamed to @target_app.get/post/delete (37 decorator sites)"
  - "Test fixture is module-scoped: bare_app_with_routes builds the bare FastAPI app once per test module, registering the 37 routes onto it; module-scope avoids 5x re-registration cost while still proving HIGH-08"
  - "test_init_api_routes_does_not_mutate_nicegui_singleton is the structural HIGH-08 acceptance test: snapshots nicegui_app.routes count, calls init_api_routes(bare), confirms count unchanged AND bare.routes > 0"
  - "Empty-state check moved BEFORE app.storage.user access in export_parallels_json handler (Rule 1 fix discovered during Task 2 test run): the original handler crashed in non-NiceGUI request contexts because storage.user requires a session cookie; reordering means the empty-state path never touches storage, and the populated path wraps storage in try/except"
  - "Search toolbar button is always-enabled (no _btn handle captured): matches existing search.py Excel/Word neighbors which never disable; intentional UX divergence from parallels.py which has lifecycle gating"
  - "Parallels toolbar button captured into export_json_btn variable with 3 lifecycle wiring sites: _reset_parallels disable, render_results-empty disable, render_results-populated enable -- mirrors export_excel_btn / export_word_btn exactly"

patterns-established:
  - "app_override pattern for FastAPI route-registration functions: any function that takes a global singleton can grow an optional override parameter for test isolation; backward-compatible additive change"
  - "Stateful FastAPI handler hardening: empty-check first, storage access wrapped in try/except -- handlers that read session storage now degrade gracefully in non-session contexts (tests, scripts, future stateless API callers)"

requirements-completed: [EXPORT-01, EXPORT-02, EXPORT-04]

# Metrics
duration: 6min
completed: 2026-04-27
---

# Phase 77 Plan 04: HTTP Handlers + Toolbar Buttons Summary

**Two new GET handlers (/api/export/json, /api/export/parallels/json) wired to toolbar buttons on /search and /parallels; init_api_routes refactored to accept an app_override parameter so the 5 handler tests register onto a bare FastAPI app instead of mutating the NiceGUI singleton; LOW-01 Hebrew translations added.**

## Performance

- **Duration:** ~6 min (17:19:37Z -> 17:25:51Z)
- **Started:** 2026-04-27T17:19:37Z
- **Completed:** 2026-04-27T17:25:51Z
- **Tasks:** 4 (4 atomic commits)
- **Files created:** 1 (tests/test_api_export_json.py)
- **Files modified:** 4 (web/api.py, web/pages/search.py, web/pages/parallels.py, genizah_translations.py)

## Accomplishments

- **HIGH-08 fix landed:** `init_api_routes(app_override=None)` accepts an optional FastAPI app to register onto. Production behavior unchanged (no caller passes args today); tests pass a bare app per fixture and the NiceGUI singleton is provably untouched (`test_init_api_routes_does_not_mutate_nicegui_singleton`).
- **All 37 existing decorators renamed** from `@app.X` to `@target_app.X` inside `init_api_routes` -- mechanical, behavior-preserving rename. Outside the function: zero `@app.` decorators existed, so module-level scope was unaffected.
- **Two new JSON handlers:**
  - `GET /api/export/json` (web/api.py:1920-1956) -- consumes `state.last_results`, calls `serialize_search_payload(...)` with envelope-echo kwargs from state, returns `JSONResponse` with `Content-Disposition: attachment; filename="genizah-search-{ts}.json"`. 400 on empty state.
  - `GET /api/export/parallels/json` (web/api.py:1957-2014) -- consumes `state.parallels_results` + `state.parallels_filtered` + `state.parallels_search_meta`, calls `serialize_parallels_payload(...)`, returns `JSONResponse` with parallels filename. 400 when both result arrays empty.
- **5 behavioral tests** in `tests/test_api_export_json.py`, all GREEN:
  1. `test_export_json_handler_empty` -- empty state -> 400 with body "No results to export" (EXPORT-01)
  2. `test_export_json_handler_populated` -- populated state -> 200, Content-Type application/json, Content-Disposition matches `genizah-search-...json`, body has `results` list (EXPORT-01)
  3. `test_export_parallels_json_handler_empty` -- empty state -> 400 with body "No parallels results to export" (EXPORT-02)
  4. `test_export_parallels_json_handler_populated` -- populated state -> 200, body has `results` AND `filtered` lists (EXPORT-02)
  5. `test_init_api_routes_does_not_mutate_nicegui_singleton` -- structural HIGH-08 acceptance test
- **Toolbar buttons:**
  - `/search` (line 1447): always-enabled `ui.button(icon='data_object', on_click=lambda: ui.download('/api/export/json')).tooltip(tr('Export JSON'))` -- matches existing Excel/Word neighbor pattern (search-page exports never disable)
  - `/parallels` (line 1236): captured into `export_json_btn`, starts disabled (`'flat round dense disable'`), with 3 lifecycle wiring sites at lines 1942, 2659, 2667 mirroring `export_excel_btn`
- **Hebrew translations** added to `genizah_translations.py` (lines 1589-1590):
  - `"Export JSON": "יצוא ל-JSON"`
  - `"Download JSON": "הורד JSON"` (LOW-01)

## Verification

- `pytest tests/test_api_export_json.py -x -q` -> 5 passed
- `pytest tests/` -> **1194 passed, 8 skipped** (was 1189 + 8 -> +5 new tests, no regressions)
- `python -c "import web.api"` -> clean
- `python -c "import ast; ast.parse(open('web/pages/{search,parallels}.py'))"` -> both parse
- `python -c "import genizah_translations"` -> clean
- `grep -c "/api/export/json\|/api/export/parallels/json" web/api.py` -> 2 decorator hits (plus inline references in docstrings)
- `grep -c "Export JSON\|Download JSON" genizah_translations.py` -> 2

## Deviations from Plan

### Rule 1 - Auto-fixed bug

**1. [Rule 1 - Bug] export_parallels_json handler crashed on `app.storage.user` access in non-NiceGUI contexts**

- **Found during:** Task 2 (running `tests/test_api_export_json.py` for the first time)
- **Issue:** The original handler called `nicegui_app.storage.user.get('parallels_source_text', '')` *before* the empty-state check. In tests (or any non-NiceGUI request context), `storage.user` raises `RuntimeError: app.storage.user needs a storage_secret passed in ui.run()`. The empty test (`test_export_parallels_json_handler_empty`) crashed even though it should have returned 400 immediately.
- **Fix:**
  - Reordered: empty-state check (`if not parallels_results and not filtered_results: return 400`) moved BEFORE storage access. The empty path never touches storage now.
  - Storage access for the populated path wrapped in `try/except Exception` -- falls back to empty string if storage is unavailable. The `meta.get('source_text')` value (populated by Plan 01 at parallels execute-time) is the primary source; storage is only the fallback.
- **Why it matters:** This makes the handler robust in three contexts: (a) tests, (b) future stateless callers, and (c) edge cases where the NiceGUI session cookie is missing. The behavior in production (active NiceGUI session) is unchanged.
- **Files modified:** web/api.py (lines 1971-1989)
- **Commit:** f8b508de (folded into Task 2 commit since the fix was discovered during test execution)

### No other deviations

The plan executed as written. The line-number deltas in plan task 3 (1923 vs actual 1942, 2607-2616 vs actual 2657-2667) are pre-existing drift from when the plan was authored vs when it executed -- the search-and-replace patterns matched correctly.

## Threat Surface Scan

No new threat-relevant surface introduced beyond what the plan's `<threat_model>` accepts. The two new GET handlers inherit the existing `/api/export/excel` security posture (T-77.04-01..05 dispositions: accept; phase 78 introduces auth/observability for the API endpoints, this plan stays in download-route scope per the threat model).

## Self-Check: PASSED

- [x] tests/test_api_export_json.py exists -- FOUND
- [x] web/api.py modified (init_api_routes signature + 2 handlers + decorator rename) -- FOUND
- [x] web/pages/search.py JSON button -- FOUND at line 1447
- [x] web/pages/parallels.py JSON button + 3 lifecycle sites -- FOUND at lines 1236, 1942, 2659, 2667
- [x] genizah_translations.py Hebrew entries -- FOUND at lines 1589-1590
- [x] Commit 20972e66 (refactor Task 1) -- FOUND in git log
- [x] Commit f8b508de (feat handlers + tests Task 2) -- FOUND in git log
- [x] Commit 2c1fa26c (feat toolbar Task 3) -- FOUND in git log
- [x] Commit 01e18602 (feat translations Task 4) -- FOUND in git log
- [x] All 5 new tests GREEN; full suite 1194 passed / 8 skipped (no regressions)

## Next Step

Plan 77-05 (manual smoke-check + docs) -- visually verify the toolbar buttons render, click them, confirm browser downloads a JSON file with the expected envelope shape and Hebrew tooltip. The HTTP path is now structurally complete; Plan 05 is observation + documentation only.
