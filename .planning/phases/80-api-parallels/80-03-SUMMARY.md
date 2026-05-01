---
phase: 80-api-parallels
plan: 03
subsystem: api
tags: [api, parallels, route-handler, hardening, statelessness]
requires:
  - shared.parallels_service.fetch_parallels_results
  - shared.parallels_service.ParallelsResultBundle
  - shared.search_serializer.serialize_parallels_payload
  - shared.api_errors.APIError
  - shared.api_errors.ERROR_CODES (composition_required, composition_too_long)
  - web.api_hardening.wrap_endpoint
  - web.api_hardening.RateLimiter
  - web.api_hardening.enforce_mode_gate
provides:
  - web.search_api.ParallelsRequest
  - web.search_api._parallels_rate_limiter
  - web.search_api.COMPOSITION_LENGTH_CAP
  - "POST /api/parallels (registered by init_search_api)"
affects: [web/search_api.py]
tech_added: []
patterns: [wrap_endpoint decorator reuse, late-bound fjms_service import, manual await request.json() guard, separate rate-limit bucket]
key_files_created: []
key_files_modified: [web/search_api.py]
decisions:
  - "Registered POST /api/parallels in existing init_search_api alongside search/browse — same idempotency marker"
  - "Decorated parallels_endpoint with @wrap_endpoint(endpoint_name='parallels') (Phase 79 R-PR-03 precedent) — handler body has no try/except/finally / capture_api_event boilerplate"
  - "Manual request.json() guard preserved (NOT FastAPI body injection) so malformed JSON flows through wrap_endpoint envelope"
  - "Third RateLimiter instance (_parallels_rate_limiter) — D-05; identity-distinct from _rate_limiter and _browse_rate_limiter"
  - "COMPOSITION_LENGTH_CAP=20000 (D-06); empty .strip() → composition_required; >cap → composition_too_long"
  - "Filter pipeline mirrors Phase 78 search_endpoint verbatim — late-bound `from shared import fjms_service as _fjms_module`"
  - "Empty restrict_sys_ids short-circuit constructs an empty ParallelsResultBundle locally (no service call) so envelope still goes through serialize_parallels_payload (D-04 filtered key always present)"
  - "Module-scope import of serialize_parallels_payload (alongside serialize_browse_payload) for testability"
metrics:
  duration_minutes: 5
  tasks_completed: 1
  files_created: 0
  files_modified: 1
  lines_added: 145
completed: 2026-05-01
---

# Phase 80 Plan 03: Route Handler Summary

Added `POST /api/parallels` to `web/search_api.py` alongside the existing `POST /api/search` (Phase 78) and `GET /api/browse` (Phase 79) routes — registered by the same `init_search_api(app_override=None)` registrar so a single idempotency marker covers all three.

## What landed

- **`ParallelsRequest`** Pydantic model with `extra='forbid'` and locked field shapes per D-01/D-02/D-03 (`text` required; `chunk_size` int Field ge=2 le=20 default 5; `mode` Literal['exact','variants','fuzzy'] default 'exact'; `max_freq` Optional[float] None; `boundary_mode` Literal['full','boundary','combined'] default 'full'; `filters` Optional[FiltersModel] None).
- **`COMPOSITION_LENGTH_CAP = 20000`** module-level constant (D-06).
- **`_parallels_rate_limiter`** module-level RateLimiter(default_limit=30) — third bucket; identity-distinct from `_rate_limiter` and `_browse_rate_limiter`. Same `SEARCH_API_RATE_LIMIT` env-var ceiling.
- **`parallels_endpoint`** route handler decorated with `@wrap_endpoint(endpoint_name='parallels')`. Handler body holds only business logic — manual `await request.json()` guard, `enforce_mode_gate(request)`, `_parallels_rate_limiter.check(client_ip)`, composition length validation (D-06), filter resolution via late-bound `_fjms_module`, short-circuit on empty `restrict_sys_ids`, `await fetch_parallels_results(...)`, group-cap warning append (D-07), `serialize_parallels_payload(...)` call. `captured_state['mode']` is set to `req.mode` for PostHog (D-09); `captured_state['result_count']` is `len(bundle.main_results)`.
- **Imports added:** `Field` from pydantic; `serialize_parallels_payload` (module scope, joining `serialize_browse_payload`); `fetch_parallels_results, ParallelsResultBundle` from `shared.parallels_service`.
- **Final `logger.info`** updated to enumerate all three routes.

## What did NOT change

- Existing `search_endpoint` and `browse_endpoint` function bodies are byte-identical (only the surrounding module imports + new `_parallels_rate_limiter` + new `COMPOSITION_LENGTH_CAP` + new ParallelsRequest class + new endpoint inside init_search_api).
- `web/api.py` legacy /api/* routes — untouched.
- `web/api_hardening.py` — no new helpers added; Phase 78 primitives reused verbatim.
- Concern #2 lock: no global FastAPI `add_exception_handler` calls.

## Verification

```
python -c "import web.search_api; ..." (full Plan 03 verify block) → OK
python -m pytest tests/test_search_api.py tests/test_api_hardening.py tests/test_browse_api.py tests/test_search_serializer.py tests/test_api_legacy_unchanged.py -q
→ 146 passed, 1 skipped
```

Statelessness D-20 grep contract holds (`state.last_results | state.parallels_results | state.current_search_query | app.storage | request.cookies` → 0 hits). Lab Engine path NOT reachable (`lab_composition_search` → 0 hits in web/search_api.py). `@wrap_endpoint(endpoint_name='parallels')` present once. `_parallels_rate_limiter` defined once and `.check(client_ip)` called once. Three identity-distinct RateLimiter instances confirmed at runtime.

## Deviations

The original plan offered two options for the `serialize_parallels_payload` import; the recommended module-scope import was used (combined with the existing `serialize_browse_payload` import on the same `from shared.search_serializer import …` line). No behavioral deviation.

The agent originally spawned for this plan was blocked by a runtime hook before any commits landed. The orchestrator completed the plan inline in the main worktree; the plan body matches the spec verbatim.

## Plan 04 readiness

The route is now reachable for `tests/test_parallels_api.py` (Plan 04). All keyword args match `fetch_parallels_results` (Plan 02) and `serialize_parallels_payload` (Phase 77) verbatim. Three rate limiters are present and identity-distinct so `test_parallels_rate_limit_independence` can burst 31 on /api/parallels and verify /api/search and /api/browse remain unaffected.
