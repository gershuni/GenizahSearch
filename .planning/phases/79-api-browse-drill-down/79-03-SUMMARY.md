---
phase: 79-api-browse-drill-down
plan: 03
subsystem: api
tags: [api, browse, route-handler, fastapi, wrap_endpoint, locator]
requirements: [API-03, API-04, API-05, API-06, HARDEN-01, HARDEN-04, HARDEN-05]
dependency_graph:
  requires:
    - "Plan 79-01: shared/api_errors.py ERROR_CODES has locator_conflict, manuscript_page_not_found, core_timeout"
    - "Plan 79-01: shared/search_serializer.serialize_browse_payload (no requested_uid/fl_id params per R-PR-09)"
    - "Plan 79-02: shared/browse_service.fetch_browse_bundle (no uid param per R-PR-04)"
    - "Phase 78: web/api_hardening.wrap_endpoint, RateLimiter, enforce_mode_gate, _resolve_rate_limit_key"
  provides:
    - "web/search_api.py: BrowseRequest Pydantic model with extra='forbid'"
    - "web/search_api.py: NormalizedLocator dataclass (effective_p_num/volume_ie/fl_id, requested_uid)"
    - "web/search_api.py: _parse_uid, _validate_locator, _resolve_text_cap helpers"
    - "web/search_api.py: _browse_rate_limiter (separate bucket per D-18)"
    - "web/search_api.py: GET /api/browse route registered by init_search_api"
  affects:
    - "Plan 79-04 (tests): exercises the route via TestClient + mocks fetch_browse_bundle"
    - "Phase 81 (skill consumer): consumes the locator round-trip search -> browse"
tech_stack:
  added: []
  patterns:
    - "@wrap_endpoint(endpoint_name='browse') decorator REUSED from web/api_hardening.py (R-PR-03) -- handler body is pure business logic, decorator owns try/except/finally + capture_api_event"
    - "NormalizedLocator dataclass: _validate_locator returns structured normalized form, handler passes effective_* fields to fetch_browse_bundle (R-PR-04)"
    - "Post-resolution uid verification (D-03b): resolved BrowsePage.uid compared to requested_uid AFTER fetch_browse_bundle returns; mismatch -> 404"
    - "Separate per-IP rate-limit buckets per endpoint (D-18) -- same SEARCH_API_RATE_LIMIT env-var ceiling, distinct counters"
    - "Best-effort image URL emission (R-PR-01): handler does NOT probe upstream availability"
key_files:
  created: []
  modified:
    - web/search_api.py
decisions:
  - "Decorator REUSE end-to-end: @wrap_endpoint(endpoint_name='browse') applied to browse_endpoint. Handler body has zero hand-rolled try/except/finally / capture_api_event / t0 boilerplate."
  - "NormalizedLocator is the one source of truth for resolution: handler reads loc.effective_* and loc.requested_uid; never re-parses uid in the handler body."
  - "search_endpoint left untouched (out of scope per plan -- the existing hand-rolled boilerplate stays; only browse_endpoint adopts the decorator pattern in this phase)."
  - "Idempotency marker (target_app.state.search_api_initialized) covers BOTH POST /api/search and GET /api/browse -- second init_search_api call is a no-op for both routes."
  - "Statelessness D-22 verified by grep: zero forbidden references."
metrics:
  duration: ~8 minutes
  completed_date: "2026-04-30"
  tasks: 1
  files_modified: 1
  files_created: 0
  tests_passed: "1298/1306 (8 skipped, 0 failed) -- no regression vs Plan 02 baseline"
---

# Phase 79 Plan 03: GET /api/browse route handler -- Summary

Registered `GET /api/browse` inside `init_search_api(app_override=None)` in `web/search_api.py`, alongside the existing `POST /api/search`. The new handler is decorated with `@wrap_endpoint(endpoint_name='browse')` from `web/api_hardening.py:333` (R-PR-03 fix); the handler body holds only business logic, with the decorator owning try/except/finally + envelope rewriting + PostHog `capture_api_event`.

## What Shipped

### `web/search_api.py` (+309 lines, -1 line)

Four blocks of additive changes; **no existing code modified beyond the final `logger.info` line**:

#### Block A: imports + module-level constants

- New imports: `os`, `re as _re`, `dataclass` from dataclasses.
- `wrap_endpoint` appended to the existing `web.api_hardening` import group.
- `from shared.browse_service import fetch_browse_bundle`
- `from shared.search_serializer import serialize_browse_payload`
- New `_browse_rate_limiter = RateLimiter(default_limit=30)` -- distinct instance from `_rate_limiter` (D-18).
- Three new constants: `DEFAULT_BROWSE_TEXT_CAP=4000`, `MIN_BROWSE_TEXT_CAP=100`, `MAX_BROWSE_TEXT_CAP=10000`.

#### Block B: BrowseRequest model + NormalizedLocator + locator helpers

1. **`BrowseRequest(BaseModel)`** -- Pydantic model with `model_config = ConfigDict(extra='forbid')`. Fields: `sys_id: str` (required), and optional `uid`, `p_num`, `volume_ie`, `fl_id`, `text_cap`.

2. **`NormalizedLocator`** -- frozen dataclass with `sys_id`, `requested_uid` (original uid string for D-03b post-resolution check), `effective_p_num`, `effective_volume_ie`, `effective_fl_id`, `text_cap`. The handler passes the effective_* fields to `fetch_browse_bundle`.

3. **`_parse_uid(uid: str) -> Optional[dict]`** -- strict regex parser using `^(IE\d+)_(P\d+)_(FL\d+)$`. Returns `{volume_ie, p_num, fl_id}` dict or `None`. Rejects `p_num=0` (must be 1-based).

4. **`_validate_locator(req: BrowseRequest) -> NormalizedLocator`** -- enforces D-03 / R-02 / R-PR-04:
   - 400 `invalid_request` if all of (uid, p_num, fl_id) absent
   - 400 `invalid_request` if `p_num < 1`
   - 400 `invalid_request` if `text_cap` out of [100, 10000]
   - 400 `locator_conflict` if uid is malformed
   - 400 `locator_conflict` if uid disagrees with explicit `volume_ie` / `p_num` / `fl_id`
   - On uid path: returns NormalizedLocator with effective_* fields parsed from uid
   - On no-uid path: returns NormalizedLocator with effective_* mirroring request fields

5. **`_resolve_text_cap(requested) -> int`** -- R-08 priority: `?text_cap` > `SEARCH_API_BROWSE_TEXT_CAP` env > 4000 default. Bounds applied to env override too.

#### Block C: GET /api/browse route handler (decorated with @wrap_endpoint)

```python
@target_app.get('/api/browse')
@wrap_endpoint(endpoint_name='browse')
async def browse_endpoint(request: Request, *, captured_state: dict):
    ...
```

Handler body (pure business logic, no boilerplate):

1. `captured_state['mode'] = None` -- browse has no mode field
2. Parse query params via `dict(request.query_params)`, coerce `p_num`/`text_cap` to int (bad cast -> APIError 'invalid_request'), then `req = BrowseRequest(**params)` (decorator catches PydanticValidationError on extra='forbid' violation)
3. `enforce_mode_gate(request)` -- D-04 disabled / D-03 localhost-only same as search
4. `_browse_rate_limiter.check(client_ip)` -- D-18 separate bucket
5. `loc = _validate_locator(req)` -- D-01/D-03/R-02/R-PR-04
6. `effective_text_cap = _resolve_text_cap(loc.text_cap)`
7. `bundle, warnings_list = await fetch_browse_bundle(sys_id=loc.sys_id, p_num=loc.effective_p_num, volume_ie=loc.effective_volume_ie, fl_id=loc.effective_fl_id)` -- pass NORMALIZED fields, NOT uid (R-PR-04)
8. If `bundle.page is None` -> 404 `manuscript_page_not_found` (D-16)
9. **Post-resolution uid verification (D-03b/R-03):** if `loc.requested_uid` and `bundle.page.uid != loc.requested_uid` -> 404 `manuscript_page_not_found`
10. Multi-IE volume_ie_defaulted warning (D-04) when sys_id-only on a multi-IE manuscript and core auto-defaulted
11. `envelope = serialize_browse_payload(page=, pgp=, fjms=, nli=, text_cap=, warnings=)` -- NO `requested_uid` / `requested_fl_id` passed (R-PR-09)
12. `captured_state['result_count'] = 1` -- decorator's finally reads this for PostHog
13. Return envelope

#### Block D: logger.info update

```python
logger.info("Search API routes initialized: POST /api/search, GET /api/browse")
```

## Key Decisions

### R-PR-03 fix -- @wrap_endpoint reused end-to-end

`browse_endpoint` is decorated with `@wrap_endpoint(endpoint_name='browse')` from `web/api_hardening.py:333`. The handler body contains ZERO hand-rolled boilerplate:

- No `try/except/finally` block in the body
- No `t0 = time.monotonic()` tracking
- No `status_code` / `error_code` variables
- No `capture_api_event(...)` call

The decorator owns all of that. Verified by regex extraction of the handler body: the substring `capture_api_event` does NOT appear in the function body between `async def browse_endpoint` and the closing `logger.info` line. The decorator's `captured_state: dict` keyword argument is the sole channel by which the handler communicates `result_count` and `mode` to the PostHog finally block.

CONTEXT.md `canonical_refs` line 247 explicitly mandates this reuse; this fix is the difference between a ~40-line handler body of pure logic and a ~140-line handler that mixes logic with try/except plumbing.

The existing `search_endpoint` is **deliberately not refactored** -- per the plan's explicit out-of-scope marker, search_endpoint keeps its hand-rolled boilerplate; only the new `browse_endpoint` adopts the decorator pattern in this phase.

### R-PR-04 fix -- NormalizedLocator + uid normalization

`_validate_locator(req)` returns a `NormalizedLocator` dataclass with `effective_p_num`, `effective_volume_ie`, `effective_fl_id` populated. When `req.uid` is supplied, the validator parses `IE{N}_P{M}_FL{K}` once and the effective_* fields hold the parsed components. When uid is absent, effective_* mirror the request fields directly.

The handler passes `loc.effective_*` -- NOT `req.uid` -- to `fetch_browse_bundle`. This means uid-only requests resolve correctly because the bundle fetcher receives the parsed components; earlier draft had `_validate_locator` only checking conflicts but never extracting uid components, so uid-only requests resolved by accident only when defaults coincided.

Verified by grep: `fetch_browse_bundle(...)` call does NOT contain `uid=` (R-PR-04 enforcement).

### D-03b post-resolution uid verification

Stays as a separate step AFTER `fetch_browse_bundle` returns. The handler compares `bundle.page.uid` (set by `WebDataService` from the resolved page) against `loc.requested_uid` (the ORIGINAL uid string). Mismatch -> `APIError('manuscript_page_not_found', http_status=404)`. Catches the cross-manuscript pairing case where sys_id from manuscript A is paired with uid from manuscript B (e.g., copy-paste from wrong search result).

### R-PR-09 fix -- no requested_uid / requested_fl_id

The handler does NOT pass `requested_uid` or `requested_fl_id` to `serialize_browse_payload`. Plan 01 dropped those parameters from the serializer signature; the locator block reads exclusively from the resolved BrowsePage's attributes.

Verified by grep: the `serialize_browse_payload(...)` call contains neither `requested_uid` nor `requested_fl_id`.

### R-PR-01 / D-14 reopened -- no image probe

The handler does not call any image proxy or upstream IIIF endpoint to verify availability. The serializer (Plan 01) emits `image.url` unconditionally based on `library_code`, and clients handle proxy failures via `image.sources[]` alternates.

The string `image_unavailable` does not appear in `web/search_api.py` (grep returns 0).

### D-18 -- separate per-IP rate-limit buckets

`_browse_rate_limiter = RateLimiter(default_limit=30)` is a distinct instance from `_rate_limiter`. Both read `SEARCH_API_RATE_LIMIT` env on every `check()` call, so a single env var controls both ceilings while the buckets count independently. A client doing search-once + browse-N-times does NOT exhaust the search bucket.

R-10 monitoring obligation captured in plan: aggregate per-IP allowance is roughly 2x the ceiling.

### Statelessness D-22

Zero references to `state.last_results`, `state.current_search_query`, `app.storage`, `request.cookies` anywhere in `web/search_api.py`. Verified by grep returning 0.

### Concern #2 lock preserved

No global FastAPI exception handlers installed. Envelope rewriting is owned by the `@wrap_endpoint` decorator's except branches via `_build_envelope_response`. `init_search_api` does not call `add_exception_handler`.

## Deviations from Plan

None -- plan executed exactly as written. The acceptance grep `_validate_locator(req)` returned count=2 instead of the literal expected 1 because the string also appears in the `NormalizedLocator` dataclass docstring ("output of _validate_locator(req)"). The actual handler invocation is unique; the docstring reference is benign documentation. No behavioral or contract impact.

## Authentication Gates

None -- plan did not interact with any external service or auth-protected resource.

## Verification Performed

| Check | Result |
|-------|--------|
| Plan-bundled verify Python block (14 assertions: imports, distinct rate limiters, _parse_uid edge cases, BrowseRequest extra='forbid', _validate_locator missing/uid/p_num/conflict/malformed/text_cap, _resolve_text_cap, init_search_api dual-route registration + idempotency) | OK |
| `from web.search_api import BrowseRequest, NormalizedLocator, _browse_rate_limiter, _parse_uid, _validate_locator, _resolve_text_cap` | imports OK |
| 24 acceptance grep checks (existence + R-PR-01 + R-PR-03 + R-PR-04 + R-PR-09 + statelessness D-22 + Concern #2) | 24/24 pass, 0 fail |
| `python -m pytest tests/test_search_api.py tests/test_api_hardening.py tests/test_api_legacy_unchanged.py tests/test_search_serializer.py -x -q` | 108 passed |
| `python -m pytest tests/ -x -q --ignore=tests/test_browse_api.py` | 1298 passed, 8 skipped (no regression vs Plan 02 baseline 1298/8) |

### Key acceptance grep results

| Pattern | Count | Required |
|---------|-------|----------|
| `_browse_rate_limiter = RateLimiter` | 1 | 1 |
| `class BrowseRequest` | 1 | 1 |
| `class NormalizedLocator` | 1 | 1 |
| `def _parse_uid` | 1 | 1 |
| `def _validate_locator` | 1 | 1 |
| `def _resolve_text_cap` | 1 | 1 |
| `@target_app.get('/api/browse')` | 1 | 1 |
| `async def browse_endpoint` | 1 | 1 |
| `wrap_endpoint` (import + decorator) | 4 | >=2 |
| `@wrap_endpoint(endpoint_name='browse')` regex | 1 | >=1 |
| `effective_p_num` | 6 | >=2 |
| `effective_volume_ie` | 7 | >=2 |
| `effective_fl_id` | 7 | >=2 |
| `loc.effective_p_num` | 2 | >=1 |
| `loc.requested_uid` | 4 | >=1 |
| `from shared.browse_service import fetch_browse_bundle` | 1 | 1 |
| `from shared.search_serializer import serialize_browse_payload` | 1 | 1 |
| `_browse_rate_limiter.check(client_ip)` | 1 | 1 |
| `state.last_results\|state.current_search_query\|app.storage\|request.cookies` | **0** | 0 (forbidden) |
| `exception_handler\|add_exception_handler` | **0** | 0 (forbidden) |
| `image_unavailable\|head_probe\|probe.*image` | **0** | 0 (R-PR-01) |
| `capture_api_event` in browse_endpoint body | **0** | 0 (R-PR-03) |
| `uid=` inside `fetch_browse_bundle(...)` call | **0** | 0 (R-PR-04) |
| `requested_uid` / `requested_fl_id` inside `serialize_browse_payload(...)` call | **0** | 0 (R-PR-09) |
| `@target_app.post('/api/search')` (existing route preserved) | 1 | 1 |

## Commits

| Task | Commit | Files | Lines |
|------|--------|-------|-------|
| 1: BrowseRequest + NormalizedLocator + helpers + decorated browse_endpoint | `6fc64d60` | web/search_api.py | +309/-1 |

## Self-Check: PASSED

- `web/search_api.py` exists and contains all 6 new symbols (`BrowseRequest`, `NormalizedLocator`, `_browse_rate_limiter`, `_parse_uid`, `_validate_locator`, `_resolve_text_cap`) -- VERIFIED via Python import.
- `init_search_api(app_override=...)` registers BOTH `POST /api/search` AND `GET /api/browse` -- VERIFIED via FastAPI route enumeration in the plan-bundled verify block.
- `browse_endpoint` is decorated with `@wrap_endpoint(endpoint_name='browse')` -- VERIFIED via grep (R-PR-03).
- `_validate_locator` returns `NormalizedLocator` with `effective_*` fields -- VERIFIED via the verify block (R-PR-04).
- `_rate_limiter is not _browse_rate_limiter` -- VERIFIED via the verify block (D-18).
- Handler statelessness D-22 -- VERIFIED via grep returning 0 forbidden references.
- Handler does NOT pass `uid=` to `fetch_browse_bundle` -- VERIFIED via regex on multi-line call (R-PR-04).
- Handler does NOT pass `requested_uid` / `requested_fl_id` to `serialize_browse_payload` -- VERIFIED via regex (R-PR-09).
- Handler does NOT probe image availability -- VERIFIED via grep returning 0 for `image_unavailable|head_probe|probe.*image` (R-PR-01).
- Phase 78's `search_endpoint` body still byte-identical to before this plan (only the final `logger.info` line was modified to mention `, GET /api/browse`).
- All Phase 77/78 targeted tests still GREEN (108/108).
- Wider test suite GREEN (1298 passed, 8 skipped, 0 failed; matches Plan 02 baseline exactly).
- Commit `6fc64d60` exists in git log.
