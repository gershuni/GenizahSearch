---
phase: 78
plan: 03
subsystem: api-search-hardening-shell
tags: [api, search, fail-closed, dependency-inversion, idempotent-registrar, threadlocal, posthog]
requires:
  - shared/api_errors.py (Plan 78-02 — APIError + ERROR_CODES)
  - web/api_hardening.py (Plan 78-02 — RateLimiter, _resolve_rate_limit_key, _build_envelope_response, capture_api_event, enforce_mode_gate)
  - tests/test_search_api.py (Plan 78-01 RED scaffold — 40 tests)
  - tests/test_api_legacy_unchanged.py (Plan 78-01 RED scaffold — 3 tests)
  - shared/search_serializer.serialize_search_payload (Phase 77)
  - genizah_core.SearchEngine.execute_search
provides:
  - genizah_core._LAST_RESPONSA_DOWNGRADE (threading.local)
  - genizah_core._set_last_responsa_downgrade(message)
  - genizah_core._consume_last_responsa_downgrade()  (read-and-clear)
  - shared/fjms_service.is_valid_domain_token(token)
  - shared/fjms_service._domain_vocabulary_is_loadable()
  - shared/fjms_service.FjmsService._discover_valid_materials()
  - shared/fjms_service.FjmsService.validate_filter_values(filters)
  - shared/fjms_service.validate_filter_values(filters)  (module-level shorthand)
  - shared/fjms_service.get_filter_sys_ids(**kwargs)  (module-level shorthand)
  - shared/api_errors.ERROR_CODES extended with 'filter_vocabulary_unavailable'
  - web/search_api.FiltersModel
  - web/search_api.SearchRequest
  - web/search_api.init_search_api(app_override=None)  (idempotent registrar)
  - web/search_api._consume_last_responsa_downgrade()  (re-export, monkeypatchable)
  - POST /api/search endpoint
affects:
  - .planning/STATE.md (plan progress 2/4 → 3/4)
  - .planning/ROADMAP.md (Phase 78 progress)
  - tests/test_search_api.py (RED → GREEN, 40/40 tests pass)
  - tests/test_api_legacy_unchanged.py (RED → GREEN, 3/3 tests pass)
  - tests/test_api_hardening.py (still GREEN, 39/39 — no regressions)
tech-stack:
  added: []
  patterns: [threadlocal-meta-channel, fail-closed-vocabulary-validation, app-state-idempotency, per-endpoint-envelope, late-binding-monkeypatch]
key-files:
  created:
    - web/search_api.py (373 lines)
  modified:
    - genizah_core.py (+48 lines: thread-local + 2 helpers + consume-on-entry + cascade signal call)
    - shared/fjms_service.py (+251 lines: module-level helpers + FjmsService methods + module-level shorthands)
    - shared/api_errors.py (+1 line: 'filter_vocabulary_unavailable' in ERROR_CODES)
decisions:
  - "Module-level get_filter_sys_ids shorthand added to shared/fjms_service.py (Rule 2 — missing critical functionality). The plan's example body had the handler calling fjms.get_filter_sys_ids (bound method), but tests/test_search_api.py:322 monkeypatches shared.fjms_service.get_filter_sys_ids (module attribute). The handler now calls via late-bound module attribute (`from shared import fjms_service as _fjms_module; _fjms_module.get_filter_sys_ids(...)`) so the monkeypatch is respected. Symmetric to the validate_filter_values module-level shorthand the plan already required."
  - "Pydantic models use `model_config = ConfigDict(extra='forbid')` rather than the dict literal `model_config = {'extra': 'forbid'}` shown in the plan example. Both syntaxes are equivalent in Pydantic v2 — `ConfigDict` is the canonical one and matches the acceptance-criterion grep `extra='forbid'` (which expected keyword-arg style)."
  - "Literal[...] uses standard PEP-8 spacing (`Literal['text', 'Title', 'Shelfmark', 'Responsa']`). The plan's acceptance grep `Literal\\[.text.,.Title.,.Shelfmark.,.Responsa.\\]` failed to account for spaces after commas — documented as a non-content deviation; the Literal usage is correct and the test suite enforces the contract."
  - "Cascade decision site signal placement: `_set_last_responsa_downgrade(responsa_warning)` placed inside the `if responsa_warning:` guard at line ~7613, BEFORE the legacy `deduped[0]['responsa_warning'] = responsa_warning` attachment. This way the thread-local is set whether or not deduped is populated — the whole point of Concern #6."
  - "RateLimiter exception path: rather than reading return tuple, the handler now relies on RateLimiter.check raising APIError(rate_limited, 429, headers={'Retry-After': N}) — that's how Plan 78-02 implemented it (Plan 02 Rule 1 deviation). The 'except APIError as exc' branch routes through _build_envelope_response which propagates the Retry-After header automatically."
metrics:
  completed: 2026-04-28
  duration: ~10min
  task_count: 3
  file_count: 4
---

# Phase 78 Plan 03: POST /api/search End-to-End Summary

Wave 2 GREEN gate. Builds the POST /api/search route end-to-end on top of Plan 78-02's hardening shell, rewrites `shared/fjms_service.validate_filter_values` with a fail-closed policy, and threads a thread-local cascade-downgrade signal through `genizah_core.py` so warnings survive empty result sets.

## What Was Built

### genizah_core.py (Task 1 — commit 9af320b3, +48 lines)

Three additions, surgically scoped to avoid touching the rest of the ~22.5K-line file:

1. **Module-level thread-local + 2 helpers** (after the `unified_variants` import block, before `# --- Shmidman Rare-Letter Helpers ---`):
   - `_LAST_RESPONSA_DOWNGRADE = threading.local()`
   - `_set_last_responsa_downgrade(message: str) -> None`
   - `_consume_last_responsa_downgrade() -> Optional[str]` (read-and-clear)
2. **R2-#1 consume-on-entry** at the very top of `SearchEngine.execute_search` body (line ~7250): `_consume_last_responsa_downgrade()` discards any stale per-thread signal from a prior failed invocation.
3. **Cascade decision site call** (line ~7613, inside `if responsa_warning:` guard): `_set_last_responsa_downgrade(responsa_warning)`. The legacy `deduped[0]['responsa_warning'] = responsa_warning` attachment is preserved as the secondary path for callers that read result rows.

The signal is one-shot per `execute_search` call — entry-time consume + handler-time consume gives at-most-once delivery semantics. No cross-request leak even on the exception path.

### shared/fjms_service.py + shared/api_errors.py (Task 2 — commit f68f4d4f, +252 lines)

**`shared/api_errors.py`:** added `'filter_vocabulary_unavailable'` to `ERROR_CODES` frozenset (one-line edit).

**`shared/fjms_service.py` additions:**

| Symbol | Layer | Role |
|--------|-------|------|
| `from shared.api_errors import APIError` | imports | Concern #3 — neutral location |
| `_domain_vocabulary_is_loadable()` | module | R2-#3 predicate: returns False when sidecar absent / loader raises / `get_all_domains()` empty |
| `is_valid_domain_token(token)` | module | R2-#3: canonicalizes via `unqualify_domain_name` + checks `Domain = ? OR ParentDomain = ?` (matches the UNION used in `get_filter_sys_ids`) |
| `FjmsService._discover_valid_materials()` | method | Returns set of distinct `FragmentMaterial` values from `catalog_fields`; cached; empty set on `_conn is None` (caller MUST treat empty as fail-closed) |
| `FjmsService.validate_filter_values(filters)` | method | R2-#3 fail-closed rewrite: known bad → 400 unresolvable_filter_value; loader unavailable / empty → 503 filter_vocabulary_unavailable; never silent allow-all |
| `validate_filter_values(filters)` | module shorthand | Singleton wrapper |
| `get_filter_sys_ids(**kwargs)` | module shorthand | Singleton wrapper (Rule 2 deviation — see below) |

`FjmsService.__init__` now initializes `self._materials_cache: Optional[set] = None`.

### web/search_api.py (Task 3 — commit ae1787b3, 373 lines NEW)

POST /api/search end-to-end. Architecture:

1. **Module-level state.** `_rate_limiter = RateLimiter(default_limit=30)` — Phases 79/80 import this same instance.
2. **`_consume_last_responsa_downgrade()` re-export** — defers to `genizah_core` impl; tests can monkeypatch `web.search_api._consume_last_responsa_downgrade` directly (Concern #6 lock-in).
3. **Pydantic models with `extra='forbid'`** — `FiltersModel` (D-15: lists for categorical, scalars for dates) and `SearchRequest` (D-05) using `ConfigDict(extra='forbid')`.
4. **Constants** — `QUERY_LENGTH_CAP = 1000` (D-08), `DEFAULT_LIMIT = 50`, `MAX_LIMIT = 200` (D-09).
5. **`init_search_api(app_override=None)`** — idempotent (R2-#2: `target_app.state.search_api_initialized`). Registers ONE route: `POST /api/search`. Does NOT install global exception handlers (Concern #2).
6. **The handler `search_endpoint(request: Request)`** — single body wrapping the full pipeline in try/except/finally. Handler steps:
   1. Parse body via `await request.json()` (own the error path).
   2. Validate body via `SearchRequest(**body)` — catch `PydanticValidationError`, pin `status_code=400, error_code='invalid_request'`, re-raise so outer except builds envelope.
   3. `enforce_mode_gate(request)` — D-02/03/04.
   4. `_rate_limiter.check(client_ip)` — raises APIError(429, headers={'Retry-After': N}) on limit hit.
   5. Query post-validation: empty → query_required, >1000 chars → query_too_long, limit ≤0 → invalid_request, limit >200 → limit_too_high.
   6. Filter pipeline (when present): `validate_filter_values` (fail-closed) → `get_filter_sys_ids(**filters_dict)`. Empty intersection → short-circuit with `results=[], total=0` (no execute_search call).
   7. Build `responsa_options` for Responsa mode.
   8. `state.searcher.execute_search(...)` with kwargs (`restrict_sys_ids`, `responsa_options`, etc.).
   9. Cap results to `req.limit`.
   10. **R2-#1 success-path consume**: `downgrade_msg = _consume_last_responsa_downgrade()`.
   11. Build `warnings_list`: prefix `'query_downgraded: '` from thread-local OR from legacy `results[0]['responsa_warning']` fallback (mutually exclusive). Strip per-row markers from `results[0]` regardless.
   12. `serialize_search_payload(results, ..., warnings=warnings_list, total=total)`.
7. **Per-endpoint envelope rendering.** Three except branches: `APIError` → `_build_envelope_response(request, exc)`; `(RequestValidationError, PydanticValidationError)` → same with `error_code='invalid_request'` (Concern #12); generic `Exception` → 500 envelope.
8. **Finally block.** `capture_api_event(...)` fires once per request (success or error) with status_code/error_code pinned by the branch above. Then `_consume_last_responsa_downgrade()` runs again as a defensive drain (R2-#1) — no-op when success path already drained, but catches the case where execute_search raised after setting the signal but before step 10.

Late-bound module imports (`from shared import fjms_service as _fjms_module; _fjms_module.validate_filter_values(...)`) so test fixtures can monkeypatch the call site (matches the pattern Plan 78-01's RED tests assumed).

## Resolution of Review Concerns

| Concern | Source | How resolved | Test evidence |
|---------|--------|--------------|---------------|
| **#2** — global handler scope | Both reviewers HIGH | `init_search_api` does NOT call any global handler installer. `_build_envelope_response` invoked from per-endpoint try/except. Three test files prove legacy /api/* validation envelope unchanged. | `test_legacy_validation_failure_envelope_unchanged`, `test_error_envelope_shape` |
| **#3** — shared→web inversion | Codex HIGH | `from shared.api_errors import APIError` at the top of `shared/fjms_service.py`. Negative grep: no `from web.api_hardening` or `^from web\\.` lines anywhere in shared/. | `test_apierror_imported_from_shared_api_errors_module` |
| **#6** — zero-result downgrade warning | Codex MED | Thread-local `_LAST_RESPONSA_DOWNGRADE` in `genizah_core`; cascade decision site sets it; handler reads via `_consume_last_responsa_downgrade` BEFORE checking results length. Surface even when `results == []`. | `test_zero_result_responsa_downgrade_warning_still_surfaced`, `test_warnings_surfaced_at_top_level` |
| **#10** — duplicate route registration on dev-reload | Codex LOW | `init_search_api` checks `target_app.state.search_api_initialized` (R2-#2 — was module-global set in round 1). Per-app, GC-safe. | `test_init_search_api_idempotent`, `test_init_search_api_uses_app_state_not_module_global` |
| **#12** — Pydantic-error PostHog gap | Codex LOW | Body catches `PydanticValidationError`; pins `status_code=400, error_code='invalid_request'` BEFORE re-raising. Finally block fires `capture_api_event` with these labels. | `test_pydantic_structural_error_captures_posthog_invalid_request_event` |
| **R2-#1** — thread-local lifecycle | Codex MED | (a) `execute_search` consume-on-entry. (b) Handler success path consume at step 10. (c) Finally-block defensive consume. Stale signals from crashed prior requests cannot leak. | `test_responsa_downgrade_threadlocal_cleared_on_exception` |
| **R2-#2** — id(app) GC reuse hazard | Codex MED | `target_app.state.search_api_initialized` instead of module-global `_INITIALIZED_APPS: set[int]`. Each FastAPI app has its own state; second app does NOT inherit the flag. | `test_init_search_api_uses_app_state_not_module_global` |
| **R2-#3** — fail-closed filter validation | Codex HIGH | Materials empty vocabulary → APIError(503), domain loader raises → APIError(503), unknown token → APIError(400). `is_valid_domain_token` canonicalizes via `unqualify_domain_name` + UNION pattern. Never silent allow-all. | `test_validate_filter_values_qualified_domain_accepted`, `_parent_domain_accepted`, `_unknown_domain_rejected`, `_domain_vocabulary_unavailable_fails_closed`, `_empty_domain_vocabulary_fails_closed`, `_materials_vocabulary_unavailable_fails_closed`, `_empty_materials_vocabulary_fails_closed` (7 tests, all GREEN) |

## Acceptance Criteria — Verification

### Task 1 (genizah_core.py thread-local)

| Criterion | Required | Actual | Status |
|-----------|----------|--------|--------|
| `_LAST_RESPONSA_DOWNGRADE` count | ≥3 | 4 | OK |
| `def _consume_last_responsa_downgrade` count | =1 | 1 | OK |
| `def _set_last_responsa_downgrade` count | =1 | 1 | OK |
| `threading.local` count | ≥1 | 1 | OK |
| `_set_last_responsa_downgrade` total uses | ≥2 | 2 | OK |
| `_consume_last_responsa_downgrade` total uses | ≥2 | 2 (def + 1 call) | OK |
| Verify command (`set/consume/consume`) prints OK | yes | yes | OK |
| R2-#1 lifecycle proof exits 0 | yes | yes | OK |
| 219 responsa tests still pass | yes | 219 passed, 8 skipped | OK |

The plan's `awk '/^    def execute_search/,/^    def [a-zA-Z]/'` scoping pattern fails because awk treats both bounds as matching the same first line — it returns only line 1 of the block. The intent (calls live inside `execute_search`) is satisfied by manual line-range inspection: `_consume_last_responsa_downgrade()` is at line 7253 (inside `execute_search` body, lines 7249-7674), and `_set_last_responsa_downgrade(responsa_warning)` is at line 7613 (also inside, before the legacy `deduped[0]['responsa_warning']` attachment).

### Task 2 (shared/fjms_service.py + api_errors.py)

| Criterion | Required | Actual | Status |
|-----------|----------|--------|--------|
| `def validate_filter_values` count | ≥2 | 2 | OK |
| `def is_valid_domain_token` count | =1 | 1 | OK |
| `def _domain_vocabulary_is_loadable` count | =1 | 1 | OK |
| `from shared.api_errors import APIError` | ≥1 | 1 | OK |
| `from web.api_hardening import APIError` (must be 0) | 0 | 0 | OK |
| `^from web\.` (must be 0) | 0 | 0 | OK |
| `unresolvable_filter_value` | ≥4 | 5 | OK |
| `filter_vocabulary_unavailable` (fjms_service) | ≥4 | 9 | OK |
| `filter_vocabulary_unavailable` (api_errors) | =1 | 1 | OK |
| `http_status=503` | ≥4 | 9 | OK |
| `http_status=400` | ≥4 | 5 | OK |
| `unqualify_domain_name` | ≥3 | 5 | OK |
| `WHERE Domain = .* OR ParentDomain = ` | ≥1 | 2 | OK |
| canonical keys (eng_desc/person_id/org_title/title_id) | ≥4 | 12 | OK |
| FragmentMaterial / `_discover_valid_materials` | ≥2 | 8 | OK |
| Verify command shows fail-closed APIError | 400 or 503 | 400 (sidecar present) | OK |
| Loader-exception → 503 fail-closed | yes | yes | OK |

### Task 3 (web/search_api.py)

| Criterion | Required | Actual | Status |
|-----------|----------|--------|--------|
| File exists, ≥250 lines | yes | 373 | OK |
| Imports succeed | yes | yes | OK |
| `extra='forbid'` count | ≥2 | 2 | OK |
| `Literal['text', 'Title', 'Shelfmark', 'Responsa']` | ≥1 | 1 (with PEP-8 spaces) | OK (deviation noted) |
| `QUERY_LENGTH_CAP = 1000` | =1 | 1 | OK |
| `MAX_LIMIT = 200` | =1 | 1 | OK |
| `DEFAULT_LIMIT = 50` | =1 | 1 | OK |
| `@target_app.post` count | ≥1 | 1 | OK |
| `@app.post` (must be 0) | 0 | 0 | OK |
| `register_exception_handlers` (must be 0) | 0 | 0 | OK |
| `_build_envelope_response` count | ≥3 | 8 | OK |
| `_INITIALIZED_APPS` (must be 0) | 0 | 0 | OK |
| `target_app.state.search_api_initialized` count | ≥2 | 3 | OK |
| `_consume_last_responsa_downgrade` count | ≥3 | 6 | OK |
| `defensive .* drain` | ≥1 | 1 | OK |
| `PydanticValidationError\|RequestValidationError` | ≥2 | 6 | OK |
| `_resolve_rate_limit_key` | ≥1 | 2 | OK |
| `get_client_ip` (must be 0) | 0 | 0 | OK |
| `enforce_mode_gate` | ≥1 | 2 | OK |
| `_rate_limiter\.check\|RateLimiter` | ≥2 | 4 | OK |
| `^[[:space:]]*capture_api_event\(` | =1 | 1 | OK |
| `serialize_search_payload` | ≥1 | 2 | OK |
| `validate_filter_values` | ≥1 | 3 | OK |
| `from shared.api_errors import APIError` | ≥1 | 1 | OK |
| D-20 forbidden surfaces (must be 0) | 0 | 0 | OK |
| `responsa_warning` (legacy fallback) | ≥1 | 2 | OK |
| `Retry-After` | ≥1 | 2 | OK |
| `query_required\|query_too_long\|limit_too_high\|invalid_request` | ≥4 | 8 | OK |
| `internal_error` | ≥1 | 2 | OK |
| `responsa_options` | ≥1 | 4 | OK |
| `exclude_none=True` | ≥1 | 1 | OK |
| `short_circuit_empty` | ≥2 | 3 | OK |
| `elif len(restrict_sys_ids) == 0` (must be 0) | 0 | 0 | OK |
| `sys.exc_info` (must be 0) | 0 | 0 | OK |
| `except APIError as exc` | ≥1 | 1 | OK |

### Test Suite Status

| Test file | Required | Actual | Status |
|-----------|----------|--------|--------|
| tests/test_search_api.py | ≥36 GREEN | 40/40 GREEN | OK |
| tests/test_api_legacy_unchanged.py | ≥3 GREEN | 3/3 GREEN | OK |
| tests/test_api_hardening.py (regression) | 39 GREEN | 39/39 GREEN | OK |
| **Combined Phase 78 total** | ≥60 | **82** | OK |
| Wider test suite (regression) | no regressions | 1295 passed, 8 skipped | OK |

## Deviations from Plan

### 1. Module-level `get_filter_sys_ids` shorthand added (Rule 2 — missing critical functionality)

**Found during:** Task 3 verification (test_filter_resolution_yields_empty_intersection)

**Issue:** `tests/test_search_api.py:321-324` monkeypatches `shared.fjms_service.get_filter_sys_ids` (module attribute, with `raising=False`). The plan's example handler body calls `fjms.get_filter_sys_ids(...)` (bound method on the singleton), which would NOT be intercepted by a module-attribute monkeypatch. Without a module-level shorthand, the test would either find no such attribute or hit the real FJMS sidecar.

**Fix:** Added module-level `get_filter_sys_ids(**kwargs)` shorthand to `shared/fjms_service.py` (mirrors the `validate_filter_values` shorthand the plan already required). Handler calls via late-bound module attribute: `from shared import fjms_service as _fjms_module; _fjms_module.get_filter_sys_ids(...)`.

**Files modified:** `shared/fjms_service.py` (+10 lines)

**Commit:** `ae1787b3` (bundled with web/search_api.py since it's a Task 3 dependency)

### 2. Pydantic config syntax: `ConfigDict(extra='forbid')` instead of dict literal (Rule 1 — bug fix)

**Found during:** Task 3 grep verification (`extra='forbid'` count = 0 with dict-literal syntax)

**Issue:** Plan's example used `model_config = {'extra': 'forbid'}` (dict literal with colon-style). The plan's acceptance grep `"extra='forbid'"` (keyword-arg style) returned 0 against the dict-literal syntax. Both are equivalent in Pydantic v2.

**Fix:** Switched to `model_config = ConfigDict(extra='forbid')` — canonical Pydantic v2 idiom AND matches the acceptance grep.

**Files modified:** `web/search_api.py`

**Commit:** `ae1787b3`

### 3. `Literal[...]` PEP-8 spacing (documented, not modified)

The plan's acceptance grep `Literal\\[.text.,.Title.,.Shelfmark.,.Responsa.\\]` (no space after comma) returns 0 against PEP-8 standard `Literal['text', 'Title', 'Shelfmark', 'Responsa']` (with spaces). Code is correct; grep was overly strict. The runtime behavior is enforced by the test suite (`test_unknown_mode_returns_invalid_request` asserts mode='NOT_A_MODE' returns 400 invalid_request).

### 4. R2-#1 `Retry-After` is propagated by RateLimiter, not the handler (documented, not modified)

The plan acceptance grep `Retry-After` ≥1 was satisfied via documentation comments referencing the header. The actual mechanism is Plan 78-02's deviation: `RateLimiter.check` raises APIError with `headers={'Retry-After': N}`, and `_build_envelope_response` propagates the headers to the JSONResponse. The handler doesn't construct the Retry-After header itself — it just routes the APIError through the per-endpoint envelope branch.

## Authentication Gates

None encountered.

## Self-Check: PASSED

**Files created (verified via Read tool / git status):**
- `web/search_api.py` (373 lines) — FOUND
- `.planning/phases/78-api-search-hardening-shell/78-03-SUMMARY.md` — FOUND (this file)

**Files modified (verified via git log + git diff):**
- `genizah_core.py` (commit 9af320b3) — FOUND
- `shared/api_errors.py` (commit f68f4d4f) — FOUND
- `shared/fjms_service.py` (commits f68f4d4f + ae1787b3) — FOUND

**Commits (verified via `git log --oneline`):**
- `9af320b3` feat(78-03): add thread-local cascade-downgrade signal to genizah_core (Concern #6, R2-#1)
- `f68f4d4f` feat(78-03): add fail-closed validate_filter_values + helpers (D-17, R2-#3, Concern #3)
- `ae1787b3` feat(78-03): implement web/search_api.py POST /api/search end-to-end (Concerns #2, #3, #6, #10, #12, R2-#1, R2-#2)

**Test verification:**
- `python -m pytest tests/test_search_api.py tests/test_api_legacy_unchanged.py tests/test_api_hardening.py` → 82 passed in 2.50s
- Full regression: `python -m pytest tests/` → 1295 passed, 8 skipped in 23.59s (no regressions)

**Import verification:**
- `python -c "from web.search_api import init_search_api, FiltersModel, SearchRequest, _consume_last_responsa_downgrade"` → exits 0
- `python -c "from shared.fjms_service import validate_filter_values, is_valid_domain_token, _domain_vocabulary_is_loadable, get_filter_sys_ids"` → exits 0
- `python -c "from shared.api_errors import ERROR_CODES; assert 'filter_vocabulary_unavailable' in ERROR_CODES"` → exits 0
- `python -c "from genizah_core import _consume_last_responsa_downgrade, _set_last_responsa_downgrade"` → exits 0

## TDD Gate Compliance

This plan is the GREEN gate for Wave 2 of the type:tdd phase:

- Plan 78-01 wrote 40 + 3 RED tests in tests/test_search_api.py + tests/test_api_legacy_unchanged.py (commits 9f47025d, 1a38158c)
- Plan 78-03 (this plan) writes web/search_api.py + shared/fjms_service.py rewrite + genizah_core.py thread-local to flip those 43 tests GREEN
- 3 `feat(...)` commits exist after the `test(...)` commits — RED → GREEN gate sequence satisfied
- No `refactor(...)` commits — implementation went straight from RED to GREEN with two non-content deviations applied inline (one Rule 2: missing module-level shorthand for monkeypatchability; one Rule 1: ConfigDict syntax to satisfy acceptance grep)

`tests/test_api_hardening.py` (39 tests) was already GREEN from Plan 78-02 and remains GREEN — no regressions from the FjmsService changes or the genizah_core.py thread-local additions.

Phase 78 is now functionally complete pending Plan 04 (wire `init_search_api()` into `web/main.py` so the route is mounted on the live NiceGUI app).
