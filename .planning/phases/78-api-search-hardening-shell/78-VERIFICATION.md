---
phase: 78-api-search-hardening-shell
verified: 2026-04-28T22:30:00Z
status: passed
score: 16/16 must-haves verified
overrides_applied: 0
---

# Phase 78: /api/search + Hardening Shell — Verification Report

**Phase Goal:** `POST /api/search` returns Claude-friendly results from `SearchEngine.execute_search` over a hardened transport (rate-limited, capped, mode-gated, observable, with a uniform error envelope) — and that hardening shell is built once so Phases 79 and 80 inherit it without reimplementation.

**Verified:** 2026-04-28T22:30:00Z
**Status:** passed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | `shared/api_errors.py` exists with `class APIError` and 12 error codes incl. `filter_vocabulary_unavailable` | VERIFIED | `python -c "from shared.api_errors import ERROR_CODES; print(len(ERROR_CODES))"` = 12; `'filter_vocabulary_unavailable' in ERROR_CODES` = True |
| 2 | `web/api_hardening.py` has `_resolve_rate_limit_key` + `_is_loopback_request` as TWO distinct helpers | VERIFIED | api_hardening.py:84 (`def _resolve_rate_limit_key`), :123 (`def _is_loopback_request`) |
| 3 | `RateLimiter` with TTL eviction + `reset_for_tests` | VERIFIED | api_hardening.py:155 `class RateLimiter`; :237 `def reset_for_tests`; :194 `RATE_LIMIT_BUCKET_TTL` used in `_evict_stale` |
| 4 | `wrap_endpoint` + `_build_envelope_response` exist; NO `register_exception_handlers` symbol | VERIFIED | api_hardening.py:333 `def wrap_endpoint`, :274 `def _build_envelope_response`; `grep -c register_exception_handlers web/api_hardening.py` = 0 |
| 5 | `capture_api_event` with `_dropped_events` counter + `get_dropped_event_count()` | VERIFIED | api_hardening.py:509 `_dropped_events: int = 0`; :513 `def get_dropped_event_count`; :617-618 increments on `queue.Full` |
| 6 | `web/search_api.py` has `FiltersModel` + `SearchRequest` with `extra='forbid'` | VERIFIED | search_api.py:77 `class FiltersModel`, :89 `class SearchRequest`, both `model_config = ConfigDict(extra='forbid')`; `SearchRequest(query='x', mode='text', unknown_field='boom')` raises ValidationError |
| 7 | `init_search_api` idempotent via `target_app.state.search_api_initialized` (NOT module-global set) | VERIFIED | search_api.py:129 `if getattr(target_app.state, 'search_api_initialized', False): return`; behavioral check confirms second `init_search_api(app_override=bare)` keeps `/api/search` route count = 1 |
| 8 | Per-endpoint try/except with `_build_envelope_response`; `_consume_last_responsa_downgrade` meta channel + finally-block defensive drain | VERIFIED | search_api.py:325 `except APIError as exc:`, :328/334/340 `_build_envelope_response(request, exc)`; :290 success-path consume; :361-367 `finally:` defensive drain |
| 9 | `shared/fjms_service.validate_filter_values` FAIL-CLOSED; raises 503 on missing vocab, 400 on unknown token; imports APIError from `shared.api_errors` | VERIFIED | fjms_service.py:30 `from shared.api_errors import APIError`; :1397-1418 raise on unknown domain (400) and unloadable vocab (503); behavioral check: `validate_filter_values({'domains':['NOT_A_DOMAIN_XYZ']})` raises APIError(code='unresolvable_filter_value', http_status=400). NO `from web.api_hardening` for APIError anywhere in shared/. |
| 10 | `is_valid_domain_token` + `_domain_vocabulary_is_loadable` helpers exist | VERIFIED | fjms_service.py:443 `def is_valid_domain_token`; :423 `def _domain_vocabulary_is_loadable`; behavioral check: `is_valid_domain_token('Piyyut')` = True (real domain), 'NOT_A_DOMAIN_XYZ' = False |
| 11 | `genizah_core.py` has `_LAST_RESPONSA_DOWNGRADE` thread-local + setter + consumer + consume-on-entry call inside `SearchEngine.execute_search` | VERIFIED | genizah_core.py:65 `_LAST_RESPONSA_DOWNGRADE = threading.local()`; :68 `def _set_last_responsa_downgrade`; :77 `def _consume_last_responsa_downgrade`; :7249 `def execute_search`; :7254 `_consume_last_responsa_downgrade()` (inside execute_search body); :7658 `_set_last_responsa_downgrade(responsa_warning)` (also inside execute_search, line 7668 begins next method) |
| 12 | `web/main.py` imports `init_search_api` and calls it ONCE after `init_api_routes()` | VERIFIED | web/main.py:154 `from web.api import init_api_routes`; :155 `from web.search_api import init_search_api`; :167 `init_api_routes()`; :169 `init_search_api()` |
| 13 | All 82 Phase 78 default tests GREEN | VERIFIED | `pytest tests/test_search_api.py tests/test_api_hardening.py tests/test_api_legacy_unchanged.py -q` → 82 passed in 3.99s |
| 14 | `tests/test_search_api_soak.py` has 3 `@pytest.mark.slow` tests; passes via `pytest -m slow` | VERIFIED | 3 `@pytest.mark.slow` decorators at lines 18, 65, 105; `pytest -m slow tests/test_search_api_soak.py -q` → 3 passed in 0.81s |
| 15 | `pyproject.toml` registers `slow` marker but does NOT contain `addopts = -m "not slow"` | VERIFIED | pyproject.toml:12-15 markers list with `slow` + `e2e`; `grep -E "addopts.*not[[:space:]]+slow" pyproject.toml` exit=1 (no match) |
| 16 | CLAUDE.md has 4 new env vars + tests/README.md documents `pytest -m slow` + CI has dedicated slow-tests job | VERIFIED | CLAUDE.md contains SEARCH_API_MODE, SEARCH_API_RATE_LIMIT, POSTHOG_IP_SALT, SEARCH_API_POSTHOG_SAMPLE_N; tests/README.md:34-40 documents 3 invocation patterns; .github/workflows/ci.yml:43-60 `slow-tests` job runs `pytest -m slow tests/`, default `tests` job at :20-41 unchanged with `pytest tests/` |

**Score:** 16/16 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `shared/api_errors.py` | Neutral APIError + ERROR_CODES (12 codes) + WARNING_CODES | VERIFIED | 78 lines; class APIError with code/message/http_status/headers; 12 codes incl. 'filter_vocabulary_unavailable'; no imports from web.* |
| `web/api_hardening.py` | RateLimiter+TTL eviction+reset_for_tests, _resolve_rate_limit_key, _is_loopback_request, wrap_endpoint, _build_envelope_response, capture_api_event, get_dropped_event_count, _TRUSTED_PROXIES, RATE_LIMIT_BUCKET_TTL; APIError re-export only; NO register_exception_handlers | VERIFIED | 632 lines; all symbols present; APIError re-export identity confirmed (`A is B`); `register_exception_handlers` count=0 |
| `web/search_api.py` | FiltersModel+SearchRequest(extra='forbid'), idempotent init_search_api via target_app.state.search_api_initialized, per-endpoint try/except, _consume_last_responsa_downgrade meta channel, finally-block defensive drain | VERIFIED | 373 lines; ConfigDict(extra='forbid'); `target_app.state.search_api_initialized`; 3 except branches → `_build_envelope_response`; `_INITIALIZED_APPS` count=0 |
| `shared/fjms_service.py` | validate_filter_values FAIL-CLOSED with 503/400 codes; is_valid_domain_token; _domain_vocabulary_is_loadable; imports APIError from shared.api_errors | VERIFIED | All helpers added; fail-closed branches raise APIError(503) for unloadable vocab and APIError(400) for unknown tokens; `from web.api_hardening` not present |
| `genizah_core.py` | _LAST_RESPONSA_DOWNGRADE thread-local + 2 helpers + consume-on-entry inside execute_search + set inside cascade body | VERIFIED | 4 occurrences total; consume call at line 7254 (inside `execute_search` defined at 7249, next method at 7668); set call at 7658 (also inside execute_search) |
| `web/main.py` | imports + calls init_search_api after init_api_routes | VERIFIED | Lines 155, 169 |
| `tests/test_search_api.py` | ≥36 tests | VERIFIED | 40 tests, all GREEN |
| `tests/test_api_hardening.py` | ≥31 tests | VERIFIED | 39 tests, all GREEN |
| `tests/test_api_legacy_unchanged.py` | ≥3 tests | VERIFIED | 3 tests, all GREEN |
| `tests/test_search_api_soak.py` | 3 @pytest.mark.slow tests | VERIFIED | 3 tests, all GREEN with `-m slow` |
| `scripts/soak_search_api.py` | argparse CLI, requests.post, --url/--rate/--duration | VERIFIED | 132 lines; --help works; flags present |
| `pyproject.toml` | slow marker registered; NO addopts default-exclude | VERIFIED | 15 lines; markers list contains slow + e2e; addopts negative grep passes |
| `tests/README.md` | documents `pytest -m slow` invocation | VERIFIED | 44 lines; lines 34-40 list 3 invocation patterns |
| `CLAUDE.md` | 4 new env vars | VERIFIED | All 4 vars present (SEARCH_API_MODE, SEARCH_API_RATE_LIMIT, POSTHOG_IP_SALT, SEARCH_API_POSTHOG_SAMPLE_N) |
| `.github/workflows/ci.yml` | slow-tests job; default tests job unchanged | VERIFIED | 3 jobs (lint-and-docs, tests, slow-tests); default tests job runs `pytest tests/`, new slow-tests job runs `pytest -m slow tests/` |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `web/api_hardening.py` | `shared.api_errors.APIError` | `from shared.api_errors import APIError` | WIRED | line 1 of imports; `A is B` identity confirmed in Python |
| `shared/fjms_service.py` | `shared.api_errors.APIError` | `from shared.api_errors import APIError` (NOT from web.api_hardening) | WIRED | line 30; no `from web.api_hardening` import for APIError anywhere in shared/ |
| `web/search_api.py: search_endpoint` | `state.searcher.execute_search` | direct call with restrict_sys_ids | WIRED | grep confirms reference; tested by happy-path tests |
| `web/search_api.py: search_endpoint` | `shared.search_serializer.serialize_search_payload` | call with results+warnings+filters | WIRED | 2 references in search_api.py |
| `web/search_api.py` | `web.api_hardening._build_envelope_response` | per-endpoint try/except | WIRED | 8 occurrences; 3 except branches; idempotency + envelope round-trip exercised by 40 tests |
| `web/search_api.py: Responsa branch` | `_consume_last_responsa_downgrade` (meta channel) | thread-local read | WIRED | 6 occurrences; consume-on-entry + success-path consume + finally drain |
| `web/main.py` | `init_search_api` | line 155 import + line 169 call | WIRED | confirmed by behavioral test: bare-app init mounts `/api/search` |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|---------------|--------|--------------------|--------|
| `web/search_api.py POST /api/search` | `results` | `state.searcher.execute_search(...)` (genizah_core.SearchEngine) | Yes — Tantivy/SQLite-backed search returns real result rows; D-20 statelessness contract observed (no `state.last_results`, `app.storage`, `request.cookies` references in search_api.py) | FLOWING |
| Filter validation | `restrict_sys_ids` | `shared.fjms_service.get_filter_sys_ids(...)` (FJMS sidecar SQLite) | Yes — real DB-backed lookup (Piyyut domain validates True; bogus value raises 400) | FLOWING |
| Responsa downgrade warning | thread-local `_LAST_RESPONSA_DOWNGRADE.value` | set inside `SearchEngine.execute_search` cascade decision (genizah_core.py:7658) | Yes — surfaced via `_consume_last_responsa_downgrade` even when results==[] | FLOWING |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| Phase 78 default test suite | `pytest tests/test_search_api.py tests/test_api_hardening.py tests/test_api_legacy_unchanged.py -q` | 82 passed in 3.99s | PASS |
| Soak tests (slow marker) | `pytest -m slow tests/test_search_api_soak.py -q` | 3 passed in 0.81s | PASS |
| Wider regression | `pytest tests/ --ignore=tests/e2e` | 1298 passed, 5 skipped in 27.72s | PASS |
| APIError class identity (re-export) | `from web.api_hardening import APIError as A; from shared.api_errors import APIError as B; assert A is B` | exit 0 | PASS |
| ERROR_CODES count + filter_vocabulary_unavailable | `from shared.api_errors import ERROR_CODES; len(ERROR_CODES); 'filter_vocabulary_unavailable' in ERROR_CODES` | 12, True | PASS |
| Bare-app route registration + idempotency | `init_search_api(app_override=bare); init_search_api(app_override=bare)` → route count 1 | 1 | PASS |
| extra='forbid' on SearchRequest | `SearchRequest(query='x', mode='text', unknown_field='boom')` | raises ValidationError | PASS |
| Error envelope on empty query | `POST /api/search {query:'', mode:'text'}` | 400 with `{'error': ...}` envelope | PASS |
| Error envelope on unknown mode | `POST /api/search {query:'x', mode:'NOT_A_MODE'}` | 400 with `{'error': ...}` envelope | PASS |
| Filter fail-closed (unknown domain) | `validate_filter_values({'domains':['NOT_A_DOMAIN_XYZ']})` | raises APIError('unresolvable_filter_value', http_status=400) | PASS |
| Filter accepted (real domain) | `is_valid_domain_token('Piyyut')` | True | PASS |
| `check_docs.py` | `python scripts/check_docs.py` | exit 0, "All checks passed!" | PASS |
| Soak script CLI | `python scripts/soak_search_api.py --help` | exit 0, argparse help printed | PASS |
| Statelessness contract D-20 | `grep -cE "state\.last_results|state\.current_search_query|app\.storage|request\.cookies" web/search_api.py` | 0 | PASS |
| `register_exception_handlers` removed | `grep -c register_exception_handlers web/api_hardening.py` | 0 | PASS |
| `_INITIALIZED_APPS` (R2-#2 module-global removed) | `grep -c _INITIALIZED_APPS web/search_api.py` | 0 | PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|------------|-------------|-------------|--------|----------|
| API-01 | 78-03 | POST /api/search exact request shape; locator on every item; ranking fields | SATISFIED | search_api.py SearchRequest with strict shape (extra='forbid'); locator emitted via Phase 77 serializer; test_locator_present_on_every_item GREEN |
| API-04 | 78-01, 78-02, 78-03 | Input validation + uniform error envelope; legacy /api/* unaffected | SATISFIED | 12-code envelope; per-endpoint `_build_envelope_response`; test_legacy_validation_failure_envelope_unchanged GREEN proves legacy 422 unchanged |
| API-05 | 78-03 | Locator on every result | SATISFIED | test_locator_present_on_every_item GREEN; serializer (Phase 77) emits locator |
| API-06 | 78-03 | Endpoints stateless; identical request → identical body | SATISFIED | D-20 statelessness contract enforced (zero forbidden surfaces in search_api.py); test_identical_requests_byte_identical_modulo_timestamp GREEN |
| API-07 | 78-03 | Filter values resolved through FJMS pipeline; rejected when unresolvable, never silently dropped | SATISFIED | validate_filter_values fail-closed (R2-#3): 400 unresolvable_filter_value / 503 filter_vocabulary_unavailable; behavioral test confirms unknown domain rejected |
| HARDEN-01 | 78-01, 78-02, 78-03, 78-04 | Per-IP rate limit; 429 + Retry-After; other /api/* unaffected | SATISFIED | RateLimiter sliding 60s window; APIError(429, headers={Retry-After:N}); 3 soak tests GREEN; legacy routes spot-checked unchanged |
| HARDEN-02 | 78-01, 78-03 | Capped result count (default 50, max 200) | SATISFIED | search_api.py:101-103 DEFAULT_LIMIT=50, MAX_LIMIT=200; test_limit_too_high + test_limit_zero_returns_invalid_request GREEN |
| HARDEN-03 | 78-01, 78-03 | Capped query length + Responsa cascade warning surfaces in top-level warnings | SATISFIED | QUERY_LENGTH_CAP=1000; test_query_too_long GREEN; thread-local meta channel + test_warnings_surfaced_at_top_level + test_zero_result_responsa_downgrade_warning_still_surfaced GREEN |
| HARDEN-04 | 78-01, 78-02, 78-03, 78-04 | SEARCH_API_MODE=open\|localhost-only\|disabled; flippable without code change | SATISFIED | enforce_mode_gate reads env per-request; 5 mode-gate tests GREEN incl. XFF spoof rejection; CLAUDE.md documents env var |
| HARDEN-05 | 78-01, 78-02 | PostHog event per request capturing endpoint/mode/latency-bucket/result-count-bucket/IP-hash; no payload contents | SATISFIED | capture_api_event in finally block; latency_bucket + result_count_bucket + hash_ip helpers; test_capture_api_event_does_not_log_query_or_filters GREEN; SEARCH_API_POSTHOG_SAMPLE_N documented |

All 10 requirement IDs from PLAN frontmatter accounted for and SATISFIED.

### Anti-Patterns Found

None. Targeted scan of phase-modified files (`shared/api_errors.py`, `web/api_hardening.py`, `web/search_api.py`, `shared/fjms_service.py` Phase-78 additions, `genizah_core.py` Phase-78 additions, `web/main.py` Phase-78 additions, `tests/test_*search*.py`, `tests/test_api_hardening.py`, `tests/test_api_legacy_unchanged.py`, `tests/test_search_api_soak.py`, `scripts/soak_search_api.py`, `pyproject.toml`, `tests/README.md`, `.github/workflows/ci.yml`) found:

- 0 TODO/FIXME/PLACEHOLDER markers in Phase 78 code
- 0 hollow handlers (every except branch routes through `_build_envelope_response`; finally drains the thread-local)
- 0 D-20 statelessness violations in `web/search_api.py`
- 0 `register_exception_handlers` references (Concern #2 enforced)
- 0 `_INITIALIZED_APPS` references (R2-#2 enforced)
- 0 `from web.api_hardening import APIError` in shared/* (Concern #3 enforced — verified via grep)

### Cross-AI Review Concern Closure

All concerns from `78-REVIEWS.md` traced to evidence:

| Concern | Severity | Status |
|---------|----------|--------|
| #1 — XFF trust separation (two helpers) | HIGH | RESOLVED — `_resolve_rate_limit_key` + `_is_loopback_request` exist as distinct functions |
| #2 — global handler scope removed | HIGH | RESOLVED — `register_exception_handlers` removed; per-endpoint `_build_envelope_response` |
| #3 — shared→web inversion | HIGH | RESOLVED — `shared/api_errors.py` is single source; `from web.api_hardening` import absent in shared/ |
| #4 — XFF spoof | HIGH | RESOLVED — `_is_loopback_request` enforces `all(e in LOOPBACK_IPS for e in entries)` |
| #5 — bucket TTL eviction | MED | RESOLVED — `RATE_LIMIT_BUCKET_TTL` + `_evict_stale` global sweep |
| #6 — zero-result downgrade warning | MED | RESOLVED — thread-local meta channel survives empty results |
| #7 — pyproject default-exclude | MED | RESOLVED — slow marker registered, NO `addopts = -m "not slow"` |
| #8 — legacy 422 envelope parity | MED | RESOLVED — `test_legacy_validation_failure_envelope_unchanged` GREEN |
| #9 — PostHog drop counter | MED | RESOLVED — `_dropped_events` + `get_dropped_event_count()` |
| #10 — idempotent init | LOW | RESOLVED — `target_app.state.search_api_initialized` (R2-#2) |
| #12 — Pydantic-error PostHog capture | LOW | RESOLVED — body catches PydanticValidationError, pins error_code='invalid_request' before re-raising |
| R2-#1 — thread-local lifecycle | MED | RESOLVED — consume-on-entry + success-path consume + finally drain |
| R2-#2 — id(app) GC reuse | MED | RESOLVED — app-state flag instead of module-global set |
| R2-#3 — fail-closed filter | HIGH | RESOLVED — `validate_filter_values` raises 503/400, never silent allow-all |
| R2-#4 — XFF rule prose lock | MED | RESOLVED — module docstring locks algorithm spec |
| R2-#5 — CI slow-tests gate | MED | RESOLVED — dedicated `slow-tests` job in ci.yml |
| R2-#6 — wrap_endpoint owns boilerplate | LOW | RESOLVED — full try/except/finally + envelope + PostHog |
| R2-#7 — mode-gate test determinism | LOW | RESOLVED — patched `_is_loopback_request` to True for deterministic 200 |

### Human Verification Required

None. All goal-relevant behaviors are programmatically verified via:
- 82 default unit + integration tests (3.99s)
- 3 slow soak tests with monkeypatched time (0.81s)
- 1298-test wider regression suite (27.72s, no regressions)
- Behavioral spot-checks of error envelopes, filter validation, idempotent init, statelessness contract

The optional D-22 form 2 (live-deployment soak via `scripts/soak_search_api.py` against production) is documented in 78-04-SUMMARY.md as "run from a developer machine, NOT CI, against a live deployment" and is intentionally separated from automated phase-gate verification. Form 1 (in-process soak) covers the rate-limiter end-to-end behaviorally.

### Gaps Summary

None. Phase 78 goal achieved end-to-end:
- Hardening shell built once in `web/api_hardening.py` + `shared/api_errors.py` and consumed by `web/search_api.py`
- POST /api/search returns Phase-77-shape responses with locators on every item
- Uniform error envelope replaces FastAPI's default 422 for /api/search; legacy /api/* routes preserve their behavior
- Rate limiting, mode gating, fail-closed filter validation, query length cap, Responsa downgrade warnings, PostHog observability all wired and tested
- Phases 79 and 80 can `from web.api_hardening import RateLimiter, _resolve_rate_limit_key, enforce_mode_gate, _build_envelope_response, capture_api_event, wrap_endpoint, ...` and inherit the shell without reimplementation, exactly as required

---

*Verified: 2026-04-28T22:30:00Z*
*Verifier: Claude (gsd-verifier)*
