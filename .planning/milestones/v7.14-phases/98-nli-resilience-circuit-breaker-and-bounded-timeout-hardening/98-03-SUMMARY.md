---
phase: 98-nli-resilience-circuit-breaker-and-bounded-timeout-hardening
plan: 03
subsystem: resilience
tags: [circuit-breaker, web-api, call-site-wiring, timeout-shortening, threadpool-safety]

# Dependency graph
requires:
  - phase: 98-02
    provides: [shared.nli_circuit_breaker module — is_open / record_failure / record_success / NLI_*_TIMEOUT constants]
provides:
  - web.api.fetch_fl_ids_from_nli (breaker-guarded, D-11 pre-acquire + D-12 post-acquire)
  - web.api._fetch_fl_ids_network IIIF + MARC fallback (breaker-guarded, D-14 + D-15 timeouts)
  - web.api.nli_image endpoint (breaker-guarded, D-16, both IIIF and Rosetta branches)
  - web.api._fetch_nli_image_bytes (breaker-guarded, D-17 pre-loop + per-_try_fl recheck)
  - web.api.proxy_image (breaker-guarded for NLI hosts only, D-18; non-NLI hosts unchanged)
  - web.api._api_test_seam (closure exposure dict for integration tests)
affects: [98-04, 98-05, 98-06]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "circuit check BEFORE + AFTER semaphore acquire with try/finally release (D-11, D-12)"
    - "failure typing by HTTP status-code class (200/429/5xx) + exception class (Timeout/ConnectionError)"
    - "narrowed except Exception → except (Timeout, ConnectionError) per RESEARCH Pitfall 7"
    - "is_nli_host boolean to scope breaker calls to NLI domains in generic proxy"
    - "fallback-boundary breaker rechecks (MARC, Rosetta, per-_try_fl iteration) per Codex REVIEW Issue 3"
    - "AST-aware static guard for NLI timeout audit per Codex REVIEW Issue 4"
    - "closure-exposed test seam (_api_test_seam dict) to enable direct call of init_api_routes-scoped helpers"

key-files:
  created:
    - tests/test_api_nli_breaker_integration.py
  modified:
    - web/api.py

key-decisions:
  - "Used a module-level _api_test_seam dict populated inside init_api_routes() to expose closure-encapsulated helpers (fetch_fl_ids_from_nli, _fetch_nli_image_bytes) to integration tests, rather than refactoring those helpers out of init_api_routes() — keeps Plan 03's blast radius minimal."
  - "AST audit (test_no_hardcoded_timeout_in_nli_paths_ast) classifies NLI calls by URL host marker OR variable name OR _nli_session usage; narrowed _nli_session filter to HTTP-method attrs ('get','post',...) to avoid false-flagging _nli_session.mount(...) at module top."
  - "Per-test fixture (_clear_nli_in_memory_cache) reaches into the closure's __closure__/co_freevars to clear _nli_cache/_nli_cache_time between tests; module-scope fixture patches _save_nli_persistent_cache to a no-op so test sys_ids do not pollute Config.INDEX_DIR/nli_fl_ids_cache.json on disk."
  - "Non-NLI hosts in proxy_image (Cambridge, Manchester, Oxford) keep their existing 15s timeout because they have not exhibited the NLI threadpool-saturation pattern (T-98-03-07 mitigation)."
  - "proxy_image returns 503 (Service Unavailable) when the NLI breaker is open against an NLI host; returning a fake 200 would mislead clients."

requirements-completed: [D-10, D-11, D-12, D-13, D-14, D-15, D-16, D-17, D-18]
decisions-closed: [D-10, D-11, D-12, D-13, D-14, D-15, D-16, D-17, D-18]

# Metrics
duration: 16min
completed: 2026-05-25
---

# Phase 98 Plan 03: Wire NLI Circuit Breaker into web/api.py Summary

**Wires the shared NLI circuit breaker (Plan 98-02) into ALL 5 NLI-touching call sites in `web/api.py` with env-driven (connect, read) timeout tuples, fallback-boundary rechecks (Codex Issue 3), failure typing by status/exception class, semaphore-timeout default dropped 20→1, and an AST-aware integration test that statically pins the timeout invariant.**

## Performance

- **Duration:** ~16 min (3 task commits between 14:37 and 14:53 UTC)
- **Started:** 2026-05-25T14:37:17Z (after base reset to 646d3fa4)
- **Completed:** 2026-05-25T14:53:18Z
- **Tasks:** 3
- **Files created:** 1 (tests/test_api_nli_breaker_integration.py — 470 lines)
- **Files modified:** 1 (web/api.py — +192 LOC across imports, semaphore-default, 5 call sites, test seam)
- **Tests added:** 15 (3 test classes covering D-06, D-07, D-08, D-10, D-11, D-12, D-13, D-14..D-18)
- **Combined Wave 1+2+3 suite:** 62 tests pass in 3.73s
- **Existing API tests unaffected:** test_api_legacy_unchanged + test_api_export_json + test_browse_synthetic + test_crawler_visibility → 49 tests pass

## Accomplishments

- Single source of truth for NLI degradation now governs ALL 5 web/api.py NLI call sites — the 2026-05-25 hang cannot recur through any of `fetch_fl_ids_from_nli`, `nli_image`, `_fetch_nli_image_bytes`, or `proxy_image` (NLI hosts).
- D-11 pre-acquire circuit check + D-12 post-acquire defensive re-check with semaphore release in `finally` — proves via test that 20 saturating workers do not leak slots when the breaker flips mid-call (RESEARCH Pitfall 3 mitigated).
- D-10 semaphore-timeout default dropped from 20s → 1s. Worst-case blocking budget per NLI call now bounded at ~6s (3s connect + 3-5s read) regardless of degradation; the only path that ever blocks longer is the 1s semaphore wait under normal-traffic saturation.
- Codex REVIEW Issue 3 closed: breaker rechecks added at 3 fallback boundaries — MARC fallback in `_fetch_fl_ids_network`, Rosetta fallback in `nli_image`, per-`_try_fl` iteration in `_fetch_nli_image_bytes`. Without these rechecks, a breaker that trips on the first call of a multi-step path would still let the remaining steps burn timeouts.
- Codex REVIEW Issue 4 closed: `test_no_hardcoded_timeout_in_nli_paths_ast` parses web/api.py with `ast`, walks every `Call` node for `_nli_session.get/post` or `requests.get/post`, classifies each by URL host marker / variable name / session identity, and asserts NLI calls use `timeout=(NLI_CONNECT_TIMEOUT, NLI_*_READ_TIMEOUT)` tuples. Non-NLI calls (Cambridge / Manchester / Oxford `timeout=30`) are correctly exempted.
- Failure typing per D-06: explicit branches for 429 (rate-limited), 5xx (server error), `requests.exceptions.Timeout`, and `requests.exceptions.ConnectionError`. Each calls `_nli_record_failure(failure_type='...', path='...')` with the correct literal — 18 record_failure call sites across the 5 call sites and their fallback branches.
- D-07 preserved: 404 / empty-manifest paths populate the existing `_NLI_FAIL_SENTINEL` per-sys_id negative cache; NOT counted by the breaker (test `test_404_does_not_trip_breaker` pins this).
- D-08 preserved: every 200-success path calls `_nli_record_success(path='...')`; the unit + integration tests prove counter resets on first success.
- T-98-03-07 mitigation: `proxy_image` computes `is_nli_host` once and gates ALL breaker calls on it. Cambridge / Manchester / Oxford failures do NOT trip the NLI breaker even though they go through the same generic `/api/proxy_image` endpoint.

## Task Commits

1. **Task 1: Wire breaker into fetch_fl_ids_from_nli + drop semaphore timeout** — `5bef5a57` (feat)
   - Import block `from shared.nli_circuit_breaker import ...` (7 names)
   - `NLI_SEMAPHORE_TIMEOUT` default `'20'` → `'1'` (D-10)
   - D-11 pre-acquire `if _nli_circuit_is_open(): return []` before semaphore.acquire
   - D-12 post-acquire defensive re-check inside `try/finally` with `_nli_semaphore.release()`
   - D-14 IIIF: `timeout=(NLI_CONNECT_TIMEOUT, NLI_IIIF_READ_TIMEOUT)` + 200/429/5xx/Timeout/ConnectionError branches
   - D-15 MARC: `timeout=(NLI_CONNECT_TIMEOUT, NLI_MARC_READ_TIMEOUT)` + same failure-typing pattern
   - Codex Issue 3: `if suffix == 1 and not _nli_circuit_is_open():` recheck before MARC try block
   - Narrowed `except Exception` → `except (requests.exceptions.Timeout, requests.exceptions.ConnectionError)` per RESEARCH Pitfall 7
2. **Task 2: Wire breaker into nli_image + _fetch_nli_image_bytes + proxy_image** — `f66f04a8` (feat)
   - D-16 `nli_image` endpoint: top-of-handler `is_open()` check, both IIIF and Rosetta use `(NLI_CONNECT_TIMEOUT, NLI_IMAGE_READ_TIMEOUT)`, Codex Issue 3 recheck between IIIF and Rosetta
   - D-17 `_fetch_nli_image_bytes`: top-of-function `is_open()` check before FL-id iteration loop, inner `_try_fl` also rechecks (Codex Issue 3) so mid-loop trip short-circuits remaining FL ids
   - D-18 `proxy_image`: `is_nli_host = parsed.netloc in nli_hosts` computed once; `is_open()` check + `(NLI_CONNECT_TIMEOUT, NLI_IMAGE_READ_TIMEOUT)` + record_failure/success calls all gated on `is_nli_host`; non-NLI hosts keep `timeout=15`
   - 503 returned for proxy_image when NLI breaker is open against an NLI host (not 404 — explicit Service Unavailable)
3. **Task 3: Add tests/test_api_nli_breaker_integration.py + _api_test_seam** — `91ac0a85` (test)
   - 470-line test file: 15 tests across 3 classes
   - `_api_test_seam` dict in `web/api.py` populated inside `init_api_routes()` with closure-encapsulated helpers (production code never reads it)
   - Per-test fixture reaches into `fetch_fl_ids_from_nli.__closure__` to clear `_nli_cache` between tests
   - Module-scope fixture patches `_save_nli_persistent_cache` to no-op so test sys_ids never pollute `Config.INDEX_DIR/nli_fl_ids_cache.json`

## Verification Map

| Decision | Verification |
|----------|--------------|
| D-10 (semaphore default 20→1) | `test_nli_semaphore_timeout_default_is_one` static grep |
| D-11 (pre-acquire check) | `test_circuit_check_before_semaphore` — proves session.get is never called when breaker is open at entry; <0.5s wall time |
| D-12 (post-acquire re-check + slot release) | `test_circuit_check_after_semaphore_releases_slot` — patches is_open() to flip mid-call, then probes the semaphore for slot leak |
| D-13 (all 5 call sites guarded) | `test_circuit_check_count_at_least_8` — 5 entry checks + 3 fallback rechecks (Codex Issue 3) |
| D-14, D-15, D-16, D-17, D-18 (timeouts) | `test_no_hardcoded_timeout_in_nli_paths_ast` (AST-aware audit per Codex Issue 4) + `test_bounded_timeout_tuples_present` (grep sanity check) |
| D-06 (failure typing) | `test_5xx_response_trips_breaker`, `test_429_response_trips_breaker`, `test_timeout_exception_trips_breaker`, `test_connection_error_trips_breaker`, `test_record_failure_typed_correctly` |
| D-07 (404 does NOT trip) | `test_404_does_not_trip_breaker` |
| D-08 (200 resets counter) | `test_successful_200_resets_counter` |
| Pitfall 7 (narrow except) | `test_specific_exception_handlers_replace_broad_except` |

## Threat Model — Per Threat Disposition

| Threat ID | Disposition | Verification |
|-----------|-------------|--------------|
| T-98-03-01 (DoS via threadpool exhaustion) | mitigate | D-11+D-12+D-14..D-18 all wired; Nyquist test (Plan 02 covers; this plan's D-11 test confirms <0.5s short-circuit) |
| T-98-03-02 (Semaphore slot leak on D-12 path) | mitigate | `test_circuit_check_after_semaphore_releases_slot` probes for leak |
| T-98-03-03 (SSRF via /api/proxy_image) | accept | `ALLOWED_IMAGE_DOMAINS` allowlist preserved unchanged (line ~2119) |
| T-98-03-04 (Info disclosure via telemetry properties) | mitigate | `path='...'` strings are static literals; no user input enters property dict |
| T-98-03-05 (env-var injection NLI_SEMAPHORE_TIMEOUT='-1') | mitigate | int cast + Semaphore.acquire(timeout=N<0) semantically equivalent to non-blocking |
| T-98-03-06 (silent failure-type miscount) | mitigate | Explicit branches per status class + pinned by tests |
| T-98-03-07 (non-NLI host trips NLI breaker via /api/proxy_image) | mitigate | `is_nli_host` boolean gates all breaker calls in proxy_image |

## Deviations from Plan

### Rule 3 (blocking issue, auto-fixed)

**1. [Rule 3 - Blocking] `fetch_fl_ids_from_nli` is closure-encapsulated, not module-level**

- **Found during:** Task 3 test authoring
- **Issue:** The plan's integration tests call `web.api.fetch_fl_ids_from_nli` as if it were a module-level function. In reality the function is defined inside `init_api_routes()` and never bound to the module namespace — calling `api_mod.fetch_fl_ids_from_nli(...)` raises `AttributeError`.
- **Fix:** Added a module-level `_api_test_seam: dict` in `web/api.py`. Inside `init_api_routes()` (after `_fetch_nli_image_bytes` is defined), the seam is populated with references to `fetch_fl_ids_from_nli` and `_fetch_nli_image_bytes`. Production code never reads this dict. Tests reach the breaker-guarded fetch path through a `_api_fetch_fl_ids_from_nli` helper that resolves through the seam.
- **Alternative considered:** Refactor `fetch_fl_ids_from_nli` out of `init_api_routes` to module level. Rejected because the function captures 6 closure variables (`_NLI_FAIL_SENTINEL`, `_nli_cache`, `_nli_cache_lock`, `_nli_cache_time`, `_prune_nli_memory_cache_locked`, `_fetch_fl_ids_network`) that would also need to move, expanding the blast radius substantially beyond Plan 98-03's scope.
- **Files modified:** web/api.py (+8 LOC for the seam + populate calls)
- **Commit:** `91ac0a85`

**2. [Rule 3 - Blocking] Disk + in-memory cache pollution between tests**

- **Found during:** Task 3, after first successful test run
- **Issue:** A 200-success test path caused `_persist_positive_cache_snapshot()` to write `sysid_recovery_2 → ['12345']` to `Config.INDEX_DIR/nli_fl_ids_cache.json`. Subsequent test runs satisfied lookups for `sysid_recovery_2` from disk WITHOUT calling the mocked `_nli_session.get`, so `record_success` was never invoked and `consecutive_failures` never reset.
- **Fix:** Two-layer cache isolation in test fixtures —
  - Module-scope fixture patches `web.api._save_nli_persistent_cache` to a no-op so test sys_ids never hit disk.
  - Per-test autouse fixture reaches into `fetch_fl_ids_from_nli.__closure__` to clear `_nli_cache` and `_nli_cache_time` between tests (via `co_freevars`).
- **Files modified:** tests/test_api_nli_breaker_integration.py (fixtures)
- **Commit:** `91ac0a85`

### Rule 1 (bug, auto-fixed)

**3. [Rule 1 - Bug] AST audit false-flagged `_nli_session.mount(...)`**

- **Found during:** Task 3 first test run of `test_no_hardcoded_timeout_in_nli_paths_ast`
- **Issue:** The AST audit treated any `_nli_session.*` call as an NLI network call, including the session-configuration call `_nli_session.mount('https://iiif.nli.org.il', _nli_adapter)` at the top of `web/api.py`. `.mount()` does not take a `timeout` kwarg, so the audit fired a violation.
- **Fix:** Narrowed `_is_nli_session_call` to only classify calls whose attribute is one of the HTTP method names (`get`, `post`, `put`, `delete`, `patch`, `head`, `options`, `request`) as network calls.
- **Files modified:** tests/test_api_nli_breaker_integration.py (AST helper)
- **Commit:** `91ac0a85`

### Rule 1 (bug, auto-fixed)

**4. [Rule 1 - Bug] Plan's recovery test assertion was inconsistent with MARC fallback behavior**

- **Found during:** Task 3, after running `test_successful_200_resets_counter`
- **Issue:** The plan's test expected `consecutive_failures == 2` after 2 fetch calls with simulated timeouts. But each `fetch_fl_ids_from_nli` call hits IIIF AND its MARC fallback, so one failing call increments the counter by 2. After 2 fully-failing calls the counter would be 4, not 2. The plan's exact-count assertion was unsound.
- **Fix:** Rewrote the test to assert "at least 1 failure" after first call, then "counter == 0" after the second (200) call. This still pins D-08 (success resets counter) without overspecifying.
- **Files modified:** tests/test_api_nli_breaker_integration.py
- **Commit:** `91ac0a85`

## Acceptance Criteria — Status

- [x] `from shared.nli_circuit_breaker import` present (exactly 1 import block in web/api.py)
- [x] All 7 imported names referenced ≥10 times in source
- [x] `NLI_SEMAPHORE_TIMEOUT', '1'` present; `NLI_SEMAPHORE_TIMEOUT', '20'` absent
- [x] `_nli_circuit_is_open()` count ≥8 (5 entry checks + 3 fallback rechecks per Codex Issue 3) — got 8
- [x] `timeout=(NLI_CONNECT_TIMEOUT, NLI_IIIF_READ_TIMEOUT)` ≥1 match — got 1
- [x] `timeout=(NLI_CONNECT_TIMEOUT, NLI_MARC_READ_TIMEOUT)` ≥1 match — got 1
- [x] `timeout=(NLI_CONNECT_TIMEOUT, NLI_IMAGE_READ_TIMEOUT)` ≥4 matches — got 4 (nli_image IIIF + nli_image Rosetta + _try_fl + proxy_image conditional)
- [x] `_nli_record_failure(failure_type='5xx', ...)` ≥2 matches — got 6
- [x] `_nli_record_failure(failure_type='429', ...)` ≥2 matches — got 6
- [x] `_nli_record_failure(failure_type='timeout', ...)` ≥2 matches — got 4
- [x] `_nli_record_failure(failure_type='connection_error', ...)` ≥2 matches — got 4
- [x] `_nli_record_success(path='fetch_fl_ids_from_nli')` ≥2 matches (IIIF + MARC) — got 2
- [x] `_nli_record_success(path='nli_image')` ≥2 matches — got 2
- [x] `_nli_record_success(path='_fetch_nli_image_bytes')` =1 match — got 1
- [x] `_nli_record_success(path='proxy_image')` =1 match — got 1
- [x] `except (requests.exceptions.Timeout, requests.exceptions.ConnectionError)` ≥2 matches — got 2
- [x] `python -c "import web.api"` exits 0
- [x] `python -m py_compile web/api.py` exits 0
- [x] tests/test_api_nli_breaker_integration.py: ≥3 test classes (got 3); ≥12 test methods (got 15)
- [x] D-11, D-12, D-07, 5xx tests exist by name
- [x] `pytest tests/test_api_nli_breaker_integration.py` → 0 failures (15 passed)
- [x] `pytest tests/test_nli_circuit_breaker.py tests/test_posthog_server.py tests/test_api_nli_breaker_integration.py -x` → 0 failures (62 passed in 3.73s)

## Self-Check: PASSED

All 3 task commits exist:

- 5bef5a57 — feat(98-03): wire NLI breaker into fetch_fl_ids_from_nli + drop semaphore timeout
- f66f04a8 — feat(98-03): wire NLI breaker into nli_image + _fetch_nli_image_bytes + proxy_image
- 91ac0a85 — test(98-03): add tests/test_api_nli_breaker_integration.py + _api_test_seam

All claimed files exist:

- web/api.py (FOUND, modified)
- tests/test_api_nli_breaker_integration.py (FOUND, 470 lines)
- .planning/phases/98-nli-resilience-circuit-breaker-and-bounded-timeout-hardening/98-03-SUMMARY.md (this file)
