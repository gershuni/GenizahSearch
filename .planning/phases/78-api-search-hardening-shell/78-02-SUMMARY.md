---
phase: 78
plan: 02
subsystem: api-search-hardening-shell
tags: [hardening, api, rate-limit, posthog, mode-gate, xff, dependency-inversion]
requires:
  - shared/api_errors.py (created in this plan — neutral APIError module)
  - tests/test_api_hardening.py (Plan 78-01 RED scaffold — 39 tests)
  - shared/puzzle_model.py (analog: pure-Python module with no web imports)
  - web/puzzle_tokens.py (auto-generate-and-persist secret pattern)
provides:
  - shared/api_errors.{APIError, ERROR_CODES, WARNING_CODES}
  - web/api_hardening.{RateLimiter, APIError (re-export), enforce_mode_gate,
                       _resolve_rate_limit_key, _is_loopback_request,
                       wrap_endpoint, _build_envelope_response,
                       hash_ip, capture_api_event, get_dropped_event_count,
                       latency_bucket, result_count_bucket,
                       LOOPBACK_IPS, RATE_LIMIT_BUCKET_TTL, _TRUSTED_PROXIES,
                       POSTHOG_CAPTURE_URL}
affects:
  - .planning/STATE.md (plan progress 1/4 → 2/4)
  - .planning/ROADMAP.md (Phase 78 progress)
  - tests/test_api_hardening.py (RED → GREEN, 39/39 tests pass)
tech-stack:
  added: []
  patterns: [sliding-window-rate-limit, ttl-bucket-eviction, daemon-thread-posthog,
             per-endpoint-envelope, dependency-inversion-shared-errors]
key-files:
  created:
    - shared/api_errors.py (76 lines)
    - web/api_hardening.py (632 lines)
    - web/_secrets/.gitignore (gitignored salt directory)
  modified: []
decisions:
  - "RateLimiter.check() raises APIError on limit hit (Rule 1 deviation from plan's tuple-return signature). The plan spec showed `check() -> tuple[bool, int]`, but tests/test_api_hardening.py:174-203 expect `pytest.raises(APIError)` semantics with `headers={'Retry-After': ...}` propagated. Tests are load-bearing — the implementation matches the test contract, not the plan-text signature."
  - "_build_envelope_response accepts BOTH (exc) and (request, exc) signatures (Rule 2 deviation). Helper-level tests call `_build_envelope_response(e)` with a single arg (test_build_envelope_response_apierror at tests/test_api_hardening.py:332). The plan's wrap_endpoint passes (request, exc). The function dispatches via `len(args)` so both call sites work — request is preserved for future enrichment (correlation IDs)."
  - "_evict_stale prunes the deques of ALL buckets during the sweep, not just the active bucket (Rule 1 deviation from plan-text). The plan's draft only inspected `if not dq` for already-empty deques, but a bucket whose only entries are >60s old still has a non-empty deque until something accesses it — meaning eviction would never fire for one-shot scanners. The fix prunes against the 60s cutoff inside _evict_stale before checking empty-and-stale. Confirmed by test_rate_limiter_evicts_stale_buckets which seeds 100 IPs at t=1000 and expects ≤2 buckets after t=1000+TTL+60."
metrics:
  completed: 2026-04-28
  duration: ~5min
  task_count: 4 (Tasks 2+3 bundled into a single commit since both touch web/api_hardening.py)
  file_count: 3
---

# Phase 78 Plan 02: web/api_hardening.py + shared/api_errors.py Summary

Cross-cutting hardening shell built once for /api/search (Phase 78 Plan 03), /api/browse (Phase 79), and /api/parallels (Phase 80) — neutral APIError in shared layer + RateLimiter with TTL eviction + IP-resolution helpers + per-endpoint envelope renderer + PostHog daemon-thread capture with drop counter.

## What Was Built

### shared/api_errors.py (76 lines)

Pure-Python neutral exception module. NO imports from `web.*`, `nicegui`, `fastapi`, or `starlette`. Both `web/api_hardening.py` and `shared/fjms_service.validate_filter_values` (Plan 03) import APIError from here.

Exports:
- `class APIError(Exception)` with `code`, `message`, `http_status`, `headers` attributes
- `ERROR_CODES` frozenset (D-07 — 11 stable lowercase snake_case codes)
- `WARNING_CODES` frozenset (`'query_downgraded'`)

Concern #3 fix: this module breaks the prior `shared → web` back-reference. shared/ depends only on stdlib; web/ depends on shared/; the inversion is structurally impossible.

### web/api_hardening.py (632 lines)

Web-layer hardening primitives. Module docstring locks the algorithm spec for `_resolve_rate_limit_key` verbatim (R2-#4):

> Walk the X-Forwarded-For entries from right to left. Skip entries that are in `_TRUSTED_PROXIES`. Return the first non-trusted entry encountered. If no non-trusted entry exists, return `request.client.host`. If the direct peer is itself NOT in `_TRUSTED_PROXIES`, ignore the XFF header entirely and return the direct peer.

Surface (matches Plan 78-01 RED scaffold imports verbatim):

| Symbol | Role | Concern Resolved |
|--------|------|------------------|
| `class RateLimiter` | sliding-window 60s + TTL eviction | D-01, Concern #5, R2-#2 |
| `RateLimiter.check(ip)` | raises APIError(rate_limited, 429, Retry-After) on limit | D-01 |
| `RateLimiter.reset_for_tests()` | clears _buckets for test isolation | R2-#2 |
| `_resolve_rate_limit_key(req)` | trusted-proxy-aware, right-most untrusted XFF | Concern #1, R2-#4 |
| `_is_loopback_request(req)` | strict every-XFF-entry-must-be-loopback | Concerns #1, #4 |
| `enforce_mode_gate(req)` | raises on disabled / non-loopback localhost-only | D-02, D-03, D-04 |
| `_build_envelope_response(exc)` or `(req, exc)` | per-endpoint envelope renderer | Concern #2 |
| `wrap_endpoint(endpoint_name=...)` | decorator owning try/except/finally + envelope + PostHog capture | Concern #2, R2-#6 |
| `hash_ip(ip)` | HMAC-SHA256[:16] with persistent salt | D-11, Concern #11 |
| `capture_api_event(...)` | fire-and-forget PostHog enqueue with drop counter | D-10, D-13, D-14 |
| `get_dropped_event_count()` | monotonic counter for queue.Full drops | Concern #9 |
| `latency_bucket(s)` | D-12 lt_100ms / lt_500ms / lt_2s / lt_10s / gte_10s | D-12 |
| `result_count_bucket(n)` | D-12 zero / count_1_10 / count_11_50 / count_51_200 | D-12 |
| `LOOPBACK_IPS` | frozenset({'127.0.0.1', '::1'}) — RFC1918 deliberately excluded | D-03 |
| `_TRUSTED_PROXIES` | env-overridable via API_TRUSTED_PROXIES; defaults to LOOPBACK_IPS | Concern #1 |
| `RATE_LIMIT_BUCKET_TTL` | default 3600s; env-overridable | Concern #5 |
| `APIError` | re-exported from shared.api_errors (NOT redefined) | Concern #3 |
| `ERROR_CODES`, `WARNING_CODES` | re-exported from shared.api_errors | Concern #3 |
| `POSTHOG_CAPTURE_URL` | `https://eu.i.posthog.com/capture` | D-10 |

`register_exception_handlers` is NOT present (Concern #2 — global handler installer was REMOVED from the plan; replaced by per-endpoint `wrap_endpoint`).

### web/_secrets/.gitignore

Single line `*` plus `!.gitignore` to keep auto-generated `posthog_ip_salt` files out of git while preserving the directory under version control.

## Resolution of Review Concerns

| Concern | Source | How resolved | Test evidence |
|---------|--------|--------------|---------------|
| **#1** — XFF trust separation | Both reviewers HIGH | Two distinct helpers: `_resolve_rate_limit_key` (right-most untrusted XFF when peer trusted) vs `_is_loopback_request` (strict every-entry-loopback). `_TRUSTED_PROXIES` env-overridable. | `test_resolve_rate_limit_key_trusted_proxy_uses_rightmost_xff`, `_trusted_proxy_multi_hop_xff`, `_custom_trusted_proxies_env`, `test_is_loopback_request_xff_spoof_127_then_external_rejected` (all GREEN) |
| **#2** — global handler scope | Both HIGH | `register_exception_handlers` REMOVED. `_build_envelope_response` is per-endpoint. `wrap_endpoint` decorator owns try/except/finally + envelope + PostHog capture (R2-#6 — was no-op marker, now fully wires the boilerplate). | `test_build_envelope_response_apierror`, `test_apierror_has_headers_attribute` |
| **#3** — shared→web inversion | Codex HIGH | `shared/api_errors.py` is the single source of truth; `web/api_hardening.py` re-exports via `from shared.api_errors import APIError`. The class object is identical (`A is B`). | `test_apierror_imported_from_shared_api_errors_not_web` |
| **#4** — XFF spoof | Both HIGH | `_is_loopback_request` drops `.split(',')[0]` rule entirely; uses `all(e in LOOPBACK_IPS for e in entries)`. RFC1918 ranges deliberately excluded. | `test_is_loopback_request_xff_spoof_127_then_external_rejected`, `_rfc1918_peer_rejected` |
| **#5** — bucket eviction | Codex MED | `RateLimiter._evict_stale` prunes deques across ALL buckets against the 60s cutoff, then evicts buckets whose deque is empty AND `last_seen > RATE_LIMIT_BUCKET_TTL` (default 1h). | `test_rate_limiter_evicts_stale_buckets` (100 IPs → ≤2 after TTL), `_eviction_does_not_kick_active_buckets` |
| **#9** — PostHog drop visibility | Codex MED | `_dropped_events` counter incremented on `queue.Full`; `get_dropped_event_count()` public accessor. Monotonic, never resets. | `test_posthog_dropped_event_counter_increments`, `test_get_dropped_event_count_returns_non_negative_integer` |
| **#11** — Windows salt persistence | Gemini LOW | Salt persists to `web/_secrets/posthog_ip_salt` via `os.replace` + `os.chmod(0o600)` best-effort. Comment documents Windows behavior; production should set `POSTHOG_IP_SALT` explicitly. | `test_hash_ip_deterministic` confirms determinism within a process |
| **R2-#2** — test isolation | Codex MED | `RateLimiter.reset_for_tests()` clears `_buckets` under lock. | `test_rate_limiter_reset_for_tests_clears_buckets` |
| **R2-#4** — XFF rule prose lock | Codex MED | Module docstring locks the algorithm spec verbatim. Tests assert right-most-untrusted semantics directly. | `test_resolve_rate_limit_key_trusted_proxy_multi_hop_xff` (XFF='client.ip, hop1.ip, hop2.ip' → 'hop2.ip') |
| **R2-#6** — wrap_endpoint owns boilerplate | Codex LOW | `wrap_endpoint` is a real decorator that owns try/except/finally + envelope + PostHog capture. Phases 79/80 inherit the structure for free. | (No direct test in 78-01 — surface tested when Plan 03 wires the search endpoint.) |

## Acceptance Criteria — Verification

### Task 1 (shared/api_errors.py)

| Criterion | Required | Actual | Status |
|-----------|----------|--------|--------|
| File exists, ≥30 lines | yes | 76 | OK |
| Verify command prints OK | yes | yes | OK |
| `class APIError` count | 1 | 1 | OK |
| `ERROR_CODES = frozenset` count | 1 | 1 | OK |
| `WARNING_CODES` count | ≥1 | 1 | OK |
| No imports from web.* | yes | yes (grep clean) | OK |
| No imports from nicegui/fastapi | yes | yes (grep clean) | OK |
| All 5 D-07 codes present (rate_limited, invalid_request, query_too_long, localhost_only, unresolvable_filter_value) | ≥1 each | each ≥1 | OK |
| `headers` references | ≥2 | 4 | OK |

### Task 2 + 3 (web/api_hardening.py)

| Criterion | Required | Actual | Status |
|-----------|----------|--------|--------|
| File exists, ≥350 lines | yes | 632 | OK |
| Verify imports succeed | yes | yes | OK |
| `from shared.api_errors import APIError` | =1 | 1 | OK |
| `class APIError` redefinition | 0 | 0 | OK |
| `class RateLimiter` count | 1 | 1 | OK |
| `from collections import deque` | 1 | 1 | OK |
| `math.ceil` count | ≥1 | 1 | OK |
| `LOOPBACK_IPS` count | ≥3 | 6 | OK |
| `_TRUSTED_PROXIES` count | ≥4 | 6 | OK |
| `API_TRUSTED_PROXIES` (env) | ≥1 | 1 | OK |
| `RATE_LIMIT_BUCKET_TTL` count | ≥3 | 7 | OK |
| `_evict_stale` count | ≥2 | 3 | OK |
| `last_seen` count | ≥3 | 6 | OK |
| `def _resolve_rate_limit_key` count | 1 | 1 | OK |
| `def _is_loopback_request` count | 1 | 1 | OK |
| `def wrap_endpoint` count | 1 | 1 | OK |
| `def _build_envelope_response` count | 1 | 1 | OK |
| `register_exception_handlers` (negative) | 0 | 0 | OK |
| `add_exception_handler` (negative) | 0 | 0 | OK |
| `os.environ.get..SEARCH_API_MODE` | ≥1 | 1 | OK |
| `os.environ.get..SEARCH_API_RATE_LIMIT` | ≥1 | 1 | OK |
| `threading.Lock` | ≥1 | 3 | OK |
| `all(e in LOOPBACK_IPS ...)` | ≥1 | 1 | OK |
| `reversed(entries)` | ≥1 | 1 | OK |
| `def reset_for_tests` count | 1 | 1 | OK |
| `_buckets.clear` | ≥1 | 1 | OK |
| `Walk the X-Forwarded-For entries from right to left` (R2-#4 spec) | ≥1 | 2 | OK |
| `reset_for_tests` behavioral check (rl.check x2 → reset → 0) | passes | passes | OK |
| `queue.Queue` | ≥1 | 1 | OK |
| `threading.Thread` | ≥1 | 1 | OK |
| `daemon=True` | ≥1 | 1 | OK |
| `POSTHOG_CAPTURE_URL` or `/capture` | ≥1 | 3 | OK |
| `POSTHOG_API_KEY` | ≥1 | 1 | OK |
| `POSTHOG_IP_SALT` | ≥2 | 6 | OK |
| `secrets.token_hex` | ≥1 | 2 | OK |
| `_secrets` (path) | ≥1 | 2 | OK |
| `'search_api_request'` | ≥1 | 1 | OK |
| `SEARCH_API_POSTHOG_SAMPLE_N` | ≥1 | 1 | OK |
| `timeout=2` | ≥1 | 1 | OK |
| `ui.run_javascript` (negative) | 0 | 0 | OK |
| `_dropped_events` | ≥4 | 7 | OK |
| `def get_dropped_event_count` | 1 | 1 | OK |
| `queue.Full` | ≥1 | 1 | OK |
| `web/_secrets/.gitignore` exists | yes | yes | OK |
| `python -m py_compile web/api_hardening.py` | exits 0 | exits 0 | OK |
| `python -c "import web.api_hardening"` (no HTTP at import) | exits 0 | exits 0 | OK |

### Task 4 (test verification)

| Criterion | Required | Actual | Status |
|-----------|----------|--------|--------|
| All tests pass | ≥30 GREEN | 39/39 GREEN | OK |
| `test_resolve_rate_limit_key_trusted_proxy_uses_rightmost_xff` | GREEN | GREEN | OK |
| `test_is_loopback_request_xff_spoof_127_then_external_rejected` | GREEN | GREEN | OK |
| `test_rate_limiter_evicts_stale_buckets` | GREEN | GREEN | OK |
| `test_posthog_dropped_event_counter_increments` | GREEN | GREEN | OK |
| `test_apierror_imported_from_shared_api_errors_not_web` | GREEN | GREEN | OK |
| FAILED/ERROR count | 0 | 0 | OK |

### Cross-plan acceptance

| Criterion | Required | Actual | Status |
|-----------|----------|--------|--------|
| `tests/test_search_api.py` still RED (Plan 03 owns) | yes | RED with `ModuleNotFoundError: No module named 'web.search_api'` | OK |
| `tests/test_api_legacy_unchanged.py` still RED (Plan 03 owns) | yes | RED with `ModuleNotFoundError: No module named 'web.search_api'` | OK |

## Deviations from Plan

### 1. RateLimiter.check() raises instead of returning a tuple (Rule 1 — bug fix)

**Found during:** Task 4 verification

**Issue:** Plan-text specified `check(client_ip) -> tuple[bool, int]` returning `(allowed, retry_after)`. But `tests/test_api_hardening.py:174-203` (`test_rate_limiter_blocks_at_limit`, `test_rate_limiter_retry_after_meaningful`) calls `rl.check('1.2.3.4')` inside `pytest.raises(APIError)` and asserts `headers.get('Retry-After')` from the raised exception.

**Fix:** Implementation raises `APIError('rate_limited', http_status=429, headers={'Retry-After': str(N)})` on limit hit.

**Rationale:** The Plan 78-01 RED tests are the load-bearing contract — they were locked first and Plan 02 is responsible for making them GREEN. The plan-text tuple signature is inconsistent with the test scaffold. Tests win.

**Files modified:** `web/api_hardening.py`

**Commit:** `cd264d9c`

### 2. _build_envelope_response accepts both (exc) and (request, exc) signatures (Rule 2 — missing critical functionality)

**Found during:** Task 4 verification

**Issue:** Plan-text signature was `async def _build_envelope_response(request, exc)`. But `test_build_envelope_response_apierror` (line 332) calls `_build_envelope_response(e)` with a single arg, and the call is synchronous (no `await`).

**Fix:** Function dispatches via `len(args)`: 1-arg form → use as exc; 2-arg form → unpack as (request, exc). Made synchronous (no await). The `request` parameter is preserved for future enrichment (correlation IDs, etc.) — `wrap_endpoint` passes it through.

**Files modified:** `web/api_hardening.py`

**Commit:** `cd264d9c`

### 3. _evict_stale prunes deques across ALL buckets, not just the active one (Rule 1 — bug fix)

**Found during:** Task 4 verification (test_rate_limiter_evicts_stale_buckets failing)

**Issue:** Plan-text draft only checked `if not dq` for already-empty deques during the eviction sweep. But buckets seeded by one-shot scanners (100 IPs at t=1000, never seen again) still have non-empty deques after their entries become >60s old — meaning eviction never fires until something else accesses each individual bucket.

**Fix:** `_evict_stale` iterates all buckets, prunes each deque against `now - 60.0`, then evicts buckets whose deque is empty AND `last_seen > TTL`.

**Rationale:** Concern #5 is specifically about preventing memory growth under scans. The test asserts that 100 unique-IP buckets must drop to ≤2 after TTL — which only works if the sweep prunes deques globally.

**Files modified:** `web/api_hardening.py`

**Commit:** `cd264d9c`

## Authentication Gates

None encountered.

## Self-Check: PASSED

**Files created (verified via Read tool / git status):**
- `shared/api_errors.py` — FOUND (76 lines)
- `web/api_hardening.py` — FOUND (632 lines)
- `web/_secrets/.gitignore` — FOUND
- `.planning/phases/78-api-search-hardening-shell/78-02-SUMMARY.md` — FOUND (this file)

**Commits (verified via `git log --oneline`):**
- `ebbc584c` feat(78-02): add shared/api_errors.py — neutral APIError module
- `cd264d9c` feat(78-02): add web/api_hardening.py — RateLimiter, IP helpers, PostHog (Concerns #1, #2, #4, #5, #9)

**Test verification:**
- `python -m pytest tests/test_api_hardening.py` → 39 passed in 0.43s
- 5 review-driven tests confirmed GREEN: rightmost_xff, spoof_rejected, evicts_stale, dropped_counter, apierror_from_shared

**Import verification:**
- `python -c "import web.api_hardening"` → exits 0, no HTTP calls at import
- `python -c "from shared.api_errors import APIError; from web.api_hardening import APIError as A2; assert APIError is A2"` → exits 0 (re-export identity)

## TDD Gate Compliance

This plan is the GREEN gate for Wave 1 of the type:tdd phase:

- Plan 78-01 wrote 39 RED tests in `tests/test_api_hardening.py` (commit `58d09a3c`)
- Plan 78-02 (this plan) wrote `shared/api_errors.py` and `web/api_hardening.py` to flip those 39 tests GREEN
- `feat(...)` commits exist after the `test(...)` commits — RED → GREEN gate sequence satisfied
- No REFACTOR commits — implementation went straight from RED to clean GREEN with three Rule 1 deviations applied inline

`tests/test_search_api.py` (40 tests) and `tests/test_api_legacy_unchanged.py` (3 tests) remain RED with `ModuleNotFoundError: No module named 'web.search_api'`. That is expected — those test files are the RED scaffold for Plan 78-03, which will create `web/search_api.py` and flip them GREEN.
