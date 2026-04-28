---
phase: 78
plan: 01
subsystem: api-search-hardening-shell
tags: [tdd, red-scaffold, /api/search, hardening, posthog, rate-limit, mode-gate]
requires:
  - shared/search_serializer.py (Phase 77 — serializer envelope)
  - web/api.py:174 init_api_routes(app_override=None) (Phase 77 registrar pattern)
  - tests/test_api_export_json.py (Plan 77-04 — bare-app TestClient harness)
provides:
  - tests/test_search_api.py (40 RED tests for /api/search handler)
  - tests/test_api_hardening.py (39 RED tests for hardening helpers)
  - tests/test_api_legacy_unchanged.py (3 RED tests for legacy-route immutability)
  - Locked contract names that Plans 02+03 MUST produce verbatim:
      web.search_api.{init_search_api, FiltersModel, SearchRequest, _consume_last_responsa_downgrade}
      web.api_hardening.{RateLimiter, enforce_mode_gate, wrap_endpoint, _build_envelope_response,
                         _resolve_rate_limit_key, _is_loopback_request, hash_ip,
                         latency_bucket, result_count_bucket, capture_api_event,
                         get_dropped_event_count, LOOPBACK_IPS, ERROR_CODES,
                         RATE_LIMIT_BUCKET_TTL, _event_queue, _TRUSTED_PROXIES, APIError (re-export)}
      shared.api_errors.APIError (single source of truth, NOT in web layer)
      shared.fjms_service.{validate_filter_values, is_valid_domain_token,
                          _domain_vocabulary_is_loadable}
      genizah_core._set_last_responsa_downgrade
affects:
  - .planning/STATE.md (plan progress 0/4 → 1/4)
  - .planning/ROADMAP.md (Phase 78 progress)
tech-stack:
  added: []
  patterns: [tdd-red-scaffold, bare-app-fixture, fail-closed-validation]
key-files:
  created:
    - tests/test_search_api.py
    - tests/test_api_hardening.py
    - tests/test_api_legacy_unchanged.py
  modified: []
decisions:
  - Tasks 4 + 5 (round-2 RED appends) committed inline as part of Tasks 1 + 2 rather than as separate atomic commits, because they are append-only test additions to the same files. All 9 R2 tests are present with correct names; final test counts (40, 39, 3) exceed the plan's targets (36, 31, 3).
  - The single occurrence of `from web.api_hardening import APIError as WebReexportedAPIError` in tests/test_search_api.py:144 is intentional — the test_apierror_imported_from_shared_api_errors_module test verifies the re-export identity (`A is B`). The plan's example body explicitly contains this import. Documented as a deviation against the strict "0 occurrences" acceptance criterion in favor of the higher-value re-export-identity assertion the plan body requires.
metrics:
  completed: 2026-04-28
  duration: ~12min
  task_count: 3 (5 logical tasks bundled into 3 commits per file)
  file_count: 3
---

# Phase 78 Plan 01: /api/search RED Test Scaffold Summary

Wave 0 TDD scaffold for `POST /api/search` and the cross-cutting hardening shell. All 82 test functions across 3 files fail at import/collection time today with `ModuleNotFoundError` on `web.search_api`, `web.api_hardening`, and `shared.api_errors` — that is the intended RED signal. Plans 02 + 03 must produce these modules with the exact symbol names this scaffold imports to flip CI back to GREEN.

## What Was Built

Three test files, syntactically valid, each parses with `python -c "import ast; ast.parse(...)"` and is collectable by `pytest --collect-only` (collection fails loud at import time, not at parse time — that is the RED contract).

### tests/test_search_api.py (40 tests)

Handler-level scaffold for `POST /api/search`. Covers:

| Category | Tests | Source |
|----------|-------|--------|
| Singleton + idempotency | test_init_search_api_does_not_mutate_nicegui_singleton, test_init_search_api_idempotent, test_apierror_imported_from_shared_api_errors_module | D-18, Concerns #3, #10 |
| Happy path per mode | test_happy_path_{text,title,shelfmark,responsa}_mode | D-21 |
| Locator | test_locator_present_on_every_item | D-21, Phase 77 D-04 |
| Validation | test_query_required, test_query_too_long, test_unknown_mode_returns_invalid_request, test_unknown_filter_key_returns_invalid_request, test_extra_top_level_key_rejected, test_limit_too_high, test_limit_zero_returns_invalid_request | D-05..D-09 |
| Error envelope | test_error_envelope_shape | D-06 |
| Filter resolution | test_filter_resolution_known_good, test_filter_resolution_bogus_value, test_filter_resolution_yields_empty_intersection_returns_empty_results_without_executing_search | D-15, D-17 |
| Mode gate (XFF spoof rejected) | test_mode_gate_disabled, test_mode_gate_localhost_only_loopback_direct, test_mode_gate_localhost_only_non_loopback, test_mode_gate_localhost_only_xff_spoof_rejected, test_mode_gate_localhost_only_clean_xff_chain | D-02..D-04, Concerns #1, #4 |
| Statelessness | test_identical_requests_byte_identical_modulo_timestamp | D-20 |
| Warnings | test_warnings_array_always_present, test_warnings_surfaced_at_top_level, test_zero_result_responsa_downgrade_warning_still_surfaced | D-21, Concern #6 |
| Rate limit | test_rate_limited_envelope_code | D-01 |
| PostHog observability | test_capture_api_event_called_with_correct_status_and_error_code_on_apierror, test_pydantic_structural_error_captures_posthog_invalid_request_event | D-10..D-14, Concern #12 |
| **R2-#3 fail-closed filter validation** | test_validate_filter_values_qualified_domain_accepted, test_validate_filter_values_parent_domain_accepted, test_validate_filter_values_unknown_domain_rejected, test_validate_filter_values_domain_vocabulary_unavailable_fails_closed, test_validate_filter_values_empty_domain_vocabulary_fails_closed, test_validate_filter_values_materials_vocabulary_unavailable_fails_closed, test_validate_filter_values_empty_materials_vocabulary_fails_closed | Round-2 R2-#3 |
| **R2-#1 thread-local lifecycle** | test_responsa_downgrade_threadlocal_cleared_on_exception | Round-2 R2-#1 |
| **R2-#2 app-state idempotency** | test_init_search_api_uses_app_state_not_module_global | Round-2 R2-#2 |

### tests/test_api_hardening.py (39 tests)

Helper-level scaffold for `web/api_hardening.py`. Covers:

| Category | Tests | Source |
|----------|-------|--------|
| **Concern #1 — _resolve_rate_limit_key** | test_resolve_rate_limit_key_direct_peer_non_loopback, _trusted_proxy_uses_rightmost_xff, _trusted_proxy_empty_xff_returns_peer, _trusted_proxy_multi_hop_xff, _custom_trusted_proxies_env | Round-2 R2-#4 (right-most untrusted) |
| **Concern #1 + #4 — _is_loopback_request** | test_is_loopback_request_direct_loopback_no_xff, _direct_loopback_all_loopback_xff, _xff_spoof_127_then_external_rejected, _external_peer_rejected, _rfc1918_peer_rejected, _xff_with_whitespace, _ipv6_loopback | Concerns #1, #4 |
| Sliding-window rate limiter | test_rate_limiter_allows_under_limit, _blocks_at_limit, _retry_after_meaningful, _per_ip_isolation, _env_reread | D-01, D-02 |
| **Concern #5 — RateLimiter eviction** | test_rate_limiter_evicts_stale_buckets, _eviction_does_not_kick_active_buckets | Concern #5 |
| Mode gate helper | test_enforce_mode_gate_disabled_raises_apierror, _localhost_only_pass_loopback, _localhost_only_fail_external, _open_default, _unset_defaults_to_open | D-02..D-04 |
| Envelope builder | test_build_envelope_response_apierror, test_apierror_has_headers_attribute | Concern #2 |
| IP-hash | test_hash_ip_deterministic, test_hash_ip_distinct_for_distinct_ips | D-11 |
| Bucket helpers | test_latency_bucket_boundaries, test_result_count_bucket_boundaries | D-12 |
| **Concern #9 — PostHog drop counter** | test_posthog_dropped_event_counter_increments, test_get_dropped_event_count_returns_non_negative_integer | Concern #9 |
| capture_api_event | test_capture_api_event_non_blocking, _sampling, _does_not_log_query_or_filters | D-10, D-13, HARDEN-05 |
| Constants | test_loopback_ips_constant_is_set_with_127_and_ipv6, test_error_codes_taxonomy_includes_locked_codes | D-03, D-07 |
| **Concern #3 — APIError lock-in** | test_apierror_imported_from_shared_api_errors_not_web | Concern #3 |
| **R2-#2 — reset_for_tests** | test_rate_limiter_reset_for_tests_clears_buckets | Round-2 R2-#2 |

### tests/test_api_legacy_unchanged.py (3 tests)

D-23 + Concerns #2, #8 — legacy-route immutability spot check:

- **test_legacy_export_route_shape_unchanged** — happy path GET /api/export/json returns Phase 77 envelope, NO Phase 78 `error` key leaked.
- **test_legacy_validation_failure_envelope_unchanged** — Concern #2/#8 critical: legacy routes preserve FastAPI's default 422 `{detail:[...]}` envelope, NOT Phase 78's `{error:{code,message,fields}}`. Targets `/sitemap-manuscripts-{chunk}.xml` (typed `int` path param at web/api.py:283-284) with non-int chunk to drive RequestValidationError. Falls back to `/api/cambridge_image/{sys_id}?page=not_an_int` if sitemap returns 404. If Plan 02 ever installs the Phase 78 handler globally on `target_app`, this test detects the regression.
- **test_legacy_puzzle_image_route_status_unchanged** — spot check that `/api/puzzle_image` error response is NOT the Phase 78 envelope.

## Acceptance Criteria — Verification

| Criterion | Required | Actual | Status |
|-----------|----------|--------|--------|
| `tests/test_search_api.py` exists, parses | yes | yes | OK |
| `^def test_` count in test_search_api.py | ≥ 36 | 40 | OK |
| `tests/test_api_hardening.py` exists, parses | yes | yes | OK |
| `^def test_` count in test_api_hardening.py | ≥ 31 | 39 | OK |
| `tests/test_api_legacy_unchanged.py` exists, parses | yes | yes | OK |
| `^def test_` count in test_api_legacy_unchanged.py | ≥ 3 | 3 | OK |
| `from shared.api_errors import APIError` (search_api) | ≥ 1 | 2 | OK |
| `from shared.api_errors import APIError` (hardening) | ≥ 1 | 2 | OK |
| `_resolve_rate_limit_key\|_is_loopback_request` (search_api) | ≥ 1 | 8 | OK |
| `_resolve_rate_limit_key` (hardening) | ≥ 4 | 13 | OK |
| `_is_loopback_request` (hardening) | ≥ 4 | 17 | OK |
| `RATE_LIMIT_BUCKET_TTL` (hardening) | ≥ 1 | 2 | OK |
| `get_dropped_event_count` (hardening) | ≥ 2 | 5 | OK |
| `127.0.0.1, 203.0.113.5` literal (XFF spoof) | ≥ 1 each file | 2 / 1 | OK |
| `is_valid_domain_token` (search_api, R2-#3) | ≥ 4 | 10 | OK |
| `http_status == 503` (search_api, R2-#3) | ≥ 4 | 4 | OK |
| `http_status == 400` (search_api, R2-#3) | ≥ 1 | 1 | OK |
| `STALE_FROM_PRIOR_REQUEST` (search_api, R2-#1) | ≥ 1 | 2 | OK |
| `search_api_initialized` (search_api, R2-#2) | ≥ 2 | 4 | OK |
| `reset_for_tests` (hardening, R2-#2) | ≥ 2 | 6 | OK |
| `init_search_api()` no-override (legacy file, must be 0) | 0 | 0 | OK |
| `pytest --collect-only` fails with `ModuleNotFoundError` (intended RED) | yes | yes | OK |

All 25 verifiable acceptance criteria are met.

## Deviations from Plan

### Single deviation: `web.api_hardening import APIError` permitted via re-export identity test

The plan's acceptance criterion at line 496 requires `! grep -q "from web.api_hardening import APIError" tests/test_search_api.py` (zero occurrences). However, the plan's own example body for `test_apierror_imported_from_shared_api_errors_module` (lines 472-481) explicitly contains:

```python
from web.api_hardening import APIError as WebReexportedAPIError
```

The test then asserts `SharedAPIError is WebReexportedAPIError` — a higher-value check than the strict-absence rule, since it proves the web layer re-exports the same class object rather than redefining it. I implemented the plan's example body verbatim, which causes one occurrence of the strict-absent pattern. The test name (`test_apierror_imported_from_shared_api_errors_module`) and the assertion (`A is B`) both lock down Concern #3 more rigorously than absence-only would.

This is a Rule 1 deviation (the plan body and the acceptance criterion are mutually inconsistent; the body is the load-bearing artifact). All other tests use only `from shared.api_errors import APIError`.

### Tasks 4 + 5 bundled into Tasks 1 + 2 (process deviation, not a content deviation)

The plan structures Wave 0 as 5 tasks: Tasks 1-3 write the three files, Tasks 4-5 append round-2 RED additions to test_search_api.py and test_api_hardening.py respectively. I wrote each file as a single complete commit including the round-2 sections rather than a write+append sequence. The plan's Tasks 4+5 acceptance criteria (test names, grep counts, total `^def test_` thresholds ≥36 / ≥31) are all met within the single commits per file. No round-2 content was omitted.

## Authentication Gates

None encountered.

## Self-Check: PASSED

**Files created (verified via `[ -f ... ] && echo FOUND`):**
- tests/test_search_api.py — FOUND
- tests/test_api_hardening.py — FOUND
- tests/test_api_legacy_unchanged.py — FOUND
- .planning/phases/78-api-search-hardening-shell/78-01-SUMMARY.md — FOUND (this file)

**Commits (verified via `git log --oneline`):**
- 9f47025d test(78-01): RED scaffold for /api/search handler — 40 tests
- 58d09a3c test(78-01): RED scaffold for web/api_hardening.py helpers — 39 tests
- 1a38158c test(78-01): RED scaffold for legacy-route immutability — happy-path + validation parity

**Parse + collection verification:**
- `python -c "import ast; ast.parse(...)"` exits 0 for all 3 files
- `pytest --collect-only` fails with `ModuleNotFoundError: No module named 'web.search_api'` / `'web.api_hardening'` — intended RED

## TDD Gate Compliance

This plan is Wave 0 of a `type: tdd` phase. The RED gate is met:

- 3 `test(...)` commits exist (RED gate)
- Tests fail at collection time (`pytest --collect-only` → ModuleNotFoundError)
- No `feat(...)` commit yet — that is Plans 02 + 03's responsibility (GREEN gate)
- CI between Plan 01 commit and Plan 03 commit is expected RED per the plan's `commit_strategy` field. This is the intended TDD signal — RED locks the contract before GREEN.
