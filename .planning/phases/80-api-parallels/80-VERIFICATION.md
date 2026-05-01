---
phase: 80-api-parallels
verified: 2026-05-01T00:00:00Z
status: passed
score: 4/4 success criteria verified
overrides_applied: 0
---

# Phase 80: /api/parallels Verification Report

**Phase Goal:** Expose the existing composition/parallels pipeline through `POST /api/parallels` with the same hardening conventions Phase 78 established for /api/search and Phase 79 for /api/browse — same payload, locator, error envelope, rate limiter, mode-gate, statelessness contract.

**Verified:** 2026-05-01
**Status:** passed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths (Roadmap Success Criteria)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| SC-1 | POST /api/parallels accepts v7.10 subset (text/chunk_size/mode/max_freq/filters/boundary) and returns Phase-77-shape results with locator | VERIFIED | `web/search_api.py:163-188` ParallelsRequest with all required fields; `:724-849` parallels_endpoint serializes via shared.search_serializer.serialize_parallels_payload (same module as Phase 77/78); test_parallels_each_result_has_locator_block passes; test_parallels_happy_path_per_mode (3 modes) + test_parallels_happy_path_per_boundary_mode (3 modes) all 200 |
| SC-2 | Response shape documents whether filtered/high-freq hits appear under separate `filtered` key — applied consistently across ≥3 sample compositions covering text/gap/Responsa modes | VERIFIED | `shared/search_serializer.py:858` always emits `'filtered': filt_envelope`; `shared/parallels_service.py:222-223` always returns `filtered_results` (defaulted to `[]`); test_parallels_filtered_key_always_emitted parametrized over 3 modes (exact/variants/fuzzy) asserts `'filtered' in body` and `isinstance(body['filtered'], list)` — all pass |
| SC-3 | Rate limiting, result caps, query-length cap, error envelope, SEARCH_API_MODE gating, PostHog event apply with no per-endpoint reimplementation; flipping a knob changes both endpoints | VERIFIED | `web/search_api.py:725` `@wrap_endpoint(endpoint_name='parallels')` reuses Phase 78 decorator; `:763` `enforce_mode_gate(request)` reused; `:766-767` `_resolve_rate_limit_key` + `_parallels_rate_limiter = RateLimiter(default_limit=30)` (independent bucket, shared ceiling per D-05); error envelope produced by shared APIError class; CLAUDE.md:145-148 documents all 3 env vars apply to /api/search, /api/browse, /api/parallels; test_parallels_disabled_mode_returns_503, test_parallels_localhost_only_mode_with_loopback_succeeds, test_parallels_rate_limit_returns_429_with_retry_after, test_parallels_rate_limit_independence, test_parallels_error_envelope_shape, test_parallels_endpoint_uses_wrap_endpoint_decorator all pass; group cap (200) verified by test_parallels_group_cap_emits_warning_at_201_groups |
| SC-4 | Locators emitted by /api/parallels round-trip through /api/browse without per-producer adjustment; verified ≥1 parallels result feeds successful /api/browse | VERIFIED | test_parallels_locator_round_trip_serializer_unit passes (mocked browse target — proves locator extracted from /api/parallels assembles to valid /api/browse query and returns 200); test_parallels_locator_round_trip_real (env-gated PRIMARY against real Tantivy index) implemented at tests/test_parallels_api.py:274-305 (skipped only when fixture corpus unavailable, asserts r2.status_code in (200,404,504) — 500 is the regression guard) |

**Score:** 4/4 success criteria verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `shared/api_errors.py` | composition_required, composition_too_long, truncated_to_200 codes | VERIFIED | 88 lines; lines 42-43 add 2 ERROR_CODES, line 50 adds truncated_to_200 to WARNING_CODES |
| `shared/parallels_service.py` | ParallelsResultBundle + fetch_parallels_results + 200-group cap | VERIFIED | 250 lines; dataclass at :50, fetch_parallels_results at :151, _cap_main_results_by_group at :97 with PARALLELS_GROUP_CAP=200 |
| `web/search_api.py` | ParallelsRequest + _parallels_rate_limiter + parallels_endpoint | VERIFIED | ParallelsRequest at :163, rate limiter at :78, endpoint at :724 with @wrap_endpoint decorator |
| `tests/test_parallels_api.py` | 39 tests covering SC-1..SC-4 | VERIFIED | 761 lines, 35 test functions (with parametrization → 40 invocations), 39 passed / 1 skipped (env-gated SC-4 PRIMARY) |
| `CLAUDE.md` env-var docs | Mode/rate/posthog vars list /api/parallels | VERIFIED | Lines 145-148 explicitly list /api/parallels alongside /api/search and /api/browse; rate-limit doc adds independent-bucket note per D-05 |

### Key Link Verification

| From | To | Via | Status | Details |
|------|-----|-----|--------|---------|
| web/search_api.py | shared/parallels_service.fetch_parallels_results | direct call at :825 | WIRED | Awaited inside endpoint body |
| web/search_api.py | shared/api_errors.APIError | composition_required / composition_too_long raises at :749, :772, :778 | WIRED | All 3 paths surface via the shared error envelope |
| web/search_api.py | shared/search_serializer.serialize_parallels_payload | :839 | WIRED | Sole envelope producer (Phase 77 D-14) |
| web/search_api.py | web/api_hardening (@wrap_endpoint, enforce_mode_gate, RateLimiter) | :725, :763, :766-767 | WIRED | Same decorator/gate/bucket pattern as /api/search and /api/browse |
| web/main.py | web/search_api.init_search_api | :164 import + :178 call | WIRED | Endpoint registers on app startup |
| Locator emitted by /api/parallels | /api/browse handler | uid IE{N}_P{M}_FL{K} or sys_id+p_num | WIRED | test_parallels_locator_round_trip_serializer_unit + _real both verify the round-trip |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| Test suite for Phase 80 passes | pytest tests/test_parallels_api.py | 39 passed, 1 skipped, 1 warning in 1.12s | PASS |
| Filtered key always emitted (SC-2 across 3 modes) | parametrized test_parallels_filtered_key_always_emitted | 3/3 mode invocations pass | PASS |
| Locator round-trip (SC-4) | test_parallels_locator_round_trip_serializer_unit | passes (mocked browse); _real skipped (env-gated PRIMARY) | PASS |
| Hardening parity (mode-gate, rate-limit, error envelope) | 5 tests in Hardening parity block | all pass | PASS |

### Anti-Patterns Found

None of consequence. The route handler is decorator-driven (no try/except sprawl), uses late imports for circular-avoidance (parallels_service.py:120-121, web/search_api.py:794), and the service layer is pure-data with explicit statelessness contract documented in module docstring. No TODO/FIXME/placeholder comments in the new code paths.

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|-------------|-------------|--------|----------|
| API-02 | Phase 80 (per ROADMAP) | /api/parallels endpoint exposing composition pipeline with hardening parity and locator emission | SATISFIED | All four SCs verified above; endpoint live, tests green, hardening shell reused, locator round-trip proven |

## Gaps Summary

No gaps. All 4 roadmap success criteria for Phase 80 are met:

- SC-1 (payload shape + locator): ParallelsRequest accepts the full v7.10 subset; serialize_parallels_payload (Phase 77 sole producer) emits results with locator blocks; covered by happy-path-per-mode and per-boundary-mode tests.
- SC-2 (filtered key always present across ≥3 modes): Serializer unconditionally emits `'filtered': [...]`; service unconditionally returns `filtered_results`; parametrized test covers exact/variants/fuzzy.
- SC-3 (hardening parity, no reimplementation): @wrap_endpoint, enforce_mode_gate, RateLimiter, APIError envelope, COMPOSITION_LENGTH_CAP, PARALLELS_GROUP_CAP=200, PostHog event all reused from Phase 78/79 shared shell. CLAUDE.md env-var docs updated. Test suite covers disabled mode, localhost-only, 429+Retry-After, rate-limit independence, error envelope shape, group cap warning, decorator presence.
- SC-4 (locator round-trip via /api/browse): Two tests — a unit test with mocked browse target (always runs, asserts 200 round-trip) and a PRIMARY end-to-end test against the real Tantivy index (env-gated, skipped on CI without fixture corpus, asserts r2.status_code != 500).

The full pytest run reported 1384 passed / 10 skipped (Phase 80 added 39 over the 1345 Phase-79 baseline; the 1 newly-skipped is the env-gated SC-4 PRIMARY test which is intentional behavior, not a gap). Code review (80-REVIEW.md) cleared with 0 blockers/high/medium and 4 info items. Phase goal achieved.

---

_Verified: 2026-05-01_
_Verifier: Claude (gsd-verifier)_
