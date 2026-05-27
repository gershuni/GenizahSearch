---
phase: 98
slug: nli-resilience-circuit-breaker-and-bounded-timeout-hardening
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-05-25
---

# Phase 98 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Derived from 98-RESEARCH.md `## Validation Architecture` section.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x (per `.planning/codebase/TESTING.md`) |
| **Config file** | `tests/conftest.py` (project-wide sys.path setup + fixtures) |
| **Quick run command** | `pytest tests/test_nli_circuit_breaker.py -x` |
| **Full suite command** | `pytest tests/` |
| **Estimated runtime** | ~5s (quick), ~4min (full ~2326 tests) |

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/test_nli_circuit_breaker.py -x` (~5s)
- **After every plan wave:** Run `pytest tests/test_nli_circuit_breaker.py tests/test_nli_cache_persist_retry.py tests/test_nli_oxford_attribution.py tests/test_nli_crossref_service.py -x` (~15s)
- **Before `/gsd-verify-work`:** Full suite must be green
- **Max feedback latency:** 5 seconds

---

## Per-Task Verification Map

> Plans (gsd-planner) will populate the task-level rows during planning. The decision-to-test
> mapping below is the contract — every task must reference at least one Test ID from this list.

| Decision | Behavior | Test Type | Automated Command | Status |
|----------|----------|-----------|-------------------|--------|
| D-01 | Single global breaker key | unit | `pytest tests/test_nli_circuit_breaker.py::TestNliCircuitBreakerUnit::test_shared_state_across_sites -x` | ⬜ pending |
| D-02 | Shared state across `web/api.py` and `genizah_core.py` | integration | `pytest tests/test_nli_circuit_breaker.py::TestSharedAcrossCallSites -x` | ⬜ pending |
| D-03 | Module-level singleton in `shared/nli_circuit_breaker.py` | static | `pytest tests/test_nli_circuit_breaker.py::test_module_location_exists -x` | ⬜ pending |
| D-04 | `time.monotonic()` not `time.time()` | static AST | `pytest tests/test_nli_circuit_breaker.py::test_monotonic_time_used_not_wall_clock -x` | ⬜ pending |
| D-06 | timeout/5xx/429 trip breaker | parametrized unit | `pytest tests/test_nli_circuit_breaker.py::TestFailureCounting -x` | ⬜ pending |
| D-07 | 404 + empty manifest do NOT trip | unit | `pytest tests/test_nli_circuit_breaker.py::test_404_does_not_trip_breaker -x` | ⬜ pending |
| D-08 | success resets counter | unit | `pytest tests/test_nli_circuit_breaker.py::test_success_resets_counter -x` | ⬜ pending |
| D-11 | circuit check BEFORE semaphore | integration | `pytest tests/test_nli_circuit_breaker.py::test_circuit_check_before_semaphore -x` | ⬜ pending |
| D-12 | recheck AFTER semaphore acquire | integration | `pytest tests/test_nli_circuit_breaker.py::test_circuit_recheck_after_semaphore -x` | ⬜ pending |
| D-14..D-23 | 10 call sites wired | static grep | `pytest tests/test_nli_circuit_breaker.py::test_all_10_call_sites_use_breaker -x` | ⬜ pending |
| D-24 | PostHog events emitted on state change | unit (mock enqueue) | `pytest tests/test_nli_circuit_breaker.py::TestNliBreakerTelemetry::test_posthog_event_emitted_on_open -x` | ⬜ pending |
| D-25 | telemetry fire-and-forget, never raises | unit | `pytest tests/test_nli_circuit_breaker.py::test_telemetry_never_raises -x` | ⬜ pending |
| D-26 | ThreadPoolExecutor saturation < 10s | concurrency | `pytest tests/test_nli_circuit_breaker.py::TestNliCircuitBreakerConcurrency -x` | ⬜ pending |
| D-27 | race condition under simultaneous increments | concurrency | `pytest tests/test_nli_circuit_breaker.py::test_record_failure_under_concurrent_threads -x` | ⬜ pending |
| (cleanup) | genizah_core class-attr breaker fully removed | static grep | `pytest tests/test_nli_circuit_breaker.py::test_no_residual_class_attribute_breaker -x` | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Nyquist-Critical Invariants (Smallest Sufficient Test Set)

These 5 tests, if green, are sufficient evidence that Phase 98's threadpool-resilience invariant holds:

1. **D-26 — Concurrency saturation:** 20 ThreadPoolExecutor workers vs hanging session. Pass: total wall time < 10s AND `_nli_session.get` called ≤ 10 times. Proves the 2026-05-25 hang cannot recur.
2. **D-14..D-23 static call-site coverage:** grep all 10 sites for `is_open()` invocation. Pass: 10 matches. Prevents future PR from adding an 11th unprotected NLI fetch.
3. **Bounded-timeout test:** no hardcoded `timeout=15` or `timeout=30` remain in any NLI fetch site; all use `(connect, read)` tuple bounded by env knobs.
4. **D-25 — Telemetry never raises:** monkeypatch enqueue to raise; breaker still trips, no exception propagates.
5. **D-27 — Lock correctness under concurrency:** N threads call `record_failure` simultaneously; final state `_consecutive_failures == N` (no lost increments); `max_concurrent == 1` inside locked region.

All other tests are correctness checks for individual decisions, not invariant proofs.

---

## Wave 0 Requirements

- [ ] `tests/test_nli_circuit_breaker.py` — new file covering D-01..D-28
- [ ] `tests/conftest.py` autouse fixture for breaker state reset (prevents test order pollution)
- [ ] No framework install — pytest + unittest.mock already in `requirements.txt`

---

## Manual-Only Verifications

| Behavior | Why Manual | Test Instructions |
|----------|------------|-------------------|
| Production canary | Real NLI upstream cannot be reliably simulated in CI | After deploy, run `curl -w "%{time_total}\n" https://genizahsearch.com/api/fl_ids/990001458630205171` 10 times. First 1-3 calls should be slow (1-5s), remaining should be < 0.1s. Confirm "Failed to fetch FL IDs" appears at most 3 times per 60s window in `journalctl -u genizah-web`. |
| PostHog dashboard | Telemetry side effects are observable only in PostHog UI | Within 24h of deploy: confirm `nli_breaker_opened` events appear (if NLI flaps at all); confirm matching `nli_breaker_closed` events with `downtime_seconds` property. Stuck breaker (open without close) indicates Pitfall 4 (`time.time` slip). |

---

## Observability Invariants

- **PostHog event symmetry:** every `nli_breaker_opened` must eventually have a matching `nli_breaker_closed`. Asymmetry suggests a stuck breaker.
- **Journal pattern:** `Failed to fetch FL IDs` ≤ 3 times per 60s window per sys_id (down from 1/request pre-fix).
- **PostHog drop counters — TWO queues to monitor (Codex REVIEW Issue 5, Option A):**
  - `web/api_hardening.get_dropped_event_count()` — drops from the `search_api_request` event queue (Phase 78 server-side capture; ALL `/api/*` endpoint instrumentation flows through this).
  - `shared/posthog_server.get_dropped_event_count()` — drops from the breaker telemetry queue (Phase 98 Plan 01; ALL `nli_breaker_opened` / `nli_breaker_closed` events flow through this).
  - **Both should stay at 0 during normal operation.** Growth in EITHER counter = the corresponding PostHog queue is saturating.
  - **Why two queues?** Plan 98-01 explicitly does NOT modify `web/api_hardening.py` to avoid breaking 5 existing test monkeypatches on `web.api_hardening._event_queue` (the source code comment at `web/api_hardening.py:653-656` confirms the test seam is intentional). The shared module `shared/posthog_server.py` is a clean factor-out so the breaker (in `shared/`) does not depend on `web/`. Future cleanup could delegate `web/api_hardening` to `shared/posthog_server` — out of scope for Phase 98.
  - **Operator check at deploy time:** verify both via `curl https://genizahsearch.com/api/internal/health` (if such endpoint exists) OR via a one-off Python REPL on the production host:
    ```python
    >>> from web.api_hardening import get_dropped_event_count as web_drops
    >>> from shared.posthog_server import get_dropped_event_count as breaker_drops
    >>> web_drops(), breaker_drops()
    (0, 0)  # expected
    ```

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 5s
- [ ] `nyquist_compliant: true` set in frontmatter (after Wave 0 complete)

**Approval:** pending
