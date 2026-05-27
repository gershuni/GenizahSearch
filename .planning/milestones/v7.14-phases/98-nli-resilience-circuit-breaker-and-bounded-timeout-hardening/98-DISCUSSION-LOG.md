# Phase 98: NLI Resilience — Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in 98-CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-05-25
**Phase:** 98-nli-resilience-circuit-breaker-and-bounded-timeout-hardening
**Areas discussed:** Breaker key scope, Failure scope, Test placement, Telemetry

---

## Breaker key scope

| Option | Description | Selected |
|--------|-------------|----------|
| Single global "NLI" | One breaker for all NLI-hosted upstreams. Simpler, matches the failure mode (when NLI is degraded, both iiif and rosetta tend to be affected together). Less state, no per-host config. | ✓ |
| Per-host | Separate breaker for each upstream hostname. More precise — rosetta could remain healthy while iiif degrades. Adds config surface and doubles the test matrix. | |

**User's choice:** Single global "NLI"
**Notes:** Selected the recommended option. Aligns with the incident's failure pattern — the outage manifested as "NLI is slow" generally, not "iiif is slow but rosetta is fine".

---

## Failure scope

| Option | Description | Selected |
|--------|-------------|----------|
| Shared global | If /api/proxy_image fails 3x, /api/fl_ids also short-circuits. Matches the root cause (threadpool exhaustion is global, not per-endpoint). Single source of truth, simplest mental model. | ✓ |
| Per-call-site | Each call site has its own counter. More isolation but defeats the purpose — if NLI is slow, every endpoint will independently rediscover that. | |

**User's choice:** Shared global
**Notes:** Selected the recommended option. The whole point of the breaker is to prevent the cascade — per-site counters would force every endpoint to independently exhaust the threadpool before they realize NLI is down.

---

## Test placement

| Option | Description | Selected |
|--------|-------------|----------|
| New tests/test_nli_circuit_breaker.py | Dedicated file for both unit (monkeypatch) and concurrency (ThreadPoolExecutor) tests. Clean separation, easier to find when investigating outage. | ✓ |
| Extend existing tests/test_nli_* | Add to whatever NLI-adjacent test file already exists. Less file proliferation but harder to find later. | |

**User's choice:** New file
**Notes:** Selected the recommended option. Codex specifically called out this test file in its critique.

---

## Telemetry

| Option | Description | Selected |
|--------|-------------|----------|
| Yes, emit on open and close | Cheap to add, gives us monitoring/alerting on NLI degradation. PostHog already wired in project. Events: nli_breaker_opened (with failure count), nli_breaker_closed (with downtime duration). | ✓ |
| No telemetry | Keep the breaker self-contained. Avoid adding analytics calls to error-handling paths. Logs are sufficient. | |

**User's choice:** Yes, emit on open and close
**Notes:** Selected the recommended option. The breaker is the canonical NLI-outage signal — emitting it to PostHog gives us a single source for monitoring/alerting without needing log scraping.

## Claude's Discretion

- Exact public API of `shared/nli_circuit_breaker.py` (function vs class, naming)
- Exception type taxonomy for failure counting (which `requests` exceptions count)
- Lazy vs eager module initialization
- Backoff curve nuance (flat 60s window vs exponential)

## Deferred Ideas

- Async refactor to `httpx.AsyncClient`
- Event-loop watchdog / heartbeat monitoring
- Multi-worker uvicorn evaluation
- `/api/browse` dedicated profiling
- Supabase `_refresh_user_session` token-reuse error
