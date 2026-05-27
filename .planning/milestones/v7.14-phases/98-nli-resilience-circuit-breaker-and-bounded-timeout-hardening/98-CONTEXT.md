# Phase 98: NLI Resilience — Context

**Gathered:** 2026-05-25
**Status:** Ready for planning

<domain>
## Phase Boundary

Prevent any single NLI/IIIF upstream slowdown from hanging `genizah-web`. Bound the per-request blocking budget on every NLI-touching code path via (a) a shared circuit breaker that short-circuits requests when NLI is degraded and (b) shorter, env-configurable read timeouts. The 2026-05-25 production outage (7 minutes unresponsive, SIGTERM ignored, SIGKILL required) is the trigger and the regression test.

**In scope:**
- Extract `shared/nli_circuit_breaker.py` — module-level, locked, `time.monotonic`-based, with `is_open()` / `record_failure()` / `record_success()` API
- Wire breaker into ALL 10 NLI/IIIF fetch sites (enumerated in §canonical_refs)
- Reduce read timeouts everywhere NLI is contacted (currently 15s+, target 3-5s read)
- Move circuit check **before** `_nli_semaphore.acquire()` (web/api.py:647), not just inside `_fetch_fl_ids_network`
- Drop `NLI_SEMAPHORE_TIMEOUT` default from 20s to 1s
- Failure-counting: timeouts + 5xx + 429 trip; 404 does NOT (per-sys_id negative-cache only)
- PostHog telemetry on breaker state changes (open/close)
- Tests: unit (monkeypatch) + concurrency (ThreadPoolExecutor saturation)

**Out of scope (explicitly):**
- Async refactor to `httpx.AsyncClient`
- Event-loop watchdog / heartbeat monitoring
- Multi-worker uvicorn decision (breaker is intentionally process-local)
- `/api/browse` profiling
- Supabase `_refresh_user_session` token-reuse error from the same outage's journal window
- Any new feature work — this is a resilience-only phase

</domain>

<decisions>
## Implementation Decisions

### Breaker Architecture
- **D-01:** Breaker key is **single global "NLI"** — not per-host. Rationale: when NLI is degraded both `iiif.nli.org.il` and `rosetta.nli.org.il` tend to be affected together; the failure mode is the upstream organization, not the specific hostname. Single counter, single open-until timestamp.
- **D-02:** Breaker state is **shared globally across all NLI call sites** — not per-call-site. Rationale: the threadpool-exhaustion failure mode is global. If `/api/proxy_image` has tripped the breaker, `/api/fl_ids` should also short-circuit immediately. One source of truth.
- **D-03:** Breaker lives in a new module `shared/nli_circuit_breaker.py` — module-level singleton with `threading.Lock`-guarded mutations. NOT a class attribute on `GenizahCore` (current location at `genizah_core.py:3940-3961` to be removed and consumers migrated). Both `web/api.py` and `genizah_core.py` import from the shared module.
- **D-04:** Use `time.monotonic()` for the open-until timestamp, not `time.time()`. Wall-clock jumps (NTP) should not affect breaker behavior.
- **D-05:** Breaker is process-local. If we later move to multi-worker uvicorn, each worker gets its own breaker — that's acceptable degradation, not a blocker for shipping.

### Failure Counting
- **D-06:** Failure events that increment the consecutive-failure counter:
  - `requests.exceptions.Timeout` (read or connect timeout)
  - `requests.exceptions.ConnectionError`
  - HTTP 5xx responses
  - HTTP 429 (rate-limited by upstream)
- **D-07:** Events that do NOT trip the breaker:
  - HTTP 404 — record as per-sys_id negative cache only (existing `NLI_FAIL_CACHE_TTL=60s` infrastructure)
  - Successful empty-manifest response (200 OK with empty FL ID list) — per-sys_id negative cache, no breaker increment
- **D-08:** `record_success()` resets the consecutive-failure counter to zero. Any successful NLI fetch by any call site resets the counter for all.

### Configuration (env knobs)
- **D-09:** New env variables, all with safe defaults:
  - `NLI_CIRCUIT_THRESHOLD=3` (consecutive failures to trip)
  - `NLI_CIRCUIT_WINDOW=60` (seconds, breaker stays open)
  - `NLI_IIIF_READ_TIMEOUT=5` (was 15s, hard-coded)
  - `NLI_MARC_READ_TIMEOUT=3` (was 10s, hard-coded)
  - `NLI_IMAGE_READ_TIMEOUT=5` (was 15s, hard-coded across multiple call sites)
  - `NLI_CONNECT_TIMEOUT=3` (new, was previously implicit in single-value timeout)
- **D-10:** Drop existing `NLI_SEMAPHORE_TIMEOUT` default from `20` to `1` second. Waiting >1s on a semaphore burns Starlette threadpool workers doing nothing.

### Circuit Check Placement
- **D-11:** Add circuit check at `web/api.py:fetch_fl_ids_from_nli` (currently line ~647), **before** `_nli_semaphore.acquire()`. If circuit is open, return `[]` immediately — do not even attempt to acquire the semaphore.
- **D-12:** Re-check circuit after semaphore acquisition (defensive — another thread may have tripped it while we were waiting on the semaphore). If open, release the semaphore and return `[]`.
- **D-13:** All other call sites: circuit check is the first thing the function does, before any other network or lock acquisition.

### Call Site Coverage (all 10 sites must be guarded + timeout-shortened)
- **D-14:** `web/api.py:680` IIIF manifest fetch — circuit guard + `(connect=NLI_CONNECT_TIMEOUT, read=NLI_IIIF_READ_TIMEOUT)`
- **D-15:** `web/api.py:713` MARC fallback fetch — circuit guard + `(connect=NLI_CONNECT_TIMEOUT, read=NLI_MARC_READ_TIMEOUT)`
- **D-16:** `web/api.py:771` `/api/nli_image/{fl_id}` (IIIF + Rosetta fetches) — circuit guard on both, image read timeout
- **D-17:** `web/api.py:834` `_fetch_nli_image_bytes` — circuit guard before the FL-id-iteration fallback loop (so a list of 20 FL ids does not multiply blocking time)
- **D-18:** `web/api.py:1994` `/api/proxy_image` — circuit guard + image read timeout
- **D-19:** `shared/puzzle_image_service.py:172` direct IIIF fetch — circuit guard + shortened from 30s
- **D-20:** `shared/puzzle_image_service.py:252` direct IIIF fetch — circuit guard + shortened from 30s
- **D-21:** `web/pages/puzzle.py:1991` direct NLI manifest fetch — circuit guard + shortened from 15s
- **D-22:** `genizah_core.py:4651` IIIF/MARC fetch path — wire to shared breaker (replaces current class-attribute breaker)
- **D-23:** `genizah_core.py:4773` IIIF/MARC fetch path — wire to shared breaker

### Telemetry
- **D-24:** Emit PostHog events on breaker state transitions:
  - `nli_breaker_opened` — properties: `consecutive_failures`, `triggering_path` (e.g., "fetch_fl_ids_from_nli"), `failure_type` (timeout / 5xx / 429 / connection_error)
  - `nli_breaker_closed` — properties: `downtime_seconds` (monotonic delta from open to close), `closed_by_path`
- **D-25:** Telemetry is fire-and-forget — must not block the request path. If `posthog_client` is unavailable (e.g., `POSTHOG_API_KEY` unset), log a debug message and continue. Never raise from telemetry into the calling code.

### Tests
- **D-26:** New file `tests/test_nli_circuit_breaker.py`. Two test classes:
  - `TestNliCircuitBreakerUnit`: monkeypatch `_nli_session.get` to raise `requests.exceptions.ReadTimeout`. Call 3 times → assert `_nli_record_failure` incremented to 3 and breaker open. Call 4th time → assert returns `[]` without invoking `_nli_session.get` (assert via mock call count). Monkeypatch `time.monotonic` forward 61s → assert call 5 invokes `_nli_session.get` again (breaker auto-recovered).
  - `TestNliCircuitBreakerConcurrency`: spawn 20 threads via `ThreadPoolExecutor`, each calling `fetch_fl_ids_from_nli` against a monkeypatched session that hangs/times-out. Assert total wall-clock time ≲ 10s (breaker trips after first 3 failures saturate the 8-slot semaphore; subsequent 17 calls return immediately). Assert `_nli_session.get` was called at most `NLI_MAX_CONCURRENT_FETCHES + NLI_CIRCUIT_THRESHOLD - 1 = 10` times.
- **D-27:** Additional unit tests in same file:
  - 5xx response trips breaker (test each of 500, 502, 503, 504)
  - 429 response trips breaker
  - 404 does NOT trip breaker (only per-sys_id negative cache populated)
  - Successful fetch resets counter
  - Race condition: simulate two threads incrementing counter simultaneously — assert final state correct (validates `threading.Lock` is doing its job)

### Telemetry verification
- **D-28:** Test PostHog emission via the existing `web/posthog_client.py` test helper (whatever pattern is already used in `tests/test_posthog_*.py`). Assert the right event names and key properties are emitted on open and close. Do NOT make real PostHog calls in tests.

### Claude's Discretion
- The exact public API of `shared/nli_circuit_breaker.py` (function vs class, naming) — Claude may choose what reads cleanly, as long as it satisfies the locked decisions above.
- Exception type taxonomy for failure counting — Claude can include `urllib3.exceptions.MaxRetryError`, `socket.timeout`, and any other equivalent error classes seen in production logs.
- Whether to lazy-initialize the breaker module or eagerly at import time — Claude can pick whichever avoids import cycles.
- Backoff curve nuance — start with a flat 60s open-window (per D-09), exponential backoff is a future enhancement.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase Specification
- [docs/INCIDENT-2026-05-25-nli-iiif-hang.md](../../../docs/INCIDENT-2026-05-25-nli-iiif-hang.md) — Root cause analysis, evidence (journal excerpts), and the initial fix plan that Codex critiqued. Read first for context on WHY this phase exists.
- [docs/INCIDENT-2026-05-25-CODEX-CRITIQUE.md](../../../docs/INCIDENT-2026-05-25-CODEX-CRITIQUE.md) — Codex's 9-point critique of the initial plan. The "Minimum ship patch" section at the bottom is the de-facto requirements list — every item there is locked in this phase's decisions above.

### Current Code Sites (must touch)
- [web/api.py:647](../../../web/api.py) — `fetch_fl_ids_from_nli` semaphore acquisition (D-11 circuit check goes here)
- [web/api.py:680](../../../web/api.py) — IIIF manifest fetch (D-14)
- [web/api.py:713](../../../web/api.py) — MARC fallback fetch (D-15)
- [web/api.py:771](../../../web/api.py) — `/api/nli_image/{fl_id}` endpoint (D-16)
- [web/api.py:834](../../../web/api.py) — `_fetch_nli_image_bytes` (D-17)
- [web/api.py:1994](../../../web/api.py) — `/api/proxy_image` (D-18)
- [shared/puzzle_image_service.py:172](../../../shared/puzzle_image_service.py) — puzzle IIIF fetch #1 (D-19)
- [shared/puzzle_image_service.py:252](../../../shared/puzzle_image_service.py) — puzzle IIIF fetch #2 (D-20)
- [web/pages/puzzle.py:1991](../../../web/pages/puzzle.py) — puzzle page NLI manifest fetch (D-21)
- [genizah_core.py:3940-3961](../../../genizah_core.py) — existing class-attribute breaker (TO BE REMOVED; logic migrates to `shared/nli_circuit_breaker.py`)
- [genizah_core.py:4651](../../../genizah_core.py) — IIIF/MARC fetch (D-22)
- [genizah_core.py:4773](../../../genizah_core.py) — IIIF/MARC fetch (D-23)

### Project Conventions
- [.planning/codebase/CONVENTIONS.md](../../codebase/CONVENTIONS.md) — coding conventions
- [.planning/codebase/TESTING.md](../../codebase/TESTING.md) — test patterns; check before authoring `tests/test_nli_circuit_breaker.py`
- [.planning/codebase/STACK.md](../../codebase/STACK.md) — NLI cache configuration knobs (`NLI_CACHE_TTL`, `NLI_FAIL_CACHE_TTL`, etc.)
- [CLAUDE.md](../../../CLAUDE.md) — section "Environment Variables" must be updated with the new `NLI_*` knobs from D-09

### Phase 87 Invariant
- [web/safe_storage.py](../../../web/safe_storage.py) — Phase 98 does not touch per-user state, but any new env vars or telemetry that touches `app.storage.user` MUST route through this chokepoint. (Almost certainly not relevant here, but flagging.)

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **Current breaker (to be migrated):** `GenizahCore._nli_circuit_is_open` / `_nli_record_failure` / `_nli_record_success` at `genizah_core.py:3940-3961` already implement the right logic but as class attributes on `GenizahCore`. Threshold=3, window=60s — match these defaults in the new module.
- **Existing negative cache:** `_nli_cache` + `NLI_FAIL_CACHE_TTL=60s` in `web/api.py:29` is per-sys_id and complementary to the breaker. Keep it. The breaker is a global outage signal; the negative cache is per-resource. Both layers should coexist.
- **Existing semaphore:** `_nli_semaphore = threading.Semaphore(NLI_MAX_CONCURRENT_FETCHES=8)` at `web/api.py:41`. Keep it; the breaker reduces the chance that the semaphore fills, but the semaphore is still useful for burst protection during normal operation.
- **PostHog client:** Already wired in project (per memory). Find the existing emission pattern in `web/` (likely `posthog_client.py` or `web/services.py`) and reuse.
- **`threading.Lock` pattern:** Phase 92.2 introduced `WeakKeyDictionary` + lock patterns for the lists memo. The lock idiom is established in the codebase — follow whatever style is used there.

### Established Patterns
- **Env-var configuration:** `web/api.py:27-37` shows the pattern: `int(os.environ.get('VAR_NAME', 'DEFAULT'))`. Apply for all new `NLI_*` knobs.
- **Fire-and-forget telemetry:** Find the existing PostHog emission idiom in the codebase before authoring D-24. It likely already handles the "client unavailable" case.
- **Test placement:** `tests/test_*.py` flat at repo root per existing convention (no subdirectories). New `tests/test_nli_circuit_breaker.py` follows the same pattern.

### Integration Points
- **Single import surface:** Both `web/api.py` and `genizah_core.py` will import from `shared/nli_circuit_breaker.py`. No other consumers needed; keep the module surface tight (3 functions: `is_open`, `record_failure`, `record_success`, plus maybe `_state_snapshot()` for tests).
- **No DB / no Supabase touching:** This phase is pure in-memory state + network timeouts. No schema migration, no Supabase RLS work.
- **No frontend changes:** No `web/pages/*.py` UI changes — only the puzzle.py NLI fetch line (D-21), which is server-side.

</code_context>

<specifics>
## Specific Ideas

- **Codex critique is the spec.** Every "Minimum ship patch" item from `docs/INCIDENT-2026-05-25-CODEX-CRITIQUE.md` is encoded as a decision above. The planner should treat that document as a contract.
- **Verification approach** (from incident doc §5): production canary — after deploy, `curl -w "%{time_total}\n" https://genizahsearch.com/api/fl_ids/990001458630205171` ten times. With the fix, after the first 1-3 slow calls (which trip the breaker), the rest should return in <0.1s. Then watch the journal for "Failed to fetch FL IDs" — should appear at most 3 times per 60s window per sys_id, not once per request.
- **The originating sys_id** is `990001458630205171` (Ms. EVR II A 2341, Karaite Prayers, NLI-hosted) — a good seed for the concurrency test if you want a realistic monkeypatch target.

</specifics>

<deferred>
## Deferred Ideas

These came up during incident analysis but belong in other phases.

- **Async refactor to `httpx.AsyncClient`** — would be the architecturally correct fix but is a much larger scope. The shorter-timeout + circuit-breaker approach in Phase 98 buys the same outage resistance at ~1% of the code change. Revisit if uvicorn workers / async event loop monitoring shows the threadpool-based fix is insufficient.
- **Event-loop watchdog / heartbeat monitoring** — systemd timer scraping `journalctl -u genizah-web --since '1 minute ago'`; if zero lines for 5 minutes, alert/auto-restart. Cheap to add but operationally separate from the breaker.
- **Multi-worker uvicorn evaluation** — currently single-worker. If we move to multi-worker, each worker has its own breaker (D-05). May need shared-state breaker (Redis/file lock) at that point.
- **`/api/browse` profiling** — the browse endpoint also touches IIIF for image URL resolution. Should benefit from this phase's fix transitively but a dedicated profiling pass might surface other slow paths.
- **Supabase `_refresh_user_session` token-reuse error** — observed at `12:09:14` in the same outage's journal window. Separate auth concern, not related to threadpool exhaustion.

</deferred>

---

*Phase: 98-nli-resilience-circuit-breaker-and-bounded-timeout-hardening*
*Context gathered: 2026-05-25*
