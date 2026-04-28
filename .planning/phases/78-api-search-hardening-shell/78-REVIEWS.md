---
phase: 78
reviewers: [gemini, codex]
reviewed_at: 2026-04-28T09:13:58Z
plans_reviewed:
  - 78-01-PLAN.md
  - 78-02-PLAN.md
  - 78-03-PLAN.md
  - 78-04-PLAN.md
note: |
  Claude CLI was skipped per user-feedback memory (we ARE Claude — review independence
  requires a different model). Gemini and Codex both run with their default models
  (no review.models.* override in .planning/config.json).
---

# Cross-AI Plan Review — Phase 78

## Gemini Review

These implementation plans for **Phase 78** are exceptionally well-structured and reflect a mature, security-first approach to API development. The separation of the "Hardening Shell" into a reusable module (`api_hardening.py`) is a strategic win that will simplify the implementation of Phases 79 and 80. The TDD Wave 0 approach ensures that the contract is locked before code is written, which is essential for a milestone introducing a new public-facing (albeit internal-helper) interface.

### Summary
Phase 78 delivers a hardened `POST /api/search` endpoint using a **Research -> Strategy -> Execution** lifecycle. By building a generic security layer (sliding-window rate limiting, loopback-aware mode gating, and server-side observability) upfront, the plans ensure that all subsequent API endpoints inherit these protections without code duplication. The use of Pydantic for strict schema enforcement and a dedicated FJMS filter validator addresses the core requirements for input integrity and statelessness.

### Strengths
*   **Reusable Hardening Shell:** Isolating rate limiting, mode gating, and PostHog logic into `web/api_hardening.py` is excellent architectural foresight.
*   **Honest Sliding-Window Rate Limiter:** Moving away from the project's legacy fixed-window pattern to a `deque`-based sliding window provides a mathematically correct `Retry-After` header, which is critical for well-behaved API clients.
*   **Strict Pydantic Enforcement:** Using `extra='forbid'` on both the request and filter models is a strong defensive measure against mass-assignment and unexpected input.
*   **Non-Blocking Observability:** The use of a background daemon thread and a thread-safe queue for PostHog events ensures that telemetry does not add latency to the search pipeline.
*   **Comprehensive TDD:** The 24+ tests planned in Wave 0 cover not only happy paths but also edge cases like loopback spoofing, combinatorial cascades, and statelessness (via byte-identical comparisons).

### Concerns

*   **[MEDIUM] X-Forwarded-For (XFF) Spoofing in `localhost-only` mode:**
    *   **Decision D-03** and **Plan 78-02 Task 1** use `.split(',')[0]` to check the "first hop" of the XFF header.
    *   **Risk:** If the app is behind an Nginx proxy that *appends* to XFF (the default `$proxy_add_x_forwarded_for` behavior), an external attacker can send a request with `X-Forwarded-For: 127.0.0.1`. Nginx will append the attacker's real IP (e.g., `127.0.0.1, 203.0.113.5`), but your code will see the first hop as `127.0.0.1` and incorrectly grant access.
    *   **Severity:** Medium (only affects the `localhost-only` restriction).

*   **[LOW] PostHog Capture Missing for Structural Validation Errors:**
    *   The `capture_api_event` call is placed inside the `search_endpoint` handler (Plan 78-03).
    *   **Risk:** Structural errors (e.g., sending an integer where a string is expected) are caught by Pydantic/FastAPI before the handler is entered. These `invalid_request` events will be returned to the user but will **not** be recorded in PostHog.
    *   **Severity:** Low (telemetry gap, not a security risk).

*   **[LOW] RateLimiter Memory Growth:**
    *   The `_buckets` dictionary in `RateLimiter` grows with every unique IP and is never pruned.
    *   **Risk:** Over a long-running process exposed to many IPs, this could lead to slow memory growth. For an internal helper API, this is likely negligible, but worth noting for long-term maintenance.
    *   **Severity:** Low.

*   **[LOW] Salt Persistence Race Condition:**
    *   `_resolve_posthog_ip_salt` (Plan 78-02 Task 2) attempts to atomically write the salt using a `.tmp` file and `os.replace`.
    *   **Risk:** On some Windows environments or network drives, `os.replace` can behave inconsistently if the target file is being read. The best-effort `os.chmod` is also correctly identified as a no-op on Windows.
    *   **Severity:** Low.

### Suggestions

*   **Fix the XFF Check:** In `web/api_hardening.py:_is_loopback_request`, instead of checking just the first hop, you should verify that **every** IP in the comma-separated XFF header is a loopback address. If the list contains any non-loopback IP, it originated from or passed through an external source.
*   **Unified Observability:** Consider moving the `capture_api_event` call into the `register_exception_handlers` function or a simple middleware. This ensures that even Pydantic `RequestValidationError` events are captured in PostHog, fulfilling **HARDEN-05** more comprehensively.
*   **Prune the RateLimiter:** Add a simple task (or check during `check()`) to occasionally clear keys from `_buckets` if their deque is empty and hasn't been touched in, say, 1 hour.
*   **Clarify IP Salt Path:** Ensure the `web/_secrets/` directory is created with appropriate permissions before writing the salt.

### Risk Assessment: LOW
The implementation risk is very low. The design is conservative, stays within existing architectural patterns (NiceGUI/FastAPI), and uses a proven TDD harness. The "hardening" components are actually more robust than the project's existing legacy `/api` routes. As long as the XFF spoofing concern is addressed in the implementation of the `_is_loopback_request` helper, the security posture is excellent for an internal research tool.

**Verdict:** The plans are approved. Proceed with execution, taking note of the XFF spoofing suggestion during the implementation of Plan 78-02.

---

## Codex Review

**Summary**

These plans are thoughtful and unusually explicit, but as written they do **not yet reliably deliver the phase goal**. The biggest problem is that two core mechanisms meant to be "generic hardening" are mis-scoped: the rate limiter keys on the **direct peer** instead of the real client IP under nginx, and the exception handlers are registered **app-wide**, which can change legacy route behavior even though the phase explicitly promises not to. Those two issues alone put HARDEN-01 and the "existing `/api/*` unchanged" success criterion at risk. The rest of the design is strong, but the plan needs a small round of architectural correction before implementation starts.

**Strengths**

- The phase boundary is clear. `web/api_hardening.py` vs `web/search_api.py` is the right split in principle.
- D-01/D-03/D-07 are well locked. The plans don't leave important semantics to chance.
- The plans are explicit about what is out of scope, especially not touching `web/api.py`.
- The test surface is strong overall, especially the separation between unit tests, handler tests, and live soak tooling.
- The warnings requirement is handled intentionally instead of being left buried in result rows.
- The `app_override` registrar pattern is a good choice for test isolation and future reuse in Phases 79/80.
- The PostHog payload discipline is good: low-cardinality buckets, no payload contents, explicit IP hashing.
- The empty-intersection short-circuit in Plan 03 is the right read of API-07 and avoids accidental "empty set means unrestricted" bugs.

**Concerns**

- **HIGH**: Plan 02's `get_client_ip(request)` is incompatible with HARDEN-01 under the deployment model described in D-03. The plan explicitly says rate limiting uses the **direct peer only**, and XFF is ignored except for localhost checks. Behind nginx, that means many or all requests may collapse to `127.0.0.1`, turning a per-IP limiter into a global limiter. This is a direct conflict between Plan 02 Task 1 and D-03/HARDEN-01.
- **HIGH**: `register_exception_handlers(target_app)` is app-global, not route-scoped. Plan 03 calls it from `init_search_api()`, which means existing routes on the NiceGUI/FastAPI app can now get the new `RequestValidationError` and `APIError` behavior. That undermines Success Criterion 2 and D-18's "legacy routes unchanged" claim. The smoke in `tests/test_api_legacy_unchanged.py` is too shallow to catch this.
- **HIGH**: The `localhost-only` trust model is only safe if nginx **overwrites/sanitizes** `X-Forwarded-For`. The plans assume "nginx is the only proxy in front" but never state or test the proxy-header sanitization requirement. If nginx passes through attacker-supplied XFF, `X-Forwarded-For: 127.0.0.1` can become a bypass candidate.
- **HIGH**: Plan 03's `shared/fjms_service.validate_filter_values()` lazily imports `web.api_hardening.APIError`. That creates a bad `shared -> web` dependency inversion. It may work technically, but it weakens the service-layer boundary and makes future desktop/shared reuse worse.
- **MEDIUM**: HARDEN-03 is only partially covered. Plan 03 hoists `responsa_warning` from `results[0]`, but if a Responsa downgrade occurs and the query yields zero results, the warning is still lost. Requirement 5 says the downgrade must surface whenever it happens.
- **MEDIUM**: `RateLimiter._buckets` grows forever by unique IP key. Each deque is bounded by time, but the dict itself has no eviction policy. For an internet-facing helper endpoint, that is a real memory-growth vector under scans or abuse.
- **MEDIUM**: The PostHog queue intentionally drops on `queue.Full`, but there is no counter/log for dropped events. Best-effort is fine; silent loss at scale is harder to diagnose.
- **MEDIUM**: Plan 04's `pyproject.toml` change is repo-wide behavior change. It may be correct, but it affects all existing slow tests, not just Phase 78. The plan does not show CI invocation changes proving those tests still run where intended.
- **MEDIUM**: The "legacy unchanged" testing is underpowered. One export route and one puzzle-image status check do not meaningfully cover the app-wide handler regression risk. In particular, there is no legacy-route test that exercises **validation failure**, which is exactly where the new global handler changes behavior.
- **LOW**: `init_search_api()` is not described as idempotent. In dev reload or repeated mounting on the same app, duplicate route/handler registration may happen.
- **LOW**: The RED strategy in 78-01 is intentionally noisy. It's defensible, but it guarantees branch CI stays red between Plans 01 and 03, which is process friction if anyone needs to stack work or review incrementally.
- **LOW**: The statelessness test (`byte-identical modulo timestamp`) is useful but not sufficient by itself. It proves one path is stable; it does not fully prove absence of hidden session/global influences.

**Suggestions**

- Split "client identity for rate limiting" from "loopback eligibility for localhost-only."
- For rate limiting, use a **trusted real-client IP resolver** that only consults XFF when the direct peer is a trusted proxy. Do not reuse the current direct-peer-only `get_client_ip()` for HARDEN-01.
- Do not install validation/APIError handlers globally on the shared app if legacy isolation matters. Use a dedicated router/sub-app, or perform envelope rewriting inside the new endpoints only.
- Move `APIError` out of `web.api_hardening` into a neutral/shared location, or make `validate_filter_values()` return structured failure info instead of raising a web-layer exception.
- Add a test for "legacy route validation failure remains unchanged" if the app-global handler approach survives.
- Add a test for the proxy trust boundary: loopback peer + attacker-controlled XFF should fail unless the proxy sanitization contract is explicitly guaranteed.
- Add eviction for stale rate-limit buckets, even if coarse. A periodic sweep or size cap is enough for v1.
- Add a test for the zero-results Responsa downgrade case. If core cannot currently surface it, note that as a requirement gap instead of assuming it away.
- For PostHog, keep silent failure on network errors, but count/log dropped events when `_event_queue` is full.
- Reconsider the repo-wide `addopts` change unless CI config is updated in the same phase.

**Risk Assessment**

**HIGH**. The plans are close, but two structural issues are serious: the current per-IP strategy likely fails under nginx, and the current exception-handler strategy likely affects routes the phase promises not to touch. Those are not polish issues; they strike at requirement compliance. Fix those two areas and the rest of the phase becomes much more likely to land cleanly.

---

## Consensus Summary

The two reviewers diverge on overall risk (Gemini: LOW; Codex: HIGH). Gemini reads the plans as security-conscious and well-structured; Codex zooms in on two architectural mismatches that put core success criteria at risk. The substantive concerns are dominated by Codex; Gemini's review functions as a sanity check confirming the design is sound *if* Codex's structural issues are resolved.

### Agreed Strengths (both reviewers)
- **Reusable hardening shell** — clean split between `web/api_hardening.py` (generic infra) and `web/search_api.py` (route-specific).
- **Sliding-window rate limiter** with honest `Retry-After` is a real upgrade over the existing fixed-window puzzle limiter.
- **Strict Pydantic enforcement** (`extra='forbid'`) on both request and filter models — strong defense against mass-assignment.
- **Non-blocking PostHog observability** via daemon thread + queue — telemetry never blocks a request.
- **Test surface scope** — Wave 0 RED tests + soak + legacy spot check + handler unit tests is thorough.
- **No payload contents logged** to PostHog — D-14 discipline holds across the design.

### Agreed Concerns (raised by both)
1. **XFF / proxy trust under nginx** (Gemini MEDIUM, Codex HIGH×2). Both reviewers flag the `localhost-only` mode's `.split(',')[0]` first-hop check as exploitable if nginx does not sanitize/overwrite client-supplied `X-Forwarded-For`. Codex extends this concern to rate-limiting itself: behind nginx, `request.client.host` is `127.0.0.1` for every external request, so the per-IP limiter would collapse to a global limiter (HARDEN-01 break). **Fix scope: redesign the client-IP resolver to consult XFF when and only when the direct peer is a trusted proxy.**
2. **RateLimiter._buckets unbounded growth** (Gemini LOW, Codex MEDIUM). The dict has no eviction; over time, unique-IP keys accumulate. **Fix scope: periodic sweep or size cap; trivial to add.**

### Codex-Only HIGH Concerns (action required)
3. **`register_exception_handlers(target_app)` is app-global, not route-scoped.** Calling it from `init_search_api()` mounts the new `RequestValidationError` / `APIError` handlers on the SAME FastAPI app that hosts every legacy `/api/*` route. Any existing endpoint that currently produces a raw 422 dump on validation failure would now produce the new error envelope. This silently violates Success Criterion 2 ("existing `/api/*` routes unchanged") and D-18 ("Phase 78 does NOT modify `web/api.py`"). The legacy-immutability spot check (D-23) only tests happy paths and headers — it cannot catch this regression. **Fix scope: either (a) use a dedicated `APIRouter` / sub-app for the new handlers, OR (b) handle envelope rewriting INSIDE the new endpoints only (no global handler install), OR (c) keep the global handler but add a legacy-route validation-failure test that proves behavioral parity. Each option is non-trivial.**
4. **`shared -> web` dependency inversion in `validate_filter_values`.** `shared/fjms_service.validate_filter_values()` would need to raise `APIError` from `web.api_hardening`. Service-layer code importing web-layer exceptions weakens the dual-app architecture (desktop also imports `shared/fjms_service`). **Fix scope: move `APIError` to a neutral location (e.g., `shared/api_errors.py`), OR make `validate_filter_values()` return a structured `Optional[FilterValidationFailure]` and let the handler raise.**

### Codex-Only MEDIUM Concerns
5. **HARDEN-03 zero-results gap.** The current plan hoists the `query_downgraded` warning from `results[0]`, but if a Responsa cascade fires AND the query then yields zero results, the warning is lost. Requirement 5 ("never hidden inside the first result item") implicitly requires the warning surface even with empty results.
6. **Repo-wide `pyproject.toml addopts` impact.** Adding `addopts = -m 'not slow'` globally changes default test invocation for the whole project, not just Phase 78's soak test. CI invocation handling needs to be confirmed.
7. **Legacy-unchanged test underpowered.** Two routes covered (export/json + puzzle_image), neither exercising validation failure — the exact path Codex's #3 concern would regress.
8. **PostHog queue.Full silent drop.** No counter/log for dropped events; hard to diagnose under load.

### Gemini-Only LOW Concerns (already partially mitigated)
9. **PostHog capture for structural validation errors** (Pydantic-caught): events fire only inside the handler, so `invalid_request` errors from Pydantic don't generate PostHog events. Gemini's suggestion to centralize capture in the global handler dovetails with Codex's #3 (and is blocked by it — fixing one informs the other).
10. **Salt persistence race on Windows.** `os.replace` semantics on some Windows environments. Edge case; current tmp+rename pattern is acceptable for v7.10.
11. **`init_search_api()` not declared idempotent.** Re-mounting on dev-reload could double-register routes.

### Divergent Views
- **Overall risk.** Gemini: LOW (design is conservative; XFF is the only meaningful concern). Codex: HIGH (two structural issues threaten requirement compliance). The divergence is real but resolvable: Gemini's reading is correct *if* Codex's structural fixes land; Codex's reading is correct *as the plans currently stand*. Treating Codex's #3 (global handler scope) and #1 (rate-limit IP source) as blockers gives a converged answer.
- **The TDD RED strategy** (78-01). Codex flags as LOW process-friction. Gemini does not mention it. Defensible either way; not a phase-blocker.

### Recommendation for `/gsd-plan-phase 78 --reviews`
Treat as **revision required** before execution. The two HIGH structural concerns (Codex #3 global handler scope, Codex #1 rate-limit client-IP source) must be resolved in the plans before Wave 1 implementation begins. The XFF-sanitization concern (both reviewers, agreed concern #1 above) can be addressed inside Plan 02 Task 1 with a stricter `_is_loopback_request` (require ALL XFF entries loopback; drop the "first hop only" rule). The `shared -> web` dependency inversion (Codex HIGH #4) is also a planning-level fix — moving `APIError` to `shared/api_errors.py` is a one-line decision.

The remaining MEDIUM/LOW items are pragmatic execution-time fixes that can be folded into the revision pass:
- RateLimiter eviction (size cap or periodic sweep)
- Zero-result Responsa warning hoist (move from `results[0]` to a dedicated meta channel)
- Legacy-validation-failure test (one new test in `test_api_legacy_unchanged.py`)
- PostHog drop counter (one `_dropped_events: int = 0` field on the queue manager)
- `pyproject.toml addopts` impact note in CI invocation
