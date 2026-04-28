---
phase: 78
reviewers: [gemini, codex]
reviewed_at: 2026-04-28T10:35:00Z
plans_reviewed:
  - 78-01-PLAN.md
  - 78-02-PLAN.md
  - 78-03-PLAN.md
  - 78-04-PLAN.md
revision_round: 2
note: |
  Second cross-AI review pass — invoked after `/gsd-plan-phase 78 --reviews` rewrote the
  plans to address the first round of feedback. Claude CLI skipped per project memory
  (we ARE Claude — review independence requires a different model). Gemini and Codex
  both run with their default models (no review.models.* override in .planning/config.json).

  Headline result: Gemini APPROVES (LOW risk). Codex flags MEDIUM risk with one HIGH
  concern (filter-validation pipeline alignment) plus 4 MEDIUM follow-ups. The two
  HIGH structural blockers from round 1 (global handler scope, shared→web inversion)
  are confirmed resolved by both reviewers. Round-2 issues are mostly *new* gaps
  introduced or sharpened by the revision, not residual round-1 issues.
---

# Cross-AI Plan Review — Phase 78 (Revision Round 2)

## Gemini Review

### Summary
The revised implementation plans for **Phase 78** are excellent. The revision land cleanly, and all four **HIGH structural concerns** from the previous review have been definitively resolved in the plan text through specific architectural improvements.

The introduction of `shared/api_errors.py` correctly decouples the exception layer from the web framework, and the per-endpoint envelope-wrapping strategy provides a robust solution for preserving legacy route behavior while hardening the new API. The two-helper approach to IP resolution (`_resolve_rate_limit_key` and `_is_loopback_request`) demonstrates high security maturity, correctly handling trusted proxy chains and strict loopback gating.

### Strengths
*   **Dependency Inversion Fix:** Moving `APIError` to a neutral `shared/api_errors.py` module is a textbook fix for the `shared -> web` back-reference issue, ensuring the dual-app architecture remains clean.
*   **Surgical Hardening:** Replacing global exception handlers with the per-endpoint `_build_envelope_response` helper (invoked inside manual JSON parsing blocks) ensures zero impact on legacy `/api/*` routes while maintaining strict schema control for the new Search API.
*   **Robust Proxy Handling:** The right-most-untrusted-XFF algorithm in `_resolve_rate_limit_key` and the every-entry-loopback requirement in `_is_loopback_request` are professionally designed and resilient against common XFF spoofing attacks.
*   **Full Spectrum Observability:** The addition of `get_dropped_event_count()` and the explicit capture of structural validation errors in PostHog fulfil **HARDEN-05** much more comprehensively than the previous iteration.
*   **Resilient Core Surface:** The use of a thread-local signal in `genizah_core.py` is an elegant way to ensure that combinatorial cascade warnings are surfaced even when a query yields zero results, closing a significant requirement gap.
*   **Conservative Test Strategy:** The revision to `pyproject.toml` correctly preserves the project's default test behavior while still allowing for a strict-markers-compliant slow-test opt-in.

### Concerns

*   **[LOW] `_LAST_RESPONSA_DOWNGRADE` Cleanup:**
    *   While the `_consume` helper correctly clears the thread-local attribute, if a search crashes *before* the handler can consume the signal, the value might technically persist on that worker thread.
    *   **Severity:** Low (unlikely to cause meaningful cross-request leakage in a standard FastAPI/uvicorn pool).

*   **[LOW] Module-level state in tests:**
    *   The `_INITIALIZED_APPS` set in `web/search_api.py` persists across tests. While the `bare_app` fixture creates fresh instances, the module-level set will grow throughout the test session.
    *   **Severity:** Low (negligible memory impact).

### Suggestions

*   **Proactive Thread-Local Reset:** Consider calling `_consume_last_responsa_downgrade()` (or a new internal reset helper) at the very beginning of `SearchEngine.execute_search`. This ensures a clean state and prevents any stale warning from a previous failed/unconsumed request from being "inherited" by a new query on the same thread.
*   **Pydantic Model Documentation:** Since the search endpoint now uses `await request.json()` followed by `SearchRequest.model_validate`, the standard FastAPI/Swagger UI will not automatically detect the request schema. This is acceptable for an internal helper API, but a manual `openapi_extra` or similar hint could be added in Phase 82 if documentation parity is ever desired.

### Risk Assessment: LOW
The implementation risk is now **LOW**. The revised plans have successfully converted every high-severity architectural risk into a proven pattern. The Wave 0 TDD approach ensures that the strict IP resolution and legacy-parity contracts are verified before the implementation is finalized. The design is well-isolated, observable, and strictly follows the project's engineering standards.

**Verdict:** APPROVED. Proceed with execution. No further revisions required.

---

## Codex Review

### Summary

The revision is materially better and it does resolve the two biggest prior blockers cleanly: the legacy-route/global-handler problem is addressed by moving envelope handling into the new endpoint, and the `shared -> web` dependency inversion is fixed by introducing `shared/api_errors.py`. The XFF/rate-limit redesign is also pointed in the right direction, but it does not land fully cleanly in plan text because the revised tests/specs are internally inconsistent about multi-hop XFF semantics, and the new filter-validation design introduces a fresh API-07 risk. So the four prior HIGH concerns are mostly addressed, but not all of them are fully closed without ambiguity.

### Strengths

- The `shared/api_errors.py` split is the right fix. It is genuinely framework-free and removes the bad `shared -> web` import direction.
- Replacing global exception-handler installation with per-endpoint envelope handling is the correct response to the legacy-route regression concern.
- Separating `_resolve_rate_limit_key()` from `_is_loopback_request()` is the right architectural correction. Those are different trust decisions and should not share one helper.
- The strict loopback rule for `localhost-only` is directionally correct and matches the intended semantics better than the earlier RFC1918 approach.
- Adding rate-limit bucket eviction and a dropped-PostHog-event counter meaningfully improves the original hardening design.
- Removing the repo-wide `addopts = -m "not slow"` default-exclude was the right call; that would have been an unnecessary project-wide behavior change.
- The revised legacy smoke test now explicitly targets validation-failure behavior, which is exactly the gap the previous review called out.

### Concerns

- **[HIGH] The new `validate_filter_values()` design is not actually aligned with the real FJMS filter pipeline, so API-07 is still at risk.**
  - `shared/fjms_service.get_filter_sys_ids()` already accepts qualified domain names and parent-domain matching via `unqualify_domain_name(...)`, but the planned validator checks only `get_all_domains()` bare `domain` values. That will falsely reject valid filter tokens such as qualified/UI-facing domain forms.
  - The materials branch is worse: the plan explicitly says `if valid_materials and v not in valid_materials`, which means an empty material vocabulary becomes "allow all." Combined with `get_filter_sys_ids()` returning `None` when `_conn is None`, a materials-only filter can silently become unfiltered search, which is exactly what API-07 says must not happen.

- **[MEDIUM] The XFF/rate-limit fix is still internally inconsistent in the revised plans.**
  - The plan text and examples disagree on the multi-hop case. One part says `client_ip` should win; another example asserts `hop2.ip`; the helper definition says "right-most untrusted entry."
  - For the current single-nginx deployment, "right-most untrusted" is fine. The problem is that the RED tests and explanatory comments do not consistently lock that rule, so the implementation target is still fuzzy.

- **[MEDIUM] The new thread-local Responsa warning channel can leak between requests.**
  - The plan introduces `_LAST_RESPONSA_DOWNGRADE` as thread-local, which is the right scope class, but the planned `web/search_api.py` only consumes it on the success path.
  - If `execute_search()` sets the downgrade signal and then raises before the handler consumes it, the same worker thread can carry that stale warning into the next request.
  - This should be cleared before each search and/or consumed in a `finally`, not only after successful result processing.

- **[MEDIUM] `init_search_api()` idempotency via `_INITIALIZED_APPS: set[int]` is brittle, and the global limiter state will make tests flaky.**
  - `id(app)` can be reused after GC, so a fresh bare FastAPI app in tests can theoretically be mistaken for an already-initialized one.
  - `_rate_limiter` is module-global, so bucket state persists across test apps unless explicitly reset. The revised plans do not account for that.
  - This is a new issue introduced by the idempotency/test-isolation fix.

- **[MEDIUM] Plan 04 is still contradictory about slow-test behavior.**
  - The revised plan correctly removes repo-wide `addopts`, but then claims the new soak tests are "opt-in via `-m slow`."
  - They are not opt-in. With marker registration only, `tests/test_search_api_soak.py` will run in the default `pytest tests/` path and therefore in current CI unless the workflow is updated.
  - Existing `tests/e2e/test_performance.py` was already in default collection; the newly introduced soak tests are the real CI-behavior change.

- **[LOW] `wrap_endpoint()` is effectively a no-op marker, so the "build once, inherit thrice" goal is only partially met.**
  - The real reusable piece is `_build_envelope_response()`.
  - As written, Phases 79/80 still need to hand-roll the same `try/except/finally` structure in each endpoint, which creates drift risk.

- **[LOW] The route-level localhost tests still do not fully pin the intended behavior.**
  - `test_mode_gate_localhost_only_clean_xff_chain` explicitly allows either `200` or `403`.
  - `test_mode_gate_localhost_only_loopback_direct` patches `_is_loopback_request` instead of exercising the real integration.
  - Helper-level tests are good, but the endpoint-level contract is still under-specified.

- **[LOW] Several verification commands are Unix-oriented (`grep`, `awk`, `head`, `tail`) despite the stated PowerShell/Windows environment.**
  - That is execution friction, not a design blocker, but it should be cleaned up.

### Suggestions

- Make `validate_filter_values()` normalize through the same domain logic as `get_filter_sys_ids()` instead of validating against `get_all_domains()` raw output. At minimum, support qualified domains and parent-domain names explicitly.
- Do not allow materials validation to degrade to "allow all" when the material vocabulary cannot be loaded. Reject the request instead, or reject all filter-based requests when FJMS is unavailable.
- Clear the Responsa thread-local at request start and consume/clear it again in a `finally` after `execute_search()`.
- Replace `_INITIALIZED_APPS: set[int]` with an app-bound marker (`target_app.state`) or a `weakref.WeakSet`, and provide a test-reset path for the module-global rate limiter.
- Clean up the XFF rule everywhere so the prose, RED tests, and helper implementation all describe the same algorithm. If multi-hop is out of scope, say so and drop the contradictory test/comments.
- Decide CI behavior for the new soak tests explicitly. Either:
  - keep them in default test runs and stop calling them opt-in, or
  - update the workflow so the main pytest step excludes `slow` and a dedicated job runs `-m slow`.
- Make the route-level localhost tests deterministic instead of accepting both pass and fail.

### Risk Assessment

**MEDIUM.** The revision fixed the most dangerous structural problems from the prior review, especially the global-handler scope issue and the `shared -> web` dependency inversion. That drops the overall risk substantially. The remaining problems are not as fundamental, but they are still real: the filter-validation plan can violate API-07, the new thread-local warning path can leak across requests, and the slow-test story is still internally inconsistent. This is close to execution-ready, but I would want one more revision pass on those points before calling it clean.

---

## Consensus Summary

Gemini and Codex agree the revision **resolved the two biggest structural blockers** from round 1 (global handler scope, `shared → web` inversion) and that the new architecture is fundamentally sound. They diverge on overall risk: Gemini reads as APPROVED / LOW; Codex reads as MEDIUM with one residual HIGH concern (filter-validation pipeline alignment) and four MEDIUM follow-ups. As in round 1, Codex zooms in on requirement-compliance details that Gemini's higher-altitude review treats as out of scope.

### Agreed Strengths (both reviewers)

- **`shared/api_errors.py` split** — Concern #3 fixed cleanly. Both reviewers explicitly call out the dependency-inversion correction as the right architectural move.
- **Per-endpoint envelope handling** — Concern #2 fixed. Both reviewers confirm legacy `/api/*` routes are no longer at risk from a global handler installer.
- **Two-helper IP resolution** — `_resolve_rate_limit_key` (rate limit) vs `_is_loopback_request` (mode gate) is the right architectural separation. Both endorse.
- **Strict every-XFF-entry-must-be-loopback rule** — Concern #4 fixed in principle. Both endorse.
- **RateLimiter bucket eviction (TTL + last_seen)** and **PostHog dropped-event counter** — both reviewers endorse as meaningful hardening upgrades.
- **`pyproject.toml addopts` revert** — both endorse the decision NOT to apply repo-wide default-exclude (Concern #7 correctly addressed).

### Agreed Concerns (raised by both)

1. **Thread-local Responsa downgrade signal can leak across requests** (Gemini LOW, Codex MEDIUM). Both reviewers flag the same root cause: `_LAST_RESPONSA_DOWNGRADE` is set unconditionally inside `execute_search` but consumed only on the success path in `web/search_api.py`. If `execute_search` raises before the handler reads it, the next request on the same worker thread inherits a stale warning. **Fix scope:** clear the thread-local at request start AND consume/clear in a `finally` block, not only after successful result processing. Gemini's specific suggestion: call `_consume_last_responsa_downgrade()` (or a new internal reset helper) at the very beginning of `SearchEngine.execute_search`.

2. **`_INITIALIZED_APPS` module-level state** (Gemini LOW, Codex MEDIUM). Both reviewers flag the persistence-across-tests issue. Codex extends with the correctness concern: `id(app)` can be reused after GC, theoretically letting a fresh bare FastAPI app inherit the "already initialized" flag from a destroyed one. **Fix scope:** replace `set[int]` with `weakref.WeakSet` (auto-cleans GC'd apps), or store an attribute on `target_app.state` for app-bound idempotency. Add a test-reset path for the module-global `_rate_limiter` while we're there.

### Codex-Only HIGH Concern (action required)

3. **`validate_filter_values()` is misaligned with the real FJMS pipeline — API-07 still at risk.** Two sub-issues:
   - **Domain validation is too narrow.** `shared/fjms_service.get_filter_sys_ids()` already supports qualified domain names and parent-domain matching via `unqualify_domain_name(...)`, but the planned validator checks only `get_all_domains()` bare values. Valid UI-facing qualified domain forms would be falsely rejected.
   - **Materials validation degrades unsafely.** The planned `if valid_materials and v not in valid_materials` means an empty material vocabulary becomes "allow all." Combined with `get_filter_sys_ids()` returning `None` when `_conn is None`, a materials-only filter can silently become unfiltered search. API-07 explicitly says this must not happen.

   **Fix scope (Plan 78-03):** rewrite the validator to (a) normalize through the same domain logic as `get_filter_sys_ids()` (support qualified + parent-domain forms), and (b) reject — not silently degrade — when material vocabulary cannot be loaded or when FJMS is unavailable.

### Codex-Only MEDIUM Concerns

4. **XFF rate-limit rule is internally inconsistent across plan text.** The prose, RED tests, and helper definition disagree on the multi-hop case (one example says `client_ip`, another says `hop2.ip`, the helper says "right-most untrusted entry"). For single-nginx deployment "right-most untrusted" is fine, but the contradictions leave the implementation target fuzzy.
   **Fix scope (Plan 78-01 + 78-02):** pick one rule, propagate to every test and comment. If multi-hop is out of scope, say so and drop the contradictory test/comments.

5. **Soak-test CI story is still inconsistent.** Plan 04 removed the repo-wide `addopts = -m 'not slow'`, but text still claims the new soak tests are "opt-in via `-m slow`." Without the global filter, `tests/test_search_api_soak.py` runs in default `pytest tests/`. The phase needs to either (a) accept that and stop calling it opt-in, or (b) update the CI workflow to add a separate `-m slow` job.
   **Fix scope (Plan 78-04):** decide explicitly. Recommended: option (b) — update CI in the same plan and document a dedicated slow-test job.

### Codex-Only LOW Concerns

6. **`wrap_endpoint()` is a no-op marker.** The real reusable piece is `_build_envelope_response()`. Phases 79/80 still need to hand-roll `try/except/finally` per endpoint, creating drift risk. *Could* be tightened to actually own the boilerplate, but acceptable for v1.

7. **Route-level localhost tests under-specified.** `test_mode_gate_localhost_only_clean_xff_chain` accepts `200 OR 403`; another test patches `_is_loopback_request` instead of exercising the real integration. Helper-level tests are solid; endpoint-level contract is fuzzy.

8. **Verification commands are Unix-oriented** (`grep`, `awk`) despite stated Windows environment. Execution friction, not a design blocker.

### Gemini-Only LOW Concern

9. **Pydantic schema not exposed to FastAPI/Swagger UI** because the endpoint uses `await request.json()` + manual `model_validate`. Acceptable for an internal helper API; future Phase 82 polish.

### Divergent Views

- **Overall risk.** Gemini APPROVED / LOW; Codex MEDIUM with one HIGH (filter-validation). The pattern matches round 1: Gemini operates at the architectural-pattern level (where the revision is genuinely clean); Codex operates at the requirement-compliance level (where the filter-validation gap directly threatens API-07). Both readings are internally consistent. **Resolution:** treat Codex's HIGH (filter-validation pipeline alignment) as a blocker for round 3; the MEDIUM/LOW items can be folded in alongside.

### Recommendation for `/gsd-plan-phase 78 --reviews` (round 3)

Treat as **revision required** before execution, but the surface is much smaller than round 1:

**Required (HIGH + agreed-MED):**
- Codex HIGH #3 — filter-validation pipeline alignment with `get_filter_sys_ids()`; reject (don't degrade) on missing vocabulary
- Agreed concern #1 — Responsa thread-local clear on request start + `finally`
- Agreed concern #2 — `_INITIALIZED_APPS` via `weakref.WeakSet` or `target_app.state`; rate-limiter test-reset path

**Recommended (Codex MEDIUM):**
- #4 — lock one XFF rule across plan text, RED tests, and comments
- #5 — decide CI soak-test story explicitly (recommended: update CI workflow in Plan 04)

**Optional (LOW polish):**
- #6 — make `wrap_endpoint` actually own the boilerplate (or document acceptance of duplication)
- #7 — tighten route-level localhost tests to deterministic outcomes
- #8 — Windows-friendly verification command alternates
- #9 — Pydantic schema docs (defer to Phase 82)

A focused round-3 revision pass should resolve the HIGH and the two agreed MEDs in a single iteration. The rest is polish.
