---
phase: 80
reviewers: [gemini, codex]
reviewed_at: 2026-05-01T00:00:00Z
plans_reviewed: [80-01-PLAN.md, 80-02-PLAN.md, 80-03-PLAN.md, 80-04-PLAN.md]
---

# Cross-AI Plan Review — Phase 80

## Gemini Review

### 1. Summary
The implementation plans are exceptionally well-structured and demonstrate a mature understanding of the project's hardening requirements (Phase 78) and API consistency goals. By strictly mirroring the service-layer abstraction found in `/api/browse` and leveraging the `@wrap_endpoint` decorator, the plans ensure that `/api/parallels` is not a "siloed" feature but a first-class citizen of the Search API. The emphasis on locator round-trip validation and independent rate-limiting buckets addresses the most significant technical risks (contract breakage and resource exhaustion) upfront.

### 2. Strengths
- **Hardening Shell Reuse:** Using the `@wrap_endpoint` decorator (Plan 03) ensures that PostHog observability, error envelopes, and global gating (SEARCH_API_MODE) are applied consistently without code duplication.
- **Resource Isolation (D-05):** The decision to use a *third* independent `RateLimiter` bucket for parallels is critical. Parallels searches are CPU-intensive; isolating their rate limit prevents a "noisy neighbor" effect where heavy composition searches could starve standard `/api/search` or `/api/browse` traffic.
- **Service Layer Abstraction (Plan 02):** Moving the logic to `shared/parallels_service.py` and using `run_in_executor` correctly handles the CPU-bound nature of the Tantivy-based composition logic, keeping the FastAPI event loop responsive.
- **Locator Integrity (SC-4):** The inclusion of an env-gated test to verify that locators emitted by parallels can be resolved by the browse endpoint is the ultimate validation of the "Search Ecosystem" vision.
- **Validation Precision:** Defining specific error codes (`composition_too_long`, `composition_required`) rather than generic 400 errors improves the developer experience for API consumers.

### 3. Concerns
- **LOW — Performance vs. Rate Limit:** With a 20,000-character cap (D-06), a fuzzy parallels search can be extremely slow. While `run_in_executor` prevents event-loop blocking, a default limit of 30 requests might still saturate the server's CPU cores if multiple users submit large texts simultaneously.
- **MEDIUM — Flattening logic in Plan 02:** Plan 02 mentions keeping the top 200 groups and "flattening back to rows." If the Phase 77 serializer (`serialize_parallels_payload`) expects grouped data or a specific sort order to perform its own grouping, flattening it in the service layer might lead to redundant processing or grouping errors.
- **LOW — Filtered Key enforcement:** While the plan states the `filtered` key must always be present (D-04), the responsibility is split between the Service Layer bundle and the Serializer. If the Serializer isn't updated to handle an empty list as a default, SC-2 might fail.

### 4. Suggestions
- **Concurrency Tuning:** Consider adding a semaphore or a smaller thread pool specifically for the `run_in_executor` calls in `parallels_service.py` to prevent composition searches from exhausting the global worker pool.
- **Clarify Serializer Contract:** Ensure `serialize_parallels_payload` from Phase 77 is explicitly checked for how it handles the "200-group" input. It is usually cleaner if the service layer returns a `List[Group]` and the serializer handles the JSON transformation, rather than flattening and re-grouping.
- **Documentation:** Ensure the `composition_too_long` error documentation in `CLAUDE.md` explicitly mentions the 20,000-character limit so consumers don't have to guess.
- **PostHog Property Consistency:** D-09 mentions using the same PostHog keys as `/api/search` but with a different value space. Ensure that dashboards are aware of the "parallels" value to avoid skewing "exact match" metrics for standard searches.

### 5. Risk Assessment: LOW
The risk is low because the implementation follows the "Hardening Shell" pattern established in Phases 78 and 79. The logic for Tantivy/Regex searching is already proven in the `searcher` class; this phase is primarily about exposure and contract enforcement. The dependency on Phase 79 (locator validation) is correctly identified, and the test plan (Plan 04) is exhaustive enough to catch regressions in the round-trip contract. The primary remaining risk is purely performance-related under heavy load, which is mitigated by the independent rate limiter.

---

## Codex Review

### Overall Summary
The plans are coherent and mostly align with the Phase 80 goal: they reuse the hardened API shell, keep serialization centralized, avoid the lab engine path, and preserve the `/api/browse` locator contract. The main risks are around ambiguity in the 200-group cap, response-size control for `filtered`, exact warning shape, and ensuring the POST body is parsed inside the wrapped endpoint so malformed JSON and Pydantic errors use the API envelope.

### Plan 01 — Foundations

**Summary:** Low-risk foundational work. It correctly extends the public error/warning taxonomy and updates operator docs, but it should also pin warning shape expectations.

**Strengths:**
- Preserves the existing Phase 78/79 error-code contract.
- Adds only the two composition-specific errors required by D-06.
- Documents that Phase 78 env vars apply to `/api/parallels`.

**Concerns:**
- **LOW:** `WARNING_CODES` is not currently the enforcement point for warning payload shape; adding `truncated_to_200` alone does not define whether warnings are strings or objects.
- **LOW:** Docs mention shared env vars, but D-05's third independent bucket means aggregate per-IP allowance increases again.

**Suggestions:**
- Document the warning representation — explicitly preserve the existing string convention.
- Add a short note that `/api/search`, `/api/browse`, and `/api/parallels` have independent buckets under the same ceiling.

**Risk Assessment: LOW.** Small, isolated changes with little behavioral blast radius.

---

### Plan 02 — Service Layer

**Summary:** The service split is the right architecture, but the cap/filter behavior needs tighter definition. This layer is where response size and grouping semantics can drift from the serializer.

**Strengths:**
- Keeps UI/NiceGUI out of the service path.
- Uses `search_composition_logic`, correctly excluding `lab_composition_search`.
- Keeps `serialize_parallels_payload` as handler-owned, preserving serializer single-source-of-truth.
- `max_freq=None -> inf` is a reasonable internal adapter as long as the response echoes `None`.

**Concerns:**
- **HIGH:** The 200-group cap is ambiguous for `filtered_results`. If only `main_results` is capped, a high-frequency query can still return a very large `filtered` payload.
- **MEDIUM:** Flattening capped groups back to rows before serializer regrouping works, but it couples the service to the serializer's private `_group_parallels_by_sys_id` behavior.
- **MEDIUM:** No timeout or concurrency guard is planned for a potentially expensive 20,000-char composition search.
- **MEDIUM:** `boundary_options` is named in the bundle, but the core returns `boundary_stats`; the plan does not define the response echo shape.
- **LOW:** The service should preserve `partial`/interrupted status from `search_composition_logic` or deliberately discard it with a documented reason.

**Suggestions:**
- Define cap policy explicitly: cap main and filtered independently, or cap combined groups; test both.
- Return the original `max_freq` for serialization, not the internal `inf`.
- Consider a service-local helper for "cap grouped raw rows" instead of importing a private serializer helper, or make that helper public.
- Add a bounded timeout/semaphore if Phase 78 hardening is expected to protect executor health.
- Specify `boundary_options` shape explicitly (e.g. `{'boundary_mode': boundary_mode}` plus optional stats if intended).

**Risk Assessment: MEDIUM.** The shape is sound, but size/cap ambiguity and expensive execution are real operational risks.

---

### Plan 03 — Route Handler

**Summary:** The handler plan follows the browse endpoint precedent well. The biggest correctness issue is making sure JSON parsing and Pydantic validation happen inside the decorated function, not via FastAPI parameter binding.

**Strengths:**
- Reuses `wrap_endpoint`, `enforce_mode_gate`, and `RateLimiter` instead of duplicating hardening code.
- Defines a distinct `_parallels_rate_limiter`, matching D-05.
- Keeps filter resolution in the handler and service input narrowed to `restrict_sys_ids`.
- Short-circuits empty filter intersections without invoking Tantivy.
- Avoids modifying `/api/search` and `/api/browse`, reducing regression risk.

**Concerns:**
- **HIGH:** If `ParallelsRequest` is used as a FastAPI body parameter, malformed JSON/Pydantic errors will bypass the wrapper. It must manually `await request.json()` and construct the model inside the wrapped handler.
- **MEDIUM:** Text cap must be enforced on the stripped text. Passing the unstripped original to the service can defeat the cap with whitespace.
- **MEDIUM:** Warning shape for `truncated_to_200` is unspecified.
- **MEDIUM:** PostHog tests must patch the function used by `web.api_hardening.wrap_endpoint`, not the old `web.search_api.capture_api_event` import path.
- **LOW:** Decorator order matters: FastAPI should register the wrapped function, matching the existing browse pattern.

**Suggestions:**
- Add `ParallelsRequest` with `Field(ge=2, le=20)` for `chunk_size`; consider `max_freq > 0` validation when not `None`.
- Set `captured_state['mode']` only after successful validation; set `result_count` from the serialized main count.
- Use the same generic item-to-browse query helper in tests for search and parallels to prove "no per-producer adjustment."

**Risk Assessment: MEDIUM.** Mostly straightforward, but one FastAPI binding mistake would break the error-envelope and observability requirements.

---

### Plan 04 — Tests

**Summary:** The test plan is broad and mostly appropriate. It should add a few sharper assertions around cap behavior, stripped text, PostHog mode capture, and service short-circuiting.

**Strengths:**
- Mirrors the established `test_browse_api.py` fixture style.
- Covers happy paths, validation, hardening, cap behavior, statelessness, and round-trip.
- Uses mocked `search_composition_logic`, avoiding a Tantivy dependency for unit tests.
- Includes real env-gated round-trip coverage for SC-4.

**Concerns:**
- **HIGH:** Cap tests mention over/under 200 groups, but not whether `filtered` is capped. That is the main response-size risk.
- **MEDIUM:** "Unknown filter key" should expect `/api/search` parity, likely `invalid_request`, unless the contract explicitly changes to `unknown_filter_key`.
- **MEDIUM:** `max_freq None -> empty` needs a controlled mock that proves `None` became `inf` internally without echoing `inf` externally.
- **MEDIUM:** The real round-trip must skip cleanly when fixture data/indexes are unavailable, not only when an env var is absent.
- **LOW:** Source-grep tests for wrapper reuse are useful but brittle; pair them with behavioral envelope tests.

**Suggestions:**
- Add tests for exactly 20,000 chars, 20,001 chars, whitespace-only text, and "large whitespace around small text."
- Assert empty filter intersection returns `200`, `count=0`, `filtered=[]`, and service not called.
- Assert PostHog captures `endpoint='parallels'` and `mode='exact'|'variants'|'fuzzy'`.
- Add locator-readiness assertions for every result: usable `sys_id` plus either item `uid` or locator `p_num`.
- Test truncation warning shape and `WARNING_CODES` membership.

**Risk Assessment: MEDIUM.** Strong coverage overall, but without filtered-cap and parsing-envelope tests it could miss the most important regressions.

---

## Consensus Summary

### Agreed Strengths
*(Mentioned by both reviewers)*

1. **`@wrap_endpoint` reuse is correct** — both reviewers confirm the decorator-based approach correctly avoids duplicating try/except/finally, PostHog capture, and error envelope boilerplate.
2. **Third independent rate-limit bucket (D-05)** — both highlight this as critical for preventing parallels CPU load from starving `/api/search` and `/api/browse`.
3. **Service layer extraction (Plan 02)** — both agree `shared/parallels_service.py` + `run_in_executor` is the right pattern for CPU-bound sync work.
4. **Locator round-trip test (SC-4 / D-08)** — both note this is the key end-to-end validation of the search ecosystem contract.
5. **`composition_required` / `composition_too_long` precision** — both approve specific error codes over generic 400.

### Agreed Concerns
*(Raised by 2+ reviewers — highest priority)*

1. **MEDIUM — `filtered_results` cap ambiguity (Plans 02 + 04):** Codex raises this as HIGH on Plan 02 and HIGH on Plan 04. The 200-group cap is specified only for `main_results`. A large `filtered` payload is an unmitigated response-size risk. **Action:** Explicitly cap or document the filtered list policy; add a test.

2. **MEDIUM — Flattening → re-grouping coupling (Plan 02):** Both reviewers flag the "flatten capped groups back to rows" pattern as creating potential for serializer grouping mismatch. **Action:** Verify `_group_parallels_by_sys_id` is idempotent on already-grouped input, or return groups directly.

3. **MEDIUM — Request body must be parsed inside the wrapper (Plan 03):** Codex raises this as HIGH. If `ParallelsRequest` is a FastAPI dependency instead of manually constructed via `await request.json()`, malformed JSON bypasses the error envelope. The plan text specifies manual parsing — this must be confirmed in implementation. **Action:** The manual `try: body = await request.json()` + `ParallelsRequest(**body)` pattern from Plan 03 is correct; test explicitly verifies malformed JSON returns `invalid_request` envelope.

4. **LOW/MEDIUM — No timeout/concurrency guard for expensive searches (Plan 02):** Both note the 20,000-char fuzzy search is the CPU bottleneck that the rate limiter alone may not adequately protect against. **Action:** Document explicitly that v7.10 accepts this risk; Phase 81 can add a semaphore if load testing warrants it.

### Divergent Views

- **Gemini** considers the overall risk LOW (hardening shell pattern is proven, execution risk is purely performance). **Codex** rates Plans 02–04 as MEDIUM due to the filtered-cap and body-parsing correctness risks. Codex's more specific concerns around `filtered_results` cap and FastAPI parameter binding are the more actionable findings.

- **Gemini** suggests returning `List[Group]` from the service to the serializer. **Codex** agrees the coupling is a problem but accepts flat rows if the round-trip through `_group_parallels_by_sys_id` is confirmed idempotent. The plan's approach (flat rows → serializer re-groups) is correct per Phase 77 D-14; no change needed unless the serializer has ordering expectations.

---

## Planner Action Items

Before executing, address these items from the consensus:

1. **Plan 02:** Explicitly define cap policy for `filtered_results` (cap at 200 groups or leave uncapped with a documented rationale). If uncapped, document the decision in CONTEXT.
2. **Plan 03:** Confirm the `await request.json()` manual parse pattern is preserved (not replaced by FastAPI body injection). The plan specifies this correctly — just don't let the executor cut corners.
3. **Plan 04:** Add test for `filtered` cap behavior — whether it's capped or not, assert the expected behavior explicitly.
4. **Plan 04:** Add boundary test: exactly `COMPOSITION_LENGTH_CAP` chars (should pass), `COMPOSITION_LENGTH_CAP + 1` chars (should reject).

To incorporate feedback:
```
/gsd-plan-phase 80 --reviews
```
