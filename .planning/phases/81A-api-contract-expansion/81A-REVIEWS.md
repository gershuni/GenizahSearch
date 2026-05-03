---
phase: 81A
reviewers: [gemini, codex]
reviewed_at: 2026-05-03
plans_reviewed:
  - 81A-01-PLAN.md
  - 81A-02-PLAN.md
  - 81A-03-PLAN.md
  - 81A-04-PLAN.md
  - 81A-05-PLAN.md
---

# Cross-AI Plan Review — Phase 81A

## Gemini Review

This is a high-quality, technically rigorous plan set that perfectly aligns with the Phase 81A requirements and the 2026-05-02 rescope decisions. The wave-based execution strategy correctly manages file-write collisions, and the technical implementation of the Responsa cascade "effective" state via a structured thread-local channel is an elegant solution to a complex observability requirement.

### 1. Summary
The plan set provides a comprehensive path to modernizing the `/api/search` contract. It replaces the legacy `mode` field with a UI-aligned `search_mode` enum, implements strict cross-field validation using Pydantic `model_validator`, and introduces a robust `request` echo block in response envelopes. The strategy for capturing effective Responsa options via `genizah_core.py` thread-locals ensures the Claude skill (Phase 81B) can detect server-side downgrades with high fidelity.

### 2. Strengths
- **Observability Fidelity:** The introduction of `_LAST_RESPONSA_DOWNGRADE_META` in Plan 02 is excellent. It allows the API to report structured boolean states for effective options, moving beyond simple string parsing of warnings.
- **Surgical Hard Cutover:** Plan 01's override of the Pydantic `ValidationError` to provide a specific "unknown field 'mode' — use search_mode instead" message is a thoughtful touch for skill authors.
- **Wave-Based Sequencing:** The distribution of tasks into four waves (1: Models, 2: Serialization, 3: Observability, 4: Verification) correctly handles dependencies on `web/search_api.py` and `genizah_core.py` without risking merge conflicts.
- **Correct Variants Wiring:** Plan 01 correctly maps `exact` and `variants` to distinct internal modes, ensuring the API actually exposes the behavioral differences between orthographic expansion tiers.
- **Regression Integrity:** Plan 04's decision to rewrite `tests/test_search_api.py` in-place preserves git history while migrating the entire Phase 78 hardening suite to the new contract.

### 3. Concerns
- **Core Variable Scope (LOW):** Plan 02 (Task 1, Step C) calls `_set_last_responsa_downgrade_meta` using local variables `variants_on`, `ja_on`, etc., at `genizah_core.py:7657`. While these are defined inside the Responsa branch, the plan assumes they are in scope. If a request returns early (e.g., via the line-break syntax check at 7284), these might be unbound. *Mitigation: The plan already includes a `read_first` and a note to replicate the call in early-return branches if needed.*
- **PostHog Property Consistency (LOW):** Plan 03 adds `search_mode_value` to the PostHog event. If the request fails Pydantic validation (e.g., `invalid_request`), `req.search_mode` won't be bound. Plan 03 handles this by initializing `posthog_search_mode_value = None`, which is correct, but the resulting PostHog data will have a "hole" for structural errors. This is acceptable for an internal API.

### 4. Suggestions
- **Model Location:** In Plan 01, Step A, consider placing the new Pydantic models in `web/search_api.py` as planned, as the file is currently manageable. If it exceeds 1000 lines after this phase, a subsequent refactor to `web/search_models.py` would be appropriate.
- **Error Code Precision:** Ensure that Plan 05's matrix tests explicitly verify the 422 vs 400 distinction for the `limit` field. Since `limit` is now a Pydantic `Field` constraint, it will trigger a 422 `invalid_request` (via the search_endpoint's wrapper), whereas `invalid_combination` is a 400. The plan already notes this, but it's a key part of the "API-04 consistent error envelope" requirement.

### 5. Risk Assessment: LOW
The risk is low. The breaking change is intentional and limited to an internal-only API. The validation logic is moved into the Pydantic layer where it belongs, and the regression test suite is comprehensive enough to catch any accidental behavioral changes in the search engine.

**Approved. Proceed with 81A-01-PLAN.md.**

---

## Codex Review

**Summary**

The plan set is strong on contract intent and sequencing, but not yet execution-ready. The biggest risks are mismatches with current error-envelope behavior, incomplete test migration outside `tests/test_search_api.py`, and acceptance tests that weaken "measurable behavior" into pass-through/echo checks. D-09 is mostly reflected correctly: no implementation of regex or `regex_pattern_too_long`, though several tests still expect Pydantic enum failures as 422, which conflicts with the current hardening wrapper.

**Strengths**

- Clear phase boundary: `/api/search` gets `search_mode`, `/api/parallels.mode` is intentionally preserved.
- Good correction from earlier risk: `exact -> exact` and `variants -> variants`, not collapsed to `text`.
- `extra='forbid'` on `SearchRequest` and `ResponsaOptions` is the right hard-cutover posture.
- Echo block design is useful and privacy-conscious: no IP hash or bucket internals.
- Wave ordering is mostly sound: Plan 02 before Plan 03 avoids same-finally-block conflicts.
- Regex drop is mostly honored: no regex mode in enum, no regex cap, no `regex_pattern_too_long`.

**Concerns**

- **HIGH:** 422 expectations do not match current code. `_build_envelope_response` returns HTTP 400 for all `PydanticValidationError`s in `web/api_hardening.py:299`, and `/api/search` routes Pydantic errors through that path in `web/search_api.py:408-416`. Plans 01/04/05 expect `limit > 100`, `limit < 1`, and `search_mode='regex'` to return 422. They will return 400 unless the wrapper is changed.

- **HIGH:** Test migration scope is incomplete. Old `/api/search` payloads remain outside `tests/test_search_api.py`, including `tests/test_browse_api.py:735`, `tests/test_browse_api.py:832`, `tests/test_parallels_api.py:568`, and `tests/test_search_api_soak.py:46/94/95/98/133-147`. The phase gate says existing baseline still passes; these will fail after hard rejection of `mode`.

- **HIGH:** Plan 04 has an impossible grep assertion. It adds an old-mode rejection test using `{'mode': 'text'}`, then requires no `mode: text|Title|Shelfmark|Responsa` literals remain in `tests/test_search_api.py`. That verification will fail unless the rejection test is explicitly allowlisted.

- **MEDIUM:** AC2/AC3 coverage is weakened. Plan 05 allows "documented empty-but-200" for modes, and for Responsa flags says "at minimum" assert echo/pass-through to the fake engine. That does not satisfy "non-empty results" or "measurable behavioral change." Use spies for API plumbing, but add deterministic core-level or integration fixtures for actual behavior.

- **MEDIUM:** New Responsa meta thread-local should be cleared on `SearchEngine.execute_search` entry, matching the legacy string channel at `genizah_core.py:7254`. Plan 02 only drains in the web handler; a stale meta signal from direct core usage could leak into a later API response.

- **MEDIUM:** PostHog values are not captured for `invalid_combination` validator errors. Plan 03 sets `posthog_search_mode_value` only after `SearchRequest` construction succeeds. But `APIError('invalid_combination')` raised inside model validators will skip that assignment even when `search_mode` was parsable.

- **MEDIUM:** `/api/parallels` echo shape is inconsistent inside Plan 02. Must-haves mention `gap`; task/action/acceptance omit it. Decide one schema and align plan, tests, and context.

- **LOW:** Some verification commands are brittle. Example: Plan 01 grep expects double-quoted `Literal[...]`, but the proposed code uses single quotes. Several `grep -c` checks may also exit nonzero on zero matches.

**Suggestions**

- Decide the Pydantic error status contract first. If 422 is required, change `_build_envelope_response` or add a constraint-specific response path. If 400 is preferred for envelope consistency, update 81A-CONTEXT and all plans/tests.
- Expand Plan 04 to migrate every `/api/search` caller found by `rg "/api/search" tests`, not just `tests/test_search_api.py`.
- Add `_consume_last_responsa_downgrade_meta()` at the start of `SearchEngine.execute_search`, next to the existing legacy drain.
- Split tests into two layers: handler spy tests for mapping/echo/PostHog, and behavior fixtures that prove mode/flag result differences. Do not let empty 200 responses satisfy AC2.
- Compute a provisional `search_mode_value` from the raw body before Pydantic construction, then overwrite with validated value on success. Keep null only when absent or invalid.
- Replace broad grep checks with small Python checks that allow the intentional old-mode rejection test.

**Risk Assessment**

**MEDIUM-HIGH.** The architecture is directionally correct, but several plan-level contradictions would cause implementation/test churn and likely fail the stated phase gate. Fixing the status-code contract, broad test migration, thread-local lifecycle, and AC2/AC3 test strength would bring the risk down to medium.

---

## Consensus Summary

The two reviewers diverge sharply: Gemini approves with LOW risk; Codex flags MEDIUM-HIGH risk with three concrete HIGH-severity issues. Codex inspected actual file:line references in the live codebase and surfaced contradictions that Gemini's structural review missed. Treat Codex's HIGH items as blockers for execution.

### Agreed Strengths
- Wave sequencing correctly avoids the `web/search_api.py` finally-block collision.
- Variants wiring at the API layer (Blocker 2 fix from iteration 1) is correct — `exact` and `variants` map to distinct internal modes consumed by `var_mgr.get_variants(term, mode)`.
- `extra='forbid'` hard-cutover posture is right for an internal API.
- Echo block is privacy-conscious (no IP, no bucket state — D-10 honored).
- D-09 (regex dropped) is fully reflected: no regex mode in enum, no `regex_pattern_too_long` code, no 256-char pattern cap.
- Regression integrity: in-place rewrite of `tests/test_search_api.py` preserves git history.

### Agreed Concerns (Codex HIGH; both flag the underlying status-code question)
1. **Pydantic error status contract (HIGH — Codex; Gemini noted as "key part of API-04"):** `_build_envelope_response` at `web/api_hardening.py:299` currently returns HTTP **400** for all `PydanticValidationError`s. Plans 01/04/05 assert **422** for `limit > 100`, `limit < 1`, and `search_mode='regex'`. **These tests will fail.** Either change the wrapper to honor Pydantic's native 422 (with `loc`-aware status), or update plans/tests/CONTEXT to assert 400 uniformly.

### Codex-Only Concerns (worth incorporating)
2. **Test migration sprawl (HIGH):** Old `mode` field payloads exist outside `tests/test_search_api.py`. Codex enumerated:
   - `tests/test_browse_api.py:735, 832`
   - `tests/test_parallels_api.py:568`
   - `tests/test_search_api_soak.py:46, 94, 95, 98, 133-147`

   After hard cutover, these will fail. Phase gate says "existing 1156-test baseline still passes". Plan 04 must broaden migration scope to every `/api/search` caller in `tests/`.

3. **Plan 04 self-contradicting grep assertion (HIGH):** Plan 04 adds a rejection test that *must* contain the literal `'mode': 'text'`, then verifies no such literal remains. Need an allowlist for the rejection test (e.g., grep out the specific test function, or use a Python AST check).

4. **AC2/AC3 weakened to pass-through (MEDIUM):** Plan 05 allows "empty-but-200" to satisfy AC2 and "echo carries the flag verbatim" for AC3. The requirement says "non-empty results" (AC2) and "measurable behavioral change" (AC3). Add a real-core layer with deterministic fixtures so the matrix proves behavior, not just plumbing.

5. **Thread-local lifecycle leak (MEDIUM):** `_consume_last_responsa_downgrade_meta()` is drained in the web handler only. A direct core caller could leave stale meta that the next web request reads. Drain at `SearchEngine.execute_search` entry, next to the legacy drain at `genizah_core.py:7254`.

6. **PostHog `search_mode_value` null on cross-field errors (MEDIUM):** Plan 03 binds the property only after successful `SearchRequest` construction. `invalid_combination` raised in `@model_validator` skips the assignment, so we lose telemetry on the most interesting error class. Capture provisional value from raw body, overwrite on success.

7. **/api/parallels echo `gap` mismatch (MEDIUM):** Plan 02 must-haves mention `gap`; task action omits it. Pick one schema and align all artifacts.

8. **Brittle grep verification (LOW):** Several `grep -c` checks expect specific quote style or exit nonzero on zero matches. Use Python checks where matter.

### Divergent Views
- **Risk level:** Gemini LOW vs Codex MEDIUM-HIGH. Codex inspected the live codebase; Gemini did not. Codex's HIGH-1 (status code contract) and HIGH-2 (test sprawl) are objectively present in current code — verified by grep against `web/api_hardening.py:299` and the listed test files.
- **Plan 03 PostHog null hole:** Gemini calls it "acceptable for an internal API"; Codex calls it MEDIUM. Codex is correct that we lose visibility on `invalid_combination` errors which are the new thing the API surfaces — exactly the case skill authors will hit.

### Recommended Next Step

The HIGH-severity issues from Codex (status code contract, test migration sprawl, Plan 04 self-contradiction) are concrete and block execution. Recommend:

```
/gsd-plan-phase 81A --reviews
```

to incorporate review feedback before executing.
