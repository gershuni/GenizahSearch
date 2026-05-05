---
phase: 83
reviewers: [gemini, codex]
reviewed_at: 2026-05-05T09:33:16Z
plans_reviewed: [83-01-PLAN.md, 83-02-PLAN.md, 83-03-PLAN.md, 83-04-PLAN.md, 83-05-PLAN.md]
---

# Cross-AI Plan Review — Phase 83 (Public Release of Search API)

## Gemini Review

This review covers the implementation plans for **Phase 83: Public Release of Search API**. 

Overall, the plans are exceptionally well-structured, reflecting a deep understanding of the project's technical constraints (specifically NiceGUI's OpenAPI behavior) and the project's release conventions. The "Nyquist-compliant" testing strategy (Wave 0 RED tests) is a strong foundation for ensuring that the public contract is both delivered and preserved.

---

### Plan 83-01: Security Audit & Test Scaffolding
**Summary:** This plan establishes the verification baseline for the entire phase. It produces a formal security audit (`83-SECURITY.md`) that re-verifies load-bearing mitigations from earlier phases and initializes three "RED" test files to track the delivery of documentation, API scope, and release artifacts.

*   **Strengths:**
    *   **Traceability:** Mapping D-05 audit items directly to file:line evidence in `83-SECURITY.md` ensures the audit is falsifiable and rigorous.
    *   **Fail-Fast Scaffolding:** Writing the Wave 0 tests first ensures that subsequent plans have immediate feedback loops.
*   **Concerns:**
    *   **Test Isolation (Low):** Ensure that `tests/test_openapi_scope.py` doesn't accidentally trigger real side-effects or heavy initialization during the import of `web.main` in a test context. The use of `skipif` is a good guard, but standard `TestClient` usage usually handles this.
*   **Suggestions:**
    *   In `83-SECURITY.md`, include a brief "Post-Deploy Verification" checklist for the person performing the manual deploy in Plan 05 to re-verify the most critical mitigations (like Rate Limiting) in the production environment.

### Plan 83-02: Documentation Reframe
**Summary:** Reframes `docs/SEARCH_API.md` from an internal helper to a public contract. It replaces the "internal-only" banner with a Stability statement, Quick Start examples, and proper academic attribution.

*   **Strengths:**
    *   **Contract Preservation:** Explicit "Preservation Rules" and acceptance greps for existing sections protect the validated 663-line reference material.
    *   **Runnable Examples:** Including `curl` examples with expected outcomes significantly lowers the barrier to entry for external researchers.
*   **Suggestions:**
    *   Ensure the link to `README.md#credits--data` uses a relative path that works both on GitHub and in local Markdown viewers.

### Plan 83-03: OpenAPI Exposure
**Summary:** Implements the technical wiring for the public API surface. It uses a FastAPI sub-mount at `/api` to bypass NiceGUI's disabled auto-docs and refactors `init_search_api` to handle parametric path prefixes, ensuring both production and tests remain functional.

*   **Strengths:**
    *   **Architecture Pattern:** The sub-mount approach is the "cleanest" solution to the NiceGUI/FastAPI docs conflict discovered in research.
    *   **Metadata Quality:** Adding `description=` kwargs to Pydantic fields ensures the Swagger UI at `/api/docs` is professional and self-documenting.
    *   **Backward Compatibility:** Defaulting `path_prefix='/api'` in the refactor preserves existing tests without requiring a massive rewrite of test URLs.
*   **Concerns:**
    *   **Path Conflict (Medium):** The pitfall of `/api/api/search` is correctly identified and mitigated by the `path_prefix=""` parameter in `main.py`. The executor must be extremely careful to ensure the default value in the function signature is consistently applied.

### Plan 83-04: README & Skill Updates
**Summary:** Focuses on discovery and consistency. Adds an "API" section to the main project `README.md` and updates the Claude Skill's documentation to point to the new public reference.

*   **Strengths:**
    *   **Visibility:** Placing the API section after "Additional Capabilities" makes it discoverable for developers without cluttering the "Getting Started" flow for general users.

### Plan 83-05: Release & Deployment
**Summary:** The final gate. Bumps the version to 7.10.0, summarizes the milestone in the CHANGELOG, performs a multi-gate manual check (including the Claude Skill smoke test), and deploys to production.

*   **Strengths:**
    *   **Release Intelligence:** The decision to **SKIP** the GitHub Release object (while still tagging the git repo) is a critical insight. It prevents triggering "Update Available" loops for desktop users on a web-only release.
    *   **Safety Gates:** The runbook for `unset GENIZAH_API_BASE` prevents the common mistake of accidentally running smoke tests against the production server.
*   **Concerns:**
    *   **Manual Steps (Medium):** Since Claude cannot SSH, the "human-in-the-loop" dependency is high here. The plan mitigates this with clear, copy-pasteable verification commands.

---

### Risk Assessment

**Overall Risk: LOW**

**Justification:**
1.  **Low Code Flux:** The phase is ~90% documentation and configuration. The actual code changes (`web/main.py` wiring and `init_search_api` refactor) are surgical and localized.
2.  **Verified Patterns:** The sub-mount pattern for OpenAPI under NiceGUI is a documented and tested FastAPI standard.
3.  **Extensive Verification:** The combination of automated Wave 0 tests, existing full-suite regressions, and a manual "Skill Smoke" test provides defense-in-depth against regressions.
4.  **Zero-Impact Rollback:** The `SEARCH_API_MODE=disabled` env-var kill switch provides an instant "undo" if production issues arise post-deploy.

### Final Recommendation
**APPROVED.** Proceed with execution starting at Plan 83-01. The executor should pay special attention to Task 1 of Plan 03 to ensure the `path_prefix` refactor is applied consistently across all route decorators.

---

## Codex Review

**Overall Assessment**

The plan set is directionally strong and covers the Phase 83 goals: audit, public docs, OpenAPI exposure, README/skill surfacing, release/deploy. The main risks are not missing scope, but execution details: dependency ordering is too loose, the OpenAPI implementation plan assumes FastAPI will infer schemas from manually parsed `Request` handlers, and the proposed OpenAPI tests do not import `web.main`, so they will not exercise the sub-mount wiring.

**83-01 Security Audit + RED Tests**

**Summary:** Good gate concept, but it currently mixes pre-implementation audit, future OpenAPI controls, and test scaffolding in a way that can give false confidence.

**Strengths**
- Explicitly covers D-05 security items.
- Requires file:line evidence, which is the right audit standard.
- Adds RED tests early for docs, OpenAPI, and release artifacts.

**Concerns**
- **HIGH:** `tests/test_openapi_scope.py` imports `nicegui.app` directly, not `web.main`; the Phase 83 sub-mount would never be registered.
- **MEDIUM:** Plan says “14 Wave 0 tests,” but the listed files define 15.
- **MEDIUM:** Audit is supposed to verify Phase 83 OpenAPI mitigations before those changes exist.
- **MEDIUM:** Other plans have no dependency on 83-01 despite this plan claiming to happen before code changes.

**Suggestions**
- Change OpenAPI tests to import `web.main` or expose/test a `create_search_helper_app()` helper.
- Split security audit into “baseline audit” plus “post-OpenAPI recheck.”
- Make 83-02/03/04 depend on 83-01 if audit truly gates all work.

**Risk Assessment:** **MEDIUM-HIGH** because the audit/test gate can pass or fail for the wrong reasons unless corrected.

**83-02 SEARCH_API.md Reframe**

**Summary:** Solid documentation plan that directly addresses PUBLIC-01 and PUBLIC-03, but it has broken relative links and slightly under-delivers on “expected response shape excerpt.”

**Strengths**
- Keeps scope focused on reframing, not rewriting the whole reference.
- Includes the required stability, quick start, attribution, and changelog sections.
- Correctly avoids creating a duplicate attribution file.

**Concerns**
- **HIGH:** Relative links from `docs/SEARCH_API.md` are wrong: `../../README.md` should be `../README.md`; `../../skills/...` should be `../skills/...`.
- **MEDIUM:** Quick Start describes responses in prose but does not include a response-shape excerpt as requested.
- **LOW:** “Preserve byte-for-byte” is hard to verify with grep-only checks.

**Suggestions**
- Fix all Markdown relative links before implementation.
- Add tiny JSON excerpts showing `schema_version`, `results`, `request`, and `error`.
- Add a targeted doc test for broken relative links, or rely on `scripts/check_docs.py` if it catches them.

**Risk Assessment:** **LOW-MEDIUM**. Mostly safe, but broken public links would be visible immediately.

**83-03 OpenAPI Sub-Mount**

**Summary:** The sub-mounted FastAPI app is the right architectural direction, but the plan underestimates schema-generation work. Adding `Field(description=...)` alone will not make useful OpenAPI request schemas because the handlers take raw `Request`.

**Strengths**
- Correctly identifies NiceGUI’s disabled docs/openapi defaults.
- Correctly avoids putting legacy routes into the public spec.
- Keeps existing `/api/search`, `/api/browse`, `/api/parallels` public paths stable.
- Uses `path_prefix` to avoid `/api/api/search`.

**Concerns**
- **HIGH:** FastAPI will not infer request bodies/query params from `SearchRequest`, `BrowseRequest`, or `ParallelsRequest` because handlers accept `Request` and instantiate models manually.
- **HIGH:** OpenAPI tests are currently flawed unless they import `web.main` or a helper that mounts the sub-app.
- **MEDIUM:** Spec paths may appear as `/search`, `/browse`, `/parallels`; clarify whether that satisfies D-07 or whether `servers: [{"url": "/api"}]` is needed.
- **MEDIUM:** Hard-coding `version="7.10.0"` in `web/main.py` will go stale; use `APP_VERSION`.
- **LOW:** Proposed descriptions include non-ASCII arrows in Python source; prefer ASCII in code strings.

**Suggestions**
- Add explicit OpenAPI metadata via `openapi_extra`, typed wrapper functions, or a dedicated sub-app factory that registers documented routes while preserving custom envelope behavior.
- Extend tests to assert request body schema exists for `/search` and `/parallels`, and query parameters exist for `/browse`.
- Use `version=APP_VERSION`.
- Add a local integration test that posts to `/api/search`, not only `/api/openapi.json`.

**Risk Assessment:** **HIGH**. This is the most technically risky plan because the published spec may be shallow or incorrect even if `/api/openapi.json` returns 200.

**83-04 README + Skill Docs**

**Summary:** Clean, appropriately scoped doc surfacing. The dependencies should include 83-01 if the RED tests are the gate.

**Strengths**
- Keeps README API section short and English-only as requested.
- Mentions all three endpoints.
- Skill update is correctly doc-only.

**Concerns**
- **MEDIUM:** Depends only on 83-02, but its tests come from 83-01.
- **LOW:** README wording says “30 req/min” as a public promise; acceptable if stable, but should align exactly with env/default docs.
- **LOW:** The README section is 4 sentences plus links, slightly above the requested 2–3 sentence target.

**Suggestions**
- Add dependency on 83-01.
- Keep rate-limit language tied to “default public deployment” if the env var can change.
- Confirm `scripts/check_docs.py` accepts the new links.

**Risk Assessment:** **LOW**. Low implementation risk once dependencies are corrected.

**83-05 Release + Deploy**

**Summary:** Good release checklist, but it misses one stated artifact and contains some stale close-out instructions.

**Strengths**
- Explicitly skips GitHub Release object, which is important for desktop update prompts.
- Correctly treats server startup, smoke test, and production deploy as manual gates.
- Includes rollback via `SEARCH_API_MODE=disabled`.
- Uses `./deploy.sh`/`deploy.sh` rather than the incorrect `scripts/deploy.sh`.

**Concerns**
- **MEDIUM:** D-14 says README “What’s New” must be updated; this plan only updates the header via `bump_version.py`.
- **MEDIUM:** Close-out instructions assume ROADMAP has TBD placeholders, but the provided roadmap already lists Phase 83 plans.
- **MEDIUM:** Git tag creation is added, but not clearly required by D-14/D-15; make this an explicit release decision.
- **LOW:** Changelog says “7 phases” but lists Phases 77–83 plus Phase 81A/81B/82/83; verify milestone count wording.
- **LOW:** Success criteria repeat “14 tests” where the scaffold has 15.

**Suggestions**
- Add a README “What’s New” edit task.
- Update ROADMAP close-out instructions to modify the existing Phase 83 section, not replace TBD.
- Decide explicitly whether `v7.10.0` git tag is required.
- Add `pytest tests/test_openapi_scope.py` after importing the real mounted app.

**Risk Assessment:** **MEDIUM**. Release mechanics are mostly sound, but doc artifact gaps and stale close-out instructions could leave the milestone incomplete.

**Highest-Priority Fixes Before Execution**

1. Fix OpenAPI tests to exercise `web.main` or a reusable sub-app factory.
2. Add explicit OpenAPI schema metadata; raw `Request` handlers will not produce useful schemas.
3. Correct `docs/SEARCH_API.md` relative links.
4. Tighten dependencies so 83-01 gates later plans if intended.
5. Add the missing README “What’s New” update in 83-05.

---


## Consensus Summary

Two independent reviewers (Gemini + Codex) agree the plan set is sound in shape and scope but disagree sharply on technical risk. **Gemini: LOW overall risk, APPROVED.** **Codex: MEDIUM-HIGH risk, four highest-priority fixes before execution.** Codex went deeper into technical execution details — its concerns are concrete and likely correct.

### Agreed Strengths
- Sub-mount approach for `/api/openapi.json` + `/api/docs` is the right architecture given NiceGUI's disabled defaults (both reviewers).
- D-15 SKIP-GitHub-Release decision (with desktop-poll rationale) is a critical correct call (both).
- `SEARCH_API_MODE=disabled` rollback path is robust (both).
- Wave 0 RED test scaffolding is sound (both).
- Doc reframe is correctly scoped (preserves the 663-line reference) (both).

### Agreed Concerns (highest priority — fix before execution)

1. **HIGH (Codex) — OpenAPI tests don't import `web.main`.** `tests/test_openapi_scope.py` imports `nicegui.app` directly; the Phase 83 sub-mount only registers when `web/main.py` runs. Tests would pass against an unmounted app and falsely confirm spec correctness. Gemini also flags this indirectly ("Test Isolation").
   **Fix:** Tests must import `web.main` (or expose a `create_search_helper_app()` factory and import that) so the sub-mount is exercised.

2. **HIGH (Codex) — FastAPI cannot infer schemas from raw `Request` handlers.** All three search-helper handlers take `Request` and instantiate Pydantic models manually inside the body. Adding `Field(description=...)` alone produces an OpenAPI spec where `/api/search` has no documented request body, no parameters, no response schema. Swagger UI at `/api/docs` will render an essentially empty endpoint. Gemini did not catch this.
   **Fix:** Either (a) refactor handlers to use FastAPI's automatic Pydantic body parsing — `async def search(req: SearchRequest)` — wrapped to preserve the existing envelope behavior; or (b) declare schemas explicitly via `openapi_extra={"requestBody": {...}}` on each route; or (c) use a typed wrapper function pattern. Plan 03 must address this before execution, or PUBLIC-04 ships shallow.

3. **MEDIUM (Codex) — Broken relative links in `docs/SEARCH_API.md`.** Reframe writes `../../README.md` and `../../skills/...` but `docs/SEARCH_API.md` is one level deep, so the correct paths are `../README.md` and `../skills/...`.
   **Fix:** Correct relative paths in Plan 02 task action before execution.

4. **MEDIUM (both reviewers) — Plan dependencies don't reflect Wave 0 ownership.** Plans 02–04 depend only on each other / Plan 02, but Plan 01 owns the Wave 0 RED tests. If 02/03/04 run before 01, their tests aren't yet collected. Gemini implicit, Codex explicit.
   **Fix:** Add `depends_on: [83-01]` to Plans 02, 03, 04 (or move Wave 0 test scaffolding into a separate Wave 0 plan that all subsequent plans depend on).

5. **MEDIUM (Codex) — README "What's New" missing.** D-14 says README "What's New" must be updated; Plan 05 only updates the version header via `bump_version.py`. Bug.
   **Fix:** Add a task in Plan 05 to write the README "What's New" section content.

6. **MEDIUM (Codex) — Hard-coded `version="7.10.0"` in `web/main.py`.** Will go stale after the next release.
   **Fix:** Use `from version import APP_VERSION` and pass `version=APP_VERSION` to FastAPI sub-app.

7. **MEDIUM (Codex) — Plan 05 close-out instructions assume TBD placeholders in ROADMAP, but ROADMAP already lists Phase 83 plans.** Stale — the plans now exist.
   **Fix:** Update Plan 05 close-out task to edit the existing Phase 83 section, not replace TBD lines.

### Lower-priority items

- Codex MEDIUM: Audit (Plan 01) is supposed to verify Phase 83 OpenAPI mitigations, but those mitigations don't exist until Plan 03 lands. **Fix:** Split into baseline audit (now) + post-OpenAPI recheck (after Plan 03), or scope Plan 01's audit to existing-mitigation verification only and add a small re-check task to Plan 03.
- Codex MEDIUM: Spec paths may render as `/search`, `/browse`, `/parallels` instead of `/api/search` — clarify whether `servers: [{"url": "/api"}]` metadata is needed.
- Codex LOW: Plan 02 Quick Start describes responses in prose but lacks JSON shape excerpts.
- Codex LOW: Plan 04 README section is 4 sentences (target was 2–3); rate-limit "30 req/min" wording should tie to "default public deployment" since env var can change.
- Codex LOW: Plan 05 success criteria says "14 tests" but scaffold has 15 (already partially fixed inline; verify).
- Codex LOW: Non-ASCII arrows in Pydantic `description=` strings.
- Gemini suggestion: Add a "Post-Deploy Verification" checklist in `83-SECURITY.md` for the operator to re-run rate-limit checks against production after deploy.
- Gemini suggestion: Tiny JSON excerpts (`schema_version`, `results`, `request`, `error`) in Plan 02 Quick Start.

### Divergent Views

| Topic | Gemini | Codex |
|-------|--------|-------|
| Overall risk | LOW (APPROVED as-is) | MEDIUM-HIGH (4 must-fix items before execution) |
| Plan 03 OpenAPI completeness | "Sub-mount approach is the cleanest solution" | "Plan underestimates schema-generation work; raw Request handlers won't produce useful schemas" |
| Test scaffolding | "TestClient usage usually handles this" | "Tests don't exercise the sub-mount wiring at all" |

**Codex's deeper-dive finding (concern #2 above) is the load-bearing risk.** If Plan 03 ships as written, the published OpenAPI spec will technically exist (200 OK, valid JSON, 3 paths listed) but its schemas will be empty — Swagger UI will show three buttonless endpoints with no documented inputs or outputs. PUBLIC-04 grep tests would pass; the actual public deliverable would be hollow.

### Recommended Action

**Re-plan Plan 03 (OpenAPI) and patch Plans 01, 02, 04, 05** before execution:
- Plan 03: Add a task that converts the three handlers to FastAPI-Pydantic body parsing OR adds explicit `openapi_extra` per route; tests must import `web.main`.
- Plan 02: Fix relative link paths.
- Plans 02, 03, 04: Add `depends_on: [83-01]`.
- Plan 05: Add README "What's New" task; replace hard-coded version with `APP_VERSION`; update ROADMAP edit instructions to reflect existing-section style.

Run via `/gsd-plan-phase 83 --reviews` to incorporate these into a revised plan set.

