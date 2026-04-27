---
phase: 77
reviewers: [gemini, codex]
reviewed_at: 2026-04-27T11:30:00Z
plans_reviewed: [77-01-PLAN.md, 77-02-PLAN.md, 77-03-PLAN.md, 77-04-PLAN.md, 77-05-PLAN.md]
---

# Cross-AI Plan Review — Phase 77 (Serializer & JSON Export)

## Gemini Review

# Plan Review: Phase 77 — Serializer & JSON Export

## Summary
Phase 77 is a well-structured and pattern-aligned bridge into the v7.10 Search API milestone. The plans effectively transition the project from UI-centric result rendering to a decoupled serialization model that will serve both user downloads and future internal API consumers. By choosing "Path A" for parallels attribution and fixing long-standing latent state bugs (e.g., `current_search_query` assignment), the plan ensures high data fidelity and improves the existing codebase as a side effect. The TDD-driven approach (Wave 0 RED tests) and strict enforcement of the "Single Source of Truth" (D-14) are high-quality engineering choices that minimize future maintenance costs.

## Strengths
- **Fidelity-First Design (D-13 Path A):** Extending `genizah_core.py` to capture `chunk_hits` is a superior choice over the "degenerate" fallback. It ensures that the Claude skill and future API consumers receive precise attribution data without needing a second search pass.
- **Architectural Integrity (D-14):** Forcing both top-level functions to share a private `_serialize_item` via structural tests prevents the inevitable drift that occurs when maintaining parallel search and parallels logic.
- **Latent Bug Resolution:** Identifying and fixing the `state.current_search_query` assignment bug ensures that the new JSON filenames are meaningful and fixes the "genizah.xlsx" default for existing Excel/Word users.
- **Context-Aware URL Construction:** Correcting the assumption that `display['img']` is a URL and implementing a server-relative page-indexed URL resolver avoids a critical integration failure with the target Claude skill.
- **Robust Validation Map:** The Wave 0 test strategy is exhaustive, covering everything from schema constants to complex parallels grouping logic before implementation begins.

## Concerns
- **Performance of Catalog Lookups (LOW):** While domain lookups are batched, `get_catalog` calls in `_safe_fjms_lookups` (Plan 77-03) are still performed in a loop. For a 50-result search, this is 50 sub-millisecond SQLite hits. This is acceptable for a download handler but may need a batch helper in Phase 78 for the stateless POST endpoint to keep latency under 100ms.
- **`tr` Helper Availability (LOW):** Tooltips in `web/pages/search.py` and `parallels.py` use `tr('Export JSON')`. If this key isn't added to the i18n table, it will fall back to English. This is noted as out-of-scope for the plan, which is fine as long as the default is acceptable.
- **Memory Pressure (LOW):** Capturing `chunk_hits` in `genizah_core.py` increases memory usage for composition searches. As noted in the STRIDE register, this is dwarfed by the existing `content` strings and is a non-risk for the current deployment scale.

## Suggestions
- **FjmsService Batching:** In a future phase (78 or 79), consider adding `FjmsService.get_catalog_batch_for_sys_ids()` to consolidate the N catalog queries into a single `IN (...)` query, mirroring the `get_domains_for_sys_ids` pattern.
- **I18n Table:** Although out of scope for Phase 77, adding `Export JSON` / `יצוא ל-JSON` to `genizah_translations.py` should be the first task of the Phase 82 documentation/cleanup pass.

## Risk Assessment
**Overall Risk: LOW**

The implementation follows established project patterns (FastAPI handlers, `shared/` services, NiceGUI buttons). The changes are largely additive and read-only with respect to existing data. Dependency ordering is sound, and the use of static-contract tests for the core extension (`tests/test_lab_composition_chunk_hits.py`) mitigates the risk of modifying the sensitive search engine.

### Requirement Coverage
- [x] **EXPORT-01..04:** Fully addressed via `shared/search_serializer.py` and the two new download handlers.
- [x] **API-05/07:** Foundation laid via the `locator` contract and `restrict_sys_ids` mirroring.
- [x] **D-14:** Structurally enforced via `test_serializers_share_serialize_item`.

**Approval Recommendation:** Approved. The plan is ready for execution.

---

## Codex Review

# Phase 77 Plan Review

## Overall
The plan set is well-researched, grounded in real code, and sequenced sensibly across waves. The biggest risks are not architectural; they are contract drift and completeness: `77-01` does not mirror all relevant state paths, `77-02` does not truly verify its core change, and `77-03` locks a few deviations (`domains[]`, image URL semantics, filename uniqueness) that should be tightened before they become the Phase 78/80 API contract.

## 77-01-PLAN
**Summary**
Strong foundation work: it fixes a real latent bug, adds the state needed for stateful exports, and sets up TDD. The main issue is that it mirrors only part of the real search/parallels state surface, so the later handlers can still export stale or incomplete metadata.

**Strengths**
- Fixes a verified bug: `state.current_search_query` is declared in `web/state.py:27` and consumed in `web/api.py:1816`, but not assigned today.
- Keeps the Phase 77 export handlers decoupled from page-local state by using `state`, matching the existing Excel/Word pattern.
- TDD-first serializer tests are appropriate for locking a cross-phase payload contract.

**Concerns**
- `[HIGH]` It patches only the normal `/search` completion path at `web/pages/search.py:4077`, but `state.last_results` is also written in the cancelled/partial-results path at `web/pages/search.py:4026`. Partial-result exports would still have stale query/filter metadata.
- `[HIGH]` The mirrored filter dict is incomplete. The real active filter surface includes `filter_include_mode` and text filters (`filter_text_all/any/not`) already used elsewhere, e.g. `web/pages/search.py:4232`.
- `[MEDIUM]` Search history restore updates `search_state.results` but not `state.last_results` at `web/pages/search.py:3740`. Export after restore can remain stale.
- `[MEDIUM]` The proposed restored parallels metadata uses `p_state.results[0].get('source_ctx', '')`, but the real snapshot already stores `source_text` at `web/pages/parallels.py:2215`. That would degrade fidelity unnecessarily.
- `[MEDIUM]` `parallels_search_meta` captures chunk/mode/freq, but not the active parallels filters or boundary settings, so the envelope is not actually self-describing.

**Suggestions**
- Patch every stateful export source, not just the happy path: normal search, cancelled partial search, and history restore.
- Reuse the already-built filter snapshot shape from `web/pages/search.py:4232` instead of manually rebuilding a smaller dict.
- For parallels history restore, use the stored `state_snapshot['source_text']` and `params` instead of reconstructing from result rows.
- Include `include_mode`, text filters, and boundary options if the contract says the JSON should be replayable/self-describing.

**Risk Assessment**
**MEDIUM-HIGH**. The scaffolding is good, but incomplete state mirroring will leak into every later plan.

## 77-02-PLAN
**Summary**
The core change itself is small, additive, and justified. The weak point is verification: the plan claims behavioral confidence, but the proposed test only inspects source text and skips the only runtime assertion that matters.

**Strengths**
- Path A is the right call if `matches[]` is meant to be truthful rather than synthetic.
- The change is additive inside `genizah_core.py:1206` and should not affect existing consumers of `total_score`/`hits_count`.
- It solves a real data-loss gap in current `lab_composition_search`.

**Concerns**
- `[HIGH]` The proposed test does not prove `chunk_hits` is populated at runtime; it only proves the source contains an append line.
- `[MEDIUM]` The plan's success criteria say "after `lab_composition_search` runs...", but the actual verification intentionally skips that behavior.
- `[LOW]` It adds per-hit text retention, which is fine here, but the memory cost should at least be acknowledged in the serializer plan too.

**Suggestions**
- Replace the skipped integration test with a narrow behavioral unit test that monkeypatches `lab_searcher.search`, `lab_searcher.doc`, `_calculate_match_metrics`, and `_is_phrase_statistically_weak`. You do not need a real Tantivy index to exercise this loop.
- Keep the additive code change, but lower the claimed certainty unless behavior is actually tested.

**Risk Assessment**
**MEDIUM**. The code change is low-risk; the test strategy is not.

## 77-03-PLAN
**Summary**
This is the strongest plan conceptually: shared serializer, single `_serialize_item`, clear envelopes, and good unit coverage. The issues are contract-level: it closes the FJMS singleton incorrectly, does not fully satisfy filename uniqueness, and locks a few deviations that should be made more explicit before Phase 78/80 inherit them.

**Strengths**
- `shared/search_serializer.py` is the right module placement.
- One `_serialize_item` is the correct structural answer to EXPORT-03.
- Graceful degradation when FJMS is unavailable is sensible.
- The per-item locator contract is explicit and testable.

**Concerns**
- `[HIGH]` `_safe_fjms_lookups()` closes the object returned by `get_fjms_service()`, but `shared/fjms_service.py:3164` is a module-level singleton. Existing code does not close it per call.
- `[HIGH]` Second-resolution filenames still do not guarantee distinct names for two fast consecutive downloads. That does not fully satisfy EXPORT-04.
- `[HIGH]` The implementation shape conflicts with one of the locked decisions: `_build_image_url()` will emit NLI URLs for any `sys_id` + `p_num`, including cases the plan says should be `null` for Oxford-only hits.
- `[MEDIUM]` `domains[]` is a reasonable long-term design, but it is a real contract change from the locked context. That should be elevated, not treated as a local implementation detail.
- `[MEDIUM]` The serializer still does not echo full parallels execution context: filters and boundary settings are missing.
- `[LOW]` `get_catalog()` per sys_id is acceptable for Phase 77 export, but it is an N-query pattern that may be poor for Phase 78 API latency.

**Suggestions**
- Do not call `close()` on the FJMS singleton in the serializer.
- Use a guaranteed-unique filename scheme: seconds plus monotonic counter, milliseconds, or a short random suffix.
- Either emit `image_url` only when known-good for the current provider, or explicitly document it as best-effort and `null` more aggressively.
- Reconfirm the `domains[]` deviation at the milestone level before Phase 78 consumes it.
- Add parallels `filters` and `boundary_options` if the envelope is supposed to be replayable.

**Risk Assessment**
**MEDIUM-HIGH**. This plan defines the future API contract, so small mistakes here become expensive later.

## 77-04-PLAN
**Summary**
The handler/button wiring is straightforward and mostly well-aligned with existing patterns. The main risk is that it depends on state completeness from `77-01`, so any holes there become user-visible here.

**Strengths**
- Mirrors current `/api/export/*` handler structure closely.
- Uses the serializer directly rather than inventing a parallel JSON export path.
- Correctly keeps search always-enabled and parallels lifecycle-gated, matching current UX.

**Concerns**
- `[HIGH]` Handler correctness depends on state fields that `77-01` does not fully populate on all search paths.
- `[MEDIUM]` The TestClient plan mutates the global NiceGUI app and re-registers routes. That is probably workable now, but fragile as the suite grows.
- `[MEDIUM]` Search history restore still won't reliably export the restored result set unless `state.last_results` is updated there too.
- `[LOW]` `tr('Export JSON')` has no translation entry today; fallback is harmless but inconsistent.

**Suggestions**
- Treat search-history restore as part of the handler dependency surface.
- Add a simple idempotence guard to `init_api_routes()` or isolate route registration in tests.
- Add the `Export JSON` translation alongside the button work.

**Risk Assessment**
**MEDIUM**. Good implementation path, but it inherits upstream state gaps.

## 77-05-PLAN
**Summary**
Good close-out plan. It respects the project's documentation discipline and adds a real manual phase gate. The only issue is that some checks left to manual verification could be automated more strongly.

**Strengths**
- Correctly updates `OPEN_ISSUES` and `CODE_INDEX`.
- Keeps a human smoke check for the browser download path, which is appropriate.
- Matches the roadmap gate of pytest green plus manual export checks.

**Concerns**
- `[MEDIUM]` Filename uniqueness is still mostly validated manually even though it is a hard success criterion.
- `[LOW]` If a new `OPEN_ISSUES` row is added, the summary counts should also be updated explicitly.
- `[LOW]` The smoke checklist is a bit broader than Phase 77 needs, which may slow sign-off.

**Suggestions**
- Automate at least one stronger handler-level filename uniqueness assertion before relying on manual click testing.
- Keep the manual checklist focused on JSON-specific behavior and stateful-route regressions.
- Explicitly mention updating issue counts if the docs change adds a new row.

**Risk Assessment**
**LOW-MEDIUM**. The plan is fine; it mostly depends on prior plans being tightened.

## Overall Risk Assessment
**MEDIUM**. The phase is well-planned and the wave ordering is basically right: `77-01` and `77-02` can run in parallel, `77-03` should follow, then `77-04`, then `77-05`. The real risks are:
- incomplete state capture in `77-01`,
- weak behavioral verification in `77-02`,
- contract drift and singleton misuse in `77-03`,
- and downstream dependence on those unresolved details in `77-04`.

If those four issues are corrected, the phase looks sound and should achieve the Roadmap goals cleanly.

---

## Consensus Summary

The two reviewers diverge sharply in depth: gemini approves with only LOW-severity nits (perf, i18n, memory), while codex finds **eight HIGH-severity** plan-level issues across Plans 77-01, 77-02, 77-03, 77-04. Gemini reviewed the plans as designed; codex reviewed them against the actual codebase and found contract drift, incomplete state mirroring, and a broken FJMS singleton interaction.

**The codex feedback should be treated as the authoritative review for this round.** Gemini's contributions are useful as forward-looking suggestions (Phase 78 batch catalog, Phase 82 i18n).

### Agreed Strengths

- D-13 Path A (extend `lab_composition_search` with `chunk_hits`) is the right call — both reviewers endorsed it as truthful over degenerate.
- Single `_serialize_item` shared by both top-level functions structurally enforces EXPORT-03 — both reviewers called this out.
- Latent `state.current_search_query` bug fix is a meaningful side-benefit beyond Phase 77 scope — gemini and codex both flagged the verified bug at `web/state.py:27`.
- TDD/RED-test scaffolding before serializer implementation is sound.
- Plan 77-03's module placement (`shared/search_serializer.py`) and graceful FJMS-unavailable fallback are correct.

### Agreed Concerns (most important — actionable)

| Severity | Concern | Surfaced by | Plan(s) |
|----------|---------|-------------|---------|
| HIGH | Filename uniqueness — second-resolution timestamps don't survive fast consecutive clicks; EXPORT-04 not actually satisfied. | codex | 77-03, 77-05 |
| HIGH | `_safe_fjms_lookups()` calling `close()` on a module-level singleton breaks subsequent calls. | codex | 77-03 |
| HIGH | Image URL strategy emits NLI URLs for Oxford-only hits, contradicting the documented "null for Oxford-only" rule. | codex | 77-03 |
| HIGH | State mirroring incomplete — only happy-path search at `web/pages/search.py:4077` is patched; cancelled/partial path at `:4026` and history restore at `:3740` leave `state` stale. | codex | 77-01, 77-04 |
| HIGH | Filter dict mirrored to `state.last_filters_applied` is incomplete — missing `filter_include_mode`, `filter_text_all/any/not`, parallels filters/boundary settings. Envelope `filters?` echo will not be replayable. | codex | 77-01, 77-03 |
| HIGH | Plan 77-02's "behavioral" test only greps the source for an append statement, not actual runtime population of `chunk_hits`. | codex | 77-02 |
| MEDIUM | `domains: list[str]` deviation from CONTEXT D-01 should be re-locked at milestone level (Phase 78 inherits this contract). | codex | 77-03 |
| MEDIUM | Plan 77-04's TestClient setup mutates the global NiceGUI app and re-registers routes — fragile as the test suite grows. | codex | 77-04 |
| LOW | N-per-call `get_catalog()` queries acceptable in Phase 77 download path but should be batched before Phase 78's stateless POST endpoint. | gemini, codex | 77-03 |
| LOW | `tr('Export JSON')` has no translation entry; English fallback is functional but inconsistent. | gemini, codex | 77-04 |

### Divergent Views

- **Overall risk:** gemini → LOW (Approved). codex → MEDIUM. The gap reflects depth of review, not genuine disagreement — gemini did not exercise the codebase deeply enough to find what codex found.
- **Performance:** gemini flagged the catalog N-query as a Phase 78 follow-up; codex agrees but adds it to the same list as the singleton-close bug. Both treat it as low-priority for Phase 77 itself.
- **`domains: list[str]` deviation:** gemini did not flag the deviation. Codex flagged it as a contract change that needs milestone-level confirmation before Phase 78 consumes it.

### Recommended Action

**Run `/gsd-plan-phase 77 --reviews`** to incorporate codex's eight HIGH-severity findings into a planner revision pass. The fixes are mechanical:

1. **Plan 77-01:** Patch all 3 state-write sites (line 4077, 4026, 3740). Reuse the existing filter snapshot shape from `web/pages/search.py:4232` rather than constructing a smaller dict. Add `include_mode`, text filters, and boundary options to the parallels meta dict.
2. **Plan 77-02:** Replace source-grep verification with a behavioral unit test that monkeypatches `lab_searcher.search`, `lab_searcher.doc`, `_calculate_match_metrics`, and `_is_phrase_statistically_weak` to exercise the populated `chunk_hits`.
3. **Plan 77-03:** Drop the `close()` call on the FJMS singleton. Use a guaranteed-unique filename scheme (monotonic counter / millisecond / short random suffix). Tighten `_build_image_url()` to emit `null` for Oxford-only hits.
4. **Plan 77-04:** Wrap `init_api_routes()` with idempotence so TestClient setup doesn't pollute global state. Add `tr('Export JSON')` translation entries.
5. **Plan 77-05:** Add an automated handler-level filename uniqueness assertion to reduce reliance on manual click testing.

The `domains[]` decision needs a quick out-of-band confirmation (escalate to user or document explicitly as ratified at milestone level) before letting it propagate into Phase 78's API-01 contract.

---

*Reviewers: gemini 0.38.1 (default model), codex 0.117.0 (default model). Self-review skipped (we are claude).*
