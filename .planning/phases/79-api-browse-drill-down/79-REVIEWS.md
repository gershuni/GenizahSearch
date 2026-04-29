---
phase: 79
reviewers: [gemini, codex]
reviewed_at: 2026-04-29T14:27:54Z
scope: PLAN review (4 plans — 79-01..79-04)
plans_reviewed:
  - 79-01-PLAN.md
  - 79-02-PLAN.md
  - 79-03-PLAN.md
  - 79-04-PLAN.md
note: |
  This file holds the Phase 79 PLAN review (Gemini + Codex).
  An earlier Codex review of CONTEXT.md (R-01..R-10, applied 2026-04-29)
  is preserved at 79-REVIEWS-context.md.
---

# Cross-AI Plan Review — Phase 79 (/api/browse Drill-Down)

## Gemini Review

An analysis of the plans for Phase 79 (/api/browse Drill-Down) reveals a well-structured approach that correctly isolates the new stateless logic from the UI-coupled state. The extraction of the pure-data enrichment fan-out into `shared/browse_service.py` is an excellent architectural choice, and the test suite proposed in Plan 04 is exceptionally comprehensive. However, there are two critical flaws that need to be addressed: a missing metadata hydration step that will cause runtime crashes during serialization, and a failure to reuse the Phase 78 hardening decorator.

### Strengths
* **Exhaustive Test Coverage:** Plan 04's test inventory covers every edge case requested in D-24, including rate-limit bucket independence, post-resolution UID mismatch, and legacy route immutability.
* **Strict Statelessness:** The decision to extract `shared/browse_service.py` and enforce statelessness (D-22) via `grep` verification ensures the endpoint remains clean and re-entrant.
* **Secure Validation:** Pydantic `BrowseRequest` with `extra='forbid'` and manual type coercion correctly guards against poisoned query parameters.
* **Rate Limit Topology:** Instantiating a second `_browse_rate_limiter` instance perfectly satisfies the D-18 requirement for independent per-IP buckets sharing the same ceiling.

### Concerns
* **HIGH — Missing Metadata Hydration in Core Fetch (Plan 02).** Plan 02's `_fetch_core` directly calls `state.searcher.get_browse_page()`, which returns a raw dict from Tantivy/csv_bank. Plan 02 then wraps this dict in a `SimpleNamespace`. However, the raw dict from `SearchEngine` **does not** contain `shelfmark`, `title`, `library_code`, or a parsed `fl_id` (these are resolved downstream by `MetadataManager`). When Plan 01's `serialize_browse_payload` attempts to access `page.fl_id` or `page.shelfmark`, it will raise an `AttributeError` and crash the endpoint.
* **HIGH — Failure to reuse the Hardening Shell Decorator (Plan 03).** `79-CONTEXT.md` explicitly mandates: *"planner verifies wrap_endpoint is reused, not reimplemented."* Phase 78 specifically built the `@wrap_endpoint` decorator to own the `try/except/finally` error handling and PostHog `capture_api_event` boilerplate. Plan 03 completely ignores this and hand-rolls the entire `try...except...finally` block inside `browse_endpoint`, introducing significant drift risk.
* **LOW — Unused Parameters (Plan 01).** `serialize_browse_payload` declares `requested_uid` and `requested_fl_id` parameters, but never uses them in the function body.

### Suggestions
* **Plan 02 (`shared/browse_service.py`):** In `_fetch_core`, do not call `state.searcher.get_browse_page` directly. Instead, call the fully hydrated `get_browse_page` from `web.services`:
  ```python
  def _sync():
      from web.services import get_service
      svc = get_service()
      if fl_id:
          return svc.get_browse_page_by_fl(fl_id, sys_id=sys_id)
      return svc.get_browse_page(sys_id, p_num=p_num, volume_ie=volume_ie)
  ```
  This returns a complete `BrowsePage` dataclass with all metadata (`shelfmark`, `title`, `library_code`, `fl_id`, `volumes`) already populated, eliminating the `AttributeError` risk and the need for `SimpleNamespace`.
* **Plan 03 (`web/search_api.py`):** Decorate `browse_endpoint` with `@wrap_endpoint(endpoint_name='browse')` from `web.api_hardening`. Remove the manual `try/except/finally`, `t0` tracking, `status_code`/`error_code` assignments, and the `capture_api_event` call from the handler body. The handler should only validate inputs, fetch the bundle, and return the envelope.
* **Plan 01 (`shared/search_serializer.py`):** Either remove `requested_uid` and `requested_fl_id` from the `serialize_browse_payload` signature or use them (e.g., to populate the locator if the resolved page happens to be missing them).

### Risk Assessment
**HIGH**. While the logical flow is correct, the failure to hydrate metadata in Plan 02 guarantees a 500 Internal Server Error (`AttributeError`) on every successful page resolution when the serializer attempts to read `page.fl_id` or `page.shelfmark`. Additionally, hand-rolling the error boilerplate in Plan 03 violates a core architectural constraint from the Phase 78 lock. Implementing the suggested fixes will lower the risk to LOW.

---

## Codex Review

### Summary

The plan set is strong on structure, traceability, and test intent, but it has three contract-level gaps that materially threaten Phase 79: the preferred `uid` path is validated but not actually resolved, Plan 02 is built on the wrong core return shape, and the locked "image graceful degrade" behavior is not implemented despite being treated as satisfied. The wave split is otherwise sensible, and the plans do a good job carrying forward Phase 78 hardening, statelessness constraints, and regression protection for legacy `/api/*` routes.

### Strengths

- The phase decomposition is clean: `79-01` establishes primitives, `79-02` isolates enrichment, `79-03` wires the endpoint, and `79-04` verifies behavior.
- The plans are unusually explicit about decision traceability. `D-01..D-27` and `R-01..R-10` are referenced directly instead of being hand-waved.
- `79-01-PLAN.md` correctly incorporates the Oxford route correction to sys_id-keyed `/api/oxford_image/{sys_id}?page=...`, matching `web/api.py:896`.
- Statelessness is treated seriously: the grep-based checks against `state.last_results`, `app.storage`, and cookies are appropriate for `D-22`.
- The rate-limit topology is well thought through. Separate browse/search buckets plus an explicit independence test in `79-04` is the right way to prove `D-18`.
- Legacy immutability coverage is good. Extending `tests/test_api_legacy_unchanged.py` is the right hedge against accidental global exception-handler regressions.
- The threat model in `79-03` is pragmatic and aligned with the actual trust boundaries of this internal API surface.

### Concerns

- **HIGH** — `uid` is never turned into an effective page lookup. In `79-03-PLAN.md`, Task 1 Block B/C, `_validate_locator()` only checks `uid`; it does not derive `p_num`, `volume_ie`, or `fl_id`. In `79-02-PLAN.md`, Task 1, `fetch_browse_bundle(..., uid=...)` explicitly accepts `uid` but does not use it. That means the preferred `GET /api/browse?sys_id=...&uid=...` path can only work by accident. This violates Success Criteria #1 and #2 and `D-02`/`D-03`.
- **HIGH** — Plan 02 assumes `state.searcher.get_browse_page()` returns a rich browse object, but it does not. The real `genizah_core.py:8246` core method returns only a minimal dict (`uid`, `p_num`, `full_header`, `text`, `total_pages`, `current_idx`, `sys_id`, `volume_ie`). The richer fields the serializer needs live in the `web/services.py:88` `BrowsePage` wrapper. As written, `79-02` will not reliably provide `shelfmark`, `title`, `library_code`, `library_name`, `volumes`, `cambridge_images`, `library_viewer_url`, or stable `fl_id`, so API-03's fixed-shape payload is at risk.
- **HIGH** — "Graceful image degrade" is not implemented even though the plan claims it is. `79-01` only builds URLs, and `79-03` explicitly says the handler just emits the URL and leaves availability to the proxy. That directly conflicts with locked `D-14` and ROADMAP Success Criterion #3, which require `image.url = null` plus a warning when the image source is unavailable.
- **MEDIUM** — The enrichment warning model in `79-02` is internally inconsistent. `_wrap_with_timeout()` is supposed to emit `enrichment_timeout` / `enrichment_failed`, but `_pgp_sync`, `_fjms_sync`, and `_nli_sync` each catch their own exceptions and return `None`. Real service failures will therefore often be silent nulls rather than warned partial failures, contrary to `D-16`.
- **MEDIUM** — The tests over-mock the critical integration seam. `79-04`'s `mock_searcher` returns a synthetic fully-enriched page dict, but the real `SearchEngine.get_browse_page` does not. That makes several tests non-representative and likely to pass even if production browse responses are missing core metadata.
- **MEDIUM** — `test_browse_locator_round_trip_from_search` in `79-04` allows a fallback that bypasses `/api/search` and calls `serialize_search_payload` directly. That weakens `D-27`: it stops being a real HTTP producer→consumer round-trip and becomes a serializer unit test plus browse request.
- **LOW** — `79-03` reimplements the endpoint try/except/finally boilerplate instead of using `wrap_endpoint`, even though the context frames that helper as inherited Phase 78 shell. This is not wrong, but it increases drift risk.
- **LOW** — `79-02` is marked `depends_on: []` even though it semantically relies on `79-01`'s `core_timeout` code addition for the final taxonomy. This is survivable because `APIError` tolerates unknown codes, but it is still an implicit dependency.

### Suggestions

- In `79-03-PLAN.md`, Task 1 Block C, add a normalization step immediately after `_validate_locator(req)`: parse `uid` once, derive `effective_volume_ie`, `effective_p_num`, and `effective_fl_id`, and pass those to `fetch_browse_bundle()`. Better: make `_validate_locator()` return a normalized locator object instead of only raising.
- In `79-02-PLAN.md`, Task 1, do not call `state.searcher.get_browse_page()` directly as the canonical browse object source. Use the richer `WebDataService.get_browse_page` (`web/services.py:294`) / `get_browse_page_by_fl()` path, or add an explicit adapter that reconstructs the missing metadata from `state.meta_mgr` and volume helpers before serialization.
- In `79-02-PLAN.md`, Task 1, for active folio resolution, prefer matching by `page.fl_id` when available instead of `folio_images[p_num - 1]`. That is safer for multi-IE manuscripts and aligns better with `R-05`.
- In `79-01-PLAN.md` and `79-03-PLAN.md`, either implement real `D-14` behavior or narrow the contract. If Phase 79 truly must null out `image.url` on proxy failure, the handler needs an availability probe or cached source-health decision. If that is too expensive, the plan should explicitly reopen the decision instead of claiming the requirement is met.
- In `79-02-PLAN.md`, Task 1, let `_pgp_sync` / `_fjms_sync` / `_nli_sync` raise and let `_wrap_with_timeout()` own warning emission. Right now the inner `try/except` blocks suppress the very warnings the outer wrapper is designed to generate.
- In `79-04-PLAN.md`, make the primary round-trip test a real `POST /api/search` → `GET /api/browse` HTTP flow. Keep the serializer-direct path only as a secondary unit test if needed.
- In `79-04-PLAN.md`, add at least one integration-style test that exercises the real core browse resolver shape, specifically to catch the mismatch between `genizah_core.py:8246` and the mocked enriched page dict.
- In `79-03-PLAN.md`, if boilerplate reuse is a goal, either switch to `wrap_endpoint` or explicitly note that Phase 79 intentionally mirrors the existing `search_endpoint` style instead of consuming the decorator.

### Risk Assessment

**Overall risk: HIGH.** The plans are disciplined, but two of the most important contract paths are currently broken at the design level: `uid`-only browse resolution and the shape of the core page object being serialized. On top of that, the image-degrade behavior required by the roadmap is not actually implemented. Those are not edge-case bugs; they are central to Success Criteria #1, #2, and #3. If those issues are fixed before execution, the rest of the plan set looks solid and the risk drops substantially.

---

## Consensus Summary

Both reviewers independently rate the plan set **HIGH risk** as written. They agree on the architecture (extract `shared/browse_service.py`, separate rate-limit bucket, exhaustive Plan 04 test surface) but converge on several design-level breakages that would manifest as runtime errors or silent contract violations on day one of execution.

### Agreed Strengths (raised by both)

- Wave decomposition is clean (01 primitives → 02 service → 03 endpoint → 04 tests).
- Statelessness is enforced via grep contract — both view the D-22 mechanism as appropriate.
- Rate-limit bucket topology (separate `_browse_rate_limiter` + independence test) cleanly satisfies D-18.
- D-12 Oxford correction (sys_id-keyed) is correctly reflected in Plan 01.
- Legacy-route immutability (D-25) is properly extended in Plan 04 Task 2.
- Decision traceability — D-01..D-27 + R-01..R-10 cited explicitly, not hand-waved.

### Agreed Concerns (HIGHEST PRIORITY — both reviewers raised)

1. **Wrong core fetch — minimal dict vs. hydrated `BrowsePage` (HIGH).** Plan 02's `_fetch_core` calls `state.searcher.get_browse_page()` (returns the minimal dict from `genizah_core.py:8246`) but Plan 01's `serialize_browse_payload` consumes fields (`shelfmark`, `title`, `library_code`, `library_name`, `library_viewer_url`, `volumes`, `cambridge_images`, parsed `fl_id`) that only exist on the `web/services.py:88` `BrowsePage` wrapper. Result: AttributeError → 500 on every successful resolution.
   - **Fix:** Switch `_fetch_core` to call `WebDataService.get_browse_page` / `get_browse_page_by_fl` from `web/services.py:294`. Drop the `SimpleNamespace` wrapping.

2. **`wrap_endpoint` not reused (HIGH per Gemini, LOW per Codex — adopt fix regardless).** Plan 03 hand-rolls the try/except/finally + `t0` + `capture_api_event` block instead of decorating with `@wrap_endpoint(endpoint='browse')`. CONTEXT.md explicitly mandates the helper is reused, not reimplemented.
   - **Fix:** Decorate `browse_endpoint` with `@wrap_endpoint(...)`. Strip duplicated boilerplate from the handler body.

### Codex-only HIGH concerns (not flagged by Gemini, but verified against canonical_refs)

3. **`uid` validated but never resolved (HIGH).** `_validate_locator()` raises on conflicts but does not parse `uid` into `effective_p_num` / `effective_volume_ie` / `effective_fl_id` to pass to `fetch_browse_bundle()`. The preferred uid-only path therefore only works by accident (when defaults coincide).
   - **Fix:** Make `_validate_locator()` return a normalized locator dataclass / dict with effective fields, or add an explicit normalization step in Plan 03 Task 1 Block C immediately after validation.

4. **D-14 graceful image degrade not actually implemented (HIGH).** Plan 01 builds URLs unconditionally; Plan 03 emits them and lets the proxy own availability. Neither performs an availability probe nor maps proxy 5xx → `image.url=null + warnings: ['image_unavailable']`. ROADMAP Success Criterion #3 is not met.
   - **Fix (option A):** Implement a HEAD probe or short-TTL source-health cache in `_build_browse_image()` (Plan 01) and gate `image.url` on the result.
   - **Fix (option B):** Reopen the locked D-14 decision: narrow the contract to "`image.url` is best-effort; clients must handle proxy errors themselves." Document the deviation in CONTEXT.md and ROADMAP success criterion #3.
   - Recommended path: option B (probe-based availability is expensive and adds latency to the skill's 5–10× browse calls per query). Treat D-14 as a real reopen; do not claim the requirement is met without implementation.

### Codex-only MEDIUM concerns (worth fixing pre-execution)

5. **Per-source try/except suppresses the warnings `_wrap_with_timeout` should emit (MEDIUM).** `_pgp_sync` / `_fjms_sync` / `_nli_sync` swallow exceptions and return `None`, so D-16's `enrichment_failed` warning never fires for normal service errors. Only timeouts surface; raises become silent nulls.
   - **Fix:** Let inner sync helpers raise; `_wrap_with_timeout` owns ALL warning emission (timeout + failed). Plan 02 Task 1.

6. **Test mocks diverge from real core shape (MEDIUM).** Plan 04's `mock_searcher` returns a fully-enriched dict; the real `SearchEngine.get_browse_page` does not. Tests will pass even when production breaks (the issue Concern #1 above).
   - **Fix:** At minimum one Plan 04 test must use the real `SearchEngine` shape (or use `WebDataService.get_browse_page` post-Concern-#1 fix). Tightens D-27.

7. **Round-trip test has serializer-direct fallback (MEDIUM).** `test_browse_locator_round_trip_from_search` allows bypassing `/api/search` HTTP and calling `serialize_search_payload` directly when the searcher is mocked — that's a unit test, not a round-trip.
   - **Fix:** Make the primary version a true HTTP `POST /api/search` → `GET /api/browse` flow. Keep the serializer-direct version only as a secondary explicit unit test.

### Lower-priority items

- **LOW (Gemini):** `serialize_browse_payload` declares `requested_uid` / `requested_fl_id` parameters that aren't used in the body. Either remove them or use them as fallback locator population.
- **LOW (Codex):** Plan 02 `depends_on: []` is missing implicit dep on Plan 01's `core_timeout` ERROR_CODES addition. Survivable but should be `depends_on: [79-01]`.

### Divergent Views

| Topic | Gemini | Codex | Resolution |
|-------|--------|-------|------------|
| `wrap_endpoint` reuse | HIGH | LOW | Treat as HIGH — CONTEXT.md mandates reuse. Strip hand-rolled boilerplate. |
| `uid` resolution gap | not raised | HIGH | Codex caught a real gap Gemini missed. Adopt the fix. |
| D-14 graceful degrade | not raised | HIGH | Codex caught it; Gemini implicitly accepted the plan-as-written. Adopt one of options A/B. |
| Per-source try/except | not raised | MEDIUM | Adopt fix. |
| Test mock shape | not raised | MEDIUM | Adopt fix; couples cleanly to Concern #1. |

Codex's deeper canonical_refs grounding (it cites exact line numbers in `genizah_core.py:8246` and `web/services.py:294`) caught contract-level gaps Gemini didn't probe for. Gemini's contribution was the cleanest surfacing of the metadata-hydration bug — both arrive at the same root cause from different angles.

---

## Recommended Next Step

```
/gsd-plan-phase 79 --reviews
```

This re-spawns the planner with REVIEWS.md as input. Expected revisions:

1. **Plan 02:** swap `_fetch_core` to `WebDataService.get_browse_page[_by_fl]`; drop SimpleNamespace; let inner sync helpers raise; depends_on: [79-01]; active-folio resolution by `fl_id` not `folio_images[p_num-1]`.
2. **Plan 03:** decorate handler with `@wrap_endpoint(endpoint='browse')`; strip try/except/finally + capture_api_event boilerplate; add normalization step that turns `uid` into effective {p_num, volume_ie, fl_id} before `fetch_browse_bundle()`; reopen D-14 with explicit narrowed contract OR add availability probe.
3. **Plan 01:** drop `requested_uid` / `requested_fl_id` from `serialize_browse_payload` signature OR use them; if D-14 is implemented via probe, expose hook here.
4. **Plan 04:** primary round-trip test does real HTTP `POST /api/search` → `GET /api/browse`; at least one test exercises real core shape (no mock); rate-limit and mock-divergence tests retained.

If D-14 is reopened (option B above), CONTEXT.md needs a new entry documenting the contract narrowing — this is a real decision change, not a clarification.
