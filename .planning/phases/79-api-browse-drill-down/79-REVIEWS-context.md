# Phase 79 Cross-AI Review — Codex CLI

**Reviewed:** 2026-04-29
**Reviewer:** Codex CLI (gpt-5.4, reasoning_effort: high)
**Source prompt:** `_tmp/codex-review-79.md`
**Tokens used:** 147,567

> Codex did multi-turn investigation of the codebase before answering — read `web/api_hardening.py`, `web/search_api.py`, `genizah_core.py:get_browse_page`, `web/services.py:BrowsePage`, `docs/OPEN_ISSUES.md`, etc. Recommendations are grounded in repo state, not generic advice.

---

## Q1

Recommendation: Keep `sys_id` required in v1. Treat `uid` as the preferred page pin inside a `sys_id`-scoped request, not as a self-sufficient locator yet.

Rationale: The current core browse entry points are `get_browse_page(sys_id, ...)` and `get_browse_page_by_fl(fl_id, sys_id=None)`; they are fundamentally `sys_id`-centric, and the `uid` format (`IE..._P..._FL...`) does not carry `sys_id`. Adding uid-only support now means building and maintaining a reverse index purely for API ergonomics, while `/api/search` already returns the needed `sys_id` for the exact follow-up flow this endpoint serves.

Pitfall: Do not silently trust mismatched `sys_id` + `uid`. Resolve within the supplied `sys_id` and verify the resolved page’s `uid` matches the requested `uid`, or fail the request.

## Q2

Recommendation: Keep per-source timeouts, lower the default to about `1s`, and add a timeout for the core fetch as well. If you want an outer guard, add a whole-request deadline on top of the per-source caps.

Rationale: These enrichments are local sidecar lookups, so `2s` is too lenient for an interactive skill that may call `/api/browse` 5-10 times. Per-source caps are better than a shared budget because they preserve deterministic partial-data semantics; a shared budget makes whichever source runs last pay the price. The core path should not be exempt, because “deadlocked thread” is exactly the case that needs a timeout.

Pitfall: `asyncio.wait_for()` around threaded sync work only times out the awaiter; it does not kill the underlying worker thread. If hangs recur, you can still exhaust the executor unless you monitor it.

## Q3

Recommendation: Use separate per-endpoint buckets, but give `/api/browse` its own optional env override rather than permanently tying both endpoints to one shared numeric ceiling.

Rationale: `search -> browse xN` is the normal workflow here, so a shared bucket would punish valid usage and make the API feel artificially brittle. Lowering both limits now just to preserve a theoretical combined ceiling is premature; the cleaner design is separate buckets plus independent tuning once you have traffic data.

Pitfall: If both endpoints inherit the same value with no global backstop, the effective aggregate per-IP allowance roughly doubles. Watch logs and be ready to add a coarse global cap upstream if abuse shows up.

## Q4

Recommendation: Keep a scalar canonical `image.url` and keep role-tagged `sources[]`. Do not turn `image` into a list for CUDL bifolios.

Rationale: The consumer wants one obvious primary URL for the common path, but alternates are worth returning in-band because library image surfaces are fragile and a second round-trip adds avoidable failure points. Bifolios are still one logical page with related companions, so `role: companion_folio` is the right relationship model; just make each source self-describing enough for a client to use safely.

Pitfall: If `sources[]` can contain both direct image URLs and viewer landing pages, add a field like `kind: image|viewer` and ideally `fl_id`/`folio_label` per source. Otherwise clients will guess wrong about what is directly renderable.

## Q5

Recommendation: Keep `metadata: {pgp, fjms, nli}`.

Rationale: It preserves provenance, keeps nullability simple, and is the easiest structure to evolve when Phase 80 adds fields. For a Claude-formatting skill, one extra `metadata.` hop is cheaper than flattening everything and then re-encoding provenance in field names or prompt logic.

Pitfall: Be strict about absence semantics. Each group should be either `null` or a stable object shape, not sometimes `null` and sometimes `{}`.

## Q6

Recommendation: Keep `4000` as the default, and allow an opt-in `text_cap` override with a hard server-side maximum.

Rationale: `/api/browse` is a grounding endpoint, so defaulting a bit generous is safer than defaulting too lean and forcing retry fetches for citation work. `4000` chars is not excessive for page-scoped text, and the real flexibility comes from letting advanced clients request less for ranking passes or somewhat more for citation passes.

Pitfall: Cap `text_cap` hard. I would keep the max closer to `10000` than `20000`, otherwise payload inflation and cache fragmentation become easy to trigger.

## Pushback on locked decisions

- Add `fl_id` to the response `locator`, not just under `metadata.nli.folio`. Accepted locator inputs should round-trip from the response even when NLI enrichment is absent or timed out.
- Do not silently accept conflicting pin forms. If a request supplies `uid` plus `fl_id` or `p_num/volume_ie`, verify they identify the same page or return a client error.
- The locked `2s` enrichment timeout is too slow as a default for this workflow. Local sidecar lookups should fail fast.
- The locked exemption for core timeouts is weak. A stuck core resolver is still an API failure mode and should surface as one.
- `image.sources[]` should not promise “always non-empty” unless you can actually guarantee a usable URL exists. If there is genuinely no image or viewer target, `[]` is a more honest contract than a synthetic placeholder.
tokens used
147,567
## Q1

Recommendation: Keep `sys_id` required in v1. Treat `uid` as the preferred page pin inside a `sys_id`-scoped request, not as a self-sufficient locator yet.

Rationale: The current core browse entry points are `get_browse_page(sys_id, ...)` and `get_browse_page_by_fl(fl_id, sys_id=None)`; they are fundamentally `sys_id`-centric, and the `uid` format (`IE..._P..._FL...`) does not carry `sys_id`. Adding uid-only support now means building and maintaining a reverse index purely for API ergonomics, while `/api/search` already returns the needed `sys_id` for the exact follow-up flow this endpoint serves.

Pitfall: Do not silently trust mismatched `sys_id` + `uid`. Resolve within the supplied `sys_id` and verify the resolved page’s `uid` matches the requested `uid`, or fail the request.

## Q2

Recommendation: Keep per-source timeouts, lower the default to about `1s`, and add a timeout for the core fetch as well. If you want an outer guard, add a whole-request deadline on top of the per-source caps.

Rationale: These enrichments are local sidecar lookups, so `2s` is too lenient for an interactive skill that may call `/api/browse` 5-10 times. Per-source caps are better than a shared budget because they preserve deterministic partial-data semantics; a shared budget makes whichever source runs last pay the price. The core path should not be exempt, because “deadlocked thread” is exactly the case that needs a timeout.

Pitfall: `asyncio.wait_for()` around threaded sync work only times out the awaiter; it does not kill the underlying worker thread. If hangs recur, you can still exhaust the executor unless you monitor it.

## Q3

Recommendation: Use separate per-endpoint buckets, but give `/api/browse` its own optional env override rather than permanently tying both endpoints to one shared numeric ceiling.

Rationale: `search -> browse xN` is the normal workflow here, so a shared bucket would punish valid usage and make the API feel artificially brittle. Lowering both limits now just to preserve a theoretical combined ceiling is premature; the cleaner design is separate buckets plus independent tuning once you have traffic data.

Pitfall: If both endpoints inherit the same value with no global backstop, the effective aggregate per-IP allowance roughly doubles. Watch logs and be ready to add a coarse global cap upstream if abuse shows up.

## Q4

Recommendation: Keep a scalar canonical `image.url` and keep role-tagged `sources[]`. Do not turn `image` into a list for CUDL bifolios.

Rationale: The consumer wants one obvious primary URL for the common path, but alternates are worth returning in-band because library image surfaces are fragile and a second round-trip adds avoidable failure points. Bifolios are still one logical page with related companions, so `role: companion_folio` is the right relationship model; just make each source self-describing enough for a client to use safely.

Pitfall: If `sources[]` can contain both direct image URLs and viewer landing pages, add a field like `kind: image|viewer` and ideally `fl_id`/`folio_label` per source. Otherwise clients will guess wrong about what is directly renderable.

## Q5

Recommendation: Keep `metadata: {pgp, fjms, nli}`.

Rationale: It preserves provenance, keeps nullability simple, and is the easiest structure to evolve when Phase 80 adds fields. For a Claude-formatting skill, one extra `metadata.` hop is cheaper than flattening everything and then re-encoding provenance in field names or prompt logic.

Pitfall: Be strict about absence semantics. Each group should be either `null` or a stable object shape, not sometimes `null` and sometimes `{}`.
