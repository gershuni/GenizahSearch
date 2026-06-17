---
phase: 117
reviewers: [codex]
reviewed_at: 2026-06-17T15:54:55Z
plans_reviewed: [117-01-PLAN.md, 117-02-PLAN.md, 117-03-PLAN.md, 117-04-PLAN.md, 117-05-PLAN.md, 117-06-PLAN.md]
review_round: 2
prior_review: "git 63c9b1b9 (round 1, 2026-06-17T15:09Z)"
---

# Cross-AI Plan Review — Phase 117 (Vertical Spine) — ROUND 2 (post-replan)

> Reviewer: **Codex** (OpenAI CLI), run with full repository read access so it could verify
> plan claims against the actual source — the plan↔code drift check the internal
> gsd-plan-checker structurally cannot perform. Single external reviewer (`--codex`); Claude
> CLI skipped per runtime independence rule (this session IS Claude Code).
>
> **This is the round-2 re-review** of the plans after the `--reviews` replan that closed the
> round-1 findings. Its job: verify each prior finding is *genuinely* closed against the real
> code, and surface any NEW drift introduced by the revisions. Round-1 review is preserved in
> git at commit `63c9b1b9`.

## Codex Review

**Closure Verdict Table**

| Finding | Verdict | Justification |
|---|---|---|
| HIGH-1 | **PARTIAL** | Plans now keep `WebSearchExecutor.get_browse_page()` narrow and route AnchorViewer through `service.get_browse_page()` instead of the Protocol dict. That matches the narrow Protocol in `shared/joins_lab.py:176-186` and `genizah_core.py:9954-9964`. **But `web/services.py:340-362` does not populate `cambridge_images`, `external_provider`, or `cambridge_alignment`**; `/browse` gets those later via enrichment in `web/pages/browse_enrichment.py:240-253` and applies them at `:343-370`. Plan 06 assumes the service call alone is rich enough. |
| HIGH-2 | **CLOSED** | `/browse` still has the unsafe fallback at `web/pages/browse.py:4222-4230`, and `web/static/manuscript_viewer.js:47` / `:130` directly hit NLI IIIF. Plans 03/06 explicitly forbid `NLI_IIIF_BASE`, `iiif.nli.org.il`, `handleImageError`, and stop at placeholder: `117-03-PLAN.md:93-96`, `117-06-PLAN.md:44`, `:117`, `:145-156`. |
| HIGH-3 | **CLOSED** | Plan 04 now wires all three acceptance legs into the task: generation increment, prior-task cancel, `asyncio.wait_for`, and stale-generation discard at `117-04-PLAN.md:196-205`, with acceptance checks at `:212-215`. This closes the prior "early return only" gap. |
| MEDIUM-4 | **CLOSED** | Plan 01's AST guard now scans `web/pages/joins_lab.py` for any `.execute_search(...)`, including `executor.execute_search`, requires the nearest function to be sync, and proves that function is passed to `run.io_bound`: `117-01-PLAN.md:158-163`, `:171-172`. |
| MEDIUM-5 | **CLOSED** | Oxford direct Bodleian is now documented as an intentional exception in Plan 03 at `117-03-PLAN.md:20`, `:47`, `:159`, `:180`, matching current `/browse` behavior at `web/pages/browse.py:3618-3625` and helper `web/services.py:193-205`. |
| MEDIUM-6 | **PARTIAL** | Plan 05 removed the placeholder escape and derives thumbnails from `Candidate.sys_id/page`; `Candidate` indeed has no thumbnail field in `shared/joins_lab.py:103-108`. **But Plan 05 only builds `/api/nli_image_by_sysid/...` at `117-05-PLAN.md:105-108`; it does not implement a true per-provider thumbnail path** like the existing search card's Oxford fork at `web/pages/search_results.py:645-681`. |
| LOW-7 | **CLOSED** | Plan 01 requires `inspect.signature` compatibility for all four adapter methods, not just runtime Protocol presence: `117-01-PLAN.md:16`, `:126-145`, against the four-method Protocol in `shared/joins_lab.py:162-191`. |
| NOTE | **PARTIAL** | Plan 04 documents the locked login-gate decision at `117-04-PLAN.md:19`, `:52`, `:160`, but its rationale is **code-wrong**: web anonymous lists DO have a local fallback when `local_mgr` exists (`web/user_lists.py:55-56`, `:92-96`), and web startup wires `ListsManager` at `web/main.py:2267-2270` through `web/state.py:20-21`, `:51-52`. Keep D-06 if locked, but rewrite the rationale as an intentional divergence from an existing local-list path. |

**New Concerns**

- **HIGH:** AnchorViewer still lacks the actual `/browse` enrichment step needed for Cambridge/Manchester/JTS/synthetic external images. `service.get_browse_page()` returns the dataclass shape but not the external image fields (`web/services.py:340-362`); `/browse` fills them asynchronously through `browse_enrichment` (`web/pages/browse.py:1028-1033`, `web/pages/browse_enrichment.py:240-253`, `:343-370`). Plan 06 tests use injected fake rich objects (`117-06-PLAN.md:177-186`), so they will **not catch this real integration drift**.
- **MEDIUM:** Plan 04's cancellation cancels the asyncio task, but not necessarily the underlying `run.io_bound` worker thread. The existing search page has a cooperative cancellation pattern via `progress_cb` raising when cancelled (`web/pages/search.py:4055-4058`); Plan 04's callback only handles the string status protocol (`117-04-PLAN.md:206`). Add a cancellation flag checked in the progress callback if resource cancellation, not just stale UI discard, is required.

**Strengths**

- The revised plans correctly separate the narrow shared `SearchExecutor` Protocol from image-viewer resolution.
- The no-direct-NLI boundary is now explicit and testable for AnchorViewer and candidate thumbnails.
- The off-loop guard is meaningfully stronger and covers the realistic `executor.execute_search(...)` call shape.
- `safe_storage` is isolated into a small versioned helper with schema invalidation and no-state-bleed tests.

**Risk Assessment**

Overall risk: **HIGH.**

Ready to execute? **No.** The search/off-loop and NLI fallback issues are mostly closed, but the AnchorViewer image path still drifts from actual `/browse` behavior because the planned resolver source does not populate external-provider image metadata. Fix that before execution, or Phase 117 can pass tests while failing provider parity in real use.

---

## Consensus Summary

Single external reviewer (Codex). Round-2 re-review of the post-replan plans.

**Round-1 → Round-2 progress: 5 of 7 findings fully CLOSED, 2 PARTIAL, 1 new HIGH surfaced.**

### Closed (de-risks execution)
- **HIGH-2** (SSRF / NLI-breaker bypass) — no-direct-NLI mode + placeholder, grep-asserted in 03/05/06.
- **HIGH-3** (timeout/cancel/stale-gen) — all three legs (`asyncio.wait_for` + prior-task `.cancel()` + generation discard) wired into 117-04 task acceptance.
- **MEDIUM-4** (off-loop AST guard) — catches `executor.execute_search(...)` + proves `run.io_bound` enclosure.
- **MEDIUM-5** (Oxford proxy-only drift) — direct-Bodleian documented as an intentional non-breaker exception.
- **LOW-7** (Protocol test) — `inspect.signature` compatibility asserted for all 4 methods.

### Still Open / Needs Another Pass (priority order)
- **HIGH (was HIGH-1, deepened) — AnchorViewer image data is still under-specified against real `/browse`.** `service.get_browse_page()` (`web/services.py:340-362`) returns the `BrowsePage` dataclass *shape* but does NOT populate the external-provider image fields (`cambridge_images`, `external_provider`, `cambridge_alignment`) — `/browse` fills those asynchronously via `browse_enrichment` (`browse_enrichment.py:240-253`, applied `browse.py:343-370`). Plan 06 assumes the bare service call is rich enough, and its tests inject fake rich objects so they won't catch the gap. → Plan 06 (and 03) must wire (or explicitly invoke) the browse enrichment path, or scope the spine to NLI/Oxford-only images with the multi-provider path deferred and documented. The test must exercise a real (non-injected) resolution for at least one external provider, or the must-have must be narrowed.
- **MEDIUM (was MEDIUM-6, partial) — candidate thumbnail is NLI-only.** Plan 05 builds only `/api/nli_image_by_sysid/...`; it lacks the per-provider fork (Oxford etc.) that `search_results.py:645-681` uses. → Either add the per-provider fork or document that spine thumbnails are NLI-proxy-only with non-NLI providers deferred.
- **MEDIUM (new) — task cancellation may not free the worker thread.** 117-04 cancels the asyncio task but the `run.io_bound` worker keeps running; only stale-UI discard happens. → Add a cooperative cancellation flag checked inside `progress_cb` (mirror `search.py:4055-4058`) if true resource cancellation is required by SC#3.
- **PARTIAL (NOTE) — login-gate rationale is code-wrong.** D-06 is a locked decision and may stay, but its plan rationale claims web anonymous users have no local list path; `web/user_lists.py:55-56/:92-96` + `web/main.py:2267-2270` show a local `ListsManager` fallback exists. → Rewrite the rationale as an *intentional divergence* from the existing local-list path, not "no local path exists."

### Divergent Views
None — single reviewer.

### Overall Risk (Codex): HIGH — NOT ready to execute
The adapter, safe_storage, off-loop, and NLI-boundary work is sound. The blocker is the AnchorViewer external-provider image path (real enrichment not wired; tests use fakes). Resolve the HIGH + the two MEDIUMs (or explicitly scope/defer the multi-provider image path with documented narrowing) via one more `/gsd:plan-phase 117 --reviews` pass before executing.
