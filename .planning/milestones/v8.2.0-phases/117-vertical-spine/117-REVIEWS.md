---
phase: 117
reviewers: [codex]
reviewed_at: 2026-06-17T16:30:00Z
plans_reviewed: [117-01-PLAN.md, 117-02-PLAN.md, 117-03-PLAN.md, 117-04-PLAN.md, 117-05-PLAN.md, 117-06-PLAN.md]
review_rounds: 4
ready_to_execute: true
final_risk: LOW
prior_rounds:
  - "round 1 (initial): git 63c9b1b9"
  - "round 2 (post --reviews replan): git 9b380485"
---

# Cross-AI Plan Review — Phase 117 (Vertical Spine) — FINAL (round 4)

> Reviewer: **Codex** (OpenAI CLI), full-repo read access (verifies plan claims against actual
> source — the plan↔code drift check the internal gsd-plan-checker structurally cannot perform).
> Single external reviewer (`--codex`); Claude CLI skipped per runtime independence (this session IS
> Claude Code). Four rounds: round 1 reviewed the initial plans; round 2 re-reviewed after the
> `--reviews` replan; rounds 3–4 verified inline fixes. **Final verdict: all findings CLOSED,
> overall risk LOW, ready to execute.** Earlier-round REVIEWS.md content is preserved in git
> (round 1 = `63c9b1b9`, round 2 = `9b380485`).

## Final Closure Table (all findings across all rounds)

| Finding | Final verdict | Where closed | Code-grounded justification |
|---|---|---|---|
| **HIGH-1 / new-HIGH** — AnchorViewer image data shape + external-provider enrichment | **CLOSED** (r3→r4) | 117-03 + 117-06 | `service.get_browse_page()` returns the rich BrowsePage but leaves `cambridge_images/external_provider/cambridge_alignment` at defaults (`web/services.py:340-362`); `/browse` fills them via `enrich_metadata`+`nli_cache` (`browse_enrichment.py:240-253`, applied `:365-370`). Plan 03 extracts a shared `resolve_external_images(sys_id)` and refactors browse_enrichment to call it (one source of truth, D-10); Plan 06's AnchorViewer calls it OFF-loop (`run.io_bound`, async `update_content`) before `resolve_image_url`, with a **non-injected** Cambridge wiring test (empty cambridge_images + external_resolver spy → would fail if the resolver were skipped). |
| **HIGH-2** — SSRF / NLI-breaker bypass | **CLOSED** (r2) | 117-03/05/06 | No `handleImageError`/`iiif.nli.org.il`/`NLI_IIIF_BASE`; no-direct-NLI placeholder mode; grep + behavioral asserts; threat models cover the boundary. |
| **HIGH-3** — timeout/cancel/stale-generation | **CLOSED** (r2) | 117-04 | `asyncio.wait_for` timeout + latest-wins `.cancel()` + `_search_generation` discard; blunt `if _is_running: return` removed. |
| **MEDIUM-4** — off-loop AST guard too narrow | **CLOSED** (r2) | 117-01/04 | Guard catches `executor.execute_search(...)` + proves `run.io_bound` enclosure; synthetic-violation sub-tests. |
| **MEDIUM-5** — Oxford "proxy-only" drift | **CLOSED** (r2) | 117-03/06 | Direct-Bodleian documented as an intentional non-breaker exception. |
| **MEDIUM-6** — candidate thumbnail NLI-only | **CLOSED** (r3→r4) | 117-05 | `build_thumbnail_url` gains `shelfmark`/`library_code` + the Oxford fork mirroring `search_results.py:666-681` (`is_oxford_manuscript` → `get_oxford_direct_image_url` / `/api/oxford_image`); Cambridge/Manchester/JTS per-card thumbnails deferred to Phase 119/CND-08 as the search card does. |
| **MEDIUM (new, r3)** — cancellation didn't free the worker / description code-wrong | **CLOSED** (r4) | 117-01/04 | `_make_progress_cb(my_gen, gen_ref)` raises `InterruptedError` when superseded; `SearchEngine.execute_search` CATCHES it internally (`genizah_core.py:9000`), aborts the scan early (frees the worker) and returns PARTIAL results (`:9005/:9071`) — it does NOT re-raise. Plan 01's false re-raise was reverted; discard is via the stale-generation guard `_should_apply_results`; an end-to-end discard test mimics the core's catch-and-return-partial. |
| **LOW-7** — Protocol test too weak | **CLOSED** (r2) | 117-01 | `inspect.signature` compatibility asserted for all 4 adapter methods. |
| **LOW (r3)** — apply-block miscited as `browse.py:365-370` | **CLOSED** (r4) | 117-03/06 | Corrected to `web/pages/browse_enrichment.py:365-370`. |
| **LOW (r3)** — Plan 05 imported Oxford helpers from heavy `web.pages.browse` | **CLOSED** (r4) | 117-05 | Specified to import from `web.services` (defined at `web/services.py:185/193`). |
| **NOTE** — "my lists → login" rationale code-wrong | **CLOSED** (r4) | 117-04 | Corrected: `UserListsManager.data` DOES fall back to a local `ListsManager` (`web/user_lists.py:92-96`) wired process-wide (`web/main.py:2270` → `state._local_lists_mgr`); D-06 gates on login because that store is a single shared process-global pkl, not per-user (Phase 87–89 multitenant posture). |

## Round-4 Verdict (Codex, verbatim summary)

- Finding #3 (cancellation): **CLOSED** — code-correct now (`genizah_core.py:9000/:9005/:9071`); Plan 01 drops the re-raise, Plan 04 discards via `_should_apply_results` and adds the fake-core test path.
- The 2 LOWs: **fixed** (citation → `browse_enrichment.py:365-370`; Plan 05 imports from `web.services`).
- New concerns: **none** in the active execution plans.
- **Overall risk: LOW. Ready to execute? YES.**

## Plan 02 (safe_storage)
"Verified correct" by Codex in round 1 with no findings — unchanged across all rounds.

## Divergent Views
None — single reviewer.
