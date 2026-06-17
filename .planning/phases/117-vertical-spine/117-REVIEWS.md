---
phase: 117
reviewers: [codex]
reviewed_at: 2026-06-17T15:09:15Z
plans_reviewed: [117-01-PLAN.md, 117-02-PLAN.md, 117-03-PLAN.md, 117-04-PLAN.md, 117-05-PLAN.md, 117-06-PLAN.md]
---

# Cross-AI Plan Review — Phase 117 (Vertical Spine)

> Reviewer: **Codex** (OpenAI CLI), run with full repository read access so it could
> verify plan claims against the actual source — the plan↔code drift check the internal
> gsd-plan-checker structurally cannot perform. Single external reviewer (`--codex`);
> Claude CLI skipped per runtime independence rule (this session IS Claude Code).

## Codex Review

**Summary**

The plans are directionally solid on the core adapter, shared `compose`/`dedup_candidates` pipeline, and `safe_storage` invariant. The biggest plan↔code drift is in the anchor image path: the plans assume `WebSearchExecutor.get_browse_page()` provides enough data to drive `/browse`’s provider-aware image resolver, but the real `SearchEngine.get_browse_page()` returns only text/navigation fields. The current `/browse` image behavior also has a direct NLI client-side fallback that would violate the “never direct IIIF” rule if copied unchanged. Overall: good spine concept, but the image/viewer seam and off-loop cancellation/guard tests need tightening before implementation.

**Plan↔Code Drift Findings**

- Verified correct: [shared/joins_lab.py](C:/Genizahsearch/shared/joins_lab.py:149) defines `SearchExecutor` as a runtime-checkable 4-method Protocol: `execute_search`, `get_browse_page`, `get_meta_for_id`, `get_library_for_id` at lines 162, 176, 188, 191. The plan’s 4-method shape is correct.

- Verified correct: [genizah_core.py](C:/Genizahsearch/genizah_core.py:3819) has `MetadataManager`; `get_meta_for_id` is at line 3968 and returns `(shelf, title)` at line 4002. `get_library_for_id` is at line 4004.

- Verified correct: [genizah_core.py](C:/Genizahsearch/genizah_core.py:7049) has `SearchEngine`; `execute_search` is at line 8600 with `responsa_options`, `restrict_sys_ids`, `text_position`, and `corpus_scope`. `get_browse_page` is at line 9869 with the Protocol-compatible signature.

- Verified correct: the dual progress-callback protocol is real. [genizah_core.py](C:/Genizahsearch/genizah_core.py:1055) calls `progress_callback(i, total_hits)`, then line 1069 calls `progress_callback("Scanning items ...")`. [web/pages/parallels.py](C:/Genizahsearch/web/pages/parallels.py:2138) has the correct string-arg guard at lines 2145-2148.

- Verified with nuance: [web/pages/search.py](C:/Genizahsearch/web/pages/search.py:3979) has the `is_running` guard, cancellation flag, generation counter, sync `run_core_search`, and `await run.io_bound(run_core_search)` at lines 3981, 4030-4036, 4055-4058, 4157-4189. However, its `search_generation` is mainly used to protect enrichment application later, e.g. lines 4285 and 4639-4713, not as a literal immediate stale-core-result discard after `run.io_bound`.

- Drift: Plan 117-01’s proposed off-loop static guard is too narrow if it only scans for `state.searcher.execute_search` in `web/pages/joins_lab.py`. The planned page calls through `executor.execute_search`, while [web/joins_executor.py] will be the file containing the actual `state.searcher.execute_search` call. The guard must detect direct `executor.execute_search(...)` calls in async handlers too.

- Drift: timeout/cancellation are required by the roadmap text, but Plan 117-04 mostly specifies `_is_running` plus stale generation. With `if _is_running: return`, a second click cannot start a newer generation, and there is no cancel button or `asyncio.wait_for` equivalent. That does not satisfy “timeout, cancellation, and stale-generation” as written.

- Verified correct: [web/safe_storage.py](C:/Genizahsearch/web/safe_storage.py:46) exposes `safe_user_get`, `safe_user_set`, and `safe_user_pop` at lines 46, 63, and 76. [tests/test_no_raw_storage_access.py](C:/Genizahsearch/tests/test_no_raw_storage_access.py:365) skips only `safe_storage.py`; [.planning/phase87_storage_allowlist.yaml](C:/Genizahsearch/.planning/phase87_storage_allowlist.yaml:20) confirms `allowed_raw_access: []`.

- Major drift: `SearchEngine.get_browse_page()` returns only `uid`, `p_num`, `full_header`, `text`, `total_pages`, `current_idx`, `internal_index`, `sys_id`, `volume_ie` at [genizah_core.py](C:/Genizahsearch/genizah_core.py:9954). But `/browse` image resolution needs richer `BrowsePage` fields such as `shelfmark`, `is_oxford`, `library_code`, `volume_suffix`, `cambridge_images`, `external_provider`, and `cambridge_alignment`, defined in [web/services.py](C:/Genizahsearch/web/services.py:89). Plan 117-06 cannot drive full provider-aware images from `WebSearchExecutor.get_browse_page()` alone.

- Verified correct with defect risk: `/browse` initially routes NLI through `/api/nli_image_by_sysid` at [web/pages/browse.py](C:/Genizahsearch/web/pages/browse.py:3645), and that endpoint is breaker-guarded through [web/api.py](C:/Genizahsearch/web/api.py:940) and [web/api.py](C:/Genizahsearch/web/api.py:1012). But `/browse` attaches `onerror="handleImageError(...)"` at [web/pages/browse.py](C:/Genizahsearch/web/pages/browse.py:4230), and [web/static/manuscript_viewer.js](C:/Genizahsearch/web/static/manuscript_viewer.js:47) directly fetches NLI manifests and line 130 constructs direct `iiif.nli.org.il` image URLs. AnchorViewer must not reuse that fallback unchanged.

- Drift: the plans repeatedly describe “provider proxy only,” but current `/browse` prefers direct Oxford browser URLs when derivable: [web/services.py](C:/Genizahsearch/web/services.py:193) builds direct Bodleian URLs, and [web/pages/browse.py](C:/Genizahsearch/web/pages/browse.py:3619) prefers `get_oxford_direct_image_url()` before `/api/oxford_image`. This is not an NLI breaker issue, but it is plan↔code drift.

- Verified correct: [desktop/join_workbench.py](C:/Genizahsearch/desktop/join_workbench.py:1473) has `_DesktopSearchExecutor` with the same four methods. Note: only `execute_search` catches exceptions and returns `[]` at lines 1497-1511; the desktop `get_browse_page`/metadata methods do not catch and fallback. The web plan’s broader fallback behavior is a web-specific hardening, not an exact mirror.

- Drift: Plan 117-05 allows a thumbnail placeholder “if non-trivial,” but CND-02 and the plan must-have say thumbnail. `Candidate` has no thumbnail field in [shared/joins_lab.py](C:/Genizahsearch/shared/joins_lab.py:103), so the component must derive a proxy thumbnail from `sys_id/page` or reuse search-result thumbnail logic from [web/pages/search_results.py](C:/Genizahsearch/web/pages/search_results.py:645), not silently degrade to placeholders.

- Drift: the “anonymous choose from my lists prompts login” assumption does not match current list architecture. [web/user_lists.py](C:/Genizahsearch/web/user_lists.py:55) explicitly says logged-out users use local `ListsManager`, and `.data` returns local data at lines 92-96. If the user decision overrides this, document the intentional divergence.

**Strengths**

- Correctly identifies the adapter as the riskiest seam and avoids `/api/search`.
- Correctly uses shared `BuilderRow`, `SideQuery`, `compose()`, and `dedup_candidates()`.
- Keeps most Phase 118/119/120 scope out: no toggles, triage, table, Compare, VS, or actions in the spine.
- The `safe_storage` plan matches the existing chokepoint and empty allowlist invariant.
- Promoting the line-numbered transcription helper is a good refactor; the existing helper is pure and escapes raw text.

**Concerns**

- HIGH: AnchorViewer is planned against the wrong browse data shape; `SearchExecutor.get_browse_page()` is too narrow for provider-aware image resolution.
- HIGH: Reusing `manuscript_viewer.js` error fallback can bypass the Phase-98 NLI breaker with direct IIIF URLs.
- HIGH: Timeout/cancellation are claimed but not actually designed; `_is_running` prevents latest-wins from handling rapid reruns.
- MEDIUM: The off-loop AST guard can miss direct `executor.execute_search()` calls unless expanded.
- MEDIUM: Oxford “proxy-only” wording conflicts with current direct-Bodleian behavior.
- MEDIUM: Candidate grid thumbnail requirements may be missed if placeholders are accepted.
- LOW: `isinstance(WebSearchExecutor(), SearchExecutor)` only checks method presence at runtime, not signature compatibility.

**Suggestions**

- Keep `WebSearchExecutor` narrow for the shared Protocol, but give `AnchorViewer` a separate web browse-page resolver using `web.services.service.get_browse_page()` plus the existing browse enrichment path where provider metadata is needed.
- Add an explicit no-direct-NLI mode for AnchorViewer: do not call `handleImageError`, or add a safe error handler that stops at proxy failure and renders the inline placeholder.
- Strengthen the off-loop test to catch `executor.execute_search(...)` in `async def`, and prove the enclosing sync function is actually passed to `run.io_bound`.
- Add `asyncio.wait_for(await run.io_bound(...))` or remove “timeout” from the success criterion. Add a cancel path if cancellation is required.
- Test `inspect.signature(WebSearchExecutor.execute_search)` against the Protocol/engine signature, not just `isinstance`.
- Make thumbnail derivation explicit for candidates, probably via the same sys_id/page proxy logic used by search result cards.

**Risk Assessment**

HIGH. The protocol adapter and storage plans are sound, but the anchor image path is currently under-specified against the real code, and the direct-NLI fallback would violate a production safety boundary if copied. The off-loop execution plan also needs real timeout/cancellation semantics and a stronger guard before it can be considered a reliable de-risking spine.

---

## Consensus Summary

Single external reviewer (Codex). Findings below are Codex's, triaged by severity. The
review's headline: the *adapter + safe_storage spine is sound and its code assumptions
verified*, but the **anchor image/viewer seam and the off-loop timeout/cancellation
semantics are under-specified against the real code** and need tightening before execution.

### Verified Correct (de-risks execution)
- `SearchExecutor` 4-method Protocol shape (`shared/joins_lab.py:149`).
- A1–A3 method ownership/locations: `execute_search`:8600 / `get_browse_page`:9869 on `SearchEngine`; `get_meta_for_id`:3968 / `get_library_for_id`:4004 on `MetadataManager`.
- Dual progress-callback protocol is real (`genizah_core.py:1055`/`:1069`); the string-arg guard exists (`parallels.py:2145-2148`).
- `safe_user_get/set/pop` exist; `tests/test_no_raw_storage_access.py` allowlist is `[]`.
- Off-loop `run.io_bound` + generation-token pattern exists in `search.py:3979+`.
- Desktop `_DesktopSearchExecutor` parity (`join_workbench.py:1473`).

### Agreed Concerns (priority order)
- **HIGH — AnchorViewer planned against the wrong data shape.** `SearchEngine.get_browse_page()` (`genizah_core.py:9954`) returns only text/nav fields; provider-aware image resolution needs the richer `BrowsePage` fields (`shelfmark`, `is_oxford`, `library_code`, `cambridge_images`, `external_provider`, …) defined in `web/services.py:89`. Plan 117-06 cannot drive full provider images from `WebSearchExecutor.get_browse_page()` alone. → Give AnchorViewer a separate web browse-page resolver via `web.services.service.get_browse_page()`; keep `WebSearchExecutor` narrow for the shared Protocol.
- **HIGH — SSRF/breaker bypass risk.** `/browse` attaches `onerror="handleImageError(...)"` (`browse.py:4230`) and `manuscript_viewer.js:47/:130` fetches NLI manifests + builds direct `iiif.nli.org.il` URLs, bypassing the Phase-98 breaker. AnchorViewer must NOT reuse that fallback unchanged — add a no-direct-NLI error mode that stops at proxy failure and renders the inline placeholder.
- **HIGH — Timeout/cancellation claimed but not designed.** SC#3 demands "timeout, cancellation, and stale-generation handling," but Plan 117-04 only specifies `_is_running` + stale generation. `if _is_running: return` blocks latest-wins on rapid reruns and there's no `asyncio.wait_for`/cancel path. → Add `asyncio.wait_for` (or drop "timeout" from the criterion) and a real cancel/latest-wins path.
- **MEDIUM — Off-loop AST guard too narrow.** Plan 117-01's guard scanning only `state.searcher.execute_search` in `joins_lab.py` misses the page's `executor.execute_search(...)` call (the real engine call lives in `web/joins_executor.py`). Guard must catch direct `executor.execute_search()` in async handlers and prove the enclosing sync fn is passed to `run.io_bound`.
- **MEDIUM — Oxford "proxy-only" wording conflicts with code.** `/browse` prefers direct Bodleian URLs (`web/services.py:193`, `browse.py:3619` → `get_oxford_direct_image_url()`). Reconcile the "proxy-only" plan language with this existing behavior (not an NLI-breaker issue, but drift).
- **MEDIUM — Candidate thumbnail may degrade to placeholder.** Plan 117-05 allows a placeholder "if non-trivial," but CND-02 requires a thumbnail and `Candidate` has no thumbnail field (`shared/joins_lab.py:103`). Derive a proxy thumbnail from `sys_id/page` (reuse `search_results.py:645` logic), don't silently degrade.
- **LOW — Protocol test too weak.** `isinstance(WebSearchExecutor(), SearchExecutor)` only checks method presence; also assert `inspect.signature` compatibility.
- **Note — "Choose from my lists → login" assumption.** `web/user_lists.py:55` says logged-out users use local `ListsManager`, so there may be no login gate to trigger. If the user decision intends to override this, document the intentional divergence.

### Divergent Views
None — single reviewer.

### Overall Risk (Codex): HIGH
Adapter + storage plans sound; the anchor image path and off-loop timeout/cancellation need to be tightened before implementation.
