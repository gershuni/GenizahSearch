---
phase: 57
reviewers: [gemini, codex]
reviewed_at: 2026-03-30T02:15:00Z
plans_reviewed: [57-01-PLAN.md, 57-02-PLAN.md, 57-03-PLAN.md]
---

# Cross-AI Plan Review -- Phase 57

## Gemini Review

The overall plan is **highly robust** and well-structured, following a logical progression from data foundation (Wave 1) to multi-platform UI implementation (Wave 2). The strategy of using a dedicated SQLite sidecar (`visual_similarity.db`) is the correct architectural choice to handle the ~15.5M unique pairs without bloating the primary manuscript database. The technical approach respects the project's established patterns (ThreadLocalConnection, shared service layer, and QThread-based desktop fetching). The inclusion of specific UI themes (orange) and Hebrew translations ensures the feature will feel native to the existing research workflow.

### Strengths
- **Architectural Consistency:** Reusing the `shared/` service pattern ensures logic for union/intersection and suggestion ranking is identical across Web and Desktop.
- **TDD Strategy:** Starting with Wave 0 test stubs that throw `NotImplementedError` is an excellent way to define the contract before implementation.
- **Performance Awareness:** The plan addresses AlmaId type mismatch (INT vs TEXT) and dataset size by proposing a dedicated sidecar and CASTing at the service boundary.
- **UX Continuity:** Integration across multiple contexts (Browse, ResultDialog, Advanced View) ensures the feature isn't siloed.
- **Desktop Strategy:** Local cache with optional full DB download perfectly balances install size with power-user performance.

### Concerns
- **[HIGH] Import Script Performance & Scalability** — 35.9M rows needs optimized SQL (PRAGMA tuning, in-SQL joins vs Python loops), or import could take hours/crash.
- **[MEDIUM] Large File Download Reliability** — 700MB FileResponse without resume (Range header) support. Users may get stuck/corrupted downloads.
- **[MEDIUM] Search State Transfer** — `SearchUIState` tab storage may lose VS context on refresh/new tab. URL params should persist the restriction.
- **[LOW] Data Staleness** — No cache invalidation strategy. Server DB updates won't reach desktop users until manual cache clear.

### Suggestions
- **Import Optimization:** Use `PRAGMA synchronous = OFF; journal_mode = MEMORY;` during import. Consider in-SQL DocumentID->AlmaId mapping.
- **URL Persistence:** Push `vs_restrict` source ID into URL params for shareable "Search within VS" links.
- **Color-Blind Accessibility:** Use a specific icon alongside orange text for Visual Similarity to help color-blind users.
- **Robust Downloader:** Use streaming download with Content-Length check, percentage progress, and retry logic.
- **Cache Metadata:** Store version/timestamp in DesktopVSCache for re-download prompts on server update.

### Risk Assessment
**MEDIUM** — Logic and UI are sound, but data volume (15.5M pairs) is the primary risk. SQLite will work well if indexed correctly but feel sluggish if queries are unoptimized. 700MB download adds deployment complexity.

---

## Codex Review

### Plan 57-01 Assessment
The right foundation plan with clean separation. Main weakness: does not define the runtime contract strongly enough for Wave 2, especially for multi-manuscript union/intersection partner pools and desktop fetch behavior.

**Strengths:** Clean order (import→service→tests→API), shared-service architecture, batch_has_suggestions avoids N+1, API-first thinking.

**Concerns:**
- **[HIGH]** API endpoints may not cover all Wave 2 needs (partner-pool resolution for union/intersection, browse-suggestions result generation).
- **[HIGH]** Deduplication, self-pair removal, canonical pair handling, and rank generation rules not explicitly defined at import time.
- **[HIGH]** Performance/indexing strategy underspecified for 15.5M-pair dataset.
- **[MEDIUM]** D-05 (future extensibility) schema affordances not mentioned.
- **[MEDIUM]** `batch_check` needs request size limits.
- **[MEDIUM]** "Enriched JSON" response schema not fixed — Wave 2 parallel work will drift.

### Plan 57-02 Assessment
Strong on UX, aligns well with D-06 through D-14. Main risk: more complete on browse UX than search-state semantics, doesn't fully prove JOIN-03 is satisfied in result rendering.

**Concerns:**
- **[HIGH]** JOIN-03 not fully covered — result row indicators ≠ "show join partners with visual distinction."
- **[HIGH]** Depends on service/API semantics not yet fully specified in Plan 01.
- **[MEDIUM]** Tab storage cross-page state may create stale state/multi-tab confusion.
- **[MEDIUM]** Interaction with existing `restrict_sys_ids` not spelled out.
- **[MEDIUM]** D-10 broader than Browse + Search page — plan doesn't clearly cover all web entry points.

### Plan 57-03 Assessment
Respects desktop constraints well, correct cache-first/fetch-second pattern. Biggest issues: scope and operational detail.

**Concerns:**
- **[HIGH]** Full DB download operationally underdesigned (checksum, versioning, resume/retry, corruption detection, disk-space validation).
- **[HIGH]** Mixes desktop work with web files (`web/pages/settings.py`, `web/api.py`) — increases coordination risk.
- **[MEDIUM]** No automated tests for cache hit/miss, fetch failure, stale cache, thread cancellation.
- **[MEDIUM]** Cache invalidation/version upgrade behavior missing.

### Cross-Plan Assessment
- **[HIGH]** Wave 2 parallelization premature unless Wave 1 includes frozen API/schema/state contract.
- **[HIGH]** JOIN-03 not explicitly implemented end-to-end.
- **[MEDIUM]** D-10 ownership fragmented across plans.
- Suggests treating full DB download as optional stretch scope.

### Risk Assessment
**MEDIUM-HIGH** — Feature set achievable if Wave 1 contract is tightened and desktop download path doesn't dominate the phase. Core risk is search semantics and cross-surface behavior, not data import.

---

## Consensus Summary

### Agreed Strengths (both reviewers)
- Architectural consistency with existing shared-service and enrichment-dialog patterns
- Sound wave structure (foundation first, then parallel UI)
- Good desktop strategy (on-demand fetch + local cache + optional full download)
- TDD approach with Wave 0 stubs
- Performance-aware design (batch_has_suggestions, AlmaId type handling)

### Agreed Concerns (both reviewers — highest priority)
1. **[HIGH] Import performance risk** — 15.5M+ rows needs SQL-level optimization (PRAGMAs, in-SQL joins), not just Python dict loops. Both reviewers flagged this.
2. **[HIGH] Full DB download underdesigned** — 700MB download needs resume/retry, checksum, disk-space check, corruption detection. Both reviewers raised this.
3. **[HIGH] JOIN-03 not fully addressed** — Codex specifically noted that result-row indicators are not the same as "showing join partners with visual distinction." The plans need clearer JOIN-03 rendering behavior.
4. **[MEDIUM] Search state fragility** — Tab storage for cross-page VS transfer risks stale state, multi-tab confusion. Both suggest URL-based persistence.
5. **[MEDIUM] Cache invalidation missing** — No versioning or staleness detection in desktop cache. Both flagged this.
6. **[MEDIUM] API contract underspecified** — Codex emphasized Wave 2 can't safely parallelize without a frozen API/schema contract from Wave 1.

### Divergent Views
- **Overall risk:** Gemini rated MEDIUM, Codex rated MEDIUM-HIGH. Codex was more concerned about Wave 2 parallelization and JOIN-03 gaps; Gemini focused more on data volume performance.
- **Import approach:** Gemini suggested in-SQL joins for the mapping; Codex focused more on defining invariants and schema affordances for future extensibility.
- **Scope:** Codex suggested splitting Plan 03 into two deliverables (desktop core + download feature); Gemini kept assessment at plan level.
