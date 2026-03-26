# Project Research Summary

**Project:** v7.3 Search Refinement & Scholarly Joins
**Domain:** Manuscript search refinement, exclusion filtering, FIST joins discovery, physical dimensions filtering
**Researched:** 2026-03-26
**Confidence:** HIGH

## Executive Summary

This milestone is a pure extension of the existing GenizahSearch architecture — no new technology, no new database schemas, and no new search engine paths are required. All four features (search within results, exclude by list, FIST joins search mode, and dimensions filter) converge on a single existing mechanism: the `restrict_sys_ids` pipeline in `genizah_core.SearchEngine`. The recommended approach is to extend existing integration points incrementally — starting with the lowest-risk features (dimensions display and search-within) and building toward the most complex (joins search mode with post-search enrichment).

The features divide naturally into two groups: pre-search filtering (dimensions range, joins toggle, exclude list — all feeding restrict_sys_ids) and post-search enrichment (dimensions display in results, join partner display). This separation is already how the existing filter panel works, so the patterns are established and proven. The only genuinely new surface is the exclude-by-file import, which requires shelfmark resolution and a resolution-failure report UI.

The key risks are data quality (dimension outliers reaching 7230mm will break range sliders without clamping), UX state confusion (search-within state must be visible and clearable at all times), and join group display (groups up to 167 fragments must be capped in inline display). All three risks have clear mitigations from existing codebase patterns and are well understood before implementation begins. Overall risk is low.

## Key Findings

### Recommended Stack

No new packages or technologies are needed. Every feature builds directly on the existing stack. The only database change is a new compound index (`idx_catsz_size` on `catalog_sizes(SizeX, SizeY)`) to speed up pre-search dimension range queries — no schema changes, no new tables.

**Core technologies:**
- `genizah_core.SearchEngine` with `restrict_sys_ids`: search filtering — the central mechanism for all four features, already fully implemented
- `shared/fjms_service.FjmsService`: FIST data access — needs three new methods (`get_all_join_sys_ids`, `get_join_groups_batch`, `get_sizes_batch`); existing `get_filter_sys_ids()` extended with dimension params
- `supabase-py`: Supabase client — fetching user lists for exclude-by-list (already integrated at `supabase_client.py:443, 595`)
- SQLite `fjms_enrichment.db`: dimensions and joins data — one new index, no schema changes
- NiceGUI / PyQt6: web and desktop UI — extend existing filter panel, result cards, and browse components

### Expected Features

**Must have (table stakes):**
- Search within results — restrict_sys_ids already supports this; users expect it in word search after experiencing it in catalog browse
- Dimensions display in browse and search results — already visible in FJMS detail dialog; promoting to first-class is expected
- Clickable join partners in browse — partners are already shown but not navigable; navigation is the natural next step

**Should have (differentiators):**
- Pre-search dimension range filter — unique to physical fragment matching research; no equivalent in other Genizah tools
- Exclude by saved Supabase list — power users with curated "known manuscripts" lists need bulk exclusion without individual clicks
- FIST joins search mode — search only within manuscripts that have scholarly joins; unique capability for join discovery workflows
- Exclude by imported shelfmark file — power users maintain external spreadsheets; file import bridges external tools into the platform
- Post-search join partner enrichment — see join partners inline with search results, avoiding browse round-trips

**Defer to v2+:**
- FIST join group text search (scholar name/comment FTS) — useful but not blocking; 15K groups are manageable with LIKE queries
- Cross-session web exclude persistence beyond Supabase lists — ephemeral sessions are acceptable; Supabase lists already provide persistence
- Automatic join partner injection into result set — anti-feature; groups up to 167 fragments make result count expansion unpredictable

### Architecture Approach

All four features integrate with the existing `restrict_sys_ids` pipeline without requiring new search engine paths. The pattern is consistent: compute a set of sys_ids from a filter source, pass to `execute_search()` or `search_composition_logic()` as `restrict_sys_ids`. Post-search enrichment (dimensions, join partners) follows the existing batch-lookup pattern already used for PGP info and domain enrichment. The exclude feature is best implemented as post-search filtering (not pre-search complement) to avoid computing `all_sys_ids - exclude_set` against the 217K corpus.

**Major components:**
1. `FjmsService` (extend) — add `get_all_join_sys_ids()`, `get_join_groups_batch()`, `get_sizes_batch()`; extend `get_filter_sys_ids()` with dimension params
2. `web/pages/search.py` SearchUIState (extend) — add `search_within_sys_ids`, `exclude_sys_ids`, `joins_mode` fields
3. `web/components/filter_panel.py` (extend) — add dimension range controls and joins-mode toggle
4. `genizah_app.py` (extend) — search-within button, joins toggle, exclude-by-list dialog, dimension inputs, browse join navigation
5. `fjms_enrichment.db` (index only) — `CREATE INDEX idx_catsz_size ON catalog_sizes(SizeX, SizeY)`

**Unified data flow (same pattern for all four features):**
```
filter source → sys_id set → intersect with existing restrict → execute_search()
                                                                      ↓
                                                          post-filter (exclusions)
                                                                      ↓
                                                          batch enrich (dimensions, joins)
                                                                      ↓
                                                                  display
```

### Critical Pitfalls

1. **restrict_sys_ids > 500 bypasses Tantivy query injection** — for joins mode with 20,088 sys_ids, the post-filter path (not Tantivy injection) is used automatically. This is correct and performant. Do NOT attempt to inject 20K IDs into Tantivy query strings. Monitor search times with joins filter active; they should be comparable to unfiltered search.

2. **Dimension data outliers (0.7mm to 7230mm)** — raw bounds from `catalog_sizes` make range sliders completely unusable. Compute P5/P95 percentiles for slider bounds before building the UI. Filter outliers (>1000mm or <5mm) from slider range. Display raw values in detail views; use clamped ranges only for the filter control. Verify units against a known manuscript.

3. **Shelfmark resolution failures in file import** — external files use unexpected formats, have typos, or reference unknown manuscripts. Always show a resolution report: "Resolved 80/100 shelfmarks. 20 not found: [list]". Never silently drop unresolved entries. Use existing `normalize_shelfmark()` pipeline and `utf-8-sig` encoding (BOM-aware, already pattern in codebase).

4. **N+1 queries for join and size enrichment** — calling per-manuscript service methods on 50 search results means 50 SQLite queries. Use batch methods (`get_join_groups_batch`, `get_sizes_batch`) with SQL `IN` clauses. This matches the existing PGP enrichment pattern.

5. **Join group display explosion** — Group 1065 has 167 fragments. Inline display of all partners in a result card is unusable. Cap inline partners at 5-10 with "and N more..." expandable link to a full-group dialog or browse navigation.

## Implications for Roadmap

Based on combined research, the four-phase build order is driven by dependency chains and incremental value delivery. Each phase delivers independently useful functionality.

### Phase 1: Dimensions Display and Pre-search Filter
**Rationale:** Lowest risk — extends the proven `get_filter_sys_ids()` pipeline with no new search paths. Proving dimension data quality in browse before building the filter prevents shipping a broken slider UI.
**Delivers:** Dimensions visible in search result cards and browse; pre-search width/height range filter in filter panel (web + desktop); `idx_catsz_size` compound index; P5/P95 bounds for slider.
**Addresses:** Dimensions display (table stakes), pre-search dimension filter (differentiator), post-search dimension display (differentiator).
**Avoids:** Pitfall 2 (outliers) — percentile bounds computed and verified before UI is built.

### Phase 2: Search Within Results
**Rationale:** High user value, trivial core changes (restrict_sys_ids is already fully implemented). Placing after Phase 1 means dimensions are visible when the user scopes their search, improving the combined experience.
**Delivers:** "Search within N results" button in web and desktop search result headers; breadcrumb chip showing active scope with one-click clear; correct intersection with pre-existing filter restrict sets.
**Addresses:** Search within results (table stakes).
**Avoids:** Moderate Pitfall 1 (state confusion) — single-level only (replace, not stack), chip always visible, clear button prominent.

### Phase 3: Exclude by List and File Import
**Rationale:** Requires Supabase list integration and shelfmark resolution. Logically follows search-within because the breadcrumb/chip UX patterns established in Phase 2 apply directly to exclusion display (showing active exclude count with source breakdown and per-source clear).
**Delivers:** List picker dialog (web + desktop) fetching user's Supabase lists; file import with shelfmark resolution and resolution report; combined exclusion count badge with breakdown by source (list vs. file); session persistence for exclude set; graceful handling for anonymous users.
**Addresses:** Exclude by saved list (differentiator), exclude by imported file (differentiator).
**Avoids:** Pitfall 3 (resolution failures) — always report stats. Anti-Pattern 1 (complement set) — post-search filter, not pre-search. Moderate Pitfall 2 (Supabase latency) — cache list items in SearchUIState. Moderate Pitfall 4 (dual-source merge confusion) — breakdown UI.

### Phase 4: FIST Joins Browse Enrichment and Search Mode
**Rationale:** Most complex phase — introduces three new service methods and a new filter panel concept (search-mode toggle vs. metadata filter). Benefits from Phases 1-3 being stable: dimensions can appear for join partners; exclude can filter join group members.
**Delivers:** Clickable join partners in browse with navigation to browse target; "Has joins" filter toggle in filter panel (restricts search to 20,088 join sys_ids); post-search join partner enrichment in result cards (capped inline + expandable full group).
**Addresses:** Clickable join partners (table stakes), FIST joins search mode (differentiator), join partner enrichment in results (differentiator).
**Avoids:** Critical Pitfall 1 (>500 restrict — use post-filter path, correct by default). Moderate Pitfall 3 (display explosion — cap at 10 inline). Minor Pitfall 2 (empty intersection — show count before search).

### Phase Ordering Rationale

- Dimensions first because it has no inter-feature dependencies, extends the most well-understood pipeline, and the data quality verification in Phase 1 is a prerequisite for trustworthy display in Phase 4 (join partner cards can show dimensions).
- Search-within second because restrict_sys_ids requires zero core changes and delivers high user value quickly.
- Exclude third because it requires Supabase integration that both apps already have wired, plus shelfmark resolution that is already implemented — the work is primarily in the UI layer and state management.
- Joins last because it has the most new service methods (three), the most complex display decisions (group capping, navigation patterns), and depends on the stable patterns from Phases 1-3 for combined-filter scenarios.
- Both apps (web + desktop) are extended within each phase to maintain feature parity throughout the milestone.

### Research Flags

Phases with well-documented patterns — skip research-phase during planning:
- **Phase 1 (Dimensions):** Extends `get_filter_sys_ids()` exactly as documented. Index creation is trivial SQL. Only investigation needed is the units verification and percentile computation (one SQL query).
- **Phase 2 (Search Within):** restrict_sys_ids is fully built and documented. UI is a button + state field with existing breadcrumb pattern.
- **Phase 3 (Exclude):** Supabase list fetch is already implemented and tested. Shelfmark normalization pipeline is established. Work is wiring and UI.

Phases that may benefit from a plan-phase design review before implementation:
- **Phase 4 (Joins):** The three new FjmsService batch methods are new code needing query design. The "joins search mode" toggle is a new filter panel concept (restricts corpus, not just metadata) with no existing model. Worth a brief design discussion before starting implementation.

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH | All technologies already in production use; no new dependencies identified |
| Features | HIGH | Grounded in user feedback letter (2026-02-27) and PROJECT.md active requirements |
| Architecture | HIGH | All integration points verified against source code with specific line numbers |
| Pitfalls | HIGH | Identified from actual data analysis (outlier ranges from DB), actual code (line 6411 threshold), actual data (Group 1065 with 167 fragments) |

**Overall confidence:** HIGH

### Gaps to Address

- **Dimension units verification:** Values are presumed mm based on data ranges (0.7–7230) but need confirmation against a known manuscript (e.g., T-S 12.123 physical description). If units are wrong, all display labels and P5/P95 bounds will be incorrect. Resolve at the start of Phase 1 with a one-off SQL spot-check.
- **Supabase anonymous user handling for exclude:** Research did not determine whether anonymous users should see a login prompt or have the exclude-by-list feature hidden. Needs a product decision before Phase 3 planning.
- **Joins search mode UI pattern:** No existing model in the filter panel for a corpus-scope toggle (vs. metadata filters). The exact UI pattern (checkbox, dedicated section, radio group) needs a design decision at the start of Phase 4 planning.

## Sources

### Primary (HIGH confidence)
- `genizah_core.py` lines 6411, 6605, 7006 — restrict_sys_ids pipeline: Tantivy injection threshold (<=500), execute_search(), search_composition_logic()
- `shared/fjms_service.py` lines 848, 1991, 2421-2440 — get_filter_sys_ids(), get_join_group(), catalog_sizes batch fetch
- `web/pages/search.py` lines 76-98 — SearchUIState dataclass fields including existing `word_search_excluded_ids`
- `web/supabase_client.py` lines 443, 595 — get_user_lists(), get_list_items() with sys_id field
- `genizah_app.py` lines 11651-11652, 18512, 29057-29058 — excluded_sys_ids, _catalog_search_in_results(), session state persistence
- `fjms_enrichment.db` runtime analysis — catalog_sizes (178,579 rows, 104,650 AlmaIds, SizeX range 0.7–7230mm); joins (48,655 rows, 20,088 AlmaIds, 14,906 groups, max group size 167 fragments)

### Secondary (MEDIUM confidence)
- User feedback letter (2026-02-27) — feature priorities, power-user workflows, exclusion use cases
- PROJECT.md active requirements — 7 items for v7.3 milestone

### Tertiary (LOW confidence)
- Dimension units (mm assumed from data ranges) — requires verification against known physical specimens before Phase 1 UI implementation

---
*Research completed: 2026-03-26*
*Ready for roadmap: yes*
