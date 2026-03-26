# Architecture Patterns: Search Refinement & Scholarly Joins

**Domain:** Search refinement, exclusion filtering, FIST joins search, dimensions filtering
**Researched:** 2026-03-26
**Confidence:** HIGH (based on existing codebase analysis -- all integration points verified in source)

## Recommended Architecture

All four features integrate with the **existing `restrict_sys_ids` pipeline** in genizah_core.py. No new search engine paths are needed. The pattern is: compute a set of sys_ids from filters/state, pass to `execute_search()` or `search_composition_logic()` as `restrict_sys_ids`.

### Component Boundaries

| Component | Responsibility | Communicates With |
|-----------|---------------|-------------------|
| `genizah_core.SearchEngine` | Search with restrict_sys_ids (EXISTING) | Tantivy index |
| `shared/fjms_service.FjmsService` | get_filter_sys_ids() (EXISTING), new: dimensions filter, joins search | fjms_enrichment.db |
| `web/components/filter_panel.py` | Pre-search filter UI logic (EXISTING, EXTEND) | FjmsService |
| `web/pages/search.py` SearchUIState | Per-session state (EXISTING, EXTEND) | filter_panel, core |
| `genizah_app.py` | Desktop search/browse tabs (EXISTING, EXTEND) | FjmsService, core |

### Data Flow: Four Features

```
Feature 1: Search Within Results
================================
Current results (sys_ids) ──> restrict_sys_ids ──> execute_search()
                                                      │
                                                      v
                                              Filtered results

Feature 2: Exclude by List
===========================
Supabase list ──> get_list_items() ──> sys_ids ──> subtract from restrict_sys_ids
  OR                                                    │
Imported file ──> parse shelfmarks ──> resolve sys_ids  │
                                                        v
                                              execute_search() / post-filter

Feature 3: FIST Joins Search Mode
===================================
Search query ──> execute_search(restrict_sys_ids=joins_sys_ids)
                                      │
                                      v
Results ──> enrich with join group info ──> display join partners

Feature 4: Dimensions Filter
==============================
                Pre-search path:
min/max W/H ──> FjmsService.get_filter_sys_ids() ──> restrict_sys_ids
                                                          │
                Post-search path:                         v
Results ──> batch lookup catalog_sizes ──> filter client-side
```

## Integration Point Analysis

### Feature 1: Search Within Results

**Mechanism:** Already 90% built. The `restrict_sys_ids` parameter on `execute_search()` and `search_composition_logic()` is the exact mechanism needed. The desktop catalog browse already has a "Search in these results" button (line 17615, `_catalog_search_in_results`).

**What exists:**
- `execute_search(..., restrict_sys_ids=set)` -- genizah_core.py:6605
- `search_composition_logic(..., restrict_sys_ids=set)` -- genizah_core.py:7006
- Desktop: `_catalog_search_in_results()` navigates browse->search with filter sys_ids (line 18512)
- Web: `SearchUIState.restrict_sys_ids` already stored per-session (search.py:94)

**What to build:**
- Web: "Search within results" button in search results header. On click: collect sys_ids from current `search_state.results`, store as `search_state.search_within_sys_ids`, combine with any pre-search `restrict_sys_ids` via intersection.
- Desktop: Same button in word search results panel. Collect sys_ids from `self.last_results`, pass as restrict_sys_ids on next search.
- Both: Show breadcrumb/chip indicating "searching within N results" with clear button.
- Core: No changes needed -- restrict_sys_ids already works.

**Key decision:** Intersection, not replacement. If user has pre-search domain filter (1000 sys_ids) AND searches within results (50 sys_ids), the effective restrict set is the intersection (<=50). This is naturally correct since the 50 results are already within the 1000.

### Feature 2: Exclude Known Manuscripts

**Mechanism:** Two sub-features: (a) exclude by Supabase list, (b) exclude by imported shelfmark file.

**What exists:**
- Desktop: `excluded_sys_ids` and `excluded_shelfmarks` sets already maintained (genizah_app.py:11651-11652), used in composition search results filtering (line 25944-25965). Session persistence for both (line 29057-29058).
- Web: `word_search_excluded_ids` set in SearchUIState (search.py:98) for per-manuscript exclusion.
- Supabase: `get_user_lists(user_id)` returns lists, `get_list_items(list_id)` returns items with `sys_id` field (supabase_client.py:443, 595).
- Both apps: Lists page with cloud sync already exists.

**What to build:**
- Pre-search exclusion: Add `exclude_sys_ids: set` parameter to the restrict_sys_ids pipeline. Computed as: `final_restrict = (restrict_sys_ids - exclude_sys_ids)` if restrict is set, else all_sys_ids minus exclude_sys_ids (expensive -- better to post-filter).
- **Recommended approach:** Post-search exclusion for list-based exclude (simpler, no restrict_sys_ids changes). Filter results after search by removing any result whose sys_id is in the exclude set. This matches the existing desktop pattern.
- List picker: Dropdown showing user's Supabase lists. On select, fetch list items, extract sys_ids, store in exclude set.
- File import: Parse a text file of shelfmarks (one per line), resolve to sys_ids via `genizah_core.csv_bank` lookup, store in exclude set.
- Shelfmark resolution: Use existing `SearchEngine.normalize_shelfmark()` and csv_bank index for lookup.

**Key decision:** Post-search filtering is simpler and matches desktop precedent. Pre-search exclusion via restrict_sys_ids would require computing `all_sys_ids - exclude_set` which is ~217K - N, wasteful when N is small. Exception: if exclude set is huge (>10K), pre-search via Tantivy NOT clauses may help, but this is an edge case.

### Feature 3: FIST Joins Search Mode

**Mechanism:** Two sub-features: (a) joins suggestions in browse enrichment, (b) dedicated "search within join groups" mode.

**What exists:**
- `FjmsService.get_join_group(sys_id)` returns join partners for a single manuscript (fjms_service.py:1991).
- joins table: 48,655 rows, 20,088 distinct AlmaIds, 14,906 groups, avg 3.3 fragments/group.
- Indexes: `idx_joins_alma(AlmaId)`, `idx_joins_group(JoinGroupId)`.
- Browse page already shows join group info via `get_join_group()`.

**What to build:**

**(a) Browse enrichment (already partially exists):**
- Enhance existing join display with clickable partners that navigate to browse.
- Show join type badges, scholar attribution (already in data).

**(b) Joins search mode:**
- New search mode or pre-search filter: "Search only in manuscripts with FIST joins."
- Compute `joins_sys_ids` = set of all 20,088 AlmaIds in joins table. Cache this set (it's static).
- Pass as restrict_sys_ids to any search mode.
- **Enhanced mode:** After search, enrich results with join partner info. For each result that has joins, show a expandable section listing partner shelfmarks with links.
- New FjmsService method: `get_all_join_sys_ids() -> set[str]` (simple SELECT DISTINCT AlmaId FROM joins).
- New FjmsService method: `get_join_groups_batch(sys_ids: list[str]) -> dict[str, list[dict]]` for post-search enrichment (avoid N+1 queries).

**New FjmsService methods needed:**

```python
def get_all_join_sys_ids(self) -> set:
    """Return set of all AlmaIds that appear in joins table. Cached."""
    cursor = self._conn.execute("SELECT DISTINCT AlmaId FROM joins")
    return {row[0] for row in cursor}

def get_join_groups_batch(self, sys_ids: list) -> dict:
    """Batch lookup join groups for multiple sys_ids. Returns {sys_id: [partners]}."""
    # Use IN clause with batching for large sets
    ...

def search_join_groups(self, query: str) -> list[dict]:
    """Search join groups by scholar name, comment text, or shelfmark."""
    # FTS5 or LIKE on ScholarName/Comment fields
    ...
```

### Feature 4: Dimensions Range Filtering

**Mechanism:** Pre-search filter (restrict_sys_ids) and/or post-search filter.

**What exists:**
- `catalog_sizes` table: 178,579 rows, 104,650 distinct AlmaIds, with SizeX/SizeY/InnerSizeX/InnerSizeY.
- Index: `idx_catsz_alma(AlmaId)`.
- `FjmsService.get_catalog_detail()` already fetches sizes per manuscript (fjms_service.py:2421-2440).
- Sizes range: 0.7-7230mm X, 0.7-8617mm Y (likely in mm, with some outliers).

**What to build:**

**(a) Pre-search dimension filter:**
- Extend `FjmsService.get_filter_sys_ids()` with `size_x_min`, `size_x_max`, `size_y_min`, `size_y_max` parameters.
- SQL: `SELECT DISTINCT AlmaId FROM catalog_sizes WHERE SizeX BETWEEN ? AND ? AND SizeY BETWEEN ? AND ?`
- Intersect with other filter criteria in the existing get_filter_sys_ids pipeline.
- UI: Two range sliders or min/max input fields in the pre-search filter panel.

**(b) Post-search dimension display:**
- Batch lookup sizes for search result sys_ids.
- New FjmsService method: `get_sizes_batch(sys_ids: list) -> dict[str, dict]` returning `{alma_id: {size_x, size_y, inner_size_x, inner_size_y}}`.
- Display in result cards (e.g., "15.2 x 22.1 cm" badge).

**(c) Post-search dimension filter:**
- Client-side filter within displayed results, similar to existing domain/printed filters.
- Filter results by dimension range after enrichment.

**New index needed:**
```sql
CREATE INDEX idx_catsz_size ON catalog_sizes(SizeX, SizeY);
```
This speeds up pre-search range queries. Current `idx_catsz_alma` only helps per-manuscript lookups.

**Units:** Values appear to be in mm (e.g., SizeX=150 = 15cm). Display should convert to cm. Need to verify units -- check a known manuscript.

## Patterns to Follow

### Pattern 1: restrict_sys_ids Pipeline (EXISTING)
**What:** All search functions accept an optional `restrict_sys_ids: set` parameter. When set, only manuscripts in this set are returned.
**When:** Any pre-search filtering scenario.
**How it works in Tantivy:** For sets <= 500, sys_ids are injected as OR clauses in the Tantivy query string: `(original_query) AND (full_header:"id1" OR full_header:"id2" ...)`. For larger sets, post-Tantivy filtering via a uid lookup set.
**Implication:** Sets > 500 are filtered post-Tantivy (still fast). Very large restrict sets (>10K) have negligible overhead since filtering is O(1) per hit.

### Pattern 2: Post-Search Enrichment (EXISTING)
**What:** After search returns results, batch-fetch additional metadata (PGP info, domains, translations) for display.
**When:** Adding dimensions or join info to search results.
**Example:** Search returns 50 results. Batch-fetch sizes for those 50 sys_ids. Display inline.

### Pattern 3: Filter Panel Extension (EXISTING)
**What:** `web/components/filter_panel.py` provides shared filter logic. `get_filter_sys_ids()` in FjmsService computes the restrict set.
**When:** Adding dimensions as a new filter criterion.
**How to extend:** Add parameters to `get_filter_sys_ids()`, add UI controls in filter panel, add state fields to SearchUIState.

### Pattern 4: Session State Persistence (EXISTING)
**What:** Desktop persists search state (excluded_sys_ids, filters, results) to JSON. Web uses per-session SearchUIState.
**When:** Preserving exclude lists and "search within" state across interactions.
**Desktop:** `_save_session_state()` / `_restore_session_state()` in genizah_app.py.
**Web:** SearchUIState dataclass fields in search.py.

## Anti-Patterns to Avoid

### Anti-Pattern 1: Computing Full Complement for Exclusion
**What:** Computing `all_sys_ids - exclude_set` to pass as restrict_sys_ids.
**Why bad:** 217K - 100 = 216,900 sys_ids. The restrict_sys_ids mechanism is optimized for small sets (<= 500 get Tantivy-level filtering). Large sets work but add overhead.
**Instead:** Post-search filter: run search normally, then remove excluded sys_ids from results. This is O(N) where N = result count, not corpus size.

### Anti-Pattern 2: N+1 Queries for Join Enrichment
**What:** Calling `get_join_group(sys_id)` for each of 50 search results.
**Why bad:** 50 separate SQLite queries when one batched query suffices.
**Instead:** Build `get_join_groups_batch(sys_ids)` that uses a single SQL query with IN clause.

### Anti-Pattern 3: Unbounded Join Group Expansion
**What:** For "search within join groups" mode, expanding all join partners of all results into the restrict set.
**Why bad:** A single group can have 167 fragments (Group 1065). If a search returns 50 results each in large groups, the expanded set could be huge and the UX confusing.
**Instead:** Show join partners as enrichment data alongside results, not as additional search results. Let users click to explore specific partners.

### Anti-Pattern 4: Mixing Pre-search and Post-search Dimensions
**What:** Applying dimension filter both pre-search and post-search independently.
**Why bad:** Confusing UX -- user applies filter pre-search, then sees a different dimension filter in results panel.
**Instead:** One path: pre-search dimension filter in the filter panel (restricts search). Post-search: display dimensions, allow sort by size, but not a separate filter.

## New vs Modified Components

### New Components
| Component | Type | Purpose |
|-----------|------|---------|
| `FjmsService.get_all_join_sys_ids()` | Method | Return cached set of all join AlmaIds |
| `FjmsService.get_join_groups_batch()` | Method | Batch join group lookup for search results |
| `FjmsService.get_sizes_batch()` | Method | Batch size lookup for search results |
| Dimension filter UI (web) | UI controls | Min/max width/height inputs in filter panel |
| Dimension filter UI (desktop) | UI controls | Same in desktop filter section |
| Search-within button (web) | UI control | Button in search results header |
| Exclude-by-list picker (web) | UI dialog | Select list or import file for exclusion |
| Exclude-by-list picker (desktop) | UI dialog | Same for desktop |
| Joins search mode toggle (web) | UI control | Checkbox/toggle in search mode options |
| Joins search mode toggle (desktop) | UI control | Same for desktop |

### Modified Components
| Component | Change |
|-----------|--------|
| `FjmsService.get_filter_sys_ids()` | Add size_x_min/max, size_y_min/max params |
| `web/pages/search.py` SearchUIState | Add search_within_sys_ids, exclude_sys_ids, joins_mode fields |
| `web/pages/search.py` search handler | Combine restrict sets (filters + search-within + joins) |
| `web/pages/search.py` results renderer | Show dimensions, join partners in result cards |
| `web/components/filter_panel.py` | Add dimension range controls |
| `genizah_app.py` search tab | Add search-within button, joins toggle, exclude-by-list |
| `genizah_app.py` browse tab | Enhanced join partner display with navigation |
| `genizah_app.py` session state | Persist new filter state fields |
| `fjms_enrichment.db` | Add idx_catsz_size index for range queries |

## Suggested Build Order

The order is driven by dependency chains and incremental value:

```
Phase 1: Dimensions Display + Pre-search Filter
  - Add get_sizes_batch() to FjmsService
  - Add idx_catsz_size index
  - Extend get_filter_sys_ids() with dimension params
  - Add dimension inputs to filter panel (web + desktop)
  - Show dimensions in search results and browse
  Rationale: Lowest risk, extends existing filter pipeline, no new search paths.
  Dependencies: None.

Phase 2: Search Within Results
  - Add search-within button to web search results header
  - Add search-within button to desktop search results
  - Collect sys_ids from current results, store in state
  - Intersect with any existing restrict_sys_ids
  - Show "searching within N results" breadcrumb
  Rationale: High user value, trivial core changes (restrict_sys_ids exists).
  Dependencies: None (but placing after Phase 1 lets dimensions filter enrich the search-within experience).

Phase 3: Exclude by List
  - Add list picker dialog (web + desktop)
  - Fetch list items from Supabase, extract sys_ids
  - Add file import with shelfmark resolution
  - Post-search exclusion filter
  - Persist exclude set in session state
  Rationale: Requires Supabase integration + shelfmark resolution.
  Dependencies: None technically, but logically follows search-within.

Phase 4: FIST Joins Browse Enrichment + Search Mode
  - Add get_all_join_sys_ids() and get_join_groups_batch()
  - Enhance browse join display with clickable partners
  - Add "Has joins" toggle in filter panel / search mode
  - Post-search enrichment with join partner display
  Rationale: Most complex feature. Benefits from phases 1-3 being stable.
  Dependencies: Phase 1 (dimensions can be shown for join partners too).
```

## Scalability Considerations

| Concern | Current | At Scale |
|---------|---------|----------|
| restrict_sys_ids size | <= 500 uses Tantivy, > 500 post-filters | 20K join sys_ids: post-filter is fine (~0.1ms per hit) |
| Dimension range query | 178K rows, indexed | Fast with compound index; <10ms |
| Join batch lookup | 50 results typical | IN clause with 50 IDs: <5ms |
| Exclude set size | Typical list: 10-500 items | Post-filter O(1) per result; handles 10K+ |
| Session state size | JSON serialization | Adding ~500 sys_ids to state: negligible |

## Database Changes

### New Index (fjms_enrichment.db)
```sql
CREATE INDEX IF NOT EXISTS idx_catsz_size ON catalog_sizes(SizeX, SizeY);
```

### No Schema Changes
All features use existing tables. No new SQLite tables or Supabase tables needed.

## Sources

- genizah_core.py: execute_search() at line 6605, restrict_sys_ids pipeline
- shared/fjms_service.py: get_filter_sys_ids() at line 848, get_join_group() at line 1991, catalog_sizes at line 2421
- web/pages/search.py: SearchUIState at line 76, filter computation at line 2856
- web/components/filter_panel.py: build_domain_options(), build_author_options()
- web/supabase_client.py: get_user_lists() at line 443, get_list_items() at line 595
- genizah_app.py: excluded_sys_ids at line 11651, _catalog_search_in_results at line 18512
- fjms_enrichment.db: catalog_sizes (178K rows, 105K AlmaIds), joins (48K rows, 20K AlmaIds, 15K groups)
