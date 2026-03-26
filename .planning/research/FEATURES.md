# Feature Landscape: Search Refinement & Scholarly Joins

**Domain:** Search refinement, manuscript exclusion, scholarly joins discovery, and physical dimensions filtering for a Cairo Genizah research platform
**Researched:** 2026-03-26

## Table Stakes

Features users expect given existing search refinement patterns in the app.

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| Search within results | Standard search UX; restrict_sys_ids already exists | Low | Button + sys_id collection + intersect |
| Dimensions display in browse | Users already see catalog sizes in FJMS detail dialog; promote to browse | Low | Batch lookup + display |
| Join partners clickable in browse | Partners already shown but not navigable | Low | Add click handler to navigate |
| Exclude list persisted in session | Desktop already persists excluded_sys_ids; web should match | Low | Extend SearchUIState |

## Differentiators

Features that set the product apart from other Genizah search tools.

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| Exclude by saved list | Researchers with curated "known" lists can skip familiar manuscripts | Medium | Supabase list fetch + shelfmark resolution for file import |
| Pre-search dimension filter | Physical size filtering unique to Genizah research (finding matching fragments) | Medium | Extend get_filter_sys_ids + UI |
| FIST joins search mode | Search only within manuscripts that have scholarly joins -- unique research capability | Medium | Cache join sys_ids + restrict_sys_ids |
| Join group enrichment in search results | See join partners inline with search results -- saves browse round-trips | Medium | Batch enrichment + expandable UI |
| Exclude by imported shelfmark file | Power users maintain external spreadsheets of reviewed manuscripts | Medium | File upload + shelfmark normalization |
| Post-search dimension display | See fragment sizes alongside search results for physical matching | Low | Batch sizes + badge display |

## Anti-Features

Features to explicitly NOT build.

| Anti-Feature | Why Avoid | What to Do Instead |
|--------------|-----------|-------------------|
| Automatic join partner injection into results | Expands result count unpredictably; groups can have 167 fragments | Show partners as enrichment on matched results |
| Separate dimension filter in post-search panel | Confusing to have pre-search AND post-search dimension filters | One dimension filter in pre-search panel; post-search shows values for sort/display only |
| Exclude by regex pattern on shelfmarks | Edge case, complex UX, error-prone | Exclude by list (explicit selection) |
| Join group FTS5 search | Over-engineering for 15K groups; LIKE queries suffice | Simple text search on ScholarName/Comment |
| Cross-session exclude persistence (web) | Web sessions are ephemeral; Supabase lists already persist | Use Supabase lists as the persistence layer |

## Feature Dependencies

```
Dimensions display ──> Dimensions pre-search filter (display proves data quality)
Search within results (standalone, no deps)
Exclude by list ──> requires Supabase list fetch (exists)
FIST joins search ──> requires get_all_join_sys_ids() (new method)
Join enrichment in results ──> requires get_join_groups_batch() (new method)
```

## MVP Recommendation

Prioritize:
1. **Search within results** -- highest user value, lowest complexity, restrict_sys_ids already works
2. **Dimensions display in browse/results** -- proves dimension data quality, enables filter
3. **Pre-search dimension filter** -- extends existing filter pipeline
4. **Exclude by saved list** -- medium complexity, high value for power users

Defer: FIST joins search mode to later phase -- most complex, requires new service methods and search mode UI.

## Sources

- User feedback letter (2026-02-27): requests for search refinement tools
- PROJECT.md active requirements: 7 items listed
- Existing codebase: restrict_sys_ids pipeline, excluded_sys_ids in desktop, filter panel
