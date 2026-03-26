# Domain Pitfalls: Search Refinement & Scholarly Joins

**Domain:** Search refinement, manuscript exclusion, scholarly joins, dimensions filtering
**Researched:** 2026-03-26

## Critical Pitfalls

### Pitfall 1: restrict_sys_ids > 500 Tantivy Bypass
**What goes wrong:** When restrict_sys_ids has > 500 entries, the Tantivy query string optimization is bypassed. Instead, ALL Tantivy results are fetched and post-filtered. For "joins search mode" with 20,088 join sys_ids, this means Tantivy returns the full result set before filtering.
**Why it happens:** The current code at genizah_core.py:6411 only injects sys_ids into the Tantivy query for sets <= 500.
**Consequences:** No performance issue for typical searches (Tantivy is fast, post-filter is O(1) per hit). But composition search with 20K restrict set + large text = full scan of all chunks.
**Prevention:** For joins mode specifically, this is fine -- 20K is well within post-filter performance. Do NOT try to inject 20K sys_ids into Tantivy query strings (would be enormous). The existing >500 path is correct.
**Detection:** Monitor search times when joins filter is active. Should be comparable to unfiltered search.

### Pitfall 2: Dimension Data Outliers (0.7mm to 7230mm)
**What goes wrong:** catalog_sizes has extreme values: SizeX ranges 0.7 to 7230.0, SizeY ranges 0.7 to 8617.0. UI range sliders with these bounds would be unusable.
**Why it happens:** Data quality issues in FIST source data. Some entries may use different units or have transcription errors.
**Consequences:** Range slider with min=0.7, max=7230 is meaningless. Users see nonsensical dimensions.
**Prevention:**
1. Determine units (likely mm for most, but verify with known manuscripts).
2. Clamp display range to sensible bounds (e.g., 10-1000mm = 1-100cm).
3. Filter out obvious outliers (> 1000mm or < 5mm) from range slider bounds.
4. Show raw values in detail views but use cleaned ranges for filters.
**Detection:** Check a known Cambridge fragment (e.g., T-S 12.123) against its physical description.

### Pitfall 3: Shelfmark Resolution Failures in File Import
**What goes wrong:** User imports a file of shelfmarks to exclude. Some shelfmarks don't resolve to sys_ids because of format differences.
**Why it happens:** Shelfmark normalization handles many variants (T-S, TS, T.S., etc.) but external files may use unexpected formats, have typos, or reference manuscripts not in the corpus.
**Consequences:** Silent exclusion failure -- user thinks they excluded 100 manuscripts but only 80 resolved.
**Prevention:** Show resolution report: "Resolved 80/100 shelfmarks. 20 not found: [list]". Use existing `normalize_shelfmark()` pipeline.
**Detection:** Always report resolution stats to user.

## Moderate Pitfalls

### Pitfall 1: Search-Within State Confusion
**What goes wrong:** User does search A -> search within -> search B -> wants to go back to "all results" but the restrict set is still active.
**Prevention:** Clear visual indicator (chip/breadcrumb) showing "Searching within N results from: [query A]". One-click clear button. Never silently accumulate restrict sets across searches.

### Pitfall 2: Supabase List Fetch Latency for Exclude
**What goes wrong:** Fetching list items from Supabase adds 200-500ms before search can start.
**Prevention:** Cache list items locally after first fetch. Invalidate on list modification. For web, cache in SearchUIState per session.

### Pitfall 3: Join Group Size Explosion in Display
**What goes wrong:** Some join groups have 100+ fragments. Displaying all partners inline with search results creates overwhelming UI.
**Prevention:** Cap inline display to 5-10 partners with "and N more..." expandable. Show full group in a dedicated dialog/panel.

### Pitfall 4: Dual Exclusion Sources (List + File) Merge
**What goes wrong:** User excludes by list AND imports a file. The two sets need to be unioned but UI doesn't make clear which exclusions are active.
**Prevention:** Show combined exclusion count with breakdown: "Excluding 150 manuscripts (80 from list 'Known Bible', 70 from imported file)". Allow clearing each source independently.

## Minor Pitfalls

### Pitfall 1: Dimension Units Display
**What goes wrong:** Values stored as mm but displayed without units, or displayed as mm when users expect cm.
**Prevention:** Always display with unit. Use cm (divide by 10) for display, store mm internally. Label clearly.

### Pitfall 2: Empty Joins Mode Results
**What goes wrong:** User enables "has joins" filter + applies other filters. Intersection may be empty, confusing the user.
**Prevention:** Show manuscript count for each filter combination before search: "1,234 manuscripts with joins in domain 'Bible'".

### Pitfall 3: Desktop Session State Growth
**What goes wrong:** Persisting exclude_sys_ids with thousands of entries grows the session JSON file.
**Prevention:** Store as set, serialize as sorted list. Typical sizes (100-1000) are negligible. Cap at 50K entries as safety guard.

## Phase-Specific Warnings

| Phase Topic | Likely Pitfall | Mitigation |
|-------------|---------------|------------|
| Dimensions filter | Outlier values break UI sliders | Compute P5/P95 percentiles for slider bounds |
| Search within results | State confusion with multiple levels | Single-level only: "search within" replaces any previous restrict, not stacks |
| Exclude by list | Supabase auth required | Handle anonymous users: show login prompt or disable feature |
| FIST joins search | Large group display | Cap inline partners at 10, expandable |
| File import | Encoding issues (UTF-8 BOM, Windows line endings) | Use utf-8-sig encoding (existing pattern in codebase) |

## Sources

- genizah_core.py: restrict_sys_ids <= 500 optimization at line 6411
- fjms_enrichment.db: catalog_sizes range analysis (0.7-7230 SizeX, 0.7-8617 SizeY)
- joins table: max group size 167 fragments (Group 1065)
- Desktop session persistence: genizah_app.py lines 29057-29058
- Existing BOM handling pattern: utf-8-sig noted in CLAUDE.md
