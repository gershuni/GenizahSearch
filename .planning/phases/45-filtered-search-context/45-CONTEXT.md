# Phase 45: Filtered Search Context - Context

**Gathered:** 2026-03-02
**Status:** Ready for planning

<domain>
## Phase Boundary

Researchers can constrain text searches by scholarly categories (domain, author, work, date, material) — either before searching (Path A: search page pre-filter) or after browsing (Path B: catalog browse "search within"). Both apps. All search modes (regular, Responsa, parallels). Pre-search filtering should make searches faster by restricting the search scope. Per-manuscript exclusion extended to word search mode. Web parallels page gets exclude-per-manuscript support.

</domain>

<decisions>
## Implementation Decisions

### "Search within" flow (Path B: Catalog Browse → Search)
- Two buttons in catalog browse toolbar area: **"Search in these results"** and **"Parallel search in these results"**
- "Search in these results" navigates to search page/tab with all active browse filters pre-populated (domain, author, work, date, text)
- "Parallel search in these results" navigates to parallels page/tab with filters pre-populated AND the selected manuscript's text auto-loaded into the parallels input
- All active browse filters carry over: domain, author, work, date range, text filter
- One-way sync only: browse → search carries filters, but editing filters on search page does NOT sync back to browse
- Filters are editable on the search page after arrival — user can tweak without going back to browse

### Search page pre-filter (Path A: Filter on search page)
- Collapsible "Advanced Filters" panel below the search bar, collapsed by default
- Full filter controls inside panel — same as catalog browse: domain tree, author search, work search, date range, material
- Pre-search filters and post-search domain exclusion coexist as two separate layers — pre-search narrows scope, post-search hides specific results
- Removing a filter chip requires manual re-search (user clicks Search again) — avoids accidental expensive searches

### Filter display & persistence
- Active filters shown as **tag/chip bar** with removable chips: [Halakha ×] [Rambam ×] [1100-1200 ×]
- Chip bar shows **total manuscript count** matching the current filters (e.g., "3,241 manuscripts")
- Filters persist with session state — restored on app reopen, same as exclusions and search history (Phase 43 infrastructure)

### Desktop filter panel
- Toolbar button labeled "Filters" opens a filter dialog with all controls (domain, author, work, date, material)
- Active filters shown as chip bar below search bar / above results after dialog closes
- Consistent chip bar placement across search tab and parallels tab

### Parallels page filters
- Identical filter panel on both search and parallels pages (web and desktop)
- Web parallels page gets per-manuscript exclude buttons (currently missing — desktop has it)
- Auto-exclude source manuscript when launching parallels from another module (mimic desktop behavior)
- Auto-excluded source manuscript appears in excluded section with reason "Source manuscript" — user can see and optionally restore it

### Word search exclusion (ד)
- Per-result exclude button added to regular word search results — same UX as composition mode
- Separate exclusion lists per search mode (word search exclusions independent from composition exclusions)
- "Import exclusions" button to copy exclusion list from one mode to the other (e.g., "Import from word search" on parallels page)

### Material filter
- Two-tier filter: **Printed** as prominent checkbox (most common use), other material types (paper, parchment, etc.) as multi-select with include/exclude capability
- Existing quick-toggle printed filter (hide/show/only printed) remains as a separate shortcut alongside the full Material filter in the advanced panel

### Filter + search history
- Pre-search filters saved as part of search history entries (query + mode + filters)
- History dropdown shows a **filter icon** on entries that had active filters; full filter details shown on hover/tooltip
- Recalling a filtered search restores query + filters, matching current recall behavior (auto-run or manual — consistent)

### Claude's Discretion
- Auto-expand behavior for the filter panel when filters are active (e.g., after arriving from browse)
- Desktop filter dialog internal layout (tabbed, scrollable, etc.)
- Excluded results display for word search (collapsible section vs badge — match existing patterns)
- Technical optimization approach for making filtered search faster (Tantivy-level, chunk-level, or hybrid)

</decisions>

<specifics>
## Specific Ideas

- Speed is a core value: pre-search filtering should make slow searches (composition, Responsa) viable on focused subsets. If pre-filtering doesn't speed up the search, post-search filtering already handles the filtering use case.
- The "Material" field (not "CreationType") comes from FragmentMaterial in catalog_fields. Printed is the most important material distinction.
- Desktop already has per-manuscript exclusion in composition/parallels — web needs parity.
- Desktop already auto-excludes source manuscript when launching parallels — web needs to mimic this.
- User explicitly wants two layers: pre-search filters narrow scope, post-search exclusions refine results within that scope.

</specifics>

<code_context>
## Existing Code Insights

### Reusable Assets
- `fjms_service.get_browse_results(domain, author, work, date_from, date_to, ...)` — core filter-to-results pipeline, returns sys_ids + metadata, paginated
- `fjms_service.get_domain_hierarchy()` — cached 2-3 level domain tree, used in both catalog browse and domain filter dialog
- `fjms_service.get_browse_authors(domain)` / `get_browse_works(domain, author)` — cross-filtered lookups, cached
- `fjms_service.get_printed_sys_ids(sys_ids)` — batch Material=Printed lookup from catalog_fields
- `shared/session_persistence.py` — session save/restore infrastructure from Phase 43
- Web: `web/pages/catalog_browse.py` — full filter UI with domain tree, author/work search, date range, text filter, pagination
- Desktop: `genizah_app.py:create_catalog_browse_tab()` (line ~11756) — "Browse by Identification" with same filter set
- Desktop: `DomainFilterDialog` (line ~4892) — post-search domain checkbox tree

### Established Patterns
- Post-search domain exclusion: `search_state.domain_exclusions` (web), `self.excluded_sys_ids` (desktop)
- Post-search printed filter: 3-state toggle ('all' / 'hide_printed' / 'only_printed')
- Session persistence: `app.storage.user` (web), JSON file (desktop) — Phase 43 infrastructure
- Search history: dropdown with search params, stored in session — Phase 43

### Integration Points
- `execute_search()` in genizah_core.py — currently no `restrict_sys_ids` parameter; needs extension for pre-search filtering
- Tantivy index schema: fields are `unique_id`, `content`, `source`, `full_header`, `shelfmark`, `scope`, `boundaries` — no sys_id or domain field
- Composition search loop (genizah_core.py ~5974-6250): per-chunk Tantivy search + regex verification
- Web search page: `web/pages/search.py` (~3,204 lines) — needs filter panel, chip bar, exclusion buttons for word search
- Web parallels page: needs filter panel + per-manuscript exclusion (currently missing)
- Desktop search tab: `genizah_app.py:create_search_tab()` (line ~6094) — needs filter toolbar button + chip bar

</code_context>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope.

</deferred>

---

*Phase: 45-filtered-search-context*
*Context gathered: 2026-03-02*
