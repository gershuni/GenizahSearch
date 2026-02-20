# Phase 40: Performance Optimization - Context

**Gathered:** 2026-02-20
**Status:** Ready for planning

<domain>
## Phase Boundary

Measurably improve responsiveness of both web and desktop apps across search execution, browse navigation, and startup — targeting the top bottlenecks identified by profiling analysis. Five specific optimizations: parallel NLI fetches, deferred desktop main-thread work, parallel web browse queries, FL ID index lookup, and unified variants cache.

</domain>

<decisions>
## Implementation Decisions

### Cache & index timing
- FL ID → sys_id index: build in background thread after startup. App launches fast; browse may hit a brief wait if used immediately before index is ready.
- Variants cache (get_variants): Claude's discretion on whether to use per-search or session-persistent LRU — pick based on usage patterns in the code.
- Web browse crossref data: cache per session. Navigating back to a previously viewed shelfmark should be instant.
- NLI metadata timeout: Claude's discretion — pick based on typical NLI response times.

### Faster perceived results
- Low-res images first: extend to ALL IIIF sources where feasible (Cambridge, NLI, Manchester, JTS). Use low-res thumbnails initially, load full-res on demand or when visible. Check which IIIF servers support size parameters and apply where possible.
- Search results: two-phase render. Show ranked results immediately with basic info (title, shelfmark, relevance), then fill in enrichment data (domain badges, catalog info) as it arrives. Relevance order is set by Tantivy before enrichment, so no reordering occurs.
- Image prefetching: prefetch images for visible results AND the next page of results (viewport + next page).
- Browse navigation prefetching: when viewing a fragment, quietly prefetch metadata for adjacent (next/previous) fragments in the background.

### Claude's Discretion
- Variants cache strategy (per-search vs session LRU)
- NLI timeout duration and fallback behavior
- Exact implementation of two-phase render transitions (skeleton placeholders, fade-in, etc.)
- Compression/sizing parameters for low-res IIIF thumbnails

</decisions>

<specifics>
## Specific Ideas

- Oxford images already demonstrate the low-res-first pattern — extend this approach to other sources
- Two-phase render: basic result cards appear instantly in relevance order, enrichment badges populate asynchronously without layout shift
- Background index build should not block app launch or initial interactions

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 40-performance-optimization*
*Context gathered: 2026-02-20*
