# Phase 27: Domain Classifications - Context

**Gathered:** 2026-02-12
**Status:** Ready for planning

<domain>
## Phase Boundary

Display FJMS domain classifications on browse pages and add domain-based search filtering in both web and desktop apps. Domains are the primary navigation/search tool — visual presentation is minimal and functional, not decorative. 187 unique domains in a 2-level hierarchy (25 roots, 45 parents), covering 93% of manuscripts (203K of ~217K).

</domain>

<decisions>
## Implementation Decisions

### Browse Page Display
- Domains shown as **clickable minimal text** in the metadata area (not badges/chips)
- Clicking a domain navigates to the search page with that domain pre-filtered
- Language follows app interface: Hebrew UI → Hebrew domain names, English UI → English names
- Show only specific (child) domains — parent is implicit and available via search filter tree
- Same behavior in both web and desktop apps (desktop: clicking switches to search tab with filter applied)

### Hierarchy Display
- Parent and child shown as **separate clickable links** when both are relevant
- Deduplicate: if child domain already appears, don't redundantly show its parent alongside it (the child carries parent info)
- The full hierarchy is navigable through the search filter tree

### Search Filter UX
- Domain filter grouped **by parent category** — hierarchical tree with parent headers and child domains nested underneath
- **Type-ahead search** to find domains quickly (187 domains needs quick filtering)
- **Manuscript counts** shown next to each domain (e.g., "Piyyut (51,228)")
- **Multi-select with OR** logic — user can pick multiple domains, results match ANY selected
- Selecting a **parent domain includes all children** automatically
- **Works standalone** — user can browse all manuscripts in a domain without typing a text query
- Standalone domain browsing in **both apps**
- Web: filter placement at Claude's discretion (integrated with existing filters or separate panel)
- Desktop: **filter button** that opens a tree widget popup — keeps search tab compact

### Search Results Display
- Search results show **one domain** (most specific) with **"+N more"** indicator when multiple domains exist
- Hovering "+N more" shows all domains in a **tooltip** — doesn't disrupt layout

### Claude's Discretion
- Exact placement of domain filter on web search page (with existing filters vs separate panel)
- Navigation flow when clicking domain link (search page with pre-filter — details of implementation)
- Cap/expand behavior for many domains on browse page
- Which domain to show as "primary" on search results (most specific child, or alphabetical)
- Desktop tree widget popup design and behavior

</decisions>

<specifics>
## Specific Ideas

- User emphasized domains are primarily for **navigation and search**, not visual decoration — "the main thing is to navigate/search using this info"
- Clickable text links preferred over styled badges — minimal, functional
- Desktop filter concept: button that opens a tree widget popup (user's suggestion, Claude may refine)

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 27-domain-classifications*
*Context gathered: 2026-02-12*
