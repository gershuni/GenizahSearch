# Phase 31: Image Navigation & Indicators - Context

**Gathered:** 2026-02-15
**Status:** Ready for planning

<domain>
## Phase Boundary

Users can navigate between individual pages/folios of a manuscript and see at a glance which digital image sources are available. Both web and desktop apps. Does NOT include metadata display (Phase 32), fragment relationships (Phase 33), or library IIIF fallback (Phase 34).

</domain>

<decisions>
## Implementation Decisions

### Page naming & ordering
- Use traditional folio notation: 1r, 1v, 2r, 2v, etc. (recto/verso) — familiar to Genizah researchers
- Navigation: Prev/Next arrow buttons plus a dropdown to jump to any specific folio
- Navigation bar positioned above the image viewer, near the shelfmark/title area
- Show total page count with current folio: "1r of 12 pages"
- Page ordering derived from NLI crossref ImageName field sequences

### Source indicators
- Clickable indicators that open the manuscript in the external viewer (NLI KTIV, Cambridge CUDL viewer, etc.) in a new tab
- Positioned near the image, grouped with the page navigation bar above the image
- When multiple image sources exist for the same manuscript, user can click a source indicator to switch which source's images are displayed in the viewer
- Must be consistent with existing image source switching in the desktop app (Oxford/Cambridge already have a switching mechanism — researcher should examine the existing pattern and build on or unify it)

### Claude's Discretion
- Visual style of source indicators (badges, chips, icons — whatever fits existing UI patterns)
- Exact layout/spacing of the navigation bar components
- How to handle manuscripts with only a single page (hide navigation or show disabled)
- Folio label extraction logic from ImageName values

</decisions>

<specifics>
## Specific Ideas

- Desktop app already has image source switching for Oxford and Cambridge — examine existing mechanism and ensure new source indicators are consistent with it (or improve it if needed, but stay consistent across all sources)
- Researchers are accustomed to folio notation (1r/1v) — this is non-negotiable for the target audience

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 31-image-navigation-indicators*
*Context gathered: 2026-02-15*
