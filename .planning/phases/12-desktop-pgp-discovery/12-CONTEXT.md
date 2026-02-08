# Phase 12: Desktop PGP Discovery - Context

**Gathered:** 2026-02-08
**Status:** Ready for planning

<domain>
## Phase Boundary

Desktop users can discover PGP content through metadata panels, search indicators, tag search, and fragment join relationships. This phase adds PGP discovery features to the desktop app and extends search indicators/filters to both apps.

</domain>

<decisions>
## Implementation Decisions

### Metadata panel (Extended Info)
- Follow the **existing "Show Extended Info" pattern** from ResultDialog — inline expand (not popup)
- Add a "Show Extended Info" button to the **Browse tab top bar**, matching the ResultDialog behavior
- PGP metadata displayed as a **separate "Princeton Geniza Project" section** within the expanded info (not merged into existing fields)
- Shows: document type, tags, dates, description
- PGP tags are **clickable** — clicking a tag launches a tag search in the Search tab
- PGP extended info appears in **both Browse tab and ResultDialog** (not just Browse)
- Aggregates info from all sources (NLI, PGP, Cambridge, Oxford) as ResultDialog already does

### Search result indicators (PGP badge)
- **Text badge** style — small "PGP" label next to manuscripts with PGP transcriptions
- Badge appears in **both web and desktop** apps
- Badge appears in **search results and browse lists** (everywhere manuscripts are listed)
- Badge is **informational only** — no click action
- Badge **remains visible** even when PGP filter is active (consistent appearance)
- Web app uses card/grid layout — badge placement is **Claude's discretion**
- Desktop badge placement in table column is **Claude's discretion**
- Data source for badge determination (pre-cached vs search-time) is **Claude's discretion**

### PGP filter toggle
- Filter toggle to show **only PGP-available manuscripts** in search results
- Available in **both web and desktop** apps
- Desktop: lives alongside **existing desktop filter system**
- Web: filter UX approach is **Claude's discretion** (based on existing web search filter patterns)

### Tag search interaction
- Clicking a PGP tag in metadata → **switches to Search tab** and runs tag search automatically
- **Dedicated tag search input** available (not just click-to-search)
- Tag input uses **dropdown of known tags** (shows all available PGP tags, user picks one)
- Tag search dropdown lives **in the Search tab** as a new search mode option alongside text search

### Claude's Discretion
- PGP badge placement on web cards (corner, below shelfmark, etc.)
- PGP badge column placement in desktop search result tables
- Data source approach for PGP badge (pre-cached set vs per-batch query)
- Web PGP filter UX (checkbox in filters, toggle chip, etc.)
- PGP joins display in Related Fragments dialog (not discussed — full discretion)
- Exact visual styling of PGP section in extended info

</decisions>

<specifics>
## Specific Ideas

- "Show Extended Info" button and inline expansion should exactly match the existing ResultDialog pattern — familiar UX, no new interaction paradigms
- Tag search switches tab context — user clicks a tag in Browse/ResultDialog metadata, the app navigates to Search tab with results populated
- PGP info section should feel like a natural addition to existing extended info, not a bolted-on feature

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 12-desktop-pgp-discovery*
*Context gathered: 2026-02-08*
