# Phase 6: Metadata Display - Context

**Gathered:** 2026-02-06
**Status:** Ready for planning

<domain>
## Phase Boundary

Show PGP document metadata (type, dates, description, tags, languages) on the browse page for fragments that have linked PGP documents. Clickable tags redirect to search results filtered by tag. No changes to the browse experience for the ~207K fragments without PGP data.

</domain>

<decisions>
## Implementation Decisions

### Placement & layout
- Add PGP metadata as a new labeled section inside the existing expandable metadata panel
- Follows the same pattern as Oxford/Cambridge external info — all external source metadata stays in one panel
- Section has a clear "Princeton Geniza Project" (or "PGP") header to distinguish from GenizahSearch fields
- Behind the existing "Show/Hide Metadata" toggle — no always-visible PGP elements
- When a fragment has no linked PGP document, the PGP section simply doesn't appear — no "not found" message
- PGP is a small (but important) part of all Genizah content — metadata display should enhance, not reshape the browse experience

### Content priority
- Priority order: Document type > Tags > Description > Languages > Dates
- Document type (Letter, Legal document, List, etc.) displayed prominently
- Subject tags shown as interactive elements (see Tags section)
- Description (English summary from PGP) shown — Claude decides truncation strategy based on typical lengths
- Languages (primary and secondary, e.g., "Judaeo-Arabic") displayed
- Dates displayed (see Date display section)

### Tags interaction
- Tags display as clickable elements (visual style at Claude's discretion)
- Clicking a tag redirects to the search results page with a tag filter
- Search results show ALL GenizahSearch fragments linked to PGP documents with that tag (not one-per-document)
- This means the search page needs to support a tag filter parameter

### Date display
- Inferred/standardized date is the primary display (e.g., "1041 CE")
- Original date shown as secondary detail
- Dates converted to CE (PGP provides standardized dates in `doc_date_standard` and `inferred_date_standard`)
- Date rationale shown inline below the date, always visible (e.g., "Based on the mention of Yefet b. David")
- Date clickability at Claude's discretion

### Claude's Discretion
- Whether to show key PGP fields (like document type) above the toggle or keep all behind it
- Description truncation strategy (full text vs truncated with expand)
- Tag visual style (chips, plain links, etc.)
- Date clickability (filter by period, or display-only)
- Exact ordering and spacing of fields within the PGP section
- How to display languages (inline with type, or separate row)

</decisions>

<specifics>
## Specific Ideas

- "PGP is a tiny (though important) part of all Genizah — it should not change the whole Genizah website way"
- External metadata (Oxford, Cambridge, PGP) should follow consistent patterns — PGP section is analogous to existing Oxford/Cambridge sections
- PGP already has a Python date converter for standardized dates — use the pre-converted values from the database

</specifics>

<deferred>
## Deferred Ideas

- Full-text search within PGP transcriptions (already noted as v2 in STATE.md)
- Tag-based browsing page (dedicated tag exploration UI beyond search redirect)
- Date-range filtering as a search feature

</deferred>

---

*Phase: 06-metadata-display*
*Context gathered: 2026-02-06*
