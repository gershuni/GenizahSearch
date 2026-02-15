# Phase 28: Catalog Enrichment - Context

**Gathered:** 2026-02-15
**Status:** Ready for planning

<domain>
## Phase Boundary

Display FJMS catalog metadata (titles, authors, dates, places, descriptions) alongside existing PGP metadata when browsing manuscripts in both web and desktop apps. The catalog data already exists in the SQLite sidecar (Phase 25). This phase wires it into the browse page UI.

**Critical complexity discovered during discussion:** FJMS catalog data is multi-source — a single shelfmark can have multiple catalog records from different scholarly sources (FGP Transcriptions, Fleischer Piyut Project, GRU-Cambridge, NLI Aleph Catalog, Shivtiel/Niessen, Uri Ehrlich, etc.). Additionally, FJMS contains NLI Aleph catalog data that may overlap with data GenizahSearch already fetches directly from NLI.

</domain>

<decisions>
## Implementation Decisions

### Layout & Placement
- **Separate sections:** FJMS catalog data lives in its own section, distinct from existing PGP metadata
- **Web:** FJMS data appears within the existing metadata area (where PGP data is now — just more data). Claude's discretion on whether to use a sub-header divider or inline labeled fields
- **Desktop:** FJMS data appears as a sub-section INSIDE the existing "Extended Information" collapsible area (closed by default)
- **Empty state:** If a manuscript has no FJMS catalog data, show nothing — no placeholder, no note
- **Section ordering:** Claude's discretion on whether FJMS appears above or below PGP metadata (pick based on existing page structure)

### Title Display
- **Language:** Show title matching the app's current interface language (Hebrew or English)
- **Dual titles (PGP + FJMS):** Claude's discretion on header placement — research should inform whether FJMS titles are typically richer or complementary to PGP titles
- **Title role (header vs section):** Research decides — depends on how FJMS titles compare to PGP titles in practice

### Multi-Record Handling (RESEARCH REQUIRED)
- A single shelfmark can have 5+ catalog records from different scholarly sources
- **Decision deferred until research:** Need to understand data distribution — how many shelfmarks have multiple records? How different are the records from each other?
- **User instinct for descriptions:** Show all descriptions stacked with source labels (but research may refine this)

### NLI Aleph Data Overlap (RESEARCH REQUIRED)
- FJMS contains NLI Aleph catalog records
- GenizahSearch already fetches some NLI data directly
- **Decision deferred until research:** Need to compare what we have from NLI vs what FJMS provides. Are they the same? Is one richer?

### Source Attribution
- Claude's discretion on badge/label pattern (consistent with existing purple FJMS badge from Phase 26, or section-header-only)
- Claude's discretion on whether to add "PGP" labels to existing metadata for symmetry
- **Author display:** Show both manuscript author prominently AND cataloger in smaller text (e.g., "Cataloged by: [scholar]")
- **Source disagreement:** Claude's discretion on whether to flag when PGP and FJMS disagree on facts

### Field Priority & Density
- **Primary fields:** Title (Hebrew/English) and content description — these are most important to researchers
- **Secondary fields:** Author, date, place, source attribution — show when available
- **Adaptive display:** Only show fields that have data — no empty rows or placeholders
- **Description length:** Claude's discretion on truncation vs full display

### Description Coexistence
- **Multiple FJMS descriptions:** Show all, stacked vertically with source labels for each
- **Description formatting:** Research the actual description patterns in the data before deciding on any formatting/normalization
- **Visual distinction (PGP vs FJMS):** Claude's discretion on styling approach

### Claude's Discretion
- Section ordering (FJMS above or below PGP)
- Web sub-header divider vs inline labeled fields
- Purple badge per-field vs section header attribution
- Whether to add "PGP" labels to existing metadata
- Flagging PGP/FJMS disagreements
- Description truncation strategy
- Visual distinction between PGP and FJMS descriptions

</decisions>

<specifics>
## Specific Ideas

- FJMS website has separate "Identifications" and "Catalog Records" tabs — we should understand the data to determine which parts map to our export, but don't need to replicate FJMS's multi-tab structure
- FJMS website lets users choose display modes (combined by shelfmark, table view, by source) — useful reference but we should find our own approach
- Multiple scholarly sources per shelfmark is the norm, not the exception — the display approach must handle this gracefully, not as an edge case
- The approach is "research then decide" — research should explore the actual sidecar data before locking in display details

</specifics>

<research_questions>
## Research Questions (for gsd-phase-researcher)

These must be answered before planning can finalize display decisions:

1. **Multi-record distribution:** How many shelfmarks in `fjms_enrichment.db` have multiple catalog records? What's the distribution (1 record, 2-3, 4+)? How different are descriptions across sources for the same shelfmark?

2. **NLI overlap analysis:** Compare NLI Aleph data in FJMS sidecar vs what GenizahSearch fetches directly from NLI. Same data? FJMS richer? Any conflicts?

3. **Sidecar data scope:** What does our Phase 25 export actually contain? Does it have both "identification" level data (domain + short description) and "catalog record" level data (full detailed metadata)? Map our schema to the FJMS website's two-tab structure.

4. **Description patterns:** What do typical FJMS descriptions look like? Average length? Hebrew/English mix patterns? Any structured notation that could benefit from light formatting?

5. **Title comparison:** When both PGP and FJMS have titles for the same manuscript, how do they compare? Is one consistently richer? Should FJMS title enhance/replace the main header?

6. **Field population rates:** What percentage of catalog records have author, date, place, description? Which fields are commonly populated vs rare?

</research_questions>

<deferred>
## Deferred Ideas

- **FTS5 catalog search UI** — Schema already in sidecar, UI deferred to future milestone
- **NLI crossreference import** (~424K PartOf relationships) — separate effort
- **FJMS display mode selector** (combined/table/by-source like FJMS website) — if research shows multi-record display is complex, this could become its own phase

</deferred>

---

*Phase: 28-catalog-enrichment*
*Context gathered: 2026-02-15*
