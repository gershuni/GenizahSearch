# Phase 29: Data Infrastructure - Context

**Gathered:** 2026-02-15
**Status:** Ready for planning

<domain>
## Phase Boundary

Import NLI crossreference data (815K image-level records) and Cambridge IIIF manifest URLs (141K records) into the SQLite sidecar database, and provide a shared service layer callable from both web and desktop apps. This phase builds the data foundation — downstream phases (30-34) consume this data for image display, metadata, and relationships.

</domain>

<decisions>
## Implementation Decisions

### Data matching & joins
- NLI crossref joins to libraries.csv via direct equality: `NLI_AlmaId = system_number`
- Cambridge IIIF joins to libraries.csv via shelfmark normalization — reuse existing normalization logic from genizah_core.py (same logic used for search)
- Cardinality of NLI crossref to libraries.csv is unknown (1-to-1 or 1-to-many) — researcher must examine actual CSV to determine whether multiple crossref rows share the same AlmaId
- Unmatched NLI records: import all 815K records regardless of whether they match a libraries.csv entry — future-proof, match later as library grows

### Import scope & filtering
- Import ALL columns from NLI crossref CSV — no filtering. Every field stored in sidecar for potential future use
- Import ALL records — no filtering by match status
- Import script must be rerunnable (idempotent) — DROP + recreate tables on each run, following the FJMS import pattern
- Cambridge IIIF data acquisition path is unknown — researcher must investigate whether data is available as a downloadable file or requires API fetching from CUDL

### Claude's Discretion
- Sidecar file strategy: same fjms_enrichment.db or separate file — decide based on technical tradeoffs after examining current sidecar schema and size
- Sidecar table schema design — normalize or denormalize based on actual CSV structure and downstream query patterns
- Service API surface — what queries NliCrossrefService exposes, batch vs single lookup, thread-safety pattern. Follow FJMS service pattern where appropriate
- Index strategy — which columns to index based on downstream phase query patterns

</decisions>

<specifics>
## Specific Ideas

- Follow the FJMS sidecar pattern that already works well (SQLite, shared service, both apps)
- Researcher must examine `nli_crossreference.csv` headers and sample rows to understand the actual data structure, column names, and cardinality before planning
- Researcher must investigate Cambridge IIIF data availability — is there a bulk download, API endpoint, or existing dataset?

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 29-data-infrastructure*
*Context gathered: 2026-02-15*
