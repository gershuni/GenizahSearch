# Phase 35: PGP Sidecar Export - Context

**Gathered:** 2026-02-17
**Status:** Ready for planning

<domain>
## Phase Boundary

Export all PGP reference data from Supabase (documents, document_sources, document_footnotes, document_fragments) into a local pgp.db SQLite sidecar file. The export must be validated, reproducible, and idempotent. This phase does NOT rewrite the service layer or change how apps consume data — that's Phase 36.

</domain>

<decisions>
## Implementation Decisions

### Export script behavior
- Script structure: Claude's discretion (standalone script or shared module — fit existing codebase conventions)
- Credentials: Use existing SUPABASE_URL and SUPABASE_ANON_KEY environment variables from .env
- Progress output: Print table name, row count, and elapsed time for each of the 4 tables
- Error handling: On failure, delete partial pgp.db — no corrupt sidecars left behind, clean slate for retry

### Data fidelity
- Verbatim export from Supabase — NULLs stay NULL, empty strings stay empty, no transformations or cleanup
- Type mapping: Claude's discretion — pick explicit mappings that ensure correct round-trips (JSONB→TEXT, TIMESTAMP→TEXT ISO-8601, etc.)
- Index strategy: Claude's discretion — determine which indexes are needed based on Phase 36's query patterns
- Validation: Claude's discretion — built-in vs separate, pick whichever is simpler and more reliable

### JSON determinism
- Idempotency level: Claude's discretion — pick the level of determinism that's practical (byte-identical vs logically identical)
- JSON key ordering: Claude's discretion — decide based on what best serves the idempotency requirement
- JSON formatting (compact vs readable): Claude's discretion — balance file size vs debuggability
- Sections column storage format: Claude's discretion — pick based on how sections are currently stored and consumed in Supabase

### Sidecar conventions
- File location: Claude's discretion — follow existing sidecar patterns (fjms_enrichment.db, nli_crossref.db are in project root)
- Version scheme: Claude's discretion — follow existing sidecar patterns (fjms_enrichment.db uses independent schema version)
- Distribution: Claude's discretion — follow existing sidecar pattern (gitignored, bundled with installer/deployment)
- Meta table fields: Claude's discretion — pick what's useful for debugging and verification

### Claude's Discretion
Many decisions in this phase are delegated to Claude — this is a pure infrastructure/ETL phase where technical correctness matters more than visual/UX preferences. Key discretion areas:
- Script structure and module organization
- SQLite type mapping strategy
- Index creation during export
- Validation approach (built-in vs separate)
- JSON serialization details (key ordering, formatting, determinism level)
- Sections storage format
- File location, versioning, distribution model
- Meta table content

</decisions>

<specifics>
## Specific Ideas

- Existing sidecar pattern to follow: fjms_enrichment.db (v2.0.0) and nli_crossref.db (v1.2.0) — both have meta tables with version tracking
- Data volumes: documents (35,839), sources (9,364), footnotes (22,757), fragments (36,155)
- Tags column is JSONB with GIN index in Supabase — will become TEXT queried with json_each() in SQLite (decided during roadmap planning)
- Sections column stores structured HTML section data parsed by the section parser (v5.7.2)

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 35-pgp-sidecar-export*
*Context gathered: 2026-02-17*
