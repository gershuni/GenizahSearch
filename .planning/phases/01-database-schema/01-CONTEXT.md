# Phase 1: Database Schema - Context

**Gathered:** 2026-02-05
**Status:** Ready for planning

<domain>
## Phase Boundary

Create Supabase tables (`documents`, `document_fragments`) to support multi-fragment PGP documents. This is database infrastructure only — no UI, no import logic, no service layer.

Requirements: DOC-01, DOC-02, DOC-03

</domain>

<decisions>
## Implementation Decisions

### Table Columns

**documents table:**
- `pgpid` (integer, primary key) — PGP document ID
- `shelfmark_combined` (text) — Raw PGP shelfmark string (e.g., "T-S 13J35.3 + AIU VII.A.23")
- `document_type` (text) — Letter, Legal document, List, etc.
- `tags` (JSONB array) — Subject tags: communal, marriage, trade, etc.
- `doc_date_original` (text) — Original date notation
- `doc_date_standard` (text) — Standardized date
- `inferred_date_display` (text) — Display format for inferred date
- `description` (text) — English scholarly description
- `created_at` (timestamp)

**document_fragments table:**
- `id` (serial, primary key)
- `document_id` (integer, FK to documents.pgpid)
- `sys_id` (text) — GenizahSearch system ID
- `sequence_order` (integer) — Order within document

**Transcription storage:**
- Claude decides: Either extend corrections table with source='pgp' or add transcription column to documents
- Attribution format: "Amir Ashur, PGP" (scholar name + PGP source)

**Claude's Discretion:**
- Whether to denormalize shelfmark on document_fragments
- Whether to add import metadata (imported_at, source_file_hash)
- JSONB array vs separate tags table (lean toward JSONB for simplicity)
- Compute PGP URL from pgpid (pattern: https://geniza.princeton.edu/documents/{pgpid}/)

### Indexing Strategy

- **Primary index:** sys_id on document_fragments (most common lookup: "find document for this fragment")
- **Index on pgpid:** For document lookups
- **GIN index on tags:** Enable tag-based filtering queries
- **Index on document_type:** Enable type-based filtering
- **No composite index** on (document_id, sequence_order) — simple index sufficient

### Joins Integration

- PGP multi-fragment documents use new `documents`/`document_fragments` tables (NOT fragment_joins)
- Keep fragment_joins table for user-created pairwise joins
- Phase 7 UI will merge both sources for display (Claude decides exact UX)
- Warn user if they create a pairwise join between fragments already in a PGP document
- Expose PGP document membership in existing joins API (unified, backward compatible)

</decisions>

<specifics>
## Specific Ideas

- Attribution must include both scholar name AND "PGP" source (e.g., "Amir Ashur, PGP")
- Store raw combined shelfmark for debugging and PGP link display

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 01-database-schema*
*Context gathered: 2026-02-05*
