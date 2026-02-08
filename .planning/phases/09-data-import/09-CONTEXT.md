# Phase 9: Data Import - Context

**Gathered:** 2026-02-08
**Status:** Ready for planning

<domain>
## Phase Boundary

Import all ~41,000 PGP documents into Supabase (upsert), completing the full dataset. This includes documents, fragment links, sources (editions/translations), and footnotes. Existing 7,090 documents get updated; ~34K new documents get inserted. No user-facing UI changes — this is a data pipeline phase.

**Inputs:**
- `pgp_data/documents.csv` — 41,193 PGP document records with full metadata
- `pgp_data/fragments.csv` — 36,162 fragment records with collection/library/provenance metadata
- `pgp_data/transcriptions_linked.csv` — 9,364 transcriptions with sys_id links
- `pgp_data/footnotes.csv` — scholarship/footnotes records (~29MB)

**Outputs:**
- ~41K documents in Supabase `documents` table (upserted)
- Fragment links in `document_fragments` with full fragment metadata (collection, library, provenance, material)
- Updated `document_sources` with any new editions/translations found
- Footnotes/scholarship data in a new table
- Full verification report (before/after counts, new/updated/failed records)

</domain>

<decisions>
## Implementation Decisions

### Data Sourcing Strategy
- Import ALL 41K documents from documents.csv — not just those with sys_id matches
- Import everything available: all metadata columns (type, tags, description, dates, languages, scholarship_records, shelfmarks_historic, etc.)
- Fetch and import any available transcription/translation text for the ~34K new documents — not just metadata
- Import full fragment metadata from fragments.csv (collection, library, provenance, material) — not just the pgpid-to-sys_id linkage
- Import footnotes.csv scholarship records into a new table

### Import Strategy (Upsert All)
- Full upsert of all 41K documents — existing 7,090 get updated if PGP data changed, new ~34K get inserted
- Full upsert of document_sources alongside documents — new transcriptions/translations added, existing ones updated
- PGP data and user corrections are separate layers — upsert freely overwrites PGP source data without conflict
- Two-pass FK-safe pattern: documents first, then fragment links and sources

### Completeness & Validation
- Documents that can't match any sys_id in libraries.csv: import the document record with all metadata, just don't create fragment links
- Success threshold: 99%+ of 41K documents must load successfully. Small number of failures (malformed rows, encoding issues) acceptable if logged
- Full verification report required: before/after counts for all tables, list of new records, updated records, and failures with reasons — written to file

### Claude's Discretion
- Dry-run vs direct execute approach (safety pattern)
- Batch sizes for Supabase operations
- Footnotes table schema design
- Fragment metadata storage (extend document_fragments vs new table)
- Script location and structure (reuse v1 script or build fresh)
- Error recovery and resume-on-failure approach
- Progress reporting format

</decisions>

<specifics>
## Specific Ideas

- v1 import used a two-pass pattern (documents first, then FK-constrained fragment links) — proven pattern to reuse
- v1 context established: Oxford parts have their own sys_ids, multi-fragment delimiter detection needed, page_info column for folio/page references
- The existing `transcriptions_linked.csv` has 96.5% match rate — most records will import successfully
- footnotes.csv is 29MB — likely needs batch processing and a dedicated table

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 09-data-import*
*Context gathered: 2026-02-08*
