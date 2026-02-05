# Phase 2: PGP Data Import - Context

**Gathered:** 2026-02-05
**Status:** Ready for planning

<domain>
## Phase Boundary

Import all PGP data (transcriptions, metadata, joins) into the Supabase tables created in Phase 1. Parse multi-fragment shelfmarks to create document-fragment linkages. Handle edge cases (unmatched records, Oxford parts, parsing failures) gracefully. Script must be repeatable.

**Inputs:**
- `pgp_data/transcriptions_linked.csv` — 9,364 transcriptions with sys_id links
- `pgp_data/documents.csv` — PGP document metadata

**Outputs:**
- Populated `documents` table with transcriptions and metadata
- Populated `document_fragments` table with sys_id linkages and page info
- Import reports (summary + detailed CSV of issues)

</domain>

<decisions>
## Implementation Decisions

### Error Handling Strategy
- **Unmatched sys_ids:** Log and skip — record in report, continue importing. Review unmatched later.
- **Parse failures:** Import document, skip joins — partial data is better than none. Don't create fragment links if shelfmark can't be parsed.
- **Report format:** Both console summary AND detailed CSV file listing all issues with pgpid, shelfmark, and failure reason.
- **Dry-run mode:** Default to dry-run — first run shows what would happen without writing to database. Explicit `--execute` flag required to actually import.

### Multi-Fragment Shelfmark Parsing
- **Delimiter detection:** Claude should analyze the PGP data to find all delimiter patterns (not just ' + ')
- **Sequence order:** Position in shelfmark string determines order (first fragment = 1, second = 2, etc.)
- **Page/folio info is crucial:** Store page references (recto, verso, folio ranges) in document_fragments — this specifies which images the document covers, not just which fragment
- **Add column:** document_fragments needs a `page_info` column (or similar) to store folio/page references

### Oxford Parts Handling
- **Key insight:** Oxford parts each have their own sys_id — there's no "parent" manuscript sys_id
- **Matching strategy:** Claude analyzes data patterns to determine best approach for matching PGP shelfmarks to Oxford part sys_ids
- **Multi-part documents:** Link to all relevant sys_ids when a document spans multiple parts — the parts will be fetched automatically

### Import Reporting & Idempotency
- **Re-run behavior:** Upsert — if pgpid exists, update its data; new pgpids get inserted
- **Progress display:** Visual progress bar during execution
- **Script location:** Claude decides based on project conventions (likely `scripts/` folder)
- **Update tracking:** Claude decides whether to add updated_at column

### Claude's Discretion
- Exact delimiter patterns to handle (analyze data)
- Oxford matching heuristics (analyze data)
- Script location (follow conventions)
- Whether to add updated_at tracking column
- Batch size for Supabase inserts
- Specific error message formats

</decisions>

<specifics>
## Specific Ideas

- Page/folio info (recto, verso, page ranges) is crucial for knowing which images belong to a document — must be preserved
- Oxford codicological parts are separate sys_ids, not child records of a parent
- The existing `transcriptions_linked.csv` already has 96.5% match rate — most records will import successfully

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 02-pgp-data-import*
*Context gathered: 2026-02-05*
