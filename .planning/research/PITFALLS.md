# Domain Pitfalls: External Data Integration

**Domain:** Scholarly metadata integration (PGP transcriptions, NLI relationships) into existing search/browse platform
**Researched:** 2026-02-05
**Confidence:** HIGH (verified against codebase analysis, official documentation, and domain patterns)

---

## Critical Pitfalls

Mistakes that cause rewrites, data corruption, or major system failures.

### Pitfall 1: Tantivy Schema Changes Require Full Reindex

**What goes wrong:** Adding transcription fields (content_pgp, transcription_source, pgpid) to the existing Tantivy schema does not automatically populate those fields for existing 217K documents. Tantivy stores schema in `meta.json` and [does not support schema migration](https://github.com/quickwit-oss/tantivy/issues/301) - existing indexed documents retain their original field structure.

**Why it happens:** Teams assume additive schema changes (new fields) will work like relational database migrations. Tantivy's inverted index architecture means fields must exist at index time, and previously-indexed documents cannot retroactively gain new fields.

**Consequences:**
- Search on new fields returns zero results for 217K existing manuscripts (only 9,364 with PGP transcriptions appear)
- Mixed behavior where some documents have transcription field, others don't
- "Why doesn't my transcription search find this document?" user complaints
- Corruption if partial reindex attempted without understanding field propagation

**Prevention:**
1. Plan for FULL INDEX REBUILD when adding any new schema field
2. Estimate rebuild time on production data volume (217K pages + 9,364 transcriptions = ~500K docs)
3. Implement [zero-downtime rebuild strategy](https://medium.com/craftsmenltd/rebuild-elasticsearch-index-without-downtime-168363829ea4): build new index alongside old, swap via alias pattern
4. Store Tantivy schema version in `Genizah_Index/schema_version.json` to detect incompatibility
5. Test rebuild in staging with full production dataset before deployment

**Detection (warning signs):**
- Schema changes in `Indexer.create_index()` without updating `build_index.py` to perform full rebuild
- Transcription searches returning only newly-indexed documents
- Inconsistent field existence when querying random document samples

**Phase:** Address in Phase 1 (Data Infrastructure) - must solve before any schema-dependent features

---

### Pitfall 2: Document vs Page Granularity Mismatch

**What goes wrong:** PGP organizes transcriptions per PGPID (document-level), while GenizahSearch indexes per fl_id (page-level). A PGPID like `11665` may have transcription text covering "T-S 13J35.3 + AIU VII.A.23" (multiple sys_ids, multiple pages). Naively storing transcription at page level causes:
- Same transcription duplicated across pages (storage bloat, search rank distortion)
- Transcription chunks assigned to wrong page (content mismatch)
- No way to view complete transcription when pages separated

**Why it happens:** The data models were designed independently:
- GenizahSearch: `sys_id` -> `fl_id` (page) -> page content
- PGP: `PGPID` (document) -> `shelfmarks` (multi-fragment) -> transcription

Mapping between them requires a **document entity** that doesn't currently exist in GenizahSearch.

**Consequences:**
- Transcription attributed to first fragment only, other fragments appear "empty"
- Search results inflated by duplicate transcription hits across pages
- Joined fragments (T-S X + T-S Y) show only one fragment's transcription
- Unable to answer "show all text for this document" when fragments separated

**Prevention:**
1. Create **Document entity** as intermediary layer (as designed in PROJECT.md)
2. Store transcription at document level, not page level
3. Index transcription once per PGPID, with sys_id references for linking
4. Add `transcription_scope: document|page` field to distinguish PGP (document) from MiDRASH (page)
5. In search results, group hits by document when transcription match, not by page

**Detection:**
- Same transcription text appearing in multiple search results
- `transcriptions_linked.csv` `matched_part` column shows fragment assignments
- User reports: "I searched and found the same text 4 times"

**Phase:** Address in Phase 2 (Document Entity) - architecture decision, then Phase 3 (Transcription Integration)

---

### Pitfall 3: NLI Relationship Import Creates Orphan References

**What goes wrong:** NLI crossreference.csv contains 815K relationship records with AlmaId references. Not all AlmaIds exist in GenizahSearch's `libraries.csv` (2,321 records missing per FIST_INTEGRATION_DESIGN.md). Importing relationships with dangling references causes:
- Foreign key violations (if enforced)
- Silent data corruption (if not enforced)
- Broken UI when clicking "view related fragment"

**Why it happens:** FIST/NLI includes AlmaIds for manuscripts not yet in GenizahSearch (private collections, embargoed materials, catalog backlogs). Bulk import without validation creates edges to non-existent nodes.

**Consequences:**
- Join groups referencing non-existent manuscripts
- UI crashes or 404s when following relationship links
- "Ghost" manuscripts appearing in join counts but not viewable
- Data integrity violations making future queries unreliable

**Prevention:**
1. Pre-validate ALL AlmaIds against `libraries.csv` BEFORE import
2. Export two files: `relationships_valid.csv` (both ends exist) and `relationships_orphan.csv` (one end missing)
3. Track orphans for future import when missing AlmaIds added to system
4. Add foreign key constraint in Supabase: joins.sys_id_a REFERENCES libraries(sys_id)
5. Use [topologically sorted import order](https://www.cockroachlabs.com/blog/common-foreign-key-mistakes/): parent tables first, then relationships

**Detection:**
- Run `SELECT sys_id FROM joins WHERE sys_id NOT IN (SELECT sys_id FROM libraries)` before going live
- Monitor for 404 errors on browse page relationship links
- Count of join records > count of unique (sys_id_a, sys_id_b) pairs in valid manuscripts

**Phase:** Address in Phase 4 (NLI Joins Import) - validation must precede import

---

### Pitfall 4: Shelfmark Normalization Divergence After Integration

**What goes wrong:** GenizahSearch has canonical `normalize_shelfmark()` in `genizah_core.py:84-120`. PGP export scripts (`pgp_transcriptions_export.py`) have their own normalization. FIST shelfmarks have different conventions (double spaces, em-dashes). When normalizations diverge:
- 96.5% match rate degrades over time
- New records fail to link
- Debugging becomes forensic archaeology

**Why it happens:** Each data source has quirks. Developers add source-specific fixes without updating canonical function. Multiple normalization paths accumulate.

From CONCERNS.md: "Shelfmark Normalization - 5 Implementations... Despite unification, if new code added elsewhere that doesn't use `normalize_shelfmark()`, inconsistency will creep back."

**Consequences:**
- PGP transcriptions stop matching GenizahSearch records
- "Why isn't this transcription showing?" bugs
- Each debugging session discovers new edge case, fix added in wrong place
- Match rate regressions go undetected without monitoring

**Prevention:**
1. ALL shelfmark normalization MUST route through `genizah_core.normalize_shelfmark()`
2. Add comprehensive shelfmark test suite (see MATCHING_SUMMARY.md for edge cases)
3. Create shelfmark matching regression test: run periodically, alert on match rate drop
4. Document normalization rules in single source of truth (update MATCHING_SUMMARY.md)
5. Add [pre-commit hook](https://github.com/pre-commit/pre-commit) grepping for non-canonical normalization patterns

**Detection:**
- Match rate monitoring: should stay at 96.5% or improve, never degrade
- grep for `\.lower\(\).*replace\(.*shelfmark` outside genizah_core.py
- New "unmatched" records appearing that previously matched

**Phase:** Ongoing vigilance - establish monitoring in Phase 1, maintain throughout

---

## Moderate Pitfalls

Mistakes that cause delays, technical debt, or degraded user experience.

### Pitfall 5: Hebrew Text Search Without Proper Normalization

**What goes wrong:** Hebrew search requires handling:
- Nikud (vowel marks): `שָׁלוֹם` vs `שלום`
- Final letters: `ם` vs `מ` (sometimes conflated in manuscripts)
- Ligatures and combining marks
- Biblical vs modern orthography

Indexing raw Hebrew text means searches fail unless exact orthographic match.

**Why it happens:** GenizahSearch has `strip_nikud()` function (line 74-81) but it's not applied consistently during indexing or search. [Hebrew morphology is complex](https://code972.com/blog/2010/05/challenges-with-indexing-hebrew-texts-hebmorph-part-1-18) - patterns don't always follow rules.

**Consequences:**
- User searches `שלום`, transcription has `שָׁלוֹם`, no match
- Search quality degradation for scholarly users expecting fuzzy matching
- "The search is broken" when it's actually normalization mismatch

**Prevention:**
1. Apply `strip_nikud()` BOTH at index time AND query time
2. Normalize final letters consistently (define policy: always finals, or always medials?)
3. Consider adding fuzzy matching for close variants
4. Test with real PGP transcription samples containing nikud
5. Document Hebrew search behavior for users

**Detection:**
- User reports of searches not finding known text
- Compare raw transcription with indexed content character-by-character
- Test suite with nikud variations

**Phase:** Address in Phase 3 (Transcription Integration) - part of indexing pipeline

---

### Pitfall 6: Unbounded Cache Growth During Import

**What goes wrong:** From CONCERNS.md: "NLI image cache (`_cache` dict) grows with every unique system_id requested... no cache size limit implemented." During bulk import of 815K relationships or 9,364 transcriptions, caches accumulate without eviction.

**Why it happens:** Production caches designed for user browsing (thousands of items) are exposed during bulk operations (hundreds of thousands of items). Memory exhaustion or OOM kills occur.

**Consequences:**
- Import process crashes at unpredictable point
- Partial imports leave database in inconsistent state
- Server memory exhaustion affects other services
- "It worked in testing" (small dataset) fails in production (full dataset)

**Prevention:**
1. Bulk import scripts should BYPASS caching layers or use separate cache instances
2. Set explicit cache size limits: `@lru_cache(maxsize=10000)` instead of unbounded dicts
3. Use batch processing with periodic cache clear: every 1000 records, clear caches
4. Monitor memory during import with alerts
5. Run imports on separate process/server to isolate memory impact

**Detection:**
- Memory usage graphs during import showing continuous growth
- Import failures after running for extended time
- OOM errors in server logs

**Phase:** Address in Phase 1 (Data Infrastructure) - fix before any bulk operations

---

### Pitfall 7: Transcription Version Conflicts Without Resolution Strategy

**What goes wrong:** A single manuscript may have:
- MiDRASH V0.8 auto-transcription (page-level)
- MiDRASH V0.7 auto-transcription (page-level)
- PGP scholar transcription (document-level)
- User corrections (word-level)

Without explicit version precedence, UI shows conflicting or arbitrary versions.

**Why it happens:** Multiple transcription sources accumulated organically. Each source has different granularity, authority level, and update frequency. No system for reconciliation.

**Consequences:**
- User sees V0.7 when V0.8 is better
- PGP transcription hidden behind auto-transcription
- User corrections overwritten by import refresh
- "Which version am I looking at?" confusion

**Prevention:**
1. Define explicit version precedence: `User Correction > PGP Scholarly > V0.8 > V0.7`
2. Store version metadata: `{source: 'pgp', scholar: 'Ben-Sasson', date: '2020', confidence: 'high'}`
3. Version selector UI showing all available versions with metadata
4. Never silently overwrite - preserve history
5. Document version policy for users and developers

**Detection:**
- User complaints about "wrong" transcription showing
- Audit: for records with multiple sources, which displays by default?
- Test: modify user correction, re-import PGP, verify correction preserved

**Phase:** Address in Phase 3 (Transcription Integration) - version selector design

---

### Pitfall 8: Import Without Rollback Capability

**What goes wrong:** Bulk import of 9,364 transcriptions or 815K relationships fails partway through. No mechanism to rollback to pre-import state.

**Why it happens:** Imports executed as sequential inserts without transaction boundaries. Each insert commits immediately. Failure at record 5000 means 4999 records inserted, 4365 missing.

**Consequences:**
- Partial data more harmful than no data (inconsistent state)
- Manual cleanup required (time-consuming, error-prone)
- Re-import may create duplicates
- "We need to restore from backup" when no backup exists

**Prevention:**
1. Wrap bulk imports in transactions with checkpoints
2. For large imports, use staged approach: Import to temp table, validate, then merge
3. Export pre-import state to backup file before starting
4. Implement idempotent imports: re-running produces same result (via upsert, not insert)
5. Add `import_batch_id` column to track which import created each record

**Detection:**
- Record count mismatches: expected 9,364, found 5,231
- Timestamps inconsistent: some records from import A, some from import B
- Duplicate key errors on re-import

**Phase:** Address in Phase 1 (Data Infrastructure) - import tooling

---

## Minor Pitfalls

Mistakes that cause annoyance but are recoverable.

### Pitfall 9: Attribution Metadata Loss

**What goes wrong:** PGP transcriptions include scholar attribution (e.g., "Menahem Ben-Sasson"). If attribution stripped during import, transcriptions appear unattributed, violating scholarly norms.

**Why it happens:** Attribution stored in separate field (`source_scholar` in transcriptions_linked.csv). Easy to omit during schema design or import mapping.

**Consequences:**
- Scholarly community criticism ("you're not crediting sources")
- License compliance issues (PGP may require attribution)
- Loss of provenance information valuable to researchers

**Prevention:**
1. Include `transcription_source` and `transcription_scholar` fields in schema
2. Display attribution on every transcription view
3. Verify attribution preserved end-to-end before launch
4. Link back to PGP source URL for full context

**Detection:**
- Sample transcriptions in UI: is attribution visible?
- Query: `SELECT * FROM transcriptions WHERE scholar IS NULL`

**Phase:** Address in Phase 3 (Transcription Integration) - schema design

---

### Pitfall 10: Ignoring Import Errors for "Later"

**What goes wrong:** Import logs show 339 unmatched records (per MATCHING_SUMMARY.md). Developer notes "fix later", deploys. Six months later, "later" never comes, and edge cases accumulate.

**Why it happens:** Pressure to ship. 96.5% match rate "good enough." Edge cases require disproportionate effort.

**Consequences:**
- Known gaps never addressed
- User reports for unmatched records create support burden
- Incomplete data undermines trust in platform

**Prevention:**
1. Track ALL unmatched records in structured format (done: `transcriptions_unmatched.csv`)
2. Categorize by cause (external collection, format issue, data quality)
3. Create actionable roadmap: "Copenhagen records: import if collection added"
4. Review unmatched quarterly - are they now matchable?
5. Set expectation with users: "96.5% coverage, see known gaps"

**Detection:**
- Growing unmatched file without review
- User reports mentioning specific unmatched records
- Stale TODO comments in import scripts

**Phase:** Ongoing - establish tracking in Phase 1, review periodically

---

### Pitfall 11: Print Statements in Import Scripts

**What goes wrong:** From CONCERNS.md: "Code uses both `print()` and `logger.info()` inconsistently." Import scripts using print() don't capture output to logs.

**Why it happens:** Quick development, copy-paste from interactive sessions.

**Consequences:**
- Import failures not logged for diagnosis
- Progress not visible when running headless
- Audit trail missing

**Prevention:**
1. All import scripts use logging framework
2. Configure log rotation before long-running imports
3. Add timing and record count logging for performance analysis
4. Review existing scripts (CONCERNS.md lists specific files)

**Detection:**
- Import fails, no error in logs
- grep for `print(` in scripts/ directory

**Phase:** Address in Phase 1 (Data Infrastructure) - code cleanup

---

## Phase-Specific Warnings

| Phase Topic | Likely Pitfall | Mitigation |
|-------------|---------------|------------|
| Data Infrastructure (Phase 1) | Cache exhaustion during bulk ops | Implement bounded caches, batch processing |
| Data Infrastructure (Phase 1) | No rollback on failed import | Staged imports with validation checkpoints |
| Document Entity (Phase 2) | Granularity mismatch | Design document layer before indexing |
| Transcription Integration (Phase 3) | Schema change without rebuild | Plan full reindex, zero-downtime swap |
| Transcription Integration (Phase 3) | Hebrew normalization gaps | Apply strip_nikud consistently |
| NLI Joins Import (Phase 4) | Orphan references | Pre-validate all AlmaIds |
| Ongoing | Shelfmark normalization divergence | Centralize in canonical function, add tests |
| Ongoing | Version conflict confusion | Define and document precedence rules |

---

## Domain-Specific Risk Matrix

| Risk Area | Severity | Likelihood | Mitigation Effort |
|-----------|----------|------------|-------------------|
| Tantivy schema rebuild | High | Certain | Medium (plan for it) |
| Granularity mismatch | High | Certain | High (architecture change) |
| Orphan relationships | High | High (2,321 known) | Low (validation script) |
| Shelfmark normalization | Medium | Ongoing | Medium (tests, monitoring) |
| Hebrew text search | Medium | High | Low (consistent normalization) |
| Cache exhaustion | Medium | High | Low (bounded caches) |
| Version conflicts | Medium | High | Medium (version selector) |
| Attribution loss | Low | Medium | Low (schema field) |

---

## Quick Reference Checklist

Before each integration phase:

- [ ] Will this require Tantivy schema changes? If yes, plan full rebuild
- [ ] Does data map cleanly to existing granularity (page vs document)?
- [ ] Are all foreign key references validated before import?
- [ ] Is shelfmark normalization routing through canonical function?
- [ ] Are caches bounded for bulk operation volumes?
- [ ] Is import idempotent (safe to re-run)?
- [ ] Is rollback possible if import fails midway?
- [ ] Are all sources properly attributed in schema?
- [ ] Is Hebrew text normalized consistently (index and query)?
- [ ] Are unmatched records tracked and categorized?

---

## Sources

**Tantivy Schema:**
- [Tantivy Issue #301: Adding fields to existing Index's schema](https://github.com/quickwit-oss/tantivy/issues/301)
- [Tantivy Documentation](https://docs.rs/tantivy/latest/tantivy/)

**Search Index Migration:**
- [Elasticsearch Zero-Downtime Reindexing](https://medium.com/craftsmenltd/rebuild-elasticsearch-index-without-downtime-168363829ea4)
- [Changing Mapping with Zero Downtime - Elastic Blog](https://www.elastic.co/blog/changing-mapping-with-zero-downtime)

**Hebrew Text Processing:**
- [Challenges with indexing Hebrew texts (HebMorph)](https://code972.com/blog/2010/05/challenges-with-indexing-hebrew-texts-hebmorph-part-1-18)

**Data Import Best Practices:**
- [Common Foreign Key Mistakes - CockroachDB](https://www.cockroachlabs.com/blog/common-foreign-key-mistakes/)
- [Data Migration Validation Best Practices 2025](https://www.quinnox.com/blogs/data-migration-validation-best-practices/)

**Digital Humanities Metadata:**
- [Data Fragmentation Risks - Keboola](https://www.keboola.com/blog/the-risks-of-data-fragmentation)
- [Yale Digital Humanities - Metadata and Collections Data](https://guides.library.yale.edu/dh/metadata)

**Codebase Analysis:**
- GenizahSearch CONCERNS.md (codebase concerns inventory)
- GenizahSearch FIST_INTEGRATION_DESIGN.md (NLI data analysis)
- GenizahSearch TRANSCRIPTIONS_INTEGRATION_DESIGN.md (PGP integration)
- GenizahSearch MATCHING_SUMMARY.md (96.5% match rate documentation)

---

*Pitfalls audit: 2026-02-05*
*Confidence: HIGH - verified against codebase, official documentation, and domain patterns*
