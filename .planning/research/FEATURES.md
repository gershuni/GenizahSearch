# Feature Landscape: PGP Sidecar Migration + FJMS Full Texts

**Domain:** SQLite sidecar migration, FJMS scholarly text integration, offline-capable browsing
**Researched:** 2026-02-16
**Overall Confidence:** HIGH (based on direct analysis of existing codebase, Supabase schema, FIST.db data)

---

## Executive Summary

GenizahSearch v6.0.0 has two complementary goals: (1) migrate all PGP read-only reference data from Supabase PostgreSQL to a local SQLite sidecar, and (2) integrate FJMS full texts (65K catalog descriptions from `dbo_UnitFullText`) as additional scholarly sources in the version selector. Together these eliminate cloud dependency for browsing, reduce latency for both apps, and add ~85K AlmaIds-worth of FJMS scholarly content alongside the existing 9,364 PGP sources.

**Important data clarification:** The "65K transcriptions" in the project scope are `dbo_UnitFullText` records from FIST.db -- scholarly catalog descriptions that identify and describe manuscript content (e.g., "Contains portions from Genesis 39:20 - 41:8. With sporadic Tiberian vowel points."). These are NOT line-by-line transcriptions of the manuscript text itself. The actual FJMS line-by-line transcriptions (56K in `dbo_UnitTranscription`) are stored as external file references and would require a separate data pipeline to extract. The catalog descriptions are still highly valuable as scholarly sources but should be labeled accurately in the UI.

---

## Table Stakes

Features users expect after migration. Missing = regression or broken product.

### TS-1: PGP Data in SQLite Sidecar

| Aspect | Detail |
|--------|--------|
| **What** | All 4 Supabase read-only tables (documents, document_sources, document_fragments, document_footnotes) exported to a `pgp_data.db` SQLite sidecar |
| **Why Expected** | Without this, browsing PGP data requires Supabase connectivity. Desktop users behind firewalls or with slow connections get degraded or broken experience |
| **Complexity** | Medium |
| **Dependencies** | New export script, similar pattern to `export_fist_enrichment.py` |
| **Data Volume** | 35,839 documents + 9,364 sources + 22,757 footnotes + 36,155 fragments = ~104K rows |
| **Notes** | SQLite sidecar pattern already proven by `fjms_enrichment.db` (245MB, 762K rows) and `nli_crossref.db` (815K rows). PGP data is much smaller |

### TS-2: Shared document_service.py Rewritten for SQLite

| Aspect | Detail |
|--------|--------|
| **What** | Rewrite `shared/document_service.py` to read from SQLite sidecar instead of Supabase. Same API surface, different backend |
| **Why Expected** | Both apps import from this service. Changing the data source must be transparent to UI code |
| **Complexity** | Medium-High |
| **Dependencies** | TS-1 (sidecar must exist) |
| **Current API** | 12 public functions: `get_document_for_fragment`, `get_sources_for_document`, `get_all_sources_for_fragment`, `get_editions_for_document`, `get_translations_for_document`, `get_sys_ids_with_transcriptions`, `get_fragments_by_tag`, `get_all_distinct_tags`, `get_section_for_page`, `parse_transcription_sections`, `get_document_metadata`, `get_transcription_for_document` |
| **Notes** | Pure functions (`parse_transcription_sections`, `get_section_for_page`, `parse_html_sections`, `PGPHTMLParser`) are data-agnostic and need zero changes. The 8 Supabase-calling functions need SQLite equivalents. Special attention needed for JSONB -> JSON text column handling (tags field) |

### TS-3: Version Selector Continues Working

| Aspect | Detail |
|--------|--------|
| **What** | The version selector (`web/components/version_selector.py`) must continue displaying PGP editions, translations, and user corrections after migration |
| **Why Expected** | Version selector is a core UX element used on every manuscript page. Breaking it would be highly visible |
| **Complexity** | Low (if TS-2 preserves API surface) |
| **Dependencies** | TS-2 |
| **Notes** | Version selector consumes `all_sources` from `get_all_sources_for_fragment()`. If that function returns the same dict structure, no version selector changes needed. The selector currently handles: PGP editions (green icon), PGP translations (blue icon, grouped by language), V0.8 original, user corrections (by name + date), pending corrections (amber). |

### TS-4: Search Result PGP Indicators

| Aspect | Detail |
|--------|--------|
| **What** | Search results continue showing which manuscripts have PGP transcriptions available |
| **Why Expected** | Researchers use this indicator to prioritize which results to examine. `get_sys_ids_with_transcriptions()` does batch lookup of sys_ids against document_fragments |
| **Complexity** | Low |
| **Dependencies** | TS-1 |
| **Notes** | Currently uses batch `IN()` queries on Supabase. SQLite equivalent is straightforward. Batch size pattern (200 chunks) may change to SQLite's 999 variable limit |

### TS-5: PGP Tag-Based Search and Browse

| Aspect | Detail |
|--------|--------|
| **What** | Tag-based search (`get_fragments_by_tag`) and distinct tags dropdown (`get_all_distinct_tags`) continue working |
| **Why Expected** | PGP tags (communal, marriage, trade, etc.) are used for subject-based discovery |
| **Complexity** | Medium |
| **Dependencies** | TS-1 |
| **Notes** | Key schema change: Supabase uses JSONB with GIN index and `@>` contains operator. SQLite will need either `json_each()` for array queries or a denormalized tags junction table. `json_each()` is available in SQLite 3.9+ (Python 3.10+ bundles 3.37+). Recommend `json_each()` approach to match existing data model |

### TS-6: PGP Footnotes Display

| Aspect | Detail |
|--------|--------|
| **What** | Bibliography/scholarship footnotes continue displaying on browse page |
| **Why Expected** | 22,757 footnotes link PGP documents to published sources |
| **Complexity** | Low |
| **Dependencies** | TS-1 |
| **Notes** | Simple table query migration. Schema already designed with source_slug deduplication |

### TS-7: Multi-Fragment Document Navigation

| Aspect | Detail |
|--------|--------|
| **What** | Joined manuscripts (multiple fragments per PGP document) continue to navigate as a unit |
| **Why Expected** | Reading Desk and browse page rely on `get_fragments_for_document()` to show all fragments in sequence |
| **Complexity** | Low |
| **Dependencies** | TS-1 |
| **Notes** | Simple SELECT with ORDER BY sequence_order |

### TS-8: Graceful Degradation When Sidecar Missing

| Aspect | Detail |
|--------|--------|
| **What** | App starts and works without PGP sidecar (returns empty results instead of crashing) |
| **Why Expected** | Pattern already established by FjmsService.is_available() and NliCrossrefService.is_available() |
| **Complexity** | Low |
| **Dependencies** | None |
| **Notes** | PgpService class should follow exact same singleton + is_available() pattern |

### TS-9: Read-Only Supabase Tables Removed (Post-Verification)

| Aspect | Detail |
|--------|--------|
| **What** | After migration verified end-to-end, remove 4 PGP tables from Supabase |
| **Why Expected** | Dual-source creates confusion and maintenance burden |
| **Complexity** | Low (but requires careful verification first) |
| **Dependencies** | TS-1 through TS-8 all verified working |
| **Notes** | Keep Supabase migration SQL as rollback option. Supabase retained for: auth, corrections, lists, comments, discoveries, joins (user-generated community data) |

---

## Differentiators

Features that set the product apart. Not expected but valued.

### D-1: FJMS Full Texts (Catalog Descriptions) as Version Selector Source

| Aspect | Detail |
|--------|--------|
| **What** | Export `dbo_UnitFullText` (65,332 records, linked to ~85K AlmaIds) into FJMS sidecar and display as additional source in version selector |
| **Value Proposition** | Researchers see FJMS catalog metadata, domain badges, and bibliography already. Showing the full scholarly catalog description alongside PGP editions is a natural extension. Covers manuscripts that have NO PGP edition at all |
| **Complexity** | Medium |
| **Dependencies** | Export pipeline from FIST.db; integration with version selector |
| **Data Profile** | 65,332 rows. ~56K with >100 chars. Only ~3,640 exceed 500 chars. Most are brief catalog identifications. Linked to 85K AlmaIds. Bilingual (Hebrew + English). Page reference column present |

### D-2: FJMS Source Badges in Version Selector

| Aspect | Detail |
|--------|--------|
| **What** | Purple FJMS badge in version selector, consistent with existing FJMS visual language |
| **Value Proposition** | Clear provenance: researchers always know which project produced the text |
| **Complexity** | Low |
| **Dependencies** | D-1 |
| **Notes** | Follow existing pattern: green for PGP verified, blue for translations, purple for FJMS, grey for V0.8, amber for pending |

### D-3: Offline Browsing for Desktop App

| Aspect | Detail |
|--------|--------|
| **What** | Desktop app can browse manuscripts, view PGP data, and access FJMS metadata without internet |
| **Value Proposition** | Researchers at conferences, in archives, or locations with limited internet can still browse |
| **Complexity** | Low (natural consequence of TS-1 + existing sidecars) |
| **Dependencies** | TS-1 |

**Offline Browsing Matrix:**

| Workflow | Before v6.0.0 | After v6.0.0 | Notes |
|----------|---------------|--------------|-------|
| Search by keyword/shelfmark | Offline (Tantivy) | Offline | Already works |
| View manuscript metadata | Offline (libraries.csv) | Offline | Already works |
| View PGP document data | **ONLINE** (Supabase) | **Offline** | Migration target |
| View PGP scholarly editions | **ONLINE** (Supabase) | **Offline** | Migration target |
| View FJMS domains/joins/catalog | Offline (sidecar) | Offline | Already works |
| View FJMS bibliography | Offline (sidecar) | Offline | Already works |
| View FJMS catalog descriptions | N/A | **Offline** | New feature |
| View manuscript images | ONLINE (IIIF) | ONLINE | Inherently requires network |
| Submit corrections | ONLINE (Supabase) | ONLINE | Community feature |
| Save to lists | ONLINE (Supabase) | ONLINE | Community feature |
| Post comments | ONLINE (Supabase) | ONLINE | Community feature |

After v6.0.0, only image viewing (requires IIIF APIs) and community features (require shared state) need internet. All reference data browsing is fully local.

### D-4: Faster PGP Queries (Latency Improvement)

| Aspect | Detail |
|--------|--------|
| **What** | Local SQLite queries instead of Supabase HTTP roundtrips |
| **Value Proposition** | Immediate responsiveness improvement. Supabase queries incur ~50-200ms network latency per call. SQLite queries complete in <1ms |
| **Complexity** | None (free benefit of migration) |
| **Dependencies** | TS-1 |
| **Notes** | Particularly impactful for: batch `get_sys_ids_with_transcriptions()` (called per search), `get_all_sources_for_fragment()` (called per manuscript view), `get_fragments_by_tag()` (called per tag search) |

### D-5: FJMS Bibliography Transcription Indicators

| Aspect | Detail |
|--------|--------|
| **What** | Show when FJMS bibliography indicates published transcriptions exist (Full/Partial badge) |
| **Value Proposition** | 14,634 manuscripts have "Full transcription" refs, 37,521 have "Partial" refs in FJMS bibliography. Helps researchers find published editions |
| **Complexity** | Low |
| **Dependencies** | Uses existing FJMS bib data (already in sidecar) |
| **Notes** | Top sources: Tarbiz (907 MSS), "In The Kingdom of Ishmael" (863), Ginzei Kedem (634), Gil Palestine (618). These indicate published scholarly works with transcriptions |

### D-6: Supabase Cost Reduction

| Aspect | Detail |
|--------|--------|
| **What** | Remove ~104K read-only rows from cloud database |
| **Value Proposition** | Reduces Supabase storage, bandwidth, and RLS evaluation overhead |
| **Complexity** | None (consequence of TS-9) |
| **Dependencies** | TS-9 |

---

## Anti-Features

Features to explicitly NOT build.

### AF-1: Line-by-Line FJMS Transcription Import

| Anti-Feature | Why Avoid | What to Do Instead |
|--------------|-----------|-------------------|
| Importing `dbo_UnitTranscription` files (56K external file references in FIST.db) as inline text | These are references to proprietary transcription files, not inline text in the database. Extracting 56K files requires separate data pipeline with copyright considerations | Show `dbo_UnitFullText` catalog descriptions as scholarly content. Use bibliography `TranscriptionType` field to indicate when published transcriptions exist |

### AF-2: Real-Time Supabase-to-SQLite Sync

| Anti-Feature | Why Avoid | What to Do Instead |
|--------------|-----------|-------------------|
| Real-time CDC/sync between Supabase and SQLite | PGP reference data changes infrequently (monthly at most). Real-time sync adds massive complexity (PowerSync, RxDB) for negligible benefit | Version the sidecar in `meta` table. Re-export on demand. Distribute with releases |

### AF-3: Fuzzy Content Deduplication Between PGP and FJMS

| Anti-Feature | Why Avoid | What to Do Instead |
|--------------|-----------|-------------------|
| Text similarity matching between PGP editions and FJMS catalog descriptions | These are fundamentally different content types. PGP editions are line-by-line transcriptions. FJMS catalog descriptions are scholarly identifications and physical descriptions. They complement, not duplicate | Show both with clear provenance labels in separate sections of version selector |

### AF-4: Automatic PGP GitHub Sync

| Anti-Feature | Why Avoid | What to Do Instead |
|--------------|-----------|-------------------|
| Automated pipeline to pull PGP data from GitHub and rebuild sidecar | Out of scope per project definition. Manual refresh sufficient given update frequency | Document export process. Export on demand before releases |

### AF-5: Migrating libraries.csv to SQLite

| Anti-Feature | Why Avoid | What to Do Instead |
|--------------|-----------|-------------------|
| Moving 217K manuscript records from CSV to SQLite | High refactoring risk (loaded in genizah_core.py, touched by many code paths). No user-visible benefit | Keep libraries.csv as-is |

### AF-6: Bidirectional Editing in Sidecar

| Anti-Feature | Why Avoid | What to Do Instead |
|--------------|-----------|-------------------|
| Making PGP sidecar writable for corrections or edits | SQLite sidecars are read-only reference data. Community features need shared state and RLS | Keep sidecar read-only (`?mode=ro`). User corrections continue through Supabase |

### AF-7: FJMS Full-Text Search via Tantivy

| Anti-Feature | Why Avoid | What to Do Instead |
|--------------|-----------|-------------------|
| Indexing FJMS catalog descriptions in Tantivy for search | Tantivy index architecture for transcriptions deferred since Phase 13 (build too slow for desktop) | Display FJMS texts in version selector. Search is separate milestone scope |

### AF-8: FTS5 Catalog Search UI

| Anti-Feature | Why Avoid | What to Do Instead |
|--------------|-----------|-------------------|
| Building UI for the FTS5 full-text search schema in FJMS sidecar | Schema was included in v5.8.0 but UI needs separate UX design | Keep FTS5 index in sidecar for future milestone |

---

## Feature Dependencies

```
TS-1 (PGP Sidecar Export) --> TS-2 (Rewrite document_service.py)
TS-2 --> TS-3 (Version Selector Works)
TS-2 --> TS-4 (Search Result Indicators)
TS-2 --> TS-5 (Tag-Based Search)
TS-2 --> TS-6 (Footnotes Display)
TS-2 --> TS-7 (Multi-Fragment Navigation)
TS-2 --> TS-8 (Graceful Degradation)
All TS verified --> TS-9 (Remove Supabase Tables)

D-1 (FJMS Full Texts Export) --> D-2 (FJMS Source Badges)
D-1 is independent of TS-1 (can proceed in parallel)

TS-1 naturally enables D-3 (Offline Browsing)
TS-1 naturally enables D-4 (Faster Queries)
D-5 (Bib Indicators) uses existing sidecar data (standalone)
TS-9 enables D-6 (Cost Reduction)
```

**Critical path:** TS-1 -> TS-2 -> [TS-3..TS-8 verification] -> TS-9
**Parallel track:** D-1 -> D-2 (FJMS full texts)

---

## FJMS Version Selector Integration Design

### How FJMS Catalog Descriptions Should Appear

Current version selector structure:
```
[PGP Editions]
  verified_icon  by Scholar Name 1
  verified_icon  by Scholar Name 2
[Translations]
  translate_icon  Hebrew Translation by Scholar
  translate_icon  English Translation by Scholar
---separator---
V0.8 (Original)
[User Corrections]
  user_icon  by User Name (date)
[My Pending Corrections]
  schedule_icon  Pending (status)
```

Proposed structure with FJMS:
```
[PGP Editions]
  verified_icon  by Scholar Name 1
  verified_icon  by Scholar Name 2
[Translations]
  translate_icon  Hebrew Translation by Scholar
  translate_icon  English Translation by Scholar
---separator---
[FJMS Catalog]  (purple badge)
  description_icon  Scholarly Description
---separator---
V0.8 (Original)
[User Corrections]
  user_icon  by User Name (date)
[My Pending Corrections]
  schedule_icon  Pending (status)
```

**Key UX decisions:**
1. FJMS catalog descriptions appear AFTER PGP editions/translations but BEFORE V0.8
2. Purple badge (consistent with FJMS visual language throughout app)
3. Label as "FJMS Catalog" or "Scholarly Description", NOT "FJMS Transcription"
4. When selected, display the catalog text with appropriate direction
5. If FJMS description is the ONLY scholarly content (no PGP), auto-select as default over V0.8

### Content Display Considerations

FJMS catalog descriptions are bilingual (Hebrew + English) and structured differently from PGP editions:
- They describe WHAT the manuscript contains (content identification)
- They provide physical description (material, size, condition)
- They reference published editions and scholarly discussions
- Most are under 500 chars; only 3,640 exceed 500 chars

Display recommendation:
- Show preferred-language version based on UI language setting
- If only one language available, show that
- Treat as single block of text (no recto/verso splitting -- these are descriptions, not page-by-page transcriptions)

---

## Deduplication UX Expectations

### What Researchers Expect

Based on PGP's own handling of multiple sources, and GenizahSearch's existing version selector pattern:

1. **Show all available sources.** In Genizah studies, different scholars offer different readings. Multiple sources is a feature, not a bug.

2. **Clear provenance labeling.** Each source attributed to its origin (PGP scholar name, FJMS catalog, V0.8 auto-transcription, user correction).

3. **Visual distinction by source type.** Existing pattern works: green=PGP verified, blue=translations, purple=FJMS, grey=V0.8, amber=pending.

4. **Do NOT deduplicate across projects.** A PGP edition (line-by-line manuscript text) and an FJMS catalog entry (scholarly description of what the manuscript contains) serve different purposes. Researchers want both.

5. **DO deduplicate within FJMS.** Multiple FJMS catalog records for the same manuscript with identical content should merge. The existing `merge_catalog_records()` in `fjms_service.py` already handles this with seen-set deduplication.

### Data Overlap Statistics

| Data Source | Records | Distinct AlmaIds | Content Type |
|-------------|---------|-----------------|--------------|
| PGP document_sources | 9,364 | ~25K (via 36K fragment links) | Line-by-line scholarly editions and translations |
| FJMS UnitFullText | 65,332 | ~85,313 | Catalog descriptions (identification + physical + scholarly) |
| FJMS TextualFrame (in catalog) | 96,419 | 57,563 | Brief content identifications (e.g., "Genesis 39:20-41:8") |

The overlap set (manuscripts with BOTH PGP sources and FJMS descriptions) is significant but content types are complementary, not duplicative.

---

## MVP Recommendation

### Phase 1: PGP Sidecar Foundation

1. **TS-1: PGP Sidecar Export** -- Foundation everything depends on
2. **TS-2: Rewrite document_service.py** -- Enables all callers without Supabase
3. **TS-3 through TS-8: Verification** -- Every existing PGP feature must work

### Phase 2: FJMS Full Texts (parallel track)

4. **D-1: FJMS Full Texts Export** -- Add `full_texts` table to `fjms_enrichment.db`
5. **D-2: FJMS Source Badges** -- Purple badge in version selector

### Phase 3: Cleanup and Cutover

6. **TS-9: Remove Supabase Tables** -- Only after Phase 1+2 fully verified
7. **D-3: Offline Browsing** -- Natural consequence, verify and document

### Defer to Post-MVP

- **D-5: Bibliography Transcription Indicators** -- Uses existing data, nice-to-have
- **AF-7: FJMS Full-Text Search** -- Separate milestone scope
- **AF-8: FTS5 Catalog Search UI** -- Separate UX design project

---

## Sources

### Codebase Analysis (HIGH confidence)
- `shared/document_service.py` -- 12 public functions, 8 need SQLite rewrite
- `shared/fjms_service.py` -- proven sidecar pattern (singleton, read-only, thread-safe, graceful degradation)
- `shared/nli_crossref_service.py` -- proven sidecar pattern
- `web/components/version_selector.py` -- 440 lines, consumes `all_sources` list
- `scripts/export_fist_enrichment.py` -- proven export script pattern (762K rows exported)
- `migrations/create_document_sources_table.sql` -- Supabase document_sources schema
- `migrations/add_pgp_documents_tables.sql` -- documents + fragments schema
- `migrations/create_footnotes_table.sql` -- footnotes schema

### Data Analysis (HIGH confidence)
- Supabase: documents (35,839), document_sources (9,364), document_footnotes (22,757), document_fragments (36,155)
- FIST.db `dbo_UnitFullText`: 65,332 rows, ~85K AlmaIds, ~56K with >100 chars, bilingual
- FIST.db `dbo_UnitTranscription`: 56,203 file references (NOT inline text -- external files)
- FJMS bibliography `TranscriptionType`: 14,634 AlmaIds="Full", 37,521="Partial"
- FJMS catalog `TextualFrameEng`: 96,419 rows, 57,563 AlmaIds

### External Research (MEDIUM confidence)
- [Princeton Geniza Project](https://geniza.princeton.edu/en/) -- PGP handles multiple editions per document with scholar attribution
- [PGP FAQ](https://geniza.princeton.edu/en/about/faq/) -- Scholarship records track transcriber and publication source
- [Friedberg Research Platform](https://pr.genizah.org/TheResearchPlatform_New.aspx) -- 15K+ transcriptions, catalog entries, bibliography in FGP
- [Friedberg Genizah Project](https://en.wikipedia.org/wiki/Friedberg_Geniza_Project) -- 739K+ images, 104K+ transcriptions across platforms
- [Supabase Offline Discussion](https://github.com/orgs/supabase/discussions/357) -- Real-time sync is complex; versioned static exports are simpler for read-only data
