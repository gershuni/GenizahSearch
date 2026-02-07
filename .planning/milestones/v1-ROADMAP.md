# Milestone v1: External Data Integration

**Status:** SHIPPED 2026-02-07
**Phases:** 1-7 (plus 2 inserted: 4.1, 4.2)
**Total Plans:** 18

## Overview

This roadmap transforms GenizahSearch from a manuscript browser into a research platform with scholarly context by integrating external data from Princeton Geniza Project (PGP). The journey begins with establishing a document entity layer in the database, imports PGP transcriptions, metadata, and join relationships (parsed from multi-fragment shelfmarks), builds service infrastructure, and culminates in UI enhancements that surface transcriptions, metadata, and fragment relationships to users. Throughout, backward compatibility with existing features is maintained.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [x] **Phase 1: Database Schema** - Create document and fragment linkage tables in Supabase
- [x] **Phase 2: PGP Data Import** - Import transcriptions, metadata, and parse joins from multi-fragment shelfmarks
- [x] **Phase 3: Document Service** - Build service layer for document-fragment operations
- [x] **Phase 4: Transcription Display** - Show PGP transcriptions in browse page version selector
- [x] **Phase 4.1: Separate Translations** - Filter Digital Translations from transcription display (INSERTED)
- [x] **Phase 4.2: Multi-Source Import** - Import all transcriptions and translations per document (INSERTED)
- [x] **Phase 5: Search Integration** - Indicate transcription availability in search results
- [x] **Phase 6: Metadata Display** - Show document type, dates, descriptions, and tags on browse page
- [x] **Phase 7: Joins UI** - Display and navigate fragment relationships on browse page

## Phase Details

### Phase 1: Database Schema
**Goal**: Document entity infrastructure exists in Supabase to support multi-fragment PGP documents
**Depends on**: Nothing (foundation phase)
**Requirements**: DOC-01, DOC-02, DOC-03
**Success Criteria** (what must be TRUE):
  1. `documents` table exists with pgpid, metadata fields, and created_at
  2. `document_fragments` table exists linking documents to sys_ids with sequence ordering
  3. Single-fragment manuscripts have no document record (DOC-02 preserved)
  4. Database schema supports querying "which document contains this sys_id"
**Plans**: 1 plan

Plans:
- [x] 01-01-PLAN.md — Create Supabase migration with documents and document_fragments tables

### Phase 2: PGP Data Import
**Goal**: All PGP transcriptions, metadata, and joins are loaded and linked to GenizahSearch records
**Depends on**: Phase 1 (schema must exist)
**Requirements**: IMP-01, IMP-02, IMP-03, IMP-04, JOIN-04
**Success Criteria** (what must be TRUE):
  1. 9,364 transcriptions from transcriptions_linked.csv are imported with sys_id links
  2. Document metadata (type, tags, dates, descriptions) from documents.csv is imported
  3. Multi-fragment shelfmarks (e.g., "T-S 13J35.3 + AIU VII.A.23") are parsed to create join groups
  4. Oxford codicological parts are handled correctly (separate metadata where needed)
  5. Import script is repeatable (can re-run without duplicates)
**Plans**: 2 plans

Plans:
- [x] 02-01-PLAN.md — Add page_info column to document_fragments for recto/verso storage
- [x] 02-02-PLAN.md — Build PGP import script with two-pass architecture and multi-fragment parsing

### Phase 3: Document Service
**Goal**: Service layer enables all downstream features to access document-fragment relationships
**Depends on**: Phase 2 (data must be imported)
**Requirements**: DOC-04
**Success Criteria** (what must be TRUE):
  1. `get_document_for_fragment(sys_id)` returns document if fragment is part of one
  2. `get_fragments_for_document(pgpid)` returns all linked fragments with ordering
  3. `get_transcription_for_document(pgpid)` returns PGP transcription text
  4. Service handles fragments not in any document gracefully (returns None)
**Plans**: 1 plan

Plans:
- [x] 03-01-PLAN.md — Create document service module with Supabase queries

### Phase 4: Transcription Display
**Goal**: Users can view PGP transcriptions on browse page with proper attribution
**Depends on**: Phase 3 (service layer required)
**Requirements**: TRANS-01, TRANS-02, TRANS-03
**Success Criteria** (what must be TRUE):
  1. When viewing a fragment with PGP transcription, user sees it in version selector
  2. Transcription shows source attribution ("Transcription by [scholar name]")
  3. User can click link to view original document on PGP website
  4. PGP transcription appears as primary version when available (above HTR versions)
  5. PGP transcription displays correctly per page (recto/verso split)
**Plans**: 3 plans

Plans:
- [x] 04-01-PLAN.md — Integrate PGP transcriptions into version selector with attribution
- [x] 04-02-PLAN.md — Add clickable "View on PGP" link (gap closure for TRANS-03)
- [x] 04-03-PLAN.md — Split transcription by recto/verso markers (UAT gap closure)

### Phase 4.1: Separate Translations from Transcriptions (INSERTED)
**Goal**: Filter Digital Translations from transcription display; optionally offer as separate version
**Depends on**: Phase 4 (transcription display complete)
**Requirements**: UAT Gap 3 (translations mixed with transcriptions)
**Success Criteria** (what must be TRUE):
  1. "PGP Transcription" shows only Digital Editions (original Hebrew/Aramaic text)
  2. Digital Translations (1,696 records) are either filtered out OR shown as separate "PGP Translation" option
  3. Users can distinguish between transcription and translation content
**Plans**: 1 plan

Plans:
- [x] 04.1-01-PLAN.md — Add doc_relation column and filter translations in document service

### Phase 4.2: Multi-Source Import (INSERTED)
**Goal**: Import all transcriptions and translations per document, with UI to choose between sources
**Depends on**: Phase 4.1 (schema already has doc_relation)
**Requirements**: UAT findings - documents can have multiple transcription sources and translations
**Success Criteria** (what must be TRUE):
  1. `document_sources` table stores all transcriptions and translations per pgpid
  2. Each source has: content, scholar attribution, doc_relation (Edition/Translation), language
  3. Documents table keeps primary transcription (last Digital Edition) for backward compatibility
  4. Version selector shows all available transcriptions with scholar names
  5. Translations shown as separate menu section (Hebrew and English distinguished)
  6. User can switch between different scholars' transcriptions
**Plans**: 3 plans

Plans:
- [x] 04.2-01-PLAN.md — Schema migration for document_sources table
- [x] 04.2-02-PLAN.md — Reimport all sources from CSV
- [x] 04.2-03-PLAN.md — Update version selector to show multiple sources

### Phase 5: Search Integration
**Goal**: Users can identify which search results have PGP transcriptions available
**Depends on**: Phase 3 (service layer required)
**Requirements**: TRANS-04
**Success Criteria** (what must be TRUE):
  1. Search results show indicator icon/badge when PGP transcription exists
  2. Indicator is visible without clicking into the result
  3. Performance remains acceptable (indicator lookup does not slow search)
**Plans**: 1 plan

Plans:
- [x] 05-01-PLAN.md — Add batch transcription lookup and indicator icon to search results

### Phase 6: Metadata Display
**Goal**: Users see PGP document metadata (type, dates, description, tags) on browse page
**Depends on**: Phase 3 (service layer required)
**Requirements**: META-01, META-02, META-03, META-04
**Success Criteria** (what must be TRUE):
  1. Document type (Letter, Legal document, List, etc.) displays on browse page
  2. Date information (original and/or inferred) displays when available
  3. English description/summary from PGP displays when available
  4. Subject tags from PGP display and are clickable/browseable
**Plans**: 3 plans

Plans:
- [x] 06-01-PLAN.md — Add missing metadata columns to DB and update import script
- [x] 06-02-PLAN.md — Build PGP metadata display section in browse page metadata panel
- [x] 06-03-PLAN.md — Implement tag-based search (service + route + results rendering)

### Phase 7: Joins UI
**Goal**: Users can see and navigate fragment relationships on browse page
**Depends on**: Phase 3 (service layer required), Phase 2 (joins imported)
**Requirements**: JOIN-01, JOIN-02, JOIN-03, JOIN-05
**Success Criteria** (what must be TRUE):
  1. Browse page shows "Related Fragments" panel when joins exist
  2. User can click any related fragment to navigate to it
  3. Relationship type is displayed (physical join, same composition)
  4. Existing pairwise joins from current system continue working unchanged
  5. PGP joins and existing joins are unified in display
**Plans**: 2 plans

Plans:
- [x] 07-01-PLAN.md — Extend joins data layer to unify PGP + user joins and thread pgpid
- [x] 07-02-PLAN.md — Add inline Related Fragments panel to metadata sidebar with translations

## Progress

**Execution Order:**
Phases execute in numeric order: 1 -> 2 -> 3 -> 4 -> 4.1 -> 4.2 -> 5 -> 6 -> 7

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Database Schema | 1/1 | Complete | 2026-02-05 |
| 2. PGP Data Import | 2/2 | Complete | 2026-02-05 |
| 3. Document Service | 1/1 | Complete | 2026-02-05 |
| 4. Transcription Display | 3/3 | Complete | 2026-02-05 |
| 4.1 Separate Translations | 1/1 | Complete | 2026-02-06 |
| 4.2 Multi-Source Import | 3/3 | Complete | 2026-02-06 |
| 5. Search Integration | 1/1 | Complete | 2026-02-06 |
| 6. Metadata Display | 3/3 | Complete | 2026-02-06 |
| 7. Joins UI | 2/2 | Complete | 2026-02-07 |

---

## Milestone Summary

**Decimal Phases:**
- Phase 4.1: Separate Translations (inserted after Phase 4 for UAT gap — translations mixed with transcriptions)
- Phase 4.2: Multi-Source Import (inserted after Phase 4.1 — documents with multiple scholars' editions)

**Key Decisions:**
- pgpid as natural PRIMARY KEY (matches PGP data source)
- Document entity for joins only (single-fragment manuscripts unchanged)
- PGP transcription as version source (extends existing selector)
- Two-pass import pattern (documents first, then FK-dependent fragments)
- Service layer pattern isolates query details from UI
- Batch lookup for search indicators (not N+1)
- JSONB tags with GIN index for flexible filtering
- filter('tags', 'cs', json.dumps()) workaround for Supabase client JSONB bug
- Lazy import pattern to avoid circular dependencies in joins_panel

**Issues Resolved:**
- TRANS-03 gap: PGP link missing from version selector (fixed in plan 04-02)
- UAT Gap 2: Recto/verso headers stripped (minor, deferred)
- UAT Gap 3: Translations mixed with transcriptions (fixed in Phase 4.1)
- Non-browseable fragments in tag search (429/7218 filtered out)

**Issues Deferred:**
- Recto/verso section headers stripped during parsing (minor cosmetic)
- Phase 6 missing formal VERIFICATION.md
- No integration tests for E2E flows
- TODO in document_service.py:253 (multi-fragment enhancement)

**Technical Debt Incurred:**
- document_service.py lives in web/ — needs extraction to shared location for desktop app access
- All PGP UI features are web-only (desktop app has no PGP integration)

---

_For current project status, see .planning/PROJECT.md_
_Archived: 2026-02-07_
