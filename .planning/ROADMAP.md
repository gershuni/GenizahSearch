# Roadmap: External Data Integration

## Overview

This roadmap transforms GenizahSearch from a manuscript browser into a research platform with scholarly context by integrating external data from Princeton Geniza Project (PGP). The journey begins with establishing a document entity layer in the database, imports PGP transcriptions, metadata, and join relationships (parsed from multi-fragment shelfmarks), builds service infrastructure, and culminates in UI enhancements that surface transcriptions, metadata, and fragment relationships to users. Throughout, backward compatibility with existing features is maintained.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [x] **Phase 1: Database Schema** - Create document and fragment linkage tables in Supabase ✓
- [x] **Phase 2: PGP Data Import** - Import transcriptions, metadata, and parse joins from multi-fragment shelfmarks ✓
- [x] **Phase 3: Document Service** - Build service layer for document-fragment operations ✓
- [x] **Phase 4: Transcription Display** - Show PGP transcriptions in browse page version selector ✓
- [x] **Phase 4.1: Separate Translations** - Filter Digital Translations from transcription display (INSERTED) ✓
- [ ] **Phase 5: Search Integration** - Indicate transcription availability in search results
- [ ] **Phase 6: Metadata Display** - Show document type, dates, descriptions, and tags on browse page
- [ ] **Phase 7: Joins UI** - Display and navigate fragment relationships on browse page

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
- [x] 01-01-PLAN.md — Create Supabase migration with documents and document_fragments tables ✓

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
- [x] 02-01-PLAN.md — Add page_info column to document_fragments for recto/verso storage ✓
- [x] 02-02-PLAN.md — Build PGP import script with two-pass architecture and multi-fragment parsing ✓

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
- [x] 03-01-PLAN.md — Create document service module with Supabase queries ✓

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
- [x] 04-01-PLAN.md — Integrate PGP transcriptions into version selector with attribution ✓
- [x] 04-02-PLAN.md — Add clickable "View on PGP" link (gap closure for TRANS-03) ✓
- [x] 04-03-PLAN.md — Split transcription by recto/verso markers (UAT gap closure) ✓

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
- [x] 04.1-01-PLAN.md — Add doc_relation column and filter translations in document service ✓

### Phase 5: Search Integration
**Goal**: Users can identify which search results have PGP transcriptions available
**Depends on**: Phase 3 (service layer required)
**Requirements**: TRANS-04
**Success Criteria** (what must be TRUE):
  1. Search results show indicator icon/badge when PGP transcription exists
  2. Indicator is visible without clicking into the result
  3. Performance remains acceptable (indicator lookup does not slow search)
**Plans**: TBD

Plans:
- [ ] 05-01: Add transcription indicator to search results

### Phase 6: Metadata Display
**Goal**: Users see PGP document metadata (type, dates, description, tags) on browse page
**Depends on**: Phase 3 (service layer required)
**Requirements**: META-01, META-02, META-03, META-04
**Success Criteria** (what must be TRUE):
  1. Document type (Letter, Legal document, List, etc.) displays on browse page
  2. Date information (original and/or inferred) displays when available
  3. English description/summary from PGP displays when available
  4. Subject tags from PGP display and are clickable/browseable
**Plans**: TBD

Plans:
- [ ] 06-01: Add metadata display component to browse page

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
**Plans**: TBD

Plans:
- [ ] 07-01: Build joins panel component for browse page

## Progress

**Execution Order:**
Phases execute in numeric order: 1 -> 2 -> 3 -> 4 -> 4.1 -> 5 -> 6 -> 7

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Database Schema | 1/1 | ✓ Complete | 2026-02-05 |
| 2. PGP Data Import | 2/2 | ✓ Complete | 2026-02-05 |
| 3. Document Service | 1/1 | ✓ Complete | 2026-02-05 |
| 4. Transcription Display | 3/3 | ✓ Complete | 2026-02-05 |
| 4.1 Separate Translations | 1/1 | ✓ Complete | 2026-02-06 |
| 5. Search Integration | 0/1 | Not started | - |
| 6. Metadata Display | 0/1 | Not started | - |
| 7. Joins UI | 0/1 | Not started | - |

---

*Created: 2026-02-05*
*Updated: 2026-02-06 - Phase 4.1 complete (translations filtered from transcription display)*
