# Research Summary: Document-Level Entity Integration

**Domain:** Cairo Genizah manuscript research platform (GenizahSearch)
**Researched:** February 5, 2026
**Overall confidence:** HIGH

---

## Executive Summary

GenizahSearch needs to integrate document-level entities from PGP (Princeton Geniza Project) while preserving its existing page-level architecture. The core challenge is that PGP organizes per-document (PGPID spans multiple fragments) while GenizahSearch organizes per-sys_id (one record per fragment).

The recommended approach introduces a **Document virtual entity layer** that groups fragments without restructuring the core data model. This creates a clean bridge between the page-level sys_id model and document-level PGP data, enabling:

1. Multi-fragment document viewing
2. PGP transcription as a version source
3. Enhanced joins with document context
4. Searchable transcriptions indexed alongside HTR content

The architecture maintains full backward compatibility - existing URLs, lists, and workflows continue unchanged.

---

## Key Findings

**Architecture:** Implement `documents` and `document_fragments` tables in Supabase as a linking layer. Documents become virtual entities computed from fragment linkages, avoiding restructuring of the core sys_id model.

**Data Model:** PGP provides 7,090 unique documents with 9,364 linked transcriptions (96.5% match rate). Each document links to 1-N fragments via sys_id. Single-fragment manuscripts continue to work unchanged.

**Version Selector:** Extend existing V0.8/V0.7/user model to include 'pgp' as a fourth source type. PGP transcriptions shown with scholar attribution and link to original.

**Tantivy Strategy:** Unified index with new `transcription` field. Search can target HTR content, PGP transcriptions, or both. Avoids complexity of dual-index architecture.

**Critical Pitfall:** Avoid restructuring sys_id to include document references. This would break all existing URLs, user lists, and external references.

---

## Implications for Roadmap

Based on research, suggested phase structure:

### Phase 1: Database Schema & Data Import
- Create `documents` and `document_fragments` tables in Supabase
- Import 7,090 PGP documents with metadata
- Link 9,364 transcriptions via sys_id from `transcriptions_linked.csv`
- **Rationale:** Data foundation must exist before any UI or search work
- **Risk:** LOW - straightforward data import with proven matching

### Phase 2: Document Service Layer
- Implement `DocumentService` class with fragment linkage methods
- Add `get_document_for_fragment(sys_id)` lookup
- Add `get_fragments_for_document(pgpid)` aggregation
- **Rationale:** Service layer enables all downstream features
- **Risk:** LOW - standard CRUD patterns

### Phase 3: Tantivy Index Enhancement
- Extend schema with `transcription`, `transcription_source`, `pgpid` fields
- Rebuild index including PGP data
- Add search toggle: "Search in transcriptions"
- **Rationale:** Search is high-value feature, depends on Phase 1
- **Risk:** MEDIUM - index rebuild time (~5-10 minutes)

### Phase 4: Version Selector Integration
- Add 'pgp' to source types
- Display PGP transcription with attribution
- Link to original PGPID on PGP website
- **Rationale:** Core user interaction, depends on Phase 2
- **Risk:** LOW - clean extension of existing component

### Phase 5: Browse Page Enhancement
- Add "Part of Document" badge when fragment linked
- Show other fragments in document via Joins Panel
- Link to document-level view
- **Rationale:** Progressive disclosure, doesn't require document view complete
- **Risk:** LOW - UI enhancement only

### Phase 6: Document View (Optional)
- Create `/document/{pgpid}` route
- Multi-fragment viewer component
- Joined transcription display
- **Rationale:** Advanced feature, can defer without blocking core value
- **Risk:** MEDIUM - new UX pattern, needs design review

**Phase ordering rationale:**
- Phases 1-2 establish data foundation (no visible change, but unblocks everything)
- Phases 3-4 deliver core value (searchable transcriptions + version selector)
- Phase 5 adds discovery (users learn about document relationships)
- Phase 6 is progressive enhancement (full document view)

---

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Data Model | HIGH | Standard relational pattern, PGP data well-structured, 96.5% match rate proven |
| Document Service | HIGH | Simple CRUD operations on well-defined schema |
| Tantivy Integration | HIGH | Verified Tantivy supports multi-field indexing |
| Version Selector | HIGH | Clean extension of existing 3-source model |
| Browse Enhancement | HIGH | Additive UI change, graceful degradation |
| Document View | MEDIUM | New UX pattern needs design work |
| Performance | MEDIUM | May need caching for document aggregation at scale |

---

## Research Flags for Phases

| Phase | Research Need |
|-------|---------------|
| Phase 3 (Tantivy) | Verify Hebrew text search performance with PGP transcriptions |
| Phase 6 (Document View) | UX research: How do scholars expect to view multi-fragment documents? |

---

## Gaps to Address

1. **NLI integration:** This research focuses on PGP. FIST joins also have document-level structure but different metadata schema. May need unified document model.

2. **User document creation:** Should users be able to create their own document groupings? Current design supports only PGP-sourced documents.

3. **Transcription quality indicators:** PGP transcriptions vary in completeness. May need "transcription coverage" indicator.

---

## Files Created

| File | Purpose |
|------|---------|
| `.planning/research/SUMMARY.md` | This file - executive summary with roadmap implications |
| `.planning/research/ARCHITECTURE.md` | Detailed architecture patterns, component boundaries, data flow |

---

## Ready for Roadmap

Research complete. Key recommendations:
1. Use Document virtual entity layer (not sys_id restructuring)
2. Extend Tantivy unified index (not separate index)
3. Add PGP as version selector source
4. Phase data first, then services, then search, then UI
5. Document view is valuable but can be deferred

Proceeding to roadmap creation.

---

*Research completed: February 5, 2026*
