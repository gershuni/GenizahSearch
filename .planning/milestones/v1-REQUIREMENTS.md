# Requirements Archive: v1 External Data Integration

**Archived:** 2026-02-07
**Status:** SHIPPED

This is the archived requirements specification for v1.
For current requirements, see `.planning/REQUIREMENTS.md` (created for next milestone).

---

## v1 Requirements

### Transcription Display (TRANS)

- [x] **TRANS-01**: User can view PGP transcription on browse page when available
- [x] **TRANS-02**: User sees transcription source attribution ("Transcription by [scholar]")
- [x] **TRANS-03**: User can click through to original PGP document page
- [x] **TRANS-04**: User sees "has transcription" indicator in search results

### Fragment Joins (JOIN)

- [x] **JOIN-01**: User can see related fragments on browse page (join group display)
- [x] **JOIN-02**: User can navigate to any joined fragment with one click
- [x] **JOIN-03**: User sees relationship type (physical join, same composition, etc.)
- [x] **JOIN-04**: System imports PGP joins (parsed from multi-fragment shelfmarks)
- [x] **JOIN-05**: Existing pairwise joins continue working (backward compatibility)

### Metadata Enrichment (META)

- [x] **META-01**: User sees document type (Letter, Legal document, List, etc.) on browse page
- [x] **META-02**: User sees date information (original and/or inferred) when available
- [x] **META-03**: User sees English description/summary when available from PGP
- [x] **META-04**: User sees subject tags from PGP

### Document Entity (DOC)

- [x] **DOC-01**: Multi-fragment PGP records create document groupings in database
- [x] **DOC-02**: Single-fragment manuscripts remain unchanged (no document wrapper)
- [x] **DOC-03**: Document links to all member fragments via sys_id
- [x] **DOC-04**: PGP transcription stored at document level (not fragment level)

### Data Import (IMP)

- [x] **IMP-01**: Import 9,364 PGP transcriptions from transcriptions_linked.csv
- [x] **IMP-02**: Import PGP document metadata (type, tags, dates, descriptions)
- [x] **IMP-03**: Parse PGP multi-fragment shelfmarks to create join groups
- [x] **IMP-04**: Handle Oxford codicological parts correctly (may have separate metadata)

---

## v2 Requirements (Deferred)

- Transcription full-text search in Tantivy (GOAL - core value for future)
- Search toggle: HTR vs PGP transcriptions
- Virtual document reconstruction view (joined fragments as single viewer)
- Network visualization of join relationships
- User-contributed document groupings
- PGP people/places integration
- NLI BifolioWith import (306K image-level bifolio pairs)

---

## Out of Scope

- Build transcription editor — link to external tools instead
- Build join detection AI — import from NLI/PGP instead
- Crowdsourcing transcription — link to Scribes of Cairo Geniza
- Bibliography management — not core value
- HTR/OCR functionality — use Transkribus if needed later

---

## Traceability

| REQ-ID | Phase | Status |
|--------|-------|--------|
| TRANS-01 | Phase 4 | Complete |
| TRANS-02 | Phase 4 | Complete |
| TRANS-03 | Phase 4 (plan 02) | Complete |
| TRANS-04 | Phase 5 | Complete |
| JOIN-01 | Phase 7 | Complete |
| JOIN-02 | Phase 7 | Complete |
| JOIN-03 | Phase 7 | Complete |
| JOIN-04 | Phase 2 | Complete |
| JOIN-05 | Phase 7 | Complete |
| META-01 | Phase 6 | Complete |
| META-02 | Phase 6 | Complete |
| META-03 | Phase 6 | Complete |
| META-04 | Phase 6 | Complete |
| DOC-01 | Phase 1 | Complete |
| DOC-02 | Phase 1 | Complete |
| DOC-03 | Phase 1 | Complete |
| DOC-04 | Phase 3 | Complete |
| IMP-01 | Phase 2 | Complete |
| IMP-02 | Phase 2 | Complete |
| IMP-03 | Phase 2 | Complete |
| IMP-04 | Phase 2 | Complete |

---

## Milestone Summary

**Shipped:** 21 of 21 v1 requirements
**Adjusted:**
- TRANS-03 initially failed verification (no clickable link) — resolved by plan 04-02
- IMP-01 clarified: 9,364 records deduplicate to 7,090 unique documents by pgpid
**Dropped:** None

---
*Archived: 2026-02-07 as part of v1 milestone completion*
