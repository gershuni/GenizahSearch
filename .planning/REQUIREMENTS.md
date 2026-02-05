# Requirements: External Data Integration

## v1 Requirements

### Transcription Display (TRANS)

- [ ] **TRANS-01**: User can view PGP transcription on browse page when available
- [ ] **TRANS-02**: User sees transcription source attribution ("Transcription by [scholar]")
- [ ] **TRANS-03**: User can click through to original PGP document page
- [ ] **TRANS-04**: User sees "has transcription" indicator in search results

### Fragment Joins (JOIN)

- [ ] **JOIN-01**: User can see related fragments on browse page (join group display)
- [ ] **JOIN-02**: User can navigate to any joined fragment with one click
- [ ] **JOIN-03**: User sees relationship type (physical join, same composition, etc.)
- [ ] **JOIN-04**: System imports NLI joins (PartOf, BifolioWith from crossreference)
- [ ] **JOIN-05**: Existing pairwise joins continue working (backward compatibility)

### Metadata Enrichment (META)

- [ ] **META-01**: User sees document type (Letter, Legal document, List, etc.) on browse page
- [ ] **META-02**: User sees date information (original and/or inferred) when available
- [ ] **META-03**: User sees English description/summary when available from PGP
- [ ] **META-04**: User sees subject tags from PGP

### Document Entity (DOC)

- [ ] **DOC-01**: Multi-fragment PGP records create document groupings in database
- [ ] **DOC-02**: Single-fragment manuscripts remain unchanged (no document wrapper)
- [ ] **DOC-03**: Document links to all member fragments via sys_id
- [ ] **DOC-04**: PGP transcription stored at document level (not fragment level)

### Data Import (IMP)

- [ ] **IMP-01**: Import 9,364 PGP transcriptions from transcriptions_linked.csv
- [ ] **IMP-02**: Import PGP document metadata (type, tags, dates, descriptions)
- [ ] **IMP-03**: Import NLI join relationships (~424K PartOf/BifolioWith records)
- [ ] **IMP-04**: Handle Oxford codicological parts correctly (may have separate metadata)

---

## v2 Requirements (Deferred)

- Transcription full-text search in Tantivy (GOAL - core value for future)
- Search toggle: HTR vs PGP transcriptions
- Virtual document reconstruction view (joined fragments as single viewer)
- Network visualization of join relationships
- User-contributed document groupings
- PGP people/places integration

---

## Out of Scope

- Build transcription editor — link to external tools instead
- Build join detection AI — import from NLI/PGP instead
- Crowdsourcing transcription — link to Scribes of Cairo Geniza
- Bibliography management — not core value
- HTR/OCR functionality — use Transkribus if needed later

---

## Constraints

- **Oxford Parts**: Codicological parts (oxford_part_id) have separate metadata from Oxford. Usually one document but not always. Must handle gracefully.
- **Backward Compatibility**: Existing URLs, user lists, pairwise joins must continue working
- **Attribution**: PGP transcriptions require scholar attribution per their license
- **Data Volume**: 41K PGP docs manageable; 815K NLI records use for joins only (not full indexing)

---

## Traceability

| REQ-ID | Phase | Status |
|--------|-------|--------|
| TRANS-01 | TBD | Pending |
| TRANS-02 | TBD | Pending |
| TRANS-03 | TBD | Pending |
| TRANS-04 | TBD | Pending |
| JOIN-01 | TBD | Pending |
| JOIN-02 | TBD | Pending |
| JOIN-03 | TBD | Pending |
| JOIN-04 | TBD | Pending |
| JOIN-05 | TBD | Pending |
| META-01 | TBD | Pending |
| META-02 | TBD | Pending |
| META-03 | TBD | Pending |
| META-04 | TBD | Pending |
| DOC-01 | TBD | Pending |
| DOC-02 | TBD | Pending |
| DOC-03 | TBD | Pending |
| DOC-04 | TBD | Pending |
| IMP-01 | TBD | Pending |
| IMP-02 | TBD | Pending |
| IMP-03 | TBD | Pending |
| IMP-04 | TBD | Pending |

---

*Generated: February 5, 2026*
