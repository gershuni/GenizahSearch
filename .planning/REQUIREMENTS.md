# Requirements: GenizahSearch

**Defined:** 2026-03-14
**Core Value:** Researchers can find what they need in the Genizah corpus

## v7.0.0 Requirements

Requirements for v7.0.0 Transcription Search. Each maps to roadmap phases.

### Transcription Import

- [ ] **TIMP-01**: FJMS ~30K transcriptions imported into fjms_enrichment.db sidecar with proper schema
- [ ] **TIMP-02**: FJMS transcriptions accessible via shared service layer (both apps)

### Transcription Indexing & Search

- [ ] **TSRCH-01**: PGP transcription text (editions/translations) indexed in Tantivy alongside OCR text
- [ ] **TSRCH-02**: FJMS transcription text indexed in Tantivy alongside OCR text
- [ ] **TSRCH-03**: User corrections text indexed in Tantivy
- [ ] **TSRCH-04**: Search results show badge/icon indicating whether hit came from OCR or transcription source
- [ ] **TSRCH-05**: Transcription hits ranked higher than OCR-only hits in search results
- [ ] **TSRCH-06**: Transcription search works with all search modes (regular, Responsa)
- [ ] **TSRCH-07**: Transcription search works in both web and desktop apps

### Index Distribution

- [ ] **IDIST-01**: Pre-built Tantivy index (with transcription fields) available for download from web server
- [ ] **IDIST-02**: Desktop app can detect and upgrade existing index to include transcription data
- [ ] **IDIST-03**: Fresh desktop installs receive complete index with transcriptions
- [ ] **IDIST-04**: Index update mechanism handles existing users with older index format

### Catalog Search (Carried from v6.5.0)

- [ ] **CAT-01**: User can search catalog data with free-text across all fields (titles, authors, dates, descriptions, identifications)
- [ ] **CAT-02**: User can filter catalog search by specific fields (author name, work title, date range)
- [ ] **CAT-03**: Catalog search is accessible as a mode/option within the main search interface
- [ ] **CAT-04**: Catalog search results link to manuscript browse view
- [ ] **CAT-05**: Catalog search works in both web and desktop apps

## Future Requirements

Deferred to future releases. Tracked but not in current roadmap.

### Deferred

- **NLI-REL-01**: NLI PartOf relationships displayed in UI (424K records, service method exists)
- **NLI-REL-02**: NLI See cross-references displayed in UI (19K records, service method exists)
- **NLI-REL-03**: NLI BifolioWith pairs displayed in UI (23K records, service method exists)
- **FJMS-VER-01**: FJMS full texts as selectable version source in version selector

## Out of Scope

Explicitly excluded. Documented to prevent scope creep.

| Feature | Reason |
|---------|--------|
| PGP people/places integration | Complexity too high, defer |
| Map-based geographic browse | Requires places.csv + UI work, defer |
| Build transcription editor | Link to external tools instead |
| Real-time index updates for new corrections | Index is a periodic snapshot; live correction indexing deferred |
| FGP direct image access | FGPImageNumberId is not equal to IIIF FL ID |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| TIMP-01 | Phase 47 | Pending |
| TIMP-02 | Phase 47 | Pending |
| TSRCH-01 | Phase 48 | Pending |
| TSRCH-02 | Phase 48 | Pending |
| TSRCH-03 | Phase 48 | Pending |
| TSRCH-04 | Phase 48 | Pending |
| TSRCH-05 | Phase 48 | Pending |
| TSRCH-06 | Phase 48 | Pending |
| TSRCH-07 | Phase 48 | Pending |
| IDIST-01 | Phase 49 | Pending |
| IDIST-02 | Phase 49 | Pending |
| IDIST-03 | Phase 49 | Pending |
| IDIST-04 | Phase 49 | Pending |
| CAT-01 | TBD | Pending |
| CAT-02 | TBD | Pending |
| CAT-03 | TBD | Pending |
| CAT-04 | TBD | Pending |
| CAT-05 | TBD | Pending |

**Coverage:**
- v7.0.0 requirements: 18 total
- Mapped to phases: 13
- Unmapped (CAT-*): 5

---
*Requirements defined: 2026-03-14*
*Last updated: 2026-03-14 after v6.5.0 milestone completion*
