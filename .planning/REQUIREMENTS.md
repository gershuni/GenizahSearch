# Requirements: GenizahSearch

**Defined:** 2026-02-22
**Core Value:** Researchers can find what they need in the Genizah corpus

## v7.0.0 Requirements

Requirements for v7.0.0 Catalog Navigation & Transcription Search. Each maps to roadmap phases.

### Catalog Browse & Navigation

- [ ] **BROWSE-01**: User can browse manuscripts by FJMS domain hierarchy (domain > sub-domain > manuscripts)
- [ ] **BROWSE-02**: User can browse manuscripts by author from catalog data (FJMS and PGP)
- [ ] **BROWSE-03**: User can browse manuscripts by work/title from catalog data
- [ ] **BROWSE-04**: User can combine browse axes (e.g., filter by domain + author simultaneously)
- [ ] **BROWSE-05**: Browse results show manuscript metadata (shelfmark, library, domain, identification)
- [ ] **BROWSE-06**: Catalog browse works in both web and desktop apps

### Catalog Search

- [ ] **CAT-01**: User can search catalog data with free-text across all fields (titles, authors, dates, descriptions, identifications)
- [ ] **CAT-02**: User can filter catalog search by specific fields (author name, work title, date range)
- [ ] **CAT-03**: Catalog search is accessible as a mode/option within the main search interface
- [ ] **CAT-04**: Catalog search results link to manuscript browse view
- [ ] **CAT-05**: Catalog search works in both web and desktop apps

### Pre-Search Filtering

- [ ] **FILT-01**: User can include/exclude FJMS domain classifications before running a text search
- [ ] **FILT-02**: User can include/exclude catalog identifications (work/title) before running a text search
- [ ] **FILT-03**: User can include/exclude author attributions before running a text search
- [ ] **FILT-04**: Pre-search filters work with all search modes (regular, Responsa, PGP tags, parallels)
- [ ] **FILT-05**: Pre-search filtering works in both web and desktop apps

### Transcription Import

- [ ] **TRANS-01**: FJMS ~30K transcriptions imported into fjms_enrichment.db sidecar
- [ ] **TRANS-02**: FJMS transcriptions accessible via shared service layer (both apps)

### Transcription Search

- [ ] **SRCH-01**: PGP transcription text (editions/translations) indexed in Tantivy alongside OCR text
- [ ] **SRCH-02**: FJMS transcription text indexed in Tantivy alongside OCR text
- [ ] **SRCH-03**: User corrections text indexed in Tantivy
- [ ] **SRCH-04**: Search results show badge/icon indicating whether hit came from OCR or transcription source
- [ ] **SRCH-05**: Transcription hits ranked higher than OCR-only hits in search results
- [ ] **SRCH-06**: Transcription search works with all search modes (regular, Responsa)
- [ ] **SRCH-07**: Transcription search works in both web and desktop apps

### Index Distribution

- [ ] **DIST-01**: Pre-built Tantivy index (with transcription fields) available for download from web server
- [ ] **DIST-02**: Desktop app can detect and upgrade existing index to include transcription data
- [ ] **DIST-03**: Fresh desktop installs receive complete index with transcriptions
- [ ] **DIST-04**: Index update mechanism handles existing users with older index format

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
| FGP direct image access | FGPImageNumberId ≠ IIIF FL ID |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| (populated by roadmapper) | | |

**Coverage:**
- v7.0.0 requirements: 28 total
- Mapped to phases: 0
- Unmapped: 28 ⚠️

---
*Requirements defined: 2026-02-22*
*Last updated: 2026-02-22 after initial definition*
