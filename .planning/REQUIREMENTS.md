# Requirements: GenizahSearch v6.0.0

**Defined:** 2026-02-16
**Core Value:** Researchers can find what they need in the Genizah corpus

## v6.0.0 Requirements

Requirements for Local Data Architecture milestone. Each maps to roadmap phases.

### Data Migration

- [ ] **MIGR-01**: PGP data (documents, sources, footnotes, fragments) exported to `pgp.db` sidecar
- [ ] **MIGR-02**: `document_service.py` rewritten to read from SQLite instead of Supabase
- [ ] **MIGR-03**: Both web and desktop apps use `pgp.db` for all PGP reference data
- [ ] **MIGR-04**: JSON data (tags, sections) preserved correctly in SQLite with query parity
- [ ] **MIGR-05**: Search result enrichment (PGP metadata batch lookup) uses `pgp.db`
- [ ] **MIGR-06**: PGP tag-based search uses SQLite `json_each()` instead of Supabase
- [ ] **MIGR-07**: All existing PGP features produce identical results from SQLite as from Supabase
- [ ] **MIGR-08**: Export script rebuilds `pgp.db` from Supabase source data (repeatable)

### FJMS Descriptions

- [ ] **FJMS-01**: FJMS catalog descriptions (65K records) exported to `fjms_enrichment.db`
- [ ] **FJMS-02**: User can view FJMS scholarly descriptions from browse page via dedicated button in both apps
- [ ] **FJMS-03**: Descriptions show source attribution (which catalog/scholar)

### Distribution

- [ ] **DIST-01**: `pgp.db` bundled in desktop app installer (`build_app.bat`)
- [ ] **DIST-02**: `pgp.db` deployed alongside web server

### Offline

- [ ] **PERF-01**: Desktop PGP metadata/transcription browsing works without internet (images excluded)

## Future Requirements

Deferred to future release. Tracked but not in current roadmap.

### Supabase Cutover

- **CUT-01**: Remove read-only PGP tables from Supabase after all desktop users updated

### FJMS Enhancements

- **FJMS-04**: FJMS full texts searchable via FTS5 index

## Out of Scope

| Feature | Reason |
|---------|--------|
| Full Supabase PGP table removal | Legacy desktop users depend on read-only tables |
| FJMS descriptions in version selector | They're catalog descriptions, not transcription versions |
| FGP direct image access | FGPImageNumberId is not an IIIF FL ID (different numbering) |
| NLI relationship UIs (PartOf, See, BifolioWith) | Service methods exist, UI deferred to future milestone |
| Transcription search in Tantivy | Needs server-side index architecture (Phase 13 deferral) |

## Constraints

- **Legacy Compatibility**: Supabase PGP tables must NOT be removed -- previous desktop app versions depend on them
- **Dual App Maintenance**: All features must work in both NiceGUI web and PyQt6 desktop
- **Shared Core**: All search logic in genizah_core.py, UI-only code in app-specific files

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| MIGR-01 | Phase 35 | Pending |
| MIGR-02 | Phase 36 | Pending |
| MIGR-03 | Phase 36 | Pending |
| MIGR-04 | Phase 35 | Pending |
| MIGR-05 | Phase 36 | Pending |
| MIGR-06 | Phase 36 | Pending |
| MIGR-07 | Phase 36 | Pending |
| MIGR-08 | Phase 35 | Pending |
| FJMS-01 | Phase 37 | Pending |
| FJMS-02 | Phase 37 | Pending |
| FJMS-03 | Phase 37 | Pending |
| DIST-01 | Phase 38 | Pending |
| DIST-02 | Phase 38 | Pending |
| PERF-01 | Phase 38 | Pending |

**Coverage:**
- v6.0.0 requirements: 14 total
- Mapped to phases: 14
- Unmapped: 0

---
*Requirements defined: 2026-02-16*
*Last updated: 2026-02-16 after roadmap creation (traceability populated)*
