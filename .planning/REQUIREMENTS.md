# Requirements: GenizahSearch v5.6.0

**Defined:** 2026-02-07
**Core Value:** Users can view and search PGP's human-curated transcriptions alongside manuscript images -- in both web and desktop apps

## v5.6.0 Requirements

Requirements for Desktop Parity & Transcription Search milestone. Each maps to roadmap phases.

### Infrastructure

- [x] **INFRA-01**: Shared Supabase client provider accessible by both web and desktop apps
- [x] **INFRA-02**: Document service extracted from web/ to shared module with reshaped API (fix TODO, clean naming)
- [x] **INFRA-03**: Web app continues working unchanged via re-export shim after extraction

### Desktop PGP Parity

- [x] **DESK-01**: User can view PGP transcriptions in desktop manuscript viewer (auto-selected when available)
- [x] **DESK-02**: User can switch between scholars' editions and translations via grouped version selector with separators
- [x] **DESK-03**: User can view PGP metadata (document type, tags, dates, description) in collapsible panel
- [x] **DESK-04**: User can see green indicator in search results for manuscripts with PGP transcriptions
- [x] **DESK-05**: User can search by PGP tag (new search mode + clickable tags in metadata panel)
- [x] **DESK-06**: User can see PGP-sourced joins in Related Fragments dialog alongside user joins

### Transcription Search

- [ ] **SRCH-01**: PGP transcriptions indexed in Tantivy alongside existing HTR content
- [ ] **SRCH-02**: User correction transcriptions indexed in Tantivy
- [ ] **SRCH-03**: User can filter search to all content (default), transcriptions only, or exclude transcriptions
- [ ] **SRCH-04**: Tantivy index rebuilt with transcription fields using safe temp-then-swap pattern

### Virtual Reading Desk

- [x] **VIEW-01**: User can view all fragments from a joined document together (images + transcriptions)
- [x] **VIEW-02**: User can add any manuscript to the reading desk by shelfmark or sys_id
- [x] **VIEW-03**: User can add manuscripts to the reading desk from personal lists
- [x] **VIEW-04**: Reading desk works in both web and desktop apps

### Data Import

- [x] **DATA-01**: Import remaining ~34K PGP documents (metadata only, no transcriptions) to Supabase
- [x] **DATA-02**: Document fragments linked for any multi-fragment documents in the new batch

## Future Requirements

Deferred to v5.7.0 or later. Tracked but not in current roadmap.

### Desktop Polish

- **POLISH-01**: Dockable PGP info panel (QDockWidget) for persistent metadata display
- **POLISH-02**: Keyboard-driven version switching (Ctrl+Shift+P for PGP, etc.)
- **POLISH-03**: Side-by-side edition comparison with diff highlighting
- **POLISH-04**: Offline PGP data cache (SQLite local store)
- **POLISH-05**: Tag cloud / tag browser for discovery

### Data Expansion

- **EXPAND-01**: NLI joins import (~424K PartOf relationships from crossreference)
- **EXPAND-02**: NLI BifolioWith import (306K image-level bifolio pairs)
- **EXPAND-03**: PGP footnotes.csv import (~24K additional bibliography)

## Out of Scope

Explicitly excluded. Documented to prevent scope creep.

| Feature | Reason |
|---------|--------|
| PGP people/places integration | Complexity too high, separate milestone |
| Map-based geographic browse | Requires places.csv + significant UI work |
| Automatic PGP sync from GitHub | Manual refresh sufficient |
| Build transcription editor | Link to external tools instead |
| Build join detection AI | Import from NLI/PGP instead |
| Full NLI crossreference indexing | 815K records too large for full indexing |
| NiceGUI reactive patterns in PyQt6 | Anti-pattern: use native Qt signal-slot instead |
| Separate "PGP mode" in desktop | Anti-pattern: integrate into existing tabs |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| INFRA-01 | Phase 8 | Complete |
| INFRA-02 | Phase 8 | Complete |
| INFRA-03 | Phase 8 | Complete |
| DESK-01 | Phase 10 | Complete |
| DESK-02 | Phase 10 | Complete |
| DESK-03 | Phase 12 | Complete |
| DESK-04 | Phase 12 | Complete |
| DESK-05 | Phase 12 | Complete |
| DESK-06 | Phase 12 | Complete |
| SRCH-01 | Phase 13 | Pending |
| SRCH-02 | Phase 13 | Pending |
| SRCH-03 | Phase 13 | Pending |
| SRCH-04 | Phase 13 | Pending |
| VIEW-01 | Phase 11 | Complete |
| VIEW-02 | Phase 11 | Complete |
| VIEW-03 | Phase 11 | Complete |
| VIEW-04 | Phase 11 | Complete |
| DATA-01 | Phase 9 | Complete |
| DATA-02 | Phase 9 | Complete |

**Coverage:**
- v5.6.0 requirements: 19 total
- Mapped to phases: 19
- Unmapped: 0

---
*Requirements defined: 2026-02-07*
*Last updated: 2026-02-08 after Phase 12 completion*
