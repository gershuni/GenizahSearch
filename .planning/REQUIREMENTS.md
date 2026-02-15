# Requirements: GenizahSearch v5.9.0

**Defined:** 2026-02-15
**Core Value:** Researchers can find what they need in the Genizah corpus

## v5.9.0 Requirements

Import NLI crossreference data (815K image-level records) and Cambridge IIIF manifest (141K URLs) into a SQLite sidecar database, enabling direct image access for 75+ libraries, faster image loading via pre-resolved FL IDs, physical metadata display (material, folio count), fragment relationships (PartOf, See), and library-specific viewer links.

### Data Import

- [ ] **DATA-01**: NLI crossreference (815K image-level records from nli_crossreference.csv) imported into SQLite sidecar database with NLI_AlmaId as join key to libraries.csv system_number
- [ ] **DATA-02**: Cambridge IIIF manifest (141K URLs from cambridge_genizah.json) imported into SQLite sidecar with manifest label → standard shelfmark normalization
- [ ] **DATA-03**: Shared NliCrossrefService providing image lookup, metadata queries, and relationship queries usable by both web and desktop apps

### Image Access

- [ ] **IMG-01**: Image URLs constructed directly from crossref FGPImageNumberId, skipping NLI manifest fetch for 766K records across all 75 libraries
- [ ] **IMG-02**: Cambridge manuscripts load images via local CUDL IIIF manifest URLs (bypass NLI entirely for 141K records)
- [ ] **IMG-03**: Image availability indicator on browse page showing which digital image sources exist for the current manuscript (both apps)
- [ ] **IMG-04**: Page-level image navigation using crossref ImageName ordering (leaf/folio/side sequences)
- [ ] **IMG-05**: Library-specific IIIF fallback for JTS (Princeton Figgy), Manchester (LUNA/MDC), and British Library IIIF as alternative image sources when NLI/FGP images unavailable

### Metadata Display

- [ ] **META-01**: Material type (paper/parchment) from NLI crossref displayed on browse page (both apps)
- [ ] **META-02**: Folio count (NumFolio/NumBifolio) from NLI crossref displayed on browse page (both apps)
- [ ] **META-03**: Clickable NLI catalog link (constructed from NLI_AlmaId) on browse page linking to NLI KTIV viewer (both apps)
- [ ] **META-04**: Clickable link to manuscript's page on its holding library's digital collection (CUDL, Princeton DPUL, Manchester LUNA, BL viewer) on browse page (both apps)

### Fragment Relationships

- [ ] **REL-01**: NLI PartOf relationships (424K records) surfaced in Related Fragments panel alongside PGP and FJMS joins (both apps)
- [ ] **REL-02**: NLI See cross-references (19K records) surfaced in Related Fragments panel (both apps)

## Future Requirements

Deferred to subsequent milestones:

### Bulk Metadata Fetch
- **BULK-01**: Fetch and cache CUDL manifest metadata (titles, descriptions, dates) for all 141K Cambridge items
- **BULK-02**: Fetch Princeton DPUL metadata for JTS items

### Relationships
- **REL-03**: NLI BifolioWith pairs (23K non-zero records) as codicological information
- **REL-04**: NLI joins file import (when received from NLI — cleaner structured data)

### Advanced Features
- **ADV-01**: FTS5 catalog search UI (schema already in FJMS sidecar)
- **ADV-02**: FJMS structured metadata search via TextualFrame tags
- **ADV-03**: Transcription search (Phase 13, needs server-side index architecture)

## Out of Scope

| Feature | Reason |
|---------|--------|
| Bulk CUDL manifest fetch (141K API calls) | High cost, rate limiting risk, PGP+crossref covers most data |
| FGP direct image access | Requires FGP registration, non-commercial terms |
| PGP people/places integration | High complexity, deferred from v1 |
| Map-based geographic browse | Requires places.csv + complex UI |
| Automatic PGP sync from GitHub | Manual refresh sufficient |
| NLI dimensions/lines data | File not yet received from NLI |
| Cambridge sub-collection browsing | UI design needed, not core to image access |

## Traceability

Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| DATA-01 | — | Pending |
| DATA-02 | — | Pending |
| DATA-03 | — | Pending |
| IMG-01 | — | Pending |
| IMG-02 | — | Pending |
| IMG-03 | — | Pending |
| IMG-04 | — | Pending |
| IMG-05 | — | Pending |
| META-01 | — | Pending |
| META-02 | — | Pending |
| META-03 | — | Pending |
| META-04 | — | Pending |
| REL-01 | — | Pending |
| REL-02 | — | Pending |

**Coverage:**
- v5.9.0 requirements: 15 total
- Mapped to phases: 0
- Unmapped: 15 (pending roadmap)

---
*Requirements defined: 2026-02-15*
*Last updated: 2026-02-15 after initial definition*
