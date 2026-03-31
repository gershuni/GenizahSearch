# Requirements: v7.7 Volume-Aware Browse

**Defined:** 2026-03-31
**Core Value:** When a user finds text from a specific IE via search, browsing that result shows matching text AND images from the same IE — no silent mismatches.

## v1 Requirements (Web MVP)

### Data Infrastructure

- [ ] **DATA-01**: IE-to-IIIF-suffix mapping exists for all 3,193 multi-IE manuscripts (derived from MARC 907 field order)
- [ ] **DATA-02**: `browse_map` stores per-IE page lists (no cross-IE dedup) so each IE's pages are independently addressable
- [ ] **DATA-03**: Single-IE manuscripts (98.5%) are structurally unchanged in browse_map — zero regression

### Search → Browse Navigation

- [ ] **NAV-01**: When a search result comes from IE X, clicking "browse" opens IE X (not the primary IE)
- [ ] **NAV-02**: The IE is determined from the search result's `full_header` (already contains IE identifier)
- [ ] **NAV-03**: If IE cannot be determined (edge case), fall back to primary IE with no crash

### Image Loading

- [ ] **IMG-01**: Browse page loads images from the IIIF manifest matching the active IE's suffix (not always `-1`)
- [ ] **IMG-02**: `fetch_fl_ids_from_nli()` accepts a suffix parameter to fetch the correct manifest
- [ ] **IMG-03**: Image and displayed text always belong to the same IE — the core invariant

### Browse Paging

- [ ] **PAG-01**: Prev/next navigation stays within the active IE's page range
- [ ] **PAG-02**: Page count and page index reflect the active IE only (not all IEs combined)
- [ ] **PAG-03**: Volume selector UI allows switching between IEs for multi-IE manuscripts
- [ ] **PAG-04**: Volume selector shows IE label and page count per volume
- [ ] **PAG-05**: Single-IE manuscripts show no volume selector — completely unchanged UX

### Regression Safety

- [ ] **REG-01**: All existing single-IE browse, search, URL, and session behaviors unchanged
- [ ] **REG-02**: Tantivy search index unchanged — all IEs remain searchable
- [ ] **REG-03**: Community features (corrections, comments, discoveries) unaffected
- [ ] **REG-04**: Desktop app continues to work with existing index (no desktop changes in v1)

## v2 Requirements (Deferred)

### Desktop Parity

- **DSK-01**: Desktop browse supports volume-aware navigation (same IE propagation)
- **DSK-02**: Desktop volume selector UI in Browse tab
- **DSK-03**: Desktop search → browse respects IE from search result

### URL & Session

- **URL-01**: Browse URL includes volume/IE parameter for shareable links
- **URL-02**: Session restore preserves active volume across browser refresh

### Community Writes

- **CW-01**: Corrections include volume/IE context in write payload
- **CW-02**: Comments reference specific volume/IE

### Corpus Validation

- **VAL-01**: Corpus-wide automated validation of 907→suffix mapping against live IIIF manifests

## Out of Scope

| Feature | Reason |
|---------|--------|
| Desktop volume-aware browse | Deferred to v2 — web ships first |
| Full URL/session volume propagation | Nice-to-have, not MVP |
| Community writes with IE context | No existing data on affected manuscripts; defer |
| IIIF manifest caching per suffix | Optimization, not correctness |
| Non-NLI multi-source volume handling | Only NLI has multi-IE; Cambridge/Manchester/JTS unaffected |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| DATA-01 | 58 | Pending |
| DATA-02 | 58 | Pending |
| DATA-03 | 58 | Pending |
| REG-02 | 58 | Pending |
| REG-04 | 58 | Pending |
| NAV-01 | 59 | Pending |
| NAV-02 | 59 | Pending |
| NAV-03 | 59 | Pending |
| IMG-01 | 59 | Pending |
| IMG-02 | 59 | Pending |
| IMG-03 | 59 | Pending |
| PAG-01 | 59 | Pending |
| PAG-02 | 59 | Pending |
| PAG-03 | 59 | Pending |
| PAG-04 | 59 | Pending |
| PAG-05 | 59 | Pending |
| REG-01 | 59 | Pending |
| REG-03 | 59 | Pending |

**Coverage:**
- v1 requirements: 18 total
- Mapped to phases: 18
- Unmapped: 0 ✓

---
*Requirements defined: 2026-03-31*
*Last updated: 2026-03-31 after initial definition*
