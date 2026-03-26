# Requirements: v7.3 Search Refinement & Scholarly Joins

**Defined:** 2026-03-26
**Core Value:** Help researchers narrow results and discover related fragments across the Cairo Genizah corpus

## v7.3 Requirements

**Scope:** Both web (NiceGUI) and desktop (PyQt6) unless noted. Implementations may differ per app.

### Search Within Results

- [ ] **SRCH-01**: User can run a second search query restricted to the current result set's sys_ids (web + desktop)
- [ ] **SRCH-02**: User can see a refinement breadcrumb showing the search chain (web + desktop)
- [ ] **SRCH-03**: User can clear refinement to return to full search (web + desktop)

### Exclude Known Manuscripts

- [ ] **EXCL-01**: User can exclude manuscripts from a saved Supabase list from search results (web + desktop)
- [ ] **EXCL-02**: User can import a shelfmark file (text/CSV) to create an exclusion set (web + desktop)
- [ ] **EXCL-03**: Exclusion resolves shelfmarks across conventions (Supabase cloud lists store sys_ids; desktop local lists may use shelfmarks; imported files may have varied formats — CUL T-S vs T-S vs full library names) (web + desktop)
- [ ] **EXCL-04**: Excluded manuscripts are hidden from results but exclusion count is shown (web + desktop)

### FIST Joins

- [ ] **JOIN-01**: User can see FIST join group suggestions in browse enrichment alongside existing FJMS scientific joins (web + desktop)
- [ ] **JOIN-02**: User can search within FIST join groups as a dedicated search mode (web + desktop)
- [ ] **JOIN-03**: Search results show join partners for matched fragments with visual distinction between matched and partner fragments (web + desktop)

### Dimensions

- [x] **DIM-01**: User can see manuscript dimensions (width x height) in browse and result views (web + desktop)
- [ ] **DIM-02**: User can filter search by dimension range (min/max width and height) as a pre-search filter (web + desktop)
- [ ] **DIM-03**: User can filter within results by dimension range as a post-search filter (web + desktop)
- [x] **DIM-04**: Dimensions are normalized across units (cm/mm/inch) with appropriate display formatting (shared service)

## v2 Requirements

Deferred to future release. Tracked but not in current roadmap.

### Search Refinement

- **SRCH-04**: User can save a refinement chain as a named search preset
- **SRCH-05**: User can do boolean combination of exclusion sets (exclude list A but not list B)

### Joins

- **JOIN-04**: User can navigate from join group to puzzle page with fragments pre-loaded
- **JOIN-05**: User can rate/validate FIST join suggestions

## Out of Scope

| Feature | Reason |
|---------|--------|
| AI-powered join suggestions | Removed AI features in v5.7.2; FIST data is sufficient |
| Dimension image overlay | Visual ruler/scale on images adds complexity with low value |
| Cross-list merge/diff | List management overhaul deferred to separate milestone |
| Real-time collaborative exclusion lists | Single-user workflow sufficient for research use case |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| DIM-01 | Phase 54 | Complete |
| DIM-02 | Phase 54 | Pending |
| DIM-03 | Phase 54 | Pending |
| DIM-04 | Phase 54 | Complete |
| SRCH-01 | Phase 55 | Pending |
| SRCH-02 | Phase 55 | Pending |
| SRCH-03 | Phase 55 | Pending |
| EXCL-01 | Phase 56 | Pending |
| EXCL-02 | Phase 56 | Pending |
| EXCL-03 | Phase 56 | Pending |
| EXCL-04 | Phase 56 | Pending |
| JOIN-01 | Phase 57 | Pending |
| JOIN-02 | Phase 57 | Pending |
| JOIN-03 | Phase 57 | Pending |

**Coverage:**
- v7.3 requirements: 14 total
- Mapped to phases: 14/14
- Unmapped: 0

---
*Requirements defined: 2026-03-26*
*Last updated: 2026-03-26 after roadmap creation (all 14 requirements mapped)*
