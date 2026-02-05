# External Data Integration

## What This Is

A major enhancement to GenizahSearch that integrates external scholarly data from Princeton Geniza Project (PGP) and NLI, enabling users to search human-curated transcriptions, view multi-fragment documents as unified entities, and access rich scholarly metadata (document types, tags, dates, descriptions). This transforms GenizahSearch from a manuscript browser into a research platform with scholarly context.

## Core Value

**Users can search and view PGP's human-curated transcriptions alongside existing content.** If everything else fails, the 9,364 PGP transcriptions must be searchable and displayable with proper attribution.

## Requirements

### Validated

- Search MiDRASH auto-transcriptions (V0.8/V0.7) per page — existing
- User correction submissions with approval workflow — existing
- Version selector showing V0.8 + user corrections — existing
- Pairwise fragment joins for navigation — existing
- Shelfmark normalization with 96.5% PGP match rate — existing (pgp_data/transcriptions_linked.csv)

### Active

- [ ] PGP transcriptions searchable in Tantivy index
- [ ] PGP transcriptions appear as a version source (primary when available)
- [ ] Document-level entity for multi-fragment PGP records (joined manuscripts)
- [ ] Unified viewer: all images from joined fragments in sequence
- [ ] PGP metadata display: type, tags, dates, descriptions in browse view
- [ ] NLI joins import: PartOf, BifolioWith relationships from crossreference
- [ ] Search results indicate when PGP transcription/metadata available

### Out of Scope

- PGP people/places integration — complexity too high for this milestone, defer
- Map-based geographic browse — requires places.csv + UI work, defer
- Cambridge IIIF local manifest lookup — optimization, not core feature
- Full NLI crossreference indexing — 815K records too large, use for joins only
- Automatic PGP sync from GitHub — manual refresh sufficient for now

## Context

### Data Sources Available

| Source | Records | Status | Use |
|--------|---------|--------|-----|
| PGP transcriptions | 9,364 linked | Exported ✓ | Primary transcription source |
| PGP documents.csv | ~41,000 | Available | Metadata (type, tags, dates, descriptions) |
| PGP footnotes.csv | ~24,000 | Available | Transcriptions extracted |
| NLI crossreference | 815,000 | Available | Join relationships (PartOf, BifolioWith) |

### Architectural Challenge

PGP organizes data per-document (PGPID), which may span multiple fragments (e.g., "T-S 13J35.3 + AIU VII.A.23"). GenizahSearch organizes per-sys_id/shelfmark. This milestone introduces a **Document entity** that groups multiple sys_ids when they form a single scholarly document.

### Current Systems to Extend

1. **Tantivy Index** - Add PGP transcriptions as indexable content
2. **Version Selector** - Add PGP as a source type alongside V0.8/user
3. **Fragment Joins** - Extend from pairwise links to support document groups
4. **Browse Viewer** - Support multi-fragment documents with sequential images

### Key Technical Decisions Made

- Single-fragment manuscripts stay as-is; Document entity only for joins
- All images from joined fragments shown in sequence (not tabs)
- PGP transcription is primary version when available
- MiDRASH per-page transcriptions remain as secondary versions

## Constraints

- **Data Volume**: PGP 41K docs manageable; NLI 815K too large for full indexing
- **Schema Evolution**: Tantivy index may need rebuild (affects users)
- **Backward Compatibility**: Existing pairwise joins must continue working
- **Attribution**: PGP transcriptions must show scholar attribution per their license

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| PGP transcriptions as version source | Integrates naturally with existing version selector UX | — Pending |
| Document entity for joins only | Avoids complexity for single-fragment majority | — Pending |
| Sequential images for joined docs | Simpler UX than tabs, mirrors physical document flow | — Pending |
| NLI joins import | Adds 424K+ relationships cheaply | — Pending |

---
*Last updated: 2026-02-05 after initialization*
