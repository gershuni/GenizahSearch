# GenizahSearch — Desktop Parity & Transcription Search

## What This Is

A research platform for the Cairo Genizah that combines manuscript image browsing with scholarly data from Princeton Geniza Project (PGP). Users can view human-curated transcriptions from multiple scholars, browse rich document metadata (types, tags, dates, descriptions), navigate fragment relationships, and search across ~217,000 manuscript records. Available as both a NiceGUI web app and a PyQt6 desktop app.

## Core Value

**Users can view and search PGP's human-curated transcriptions alongside manuscript images.** The 7,090 PGP documents with 9,364 transcription/translation sources are displayable with proper scholar attribution.

## Requirements

### Validated

- Search MiDRASH auto-transcriptions (V0.8/V0.7) per page — existing
- User correction submissions with approval workflow — existing
- Version selector showing V0.8 + user corrections — existing
- Pairwise fragment joins for navigation — existing
- Shelfmark normalization with 96.5% PGP match rate — existing
- PGP transcriptions appear as a version source (primary when available) — v1
- Document-level entity for multi-fragment PGP records (joined manuscripts) — v1
- Unified viewer: all images from joined fragments in sequence — v1
- PGP metadata display: type, tags, dates, descriptions in browse view — v1
- Search results indicate when PGP transcription available — v1
- Multi-source selector: switch between scholars' editions and translations — v1
- Tag-based search from PGP metadata — v1

### Active

- [ ] Shared service layer: reshape and extract document_service from web/ to shared module
- [ ] Desktop app: PGP transcription display in browse tab (full parity with web)
- [ ] Desktop app: PGP metadata display (type, tags, dates, descriptions)
- [ ] Desktop app: Related Fragments / joins display
- [ ] Desktop app: Tag-based search from PGP metadata
- [ ] Desktop app: Multi-source version selector (scholars' editions and translations)
- [ ] Desktop app: PGP transcription indicator in search results
- [ ] Transcriptions searchable in Tantivy (PGP + user corrections), with filter toggle
- [ ] Tantivy index rebuild with transcription fields

### Out of Scope

- PGP people/places integration — complexity too high, defer
- Map-based geographic browse — requires places.csv + UI work, defer
- Cambridge IIIF local manifest lookup — optimization, not core feature
- Full NLI crossreference indexing — 815K records too large, use for joins only
- Automatic PGP sync from GitHub — manual refresh sufficient for now
- NLI BifolioWith import — 306K image-level bifolio pairs, defer to v2+
- Build transcription editor — link to external tools instead
- Build join detection AI — import from NLI/PGP instead
- NLI joins import — deferred to v5.7.0 (not blocking desktop parity)
- Import remaining ~34K PGP documents — deferred to v5.7.0 (metadata only, low priority)

## Current Milestone: v5.6.0 Desktop Parity & Transcription Search

**Goal:** Bring all PGP features to the desktop app via a shared service layer, and make transcriptions (PGP + user) searchable in Tantivy with filter controls.

**Target features:**
- Shared service layer extracted and reshaped from web/document_service.py
- Desktop app gains full PGP feature parity with web (transcriptions, metadata, joins, tag search, version selector)
- Transcriptions indexed in Tantivy with search filter toggle (all content / transcriptions only / exclude transcriptions)

## Context

### Current State (after v1)

**Shipped:** v1 External Data Integration (2026-02-07, git tag v5.5.0)
- 7,090 PGP documents imported to Supabase (documents, document_fragments, document_sources tables)
- 9,364 transcription/translation sources with scholar attribution
- 492 multi-fragment documents with join relationships
- Web app has full PGP feature set (transcriptions, metadata, joins, tag search)
- Desktop app has NO PGP features yet — this is the primary gap

**Architecture:**
- Web: NiceGUI → Supabase (PGP data) + Tantivy (search index)
- Desktop: PyQt6 → Supabase (community features) + Tantivy (search index)
- Shared: genizah_core.py (search engine, metadata, variants)
- Web-only: web/document_service.py (PGP data access) — needs extraction to shared

**Codebase:**
- genizah_app.py: 15,839 lines (desktop)
- web/pages/browse.py: ~3,178 lines (web browse)
- web/document_service.py: ~507 lines (PGP service, web-only currently)
- genizah_core.py: 7,057 lines (shared core)

### Data Sources

| Source | Records | Status | Use |
|--------|---------|--------|-----|
| PGP transcriptions | 9,364 sources | Imported | Transcription/translation display |
| PGP documents | 7,090 imported / ~41,000 total | Partial | Metadata + transcriptions |
| PGP footnotes.csv | ~24,000 | Available | Additional bibliography |
| NLI crossreference | 815,000 | Available | Join relationships (PartOf: ~424K) |

### Architectural Principle

**Both apps must be maintained.** New features that touch data access or display should work in both web and desktop apps. The shared service layer pattern (Option C) ensures both apps consume the same Supabase queries without code duplication.

## Constraints

- **Dual App Maintenance**: All new data features must plan for both web (NiceGUI) and desktop (PyQt6)
- **Shared Service Layer**: Business logic in shared modules, UI in app-specific code
- **Data Volume**: PGP 41K docs manageable; NLI 815K too large for full indexing
- **Schema Evolution**: Tantivy index may need rebuild (affects users)
- **Backward Compatibility**: Existing features in both apps must continue working
- **Attribution**: PGP transcriptions must show scholar attribution per their license
- **Supabase Free Tier**: 500MB database limit

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| PGP transcriptions as version source | Integrates naturally with existing version selector UX | Good |
| Document entity for joins only | Avoids complexity for single-fragment majority | Good |
| Sequential images for joined docs | Simpler UX than tabs, mirrors physical document flow | Good |
| pgpid as natural PRIMARY KEY | Matches PGP data source, avoids synthetic IDs | Good |
| Two-pass import (docs then fragments) | Respects FK constraints, clean deduplication | Good |
| JSONB tags with GIN index | Flexible tag queries without join table | Good |
| Service layer pattern | Isolates Supabase queries from UI code | Good — needs extraction from web/ |
| Shared service layer (Option C) | Both apps consume same Supabase functions | Pending — this milestone |
| NLI joins deferred to v5.7.0 | PartOf relationships add 424K links cheaply, not blocking desktop parity | — Pending |
| Reshape service API during extraction | Fix TODO, clean up naming, improve API surface while extracting | — Pending |
| Transcription search with filter toggle | Default=everything, optional filter for transcriptions only or exclude | — Pending |
| v5.6.0 version tag | Aligns with existing git tag scheme (v5.5.0 → v5.6.0) | — Pending |

---
*Last updated: 2026-02-07 after v5.6.0 milestone start*
