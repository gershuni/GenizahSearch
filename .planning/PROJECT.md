# GenizahSearch

## What This Is

A research platform for the Cairo Genizah that combines manuscript image browsing with scholarly data from Princeton Geniza Project (PGP) and Fragment of the Jewish Manuscript Studies (FJMS). Users can view human-curated transcriptions from multiple scholars, browse rich document metadata with domain classifications and catalog enrichment, navigate fragment relationships including scientific joins, search across ~217,000 manuscript records with domain-based filtering, and perform advanced Responsa-Project style searches with grammatical expansion, Judeo-Arabic support, and a visual query builder. All scholarly reference data served from local SQLite sidecars for offline-capable, sub-millisecond browsing. Available as both a NiceGUI web app and a PyQt6 desktop app.

## Core Value

**Researchers can find what they need in the Genizah corpus.** The platform brings together manuscript images, scholarly transcriptions, PGP metadata, FJMS domain classifications, scientific joins, catalog records, and powerful search tools -- from simple keyword search to Responsa-Project style syntax with grammatical prefix expansion, Judeo-Arabic forms, and flexible spacing.

## Current Milestone: v7.0.0 Transcription Search

**Goal:** Import FJMS transcriptions and build unified searchable index over all human transcription text alongside OCR.

**Target features:**
- FJMS transcription import (~30K from FIST.db)
- Unified Tantivy index (PGP + FJMS + user corrections) with source badges and ranking
- Pre-built index distribution and desktop upgrade path

## Current State (after v6.5.0 shipped)

**Shipped:** v6.5.0 Search UX & Filtered Search (2026-03-14, git tag v6.5.0)
- Search UX overhaul: elapsed timer, ETA, partial results on cancel, chunk count, min-chunks filter, 3-state printed filter, CreationType badge (both apps)
- Session persistence: full state + exclusion restore on reopen, search/composition history dropdowns (both apps)
- Quick UX wins: desktop notifications, sleep prevention, Hebrew library names (81 codes), copy context menu
- Bidirectional filtered search: pre-search filtering by domain/author/work/date/material across all modes including parallels, browse-to-search navigation
- Dicta translation: ~580K translations across all scholarly data with translation toggle and per-record display
- Translation QA: 10-heuristic QC module, 12,827 data fixes applied

**Architecture:**
- Web: NiceGUI -> Tantivy (search) + SQLite sidecars (pgp.db + FJMS + NLI + libraries_translations.db) + Supabase (community features only)
- Desktop: PyQt6 -> Tantivy (search) + SQLite sidecars (pgp.db + FJMS + NLI) + Supabase (community features only)
- Shared: genizah_core.py (~8,300 lines -- search engine, metadata, variants, Responsa, filtered search)
- Shared: shared/document_service.py (PGP data from pgp.db SQLite)
- Shared: shared/corrections_service.py (corrections data access)
- Shared: shared/fjms_service.py (FJMS domain, join, catalog, bibliography queries from fjms_enrichment.db)
- Shared: shared/nli_crossref_service.py (NLI crossref, images, metadata, library URLs from nli_crossref.db)
- Shared: shared/translation_service.py (Dicta translations from pgp.db, fjms_enrichment.db, libraries_translations.db)
- Shared: shared/dicta_client.py (Dicta Translate API client with few-shot scholarly prompts)
- Shared: shared/translation_qc.py (translation QC heuristics)

**Data:**
- pgp.db: 35,839 documents, 9,364 sources, 22,757 footnotes, 36,155 fragments, 34,954 translations (v1.0.0)
- manuscripts (libraries.csv): ~217,000 records
- libraries_translations.db: 184,514 title translations (76MB)
- fjms_enrichment.db: 390K domains, 48K joins, 685K catalog (37 cols), 542K bib, 64K catalog_refs, ~260K translations (v5.0.0)
- nli_crossref.db: 815K NLI images, 141K Cambridge manifests, 28K Manchester LUNA, 453 JTS DPUL (v1.2.0)

## Requirements

### Validated

<details>
<summary>v1 through v5.9.0 requirements (55 items)</summary>

- Search MiDRASH auto-transcriptions (V0.8/V0.7) per page -- existing
- User correction submissions with approval workflow -- existing
- Version selector showing V0.8 + user corrections -- existing
- Pairwise fragment joins for navigation -- existing
- Shelfmark normalization with 96.5% PGP match rate -- existing
- PGP transcriptions appear as a version source (primary when available) -- v1
- Document-level entity for multi-fragment PGP records (joined manuscripts) -- v1
- Unified viewer: all images from joined fragments in sequence -- v1
- PGP metadata display: type, tags, dates, descriptions in browse view -- v1
- Search results indicate when PGP transcription available -- v1
- Multi-source selector: switch between scholars' editions and translations -- v1
- Tag-based search from PGP metadata -- v1
- Shared service layer for Supabase access -- v5.6.0
- Desktop PGP feature parity (transcriptions, metadata, joins, tag search, version selector) -- v5.6.0
- Virtual Reading Desk (multi-manuscript viewer in both apps) -- v5.6.0
- Responsa syntax parsing: wildcards, grammatical prefixes/suffixes, OR groups, plene/defective, gap notation -- v5.7.0
- Judeo-Arabic definite article expansion (8 forms, simplified al- model) -- v5.7.0
- Flexible spacing for OCR error tolerance (basic + advanced on original terms) -- v5.7.0
- Bidirectional gap search (both word orders) -- v5.7.0
- Combinatorial explosion guard with 6-step cascade (MAX=500) -- v5.7.0
- Web UI: Responsa as dropdown mode with sub-options, syntax legend, URL state -- v5.7.0
- Desktop UI: Responsa as combo mode with sub-options, syntax legend -- v5.7.0
- Tabular query builder (web dialog + desktop QDialog) with 2-4 components, per-word modifiers -- v5.7.0
- Cross-app parity: identical Responsa results in web and desktop (221 tests) -- v5.7.0
- AI Search artifacts removed from both apps (desktop + web + help docs + core) -- v5.7.2
- Unicode search normalization: combining marks, geresh, apostrophe variants stripped at query time -- v5.7.2
- Mark-tolerant search highlighting (matches through interleaved combining marks) -- v5.7.2
- Full green test suite: 447 tests passing, 0 failures -- v5.7.2
- Structural HTML section parser for PGP transcriptions (canvas-based, replaces regex) -- v5.7.2
- Sections JSONB schema with language/direction metadata per source -- v5.7.2
- Cross-app canvas section display with language-based translation ordering -- v5.7.2
- Pending corrections visible as selectable version in web version selector (amber styling, schedule icon) -- v5.7.3
- Pending corrections visible in desktop Browse tab and Reading Desk (emoji labels) -- v5.7.3
- Shared corrections service with auth-filtered pending corrections (only submitter sees own) -- v5.7.3
- Export FJMS domain classifications, scientific joins, and catalog records into SQLite sidecar -- v5.8.0
- Domain-based filtering in search results (both apps) -- v5.8.0
- Domain display on browse page (both apps) -- v5.8.0
- FJMS join group display with scholar attribution on browse page (both apps) -- v5.8.0
- Catalog enrichment display (titles, authors, dates) on browse page (both apps) -- v5.8.0
- FTS5 schema in sidecar (UI deferred) -- v5.8.0
- NLI crossref sidecar (815K records) imported with NliCrossrefService (16 methods) -- v5.9.0
- Cambridge IIIF manifests (141K) imported into sidecar with local image resolution -- v5.9.0
- Cambridge images load via local CUDL IIIF, bypassing NLI -- v5.9.0
- Image availability source indicators and folio navigation in both apps -- v5.9.0
- Physical metadata (material, folios) and library collection links (KTIV, CUDL, LUNA, DPUL) -- v5.9.0
- FIST bibliography (542K references) with mention type badges and scholar attribution -- v5.9.0
- Catalog cross-references (64K entries across 80 catalogs), Neubauer-Cowley -- v5.9.0
- Manchester LUNA (28K IDs) and JTS/Princeton Figgy integration with detail page links -- v5.9.0

</details>

- PGP data (documents, sources, footnotes, fragments) exported to pgp.db sidecar -- v6.0.0
- document_service.py rewritten to read from SQLite instead of Supabase -- v6.0.0
- Both web and desktop apps use pgp.db for all PGP reference data -- v6.0.0
- JSON data (tags, sections) preserved correctly in SQLite with query parity -- v6.0.0
- Search result enrichment (PGP metadata batch lookup) uses pgp.db -- v6.0.0
- PGP tag-based search uses SQLite json_each() instead of Supabase -- v6.0.0
- FJMS catalog descriptions exported and accessible via dedicated dialog in both apps -- v6.0.0
- pgp.db bundled in desktop installer and web deployment -- v6.0.0
- Desktop PGP browsing works without internet (images excluded) -- v6.0.0
- Paginated search results (PAGE_SIZE=50) replacing 200-result cap -- v6.0.0
- PostHog analytics integrated alongside Google Analytics -- v6.0.0
- Desktop crash fixes (sip.isdeleted guards on all Qt lifecycle sites) -- v6.0.0
- Performance: parallel NLI fetch, browse crossref parallelization, variant cache unification -- v6.0.0
- Search UX: elapsed timer, ETA, partial results on cancel, chunk count, min-chunks filter, CreationType badge (both apps) -- v6.5.0
- Session persistence: full state + exclusion restore on reopen, search/composition history dropdowns (both apps) -- v6.5.0
- Quick UX wins: desktop notifications, sleep prevention, Hebrew library names (81 codes), copy context menu -- v6.5.0
- Bidirectional filtered search: pre-search filtering by domain/author/work/date/material across all modes (both apps) -- v6.5.0
- Dicta translation: ~580K translations for multilingual access with translation toggle (both apps) -- v6.5.0

### Active

- FJMS transcription import: ~30K transcriptions from FIST.db into fjms_enrichment.db
- Transcription search: unified Tantivy index over PGP + FJMS + user transcription text, prioritizing human transcriptions (both apps)
- Index distribution: pre-built index download, desktop upgrade path

### Out of Scope

- PGP people/places integration -- complexity too high, defer
- Map-based geographic browse -- requires places.csv + UI work, defer
- Automatic PGP sync from GitHub -- manual refresh sufficient
- Build transcription editor -- link to external tools instead
- Build join detection AI -- import from NLI/PGP instead
- NLI PartOf relationships UI (424K records) -- service method exists, UI deferred
- NLI See cross-references UI (19K records) -- service method exists, UI deferred
- NLI BifolioWith pairs UI (23K records) -- service method exists, UI deferred
- FJMS full texts as version selector sources -- deferred (catalog descriptions only)
- Migrating libraries.csv to SQLite -- high refactoring risk, no user-visible benefit yet
- FGP direct image access -- FGPImageNumberId ≠ IIIF FL ID, different numbering systems
- Search tabs / multi-search workspace (יג) -- deferred, too architectural

## Context

### Search Engine (Two-Phase Architecture)

1. **Phase 1 (Tantivy)**: Fast full-text index -> retrieves candidate documents via OR groups
2. **Phase 2 (Regex)**: Precise pattern matching -> filters, highlights results

Responsa adds a **parsing layer** before both phases -- `parse_responsa_query()` translates syntax into structured components, which feed into `build_tantivy_query()` (OR groups with boosting) and `build_regex_pattern()` (wildcards, alternations).

### Architectural Principle

**Both apps must be maintained.** All search logic lives in `genizah_core.py` (shared). UI is app-specific.

## Constraints

- **Dual App Maintenance**: All features must work in both web and desktop
- **Shared Core**: All search logic in genizah_core.py -- UI-only code in app-specific files
- **Backward Compatibility**: All existing search modes unchanged when Responsa mode OFF
- **Combinatorial Cap**: MAX_EXPANDED_TERMS = 500 with 6-step downgrade cascade
- **PGP Tags Interaction**: Responsa sub-options hidden when PGP Tags mode active
- **Legacy Supabase**: PGP tables kept in Supabase (legacy desktop users depend on them)

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Supabase direct (no FastAPI) | Simplicity, reduced infrastructure | Good |
| Tantivy local index | Fast search, no server dependency | Good |
| Two-phase search (Tantivy + Regex) | Best of both: speed + precision | Good |
| Shared service layer (document_service.py) | Both apps consume PGP data | Good |
| Option II (Hybrid) for Responsa core | Tantivy OR groups + Regex patterns, best balance | Good |
| SQLite sidecar pattern (3 sidecars) | Read-only reference data, both apps, offline capable | Good |
| pgp.db as separate sidecar (not extending existing) | Distinct domain boundary, different update cycle | Good |
| Tags as TEXT JSON with json_each() | Simple start, 115ms acceptable for 2695 tags | Good |
| Post-search domain filtering (not pre-search) | Users see all results first, then narrow by domain | Good |
| Separate nli_crossref.db sidecar | Different provenance and update cycles from FJMS | Good |
| FGP ≠ FL (crossref FGPImageNumberId not usable for IIIF) | Friedberg photo numbers are different numbering system | Lesson Learned |
| Phase 13 deferred | Transcription index build too slow for desktop | Revisit in v7.0.0 |
| v6.5.0 UX-first ordering | Power user feedback: search UX pain > catalog features | Good — addressed 15/17 user requests |
| Bidirectional filtered search | Pre-filter from search + "search within" from browse — same restrict_sys_ids mechanism | Good |
| Dicta Translation for all data | Multilingual access + search completeness, scholarly few-shot prompts | Good — 580K translations, 0 failures |
| Translation QA with heuristic checks | Catch hallucinations, script mismatches, length anomalies before display | Good — found and fixed 12,827 issues |
| Transcription deferred to v7.0.0 | v6.5.0 focuses on UX + filtering; transcription is separate milestone | Good |

---
*Last updated: 2026-03-14 after v6.5.0 milestone shipped*
