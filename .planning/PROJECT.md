# GenizahSearch

## What This Is

A research platform for the Cairo Genizah that combines manuscript image browsing with scholarly data from Princeton Geniza Project (PGP). Users can view human-curated transcriptions from multiple scholars, browse rich document metadata, navigate fragment relationships, search across ~217,000 manuscript records, and perform advanced Responsa-style searches with grammatical expansion, Judeo-Arabic support, and a visual query builder. Available as both a NiceGUI web app and a PyQt6 desktop app.

## Core Value

**Researchers can find what they need in the Genizah corpus.** The platform brings together manuscript images, scholarly transcriptions, PGP metadata, and powerful search tools -- from simple keyword search to Responsa-style syntax with grammatical prefix expansion, Judeo-Arabic forms, and flexible spacing.

## Requirements

### Validated

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

### Active

#### Current Milestone: v5.7.1 Cleanup & Polish

**Goal:** Remove dead code, fix diacritics normalization for JA search, and resolve all pre-existing test failures.

- [ ] Remove AI Search component (desktop + web + help docs)
- [ ] JA diacritics normalization (combining marks + geresh/apostrophes stripped, mark-tolerant highlighting)
- [ ] Fix/delete all pre-existing test failures (export filenames, boundary search, obsolete backend tests)

### Out of Scope

- PGP people/places integration -- complexity too high, defer
- Map-based geographic browse -- requires places.csv + UI work, defer
- Automatic PGP sync from GitHub -- manual refresh sufficient
- Build transcription editor -- link to external tools instead
- Build join detection AI -- import from NLI/PGP instead
- Transcription search in Tantivy -- deferred (Phase 13, needs server-side index architecture)
- NLI joins import (~424K PartOf relationships) -- deferred to future milestone
- Per-component search modes (exact/variants per column) -- Option III, defer unless demand
- Scope search (sentence/paragraph/document) -- Option III feature, defer
- Per-component negation (NOT clause) -- Option III feature, defer
- Bidirectional sync between tabular and text field -- one-way only
- `\s*` per char on variants/JA expansions -- only on original terms
- Desktop Responsa checkbox persistence between sessions -- defaults on startup
- `##` double-hash syntax -- checkbox approach preferred
- Query preview line -- user explicitly excluded

## Context

### Current State (after v5.7.0)

**Shipped:** v5.7.0 Responsa Search (2026-02-10, git tag v5.7.0)
- Responsa Project-style advanced search in both apps
- Full syntax: `#`prefix, suffix`#`, `*`wildcards, `%`plene/defective, `(a/b)` OR, `[N]` gaps
- Tabular query builder with 2-4 components and per-word modifiers
- 221 automated Responsa tests, 25/25 requirements satisfied
- Explosion guard with 6-step cascade

**Architecture:**
- Web: NiceGUI -> Supabase (PGP data) + Tantivy (search index)
- Desktop: PyQt6 -> Supabase (community features) + Tantivy (search index)
- Shared: genizah_core.py (~8,233 lines -- search engine, metadata, variants, Responsa)
- Shared: shared/document_service.py (PGP data access)

**Codebase:**
- genizah_app.py: ~18,454 lines (desktop)
- web/pages/search.py: ~3,204 lines (web search)
- genizah_core.py: ~8,233 lines (shared core)
- gui_threads.py: ~616 lines (desktop async)
- Total core: ~30,507 lines Python

**Data:**
- documents: 35,839 records (full PGP corpus)
- document_sources: 9,364 (7,664 editions + 1,696 translations)
- document_footnotes: 22,757 records
- document_fragments: 36,155 links
- manuscripts (libraries.csv): ~217,000 records

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

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Supabase direct (no FastAPI) | Simplicity, reduced infrastructure | Good |
| Tantivy local index | Fast search, no server dependency | Good |
| Two-phase search (Tantivy + Regex) | Best of both: speed + precision | Good |
| Shared service layer (document_service.py) | Both apps consume PGP data | Good |
| Option II (Hybrid) for Responsa core | Tantivy OR groups + Regex patterns, best balance | Good |
| Option IIb (Dialog/Panel) for tabular | Separate builder, one-way sync to text | Good |
| Responsa as dropdown mode (not checkbox row) | Cleaner UX, first-class mode | Good |
| `#` conflict: Responsa ON = shortcuts OFF | Clear separation, no ambiguity | Good |
| Simplified JA: always 'al' (no assimilation) | 8 forms vs 14, simpler and sufficient for Genizah | Good |
| 6-step explosion guard cascade | Prevents query explosion while maximizing coverage | Good |
| Flex spacing on originals only | Performance-safe, covers most OCR cases | Good |
| Desktop QDialog for tabular | Avoids crowding, follows existing patterns | Good |
| One-way tabular -> text sync | Complexity-appropriate for MVP | Good |
| Phase 13 deferred | Transcription index build too slow for desktop | Revisit |

---
*Last updated: 2026-02-11 after v5.7.1 milestone start*
