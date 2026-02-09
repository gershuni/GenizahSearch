# GenizahSearch — Responsa Search

## What This Is

A research platform for the Cairo Genizah that combines manuscript image browsing with scholarly data from Princeton Geniza Project (PGP). Users can view human-curated transcriptions from multiple scholars, browse rich document metadata (types, tags, dates, descriptions), navigate fragment relationships, and search across ~217,000 manuscript records. Available as both a NiceGUI web app and a PyQt6 desktop app.

## Core Value

**Users can search the Genizah corpus using Responsa Project-style syntax and a tabular query builder.** Advanced search features — wildcards, grammatical prefixes, OR alternatives, Judeo-Arabic expansion, and flexible spacing — give researchers fine-grained control over Hebrew and Judeo-Arabic manuscript search, in both web and desktop apps.

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
- Shared service layer for Supabase access — v5.6.0
- Desktop PGP feature parity (transcriptions, metadata, joins, tag search, version selector) — v5.6.0
- Virtual Reading Desk (multi-manuscript viewer in both apps) — v5.6.0

### Active

- [ ] Responsa syntax parsing: wildcards (`*`), grammatical prefixes (`#`), OR alternatives (`(/)`)
- [ ] Judeo-Arabic definite article expansion with sun letter assimilation
- [ ] Flexible spacing (zero-width word boundaries for OCR errors)
- [ ] Bidirectional gap search (both word orders)
- [ ] Combinatorial explosion guard with auto-downgrade cascade
- [ ] Web UI: Responsa checkboxes + tabular query builder (expansion panel)
- [ ] Desktop UI: Responsa checkboxes + tabular query builder (QDialog)
- [ ] Both apps produce identical search results for Responsa queries

### Out of Scope

- PGP people/places integration — complexity too high, defer
- Map-based geographic browse — requires places.csv + UI work, defer
- Automatic PGP sync from GitHub — manual refresh sufficient
- Build transcription editor — link to external tools instead
- Build join detection AI — import from NLI/PGP instead
- Transcription search in Tantivy — deferred (Phase 13 reverted, needs server-side index architecture)
- NLI joins import — deferred to future milestone
- Per-component search modes (exact/variants per column) — Option III, defer unless demand
- Scope search (sentence/paragraph/document) — Option III feature, defer
- Per-component negation — Option III feature, defer
- Bidirectional sync between tabular and text field — one-way only (tabular -> text)
- `\s*` per char on variants/JA expansions — only on original terms
- Desktop checkbox persistence between sessions — defaults on startup, defer

## Current Milestone: v5.7.0 Responsa Search

**Goal:** Add Responsa Project-style search capabilities to both web and desktop apps — syntax-based advanced search with a tabular query builder, Judeo-Arabic and flexible spacing support, and smart explosion guards.

**Target features:**
- Responsa syntax: `*` wildcards, `#` grammatical prefixes, `(/)` OR alternatives
- Global checkboxes: Responsa Mode, Variants, Judeo-Arabic (al-), Flexible Spacing, Bidirectional Gap
- Tabular query builder: 2-3 components, one-way sync (tabular -> text field)
- Web: expansion panel for tabular; Desktop: QDialog for tabular
- Combinatorial explosion guard with downgrade cascade

## Context

### Current State (after v5.6.0)

**Shipped:** v5.6.0 Desktop Parity & PGP Integration (2026-02-09, git tag v5.6.0)
- Desktop has full PGP feature parity with web
- Virtual Reading Desk in both apps
- 35,839 PGP documents imported
- Phase 13 (Transcription Search) deferred

**Architecture:**
- Web: NiceGUI -> Supabase (PGP data) + Tantivy (search index)
- Desktop: PyQt6 -> Supabase (community features) + Tantivy (search index)
- Shared: genizah_core.py (search engine, metadata, variants)
- Shared: shared/document_service.py (PGP data access)

**Codebase:**
- genizah_app.py: ~17,722 lines (desktop)
- web/pages/search.py: ~3,761 lines (web search)
- genizah_core.py: ~7,066 lines (shared core)
- gui_threads.py: SearchThread/LabSearchThread (desktop async)

### Search Engine (Two-Phase Architecture)

The Responsa feature integrates into the existing two-phase search:
1. **Phase 1 (Tantivy)**: Fast full-text index -> retrieves candidate documents via OR groups
2. **Phase 2 (Regex)**: Precise pattern matching -> filters, highlights results

Responsa adds a **parsing layer** before both phases -- `parse_responsa_query()` translates syntax into structured components, which feed into `build_tantivy_query()` (OR groups with boosting) and `build_regex_pattern()` (wildcards, alternations).

### Architectural Principle

**Both apps must be maintained.** Responsa search logic lives in `genizah_core.py` (shared). UI is app-specific: web checkboxes + expansion panel, desktop checkboxes + QDialog.

## Constraints

- **Dual App Maintenance**: All Responsa search features must work in both web and desktop
- **Shared Core**: All search logic in genizah_core.py -- UI-only code in app-specific files
- **`#` Prefix Conflict**: When Responsa mode ON, `#` = grammatical prefixes (not Shelfmark). All prefix shortcuts disabled in Responsa mode
- **Combinatorial Cap**: MAX_EXPANDED_TERMS = 500. Downgrade: variants->basic->off->JA off->error
- **Flexible Spacing**: `\s*` per char only on original terms, not on variant/JA expansions
- **Backward Compatibility**: All existing search modes unchanged when Responsa checkbox OFF
- **PGP Tags Interaction**: Responsa checkboxes hidden when PGP Tags mode active
- **Desktop Persistence**: Checkbox defaults on startup (no QSettings persistence for now)
- **URL State**: Web stores text + checkbox states in URL, not tabular state

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Option II (Hybrid Integration) for core | Best balance: Tantivy-aware OR groups, foundation for tabular | Recommended |
| Option IIb (Dialog/Panel) for tabular | Separate mode, one-way sync to text field | Recommended |
| `#` conflict: Responsa ON = shortcuts OFF | Clear separation, no ambiguity | Confirmed |
| Downgrade cascade: variants->JA->error | Prevents query explosion while maximizing coverage | Confirmed |
| Flex spacing: basic + advanced on originals only | Performance-safe, covers most OCR cases | Confirmed |
| PGP Tags: hide Responsa checkboxes | Separate search paradigm, no interaction needed | Confirmed |
| Desktop: QDialog for tabular | Avoids Row 2 crowding, follows existing dialog patterns | Confirmed |
| Web: expansion panel for tabular | Collapsible, doesn't clutter search page | Confirmed |
| 2-3 components max in tabular | Matches Responsa Project, prevents UI complexity | Confirmed |
| Highlighting: existing mechanism sufficient | Regex match -> colored span already handles multi-term | Confirmed |
| URL: text + checkboxes only | Tabular is a builder tool, its state is transient | Confirmed |
| Desktop: defaults on startup | Persistence deferred, simpler MVP | Confirmed |

---
*Last updated: 2026-02-09 after v5.7.0 milestone start*
