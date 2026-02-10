# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-09)

**Core value:** Users can search the Genizah corpus using Responsa Project-style syntax and a tabular query builder
**Current focus:** v5.7.0 Responsa Search -- Phase 16 (Tabular Builder)

## Current Position

Phase: 16 of 17 (Tabular Builder)
Plan: 1 of 3 complete
Status: **Phase 16 in progress -- Plan 01 (Core Engine Extensions) complete, ready for Plan 02 (Web Builder)**
Last activity: 2026-02-10 -- 16-01 complete: gap parsing, syntax generation, per-pair regex gaps

Progress: [############________] 64% (7/11 plans across phases 14-17)

## Milestone History

- **v5.7.0 Responsa Search** -- In progress (started 2026-02-09)
  - 4 phases (14-17), plans TBD
  - See: .planning/ROADMAP.md

- **v5.6.0 Desktop Parity & PGP Integration** -- Shipped 2026-02-09 (git tag v5.6.0)
  - 5 phases (8-12), 25 plans, ~134 min execution
  - Phase 13 (Transcription Search) deferred -- index build too slow for desktop
  - See: .planning/ROADMAP.md (collapsed section)

- **v1 External Data Integration** -- Shipped 2026-02-07 (git tag v5.5.0)
  - 9 phases, 18 plans, 173 min execution
  - See: .planning/milestones/v1-ROADMAP.md

## Accumulated Context

### Decisions

Decisions are logged in PROJECT.md Key Decisions table.
Recent decisions affecting current work:

- Simplified Judeo-Arabic: always 'al' (no sun letter assimilation) -- 8 forms per word
- Option II (Hybrid Integration) for Responsa core engine
- Option IIb (Dialog/Panel) for tabular query builder
- `#` conflict: Responsa ON = prefix shortcuts OFF
- Downgrade cascade: variants->basic->off->JA off->error (MAX_EXPANDED_TERMS=500)
- Flex spacing: basic (`+`->`*`) + advanced (`\s*` per char) on original terms only
- PGP Tags mode: hide Responsa checkboxes (not disable)
- Desktop: QDialog for tabular, expansion panel for web
- Desktop: checkbox defaults on startup, no persistence
- URL: text + checkbox states only, no tabular state
- Highlighting: existing regex match mechanism sufficient
- Expansion order: plene/defective -> prefixes -> suffixes -> JA -> variants
- Component dict keys: tantivy_terms, regex_terms, original_words, wildcard, wildcard_pattern, flex_patterns, inline_pattern
- responsa_options dict: {responsa_mode, variants, ja, flex_spacing, bidirectional, variant_mode}
- Web Responsa checkbox order: Responsa Mode | Variants | JA | Flex Spacing
- Bidirectional Gap in Advanced Options (not main row)
- Mobile Responsa controls: icon button with ui.menu popup
- URL state: history.replaceState with ?responsa=1&variants=1&ja=1&flex_spaces=1&bidirectional=1
- Desktop no QSettings persistence for Responsa checkboxes
- Desktop+Web: Responsa as combo mode option (dropdown), not separate checkbox row
- Desktop: Responsa at combo index 2, all subsequent modes shifted +1
- Desktop: Responsa base mode is 'exact', pipeline via responsa_options dict
- Desktop: Old amber label, master toggle, _on_responsa_mode_toggled removed
- [Phase 15]: Responsa is a first-class dropdown mode, not a separate checkbox toggle (15-03)
- [Phase 15]: R shortcut for Responsa mode, live shortcut detection (prefix+space) for all modes
- [Phase 15]: Sofit-to-normal conversion before suffix expansion (ם→מ, ן→נ, ץ→צ, ף→פ, ך→כ)
- [Phase 15]: Flex spacing splits added to Tantivy query (not just regex)
- [Phase 16]: Tabular builder state format: components [{words: [{text, mods}]}], distances, scope
- [Phase 16]: [N] gap notation for per-pair distances; distance 0 = no bracket token
- [Phase 16]: Negated words extracted separately, not in syntax string
- [Phase 16]: generate_tabular_syntax() modifier order: plene -> prefix -> suffix -> wildcard_prefix -> wildcard_suffix

### Data State

- documents table: 35,839 records (all PGP documents with full metadata)
- document_sources: 9,364 records (7,664 editions + 1,696 translations)
- document_footnotes: 22,757 records (bibliography/scholarship)
- document_fragments: 36,155 records (with collection/library/URL metadata)

### Planning Documents

Comprehensive Responsa search planning at docs/plans/responsa-search/:
- AGENT_BRIEF.md -- Master overview, read first
- 01_feature_analysis.md -- Gap analysis vs Responsa Project
- 02_options_report.md -- Options I/II/III comparison
- 03_review_insights.md -- Critical review
- 04_implementation_response.md -- Response to review
- 05_judeo_arabic_spacing.md -- JA and OCR spacing (Genizah-specific)
- 06_ui_integration_sketch.md -- UI mockups for web + desktop

### Blockers/Concerns

- Recto/verso section headers stripped during parsing (v1 tech debt, not blocking)
- Phase 13 (Transcription Search) deferred -- needs server-side index architecture
- ~~BUG (Flex Spacing + Tantivy)~~: **FIXED** (8ded7a0) — split alternatives added to Tantivy query
- ~~BUG (Variants checkbox)~~: **FIXED** (216dde5) — variant_mode was 'exact', now 'variants' when checked
- ~~BUG (Prefix shortcuts in Responsa)~~: **FIXED** (dfe9914) — responsa_mode=True passed to parse_query_syntax
- ~~BUG (Sofit letters before suffixes)~~: **FIXED** (f1553a4) — ם→מ, ן→נ, ץ→צ, ף→פ, ך→כ conversion
- ~~BUG (#% combined operators)~~: **FIXED** (cd7be21) — any-order parsing with while loop

## Session Continuity

Last session: 2026-02-10
Stopped at: Completed 16-01-PLAN.md (Core Engine Extensions)
Resume file: None
Notes: Phase 16 Plan 01 complete. 2 TDD commits: gap parsing, syntax generation, per-pair regex gaps. 124 Responsa tests passing. Next: 16-02 (Web Tabular Builder).
