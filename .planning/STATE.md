# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-09)

**Core value:** Users can search the Genizah corpus using Responsa Project-style syntax and a tabular query builder
**Current focus:** v5.7.0 Responsa Search -- Phase 15 (Search UI)

## Current Position

Phase: 15 of 17 (Search UI) -- IN PROGRESS
Plan: 1 of 2 complete
Status: **Plan 15-01 (Web Responsa Checkboxes) complete, ready for Plan 15-02 (Desktop UI)**
Last activity: 2026-02-09 -- Plan 15-01 (Web Responsa Checkboxes) complete

Progress: [######______________] 37% (3/8 plans across phases 14-17)

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
- Amber badge indicator when Responsa mode hides mode dropdown
- Mobile Responsa controls: icon button with ui.menu popup
- URL state: history.replaceState with ?responsa=1&variants=1&ja=1&flex_spaces=1&bidirectional=1

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

## Session Continuity

Last session: 2026-02-09
Stopped at: Completed 15-01-PLAN.md (Web Responsa Checkboxes)
Resume file: None
Notes: Plan 15-01 complete. Web search page has Responsa checkbox row with master toggle, sub-options, mode interaction, URL state persistence, explosion warning display, expanded term count in results header, and mobile responsive layout. 99 tests still passing. Next: Plan 15-02 (Desktop UI) to add equivalent checkboxes to PyQt6 app.
