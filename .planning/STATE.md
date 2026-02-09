# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-09)

**Core value:** Users can search the Genizah corpus using Responsa Project-style syntax and a tabular query builder
**Current focus:** v5.7.0 Responsa Search -- Phase 14 (Responsa Core Engine)

## Current Position

Phase: 14 of 17 (Responsa Core Engine) -- IN PROGRESS
Plan: 1 of 2 complete
Status: **Plan 14-01 complete, ready for Plan 14-02**
Last activity: 2026-02-09 -- Plan 14-01 (Responsa Core Functions) complete

Progress: [##__________________] 12% (1/8 plans across phases 14-17)

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
Stopped at: Completed 14-01-PLAN.md (Responsa Core Functions)
Resume file: None
Notes: Plan 14-01 complete. ResponsaComponent dataclass + 4 pure functions implemented with TDD. JA simplified to 8 forms (no sun letter assimilation). Next: execute Plan 14-02 to wire functions into search pipeline.
