# Requirements: GenizahSearch v5.7.0

**Defined:** 2026-02-09
**Core Value:** Users can search the Genizah corpus using Responsa Project-style syntax and a tabular query builder, in both web and desktop apps

## v5.7.0 Requirements

Requirements for Responsa Search milestone. Each maps to roadmap phases.

### Core Engine

- [ ] **CORE-01**: `parse_responsa_query()` parses Responsa syntax (`*` wildcards, `#` prefixes, `(/)` OR, inline alternatives) into structured `ResponsaComponent` list
- [ ] **CORE-02**: `expand_grammatical_prefixes(word)` expands Hebrew grammatical prefixes (ו,ה,ב,כ,ל,מ,ש) producing ~10 forms per word
- [ ] **CORE-03**: `expand_judeo_arabic(word)` expands definite article (אל-) with sun letter assimilation and preposition combinations, producing 8-14 forms per word
- [ ] **CORE-04**: `build_tantivy_query()` supports Responsa components as Tantivy OR groups with boosting (exact terms boosted ^5)
- [ ] **CORE-05**: `build_regex_pattern()` supports wildcards (`\S*`), alternations, and flexible spacing (`\s*` per char on original terms only)
- [ ] **CORE-06**: Bidirectional gap via regex alternation `(forward)|(backward)` when checkbox enabled
- [ ] **CORE-07**: Combinatorial explosion guard: MAX_EXPANDED_TERMS=500, downgrade cascade (variants->basic->off->JA off->error) with user notification
- [ ] **CORE-08**: When Responsa mode ON, prefix shortcuts (`#`, `?`, `/`, `$`, `~`, `=`) are disabled -- query goes to Responsa parser instead

### Web UI

- [ ] **WEB-01**: Checkboxes row: Responsa Mode, Variants, Judeo-Arabic, Flexible Spacing on search page
- [ ] **WEB-02**: Bidirectional Gap checkbox in Advanced Options section
- [ ] **WEB-03**: When Responsa checkbox ON, mode dropdown hidden; when OFF, everything works as today
- [ ] **WEB-04**: Tabular query builder as collapsible expansion panel with 2-3 component columns
- [ ] **WEB-05**: Tabular generates Responsa syntax text and inserts into search field (one-way sync)
- [ ] **WEB-06**: URL persistence: `?responsa=1&variants=1&ja=1&flex_spaces=1&bidirectional=1` + query text
- [ ] **WEB-07**: Responsa checkboxes hidden when PGP Tags mode is active

### Desktop UI

- [ ] **DESK-01**: Checkboxes in search tab: Responsa Mode, Variants, Judeo-Arabic, Flexible Spacing
- [ ] **DESK-02**: Bidirectional Gap checkbox near gap input
- [ ] **DESK-03**: When Responsa checkbox ON, mode dropdown hidden; when OFF, everything works as today
- [ ] **DESK-04**: "Query Builder" button opens QDialog with 2-3 component columns
- [ ] **DESK-05**: QDialog generates Responsa syntax text and inserts into search field (one-way sync)
- [ ] **DESK-06**: `SearchThread` extended with optional `responsa_options` parameter (backward-compatible)
- [ ] **DESK-07**: Responsa checkboxes hidden when PGP Tags mode is active
- [ ] **DESK-08**: Checkbox defaults on startup (no persistence between sessions)

### Cross-App

- [ ] **XAPP-01**: Both web and desktop apps produce identical search results for the same Responsa query
- [ ] **XAPP-02**: All Responsa logic lives in genizah_core.py (shared) -- no search logic in UI code

## Future Requirements

Deferred to v5.8.0 or later. Tracked but not in current roadmap.

### Option III Features

- **OPT3-01**: Per-component search mode (exact/variants/wildcard per column)
- **OPT3-02**: Scope search (sentence/paragraph/document)
- **OPT3-03**: Per-component negation (NOT clause)
- **OPT3-04**: Bidirectional sync between tabular and text field

### Advanced Spacing

- **SPACE-01**: `\s*` per char applied to variant/JA expansions (currently only originals)
- **SPACE-02**: Dual-track search with content_nospaces Tantivy field
- **SPACE-03**: N-gram character-level index for space-agnostic search

### Persistence & Polish

- **PERSIST-01**: Desktop QSettings persistence for Responsa checkbox states
- **PERSIST-02**: Saved Responsa queries (serialized SearchPlan)
- **PERSIST-03**: Query history with Responsa metadata

### Deferred from v5.6.0

- **SRCH-01**: PGP transcriptions indexed in Tantivy (Phase 13, reverted)
- **SRCH-02**: User corrections indexed in Tantivy
- **SRCH-03**: Search filter toggle (all/transcriptions only/exclude)
- **EXPAND-01**: NLI joins import (~424K PartOf relationships)

## Out of Scope

Explicitly excluded. Documented to prevent scope creep.

| Feature | Reason |
|---------|--------|
| Transcription search in Tantivy | Deferred from v5.6.0, needs server-side index architecture |
| PGP people/places integration | Separate milestone |
| Map-based geographic browse | Separate milestone |
| Two-way tabular<->text sync | Complexity too high for MVP, one-way sufficient |
| `##` double-hash syntax | Checkbox approach preferred (doc 5 recommendation) |
| Query preview line | User explicitly excluded ("no need for expanded query preview every time") |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| CORE-01 | Phase 14 | Pending |
| CORE-02 | Phase 14 | Pending |
| CORE-03 | Phase 14 | Pending |
| CORE-04 | Phase 14 | Pending |
| CORE-05 | Phase 14 | Pending |
| CORE-06 | Phase 14 | Pending |
| CORE-07 | Phase 14 | Pending |
| CORE-08 | Phase 14 | Pending |
| WEB-01 | Phase 15 | Pending |
| WEB-02 | Phase 15 | Pending |
| WEB-03 | Phase 15 | Pending |
| WEB-04 | Phase 16 | Pending |
| WEB-05 | Phase 16 | Pending |
| WEB-06 | Phase 15 | Pending |
| WEB-07 | Phase 15 | Pending |
| DESK-01 | Phase 15 | Pending |
| DESK-02 | Phase 15 | Pending |
| DESK-03 | Phase 15 | Pending |
| DESK-04 | Phase 16 | Pending |
| DESK-05 | Phase 16 | Pending |
| DESK-06 | Phase 15 | Pending |
| DESK-07 | Phase 15 | Pending |
| DESK-08 | Phase 15 | Pending |
| XAPP-01 | Phase 17 | Pending |
| XAPP-02 | Phase 14 | Pending |

**Coverage:**
- v5.7.0 requirements: 25 total
- Mapped to phases: 25
- Unmapped: 0

---
*Requirements defined: 2026-02-09*
