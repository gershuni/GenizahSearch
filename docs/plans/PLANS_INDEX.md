# GenizahSearch - Plans & Documentation Index

**Last Updated:** 2026-03-13

---

## Completed Plans

### Translation & Localization (v6.5.0, March 2026)

| Plan | Status | Description |
|------|--------|-------------|
| [TRANSLATION_MASTER_PLAN.md](TRANSLATION_MASTER_PLAN.md) | Completed | ~580K Dicta translations (HE/EN) for catalog data |
| [TRANSLATION_QA_IMPROVEMENT_PLAN.md](TRANSLATION_QA_IMPROVEMENT_PLAN.md) | Completed | QC heuristics, audit sampling, report UI, data fixes |

### Search Features (v5.7.0-v6.2.0, Feb-Mar 2026)

| Plan | Status | Description |
|------|--------|-------------|
| Responsa Search | Completed (Feb 2026) | Syntax parsing, grammatical expansion, Judeo-Arabic |
| Cross-Paragraph Search | Completed (Feb 2026) | Boundary-crossing parallel detection |
| [HELP_UPDATE_PLAN.md](HELP_UPDATE_PLAN.md) | Completed (Mar 2026) | Help page updates for new features |

*Design docs: [responsa-search/](responsa-search/) (6 design documents)*

### Data Integration (v5.8.0-v6.1.0, Feb 2026)

| Plan | Status | Description |
|------|--------|-------------|
| [FIST_INTEGRATION_DESIGN.md](FIST_INTEGRATION_DESIGN.md) | Completed | FJMS enrichment via SQLite sidecar |
| [FIST_STORAGE_ARCHITECTURE_DECISION.md](FIST_STORAGE_ARCHITECTURE_DECISION.md) | Completed | Architecture decision: SQLite sidecars |
| [EXTERNAL_DATA_INTEGRATION_EXPLORATION.md](EXTERNAL_DATA_INTEGRATION_EXPLORATION.md) | Completed | PGP + NLI data analysis and integration |
| [TRANSCRIPTIONS_INTEGRATION_DESIGN.md](TRANSCRIPTIONS_INTEGRATION_DESIGN.md) | Deferred | Phase 13 deferred (index build too slow for desktop) |

### Infrastructure (Jan-Feb 2026)

| Plan | Status | Description |
|------|--------|-------------|
| Supabase Migration | Completed (Jan 2026) | Replaced FastAPI with Supabase cloud |
| Library Location | Completed (Feb 2026) | Library codes for 217K records |
| [IN_APP_UPDATE_PLAN.md](IN_APP_UPDATE_PLAN.md) | Completed (Feb 2026) | Desktop in-app updates via Inno Setup |
| [LISTS_IMPLEMENTATION_PLAN.md](LISTS_IMPLEMENTATION_PLAN.md) | Completed | Core lists functionality |

---

## Future Plans

| Plan | Status | Description |
|------|--------|-------------|
| [LISTS_UNIFICATION_PLAN.md](LISTS_UNIFICATION_PLAN.md) | Planned | Unifying Projects & Lists (post-Supabase) |
| [MOBILE_RESPONSIVE_PLAN.md](MOBILE_RESPONSIVE_PLAN.md) | Planned | Mobile/tablet responsive design |
| [JOINS_FEED_PLAN.md](JOINS_FEED_PLAN.md) | Planned | Fragment joins in discovery feed |
| [USER_TEXT_SEARCH_PLAN.md](USER_TEXT_SEARCH_PLAN.md) | Planned | User-added text search for parallels |
| [FIX_PLAN.md](FIX_PLAN.md) | Ongoing | Bug fix tracking |

> **Note:** LISTS_UNIFICATION_PLAN.md and JOINS_FEED_PLAN.md have warning banners noting they pre-date the FastAPI removal (Jan 2026). Architecture references in those plans are outdated but the feature concepts remain valid.

---

## Bug Tracking

Active issue tracking is in **[../OPEN_ISSUES.md](../OPEN_ISSUES.md)** (primary).

[FIX_PLAN.md](FIX_PLAN.md) tracks specific bug fixes and their status.

---

## Technical Specifications

| Spec | Description |
|------|-------------|
| [../specs/JOINS_TECHNICAL_SPEC.md](../specs/JOINS_TECHNICAL_SPEC.md) | Fragment joins system architecture |
| [../specs/JOINS_SIMPLIFIED_SPEC.md](../specs/JOINS_SIMPLIFIED_SPEC.md) | Simplified joins for first release |

---

## Key Decisions Made

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Cloud backend | Supabase | Simple, reliable, free tier sufficient |
| Reference data | SQLite sidecars | Offline-capable, no server dependency |
| Translation engine | Dicta API | Best Hebrew NLP, batch-friendly |
| Translation QC | 10-heuristic scoring | Catches hallucinations, script errors |
| List colors | Inherited from projects | Consistency, simpler UX |
| User data storage | Cloud only | Data safety, cross-device sync |
| Service layer | shared/ + web shims | Both apps share business logic |

---

## Milestone History

| Version | Date | Key Features |
|---------|------|-------------|
| v6.5.0 | Mar 2026 | Filtered search, ~580K translations, translation toggle |
| v6.2.0 | Mar 2026 | Composition UX, session persistence, search history |
| v6.1.0 | Feb 2026 | Catalog browse (domain/author/work), FIST v5.0.0 |
| v6.0.0 | Feb 2026 | Local data architecture (PGP sidecar), offline browsing |
| v5.9.0 | Feb 2026 | NLI crossref, Cambridge/Manchester/JTS IIIF |
| v5.8.0 | Feb 2026 | FJMS integration, domain classifications |
| v5.7.0 | Feb 2026 | Responsa search, tabular query builder |
| v5.6.0 | Feb 2026 | Desktop parity, PGP integration |

---

*Documentation is a love letter to your future self.*
