# GenizahSearch - Plans Index

**Last Updated:** 2026-03-26

---

## Future Plans

| Plan | Status | Description |
|------|--------|-------------|
| [MOBILE_RESPONSIVE_PLAN.md](MOBILE_RESPONSIVE_PLAN.md) | Planned | Mobile/tablet responsive design |
| [JOIN_FINDER_IMPLEMENTATION_PLAN.md](JOIN_FINDER_IMPLEMENTATION_PLAN.md) | Planned | Direction-aware join finder, caching, and manuscript-view UI |
| [USER_TEXT_SEARCH_PLAN.md](USER_TEXT_SEARCH_PLAN.md) | Planned | User-added text search for parallels |

---

## Bug Tracking

Active issue tracking is in **[../OPEN_ISSUES.md](../OPEN_ISSUES.md)**.

---

## Technical Specifications

| Spec | Description |
|------|-------------|
| [../specs/JOINS_TECHNICAL_SPEC.md](../specs/JOINS_TECHNICAL_SPEC.md) | Fragment joins system architecture |
| [../specs/JOINS_SIMPLIFIED_SPEC.md](../specs/JOINS_SIMPLIFIED_SPEC.md) | Simplified joins for first release |
| [../specs/PUZZLE_WEB_TECHNICAL_SPEC.md](../specs/PUZZLE_WEB_TECHNICAL_SPEC.md) | Web puzzle architecture and browser extension |

---

## Archived Plans

Completed and historical plans have been moved to `../archive/plans/`. This includes:

- Translation plans (TRANSLATION_MASTER_PLAN, TRANSLATION_QA_IMPROVEMENT_PLAN)
- Data integration plans (FIST_INTEGRATION_DESIGN, EXTERNAL_DATA_INTEGRATION_EXPLORATION, etc.)
- Infrastructure plans (IN_APP_UPDATE_PLAN, LISTS_IMPLEMENTATION_PLAN, etc.)
- Feature plans (HELP_UPDATE_PLAN, BROWN_BG_REMOVAL, INTERACTIVE_BG_REMOVAL_DESIGN, etc.)
- Responsa search design docs (7 files in `archive/plans/responsa-search/`)
- Pre-Supabase plans (LISTS_UNIFICATION_PLAN, JOINS_FEED_PLAN)

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
| v7.3.0 | Mar 2026 | Measurements dialog, bibliography dedup, desktop stability |
| v7.2.4 | Mar 2026 | JTS DPUL images, shelfmark search fixes, shared JS extraction |
| v7.0.0 | Mar 2026 | Fragment Puzzle, community publishing, browser extension |
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
