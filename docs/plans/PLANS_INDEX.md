# GenizahSearch - Plans Index

**Last Updated:** 2026-10-21

---

## Future Plans

| Plan | Status | Description |
|------|--------|-------------|
| [RESPONSA_TRUE_WILDCARDS_PLAN.md](RESPONSA_TRUE_WILDCARDS_PLAN.md) | Rev. 4, Codex SHIP (4 rounds, 2026-10-21); §9 decisions 7-8 taken 2026-10-21, 1-6 and 9 wait for Phase 0; Phases 0 and 1 may start | Responsa `*` as any sequence of letters through the index's pattern lookup, `#` kept as the grammatical list, whole words at the query's edges, `#word#` keeps its suffixes, Responsa reads pages first; measured on the local index |
| [NLI_THUMBNAIL_PREFETCH_PLAN.md](NLI_THUMBNAIL_PREFETCH_PLAN.md) | Plan (2026-10-21); four owner decisions pending (NLI terms, disk, rate, desktop) | Fill the server's NLI thumbnail cache ahead of time, so a Visual Similarity or Joins Lab grid never waits on NLI manifests; whether the desktop asks the server for thumbnails |
| [JOINS_LAB_VS_IMPROVEMENTS_PLAN.md](JOINS_LAB_VS_IMPROVEMENTS_PLAN.md) | PRs 1-5 and PR 6 step (a) merged 2026-10-19 (#407-#412, follow-ups #415 #416), not yet released; PR 6 step (b) waits on the owner | The owner's six Joins Lab / Visual Similarity ideas and a user's requests: Browse links in new tabs, a fuller Compare window, a share link, image sizes, sort and filters, Browse icons, thumbnail caching |
| [SHELFMARK_PREFIX_SCOPE_PLAN.md](SHELFMARK_PREFIX_SCOPE_PLAN.md) | Implemented; merged as PR #402 (`a6b7202e`, 2026-10-18), ships in 9.7.0 | A library prefix keeps its library: `Halper 77` and `The University of Manchester Library, Ms. P 45` reach that library's records (and the records that store the typed text) instead of records numbered alike elsewhere |
| [CATALOGUE_SHELFMARK_FORMS_PLAN.md](CATALOGUE_SHELFMARK_FORMS_PLAN.md) | Implemented; merged as PR #400 (`55ced1a1`, 2026-10-18), ships in 9.7.0 | Accept shelfmarks as the NLI catalogue writes them (`Cambridge University Library, Ms. T-S K 08.026`, `Mosseri, Jacques, Ms. VIII.402`) on every shelfmark input, with pickers where the web takes the first match today; autocomplete untouched |
| [AI_LISTINGS_LAUNCH_PLAN.md](AI_LISTINGS_LAUNCH_PLAN.md) | Plan (2026-10-11); owner decisions pending | The `/ai` page, homepage strip and `/chatgpt` switched to the ChatGPT plugin and Claude connector per listing, by configuration, with an announcement |
| [MCP_CHUNK_AND_VARIANT_LEVELS_PLAN.md](MCP_CHUNK_AND_VARIANT_LEVELS_PLAN.md) | Rev 3 (2026-10-11): built; Maximum and chunk levels held by measurement | Chunk parallels in the MCP server (`find_chunk_parallels`) and the website's Extended/Maximum variant levels in MCP and the public API |
| [MCP_SERVER_PLAN.md](MCP_SERVER_PLAN.md) | Codex SHIP (rev. 5, 2026-10-08); owner decisions recorded 2026-10-09; spike next | Public MCP server at `/mcp` over the `/api/chatgpt` facade, replacing the GenizahSearch GPT before OpenAI retires GPTs on 2026-12-11; ChatGPT plugin + Claude connector listings |
| [SEARCH_PERF_HANDOFF.md](SEARCH_PERF_HANDOFF.md) | In progress (step 1 on `ccr-b3b8c7ac-jyxlw3`) | Desktop search ~28 s -> index is 0.24 s of it; per-hit Python loop, duplicates, silent 50,000 cap; next steps + equivalence tool |
| [MOBILE_RESPONSIVE_PLAN.md](MOBILE_RESPONSIVE_PLAN.md) | Planned | Mobile/tablet responsive design |
| [JOIN_FINDER_IMPLEMENTATION_PLAN.md](JOIN_FINDER_IMPLEMENTATION_PLAN.md) | Planned | Direction-aware join finder, caching, and manuscript-view UI |
| [USER_TEXT_SEARCH_PLAN.md](USER_TEXT_SEARCH_PLAN.md) | Planned | User-added text search for parallels |
| [FGP_TRANSCRIPTIONS_INTEGRATION_PLAN.md](FGP_TRANSCRIPTIONS_INTEGRATION_PLAN.md) | Data ready, integration pending | Incorporate the FGP transcription corpus (45K rows, `sys_id`-keyed, in gitignored `fgp_data/`) into search + the transcription chooser |
| [PGP_TRANSLATION_QUALITY.md](PGP_TRANSLATION_QUALITY.md) | Deferred (2026-09-21) | ~28% of the restored PGP Hebrew translations are materially wrong; why the mechanical QC cannot see it, and the four remediation options |
| [FGP_CHOOSER_MILESTONE.md](FGP_CHOOSER_MILESTONE.md) | Readiness doc (prepared 2026-06-18; see its status header) | Milestone readiness doc for FGP transcriptions as a version-chooser source; the runbook says when to instantiate it as a GSD milestone |

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
