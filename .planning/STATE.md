# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-02-12)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** v5.8.0 FJMS Integration -- Phase 27 complete, ready for Phase 28

## Current Position

Phase: 27 of 28 (Domain Classifications) -- third of 4 in v5.8.0 -- Gap closure complete
Plan: 5 of 5 in current phase (all plans complete)
Status: Phase 27 complete -- domain classifications fully integrated in both apps
Last activity: 2026-02-13 -- Completed 27-05 desktop post-search domain filter

Progress: [██████████] 100% (11/11 plans)

## Performance Metrics

**Velocity:**
- Total plans completed: 81 (across all milestones)
- Average duration: ~8 min
- Total execution time: ~9.1 hours

**By Milestone:**

| Milestone | Phases | Plans | Total Time |
|-----------|--------|-------|------------|
| v1 | 1-7 | 18 | 173 min |
| v5.6.0 | 8-12 | 25 | ~134 min |
| v5.7.0 | 14-17 | 14 | ~140 min |
| v5.7.2 | 18-21 | 11 | ~1 day |
| v5.7.3 | 22-24 | 3 | 6 min |
| v5.8.0 | 25-28 | 11/11 | 47 min |

## Accumulated Context

### Decisions

- SQLite sidecar (`fjms_enrichment.db`) chosen over CSV+dict, Supabase, Tantivy (see FIST_STORAGE_ARCHITECTURE_DECISION.md)
- FTS5 schema included in sidecar now, UI deferred to future milestone
- Phase ordering: Data infra -> Joins -> Domains -> Catalog (user priority)
- AlmaId maps directly to libraries.csv system_number (no normalization needed)
- Catalog table has 322K rows (vs 243K estimated) due to richer join paths; more data is better
- fist_data/ and FIST_DB_BACKUP/ added to .gitignore (large binary files)
- Read-only SQLite via URI mode (file:path?mode=ro) enforces immutability at database level
- Module-level singleton pattern (get_fjms_service) matches existing shared service conventions
- Column names mapped to snake_case keys in returned dicts for Python convention
- FJMS joins merged as third source after user and PGP in fetch_connected_fragments
- Purple badge for FJMS source distinction (user=none, PGP=blue, FJMS=purple)
- Three-way join merge pipeline: user -> PGP -> FJMS with dedup at each stage
- GROUP BY + GROUP_CONCAT(DISTINCT) for multi-group join dedup at SQL level (not Python post-processing)
- Sources stored as list ('sources') instead of string ('source') in formatted_joins for multi-source badge support
- Merge-on-collision dedup: append source to existing entry instead of dropping overlapping FJMS entries
- Domain parent/child deduplication at display time (skip parent if child already shown)
- English domain names in URLs, Hebrew for display (language-aware UI)
- Domain filter uses multi-select with type-ahead, OR logic for filtering
- Standalone domain browsing capped at 500 results for performance
- Domain indicators show primary + "+N more" pattern on result cards
- Batch domain lookup method (get_domains_for_sys_ids) for efficient post-search collection (500-item batches)
- [Phase 27]: Post-search dynamic domain filtering with exclude-by-unchecking pattern in both web and desktop apps
- Web domain filter: post-search Domains button with checkbox tree dialog, client-side exclusion filtering
- Desktop domain filter: post-search button with hierarchical tree, setRowHidden() for instant filtering

### Blockers/Concerns

- Phase 13 (Transcription Search) still deferred -- needs server-side index architecture
- FIST.db is 13 GB; export script completed in ~20 seconds with batched inserts
- Thread-safe SQLite for NiceGUI web app (concurrent requests) -- RESOLVED in 25-02 (FjmsService thread_safe=True)

### Future Improvements

- FTS5 catalog search UI (schema ready, deferred to future milestone)
- NLI joins import (~424K PartOf relationships)
- Transcription search (Phase 13, needs server-side index architecture)

## Session Continuity

Last session: 2026-02-13
Stopped at: Completed 27-04-PLAN.md (web post-search domain filter)
Resume file: None
Notes: Phase 27 complete (5 of 5 plans). Web domain filter redesigned as post-search dynamic exclude filter with checkbox tree dialog. Desktop domain filter also redesigned (27-05, completed earlier). All domain classification features fully integrated in both web and desktop apps. Phase 28 (Catalog Search) is next.
