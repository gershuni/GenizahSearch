---
gsd_state_version: 1.0
milestone: v7.10
milestone_name: Search API
status: executing
stopped_at: Plan 77-01 complete; ready for Plan 77-02 (lab_composition_search chunk_hits)
last_updated: "2026-04-27T16:49:18Z"
last_activity: 2026-04-27 -- Plan 77-01 complete (3 commits, 22 RED tests scaffolded, ~8 min)
progress:
  total_phases: 6
  completed_phases: 0
  total_plans: 5
  completed_plans: 1
  percent: 3
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-04-27)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Phase 77 — serializer-json-export

## Current Position

Phase: 77 (serializer-json-export) — EXECUTING
Plan: 2 of 5 (next; 77-01 complete)
Status: Executing Phase 77
Last activity: 2026-04-27 -- Plan 77-01 complete (envelope-echo state + Wave 0 RED tests)

Progress: [          ] 3% (0/6 phases complete; 1/5 Phase 77 plans complete)

**Phase queue (v7.10):**

1. **Phase 77** — Serializer & JSON Export (EXPORT-01..04) ← next
2. Phase 78 — /api/search + Hardening Shell (API-01,04,05,06,07 + HARDEN-01..05)
3. Phase 79 — /api/browse Drill-Down (API-03) — Codex-recommended: validates locator round-trip via real consumer before a second producer
4. Phase 80 — /api/parallels (API-02)
5. Phase 81 — Claude Skill Consumer (SKILL-01..03)
6. Phase 82 — Internal Documentation (DOC-01, DOC-02)

Next step: `/gsd-plan-phase 77` to decompose Phase 77 into plans.

## Performance Metrics

**Velocity:**

- Total plans completed: ~210 (across 15 shipped milestones)
- Average duration: ~12 min (historical)

**Recent Trend:**

- v7.9: 10 phases, 23 plans (complete 2026-04-17, internal milestone)
- v7.8: 4 phases, 9 plans (shipped 2026-04-15, ~14 hours wall clock)
- v7.7: 4 phases, 8 plans
- Trend: Stable

## Accumulated Context

### Decisions

See PROJECT.md Key Decisions table for full history.

**v7.10 roadmap-time decisions:**

- Serializer module is built **first** (Phase 77) so the JSON export and API responses share a single source of truth from day one — preventing drift before any consumer exists.
- Hardening primitives (rate limit, mode flag, error envelope, query/result caps, PostHog) bundle into the **first** API endpoint phase (Phase 78) rather than a separate hardening phase, so /api/parallels and /api/browse inherit them by reuse rather than retrofit.
- API-05 (drill-down locator) is mapped to Phase 78 only; Phase 79 inherits the locator on parallels responses as a behavioral consequence reflected in its success criteria. This keeps every requirement single-mapped while preserving the cross-phase obligation.
- The Claude skill (Phase 81) is the milestone's acceptance harness — it must run end-to-end against a live deployment before documentation closeout in Phase 82, so the docs reflect what shipped, not what was planned.

**Plan 77-01 decisions (2026-04-27):**

- Filter dict shape locked to **10 keys** matching the live snapshot at search.py:4232-4242 (HIGH-02 fix — earlier 6-key dict was incomplete, would not survive replay through search history restore).
- Search history restore extends to populate **state.last_results AND envelope-echo fields** drawn from the snapshot's stored query/mode/gap/filters (HIGH-01) — restored exports are now byte-identical-shape to live exports.
- Parallels history restore uses **state_snapshot['source_text'] + params dict** as canonical source, NOT inferred from p_state.results[0]['source_ctx'] (HIGH-03 — result rows lose chunk_size/mode/filters fidelity).
- Side-effect: `state.current_search_query` latent bug (declared at web/state.py:27, never assigned per RESEARCH §Pitfall 2) fixed at all 3 search-execute paths. Excel/Word filenames will produce meaningful filenames as a ride-along benefit.
- Wave 0 TDD: 22 RED tests written before implementation module exists. `pytest tests/test_search_serializer.py --collect-only` succeeds (file syntactically valid); each test fails with `ModuleNotFoundError: No module named 'shared.search_serializer'` until Plan 03 lands.

### Pending Todos

- Migrate desktop corrections fetch to shared corrections_service
- CUT-01: Remove read-only PGP tables from Supabase (legacy desktop users depend on them)
- Date range filter using CopyToDate (21K rows)
- Creation type filter via code_values (CreationTypeCode, 69K rows)

### Blockers/Concerns

- DESK-03/DESK-02 shared image helpers: ManuscriptViewerWidget and PuzzleCanvasWindow may share IIIF fetch / image adjustment code. Phase 69 discuss-phase must map this surface before extraction.
- WEBM-03 architectural risk: page-scoped state refactor changes runtime data flow, not just file layout. Phases 72-73 splits should be stable before attempting.
- v7.10 watch: existing `/api/*` routes (image proxies, puzzle uploads, NLI proxies) must remain unchanged through every phase touching `web/api.py`. Each phase gate spot-checks at least the image proxy + puzzle upload routes for response parity.

### Quick Tasks Completed

| # | Description | Date | Commit | Status | Directory |
|---|-------------|------|--------|--------|-----------|
| 260419-nwv | Bug: images don't fit the text on paired-leaf CUL shelfmarks (T-S NS 158.112) — parse_folio_label regex fix; CUL positional follow-up logged | 2026-04-19 | 5e87f55d | | [260419-nwv-bug-with-some-shelfmarks-images-esp-cul-](./quick/260419-nwv-bug-with-some-shelfmarks-images-esp-cul-/) |
| 260419-cfx | CUL CUDL positional canvas mismatch fix (H1) — folio+side resolver + NLI fallback in web `/api/cambridge_image` and desktop browse; H3 retracted (text-layer vs image-layer FL ids, not an IE bug) | 2026-04-19 | a854a5ee | Needs Review | [260419-cfx-cul-cudl-folio-side-mapping](./quick/260419-cfx-cul-cudl-folio-side-mapping/) |

## Session Continuity

Last session: 2026-04-27T16:49:18Z
Stopped at: Plan 77-01 complete; ready for Plan 77-02 (lab_composition_search chunk_hits, D-13 Path A)
Resume file: .planning/phases/77-serializer-json-export/77-02-PLAN.md

## Performance Metrics — Phase 77

| Plan | Duration | Tasks | Files | Commits |
|------|----------|-------|-------|---------|
| 77-01 | ~8 min | 3 | 4 | cdd91928, 2c5e94d5, d64ccb2b |
