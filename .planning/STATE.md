---
gsd_state_version: 1.0
milestone: v7.10
milestone_name: Search API
status: executing
stopped_at: Phase 77 context gathered
last_updated: "2026-04-27T16:27:23.688Z"
last_activity: 2026-04-27 -- Phase 77 planning complete
progress:
  total_phases: 6
  completed_phases: 0
  total_plans: 5
  completed_plans: 0
  percent: 0
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-04-27)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** v7.10 Search API — roadmap complete (Phases 77-82), Phase 77 next

## Current Position

Phase: 77 (Serializer & JSON Export) — Not started
Plan: —
Status: Ready to execute
Last activity: 2026-04-27 -- Phase 77 planning complete

Progress: [          ] 0% (0/6 phases complete)

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

Last session: 2026-04-27T10:59:22.871Z
Stopped at: Phase 77 context gathered
Resume file: .planning/phases/77-serializer-json-export/77-CONTEXT.md
