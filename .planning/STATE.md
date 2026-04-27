---
gsd_state_version: 1.0
milestone: v7.10
milestone_name: Search API
status: defining
stopped_at: Defining requirements for v7.10 Search API
last_updated: "2026-04-27T00:00:00.000Z"
last_activity: 2026-04-27 -- v7.10 Search API milestone started; PROJECT.md updated, requirements + roadmap pending
progress:
  total_phases: 0
  completed_phases: 0
  total_plans: 0
  completed_plans: 0
  percent: 0
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-04-27)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** v7.10 Search API — defining requirements

## Current Position

Phase: Not started (defining requirements)
Plan: —
Status: Defining requirements
Last activity: 2026-04-27 — Milestone v7.10 Search API started

Progress: [          ] 0%

Next step: define REQUIREMENTS.md, then spawn roadmapper to create ROADMAP.md (phases continue from v7.9's last phase 76 → first phase of v7.10 is phase 77).

## Performance Metrics

**Velocity:**

- Total plans completed: ~210 (across 15 shipped milestones)
- Average duration: ~12 min (historical)

**Recent Trend:**

- v7.8: 4 phases, 9 plans (shipped 2026-04-15, ~14 hours wall clock)
- v7.7: 4 phases, 8 plans
- v7.6: 5 phases, 17 plans
- Trend: Stable

## Accumulated Context

### Decisions

See PROJECT.md Key Decisions table for full history.

### Pending Todos

- Migrate desktop corrections fetch to shared corrections_service
- CUT-01: Remove read-only PGP tables from Supabase (legacy desktop users depend on them)
- Date range filter using CopyToDate (21K rows)
- Creation type filter via code_values (CreationTypeCode, 69K rows)

### Blockers/Concerns

- DESK-03/DESK-02 shared image helpers: ManuscriptViewerWidget and PuzzleCanvasWindow may share IIIF fetch / image adjustment code. Phase 69 discuss-phase must map this surface before extraction.
- WEBM-03 architectural risk: page-scoped state refactor changes runtime data flow, not just file layout. Phases 72-73 splits should be stable before attempting.

### Quick Tasks Completed

| # | Description | Date | Commit | Status | Directory |
|---|-------------|------|--------|--------|-----------|
| 260419-nwv | Bug: images don't fit the text on paired-leaf CUL shelfmarks (T-S NS 158.112) — parse_folio_label regex fix; CUL positional follow-up logged | 2026-04-19 | 5e87f55d | | [260419-nwv-bug-with-some-shelfmarks-images-esp-cul-](./quick/260419-nwv-bug-with-some-shelfmarks-images-esp-cul-/) |
| 260419-cfx | CUL CUDL positional canvas mismatch fix (H1) — folio+side resolver + NLI fallback in web `/api/cambridge_image` and desktop browse; H3 retracted (text-layer vs image-layer FL ids, not an IE bug) | 2026-04-19 | a854a5ee | Needs Review | [260419-cfx-cul-cudl-folio-side-mapping](./quick/260419-cfx-cul-cudl-folio-side-mapping/) |

## Session Continuity

Last session: 2026-04-17T11:21:20.287Z
Stopped at: Phase 75 context gathered
Resume file: .planning/phases/75-non-regression-verification/75-CONTEXT.md
