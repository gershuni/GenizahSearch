---
gsd_state_version: 1.0
milestone: v7.9
milestone_name: Decomposition
status: complete
stopped_at: v7.9 milestone complete — ready for release handoff
last_updated: "2026-04-19T00:00:00.000Z"
last_activity: 2026-04-19 -- Completed quick task 260419-nwv: parse_folio_label bifolio regex fix (CUL image-text mismatch bug); CUL positional follow-up logged in OPEN_ISSUES.md
progress:
  total_phases: 10
  completed_phases: 10
  total_plans: 23
  completed_plans: 23
  percent: 100
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-04-15)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** v7.9 milestone COMPLETE — release handoff next

## Current Position

Milestone: v7.9 Decomposition — COMPLETE (2026-04-17)
Phases: 10 of 10 complete (67, 68, 69, 70, 71, 72, 73, 74, 75, 76)
Plans: 23 of 23 complete
Last activity: 2026-04-19 - Completed quick task 260419-cfx: CUL CUDL positional canvas mismatch fixed via folio+side resolver with NLI fallback (web + desktop); H3 retracted

Progress: [██████████] 100%

Next step: `/release` to version-bump (likely 7.8.0 minor for the internal refactor + back-nav bugfix headline), draft What's New, code review, build, deploy, cut GitHub release. Release bundles: (a) v7.9 decomposition (internally invisible file reorg), (b) back-nav state loss fix from 75-03 (user-visible bugfix, regression origin commit 829cd7cf 2026-03-27). If server translation batch is complete, optionally bundle deduped fjms_enrichment.db + RunningTitle/FullText wiring per memory `project_bib_dedup_and_release.md`.

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
