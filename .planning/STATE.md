---
gsd_state_version: 1.0
milestone: v7.13
milestone_name: "Research-Grade Downloads & PGP Filter"
status: roadmap-locked
stopped_at: "v7.13 roadmap locked 2026-05-19 + scope-clarified same day (Phase 93 web-only confirmed; Phase 94 expanded to web + desktop xlsx; EXPORT-META-09 added); 14/14 reqs mapped, traceability locked; awaiting `/gsd-review-backlog` to rename 999.2-* / 999.3-* directories, then `/gsd-plan-phase 93` and `/gsd-discuss-phase 94 --revise` (CONTEXT.md needs desktop-parity refresh) followed by `/gsd-plan-phase 94`."
last_updated: "2026-05-19T00:00:00.000Z"
last_activity: 2026-05-19 -- Completed quick task 260519-9pk: re-opened P1 web memory leak in OPEN_ISSUES.md after 11h soak measured 411 MB/hr growth (⚠️ secondary-leak verdict); follow-up attribution phase scoped at .planning/todos/pending/2026-05-19-leak-attribution-phase.md
progress:
  total_phases: 2
  completed_phases: 0
  total_plans: 5
  completed_plans: 0
  percent: 0
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-05-19)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** v7.13 Research-Grade Downloads & PGP Filter — promote backlog 999.2 (post-search PGP filter) and 999.3 (multi-sheet xlsx + 3 JSON flags) into phases 93 + 94.

## Current Position

Phase: Not started (roadmap locked)
Plan: —
Status: Roadmap locked; awaiting backlog rename + planning
Last activity: 2026-05-19 -- Completed quick task 260519-9pk: re-opened P1 web memory leak (OPEN_ISSUES.md); 411 MB/hr post-soak verdict ⚠️ secondary leak

Progress: [          ] v7.13 0% (0/14 reqs, 0/5 plans, 0/2 phases)

## Phase Plan Estimates

| Phase | Name | Reqs | Plan slots (est) | Scope | CONTEXT.md status |
|-------|------|------|------------------|-------|-------------------|
| 93    | PGP Filter on `/search` | 5 | 1 | web only | LOCKED (`.planning/phases/999.2-filtering-by-pgp/999.2-CONTEXT.md`) |
| 94    | Research-Grade Export Metadata | 9 | 4 | web + desktop xlsx (JSON + state plumbing web-only) | LOCKED, BROADENED 2026-05-17, FURTHER EXPANDED 2026-05-19 with desktop parity (`.planning/phases/999.3-adding-pgp-to-downloaded-data/999.3-CONTEXT.md`); CONTEXT.md needs refresh OR planner incorporates EXPORT-META-09 directly; prior `999.3-01-PLAN.md` SUPERSEDED |

**Total:** 14 requirements, 5 plan slots (estimated), 2 phases.

Phase 93 and Phase 94 are independent (neither depends on the other) and can ship in parallel.

## Promoted Backlog (this milestone)

| Phase | Source slug | Name | Requirements |
|-------|-------------|------|--------------|
| 93    | 999.2-filtering-by-pgp | PGP filter on /search | PGP-FILTER-01..05 |
| 94    | 999.3-adding-pgp-to-downloaded-data | Research-grade xlsx + JSON metadata | EXPORT-META-01..08 |

Run `/gsd-review-backlog` after this milestone scaffold is committed to physically rename the phase directories (`999.2-*` → `93-*` and `999.3-*` → `94-*`) and update phase identifiers inside their CONTEXT.md / PLAN files.

## Deferred Items

Items acknowledged and deferred at v7.12 milestone close on 2026-05-18:

| Category | Count | Notes |
|----------|-------|-------|
| Debug sessions | 38 | Historical accumulation predating v7.12; spans many prior milestones |
| Verification gaps | 2 | `88-VERIFICATION.md` + `92.1-VERIFICATION.md` flagged `human_needed`; SWEEP-05 smoke run 2 PASS (2026-05-18) substantively closes both; status flag flip deferred |
| Quick tasks | 50 | Historical backlog (oldest from 2026-02); use `/gsd-cleanup` to triage between milestones |
| Pending todos | 5 | Largest: server-side search with email notification; NLI MARC crawl; unified metadata text search |
| Unimplemented seeds | 1 | SEED-001 server-side IIIF image cache (dormant; blocked on NLI TOS) |

The full `gsd-tools.cjs audit-open` report at close included 96 items. The v7.12-specific items (the 2 verification gaps + the `app-storage-user-assertion-sweep` debug session) are all substantively closed by the milestone work; only the status-flag bookkeeping was deferred to avoid blocking the close commit.

## Recently Closed Milestones

- **v7.12 Multitenant Architecture (Path B)** — shipped 2026-05-18; 10 phases (87-92 + 92.1/92.2 inserted + 999.1/999.4 promoted); 28 plans; 49/49 requirements satisfied. See `.planning/milestones/v7.12-ROADMAP.md`.

## Quick Tasks Completed

| # | Description | Date | Commit | Directory |
|---|-------------|------|--------|-----------|
| 260519-9pk | Re-open P1 web memory leak — investigate secondary leak after export-cap fix | 2026-05-19 | 0a91bc97 | [260519-9pk-re-open-p1-web-memory-leak-investigate-s](./quick/260519-9pk-re-open-p1-web-memory-leak-investigate-s/) |

## Session Continuity

Last session: 2026-05-19T00:00:00.000Z
Stopped at: "v7.13 roadmap locked + Phase 94 scope expanded to web + desktop xlsx (EXPORT-META-09 added). Phases 93+94 + per-milestone roadmap written. Traceability locked. Awaiting backlog rename + per-phase planning."
Resume file: None
Next step: Run `/gsd-review-backlog` to rename `999.2-*` / `999.3-*` phase directories into `93-*` / `94-*`. Then:
- `/gsd-plan-phase 93` — Phase 93 CONTEXT.md is ready, prior `999.2-01-PLAN.md` is a valid baseline, web only.
- `/gsd-discuss-phase 94 --revise` first to refresh CONTEXT.md with the desktop-parity scope (or planner incorporates EXPORT-META-09 directly from REQUIREMENTS.md), then `/gsd-plan-phase 94`. Prior `999.3-01-PLAN.md` is SUPERSEDED — re-plan from scratch covering web + desktop xlsx + web-only JSON.
Phases independent — order is human's choice.
