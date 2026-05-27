---
gsd_state_version: 1.0
milestone: none
milestone_name: (planning next after v7.14)
status: milestone_complete
stopped_at: v7.13 + v7.14 milestones CLOSED (retroactive reconciliation)
last_updated: "2026-05-27T00:00:00.000Z"
last_activity: 2026-05-27 -- v7.13 + v7.14 milestones closed via /gsd-complete-milestone (MILESTONES.md entries added, v7.13-REQUIREMENTS.md + v7.14-ROADMAP.md archived, REQUIREMENTS.md deleted, ROADMAP/PROJECT/RETROSPECTIVE updated)
progress:
  total_phases: 6
  completed_phases: 6
  total_plans: 37
  completed_plans: 37
  percent: 100
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-05-27)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** No active milestone — run `/gsd-new-milestone` to scope the next one.

## Current Position

Phase: none (between milestones)
Plan: n/a
Status: v7.13 + v7.14 both closed 2026-05-27. Latest shipped work: v7.14.0 (My Library) + Phase 98 NLI resilience.
Last activity: 2026-05-27 -- v7.13 + v7.14 milestones closed via /gsd-complete-milestone reconciliation

Progress: [██████████] v7.14 100% (6/6 phases, 37/37 plans) — CLOSED

## Phase Plan Estimates

| Phase | Name | Reqs | Plan slots (est) | Scope | CONTEXT.md status |
|-------|------|------|------------------|-------|-------------------|
| 93    | PGP Filter on `/search` | 5 | 1 | web only | LOCKED (`.planning/phases/93-filtering-by-pgp/93-CONTEXT.md`) |
| 94    | Research-Grade Export Metadata | 9 | 4 | web + desktop xlsx (JSON + state plumbing web-only) | LOCKED, BROADENED 2026-05-17, FURTHER EXPANDED 2026-05-19 with desktop parity (`.planning/phases/94-adding-pgp-to-downloaded-data/94-CONTEXT.md`); CONTEXT.md needs refresh OR planner incorporates EXPORT-META-09 directly; prior `94-01-PLAN.SUPERSEDED.md` SUPERSEDED |

**Total:** 14 requirements, 5 plan slots (estimated), 2 phases.

Phase 93 and Phase 94 are independent (neither depends on the other) and can ship in parallel.

## Promoted Backlog (this milestone)

| Phase | Source slug | Name | Requirements |
|-------|-------------|------|--------------|
| 93    | 999.2-filtering-by-pgp | PGP filter on /search | PGP-FILTER-01..05 |
| 94    | 999.3-adding-pgp-to-downloaded-data | Research-grade xlsx + JSON metadata | EXPORT-META-01..08 |

`/gsd-review-backlog` (2026-05-19) renamed the phase directories (`999.2-*` → `93-filtering-by-pgp` and `999.3-*` → `94-adding-pgp-to-downloaded-data`) and updated frontmatter `phase:` fields in all 5 plan files. Internal historical references to "999.2" / "999.3" inside plan bodies are preserved as-is for git-history continuity.

## Deferred Items

Items acknowledged and deferred at the v7.13 + v7.14 milestone close on 2026-05-27 (`gsd-tools.cjs audit-open` reported 104 items):

| Category | Count | Notes |
|----------|-------|-------|
| Debug sessions | 40 | Historical accumulation predating v7.13 (mostly diagnosed-not-closed); spans many prior milestones. Includes 2 post-97.2 UAT brief/critique entries (work already shipped). |
| UAT gaps | 3 phases | Phase 95 (3 pending scenarios), Phase 96 96-06/96-08 (0 pending — effectively done). My Library shipped as v7.14.0; scenarios substantively exercised in live use. |
| Verification gaps | 2 | Phase 95 + Phase 97 `human_needed` flags. Substantively closed by the shipped v7.14.0 release + 97.x hotfix chain; status flag not flipped. |
| Quick tasks | 53 | Historical backlog (oldest from 2026-02). Use `/gsd-cleanup` to triage between milestones. |
| Pending todos | 5 | Largest: server-side search with email notification; NLI MARC crawl; unified metadata text search. |
| Unimplemented seeds | 1 | SEED-001 server-side IIIF image cache (dormant; blocked on NLI TOS). |

The v7.13/v7.14-specific items are all substantively closed by the shipped releases; only status-flag bookkeeping and the long historical backlog (predating v7.12) were deferred. Recommend a `/gsd-cleanup` pass before the next milestone.

---

### Prior deferral — v7.12 milestone close on 2026-05-18:

| Category | Count | Notes |
|----------|-------|-------|
| Debug sessions | 38 | Historical accumulation predating v7.12; spans many prior milestones |
| Verification gaps | 2 | `88-VERIFICATION.md` + `92.1-VERIFICATION.md` flagged `human_needed`; SWEEP-05 smoke run 2 PASS (2026-05-18) substantively closes both; status flag flip deferred |
| Quick tasks | 50 | Historical backlog (oldest from 2026-02); use `/gsd-cleanup` to triage between milestones |
| Pending todos | 5 | Largest: server-side search with email notification; NLI MARC crawl; unified metadata text search |
| Unimplemented seeds | 1 | SEED-001 server-side IIIF image cache (dormant; blocked on NLI TOS) |

The full `gsd-tools.cjs audit-open` report at close included 96 items. The v7.12-specific items (the 2 verification gaps + the `app-storage-user-assertion-sweep` debug session) are all substantively closed by the milestone work; only the status-flag bookkeeping was deferred to avoid blocking the close commit.

## Recently Closed Milestones

- **v7.14 My Library — Local Document Search** — shipped 2026-05-24 (v7.14.0), closed 2026-05-27; 6 phases (95, 96, 97, 97.2/97.3 inserted, 98); 37 plans. Desktop local document search + Phase 98 NLI resilience. See `.planning/milestones/v7.14-ROADMAP.md`.
- **v7.13 Research-Grade Downloads & PGP Filter** — shipped 2026-05-21 (v7.13.0), closed 2026-05-27; 2 phases (93, 94); 5 plans; 14/14 requirements. See `.planning/milestones/v7.13-ROADMAP.md` / `v7.13-REQUIREMENTS.md`.
- **v7.12 Multitenant Architecture (Path B)** — shipped 2026-05-18; 10 phases (87-92 + 92.1/92.2 inserted + 999.1/999.4 promoted); 28 plans; 49/49 requirements satisfied. See `.planning/milestones/v7.12-ROADMAP.md`.

> Note (2026-05-27): v7.13 and v7.14 both shipped as app releases earlier but the GSD close ritual was skipped at the time; both were reconciled together on 2026-05-27 (MILESTONES.md entries, archives, REQUIREMENTS.md deletion).

## Quick Tasks Completed

| # | Description | Date | Commit | Status | Directory |
|---|-------------|------|--------|--------|-----------|
| 260519-9pk | Re-open P1 web memory leak — investigate secondary leak after export-cap fix | 2026-05-19 | 0a91bc97 | — | [260519-9pk-re-open-p1-web-memory-leak-investigate-s](./quick/260519-9pk-re-open-p1-web-memory-leak-investigate-s/) |
| 260519-hoi | Ship SEED-002 uid-only export payload (44x per-row reduction) | 2026-05-19 | 2a7440d6 | Verified | [260519-hoi-ship-seed-002-uid-only-export-payload](./quick/260519-hoi-ship-seed-002-uid-only-export-payload/) |

## Accumulated Context

### Roadmap Evolution

- Phase 96 added (2026-05-24): Completing My Library feature: add features and fix bugs
- Phase 97 added (2026-05-25): More LOCAL features
- Phase 98 added (2026-05-25): NLI Resilience — circuit-breaker and bounded-timeout hardening for all NLI/IIIF code paths
- Phase 97.2 inserted after Phase 97 (2026-05-26, URGENT): Recovery cascade hotfix — fix 5 interacting Phase 97 bugs (redundant tantivy.Index reopen leaking writer lock; stale `.tantivy-writer.lock` carried through os.rename; `discard_run` field-name failure on stale Phase 95 schema; missing `self._writer is None` guards; "Reset My Library" UX never implemented despite being referenced in 2 error messages). Triggered by user repro on a 100K-file Dropbox tree quit mid-scan. Phase 97.1 was the prior inline `/gsd-fast` hotfix for the freeze + WinError 3 storm (commit `2e1b846e`) and was never registered as a tracked phase.

## Session Continuity

Last session: 2026-05-26T15:02:14.462Z
Stopped at: Phase 97.3 context gathered
Resume file: .planning/phases/97.3-my-library-uat-stability/97.3-CONTEXT.md
Next step: `/gsd-review-backlog` (2026-05-19) already renamed the directories. Now:

- `/gsd-plan-phase 93` — Phase 93 CONTEXT.md is ready, prior `93-01-PLAN.md` (originally `999.2-01-PLAN.md`) is a valid baseline, web only.
- `/gsd-discuss-phase 94 --revise` first to refresh CONTEXT.md with the desktop-parity scope (or planner incorporates EXPORT-META-09 directly from REQUIREMENTS.md), then `/gsd-plan-phase 94`. Prior Plan 1 (now `94-01-PLAN.SUPERSEDED.md`) is SUPERSEDED — re-plan from scratch covering web + desktop xlsx + web-only JSON.

Phases independent — order is human's choice.
