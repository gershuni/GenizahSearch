---
gsd_state_version: 1.0
milestone: v5.6.0
milestone_name: milestone
status: Ready to execute
stopped_at: Phase 102 context gathered
last_updated: "2026-05-29T11:32:26.747Z"
progress:
  total_phases: 2
  completed_phases: 1
  total_plans: 7
  completed_plans: 2
  percent: 29
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-05-28)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** v7.15 SHIPPED 2026-05-28; release pipeline next (/release skill), then planning v7.16

## Current Position

Milestone: v7.15 My Library Visual — CLOSED 2026-05-28
Phases shipped: 99 (PDF Page Renderer), 100 (LOCAL PDF Image in ResultDialog + Browse), 101 (RTL fix + remnant cleanup + UAT follow-ons)
Next: `/release` bundles desktop installer + builds + GitHub Release; or `/gsd-new-milestone` to start v7.16

## Deferred Items

Items acknowledged and deferred at v7.15 milestone close on 2026-05-28 (`gsd-tools.cjs audit-open` reported 100 items):

| Category | Count | Notes |
|----------|-------|-------|
| Debug sessions | 40 | Same historical accumulation as v7.14 close; mostly diagnosed-not-closed entries predating v7.13. |
| UAT gaps | 1 | Phase 100 `100-HUMAN-UAT.md` — 0 pending scenarios (effectively done; status flag not flipped). |
| Quick tasks | 53 | Historical backlog (oldest from 2026-02). Use `/gsd-cleanup` to triage between milestones. |
| Pending todos | 5 | Largest: server-side search with email notification; NLI MARC crawl; unified metadata text search. |
| Unimplemented seeds | 1 | SEED-001 server-side IIIF image cache (dormant; blocked on NLI TOS). |

NEW deferred item from v7.15 UAT: D-F12 (regular Search ~constant 8s wall-clock investigation) logged in `docs/OPEN_ISSUES.md` for v7.16+ work. Recommended next-milestone approach: instrument hot path with 5-6 timing markers (Tantivy candidate fetch, regex post-filter, enrichment, highlight build, return-to-UI), profile 3 search shapes (LOCAL-only, Genizah unfiltered, Genizah filtered), then optimize the actual bottleneck — explicitly do NOT guess.

## Recently Closed Milestones

- **v7.15 My Library Visual** — shipped 2026-05-28; 3 phases (99, 100, 101); 7 plans; 6/6 PDFIMG-* requirements. PDF page image rendering alongside LOCAL extracted text in ResultDialog + Browse, RTL/bidi reflow fixes, "Re-index All" recovery button. See `.planning/milestones/v7.15-ROADMAP.md`.
- **v7.14 My Library — Local Document Search** — shipped 2026-05-24 (v7.14.0), closed 2026-05-27; 6 phases (95, 96, 97, 97.2/97.3 inserted, 98); 37 plans. Desktop local document search + Phase 98 NLI resilience.
- **v7.13 Research-Grade Downloads & PGP Filter** — shipped 2026-05-21 (v7.13.0), closed 2026-05-27; 2 phases (93, 94); 5 plans; 14/14 requirements.
- **v7.12 Multitenant Architecture (Path B)** — shipped 2026-05-18; 10 phases; 28 plans; 49/49 requirements.

## Accumulated Context

### Roadmap Evolution

- Phase 102 added (2026-05-29): PDF Extraction Reorder — adopt Meiri glyph-level parser (closes D-F13 letter-spaced emphasis + D-F14 rawdict reorder). First piece of v7.16 work; appended to the roadmap after the shipped v7.15 (Phases 99-101) per user choice of a single inserted phase rather than a full new milestone. Not yet planned.
- Phase 102 RE-SCOPED (2026-05-29) after Spike 001 (`.planning/spikes/001-meiri-glyph-reorder-vs-current/`, verdict PARTIAL): now "LOCAL PDF Text-Layer Extraction Rewrite (RTL-gated reorder + letter-spacing de-collapse)". Spike findings: (1) Meiri's reorder helps Hebrew order/headers/brackets but NOT letter-spacing, and HURTS Latin → must be RTL-gated, no LTR regression; (2) the dominant text-layer bug is letter-spacing fragmentation (אוצר הגאונים 46%), fixable via rawdict per-line adaptive gap de-collapse (prototyped); (3) a LARGE share of the real library is image-only scans → OCR (D-F2) deferred as optional opt-in extension `SEED-003`; (4) new failure mode D-F16 corrupt text-layer encoding (Vilna Shabbat) → detect+flag in 102. Catalog F-A..F-G in spike README. Still not planned — next: discuss/plan Phase 102.

## Session Continuity

Last session: 2026-05-29T09:40:33.800Z
Stopped at: Phase 102 context gathered
Resume file: .planning/phases/102-pdf-extraction-reorder-adopt-meiri-glyph-level-parser-d-f13-/102-CONTEXT.md
Next step: `/release` skill — version bump 7.15.0, What's New drafting, code review, build, deploy, GitHub release.
