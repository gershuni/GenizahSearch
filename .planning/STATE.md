---
gsd_state_version: 1.0
milestone: v7.8
milestone_name: Structural Quality
status: defining_requirements
stopped_at: null
last_updated: "2026-04-14T12:00:00.000Z"
last_activity: 2026-04-14 -- Milestone v7.8 Structural Quality started (prev Image Cache deferred)
progress:
  total_phases: 0
  completed_phases: 0
  total_plans: 0
  completed_plans: 0
  percent: 0
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-04-14)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Defining requirements for v7.8 Structural Quality

## Current Position

Phase: Not started (defining requirements)
Plan: —
Status: Defining requirements
Last activity: 2026-04-14 — Milestone v7.8 Structural Quality started

## Performance Metrics

**Velocity:**

- Total plans completed: ~201 (across 14 shipped milestones)
- Average duration: ~12 min (historical)

**Recent Trend:**

- v7.7: 4 phases, 8 plans
- v7.6: 5 phases, 17 plans
- Trend: Stable

## Accumulated Context

### Decisions

See PROJECT.md Key Decisions table for full history.

Recent decisions affecting current work:

- NLI blocks ALL datacenter IPs (verified 2026-03-17) -- batch fetcher MUST run from residential IP
- INV-04 (NLI TOS outreach) is a hard go/no-go gate before Phase 63
- FETCH-02 requires 90%+ NLI-only coverage before cache-first rollout in Phase 64
- No CDN/S3 -- nginx static serving sufficient for current scale
- No EC2 read-through from NLI -- fundamentally infeasible due to IP blocking

### Pending Todos

- Migrate desktop corrections fetch to shared corrections_service
- CUT-01: Remove read-only PGP tables from Supabase (legacy desktop users depend on them)
- Date range filter using CopyToDate (21K rows)
- Creation type filter via code_values (CreationTypeCode, 69K rows)

### Blockers/Concerns

- NLI rate limit tolerance from residential IPs is unknown (Phase 62 will test)
- Storage estimate (86GB) based on single test image -- needs 1000-image validation (Phase 62)
- NLI TOS position on academic bulk caching unknown -- could kill Phase 63
- Six independent image-loading codepaths need unified resolver (Phase 64)

## Quick Tasks Completed

| Task ID | Date | Description | Version | Commits |
|---------|------|-------------|---------|---------|
| 260413-eqk | 2026-04-13 | SEO Round 2: bilingual meta, JSON-LD, perf quick-wins. Original 5 commits revised by 3 pre-deploy correction commits per Codex review. Ready for deploy. | 7.7.1 | c42c4fa5, 99018426, ddc2bc53, 48282467, e0cea8f2, aba02f6b, 945723ad, 06b808db |
| 260413-jil | 2026-04-13 | PageSpeed quick wins: html lang, aria-labels (10 buttons), color contrast (slate-400→slate-500, link override), heading order h3→h2, font-display: swap middleware, conditional iiif preconnect. Awaiting human Lighthouse verification. | pending | 3937384c, 1fcbe6b2 |

## Session Continuity

Last session: 2026-04-13
Stopped at: Quick task 260413-jil PageSpeed wins -- 2/3 tasks committed, awaiting human Lighthouse verification
Resume file: .planning/quick/260413-jil-pagespeed-quick-wins-a11y-perf/260413-jil-SUMMARY.md
