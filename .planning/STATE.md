---
gsd_state_version: 1.0
milestone: v7.8
milestone_name: Server-Side Image Cache
status: ready_to_plan
stopped_at: null
last_updated: "2026-04-03"
last_activity: 2026-04-03
progress:
  total_phases: 3
  completed_phases: 0
  total_plans: 0
  completed_plans: 0
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-04-03)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Phase 62 - Investigation & Validation (v7.8 Server-Side Image Cache)

## Current Position

Phase: 62 (1 of 3 in v7.8 milestone)
Plan: 0 of TBD in current phase
Status: Ready to plan
Last activity: 2026-04-03 -- Roadmap created for v7.8 (3 phases, 18 requirements)

Progress: [░░░░░░░░░░] 0%

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

## Session Continuity

Last session: 2026-04-03
Stopped at: Roadmap created for v7.8, ready to plan Phase 62
Resume file: None
