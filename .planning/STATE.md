---
gsd_state_version: 1.0
milestone: v8.2.0
milestone_name: Web Joins Lab
status: Roadmap created 2026-06-17
stopped_at: Phase 117 context gathered
last_updated: "2026-06-17T13:32:02.302Z"
last_activity: 2026-06-17 — Roadmap for v8.2.0 Web Joins Lab created (5 phases, 37 requirements mapped)
progress:
  total_phases: 5
  completed_phases: 0
  total_plans: 0
  completed_plans: 0
  percent: 0
---

# Project State

## Project Reference

See: .planning/PROJECT.md

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Phase 117 — vertical-spine (not yet started)

## Current Position

Phase: Not started (roadmap created, awaiting plan-phase 117)
Plan: —
Status: Roadmap created 2026-06-17
Last activity: 2026-06-17 — Roadmap for v8.2.0 Web Joins Lab created (5 phases, 37 requirements mapped)

## Accumulated Context

### Key Decisions (v8.2.0)

- **Vertical spine first (Phase 117):** The `WebSearchExecutor` adapter is the riskiest seam — it wraps `state.searcher.execute_search` directly, off the event loop, NOT `/api/search` (which omits `text_position`/`corpus_scope`). Phase 117 proves this seam end-to-end before building the full feature surface.
- **Build order (5 phases, condensed from an initial 9 per user request 2026-06-17 — spine kept thin):** Spine (117) → Joins, Entry & Full Builders (118) → Candidates, Compare & Visual Similarity (119) → Actions & Persistence (120) → i18n Polish (121).
- **Persistence = server-side per-session, NOT browser localStorage:** `web/safe_storage.py` wraps server-side `app.storage.user` (NiceGUI session cookie). `browser` = small encrypted cookie; `client` dies on refresh; `tab` is volatile per-tab. Decision: use `safe_user_*` helpers (survives refresh for anonymous users, keyed by session cookie). Persist inputs + triage only; re-run search on restore (avoids search-history payload-bloat class of bug).
- **Phase 87 invariant preserved throughout:** Zero raw `app.storage.user` accesses; `tests/test_no_raw_storage_access.py` allowlist stays `[]`; CI-guarded on every phase.
- **No new Supabase schema this milestone:** Known-joins leverage existing pairwise-join path + BFS group; cross-device sync deferred to PST-F1 (future).
- **ANC-05 multitenant known-joins safety:** `fetch_connected_fragments` has a process-global cache — Phase 118 must surface only public/confirmed joins or implement user/status-aware cache isolation to prevent User A's unconfirmed joins leaking to User B.
- **ACT-02 bulk puzzle handoff:** `/puzzle?add=` currently accepts one `sys_id` — Phase 120 ships a bulk staging payload/API so anchor + selected candidates open together.
- **Image resolution:** All images through existing per-provider proxies (NLI, Oxford, Cambridge, Manchester, JTS) + Phase-98 circuit breaker. No direct unguarded IIIF fetches.
- **Web-only milestone:** No GitHub Release (desktop-poll prompt avoidance per project convention). Version bumps but no GitHub Release object.

### Blockers/Concerns

None at roadmap creation. Phase 117 first plan will address the `WebSearchExecutor` adapter design in detail.

### Pending Todos

None yet. Begin with `/gsd:plan-phase 117`.

### Quick Tasks Completed

| # | Description | Date | Commit | Directory |
|---|-------------|------|--------|-----------|
| (none yet) | — | — | — | — |

## Deferred Items

Items carried forward from v8.1.0:

| Category | Item | Status | Deferred At |
|----------|------|--------|-------------|
| CONSENT-F1 | "Reset telemetry id" affordance in Settings | Future | v8.1.0 |
| ERR-01 | Handled/non-fatal error counting at high-value sites | Future | v8.1.0 |
| CRASH-F1 | "Send logs" flow for local faulthandler log | Future | v8.1.0 |
| WEB-F1 | Clean web `search_executed` query-text property (privacy gap) | Future | v8.1.0 |
| FLAG-F1 | PostHog feature flags / remote config on desktop | Future | v8.1.0 |
| PST-F1 | Cloud cross-device sync of Joins Lab candidate lists / triage | Future | v8.2.0 (desktop is local-only; nothing to sync with yet) |
| D-F12 | Regular Search ~8s wall-clock (profile-first) | Future | v8.1.0 |
| D-F18 | Context-menu LOCAL detection via `display` | Future | v8.0.0 |
| JSA-01 | Anchor parallels seeding (Component B) | Future | v8.0.0 |
| JSA-02 | Corpus-driven suggest-then-search completion (Component B) | Future | v8.0.0 |
| JSA-03 | Torn-word completion (Component B) | Future | v8.0.0 |
| JWB-05 | Tear-side assist (Component B) | Future | v8.0.0 |

## Session Continuity

Last session: 2026-06-17T13:32:02.296Z
Stopped at: Phase 117 context gathered
Resume file: .planning/phases/117-vertical-spine/117-CONTEXT.md
Next step: `/gsd:plan-phase 117` (Vertical Spine)

## Performance Metrics

| Phase | Plan | Duration | Notes |
|-------|------|----------|-------|
| (none yet) | — | — | — |

## Decisions

(Accumulated during execution — none yet at roadmap creation)

## Operator Next Steps

- Run `/gsd:plan-phase 117` to plan the Vertical Spine
