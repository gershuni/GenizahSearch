---
gsd_state_version: 1.0
milestone: v8.1.0
milestone_name: Desktop Telemetry
status: verifying
stopped_at: Completed 111-03-PLAN.md
last_updated: "2026-06-14T09:55:32.924Z"
last_activity: 2026-06-14
progress:
  total_phases: 6
  completed_phases: 1
  total_plans: 3
  completed_plans: 3
  percent: 17
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-06-11 after v8.0.0 close)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Phase 111 — telemetry-foundation

## Current Position

Phase: 111 (telemetry-foundation) — EXECUTING
Plan: 3 of 3
Status: Phase complete — ready for verification
Last activity: 2026-06-14

Progress: [░░░░░░░░░░] 0%

## Accumulated Context

### Key Decisions (v8.1.0)

- **Foundation-first invariant**: no event can fire before Phase 111 (consent gate + scrubber + allowlist) is complete and tested. Phases 113-115 all depend on Phase 111.
- **Consent storage**: `config.pkl` via `load_app_config`/`save_app_config` — NOT `QSettings`, NOT `session.json`. `session.json` is cleared by crash recovery.
- **Opt-out keeps install ID on disk** (per user decision — CONSENT-06). Re-opt-in preserves install continuity. `CONSENT-F1` (reset ID affordance) deferred.
- **UUID lifecycle**: minted inside `set_consent(True)` only — never at import time, never before user opts in. Always `uuid.uuid4()`, never `uuid1()` (MAC-based).
- **Crash hook**: wraps (chains) existing `_setup_crash_handler()` via `try/finally` — never replaces it. `crash_log.txt` must keep working after hooks are installed.
- **No `posthog` SDK**: reuse `shared/posthog_server.py` fire-and-forget queue only. Zero new pip deps, no spec-file changes.
- **Separate desktop PostHog project** (INFRA-01): distinct from web project 134161. The embedded write-only key is treated as abuse-tolerant (publishable), not a secret.
- **Session summary for perf** (PERF-03): aggregated per-session flush, not per-search events. ~tens/day target for heavy users, not ~50/day × dozens.

### Blockers/Concerns

- PyInstaller SSL cert bundle for `certifi` needs manual verification on a clean Windows VM (Phase 116 success criterion 3).
- New desktop PostHog project must be created and its `phc_...` key obtained before Phase 111 implementation starts (INFRA-01 — the key gets embedded in `desktop/telemetry.py`).

### Pending Todos

None specific to v8.1.0 yet.

## Deferred Items

| Category | Item | Status | Deferred At |
|----------|------|--------|-------------|
| CONSENT-F1 | "Reset telemetry id" affordance in Settings | Future | v8.1.0 (user chose keep-id-on-opt-out) |
| ERR-01 | Handled/non-fatal error counting at high-value sites | Future | v8.1.0 (hard crashes only in this milestone) |
| CRASH-F1 | "Send logs" flow for local faulthandler log | Future | v8.1.0 |
| WEB-F1 | Clean web `search_executed` query-text property (privacy gap) | Future | v8.1.0 (web follow-up) |
| FLAG-F1 | PostHog feature flags / remote config on desktop | Future | v8.1.0 |

## Session Continuity

Last session: 2026-06-14T09:55:32.918Z
Stopped at: Completed 111-03-PLAN.md
Resume file: None
Next step: `/gsd-plan-phase 111` — Telemetry Foundation

## Performance Metrics

| Phase | Plan | Duration | Notes |
|-------|------|----------|-------|
| Phase 111-telemetry-foundation P01 | 4min | 2 tasks | 2 files |
| Phase 111-telemetry-foundation P02 | 9min | 3 tasks | 5 files |
| Phase 111-telemetry-foundation P03 | 2min | 1 tasks | 1 files |

## Decisions

- [Phase 111-telemetry-foundation]: PRIV-03 AST guard delivered early in Phase 111-03 (vs Phase-116 slot) — no allowlist, absolute invariant, resolved-path exemption
