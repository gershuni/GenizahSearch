---
gsd_state_version: 1.0
milestone: v8.1.0
milestone_name: Desktop Telemetry
status: executing
stopped_at: Phase 116 context gathered
last_updated: "2026-06-16T11:09:12.791Z"
last_activity: 2026-06-16
progress:
  total_phases: 6
  completed_phases: 5
  total_plans: 20
  completed_plans: 19
  percent: 83
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-06-11 after v8.0.0 close)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Phase 116 — privacy-audit-ci-gate

## Current Position

Phase: 116 (privacy-audit-ci-gate) — EXECUTING
Plan: 3 of 3
Status: Ready to execute
Last activity: 2026-06-16

Progress: [███████░░░] 67%

## Accumulated Context

### Key Decisions (v8.1.0)

- **Foundation-first invariant**: no event can fire before Phase 111 (consent gate + scrubber + allowlist) is complete and tested. Phases 113-115 all depend on Phase 111.
- **Consent storage**: `config.pkl` via `load_app_config`/`save_app_config` — NOT `QSettings`, NOT `session.json`. `session.json` is cleared by crash recovery.
- **Opt-out keeps install ID on disk** (per user decision — CONSENT-06). Re-opt-in preserves install continuity. `CONSENT-F1` (reset ID affordance) deferred.
- **UUID lifecycle**: minted inside `set_consent(True)` only — never at import time, never before user opts in. Always `uuid.uuid4()`, never `uuid1()` (MAC-based).
- **Crash hook**: wraps (chains) existing `_setup_crash_handler()` via `try/finally` — never replaces it. `crash_log.txt` must keep working after hooks are installed.
- **No `posthog` SDK**: reuse `shared/posthog_server.py` fire-and-forget queue only. Zero new pip deps, no spec-file changes.
- **Reuse the SHARED PostHog project** (INFRA-01 — FINAL 2026-06-14 per `.planning/research/POSTHOG-PROJECT-DECISION.md`; wired into code 2026-06-15, re-Codex-reviewed): desktop reuses the existing web project (id 134161, EU), **identity-aligned** with web (logged-in → Supabase `user.id`, IDENT-01..04 in Phase 114; logged-out → anon uuid4), web↔desktop separated by `platform: 'desktop'` + the `desktop_` event-name namespace. Uses the existing web publishable key (`web/main.py` `_posthog_key`, sourced from `POSTHOG_API_KEY` — already public in web client JS, so embedding in the .exe adds no new exposure). Key resolution in `_wire_transport_config` (2026-06-15): `GENIZAH_TELEMETRY_KEY` (all builds) → `POSTHOG_API_KEY` (source/dev only — a frozen .exe ignores it) → embedded `_TELEMETRY_KEY_DEFAULT` (baked at build; stays `_UNFILLED_KEY_SENTINEL` until then → drop locally). Accept only phc_; NEVER `POSTHOG_PERSONAL_API_KEY` (phx_). Default host already eu.i.posthog.com.
- **Session summary for perf** (PERF-03): aggregated per-session flush, not per-search events. ~tens/day target for heavy users, not ~50/day × dozens.

### Blockers/Concerns

- PyInstaller SSL cert bundle for `certifi` needs manual verification on a clean Windows VM (Phase 116 success criterion 3).
- ~~Embed the real phc_ key~~ **DONE (2026-06-15):** the publishable shared-project key `phc_CGTsV72…` is now baked into `_TELEMETRY_KEY_DEFAULT`, so a shipped .exe emits without any env var (guarded by `test_embedded_default_is_a_real_phc_key` + `test_frozen_build_uses_embedded_key`). Remaining gate for the two 113-HUMAN-UAT items is now ONLY the packaged .exe build (deferred to /release / Phase 116).

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
| INFRA-F2 | ~~Shared-emitter chokepoint tagging~~ **DONE 2026-06-15:** desktop registers `_desktop_default_props_hook` (via `register_scrub_hook`) while consent is on → every shared-queue event (incl. `nli_breaker_opened/closed`) gets `platform=desktop` + `$process_person_profile=False` (fill-when-absent). Verified live + `tests/test_telemetry_shared_emitter_tagging.py`. | — | v8.1.0 |
| INFRA-F3 | Identity hygiene — `$process_person_profile=False` on anonymous desktop events (already in IDENT-01..04 scope); Codex 2026-06-15 add: avoid bare `system` crash distinct_id (use `desktop:system` or drop) | Phase 114 (IDENT-01..04) | v8.1.0 |
| INFRA-F4 | Shared-project guardrails — platform filter on web insights, separate web/desktop dashboards, flag/experiment namespacing, MTU monitoring, release test "embedded key + no env → used" (Codex 2026-06-15) | Phase 116 + ops | v8.1.0 |

## Session Continuity

Last session: 2026-06-16T11:09:12.785Z
Stopped at: Phase 116 context gathered
Resume file: None
Next step: Phase 114 complete — proceed to Phase 115

## Performance Metrics

| Phase | Plan | Duration | Notes |
|-------|------|----------|-------|
| Phase 111-telemetry-foundation P01 | 4min | 2 tasks | 2 files |
| Phase 111-telemetry-foundation P02 | 9min | 3 tasks | 5 files |
| Phase 111-telemetry-foundation P03 | 2min | 1 tasks | 1 files |
| Phase 112-consent-ux P01 | 25min | 2 tasks | 2 files |
| Phase 112-consent-ux P02 | 8min | 2 tasks | 2 files |
| Phase 112-consent-ux P03 | 30min | 3 tasks | 2 files |
| Phase 113-crash-reporting P01 | 5min | 2 tasks | 6 files |
| Phase 113-crash-reporting P02 | 15min | 2 tasks | 4 files |
| Phase Phase 113-crash-reporting PP03 | 25min | 3 tasks | 4 files |
| Phase 114-usage-analytics P02 | 35min | 5 tasks | 2 files |
| Phase 114-usage-analytics P03 | 30min | 3 tasks | 4 files |
| Phase 114-usage-analytics P114-04 | 7min | 3 tasks | 2 files |
| Phase 115-performance-metrics P03 | 30min | 3 tasks | 2 files |
| Phase 116-privacy-audit-ci-gate P01 | 3min | 3 tasks | 2 files |

## Decisions

- [Phase 111-telemetry-foundation]: PRIV-03 AST guard delivered early in Phase 111-03 (vs Phase-116 slot) — no allowlist, absolute invariant, resolved-path exemption
- [Phase ?]: show_first_run_prompt() lazy-imports ConsentDialog; chained from _show_citation_reminder for strict ordering; activeModalWidget reschedule guard added
- [Phase ?]: [Phase 113-01]: send_crash_event_direct reads lock-free snapshot globals — no _capture_config_lock in crash path (D-05/REVIEWS HIGH-1)
- [Phase ?]: Phase 113-02: module-top send_crash_event_direct import requires monkeypatching tel.send_crash_event_direct in tests, not ph
- [Phase ?]: Phase 114-02: _telemetry_result_bucket module-level; corpus_scope via currentData(); drain thread isolation in autouse fixture; _app_shutting_down first-guard in all 3 emit helpers
- [Phase 114-04]: CR-114-01: PGP-tag per-run token (_pgp_tag_run_seq) + drain+disconnect before run-object install closes stale-slot race
- [Phase 114-04]: CR-114-02/03: _reset_search/_reset_composition now emit cancelled in the isRunning() branch (mirror stop_search pattern)
- [Phase 114-04]: CR-114-04: closeEvent session_end gated on _telemetry_ready() AND truthy _session_id — orphan session_end='' prevented
- [Phase 114-04]: CR-114-05: open_join_workbench(emit_telemetry=False) for restore suppression; CR-114-06: comp-resume uses _set_active_tab
