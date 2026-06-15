---
phase: 114-usage-analytics
plan: 01
subsystem: desktop-telemetry
tags: [telemetry, identity, session, posthog, desktop]
dependency_graph:
  requires: [Phase 111 telemetry foundation, Phase 112 consent UX, Phase 113 crash reporting]
  provides: [identity lifecycle, session foundation, ACTIVE_PING event, _telemetry_ready gate]
  affects: [genizah_app.py, desktop/telemetry.py, Plans 02+03 producers]
tech_stack:
  added: []
  patterns: [QTimer.singleShot deferred coordinator, try/except best-effort wrapper, per-instance session_id, _telemetry_session_started one-shot guard]
key_files:
  created: [tests/test_telemetry_phase114.py]
  modified: [desktop/telemetry.py, genizah_app.py]
decisions:
  - Identity uses current_user._uuid (raw Supabase UUID), never .id (int hash) — D-10
  - Identity-sync (_sync_telemetry_identity) runs UNCONDITIONALLY before session_start one-shot guard — HIGH-4 fix
  - _telemetry_ready() is the single producer gate for all usage events — MEDIUM-9 fix
  - session_end emitted at very top of closeEvent before any thread teardown — D-15
  - _app_shutting_down set at very top of closeEvent before session_end — D-09
metrics:
  duration: ~25min
  completed_date: "2026-06-15"
  tasks: 3
  files: 3
---

# Phase 114 Plan 01: Identity Lifecycle and Session Foundation Summary

Established the identity lifecycle and session foundation for Phase 114 usage analytics: ACTIVE_PING enum member, startup identity coordinator with identity-sync split from one-shot session_start, producer gate, login/logout/register wiring, and best-effort session_end on clean exit.

## What Was Built

**desktop/telemetry.py** — Added `ACTIVE_PING = 'desktop_active_ping'` to the `DesktopEvent` enum between `FEATURE_OPENED` and the Performance section. `_VALID_EVENT_VALUES` auto-rebuilds at import time; no other changes needed.

**genizah_app.py** — Three new methods + four wiring sites:

1. `_sync_telemetry_identity()` — Always-runs identity reconciler (NOT guarded by `_telemetry_session_started`). Calls `identify(user._uuid)` when logged in (D-10: `_uuid` only, never `.id`), or `reset_identity()` for stale `IDENTIFIED_USER_KEY` (D-12). Re-runs on every coordinator call so mid-session opt-out→opt-in re-identifies before any usage event (HIGH-4).

2. `_run_startup_telemetry_coordinator()` — Boot/opt-in sequence. Calls `_sync_telemetry_identity()` unconditionally BEFORE the `_telemetry_session_started` one-shot guard. Mints `_session_id` (uuid4), records `_session_start_date_utc`, emits `SESSION_START` with allowlisted env props only (session_id, ui_language, python_version, pyqt_version). Stub-calls `_setup_active_ping()` guarded by `hasattr` (Plan 03 adds the real method). Wired via `QTimer.singleShot(700)` in `on_startup_finished`, and re-invoked on mid-session opt-in from Settings toggle and first-run prompt.

3. `_telemetry_ready() -> bool` — Producer gate. Returns `bool(_telemetry_session_started)`. All Plans 02/03 producers gate on this so no usage event fires before `_session_id` exists (MEDIUM-9).

**Login/logout/register identity wiring** — `_show_login_dialog` and `_show_register_dialog` success blocks call `self._sync_telemetry_identity()` (D-13). `_do_logout` calls `telemetry.reset_identity()` (D-13).

**closeEvent** — Sets `self._app_shutting_down = True` at the very top (before session_end and all thread teardown), then emits `SESSION_END` exactly once via `_session_end_emitted` guard (D-09/D-15).

**tests/test_telemetry_phase114.py** — Wave-0 test scaffold with canonical autouse fixture (monkeypatches `load_app_config`/`save_app_config` on both `genizah_core` and `desktop.telemetry`, resets `ph` + `tel` state, fresh `queue.Queue(maxsize=10000)`). 14 tests covering:
- ACTIVE_PING enum member, track enqueues, consent gate (3 tests)
- Coordinator consent-off, identify _uuid not .id, allowlisted props, idempotent session_start, stale identity reset (5 tests)
- Re-opt-in HIGH-4: second identify fires, no second session_start (1 test)
- _telemetry_ready gate pre/post coordinator (1 test)
- Login identify, logout reset, session_end exactly once, double-close guard (4 tests)

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| Task 1 | 632dff97 | ACTIVE_PING enum + Wave-0 scaffold |
| Task 2 | 479413d3 | Startup coordinator + producer gate |
| Task 3 | bdd91b28 | Login/logout wiring + session_end + shutdown flag |

## Verification

- `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_phase114.py -q` — 14 passed
- `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_identity.py tests/test_telemetry_consent_gate.py -q` — 20 passed (no regressions)
- `grep -nE "identify\(\s*user\.id" genizah_app.py` — zero matches (D-10 enforced)
- `python -m ruff check desktop/telemetry.py genizah_app.py tests/test_telemetry_phase114.py` — all checks passed

## Deviations from Plan

None — plan executed exactly as written.

The one test that needed re-implementation was `test_reopt_in_reidentifies_without_second_session_start`: the original test body attempted to reset module state mid-sequence (confusing the test logic). Rewritten to avoid the `tel._reset_for_tests()` mid-test call and instead measure events in two clean phases (first opt-in → drain → opt-out without reset → opt-in again → drain). This tests the same behavior (HIGH-4) more precisely. Classified as a test-quality fix, not a deviation.

## Known Stubs

- `_setup_active_ping()` call in coordinator is guarded by `hasattr(self, '_setup_active_ping')` — Plan 03 adds the real method. The stub guard is intentional and documented in the task spec.

## Threat Flags

None — all T-114-01 through T-114-04 mitigations implemented as planned:
- T-114-01 (identity spoofing): identity uses `_uuid`, behavior test + grep assertion confirm no `.id` usage
- T-114-02 (session_start PII): test asserts absence of hostname/username/path keys
- T-114-03 (pre-consent events): coordinator consent gate + `_telemetry_ready()` gate tested
- T-114-04 (stale identity): stale key test passes; re-opt-in re-identify test passes

## Self-Check: PASSED

- desktop/telemetry.py: FOUND
- tests/test_telemetry_phase114.py: FOUND
- 114-01-SUMMARY.md: FOUND
- Commit 632dff97: FOUND
- Commit 479413d3: FOUND
- Commit bdd91b28: FOUND
