---
phase: 116-privacy-audit-ci-gate
plan: 02
subsystem: infra
tags: [telemetry, posthog, ssl, selftest, packaging, desktop]

requires:
  - phase: 116-privacy-audit-ci-gate
    provides: plan 01 (telemetry tests, _safe_context hardening, runbook, REQUIREMENTS amendment)

provides:
  - send_selftest_event_sync() in shared/posthog_server.py — synchronous, return-valued SSL/delivery probe
  - --telemetry-selftest / --telemetry-selftest-offline headless block in genizah_app.py __main__
  - SSL_OK / SSL_FAIL / NO_KEY / OFFLINE_OK exit tokens for release engineering

affects:
  - release engineering (--telemetry-selftest used at /release time on clean Windows VM)
  - Phase 116 Task 3 HUMAN-UAT (deferred to /release)
  - INFRA-06 requirement completion (deferred to milestone verification pass)

tech-stack:
  added: []
  patterns:
    - "send_selftest_event_sync(): synchronous return-valued POST sibling to send_crash_event_direct — one POST, returns status token, never raises, never touches queue/daemon"
    - "In-memory consent toggle (WR-03 pattern): _enabled_lock + _enabled=True in try/finally, never calls set_consent"
    - "SSL_OK/SSL_FAIL/NO_KEY driven by HTTP-status-checked synchronous return, NOT drop counter (REVIEWS HIGH #1)"

key-files:
  created: []
  modified:
    - shared/posthog_server.py
    - genizah_app.py

key-decisions:
  - "send_selftest_event_sync() returns NO_KEY without making any network call when no key is configured — distinct from SSL_FAIL (REVIEWS HIGH #1)"
  - "Offline arm (--telemetry-selftest-offline) makes zero network calls and returns OFFLINE_OK immediately — smoke token only, not delivery proof"
  - "In-memory consent toggle via _enabled_lock, never set_consent() — config.pkl untouched (D-04/D-05)"
  - "Task 3 HUMAN-UAT deferred to /release: code is complete, awaiting frozen exe build on clean no-Python Windows VM"
  - "INFRA-06 NOT marked complete — deferred to milestone verification pass per 116-VERIFICATION.md"

requirements-completed: []  # INFRA-06 deferred to milestone verification pass — NOT flipped here

duration: 15min
completed: 2026-06-16
---

# Phase 116 Plan 02: Telemetry SSL Self-Test Summary

**Synchronous `send_selftest_event_sync()` + `--telemetry-selftest` headless CLI block for frozen-exe SSL/delivery proof at /release time**

## Performance

- **Duration:** ~15 min
- **Started:** 2026-06-16T11:45:00Z
- **Completed:** 2026-06-16T12:00:00Z
- **Tasks:** 2 of 3 complete (Task 3 deferred — HUMAN-UAT at /release)
- **Files modified:** 2

## Accomplishments

- Added `send_selftest_event_sync(timeout=2.0) -> str` to `shared/posthog_server.py`: one synchronous POST, returns `SSL_OK` (HTTP 2xx) / `SSL_FAIL` (any failure) / `NO_KEY` (unconfigured), never raises, never touches queue or drop counter, exported in `__all__`
- Added `--telemetry-selftest` / `--telemetry-selftest-offline` headless block to `genizah_app.py __main__` (before QApplication), toggling consent in-memory only (WR-03 pattern, no `config.pkl` write)
- Online arm drives SSL_OK/exit-0 SOLELY off the synchronous helper's HTTP-status return (NOT `get_dropped_event_count` — REVIEWS HIGH #1 fix confirmed)
- Offline arm prints `OFFLINE_OK` fast with zero network calls (smoke token)
- All 4 required exit tokens present: `SSL_OK` / `SSL_FAIL` / `NO_KEY` / `OFFLINE_OK`

## Task Commits

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Add send_selftest_event_sync() to shared/posthog_server.py | `7f943598` | shared/posthog_server.py |
| 2 | Add --telemetry-selftest / --telemetry-selftest-offline block | `9df8897b` | genizah_app.py |
| 3 | HUMAN-UAT: clean no-Python Windows VM SSL proof | DEFERRED | (no source change) |

## Files Created/Modified

- `shared/posthog_server.py` — added `send_selftest_event_sync()` function + `__all__` export (49 lines added)
- `genizah_app.py` — added `--telemetry-selftest` / `--telemetry-selftest-offline` block before QApplication (44 lines added)

## Decisions Made

- `send_selftest_event_sync()` returns `NO_KEY` without any network call when unconfigured — this is a distinct signal from `SSL_FAIL` (REVIEWS HIGH #1: the drop counter only counts `queue.Full` and is NOT a delivery signal; conflating them would falsely pass when nothing was delivered)
- Offline arm emits `OFFLINE_OK` immediately with zero network calls — it is a smoke token proving the no-GUI/fast-return code path ran, NOT a delivery proof (the real offline degradation is proven by the D-06 HUMAN-UAT normal offline launch)
- In-memory consent toggle pattern: `with _tel._enabled_lock: _tel._enabled = True` in `try/finally` (WR-03, matches `desktop/telemetry.py:1756-1768`) — NEVER calls `set_consent()` to avoid writing `config.pkl`
- Task 3 HUMAN-UAT is deferred to `/release` time: the code is complete but the proof requires a frozen exe on a clean no-Python Windows VM (no such build exists in this session)
- INFRA-06 NOT flipped to Complete: per `116-VERIFICATION.md`, completion is gated on the Task 3 HUMAN-UAT at /release time

## Deviations from Plan

**1. [Rule 1 - Bug] Inline comment referenced `get_dropped_event_count` within 1200 chars of selftest flag**
- **Found during:** Task 2 verification (AST_OK check)
- **Issue:** The plan's verify script asserts `'get_dropped_event_count' not in src.split('--telemetry-selftest',1)[1][:1200]`; an explanatory comment in the new block named that function
- **Fix:** Rephrased comment to "queue-saturation drop counter is NOT the delivery signal" — same meaning, no function name literal
- **Files modified:** genizah_app.py
- **Committed in:** `9df8897b` (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (comment phrasing to satisfy plan verification constraint)
**Impact on plan:** Trivial. The intent and behavior are identical.

## Task 3: CHECKPOINT — DEFERRED TO /RELEASE (HUMAN-UAT)

**Status:** Code-complete. Awaiting HUMAN-UAT at `/release` time.

**What is awaited:** A human running `GenizahSearchPro.exe --telemetry-selftest` on a CLEAN no-Python Windows VM:
1. `.\GenizahSearchPro.exe --telemetry-selftest` → expect stdout `SSL_OK` + exit code 0 (proves certifi/SSL bundled)
2. Confirm `desktop_selftest` event in PostHog project 134161 (EU) — closes Phase 114 live-delivery UAT
3. `.\GenizahSearchPro.exe --telemetry-selftest-offline` (network adapter disabled) → expect `OFFLINE_OK` fast
4. Normal launch with adapter disabled → app usable + silent (real offline-degradation proof)

**This task closes:** SC#3 (frozen-binary SSL) + Phase 114 live-delivery UAT (RESEARCH.md A1)

## Issues Encountered

None beyond the comment phrasing deviation documented above.

## User Setup Required

None — no external service configuration required for the code changes. Task 3 HUMAN-UAT is performed at `/release` time by the release engineer.

## Self-Check

**Files exist:**
- [x] `shared/posthog_server.py` modified (send_selftest_event_sync added)
- [x] `genizah_app.py` modified (--telemetry-selftest block added)

**Commits exist:**
- [x] `7f943598` — feat(116-02): add send_selftest_event_sync()
- [x] `9df8897b` — feat(116-02): add --telemetry-selftest block

## Self-Check: PASSED

## Next Phase Readiness

- Task 3 HUMAN-UAT is the only remaining gate for INFRA-06 and SC#3
- At `/release` time: build `GenizahSearchPro.exe`, run `--telemetry-selftest` on clean Windows VM, confirm SSL_OK + PostHog event, OFFLINE_OK fast, offline launch silent
- All source code complete and committed; no further code changes needed for this plan

---
*Phase: 116-privacy-audit-ci-gate*
*Completed (Tasks 1-2): 2026-06-16*
*Task 3: DEFERRED to /release HUMAN-UAT*
