---
status: partial
phase: 113-crash-reporting
source: [113-VERIFICATION.md]
started: 2026-06-15T10:14:20Z
updated: 2026-06-15T14:18:54Z
---

## Current Test

[testing complete — both items deferred to packaged build (frozen-binary specifics only)]

## Dev Smoke Test (2026-06-15) — pipeline VERIFIED

A source-run smoke test (`_tmp/crash_smoke.py`) drove the real chained `sys.excepthook`
crash path with the embedded key. Confirmed in PostHog project 134161:
- `desktop_crash` event delivered, `distinct_id=smoke-test-desktop-crash`, `platform=desktop`.
- Allowlisted props only (`os_family=Windows`, `os_version=10`, `app_version=8.0.0`,
  `exc_type=ValueError`, `exc_module`, `exc_lineno`, `error_fingerprint`, `is_background_thread`).
- ZERO leakage: the secret exception message, the fake file path, and traceback text were
  all absent from the event (`position()` checks = 0).
- The chained prior handler still wrote the local crash log (full-fidelity, on disk).
This de-risks the telemetry-delivery + scrubbing for item 1; the FROZEN-binary specifics
below (PyInstaller SSL/certifi + Qt-slot `sys.excepthook` reachability, and the native-crash
relaunch) remain to be confirmed on a packaged `.exe`.

## Tests

### 1. Frozen-binary Qt slot exception → crash_log.txt + desktop_crash event
expected: In a packaged PyInstaller .exe (not a dev run), an uncaught exception raised inside a Qt signal/slot still (a) writes `crash_log.txt` via the chained prior handler, and (b) emits exactly one `desktop_crash` event with allowlisted props only. Dev-build pytest passes, but frozen behavior (SSL/certifi bundle, sys.excepthook reachability under PyInstaller) is unconfirmed. Overlaps the Phase 116 frozen-binary SSL self-test success criterion.
result: blocked
blocked_by: release-build
reason: "Requires a packaged PyInstaller .exe; deferred to /release / Phase 116 frozen-binary self-test (user decision 2026-06-15)."

### 2. Real native C-extension crash → next-launch desktop_prior_crash
expected: Force a genuine native crash (e.g. a segfault in a C extension), relaunch the app, and confirm exactly one `desktop_prior_crash` event is emitted after consent is confirmed, carrying a fixed-enum `fatal_error` label (never raw faulthandler dump text). Requires a manual crash-then-relaunch cycle that cannot be exercised in the dev pytest harness.
result: blocked
blocked_by: release-build
reason: "Real shared-project key now baked into _TELEMETRY_KEY_DEFAULT (2026-06-15), so the key prerequisite is resolved. Remaining gate is ONLY a packaged build + a forced native crash. Deferred to /release / Phase 116 (user decision 2026-06-15)."

## Summary

total: 2
passed: 0
issues: 0
pending: 0
skipped: 0
blocked: 2

## Gaps

[none — both items are prerequisite-gated (release build + INFRA-01 real key), not code defects]
