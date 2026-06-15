---
status: partial
phase: 113-crash-reporting
source: [113-VERIFICATION.md]
started: 2026-06-15T10:14:20Z
updated: 2026-06-15T10:14:20Z
---

## Current Test

[awaiting human testing]

## Tests

### 1. Frozen-binary Qt slot exception → crash_log.txt + desktop_crash event
expected: In a packaged PyInstaller .exe (not a dev run), an uncaught exception raised inside a Qt signal/slot still (a) writes `crash_log.txt` via the chained prior handler, and (b) emits exactly one `desktop_crash` event with allowlisted props only. Dev-build pytest passes, but frozen behavior (SSL/certifi bundle, sys.excepthook reachability under PyInstaller) is unconfirmed. Overlaps the Phase 116 frozen-binary SSL self-test success criterion.
result: [pending]

### 2. Real native C-extension crash → next-launch desktop_prior_crash
expected: Force a genuine native crash (e.g. a segfault in a C extension), relaunch the app, and confirm exactly one `desktop_prior_crash` event is emitted after consent is confirmed, carrying a fixed-enum `fatal_error` label (never raw faulthandler dump text). Requires a manual crash-then-relaunch cycle that cannot be exercised in the dev pytest harness.
result: [pending]

## Summary

total: 2
passed: 0
issues: 0
pending: 2
skipped: 0
blocked: 0

## Gaps
