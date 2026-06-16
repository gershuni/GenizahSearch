---
status: partial
phase: 116-privacy-audit-ci-gate
source: [116-VERIFICATION.md, 116-02-PLAN.md Task 3]
started: 2026-06-16T12:00:00Z
updated: 2026-06-16T12:00:00Z
---

## Current Test

[awaiting human testing — runs at /release time, requires a frozen GenizahSearchPro.exe on a clean no-Python Windows VM]

## Tests

### 1. SC#3 — Clean no-Python Windows VM SSL proof + offline degradation + Phase 114 live-delivery
expected: |
  On a CLEAN Windows VM with NO Python installed:
  1. Network UP — run `.\GenizahSearchPro.exe --telemetry-selftest`
     → stdout `SSL_OK`, exit code 0. Proves certifi `cacert.pem` is bundled INSIDE the
       frozen binary (not borrowed from a dev-machine Python). `SSL_FAIL` = certifi/SSL not
       bundled or transport failed (RELEASE BLOCKER); `NO_KEY` = the embedded phc_ key was
       not baked into the build (a DIFFERENT release blocker than SSL).
  2. Confirm the `desktop_selftest` event appears in PostHog project 134161 (EU) — this
     closes the still-open Phase 114 "live PostHog event delivery" UAT.
  3. Disable the VM network adapter — run `.\GenizahSearchPro.exe --telemetry-selftest-offline`
     → `OFFLINE_OK` printed fast (well under ~2s; the offline arm makes NO network call).
  4. With the adapter still disabled, launch `GenizahSearchPro.exe` normally
     → app is usable and silent (no telemetry dialog, no delay, no crash). This is the REAL
       offline-degradation proof (INFRA-05 fire-and-forget for the frozen exe).
  Passing this single run satisfies ROADMAP SC#3, closes INFRA-06's last gate, and closes the
  Phase 114 live-delivery UAT.
result: [pending]

## Summary

total: 1
passed: 0
issues: 0
pending: 1
skipped: 0
blocked: 0

## Gaps

(none — the only outstanding item is the release-time clean-VM HUMAN-UAT above. PRIV-04 and
INFRA-06 completion flips in REQUIREMENTS.md are deliberately deferred to the milestone
verification pass that runs after this UAT passes — see the deferral note in 116-VERIFICATION.md.)
