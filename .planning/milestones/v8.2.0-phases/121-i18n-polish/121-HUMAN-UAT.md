---
status: partial
phase: 121-i18n-polish
source: [121-VERIFICATION.md]
started: 2026-06-21T14:38:04Z
updated: 2026-06-21T14:38:04Z
---

## Current Test

[awaiting human testing — blocked on NLI image API availability]

## Tests

### 1. SEED-010 — Joins Lab image-resolution + zoom defect (deferred, out of i18n scope)
expected: Across all three Joins Lab surfaces (main anchor pane, candidate grid, Compare modal), images for every provider (NLI, Oxford, CUDL/Cambridge, Manchester, JTS) resolve consistently — including the NLI-down fallback path — and the zoom controls work for every successfully-loaded image regardless of provider.
result: [pending]
notes: Found during the 2026-06-21 HE-mode UAT while NLI's image API was DOWN. LANGUAGE-INDEPENDENT (reproduces in English) → NOT an i18n-polish gap; it does not block the Phase-121 goal. Hillel explicitly DEFERRED it during sign-off (untestable during the NLI outage). Full diagnosis (three divergent resolver paths + zoom-init coupled to `<img> onload`), per-surface × per-provider matrix, and the proposed unify-into-one-breaker-aware-resolver fix are in `.planning/seeds/SEED-010-joins-lab-image-resolution-and-zoom.md`; tracked as a P2 row in `docs/OPEN_ISSUES.md`. MUST be tested with NLI both UP and DOWN — to be run as a dedicated `/gsd:debug` or small phase on a cloud branch before v8.2.0 ships.

## Summary

total: 1
passed: 0
issues: 0
pending: 1
skipped: 0
blocked: 0

## Gaps
