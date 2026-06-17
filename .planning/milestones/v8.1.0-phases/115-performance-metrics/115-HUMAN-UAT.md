---
status: resolved
phase: 115-performance-metrics
source: [115-VERIFICATION.md, 115-REVIEW.md]
started: "2026-06-16"
updated: "2026-06-16"
---

## Current Test

[all items resolved — user approved "fix all 3"]

## Tests

### 1. Phase 115 test suite + regression pass
expected: phase115 suite + full prior-phase telemetry/crash/posthog regression pass under headless Qt.
result: passed — 11/11 phase115 (17 incl. guard); 290 passed / 1 xpassed across the full telemetry/crash/posthog regression after the WR fixes.

### 2. WR-02 — LAB search/comp modes collapse to 'unknown'
expected: decide expand allowlist vs accept collapse.
result: resolved — FIXED. Added the 9 missing `lab_*` modes to `_PERF_ALLOWED_MODES` (lab_keyword, lab_responsa, lab_fuzzy, lab_regex, lab_title, lab_shelfmark, lab_pgp_tags, lab_comp_variants, lab_comp_fuzzy). LAB searches now keep per-mode attribution. Commit b1902213.

### 3. WR-01 — perf summary session_id cannot join to session_start/session_end
expected: decide plumb per-process _session_id vs accept install-id.
result: resolved — FIXED. `_flush_perf_summary` now sources `_session_id` (set by genizah_app at session mint via `telemetry.set_session_id`), joinable to session_start/end; falls back to distinct/install id. Wrong inline comment corrected. Commit b1902213.

### 4. WR-04 — dead path-leak assertion in test
expected: fix the tautological assertion.
result: resolved — FIXED. `test_perf_summary_buckets_only` now asserts `':\\' not in payload_repr and '/' not in payload_repr` (no `or True`), so a leaked path fails the test. Commit b1902213.

## Summary

total: 4
passed: 4
issues: 0
pending: 0
skipped: 0
blocked: 0

## Gaps

None — all items resolved by fixing WR-01/WR-02/WR-04 (commit b1902213).
