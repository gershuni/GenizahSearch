---
status: partial
phase: 115-performance-metrics
source: [115-VERIFICATION.md, 115-REVIEW.md]
started: "2026-06-16"
updated: "2026-06-16"
---

## Current Test

[awaiting human disposition of code-review warnings]

## Tests

### 1. Phase 115 test suite + regression pass
expected: `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_phase115.py` → 11 pass; full prior-phase telemetry/crash/posthog regression → all pass.
result: passed (orchestrator ran during execution — 11/11 phase115 + 17 incl. guard, and 273 passed / 1 xpassed regression)

### 2. WR-02 — LAB search/comp modes collapse to 'unknown'
expected: Decide whether `_PERF_ALLOWED_MODES` should include the 9 missing `lab_*` modes (`lab_keyword`, `lab_fuzzy`, `lab_responsa`, `lab_regex`, `lab_title`, `lab_shelfmark`, `lab_pgp_tags`, `lab_comp_variants`, `lab_comp_fuzzy`) so LAB perf data is attributed per mode, or accept the `'unknown'` collapse for v8.1.0 (LAB is lightly used). Data-quality issue, not privacy/safety.
result: [pending]

### 3. WR-01 — perf summary session_id cannot join to session_start/session_end
expected: Decide whether to plumb the per-process `self._session_id` into `_flush_perf_summary` (so `desktop_session_performance_summary.session_id` matches `session_start`/`session_end`), or accept the install-id value for v8.1.0. Fixing also corrects the wrong inline comment at `telemetry.py:~1578`.
result: [pending]

### 4. WR-04 — dead path-leak assertion in test
expected: Fix the tautological `assert (':\\' not in payload_repr and '/' not in payload_repr) or True` in `tests/test_telemetry_phase115.py` (~line 262) so the path-leak privacy check actually fails on a leak.
result: [pending]

## Summary

total: 4
passed: 1
issues: 0
pending: 3
skipped: 0
blocked: 0

## Gaps
