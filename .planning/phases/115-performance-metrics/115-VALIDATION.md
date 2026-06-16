---
phase: 115
slug: performance-metrics
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-06-16
---

# Phase 115 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Derived from `115-RESEARCH.md` § Validation Architecture.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (existing) |
| **Config file** | none — standard pytest discovery |
| **Quick run command** | `pytest tests/test_telemetry_phase115.py -x` |
| **Full suite command** | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/ -x` |
| **Estimated runtime** | ~5 seconds (phase file); full suite minutes (run headless per `feedback_full_suite_testing_windows`) |

All telemetry tests mock at the `ph._event_queue` level (per `111-PATTERNS.md`). No live PostHog required.

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/test_telemetry_phase115.py -x`
- **After every plan wave:** Run the full headless suite (telemetry + AST guard files)
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** ~5 seconds (phase file)

---

## Per-Task Verification Map

> Task IDs (`115-NN-NN`) are assigned by the planner. Rows below map each phase requirement to its
> automated proof from `115-RESEARCH.md`; the planner/executor fills Task ID + Wave per row.

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| TBD | — | 0 | — | — | test scaffolding + `_reset_for_tests()` extension | unit | `pytest tests/test_telemetry_phase115.py --collect-only` | ❌ W0 | ⬜ pending |
| TBD | — | — | PERF-01 (search) | — | `perf_signal` emitted on SearchThread completion (completed runs only) | unit | `pytest tests/test_telemetry_phase115.py::test_search_thread_emits_perf_signal` | ❌ W0 | ⬜ pending |
| TBD | — | — | PERF-01 (indexing) | — | indexing event emitted with `duration_ms` + operation_kind constant | unit | `pytest tests/test_telemetry_phase115.py::test_indexing_complete_event_shape` | ❌ W0 | ⬜ pending |
| TBD | — | — | PERF-02 | — | `result_count` in summary is bucketed only (no raw int) | unit | `pytest tests/test_telemetry_phase115.py::test_perf_summary_buckets_only` | ❌ W0 | ⬜ pending |
| TBD | — | — | PERF-03 (aggregate) | — | accumulate 10 searches → 0 events; flush → exactly 1 event | unit | `pytest tests/test_telemetry_phase115.py::test_no_per_search_events` | ❌ W0 | ⬜ pending |
| TBD | — | — | PERF-03 (reset / D-06) | — | accumulator resets after flush (no double-count) | unit | `pytest tests/test_telemetry_phase115.py::test_accumulator_resets_on_flush` | ❌ W0 | ⬜ pending |
| TBD | — | — | PERF-03 (env config / D-05) | — | `GENIZAH_PERF_SAMPLE_N=2` skips odd runs; interval env-tunable | unit | `pytest tests/test_telemetry_phase115.py::test_sample_n_skips_runs` | ❌ W0 | ⬜ pending |
| TBD | — | — | D-09 (consent gate) | T-priv | `accumulate_performance()` no-ops when disabled | unit | `pytest tests/test_telemetry_phase115.py::test_accumulate_disabled_when_no_consent` | ❌ W0 | ⬜ pending |
| TBD | — | — | D-07 (nested payload) | T-priv | `perf_summary` container allowlisted; nested dict survives scrubber | unit | `pytest tests/test_telemetry_phase115.py::test_perf_summary_survives_scrubber` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_telemetry_phase115.py` — the 8 test cases above (new file)
- [ ] `desktop/telemetry._reset_for_tests()` — extend to clear `_perf_accumulator`, `_perf_last_flush_time`, `_perf_sample_counter` (edit existing function, no new file)

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Periodic flush actually fires on a ~30-min active-use cadence (not a naive timer) | PERF-03 / D-04 | Wall-clock-dependent; the active_ping mirror is unit-tested by forcing `_perf_last_flush_time` into the past, but real cadence under focus/resume is observational | Run the app, perform searches across >30 min of active use, confirm a single summary event flushes (PostHog desktop project) without per-search events |
| Close flush delivers final partial window on hard-exit paths | PERF-03 / D-09 | SIGKILL/crash exit behavior; `_flush_before_exit` bounded sync flush is unit-tested but real crash delivery is observational | Trigger app close + an abrupt exit; confirm the final `desktop_session_performance_summary` arrives |

---

## Existing Guard Tests That MUST Continue to Pass

| Test file | What it guards |
|-----------|----------------|
| `tests/test_telemetry_no_direct_posthog.py` | No `desktop/` file except `telemetry.py` may call `enqueue_event` |
| `tests/test_no_dynamic_telemetry_strings.py` | No forbidden UI accessors (`currentText()`, `windowTitle()`, paths) in telemetry call args |
| `tests/test_telemetry_allowlist.py` | All `DesktopEvent` values start with `desktop_` (new indexing member must comply) |
| `tests/test_no_raw_storage_access.py` | safe-storage chokepoint invariant |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 5s (phase file)
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
