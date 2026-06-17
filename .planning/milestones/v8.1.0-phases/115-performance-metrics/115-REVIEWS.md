---
phase: 115
reviewers: [codex]
reviewed_at: 2026-06-16
plans_reviewed: [115-01-PLAN.md, 115-02-PLAN.md, 115-03-PLAN.md, 115-04-PLAN.md]
risk_assessment: MEDIUM
---

# Cross-AI Plan Review — Phase 115 (Performance Metrics)

> Adversarial pre-execution review. The internal gsd-plan-checker already passed (0 blockers,
> 12/12 dimensions). Codex's job here was the plan↔code-drift pass: catch wrong assumptions about
> the live PyQt6/telemetry code, Qt thread-safety hazards, and privacy gaps. It found 3 HIGH issues.

## Codex Review

**Summary**

The plans are directionally solid and mostly respect the Phase 115 boundary: they keep emission behind `desktop/telemetry.py`, avoid per-search PostHog events, use Qt signals for worker-to-UI handoff, and split the work cleanly. The adversarial issues are not about the overall architecture; they are about privacy buffer semantics, stale run-state attribution, and a few live-code mismatches that could produce wrong metrics or false test confidence.

**Strengths**

- Uses the telemetry chokepoint and enum/allowlist model consistently.
- The accumulator design satisfies "never one event per search" if implemented as specified.
- Search-thread telemetry crosses threads as numeric `perf_signal(float, int)`, not by calling telemetry from `run()`.
- Periodic flush reuses the existing active-ping timer pattern rather than adding a naive 30-minute timer.
- `LabRebuildWorker.finished_signal(float, int)` appears low-risk: live code shows only one connected slot to update.
- Wave 2 parallelism is valid: 115-03 and 115-04 have no file overlap.

**Concerns**

- **[HIGH] 115-02 misses opt-out buffer clearing.** The perf accumulator is an unsent telemetry buffer, but `set_consent(False)` currently drains only the PostHog queue (`desktop/telemetry.py:522`). If a user opts out after searches accumulate and later opts back in, stale pre-opt-out summary data could be flushed. This violates the spirit of CONSENT-08.

- **[HIGH] 115-03 Task 2 misattributes perf events.** `_on_perf_signal` checks `_current_search_run` first and only falls back to `_current_comp_search_run`. Live code does not clear `_current_search_run` after completion, so a later composition perf signal can be recorded as the stale regular search mode/corpus.

- **[HIGH] 115-02 uses `mode` as a nested `perf_summary` dict key without validation.** The scrubber recurses into dict values, but it does not scrub dict keys (`desktop/telemetry.py:245`). Since `accumulate_performance()` is a public API taking `mode: str`, this weakens the structural privacy guarantee.

- **[MEDIUM] No `session_id` on `desktop_session_performance_summary`.** That makes the "per-session summary" hard to join to `desktop_session_start` / `desktop_session_end`, and diverges from the Phase 114 usage-event pattern.

- **[MEDIUM] 115-03 counts composition results incorrectly.** The plan uses `len(result['main'])`, but live `on_comp_scan_finished()` reports `len(main) + len(filtered)` (`genizah_app.py:23157`). This will skew zero-result counts and result buckets.

- **[MEDIUM] 115-04's `_last_operation_kind` is fragile.** `_on_worker_finished()` processes queued actions at the end; a queued worker can overwrite tab-level operation state before the prior event is emitted. Also `_queued_action` must preserve the new `operation_kind` parameter.

- **[MEDIUM] 115-04's dynamic-string guard verification is weaker than claimed.** `tests/test_no_dynamic_telemetry_strings.py` does not scan `desktop/my_library_tab.py`, and its telemetry-call detector does not include `track_performance`. Passing that test would not actually validate the new indexing telemetry callsites.

- **[MEDIUM] Env parsing is not robust.** Invalid `GENIZAH_PERF_SAMPLE_N` or `GENIZAH_PERF_FLUSH_INTERVAL` values would be swallowed by broad `try/except`, potentially disabling accumulation/flush silently. Values like `0` should be clamped.

- **[LOW] D-03 bucket logic is inlined, not imported.** 115-02 inlines the coarse-bucket logic to avoid an import cycle. The scheme is correct, but duplication invites drift; a small shared helper would be cleaner.

- **[LOW] `initial_scan` defined but unused** is acceptable if documented. It should not be advertised as a populated analytics dimension unless first-scan detection is actually implemented. *(Matches internal checker W-1.)*

- **[LOW] Several verification commands use bash/MSYS syntax** (`cd /c/Genizahsearch`, `grep`) despite the stated PowerShell environment, making the plans less directly executable.

**Suggestions**

- Add `_clear_perf_accumulator()` in `desktop/telemetry.py`; call it from `set_consent(False)` and `_reset_for_tests()`. Add a test: accumulate with consent on, opt out, re-opt in, flush, assert no stale event.
- Replace the generic `_on_perf_signal` with separate slots or lambda connections that capture `mode`, `corpus_scope`, and `session_id` at thread start. Avoid reading stale `_current_*_run` objects at signal time.
- Normalize `mode`, `corpus_scope`, `operation_kind`, and `flush_reason` against fixed allowed sets inside `desktop/telemetry.py`; map unknown values to `unknown`.
- Add `session_id` to `accumulate_performance()` or to `flush_perf_*()` so summary events are joinable to the telemetry session.
- Count composition perf results as `len(main) + len(filtered)`, matching the live UI/usage telemetry.
- In `my_library_tab.py`, stash `operation_kind` from `self._worker` into a local before clearing it, and propagate `operation_kind` through `_queued_action`.
- Extend `tests/test_no_dynamic_telemetry_strings.py` to scan `desktop/my_library_tab.py` and recognize `track_performance` / `accumulate_performance`.
- Fix verification commands to PowerShell-compatible forms or rely on `python -m pytest ...` from the repo cwd.

**Risk Assessment: MEDIUM** — The plans can achieve the phase goal, but should not be executed unchanged. The opt-out accumulator issue is a real privacy regression, and the generic perf slot can silently corrupt search-mode metrics. Both are straightforward to fix before implementation; after those changes, the remaining risks are mostly test coverage and data-quality polish.

---

## Consensus Summary

Single reviewer (Codex) by request — no cross-reviewer consensus to synthesize. Priority ranking of findings:

### Must-fix before execution (HIGH)
1. **Opt-out doesn't clear the perf accumulator** (115-02) — CONSENT-08 privacy regression. Add `_clear_perf_accumulator()`, wire into `set_consent(False)` + `_reset_for_tests()`, add a re-opt-in test.
2. **`_on_perf_signal` reads stale `_current_search_run`** (115-03) — composition signals mis-tagged as regular-search mode/corpus because live code never clears `_current_search_run`. Capture `mode`/`corpus_scope`/`session_id` at thread start (lambda/closure), don't read shared state at signal time. *(Upgrades the internal checker's WARNING to HIGH — Codex verified the no-clear behavior against live code.)*
3. **`mode` becomes an unscrubbed nested dict key** (115-02) — scrubber recurses into values, not keys. Normalize `mode` against a fixed allowed set → `unknown` for misses.

### Should-fix before execution (MEDIUM)
4. Add `session_id` to the summary event (join to session_start/end; Phase-114 parity).
5. Composition result count must be `len(main)+len(filtered)`, not `len(main)` (`genizah_app.py:23157`).
6. `_last_operation_kind` race in `my_library_tab.py` queued-action path; propagate `operation_kind` through `_queued_action`.
7. Extend `tests/test_no_dynamic_telemetry_strings.py` to cover `my_library_tab.py` + `track_performance`/`accumulate_performance` — otherwise the cited acceptance criterion is a false-confidence check.
8. Clamp/validate env knobs (`GENIZAH_PERF_SAMPLE_N`, `GENIZAH_PERF_FLUSH_INTERVAL`); reject/clamp `0` and non-numeric.

### Polish (LOW)
9. Consider a shared bucket helper instead of inlining (drift risk).
10. Document `initial_scan` as defined-but-unused (or implement first-scan detection).
11. Convert bash/MSYS verification commands to PowerShell / `python -m pytest`.

**Recommended action:** revise the plans with `/gsd:plan-phase 115 --reviews` before executing. Findings 1–3 (HIGH) and 4–8 (MEDIUM) are all in-scope, low-effort changes that materially improve privacy correctness and data quality.
