---
phase: 115-performance-metrics
verified: 2026-06-16T12:00:00Z
status: passed
score: 3/3 must-haves verified
overrides_applied: 0
resolution: "All 4 human-verification items resolved 2026-06-16 (user approved 'fix all 3'). (1)+(2) 11 phase-115 + 290-test prior-phase telemetry/crash/posthog regression run GREEN under headless Qt by the orchestrator. (3) WR-02 fixed: 9 missing lab_* modes added to _PERF_ALLOWED_MODES. (4) WR-01 fixed: perf summary session_id now sources the per-process _session_id via telemetry.set_session_id() at session mint, joinable to session_start/end. WR-04 dead path-leak assertion also made real. Commit b1902213; ruff clean."
human_verification:
  - test: "Run the full 17-test Phase 115 suite under headless Qt"
    expected: "All 17 tests pass (GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_phase115.py tests/test_no_dynamic_telemetry_strings.py -v)"
    why_human: "Tests require PyQt6 QThread infrastructure; cannot run headlessly from a verifier bash session without the Qt env variables set and the GUI test runner configured. The REVIEW.md states '17 Phase 115 tests pass locally' but verifier cannot execute Qt tests."
  - test: "Run the prior-phase telemetry regression suite (273 tests)"
    expected: "All 273 existing telemetry tests pass without regression"
    why_human: "Full suite requires the Qt offscreen platform and the split CI job configuration (c9571f0d). Verifier cannot execute the full Qt test suite."
  - test: "Confirm WR-02 is acceptable: LAB mode search telemetry collapses to 'unknown'"
    expected: "Developer confirms that lab_keyword, lab_fuzzy, lab_responsa, lab_regex, lab_title, lab_shelfmark, lab_pgp_tags, lab_comp_variants, lab_comp_fuzzy are NOT in _PERF_ALLOWED_MODES and collapse to 'unknown' in the perf summary — and that this data-quality degradation is acceptable for v8.1.0"
    why_human: "WR-02 is a telemetry data quality decision, not a crash or privacy issue. The code is structurally correct and safe; only the per-mode attribution of LAB searches is lost. Needs explicit developer sign-off before marking as acceptable."
  - test: "Confirm WR-01 is acceptable: perf summary session_id is the install identity, not the per-process session UUID"
    expected: "Developer confirms that the session_id on desktop_session_performance_summary is the install-level distinct_id (stable across sessions), NOT the per-process _session_id UUID that session_start/session_end carry, and that joining perf summaries to their session is not required for v8.1.0"
    why_human: "WR-01 is a semantic mismatch in telemetry join keys. The inline comment at telemetry.py:1578 claims 'same value SESSION_END uses' — this is incorrect (SESSION_END uses the per-process UUID; the flush uses _current_distinct_id/_install_id). The test only asserts non-emptiness. Developer must decide if this is acceptable."
---

# Phase 115: Performance Metrics Verification Report

**Phase Goal:** Search and indexing durations are measured on worker threads and accumulated into a per-session summary (aggregated result counts and latency buckets) that is flushed once at session close and periodically — never one event per search — so heavy users (~50 searches/day) do not flood the PostHog stream.

**Verified:** 2026-06-16T12:00:00Z
**Status:** passed (human items resolved 2026-06-16 — see frontmatter `resolution`)
**Re-verification:** Yes — human-needed items closed by fixing WR-01/WR-02/WR-04 (commit b1902213)

---

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | SearchThread, CompositionThread, and LabSearchThread each emit `perf_signal(float, int)` on completion without exposing query text | ✓ VERIFIED | `gui_threads.py` lines 89, 138, 188, 257: `perf_signal = pyqtSignal(float, int)` on all four thread classes. Emits `(time.perf_counter() - t0) * 1000.0, len(results)` on the success path only, before the `except` blocks. LabCompositionThread also has the signal. Signal carries only `(float, int)` — structurally cannot carry query text. |
| 2 | Result counts reported exclusively as bounded buckets (0/1-9/10-99/100+), never raw integers | ✓ VERIFIED | `desktop/telemetry.py:1550-1558`: `_flush_perf_summary` converts `result_counts` list to `bucket_0`/`bucket_1_9`/`bucket_10_99`/`bucket_100plus` integer counts only. The raw `result_counts` list is never placed on the emitted event. `test_perf_summary_buckets_only` asserts only the four bucket keys plus statistical keys exist in the mode dict. `desktop/my_library_tab.py:1888-1892` and `1238-1241`: indexing events emit `doc_count_bucket` as a coarse string ('0'/'1-9'/'10-99'/'100+'), never the raw `indexed` integer. |
| 3 | Per-session in-memory summary flushes as SINGLE event at close AND periodically; sampling/flush interval tunable without code change | ✓ VERIFIED | `desktop/telemetry.py:1487-1525` (`accumulate_performance` — never emits, only writes to `_perf_accumulator`). `telemetry.py:1528-1599` (`_flush_perf_summary` — emits exactly one `desktop_session_performance_summary`). `genizah_app.py:3719` (`self._ping_check_timer.timeout.connect(self._maybe_flush_perf_summary)` — reuses 5-min timer). `genizah_app.py:27009-27018` (close flush via `flush_perf_unconditionally()` after SESSION_END). Env vars `GENIZAH_PERF_SAMPLE_N` and `GENIZAH_PERF_FLUSH_INTERVAL` read via validated `_perf_env_int`/`_perf_env_float` readers — non-numeric values default, never disable. |

**Score:** 3/3 truths verified

---

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `desktop/telemetry.py` | accumulator globals, `accumulate_performance`, `_flush_perf_summary`, `flush_perf_if_due`, `flush_perf_unconditionally`, `_clear_perf_accumulator`, `_normalize_*` helpers, `_perf_env_int/_perf_env_float`, `INDEXING_COMPLETE` enum, 3 allowlist keys, `_reset_for_tests` extension | ✓ VERIFIED | All present. Line 164: `INDEXING_COMPLETE = 'desktop_indexing_complete'`. Lines 294-319: `_ALLOWED_PROPS` includes `perf_summary`, `operation_kind`, `doc_count_bucket`. Lines 1368-1629: full accumulator + flush machinery. Line 1351: `_reset_for_tests()` calls `_clear_perf_accumulator()`. Line 602: `set_consent(False)` calls `_clear_perf_accumulator()`. |
| `gui_threads.py` | `perf_signal = pyqtSignal(float, int)` on SearchThread, LabSearchThread, CompositionThread, LabCompositionThread; NOT on GroupingThread | ✓ VERIFIED | Lines 89, 138, 188, 257: four `perf_signal = pyqtSignal(float, int)` declarations. GroupingThread (line 326+) has no `perf_signal`. Composition threads use `len(main)+len(filtered)` formula. Cancelled SearchThread does NOT emit perf_signal (InterruptedError branch, line 123-125). |
| `genizah_app.py` | `_on_perf_signal` slot, two `perf_signal.connect` sites (covering all 4 threads), `_maybe_flush_perf_summary`, periodic timer wiring, close flush | ✓ VERIFIED | Line 17690: `def _on_perf_signal(self, elapsed_ms, result_count, mode, corpus_scope)`. Lines 17648-17651: search thread connect. Lines 23113-23116: comp thread connect. Both use default-arg closures. Line 3719: timer wiring. Line 3772: `_maybe_flush_perf_summary`. Line 27015-27017: close flush with `_perf_flushed_on_close` guard. |
| `desktop/my_library_tab.py` | `LocalIndexerWorker` timed, `operation_kind` threaded, `LabRebuildWorker.finished_signal = pyqtSignal(float, int)`, INDEXING_COMPLETE emits from UI-thread slots | ✓ VERIFIED | Line 724: `operation_kind` ctor arg. Lines 731-732: `_elapsed_ms`/`_operation_kind` stored. Line 783: `finished_signal = pyqtSignal(float, int)` on LabRebuildWorker. Lines 1760-1766: stash before worker clear. Lines 1884-1900: INDEXING_COMPLETE emit in `_on_worker_finished`. Lines 1233-1250: INDEXING_COMPLETE emit in `_on_lab_rebuild_finished`. Line 2400: `operation_kind='reindex_all'` at reindex-all call site. Line 1697-1701: queued-action lambda preserves operation_kind. |
| `tests/test_telemetry_phase115.py` | 11 named test cases, autouse reset fixture, `_enable_telemetry` helper | ✓ VERIFIED | 11 test functions confirmed. Autouse `_reset_telemetry_state` fixture monkeypatches config + queue. `_enable_telemetry` helper present. Tests cover PERF-01/02/03 + CONSENT-08 + all three REVIEWS additions (WR finding 1, 3, 8). |
| `tests/test_no_dynamic_telemetry_strings.py` | Extended `TARGET_FILES` includes `desktop/my_library_tab.py`; `EMIT_HELPERS` includes `track_performance` and `accumulate_performance`; new synthetic test | ✓ VERIFIED | Lines 30-32: `my_library_tab.py` in `TARGET_FILES`. Lines 64-67: `track_performance` and `accumulate_performance` in `EMIT_HELPERS`. Lines 286-304: `test_lint_rejects_perf_accessor_violation` confirms the detector flags `currentText()` inside a `track_performance` call. |

---

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|----|--------|---------|
| `gui_threads.py::SearchThread.run` | `genizah_app.py::_on_perf_signal` | `perf_signal.connect(lambda ms, rc, m=_mode, c=_corpus: ...)` | ✓ WIRED | `genizah_app.py:17648-17651`. Default-arg closure binds mode/corpus at thread start. `hasattr` guard handles the case where signal is absent. |
| `gui_threads.py::LabSearchThread.run` | `genizah_app.py::_on_perf_signal` | Same `search_thread.perf_signal.connect` block (both LabSearch and Search assign to `self.search_thread`) | ✓ WIRED | `genizah_app.py:17627` sets `self.search_thread = LabSearchThread(...)` in the LAB branch; `17648` connects `self.search_thread.perf_signal` covering both cases. |
| `gui_threads.py::CompositionThread.run` and `LabCompositionThread.run` | `genizah_app.py::_on_perf_signal` | `comp_thread.perf_signal.connect(lambda ...)` | ✓ WIRED | `genizah_app.py:23113-23116`. Both thread types assign to `self.comp_thread` and are covered by this single connect block. |
| `genizah_app.py::_on_perf_signal` | `desktop/telemetry.py::accumulate_performance` | `telemetry.accumulate_performance(elapsed_ms, result_count, mode, corpus_scope)` | ✓ WIRED | `genizah_app.py:17706-17713`. Mode/corpus arrive as bound args (never from stale `_current_*_run`). |
| `desktop/telemetry.py::_flush_perf_summary` | `desktop/telemetry.py::track_performance` | `track_performance(DesktopEvent.SESSION_PERF, ...)` | ✓ WIRED | `telemetry.py:1586-1593`. One call per flush. Resets accumulator after (`telemetry.py:1595`). |
| `genizah_app.py::_setup_active_ping` | `desktop/telemetry.py::flush_perf_if_due` | `_ping_check_timer.timeout.connect(_maybe_flush_perf_summary)` | ✓ WIRED | `genizah_app.py:3719`. No new timer — reuses 5-min `_ping_check_timer`. |
| `genizah_app.py::closeEvent` | `desktop/telemetry.py::flush_perf_unconditionally` | `telemetry.flush_perf_unconditionally()` after SESSION_END | ✓ WIRED | `genizah_app.py:27009-27018`. Guarded by `_perf_flushed_on_close` flag. Placed after SESSION_END block as required. |
| `desktop/my_library_tab.py::_on_worker_finished` | `desktop/telemetry.py::track_performance` | `telemetry.track_performance(DesktopEvent.INDEXING_COMPLETE, ...)` | ✓ WIRED | `my_library_tab.py:1894-1899`. Uses stashed locals `_elapsed_ms`/`_operation_kind` captured before `self._worker = None`. Emitted before `_queued_action` dispatch. |
| `desktop/my_library_tab.py::_on_lab_rebuild_finished` | `desktop/telemetry.py::track_performance` | `telemetry.track_performance(DesktopEvent.INDEXING_COMPLETE, ...)` | ✓ WIRED | `my_library_tab.py:1244-1249`. `operation_kind='lab_rebuild'` literal. Signal carries `(elapsed_ms, 0)` since rebuild returns no doc count. |
| `desktop/telemetry.py::set_consent(False)` | `desktop/telemetry.py::_clear_perf_accumulator` | `_clear_perf_accumulator()` called after `_drain_and_discard()` | ✓ WIRED | `telemetry.py:602`. CONSENT-08 parity: opt-out clears both the queue and the in-memory perf buffer. |

---

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|---------------|--------|-------------------|--------|
| `_flush_perf_summary` | `_perf_accumulator` | Populated by `accumulate_performance()` called from `_on_perf_signal` slot | Yes — real elapsed_ms from `perf_counter()` + real `len(results)` from search workers | ✓ FLOWING |
| `_on_perf_signal` | `elapsed_ms`, `result_count` | Bound at thread-start from `perf_signal(float, int)` carrying live timing + count | Yes — float from `time.perf_counter()` in worker `run()` | ✓ FLOWING |
| `_on_worker_finished` INDEXING_COMPLETE | `_elapsed_ms` | `getattr(self._worker, '_elapsed_ms', 0.0)` — worker stores `time.monotonic()` delta | Yes — real monotonic elapsed time | ✓ FLOWING |
| `_on_lab_rebuild_finished` INDEXING_COMPLETE | `elapsed_ms` | Signal arg from `LabRebuildWorker.finished_signal.emit(elapsed_ms, 0)` via `time.monotonic()` | Yes — real monotonic elapsed; doc count is unknown sentinel 0 | ✓ FLOWING (with WR-03 caveat) |

---

### Behavioral Spot-Checks

Step 7b is SKIPPED for the search-thread and GUI wiring tests because they require PyQt6 QApplication context and headless Qt setup (`GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen`). The pure-Python accumulator/flush tests (tests 2-11, not requiring Qt) are verifiable structurally from code reading.

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| `accumulate_performance` consent gate | Code read: `if not is_enabled(): return` is the first statement inside try block | Confirmed | ✓ PASS |
| `accumulate_performance` never emits | Code read: no `_emit()` or `track_performance()` call in function body | Confirmed — body only writes to `_perf_accumulator` | ✓ PASS |
| `_flush_perf_summary` resets accumulator | Code read: `_perf_accumulator.clear()` at line 1595, `_perf_last_flush_time = time.monotonic()` at 1597 | Confirmed | ✓ PASS |
| Out-of-set mode → 'unknown' key | Code read: `mode = _normalize_mode(mode)` before `_perf_accumulator.setdefault(mode, ...)` | Confirmed — normalized value becomes the key | ✓ PASS |
| `perf_signal` not emitted on cancel | Code read: `self.perf_signal.emit(...)` is after `self.results_signal.emit(results)`, BEFORE `except InterruptedError` | Confirmed — cancelled path never reaches the emit line | ✓ PASS |
| Qt tests (all 17) | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_phase115.py -v` | Cannot run in verifier session | ? SKIP — needs human |

---

### Probe Execution

Step 7c: No probe scripts defined for Phase 115. Phase is a telemetry/instrumentation phase; success criteria are verified by the unit test suite, not by executable probes. SKIPPED.

---

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| PERF-01 | Plans 03, 04 | Search and indexing durations measured without query text | ✓ SATISFIED | `perf_signal(float, int)` on all 4 search threads. `LocalIndexerWorker` and `LabRebuildWorker` timed with monotonic clock. No query text structurally possible on the `(float, int)` signal. |
| PERF-02 | Plans 02, 04 | Result counts as bounded buckets, not raw values | ✓ SATISFIED | `_flush_perf_summary` only emits bucket counts. Indexing events emit `doc_count_bucket` string. `test_perf_summary_buckets_only` asserts no raw integer keys exist in mode dict. |
| PERF-03 | Plans 02, 03 | Aggregated per-session summary flushed periodically + at close; sampling tunable | ✓ SATISFIED | `accumulate_performance` never emits. Periodic flush via 5-min timer + `flush_perf_if_due`. Close flush via `flush_perf_unconditionally()` in `closeEvent`. `GENIZAH_PERF_SAMPLE_N`/`GENIZAH_PERF_FLUSH_INTERVAL` configurable via env vars with validated readers. |

---

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `tests/test_telemetry_phase115.py` | 262 | `assert ':\\' not in payload_repr and '/' not in payload_repr or True` — tautological assertion (always True due to Python precedence: `(A and B) or True`) | ⚠️ WARNING (WR-04) | Path-leak guard is dead code. A real path in the payload would silently pass this assertion. The privacy protection relies on the scrubber tests (`test_perf_summary_survives_scrubber`) which test scrubber behavior correctly; the dead assertion here is redundant but does not reduce functional scrubber coverage. Not a goal blocker. |
| `desktop/telemetry.py` | 1391-1395 | `_PERF_ALLOWED_MODES` missing most `lab_*` mode variants — `lab_keyword`, `lab_fuzzy`, `lab_responsa`, `lab_regex`, `lab_title`, `lab_shelfmark`, `lab_pgp_tags`, `lab_comp_variants`, `lab_comp_fuzzy` all absent | ⚠️ WARNING (WR-02) | Most LAB-mode searches collapse to 'unknown' in the perf summary. The privacy guarantee is intact (these are all fixed enum strings, not free text). The telemetry data quality for LAB performance is severely degraded — a primary use case of Phase 115. Functionality is structurally correct; this is a telemetry usefulness gap requiring developer decision. |
| `desktop/telemetry.py` | 1578-1582 | `session_id` on `desktop_session_performance_summary` is `_current_distinct_id or _install_id` (install identity), not `self._session_id` (per-process UUID used by session_start/session_end). Inline comment `# same value SESSION_END uses` is incorrect. | ⚠️ WARNING (WR-01) | `desktop_session_performance_summary` events cannot be joined to their session's `session_start`/`session_end` events by `session_id`. The `test_no_per_search_events` test only asserts non-emptiness, so this semantic error passes silently. Requires developer decision on v8.1.0 acceptability. |
| `desktop/my_library_tab.py` | 797-799 | `LabRebuildWorker.run()` emits `finished_signal.emit(elapsed_ms, 0)` unconditionally — '0' as the doc count is the same value as a genuinely empty rebuild | ℹ️ INFO (WR-03) | `doc_count_bucket='0'` on all LAB rebuilds is indistinguishable from a zero-doc rebuild. Acknowledged in code comments as a documented sentinel. No privacy impact. |
| `desktop/telemetry.py` | 1571-1573 | `_flush_perf_summary` emits `corpus_genizah`/`corpus_local`/`corpus_all` but NOT `corpus_unknown` — searches with unknown corpus contribute to `count` but have no corpus attribution | ℹ️ INFO (IN-01) | Minor data quality gap; `count` minus the three `corpus_*` fields equals the unknown tally but is not self-documenting. |

No `TBD`, `FIXME`, or `XXX` markers found in Phase 115 modified files.

---

### Human Verification Required

#### 1. Phase 115 Test Suite (17 tests)

**Test:** Run `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_phase115.py tests/test_no_dynamic_telemetry_strings.py -v` from the repo root.
**Expected:** All 17 phase-115 tests pass; `test_no_dynamic_telemetry_strings.py` also passes (including the new `test_lint_rejects_perf_accessor_violation`).
**Why human:** Requires PyQt6 QApplication headless environment. The REVIEW.md states local pass but verifier cannot execute Qt tests.

#### 2. Prior-Phase Telemetry Regression Suite (273 tests)

**Test:** Run the full existing telemetry test suite (Phases 111-114 tests) to confirm no regression.
**Expected:** 273 prior tests pass.
**Why human:** Full test suite requires Qt headless setup and the CI split-job configuration.

#### 3. WR-02 Decision: LAB-mode mode attribution

**Test:** Review `_PERF_ALLOWED_MODES` in `desktop/telemetry.py` lines 1391-1395. Confirm or reject the finding that `lab_keyword`, `lab_fuzzy`, `lab_responsa`, `lab_regex`, `lab_title`, `lab_shelfmark`, `lab_pgp_tags`, `lab_comp_variants`, `lab_comp_fuzzy` are absent from the allowed set and collapse to `'unknown'` in all perf summaries.
**Expected:** Developer either (a) accepts the degraded LAB attribution for v8.1.0 and adds it as a known issue, or (b) expands `_PERF_ALLOWED_MODES` to include the full `lab_*` family before shipping.
**Why human:** This is a telemetry data quality decision, not a structural/privacy/safety issue. The code is safe either way. But WR-02 means a core goal of Phase 115 ("LAB-mode performance visible") is not fully achieved.

#### 4. WR-01 Decision: session_id semantic mismatch

**Test:** Read `desktop/telemetry.py:1578-1582` and compare with `genizah_app.py:27005` (SESSION_END). Confirm that `_flush_perf_summary` uses `_current_distinct_id` (install-level persistent ID), while SESSION_END uses `self._session_id` (per-process UUID minted at startup).
**Expected:** Developer either (a) accepts that the `session_id` on perf summaries cannot join to session_start/end events for v8.1.0 and updates the comment at telemetry.py:1578, or (b) plumbs the real per-process session UUID through to the flush.
**Why human:** Semantic correctness of the join key is a product decision. The test asserts non-emptiness only, which passes regardless.

---

### Gaps Summary

No hard FAILED truths or missing artifacts were found. The three success criteria are structurally achieved in the codebase. Four warnings from the code review (WR-01 through WR-04) require human decisions:

- **WR-02** (most LAB-mode searches collapse to 'unknown') is the most significant — it directly degrades the "per search mode" attribution promised by SC#1 and SC#3 for LAB users. The code is architecturally correct, the privacy guarantees hold, and the flush fires one aggregate event per session as required. But if LAB-mode telemetry was a primary motivation, the usefulness is severely limited until `_PERF_ALLOWED_MODES` is expanded.

- **WR-01** (wrong session_id value) means the documented goal of joining perf summaries to their session cannot be fulfilled with the current implementation. The comment at `telemetry.py:1578` is incorrect.

- **WR-04** (tautological test assertion) leaves a dead path-leak guard. The scrubber tests cover the actual behavior; the dead line should be fixed but is not a goal blocker.

- **WR-03** (LAB-rebuild always reports bucket='0') is acknowledged as a known limitation in the plan.

Status is `human_needed` because the test suite result for the 17 Qt-dependent tests cannot be confirmed without running PyQt6 headlessly, and because WR-01 and WR-02 require explicit developer decisions before the phase can be fully closed.

---

_Verified: 2026-06-16T12:00:00Z_
_Verifier: Claude (gsd-verifier)_
