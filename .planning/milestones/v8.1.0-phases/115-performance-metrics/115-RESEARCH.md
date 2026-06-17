# Phase 115: Performance Metrics - Research

**Researched:** 2026-06-16
**Domain:** PyQt6 desktop telemetry — performance-measurement producers, in-memory accumulator, periodic flush
**Confidence:** HIGH (all code read from live files; no assumptions about internal behavior)

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**D-01:** Include indexing-duration telemetry this phase — `LocalIndexerWorker` + LAB rebuild.
Own event, NOT folded into perf summary.

**D-02:** Indexing event carries: operation-kind enum (hardcoded constant — one of
`initial_scan / incremental_add / reindex_all / lab_rebuild`), `duration_ms` (monotonic),
doc-count bucket. NEVER folder paths / filenames / raw doc counts.

**D-03:** Reuse `_telemetry_result_bucket()` (`genizah_app.py:3278`) — the coarse `0 / 1-9 /
10-99 / 100+` scheme. NO finer perf-specific buckets.

**D-04:** Flush cadence = periodic ~30-min ACTIVE use + close flush. Mirror `active_ping`
mechanism (`_setup_active_ping` / `_maybe_emit_active_ping`, `genizah_app.py:3705`). NOT a
naive 30-min QTimer.

**D-05:** Both cadence and sampling tunable via env var / config WITHOUT a code change.
Default sampling = OFF (`sample_n = 1`).

**D-06:** Each flush RESETS the accumulator — self-contained windows, no double-count.

**D-07:** Single `desktop_session_performance_summary` event carries, per search mode:
median ms, p95 ms, search count, zero-result count, min ms, max ms, result-bucket
distribution, corpus-scope split (Genizah/Local/ALL). Low-cardinality numbers nested in
ONE event.

**D-08:** `SearchThread`, `LabSearchThread`, `CompositionThread`, `LabCompositionThread` each
get `perf_signal(float, int)`. Hung off same completion points as Phase-114 per-run
search-state object. Completed runs only — no duration on cancelled runs. `GroupingThread`
NOT timed.

**D-09:** All emission inside `desktop/telemetry.py`. Monotonic clock. Behind
`_telemetry_ready()` + `is_enabled()`. Close flush rides `closeEvent` session_end path.

### Claude's Discretion

- Accumulator location and API: module-level in `desktop/telemetry.py` vs on the GUI.
- How `track_performance()` (SC#1) reconciles with "never one event per search" (SC#3).
- Exact env var names + default periodic interval.
- Nested-payload container key name(s) to add to `_ALLOWED_PROPS`.
- Whether indexing operation-kind reuses `action` prop or a new prop.
- Doc-count bucket boundaries.

### Deferred Ideas (OUT OF SCOPE)

- Privacy CI audit + frozen-binary self-test + operational runbook (Phase 116).
- Handled/non-fatal error counting (ERR-01, Future).
- PostHog feature flags / remote config (FLAG-F1, Future).
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| PERF-01 | Search and indexing durations measured (perf signal on threads), no query text | Verified `perf_signal` pattern; `LocalIndexerWorker.finished_signal` wiring path confirmed |
| PERF-02 | Result counts as bounded buckets, not raw values | `_telemetry_result_bucket()` at `genizah_app.py:3278` verified; reuse confirmed |
| PERF-03 | Perf data aggregated per-session summary, periodic+close flush, sampling configurable | `active_ping` pattern (`genizah_app.py:3705`) confirmed as the flush-cadence template |
</phase_requirements>

---

## Summary

Phase 115 wires performance-measurement **producers** — new `perf_signal` Qt signals on four
search threads — into a **per-session in-memory accumulator** inside `desktop/telemetry.py`,
flushed as a single `desktop_session_performance_summary` event on a periodic active-use
cadence (mirroring the existing active-ping mechanism) plus at app close. A separate, lighter
`desktop_indexing_complete` event is emitted from the `LocalIndexerWorker` and `LabRebuildWorker`
completion slots on the UI thread.

The telemetry foundation (Phases 111–114) is fully built. This phase adds no new chokepoint
machinery — it adds two `DesktopEvent` enum members (one already exists: `SESSION_PERF`; one
is new: the indexing event), three allowlist container keys, producer signals in `gui_threads.py`,
accumulator + flush logic in `desktop/telemetry.py`, and wiring in `genizah_app.py` and
`desktop/my_library_tab.py`.

**Primary recommendation:** Use a dedicated `accumulate_performance()` function in
`desktop/telemetry.py` for the per-search data ingest (called from the UI-thread signal
handler), and reserve `track_performance()` / `SESSION_PERF` exclusively for the flush
(`_flush_perf_summary()`). This resolves the SC#1/SC#3 tension cleanly and preserves
`track_performance()`'s existing emit-immediately semantics for the indexing event.

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Duration measurement | Worker thread (SearchThread etc.) | — | Only the thread knows elapsed wall time |
| Perf signal emission | Worker thread (via `pyqtSignal`) | — | Qt signal auto-delivers to UI thread |
| Accumulator writes | UI thread (signal handler) | — | Qt signal-slot: slot runs on receiver thread |
| Accumulator flush — periodic | UI thread (QTimer slot) | — | Must stay on UI thread, mirrors active_ping |
| Accumulator flush — close | UI thread (closeEvent) | — | Already on UI thread |
| Indexing duration | Worker thread signal → UI slot | — | Same signal-slot pattern as LocalIndexerWorker |
| All PostHog emission | `desktop/telemetry.py` | — | AST guard enforces this invariant |

---

## Verified Code Map

### `desktop/telemetry.py` — what exists vs what Phase 115 adds

**Line references verified from live file (1,445 lines):**

| Item | Line | Status |
|------|------|--------|
| `DesktopEvent` enum | 132 | EXISTS — `SESSION_PERF = 'desktop_session_performance_summary'` at line 162 |
| `_ALLOWED_PROPS` | 292 | EXISTS — has `duration_ms`, `result_count`, `sample_n`, `duration_bucket_ms` |
| `_scrub_value` | 245 | EXISTS — recurses into `dict` and `list/tuple` at lines 263-267 |
| `_scrub_props` | 271 | EXISTS — top-level banned-key drop + delegates to `_scrub_value` |
| `track_performance()` | 710 | EXISTS — EMITS immediately via `_emit()`; does NOT accumulate |
| `is_enabled()` | 500 | EXISTS — lock-protected `_enabled` read |
| `_is_enabled_nolock()` | 844 | EXISTS — lock-free version for crash path |
| `_BASE_PROPS()` | 354 | EXISTS — `platform`, `app_version`, `os_family`, `os_version` only |
| `install_exception_hooks()` | 1165 | EXISTS — atexit `_atexit_flush` registered inside (~line 1254) |
| `_reset_for_tests()` | 1300 | EXISTS — resets Phase 111+113 globals |
| `SESSION_PERF` enum member | 162 | EXISTS — value `'desktop_session_performance_summary'` |
| Indexing event enum member | — | **MISSING — must ADD** (e.g. `INDEXING_COMPLETE = 'desktop_indexing_complete'`) |
| Summary container key(s) in `_ALLOWED_PROPS` | — | **MISSING — must ADD** |
| Indexing prop key(s) in `_ALLOWED_PROPS` | — | **MISSING — must ADD** `operation_kind` |
| `accumulate_performance()` | — | **MISSING — must ADD** |
| `_flush_perf_summary()` | — | **MISSING — must ADD** |
| Accumulator module globals | — | **MISSING — must ADD** |

**Allowlist additions required (lines 292–313 of live file):**
```python
# Phase 115 additions to _ALLOWED_PROPS frozenset:
'perf_summary',       # top-level container key for nested per-mode stats dict
'operation_kind',     # indexing event: enum literal ('initial_scan' etc.)
'doc_count_bucket',   # indexing event: coarse doc count bucket
```

### `gui_threads.py` — verified thread classes and signals

All four target threads confirmed. **None has a monotonic timing anchor today.**

| Class | Line | Completion point | Has `perf_signal`? | Has timing? |
|-------|------|------------------|--------------------|-------------|
| `SearchThread` | 80 | `self.results_signal.emit(results)` at line 115 | NO — must ADD | NO |
| `LabSearchThread` | 123 | `self.results_signal.emit(results)` at line 163 | NO — must ADD | NO |
| `CompositionThread` | 168 | `self.scan_finished_signal.emit(result)` at line 222 | NO — must ADD | NO |
| `LabCompositionThread` | 227 | `self.scan_finished_signal.emit(result)` at line 289 | NO — must ADD | NO |
| `GroupingThread` | 294 | `self.finished_signal.emit(...)` at line 340 | NO — should NOT add (D-08 excludes it) | NO |

**Existing signals per class:**
- `SearchThread`: `results_signal(list)`, `progress_signal(int, int)`, `error_signal(str)`
- `LabSearchThread`: `results_signal(list)`, `progress_signal(int, int)`, `status_signal(str)`, `error_signal(str)`
- `CompositionThread`: `progress_signal(int, int)`, `status_signal(str)`, `scan_finished_signal(object)`, `error_signal(str)`
- `LabCompositionThread`: `progress_signal(int, int)`, `status_signal(str)`, `scan_finished_signal(object)`, `error_signal(str)`

**Existing timing:**
- `SearchThread` and `CompositionThread` have `self.cancel_flag` — no monotonic clock.
- None of the four threads records start time internally; `genizah_app.py` uses `time.time()` (wall clock) in `self.search_start_time` (line 17541) and `self.comp_search_start_time` (line 22883). Phase 115 must use `time.monotonic()` in the thread's `run()`.

### `genizah_app.py` — verified wire-in points

| Item | Line | Notes |
|------|------|-------|
| `_telemetry_result_bucket()` | 3278 | Module-level function. Returns `'0'` / `'1-9'` / `'10-99'` / `'100+'` |
| `_setup_active_ping()` | 3705 | Sets `self._ping_check_timer` (5-min QTimer) + `applicationStateChanged` connection |
| `_maybe_emit_active_ping()` | 3731 | Guards: `_telemetry_ready()`, `is_enabled()`, date comparison, app-active state |
| `_on_app_state_changed()` | 3725 | Calls `_maybe_emit_active_ping()` on `ApplicationActive` |
| `_telemetry_ready()` | 3660 | Returns `bool(getattr(self, '_telemetry_session_started', False))` |
| `_current_search_run` dict | 17509 | Set at search-start with `{'mode': ..., 'corpus': ..., 'emitted': False}` |
| `_current_comp_search_run` dict | 22963 | Same shape for composition |
| `search_start_time` | 17541 | `time.time()` — wall clock, NOT monotonic |
| `comp_search_start_time` | 22883 | `time.time()` — wall clock, NOT monotonic |
| `_emit_search_telemetry()` | 17611 | UI-thread handler; first line checks `_app_shutting_down`, then `_telemetry_ready()` |
| `_emit_comp_search_telemetry()` | 22829 | Same pattern |
| `on_search_finished()` | 18052 | Calls `_emit_search_telemetry('completed', len(results))` at line 18236 |
| `closeEvent()` | 26907 | Sets `_app_shutting_down=True` first; emits `SESSION_END` at line 26924 |
| `_app_shutting_down` | 26911 | Set first thing in closeEvent — used as first guard in all telemetry handlers |

**The `_current_search_run` object (D-09 of Phase 114)** is the per-run state object. Phase 115
adds a monotonic start-time from within `SearchThread.run()` / `LabSearchThread.run()` via
`perf_signal`, which the UI-thread slot receives and feeds to the accumulator.

### `desktop/my_library_tab.py` — indexing completion paths

| Class | Line | Signal | UI-thread slot |
|-------|------|--------|----------------|
| `LocalIndexerWorker` | 699 | `finished_signal = pyqtSignal(dict)` at line 715 | `_on_worker_finished(result, toast)` at line 1699 |
| `LabRebuildWorker` | 760 | `finished_signal = pyqtSignal()` at line 771 | `_on_lab_rebuild_finished()` at line 1207 |

The `LocalIndexerWorker.run()` (line 729) calls `self._indexer.scan_all(...)` and emits
`self.finished_signal.emit(result)` at line 750. The result dict carries counts but NOT timing.
The timing anchor must be captured as `time.monotonic()` at `run()` entry and emitted alongside.

**LAB rebuild:** `LabRebuildWorker.run()` (line 780) calls `self._search.rebuild_local_lab_index(...)`.
Its `finished_signal` carries no arguments. Duration and a doc-count bucket (from the result if
available, else a fixed `'unknown'` fallback) must be captured inside `run()`.

**Operation-kind mapping:**

| Worker + trigger | operation_kind constant |
|-----------------|------------------------|
| `LocalIndexerWorker` started by "Refresh" / first-add | `'initial_scan'` or `'incremental_add'` (see Note 1) |
| `LocalIndexerWorker` started by "Re-index All" | `'reindex_all'` |
| `LabRebuildWorker` | `'lab_rebuild'` |

*Note 1:* The tab currently has no explicit "operation kind" flag. The simplest approach: pass
`operation_kind` as a constructor argument to `LocalIndexerWorker` (a new string attribute,
defaulting to `'incremental_add'`; callers that trigger "Re-index All" pass `'reindex_all'`).
The "first scan ever" case could be detected by checking if the index was empty before scan,
but that adds complexity — acceptable to use `'incremental_add'` for all non-reindex cases and
document this in the runbook.

### AST guard tests — what they forbid

**`tests/test_telemetry_no_direct_posthog.py`** (PRIV-03):
- Scans all `desktop/*.py` except `desktop/telemetry.py` (matched by resolved path).
- Forbids: `import shared.posthog_server`, `from shared.posthog_server import ...`,
  `from shared import posthog_server`, and any bare or aliased call to `enqueue_event`.
- **Phase 115 impact:** The new accumulator, flush logic, and indexing event emission must all
  live in `desktop/telemetry.py`. Any helper in `desktop/my_library_tab.py` or `genizah_app.py`
  that calls the telemetry API must call `telemetry.accumulate_performance()` or
  `telemetry.track_performance()` — NOT `enqueue_event` directly.

**`tests/test_no_dynamic_telemetry_strings.py`** (D-17):
- Scans argument expressions at telemetry call sites in `genizah_app.py`, `gui_threads.py`,
  `desktop/result_dialog.py`.
- Forbids these accessor methods as telemetry call arguments:
  `currentText`, `tabText`, `windowTitle`, `text`, `selectedFiles`, `toPlainText`.
- **Phase 115 impact:** `perf_signal` emits `(float, int)` — no string UI accessors involved.
  The `operation_kind` value must be a hardcoded string literal, not sourced from any UI widget.
  This guard will NOT break for Phase 115 if literals are used. The new `accumulate_performance()`
  and `_flush_perf_summary()` calls in `genizah_app.py` must also pass only literal constants,
  not `currentText()` etc. — already satisfied by the design.

---

## Resolution of 8 Key Questions

### KQ-1: Accumulator-API tension (SC#1 vs SC#3)

**Recommendation: Add `accumulate_performance()` and use `track_performance()` only for the
indexing event flush.**

`track_performance()` at line 710 calls `_emit()` immediately — it is an **emit**, not an
accumulate. SC#1 says "the UI-thread handler calls `track_performance()`" but SC#3 says "never
one event per search". These two are reconciled by:

- Adding a new public function `accumulate_performance(elapsed_ms: float, result_count: int,
  mode: str, corpus_scope: str)` in `desktop/telemetry.py` that writes to the module-level
  accumulator. This is called from the UI-thread `perf_signal` handler.
- Adding `_flush_perf_summary()` (internal) that reads the accumulator, builds the nested payload,
  calls `track_performance(DesktopEvent.SESSION_PERF, ...)`, and resets the accumulator.
- `track_performance()` continues to emit immediately — it is called by `_flush_perf_summary()`
  with the summarized payload, not per-search.
- For the **indexing event**, `track_performance(DesktopEvent.INDEXING_COMPLETE, ...)` IS called
  per-event (one event per indexing run) — that is acceptable because indexing runs are rare
  (once per folder scan, not 50×/day).

This approach:
1. Keeps `track_performance()` semantics unchanged (immediate emit, AST guard passes).
2. `accumulate_performance()` is the new accumulate-only API, never emits.
3. The "never one event per search" rule is enforced in `_flush_perf_summary()`, not the signal handler.
4. The D-17 AST guard does not scan `desktop/telemetry.py` internals (only `genizah_app.py`,
   `gui_threads.py`, `desktop/result_dialog.py`), so no new guard extensions are needed.

**Alternative rejected:** Making `track_performance()` itself decide whether to accumulate or
emit (based on event type) would require conditional behavior and make the function harder to
test. The dedicated `accumulate_performance()` surface is cleaner.

### KQ-2: Accumulator location

**Recommendation: Module-level object in `desktop/telemetry.py`.**

Reasons:
1. AST guards enforce that all emission lives in `desktop/telemetry.py`. A GUI-side accumulator
   would have to call back into `telemetry.py` for the flush, adding indirection.
2. The crash path already uses module-level globals (`_crash_distinct_id`, `_in_crash_hook`).
   The module-level singleton pattern (Phase 111) is well-established here.
3. UI-thread-safety: the accumulator will only ever be written from the UI thread (Qt signal
   slots), so no lock is needed. A simple `dict` or dataclass is sufficient.
4. Adding it to the GUI class would require passing the accumulator reference to
   `desktop/telemetry.py` for the flush, breaking the encapsulation principle that the chokepoint
   owns all state.

**Accumulator shape** (module-level in `desktop/telemetry.py`):
```python
# Module-level perf accumulator (Phase 115)
# Written from UI thread only (Qt signal-slot guarantee). No lock needed.
# Reset by _flush_perf_summary().
_perf_accumulator: dict  = {}  # key: mode_key, value: dict with lists + counts
_perf_flush_timer_active: bool = False  # tracks whether a periodic flush timer is running
_perf_last_flush_elapsed: float = 0.0   # time.monotonic() of last flush (for interval math)
```

where each mode entry in `_perf_accumulator` is:
```python
{
    'durations_ms': [],          # list of float, one per completed run
    'result_counts': [],         # list of int, parallel to durations_ms
    'zero_result_count': 0,
    'corpus_counts': {'genizah': 0, 'local': 0, 'all': 0},
}
```

The accumulator is keyed by `mode` string (the existing search-mode enum value from Phase 114).

**`_reset_for_tests()` must be extended** to also clear the accumulator globals (lines 1300–1348).

### KQ-3: Streaming median/p95

**Recommendation: List-based exact percentile; reset on flush (D-06).**

Volume ceiling: ~50 searches/day for a heavy user, with `sample_n=1` (accumulate every search).
Even in a full 30-minute active-use window, the list grows to at most ~25 entries per mode.
`sorted(durations_ms)` then `durations[int(0.95 * len(durations))]` is instantaneous at that
scale. A streaming estimator (T-Digest, P2 algorithm) adds code complexity for no measurable
benefit.

**D-06 (reset on flush) interaction:** each `_flush_perf_summary()` call empties the list,
so list length stays bounded at `sample_n * (window_search_count)`. Even at `sample_n=1`,
the worst case (100 searches before first flush) is a 100-element list — trivial.

**Percentile implementation:**
```python
def _percentile(durations: list[float], p: float) -> float:
    """Exact percentile over a sorted list. Returns 0.0 for empty list."""
    if not durations:
        return 0.0
    s = sorted(durations)
    idx = min(int(p * len(s)), len(s) - 1)
    return s[idx]
```

### KQ-4: Periodic flush via active_ping (D-04) — exact mechanism

**`_setup_active_ping()` and `_maybe_emit_active_ping()` structure (verified from lines 3705–3766):**

```
_setup_active_ping():
  - Creates self._ping_check_timer (QTimer, 5-min interval)
  - Connects timer.timeout → self._maybe_emit_active_ping
  - Connects QApplication.applicationStateChanged → self._on_app_state_changed

_on_app_state_changed(state):
  - If state == ApplicationActive: call self._maybe_emit_active_ping()

_maybe_emit_active_ping():
  - Guard 1: _telemetry_ready()
  - Guard 2: is_enabled()
  - Guard 3: today != _session_start_date_utc  (no launch-day double-count)
  - Guard 4: _last_ping_date_utc != today      (at most once per UTC day)
  - Guard 5: app.applicationState() == ApplicationActive
  - Emits ACTIVE_PING
```

**For Phase 115, the periodic flush mirrors this exactly, but the gate condition is time-based
(~30 min of active use), not date-based.** The recommendation:

Add `_setup_perf_flush()` and `_maybe_flush_perf_summary()` in `genizah_app.py`, wired to the
same `_ping_check_timer` (reuse the existing 5-min timer — no new timer). In `_maybe_emit_active_ping()`,
after the active_ping emit, also call `self._maybe_flush_perf_summary()`. OR, add a second
`timeout.connect` on `_ping_check_timer` to `_maybe_flush_perf_summary`.

`_maybe_flush_perf_summary()` guards:
1. `_telemetry_ready()` and `is_enabled()`
2. `_app_shutting_down` (suppress during shutdown — close flush handles it)
3. Accumulator non-empty (no-op if nothing accumulated)
4. `time.monotonic() - _perf_last_flush_time >= _PERF_FLUSH_INTERVAL_ACTIVE_SECS`
   (default 1800s = 30 min; env-var overridable)
5. `app.applicationState() == ApplicationActive` (mirrors active_ping belt-and-braces)

On each `applicationStateChanged` to `ApplicationActive` (via `_on_app_state_changed`), also
call `_maybe_flush_perf_summary()`. This way an app that was paused for 30 min and then resumed
triggers a flush.

**Why NOT a separate 30-min QTimer:** a naive 30-min timer fires even when the app has been
idle/backgrounded for 30 min. The 5-min check + focus-resume pattern means the interval tracks
actual active-use minutes, matching D-04's "~30 min of ACTIVE use".

**Recommended env var:** `GENIZAH_PERF_FLUSH_INTERVAL` (seconds, integer). Default: `1800`.
Read once at startup in `_setup_active_ping()` (or a new `_setup_perf_flush()`) via
`int(os.environ.get('GENIZAH_PERF_FLUSH_INTERVAL', '1800'))`.

**`_perf_last_flush_time` should live in `desktop/telemetry.py`** (not on the GUI), because
`_flush_perf_summary()` updates it — the flush is in the chokepoint. The GUI's
`_maybe_flush_perf_summary()` can call `telemetry.flush_perf_if_due()` which internally checks
the interval.

### KQ-5: Exit-path flush (D-09)

**closeEvent sequence (verified from lines 26907–26929):**

```
closeEvent():
  1. self._app_shutting_down = True          # line 26911 — suppresses emission guard
  2. emit SESSION_END (telemetry)             # line 26917
  3. flush_pending() on opt-out tree         # line 26937
  4. sweep_running_scan_runs()               # line 26946
  5. _save_session()                         # line 26955
  6. stop worker threads                     # lines 26957+
  7. accept() / super().closeEvent()         # implicit
```

**Phase 115 adds a close flush between step 2 (SESSION_END) and step 3:**
```python
# Phase 115: flush the perf accumulator before close (D-09)
try:
    from desktop import telemetry
    if self._telemetry_ready() and not getattr(self, '_perf_flushed_on_close', False):
        self._perf_flushed_on_close = True
        telemetry.flush_perf_unconditionally()  # flush even if interval not elapsed
except Exception:
    pass
```

`flush_perf_unconditionally()` is a variant of `_flush_perf_summary()` that skips the interval
check (always flushes the partial window).

**Phase 113 `_atexit_flush` interaction:** `_atexit_flush` calls `_flush_before_exit(1.5s)` which
drains the PostHog queue. The perf summary event will already be in the queue (enqueued by
`flush_perf_unconditionally()` during closeEvent) before `_atexit_flush` runs. No ordering
conflict — `_flush_before_exit` drains what's already in the queue.

**`_flush_before_exit` is NOT needed for the perf flush itself** — the perf flush just calls
`track_performance()` → `_emit()` → `enqueue_event()` (puts in queue). The `_atexit_flush` then
drains the queue. The only risk is a crash between `track_performance()` and `_atexit_flush`,
which is covered by the periodic flush (crash-resilience rationale).

### KQ-6: Indexing event shape

**New `DesktopEvent` member:**
```python
INDEXING_COMPLETE = 'desktop_indexing_complete'
```

**Props:**
```python
{
    'operation_kind':  'initial_scan',   # one of 4 literal constants (D-02)
    'duration_ms':     12345.6,          # monotonic ms (already allowlisted)
    'doc_count_bucket': '100+',          # coarse bucket (see below)
}
```

**`operation_kind` — new prop vs reuse `action`:**
Recommend a **new prop `operation_kind`** rather than reusing `action`. Rationale:
- `action` is used for `completed` / `cancelled` status on search events. Mixing it with an
  operation-type enum creates ambiguity in PostHog queries.
- `operation_kind` is semantically distinct (type of operation, not outcome).
- Cost: one new `_ALLOWED_PROPS` key (`operation_kind`) — trivial.

**Doc-count bucket — recommendation:**
Reuse `_telemetry_result_bucket()` for the doc-count bucket (D-03 says "may reuse the same
function or a parallel small constant set"). Since `LocalIndexerWorker.finished_signal(dict)`
carries the result dict with total processed file count (the dict returned by `scan_all()`
includes counts), apply `_telemetry_result_bucket(total_files)` directly.

If the exact key in the result dict is not available from the UI slot, fall back to the worker's
`progress_updated` signal's last `total_files` value (cached on the tab).

**`doc_count_bucket` is a NEW `_ALLOWED_PROPS` key.**

**Emission path:** `track_performance()` called from the UI-thread slot
(`_on_worker_finished()` in `desktop/my_library_tab.py` calls
`telemetry.track_performance(DesktopEvent.INDEXING_COMPLETE, duration_ms=..., **{'operation_kind': ..., 'doc_count_bucket': ...})`).

Wait — the AST guard `test_telemetry_no_direct_posthog.py` scans `desktop/` files other than
`telemetry.py`. `my_library_tab.py` is in `desktop/`. The guard only blocks `enqueue_event`
import/calls, NOT `telemetry.track_performance()`. So calling `telemetry.track_performance()`
from `desktop/my_library_tab.py` is fine (the chokepoint is telemetry.py, not the caller).

### KQ-7: Env/config tunability (D-05)

**Prior-phase pattern (from codebase):**
`shared/nli_circuit_breaker.py` uses `int(os.environ.get('NLI_CIRCUIT_THRESHOLD', '3'))` at
module level. `CLAUDE.md` documents env vars in the "Environment Variables" section.

**Proposed env vars for Phase 115:**
```
GENIZAH_PERF_FLUSH_INTERVAL=1800  # seconds between periodic flushes (default: 30 min)
GENIZAH_PERF_SAMPLE_N=1           # accumulate every Nth completed search; 1 = all, 5 = every 5th
```

**Resolution:** Read at startup in `_setup_active_ping()` (or a sibling `_setup_perf_flush()`),
stored as `self._perf_flush_interval` and `self._perf_sample_n`. The sample_n knob is applied in
the `perf_signal` handler: accumulate only if `run_counter % sample_n == 0`. Counter is a
module-level int in `desktop/telemetry.py` incremented per call to `accumulate_performance()`.

**CLAUDE.md update required** for both new env vars (enforcement: update BEFORE closing phase,
per CLAUDE.md docs maintenance section).

### KQ-8: Nested payload + scrubber

**Confirmed from live code (lines 245–268):**

`_scrub_value()` recurses into `dict`: at line 263–265:
```python
if isinstance(v, dict):
    return {k: _scrub_value(val) for k, val in v.items()
            if not _is_banned_key(k)}
```

**The allowlist gates TOP-LEVEL keys only** — `_validate_props()` (lines 316–324) iterates
`props.items()` and drops keys not in `_ALLOWED_PROPS`. It does NOT descend into nested dicts.

**Implication:** the nested per-mode stats dict must sit under a SINGLE allowlisted top-level
key. That key is the "summary container key". The planner must add it to `_ALLOWED_PROPS`.

**Recommendation:** Use `'perf_summary'` as the container key. Example payload:
```python
{
    'perf_summary': {             # <-- allowlisted top-level key
        'keyword': {              # <-- mode key (not allowlist-checked; nested value)
            'count': 12,
            'median_ms': 450.0,
            'p95_ms': 1200.0,
            'min_ms': 120.0,
            'max_ms': 3400.0,
            'zero_result_count': 2,
            'bucket_0': 2,         # result count bucket counts
            'bucket_1_9': 4,
            'bucket_10_99': 5,
            'bucket_100plus': 1,
            'corpus_genizah': 10,
            'corpus_local': 1,
            'corpus_all': 1,
        },
        'comp_variants': { ... },
    },
    'sample_n': 1,                # <-- already allowlisted
    'session_id': '...',          # <-- already allowlisted
    'flush_reason': 'periodic',   # <-- NEW allowlisted key (or reuse 'action')
}
```

**`_scrub_value` on the nested dict:** will pass through integer/float values unchanged (lines
257–268: `other: passed through unchanged (int, bool, float, None, etc.)`). String mode keys
like `'keyword'` will run through path/Hebrew redaction — these are code-word strings, not
paths or Hebrew, so they survive unchanged. Safe.

**Banned-key check on nested dict:** `_is_banned_key('count')` → `'count' not in _BANNED_KEYS`
(verified: `_BANNED_KEYS` does not contain `'count'`). `'median_ms'`, `'p95_ms'`, `'min_ms'`,
`'max_ms'`, `'zero_result_count'`, `'bucket_*'`, `'corpus_*'` are all safe — none match
`_BANNED_KEYS` entries (`query`, `text`, `content`, `filename`, `path`, etc.).

**Three new `_ALLOWED_PROPS` keys needed:**
1. `'perf_summary'` — container for nested per-mode stats
2. `'operation_kind'` — indexing event operation type
3. `'doc_count_bucket'` — indexing event doc count bucket

`'flush_reason'` can reuse the existing `'action'` key (already allowlisted) — value would be
`'periodic'` or `'close'` or `'manual'`. This avoids a 4th new key.

---

## Standard Stack (no new packages)

This phase adds zero new dependencies. All needed tools are built-in or already present.

| Component | Location | Version | Notes |
|-----------|----------|---------|-------|
| `time.monotonic()` | Python stdlib | all | For duration measurement in threads |
| `statistics` module | Python stdlib | all | NOT needed — hand-roll percentile is simpler |
| `pyqtSignal` | PyQt6 (existing) | 6.x | For `perf_signal(float, int)` |
| `QTimer` | PyQt6 (existing) | 6.x | Reuse `_ping_check_timer` |
| `desktop/telemetry.py` | Project | Phase 111 | Accumulator + flush live here |

## Package Legitimacy Audit

No new packages. Section N/A.

---

## Architecture Patterns

### Data Flow Diagram

```
SearchThread.run()
  ├── [start] t0 = time.monotonic()
  ├── searcher.execute_search(...)
  ├── results_signal.emit(results)           ← existing
  └── perf_signal.emit(elapsed_ms, len(results))  ← NEW

UI thread (GenizahGUI._on_perf_signal):
  └── telemetry.accumulate_performance(elapsed_ms, result_count, mode, corpus_scope)
        └── _perf_accumulator[mode]['durations_ms'].append(elapsed_ms)
              (+ result count, corpus, zero-result tracking)

QTimer(5-min) timeout  ──────────────────────────────────────────────────────────┐
applicationStateChanged (focus) ─────────────────────────────────────────────────┤
                                                                                  ▼
                                      GenizahGUI._maybe_flush_perf_summary()
                                        ├── Guard: interval elapsed?
                                        └── telemetry.flush_perf_if_due()
                                              ├── compute median/p95/min/max per mode
                                              ├── build 'perf_summary' nested payload
                                              ├── track_performance(SESSION_PERF, ...)  → enqueue_event
                                              └── reset accumulator; update _perf_last_flush_time

closeEvent():
  └── telemetry.flush_perf_unconditionally()  (skip interval check)
        └── track_performance(SESSION_PERF, ...)  → enqueue_event → _atexit_flush drains


LocalIndexerWorker.run():
  ├── t0 = time.monotonic()
  ├── result = self._indexer.scan_all(...)
  ├── elapsed_ms = (time.monotonic() - t0) * 1000
  └── finished_signal.emit(result, elapsed_ms)    ← NEW (or store as instance var)

MyLibraryTab._on_worker_finished(result, toast):
  └── telemetry.track_performance(
        DesktopEvent.INDEXING_COMPLETE,
        duration_ms=elapsed_ms,
        operation_kind='reindex_all',   # literal constant from MyLibraryTab
        doc_count_bucket=_telemetry_result_bucket(total_files),
      )
```

### Recommended Project Structure additions

```
desktop/
└── telemetry.py    # Add: accumulate_performance(), flush_perf_if_due(),
                    #       flush_perf_unconditionally(), _flush_perf_summary(),
                    #       _perf_accumulator dict, _perf_last_flush_time,
                    #       _perf_sample_counter, _percentile()
                    #       DesktopEvent.INDEXING_COMPLETE (new enum member)
                    #       'perf_summary', 'operation_kind', 'doc_count_bucket' in _ALLOWED_PROPS
gui_threads.py      # Add: perf_signal(float, int) to 4 classes; monotonic timing
genizah_app.py      # Add: _on_perf_signal(), _maybe_flush_perf_summary() in _maybe_emit_active_ping chain,
                    #       close-flush in closeEvent, perf_signal connection at thread-start sites
desktop/my_library_tab.py  # Add: elapsed_ms tracking in LocalIndexerWorker;
                            #       indexing telemetry emit in _on_worker_finished, _on_lab_rebuild_finished
tests/              # Add: test_telemetry_phase115.py
```

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Streaming percentile estimator | T-Digest, P2 | Plain sorted list + index | Volume is <100 elements; exact is simpler and sufficient |
| New timer for periodic flush | New QTimer | Reuse existing `_ping_check_timer` (5-min) | Already connected to applicationStateChanged; no new timer needed |
| New allowlist mechanism | Custom property validator | Extend existing `_ALLOWED_PROPS` frozenset | Already enforced by `_validate_props()` |
| Thread-safe dict for accumulator | Locks + shared dict | Module-level dict written only from UI thread | Qt signal-slot guarantees UI-thread delivery; no lock needed |

---

## Common Pitfalls

### Pitfall 1: Emitting `perf_signal` on the worker thread bypassing the consent gate

**What goes wrong:** If a developer calls `telemetry.accumulate_performance()` directly from
inside `SearchThread.run()` (worker thread), they bypass the UI-thread invariant and call into
`desktop/telemetry.py` from a non-UI thread. While the accumulator write itself would be safe
(list.append is GIL-safe), it establishes a bad precedent and breaks the "accumulator only on
UI thread" contract.

**How to avoid:** The `perf_signal(float, int)` Qt signal delivers to the UI thread
automatically. The handler (`_on_perf_signal`) calls `telemetry.accumulate_performance()` from
the UI thread. Never call `accumulate_performance()` from inside thread `run()`.

**Warning signs:** Any direct `import desktop.telemetry` in `gui_threads.py`. The PRIV-03 AST
guard allows `desktop/` files to import `desktop/telemetry` (it only bans `shared.posthog_server`).
So the guard won't catch this — it requires code review discipline.

### Pitfall 2: `search_start_time` is `time.time()` (wall clock) — not reusable for perf

**What goes wrong:** `search_start_time = time.time()` (line 17541) exists on the GUI and looks
tempting to reuse. But it is wall clock (not monotonic) and measured from the UI thread, not
from inside `SearchThread.run()` where execution actually begins (there is a scheduling delay).

**How to avoid:** Measure `t0 = time.monotonic()` as the FIRST LINE of each thread's `run()`
method. Emit `(time.monotonic() - t0) * 1000` as `elapsed_ms` in `perf_signal`. The existing
`search_start_time` is for the UI elapsed-display and must not be replaced.

### Pitfall 3: Cancelled run emitting a partial duration

**What goes wrong:** `InterruptedError` in `SearchThread.run()` at line 116 causes the thread
to exit without reaching the `perf_signal.emit()` call. If `perf_signal` is placed before the
`except` block, a cancelled run emits a truncated duration.

**How to avoid:** `perf_signal` must be emitted ONLY from the success path — after
`results_signal.emit(results)` at line 115 (BEFORE the `except InterruptedError` at line 116).
If the search was cancelled, `results_signal.emit([])` fires at line 118 — do NOT emit
`perf_signal` for this path. D-08 says "completed runs only." The `was_cancelled` flag on the UI
side confirms the cancelled state; `perf_signal` simply not being emitted on the cancelled path
is the correct guard.

### Pitfall 4: Qt hard-exit (SIGKILL) losing the accumulator

**What goes wrong:** Phase 113 documents that hard exits are common. If the app is SIGKILLed,
the `_atexit_flush` and `closeEvent` do not run — any accumulated perf data since the last
periodic flush is lost.

**Mitigation (not a bug, just a design tradeoff):** The periodic flush (D-04) is chosen
specifically for crash-resilience. With a 30-min cadence, at most 30 min of data is lost on a
hard exit. This is acceptable per the user's stated priority ("crash-resilience prioritized over
minimal volume"). Phase 115 does not add a new persistence mechanism. Document the ~30-min loss
window in the runbook (Phase 116).

### Pitfall 5: LabRebuildWorker.finished_signal() carries no arguments

**What goes wrong:** `LabRebuildWorker.finished_signal = pyqtSignal()` at line 771 carries zero
arguments. The current `run()` method (lines 780–786) does not return a doc count.

**How to avoid:** Either (a) change `finished_signal` to carry `(int,)` (total docs indexed) and
update the signature — but this changes a connected signal which might require updating
`_on_lab_rebuild_finished` — or (b) capture the `elapsed_ms` inside `run()` and store it as
`self._lab_elapsed_ms`, then read it in the UI-thread slot. Option (b) is simpler and avoids
signal signature changes. Since `LabRebuildWorker` is only connected in one place
(`_on_lab_rebuild_finished` at line 1207), option (a) is also low-risk. Recommend option (a) for
cleanliness: add `(float, int)` to the signal (elapsed_ms, total_docs).

### Pitfall 6: `_ALLOWED_PROPS` is a `frozenset` — can't mutate at runtime

**What goes wrong:** If the planner tries to add keys to `_ALLOWED_PROPS` dynamically (e.g.
`_ALLOWED_PROPS.add('perf_summary')`), it raises `AttributeError` because it is a `frozenset`
(line 292).

**How to avoid:** Replace the `frozenset(...)` literal at line 292 with a new `frozenset(...)` that
includes the Phase 115 keys. This is the only correct approach. The planner must edit the literal
in `desktop/telemetry.py`.

### Pitfall 7: `accumulate_performance()` called before `_telemetry_ready()` / `is_enabled()`

**What goes wrong:** A `perf_signal` emitted by a thread started before the
telemetry coordinator ran would call `accumulate_performance()` and write to the accumulator
before consent is confirmed. On flush, the accumulated data would be emitted even if the user
later declines consent.

**How to avoid:** `accumulate_performance()` must check `is_enabled()` as its first guard
(consistent with all other public telemetry functions). Additionally, `_flush_perf_summary()`
checks `_telemetry_ready()` + `is_enabled()` before emitting. Accumulating before consent and
then dropping on flush (because `is_enabled()` is False at flush time) is acceptable — the data
never leaves the process.

### Pitfall 8: `perf_signal` naming collision with existing `progress_signal`

**What goes wrong:** All four target threads already have a `progress_signal = pyqtSignal(int, int)`.
Adding a new signal with the same type signature could cause confusion or accidental cross-connection.

**How to avoid:** Use a distinct name `perf_signal` (or `search_perf_signal`) that does not
shadow any existing signal. The `(float, int)` type signature is also distinct from the existing
`(int, int)` progress signal.

---

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest (existing) |
| Config file | none detected — standard pytest discovery |
| Quick run command | `pytest tests/test_telemetry_phase115.py -x` |
| Full suite command | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/ -x --ignore=tests/test_telemetry_phase115.py` then add 115 |
| Existing telemetry test files | 12 files in `tests/test_telemetry_*.py` |

### Testing Without Live PostHog

All existing telemetry tests mock at the `ph._event_queue` level (per 111-PATTERNS.md pattern).
For Phase 115:

**Monkeypatch points:**
1. `ph._event_queue` (as existing tests do) — confirms emission occurs.
2. `desktop.telemetry._perf_accumulator` — inspect accumulator state.
3. `desktop.telemetry._perf_last_flush_time` — set to past values to trigger flush.
4. `telemetry.is_enabled` → monkeypatch `_enabled = True` using `_reset_for_tests()` + manual
   state set (same pattern as `test_telemetry_consent_gate.py`).

**Assert "never one event per search":** Call `accumulate_performance()` 10 times; assert that
`ph._event_queue.qsize()` is still 0 (no event emitted). Then call `_flush_perf_summary()`;
assert exactly 1 event in queue with `event == 'desktop_session_performance_summary'`.

**Assert "buckets only, no raw counts/text":** After flush, inspect the queued event's
`properties['perf_summary']` nested dict. Assert no key is a plain unbucketed integer count
other than the defined stats keys (count, zero_result_count, corpus_*, bucket_*). Assert no
Hebrew text, no paths.

**Assert "reset on flush" (D-06):** Call `accumulate_performance()` 3 times, flush, then call
`accumulate_performance()` 2 more times. Assert the second flush event's `count` equals 2, not 5.

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| PERF-01 (search) | `perf_signal` emitted on SearchThread completion | unit | `pytest tests/test_telemetry_phase115.py::test_search_thread_emits_perf_signal` | ❌ Wave 0 |
| PERF-01 (indexing) | `INDEXING_COMPLETE` event emitted with duration_ms | unit | `pytest tests/test_telemetry_phase115.py::test_indexing_complete_event_shape` | ❌ Wave 0 |
| PERF-02 | result_count in summary is bucketed | unit | `pytest tests/test_telemetry_phase115.py::test_perf_summary_buckets_only` | ❌ Wave 0 |
| PERF-03 (aggregate) | accumulate 10 searches → 1 flush event | unit | `pytest tests/test_telemetry_phase115.py::test_no_per_search_events` | ❌ Wave 0 |
| PERF-03 (reset) | accumulator resets after flush (D-06) | unit | `pytest tests/test_telemetry_phase115.py::test_accumulator_resets_on_flush` | ❌ Wave 0 |
| PERF-03 (env config) | GENIZAH_PERF_SAMPLE_N=2 skips odd runs | unit | `pytest tests/test_telemetry_phase115.py::test_sample_n_skips_runs` | ❌ Wave 0 |
| D-09 (consent gate) | accumulate_performance() no-ops when disabled | unit | `pytest tests/test_telemetry_phase115.py::test_accumulate_disabled_when_no_consent` | ❌ Wave 0 |
| D-07 (nested payload) | perf_summary key allowlisted; nested dict survives scrubber | unit | `pytest tests/test_telemetry_phase115.py::test_perf_summary_survives_scrubber` | ❌ Wave 0 |

### Existing Guard Tests That Must Continue to Pass

| Test file | What it guards | Phase 115 risk |
|-----------|---------------|----------------|
| `tests/test_telemetry_no_direct_posthog.py` | No desktop/ file except telemetry.py may call enqueue_event | LOW — new code in telemetry.py only; PRIV-03 passes |
| `tests/test_no_dynamic_telemetry_strings.py` | No forbidden UI accessors in telemetry call arguments | LOW — only literal constants used |
| `tests/test_telemetry_allowlist.py` | All DesktopEvent values start with `desktop_` | Must add `INDEXING_COMPLETE` to enum; new value `'desktop_indexing_complete'` passes |

### Wave 0 Gaps

- [ ] `tests/test_telemetry_phase115.py` — 8 test cases listed above
- [ ] `desktop/telemetry._reset_for_tests()` — must be extended to clear `_perf_accumulator`,
      `_perf_last_flush_time`, `_perf_sample_counter` (no new file, just edit existing function)

---

## Security Domain

The accumulator stores only duration floats, integer counts, and string mode-enum keys. No query
text, no file paths, no PII. The existing scrubber and allowlist are sufficient. No new
ASVS categories introduced.

| ASVS Category | Applies | Control |
|---------------|---------|---------|
| V5 Input Validation | Yes — perf values are floats/ints | `_ALLOWED_PROPS` + type checking in `accumulate_performance()` |
| V6 Cryptography | No | — |
| PII leakage | Mitigated | No raw counts, no query text; allowlist + scrubber |

---

## Scope Assessment — Should the Planner Split or Reconsider?

**No split recommended.** The work is well-scoped:

1. **`gui_threads.py`** — mechanical signal addition to 4 classes (~5 lines per class, ~20 lines total).
2. **`desktop/telemetry.py`** — accumulator + flush logic (~60–80 lines new).
3. **`genizah_app.py`** — signal connections + flush wiring (~40–50 lines).
4. **`desktop/my_library_tab.py`** — timing + telemetry calls (~15–20 lines).
5. **Tests** — single test file (~100–150 lines).

**Total estimated delta: ~230–340 lines across 5 files.** Well within one phase. No architectural
risk beyond the Qt thread-safety point (which is handled by the signal-slot pattern).

**One potential scope creep to reject:** Do not time `GroupingThread` (confirmed D-08 exclusion).
Do not add a "sample_n skip" at the accumulator read level (only skip at accumulate-write time).
Do not add a disk spool or SQLite persistence for the accumulator — INFRA-05 forbids disk
spooling; memory-only is correct.

---

## Sources

### Primary (HIGH confidence)
- `C:\Genizahsearch\desktop\telemetry.py` — read in full (1,445 lines); all line numbers verified
- `C:\Genizahsearch\gui_threads.py` — read in full (1,186 lines); all thread classes verified
- `C:\Genizahsearch\genizah_app.py` — targeted reads at lines 3278, 3655–3766, 17505–17700, 18052–18236, 22829–22968, 26907–26956
- `C:\Genizahsearch\desktop\my_library_tab.py` — targeted reads at lines 699–820, 1190–1200, 1685–1715
- `C:\Genizahsearch\tests\test_telemetry_no_direct_posthog.py` — read in full
- `C:\Genizahsearch\tests\test_no_dynamic_telemetry_strings.py` — read in full
- `.planning/phases/115-performance-metrics/115-CONTEXT.md` — all decisions verified
- `.planning/REQUIREMENTS.md` — PERF-01..03 rows verified
- `.planning/phases/114-usage-analytics/114-CONTEXT.md` — D-09 per-run object, D-14 monotonic, D-16 active_ping
- `.planning/phases/113-crash-reporting/113-CONTEXT.md` — _flush_before_exit, atexit pattern
- `.planning/phases/111-telemetry-foundation/111-CONTEXT.md` — chokepoint, _ALLOWED_PROPS, PRIV-06
- `.planning/phases/111-telemetry-foundation/111-PATTERNS.md` — test autouse fixture pattern

### Secondary (MEDIUM confidence)
- `shared/local_indexer.py` line 4285 — `build_lab_side_index` exists; LabRebuildWorker wraps it at `my_library_tab.py:760`

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — zero new packages; all existing
- Architecture: HIGH — all wire-in points verified from live code
- Pitfalls: HIGH — derived from verified code behavior + prior-phase patterns
- Line numbers: VERIFIED from live files (may drift with future edits)

**Research date:** 2026-06-16
**Valid until:** 2026-07-16 (stable; line numbers drift if genizah_app.py is edited before planning)
