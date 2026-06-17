# Phase 115: Performance Metrics - Pattern Map

**Mapped:** 2026-06-16
**Files analyzed:** 6 (5 edits, 1 new test file)
**Analogs found:** 6 / 6

---

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `desktop/telemetry.py` (edit) | service/chokepoint | event-driven | itself (Phases 111/113/114) — new accumulator + flush logic mirrors existing `track_performance()` + `_reset_for_tests()` | exact |
| `gui_threads.py` (edit) | worker thread | event-driven | itself — new `perf_signal` mirrors existing `progress_signal`/`results_signal` declarations on the same 4 classes | exact |
| `genizah_app.py` (edit) | application controller | event-driven | itself — `_maybe_emit_active_ping()` (`:3731`) + `_emit_search_telemetry()` (`:17611`) + `closeEvent()` (`:26907`) | exact |
| `desktop/my_library_tab.py` (edit) | component/worker | event-driven | itself — `LocalIndexerWorker.run()` (`:729`) + `_on_worker_finished()` (`:1699`) + `LabRebuildWorker.run()` (`:780`) + `_on_lab_rebuild_finished()` (`:1207`) | exact |
| `shared/local_indexer.py` (no edit needed per RESEARCH §Scope) | service | batch | N/A — timing injected in `my_library_tab.py` worker wrapper, not in LocalIndexer itself | N/A |
| `tests/test_telemetry_phase115.py` (new) | test | transform + event-driven | `tests/test_telemetry_consent_gate.py` (autouse fixture shape) + `tests/test_posthog_server.py` (queue-drain assertions) | exact |

---

## Pattern Assignments

### `desktop/telemetry.py` — accumulator + flush additions

**Analog:** itself (Phases 111–114). All new code is ADDITIVE — no existing functions modified
except `_reset_for_tests()` (extended) and the `_ALLOWED_PROPS` frozenset literal (replaced with
a larger set) and the `DesktopEvent` enum (one member added).

#### 1. New `DesktopEvent` enum member

**Pattern source:** `desktop/telemetry.py` lines 130–165 (the existing enum block).

Add the new member immediately after `SESSION_PERF` in the `# Performance (Phase 115)` block:

```python
# desktop/telemetry.py lines 161-162 (existing, keep)
    # Performance (Phase 115)
    SESSION_PERF   = 'desktop_session_performance_summary'
    # ADD after SESSION_PERF (Phase 115 D-01/D-02):
    INDEXING_COMPLETE = 'desktop_indexing_complete'
```

Convention enforced by `tests/test_telemetry_allowlist.py`: every value must start with
`desktop_` (or `$` for protocol events). `'desktop_indexing_complete'` passes.

#### 2. `_ALLOWED_PROPS` frozenset expansion

**Pattern source:** `desktop/telemetry.py` lines 292–313 (the existing frozenset literal).
`_ALLOWED_PROPS` is a `frozenset` — it cannot be mutated at runtime. Replace the literal
at line 292 with a new one containing the Phase 115 additions.

```python
# desktop/telemetry.py lines 292-313 (existing block — shown for context)
_ALLOWED_PROPS: frozenset[str] = frozenset({
    # Identity / session
    'platform', 'app_version', 'os_family', 'os_version',
    'python_version', 'pyqt_version', 'ui_language',
    'session_id', 'install_id',
    '$process_person_profile',
    # PostHog identity protocol
    '$anon_distinct_id', '$set', '$set_once',
    # Usage (Phases 114+)
    'tab_name', 'search_mode', 'corpus_scope',
    'result_count_bucket', 'duration_bucket_ms',
    'feature_name', 'dialog_name', 'action',
    # Crash (Phase 113+)
    'exc_type', 'exc_module', 'exc_lineno',
    'error_fingerprint', 'is_background_thread', 'fatal_error',
    # Perf (Phase 115+) — existing
    'duration_ms', 'result_count', 'sample_n',
    # Context label
    'context',
    # Phase 115 NEW additions:
    'perf_summary',       # top-level container key for nested per-mode stats dict (D-07/KQ-8)
    'operation_kind',     # indexing event: enum literal ('initial_scan' etc.) (D-02)
    'doc_count_bucket',   # indexing event: coarse doc count bucket (D-02/KQ-6)
})
```

Note: `'action'` (already allowlisted) is reused for `flush_reason` on the summary event
(`'periodic'` or `'close'`) — no 4th new key needed.

#### 3. Module-level accumulator globals

**Pattern source:** `desktop/telemetry.py` lines 1300–1348 (`_reset_for_tests()` shows all
existing module globals that get reset). New globals follow the same plain-assignment, no-lock
pattern (safe because the accumulator is only ever written from the UI thread via Qt signal-slot).

```python
# Phase 115 accumulator globals — add near the bottom of the module-level state block,
# before _load_consent_state() at line 1358.
# Written from UI thread only (Qt signal-slot guarantee). No lock needed.
_perf_accumulator: dict = {}   # key: mode_str, value: {'durations_ms': [], 'result_counts': [], ...}
_perf_last_flush_time: float = 0.0   # time.monotonic() of last flush; 0.0 = never flushed
_perf_sample_counter: int = 0        # incremented per accumulate_performance() call; sample_n gate
```

Shape of each mode entry in `_perf_accumulator`:
```python
{
    'durations_ms':    [],   # list[float], one per completed search (after sample_n gate)
    'result_counts':   [],   # list[int], parallel to durations_ms
    'zero_result_count': 0,
    'corpus_counts': {'genizah': 0, 'local': 0, 'all': 0},
}
```

#### 4. `accumulate_performance()` — new public function

**Pattern source:** `desktop/telemetry.py` lines 710–741 (`track_performance()`) for the
guard pattern (is_enabled first, broad try/except, never raise). This function ONLY writes to
the accumulator — it never calls `_emit()`.

```python
def accumulate_performance(
    elapsed_ms: float,
    result_count: int,
    mode: str,
    corpus_scope: str,
) -> None:
    """Accumulate one completed-search perf record into the in-memory summary.

    Called from the UI thread (Qt perf_signal slot). Never emits — the flush
    functions emit. Never raises. Respects GENIZAH_PERF_SAMPLE_N env knob.
    Guards: is_enabled() only (consent gate). No _telemetry_ready() needed here —
    the GUI handler already guards with _telemetry_ready() before calling this.
    """
    try:
        if not is_enabled():
            return
        global _perf_sample_counter
        _perf_sample_counter += 1
        sample_n = int(os.environ.get('GENIZAH_PERF_SAMPLE_N', '1'))
        if sample_n > 1 and (_perf_sample_counter % sample_n) != 1:
            return  # skip this sample
        entry = _perf_accumulator.setdefault(mode, {
            'durations_ms': [],
            'result_counts': [],
            'zero_result_count': 0,
            'corpus_counts': {'genizah': 0, 'local': 0, 'all': 0},
        })
        entry['durations_ms'].append(float(elapsed_ms))
        entry['result_counts'].append(int(result_count))
        if result_count == 0:
            entry['zero_result_count'] += 1
        scope_key = corpus_scope if corpus_scope in ('genizah', 'local', 'all') else 'genizah'
        entry['corpus_counts'][scope_key] = entry['corpus_counts'].get(scope_key, 0) + 1
    except Exception:
        logger.debug('telemetry: accumulate_performance() silently failed', exc_info=True)
```

#### 5. `_percentile()` helper — internal

**Pattern source:** `desktop/telemetry.py` overall pure-function style. No analog exists; hand-roll
(RESEARCH KQ-3 confirms list-based exact percentile is correct at this scale):

```python
def _percentile(durations: list[float], p: float) -> float:
    """Exact percentile over a pre-sorted list. Returns 0.0 for empty."""
    if not durations:
        return 0.0
    s = sorted(durations)
    idx = min(int(p * len(s)), len(s) - 1)
    return s[idx]
```

#### 6. `_flush_perf_summary()` — internal flush

**Pattern source:** `desktop/telemetry.py` lines 710–741 (`track_performance()`) for the
guard + try/except shape. Also mirrors `_maybe_emit_active_ping()` guard ordering
(`genizah_app.py:3741–3766`). Calls `track_performance()` with the pre-built summary payload
and resets the accumulator (D-06).

```python
def _flush_perf_summary(flush_reason: str = 'periodic') -> None:
    """Build and emit desktop_session_performance_summary, then reset accumulator (D-06).

    Called from genizah_app.py flush helpers (UI thread). Never raises.
    flush_reason: 'periodic' | 'close' | 'manual' — maps to allowlisted 'action' key.
    """
    try:
        if not is_enabled():
            return
        if not _perf_accumulator:
            return  # nothing to flush
        perf_summary: dict = {}
        for mode_key, entry in _perf_accumulator.items():
            durs = entry['durations_ms']
            counts = entry['result_counts']
            if not durs:
                continue
            # Build per-bucket distribution using _telemetry_result_bucket (D-03).
            # Import here (runtime) to avoid circular import — genizah_app is not
            # importable from telemetry. Mirror D-03: coarse 0/1-9/10-99/100+.
            buckets = {'0': 0, '1-9': 0, '10-99': 0, '100+': 0}
            for rc in counts:
                b = ('0' if rc == 0 else '1-9' if rc < 10 else '10-99' if rc < 100 else '100+')
                buckets[b] = buckets.get(b, 0) + 1
            perf_summary[mode_key] = {
                'count':             len(durs),
                'median_ms':         round(_percentile(durs, 0.5), 1),
                'p95_ms':            round(_percentile(durs, 0.95), 1),
                'min_ms':            round(min(durs), 1),
                'max_ms':            round(max(durs), 1),
                'zero_result_count': entry['zero_result_count'],
                'bucket_0':          buckets['0'],
                'bucket_1_9':        buckets['1-9'],
                'bucket_10_99':      buckets['10-99'],
                'bucket_100plus':    buckets['100+'],
                'corpus_genizah':    entry['corpus_counts'].get('genizah', 0),
                'corpus_local':      entry['corpus_counts'].get('local', 0),
                'corpus_all':        entry['corpus_counts'].get('all', 0),
            }
        if not perf_summary:
            _perf_accumulator.clear()
            return
        sample_n = int(os.environ.get('GENIZAH_PERF_SAMPLE_N', '1'))
        track_performance(
            DesktopEvent.SESSION_PERF,
            duration_ms=0.0,    # not meaningful for summary; required by track_performance signature
            perf_summary=perf_summary,
            sample_n=sample_n,
            action=flush_reason,   # 'action' already allowlisted; reuse for flush_reason (KQ-8)
        )
        # D-06: reset so next window is independent
        _perf_accumulator.clear()
        global _perf_last_flush_time
        _perf_last_flush_time = time.monotonic()
    except Exception:
        logger.debug('telemetry: _flush_perf_summary() silently failed', exc_info=True)
```

#### 7. Public flush entry-points (called from `genizah_app.py`)

**Pattern source:** same try/except + is_enabled guard shape.

```python
def flush_perf_if_due(flush_interval_secs: float = 1800.0) -> None:
    """Flush only if enough active time has elapsed (D-04/D-05 periodic flush).

    Called from GenizahGUI._maybe_flush_perf_summary() on the UI thread.
    Default interval 1800s (30 min); override via GENIZAH_PERF_FLUSH_INTERVAL env var.
    """
    try:
        interval = float(os.environ.get('GENIZAH_PERF_FLUSH_INTERVAL', str(int(flush_interval_secs))))
        if time.monotonic() - _perf_last_flush_time >= interval:
            _flush_perf_summary(flush_reason='periodic')
    except Exception:
        logger.debug('telemetry: flush_perf_if_due() silently failed', exc_info=True)


def flush_perf_unconditionally() -> None:
    """Flush regardless of elapsed time (D-09 close flush).

    Called from GenizahGUI.closeEvent() on the UI thread.
    """
    try:
        _flush_perf_summary(flush_reason='close')
    except Exception:
        logger.debug('telemetry: flush_perf_unconditionally() silently failed', exc_info=True)
```

#### 8. `_reset_for_tests()` extension

**Pattern source:** `desktop/telemetry.py` lines 1300–1348 (existing `_reset_for_tests()`).
Extend by adding three new `global` declarations and reset assignments:

```python
# Extend _reset_for_tests() — add these lines immediately after the existing Phase 113 resets:
    # Phase 115 perf-accumulator globals (no locks — UI-thread-only writes)
    global _perf_accumulator, _perf_last_flush_time, _perf_sample_counter
    _perf_accumulator = {}
    _perf_last_flush_time = 0.0
    _perf_sample_counter = 0
```

---

### `gui_threads.py` — `perf_signal` additions on 4 thread classes

**Analog:** `gui_threads.py` itself — the existing signal declarations on `SearchThread`
(lines 83–85), `LabSearchThread` (lines 126–129), `CompositionThread` (lines 171–173),
`LabCompositionThread` (lines 230–233). Copy the `pyqtSignal` class-attribute pattern exactly.

#### Signal declaration pattern (lines 83–85 — copy for all 4 classes)

```python
# gui_threads.py lines 80-85 (existing SearchThread signals — the pattern to extend)
class SearchThread(QThread):
    """Execute a search query asynchronously."""

    results_signal = pyqtSignal(list)
    progress_signal = pyqtSignal(int, int)
    error_signal = pyqtSignal(str)
    # Phase 115: performance signal — (elapsed_ms: float, result_count: int)
    # (float, int) is distinct from progress_signal's (int, int) — no confusion possible.
    perf_signal = pyqtSignal(float, int)    # ADD after error_signal
```

Apply the same declaration to `LabSearchThread` (after line 129), `CompositionThread` (after
line 173), `LabCompositionThread` (after line 233). `GroupingThread` does NOT get
`perf_signal` (D-08 exclusion).

#### Timing + emit pattern in `run()` — SearchThread

**Pattern source:** `gui_threads.py` lines 96–121 (`SearchThread.run()`). The key constraints
from RESEARCH §Pitfall 2 and §Pitfall 3: `t0` is the FIRST line of `run()`; emit is ONLY on
the success path, after `results_signal.emit(results)`, BEFORE the `except InterruptedError` block.

```python
# gui_threads.py lines 96-121 (existing SearchThread.run() — annotated with Phase 115 changes)
def run(self):
    _prevent_sleep()
    t0 = time.monotonic()           # ADD as first line (Pitfall 2 — must be before any work)
    try:
        def cb(curr, total):
            if self.cancel_flag:
                raise InterruptedError("Search cancelled by user")
            self.progress_signal.emit(curr, total)
        results = self.searcher.execute_search(
            self.query, self.mode, self.gap, progress_callback=cb,
            exclude_words=self.exclude_words, responsa_options=self.responsa_options,
            restrict_sys_ids=self.restrict_sys_ids, text_position=self.text_position,
            corpus_scope=self.corpus_scope,
        )
        self.results_signal.emit(results)
        # Phase 115: emit perf signal — ONLY on success path (D-08 / Pitfall 3)
        elapsed_ms = (time.monotonic() - t0) * 1000.0
        self.perf_signal.emit(elapsed_ms, len(results))    # ADD here
    except InterruptedError:
        # Cancelled — do NOT emit perf_signal (Pitfall 3 / D-08 "completed runs only")
        self.results_signal.emit([])
    except Exception as e:
        self.error_signal.emit(str(e))
    finally:
        _allow_sleep()
```

#### Timing + emit pattern for `LabSearchThread` (lines 144–166)

```python
# LabSearchThread.run() — success emit is at line 163
def run(self):
    _prevent_sleep()
    t0 = time.monotonic()           # ADD first
    try:
        def cb(arg1, arg2=None):
            ...
        results = self.lab_engine.lab_search(...)
        self.results_signal.emit(results)
        # Phase 115: emit on success path only
        self.perf_signal.emit((time.monotonic() - t0) * 1000.0, len(results))   # ADD
    except Exception as e:
        self.error_signal.emit(str(e))    # cancelled/error — no perf_signal
    finally:
        _allow_sleep()
```

Note: `LabSearchThread` has no `cancel_flag` / `InterruptedError` path — the `except Exception`
already handles all exits. No `perf_signal` in the exception branch (Pitfall 3 applies to
SearchThread; for LabSearchThread, exceptions also suppress the signal).

#### Timing + emit pattern for `CompositionThread` (lines 201–225)

```python
# CompositionThread.run() — success emit is scan_finished_signal at line 222
def run(self):
    _prevent_sleep()
    t0 = time.monotonic()           # ADD first
    try:
        ...
        result = self.searcher.search_composition_logic(...)
        self.scan_finished_signal.emit(result)
        # Phase 115: result length: dict case vs list case
        _rc = len(result.get('main', [])) if isinstance(result, dict) else len(result)
        self.perf_signal.emit((time.monotonic() - t0) * 1000.0, _rc)   # ADD
    except Exception as e:
        self.error_signal.emit(str(e))
    finally:
        _allow_sleep()
```

#### Timing + emit pattern for `LabCompositionThread` (lines 259–292)

Same as `CompositionThread` — `scan_finished_signal.emit(result)` at line 289, then `perf_signal`.

---

### `genizah_app.py` — signal wiring + flush hooks

**Analogs:** three sites in `genizah_app.py` itself:
- `_setup_active_ping()` at line 3705 — timer + signal wiring pattern
- `_maybe_emit_active_ping()` at line 3731 — periodic guard chain pattern
- `_emit_search_telemetry()` at line 17611 — per-search UI-thread handler pattern
- `closeEvent()` at line 26907 — session-end try/except block pattern
- Thread start / signal connection at lines 17600–17609

#### Periodic flush wiring — add to `_setup_active_ping()`

**Pattern source:** `genizah_app.py` lines 3705–3723.

```python
# genizah_app.py lines 3705-3723 (existing _setup_active_ping — shown for context)
def _setup_active_ping(self) -> None:
    self._last_ping_date_utc: str | None = None
    self._ping_check_timer = QTimer(self)
    self._ping_check_timer.setInterval(5 * 60 * 1000)  # 5 minutes
    self._ping_check_timer.timeout.connect(self._maybe_emit_active_ping)
    self._ping_check_timer.start()
    # Phase 115 ADD: also connect the 5-min timer to the perf flush check.
    # Reuse existing timer — no second QTimer (RESEARCH KQ-4 / D-04).
    self._ping_check_timer.timeout.connect(self._maybe_flush_perf_summary)
    try:
        QApplication.instance().applicationStateChanged.connect(
            self._on_app_state_changed
        )
    except Exception:
        pass
```

#### `_on_app_state_changed()` extension — add perf flush on focus

**Pattern source:** `genizah_app.py` lines 3725–3729.

```python
# genizah_app.py lines 3725-3729 (existing — extend)
def _on_app_state_changed(self, state) -> None:
    from PyQt6.QtCore import Qt
    if state == Qt.ApplicationState.ApplicationActive:
        self._maybe_emit_active_ping()
        self._maybe_flush_perf_summary()    # ADD: also check perf flush on resume (D-04)
```

#### `_maybe_flush_perf_summary()` — new method

**Pattern source:** `genizah_app.py` lines 3731–3766 (`_maybe_emit_active_ping()`). Mirror
the guard chain exactly — same ordering, same try/except, same never-raise discipline.

```python
# genizah_app.py — new method, add immediately after _maybe_emit_active_ping() (~line 3767)
def _maybe_flush_perf_summary(self) -> None:
    """Periodically flush the per-session perf accumulator (D-04/D-05/KQ-4).

    Guards mirror _maybe_emit_active_ping() (D-04):
    1. _telemetry_ready() — coordinator must have run
    2. is_enabled() — consent gate
    3. not _app_shutting_down — close flush handles exit path
    4. ApplicationActive — only flush when app is active
    5. Interval check is delegated to telemetry.flush_perf_if_due()
    """
    try:
        if not self._telemetry_ready():
            return
        from desktop import telemetry
        from PyQt6.QtCore import Qt
        if not telemetry.is_enabled():
            return
        if getattr(self, '_app_shutting_down', False):
            return  # close flush handles this
        app = QApplication.instance()
        if app is not None and app.applicationState() != Qt.ApplicationState.ApplicationActive:
            return
        telemetry.flush_perf_if_due()
    except Exception:
        pass  # perf flush is best-effort; never raise
```

#### `_on_perf_signal()` — new UI-thread slot

**Pattern source:** `genizah_app.py` lines 17611–17645 (`_emit_search_telemetry()`).
The guard ordering (shutdown first, then ready, then try/except) is the established pattern.

```python
# genizah_app.py — new slot, add near _emit_search_telemetry() (~line 17646)
def _on_perf_signal(self, elapsed_ms: float, result_count: int) -> None:
    """UI-thread slot for SearchThread/LabSearchThread/CompositionThread perf_signal.

    Feeds the in-memory accumulator. NEVER emits directly — accumulate only.
    Mode and corpus_scope are read from the current-search-run object (Phase 114 D-09).
    Guard pattern mirrors _emit_search_telemetry(): shutdown first (REVIEWS HIGH-2).
    """
    # Guard 1 — shutdown flag (mirrors _emit_search_telemetry() line 17624)
    if getattr(self, '_app_shutting_down', False):
        return
    # Guard 2 — session must be started
    if not self._telemetry_ready():
        return
    try:
        run = getattr(self, '_current_search_run', None)
        if run is None:
            return
        mode = run.get('mode', 'keyword')
        corpus = run.get('corpus', 'genizah')
        from desktop import telemetry
        telemetry.accumulate_performance(
            elapsed_ms=elapsed_ms,
            result_count=result_count,
            mode=str(mode),        # mode is a DesktopEvent-style enum; str() is safe
            corpus_scope=corpus,
        )
    except Exception:
        pass
```

For composition threads, the same `_on_perf_signal()` handler works if `_current_comp_search_run`
is also consulted. The planner may either (a) use one slot for both by checking which run object
is set, or (b) add a separate `_on_comp_perf_signal()` that reads `_current_comp_search_run`.
The pattern is identical in either case.

#### Signal connection at thread-start sites

**Pattern source:** `genizah_app.py` lines 17602–17609 (existing signal connections).

```python
# genizah_app.py lines 17602-17609 (existing — shown for context)
self.search_thread.results_signal.connect(self.on_search_finished)
self.search_thread.progress_signal.connect(self._on_search_progress)
# ...
self.search_thread.error_signal.connect(self.on_error)
self.search_thread.start()

# Phase 115 ADD: connect perf_signal before .start()
self.search_thread.perf_signal.connect(self._on_perf_signal)   # ADD before .start()
```

Same pattern at the `LabSearchThread`, `CompositionThread`, and `LabCompositionThread` start sites.

#### `closeEvent()` — perf flush addition

**Pattern source:** `genizah_app.py` lines 26907–26929 (existing closeEvent session_end block).
Insert the close flush immediately AFTER the SESSION_END emission block (after line 26929),
BEFORE the opt-out tree flush (line 26934+).

```python
# genizah_app.py lines 26916-26929 (existing SESSION_END block — shown for context)
try:
    from desktop import telemetry
    if (
        self._telemetry_ready()
        and getattr(self, '_session_id', '')
        and not getattr(self, '_session_end_emitted', False)
    ):
        self._session_end_emitted = True
        telemetry.track(telemetry.DesktopEvent.SESSION_END, session_id=self._session_id)
except Exception:
    pass

# Phase 115 ADD: flush perf accumulator before close (D-09 / KQ-5)
# Must come AFTER SESSION_END (so session_id is already in PostHog for the perf event)
# and BEFORE worker teardown (so the queue drains via _atexit_flush).
try:
    from desktop import telemetry
    if self._telemetry_ready() and not getattr(self, '_perf_flushed_on_close', False):
        self._perf_flushed_on_close = True
        telemetry.flush_perf_unconditionally()
except Exception:
    pass
```

---

### `desktop/my_library_tab.py` — indexing-duration producers

**Analog:** `desktop/my_library_tab.py` itself.
- `LocalIndexerWorker.run()` lines 729–753 — the timing anchor and result emission.
- `LabRebuildWorker.run()` lines 780–786 — the no-argument finished_signal pattern.
- `_on_worker_finished()` lines 1699–1740 — the UI-thread completion slot.
- `_on_lab_rebuild_finished()` lines 1207–1216 — the zero-arg slot pattern.

#### `LocalIndexerWorker` — timing injection in `run()`

**Pattern:** Store `elapsed_ms` as an instance attribute (Pitfall 5 option (b)) so the existing
`finished_signal(dict)` signature does not need to change.

```python
# desktop/my_library_tab.py lines 729-753 (existing LocalIndexerWorker.run() — annotated)
def run(self) -> None:
    try:
        _t0 = time.monotonic()       # ADD as first line (Phase 115 / Pitfall 2 pattern)
        def _on_progress(...): ...
        def _on_file_done(...): ...
        self._indexer._progress_cb = _on_progress
        self._indexer._file_finished_cb = _on_file_done
        self.status_updated.emit("Discovering files… / מאתר קבצים…")
        result = self._indexer.scan_all(cancel_check=lambda: self._cancel_requested)
        self._elapsed_ms = (time.monotonic() - _t0) * 1000.0   # ADD: store for UI-thread slot
        self.finished_signal.emit(result)
    except Exception as exc:
        logger.exception("LocalIndexerWorker: unhandled error")
        self.error_signal.emit(str(exc))
```

Add `operation_kind: str = 'incremental_add'` as a constructor parameter to `LocalIndexerWorker.__init__()`:

```python
# desktop/my_library_tab.py line 720 (existing __init__ — annotated)
def __init__(self, indexer: LocalIndexer, operation_kind: str = 'incremental_add') -> None:
    super().__init__()
    self._indexer = indexer
    self._cancel_requested = False
    self._elapsed_ms: float = 0.0          # ADD: populated in run(), read in slot
    self._operation_kind: str = operation_kind   # ADD: literal constant from caller (D-02)
```

Callers that trigger "Re-index All" pass `operation_kind='reindex_all'`; initial/incremental
callers pass the default `'incremental_add'`. This keeps the value a literal constant, never
derived from UI text (D-04 discipline).

#### `_on_worker_finished()` — telemetry emit

**Pattern source:** `genizah_app.py` lines 17611–17645 (`_emit_search_telemetry()`) for the
guard chain. `my_library_tab.py` imports `desktop.telemetry` lazily (AST guard allows this —
it bans `enqueue_event`, not `telemetry.track_performance()`).

```python
# desktop/my_library_tab.py lines 1699-1740 (existing _on_worker_finished() — annotated)
def _on_worker_finished(self, result: dict, toast: bool) -> None:
    ...
    # (existing UI teardown lines 1709-1740 unchanged)
    ...
    # Phase 115 ADD: emit indexing telemetry AFTER reload (so UI is unblocked first)
    # Must be at end of method to avoid interfering with existing logic.
    try:
        from desktop import telemetry
        if telemetry.is_enabled():
            elapsed_ms = getattr(self._worker_last_elapsed_ms, 'value', 0.0)
            # Retrieve elapsed from the completed worker before _worker = None cleared it.
            # Pattern: stash elapsed on a local var before None-assignment.
            total_files = result.get('total', result.get('processed', 0))
            operation_kind = getattr(self, '_last_operation_kind', 'incremental_add')
            doc_count_bucket = (
                '0' if total_files == 0 else
                '1-9' if total_files < 10 else
                '10-99' if total_files < 100 else
                '100+'
            )
            telemetry.track_performance(
                telemetry.DesktopEvent.INDEXING_COMPLETE,
                duration_ms=elapsed_ms,
                operation_kind=operation_kind,    # literal constant (D-04)
                doc_count_bucket=doc_count_bucket,
            )
    except Exception:
        pass  # telemetry is best-effort; never raise
```

Implementation note: because `self._worker = None` happens at line 1718 (before the telemetry
call placement above), the planner should stash `elapsed_ms = worker._elapsed_ms` and
`operation_kind = worker._operation_kind` BEFORE line 1718. Alternatively, store them on the
tab instance at worker-start time.

#### `LabRebuildWorker` — timing in `run()`

**Pattern source:** RESEARCH §Pitfall 5 option (a) — change `finished_signal` to carry
`(float, int)` for `(elapsed_ms, total_docs)`. This is low-risk (one connection site):

```python
# desktop/my_library_tab.py lines 771 (existing LabRebuildWorker class)
# CHANGE: finished_signal = pyqtSignal()  →  finished_signal = pyqtSignal(float, int)
finished_signal = pyqtSignal(float, int)   # Phase 115: (elapsed_ms, total_docs)

# run() annotated:
def run(self) -> None:
    _t0 = time.monotonic()          # ADD first
    try:
        self._search.rebuild_local_lab_index(self._indexer, lab_engine=self._lab)
        elapsed_ms = (time.monotonic() - _t0) * 1000.0
        # No direct doc count from rebuild; use 0 as unknown-but-nonzero sentinel
        # (doc_count_bucket will be '0' — acceptable, documented in runbook per D-02)
        self.finished_signal.emit(elapsed_ms, 0)     # CHANGE: was .emit()
    except Exception as exc:
        logger.warning("LabRebuildWorker: LOCAL LAB rebuild failed: %s", exc)
        self.error_signal.emit(str(exc))
```

#### `_on_lab_rebuild_finished()` — telemetry emit

**Pattern source:** `desktop/my_library_tab.py` lines 1207–1216 (existing zero-arg slot).

```python
# desktop/my_library_tab.py lines 1207-1216 (existing — annotated)
def _on_lab_rebuild_finished(self, elapsed_ms: float, total_docs: int) -> None:  # sig change
    """Background LAB rebuild done — reload searchers on the UI thread."""
    self._lab_rebuild_worker = None
    try:
        self._reload_all_local_indexes()
    except Exception as exc:
        logger.warning("MyLibraryTab._on_lab_rebuild_finished: reload failed: %s", exc)
    # Phase 115 ADD: emit indexing telemetry
    try:
        from desktop import telemetry
        if telemetry.is_enabled():
            doc_count_bucket = (
                '0' if total_docs == 0 else
                '1-9' if total_docs < 10 else
                '10-99' if total_docs < 100 else
                '100+'
            )
            telemetry.track_performance(
                telemetry.DesktopEvent.INDEXING_COMPLETE,
                duration_ms=elapsed_ms,
                operation_kind='lab_rebuild',      # D-02 literal constant
                doc_count_bucket=doc_count_bucket,
            )
    except Exception:
        pass
```

The `finished_signal` connection at line 1195 must update its slot signature to match the new
`(float, int)` signal:

```python
# desktop/my_library_tab.py line 1195 (existing connection — unchanged, slot sig update handles it)
worker.finished_signal.connect(self._on_lab_rebuild_finished)
# (PyQt6 auto-routes (float, int) args to the updated slot)
```

---

### `tests/test_telemetry_phase115.py` — new test file

**Analog:** `tests/test_telemetry_consent_gate.py` (autouse fixture shape, lines 24–59) +
`tests/test_posthog_server.py` lines 53–64 (queue-drain assertion shape).

#### Autouse fixture — copy from `test_telemetry_consent_gate.py` lines 24–59

```python
# tests/test_telemetry_phase115.py — autouse fixture (exact copy with Phase 115 extension)
@pytest.fixture(autouse=True)
def _reset_telemetry_state(monkeypatch):
    """Reset desktop.telemetry + posthog_server state before/after each test."""
    fake_config: dict = {}

    def fake_load_app_config():
        return dict(fake_config)

    def fake_save_app_config(new_data: dict):
        fake_config.update(new_data)

    import genizah_core
    monkeypatch.setattr(genizah_core, 'load_app_config', fake_load_app_config)
    monkeypatch.setattr(genizah_core, 'save_app_config', fake_save_app_config)

    import desktop.telemetry as tel
    monkeypatch.setattr(tel, 'load_app_config', fake_load_app_config)
    monkeypatch.setattr(tel, 'save_app_config', fake_save_app_config)

    ph._reset_for_tests()
    fresh_q: queue.Queue = queue.Queue(maxsize=10000)
    monkeypatch.setattr(ph, '_event_queue', fresh_q)

    tel._reset_for_tests()       # ALSO clears _perf_accumulator (Phase 115 extension)
    tel._load_consent_state()

    yield fake_config

    tel._reset_for_tests()
    ph._reset_for_tests()
```

**Enable consent for perf tests** (most tests need telemetry enabled):

```python
# Helper to enable telemetry in tests — pattern from test_telemetry_consent_gate.py
def _enable_telemetry(tel, fake_config):
    """Enable telemetry via set_consent(True), which saves to fake_config."""
    tel.set_consent(True)
    assert tel.is_enabled()
```

#### Test: `test_search_thread_emits_perf_signal` (PERF-01)

**Pattern source:** plain PyQt6 signal test — no queue needed. Use `QSignalSpy` or a
simple list-append slot.

```python
def test_search_thread_emits_perf_signal():
    """SearchThread emits perf_signal(elapsed_ms, result_count) on successful completion."""
    from gui_threads import SearchThread
    received = []
    # Mock searcher that returns a 3-result list
    class FakeSearcher:
        def execute_search(self, *a, **kw):
            return [1, 2, 3]
    thread = SearchThread(FakeSearcher(), 'test', 'keyword', 0)
    thread.perf_signal.connect(lambda ms, rc: received.append((ms, rc)))
    thread.run()   # run synchronously (not start()) to stay in test thread
    assert len(received) == 1
    elapsed_ms, result_count = received[0]
    assert elapsed_ms > 0.0
    assert result_count == 3
```

#### Test: `test_no_per_search_events` (PERF-03 aggregate)

**Pattern source:** queue-drain assertion from `test_posthog_server.py` lines 53–64.

```python
def test_no_per_search_events(_reset_telemetry_state):
    """accumulate_performance() 10× → 0 events queued; _flush_perf_summary() → exactly 1."""
    import desktop.telemetry as tel
    import shared.posthog_server as ph
    tel.set_consent(True)

    for _ in range(10):
        tel.accumulate_performance(100.0, 5, 'keyword', 'genizah')

    # No event emitted yet
    assert ph._event_queue.qsize() == 0

    tel._flush_perf_summary(flush_reason='periodic')

    assert ph._event_queue.qsize() == 1
    payload = ph._event_queue.get_nowait()
    assert payload['event'] == 'desktop_session_performance_summary'
    assert 'perf_summary' in payload['properties']
    assert payload['properties']['perf_summary']['keyword']['count'] == 10
```

#### Test: `test_accumulator_resets_on_flush` (D-06)

```python
def test_accumulator_resets_on_flush(_reset_telemetry_state):
    """Flush resets accumulator — second flush event reflects only post-flush data."""
    import desktop.telemetry as tel
    import shared.posthog_server as ph
    tel.set_consent(True)

    for _ in range(3):
        tel.accumulate_performance(200.0, 10, 'keyword', 'genizah')
    tel._flush_perf_summary()
    ph._event_queue.get_nowait()  # drain first event

    for _ in range(2):
        tel.accumulate_performance(300.0, 20, 'keyword', 'genizah')
    tel._flush_perf_summary()

    payload = ph._event_queue.get_nowait()
    assert payload['properties']['perf_summary']['keyword']['count'] == 2  # not 5
```

#### Test: `test_indexing_complete_event_shape` (PERF-01 indexing clause)

**Pattern source:** queue-drain shape + `track_performance()` call pattern.

```python
def test_indexing_complete_event_shape(_reset_telemetry_state):
    """INDEXING_COMPLETE event carries operation_kind, duration_ms, doc_count_bucket."""
    import desktop.telemetry as tel
    import shared.posthog_server as ph
    tel.set_consent(True)

    tel.track_performance(
        tel.DesktopEvent.INDEXING_COMPLETE,
        duration_ms=5000.0,
        operation_kind='reindex_all',
        doc_count_bucket='100+',
    )
    payload = ph._event_queue.get_nowait()
    assert payload['event'] == 'desktop_indexing_complete'
    props = payload['properties']
    assert props['duration_ms'] == 5000.0
    assert props['operation_kind'] == 'reindex_all'
    assert props['doc_count_bucket'] == '100+'
```

#### Test: `test_accumulate_disabled_when_no_consent` (D-09 consent gate)

```python
def test_accumulate_disabled_when_no_consent(_reset_telemetry_state):
    """accumulate_performance() is a no-op when telemetry is not enabled."""
    import desktop.telemetry as tel
    import shared.posthog_server as ph
    # tel is disabled by default after _reset_for_tests()
    assert not tel.is_enabled()

    tel.accumulate_performance(100.0, 5, 'keyword', 'genizah')
    tel._flush_perf_summary()

    assert ph._event_queue.qsize() == 0
```

#### Test: `test_perf_summary_survives_scrubber` (D-07 nested payload / KQ-8)

```python
def test_perf_summary_survives_scrubber():
    """'perf_summary' top-level key is allowlisted; numeric nested dict passes scrubber."""
    from desktop.telemetry import _scrub_props, _validate_props
    props = {
        'perf_summary': {
            'keyword': {'count': 5, 'median_ms': 300.0, 'p95_ms': 800.0},
        },
        'sample_n': 1,
    }
    scrubbed = _scrub_props(props)
    validated = _validate_props(scrubbed)
    assert 'perf_summary' in validated
    assert validated['perf_summary']['keyword']['count'] == 5
    assert validated['perf_summary']['keyword']['median_ms'] == 300.0
```

---

## Shared Patterns

### Gate guard ordering (shutdown → ready → enabled → try/except)
**Source:** `genizah_app.py` lines 17623–17645 (`_emit_search_telemetry()`)
**Apply to:** `_on_perf_signal()`, `_maybe_flush_perf_summary()`, and both `closeEvent` blocks.

```python
# genizah_app.py lines 17623-17628 — the established guard chain
if getattr(self, '_app_shutting_down', False):
    return                         # Guard 1: shutdown first (REVIEWS HIGH-2)
if not self._telemetry_ready():
    return                         # Guard 2: session must be started (MEDIUM-9)
# Guard 3: is_enabled() — checked inside telemetry functions (or explicitly here)
try:
    ...
except Exception:
    pass                           # Never raise into caller
```

### Never-raise / best-effort wrapper
**Source:** `desktop/telemetry.py` lines 706–707 + `shared/posthog_server.py` lines 96–97
**Apply to:** All new public functions in `desktop/telemetry.py`; all telemetry calls in
`genizah_app.py` and `desktop/my_library_tab.py`.

```python
# The outer shell every public telemetry callable uses:
try:
    ...  # all logic here
except Exception:
    logger.debug('telemetry: <function_name>() silently failed', exc_info=True)
```

### Literal-constant value discipline (D-04 / D-02)
**Source:** `genizah_app.py` lines 17634–17643 (only `action='completed'` or `'cancelled'`
as hardcoded string literals, never from a UI accessor).
**Apply to:** `operation_kind` values in `my_library_tab.py` and `flush_reason` values in
`telemetry.py`. These must be Python string literals at the call site, not derived from
`currentText()`, `windowTitle()`, `selectedFiles()`, etc.

### Qt signal `perf_signal = pyqtSignal(float, int)` declaration
**Source:** `gui_threads.py` lines 83–85 (the `pyqtSignal` class-attribute declaration pattern).
**Apply to:** All 4 thread classes: `SearchThread`, `LabSearchThread`, `CompositionThread`,
`LabCompositionThread`. Declare as a class attribute alongside the existing signals.

### Module-level accumulator (no lock, UI-thread-only)
**Source:** `shared/nli_circuit_breaker.py` lines 50–80 (module-level state with dedicated
locks). Phase 115 is simpler — no lock because Qt signal-slot guarantees UI-thread delivery.
**Apply to:** `_perf_accumulator`, `_perf_last_flush_time`, `_perf_sample_counter` in
`desktop/telemetry.py`.

### `_reset_for_tests()` extension pattern
**Source:** `desktop/telemetry.py` lines 1300–1348 (existing `_reset_for_tests()`). Every
phase that adds module-level globals MUST also add them to the reset function (same `global`
declarations, same plain-assignment reset). Tests depend on this seam.
**Apply to:** Phase 115 adds 3 globals — all 3 must appear in the extended `_reset_for_tests()`.

---

## No Analog Found

All files have analogs (all are edits to existing files, or a new test file with an established
test pattern). No entry needed.

---

## Metadata

**Analog search scope:** `desktop/`, `gui_threads.py`, `genizah_app.py`, `tests/test_telemetry_*.py`
**Files read for pattern extraction:** 9 files (desktop/telemetry.py, gui_threads.py,
genizah_app.py × 5 targeted ranges, desktop/my_library_tab.py × 3 targeted ranges,
tests/test_telemetry_consent_gate.py, 111-PATTERNS.md)
**Pattern extraction date:** 2026-06-16

**Pitfall index (see 115-RESEARCH.md for full text):**
- Pitfall 1: Never call `accumulate_performance()` from inside thread `run()` (only from UI-thread slot)
- Pitfall 2: `t0 = time.monotonic()` must be the FIRST line of `run()`, not reused from `search_start_time`
- Pitfall 3: `perf_signal` emitted ONLY on success path — cancelled `InterruptedError` branch suppresses it
- Pitfall 5: `LabRebuildWorker.finished_signal` carries no args — change to `(float, int)` (option a)
- Pitfall 6: `_ALLOWED_PROPS` is a `frozenset` — replace the literal, do not call `.add()`
- Pitfall 7: `accumulate_performance()` must check `is_enabled()` as first guard
