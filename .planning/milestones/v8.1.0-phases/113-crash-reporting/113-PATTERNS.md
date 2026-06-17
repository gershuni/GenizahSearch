# Phase 113: Crash Reporting - Pattern Map

**Mapped:** 2026-06-15
**Files analyzed:** 7 (3 modified modules, 4 new test files)
**Analogs found:** 7 / 7

---

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `desktop/telemetry.py` (modify) | service/chokepoint | event-driven | itself — Phase 111 body; new functions follow `_emit()` / `is_enabled()` / `_reset_for_tests()` shapes | exact |
| `shared/posthog_server.py` (modify) | service/transport | event-driven | itself — `_flush_before_exit` (lines 272-311) + `_drain_and_discard` (lines 257-269); same "direct POST, no queue" shape | exact |
| `genizah_app.py` (modify) | application entry | request-response | itself — `_setup_crash_handler()` (lines 148-170) + `_maybe_show_first_run_prompt()` (line 15856) call sites | exact |
| `tests/test_crash_hooks.py` (new) | test | event-driven | `tests/test_telemetry_consent_gate.py` (autouse fixture + monkeypatch shape) | exact |
| `tests/test_crash_payload.py` (new) | test | transform | `tests/test_telemetry_allowlist.py` + `tests/test_telemetry_scrubbing.py` (pure-function assertion shape) | exact |
| `tests/test_native_crash.py` (new) | test | event-driven | `tests/test_telemetry_consent_gate.py` (consent-state fixture + config mock pattern) | exact |
| `tests/test_crash_priority_send.py` (new) | test | event-driven | `tests/test_telemetry_posthog_server_ext.py` (queue monkeypatch + requests.post capture pattern) | exact |

---

## Pattern Assignments

### `desktop/telemetry.py` — new module-global state additions

**Analog:** itself, lines 79-84 (existing module-level state block).

**Existing module globals to copy the shape of** (lines 79-84):
```python
_enabled: bool = False
_enabled_lock = threading.Lock()
_install_id: str | None = None
_current_distinct_id: str | None = None
_identified: bool = False
_state_lock = threading.Lock()
```

**New module globals to add after the existing block** (same shape, no locks — GIL-safe atomic reads):
```python
# Phase 113 crash-hook globals — read lock-free in the crash hook (D-05)
_crash_distinct_id: str | None = None   # snapshot; written by set_consent/identity, read without lock
_in_crash_hook: bool = False            # recursion guard; plain bool is GIL-safe for single-thread re-entrancy
_hooks_installed: bool = False          # idempotency guard for install_exception_hooks()
_faulthandler_handle = None             # kept open for whole process lifetime (Pitfall 2)
_pending_native_crash: str | None = None  # held when prior native crash but consent not yet True (D-03)
```

---

### `desktop/telemetry.py` — `_is_enabled_nolock()` (new helper)

**Analog:** `is_enabled()` (lines 377-387). New sibling that skips the lock.

**Existing `is_enabled()`** (lines 377-387 — shape to mirror, then strip the lock):
```python
def is_enabled() -> bool:
    """Cached no-throw consent check. ... MUST never raise — called from crash hooks."""
    try:
        with _enabled_lock:
            return _enabled
    except Exception:
        return False
```

**Pattern for `_is_enabled_nolock()`:**
```python
def _is_enabled_nolock() -> bool:
    """Lock-free consent read for crash hooks (D-05 / SC#4).
    Reads _enabled directly (no lock). Safe: CPython GIL ensures a bool read is
    atomic. threading.excepthook runs on the FAILING thread — if that thread held
    _enabled_lock we would deadlock. Worst case: stale False-negative (miss one
    event); stale True-positive is impossible (opt-out clears the key first).
    """
    return _enabled  # direct global read, GIL-safe
```

---

### `desktop/telemetry.py` — `_ALLOWED_PROPS` reconciliation (lines 247-266)

**Analog:** itself, lines 247-266.

**Existing block** (lines 247-266 — read-only reference, surgical changes only):
```python
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
    'traceback_scrubbed', 'thread_name',       # <-- REMOVE BOTH (D-07)
    # Perf (Phase 115+)
    'duration_ms', 'result_count', 'sample_n',
    # Context label
    'context',
})
```

**D-07 changes to apply:**
- REMOVE `'traceback_scrubbed'` — no full-traceback string may ever leave the chokepoint
- REMOVE `'thread_name'` — replaced by a boolean
- ADD `'error_fingerprint'` — `"{exc_type}:{exc_module}:{exc_lineno}"` string
- ADD `'is_background_thread'` — boolean; True when emitted from `threading.excepthook`
- ADD `'fatal_error'` — fixed enum label for native crashes (`desktop_prior_crash` only)

---

### `desktop/telemetry.py` — `_make_crash_props()` (new function)

**Analog:** `track_error()` (lines 603-620) for the "no message, exc_type name only" pattern; `_scrub_value()` (lines 200-223) for the "basename only" path redaction shape.

**`track_error()` shape** (lines 603-620 — shows what NOT to reuse for the hook path):
```python
def track_error(context: str, exc: Exception) -> None:
    """... NEVER includes exception message text (CRASH-04) ..."""
    try:
        if not is_enabled():
            return
        props: dict = {
            'context': context,
            'exc_type': type(exc).__name__,  # safe: class name only, never message
        }
        _emit(DesktopEvent.CRASH.value, props)
    except Exception:
        logger.debug('telemetry: track_error() silently failed', exc_info=True)
```

**Pattern for `_make_crash_props()`** — frame-walk, no `format_exception`, no `str(exc)` (D-07):
```python
# In-app module root basenames — planner finalizes; DO NOT use full paths
_IN_APP_ROOTS: frozenset[str] = frozenset({
    'genizah_app.py', 'genizah_core.py', 'gui_threads.py',
    # All basenames under desktop/ and shared/ added by planner
})

def _make_crash_props(
    exc_type: type,
    exc_tb,            # TracebackType | None
    is_background: bool,
) -> dict:
    """Build crash payload by walking the traceback — no format_exception, no str(exc).

    Finds the innermost IN-APP frame (co_filename basename in _IN_APP_ROOTS).
    Falls back to the deepest frame with error_module='external'.
    D-07 compliance.  Never raises.
    """
    error_module = 'external'
    error_line = 0
    # Walk to find innermost in-app frame
    in_app_frame = None
    frame = exc_tb
    while frame is not None:
        basename = os.path.basename(frame.tb_frame.f_code.co_filename)
        if basename in _IN_APP_ROOTS:
            in_app_frame = frame  # keep walking; want INNERMOST
        frame = frame.tb_next
    # Fallback: deepest frame if no in-app frame found
    if in_app_frame is None and exc_tb is not None:
        frame = exc_tb
        while frame.tb_next:
            frame = frame.tb_next
        in_app_frame = frame
    if in_app_frame is not None:
        error_module = os.path.basename(in_app_frame.tb_frame.f_code.co_filename)
        error_line = in_app_frame.tb_lineno
    error_type = exc_type.__name__ if exc_type else 'UnknownException'
    fingerprint = f'{error_type}:{error_module}:{error_line}'
    return {
        'exc_type': error_type,
        'exc_module': error_module,
        'exc_lineno': error_line,
        'error_fingerprint': fingerprint,
        'is_background_thread': is_background,
    }
```

---

### `desktop/telemetry.py` — `_emit_crash_direct()` (new function)

**Analog:** `_emit()` (lines 495-527) for the "merge base props → validate → scrub → deliver" pipeline shape, but without `_state_lock` and without `enqueue_event`.

**`_emit()` pipeline shape** (lines 495-527 — the template):
```python
def _emit(event_value: str, props: dict, distinct_id: str | None = None) -> None:
    try:
        merged = dict(_BASE_PROPS())
        with _state_lock:                         # <-- crash path MUST bypass this lock
            identified = _identified
            effective_id = distinct_id or _current_distinct_id or 'system'
        merged['$process_person_profile'] = identified
        merged.update(props)
        merged['$process_person_profile'] = identified
        validated = _validate_props(merged)
        scrubbed = _scrub_props(validated)
        ...
        enqueue_event(event_value, scrubbed, distinct_id=effective_id)  # <-- crash path bypasses
    except Exception:
        logger.debug('telemetry: _emit silently failed for %r', event_value, exc_info=True)
```

**Pattern for `_emit_crash_direct()`** — same pipeline, all locks stripped (D-05):
```python
def _emit_crash_direct(
    exc_type: type,
    exc_tb,            # TracebackType | None
    is_background: bool,
) -> None:
    """Lock-free crash emission (D-05, SC#4). Call ONLY from sys/threading excepthook.

    NOTE: _BASE_PROPS() reads only module-level constants (verified: 'platform' +
    _APP_VERSION). No _state_lock acquired. Confirmed at telemetry.py line 307-309.
    """
    global _in_crash_hook
    if _in_crash_hook:
        return  # recursion guard (D-05)
    _in_crash_hook = True
    try:
        if not _is_enabled_nolock():
            return
        distinct_id = _crash_distinct_id or 'system'
        props = _make_crash_props(exc_type, exc_tb, is_background)
        merged = dict(_BASE_PROPS())          # no lock — reads only constants
        merged.update(props)
        validated = _validate_props(merged)
        scrubbed = _scrub_props(validated)
        from shared.posthog_server import send_crash_event_direct  # lazy — avoids circular at module level
        send_crash_event_direct(DesktopEvent.CRASH.value, scrubbed, distinct_id, timeout=0.5)
    except Exception:
        pass  # hook body MUST never raise (SC#4)
    finally:
        _in_crash_hook = False
```

**Verification note:** `_BASE_PROPS()` at line 307-309 reads only `_APP_VERSION` (a module-level string constant) and the literal `'desktop'` — it does NOT acquire `_state_lock`. This was confirmed before writing this pattern. If a future change adds identity state reads to `_BASE_PROPS()`, a lock-free `_crash_base_props()` alternative must be created.

---

### `desktop/telemetry.py` — faulthandler helpers (new functions)

**Analog:** `_load_consent_state()` (lines 342-370) for the "read → classify → update module global → handle failure silently" startup pattern; `set_consent()` (lines 399-480) for the "update global, then possibly trigger deferred action" pattern.

**`_load_consent_state()` shape** (lines 342-370 — the startup-read-then-cache pattern):
```python
def _load_consent_state() -> None:
    """Populate module-level cache from config.pkl. Called at import time.
    Never raises — all exceptions are swallowed per CRASH-05 contract.
    """
    global _enabled, _install_id, _current_distinct_id, _identified
    try:
        cfg = load_app_config()
        enabled = bool(cfg.get(TELEMETRY_ENABLED_KEY, False))
        with _enabled_lock:
            _enabled = enabled
        with _state_lock:
            ...
    except Exception:
        logger.debug('telemetry: _load_consent_state silently failed', exc_info=True)
```

**Pattern for `_setup_faulthandler()`** — read-before-enable ordering is the invariant (D-03):
```python
def _setup_faulthandler() -> None:
    """Read + classify previous dump, enable faulthandler for this run. D-03.

    ORDERING: read BEFORE faulthandler.enable() (opening for write erases evidence).
    """
    global _faulthandler_handle, _pending_native_crash
    try:
        import faulthandler
        from genizah_core import Config  # lazy — avoids circular at module level
        dump_path = os.path.join(Config.INDEX_DIR, 'faulthandler_dump.txt')
        # Step 1: read BEFORE enable()
        prior_dump_text = ''
        if os.path.exists(dump_path):
            try:
                with open(dump_path, 'r', encoding='utf-8', errors='replace') as f:
                    prior_dump_text = f.read().strip()
            except OSError:
                pass
        # Step 2: classify + emit or hold pending
        if prior_dump_text:
            label = _classify_native_crash(prior_dump_text)
            if _is_enabled_nolock():
                _emit_native_crash(label)
                try:
                    open(dump_path, 'w').close()  # truncate after emit
                except OSError:
                    pass
            else:
                _pending_native_crash = label  # hold; emit on consent
        # Step 3: open for THIS run (creates/truncates the file for faulthandler)
        _faulthandler_handle = open(dump_path, 'w', encoding='utf-8')
        faulthandler.enable(file=_faulthandler_handle, all_threads=True)
    except Exception:
        pass  # faulthandler is best-effort; failure must never block startup
```

**Fixed enum mapping** (D-02 — the only values that may ever be transmitted):
```python
_NATIVE_CRASH_LABELS: dict[str, str] = {
    'windows fatal exception: access violation': 'access_violation',
    'windows fatal exception: stack overflow':   'stack_overflow',
    'windows fatal exception: int divide by zero': 'abort',
    'windows fatal exception: float divide by zero': 'abort',
    'segmentation fault':                        'segmentation_fault',
    'aborted':                                   'abort',
    'floating-point exception':                  'abort',
    'bus error':                                 'abort',
    'fatal python error:':                       'unknown_native',
}
# Anything unrecognized -> 'unknown_native'. Raw text NEVER transmitted.
```

**Pattern for `_classify_native_crash()`:**
```python
def _classify_native_crash(text: str) -> str:
    """Map faulthandler dump first-line to a fixed enum label. Never returns raw text."""
    if not text:
        return 'unknown_native'
    first_line = text.splitlines()[0].lower().strip()
    for prefix, label in _NATIVE_CRASH_LABELS.items():
        if first_line.startswith(prefix):
            return label
    return 'unknown_native'
```

**Pattern for `_emit_pending_native_crash()`** — called from `set_consent(True)` path (D-03):
```python
def _emit_pending_native_crash() -> None:
    """Emit held pending native crash exactly once when consent becomes True.

    Called from set_consent(True) AFTER _enabled is flipped True and
    _crash_distinct_id is populated. The 'exactly once' guarantee:
    clear _pending_native_crash BEFORE emitting, not after.
    """
    global _pending_native_crash
    label = _pending_native_crash
    if label is None:
        return
    _pending_native_crash = None  # clear BEFORE emit to ensure exactly-once
    _emit_native_crash(label)
```

---

### `desktop/telemetry.py` — `install_exception_hooks()` body (replace stub at line 704)

**Analog:** `_setup_crash_handler()` in `genizah_app.py` (lines 148-170) for the chained-hook closure pattern; `set_consent()` (lines 399-480) for the module-global mutation + try/except shape.

**`_setup_crash_handler()` chain pattern** (lines 148-170 — the pattern that Phase 113 wraps):
```python
def _setup_crash_handler():
    import traceback
    from datetime import datetime

    def exception_hook(exc_type, exc_value, exc_tb):
        try:
            crash_log = os.path.join(os.path.dirname(__file__), 'crash_log.txt')
            with open(crash_log, 'a', encoding='utf-8') as f:
                ...
        except OSError:
            pass
        traceback.print_exception(exc_type, exc_value, exc_tb)
        sys.__excepthook__(exc_type, exc_value, exc_tb)   # <-- always chain

    sys.excepthook = exception_hook

_setup_crash_handler()
```

**Pattern for `install_exception_hooks()` body** — idempotent, chaining, KeyboardInterrupt+SystemExit excluded (D-08):
```python
def install_exception_hooks() -> None:
    """Install crash-capture exception hooks + faulthandler. Idempotent. Never raises."""
    global _hooks_installed
    try:
        if _hooks_installed:
            return  # idempotency guard (D-08)
        _hooks_installed = True

        # 1. Wrap sys.excepthook: telemetry -> crash_log.txt -> sys.__excepthook__
        #    _prior_hook captured AFTER _setup_crash_handler() runs, so it IS the
        #    crash-log writer (genizah_app.py:168). Chain to it, not to sys.__excepthook__
        #    directly — that is what crash_log.txt currently does.
        _prior_hook = sys.excepthook

        def _telemetry_excepthook(exc_type, exc_value, exc_tb):
            try:
                if exc_type is not KeyboardInterrupt and exc_type is not SystemExit:
                    _emit_crash_direct(exc_type, exc_tb, is_background=False)
            except Exception:
                pass
            _prior_hook(exc_type, exc_value, exc_tb)  # always chain (SC#1 try/finally shape)

        sys.excepthook = _telemetry_excepthook

        # 2. threading.excepthook — Python 3.10+ __excepthook__ is the true default
        import threading as _threading
        _prior_thread_hook = getattr(_threading, '__excepthook__', _threading.excepthook)

        def _telemetry_threading_hook(args):
            try:
                if args.exc_type is not KeyboardInterrupt and args.exc_type is not SystemExit:
                    _emit_crash_direct(args.exc_type, args.exc_traceback, is_background=True)
            except Exception:
                pass
            _prior_thread_hook(args)

        _threading.excepthook = _telemetry_threading_hook

        # 3. faulthandler + next-launch native crash detection (D-02/D-03)
        _setup_faulthandler()

        # 4. atexit flush for clean exits — registered HERE, NOT in posthog_server (D-08)
        import atexit

        def _atexit_flush():
            try:
                from shared.posthog_server import _flush_before_exit
                _flush_before_exit(1.5)  # ~1-2s clean-exit budget (crash path stays 0.5s)
            except Exception:
                pass

        atexit.register(_atexit_flush)

    except Exception:
        logger.debug('telemetry: install_exception_hooks failed', exc_info=True)
```

---

### `desktop/telemetry.py` — `set_consent()` additions for pending-emit wiring

**Analog:** itself, lines 399-480 (the opt-in branch). The additions go in the opt-in branch after `_enabled` is set True and `_crash_distinct_id` is populated.

**Existing opt-in branch hook point** (lines 441-445):
```python
            with _enabled_lock:
                _enabled = True
            with _state_lock:
                distinct_id = _current_distinct_id or _install_id
            set_default_distinct_id(distinct_id)
            # <-- ADD AFTER THIS POINT:
            #     _crash_distinct_id = distinct_id   (no lock — GIL-safe write)
            #     _emit_pending_native_crash()
```

**Pattern for `_crash_distinct_id` snapshot write:**  
Write `_crash_distinct_id` as a plain module-global assignment at every point where `_current_distinct_id` changes (set_consent opt-in, `_set_current_distinct_id()`). No lock needed — plain string assignment is GIL-atomic in CPython.

---

### `desktop/telemetry.py` — `_reset_for_tests()` extension

**Analog:** itself, lines 738-750. Add Phase 113 globals to the existing reset function.

**Existing reset function** (lines 738-750):
```python
def _reset_for_tests() -> None:
    global _enabled, _install_id, _current_distinct_id, _identified
    with _enabled_lock:
        _enabled = False
    with _state_lock:
        _install_id = None
        _current_distinct_id = None
        _identified = False
```

**Extension — add Phase 113 globals** (no locks — plain bool/str):
```python
    global _crash_distinct_id, _in_crash_hook, _hooks_installed, _pending_native_crash
    _crash_distinct_id = None
    _in_crash_hook = False
    _hooks_installed = False
    _pending_native_crash = None
    # NOTE: do NOT reset _faulthandler_handle — tests should not close a real file handle
```

---

### `shared/posthog_server.py` — `send_crash_event_direct()` (new helper)

**Analog:** `_flush_before_exit()` (lines 272-311) for the "direct `requests.post`, no queue, deadline-bounded, never raises" shape; `_drain_posthog_queue()` (lines 216-241) for the per-event POST payload construction.

**`_flush_before_exit()` shape** (lines 272-311 — the existing direct-POST pattern):
```python
def _flush_before_exit(timeout: float = 0.5) -> None:
    deadline = time.monotonic() + timeout
    api_key = _resolve_api_key()
    url = _resolve_capture_url()
    while True:
        try:
            event = _event_queue.get_nowait()
        except queue.Empty:
            break
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            continue
        if not api_key:
            continue
        try:
            payload = {
                'api_key': api_key,
                'event': event['event'],
                'distinct_id': event['distinct_id'],
                'properties': event['properties'],
                'timestamp': event['timestamp'],
            }
            requests.post(url, json=payload, timeout=min(remaining, 2.0))
        except Exception:
            pass
```

**Pattern for `send_crash_event_direct()`** — single POST, no queue, no locks (D-06):
```python
def send_crash_event_direct(
    event: str,
    properties: dict,
    distinct_id: str,
    timeout: float = 0.5,
) -> None:
    """Synchronous, priority POST for crash events (D-06).

    Bypasses _event_queue entirely — crash events are never subject to FIFO
    ordering or daemon-thread drain timing. The 5 existing _event_queue
    monkeypatches are completely unaffected (NEUTRAL addition per Phase 111 D-04).

    Does NOT touch: _event_queue, _default_distinct_id_lock, _scrub_hook_lock,
    drain thread. Web callers do not call this function.
    Never raises.
    """
    try:
        api_key = _resolve_api_key()   # acquires _capture_config_lock briefly — acceptable
        if not api_key:
            return
        url = _resolve_capture_url()   # acquires _capture_config_lock briefly — acceptable
        payload = {
            'api_key': api_key,
            'event': event,
            'distinct_id': distinct_id,
            'properties': dict(properties) if properties else {},
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }
        requests.post(url, json=payload, timeout=timeout)
    except Exception:
        pass  # fire-and-forget in crash context
```

**`__all__` extension** — add to the existing list (currently ends at `_drain_and_discard`):
```python
    # Phase 113 addition:
    'send_crash_event_direct',
```

---

### `genizah_app.py` — wiring `install_exception_hooks()` after `_setup_crash_handler()`

**Analog:** itself, lines 148-170 (`_setup_crash_handler()` call site + module-level call pattern).

**Existing call site** (line 170 — the module-level call):
```python
_setup_crash_handler()
# <-- ADD IMMEDIATELY AFTER:
```

**Pattern** (D-08 — best-effort, never blocks startup):
```python
# Phase 113: install telemetry crash hooks (chained AFTER _setup_crash_handler so
# _prior_hook captures the crash-log writer, not the bare sys.__excepthook__).
try:
    from desktop import telemetry as _telemetry
    _telemetry.install_exception_hooks()
except Exception:
    pass  # crash hooks are best-effort; never block app startup
```

**Pending-emit trigger wiring** (D-03 — in `set_consent(True)` path called from `_maybe_show_first_run_prompt()` at line 15856):  
The consent dialog's `done()` finalizer calls `telemetry.set_consent(True)`. The `_emit_pending_native_crash()` call is placed INSIDE `set_consent()` itself (in `desktop/telemetry.py`) at the end of the opt-in branch, after `_crash_distinct_id` is populated. No additional wiring in `genizah_app.py` is needed for the pending-emit path.

---

## Shared Patterns

### Never-Raise / Fire-and-Forget
**Source:** `shared/posthog_server.py` lines 212-213; `desktop/telemetry.py` lines 526-527  
**Apply to:** `_emit_crash_direct()`, `_setup_faulthandler()`, `install_exception_hooks()`, `send_crash_event_direct()`, `_emit_native_crash()`, `_emit_pending_native_crash()`

```python
    except Exception:
        pass  # or logger.debug(... exc_info=True) for non-hook-body functions
```

The crash hook body MUST use bare `pass` (not `logger.debug`) for the inner-most exception guard — `logger.debug` could itself raise if the logging system is broken during process teardown.

### Module-Global State Reset for Tests
**Source:** `desktop/telemetry.py` lines 738-750; `shared/posthog_server.py` lines 314-338  
**Apply to:** all four new test files — autouse fixture calls `tel._reset_for_tests()` + `ph._reset_for_tests()` + `monkeypatch.setattr(ph, '_event_queue', fresh_q)`

```python
@pytest.fixture(autouse=True)
def _reset_telemetry_state(monkeypatch):
    fake_config: dict = {}
    def fake_load_app_config(): return dict(fake_config)
    def fake_save_app_config(new_data: dict): fake_config.update(new_data)

    import genizah_core
    monkeypatch.setattr(genizah_core, 'load_app_config', fake_load_app_config)
    monkeypatch.setattr(genizah_core, 'save_app_config', fake_save_app_config)

    import desktop.telemetry as tel
    monkeypatch.setattr(tel, 'load_app_config', fake_load_app_config)
    monkeypatch.setattr(tel, 'save_app_config', fake_save_app_config)

    ph._reset_for_tests()
    fresh_q: queue.Queue = queue.Queue(maxsize=10000)
    monkeypatch.setattr(ph, '_event_queue', fresh_q)

    tel._reset_for_tests()
    tel._load_consent_state()

    yield fake_config

    tel._reset_for_tests()
    ph._reset_for_tests()
```

### Queue Monkeypatch for Direct-Send Testing
**Source:** `tests/test_telemetry_posthog_server_ext.py` (autouse fixture, lines 24-55)  
**Apply to:** `tests/test_crash_priority_send.py`

To prove `send_crash_event_direct` does NOT touch `_event_queue`, fill the queue to capacity THEN call `send_crash_event_direct` and assert the queue is still full AND a POST was made:
```python
tiny_q = queue.Queue(maxsize=5)
for i in range(5): tiny_q.put_nowait({...})
monkeypatch.setattr(ph, '_event_queue', tiny_q)
posted = []
monkeypatch.setattr(requests, 'post', lambda url, json=None, timeout=None: posted.append(json))
monkeypatch.setattr(ph, '_api_key_override', 'test_key')
ph.send_crash_event_direct('desktop_crash', {'exc_type': 'ValueError'}, 'did')
assert len(posted) == 1
assert tiny_q.full()   # queue untouched
```

### PRIV-03 AST Guard Compatibility
**Source:** `tests/test_telemetry_no_direct_posthog.py` — the existing guard (read-only reference, must stay green)  
**Apply to:** All new crash code in `desktop/telemetry.py`

The PRIV-03 guard scans `desktop/` for `import shared.posthog_server` or `enqueue_event` calls in any file EXCEPT `desktop/telemetry.py`. The new `send_crash_event_direct` is in `shared/posthog_server.py` (transport layer) — NOT a posthog_server import in another `desktop/` file. The lazy `from shared.posthog_server import send_crash_event_direct` inside `_emit_crash_direct()` lives in `desktop/telemetry.py` (the exempted chokepoint), so PRIV-03 stays green without any guard modification.

---

## Pattern Assignments — Test Files

### `tests/test_crash_hooks.py` (new)

**Analog:** `tests/test_telemetry_consent_gate.py` (autouse fixture shape) + `tests/test_telemetry_posthog_server_ext.py` (monkeypatch + call-capture pattern).

**Core shape** — monkeypatch send path, invoke hook, assert chain:
```python
def test_prior_hook_chained(monkeypatch):
    """CRASH-01: crash_log.txt writer still called after install_exception_hooks()."""
    prior_called = []
    monkeypatch.setattr(sys, 'excepthook', lambda t, v, tb: prior_called.append(t))
    tel.install_exception_hooks()
    # fire the hook
    sys.excepthook(ValueError, ValueError('x'), None)
    assert ValueError in prior_called

def test_hook_acquires_no_locks(monkeypatch):
    """D-05 BLOCKER: crash hook acquires no locks."""
    class _FailLock:
        def acquire(self, *a, **kw): raise AssertionError("lock acquired in crash hook")
        def __enter__(self): self.acquire()
        def __exit__(self, *a): pass
    monkeypatch.setattr(tel, '_enabled_lock', _FailLock())
    monkeypatch.setattr(tel, '_state_lock', _FailLock())
    monkeypatch.setattr(tel, '_enabled', True)
    monkeypatch.setattr(tel, '_crash_distinct_id', 'test-uuid')
    sent = []
    monkeypatch.setattr(ph, 'send_crash_event_direct',
                        lambda ev, props, did, timeout=0.5: sent.append(ev))
    tel._emit_crash_direct(ValueError, None, is_background=False)  # must not raise
    assert 'desktop_crash' in sent
```

**KeyboardInterrupt/SystemExit exclusion shape:**
```python
def test_keyboard_interrupt_excluded(monkeypatch):
    sent = []
    monkeypatch.setattr(ph, 'send_crash_event_direct', lambda *a, **kw: sent.append(a))
    monkeypatch.setattr(tel, '_enabled', True)
    tel.install_exception_hooks()
    sys.excepthook(KeyboardInterrupt, KeyboardInterrupt(), None)
    assert len(sent) == 0

def test_system_exit_excluded(monkeypatch):
    sent = []
    monkeypatch.setattr(ph, 'send_crash_event_direct', lambda *a, **kw: sent.append(a))
    monkeypatch.setattr(tel, '_enabled', True)
    tel.install_exception_hooks()
    sys.excepthook(SystemExit, SystemExit(0), None)
    assert len(sent) == 0
```

---

### `tests/test_crash_payload.py` (new)

**Analog:** `tests/test_telemetry_scrubbing.py` (pure-function assertion on `_scrub_props`) + `tests/test_telemetry_allowlist.py` (allowlist key iteration).

**Core shape** — direct pure-function calls, no network:
```python
def test_payload_keys_allowlisted():
    """CRASH-04: all crash prop keys are in _ALLOWED_PROPS."""
    from desktop.telemetry import _make_crash_props, _ALLOWED_PROPS
    props = _make_crash_props(ValueError, None, is_background=False)
    for key in props:
        assert key in _ALLOWED_PROPS, f"Key {key!r} not in _ALLOWED_PROPS"

def test_no_forbidden_keys_in_payload():
    """CRASH-04: 'traceback_scrubbed' and 'thread_name' never appear."""
    from desktop.telemetry import _make_crash_props
    props = _make_crash_props(ValueError, None, is_background=False)
    assert 'traceback_scrubbed' not in props
    assert 'thread_name' not in props
    assert 'message' not in props

def test_external_module_fallback():
    """CRASH-04 D-07: stdlib frame -> error_module='external'."""
    # Pass a traceback whose co_filename is not in _IN_APP_ROOTS
    # (use inspect to get a stdlib frame, or create a synthetic traceback)
    ...
```

---

### `tests/test_native_crash.py` (new)

**Analog:** `tests/test_telemetry_consent_gate.py` (autouse fixture + consent state manipulation).

**Core shape** — file I/O mocked via tmp_path, consent toggled via monkeypatch:
```python
def test_classify_all_prefixes():
    """CRASH-07 D-02: all _NATIVE_CRASH_LABELS prefixes map to known labels."""
    from desktop.telemetry import _classify_native_crash
    assert _classify_native_crash('Windows fatal exception: access violation') == 'access_violation'
    assert _classify_native_crash('Windows fatal exception: stack overflow') == 'stack_overflow'
    assert _classify_native_crash('') == 'unknown_native'

def test_pending_emit_after_consent(monkeypatch, tmp_path):
    """CRASH-07 D-03: prior crash held pending, emitted once when consent becomes True."""
    import desktop.telemetry as tel
    sent = []
    monkeypatch.setattr(ph, 'send_crash_event_direct', lambda *a, **kw: sent.append(a[0]))
    # Simulate: prior dump exists, consent is False
    dump = tmp_path / 'faulthandler_dump.txt'
    dump.write_text('Windows fatal exception: access violation\n...')
    # monkeypatch Config.INDEX_DIR to tmp_path
    ...
    # After _setup_faulthandler: label is pending, not sent
    assert len(sent) == 0
    assert tel._pending_native_crash == 'access_violation'
    # Trigger consent
    tel.set_consent(True)
    assert len(sent) == 1
    assert tel._pending_native_crash is None
```

---

### `tests/test_crash_priority_send.py` (new)

**Analog:** `tests/test_telemetry_posthog_server_ext.py` (queue assertions + `requests.post` capture via monkeypatch).

**Core shape** — saturated queue + direct send, assert queue untouched:
```python
def test_crash_send_bypasses_full_queue(monkeypatch):
    """CRASH-06 D-06: send_crash_event_direct bypasses the FIFO queue."""
    tiny_q = queue.Queue(maxsize=5)
    for i in range(5):
        tiny_q.put_nowait({'event': f'dummy_{i}', 'distinct_id': 'x',
                           'properties': {}, 'timestamp': ''})
    monkeypatch.setattr(ph, '_event_queue', tiny_q)
    posted = []
    import requests
    monkeypatch.setattr(requests, 'post',
                        lambda url, json=None, timeout=None: posted.append(json))
    monkeypatch.setattr(ph, '_api_key_override', 'test_key')
    ph.send_crash_event_direct('desktop_crash', {'exc_type': 'ValueError'}, 'did')
    assert len(posted) == 1
    assert posted[0]['event'] == 'desktop_crash'
    assert tiny_q.full()  # queue untouched
```

---

## No Analog Found

All files have close analogs in the Phase 111 foundation. No entry needed.

---

## Metadata

**Analog search scope:** `desktop/telemetry.py`, `shared/posthog_server.py`, `genizah_app.py`, `tests/test_telemetry_*.py`, `.planning/phases/111-telemetry-foundation/111-PATTERNS.md`
**Files read:** 11 source files + Phase 111 PATTERNS.md
**Pattern extraction date:** 2026-06-15

**Critical ordering constraints preserved:**
1. `_setup_crash_handler()` runs at module level (line 170 of `genizah_app.py`) — `install_exception_hooks()` is called immediately after so `_prior_hook` captures the crash-log writer.
2. `faulthandler` dump is READ before `faulthandler.enable()` is called — opening for write first would erase the previous-run evidence (D-03).
3. `_crash_distinct_id` is populated by `set_consent(True)` BEFORE `_emit_pending_native_crash()` is called — crash events need a valid distinct_id.
4. `atexit` is registered INSIDE `install_exception_hooks()` in `desktop/telemetry.py`, NOT in `shared/posthog_server.py` — the web process imports `posthog_server` and must not trigger a desktop exit-flush on web server restart.
5. The 5 existing `_event_queue` monkeypatches remain valid — `send_crash_event_direct` never touches `_event_queue`.
6. `_BASE_PROPS()` (line 307-309) reads only `_APP_VERSION` (module-level constant) and the literal `'desktop'` — confirmed lock-free; no `_state_lock` acquired. If this changes, `_emit_crash_direct()` needs a separate `_crash_base_props()` that reads only constants.
