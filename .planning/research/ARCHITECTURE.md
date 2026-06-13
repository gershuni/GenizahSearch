# Architecture Research

**Domain:** Desktop telemetry integration (opt-in, privacy-preserving) into an existing PyQt6 app
**Researched:** 2026-06-13
**Confidence:** HIGH — all integration points verified against real source files

---

## System Overview

```
genizah_app.py (GenizahGUI, SettingsDialog, tab widgets)
    |
    |  <- consent gate (is_enabled check) --------------------------------+
    |                                                                     |
    v                                                                     |
desktop/telemetry.py  [NEW] <- single public chokepoint                 |
    |  track(event, **props)                                             |
    |  track_performance(...)                                            |
    |  track_error(context, exc)                                         |
    |  is_enabled() -> bool                                              |
    |  set_consent(bool)                                                 |
    |  get_install_id() -> str | None                                    |
    |  install_exception_hooks()                                         |
    |  show_first_run_prompt(parent)                                     |
    |  _scrub_props(props) -> props  [internal only]                    |
    |                                                                     |
    +-------> shared/posthog_server.py  [EXISTING]                      |
                  enqueue_event(event, properties, distinct_id)          |
                  fire-and-forget queue, daemon thread, EU endpoint      |
                  Queue(maxsize=10000) [verified line 47]                |
                      |                                                   |
                      v                                                   |
                PostHog EU (https://eu.i.posthog.com/capture)           |
                project "Default project" (id: 134161), org "Dicta"    |
                                                                         |
genizah_core.load_app_config() / save_app_config()  [EXISTING] --------+
    config.pkl in %LOCALAPPDATA%\GenizahSearchPro\ [verified line 2377]
    telemetry_enabled | telemetry_first_run_shown | telemetry_install_id
```

### Component Responsibilities

| Component | Responsibility | File |
|-----------|----------------|------|
| `desktop/telemetry.py` | Single consent gate, scrubbing, install-ID, all public API | NEW |
| `shared/posthog_server.py` | Thread-safe fire-and-forget queue + drain daemon | EXISTING |
| `genizah_core.Config` | Canonical path: `Config.CONFIG_FILE` = `%LOCALAPPDATA%\GenizahSearchPro\config.pkl` | EXISTING |
| `genizah_core.load_app_config / save_app_config` | Key-value pickle config store | EXISTING |
| `_setup_crash_handler()` in `genizah_app.py` | Installs `sys.excepthook`; chains to `sys.__excepthook__` [lines 148-170] | EXISTING — extend, not replace |
| `SettingsDialog._build_general_tab()` | UI for consent toggle (General tab) [line 2210] | EXISTING — add telemetry row |
| `GenizahGUI.__init__` | App startup; first-run dialog trigger | EXISTING — add one call |
| `SearchThread.run()` in `gui_threads.py` | Wraps `execute_search`; start/end timestamps [lines 96-121] | EXISTING — add perf timing |

---

## 1. Consent Chokepoint Design

### Module placement: `desktop/telemetry.py` (NOT `shared/`)

The `shared/` contract (verified against `shared/joins_lab.py` Phase 106 AST guard and the `shared/posthog_server.py` docstring) requires zero PyQt imports and web-reusability. The first-run consent dialog is a `QDialog` — it cannot live in `shared/`. Consent-state reads/writes call `genizah_core.load_app_config` / `save_app_config`, which are desktop-first. Placing the chokepoint in `desktop/` preserves the `shared/` boundary while still delegating network I/O to `shared/posthog_server.enqueue_event`.

### Public API

```python
# desktop/telemetry.py

def is_enabled() -> bool:
    """Return True iff user has opted in AND an install_id exists."""

def track(event: str, **props) -> None:
    """Gate-check consent, scrub props, enqueue. Never raises. Non-blocking."""

def track_performance(
    event: str,
    duration_ms: float,
    result_count: int | None = None,
    **extra_props,
) -> None:
    """Specialised track for timed operations. Applies sampling before enqueue."""

def track_error(context: str, exc: Exception) -> None:
    """Capture a handled (non-fatal) exception with context label.
    context = 'pdf_extract' | 'nli_fetch' | 'local_index' | etc.
    Never raises.
    """

def get_install_id() -> str | None:
    """Return the persisted anonymous UUID, or None if not opted in."""

def set_consent(enabled: bool) -> None:
    """Persist consent flag. On True: mint install_id if absent.
    On False: clear install_id from config.pkl."""

def install_exception_hooks() -> None:
    """Wrap existing sys.excepthook + install threading.excepthook.
    Must be called AFTER _setup_crash_handler() so the chain is correct."""

def show_first_run_prompt(parent_widget) -> None:
    """Display the bilingual first-run consent dialog if not yet shown.
    Marks telemetry_first_run_shown=True regardless of user's answer."""
```

All eight public callables are the ONLY sanctioned way to emit desktop telemetry. The scrubbing step (`_scrub_props`) is internal — callers cannot bypass it by reaching `shared/posthog_server` directly.

---

## 2. Anonymous Install ID

### Storage location

`genizah_core.load_app_config()` / `save_app_config()` reads and writes `Config.CONFIG_FILE` — which resolves to `%LOCALAPPDATA%\GenizahSearchPro\config.pkl` on a standard install (verified: `genizah_core.py` lines 2344-2378). This file already stores all other persistent preferences (variant settings, language choice, lab config path). It survives crashes and version upgrades.

**No new file. No QSettings. No session.json.** The session store (`session.json` at `Config.SESSION_FILE`) is session-scoped and may be cleared on crash recovery — wrong store for permanent preferences.

### Keys in config.pkl

| Key | Type | Semantics |
|-----|------|-----------|
| `telemetry_enabled` | `bool` | User's opt-in choice; absent = not yet asked |
| `telemetry_first_run_shown` | `bool` | Whether the first-run dialog has fired; absent = not yet |
| `telemetry_install_id` | `str` (uuid4 hex) | Anonymous per-install identifier; absent until first opt-in |

### Lifecycle

- **First opt-in:** `uuid.uuid4().hex` minted inside `set_consent(True)` and written as `telemetry_install_id`.
- **Subsequent calls:** `get_install_id()` reads from config.pkl; stable across restarts and upgrades.
- **Opt-out:** `set_consent(False)` writes `telemetry_enabled=False` and **deletes** `telemetry_install_id`. Subsequent `track()` calls gate-check `is_enabled()` (reads `config.pkl`) and return immediately.
- **distinct_id in PostHog:** the `telemetry_install_id` hex; never the user's Supabase UUID or email.
- **ID is NOT minted at import time.** It does not exist until the user actively opts in.

---

## 3. Consent State Storage

Both `telemetry_enabled` and `telemetry_first_run_shown` live in `config.pkl` — the single source of truth. No duplication in session.json, lang.pkl, or any other file.

### First-run dialog trigger

`GenizahGUI.__init__` (or its post-show hook, to avoid dialog-before-window) calls `desktop.telemetry.show_first_run_prompt(self)` once per app lifetime. The function:

1. Reads `telemetry_first_run_shown` from `load_app_config()`.
2. If falsy: shows a `QDialog` (bilingual EN/HE, default OFF, two buttons: opt in / decline). Privacy disclosure is inline — no external link required, consistent with the existing posture.
3. Calls `set_consent(user_chose_yes)`.
4. Writes `telemetry_first_run_shown = True` via `save_app_config` unconditionally. The dialog never fires again even if the user declined.

### Settings toggle (existing `SettingsDialog`)

`SettingsDialog._build_general_tab()` (verified: `genizah_app.py` line 2210, within class at line 2145) already snapshots `load_app_config()` on open and writes on OK/Cancel via `save_app_config`. A new checkbox row:
- **On open:** `chk_telemetry.setChecked(cfg.get('telemetry_enabled', False))`
- **On OK:** calls `desktop.telemetry.set_consent(chk_telemetry.isChecked())`
- **On Cancel:** the existing `_on_cancel` snapshot-restore reverts it automatically (no special handling needed)

---

## 4. Global Exception Handling

### Existing handler (verified)

`genizah_app.py` lines 148-170 define `_setup_crash_handler()`, which:
- Writes a full traceback to `crash_log.txt` adjacent to `genizah_app.py`.
- Prints to stderr via `traceback.print_exception`.
- Chains to `sys.__excepthook__` explicitly.
- Is **installed at module-import time** (line 170), before `GenizahGUI` is constructed.

`threading.excepthook` is **not** currently set (confirmed by grep: zero hits). The existing hook only covers the main thread.

### Extension strategy: wrap, do not replace

```python
# desktop/telemetry.py

def install_exception_hooks() -> None:
    """Wrap existing sys.excepthook and install threading.excepthook.

    Call from genizah_app.py AFTER _setup_crash_handler() (line ~171)
    so _prior_hook captures the crash-log writer.
    """
    _prior_hook = sys.excepthook  # captures genizah_app's crash-log handler

    def _telemetry_excepthook(exc_type, exc_value, exc_tb):
        # 1. Telemetry (non-blocking enqueue only)
        if is_enabled() and exc_type is not KeyboardInterrupt:
            _capture_crash(exc_type, exc_value, exc_tb)
        # 2. Chain: crash_log.txt + stderr + sys.__excepthook__
        _prior_hook(exc_type, exc_value, exc_tb)

    sys.excepthook = _telemetry_excepthook

    # threading.excepthook covers QThread.run() exceptions that escape
    # the per-thread try/except blocks (backstop for rare cases).
    # Note: most QThread workers already have try/except + error_signal --
    # this captures the ones that don't.
    _prior_thread_hook = threading.excepthook

    def _telemetry_threading_hook(args):
        if is_enabled() and args.exc_type is not KeyboardInterrupt:
            _capture_crash(args.exc_type, args.exc_value, args.exc_traceback)
        _prior_thread_hook(args)

    threading.excepthook = _telemetry_threading_hook
```

`install_exception_hooks()` is called in `genizah_app.py` immediately after `_setup_crash_handler()` (line ~172). The chain is then: `_telemetry_excepthook` → `_prior_hook` (crash_log.txt writer) → `sys.__excepthook__` (Python default).

**Qt-level handler:** PyQt6's `sys.excepthook` wrapping is sufficient for exceptions propagating out of Qt slots. A `QApplication.notify` override is unnecessary and fragile — skip it.

### Non-blocking invariant

`_capture_crash()` executes only:
1. `traceback.format_exception()` — pure Python string formatting.
2. `_scrub_props()` — pure Python dict manipulation.
3. `shared.posthog_server.enqueue_event()` — `put_nowait` onto an in-memory `queue.Queue`; returns immediately even on `queue.Full`.

No network calls, no disk I/O, no locks inside the hook body.

### Handled/non-fatal errors

`track_error(context, exc)` is the public API for existing `try/except` sites. It is added selectively to high-value sites (PDF extractor, NLI fetch, local indexer errors). Which specific sites get it is a requirements-phase decision, not an architecture decision. The key structural rule: every call goes through `desktop/telemetry.track_error()`, never directly to `enqueue_event`.

---

## 5. Scrubbing Layer

### Location and structural guarantee

`desktop/telemetry.py::_scrub_props(props: dict) -> dict` — called internally by every public function before any data reaches `enqueue_event`. It is not in `__all__` and not exported. The path to the network is always:

```
track() / track_performance() / track_error() / _capture_crash()
    -> _scrub_props(props)  [always]
    -> shared.posthog_server.enqueue_event(...)
```

There is no way to reach `enqueue_event` from desktop code without passing through `_scrub_props`. This is the structural invariant that makes the privacy guarantee hold even as new `track()` callsites are added in the future.

### Scrubbing rules

```python
import re as _re

# Windows path: C:\..., POSIX path: /home/..., bare filename: foo.pdf
_PATH_RE = _re.compile(
    r'[A-Za-z]:\\[^\s,\"\']+|/[^\s,\"\']{3,}|\S+\.\w{2,4}\b'
)

# Keys whose values are always dropped (even if not path-like)
_BANNED_KEYS = frozenset({
    'query', 'text', 'content', 'filename', 'path', 'filepath',
    'frame_locals', 'traceback_raw', 'search_term', 'query_text',
})

def _scrub_props(props: dict) -> dict:
    out = {}
    for k, v in props.items():
        if any(b in k.lower() for b in _BANNED_KEYS):
            continue
        if isinstance(v, str):
            v = _PATH_RE.sub('[REDACTED]', v)
            v = v[:500]
        out[k] = v
    return out
```

**Crash tracebacks specifically:** `_capture_crash()` uses `traceback.format_exception()`, then strips lines matching the frame-local pattern (`^\s+\w+ = ` — Python's "local variables" section in tracebacks printed by some formatters). The stripped, truncated string goes into props under key `traceback_scrubbed`. The key `traceback_raw` is in `_BANNED_KEYS` as defence-in-depth.

### AST guard (CI test)

`tests/test_telemetry_no_direct_posthog.py` — an AST-walk test that asserts no file under `desktop/` (except `desktop/telemetry.py` itself) imports `shared.posthog_server` or calls `enqueue_event` directly. This mirrors the `test_no_raw_storage_access.py` pattern from Phase 87 (`web/safe_storage.py` chokepoint).

---

## 6. Performance Sampling

### Integration point: SearchThread.run() in gui_threads.py

`SearchThread.run()` (verified: `gui_threads.py` lines 96-121) is the correct and only timing boundary for regular search. The pattern:

```python
# gui_threads.py — SearchThread.run() modification

perf_signal = pyqtSignal(float, int)  # new signal: elapsed_ms, result_count

def run(self):
    _prevent_sleep()
    _t0 = time.perf_counter()
    try:
        ...
        results = self.searcher.execute_search(...)
        _elapsed_ms = (time.perf_counter() - _t0) * 1000
        self.results_signal.emit(results)
        self.perf_signal.emit(_elapsed_ms, len(results))   # new
    except InterruptedError:
        self.results_signal.emit([])
    except Exception as e:
        self.error_signal.emit(str(e))
    finally:
        _allow_sleep()
```

The `perf_signal` connects in `GenizahGUI` to a handler that calls `desktop.telemetry.track_performance(...)`. The call is made on the **UI thread** (not the worker thread) via Qt signal delivery — consistent with all other result-handling callbacks in this codebase.

**Same pattern for `CompositionThread` and `LabSearchThread`** (both in `gui_threads.py`) — add `perf_signal = pyqtSignal(float, int)` and identical timing wrapper in their `run()` methods.

### Sampling within track_performance

Heavy users run ~50 searches/day (stated requirement). At 30 such users that is ~1,500 performance events/day — manageable without sampling at PostHog's scale. However, `track_performance` applies a lightweight 1-in-N guard so future increases in user count do not require code changes:

```python
_PERF_SAMPLE_N = int(os.environ.get('TELEMETRY_PERF_SAMPLE_N', '3'))
_perf_counter = 0  # module-level, GIL-safe

def track_performance(event, duration_ms, result_count=None, **extra):
    if not is_enabled():
        return
    global _perf_counter
    _perf_counter = (_perf_counter + 1) % _PERF_SAMPLE_N
    if _perf_counter != 0:
        return
    props = {
        'duration_ms': round(duration_ms, 1),
        'result_count': result_count,
        'search_mode': extra.get('search_mode'),  # 'keyword'|'responsa'|'composition'|'parallels'
        'corpus_scope': extra.get('corpus_scope'), # 'genizah'|'local'|'all'
    }
    _emit(event, props)  # internal: _scrub_props -> enqueue_event
```

`_emit` is the private helper that calls `_scrub_props` then `enqueue_event` with the install ID as `distinct_id`.

---

## Data Flow Summary

### Normal usage event

```
User action (tab switch, search mode, key dialog)
    |
    v  desktop.telemetry.track('tab_switched', tab='Browse by Shelfmark')
desktop/telemetry.py::track()
    |  1. is_enabled() -> False? return immediately
    |  2. _scrub_props({'tab': 'Browse by Shelfmark'}) -> unchanged
    |  3. _emit(): add app_version, os_version (module-level constants)
    v
shared/posthog_server.enqueue_event('tab_switched', props, install_id)
    |  put_nowait -> Queue(maxsize=10000)
    v
posthog-shared-drain daemon -> POST eu.i.posthog.com/capture
```

### Crash capture

```
Unhandled exception (any thread)
    |
    v
sys.excepthook / threading.excepthook (telemetry wrapper)
    |  1. is_enabled()? yes -> _capture_crash()
    |     a. format + strip frame-locals + truncate to 2000 chars
    |     b. _scrub_props() -> paths redacted, banned keys dropped
    |     c. enqueue_event('app_crash', scrubbed_props, install_id)
    |     <- RETURNS IMMEDIATELY (put_nowait, never blocks)
    |  2. _prior_hook (crash_log.txt + stderr + sys.__excepthook__)
    v
sys.__excepthook__ (Python default)
```

### Consent state transitions

```
First launch:
    GenizahGUI.__init__ (post-show)
        -> telemetry.show_first_run_prompt(self)
            -> load_app_config()['telemetry_first_run_shown'] absent
            -> show QDialog (bilingual, default OFF)
            -> set_consent(True/False)
                -> save_app_config({'telemetry_enabled': bool,
                                    'telemetry_install_id': uuid4.hex  [if True]
                                    OR delete key               [if False]})
            -> save_app_config({'telemetry_first_run_shown': True})

Settings toggle:
    SettingsDialog._build_general_tab()
        -> chk.setChecked(load_app_config().get('telemetry_enabled', False))
    SettingsDialog.accept()
        -> desktop.telemetry.set_consent(chk.isChecked())
    SettingsDialog._on_cancel()
        -> save_app_config(self._config_snapshot)  [already handles rollback]
```

---

## Recommended File Layout

```
desktop/
    telemetry.py          [NEW] - single consent chokepoint, scrubbing, public API
    join_workbench.py     [existing]
    my_library_tab.py     [existing]
    ...

tests/
    test_telemetry_consent_gate.py       [NEW] - is_enabled gate; opt-out clears ID
    test_telemetry_scrubbing.py          [NEW] - _scrub_props rules; path patterns
    test_telemetry_no_direct_posthog.py  [NEW] - AST guard: no direct enqueue_event
    test_telemetry_exception_hooks.py    [NEW] - chain preserved; non-blocking
    test_telemetry_performance_sampling.py [NEW] - 1-in-N gate; perf_signal timing
```

---

## Build Order (Dependency-Ordered)

Each phase must be green before the next begins.

### Phase 111 — Foundation: desktop/telemetry.py module + consent storage

- `desktop/telemetry.py` scaffold with all eight public functions.
- All functions return immediately if `not is_enabled()` (stubs for non-consent logic).
- Consent + install-ID read/write against `config.pkl` via `load_app_config / save_app_config`.
- `_scrub_props()` with full rules.
- `test_telemetry_consent_gate.py`, `test_telemetry_scrubbing.py`, `test_telemetry_no_direct_posthog.py`.
- **No UI wiring yet.**

**Why first:** every other phase depends on `is_enabled()` and `track()` existing.

### Phase 112 — Consent UX: first-run dialog + Settings toggle

- `show_first_run_prompt()` implementation: bilingual `QDialog`, default OFF, two buttons.
- Wired into `GenizahGUI.__init__` after `self.show()` or equivalent post-show hook.
- Checkbox row added to `SettingsDialog._build_general_tab()`.
- Tests: dialog fires exactly once; subsequent launches skip it; Settings toggle calls `set_consent`.

**Why second:** consent must exist and be plumbable before any events can be emitted.

### Phase 113 — Exception hooks

- `install_exception_hooks()` implementation.
- Called in `genizah_app.py` immediately after `_setup_crash_handler()` (line ~171).
- Crash scrubbing: frame-local stripping + path redaction + 2000-char truncation.
- `test_telemetry_exception_hooks.py`: prior hook still runs; enqueue is non-blocking; `KeyboardInterrupt` not captured.

**Why third:** hooks are installed at startup; gate on `is_enabled()` from Phase 111.

### Phase 114 — Usage events

- `track()` calls added to high-value action sites in `genizah_app.py`: tab switches, search mode changes (keyword/Responsa/composition/parallels), Lab mode toggle, Joins Lab open, key dialog opens.
- Session start/end events (`app_started` in `__init__`, `app_closed` in `GenizahGUI.closeEvent`).
- App version + OS version set as module-level constants in `desktop/telemetry.py`, added to every event's properties automatically in `_emit()`.
- Tests: events gated; scrubbing rules exercised on real callsite shapes.

**Why fourth:** needs consent gate and scrubbing from Phases 111-112.

### Phase 115 — Performance events

- `perf_signal = pyqtSignal(float, int)` added to `SearchThread`, `CompositionThread`, `LabSearchThread`.
- Timing wrapper (`time.perf_counter`) in each `run()` method.
- UI-thread signal handlers in `GenizahGUI` call `track_performance()`.
- `_PERF_SAMPLE_N = 3` default; env-var override.
- `test_telemetry_performance_sampling.py`: timing wrapper measures elapsed; 1-in-3 sampling reduces emit count.

**Why fifth:** independent of crash hooks; needs `track_performance()` from Phase 111.

### Phase 116 — Privacy audit + CI gate

- Test suite exercises every `track()` callsite with `is_enabled()` forced True and asserts no banned keys in queued payloads.
- `test_telemetry_no_direct_posthog.py` AST guard confirmed green in CI matrix (Windows + Ubuntu).
- `test_telemetry_scrubbing.py` covers all path pattern variants: Windows paths, POSIX paths, bare filenames, Hebrew + Latin mixed strings, Unicode filenames.
- Bilingual privacy disclosure text finalized for first-run dialog and Settings tab.

**Why last:** validation phase; requires all prior phases to be wired.

---

## Anti-Patterns to Avoid

### Anti-Pattern 1: Calling enqueue_event from anywhere except desktop/telemetry.py

**What:** Importing `shared.posthog_server.enqueue_event` directly in `genizah_app.py`, `gui_threads.py`, or other desktop modules.
**Why bad:** Bypasses the consent gate. A single forgotten `is_enabled()` check sends data after opt-out. The AST guard enforces this structurally.
**Do this instead:** All callsites use `desktop.telemetry.track(...)` exclusively.

### Anti-Pattern 2: Installing a new sys.excepthook without chaining

**What:** `sys.excepthook = my_new_hook` — replacing instead of wrapping.
**Why bad:** The existing `_setup_crash_handler` crash-log write is lost. Exceptions stop printing to stderr. The chain to `sys.__excepthook__` is severed.
**Do this instead:** Capture `sys.excepthook` after `_setup_crash_handler()` runs, wrap it, and call it unconditionally at the end of the wrapper.

### Anti-Pattern 3: Placing consent state in session.json

**What:** Writing `telemetry_enabled` to `Config.SESSION_FILE`.
**Why bad:** `session.json` is session-scoped — it is cleared on interrupted-search recovery and may be wiped on crash recovery. The user's consent preference would be silently lost.
**Do this instead:** All telemetry preferences go in `config.pkl` via `load_app_config / save_app_config`, which is never touched by session recovery.

### Anti-Pattern 4: Any blocking call inside an exception hook

**What:** `requests.post()`, file I/O, or lock acquisition inside `_telemetry_excepthook`.
**Why bad:** An exception hook runs in a failing program state. Blocking inside the hook can deadlock.
**Do this instead:** The hook body does only: `is_enabled()` check + scrub/format props + `put_nowait` onto the queue. All network I/O stays in the daemon drain thread.

### Anti-Pattern 5: Minting the install ID before consent is given

**What:** Creating a `uuid4` at import time or app startup, before the user has seen the dialog.
**Why bad:** An ID that exists before consent undermines the "default OFF" posture.
**Do this instead:** `uuid4` is minted inside `set_consent(True)` — only when the user actively opts in. `telemetry_install_id` does not exist in `config.pkl` until that moment.

---

## Integration Points

### Internal boundaries

| Boundary | Communication | Notes |
|----------|---------------|-------|
| `desktop/telemetry.py` -> `shared/posthog_server.py` | Direct call: `enqueue_event()` | Only boundary permitted; AST guard enforces |
| `desktop/telemetry.py` -> `genizah_core` | `load_app_config()` / `save_app_config()` | No PyQt in `genizah_core`; safe call |
| `genizah_app.py` -> `desktop/telemetry.py` | Import; `install_exception_hooks()` + `show_first_run_prompt()` at startup; `track()` at action sites | PyQt UI stays in `genizah_app.py`, not in `desktop/telemetry.py` |
| `gui_threads.py` -> `genizah_app.py` | `perf_signal` Qt signal -> UI-thread slot -> `track_performance()` | Keeps threading clean; telemetry always called on UI thread |

### External services

| Service | Integration Pattern | Notes |
|---------|---------------------|-------|
| PostHog EU (`eu.i.posthog.com`) | Fire-and-forget via `shared/posthog_server` daemon | Existing project id 134161, org "Dicta". `POSTHOG_API_KEY` env var; publishable key embedded in binary is standard PostHog practice |
| `config.pkl` (local disk) | `load_app_config()` / `save_app_config()` pickle RW | Lives in `%LOCALAPPDATA%\GenizahSearchPro\` on standard install; `Genizah_Index/` on portable. Never cleared by session recovery |

---

## Confidence Assessment

| Area | Confidence | Basis |
|------|------------|-------|
| `desktop/` chokepoint placement | HIGH | `shared/` contract verified against `shared/joins_lab.py` AST guard + `shared/posthog_server.py` docstring |
| `config.pkl` as consent store | HIGH | `genizah_core.py` lines 2344-2378 + 2871-2891 read directly; `SettingsDialog` already snapshots `load_app_config()` on open |
| Existing crash handler exact shape | HIGH | `genizah_app.py` lines 148-170 read directly; chain sequence (`sys.__excepthook__`) confirmed |
| No existing `threading.excepthook` | HIGH | Grep across entire codebase: zero hits |
| `SearchThread` timing insertion point | HIGH | `gui_threads.py` lines 96-121 read directly; `perf_signal` pattern consistent with `results_signal` / `error_signal` |
| No `QSettings` in codebase | HIGH | Grep across `genizah_app.py`: zero `QSettings` instantiation |
| PostHog queue capacity sufficient | HIGH | `shared/posthog_server.py` line 47: `Queue(maxsize=10000)`; desktop volume (dozens of users x 50 searches/day) is far below this |

---

*Architecture research for: v8.1.0 Desktop Telemetry integration into the existing PyQt6 + shared/ architecture*
*Researched: 2026-06-13*
