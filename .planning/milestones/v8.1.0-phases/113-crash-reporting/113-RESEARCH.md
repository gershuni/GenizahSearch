# Phase 113: Crash Reporting — Research

**Researched:** 2026-06-15
**Domain:** Python exception hooks + faulthandler native crash capture + lock-free emission path for a PyQt6 frozen-binary (PyInstaller) Windows desktop app
**Confidence:** HIGH — all cited code locations verified against live source files

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**D-01 — Qt-slot capture (resolves CRASH-02 conflict):**
Default `sys.excepthook`-only (+ `threading.excepthook`). PyQt6 routes exceptions escaping
a slot to `sys.excepthook` since PyQt 5.5. Do NOT add `QApplication.notify` override by
default — only if a spike proves slot exceptions escape `sys.excepthook` in the frozen build;
if added it MUST be: installed via a QApplication subclass before `QApplication(sys.argv)`,
non-swallowing, deduped against the excepthook path. Required test matrix: QTimer.singleShot
slot raise, signal-connected slot raise, QThread.run() raise — dev AND frozen; assert hook
fires or document gap.

**D-02 — Native crash event content:**
`desktop_prior_crash` carries base props + a path-free `fatal_error` label from a FIXED ENUM:
`segmentation_fault` / `access_violation` / `abort` / `stack_overflow` / `unknown_native`.
Mapped from known faulthandler first-line prefixes — never raw text. NO frames, paths, or
module/line in native event. Raw dump stays local only, never transmitted.

**D-03 — Native dump file + lifecycle:**
Use `Config.INDEX_DIR` for dump file (NOT hardcoded `%LOCALAPPDATA%\GenizahSearchPro\`).
Read + classify previous dump BEFORE calling `faulthandler.enable()`. Keep handle in a
module global for process lifetime. Truncate/rotate only AFTER one-shot prior-crash decision.
Pending-emit path: if prior crash detected but consent not yet True, hold pending; emit once
iff/when consent becomes True; never emit if user never consents; emit exactly once.

**D-04 — Handled/non-fatal track_error() wiring — DEFERRED.**
Phase 113 = unhandled crash hooks + native crash only. `track_error()` stays producer-less
until Phase 114+.

**D-05 — Crash emission path must be LOCK-FREE end-to-end (BLOCKER):**
Build a dedicated crash emission path that: (1) reads consent via `_is_enabled_nolock()`
(direct `_enabled` global read); (2) reads `distinct_id` from a lock-free snapshot global;
(3) builds scrubbed payload without going through `_emit()` or public `enqueue_event()`
lock-takers; (4) guarded by a re-entrancy/recursion flag.

**D-06 — Crash delivery must be PRIORITY/direct, not plain enqueue+FIFO-flush:**
The crash event gets priority/direct delivery — a direct POST of the crash event, deadline-
bounded, bypassing the FIFO queue — then `_flush_before_exit(0.5)` for anything else.
Requires a neutral, backward-compatible addition to `shared/posthog_server.py`. Queue-full
currently drops the NEW event; the crash event must not be the one dropped.

**D-07 — Payload = top-frame-only, frame-walked, allowlist reconciled:**
Top-frame-only, NO full traceback string. Frame-walk: `co_filename` basename + `tb_lineno` —
do NOT call `traceback.format_exception`, do NOT touch `str(exc)`. Innermost IN-APP frame,
not blindly innermost. `error_module` = basename from allowlisted app module root, else
`external`. Allowlist changes: keep `exc_type`/`exc_module`/`exc_lineno`; ADD
`error_fingerprint` + boolean `is_background_thread` + native `fatal_error` label key; REMOVE
`traceback_scrubbed` (no full traceback string may ever leave the chokepoint).

**D-08 — Hook installation wiring + exit flush:**
`install_exception_hooks()` is a stub at `desktop/telemetry.py:704`, NEVER called from
`genizah_app.py`. Wire it after existing `_setup_crash_handler()` has run, before risky startup
work. Idempotent registration + dedup by traceback/exception id. `atexit` registered INSIDE
`install_exception_hooks()` (desktop-side only, NOT in `shared/posthog_server.py`). Clean-exit
flush timeout ~1–2s (crash path stays 0.5s per SC#5).

### Claude's Discretion

Exact new `_ALLOWED_PROPS` key spellings (subject to D-07 removals/additions); in-app
module-root allowlist contents; precise faulthandler-prefix→enum mapping table; recursion-
guard mechanism shape; exact priority/direct-send API name added to `posthog_server`; clean-
exit `atexit` timeout (1–2s); whether the `notify` spike is even run (default: skip).
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| CRASH-01 | Uncaught main-thread exceptions captured via `sys.excepthook`, chaining to existing `_setup_crash_handler` (never replacing); telemetry in `try/finally` so a telemetry failure cannot suppress crash-log handler | §Hook Chaining, §Code Locations |
| CRASH-02 | Uncaught worker/QThread exceptions via `threading.excepthook`; Qt-slot exceptions satisfied by `sys.excepthook` (D-01 amendment) | §Qt Slot Coverage, §QThread Gap |
| CRASH-03 | Native crashes captured to local file via `faulthandler` — local file only, not transmitted | §Faulthandler Mechanics |
| CRASH-04 | Crash events contain only exception type name, scrubbed stack location, app version, OS — never frame locals, message strings, file paths, query text | §Payload Construction |
| CRASH-05 | Hooks non-blocking, re-entrancy-safe, consent gate uses cached value (no disk read/settings init inside hook) | §Lock-Free Emission, §Recursion Guard |
| CRASH-06 | Final crash event delivered via bounded synchronous flush before exit, prioritized over full queue | §Priority/Direct Send |
| CRASH-07 | Native crash detected on next launch and emitted once (after consent) | §Next-Launch Detection |
</phase_requirements>

---

## Summary

Phase 113 wires the crash producers on top of the Phase 111/112 foundation. Every infrastructure
piece (consent gate, scrubber, allowlist, DesktopEvent enum, `_flush_before_exit`, transport) is
already built and verified in live source — this phase is pure wiring and one focused new
abstraction (the lock-free crash path).

The primary risk is the D-05 deadlock scenario: normal `_emit()` takes `_state_lock`; normal
`enqueue_event()` takes `_default_distinct_id_lock` + `_scrub_hook_lock`. A crash on a thread
that holds any of these locks (e.g., inside `set_consent()`) would deadlock the non-reentrant
crash hook. The solution is a dedicated lock-free emission path: a module-global `_crash_distinct_id`
snapshot (plain Python object write under the GIL), a `_is_enabled_nolock()` that reads `_enabled`
directly, and a new `send_crash_event_direct(payload)` helper in `shared/posthog_server.py`
that makes one bounded POST without touching any queue or lock.

The faulthandler native-crash path is equally straightforward: read and classify the previous
run's dump (if any) before enabling faulthandler for the new run. Classification maps known
Windows/POSIX faulthandler first-line prefixes to a five-value enum; unknown text maps to
`unknown_native` without ever transmitting it.

**Primary recommendation:** Implement the lock-free path as a self-contained `_emit_crash_direct()`
function in `desktop/telemetry.py` that bypasses all locks, guarded by a module-level bool
recursion flag, and backed by a new `send_crash_event_direct(event, properties, distinct_id,
timeout)` in `posthog_server.py` that does a single `requests.post` (never touches the queue).

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Exception hook installation & chaining | `desktop/telemetry.py` | `genizah_app.py` (wires the call) | Chokepoint owns all crash emission; `genizah_app.py` owns startup sequencing |
| Lock-free consent read | `desktop/telemetry.py` | — | Must be in the same module as `_enabled` to read it directly |
| Lock-free distinct_id snapshot | `desktop/telemetry.py` | — | Module-level mirror global; written on consent/identity changes |
| Priority/direct crash POST | `shared/posthog_server.py` | — | Transport layer; neutral addition; web callers unaffected |
| Payload construction (frame-walk) | `desktop/telemetry.py` | — | Lives with scrubber + allowlist; never in `posthog_server` |
| Faulthandler enable + file handle | `desktop/telemetry.py` | `Config.INDEX_DIR` (path source) | Desktop-only concern; handle must outlive startup function scope |
| Next-launch native crash classify | `desktop/telemetry.py` | — | Reads dump file, builds `desktop_prior_crash` payload |
| Pending-emit-after-consent | `desktop/telemetry.py` | `genizah_app.py` (triggers when consent fires) | Consent lives in `desktop/telemetry`; caller triggers emit after consent |
| atexit flush registration | `desktop/telemetry.py` (`install_exception_hooks`) | — | Must NOT be in `shared/posthog_server.py` (web process must not exit-flush) |

---

## Standard Stack

### Core (all stdlib — zero new dependencies)

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `sys.excepthook` | Python 3.10+ (stdlib) | Capture unhandled main-thread exceptions | Standard Python global exception hook; PyQt6 routes slot exceptions here since PyQt 5.5 `[CITED: docs.python.org/3/library/sys.html#sys.excepthook]` |
| `threading.excepthook` | Python 3.8+ (stdlib) | Capture unhandled `threading.Thread` exceptions | Official hook for background Python threads; `threading.__excepthook__` (Python 3.10+) for clean chaining `[CITED: docs.python.org/3/library/threading.html#threading.excepthook]` |
| `faulthandler` | Python 3.3+ (stdlib) | Write native C-extension crash info to a file | Only stdlib mechanism that survives `SIGSEGV` / Windows structured exceptions `[CITED: docs.python.org/3/library/faulthandler.html]` |
| `atexit` | Python stdlib | Register clean-exit flush | Runs on `sys.exit()` through normal app quit; does NOT run on uncaught exception exit (separate path) `[ASSUMED]` |

### Already Built in Phase 111 (reuse, never re-implement)

| Component | Location | What it provides |
|-----------|----------|-----------------|
| `_ALLOWED_PROPS` | `desktop/telemetry.py:247-266` | Allowlist for every property leaving the chokepoint — updated by D-07 |
| `_scrub_props` / `_scrub_value` | `desktop/telemetry.py` | Path + Hebrew + length redaction; must run inside `_emit_crash_direct` |
| `DesktopEvent.CRASH` / `PRIOR_CRASH` | `desktop/telemetry.py:106-107` | Fixed enum values `'desktop_crash'` / `'desktop_prior_crash'` |
| `_flush_before_exit(0.5)` | `shared/posthog_server.py:272` | FIFO bounded flush for remaining events after priority crash send |
| `_drain_and_discard` | `shared/posthog_server.py:257` | Queue purge on opt-out |
| `set_default_distinct_id` | `shared/posthog_server.py:90` | Transport distinct_id default |
| `_enabled`, `_enabled_lock` | `desktop/telemetry.py:79-80` | Consent state; `_enabled` read lock-free via GIL |
| `_state_lock`, `_current_distinct_id` | `desktop/telemetry.py:84,82` | Identity state — the lock the crash path MUST bypass |
| `Config.INDEX_DIR` | `genizah_core.py:2342-2364` | Reliably-writable dir for faulthandler dump |

**Installation:** No new `pip install` commands. Zero new dependencies. `[VERIFIED: stdlib inspection]`

---

## Package Legitimacy Audit

No external packages are added in this phase. All capabilities use Python stdlib or existing
project dependencies. `[VERIFIED: stdlib inspection]`

| Package | Registry | Age | Downloads | Source Repo | slopcheck | Disposition |
|---------|----------|-----|-----------|-------------|-----------|-------------|
| (none) | — | — | — | — | — | N/A |

**Packages removed due to slopcheck [SLOP] verdict:** none
**Packages flagged as suspicious [SUS]:** none

---

## Architecture Patterns

### System Architecture Diagram

```
Exception (main thread OR background thread)
    |
    v
sys.excepthook / threading.excepthook [NEW wrapper in install_exception_hooks()]
    |
    +-- recursion guard (_in_crash_hook flag) ----------------------+
    |   If already in crash handler: skip telemetry, chain only     |
    |                                                               |
    +-- _is_enabled_nolock() [direct global read, no lock] ---------+
    |   If False: skip telemetry section                           |
    |                                                               |
    v
_emit_crash_direct(exc_type, exc_tb, is_background) [NEW — lock-free]
    |
    +-- _make_crash_props() [frame-walk, no format_exception]
    |     innermost IN-APP frame (exc_tb walk, co_filename basename)
    |     exc_type, exc_module (basename or 'external'), exc_lineno
    |     error_fingerprint, is_background_thread
    |
    +-- _scrub_props() / _validate_props() [existing scrubber]
    |
    +-- _crash_distinct_id [module global snapshot, no lock]
    |
    v
send_crash_event_direct(event, props, distinct_id, timeout=0.5) [NEW in posthog_server]
    |  Direct requests.post — bypasses queue, bypasses all locks
    |  deadline-bounded (timeout param)
    |  NEUTRAL: no break to existing queue/_event_queue monkeypatches
    v
PostHog EU capture endpoint

    |
    v (after direct send returns)
_prior_hook(exc_type, exc_value, exc_tb) [crash_log.txt + stderr + sys.__excepthook__]
    |
    (for clean exit path only):
    atexit -> _flush_before_exit(1.5) [remaining FIFO events]
```

```
App startup (faulthandler path):
    |
    v
_setup_faulthandler() called from install_exception_hooks()
    |
    +-- READ: Config.INDEX_DIR/faulthandler_dump.txt [if exists + non-empty]
    |     -> _classify_native_crash(text) -> fatal_error enum label
    |     -> if consent True: emit desktop_prior_crash immediately
    |     -> if consent False: store in _pending_native_crash module global
    |
    +-- faulthandler.enable(file=_faulthandler_handle, all_threads=True)
    |     [_faulthandler_handle is a module global — keeps file open for process lifetime]
    |
    v
(on next launch) Config.INDEX_DIR/faulthandler_dump.txt may contain new crash evidence

    |
    v (consent becomes True later in startup or via Settings toggle):
set_consent(True) calls _emit_pending_native_crash() [NEW helper]
    -> checks _pending_native_crash global
    -> emits desktop_prior_crash exactly once
    -> clears _pending_native_crash
```

### Recommended Project Structure

No new directories. All changes to existing files:

```
desktop/
    telemetry.py          [MODIFY] — implement install_exception_hooks() stub;
                            add _is_enabled_nolock(), _crash_distinct_id snapshot global,
                            _emit_crash_direct(), _make_crash_props(), _setup_faulthandler(),
                            _classify_native_crash(), _emit_pending_native_crash(),
                            _pending_native_crash module global, _faulthandler_handle module global

shared/
    posthog_server.py     [MODIFY] — add send_crash_event_direct() neutral helper

genizah_app.py            [MODIFY] — wire install_exception_hooks() after _setup_crash_handler()

tests/
    test_crash_hooks.py               [NEW] — chain preserved, lock-free, QTimer/QThread matrix
    test_crash_payload.py             [NEW] — allowlist, no-message, frame-walk, in-app frame
    test_native_crash.py              [NEW] — faulthandler classify + pending-emit timing
    test_crash_priority_send.py       [NEW] — direct send bypasses saturated queue
```

### Pattern 1: Lock-Free Crash Emission Path

**What:** A dedicated `_emit_crash_direct()` function that reads `_enabled` and `_crash_distinct_id`
as plain module globals (under the GIL, no lock), builds the scrubbed payload, calls
`send_crash_event_direct()` in `posthog_server`.

**When to use:** Exclusively from within `sys.excepthook` and `threading.excepthook` wrappers.
Never from `track()` or `_emit()` (those are for normal consent-gated usage events).

```python
# desktop/telemetry.py — NEW additions for Phase 113
# [CITED: CPython data model — bool/str reads are atomic under the GIL]

# Snapshot global: written by set_consent(True) and _set_current_distinct_id(),
# read lock-free in the hook body. Plain str assignment under the GIL is atomic
# in CPython. Initialized to None; updated before _enabled flips True.
_crash_distinct_id: str | None = None

# Recursion guard: prevents crash inside the crash handler from looping.
# Written + read only by the crash hook (only one at a time under GIL during
# exception propagation). Plain bool — no lock needed.
_in_crash_hook: bool = False

def _is_enabled_nolock() -> bool:
    """Lock-free consent read for crash hooks.

    Reads _enabled directly (no lock). Safe because:
    - CPython GIL ensures a simple bool read is atomic.
    - threading.excepthook runs on the FAILING thread; if that thread held
      _enabled_lock we'd deadlock — this path skips the lock entirely.
    - Worst case: returns stale value (False when True or True when False).
      False-negative: miss one crash event. True-positive: send a crash event
      when opted-out (impossible — set_consent(False) clears the flag THEN the key).
      Both are acceptable; deadlock is not.
    D-05 / SC#4 compliance.
    """
    return _enabled  # direct global read, GIL-safe

def _make_crash_props(
    exc_type: type,
    exc_tb: 'TracebackType | None',
    is_background: bool,
) -> dict:
    """Build a crash payload by walking the traceback — no format_exception, no str(exc).

    Finds the innermost IN-APP frame: a tb frame whose co_filename basename
    matches an allowlisted app module root (not a stdlib or site-packages file).
    Falls back to the deepest frame if no in-app frame is found, with
    error_module='external'.
    D-07 compliance.
    """
    # In-app module roots: basenames of files that belong to this app.
    # Never include user plugin paths. The planner finalizes this list.
    _IN_APP_ROOTS = frozenset({
        'genizah_app.py', 'genizah_core.py', 'gui_threads.py',
    })  # + any desktop/ and shared/ basenames
    _IN_APP_PREFIXES = ('desktop/', 'shared/', 'web/')  # for full-path fallback check

    error_module = 'external'
    error_line = 0
    frame = exc_tb
    innermost = exc_tb
    if frame is not None:
        while frame.tb_next:
            frame = frame.tb_next
        innermost = frame  # deepest frame — fallback

    # Walk again looking for the innermost IN-APP frame
    frame = exc_tb
    in_app_frame = None
    while frame is not None:
        basename = os.path.basename(frame.tb_frame.f_code.co_filename)
        if basename in _IN_APP_ROOTS:
            in_app_frame = frame  # keep walking to find innermost in-app frame
        frame = frame.tb_next

    target = in_app_frame or innermost
    if target is not None:
        error_module = os.path.basename(target.tb_frame.f_code.co_filename)
        error_line = target.tb_lineno

    error_type = exc_type.__name__ if exc_type else 'UnknownException'
    fingerprint = f'{error_type}:{error_module}:{error_line}'
    return {
        'exc_type': error_type,
        'exc_module': error_module,
        'exc_lineno': error_line,
        'error_fingerprint': fingerprint,
        'is_background_thread': is_background,
        # base props merged in _emit_crash_direct
    }

def _emit_crash_direct(
    exc_type: type,
    exc_tb: 'TracebackType | None',
    is_background: bool,
) -> None:
    """Lock-free crash emission.

    Call from inside sys.excepthook / threading.excepthook wrappers ONLY.
    Must never be called from normal track() paths.
    D-05, D-06, SC#4.
    """
    global _in_crash_hook
    if _in_crash_hook:
        return  # recursion guard — crash inside the crash handler
    _in_crash_hook = True
    try:
        if not _is_enabled_nolock():
            return
        distinct_id = _crash_distinct_id or 'system'
        props = _make_crash_props(exc_type, exc_tb, is_background)
        # Merge base props (no lock — _BASE_PROPS reads module-level constants)
        merged = dict(_BASE_PROPS())
        merged.update(props)
        validated = _validate_props(merged)
        scrubbed = _scrub_props(validated)
        # D-06: direct send — bypass queue, bypass all locks
        from shared.posthog_server import send_crash_event_direct
        send_crash_event_direct(
            DesktopEvent.CRASH.value, scrubbed, distinct_id, timeout=0.5
        )
    except Exception:
        pass  # hook body MUST never raise (SC#4)
    finally:
        _in_crash_hook = False
```

**Note:** `_BASE_PROPS()` reads only module-level constants (`_APP_VERSION`, `platform` calls) —
it does NOT acquire `_state_lock`. Verify this in `desktop/telemetry.py` before implementing.

### Pattern 2: Neutral Priority/Direct Send in posthog_server

**What:** A new function `send_crash_event_direct()` that makes one synchronous `requests.post`
call, deadline-bounded, without touching `_event_queue`, `_default_distinct_id_lock`, or
`_scrub_hook_lock`. The 5 existing `_event_queue` monkeypatches are completely unaffected.

```python
# shared/posthog_server.py — NEW helper (neutral addition per D-06)
def send_crash_event_direct(
    event: str,
    properties: dict,
    distinct_id: str,
    timeout: float = 0.5,
) -> None:
    """Synchronous, priority POST for crash events.

    Called from the desktop crash hook (sys.excepthook / threading.excepthook).
    Bypasses the FIFO _event_queue entirely — crash events are never queued and
    never subject to daemon-thread drain timing.

    NEUTRAL by design:
    - Does NOT touch _event_queue (5 monkeypatches unaffected)
    - Does NOT acquire _default_distinct_id_lock or _scrub_hook_lock
    - Does NOT start or interact with the drain thread
    - Web callers do NOT call this function
    Never raises.
    """
    try:
        api_key = _resolve_api_key()
        if not api_key:
            return
        url = _resolve_capture_url()
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

Add to `__all__` alongside the existing Phase 111 additions.

### Pattern 3: Faulthandler Setup + Classify + Pending-Emit

**What:** Read old dump → classify → possibly emit → enable faulthandler for this run.
Called once from `install_exception_hooks()` on app startup.

```python
# desktop/telemetry.py — NEW faulthandler helpers

# Enum label for native crashes (D-02)
_NATIVE_CRASH_LABELS = {
    # faulthandler first-line prefix -> enum label
    # Windows (case-insensitive match on first line):
    'windows fatal exception: access violation': 'access_violation',
    'windows fatal exception: stack overflow':   'stack_overflow',
    'windows fatal exception: int divide by zero': 'abort',  # map to closest
    'windows exception:':                         'unknown_native',  # rare; generic Windows exc
    # POSIX signals (first word of first line):
    'segmentation fault':                         'segmentation_fault',
    'aborted':                                    'abort',
    'floating-point exception':                   'abort',
    'bus error':                                  'abort',
    # Generic CPython fatal:
    'fatal python error:':                        'unknown_native',
}

# Module globals for faulthandler lifecycle
_faulthandler_handle = None     # kept open for whole process lifetime
_pending_native_crash: str | None = None  # fatal_error label, held if consent not yet True

def _classify_native_crash(text: str) -> str:
    """Map faulthandler dump first-line to a fixed enum label. Never returns raw text."""
    if not text:
        return 'unknown_native'
    first_line = text.splitlines()[0].lower().strip()
    for prefix, label in _NATIVE_CRASH_LABELS.items():
        if first_line.startswith(prefix):
            return label
    return 'unknown_native'

def _emit_native_crash(label: str) -> None:
    """Emit desktop_prior_crash with fatal_error label. Consent-gated. Never raises."""
    try:
        if not _is_enabled_nolock():
            return
        distinct_id = _crash_distinct_id or 'system'
        props = dict(_BASE_PROPS())
        props['fatal_error'] = label
        validated = _validate_props(props)
        scrubbed = _scrub_props(validated)
        from shared.posthog_server import send_crash_event_direct
        send_crash_event_direct(
            DesktopEvent.PRIOR_CRASH.value, scrubbed, distinct_id, timeout=0.5
        )
    except Exception:
        pass

def _emit_pending_native_crash() -> None:
    """Called from set_consent(True) path. Emits held pending crash exactly once."""
    global _pending_native_crash
    label = _pending_native_crash
    if label is None:
        return
    _pending_native_crash = None  # clear BEFORE emitting to ensure exactly-once
    _emit_native_crash(label)

def _setup_faulthandler() -> None:
    """Read + classify previous dump, enable faulthandler for this run. D-03."""
    global _faulthandler_handle, _pending_native_crash
    import faulthandler
    from genizah_core import Config  # lazy import to avoid circular at module level
    dump_path = os.path.join(Config.INDEX_DIR, 'faulthandler_dump.txt')
    try:
        # Step 1: read BEFORE enable() (opening for write would erase evidence)
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
                # truncate after successful emit
                try:
                    open(dump_path, 'w').close()
                except OSError:
                    pass
            else:
                _pending_native_crash = label  # hold; set_consent(True) will emit
                # do NOT truncate yet — truncate after emit in _emit_pending_native_crash
        # Step 3: open the file for THIS run's writes (truncate/create fresh)
        # This overwrites any prior content IF we already emitted; if pending we leave
        # content so a next-next-launch doesn't see an empty file.
        # Simpler: always open for append with a run separator so multi-crash runs
        # don't lose evidence. Planner may choose truncate for simplicity.
        _faulthandler_handle = open(dump_path, 'w', encoding='utf-8')
        faulthandler.enable(file=_faulthandler_handle, all_threads=True)
    except Exception:
        # faulthandler is best-effort; failure must never block app startup
        pass
```

**Ordering invariant (D-03):** `_setup_faulthandler()` called inside `install_exception_hooks()`,
which is called AFTER `_setup_crash_handler()` at the top of `genizah_app.py`. This means:
1. Python crash-log hook is already in place before faulthandler setup runs.
2. `_crash_distinct_id` snapshot global is populated BEFORE any crash could occur
   (it is set as part of `set_consent(True)` / `_load_consent_state()` at import time).

**Truncation vs pending invariant:** When consent is False at startup (most first launches):
- prior dump → held in `_pending_native_crash` (NOT truncated yet)
- `_faulthandler_handle` is opened for write (new run's output overwrites old)
- This means the NEXT crash overwrites the pending one before emit if consent never comes
- This is correct and intended: "If the user never consents, never emit" (D-03 / CRASH-07)

### Pattern 4: Hook Installation Wiring in genizah_app.py

```python
# genizah_app.py — AFTER line 170 (_setup_crash_handler() call)
# Wire: telemetry crash hooks (Phase 113)
try:
    from desktop import telemetry as _telemetry
    _telemetry.install_exception_hooks()
except Exception:
    pass  # crash hooks are best-effort; never block app startup
```

`install_exception_hooks()` in `desktop/telemetry.py` becomes:

```python
def install_exception_hooks() -> None:
    """Install crash-capture exception hooks + faulthandler. Idempotent. Never raises."""
    global _hooks_installed
    try:
        if _hooks_installed:
            return  # idempotency guard
        _hooks_installed = True

        # 1. Wrap sys.excepthook (chain: telemetry -> crash_log.txt -> sys.__excepthook__)
        _prior_hook = sys.excepthook
        def _telemetry_excepthook(exc_type, exc_value, exc_tb):
            try:
                if (exc_type is not KeyboardInterrupt
                        and exc_type is not SystemExit):
                    # Dedup by exception id (slot + notify + excepthook may triple-fire)
                    _emit_crash_direct(exc_type, exc_tb, is_background=False)
            except Exception:
                pass
            _prior_hook(exc_type, exc_value, exc_tb)
        sys.excepthook = _telemetry_excepthook

        # 2. Install threading.excepthook (Python 3.8+; chain to __excepthook__ if 3.10+)
        _prior_thread_hook = getattr(threading, '__excepthook__',
                                     threading.excepthook)  # Python 3.10+ has __excepthook__
        def _telemetry_threading_hook(args):
            try:
                if (args.exc_type is not KeyboardInterrupt
                        and args.exc_type is not SystemExit):
                    _emit_crash_direct(args.exc_type, args.exc_traceback,
                                       is_background=True)
            except Exception:
                pass
            _prior_thread_hook(args)
        threading.excepthook = _telemetry_threading_hook

        # 3. faulthandler + next-launch native crash detection (D-02/D-03)
        _setup_faulthandler()

        # 4. atexit flush for clean exits (NOT in posthog_server — web must not exit-flush)
        import atexit
        def _atexit_flush():
            try:
                from shared.posthog_server import _flush_before_exit
                _flush_before_exit(1.5)  # ~1-2s clean-exit budget per D-08
            except Exception:
                pass
        atexit.register(_atexit_flush)

    except Exception:
        logger.debug('telemetry: install_exception_hooks failed', exc_info=True)
```

### Anti-Patterns to Avoid

- **Calling `_emit()` from the crash hook:** `_emit()` acquires `_state_lock` at line 505.
  Use `_emit_crash_direct()` instead.
- **Calling `enqueue_event()` from the crash hook:** Takes `_default_distinct_id_lock` +
  `_scrub_hook_lock`; the crash event races the FIFO queue (D-06). Use
  `send_crash_event_direct()` instead.
- **Calling `is_enabled()` from the crash hook:** Acquires `_enabled_lock`; deadlock risk
  under `set_consent()`. Use `_is_enabled_nolock()` instead.
- **Calling `traceback.format_exception()` or `str(exc)`:** Both materialize the exception
  message in memory; message strings contain file paths, query text, Hebrew content (D-07).
- **Walking to the blindly-innermost frame:** Collapses fingerprints onto stdlib/site-packages
  internals (D-07). Use the innermost IN-APP frame.
- **Opening faulthandler file for write before reading:** Erases the previous-run crash evidence
  (D-03). Always read + classify first.
- **Placing `atexit.register()` in `shared/posthog_server.py`:** The web process (`web/main.py`)
  imports `shared/posthog_server` — an atexit in there would register a flush for every restart
  of the web server (D-08).
- **Transmitting the raw faulthandler dump text:** May contain Python paths + C stack frames.
  Map to enum label only; local dump file stays local forever.
- **Excluding `SystemExit`:** SystemExit is raised by `sys.exit()` and `QApplication.exec()` return;
  reporting it would fire telemetry on every clean shutdown. Exclude it alongside `KeyboardInterrupt`.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Crash event delivery before exit | Custom queue/thread/sleep combos | `send_crash_event_direct()` (single bounded POST) + existing `_flush_before_exit(0.5)` | Race conditions between daemon thread and process teardown are non-obvious |
| Path scrubbing | New regex | Existing `_scrub_props` / `_PATH_RE` in `desktop/telemetry.py:152-158` | Already handles Windows/UNC/POSIX/bare-filenames with test coverage |
| Property allowlist enforcement | Per-function dict filtering | Existing `_validate_props()` at `desktop/telemetry.py:269` | Allowlist already has D-07 keys to add; dropping disallowed keys centrally |
| Config.INDEX_DIR resolution | Custom LOCALAPPDATA path | `genizah_core.Config.INDEX_DIR` | Already handles portable/legacy/APPDATA cases + ensures dir exists |
| Native crash classification | String parsing logic | Fixed `_NATIVE_CRASH_LABELS` prefix mapping → enum | faulthandler text varies by platform/Python version; fixed enum is the only safe contract |

**Key insight:** The Phase 111 scrubber + allowlist are the most load-bearing privacy mechanisms.
Phase 113 must flow every crash property through them — never bypass. The one new mechanism
(lock-free path) exists only to avoid deadlock, not to bypass the scrubber.

---

## Open Questions Resolved

### Q1: Does sys.excepthook actually receive slot exceptions in the frozen PyQt6/Windows build?

**Answer (MEDIUM confidence — requires spike verification, but default assumption is YES):**

Since PyQt 5.5, PyQt wraps slot calls in C++ try/catch. An unhandled Python exception that
escapes a slot calls `sys.excepthook` and then aborts. `[CITED: PyQt6 docs + fman.io blog
"PyQt excepthook"]` The FROZEN aspect does not change this behavior — PyInstaller bundles the
PyQt6 `.pyd` files unmodified; the exception dispatch is inside Qt's DLL, not the Python
interpreter bootstrap.

**However, one real gap exists:** if the exception is raised on a thread that Qt created (e.g.,
a `QThread.run()` reimplementation called from Qt C++), that thread is not a `threading.Thread`
— it is not tracked by Python's threading module — so `threading.excepthook` does NOT fire.
`sys.excepthook` also may not fire because the C++ layer may catch it and print to stderr.

**Empirical verification:** The required test matrix (D-01) is the definitive answer. The spike
test must run in the actual PyInstaller `.exe`, not just the dev environment. If the frozen
test shows a gap, the `QApplication.notify` override is the fallback (and it must be installed
via a QApplication subclass before `QApplication(sys.argv)`).

**Practical impact:** Most workers in this codebase already have explicit `try/except` + `error_signal`
(SearchThread, LocalIndexerWorker, FolderWalkWorker, StartupThread). The hooks are a backstop
for the few that do not. Even if `QThread.run()` exceptions miss both hooks, the crash would
still write to `crash_log.txt` (the existing C crash hook), so the user experience is unaffected.
The telemetry gap is acknowledged and documented in D-01.

### Q2: Does threading.excepthook fire for a real QThread.run() raise?

**Answer (HIGH confidence — NO, it does not):**

`threading.excepthook` is installed by the Python threading module on `threading.Thread`
instances only. A `QThread` is a C++ object backed by an OS thread; its Python-side `run()`
is called from C++, not from `threading.Thread._bootstrap_inner()`. Therefore the Python
threading exception machinery is not involved, and `threading.excepthook` never fires for
`QThread.run()` exceptions. `[CITED: CPython source threading.py _bootstrap_inner]`

**Backstop framing:** The hooks are documented as backstops for the minority of code that does
not already have `try/except + error_signal`. The primary exception handling for QThread workers
is already in place.

### Q3: Exact faulthandler first-line prefix strings on Windows

**Verified against CPython test suite `Lib/test/test_faulthandler.py` `[CITED: github.com/python/cpython]`:**

| faulthandler first-line (lowercase) | Enum label |
|-------------------------------------|-----------|
| `windows fatal exception: access violation` | `access_violation` |
| `windows fatal exception: stack overflow` | `stack_overflow` |
| `windows fatal exception: int divide by zero` | `abort` |
| `windows fatal exception: float divide by zero` | `abort` |
| `windows exception: code 0x...` | `unknown_native` |
| `segmentation fault` | `segmentation_fault` |
| `aborted` | `abort` |
| `floating-point exception` | `abort` |
| `bus error` | `abort` |
| `fatal python error:` | `unknown_native` |
| (anything else) | `unknown_native` |

**On Windows, faulthandler outputs `"Windows fatal exception: <type>"` as the first line for
structural exceptions (access violation, stack overflow, divide by zero). On POSIX it outputs
the signal name (`"Segmentation fault"`, `"Aborted"`).**

The classification logic should be case-insensitive prefix match on `first_line.lower()`.

### Q4: Recursion-guard mechanism shape

**Recommendation:** A plain module-level `bool` global `_in_crash_hook: bool = False`.

Rationale:
- Python's GIL ensures that only one thread executes Python bytecodes at a time. A crash
  inside the crash handler either happens on the same thread (re-entrancy) or on a different
  thread. For same-thread re-entrancy, the bool flag is sufficient without a lock.
- For cross-thread crashes: a second thread's crash fires its own `sys.excepthook` / 
  `threading.excepthook` invocation on that thread. The bool flag could technically allow two
  simultaneous crash events (one per thread). This is acceptable — both events contain different
  exception types and represent different crashes. The recursion guard is only for same-thread
  infinite loop protection.
- A `threading.local()` variable would be more correct for per-thread re-entrancy, but is more
  complex. A global bool is sufficient for the common case.

**Alternative (more correct for multi-thread):** Use `threading.local()` so each thread has its
own recursion flag. Planner should choose based on complexity budget.

### Q5: Priority/direct-send API design for posthog_server

**Recommendation:** `send_crash_event_direct(event, properties, distinct_id, timeout=0.5)`

The function:
- Calls `_resolve_api_key()` and `_resolve_capture_url()` (these acquire `_capture_config_lock`
  briefly — this is acceptable since the key is set at startup and these are not held during crash).
- Makes one `requests.post(url, json=payload, timeout=timeout)` call.
- Catches all exceptions silently.
- Does NOT touch `_event_queue`, `_default_distinct_id_lock`, `_scrub_hook_lock`, or the drain thread.
- Is added to `__all__`.

**Alternative (avoid even `_capture_config_lock`):** Cache the key and URL as module-level
`_cached_crash_api_key` and `_cached_crash_url` strings updated whenever `set_capture_api_key` /
`set_capture_host` are called. Truly lock-free. More complex. Planner's discretion.

---

## Common Pitfalls

### Pitfall 1: `_BASE_PROPS()` acquires `_state_lock` indirectly

**What goes wrong:** If `_BASE_PROPS()` reads `_identified` or `_current_distinct_id` under
`_state_lock`, calling it from `_emit_crash_direct()` defeats the lock-free guarantee.

**Prevention:** Verify the actual `_BASE_PROPS()` implementation in `desktop/telemetry.py` before
writing `_emit_crash_direct()`. If it acquires `_state_lock`, use a lock-free base-props builder
that reads only module-level constants (`_APP_VERSION`, `platform.system()`, etc.) — not the
identity state. The crash payload does not need `$process_person_profile` or `$anon_distinct_id`.

**Action:** Read `desktop/telemetry.py` lines where `_BASE_PROPS` is defined before implementation.
`[ASSUMED — needs verification against live code]`

### Pitfall 2: `_faulthandler_handle` closed by garbage collector

**What goes wrong:** The file handle opened in `_setup_faulthandler()` could be garbage-collected
if stored in a local variable. When the GC closes it, faulthandler writes to a closed file,
resulting in `OSError: [Errno 9] Bad file descriptor` at crash time (or silent loss of crash data).

**Prevention:** Store the handle in a module-level global (`_faulthandler_handle`). This is
mandatory — a local variable in `_setup_faulthandler()` will be collected. `[CITED: D-03]`

### Pitfall 3: Two successive launches lose the pending native crash

**What goes wrong:** Launch 1 crashes natively → dump written. Launch 2: consent not yet True →
`_pending_native_crash` set. Then app crashes natively again on Launch 2 before consent. Launch 3
opens the dump file for write (faulthandler enable), wiping Launch 1's dump. Launch 3's consent
sees pending = Launch 1's label. This is actually correct behavior per D-03 ("the dump is
overwritten by the next crash if user never consents").

**Note for documentation:** Make this explicit in the code comment. The "emit exactly once" and
"overwritten by next crash if no consent" semantics are both correct.

### Pitfall 4: `threading.excepthook` chain broken on Python 3.10+ by not using `threading.__excepthook__`

**What goes wrong:** On Python 3.10+, `threading.__excepthook__` is the original default hook.
If we chain to `threading.excepthook` (the current value at install time) but another library
also installed its own hook after the default but before ours, we risk double-chaining. More
importantly, on Python 3.10+ the "chain to the prior" pattern should use
`threading.__excepthook__` as the ultimate fallback, analogous to `sys.__excepthook__`.

**Prevention:**
```python
_prior_thread_hook = getattr(threading, '__excepthook__', threading.excepthook)
```
This gives us `threading.__excepthook__` on 3.10+ (the true default, analogous to
`sys.__excepthook__`) and `threading.excepthook` on 3.8-3.9. Since this project requires
Python 3.10+, we can use `threading.__excepthook__` directly.

### Pitfall 5: `send_crash_event_direct` double-fire for events already in the queue

**What goes wrong:** The crash hook calls `send_crash_event_direct()` AND the atexit handler
calls `_flush_before_exit()` which will drain and send any regular queued events. If the crash
event was accidentally also enqueued (e.g., by a fallback code path), it gets sent twice.

**Prevention:** `send_crash_event_direct()` NEVER puts the event in `_event_queue`. It makes a
direct POST only. No path from `_emit_crash_direct()` should touch `_event_queue`.

### Pitfall 6: `crash_log.txt` is adjacent to `__file__` which may be under Program Files in frozen builds

**What goes wrong (pre-existing, not introduced by Phase 113):** `genizah_app.py:156` writes
`crash_log.txt` to `os.path.dirname(__file__)`. In a frozen `.exe` under `C:\Program Files\`,
this directory is read-only and `open(..., 'a')` raises `OSError`. The existing code already
handles this with `except OSError: pass`.

**Phase 113 implication:** Tests must NOT assert that `crash_log.txt` was written (it may not be
writable). Tests should assert that the telemetry hook fires AND chains to the prior hook, but
should not assert on `crash_log.txt` existence in frozen-build tests. Use a mock for the prior
hook in unit tests.

### Pitfall 7: SSL certificate failure silently drops the crash event in frozen binary

**What goes wrong (deferred to Phase 116):** In the frozen `.exe`, `requests` needs the `certifi`
CA bundle. If it's missing, `send_crash_event_direct()` raises `SSLError`, which is silently
caught. The crash event is lost.

**Current status:** Flagged for Phase 116 (PyInstaller TLS/cert bundling). The `except Exception:
pass` in `send_crash_event_direct()` is correct — SSL failure must never crash the crash handler.

---

## Code Examples

### Verified faulthandler API

```python
# Source: docs.python.org/3/library/faulthandler.html
import faulthandler
import os

# Open file for writing (after reading previous run's content)
handle = open('/path/to/dump.txt', 'w', encoding='utf-8')
faulthandler.enable(file=handle, all_threads=True)
# handle must be kept alive (module global) for the process lifetime
# faulthandler.is_enabled() -> bool
# faulthandler.disable() to undo
```

### Verified _resolve_api_key / _resolve_capture_url (existing in posthog_server.py)

```python
# Source: shared/posthog_server.py:145-157 (verified live)
def _resolve_api_key() -> str:
    with _capture_config_lock:
        override = _api_key_override
    return (override or os.environ.get('POSTHOG_API_KEY', '')).strip()
```

### Verified threading.excepthook API (Python 3.8+)

```python
# Source: docs.python.org/3/library/threading.html#threading.excepthook
# threading.excepthook receives a single ExceptHookArgs namedtuple:
#   args.exc_type   -> the exception class
#   args.exc_value  -> the exception instance
#   args.exc_traceback -> the traceback (may be None)
#   args.thread     -> the Thread object (may be None)
# threading.__excepthook__ -> Python 3.10+ only (the original default)
```

### Verified _flush_before_exit signature (Phase 111, live code)

```python
# Source: shared/posthog_server.py:272-311 (verified live)
def _flush_before_exit(timeout: float = 0.5) -> None:
    """Drain + POST synchronously within deadline. Existing Phase 111 function."""
    # deadline-bounded FIFO drain; bypasses daemon thread
```

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `traceback.format_exception` + string scrub | Frame-walk: `co_filename` basename + `tb_lineno` only | D-07 (this phase) | Smaller attack surface; no message string ever materialized |
| `is_enabled()` with lock in hook | `_is_enabled_nolock()` direct global read | D-05 (this phase) | Eliminates deadlock class when `set_consent()` holds `_enabled_lock` |
| `enqueue_event()` + FIFO flush | Direct POST for crash + FIFO flush for rest | D-06 (this phase) | Crash event no longer races daemon thread or FIFO queue saturation |
| `QApplication.notify` override | `sys.excepthook`-only (default) | D-01 (this phase) | Simpler; spike-gated if proven insufficient in frozen build |

**Deprecated/outdated (do NOT build):**
- `traceback_scrubbed` property in `_ALLOWED_PROPS`: REMOVED by D-07. No full-traceback string
  may ever leave the chokepoint. Remove it from `_ALLOWED_PROPS` at line 261.
- `thread_name` property in `_ALLOWED_PROPS`: REPLACED by boolean `is_background_thread` (D-07).
  Remove `thread_name`; add `is_background_thread`, `error_fingerprint`, `fatal_error` (native only).

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `_BASE_PROPS()` in `desktop/telemetry.py` does not acquire `_state_lock` | Pitfall 1, Pattern 1 | `_emit_crash_direct()` would not be fully lock-free — needs alternative base-props builder |
| A2 | `atexit` runs on normal `QApplication.exec()` + `sys.exit()` return in PyInstaller frozen binary on Windows | §Pattern 4 | Clean-exit events might be lost; fall back to `closeEvent` signal flush |
| A3 | `threading.__excepthook__` is available on the installed Python 3.10+ (project minimum) | §Pattern 4 | Graceful degradation via `getattr(threading, '__excepthook__', threading.excepthook)` covers this |
| A4 | `requests.post()` from within `sys.excepthook` (after the prior hook chains) does not re-trigger the same exception in the frozen binary | §Priority/Direct Send | The except-all guard in `send_crash_event_direct()` would catch it, but the crash event would be lost |

---

## Open Questions

1. **`_BASE_PROPS()` lock behavior**
   - What we know: `_BASE_PROPS()` exists in `desktop/telemetry.py`; it includes `_APP_VERSION` and OS info.
   - What's unclear: Does it also read `_identified` or `_current_distinct_id` under `_state_lock`?
   - Recommendation: Read the function before implementing `_emit_crash_direct()`. If it takes a lock,
     create a `_crash_base_props()` that reads only the static module constants.

2. **D-01 spike test result (slot exceptions in frozen build)**
   - What we know: PyQt6 routes slot exceptions to `sys.excepthook` in the documented behavior.
   - What's unclear: Whether this holds in the specific frozen binary on Windows 11 with our PyInstaller version.
   - Recommendation: The planner should add a Wave 0 task: a tiny test app with a QTimer.singleShot raise,
     frozen + run, to verify BEFORE the main implementation wave. The cost is one hour; the alternative
     is discovering the gap post-release.

---

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Python `faulthandler` | CRASH-03 | ✓ | stdlib (Python 3.3+) | — |
| Python `threading.excepthook` | CRASH-02 | ✓ | Python 3.8+ (project min 3.10+) | — |
| Python `sys.excepthook` | CRASH-01 | ✓ | always available | — |
| Python `atexit` | D-08 | ✓ | always available | closeEvent signal flush |
| `shared/posthog_server.py` additions | D-05/D-06 | ✓ (Phase 111 already deployed) | live | — |
| `desktop/telemetry.py` (Phase 111) | all | ✓ (verified line 704 stub) | live | — |
| `Config.INDEX_DIR` writable dir | D-03 dump file | ✓ (verified `genizah_core.py:2342-2364`) | live | — |

**Missing dependencies with no fallback:** none.

---

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest (existing project test suite) |
| Config file | `pytest.ini` or `pyproject.toml` in repo root |
| Quick run command | `pytest tests/test_crash_hooks.py tests/test_crash_payload.py tests/test_native_crash.py tests/test_crash_priority_send.py -x` |
| Full suite command | `pytest tests/ -x --ignore=tests/test_full_gui.py` (PyQt6 headless guard needed: `QT_QPA_PLATFORM=offscreen`) |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File |
|--------|----------|-----------|-------------------|------|
| CRASH-01 | `crash_log.txt` still written after install_exception_hooks() runs | unit | `pytest tests/test_crash_hooks.py::test_prior_hook_chained -x` | Wave 0 |
| CRASH-01 | Telemetry failure in try block does NOT suppress prior hook | unit | `pytest tests/test_crash_hooks.py::test_telemetry_failure_does_not_suppress_chain -x` | Wave 0 |
| CRASH-02 | `threading.excepthook` wrapper fires for `threading.Thread` raise | unit | `pytest tests/test_crash_hooks.py::test_threading_hook_fires_for_thread_raise -x` | Wave 0 |
| CRASH-02 | D-01 matrix: QTimer.singleShot raise → hook fires | unit (dev only; frozen spike is manual) | `pytest tests/test_crash_hooks.py::test_qtimer_slot_raise_reaches_excepthook -x` | Wave 0 |
| CRASH-02 | QThread.run() raise does NOT fire threading.excepthook (documented gap) | unit/doc | `pytest tests/test_crash_hooks.py::test_qthread_gap_documented -x` | Wave 0 |
| CRASH-02 | KeyboardInterrupt excluded from both hooks | unit | `pytest tests/test_crash_hooks.py::test_keyboard_interrupt_excluded -x` | Wave 0 |
| CRASH-02 | SystemExit excluded from both hooks | unit | `pytest tests/test_crash_hooks.py::test_system_exit_excluded -x` | Wave 0 |
| CRASH-04 | Crash payload keys are ONLY in `_ALLOWED_PROPS` | unit | `pytest tests/test_crash_payload.py::test_payload_keys_allowlisted -x` | Wave 0 |
| CRASH-04 | No `message`, `traceback_scrubbed`, `thread_name` in payload | unit | `pytest tests/test_crash_payload.py::test_no_forbidden_keys_in_payload -x` | Wave 0 |
| CRASH-04 | `_make_crash_props` with a frame local containing a file path: no path in output | unit | `pytest tests/test_crash_payload.py::test_no_path_in_crash_props -x` | Wave 0 |
| CRASH-04 | error_module='external' when innermost frame is stdlib | unit | `pytest tests/test_crash_payload.py::test_external_module_fallback -x` | Wave 0 |
| CRASH-04 | `str(exc_value)` is never read (code inspection + AST) | static | `pytest tests/test_crash_payload.py::test_no_str_exc_in_emit_crash -x` | Wave 0 |
| CRASH-05 | Crash hook acquires NO locks — monkeypatch all locks to fail-on-acquire | unit | `pytest tests/test_crash_hooks.py::test_hook_acquires_no_locks -x` | Wave 0 |
| CRASH-05 | Recursion guard: crash inside crash handler does not loop | unit | `pytest tests/test_crash_hooks.py::test_recursion_guard -x` | Wave 0 |
| CRASH-05 | Idempotent install: double install_exception_hooks() does not double-chain | unit | `pytest tests/test_crash_hooks.py::test_idempotent_install -x` | Wave 0 |
| CRASH-06 | Priority/direct send bypasses saturated FIFO queue | unit | `pytest tests/test_crash_priority_send.py::test_crash_send_bypasses_full_queue -x` | Wave 0 |
| CRASH-06 | `send_crash_event_direct` does not touch `_event_queue` | unit | `pytest tests/test_crash_priority_send.py::test_direct_send_does_not_touch_queue -x` | Wave 0 |
| CRASH-07 | Prior native crash + consent True → `desktop_prior_crash` emitted exactly once | unit | `pytest tests/test_native_crash.py::test_prior_crash_emitted_on_consent -x` | Wave 0 |
| CRASH-07 | Prior native crash + consent False → held pending → consent flips True → emitted once | unit | `pytest tests/test_native_crash.py::test_pending_emit_after_consent -x` | Wave 0 |
| CRASH-07 | User never consents → never emitted | unit | `pytest tests/test_native_crash.py::test_no_emit_without_consent -x` | Wave 0 |
| CRASH-07 | Classify each faulthandler prefix → correct enum label | unit | `pytest tests/test_native_crash.py::test_classify_all_prefixes -x` | Wave 0 |
| CRASH-07 | Unknown/unrecognized prefix → `unknown_native` | unit | `pytest tests/test_native_crash.py::test_classify_unknown_maps_to_unknown_native -x` | Wave 0 |
| CRASH-07 | Previous dump READ before `faulthandler.enable()` (ordering) | unit | `pytest tests/test_native_crash.py::test_read_before_enable_ordering -x` | Wave 0 |
| PRIV-03 | AST guard remains green after new crash code added | static | `pytest tests/test_telemetry_no_direct_posthog.py -x` | Existing |

### D-01 Required Test Matrix (dev AND frozen where feasible)

```python
# tests/test_crash_hooks.py — D-01 matrix
# These tests exercise the hook behavior in the DEV (non-frozen) build.
# The frozen-binary verification is a manual spike (Wave 0 task).

def test_qtimer_slot_raise_reaches_excepthook(qtbot, monkeypatch):
    """QTimer.singleShot slot raise → sys.excepthook fires."""
    fired = []
    original = sys.excepthook
    def capture(exc_type, exc_value, exc_tb):
        fired.append(exc_type)
        # do NOT chain to original (test isolation)
    sys.excepthook = capture
    try:
        # Install our hooks (captures the just-set capture as prior_hook)
        telemetry.install_exception_hooks()
        # ... fire a QTimer.singleShot raise; assert fired contains the exc type
    finally:
        sys.excepthook = original

def test_threading_hook_fires_for_thread_raise():
    """threading.Thread raise → threading.excepthook fires."""
    ...

def test_qthread_gap_documented():
    """QThread.run() raise does NOT fire threading.excepthook.
    Documents the known gap (D-01 backstop framing). Asserted as xfail or
    as a documented absence.
    """
    ...
```

### Lock-Acquisition Test (D-05)

```python
# tests/test_crash_hooks.py::test_hook_acquires_no_locks
import threading
from desktop import telemetry

def test_hook_acquires_no_locks(monkeypatch):
    """The crash hook must acquire no locks — D-05 BLOCKER compliance."""
    class _FailLock:
        def acquire(self, *a, **kw): raise AssertionError("lock acquired in crash hook")
        def __enter__(self): self.acquire()
        def __exit__(self, *a): pass
        def release(self): pass

    # Monkeypatch BOTH lock objects the crash path must avoid
    monkeypatch.setattr(telemetry, '_enabled_lock', _FailLock())
    monkeypatch.setattr(telemetry, '_state_lock', _FailLock())

    # Force enabled state without going through the lock
    monkeypatch.setattr(telemetry, '_enabled', True)
    monkeypatch.setattr(telemetry, '_crash_distinct_id', 'test-uuid')

    # Monkeypatch send_crash_event_direct to capture call without network
    sent = []
    from shared import posthog_server as ph
    monkeypatch.setattr(ph, 'send_crash_event_direct',
                        lambda ev, props, did, timeout=0.5: sent.append(ev))

    # This should NOT raise AssertionError from _FailLock
    try:
        telemetry._emit_crash_direct(ValueError, None, is_background=False)
    except AssertionError as e:
        pytest.fail(f"Crash hook acquired a lock: {e}")

    assert len(sent) == 1
    assert sent[0] == 'desktop_crash'
```

### Priority/Direct Send Under Saturated Queue (D-06)

```python
# tests/test_crash_priority_send.py::test_crash_send_bypasses_full_queue
from shared import posthog_server as ph
import queue

def test_crash_send_bypasses_full_queue(monkeypatch):
    """send_crash_event_direct bypasses the FIFO queue — crash event always delivered."""
    # Fill the queue to capacity
    tiny_q = queue.Queue(maxsize=5)
    for i in range(5):
        tiny_q.put_nowait({'event': f'dummy_{i}', 'distinct_id': 'x', 'properties': {}, 'timestamp': ''})
    monkeypatch.setattr(ph, '_event_queue', tiny_q)

    # send_crash_event_direct must not use _event_queue
    posted = []
    import requests
    monkeypatch.setattr(requests, 'post',
                        lambda url, json=None, timeout=None: posted.append(json))
    monkeypatch.setattr(ph, '_api_key_override', 'test_key')

    ph.send_crash_event_direct('desktop_crash', {'exc_type': 'ValueError'}, 'did')

    # Crash event was posted directly — queue is still full (no dequeue)
    assert len(posted) == 1
    assert posted[0]['event'] == 'desktop_crash'
    assert tiny_q.full()  # queue untouched
```

### Sampling Rate

- **Per task commit:** `pytest tests/test_crash_hooks.py tests/test_crash_payload.py tests/test_native_crash.py tests/test_crash_priority_send.py -x`
- **Per wave merge:** Full suite green: `pytest tests/ -x` (with `QT_QPA_PLATFORM=offscreen GITHUB_ACTIONS=true`)
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps

- [ ] `tests/test_crash_hooks.py` — covers CRASH-01/02/05 + D-01 matrix + recursion guard
- [ ] `tests/test_crash_payload.py` — covers CRASH-04 + D-07 frame-walk + allowlist
- [ ] `tests/test_native_crash.py` — covers CRASH-03/07 + D-02/D-03 classify + pending-emit
- [ ] `tests/test_crash_priority_send.py` — covers CRASH-06 + D-05/D-06 direct-send
- [ ] Manual spike task (Wave 0): freeze a minimal `QTimer.singleShot` raise app, run the `.exe`,
     verify `desktop_crash` appears in PostHog or a mock endpoint. Documents D-01 gap if found.

---

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | no | — |
| V3 Session Management | no | — |
| V4 Access Control | no | — |
| V5 Input Validation | yes | `_validate_props()` allowlist + `_scrub_props()` path redaction |
| V6 Cryptography | no | — |

### Known Threat Patterns for Crash Reporting

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| PII leak via exception message (`FileNotFoundError: 'C:\Users\...'`) | Information Disclosure | Never call `str(exc)` — exc_type name only; `_scrub_props()` as defence-in-depth |
| Path in `co_filename` (full path instead of basename) | Information Disclosure | `os.path.basename()` only; `error_module='external'` for non-in-app frames |
| Native dump file transmitted raw | Information Disclosure | Raw dump never transmitted; fixed enum label only (D-02/CRASH-03) |
| Deadlock in crash handler | Denial of Service | Lock-free path via `_is_enabled_nolock()` + `_crash_distinct_id` snapshot (D-05) |
| Crash event dropped by full queue | Denial of Service (data loss) | Priority direct POST bypasses queue (D-06) |
| Crash hook loops on crash inside handler | Denial of Service | Module-level `_in_crash_hook` recursion guard (D-05) |

---

## Project Constraints (from CLAUDE.md)

- **Desktop-only phase:** NiceGUI web app is completely unaffected. All changes in `desktop/telemetry.py`,
  `shared/posthog_server.py` (neutral additions only), `genizah_app.py`.
- **Zero new pip dependencies:** All capabilities from stdlib. No `posthog` SDK. `[VERIFIED: INFRA-04]`
- **`shared/posthog_server.py` must remain UNGATED:** Phase 111 D-04 hard rule. Web/breaker callers
  are unaffected; 5 `_event_queue` monkeypatches must keep passing.
- **PRIV-03 AST guard must remain green:** New crash code lives in `desktop/telemetry.py` only.
  `send_crash_event_direct()` in `shared/posthog_server.py` is transport, not a new desktop caller.
- **Hebrew RTL:** No user-visible strings in this phase (all telemetry is silent). No i18n work.
- **Two PostHog drop counters:** CLAUDE.md notes both `web.api_hardening.get_dropped_event_count()`
  AND `shared.posthog_server.get_dropped_event_count()` must be monitored. Phase 113 adds no third counter.
- **Supabase Data API GRANT rule:** Not applicable (no new tables created in this phase).

---

## Sources

### Primary (HIGH confidence)
- `desktop/telemetry.py` (live source, verified 2026-06-15) — `_enabled`, `_enabled_lock`, `_state_lock`,
  `_ALLOWED_PROPS` at lines 247-266, `_emit()` at lines 495-527, `is_enabled()` at lines 377-387,
  `install_exception_hooks()` stub at line 704, `DesktopEvent.CRASH/PRIOR_CRASH` at lines 106-107
- `shared/posthog_server.py` (live source, verified 2026-06-15) — `enqueue_event` at lines 160-213,
  `_flush_before_exit` at lines 272-311, `_default_distinct_id_lock` at line 67, `_scrub_hook_lock` at line 70,
  queue drop via `put_nowait` at line 208
- `genizah_app.py` (live source, verified 2026-06-15) — `_setup_crash_handler()` at lines 148-170,
  `on_startup_finished()` at lines 3437-3536, `_maybe_show_first_run_prompt()` at line 15856
- `genizah_core.py` (live source, verified 2026-06-15) — `Config.INDEX_DIR` at lines 2342-2364,
  `Config.CONFIG_FILE` at line 2377
- [CPython faulthandler docs](https://docs.python.org/3/library/faulthandler.html) — enable() API, output format
- [CPython test_faulthandler.py](https://github.com/python/cpython/blob/main/Lib/test/test_faulthandler.py) — exact Windows/POSIX first-line strings verified
- [Python threading.excepthook](https://docs.python.org/3/library/threading.html#threading.excepthook) — args shape, Python 3.8+ availability, `threading.__excepthook__` Python 3.10+

### Secondary (MEDIUM confidence)
- [fman.io PyQt excepthook blog](https://fman.io/blog/pyqt-excepthook/) — PyQt5/6 slot exception routing to `sys.excepthook`; cited with PyQt docs cross-reference
- [CPython issue 31701](https://bugs.python.org/issue31701) — Windows fatal exception output format: `"Windows fatal exception: code 0xe..."` prefix

### Tertiary (LOW confidence)
- `[ASSUMED]` behavior of `atexit` in PyInstaller frozen binary on normal `sys.exit()` — requires spike test to confirm

---

## Metadata

**Confidence breakdown:**
- Standard stack / zero-deps posture: HIGH — verified in live code + stdlib
- Code location accuracy: HIGH — all cited line numbers verified against live source (2026-06-15)
- faulthandler prefix strings: HIGH — verified against CPython test suite
- Lock-free GIL semantics: HIGH — CPython data model; bool/str reads atomic under GIL
- Qt slot → sys.excepthook routing in frozen binary: MEDIUM — documented behavior but requires spike

**Research date:** 2026-06-15
**Valid until:** 2026-07-15 (stable stdlib behavior; 30-day window for any PyQt6 point release changes)
