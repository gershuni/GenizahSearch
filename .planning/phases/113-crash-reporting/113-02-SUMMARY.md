---
phase: 113-crash-reporting
plan: "02"
subsystem: telemetry/crash-hook-foundation
tags: [telemetry, crash-reporting, lock-free, tdd, posthog, payload-builder]
dependency_graph:
  requires: [Phase 113 Plan 01 send_crash_event_direct transport]
  provides: [_is_enabled_nolock, _emit_crash_direct, _make_crash_props, _crash_distinct_id snapshot, _BASE_PROPS OS props, _ALLOWED_PROPS reconciliation, install_exception_hooks hook capture, _reset_for_tests hook restore]
  affects: [desktop/telemetry.py, tests/test_crash_hooks.py, tests/test_crash_payload.py, tests/test_native_crash.py]
tech_stack:
  added: [platform (stdlib, import-time OS detection)]
  patterns: [lock-free snapshot globals, GIL-atomic bool read, frame-walk payload builder, resolved-source-root in-app classifier, traceback-id dedup]
key_files:
  created: []
  modified:
    - desktop/telemetry.py
    - tests/test_crash_hooks.py
    - tests/test_crash_payload.py
    - tests/test_native_crash.py
decisions:
  - "_emit_crash_direct uses module-top imported send_crash_event_direct (not ph.send_crash_event_direct) — tests must monkeypatch tel.send_crash_event_direct, not ph"
  - "_reset_for_tests uses try/except around _enabled_lock/_state_lock so tests that mock locks with FailLock objects don't fail during fixture teardown"
  - "In-app classifier uses _APP_SOURCE_ROOTS (desktop/ + shared/ only, NOT repo root) + _APP_SOURCE_FILES (exact realpaths) — repo root excluded to avoid misclassifying venv/Lib/site-packages frames"
  - "_EXCLUDED_PATH_SEGMENTS provides defense-in-depth: force 'external' if any path segment matches site-packages/.venv/venv regardless of root classification"
  - "_make_crash_props implemented in Task 1 (required by _emit_crash_direct) — Task 2 tests pass immediately at GREEN, no RED state for payload tests"
metrics:
  duration: "~15 minutes"
  completed: "2026-06-15T12:40:00Z"
  tasks_completed: 2
  files_changed: 4
---

# Phase 113 Plan 02: Lock-Free Crash Emission Foundation Summary

**One-liner:** Lock-free `_emit_crash_direct()` with frame-walked payload builder, OS base props, traceback-id dedup, startup distinct-id snapshot, and hook-restoring test reset — D-05 BLOCKER + REVIEWS HIGH-2/HIGH-3/MEDIUM-8/MEDIUM-9 + PASS2 closed.

## What Was Built

### Task 1: Module-Top Import + Lock-Free Primitives + Snapshot Global (3 Sites) + Recursion Guard + Hook-Restoring Reset

Modified `desktop/telemetry.py`:

1. **`import platform` + `_OS_FAMILY`/`_OS_VERSION` constants** — computed once at import time (lock-free), added to `_BASE_PROPS()` so crash + native events include OS (CRASH-04/SC#3/D-02, REVIEWS PASS2).

2. **`send_crash_event_direct` added to module-top import block** (alongside `enqueue_event`, `set_default_distinct_id`, etc.) — prevents import-lock deadlock in the crash hook path (REVIEWS HIGH-2).

3. **Phase 113 crash-hook globals** added after existing state block:
   - `_crash_distinct_id` — lock-free snapshot of the current distinct_id
   - `_in_crash_hook` — recursion guard (plain bool, GIL-safe)
   - `_hooks_installed` — idempotency guard for `install_exception_hooks()`
   - `_faulthandler_handle` — kept open for process lifetime (Plan 03)
   - `_pending_native_crash` — holds label when prior crash but consent not yet True (Plan 03)
   - `_last_reported_tb_id` — lock-free traceback-id dedup (D-08/REVIEWS PASS2)
   - `_prior_excepthook` / `_prior_threading_hook` — captured at install time for test restoration (REVIEWS MEDIUM-8)

4. **`_BASE_PROPS()` extended** with `'os_family': _OS_FAMILY, 'os_version': _OS_VERSION`. Both keys were already in `_ALLOWED_PROPS`. RESEARCH A1 invariant preserved: only module-level constants, no lock.

5. **`_ALLOWED_PROPS` reconciled** (D-07): removed `'traceback_scrubbed'` + `'thread_name'`; added `'error_fingerprint'`, `'is_background_thread'`, `'fatal_error'`.

6. **`_is_enabled_nolock()`** — returns `_enabled` directly (no lock; GIL-safe; docstring cites D-05/SC#4).

7. **`_emit_crash_direct(exc_type, exc_tb, is_background)`** — lock-free crash emission:
   - Recursion guard on `_in_crash_hook` (return if set; reset in `finally`)
   - Returns early if `_is_enabled_nolock()` is False
   - Traceback-id dedup: if `id(exc_tb) == _last_reported_tb_id`, skip; else record BEFORE sending (D-08/PASS2)
   - `distinct_id = _crash_distinct_id or 'system'`
   - Builds props via `_make_crash_props`, merges `dict(_BASE_PROPS())`, validates, scrubs
   - Calls module-level `send_crash_event_direct` (no `import` statement in body — HIGH-2)
   - Whole body `try/except Exception: pass`

8. **`_crash_distinct_id` wired at THREE sites** (REVIEWS HIGH-3):
   - `set_consent(True)` opt-in branch: after `set_default_distinct_id(distinct_id)`
   - `_set_current_distinct_id()`: after `set_default_distinct_id(distinct_id)`
   - `_load_consent_state()`: inside `if enabled:` after `set_default_distinct_id(distinct_id)` — so persisted-consent users' crashes emit with correct identity before any `set_consent()` call

9. **`install_exception_hooks()` updated** from a stub to capture `_prior_excepthook` / `_prior_threading_hook` before wrapping, and install `sys.excepthook` + `threading.excepthook` wrappers with KeyboardInterrupt/SystemExit exclusion.

10. **`_reset_for_tests()` extended** (REVIEWS MEDIUM-8): resets all Phase 113 globals; restores `sys.excepthook` / `threading.excepthook` from `_prior_excepthook` / `_prior_threading_hook`; uses `try/except` around locked sections so tests that monkeypatch lock objects with FailLock don't fail in teardown.

### Task 2: Frame-Walk Crash Payload Builder + Robust In-App Classification + _ALLOWED_PROPS Reconciliation

Modified `desktop/telemetry.py` (also implemented in Task 1 since `_make_crash_props` is called by `_emit_crash_direct`):

1. **`_APP_SOURCE_ROOTS`** = `(realpath(desktop/), realpath(shared/))` — NOT the repo root (which contains `.venv/` and `venv/`).

2. **`_APP_SOURCE_FILES`** = frozenset of realpaths for `genizah_app.py`, `genizah_core.py`, `gui_threads.py` (top-level app modules matched by exact path).

3. **`_EXCLUDED_PATH_SEGMENTS`** = `('site-packages', os.sep+'.venv'+os.sep, os.sep+'venv'+os.sep, '/.venv/', '/venv/')` — force-external if any present.

4. **`_GENERIC_BASENAMES`** = `frozenset({'__init__.py', '__main__.py'})` — never in-app even if under app roots.

5. **`_is_in_app_frame(co_filename)`** — pure helper resolving path and applying all four checks.

6. **`_make_crash_props(exc_type, exc_tb, is_background)`** — frame-walk via `tb_next`, track innermost in-app frame, fallback to deepest with `exc_module='external'`; returns exactly five keys; never calls `traceback.format_exception` or `str(exc)`.

## Verification Results

```
tests/test_crash_hooks.py — 11 collected; 7 specific tests PASSED
tests/test_crash_payload.py — 14 PASSED
tests/test_native_crash.py — 7 collected; 1 PASSED (HIGH-3), 6 SKIPPED (Plan 03 stubs)
tests/test_crash_priority_send.py — 8 PASSED (no regression)
tests/test_telemetry_no_direct_posthog.py — 6 PASSED (PRIV-03 guard green)
tests/test_telemetry_allowlist.py — 6 PASSED (no regression)
tests/test_telemetry_consent_gate.py — 13 PASSED (no regression)
tests/test_telemetry_posthog_server_ext.py — 18 PASSED (no regression)
ruff check — all clean
```

## TDD Gate Compliance

| Gate | Commit | Status |
|------|--------|--------|
| RED (Task 1) | `77c4da00` — `test(113-02): add failing tests for lock-free primitives, OS props, dedup, hook-reset` | PASS — 7 tests failed (AttributeError: `_crash_distinct_id` not in module) |
| GREEN (Task 1) | `ebea3fb0` — `feat(113-02): lock-free crash primitives + OS props + dedup + hook-restoring reset` | PASS — 7 tests pass |
| Task 2 | `e20dc6fc` — `feat(113-02): frame-walk payload builder tests + _ALLOWED_PROPS reconciliation` | PASS — 14 tests pass (implementation was in Task 1 GREEN) |
| Style | `7e279ec0` — `style(113-02): remove unused imports (ruff F401)` | PASS |

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Extra safety] `_reset_for_tests()` uses try/except around locked sections**
- **Found during:** Task 1 GREEN run
- **Issue:** The test `test_hook_acquires_no_locks` patches `_enabled_lock` with a `_FailLock` that raises `AssertionError` on `acquire()`. The `crash_telemetry_state` fixture teardown calls `_reset_for_tests()` which does `with _enabled_lock:`, causing an `ERROR at teardown` even though the test itself passed.
- **Fix:** Added `try/except Exception` around both `with _enabled_lock:` and `with _state_lock:` blocks in `_reset_for_tests()`, with plain-assignment fallback. Safe since tests are single-threaded.
- **Files modified:** `desktop/telemetry.py`
- **Commit:** `ebea3fb0`

**2. [Rule 1 - Bug] Tests must monkeypatch `tel.send_crash_event_direct`, not `ph.send_crash_event_direct`**
- **Found during:** Task 1 GREEN run (first test run)
- **Issue:** The PATTERNS.md showed `monkeypatch.setattr(ph, 'send_crash_event_direct', ...)`. Since `_emit_crash_direct` uses the module-top imported name (bound in `desktop.telemetry`'s namespace), patching `ph.send_crash_event_direct` has no effect on the function's execution — the local binding in `tel` is unaffected.
- **Fix:** Changed all test monkeypatches to `monkeypatch.setattr(tel, 'send_crash_event_direct', ...)`. Removed unused `import shared.posthog_server as ph` from `test_crash_hooks.py`.
- **Files modified:** `tests/test_crash_hooks.py`
- **Commit:** `ebea3fb0`, `7e279ec0`

**3. [Rule 2 - Missing] `_make_crash_props` implemented in Task 1 (required by `_emit_crash_direct`)**
- **Found during:** Task 1 implementation
- **Issue:** `_emit_crash_direct` calls `_make_crash_props`; the plan placed `_make_crash_props` in Task 2. To avoid a NameError when Task 1's implementation runs, `_make_crash_props` and its supporting constants (`_APP_SOURCE_ROOTS`, etc.) were implemented alongside Task 1. Task 2's tests therefore passed immediately (no RED state for the payload tests specifically).
- **Files modified:** `desktop/telemetry.py`
- **Commit:** `ebea3fb0`

## Known Stubs

The `install_exception_hooks()` now installs `sys.excepthook` and `threading.excepthook` wrappers (Plan 02 scope), but the `faulthandler` setup and `atexit` registration are explicitly deferred to Plan 03 with comments. These are documented stubs (not blocking Plan 02's goal):
- `# Phase 113 Plan 03 will add: _setup_faulthandler() + atexit.register(_atexit_flush)`

## Threat Flags

No new network endpoints, auth paths, file access patterns, or schema changes introduced. All changes are internal to `desktop/telemetry.py` (the existing PRIV-03 chokepoint).

## Self-Check: PASSED

Files exist:
- `C:\Genizahsearch\desktop\telemetry.py` — FOUND (contains `def _is_enabled_nolock`, `def _emit_crash_direct`, `def _make_crash_props`, `_crash_distinct_id`, `_OS_FAMILY`, `_OS_VERSION`, `send_crash_event_direct` in module-top import)
- `C:\Genizahsearch\tests\test_crash_hooks.py` — FOUND (contains `def test_hook_acquires_no_locks`, `def test_reset_for_tests_restores_hooks`)
- `C:\Genizahsearch\tests\test_crash_payload.py` — FOUND (contains `def test_generic_basename_not_in_app`, `def test_venv_frame_external`)
- `C:\Genizahsearch\tests\test_native_crash.py` — FOUND (contains `def test_persisted_consent_populates_crash_distinct_id`)

Commits verified:
- `77c4da00` — RED tests (Task 1)
- `ebea3fb0` — GREEN implementation (Task 1 + 2 foundation)
- `e20dc6fc` — Task 2 payload tests
- `7e279ec0` — ruff style fixes
