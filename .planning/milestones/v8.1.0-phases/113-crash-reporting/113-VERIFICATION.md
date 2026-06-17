---
phase: 113-crash-reporting
verified: 2026-06-15T16:00:00Z
status: human_needed
score: 7/7
overrides_applied: 0
human_verification:
  - test: "Frozen-binary Qt slot exception fires sys.excepthook + writes crash_log.txt + delivers desktop_crash event"
    expected: "In the packaged .exe, raising inside a QTimer.singleShot slot causes (1) crash_log.txt to be written and (2) a desktop_crash event to reach PostHog (or a mock endpoint). crash_log.txt must be written even when telemetry fails."
    why_human: "Requires a built PyInstaller .exe; frozen Qt slot behavior cannot be reproduced deterministically under pytest headless offscreen. The pytest test test_qtimer_slot_raise_reaches_excepthook passes in the dev build but frozen-binary behavior differs (PyQt6 routing in packaged executables is unconfirmed)."
  - test: "Real native C-extension crash produces faulthandler dump + next-launch desktop_prior_crash"
    expected: "Forcing a native crash (e.g. ctypes null-deref or Tantivy SIGSEGV), relaunching with consent=True results in exactly one desktop_prior_crash event with a fixed-enum fatal_error value (no paths, no frames, no raw dump text)."
    why_human: "Real segfaults cannot be triggered deterministically in-process. Requires a manual crash-then-relaunch cycle with PostHog capture enabled."
---

# Phase 113: Crash Reporting Verification Report

**Phase Goal:** Uncaught exceptions on any thread are captured, scrubbed, and enqueued non-blockingly before the existing crash-log handler runs; faulthandler captures native C-extension crashes to a local file; a bounded synchronous flush delivers the crash event before process exit; next-launch detection re-emits native crash signals after consent is confirmed.
**Verified:** 2026-06-15T16:00:00Z
**Status:** human_needed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Uncaught exceptions captured via sys.excepthook AND threading.excepthook, chaining to the existing crash_log.txt writer even when telemetry fails | VERIFIED | `install_exception_hooks()` wraps both hooks; `_telemetry_excepthook` calls `prior_sys_hook(...)` UNCONDITIONALLY after `try/except`; `test_prior_hook_chained` + `test_telemetry_failure_does_not_suppress_chain` pass |
| 2 | Crash payloads contain only exception type, basename + lineno, app version, OS — no message strings, no paths, no frame locals | VERIFIED | `_make_crash_props` walks traceback via `tb_next`/`tb_frame.f_code.co_filename` (basename only); never calls `format_exception` or `str(exc)`; CR-01 carve-out restores `exc_module`/`error_fingerprint` post-scrub; `test_no_str_exc_in_emit_crash`, `test_no_path_in_crash_props`, `test_no_forbidden_keys_in_payload`, `test_emit_crash_direct_preserves_inapp_module_and_fingerprint` all pass |
| 3 | Hook body is lock-free and non-blocking — no network I/O in the hook itself, no lock acquisition, consent read from cached global | VERIFIED | `_emit_crash_direct` reads `_enabled` (GIL-atomic bool), `_crash_distinct_id` (plain global snapshot); `send_crash_event_direct` reads `_crash_*_snapshot` globals without locks; `test_hook_acquires_no_locks` passes with FailLock on both `_enabled_lock` and `_state_lock` |
| 4 | A bounded synchronous flush delivers the crash event before process exit; the crash event is prioritized over a saturated queue | VERIFIED | `send_crash_event_direct` POSTs directly (0.5s timeout), bypassing `_event_queue` entirely; `test_crash_send_bypasses_full_queue` confirms queue stays full; `test_direct_send_lock_free_static` confirms no lock-taking symbols in executable code |
| 5 | faulthandler captures native C-extension crashes to a local file (Config.INDEX_DIR/faulthandler_dump.txt); raw dump never transmitted | VERIFIED | `_setup_faulthandler` reads prior dump BEFORE enabling, classifies to fixed-enum label (`_classify_native_crash`), calls `faulthandler.enable(file=_faulthandler_handle, all_threads=True)`; `test_read_before_enable_ordering` passes |
| 6 | Next-launch detection re-emits a native crash signal exactly once after consent; pending when consent deferred, never emitted if never consented | VERIFIED | `_setup_faulthandler` sets `_pending_native_crash = label` when consent=False; `set_consent(True)` calls `_emit_pending_native_crash()` which clears before emitting; `test_pending_emit_after_consent`, `test_no_emit_without_consent` pass |
| 7 | All events consent-gated; no event of any kind emitted before consent; no new pip dependencies | VERIFIED | `_is_enabled_nolock()` gates `_emit_crash_direct` and `_emit_native_crash`; `_enabled=False` after reset means zero POSTs; no `posthog` SDK installed; only stdlib (`faulthandler`, `atexit`, `platform`) added |

**Score:** 7/7 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `shared/posthog_server.py` | lock-free crash snapshot globals + `send_crash_event_direct()` | VERIFIED | `_crash_api_key_snapshot`, `_crash_capture_url_snapshot` present as plain globals; `send_crash_event_direct` exported in `__all__`; 8 tests pass |
| `desktop/telemetry.py` | `_is_enabled_nolock`, `_emit_crash_direct`, `_make_crash_props`, `install_exception_hooks` body, faulthandler helpers | VERIFIED | All 6 functions present and fully implemented; no stubs; 43 crash-related tests pass |
| `genizah_app.py` | `install_exception_hooks()` called after `_setup_crash_handler()` | VERIFIED | Lines 172-179: best-effort try/except block; string-index confirms `install_exception_hooks` at char 8342 > `_setup_crash_handler()` at char 7268 |
| `tests/test_crash_priority_send.py` | CRASH-06 direct-send + lock-free assertions | VERIFIED | 8 tests pass |
| `tests/test_crash_payload.py` | CRASH-04 payload builder + allowlist | VERIFIED | 14 tests pass (incl. CR-01 regression test) |
| `tests/test_crash_hooks.py` | CRASH-01/02/05 hook chain + lock-free | VERIFIED | 19 passed + 1 xpassed |
| `tests/test_native_crash.py` | CRASH-03/07 faulthandler + next-launch | VERIFIED | 8 tests pass |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `genizah_app.py` (line ~172) | `desktop.telemetry.install_exception_hooks` | `try: from desktop import telemetry; _telemetry.install_exception_hooks()` | WIRED | Placement is after `_setup_crash_handler()` call; best-effort try/except confirmed |
| `desktop/telemetry.py` module-top | `shared.posthog_server.send_crash_event_direct` | `from shared.posthog_server import ... send_crash_event_direct` at line ~51 | WIRED | Module-top import confirmed (REVIEWS HIGH-2 fix); `test_no_in_function_import` + `test_send_crash_event_direct_imported_at_module_top` pass |
| `_emit_crash_direct` | `_prior_excepthook` (crash-log writer) | unconditional chain in `_telemetry_excepthook` | WIRED | `prior_sys_hook(exc_type, exc_value, exc_tb)` called after `try/except` block; verified by `test_prior_hook_chained` |
| `set_consent(True)` | `_emit_pending_native_crash()` | direct call after `_crash_distinct_id` populated | WIRED | Grep confirmed `_emit_pending_native_crash()` in `set_consent` opt-in branch |
| `_load_consent_state` | `_crash_distinct_id` | `_crash_distinct_id = distinct_id` inside `if enabled:` block | WIRED | REVIEWS HIGH-3 fix confirmed; `test_persisted_consent_populates_crash_distinct_id` passes |
| `set_capture_api_key` / `set_capture_host` | `_crash_api_key_snapshot` / `_crash_capture_url_snapshot` | mirror writes inside existing lock blocks | WIRED | Both setters write snapshots under `_capture_config_lock`; read without lock in crash path |

### Data-Flow Trace (Level 4)

Not applicable — this phase produces event emission infrastructure (hooks, transport), not UI rendering components with state.

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| send_crash_event_direct bypasses full queue | `pytest tests/test_crash_priority_send.py::test_crash_send_bypasses_full_queue` | PASS | PASS |
| Lock-free: no lock symbols in crash path executable code | `ast.unparse` on `send_crash_event_direct` + `_emit_crash_direct` bodies | All 4 forbidden symbols absent | PASS |
| No str(exc) or format_exception in crash emission functions | Source inspection of `_emit_crash_direct`, `_make_crash_props`, `_emit_native_crash` | Absent in executable code; docstring mention only | PASS |
| Consent gate prevents emission | `_emit_crash_direct` with `_enabled=False` | Zero POSTs confirmed | PASS |
| Prior hook chained despite telemetry failure | `test_telemetry_failure_does_not_suppress_chain` | PASS | PASS |
| PRIV-03 AST guard unbroken | `pytest tests/test_telemetry_no_direct_posthog.py` | 6 passed | PASS |
| 5 existing `_event_queue` monkeypatches neutral | `pytest tests/test_telemetry_posthog_server_ext.py` | 18 passed | PASS |
| Full phase test suite | All 4 crash test files | 50 passed, 1 xpassed | PASS |

### Probe Execution

No probe scripts declared for this phase. Step 7c: SKIPPED (no `probe-*.sh` files in phase directory).

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|---------|
| CRASH-01 | 113-03 | sys.excepthook chained, never replacing crash_log.txt writer | SATISFIED | `install_exception_hooks` wraps `_setup_crash_handler`; try/finally chain unconditional |
| CRASH-02 | 113-03 | threading.excepthook + QApplication.notify for worker exceptions | SATISFIED | `threading.excepthook` installed; Qt slot coverage via `sys.excepthook` (D-01 resolution); `test_threading_hook_fires_for_thread_raise` + `test_qtimer_slot_raise_reaches_excepthook` pass |
| CRASH-03 | 113-03 | faulthandler captures native crashes to local file only | SATISFIED | `_setup_faulthandler` writes to `Config.INDEX_DIR/faulthandler_dump.txt`; raw dump never transmitted; `test_read_before_enable_ordering` pass |
| CRASH-04 | 113-02 | Crash events: exc type, scrubbed location, app version, OS only — no frame locals, no message strings, no paths | SATISFIED | `_make_crash_props` frame-walk; `_ALLOWED_PROPS` reconciled; CR-01 carve-out preserves `exc_module`/`error_fingerprint`; 14 payload tests pass |
| CRASH-05 | 113-02 | Hooks non-blocking, re-entrancy-safe, consent-cached | SATISFIED | `_is_enabled_nolock()` (GIL-safe bool); `_in_crash_hook` recursion guard; module-top import (no import lock); `test_hook_acquires_no_locks`, `test_recursion_guard` pass |
| CRASH-06 | 113-01 | Bounded synchronous flush delivers crash event before exit | SATISFIED | `send_crash_event_direct` (direct POST, 0.5s, bypasses queue); atexit `_flush_before_exit(1.5)` for clean exits; `test_crash_send_bypasses_full_queue` pass; SC#5 reconciliation documented in code |
| CRASH-07 | 113-03 | Next-launch native crash detection + emit after consent | SATISFIED | `_setup_faulthandler` read-before-enable; pending hold; `set_consent` triggers `_emit_pending_native_crash`; `test_pending_emit_after_consent`, `test_no_emit_without_consent` pass |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| None found | — | — | — | — |

Ruff check on all 7 phase-modified files: clean. No TODO/FIXME/TBD/XXX debt markers in modified files. No stub patterns (`return null`, `return {}`, empty handlers). No hardcoded empty data flowing to user-visible output.

**CR-01 fix verified:** `_emit_crash_direct` lines 940-942 restore `exc_module` and `error_fingerprint` from the pre-scrub `validated` dict after `_scrub_props`, mirroring the `context` carve-out in `_emit()`. The regression test `test_emit_crash_direct_preserves_inapp_module_and_fingerprint` drives the full post-scrub path and asserts `exc_module == 'fake_app_module.py'` and fingerprint starts with `'ValueError:fake_app_module.py:'`.

**Advisory warnings (WR-01 through WR-04) — none undermine a success criterion:**

- **WR-01** (`_in_crash_hook` shared-global, not per-thread): Can cause a concurrent second-thread crash to be silently dropped during a 0.5s POST window. This is crash-data loss at the edge, not a privacy issue and not a chain-suppression. SC#1 (chain always runs) is unaffected. Acceptable advisory.
- **WR-02** (`id(exc_tb)` address-reuse dedup): Low-probability address reuse could drop a coincident distinct crash. Same: data loss edge case, not a SC violation. Acceptable advisory.
- **WR-03** (placeholder key falls back to `POSTHOG_API_KEY` env var): The desktop `_TELEMETRY_KEY_DEFAULT` placeholder causes `set_capture_api_key(None)` which would inherit `POSTHOG_API_KEY` from env. In the current environment `POSTHOG_API_KEY` is not set, so this has no practical effect. Consent gate still applies in all cases — this is wrong-project routing when a real key eventually lands, not a PII leak. No SC violation. Advisory.
- **WR-04** (`_hooks_installed = True` before wrapping completes): Fragile against future edits but wrapping statements are simple assignments unlikely to raise. No SC violation today. Advisory.

### Human Verification Required

#### 1. Frozen-binary Qt Slot Exception

**Test:** Build the desktop .exe (PyInstaller), raise an exception inside a `QTimer.singleShot` slot (e.g. via a debug menu action), confirm crash_log.txt is written AND a `desktop_crash` event reaches PostHog (or a local mock PostHog endpoint with a real telemetry key and consent=True).

**Expected:** (1) `crash_log.txt` written with traceback; (2) exactly one `desktop_crash` event in PostHog with `exc_type`, `exc_module` (a basename), `error_fingerprint`, `os_family`, `os_version` — no paths, no message text.

**Why human:** The pytest test `test_qtimer_slot_raise_reaches_excepthook` passes in the offscreen dev build, but frozen PyInstaller binaries with PyQt6 on Windows route Qt exceptions differently. This cannot be verified programmatically without a built executable.

#### 2. Real Native C-Extension Crash

**Test:** Force a native crash (e.g. `ctypes.string_at(0)` or trigger a real Tantivy SIGSEGV), then relaunch the app with a consented user; confirm exactly one `desktop_prior_crash` event arrives in PostHog with a fixed-enum `fatal_error` value.

**Expected:** `fatal_error` is one of `{segmentation_fault, access_violation, abort, stack_overflow, unknown_native}`; no raw dump text, no paths, no frames. The event fires exactly once (subsequent launches emit nothing). The faulthandler dump file is truncated (memory-only pending semantics).

**Why human:** Real segfaults cannot be triggered deterministically in-process. Requires a manual crash-then-relaunch cycle.

### Gaps Summary

No automated gaps identified. All 7 CRASH requirements are satisfied in code, all 51 tests pass (50 passed, 1 xpassed), PRIV-03 AST guard holds, ruff is clean, and the CR-01 BLOCKER from the code review is fixed with a regression test. Two human verification items remain for scenarios requiring a packaged binary or a real native crash.

---

_Verified: 2026-06-15T16:00:00Z_
_Verifier: Claude (gsd-verifier)_
