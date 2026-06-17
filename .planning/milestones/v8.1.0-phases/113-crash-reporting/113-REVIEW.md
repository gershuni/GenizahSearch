---
phase: 113-crash-reporting
reviewed: 2026-06-15T10:01:13Z
depth: standard
files_reviewed: 8
files_reviewed_list:
  - desktop/telemetry.py
  - genizah_app.py
  - shared/posthog_server.py
  - tests/conftest.py
  - tests/test_crash_hooks.py
  - tests/test_crash_payload.py
  - tests/test_crash_priority_send.py
  - tests/test_native_crash.py
findings:
  critical: 0
  warning: 4
  info: 3
  total: 8
critical_resolved: 1
status: issues_found
resolution_note: "CR-01 fixed in 0db270a3 (carve-out + full-path regression test)."
---

# Phase 113: Code Review Report

**Reviewed:** 2026-06-15T10:01:13Z
**Depth:** standard
**Files Reviewed:** 8
**Status:** issues_found

## Summary

Phase 113 adds lock-free crash reporting to the PyQt6 desktop app: a direct-send
transport (`shared/posthog_server.send_crash_event_direct`), crash-emission
primitives + payload builder (`desktop/telemetry._emit_crash_direct`,
`_make_crash_props`, native-crash classifier), and chained sys/threading
excepthooks + faulthandler wiring (`genizah_app.py`, `install_exception_hooks`).

The **privacy/safety invariants are well-implemented and hold up under tracing:**

- **Lock-free crash path:** verified. `_emit_crash_direct` reads `_enabled`,
  `_crash_distinct_id`, `_in_crash_hook`, `_last_reported_tb_id` as plain globals;
  `_BASE_PROPS()` reads only constants; `send_crash_event_direct` reads
  `_crash_*_snapshot` globals without locks. The FailLock tests pin this.
- **No message/traceback text leaks:** `exc_value` is passed ONLY to the prior
  crash-log writer (local file), never to telemetry. `_make_crash_props` reads
  only `co_filename` (basename) + `tb_lineno` — never `f_locals`, never
  `str(exc)`, never `format_exception`. Verified by trace and AST tests.
- **Prior hook always chained:** `_telemetry_excepthook`/`_telemetry_threading_hook`
  call the prior hook UNCONDITIONALLY after a guarded telemetry step. Verified.
- **No new pip deps; reuses posthog_server.** Confirmed.
- The path scrubber (Windows/UNC/POSIX/bare-filename) and Hebrew scrubber both
  work correctly against the real module (initial regex concern was a shell
  escaping artifact, retested against the live module — sound).

**However**, there is one BLOCKER that defeats the feature's core purpose: the
generic path scrubber redacts the crash payload's own `exc_module` and
`error_fingerprint`, so every in-app crash arrives at PostHog as
`[REDACTED]:lineno`. This is privacy-safe (over-redaction) but renders the crash
data useless for grouping/diagnosis, and it is not covered by any test (all
payload tests assert on `_make_crash_props` output BEFORE the scrubber runs).

## Critical Issues

### CR-01: Path scrubber destroys `exc_module` and `error_fingerprint` in every in-app crash payload

> **✅ RESOLVED (0db270a3):** Verified live (`exc_module: 'genizah_app.py' → '[REDACTED]'`, `error_fingerprint → '[REDACTED]:42'`). Fixed via Option A — `_emit_crash_direct` restores `exc_module`/`error_fingerprint` from the pre-scrub `validated` dict after `_scrub_props` (mirrors the `context` carve-out in `_emit`; both keys are trusted: basename + `type:basename:lineno`, never a path). Added `test_emit_crash_direct_preserves_inapp_module_and_fingerprint` driving the full emit path (post-scrub assertion).


**File:** `desktop/telemetry.py:838-936` (`_make_crash_props` produces a `.py`
basename; `_emit_crash_direct` then runs it through `_scrub_props` at line 932)

**Issue:** `_make_crash_props` deliberately emits the source-file **basename**
(e.g. `genizah_app.py`) as `exc_module`, and builds
`error_fingerprint = "{exc_type}:{exc_module}:{exc_lineno}"`
(e.g. `ValueError:genizah_app.py:42`). `_emit_crash_direct` then passes the merged
payload through `_scrub_props` → `_scrub_value`, whose bare-filename branch
`\S+\.[A-Za-z]\w{0,7}\b` matches any `*.py` token and replaces it with
`[REDACTED]`.

Verified against the live module — the payload that actually reaches
`send_crash_event_direct` for an in-app crash is:

```
exc_module:         '[REDACTED]'
error_fingerprint:  '[REDACTED]:1'
```

Consequences:
- Every in-app crash collapses to the same fingerprint shape `[REDACTED]:<lineno>`,
  destroying crash grouping (the stated value of `error_fingerprint`, D-07).
- `exc_module` — the field whose comment says "transmit only the basename" — is
  always wiped for the useful (in-app) case. Ironically, `external` frames
  survive (no dot), so only the diagnostically-valuable rows are destroyed.
- **Untested:** all `exc_module`/`error_fingerprint` assertions in
  `tests/test_crash_payload.py` (lines 60, 67, 118, 151, 169, 194, 209, 232, 246)
  check the output of `_make_crash_props` DIRECTLY, before scrubbing. The only
  test that captures the scrubbed payload (`test_base_props_includes_os`,
  `tests/test_crash_hooks.py:188`) asserts only `os_family`/`os_version`. So this
  defect ships green.

**Fix:** Do not run the already-fixed-enum crash keys through the generic value
scrubber. The crash keys are produced by trusted code (a basename + a fixed
fingerprint), so they should bypass `_scrub_value`. Two options:

Option A — preserve the trusted crash keys after scrubbing (mirrors the existing
`context` carve-out in `_emit`):

```python
# in _emit_crash_direct, after scrubbed = _scrub_props(validated)
for _k in ('exc_module', 'error_fingerprint'):
    if _k in validated:
        scrubbed[_k] = validated[_k]   # trusted: basename / fixed fingerprint
```

Option B — make `_scrub_value` skip a small set of trusted-key names (pass the key
into the scrub, or scrub the payload key-aware). Whichever is chosen, add a test
that drives the FULL `_emit_crash_direct` path with a real in-app traceback and
asserts the captured `send_crash_event_direct` props still carry
`exc_module == 'fake_app_module.py'` and an intact `error_fingerprint`.

## Warnings

### WR-01: Recursion guard `_in_crash_hook` is a shared global, not per-thread — drops a concurrent crash

**File:** `desktop/telemetry.py:102, 912-940`

**Issue:** `_in_crash_hook` is a single module global used as the re-entrancy
guard. The threading excepthook and the sys excepthook both call
`_emit_crash_direct`, which sets `_in_crash_hook = True` for the duration of the
hook — including the synchronous `send_crash_event_direct` POST (up to ~0.5s).
If a background thread crashes and is mid-POST while the main thread also crashes,
the main-thread hook sees `_in_crash_hook is True` and returns early (line 913),
silently dropping the main-thread crash event. The comment ("plain bool is
GIL-safe for single-thread re-entrancy") acknowledges single-thread intent but
the guard is process-global and the hooks are explicitly multi-thread (CRASH-02).

This is crash-report data loss, not a privacy issue, and requires two
near-simultaneous crashes — but the 0.5s POST window widens the race
meaningfully (e.g. a thread crash followed by a UI crash during a network stall).

**Fix:** Use a `threading.local()` flag for the re-entrancy guard so each thread
guards only its own re-entry, while still allowing distinct threads to each emit
once:

```python
import threading
_crash_local = threading.local()
...
if getattr(_crash_local, 'in_hook', False):
    return
_crash_local.in_hook = True
try:
    ...
finally:
    _crash_local.in_hook = False
```

(The dedup via `_last_reported_tb_id` already prevents double-emit of the *same*
traceback, so per-thread guarding does not reintroduce duplicates.)

### WR-02: `id(exc_tb)` dedup can silently drop an unrelated later crash (address reuse)

**File:** `desktop/telemetry.py:922-926`

**Issue:** The dedup stores `id(exc_tb)` (the memory address of the traceback
object) in `_last_reported_tb_id`. Once the reported traceback is garbage
collected, CPython can allocate a *new, unrelated* traceback object at the same
address, yielding the same `id()`. If that new crash's `exc_tb` happens to reuse
the recorded address, `_emit_crash_direct` treats it as a duplicate and returns
without emitting — a real (if low-probability) crash-report loss. `id()`-based
identity caches are a known footgun precisely because of address reuse after GC.

**Fix:** Dedup on a value that is stable for the *logical* crash rather than the
object address. Reasonable options: dedup on the computed `error_fingerprint`
(type:module:lineno) for a short window, or keep a small recent-set rather than a
single last-id, or pair the id with a generation counter. At minimum, document
that the dedup is best-effort and may both miss duplicates and drop a coincident
distinct crash.

### WR-03: Desktop crash events fall back to the web `POSTHOG_API_KEY` when the embedded key is the placeholder

**File:** `desktop/telemetry.py:364-370` (`_wire_transport_config`),
`shared/posthog_server.py:143` (snapshot mirror), `:168` (`_resolve_api_key`)

**Issue:** `_wire_transport_config` treats the placeholder key as `None` (WR-05
intent: drop locally). It then calls `set_capture_api_key(None)`, but
`set_capture_api_key` mirrors the snapshot as
`_crash_api_key_snapshot = (None or os.environ.get('POSTHOG_API_KEY','')).strip()`.
So on any desktop machine where `POSTHOG_API_KEY` is set in the environment (e.g.
a developer who also runs the web app, or an inherited shell var), the crash path
will POST desktop crash events using the **web** project's key — sending
desktop telemetry into the web PostHog project. The milestone explicitly intends
a separate desktop PostHog project (project memory). Consent still gates the
emit, so this is not an unconsented-leak, but it is wrong-project routing and the
placeholder→env fallback is non-obvious.

**Fix:** For the desktop transport, do not silently inherit `POSTHOG_API_KEY`.
Either gate the crash snapshot on a desktop-specific key only (ignore
`POSTHOG_API_KEY` on the desktop path), or have `_wire_transport_config` pass an
explicit sentinel that prevents the env fallback. At minimum, document the
precedence and verify `POSTHOG_API_KEY` is never present in the packaged desktop
runtime.

### WR-04: `_hooks_installed` is set True before wrapping completes — a mid-install failure leaves hooks partially installed with no retry

**File:** `desktop/telemetry.py:1106` (set True) vs `1114-1175` (actual wrapping)

**Issue:** `_hooks_installed = True` is set at the top of the try block, before
any of the hook assignments, `_setup_faulthandler()`, or `atexit.register`. If
any step between 1114 and 1175 raised, the outer `except` (line 1177) swallows
it, but `_hooks_installed` stays `True`, so a subsequent `install_exception_hooks()`
returns immediately (line 1104) and the partially-installed state can never be
repaired. In practice the wrapping statements are simple assignments and
`_setup_faulthandler` has its own try/except, so this is unlikely to trigger
today — but it is fragile against future edits.

**Fix:** Set `_hooks_installed = True` only after all wrapping has succeeded (move
it just before the closing of the try block), or guard idempotency on a separate
flag set at the end. Keep the early-return idempotency check at the top reading
the same flag.

## Info

### IN-01: `_reset_for_tests` does not close/reset `_faulthandler_handle`

**File:** `desktop/telemetry.py:1207-1255`

**Issue:** `_reset_for_tests` resets the Phase 113 globals but never closes or
clears `_faulthandler_handle`. A test (or repeated prod `_setup_faulthandler`)
that opens the dump file leaks the file handle across resets. The native-crash
tests sidestep this by monkeypatching `_faulthandler_handle = None`, so the suite
is unaffected, but the reset seam is incomplete.

**Fix:** In `_reset_for_tests`, attempt `_faulthandler_handle.close()` in a
try/except and set `_faulthandler_handle = None`.

### IN-02: "int/float divide by zero" classified as `abort`

**File:** `desktop/telemetry.py:952-953`

**Issue:** `'windows fatal exception: int divide by zero'` and
`'...float divide by zero'` both map to the `'abort'` enum label, which is
semantically inaccurate (a divide-by-zero is not an abort). Not a correctness bug
— the label is a fixed safe enum — but it muddies crash-tracking analytics by
folding divide-by-zero into the abort bucket.

**Fix:** Add a dedicated label (e.g. `'arithmetic_error'`) to
`_NATIVE_CRASH_LABELS` and the documented enum set if divide-by-zero distinction
matters for triage; otherwise leave as-is and note it intentionally.

### IN-03: `_classify_native_crash` reads `text.splitlines()[0]` after a non-empty `strip()` guard — safe but worth a defensive note

**File:** `desktop/telemetry.py:970-972`

**Issue:** The guard `if not text or not text.strip()` protects against empty/blank
input, after which `text.splitlines()[0]` is taken. This is safe because a
non-blank string always yields at least one splitline element. No bug — flagged
only because indexing `[0]` after a transform is a pattern that warrants a comment
so a future edit to the guard does not introduce an IndexError.

**Fix:** None required; optionally add `lines = text.splitlines(); first =
lines[0] if lines else ''` for defensiveness.

---

_Reviewed: 2026-06-15T10:01:13Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
