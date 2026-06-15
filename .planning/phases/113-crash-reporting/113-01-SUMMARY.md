---
phase: 113-crash-reporting
plan: "01"
subsystem: telemetry/crash-transport
tags: [telemetry, crash-reporting, posthog, lock-free, tdd]
dependency_graph:
  requires: [Phase 111 posthog_server neutral additions, Phase 112 consent UX]
  provides: [send_crash_event_direct, crash snapshot globals, Wave 0 test scaffolds]
  affects: [shared/posthog_server.py, tests/conftest.py]
tech_stack:
  added: []
  patterns: [lock-free snapshot globals, TDD RED/GREEN, opt-in fixture pattern]
key_files:
  created:
    - tests/test_crash_hooks.py
    - tests/test_crash_payload.py
    - tests/test_native_crash.py
    - tests/test_crash_priority_send.py
  modified:
    - shared/posthog_server.py
    - tests/conftest.py
decisions:
  - "Snapshot globals (_crash_api_key_snapshot / _crash_capture_url_snapshot) are plain str module globals — written under _capture_config_lock by existing setters, read without any lock in crash path (CPython GIL guarantees atomic str read)"
  - "Static test uses ast.unparse on function body (docstring stripped) to avoid false positives from forbidden symbols appearing only in comments/docstrings"
  - "Stub test files import only pytest (no unused tel/ph imports) to satisfy ruff F401; Plan 02/03 will add needed imports back when filling stubs"
  - "test_snapshot_globals_reset_to_empty_by_reset added (not in original plan list) for coverage completeness — accepted as scope-adjacent correctness test"
metrics:
  duration: "~5 minutes"
  completed: "2026-06-15T09:25:56Z"
  tasks_completed: 2
  files_changed: 6
---

# Phase 113 Plan 01: Lock-Free Crash Transport + Wave 0 Scaffold Summary

**One-liner:** Lock-free `send_crash_event_direct()` priority/direct POST with plain snapshot globals, plus Wave 0 test scaffold (32 named tests) for all Phase 113 crash requirements.

## What Was Built

### Task 1: Wave 0 Test Scaffolds + Crash-LOCAL Reset Fixture

Added an opt-in `crash_telemetry_state` fixture to `tests/conftest.py` — NOT autouse (REVIEWS MEDIUM-5). Each crash test file opts in via a local `@pytest.fixture(autouse=True) def _use(crash_telemetry_state): yield` wrapper scoped only to that file.

Created four test files with 32 named stubs covering all Phase 113 requirements:
- `tests/test_crash_hooks.py` — 11 stubs (CRASH-01/02/05): prior hook chain, threading.excepthook, KI/SystemExit exclusion, lock-free body, recursion guard, idempotent install, reset
- `tests/test_crash_payload.py` — 6 stubs (CRASH-04): allowlist keys, no forbidden keys, no paths, external fallback, no str(exc), generic basename
- `tests/test_native_crash.py` — 7 stubs (CRASH-03/07): emit on consent, pending emit, no emit, classify all prefixes, unknown native, read-before-enable ordering, persisted consent
- `tests/test_crash_priority_send.py` — 8 live tests (CRASH-06/D-05): queue bypass, queue untouched, snapshot globals, no-key-no-post, payload shape, never-raises, lock-free static

All 32 collect cleanly; the 24 stubs skip; the 8 live tests pass.

### Task 2: Lock-Free `send_crash_event_direct()` + Snapshot Globals (TDD RED/GREEN)

Modified `shared/posthog_server.py`:

1. **Two new plain module globals** after `_capture_config_lock`:
   - `_crash_api_key_snapshot: str = ''`
   - `_crash_capture_url_snapshot: str = ''`

2. **`set_capture_api_key()` extended**: inside the existing `with _capture_config_lock:` block, also writes `_crash_api_key_snapshot = (key or os.environ.get('POSTHOG_API_KEY', '')).strip()` — mirrors the `_resolve_api_key` formula.

3. **`set_capture_host()` extended**: inside its existing lock block, also writes `_crash_capture_url_snapshot = f"{(host or POSTHOG_HOST).rstrip('/')}/capture"` — mirrors `_resolve_capture_url`.

4. **`_reset_for_tests()` extended**: clears both snapshot globals to `''` (no lock needed — plain assignment).

5. **New `send_crash_event_direct(event, properties, distinct_id, timeout=0.5)`**: reads `api_key = _crash_api_key_snapshot` and `url = _crash_capture_url_snapshot` as plain global reads (NO lock, NO `_resolve_*` call); returns early if no key/url; builds payload dict; makes ONE `requests.post`. Whole body wrapped `try/except Exception: pass`. Does NOT touch `_event_queue`, `_default_distinct_id_lock`, `_scrub_hook_lock`, or `_capture_config_lock`.

6. **`__all__` extended** with `'send_crash_event_direct'`.

## Verification Results

```
tests/test_crash_priority_send.py — 8 passed (lock-free + queue-bypass assertions)
tests/test_telemetry_posthog_server_ext.py — 18 passed (NEUTRAL — 5 _event_queue monkeypatches still green)
tests/test_telemetry_no_direct_posthog.py — 6 passed (PRIV-03 AST guard still green)
All 4 crash scaffold files — 32 collected, 24 skipped, 8 passed
ruff check — all clean
```

## TDD Gate Compliance

| Gate | Commit | Status |
|------|--------|--------|
| RED | `3121d98e` — `test(113-01): add failing tests...` | PASS — all 8 tests failed with AttributeError |
| GREEN | `01571f2d` — `feat(113-01): lock-free send_crash_event_direct...` | PASS — all 8 tests pass |
| REFACTOR | (not needed — implementation was clean) | N/A |

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Static test used raw `inspect.getsource` — false positive from docstring**
- **Found during:** Task 2 GREEN run
- **Issue:** `test_direct_send_lock_free_static` used `inspect.getsource(ph.send_crash_event_direct)` and checked for absence of `_resolve_api_key` / `_resolve_capture_url`. The function's docstring legitimately mentions these names to explain what the function does NOT do — causing the assertion to fail.
- **Fix:** Updated the test to use `ast.parse` + `ast.unparse` on the function body after stripping the leading docstring node, so only actual code (not doc comments) is checked.
- **Files modified:** `tests/test_crash_priority_send.py`
- **Commit:** `01571f2d`

**2. [Rule 2 - Extra coverage] Added `test_snapshot_globals_reset_to_empty_by_reset`**
- **Found during:** Task 2 implementation
- **Issue:** The plan listed 7 named stubs + the main `test_crash_send_bypasses_full_queue` but did not include an explicit test for `_reset_for_tests()` clearing the new snapshot globals. The acceptance criteria required `_reset_for_tests` to clear them.
- **Fix:** Added `test_snapshot_globals_reset_to_empty_by_reset` as the 8th live test (8 total instead of 7).
- **Files modified:** `tests/test_crash_priority_send.py`
- **Commit:** `01571f2d`

**3. [Rule 1 - Bug] Stub files had unused imports (ruff F401)**
- **Found during:** Task 2 ruff check
- **Issue:** `test_crash_hooks.py`, `test_crash_payload.py`, `test_native_crash.py` imported `tel` and `ph` that Plan 02/03 will use but are unused in pure-stub files.
- **Fix:** Removed unused imports from the three stub files; Plan 02/03 will add them back when filling implementations.
- **Files modified:** `tests/test_crash_hooks.py`, `tests/test_crash_payload.py`, `tests/test_native_crash.py`
- **Commit:** `01571f2d`

## Threat Flags

None — this plan adds no new network endpoints, auth paths, or schema changes at trust boundaries. The `send_crash_event_direct` function accesses the same PostHog EU endpoint as `_flush_before_exit` (already in the threat model as T-113-06-* entries in the plan).

## Known Stubs

The three plan-02/03 scaffold files contain intentional stubs (`pytest.skip("filled by plan 02/03")`). These are NOT blocking — they are the Wave 0 Nyquist scaffold whose purpose is to list verify targets for Plans 02 and 03. They are not features missing from Plan 01's goal.

## Self-Check: PASSED

Files created/exist:
- `shared/posthog_server.py` — FOUND (contains `def send_crash_event_direct`, `_crash_api_key_snapshot`, `_crash_capture_url_snapshot`)
- `tests/test_crash_priority_send.py` — FOUND (contains `def test_crash_send_bypasses_full_queue`)
- `tests/test_crash_hooks.py` — FOUND (contains `def test_reset_for_tests_restores_hooks`)
- `tests/test_crash_payload.py` — FOUND (contains `def test_generic_basename_not_in_app`)
- `tests/test_native_crash.py` — FOUND (contains `def test_persisted_consent_populates_crash_distinct_id`)
- `tests/conftest.py` — FOUND (contains `def crash_telemetry_state`, NOT autouse)

Commits verified:
- `677886e3` — Wave 0 scaffold Task 1
- `3121d98e` — RED tests Task 2
- `01571f2d` — GREEN implementation Task 2
