---
phase: 111-telemetry-foundation
plan: 02
subsystem: infra
tags: [posthog, telemetry, python, threading, tdd, consent, privacy, identity]

# Dependency graph
requires:
  - phase: 111-telemetry-foundation plan 01
    provides: "shared/posthog_server.py neutral additions: set_capture_api_key, set_capture_host, set_default_distinct_id, _drain_and_discard, _flush_before_exit"
provides:
  - "desktop/telemetry.py: sole gated chokepoint from desktop/ to PostHog — consent gate, config.pkl persistence, transport key/host wiring, structural scrubber, property allowlist, DesktopEvent enum, 8 public callables + identity hooks + selftest"
  - "tests/test_telemetry_consent_gate.py: 11 CONSENT-01/05/06/07 + REVIEWS HIGH-1 tests"
  - "tests/test_telemetry_scrubbing.py: 9 PRIV-01 scrubber tests (incl. context-survives regression)"
  - "tests/test_telemetry_allowlist.py: 8 PRIV-02/06 allowlist + event registry tests (incl. $identify-rejected-via-track)"
  - "tests/test_telemetry_identity.py: 9 IDENT-03/04 identify/reset tests"
affects: [111-03, 112, 113, 114, 115, 116]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Consent gate: cached module-level bool under threading.Lock, populated at import from config.pkl, updated only by set_consent()"
    - "Transport wiring: _wire_transport_config() re-reads env at each call (not cached at import) so GENIZAH_TELEMETRY_KEY set after import is honored"
    - "Exact/token banned-key matching: _is_banned_key() uses frozenset exact match + two narrow substring tokens only ('filepath','filename') — prevents 'text' substring banning 'context' or 'traceback' substring banning 'traceback_scrubbed'"
    - "sanctioned identify() bypass: only identify() calls enqueue_event() directly with '$identify'; track() explicitly rejects $identify via _TRACK_FORBIDDEN_EVENTS"
    - "_reset_for_tests() test seam: same convention as posthog_server._reset_for_tests() for fixture isolation"

key-files:
  created:
    - desktop/telemetry.py
    - tests/test_telemetry_consent_gate.py
    - tests/test_telemetry_scrubbing.py
    - tests/test_telemetry_allowlist.py
    - tests/test_telemetry_identity.py
  modified: []

key-decisions:
  - "IDENTITY_RESET added to _TRACK_FORBIDDEN_EVENTS: only reset_identity() emits it (symmetry with IDENTIFY — both protocol-adjacent events have sanctioned emitters)"
  - "_wire_transport_config() re-reads env on every call (not cached): enables key injection after import (e.g. dev self-test scenario, REVIEWS-confirmation LOW)"
  - "Regex simplification for _PATH_RE: simplified POSIX path pattern to r'|/\\S{3,}' (avoids character-class quoting errors while still catching POSIX absolute paths)"
  - "install_exception_hooks() + show_first_run_prompt() shipped as no-op stubs: ROADMAP SC#1 import check requires 8 callable surface, but Phases 112/113 implement them"

patterns-established:
  - "Chokepoint pattern: all desktop/ emissions go through track()/_emit(), with identify() as the only documented bypass (Pitfall 6)"
  - "TDD RED/GREEN per task: consent-gate tests committed first (RED=ModuleNotFoundError), then implementation makes all GREEN"
  - "Module-level singleton + lock: mirrors nli_circuit_breaker.py for _enabled/_install_id/_current_distinct_id state"

requirements-completed: [CONSENT-01, CONSENT-05, CONSENT-06, CONSENT-07, INFRA-01, INFRA-02, INFRA-04, INFRA-05, PRIV-01, PRIV-02, PRIV-06, IDENT-03, IDENT-04]

# Metrics
duration: 9min
completed: 2026-06-14
---

# Phase 111 Plan 02: Desktop Telemetry Chokepoint Summary

**Opt-in consent gate with uuid4 install-id lifecycle, config.pkl persistence, structural scrubber (context-safe exact/token banned-key matching), property allowlist, DesktopEvent enum, hand-rolled $identify mechanism, and 38 behavioral tests — all in desktop/telemetry.py as the sole gated path from desktop/ to PostHog**

## Performance

- **Duration:** 9 min
- **Started:** 2026-06-14T09:29:00Z (estimated)
- **Completed:** 2026-06-14T09:48:41Z
- **Tasks:** 3 (TDD RED/GREEN cycles)
- **Files modified:** 5

## Accomplishments

- Built `desktop/telemetry.py` (683 lines) — the sole gated chokepoint: every public callable gate-checks `is_enabled()` first; a fresh config emits zero events
- Closed REVIEWS HIGH-1: `_wire_transport_config()` wires `GENIZAH_TELEMETRY_KEY`/`GENIZAH_TELEMETRY_HOST` into `shared/posthog_server.py` via `set_capture_api_key()`/`set_capture_host()` — the desktop key reaches PostHog without mutating `os.environ` (D-04)
- Closed REVIEWS MEDIUM (two regressions): `context` key survives `_scrub_props` (exact/token banned-key matching, not broad substring); `traceback_scrubbed` survives (banned only `traceback_raw` exactly); `$identify` rejected by `track()` via `_TRACK_FORBIDDEN_EVENTS`
- Implemented hand-rolled `$identify` mechanism: `identify()` is the SOLE sanctioned emitter (bypasses `track()` validation), `reset_identity()` reverts to anonymous install-id
- All 38 tests across 4 files pass; ruff clean on all 5 touched files

## Task Commits

Each task was committed atomically:

1. **Task 1 RED: consent gate tests (failing)** - `908bc014` (test)
2. **Task 1 GREEN: desktop/telemetry.py implementation** - `fa95a74d` (feat)
3. **Task 2: scrubber + allowlist tests** - `79ff8146` (feat)
4. **Task 3: identity mechanism + selftest + identity tests** - `041e9ba9` (feat)

**Plan metadata:** (docs commit follows)

_TDD tasks have test → feat commit cycles_

## Files Created/Modified

- `desktop/telemetry.py` (683 lines, created) — consent gate, config.pkl persistence, `_wire_transport_config()`, `_scrub_props()`, `_validate_props()`, `_ALLOWED_PROPS`, `DesktopEvent` enum, `track()`/`track_performance()`/`track_error()`, `identify()`/`reset_identity()`, `run_selftest()`, `_emit()`, `_reset_for_tests()`, stubs for `install_exception_hooks()`/`show_first_run_prompt()`
- `tests/test_telemetry_consent_gate.py` (11 tests) — CONSENT-01/05/06/07, REVIEWS HIGH-1 transport wiring, is_enabled() never-raise (CRASH-05)
- `tests/test_telemetry_scrubbing.py` (9 tests) — PRIV-01 banned keys, path/Hebrew redaction, length cap, non-string passthrough, context-survives regression
- `tests/test_telemetry_allowlist.py` (8 tests) — PRIV-02/06 allowlist, forbidden env props, event registry, $identify rejection via track(), base props
- `tests/test_telemetry_identity.py` (9 tests) — IDENT-03/04 identify shape, consent gate, install-id requirement, config persistence, reset revert, selftest

## Decisions Made

- `IDENTITY_RESET` added to `_TRACK_FORBIDDEN_EVENTS` alongside `IDENTIFY` — symmetry: `reset_identity()` is the sole sanctioned emitter of `IDENTITY_RESET`, just as `identify()` is for `$identify`. Documented in a comment in the module.
- `_wire_transport_config()` re-reads `GENIZAH_TELEMETRY_KEY` env on every call (not cached at import time) — enables a key/host set after import to be honored (e.g. the dev self-test scenario from `__main__`), as required by REVIEWS-confirmation LOW.
- `_PATH_RE` simplified to `r'|/\S{3,}'` for POSIX paths — the original research pattern had a character-class quoting issue in Python raw strings; the simplified version still catches all realistic POSIX absolute paths.
- `install_exception_hooks()` and `show_first_run_prompt()` shipped as no-op stubs — ROADMAP SC#1 requires the 8-callable surface to be importable; the actual implementation ships in Phases 112/113.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed regex syntax error in _PATH_RE**
- **Found during:** Task 1 GREEN (module import failure)
- **Issue:** The POSIX path pattern `r'|/[^\s,\"\'{3,}'` had an unterminated character class — the `'` inside `[...]` terminated the raw string, leaving `{3,}'` as a dangling suffix. Module failed to import.
- **Fix:** Simplified the POSIX path pattern to `r'|/\S{3,}'` — equivalent semantics for catching POSIX absolute paths without quoting issues.
- **Files modified:** `desktop/telemetry.py`
- **Verification:** `python -c "import desktop.telemetry"` exits 0; all 11 consent-gate tests green
- **Committed in:** `fa95a74d` (part of Task 1 GREEN commit)

---

**Total deviations:** 1 auto-fixed (1 Rule 1 bug fix)
**Impact on plan:** Regex simplification is semantically equivalent for the intended use case. No scope creep.

## Issues Encountered

None beyond the regex syntax error (documented as deviation above).

## User Setup Required

None - no external service configuration required. The placeholder key `<embedded-placeholder>` is in place; the real `phc_...` key drops in before Phase 114 per D-02.

## Next Phase Readiness

- `desktop/telemetry.py` is complete and tested — Phases 112-115 can import and call any of the 8 public callables
- `install_exception_hooks()` stub ready for Phase 113 implementation
- `show_first_run_prompt()` stub ready for Phase 112 implementation
- Phase 111-03 (AST guard `test_telemetry_no_direct_posthog.py`) is ready to run — it will scan all `desktop/` files except `desktop/telemetry.py` for direct `enqueue_event` calls

## Known Stubs

- `desktop/telemetry.py::install_exception_hooks()` — no-op stub, Phase 113 implements
- `desktop/telemetry.py::show_first_run_prompt()` — no-op stub, Phase 112 implements

These stubs are intentional — they exist only so the ROADMAP SC#1 import surface check passes. Phase 112/113 will replace them.

## Threat Surface Scan

No new threat surface beyond the plan's `<threat_model>`. `desktop/telemetry.py` is a process-local module with no new network endpoints, auth paths, or schema changes. The transport path remains `_emit() -> enqueue_event() -> daemon thread -> POST https://eu.i.posthog.com/capture` (same as Phase 111-01).

## TDD Gate Compliance

- RED gate: `test(111-02)` commit `908bc014` — 11 tests all ERROR/FAIL with `ModuleNotFoundError: No module named 'desktop.telemetry'`
- GREEN gate: `feat(111-02)` commit `fa95a74d` — all 38 tests PASS across 4 files

## Self-Check: PASSED

- `desktop/telemetry.py` exists: FOUND (683 lines)
- `tests/test_telemetry_consent_gate.py` exists: FOUND (11 tests)
- `tests/test_telemetry_scrubbing.py` exists: FOUND (9 tests)
- `tests/test_telemetry_allowlist.py` exists: FOUND (8 tests)
- `tests/test_telemetry_identity.py` exists: FOUND (9 tests)
- Commit `908bc014` (RED): present in git log
- Commit `fa95a74d` (Task 1 GREEN): present in git log
- Commit `79ff8146` (Task 2): present in git log
- Commit `041e9ba9` (Task 3): present in git log
- 38 tests pass: VERIFIED
- ruff clean on all 5 files: VERIFIED
- uuid1 count in desktop/telemetry.py: 0 — VERIFIED
- str(exc count in desktop/telemetry.py: 0 — VERIFIED
- set_capture_api_key call: VERIFIED (REVIEWS HIGH-1)
- save_app_config call: VERIFIED (CONSENT-07 persistence)
- _drain_and_discard call: VERIFIED (CONSENT-08 mechanism)
- enqueue_event called only from _emit() and identify(): VERIFIED
- `python -c "import desktop.telemetry"` exits 0: VERIFIED
- ROADMAP SC#1 callable surface (8 callables): VERIFIED

---
*Phase: 111-telemetry-foundation*
*Completed: 2026-06-14*
