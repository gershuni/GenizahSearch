---
phase: 112-consent-ux
plan: "02"
subsystem: desktop-telemetry-ui
tags: [telemetry, consent, startup, qt, wiring]
dependency_graph:
  requires: [desktop/consent_dialog.py (Plan 01), desktop/telemetry.py (Phase 111)]
  provides: [show_first_run_prompt() implementation, GenizahGUI._maybe_show_first_run_prompt]
  affects: [genizah_app.py startup sequence, desktop/telemetry.py public API]
tech_stack:
  added: []
  patterns: [lazy-import-in-function, activeModalWidget-reschedule-guard, citation-chain-pattern, no-raise-contract]
key_files:
  created: []
  modified:
    - desktop/telemetry.py
    - genizah_app.py
decisions:
  - "show_first_run_prompt() accepts optional parent=None kwarg passed from the startup hook (GenizahGUI passes self)"
  - "activeModalWidget() reschedule guard uses 300ms fixed delay and self-reschedules until no modal is up (REVIEWS MED)"
  - "Consent chained directly at end of _show_citation_reminder (strict ordering, no timing guesswork) + else-branch QTimer.singleShot(500, ...) for already-seen installs"
  - "Double-gate: both show_first_run_prompt() engine and _maybe_show_first_run_prompt() UI-helper check FIRST_RUN_SHOWN_KEY independently"
metrics:
  duration: "~8 minutes"
  completed: "2026-06-15"
  tasks: 2
  files: 2
---

# Phase 112 Plan 02: show_first_run_prompt() Wiring Summary

**One-liner:** Filled show_first_run_prompt() stub with FIRST_RUN_SHOWN_KEY gate + lazy ConsentDialog import/exec, and wired GenizahGUI._maybe_show_first_run_prompt() into the startup chain (after citation reminder, with activeModalWidget reschedule guard).

## What Was Built

### `desktop/telemetry.py` — stub filled (show_first_run_prompt)

Replaced the no-op stub body with:
- Reads `cfg = load_app_config()`; returns early if `FIRST_RUN_SHOWN_KEY` is truthy (D-05 gate — never show twice)
- Lazy-import `from desktop.consent_dialog import ConsentDialog` inside the function body (keeps PyQt6 out of module-level imports so headless telemetry tests stay Qt-free)
- Constructs `ConsentDialog(parent)` and calls `.exec()`
- Entire body wrapped in `try/except Exception` with `logger.debug(..., exc_info=True)` on failure (no-raise contract, consistent with module docstring)
- Does NOT write `FIRST_RUN_SHOWN_KEY` or call `set_consent()` — the dialog's single `done()` finalizer (Plan 01) owns those writes on every exit path

### `genizah_app.py` — startup hook wired

**New method `GenizahGUI._maybe_show_first_run_prompt()`:**
- Double-gates on `FIRST_RUN_SHOWN_KEY` (load_app_config check — D-05, defense in depth)
- `QApplication.activeModalWidget()` reschedule guard: if a modal is open, schedules `QTimer.singleShot(300, self._maybe_show_first_run_prompt)` and returns — prevents stacking on Settings, index-missing dialog, recovery modal, or any future sync prompt (REVIEWS MED)
- Calls `show_first_run_prompt(self)` when the coast is clear
- Entire method wrapped in `try/except Exception: pass` (never blocks startup)

**Chaining in `_show_citation_reminder`:**
- Added `self._maybe_show_first_run_prompt()` after `save_app_config({'citation_reminder_seen': True})` — consent fires immediately after the citation modal closes (strict ordering, no timing guesswork)

**Else-branch in `on_startup_finished`:**
- When `citation_reminder_seen` is already True (existing installs), adds `QTimer.singleShot(500, self._maybe_show_first_run_prompt)` in the `else` branch so the consent dialog still appears on first launch for these users
- `_maybe_show_first_run_prompt` token count: **4** (def + citation-chain + else-branch + self-reschedule inside method)

## Test Results

| Command | Result |
|---------|--------|
| `pytest tests/test_telemetry_consent_ux.py -k "gate_skips or constructs_and_execs_once" -x -q` | 2 passed |
| `pytest tests/test_telemetry_consent_gate.py -x -q` | 11 passed |
| `pytest tests/test_telemetry_consent_ux.py -x -q` (full file) | **12 passed** (previously 11 passed + 1 xfail) |
| `pytest tests/test_telemetry_no_direct_posthog.py -x -q` | 6 passed (PRIV-03 still green) |
| `python -c "import ast; ast.parse(...)"` — telemetry.py | parse-ok |
| `python -c "import ast; ast.parse(...)"` — genizah_app.py | parse-ok |
| wiring assertion (count>=3 + activeModalWidget) | wiring-ok, count=4 |

`test_first_run_constructs_and_execs_once` (the REVIEWS MED positive test authored in Plan 01 as an expected-fail) is now GREEN — this was the primary acceptance criterion for Plan 02.

## Deviations from Plan

None — plan executed exactly as written.

## Threat Surface Scan

No new network endpoints, auth paths, file access patterns, or schema changes introduced. Both modified files are pure startup sequencing and desktop-local logic. PRIV-03 AST guard verified green.

## Known Stubs

None in this plan. The `show_first_run_prompt()` stub that was deferred from Plan 01 is now fully implemented.

## Self-Check: PASSED

Files verified:
- `desktop/telemetry.py` — FOUND (modified, show_first_run_prompt filled)
- `genizah_app.py` — FOUND (modified, _maybe_show_first_run_prompt wired)

Commits verified:
- `4c8b8c07` — feat(112-02): implement show_first_run_prompt() — gate + lazy-import ConsentDialog
- `9be1d706` — feat(112-02): wire _maybe_show_first_run_prompt startup hook in genizah_app.py
