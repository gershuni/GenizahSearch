---
phase: 114-usage-analytics
plan: 02
subsystem: desktop-telemetry
tags: [telemetry, usage-analytics, posthog, desktop, tab-activated, search-executed]
dependency_graph:
  requires: [Phase 114-01 identity/session foundation, _telemetry_ready gate, _app_shutting_down flag]
  provides: [desktop_tab_activated producer, desktop_search_executed producer (regular + PGP-tags + composition)]
  affects: [genizah_app.py, tests/test_telemetry_phase114.py]
tech_stack:
  added: []
  patterns:
    - programmatic tab change guard (_programmatic_tab_change flag + _set_active_tab helper)
    - per-run state object with emitted idempotency guard (prevents double-emission and shutdown emission)
    - module-level _telemetry_result_bucket (avoids AttributeError on SimpleNamespace stubs in tests)
    - TDD RED/GREEN per task (5 task pairs, 10 commits)
key_files:
  created: []
  modified: [genizah_app.py, tests/test_telemetry_phase114.py]
decisions:
  - _telemetry_result_bucket is module-level (not class static) — SimpleNamespace test stubs cannot access class static methods; module-level avoids AttributeError being silently swallowed by try/except (D-07/D-08)
  - corpus_scope reads currentData() not currentText() — returns fixed code ('genizah'/'local'/'all'), immune to i18n label changes (D-04/D-05)
  - _app_shutting_down guard is the FIRST line in all three emit helpers (_emit_search_telemetry, _emit_pgp_tag_search_telemetry, _emit_comp_search_telemetry) — prevents shutdown-race emission (REVIEWS HIGH-2)
  - PGP-tags path has its own per-run object and emit helper to keep the tag argument away from any props (REVIEWS HIGH-1; tag text NEVER in telemetry)
  - drain thread isolation added to autouse fixture (_start_drain_thread_once patched to no-op) — prevents daemon thread consuming test events from the patched queue before assertions run
metrics:
  duration: ~35min
  completed_date: "2026-06-15"
  tasks: 5
  files: 2
---

# Phase 114 Plan 02: Usage Analytics Producers (Tab + Search) Summary

Two high-fidelity usage producers wired into the desktop app: `desktop_tab_activated` on every user tab switch (programmatic changes suppressed via `_programmatic_tab_change` flag) and `desktop_search_executed` exactly once per user-initiated run (completed or cancelled, never on app shutdown) across all three search dispatch paths: regular SearchThread, PGP-Tags `_execute_tag_search`, and CompositionThread.

## What Was Built

**Task 1 — Tab activated producer**

`genizah_app.py`:
- `_programmatic_tab_change = False` in `__init__`
- `_set_active_tab(target)` helper wraps all programmatic `setCurrentWidget`/`setCurrentIndex` calls on `self.tabs` with `_programmatic_tab_change = True` inside a `try/finally`
- Telemetry block at top of `_on_tab_changed`: guarded by `_telemetry_ready()` AND `not _restoring_session` AND `not _programmatic_tab_change`. Uses `_TAB_NAME_MAP = {0:'search', 1:'composition', 2:'browse_shelfmark', 3:'browse_catalog', 4:'lists', 5:'community', 6:'my_library'}` — hardcoded constants, never `tabText()`.

**Task 2 — Regular search: per-run state + emit helper**

`genizah_app.py`:
- Module-level `_telemetry_result_bucket(count) -> str` placed just before `class GenizahGUI` — returns `'0'`, `'1-9'`, `'10-99'`, or `'100+'`
- `_SEARCH_MODE_ENUM` hardcoded dict `{0:'keyword', 1:'variants', 2:'responsa', 3:'fuzzy', 4:'regex', 5:'title', 6:'shelfmark', 7:'pgp_tags'}` built in `start_search`
- `self._current_search_run = {'mode': ..., 'corpus': ..., 'emitted': False}` per-run object minted in `start_search`; mode gets `lab_` prefix when lab toggle checked; corpus from `currentData()` only
- `_emit_search_telemetry(action, result_count=None)`: first-guard `_app_shutting_down`, then `_telemetry_ready()`, then per-run `emitted` idempotency guard, then `telemetry.track(SEARCH_EXECUTED, ...)`; `result_count_bucket` included only for `action='completed'`

**Task 3 — Regular search: wiring into stop_search + on_search_finished**

Three call sites:
- `stop_search`: calls `self._emit_search_telemetry('cancelled')` after setting cancel flags
- `on_search_finished` empty-results branch: calls `self._emit_search_telemetry('cancelled' if was_cancelled else 'completed', 0)`
- `on_search_finished` non-empty path (end): calls `self._emit_search_telemetry('cancelled' if was_cancelled else 'completed', len(results))`

**Task 4 — PGP-tags dispatch path**

`genizah_app.py`:
- `self._current_pgp_tag_search_run = {'mode': 'pgp_tags', 'corpus': 'genizah', 'emitted': False}` minted in `_execute_tag_search` (before queue submission)
- `_emit_pgp_tag_search_telemetry(action, result_count=None)`: same structure as `_emit_search_telemetry` but reads `_current_pgp_tag_search_run`; mode is always `'pgp_tags'`; the `tag` argument from `_on_tag_search_results(self, tag, results)` is never touched
- Wired into all three outcome branches of `_on_tag_search_results`: cancelled-before-results, zero-results, and non-empty-results

**Task 5 — Composition/parallels dispatch path**

`genizah_app.py`:
- `_COMP_SEARCH_MODE_ENUM = {0:'comp_exact', 1:'comp_variants', 2:'comp_fuzzy'}` in `run_composition`
- `self._current_comp_search_run = {'mode': ..., 'corpus': _comp_scope, 'emitted': False}` per-run object; `lab_` prefix when `btn_lab_mode_toggle_comp` checked; corpus from `_comp_scope` (fixed code, already computed above the emit point)
- `_emit_comp_search_telemetry(action, result_count=None)`: same guard structure; first-guard `_app_shutting_down`; covers the parallels path because parallels seeds the composition tab via `send_result_to_composition` → `run_composition`
- Wired into `on_comp_scan_finished` for both completed and partial (cancelled) outcomes

**tests/test_telemetry_phase114.py** — extended from 14 tests (Plan 01) to 47 tests (Plan 02), adding 33 tests across 5 task groups:
- Task 1 (5 tests): tab emit fires, suppressed during restore, suppressed for programmatic change, unknown tab index ignored, _telemetry_ready gate
- Task 2 (6 tests): emit helper shutdown guard, telemetry_ready gate, cancelled has no bucket, completed has bucket, bucket edge cases (0/1/9/10/99/100), idempotency (second call skipped)
- Task 3 (5 tests): stop_search wires emit, on_search_finished empty cancelled, on_search_finished empty completed, on_search_finished non-empty cancellation, non-empty completion
- Task 4 (6 tests): PGP tag search per-run state, emit fires, shutdown guard, tag text never in props (privacy), zero-result PGP, cancellation before results
- Task 5 (11 tests): comp run state minting, lab prefix, emit helper fires completed, emit helper fires cancelled, bucket present on completed, no bucket on cancelled, _app_shutting_down guard, _telemetry_ready gate, idempotency, parallels path wired via run_composition, corpus scope from _comp_scope

Autouse fixture extended with `monkeypatch.setattr(ph, '_start_drain_thread_once', lambda: None)` to prevent the PostHog drain daemon thread from consuming events from `fresh_q` before test assertions run.

## Commits

| Task | RED Commit | GREEN Commit | Description |
|------|-----------|-------------|-------------|
| 1 | b6a19e93 | 1f408170 | Tab activated + _programmatic_tab_change + _set_active_tab |
| 2 | bca60619 | 67978989 | _emit_search_telemetry + per-run state + _telemetry_result_bucket |
| 3 | 8aa11908 | 9c2bf184 | Wire into stop_search + on_search_finished |
| 4 | 6d390811 | 3254bdb2 | PGP-tags per-run state + _emit_pgp_tag_search_telemetry |
| 5 | aeaebbed | 568dcf92 | Composition per-run state + _emit_comp_search_telemetry |

## Verification

- `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_phase114.py -q` — 47 passed
- `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_phase114.py tests/test_telemetry_identity.py tests/test_telemetry_consent_gate.py -q` — 67 passed (no regressions)
- `grep -n "currentText" genizah_app.py | grep -i "emit\|mode\|corpus"` — zero matches in emit functions (D-04/D-05 enforced)
- `_app_shutting_down` confirmed as first guard in all three emit helpers (lines 17503, 18929, 22661)
- `_telemetry_result_bucket` confirmed module-level before class (line 3278 vs class at 3293)
- `python -m ruff check genizah_app.py tests/test_telemetry_phase114.py` — all checks passed

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] _telemetry_result_bucket moved from class static method to module-level function**
- **Found during:** Task 2 GREEN implementation
- **Issue:** `_emit_search_telemetry` calls `self._result_count_bucket(result_count)`. When the method is a class `@staticmethod` and the test uses a `SimpleNamespace` stub (not a real `GenizahGUI` instance), `self._result_count_bucket` raises `AttributeError`, which is silently swallowed by the surrounding `try/except Exception` — meaning no event is emitted and the test fails with 0 events in queue.
- **Fix:** Renamed to `_telemetry_result_bucket` and placed as a module-level function just before `class GenizahGUI`. All three emit helpers call it as `_telemetry_result_bucket(result_count)` (no `self.`). This also makes the function testable in isolation without a GUI instance.
- **Files modified:** `genizah_app.py`
- **Commit:** 67978989

**2. [Rule 1 - Bug] Drain thread isolation added to autouse fixture**
- **Found during:** Task 1 GREEN test run
- **Issue:** The PostHog `_start_drain_thread_once` starts a daemon thread on the first `enqueue_event()` call. After the autouse fixture monkeypatches `_event_queue` to `fresh_q`, the daemon thread reads `_event_queue` as a module global and sees `fresh_q` — consuming events before the test assertion checks `qsize()`. This caused intermittent zero-event failures even with correct implementation.
- **Fix:** Added `monkeypatch.setattr(ph, '_start_drain_thread_once', lambda: None)` to the **autouse** `_reset_telemetry_state` fixture so no new drain threads start during any test in the file. The per-stub version in `_make_tab_gui_stub` was redundant and removed.
- **Files modified:** `tests/test_telemetry_phase114.py`
- **Commit:** 1f408170

## Known Stubs

None — all five producers are fully wired. The `_setup_active_ping()` stub from Plan 01 is unchanged (Plan 03 adds the real implementation).

## Threat Flags

All T-114-05 through T-114-09 mitigations implemented:
- T-114-05 (tab name PII): hardcoded `_TAB_NAME_MAP` dict, never `tabText()` — verified by grep
- T-114-06 (search content PII): `_emit_search_telemetry` props contain only `search_mode`, `corpus_scope`, `action`, `result_count_bucket` — never query text; tag text never touches any prop (`_on_tag_search_results` `tag` argument is fully isolated from emit helper)
- T-114-07 (double emit): per-run `emitted` flag + `_app_shutting_down` first-guard closes all paths; 47 tests cover idempotency
- T-114-08 (pre-consent emit): `_telemetry_ready()` gate in all three helpers; tests cover gate
- T-114-09 (corpus_scope i18n leak): `currentData()` used in all corpus reads, never `currentText()`

## Self-Check: PASSED

- genizah_app.py: FOUND
- tests/test_telemetry_phase114.py: FOUND
- 114-02-SUMMARY.md: FOUND
- Commit b6a19e93 (Task 1 RED): FOUND
- Commit 1f408170 (Task 1 GREEN): FOUND
- Commit bca60619 (Task 2 RED): FOUND
- Commit 67978989 (Task 2 GREEN): FOUND
- Commit 8aa11908 (Task 3 RED): FOUND
- Commit 9c2bf184 (Task 3 GREEN): FOUND
- Commit 6d390811 (Task 4 RED): FOUND
- Commit 3254bdb2 (Task 4 GREEN): FOUND
- Commit aeaebbed (Task 5 RED): FOUND
- Commit 568dcf92 (Task 5 GREEN): FOUND
