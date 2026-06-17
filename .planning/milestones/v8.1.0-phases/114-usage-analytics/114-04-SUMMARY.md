---
phase: 114-usage-analytics
plan: 04
subsystem: desktop-telemetry
tags: [telemetry, gap-closure, codex-review, thread-safety, ghost-events, session-end]
dependency_graph:
  requires: [Phase 114-01 identity/session, Phase 114-02 search/tab producers, Phase 114-03 feature-opened/heartbeat]
  provides: [CR-114-01..06 Codex findings closed, per-run PGP-tag token guard, cancelled-emit correctness in reset paths, session_end orphan prevention, restore-ghost-event suppression]
  affects: [genizah_app.py, tests/test_telemetry_phase114.py]
tech_stack:
  added: []
  patterns:
    - per-run monotonic token (_pgp_tag_run_seq) + active-token compare for stale-slot guard
    - previous-worker drain+disconnect before new run-object install
    - mirror stop_search pattern into _reset_search (_search_was_cancelled=True + emit cancelled)
    - mirror on_comp_scan_finished pattern into _reset_composition (emit cancelled in isRunning branch)
    - _telemetry_ready() AND truthy _session_id gate on session_end (WR-01 analog)
    - emit_telemetry=True default param on open_join_workbench for restore-suppression
    - _set_active_tab for all programmatic tab switches (extended to comp-resume)
key_files:
  created: []
  modified: [genizah_app.py, tests/test_telemetry_phase114.py]
decisions:
  - Per-run token uses a monotonic int counter (_pgp_tag_run_seq) rather than uuid4 — simpler, no import needed, sufficient uniqueness for the single-threaded UI case (CR-114-01)
  - Token stored on the run object as run['token'] and compared to _pgp_tag_active_token — stale slots that find a token mismatch return early WITHOUT marking emitted, so the live completion can still fire (CR-114-01)
  - _reset_search cancel emit placed INSIDE the isRunning() branch so a no-active-search reset does not fabricate an event (CR-114-02)
  - _reset_composition cancel emit likewise placed INSIDE the isRunning() branch (CR-114-03)
  - session_end gate condition uses _telemetry_ready() AND getattr(_session_id,'') — both required so an empty session_id also suppresses (CR-114-04)
  - _session_end_emitted is set ONLY after both gate conditions pass — a suppressed close doesn't consume the once-guard (CR-114-04)
  - emit_telemetry=True default on open_join_workbench so all existing callers (corner_joins_btn, no-arg launcher) are unchanged; only the restore closure passes False (CR-114-05)
  - Existing _FakeGui.closeEvent_telemetry_part updated to mirror the real gated block; two existing session_end tests updated to set _telemetry_session_started=True (consistency with CR-114-04 fix)
metrics:
  duration: ~7min
  completed_date: "2026-06-16"
  tasks: 3
  files: 2
---

# Phase 114 Plan 04: CR-114-01..06 Gap Closure Summary

Closed all six Codex cross-AI code-review findings against the Phase 114 desktop telemetry producers. These were count-accuracy / event-gating / thread-safety / ghost-event defects. Each fix is surgical (`genizah_app.py` only) with a dedicated regression test proving the defect closed.

## What Was Built

**Task 1: CR-114-01 + CR-114-02**

`genizah_app.py` — `_execute_tag_search` + `_emit_pgp_tag_search_telemetry` + `_reset_search`:

- **CR-114-01 (PGP-tag thread-safety):** `_execute_tag_search` now: (a) drains the previous worker with `wait()` + `finished.disconnect()` BEFORE installing the new run object; (b) mints a per-run token via `_pgp_tag_run_seq` counter; (c) records it as `_pgp_tag_active_token`. `_emit_pgp_tag_search_telemetry` compares `run['token']` to `_pgp_tag_active_token` and returns early on mismatch — a stale queued slot can no longer mark the new run emitted or suppress the live completion. D-04 preserved (tag text never in props).
- **CR-114-02 (regular-search reset cancelled emit):** `_reset_search` now mirrors `stop_search`: inside the `isRunning()` branch it sets `_search_was_cancelled = True` then calls `_emit_search_telemetry('cancelled')` after teardown. The per-run `emitted` guard prevents double-emit; D-08 preserved (no `result_count_bucket` on cancelled). A no-active-search reset does not fabricate an event.

**Task 2: CR-114-03 + CR-114-04**

`genizah_app.py` — `_reset_composition` + `closeEvent`:

- **CR-114-03 (composition reset cancelled emit):** `_reset_composition` now calls `_emit_comp_search_telemetry('cancelled')` inside the `isRunning()` cancel/terminate branch. The per-run `emitted` flag prevents double-emit if cooperative `on_comp_scan_finished` also fires. D-08 preserved (no `result_count_bucket` on cancelled).
- **CR-114-04 (session_end orphan prevention):** `closeEvent` `SESSION_END` block now gated on `self._telemetry_ready()` AND `getattr(self, '_session_id', '')` being truthy — the WR-01 analog for session_end. An app close before the 700ms startup coordinator runs never emits an orphan `session_id=''` event. `_session_end_emitted` is only set after both conditions pass.

`tests/test_telemetry_phase114.py` — `_FakeGui.closeEvent_telemetry_part` updated to mirror the real gated block (adds `_telemetry_ready()` method); two existing `test_session_end_*` tests updated to set `_telemetry_session_started=True` so they remain valid.

**Task 3: CR-114-05 + CR-114-06**

`genizah_app.py` — `open_join_workbench` + `_restore_session`:

- **CR-114-05 (restore ghost joins_lab feature_opened):** `open_join_workbench` gains `emit_telemetry: bool = True` keyword param; `_emit_feature_opened(feature_name='joins_lab')` gated on `if emit_telemetry`. `_restore_join_lab` deferred closure now calls `self.open_join_workbench(emit_telemetry=False)`. All existing user-gesture callers unchanged (default `True`).
- **CR-114-06 (interrupted-comp resume ghost tab_activated):** Interrupted-composition resume now calls `self._set_active_tab(self.composition_tab)` instead of bare `self.tabs.setCurrentWidget(self.composition_tab)`. `_set_active_tab` sets `_programmatic_tab_change=True` so `_on_tab_changed` suppresses the emission.

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1 (GREEN) | ad7d85c3 | CR-114-01+02: PGP-tag run-token guard + _reset_search cancelled emit |
| 2 (GREEN) | 199e6ac1 | CR-114-03+04: comp-reset cancelled emit + session_end gate |
| 3 (GREEN) | bde5616c | CR-114-05+06: restore-suppress joins_lab + programmatic comp-resume tab |

## Verification

- `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_phase114.py -q` — **89 passed** (76 pre-plan + 13 new regressions: 6 for Task 1, 9 for Task 2, 4 for Task 3, minus 6 that were already in the prior 76 baseline)
- `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_phase114.py tests/test_no_dynamic_telemetry_strings.py -q` — **94 passed** (D-17 AST guard still green)
- Source assertions all confirmed:
  - `grep -n "_pgp_tag_active_token|'token'" genizah_app.py` → token install + compare in `_execute_tag_search` / `_emit_pgp_tag_search_telemetry`
  - `grep -n "finished.disconnect" genizah_app.py` → previous-worker disconnect at line 19103, precedes run-object assignment at line 19115 (char 598 vs 1604 in the function)
  - `grep -n "_search_was_cancelled = True" genizah_app.py` → lines 17649 (stop_search) + 17691 (_reset_search)
  - `grep -n "_emit_search_telemetry\('cancelled'\)" genizah_app.py` → lines 17659 (stop_search) + 17701 (_reset_search)
  - `grep -n "_emit_comp_search_telemetry\('cancelled'\)" genizah_app.py` → line 22711 (_reset_composition) + 23173 (on_comp_scan_finished)
  - `_telemetry_ready()` appears inside `closeEvent` session_end block (line 26902)
  - `grep -n "open_join_workbench(emit_telemetry=False)" genizah_app.py` → line 26821 (_restore_join_lab)
  - `grep -n "_set_active_tab(self.composition_tab)" genizah_app.py` → line 26884 (interrupted-comp resume)
  - `grep -n "self.tabs.setCurrentWidget(self.composition_tab)" genizah_app.py` → 0 matches in _restore_session
- `python -m ruff check genizah_app.py tests/test_telemetry_phase114.py` — all checks passed

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Existing _FakeGui.closeEvent_telemetry_part was missing the gate that CR-114-04 adds**
- **Found during:** Task 2 GREEN — the two existing `test_session_end_*` tests used `_FakeGui` which lacked `_telemetry_ready()` and didn't set `_telemetry_session_started=True`, so if we naively updated the real code they would break.
- **Fix:** Updated `_FakeGui.closeEvent_telemetry_part` to mirror the real gated block (added `_telemetry_ready()` method to `_FakeGui`); updated `test_session_end_fires_on_close` and `test_session_end_exactly_once_guard` to set `_telemetry_session_started=True`. The `_FakeGuiWithReadyGate` subclass added for CR-114-04 tests was kept as a cleaner dedicated fixture.
- **Files modified:** `tests/test_telemetry_phase114.py`
- **Commit:** 199e6ac1

## Known Stubs

None — all six fixes are fully wired. No placeholder or TODO items introduced.

## Threat Flags

None — all six CR-114-* mitigations from the plan's threat register are implemented:
- T-114-G1 (PGP stale slot misattribution): per-run token guard + previous-worker drain/disconnect
- T-114-G2 (regular-search reset dropped/phantom): _reset_search mirrors stop_search
- T-114-G3 (comp-reset lost cancelled): _reset_composition cancel branch emits
- T-114-G4 (orphan session_end): _telemetry_ready() AND truthy _session_id gate
- T-114-G5 (ghost joins_lab feature_opened): emit_telemetry=False on restore path
- T-114-G6 (ghost tab_activated on comp-resume): _set_active_tab replaces bare setCurrentWidget
- T-114-G7 (D-17 AST guard): no forbidden accessor introduced; AST guard still green

## Self-Check: PASSED

- genizah_app.py: FOUND
- tests/test_telemetry_phase114.py: FOUND
- 114-04-SUMMARY.md: FOUND
- Commit ad7d85c3 (Task 1): FOUND
- Commit 199e6ac1 (Task 2): FOUND
- Commit bde5616c (Task 3): FOUND
