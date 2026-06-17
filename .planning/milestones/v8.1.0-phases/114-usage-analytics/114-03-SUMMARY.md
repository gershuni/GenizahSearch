---
phase: 114-usage-analytics
plan: 03
subsystem: desktop-telemetry
tags: [telemetry, feature-opened, heartbeat, posthog, desktop, ast-guard]
dependency_graph:
  requires: [Phase 114-01 identity/session foundation, Phase 114-02 tab+search producers, _telemetry_ready gate]
  provides: [desktop_feature_opened producers (all D-03 surfaces), desktop_active_ping heartbeat, D-17 AST guard]
  affects: [genizah_app.py, desktop/result_dialog.py, tests/test_telemetry_phase114.py, tests/test_no_dynamic_telemetry_strings.py]
tech_stack:
  added: []
  patterns:
    - centralized _emit_feature_opened() helper (single gate+try/except for all D-03 surfaces)
    - class-level static fmt→action map (_EXPORT_ACTION_BY_FMT) for export events
    - QTimer (5-min) + applicationStateChanged focus/resume heartbeat (not a naive 24h timer)
    - telemetry-argument-scoped AST guard (REVIEWS HIGH-3 re-scope from whole-FunctionDef)
    - identify() callsite AST check (REVIEWS LOW-10)
key_files:
  created: [tests/test_no_dynamic_telemetry_strings.py]
  modified: [genizah_app.py, desktop/result_dialog.py, tests/test_telemetry_phase114.py]
decisions:
  - _emit_feature_opened() centralized on GenizahGUI — single gate+try/except for all D-03 feature opens
  - _EXPORT_ACTION_BY_FMT class attribute (not inside function) — static map accessible via inspect in tests
  - open_joins_workbench branches on source in ('visual','combined') for VS vs joins_lab (mutually exclusive, T-114-13)
  - Both _open_puzzle_window AND add_to_puzzle emit fragment_puzzle (REVIEWS MEDIUM-7) — single user gesture hits only one
  - Both Browse-tab (_show_fjms_catalog_dialog) AND ResultDialog (_show_rd_catalog) emit fjms_catalog (REVIEWS MEDIUM-6) — two distinct surfaces
  - export_comp_report emit placed AFTER no-data early-return and BEFORE save dialog (REVIEWS MEDIUM-8)
  - action='export_*' placed AFTER if-not-path-return in both export functions (no count on cancel)
  - ResultDialog.__init__ emits result_detail directly (not via _emit_feature_opened) — no _telemetry_ready in ResultDialog
  - _show_rd_catalog emits fjms_catalog directly — same reason, parent._session_id fallback to ''
  - AST guard re-scoped to telemetry-call argument subtrees (REVIEWS HIGH-3) — prevents false-positive on export_results/on_search_finished
metrics:
  duration: ~30min
  completed_date: "2026-06-15"
  tasks: 3
  files: 4
---

# Phase 114 Plan 03: Feature Opened Producers + Heartbeat + AST Guard Summary

Completed the Phase 114 producer set: `desktop_feature_opened` for all locked D-03 surfaces, the focus-aware daily active-user heartbeat (`desktop_active_ping`), and the D-17 producer-layer AST guard re-scoped to telemetry-call argument expressions.

## What Was Built

**genizah_app.py** — Five new methods + six producer wiring sites:

1. `_emit_feature_opened(self, *, feature_name=None, dialog_name=None, action=None)` — centralized helper. FIRST guard: `_telemetry_ready()` (REVIEWS MEDIUM-9). Then try/except best-effort. Builds props from only the non-None kwargs + `session_id`. Calls `telemetry.track(FEATURE_OPENED, **props)`. All argument values are hardcoded constants (D-04).

2. `_EXPORT_ACTION_BY_FMT = {'xlsx': 'export_xlsx', 'csv': 'export_csv', 'txt': 'export_txt', 'docx': 'export_docx'}` — class-level static map for export action values (D-04: never from QFileDialog/selectedFiles).

3. `_setup_active_ping()` — inits `_last_ping_date_utc = None`, creates `_ping_check_timer` (5-min QTimer), connects `timeout → _maybe_emit_active_ping`, starts timer, and wires `QApplication.instance().applicationStateChanged → _on_app_state_changed` (D-16 focus/resume awareness).

4. `_on_app_state_changed(state)` — fires `_maybe_emit_active_ping()` on `ApplicationActive` state.

5. `_maybe_emit_active_ping()` — five guards in order: `_telemetry_ready()` (MEDIUM-9), `is_enabled()` (consent), `today != _session_start_date_utc` (D-16 launch-day), `_last_ping_date_utc != today` (at-most-once), `applicationState() == ApplicationActive`. Sets `_last_ping_date_utc = today` before emit.

**Producer wiring sites in genizah_app.py** (all hardcoded constants, D-04):
- `open_join_workbench` (no-arg launcher): `feature_name='joins_lab'`
- `open_joins_workbench` (anchor variant): `dialog_name='visual_similarity'` when `source in ('visual','combined')`, else `feature_name='joins_lab'` (mutually exclusive, T-114-13)
- `_open_puzzle_window` (explicit open): `feature_name='fragment_puzzle'`
- `add_to_puzzle` (Browse/ResultDialog/lists/VS add path): `feature_name='fragment_puzzle'` (REVIEWS MEDIUM-7)
- `_show_fjms_catalog_dialog` (Browse-tab path): `dialog_name='fjms_catalog'`
- `export_results`: `dialog_name='export'` BEFORE save dialog; `action=_EXPORT_ACTION_BY_FMT.get(fmt)` AFTER `if not path: return`
- `export_comp_report`: `dialog_name='export'` AFTER no-data early-return and BEFORE save dialog; `action=_EXPORT_ACTION_BY_FMT.get(fmt)` AFTER `if not path: return` (REVIEWS MEDIUM-8)

**desktop/result_dialog.py** — Two producer wiring sites (direct telemetry.track calls, not via _emit_feature_opened since ResultDialog has no _telemetry_ready):
- `ResultDialog.__init__` (after `self._app = parent`): `dialog_name='result_detail'`, `session_id=getattr(parent, '_session_id', '')` — single canonical site covering all 6 construction sites in genizah_app.py
- `_show_rd_catalog` (before FjmsCatalogDialog construction): `dialog_name='fjms_catalog'`, `session_id=getattr(self._app, '_session_id', '')` (REVIEWS MEDIUM-6)

**tests/test_no_dynamic_telemetry_strings.py** (new, 5 tests):
- `FORBIDDEN_ACCESSORS = frozenset({'currentText', 'tabText', 'windowTitle', 'text', 'selectedFiles', 'toPlainText'})`
- `_ForbiddenAccessorInTelemetryArgsVisitor`: inspects ONLY telemetry-call argument/keyword-value subtrees via `ast.walk` — does NOT scan whole FunctionDef (REVIEWS HIGH-3 load-bearing fix)
- Identity callsite check: `identify(user.id)` flagged, `identify(user._uuid)` passes (REVIEWS LOW-10)
- Test 1 (`test_lint_rejects_synthetic_arg_violation`): `telemetry.track(..., x=w.currentText())` IS flagged
- Test 2 (`test_lint_accepts_forbidden_accessor_outside_telemetry_args`): function calling `w.currentText()` for non-telemetry work AND `telemetry.track(..., a='lit')` is NOT flagged (proves re-scope correctness — HIGH-3)
- Test 3 (`test_lint_accepts_clean_producer`): literals + dict-lookup are NOT flagged
- Test 4 (`test_lint_rejects_identify_non_uuid`): `telemetry.identify(user.id)` IS flagged; `telemetry.identify(user._uuid)` is NOT (LOW-10)
- Test 5 (`test_no_dynamic_telemetry_strings_in_producers`): production scan of TARGET_FILES passes — all Phase 114 producers use hardcoded constants

**tests/test_telemetry_phase114.py** — extended from 47 tests (Plan 02) to 70 tests (Plan 03), adding 23 tests:
- Task 1 (14 tests): emit_feature_opened method exists, joins_lab emit, fragment_puzzle emit, fjms_catalog emit, result_detail emit, visual_similarity emit, ready gate, both puzzle paths source check (MEDIUM-7), live VS path source check, VS text-path-emits-joins_lab, fjms_catalog in result_dialog source (MEDIUM-6), result_detail in result_dialog, export dialog + action source check (MEDIUM-8), export action map, consent gate
- Task 2 (8 tests): heartbeat methods exist, fires when conditions met, not on session_start day (D-16), at-most-once-per-day, not when app inactive, _telemetry_ready gate (MEDIUM-9), consent gate, applicationStateChanged wired in source (D-16)

## Commits

| Task | Phase | Commit | Description |
|------|-------|--------|-------------|
| Task 1+2 | RED | 827fd302 | Failing tests for feature_opened + active_ping (Wave 3) |
| Task 1+2 | GREEN | 10d51c9c | feature_opened producers + active_ping heartbeat |
| Task 3 | GREEN | 54e39fb8 | D-17 re-scoped AST guard + ruff cleanup |

## Verification

- `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_phase114.py tests/test_no_dynamic_telemetry_strings.py -q` — 75 passed
- `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_telemetry_phase114.py tests/test_telemetry_identity.py tests/test_telemetry_consent_gate.py tests/test_no_dynamic_telemetry_strings.py -q` — 95 passed (no regressions)
- `grep -n "applicationStateChanged" genizah_app.py` — match confirmed (focus/resume awareness, not naive 24h timer)
- `grep -n "visual_similarity" genizah_app.py` — match at line 15961, inside `open_joins_workbench` (LIVE path), NOT inside `_browse_view_visual_similarity` (dead)
- `grep -n "feature_name='fragment_puzzle'" genizah_app.py` — TWO matches (lines 15873, 15885 — REVIEWS MEDIUM-7)
- `grep -n "fjms_catalog" desktop/result_dialog.py` — match inside `_show_rd_catalog` (REVIEWS MEDIUM-6)
- `grep -n "result_detail" desktop/result_dialog.py` — match inside `ResultDialog.__init__`
- `python -m ruff check genizah_app.py desktop/result_dialog.py tests/test_telemetry_phase114.py tests/test_no_dynamic_telemetry_strings.py` — all checks passed

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] test_export_action_map_helper checked export_results source for literal strings**
- **Found during:** Task 1 GREEN test run
- **Issue:** The initial test asserted `'export_xlsx' in inspect.getsource(app.GenizahGUI.export_results)`. The static map `_EXPORT_ACTION_BY_FMT` is a class attribute (defined between `_telemetry_ready` and `_setup_active_ping`), not inside `export_results`. The function body contains `self._EXPORT_ACTION_BY_FMT.get(fmt)` but not the string `'export_xlsx'` literally.
- **Fix:** Rewrote the test to check `app.GenizahGUI._EXPORT_ACTION_BY_FMT` directly (much cleaner — tests the actual mapping correctness rather than source text pattern).
- **Files modified:** `tests/test_telemetry_phase114.py`
- **Commit:** 10d51c9c

## Known Stubs

None — all three tasks fully implemented. The `_setup_active_ping` stub guard from Plan 01 (`hasattr(self, '_setup_active_ping')`) is now satisfied by the real implementation.

## Threat Flags

All T-114-09 through T-114-15 mitigations implemented per plan:
- T-114-09 (feature/dialog/action value leaks): hardcoded constants only; dead VS handler not instrumented; `_EXPORT_ACTION_BY_FMT` static map; D-17 AST guard enforces structurally
- T-114-10 (heartbeat payload): ACTIVE_PING carries only `session_id` (uuid4 hex) — no content
- T-114-11 (fabricated DAU): once-per-UTC-day + active-only + not-launch-day + _telemetry_ready() guards all enforced; tested explicitly
- T-114-13 (double-count ghost): VS vs joins_lab mutually exclusive per source guard; puzzle ×2 paths = distinct user gestures; FJMS catalog ×2 paths = distinct surfaces
- T-114-15 (non-events counted): export dialog emit after no-data guard + before save dialog; action emit after path chosen only
- T-114-12 (future producer leak / identity-source drift): D-17 AST guard (production scan test) provides durable structural enforcement across all three target files

## Self-Check: PASSED

- genizah_app.py: FOUND
- desktop/result_dialog.py: FOUND
- tests/test_telemetry_phase114.py: FOUND
- tests/test_no_dynamic_telemetry_strings.py: FOUND
- 114-03-SUMMARY.md: FOUND
- Commit 827fd302 (RED): FOUND
- Commit 10d51c9c (GREEN Tasks 1+2): FOUND
- Commit 54e39fb8 (GREEN Task 3 + ruff): FOUND
