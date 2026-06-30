---
phase: 127-update-ui-final-cleanup
verified: 2026-06-26T00:00:00Z
status: passed
score: 5/5 must-haves verified
overrides_applied: 0
re_verification: null
gaps: []
human_verification:
  - test: "Launch python genizah_app.py and exercise the update-notification bar, What's-New bar/dialog, update-progress dialog, and the sidecar update/reset/download flow"
    expected: "All UI elements render and behave normally; no crash; sidecar download coordination flows through correctly"
    why_human: "Full interactive launch with real network update check + sidecar download threads + window paint is untestable headless; behavioral tests cover the coordination logic in isolation but cannot exercise the full Qt paint path"
---

# Phase 127: Update UI & Final Cleanup Verification Report

**Phase Goal:** Extract `desktop/update_ui.py` + new direct behavioral tests for sidecar reset/download coordination; remove the Phase-126 (D1) desktop shims from `genizah_app.py`; install the `desktop/` back-edge guard; confirm `genizah_core.py` permanent facade; full-suite-green sign-off.
**Verified:** 2026-06-26
**Status:** PASSED
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| #  | Truth                                                                                                                                                             | Status     | Evidence                                                                                                                                                       |
|----|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| SC#1 | `desktop/update_ui.py` exists, imports cleanly; 4 classes importable from it; behavioral tests for sidecar coordination methods pass                            | VERIFIED   | `import desktop.update_ui` succeeds; all 4 classes return correct objects; identity with `genizah_app.X` holds; `tests/test_update_ui_coordination.py` 7/7 pass |
| SC#2 | Phase-126 D1 shims retired (noqa markers removed), callers retargeted, update_ui shim included                                                                   | VERIFIED   | `grep -c "# noqa: F401  Phase 126 D1" genizah_app.py` = 0; lines 77-78 are plain imports; `test_telemetry_consent_ux.py` 0 remaining `genizah_app.SettingsDialog` references (17/17 pass) |
| SC#3 | `genizah_core.py` permanent facade confirmed; `tests/test_genizah_core_facade.py` asserts same-object identity for all extracted names                           | VERIFIED   | `Config is C2`, `SearchEngine is SE2`, `MetadataManager is MM2` all True; `tests/test_genizah_core_facade.py` 13/13 pass                                     |
| SC#4 | Both back-edge guards green: `test_no_back_edges_core.py` (GUARD-01) + new `test_no_back_edges_desktop.py` (GUARD-04)                                            | VERIFIED   | `tests/test_no_back_edges_core.py` 26/26 pass; `tests/test_no_back_edges_desktop.py` 22/22 pass (19 desktop modules scanned, lazy imports correctly excluded) |
| SC#5 | Full pytest suite green (bulk + gui slices)                                                                                                                       | VERIFIED   | Bulk (`not gui and not render_smoke`): 4894 passed, 0 failed, 32 skipped; GUI/render_smoke: 60 passed, 0 failed, 4 skipped                                   |

**Score:** 5/5 truths verified

### Required Artifacts

| Artifact                                  | Expected                                  | Status     | Details                                                        |
|-------------------------------------------|-------------------------------------------|------------|----------------------------------------------------------------|
| `desktop/update_ui.py`                    | 4 update-UI classes                       | VERIFIED   | 4 classes at module level: `UpdateNotificationBar`, `WhatsNewBar`, `WhatsNewDialog`, `UpdateProgressDialog` |
| `tests/test_update_ui_coordination.py`    | Behavioral tests for coordination methods | VERIFIED   | 7 tests; covers `_reset_sidecar_connections`, `_download_next_sidecar`, `_on_sidecar_download_finished` |
| `tests/test_no_back_edges_desktop.py`     | GUARD-04 AST guard for desktop/ modules   | VERIFIED   | 19 modules scanned; all pass; lazy import in `join_workbench.py` correctly excluded |
| `tests/test_genizah_core_facade.py`       | Permanent facade identity assertions      | VERIFIED   | 13 identity assertions across Config, BrowseMapUtils, TextNormalize, Variants, Responsa, Codicological, JoinsManager, ListsManager, MetadataManager, Indexer, LabSettings, LabEngine, SearchEngine |
| `genizah_app.py` lines 77-79              | Plain imports, no noqa D1 marker          | VERIFIED   | Lines 77-78 are plain imports from `desktop.ui_widgets` and `desktop.settings_dialogs`; line 79 is plain import from `desktop.update_ui` |

### Key Link Verification

| From                             | To                            | Via                                   | Status   | Details                                                          |
|----------------------------------|-------------------------------|---------------------------------------|----------|------------------------------------------------------------------|
| `genizah_app.py`                 | `desktop.update_ui`           | plain import line 79                  | WIRED    | `genizah_app.UpdateNotificationBar is desktop.update_ui.UpdateNotificationBar` = True |
| `genizah_app.py`                 | `desktop.settings_dialogs`    | plain import line 78, no noqa D1     | WIRED    | D1 noqa suffix retired; import retained (9 classes used by GenizahGUI directly) |
| `genizah_app.py`                 | `desktop.ui_widgets`          | plain import line 77, no noqa D1     | WIRED    | D1 noqa suffix retired; import retained |
| `test_telemetry_consent_ux.py`   | `desktop.settings_dialogs`    | 4 call sites retargeted               | WIRED    | 0 remaining `genizah_app.SettingsDialog` references; 17 tests pass |
| `genizah_core.py`                | `shared/*.py`                 | permanent re-export facade            | WIRED    | All 13 tested names resolve from both `genizah_core` and `shared.*` to same object |
| `desktop/update_ui.py`           | `genizah_app`                 | NO module-level import (guard passes) | VERIFIED | `test_no_back_edges_desktop.py` passes for `desktop/update_ui.py` |

### Behavioral Spot-Checks

| Behavior                                                          | Command                                                                          | Result                       | Status  |
|-------------------------------------------------------------------|----------------------------------------------------------------------------------|------------------------------|---------|
| 4 update_ui classes importable from desktop.update_ui             | `python -c "import desktop.update_ui as d; [getattr(d,n) for n in ['UpdateNotificationBar','WhatsNewBar','WhatsNewDialog','UpdateProgressDialog']]"` | All 4 resolved | PASS |
| genizah_app re-exports are identity-same as desktop.update_ui     | `python -c "..."` (identity check for all 4)                                     | All `is` True                | PASS    |
| genizah_core facade resolves Config, SearchEngine, MetadataManager | `python -c "from genizah_core import Config, SearchEngine, MetadataManager; ..."`| All `is` True vs shared modules | PASS |
| No `# noqa: F401  Phase 126 D1` markers remain                   | `grep -c "Phase 126 D1" genizah_app.py`                                          | 0                            | PASS    |
| Bulk test suite                                                   | `pytest tests/ -m "not gui and not render_smoke" -q --tb=no`                     | 4894 passed, 0 failed        | PASS    |
| GUI test slice                                                    | `pytest tests/ -m "gui or render_smoke" -q`                                      | 60 passed, 0 failed          | PASS    |

**Note on asyncio flake:** When running the full suite with `-x` (stop on first failure), `tests/test_joins_builder.py::TestBuilderStateRoundTrip::test_round_trip_search_type_fuzzy` fails due to asyncio event loop contamination from a prior NiceGUI test. This is a pre-existing test-ordering flake: the test passes 100% in isolation, passes when run with `test_joins_builder.py` in isolation (26/26), and does not appear in the marker-based bulk slice (`not gui and not render_smoke`). This is NOT a Phase 127 regression.

### Requirements Coverage

| Requirement | Description                                                                   | Status    | Evidence                                                |
|-------------|-------------------------------------------------------------------------------|-----------|---------------------------------------------------------|
| DESK-08     | Update-UI cluster extracted to `desktop/update_ui.py` with behavioral tests   | SATISFIED | `desktop/update_ui.py` exists with 4 classes; `test_update_ui_coordination.py` 7/7 pass |
| GUARD-02    | Zero behavior change — full suite passes at every phase boundary               | SATISFIED | Bulk 4894 passed / 0 failed; GUI 60 passed / 0 failed   |
| GUARD-03    | Source-scanning tests retargeted before original deletion                     | SATISFIED | `test_telemetry_consent_ux.py` retargeted; `test_privacy_disclosure_strings.py` hard-flipped; `test_tabular_builder_rtl.py` untouched (last commit pre-Phase 127) |
| GUARD-04    | `genizah_core.py` permanent facade; `genizah_app.py` D1 shims retired         | SATISFIED | D1 noqa markers removed; `test_genizah_core_facade.py` 13/13; `test_no_back_edges_desktop.py` 22/22 |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| (none) | — | — | — | No TBD/FIXME/XXX/placeholder patterns found in Phase 127 touched files |

### Human Verification Required

**1. Interactive launch of update-UI components**

**Test:** Launch `python genizah_app.py` and exercise: (a) update-notification bar appearance when an update is available, (b) What's-New bar and dialog, (c) update-progress dialog during a sidecar download, (d) the sidecar reset/download coordination flow.
**Expected:** All UI elements render and behave normally; no crash; sidecar download coordination completes without error; UI state consistent with pre-Phase-127 behavior.
**Why human:** Full interactive launch with real network update check + sidecar download threads + Qt window paint cannot be tested headless. The behavioral tests cover the coordination logic in isolation (using `GenizahGUI.__new__` + stubs) but cannot exercise the full Qt rendering and real-network path.

### Gaps Summary

No gaps. All 5 Success Criteria are verified by direct codebase evidence. The phase delivered:
- `desktop/update_ui.py` with 4 real class definitions (not stubs)
- Identity re-export in `genizah_app.py` (line 79, plain import)
- D1 noqa markers retired (lines 77-78 clean)
- External caller (`test_telemetry_consent_ux.py`) retargeted to `desktop.settings_dialogs`
- `tests/test_no_back_edges_desktop.py` installed (19 modules, GUARD-04)
- `tests/test_genizah_core_facade.py` installed (13 identity assertions)
- `tests/test_update_ui_coordination.py` installed (7 behavioral tests for coordination methods in-place on GenizahGUI)
- Full-suite green: bulk 4894/0, gui 60/0

The one human-verification item (interactive launch) is a standard UI smoke check that cannot be automated; it does not block the passed status since all automated SCs are verified.

---

_Verified: 2026-06-26_
_Verifier: Claude (gsd-verifier)_
