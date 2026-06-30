---
phase: 127-update-ui-final-cleanup
plan: "03"
subsystem: desktop
tags: [refactor, decomposition, noqa-retirement, test-retarget, milestone-closeout]
dependency_graph:
  requires: [127-02]
  provides: [v8.3.0-milestone-complete, GUARD-03, GUARD-04]
  affects: [genizah_app.py, tests/test_telemetry_consent_ux.py, tests/test_privacy_disclosure_strings.py]
tech_stack:
  added: []
  patterns: [move-and-shim noqa-retirement, OR-location hard flip, identity-hold verification]
key_files:
  modified:
    - genizah_app.py
    - tests/test_telemetry_consent_ux.py
    - tests/test_privacy_disclosure_strings.py
decisions:
  - "D1 noqa suffix retired — all 9 classes are used directly by GenizahGUI, so plain imports are clean (no F401)"
  - "EN privacy disclosure test hard-flipped to desktop/settings_dialogs.py only; OR-location pattern no longer needed after Phase 126 D1 MOVE"
  - "test_telemetry_consent_ux.py retargeted from genizah_app.SettingsDialog to desktop.settings_dialogs.SettingsDialog (0 remaining genizah_app.SettingsDialog)"
metrics:
  duration: "~30 min"
  completed: "2026-06-26"
  tasks_completed: 2
  tasks_total: 2
---

# Phase 127 Plan 03: D1 Noqa Retirement & Final Milestone Sign-Off Summary

**One-liner:** Retired Phase-126 D1 noqa markers from genizah_app.py plain imports, retargeted the one external SettingsDialog caller, hard-flipped the EN disclosure test to desktop-only, and signed off the v8.3.0 zero-behavior-change milestone with a full-suite green.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Retire D1 noqa suffix + retarget telemetry test + flip EN disclosure test | b5ac8b75 | genizah_app.py, tests/test_telemetry_consent_ux.py, tests/test_privacy_disclosure_strings.py |
| 2 | FINAL full-suite sign-off (bulk + gui) — v8.3.0 milestone gate | (verification only) | — |

## What Was Built

**Task 1 — Three precise edits:**

1. **D1 noqa suffix retirement** (`genizah_app.py` lines 77-78): removed the `  # noqa: F401  Phase 126 D1` suffix from both D1 import lines. The imports themselves are retained — all 9 D1 classes (ShelfmarkTableWidgetItem, CheckBoxHeader, HiddenScrollArea, ListsTreeWidget, SettingsDialog, SearchSettingsDialog, HelpDialog, TabularQueryBuilderDialog, LabScoringDialog) are used directly by GenizahGUI, so ruff sees them as plain used imports and emits no F401.

2. **Stale comment update** (`genizah_app.py` ~line 542): updated the block comment from "re-exported via the # noqa: F401 shim" to "re-exported via the plain import shim" and added a Phase-127 note. Cosmetic: corrected the update_ui shim label from "# Phase 127 D1" to "# Phase 127 update_ui".

3. **External caller retarget** (`tests/test_telemetry_consent_ux.py`): replaced all 4 occurrences of `genizah_app.SettingsDialog.__new__(genizah_app.SettingsDialog)` with `desktop.settings_dialogs.SettingsDialog.__new__(desktop.settings_dialogs.SettingsDialog)` plus added `import desktop.settings_dialogs` at each call site. Post-edit `grep -c "genizah_app.SettingsDialog" = 0`. All 21 tests in the file pass.

4. **GUARD-03 hard flip** (`tests/test_privacy_disclosure_strings.py`): flipped `test_about_dialog_contains_local_cache_disclosure_en` from scanning `app_src + dialogs_src` (OR-location) to scanning `desktop/settings_dialogs.py` only. Updated docstring to note Phase-127 final flip. The `"zstd"` and `"never uploaded"/"not uploaded"/"never upload"` assertions are retained. The HE test (`..._he`) is unchanged (its OR-location is the i18n tr() move, not the D1 shim).

**Task 2 — Full-suite sign-off:**

- All 7 phase-touched guard/test files: **105 passed**
- Bulk slice (`not gui and not render_smoke`): **4894 passed, 0 failed, 32 skipped, 3 xfailed, 26 xpassed** — zero failures
- GUI/render_smoke slice: **60 passed, 0 failed, 4 skipped**
- All 13 identity relationships verified: 9 D1 classes (via `desktop.settings_dialogs` / `desktop.ui_widgets`) + 4 update_ui classes — all `genizah_app.X is desktop.X.X: True`

## Acceptance Criteria Verification

| Criterion | Result |
|-----------|--------|
| genizah_app.py lines 77-78 no `# noqa: F401 Phase 126 D1` | PASS — suffix removed, imports retained |
| `grep -c "genizah_app.SettingsDialog" test_telemetry_consent_ux.py` = 0 | PASS — 0 |
| `desktop.settings_dialogs.SettingsDialog` referenced in test file | PASS — 4 occurrences |
| EN disclosure test scans desktop/settings_dialogs.py only | PASS |
| HE disclosure test unchanged | PASS |
| test_tabular_builder_rtl.py NOT modified | PASS |
| ruff clean on all 3 edited files | PASS |
| 9 D1 identity relationships still hold | PASS — 13/13 (includes 4 update_ui) |
| test_no_back_edges_core.py (GUARD-01) green | PASS |
| Bulk slice == 0 failures (better than 6-env baseline) | PASS — 0 failures |
| GUI/render_smoke slice green | PASS — 60 passed |

## Live Line Numbers Edited (post-Wave-2 shift)

- `genizah_app.py` line 77: `from desktop.ui_widgets import ...` — noqa suffix removed
- `genizah_app.py` line 78: `from desktop.settings_dialogs import ...` — noqa suffix removed
- `genizah_app.py` line 79: cosmetic comment `D1` → `update_ui`
- `genizah_app.py` lines 540-545: stale comment updated (`# noqa: F401 shim` → `plain import shim`)
- `tests/test_telemetry_consent_ux.py` lines 522, 589, 653, 722 (pre-edit): all 4 `genizah_app.SettingsDialog` occurrences replaced
- `tests/test_privacy_disclosure_strings.py` lines 66-87: EN about-dialog test hard-flipped

## Deviations from Plan

None — plan executed exactly as written.

## Threat Flags

None. This plan makes no changes to network endpoints, auth paths, file access patterns, or schema. T-127-03 (telemetry consent snapshot) confirmed unchanged; test_telemetry_consent_ux.py re-run green after caller retarget.

## Known Stubs

None.

## Self-Check: PASSED

- genizah_app.py edits verified by ruff check (clean) and identity assertions
- test_telemetry_consent_ux.py: `grep -c "genizah_app.SettingsDialog"` = 0; 4 `desktop.settings_dialogs.SettingsDialog` references present; 21 tests pass
- test_privacy_disclosure_strings.py: EN test scans desktop/settings_dialogs.py only; all tests pass
- Commit b5ac8b75 exists (verified by git log)
- Bulk slice: 4894 passed, 0 failed
- GUI slice: 60 passed, 0 failed
- All 13 identity checks: PASS

## v8.3.0 God-File Decomposition Milestone — SIGNED OFF

Phase 127 (Wave 3) is the final wave of the v8.3.0 milestone. With this plan complete:

- **Phase 122**: GUARD-01 + CONFIG-01 (Config cycle pivot)
- **Phase 123**: shared/responsa cluster (variants, codicological, responsa, joins, lists, metadata, indexer)
- **Phase 124**: core metadata + index extraction
- **Phase 125**: core engines (LabSettings, LabEngine, SearchEngine)
- **Phase 126**: desktop D1 — 9 classes MOVE-and-shimmed to desktop/settings_dialogs.py + desktop/ui_widgets.py
- **Phase 127**: update_ui extraction (Wave 1) + 3 new test guard files (Wave 2) + final cleanup + full-suite sign-off (Wave 3)

genizah_core.py is the permanent 20-name compat facade (NEVER removed). genizah_app.py D1 shims are retired. Zero behavior change confirmed by full-suite green.
