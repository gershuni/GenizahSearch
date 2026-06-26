---
phase: 126-desktop-panels
plan: 01
subsystem: ui
tags: [pyqt6, desktop, refactor, god-file-decomposition, move-and-shim, settings, telemetry-consent]

# Dependency graph
requires:
  - phase: 122-125 (genizah_core decomposition)
    provides: MOVE-and-shim recipe (code MOVED to module + `# noqa: F401` re-export; identity holds)
  - phase: 95 (My Library)
    provides: desktop/my_library_tab.py panel-extraction template + SP-1 lazy-import idiom
provides:
  - desktop/settings_dialogs.py (SettingsDialog, SearchSettingsDialog, HelpDialog, TabularQueryBuilderDialog, LabScoringDialog)
  - desktop/ui_widgets.py (ShelfmarkTableWidgetItem, CheckBoxHeader, HiddenScrollArea, ListsTreeWidget)
  - GenizahGUI.apply_settings()/cancel_settings() thin named API (SP-4 boundary)
  - genizah_app.py re-export shims for all 9 D1 classes (originals DELETED)
affects: [127-desktop-shim-cleanup, SEED-028 (method-based panels D2-D5)]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "MOVE-and-shim for top-level desktop classes (delete original + # noqa: F401 re-export → identity holds)"
    - "GUARD-03 additive OR-location source-scan retarget (accept BOTH old + new module, never premature-flip)"

key-files:
  created:
    - desktop/settings_dialogs.py
    - desktop/ui_widgets.py
  modified:
    - genizah_app.py
    - tests/test_tabular_builder_rtl.py
    - tests/test_privacy_disclosure_strings.py

key-decisions:
  - "D-07b telemetry-consent snapshot strip moved VERBATIM into settings_dialogs.py — Cancel must not overwrite freshly-set consent (save_app_config is additive-merge)"
  - "SettingsDialog OK/Cancel buttons route through GenizahGUI.apply_settings()/cancel_settings() (DESK-01); the dialog still owns the D-07b _on_cancel snapshot"
  - "self._run_startup_telemetry_coordinator() in the moved dialog kept verbatim (self = dialog) for ZERO behavior change — not rerouted"
  - "6 now-unused PyQt6 imports dropped from genizah_app.py (QStyleOptionButton/QAbstractItemView/QRect/QPen/QBrush/QPainterPath) — used only by moved widgets, zero genizah_app.<sym> references"

patterns-established:
  - "Top-level class extraction: re-grep `^class ` before cutting; one cohesive cluster per atomic commit"
  - "Per-file ruff only (never repo-wide --fix); preserve # noqa: F401 shims"

requirements-completed: [DESK-01, DESK-02, GUARD-02, GUARD-03, GUARD-04]

# Metrics
duration: ~55min
completed: 2026-06-26
---

# Phase 126 Plan 01: Desktop D1 Dialogs + Widgets Extraction Summary

**Moved the five top-level modal dialogs to `desktop/settings_dialogs.py` and the four reusable widget subclasses to `desktop/ui_widgets.py` via MOVE-and-shim (originals DELETED, `# noqa: F401` re-exports preserve `genizah_app.X is desktop.Y.X` identity), with the D-07b telemetry-consent strip moved verbatim and a new GenizahGUI.apply_settings()/cancel_settings() boundary — ZERO behavior change.**

## Performance

- **Duration:** ~55 min (excluding ~25 min of background full-suite runs)
- **Tasks:** 3
- **Files created:** 2
- **Files modified:** 3

## Accomplishments
- `desktop/ui_widgets.py` (NEW) — 4 widget subclasses moved verbatim; imports cleanly in isolation, no module-level `import genizah_app` (GUARD-01).
- `desktop/settings_dialogs.py` (NEW) — 5 dialog classes moved verbatim incl. the load-bearing D-07b telemetry-consent snapshot strip (`_TELEMETRY_SNAPSHOT_EXCLUDE` / `self._config_snapshot` / `_on_cancel`).
- `genizah_app.py` — all 9 original class defs DELETED, re-exported via two `# noqa: F401` shim lines; `genizah_app.py` shrank ~1,860 lines. `LabPanel` and `_CatalogRefreshWorker` correctly left in place (deferred to E2 / D2).
- `GenizahGUI.apply_settings()` / `cancel_settings()` thin named API added (DESK-01 / SP-4); SettingsDialog OK/Cancel buttons route through them.
- All 9 D1 names hold same-object identity through the shims (GUARD-04 verified via base-vs-HEAD NAME diff, not a failure count).

## Task Commits

Each task was committed atomically:

1. **Task 1: MOVE D1 widget classes → desktop/ui_widgets.py** - `9ce4bf0a` (refactor)
2. **Task 2: MOVE D1 dialog classes → desktop/settings_dialogs.py + apply/cancel API** - `efacf998` (refactor)
3. **Task 3: Additively retarget GUARD-03 source-scan tests + run wave slices** - `432c1598` (test)

_(Final plan-metadata/STATE commit follows this SUMMARY.)_

## Files Created/Modified
- `desktop/ui_widgets.py` (NEW) - ShelfmarkTableWidgetItem, CheckBoxHeader, HiddenScrollArea, ListsTreeWidget.
- `desktop/settings_dialogs.py` (NEW) - LabScoringDialog, SearchSettingsDialog, HelpDialog, TabularQueryBuilderDialog, SettingsDialog (+ verbatim D-07b strip).
- `genizah_app.py` (MOD) - deleted 9 class defs; added 2 shim import lines; added GenizahGUI.apply_settings()/cancel_settings(); dropped 6 now-unused PyQt6 imports.
- `tests/test_tabular_builder_rtl.py` (MOD) - OR-location AST scan (genizah_app.py OR desktop/settings_dialogs.py); RTL behavioral assertions unchanged.
- `tests/test_privacy_disclosure_strings.py` (MOD) - EN About-dialog zstd/'never uploaded' scan now reads the combined genizah_app.py + desktop/settings_dialogs.py source.

## Decisions Made
- **D-07b strip moved verbatim.** Cancel restores a config snapshot with the 7 telemetry-consent keys stripped so `save_app_config` (additive-merge) cannot clobber freshly-set consent. Preserved exactly; `test_telemetry_consent_ux.py` green (17 passed).
- **apply/cancel routing.** Dialog buttons call `self.main_win.apply_settings`/`cancel_settings`; `cancel_settings` delegates to the dialog's `_on_cancel` (snapshot still owned by the dialog). Behavior-identical to the prior direct `accept()`/`_on_cancel()` wiring.
- **`_run_startup_telemetry_coordinator` NOT rerouted.** The moved dialog keeps `self._run_startup_telemetry_coordinator()` (where `self` is the dialog) verbatim for ZERO behavior change; rerouting was out of scope and would alter behavior.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug / GUARD-03] Additively retargeted `test_privacy_disclosure_strings.py::test_about_dialog_contains_local_cache_disclosure_en`**
- **Found during:** Task 3 (bulk wave slice — 1 real failure)
- **Issue:** The EN About-tab disclosure strings (`zstd`, `never uploaded`) live in `SettingsDialog`'s About HTML, which moved to `desktop/settings_dialogs.py`. This source-scan test still read only `genizah_app.py` → failed. The plan named only `test_tabular_builder_rtl.py` as the D1 GUARD-03 retarget; this is a second source-scan test pinning moved content.
- **Fix:** OR-location additive retarget — scan the combined `genizah_app.py + desktop/settings_dialogs.py` source (consistent with the plan's additive-retarget rule and Codex pre-flight HIGH-2 / Cross-Cutting "never premature-flip").
- **Files modified:** tests/test_privacy_disclosure_strings.py
- **Verification:** 4 passed; full bulk slice re-run 4853 passed / 0 failed.
- **Committed in:** 432c1598 (Task 3 commit)

**2. [Rule 3 - Blocking] Dropped 6 now-unused PyQt6 imports from genizah_app.py**
- **Found during:** Task 1 (per-file ruff)
- **Issue:** After moving CheckBoxHeader/ListsTreeWidget, the imports QStyleOptionButton, QAbstractItemView, QRect, QPen, QBrush, QPainterPath became F401-unused, breaking the per-file ruff gate.
- **Fix:** Removed the 6 names from the genizah_app.py import lines. Confirmed each had exactly 1 occurrence (the import itself) and zero `genizah_app.<sym>` references anywhere (Grep across `*.py`), so no consumer relies on them being re-exported.
- **Files modified:** genizah_app.py
- **Verification:** ruff clean; GUARD-04 NAME diff shows these as the ONLY lost names (all transitive PyQt symbols, not public API).
- **Committed in:** 9ce4bf0a (Task 1 commit)

---

**Total deviations:** 2 auto-fixed (1 GUARD-03 test retarget, 1 blocking import cleanup)
**Impact on plan:** Both necessary to satisfy GUARD-02 (zero behavior change) and the per-file ruff gate. No scope creep — both confined to the D1 move's direct consequences.

## Issues Encountered
- **Env baseline differs from the plan's stated 6-failure baseline.** The plan anticipated 6 `test_search_api_v2::..._real_index[*]` failures (no real index in test env). This environment HAS a real index, so those 6 tests **xpass** (counted in the 26 xpassed) rather than fail. Net result is identical: zero real failures. Bulk slice: 4853 passed, 32 skipped, 3 xfailed, 26 xpassed, 0 failed. GUI slice: 60 passed, 4 skipped, 0 failed.

## Verification Results
- **Isolation imports:** both new modules import cleanly headless; GUARD-01 grep clean (no module-level `import genizah_app`).
- **Identity (GUARD-04):** all 9 names `genizah_app.X is desktop.<module>.X`; `LabPanel` + `_CatalogRefreshWorker` still present; base-vs-HEAD NAME diff = only the 6 unused PyQt symbols.
- **D-07b (T-126D1-01):** `test_telemetry_consent_ux.py` 17 passed — consent-snapshot strip behaves identically.
- **GUARD-03:** `test_tabular_builder_rtl.py` 2 passed (resolves the class from the NEW module via OR-location); `test_privacy_disclosure_strings.py` 4 passed.
- **GUARD-02:** bulk slice 0 real failures; gui slice fully green.
- **ruff:** per-file clean on all touched files; shims intact; no repo-wide `--fix`.
- **BOM:** none on any touched `.py`.

## Threat Flags
None — pure desktop code relocation; no new network endpoints, auth paths, schema changes, or trust boundaries (T-126D1-01 mitigation = D-07b strip preserved verbatim, gated by test_telemetry_consent_ux.py).

## Known Stubs
None.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- D1 cluster fully extracted; the MOVE-and-shim pipeline is proven on top-level classes for the harder method-cluster panels (D2-D5, deferred to SEED-028).
- Phase 127 should retarget external `from genizah_app import {SettingsDialog,...}` callers to the new modules, delete the two shim lines, and flip the two OR-location source-scan tests to new-only.

## Self-Check: PASSED

- Created files exist: desktop/ui_widgets.py, desktop/settings_dialogs.py, .planning/phases/126-desktop-panels/126-01-SUMMARY.md
- Task commits exist: 9ce4bf0a, efacf998, 432c1598

---
*Phase: 126-desktop-panels*
*Completed: 2026-06-26*
