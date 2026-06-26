# Phase 127: Update UI & Final Cleanup - Research

**Researched:** 2026-06-26
**Domain:** Desktop PyQt6 module extraction (MOVE-and-shim) + AST guard installation + genizah_core facade confirmation + full-suite sign-off
**Confidence:** HIGH

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**DESK-08 — extract `desktop/update_ui.py` (MOVE-and-shim, the genizah_core/D1 recipe)**
- Move the update-UI **classes**: `UpdateNotificationBar`, `WhatsNewBar`, `WhatsNewDialog`,
  `UpdateProgressDialog` → `desktop/update_ui.py`; DELETE the originals from `genizah_app.py`, replace
  with a `# noqa: F401` re-export shim so `genizah_app.X is desktop.update_ui.X` identity holds.
- **NEW direct behavioral tests** for the sidecar reset/download coordination methods (SEED-020 §7 C-6 —
  these GUI methods have no direct test today). Register any new GUI test in conftest `_GUI_TEST_FILES`.
- `pyqtSignal`-bearing workers stay at module level in the new home.

**Final cleanup**
- **Retire the D1 shims:** retarget every `from genizah_app import <D1 class>` / `genizah_app.<D1 class>`
  caller to `desktop.settings_dialogs` / `desktop.ui_widgets`, then DELETE the two D1 shim lines from
  `genizah_app.py` (genizah_app.py:77-78). This is the GUARD-03/04 "flip" deferred from Phase 126.
- **Install `tests/test_no_back_edges_desktop.py`** — AST guard mirroring `test_no_back_edges_core.py`:
  no `desktop/` module imports `genizah_app` at MODULE level (lazy `# noqa: PLC0415` only).
- **Confirm the `genizah_core` permanent facade** stays intact (`tests/test_genizah_core_facade.py`,
  new or updated): `genizah_core.X is shared.Y.X` for the moved core names. The `genizah_core` facade is
  PERMANENT — NEVER removed (contrast: the `genizah_app` D1 shims ARE retired here).
- **Full-suite sign-off** (bulk + gui slices) — the milestone's final zero-behavior-change gate.

### Claude's Discretion

Phase 127 is a pure internal refactor with no gray areas. Discuss was skipped per the standing v8.3.0
autonomous directive. All decisions are either locked in ROADMAP/SEED-020 or delegated to research/
Codex preflight. The one real unknown — the sidecar coordination coupling — is explicitly delegated
to research below.

### Deferred Ideas (OUT OF SCOPE)

- D2-D5 method-based panels (catalog/search/browse/lists) → SEED-028 (DESK-03..07, deferred).
- Composition tab (DEFER-02/03) + startup/session remainder (DEFER-04).
- The ≥70% `genizah_app.py` shrink — explicitly NOT a v8.3.0 target.
- Removing the `genizah_core` facade (it is PERMANENT).
- Any web change / any behavior change.
</user_constraints>

---

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| DESK-08 | Update-UI sub-cluster (notification / What's-New / progress dialogs + sidecar reset/download coordination) extracted to `desktop/update_ui.py`, with new direct behavioral tests for the sidecar reset/download coordination methods plus the existing sidecar tests. | All 4 classes confirmed as clean top-level MOVE-and-shim candidates; coordination methods are entangled (self.* coupling) — defer method extraction, test in place. |
| GUARD-02 | Zero behavior change — full existing pytest suite passes at every phase boundary. | Same discipline as Phases 122-126; bulk 4853 + gui 60 baseline; per-file ruff only. |
| GUARD-03 | Every source-scanning / AST test retargeted before deletion. | Specifically: `test_privacy_disclosure_strings.py` OR-location (genizah_app.py + desktop/settings_dialogs.py) must be flipped to new-only (desktop/settings_dialogs.py) since it's the Phase-126-deferred "flip" step. No other source-scanning test references the 4 update_ui class names — no additional retarget needed for DESK-08. |
| GUARD-04 | `genizah_core.py` remains permanent compat facade; `genizah_app.py` implementation shims removed in clean deletion pass. | genizah_core facade: 20 names already confirmed identical by test_no_back_edges_core.py; new test_genizah_core_facade.py consolidates the assertion. genizah_app shims: D1 lines 77-78 retired after callers retargeted. |
</phase_requirements>

---

## Summary

Phase 127 is the final phase of the v8.3.0 God-File Decomposition milestone. It has three distinct workstreams:

**Workstream 1 (DESK-08): Extract `desktop/update_ui.py`.** The four update-UI classes (`UpdateNotificationBar`, `WhatsNewBar`, `WhatsNewDialog`, `UpdateProgressDialog`) are confirmed as clean top-level PyQt6 classes at lines 184, 243, 295, and 364 of `genizah_app.py` — identical to the D1 dialogs and widgets extracted in Phase 126. MOVE-and-shim applies cleanly. However, the sidecar reset/download coordination (a cluster of ~11 GenizahGUI methods from line ~24316 onward) is MODERATELY ENTANGLED: it accesses `self.update_bar`, `self.whats_new_bar`, `self.btn_check_updates`, `self.update_thread`, `self.sidecar_update_thread`, `self._sidecar_download_queue`, `self._sidecar_data_dir`, `self._current_sidecar_download`, `self.update_dialog`, and calls the module-level `reset_catalog_filter_sets()` function. The coupling pattern is lighter than the D2-D5 panels (none of the update methods touch 100+ self.* attributes) but still accesses a mix of update-owned state (`self.update_bar`, `self.update_dialog`) and settings-panel state (`self.btn_check_updates`). The verdict: **extract the 4 classes (MOVE-and-shim), keep the coordination methods on GenizahGUI, and write the new DESK-08 behavioral tests AGAINST the methods in place on GenizahGUI.** This is the lowest-risk, zero-behavior-change path.

**Workstream 2 (D1 shim retirement + GUARD-03 flip).** The only external caller of the D1 classes is `tests/test_telemetry_consent_ux.py`, which references `genizah_app.SettingsDialog` at four locations via attribute-access (`genizah_app.SettingsDialog.__new__(...)`). These must be retargeted to `desktop.settings_dialogs.SettingsDialog`. The test `test_tabular_builder_rtl.py` already has OR-location scanning and requires no change. The `test_privacy_disclosure_strings.py` OR-location must be flipped from `genizah_app.py + desktop/settings_dialogs.py` to `desktop/settings_dialogs.py` alone (the Phase-126-deferred hard flip). The two shim lines (`genizah_app.py:77-78`) are then deleted.

**Workstream 3 (Guards + facade + full-suite).** Install `tests/test_no_back_edges_desktop.py` (mirrors `test_no_back_edges_core.py`; scans all 19 current `desktop/*.py` modules plus the new `desktop/update_ui.py`). Create `tests/test_genizah_core_facade.py` (consolidates the 20 existing identity assertions currently scattered in `test_no_back_edges_core.py` into one dedicated file, as required by ROADMAP SC#3). Full-suite green is the milestone sign-off.

**Primary recommendation:** Three-wave execution — Wave 1: extract `desktop/update_ui.py` (MOVE-and-shim) + add shim to `genizah_app.py`; Wave 2: retarget D1 callers + delete D1 shims + flip GUARD-03 test + DESK-08 behavioral tests; Wave 3: install desktop back-edge guard + genizah_core facade test + full-suite sign-off.

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Update notification bar widget | `desktop/update_ui.py` | GenizahGUI (wiring only) | Pure UI widget; self-contained QFrame with signals — no GenizahGUI self.* access |
| What's New bar/dialog widgets | `desktop/update_ui.py` | GenizahGUI (wiring only) | Same pattern as notification bar |
| Update progress dialog | `desktop/update_ui.py` | GenizahGUI (wiring only) | Self-contained download + install flow; only parent-passthrough for QMessageBox |
| Sidecar reset/download coordination | GenizahGUI methods (stay) | — | Accesses self.btn_check_updates (owned by SettingsDialog-built row), self.update_bar/whats_new_bar (owned by update_ui), self._sidecar_* state; too entangled across multiple ownership domains for a safe method move |
| D1 shim retirement | genizah_app.py (delete lines 77-78) | test_telemetry_consent_ux.py (retarget) | Consumer retargets to desktop.settings_dialogs; shim lines become dead code |
| Desktop back-edge AST guard | tests/test_no_back_edges_desktop.py (new) | — | Mirrors GUARD-01 pattern; enforces no desktop/* module imports genizah_app at module level |
| genizah_core facade confirmation | tests/test_genizah_core_facade.py (new) | test_no_back_edges_core.py (existing) | ROADMAP SC#3 requires explicit facade test; identity tests already pass inside back-edge guard file |

---

## Research Directive Answers

### Directive #1: Are the 4 update-UI classes top-level classes?

**CONFIRMED [VERIFIED: grep ^class genizah_app.py].**

```
184:class UpdateNotificationBar(QFrame):
243:class WhatsNewBar(QFrame):
295:class WhatsNewDialog(QDialog):
364:class UpdateProgressDialog(QDialog):
```

All four are `^class`-level definitions at the module scope of `genizah_app.py` — NOT nested inside GenizahGUI, NOT methods. They live immediately before `LabPanel` (line 598) and `_CatalogRefreshWorker` (line 878) and far above the `GenizahGUI(QMainWindow)` definition at line 1538. Clean MOVE-and-shim candidates, exactly like D1.

`pyqtSignal` bearers within each class:
- `UpdateNotificationBar`: `dismissed = pyqtSignal(str)`, `update_requested = pyqtSignal(str, str, str)`
- `WhatsNewBar`: `dismissed = pyqtSignal()`, `learn_more = pyqtSignal()`
- `WhatsNewDialog`: no pyqtSignal (pure dialog)
- `UpdateProgressDialog`: no pyqtSignal (uses `gui_threads.UpdateDownloaderThread`)

All signals are class-level attributes within the class body — this is correct for PyQt6 and will work unchanged after the move. No module-level `pyqtSignal` workers need to move.

### Directive #2 (THE CRUX): Is the sidecar reset/download coordination entangled?

**MODERATELY ENTANGLED — RECOMMEND DEFERRING METHOD EXTRACTION. Extract only the 4 classes.**

**Coordination method cluster (all on GenizahGUI, lines ~24316–24480):**

| Method | self.* accessed | External callers outside this cluster |
|--------|----------------|---------------------------------------|
| `check_updates_auto()` | `self.update_thread`, `self.sidecar_update_thread` | Called from GenizahGUI startup (line 1797) |
| `check_updates_manual()` | `self.btn_check_updates`, `self.update_thread` | Wired to `corner_version_btn.clicked` (line 2183) |
| `on_update_result(...)` | `self.btn_check_updates`, `self.update_bar`, calls `self.start_in_app_update` | Signal handler for `update_thread.finished_signal` |
| `on_update_error(...)` | `self.btn_check_updates` | Signal handler for `update_thread.error_signal` |
| `_on_sidecar_updates(updates)` | calls `self._start_sidecar_download` | Signal handler for `sidecar_update_thread.update_available` |
| `_start_sidecar_download(updates)` | `self._sidecar_download_queue`, `self._sidecar_data_dir`, calls `self._reset_sidecar_connections`, `self._download_next_sidecar` | Called from `_on_sidecar_updates` |
| `_reset_sidecar_connections()` | None (calls module-level `reset_catalog_filter_sets()` + lazy shared service resets) | Called from `_start_sidecar_download`, `_download_next_sidecar`, and `UpdateProgressDialog.execute_update()` |
| `_download_next_sidecar()` | `self._sidecar_download_queue`, `self._sidecar_data_dir`, `self._current_sidecar_download`, calls `self._reset_sidecar_connections` | Called from `_start_sidecar_download`, `_on_sidecar_download_finished` |
| `_on_sidecar_download_finished(...)` | calls `self._download_next_sidecar` | Signal handler for `_current_sidecar_download.finished_signal` |
| `on_update_dismissed(version)` | none — calls `save_app_config` | Signal handler wired to `update_bar.dismissed` (line 2264) |
| `on_whats_new_dismissed()` | none — calls `save_app_config` | Signal handler wired to `whats_new_bar.dismissed` (line 2270) |
| `show_whats_new_dialog()` | `self.whats_new_bar`, `self.on_whats_new_dismissed` | Signal handler wired to `whats_new_bar.learn_more` (line 2271) |
| `start_in_app_update(version, html_url, installer_url)` | `self.update_bar`, `self.update_dialog`, calls `QTimer.singleShot` | Called from `on_update_result` and wired to `update_bar.update_requested` (line 2265) |

**Key coupling analysis:**

1. `self.btn_check_updates` is NOT owned by GenizahGUI's own `__init__` — it is created by `SettingsDialog` at `settings_dialogs.py:1292` as `self.main_win.btn_check_updates = QPushButton(...)`. This is a cross-ownership dependency (SettingsDialog assigns a widget to GenizahGUI's namespace) that makes `check_updates_manual` and `on_update_result`/`on_update_error` impossible to move without also owning `btn_check_updates`.

2. `_reset_sidecar_connections()` calls the module-level function `reset_catalog_filter_sets()` which lives in `genizah_app.py` itself (at line 870, not a shared function). After extraction this would need a lazy `from genizah_app import reset_catalog_filter_sets` inside the method — which is an acceptable lazy back-edge (`# noqa: PLC0415`), but adds moving-part complexity.

3. The coordination methods collectively span ~165 lines (~24316–24480) with 9-13 `self.*` accesses across 4 distinct ownership domains (update thread management, sidecar queue state, widget refs, config calls). NOT as densely coupled as `on_search_finished` (109 self.*) but non-trivial.

**VERDICT: Do NOT extract the coordination methods. Extract only the 4 bar/dialog CLASSES (lines 184–593) via MOVE-and-shim. Write the new DESK-08 behavioral tests AGAINST THE METHODS IN PLACE on GenizahGUI.** This is identical to how D2-D5 were handled — the method logic stays, only the standalone classes move.

### Directive #3: Full external-caller set for the 9 D1 classes

**Verified [VERIFIED: grep -rn]**

The only external caller of the D1 classes (outside `genizah_app.py` itself) is:

**`tests/test_telemetry_consent_ux.py`** — 4 references, all via `genizah_app.SettingsDialog` attribute-access:
- Line 522: `genizah_app.SettingsDialog.__new__(genizah_app.SettingsDialog)`
- Line 589: `genizah_app.SettingsDialog.__new__(genizah_app.SettingsDialog)`
- Line 653: `genizah_app.SettingsDialog.__new__(genizah_app.SettingsDialog)`
- Line 722: `genizah_app.SettingsDialog.__new__(genizah_app.SettingsDialog)`

These must become `desktop.settings_dialogs.SettingsDialog.__new__(desktop.settings_dialogs.SettingsDialog)`.

**`tests/test_tabular_builder_rtl.py`** — already has OR-location scanning (Phase 126 GUARD-03 additive). **No change needed** — it already accepts the class in either location.

**`tests/test_privacy_disclosure_strings.py`** — OR-location scan (`genizah_app.py + desktop/settings_dialogs.py`) with a Phase-126-deferred hard flip comment ("Phase 127's job"). **Flip required**: after deleting D1 shims, the scan should read only `desktop/settings_dialogs.py`.

**Other `from genizah_app import` callers** found in tests (e.g., `_build_search_results_xlsx_bytes`, `_format_txt_genizah_block`, `_aggregate_local_pages_with_separators`) — these import **non-D1 functions** and are unaffected by D1 shim deletion. They should be left as-is.

**`desktop/join_workbench.py:4135`** — lazy `from genizah_app import _build_search_results_xlsx_bytes` inside a function body (`# noqa: PLC0415` pattern). This is a **non-D1 function**, unaffected.

**genizah_app.py's OWN usage of the D1 classes** (critical for determining which imports stay after shim deletion):

| Class | Usage in genizah_app.py (outside its own definition) | Import needed after D1 shim deletion? |
|-------|------------------------------------------------------|----------------------------------------|
| `SettingsDialog` | line 2159 `self.settings_dialog = SettingsDialog(self)`, line 14651 comment, line 14716 `SettingsDialog.FULL_CITATION` | YES — genizah_app.py uses it. The import from `desktop.settings_dialogs` STAYS (drops `# noqa: F401`, becomes a plain used import) |
| `SearchSettingsDialog` | line 15016 `d = SearchSettingsDialog(self, ...)` | YES — same treatment |
| `HelpDialog` | line 14820 `dlg = HelpDialog(...)` | YES |
| `TabularQueryBuilderDialog` | line 16007 `dlg = TabularQueryBuilderDialog(self)` | YES |
| `LabScoringDialog` | line 764 `d = LabScoringDialog(self, ...)` | YES |
| `ShelfmarkTableWidgetItem` | lines 16562, 16565 | YES |
| `CheckBoxHeader` | lines 5247, 5725 | YES |
| `HiddenScrollArea` | lines 22088, 22184, 22187 | YES |
| `ListsTreeWidget` | line 11427 | YES |

**All 9 D1 classes are actively used by genizah_app.py itself.** After D1 shim retirement:
1. The `# noqa: F401` comment is REMOVED from lines 77-78
2. The imports are NOT deleted — they become normal (used) imports that just don't need the `noqa` suppressor

This is a two-character edit (remove `# noqa: F401  Phase 126 D1` suffix) not a line deletion.

Similarly for update_ui: after DESK-08 shim is placed, genizah_app.py will use `UpdateNotificationBar`, `WhatsNewBar`, `WhatsNewDialog`, `UpdateProgressDialog` at lines 2263, 2269, 24464, 24476 — so the shim import stays as a used import (no `# noqa: F401` needed at all, since it IS used).

### Directive #4: `test_no_back_edges_desktop.py` — modules to scan

**Verified [VERIFIED: ls desktop/*.py]**

Current `desktop/*.py` modules (18 files, 19 after adding `update_ui.py`):

```python
DESKTOP_MODULES = [
    "desktop/__init__.py",
    "desktop/consent_dialog.py",
    "desktop/dialogs_filter.py",
    "desktop/dialogs_scholarly.py",
    "desktop/file_actions.py",
    "desktop/image_loader.py",
    "desktop/join_workbench.py",
    "desktop/my_library_tab.py",
    "desktop/pdf_image_controller.py",
    "desktop/pdf_page_renderer.py",
    "desktop/puzzle.py",
    "desktop/result_dialog.py",
    "desktop/settings_dialogs.py",   # Phase 126
    "desktop/telemetry.py",
    "desktop/title_helpers.py",
    "desktop/ui_widgets.py",          # Phase 126
    "desktop/update_ui.py",           # Phase 127 (new)
    "desktop/viewers.py",
    "desktop/vs_cache.py",
]
```

**Existing back-edges to verify:** A sweep confirmed zero module-level `import genizah_app` or `from genizah_app import` in ANY current `desktop/*.py` file. The only genizah_app import in the desktop directory is `desktop/join_workbench.py:4135`, which is LAZY (inside a `try:` block inside a function body) — acceptable per the `# noqa: PLC0415` convention and NOT flagged by the guard.

The new `desktop/update_ui.py` must likewise have zero module-level `import genizah_app`. `UpdateProgressDialog.execute_update()` calls `reset_pgp_service()` etc. via lazy `from shared.document_service import ...` (already lazy in the original) — no issue.

**Guard shape**: The guard's AST traversal logic is IDENTICAL to `test_no_back_edges_core.py`, substituting `genizah_app` for `genizah_core` as the forbidden module name, and the `DESKTOP_MODULES` list above as the scan targets.

### Directive #5: `test_genizah_core_facade.py` — does it exist, what must it cover?

**`tests/test_genizah_core_facade.py` does NOT exist** [VERIFIED: ls command]. The facade identity tests (20 names) are currently embedded in `tests/test_no_back_edges_core.py` as individual `test_*_identity()` and `test_*_standalone_import()` functions. ROADMAP SC#3 requires "new or updated" — Phase 127 should **create** `tests/test_genizah_core_facade.py` as a dedicated file.

**What the new file must assert (extracted from `test_no_back_edges_core.py`):**

The facade currently covers these 20 identity assertions across 13 symbols/groups:

| shared module | Facade names confirmed in test_no_back_edges_core.py |
|--------------|-------------------------------------------------------|
| `shared.config` | `Config` |
| `shared.browse_map_utils` | `normalize_shelfmark`, `natural_sort_key`, `get_library_display`, `LIBRARY_CODES`, `dedupe_browse_map` |
| `shared.text_normalize` | `strip_nikud`, `strip_search_diacritics`, `NIKUD_PATTERN`, `COMBINING_DIACRITICALS_PATTERN` |
| `shared.variants` | `VariantManager` |
| `shared.responsa` | `ResponsaComponent`, `parse_responsa_query`, `_apply_explosion_guard`, `_count_expanded_terms`, `GRAMMATICAL_PREFIXES` |
| `shared.codicological` | `CodicologicalManager` |
| `shared.joins_manager` | `JoinsManager` |
| `shared.lists_manager` | `ListsManager` |
| `shared.metadata_manager` | `MetadataManager`, `_BoundedLRUCache`, `MARC_FUTURE_TIMEOUT`, `_NLI_CACHE_MAX_ENTRIES` |
| `shared.indexer` | `Indexer` |
| `shared.lab_settings` | `LabSettings` |
| `shared.lab_engine` | `LabEngine` |
| `shared.search_engine` | `SearchEngine` |

The new `test_genizah_core_facade.py` file should contain one test per shared module asserting at least the primary class/symbol identity. It can be thin (just the `X is Y` assertions, no smoke instantiation — those stay in the back-edge test file). The key requirement is that the file name matches what ROADMAP SC#3 references.

**Note:** Since `test_no_back_edges_core.py` already has these tests and they already pass, `test_genizah_core_facade.py` is additive documentation — it will not introduce new passing/failing conditions, just provide a dedicated, named home for the facade contract.

### Directive #6: GUARD-03 source-scan tests that need retarget/flip

**Verified [VERIFIED: grep searches]**

Tests touching D1 classes or update_ui symbols that require attention:

| Test file | Current state | Phase 127 action |
|-----------|---------------|-----------------|
| `test_telemetry_consent_ux.py` | 4x `genizah_app.SettingsDialog` attribute-access | **Retarget** to `desktop.settings_dialogs.SettingsDialog` (part of D1 shim retirement) |
| `test_tabular_builder_rtl.py` | OR-location (both genizah_app.py AND desktop/settings_dialogs.py) | **No change needed** — OR is still valid (tabular AST guard) |
| `test_privacy_disclosure_strings.py` | OR-location (`genizah_app.py + desktop/settings_dialogs.py`) with comment "Phase 127's job" | **Flip to new-only** (`desktop/settings_dialogs.py` alone) after D1 shim deletion |
| Any test referencing `UpdateNotificationBar`, `WhatsNewBar`, `WhatsNewDialog`, `UpdateProgressDialog` | **Zero such tests found** | No retarget needed |

No source-scanning test reads `genizah_app.py` for update_ui class names. The GUARD-03 footprint for DESK-08 is empty — the 4 new classes have no existing test references outside `genizah_app.py`.

---

## Standard Stack

### Core (no new packages — pure refactor)

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| PyQt6 | project dep | QFrame, QDialog, QThread, pyqtSignal | Already in project; update_ui classes are PyQt6 widgets |
| Python stdlib ast | stdlib | AST guard for `test_no_back_edges_desktop.py` | Same as `test_no_back_edges_core.py` — no new dep |
| pytest | project dep | New behavioral tests + guard tests | Project test framework |

**No new packages needed.** Phase 127 is a pure code reorganization — all dependencies are already present.

### Supporting

The new `desktop/update_ui.py` module will import:

```python
# Module-level (safe — all from shared/ or stdlib, no genizah_app back-edge)
from genizah_core import tr, CURRENT_LANG, load_app_config, save_app_config, APP_VERSION
from gui_threads import SidecarDownloadThread, UpdateDownloaderThread
from PyQt6.QtWidgets import (QFrame, QDialog, QHBoxLayout, QVBoxLayout, QLabel,
                              QPushButton, QProgressBar, QScrollArea, QMessageBox)
from PyQt6.QtCore import Qt, QTimer, pyqtSignal
from PyQt6.QtGui import QDesktopServices
from PyQt6.QtCore import QUrl
```

All of these are already used by the classes in `genizah_app.py` and are available in the project environment.

---

## Package Legitimacy Audit

No new packages are installed in this phase. This section is not applicable.

---

## Architecture Patterns

### The MOVE-and-shim Recipe (Phase 126 proven)

Phase 126 established the definitive recipe for desktop class extraction. Phase 127 repeats it exactly for the 4 update_ui classes:

**Step A (additive — Wave 1):**
1. Create `desktop/update_ui.py` with the 4 classes MOVED (not copied) from `genizah_app.py`
2. DELETE the 4 originals from `genizah_app.py`
3. ADD shim line to `genizah_app.py`: `from desktop.update_ui import UpdateNotificationBar, WhatsNewBar, WhatsNewDialog, UpdateProgressDialog`
   - These ARE used by genizah_app.py (at lines 2263, 2269, 24464, 24476), so **no `# noqa: F401`** needed
4. Verify `genizah_app.UpdateNotificationBar is desktop.update_ui.UpdateNotificationBar` etc.

**Step B (D1 shim retirement — Wave 2):**
1. Retarget `tests/test_telemetry_consent_ux.py`: 4x `genizah_app.SettingsDialog` → `desktop.settings_dialogs.SettingsDialog`
2. Flip `tests/test_privacy_disclosure_strings.py` from OR-location to new-only
3. Edit `genizah_app.py:77-78`: remove `# noqa: F401  Phase 126 D1` suffix from the two D1 import lines (the imports themselves STAY — they are used)
4. Verify `test_telemetry_consent_ux.py` green, `test_privacy_disclosure_strings.py` green

**Step C (guards + facade + sign-off — Wave 3):**
1. Create `tests/test_no_back_edges_desktop.py` (AST guard for desktop/ modules)
2. Create `tests/test_genizah_core_facade.py` (facade identity assertions)
3. Run full suite (bulk + gui)

### System Architecture Diagram

```
genizah_app.py (28k lines, stays)
    |
    +-- [import line, used] from desktop.update_ui import UpdateNotificationBar, WhatsNewBar, WhatsNewDialog, UpdateProgressDialog
    +-- [import line, used, no noqa] from desktop.settings_dialogs import SettingsDialog, ...  (was noqa shim, now plain used)
    +-- [import line, used, no noqa] from desktop.ui_widgets import ShelfmarkTableWidgetItem, ...  (was noqa shim, now plain used)
    |
    +-- GenizahGUI.__init__
    |       self.update_bar = UpdateNotificationBar()   <-- resolves via desktop.update_ui
    |       self.whats_new_bar = WhatsNewBar()          <-- same
    |       self.settings_dialog = SettingsDialog(self) <-- resolves via desktop.settings_dialogs
    |
    +-- GenizahGUI.check_updates_auto / check_updates_manual / ... (STAY IN PLACE)
    +-- GenizahGUI._on_sidecar_updates / _start_sidecar_download / ... (STAY IN PLACE)

desktop/update_ui.py (NEW)
    +-- class UpdateNotificationBar(QFrame)    [moved from genizah_app.py:184]
    +-- class WhatsNewBar(QFrame)              [moved from genizah_app.py:243]
    +-- class WhatsNewDialog(QDialog)          [moved from genizah_app.py:295]
    +-- class UpdateProgressDialog(QDialog)    [moved from genizah_app.py:364]

tests/test_no_back_edges_desktop.py (NEW)
    +-- DESKTOP_MODULES = [19 paths]
    +-- test_no_module_level_genizah_app_import(rel_path) [parametrized]

tests/test_genizah_core_facade.py (NEW)
    +-- test_config_facade_identity()
    +-- test_search_engine_facade_identity()
    +-- ... (13 per-module identity assertions)

tests/test_update_ui_coordination.py (NEW, DESK-08 behavioral tests)
    +-- test_reset_sidecar_connections_calls_all_three_services()
    +-- test_download_next_sidecar_pops_queue()
    +-- test_download_next_sidecar_calls_reset_when_queue_empty()
    +-- test_on_sidecar_download_finished_advances_queue()
    +-- ... (register in conftest _GUI_TEST_FILES if they require QApplication)
```

### Recommended Project Structure (desktop/ after Phase 127)

```
desktop/
├── __init__.py
├── consent_dialog.py
├── dialogs_filter.py
├── dialogs_scholarly.py
├── file_actions.py
├── image_loader.py
├── join_workbench.py
├── my_library_tab.py
├── pdf_image_controller.py
├── pdf_page_renderer.py
├── puzzle.py
├── result_dialog.py
├── settings_dialogs.py    # Phase 126: 5 dialogs
├── telemetry.py
├── title_helpers.py
├── ui_widgets.py           # Phase 126: 4 widgets
├── update_ui.py            # Phase 127: 4 update/news bars+dialogs
├── viewers.py
└── vs_cache.py
```

### Pattern: DESK-08 Behavioral Test Shape

The coordination methods have no direct test today (SEED-020 §7 C-6). The behavioral tests must exercise the coordination logic without requiring a full `GenizahGUI` construction (which would need a real Tantivy index). Pattern from Phase 126 D-07b and Phase 116 telemetry tests:

```python
# Source: Phase 126 D1 MOVE-and-shim recipe; Phase 116 telemetry behavioral tests
import pytest
from unittest.mock import MagicMock, patch, call

def _make_coordinator():
    """Build a minimal GenizahGUI-duck with just the update coordinator attrs."""
    # Import genizah_app with QApplication mocked
    import genizah_app
    # Use __new__ to skip __init__ (same pattern as test_telemetry_consent_ux.py)
    gui = genizah_app.GenizahGUI.__new__(genizah_app.GenizahGUI)
    gui._sidecar_download_queue = []
    gui._sidecar_data_dir = "/fake/data"
    gui._current_sidecar_download = None
    return gui

def test_reset_sidecar_connections_invalidates_catalog_filter():
    """_reset_sidecar_connections must call reset_catalog_filter_sets."""
    ...
```

**GUI test registration**: If these tests use QApplication event dispatching, add `"test_update_ui_coordination.py"` to `_GUI_TEST_FILES` in `tests/conftest.py`. If they are pure unit tests (using `__new__` + Mock, no event loop), they can run in the bulk slice without gui registration.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Module-level import detection | Custom AST walker | Copy `_has_module_level_genizah_core_import` from `test_no_back_edges_core.py`, rename to `_has_module_level_genizah_app_import` | Already handles try:/if:/ClassDef scope correctly |
| genizah_core facade | New re-export shims | Already complete — all 20 names shimmed in Phases 122-125 | No new shims needed |
| sidecar download logic | New implementation | Keep existing GenizahGUI methods in place | Zero-behavior-change constraint |
| pkg version comparison | Custom semver | Already in `SidecarUpdateThread._is_newer()` in `gui_threads.py` | Already ships |

---

## Common Pitfalls

### Pitfall 1: Deleting the D1 import lines instead of removing the noqa suffix

**What goes wrong:** Developer sees `# noqa: F401` and deletes the import lines instead of just removing the `# noqa: F401  Phase 126 D1` suffix. This breaks genizah_app.py at runtime (all 9 D1 classes are used by GenizahGUI methods).

**Why it happens:** The `# noqa: F401` suffix is how shims are suppressed, and deleting shims = deleting the import. But these imports are USED — the `# noqa` was only needed because they appeared unused to ruff (the re-export was the only "use" before external callers were retargeted). After retargeting, genizah_app.py itself still uses them.

**How to avoid:** Per-class check: does genizah_app.py use `SettingsDialog` / `CheckBoxHeader` etc. in its methods? YES (verified above). Therefore the import stays, only `# noqa: F401  Phase 126 D1` is removed.

**Warning signs:** ruff F401 "imported but unused" after the `# noqa` removal → you accidentally deleted a real usage.

### Pitfall 2: Adding `# noqa: F401` to the update_ui shim line when it's not needed

**What goes wrong:** Developer adds `# noqa: F401` to the new `from desktop.update_ui import ...` line, consistent with D1 shim pattern. But the update_ui classes ARE used by genizah_app.py (at lines 2263, 2269, 24464, 24476) so no `# noqa: F401` is needed.

**Why it happens:** D1 shims needed `# noqa: F401` because their external callers were in test files, not genizah_app.py itself. Update_ui classes are instantiated directly by genizah_app.py.

**How to avoid:** Before adding `# noqa: F401`, grep for usage in genizah_app.py.

### Pitfall 3: The `test_no_back_edges_desktop.py` guard flagging join_workbench.py's lazy import

**What goes wrong:** The new guard scans `desktop/join_workbench.py` and flags line 4135 (`from genizah_app import _build_search_results_xlsx_bytes`) as a back-edge violation.

**Why it happens:** The guard's scope-aware traversal descends into compound statements but NOT into `FunctionDef` bodies. Line 4135 is inside a try: block that is itself inside a method body — so it is a lazy import, correctly not flagged.

**How to avoid:** Verify the guard's `_LAZY_SCOPE = (ast.FunctionDef, ast.AsyncFunctionDef)` stop-descent logic (copied from `test_no_back_edges_core.py`) correctly handles this. Add a test case that verifies a function-body lazy import is NOT flagged (mirror `test_guard_ignores_lazy_function_body_import`).

**Warning signs:** Test reports `join_workbench.py` as a GUARD-01 violation → the scope-aware traversal logic has a bug.

### Pitfall 4: Retargeting test_telemetry_consent_ux.py incompletely (missing one of 4 references)

**What goes wrong:** Developer updates 3 of 4 `genizah_app.SettingsDialog` references. The 4th still resolves via the shim (since shim deletion is the last step), so tests pass during retarget — but after shim deletion, the 4th reference breaks.

**Why it happens:** The 4 references are in different test functions (lines 522, 589, 653, 722) — easy to miss one if editing by search+replace with incomplete context.

**How to avoid:** Use global search-replace; verify grep count BEFORE and AFTER (expect 0 matches for `genizah_app.SettingsDialog` after retarget).

### Pitfall 5: test_genizah_core_facade.py duplicating existing guards

**What goes wrong:** The new facade test file re-asserts the same identity checks already in `test_no_back_edges_core.py`, causing double-test overlap (both pass, no problem, but adds maintenance cost).

**Why it happens:** ROADMAP SC#3 says "new or updated" — it's unclear whether to update the existing file or create a new one.

**How to avoid:** The cleanest interpretation per ROADMAP SC#3: create `tests/test_genizah_core_facade.py` as a thin wrapper that delegates — or simply duplicates the identity assertions with clear `test_genizah_core_facade.py` documentation markers. The existing `test_no_back_edges_core.py` identity tests are not removed — they serve a different purpose (enforcing no new back-edges; the facade identities confirm the correct refactoring).

---

## Code Examples

### MOVE-and-shim pattern (desktop/update_ui.py module header)

```python
# Source: Phase 126 126-01 recipe; desktop/settings_dialogs.py:1-20
"""Desktop update-UI cluster: notification bar, What's-New bar/dialog, update progress dialog.

Extracted from genizah_app.py (Phase 127 DESK-08) via MOVE-and-shim.
genizah_app.py re-exports these classes for backward compatibility:
    UpdateNotificationBar, WhatsNewBar, WhatsNewDialog, UpdateProgressDialog

GUARD-01: NO module-level ``import genizah_app`` — symbols come from
genizah_core (tr, CURRENT_LANG, load_app_config, save_app_config, APP_VERSION),
gui_threads (SidecarDownloadThread, UpdateDownloaderThread), and PyQt6.
"""
from __future__ import annotations

from genizah_core import tr, CURRENT_LANG, load_app_config, save_app_config, APP_VERSION
from gui_threads import SidecarDownloadThread, UpdateDownloaderThread  # noqa: F401 (used by UpdateProgressDialog.start_download)

from PyQt6.QtWidgets import (
    QFrame, QDialog, QHBoxLayout, QVBoxLayout, QLabel,
    QPushButton, QProgressBar, QScrollArea, QMessageBox,
)
from PyQt6.QtCore import Qt, QTimer, pyqtSignal
from PyQt6.QtGui import QDesktopServices
from PyQt6.QtCore import QUrl
```

### Shim line format for genizah_app.py (no `# noqa: F401` since classes are used)

```python
# Source: Phase 127 DESK-08 pattern (classes are USED by genizah_app.py methods, no noqa needed)
from desktop.update_ui import UpdateNotificationBar, WhatsNewBar, WhatsNewDialog, UpdateProgressDialog
```

### D1 shim retirement (lines 77-78 edit — remove noqa suffix, keep import)

Before:
```python
from desktop.ui_widgets import ShelfmarkTableWidgetItem, CheckBoxHeader, HiddenScrollArea, ListsTreeWidget  # noqa: F401  Phase 126 D1
from desktop.settings_dialogs import SettingsDialog, SearchSettingsDialog, HelpDialog, TabularQueryBuilderDialog, LabScoringDialog  # noqa: F401  Phase 126 D1
```

After (shim retired — noqa suffix deleted, imports kept because used by GenizahGUI):
```python
from desktop.ui_widgets import ShelfmarkTableWidgetItem, CheckBoxHeader, HiddenScrollArea, ListsTreeWidget
from desktop.settings_dialogs import SettingsDialog, SearchSettingsDialog, HelpDialog, TabularQueryBuilderDialog, LabScoringDialog
```

### Desktop back-edge guard module list (for test_no_back_edges_desktop.py)

```python
# Source: ls desktop/*.py (verified 2026-06-26) + new Phase 127 module
DESKTOP_MODULES = [
    "desktop/__init__.py",
    "desktop/consent_dialog.py",
    "desktop/dialogs_filter.py",
    "desktop/dialogs_scholarly.py",
    "desktop/file_actions.py",
    "desktop/image_loader.py",
    "desktop/join_workbench.py",
    "desktop/my_library_tab.py",
    "desktop/pdf_image_controller.py",
    "desktop/pdf_page_renderer.py",
    "desktop/puzzle.py",
    "desktop/result_dialog.py",
    "desktop/settings_dialogs.py",
    "desktop/telemetry.py",
    "desktop/title_helpers.py",
    "desktop/ui_widgets.py",
    "desktop/update_ui.py",   # Phase 127 (pre-registered; skip-until-exists guard)
    "desktop/viewers.py",
    "desktop/vs_cache.py",
]
```

### Behavioral test template for sidecar coordination (DESK-08)

```python
# Source: Phase 116 test_telemetry_consent_ux.py __new__ pattern
import pytest
from unittest.mock import MagicMock, patch, call

def _make_gui_coordinator():
    """Build a minimal GenizahGUI duck with only update-coordinator state.
    Uses __new__ to skip __init__ (no QApplication, no Tantivy index).
    """
    import genizah_app
    gui = genizah_app.GenizahGUI.__new__(genizah_app.GenizahGUI)
    gui._sidecar_download_queue = []
    gui._sidecar_data_dir = "/fake/data"
    gui._current_sidecar_download = None
    return gui

def test_reset_sidecar_connections_calls_three_services():
    """_reset_sidecar_connections must reset all three sidecar services + catalog filter."""
    gui = _make_gui_coordinator()
    with patch("genizah_app.reset_catalog_filter_sets") as mock_cat, \
         patch("shared.document_service.reset_pgp_service") as mock_pgp, \
         patch("shared.fjms_service.reset_fjms_service") as mock_fjms, \
         patch("shared.nli_crossref_service.reset_nli_crossref_service") as mock_nli:
        gui._reset_sidecar_connections()
    mock_pgp.assert_called_once()
    mock_fjms.assert_called_once()
    mock_nli.assert_called_once()
    mock_cat.assert_called_once()
```

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| All GUI classes in genizah_app.py | MOVE-and-shim to desktop/*.py | v7.9 (2026-04), v8.3.0 (2026-06) | genizah_app.py shrinks; desktop/ modules are independently testable |
| Sidecar reset methods — no direct test | New behavioral tests against methods in place | Phase 127 (DESK-08) | SEED-020 §7 C-6 closes |
| D1 class shims (`# noqa: F401`) | Plain imports (noqa removed) | Phase 127 | Cleanup; ruff treats them as used |
| genizah_core facade — tested inline in GUARD-01 file | Dedicated `test_genizah_core_facade.py` | Phase 127 | Explicit contractual documentation of the permanent facade |

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `gui_threads.UpdateDownloaderThread` is already imported lazily inside `UpdateProgressDialog.start_download()` and does not need a module-level import in `desktop/update_ui.py` | Standard Stack / Code Examples | Causes NameError at runtime if it needs module-level scope. Mitigation: verify `UpdateDownloaderThread` usage at line 447 of UpdateProgressDialog — it IS imported lazily inside the method. Confirmed [VERIFIED: Read]. |
| A2 | `reset_catalog_filter_sets()` is called LAZILY inside `_reset_sidecar_connections()` via module-scope access (not `self.`) in genizah_app.py | Directive #2 analysis | If called as `genizah_app.reset_catalog_filter_sets()` from desktop/update_ui.py, it would be a back-edge. But the coordination methods STAY on GenizahGUI so this is irrelevant. [ASSUMED] — the coordination methods are NOT being extracted. |

**If this table is empty:** N/A — two low-risk assumptions logged above.

---

## Open Questions

1. **Should `test_update_ui_coordination.py` use `pytest.mark.gui` (gui test split) or not?**
   - What we know: Phase 116 telemetry tests used `__new__` + Mock and were registered in `_GUI_TEST_FILES`. Phase 126 D-07b tests also in `_GUI_TEST_FILES`.
   - What's unclear: Do the sidecar coordination behavioral tests require a running QApplication event loop, or can they run with just `__new__` + Mock?
   - Recommendation: Start without `_GUI_TEST_FILES` registration; if QApplication errors appear during test collection, add to `_GUI_TEST_FILES` per the conftest pattern. The `_make_gui_coordinator()` approach using `__new__` should avoid QApplication requirements.

2. **Should `test_genizah_core_facade.py` import from both `shared.*` and `genizah_core` (identity check), or just assert `genizah_core.X` has the expected names?**
   - What we know: `test_no_back_edges_core.py` does the full `shared.X is genizah_core.X` identity check.
   - Recommendation: The new facade test should do the same identity checks (not just `hasattr`) to be meaningful. It can be slimmer than the back-edge file — one `is` assertion per shared module (13 assertions total), no standalone-import smoke tests (those stay in the back-edge file).

---

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| PyQt6 | desktop/update_ui.py construction | ✓ | project dep | — |
| pytest | New behavioral tests | ✓ | project dep | — |
| Python ast (stdlib) | test_no_back_edges_desktop.py | ✓ | stdlib | — |
| genizah_core (facade) | test_genizah_core_facade.py | ✓ | current HEAD | — |

All dependencies available. No missing dependencies.

---

## Validation Architecture

> `workflow.nyquist_validation` is `true` in `.planning/config.json` — this section is required.

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest 9.0.2 |
| Config file | `pytest.ini` / `pyproject.toml` in repo root |
| Quick run command | `pytest tests/test_update_ui_coordination.py tests/test_no_back_edges_desktop.py tests/test_genizah_core_facade.py -x` |
| Full suite command | `pytest tests/ -m "not gui and not render_smoke and not scale" -x` then `pytest tests/ -m gui -x` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| DESK-08 | `desktop/update_ui.py` imports cleanly; 4 classes resolve; `genizah_app.X is desktop.update_ui.X` | unit | `python -c "import desktop.update_ui; import genizah_app; assert genizah_app.UpdateNotificationBar is desktop.update_ui.UpdateNotificationBar"` | ❌ Wave 0 |
| DESK-08 | `_reset_sidecar_connections` calls all three services + catalog filter | unit | `pytest tests/test_update_ui_coordination.py -x` | ❌ Wave 0 |
| DESK-08 | `_download_next_sidecar` pops queue + fires SidecarDownloadThread | unit | `pytest tests/test_update_ui_coordination.py -x` | ❌ Wave 0 |
| DESK-08 | `_on_sidecar_download_finished` advances queue | unit | `pytest tests/test_update_ui_coordination.py -x` | ❌ Wave 0 |
| GUARD-02 | Full suite green (zero-behavior-change) | integration | `pytest tests/ -m "not gui and not render_smoke and not scale" -x` | ✅ existing |
| GUARD-03 | `test_telemetry_consent_ux.py` passes after D1 caller retarget | unit | `pytest tests/test_telemetry_consent_ux.py -x` | ✅ existing |
| GUARD-03 | `test_privacy_disclosure_strings.py` passes after OR-location flip | unit | `pytest tests/test_privacy_disclosure_strings.py -x` | ✅ existing |
| GUARD-03 | `test_tabular_builder_rtl.py` still passes (unchanged) | unit | `pytest tests/test_tabular_builder_rtl.py -x` | ✅ existing |
| GUARD-04 | No `desktop/` module imports `genizah_app` at module level | unit | `pytest tests/test_no_back_edges_desktop.py -x` | ❌ Wave 0 |
| GUARD-04 | `genizah_core.X is shared.Y.X` for all 20 facade names | unit | `pytest tests/test_genizah_core_facade.py -x` | ❌ Wave 0 |

### Sampling Rate

- **Per task commit:** `pytest tests/test_update_ui_coordination.py tests/test_no_back_edges_desktop.py tests/test_genizah_core_facade.py tests/test_telemetry_consent_ux.py tests/test_privacy_disclosure_strings.py tests/test_tabular_builder_rtl.py -x`
- **Per wave merge:** `pytest tests/ -m "not gui and not render_smoke and not scale" -x`
- **Phase gate:** Full suite (bulk + gui slices) green before `/gsd-verify-work`

### Wave 0 Gaps

- [ ] `tests/test_update_ui_coordination.py` — DESK-08 behavioral tests for sidecar coordination methods
- [ ] `tests/test_no_back_edges_desktop.py` — GUARD-04 AST guard for desktop/ back-edges
- [ ] `tests/test_genizah_core_facade.py` — GUARD-04 permanent facade identity assertions
- [ ] `desktop/update_ui.py` — the extracted module itself (created in Wave 1 execution)

*(Existing test infrastructure covers GUARD-02/03 via existing files.)*

---

## Security Domain

No security-relevant changes in this phase. This is a pure code reorganization with zero behavior change. No authentication, authorization, input validation, cryptography, or session management is affected. ASVS categories V2-V6 are not applicable.

---

## Sources

### Primary (HIGH confidence)

- Live codebase grep (2026-06-26): `genizah_app.py` line numbers for all 4 class definitions, sidecar method cluster, `self.*` reference map — all VERIFIED.
- `tests/test_no_back_edges_core.py` (full read) — template for `test_no_back_edges_desktop.py` AST guard.
- `.planning/phases/126-desktop-panels/126-VERIFICATION.md` (full read) — confirms D1 recipe that Phase 127 repeats.
- `.planning/seeds/SEED-020-decomposition-map.md` §7 C-6 — authoritative strategy for update_ui extraction.
- `.planning/REQUIREMENTS.md` (full read) — DESK-08, GUARD-02/03/04 definition.
- `.planning/phases/127-update-ui-final-cleanup/127-CONTEXT.md` (full read) — locked decisions and all 6 research directives.
- `.planning/ROADMAP.md` Phase 127 section — success criteria SC#1–5.

### Secondary (MEDIUM confidence)

- `genizah_app.py` lines 24316–24480 (full read) — coordination method cluster analyzed for `self.*` coupling.
- `desktop/settings_dialogs.py:1292` — `btn_check_updates` cross-ownership confirmed.
- `tests/test_telemetry_consent_ux.py` — confirmed exactly 4 `genizah_app.SettingsDialog` references at lines 522/589/653/722.
- `tests/test_privacy_disclosure_strings.py` (full read) — confirmed OR-location with Phase-127 flip comment.

### Tertiary (LOW confidence)

None. All claims verified against live codebase.

---

## Metadata

**Confidence breakdown:**
- Class extraction scope (4 classes, lines confirmed): HIGH — verified by grep ^class
- Coordination method coupling verdict (defer method extraction): HIGH — verified by self.* analysis + btn_check_updates cross-ownership
- D1 shim retirement caller set: HIGH — verified by grep -rn across entire tests/ tree
- Desktop module list for back-edge guard: HIGH — verified by ls desktop/*.py
- test_genizah_core_facade.py non-existence: HIGH — verified by ls
- Behavioral test shape for coordination methods: MEDIUM — patterned on test_telemetry_consent_ux.py __new__ approach; actual test execution unverified (headless)

**Research date:** 2026-06-26
**Valid until:** 2026-07-10 (stable codebase; only invalidated if additional desktop modules are added before planning completes)

---

## RESEARCH COMPLETE

**Phase:** 127 - Update UI & Final Cleanup
**Confidence:** HIGH

### Key Findings

- All 4 update-UI classes (`UpdateNotificationBar` line 184, `WhatsNewBar` line 243, `WhatsNewDialog` line 295, `UpdateProgressDialog` line 364) are confirmed top-level `^class` definitions — clean MOVE-and-shim candidates, identical to the D1 pattern from Phase 126.
- **THE CRUX VERDICT**: The sidecar reset/download coordination (~13 GenizahGUI methods, lines 24316–24480) is MODERATELY ENTANGLED. `self.btn_check_updates` is cross-owned by SettingsDialog (not GenizahGUI's own init), and the methods collectively span 4 ownership domains. RECOMMENDATION: **extract only the 4 classes; keep all coordination methods on GenizahGUI; write new DESK-08 behavioral tests against the methods in place**. Do NOT force a risky method-move under the zero-behavior-change constraint.
- The only external caller of the 9 D1 classes is `tests/test_telemetry_consent_ux.py` (4x `genizah_app.SettingsDialog` attribute-access) — trivially retargetable to `desktop.settings_dialogs.SettingsDialog`. All 9 D1 classes are also used by genizah_app.py itself, so the D1 import lines STAY (only the `# noqa: F401` suffix is removed).
- Zero existing tests reference the 4 update_ui class names — no GUARD-03 retarget needed for DESK-08 specifically.
- `tests/test_no_back_edges_desktop.py` must scan 19 modules (18 existing + new `update_ui.py`). The only existing genizah_app import in desktop/ is `join_workbench.py:4135` — LAZY (inside function body), correctly not flagged by the scope-aware guard.
- `tests/test_genizah_core_facade.py` does not exist; the 20 identity assertions live embedded in `test_no_back_edges_core.py`. Phase 127 creates the new file per ROADMAP SC#3.

### File Created

`.planning/phases/127-update-ui-final-cleanup/127-RESEARCH.md`

### Confidence Assessment

| Area | Level | Reason |
|------|-------|--------|
| Update_ui class extraction shape | HIGH | Confirmed by grep; D1 recipe proven in Phase 126 |
| Coordination coupling verdict | HIGH | Verified by self.* map + btn_check_updates cross-ownership discovery |
| D1 shim retirement (callers + import retention) | HIGH | Verified by grep across all tests/ and genizah_app.py usage check |
| Desktop modules for back-edge guard | HIGH | Verified by ls desktop/*.py |
| Behavioral test approach | MEDIUM | Pattern from Phase 116; actual QApplication behavior unverified headless |

### Open Questions

- Whether `test_update_ui_coordination.py` needs `_GUI_TEST_FILES` registration (start without; add if QApplication errors appear)
- Whether `test_genizah_core_facade.py` should duplicate the full identity assertions or be a thin import-delegate to `test_no_back_edges_core.py`'s existing assertions

### Ready for Planning

Research complete. Planner can create PLAN.md files.
