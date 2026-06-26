# Phase 126: Desktop Panels - Research

**Researched:** 2026-06-26
**Domain:** Python/PyQt6 god-class decomposition (pure internal refactor, zero behavior change)
**Confidence:** HIGH (codebase-verified; live grep against `genizah_app.py` @ base `aa215b37`)

<user_constraints>
## User Constraints (from 126-CONTEXT.md)

### Locked Decisions (do NOT re-litigate)
- **Scope = D1–D5 (seven panels, five plan-clusters).** Discuss-phase SKIPPED (no genuine user-facing gray areas; pure internal refactor). [VERIFIED: 126-CONTEXT.md]

| Plan | Cluster → module(s) | Risk | Dep | GUARD-03 retarget tests |
|------|---------------------|------|-----|--------------------------|
| D1 | Settings/Help/Tabular dialogs → `desktop/settings_dialogs.py` **+** table/header/scroll widgets → `desktop/ui_widgets.py`. Give `GenizahGUI` a clean `apply_settings`/`cancel_settings` API. **D-07b telemetry snapshot stripping is load-bearing — preserve.** | medium | none | `test_telemetry_consent_ux.py`, `test_tabular_builder_rtl.py` |
| D2 | Catalog "Browse-by-Identification" tab → `desktop/catalog_browse.py`. `_CatalogRefreshWorker` **stays module-level** (pyqtSignal). | low | none | `test_seed023_catalog_filters.py`, `test_catalog_availability_filter.py` |
| D3 | Search results lifecycle → `desktop/search_results_panel.py` (`SearchResultsPanel(QWidget)`; **inject** `searcher`/`meta_mgr`; **expose** signals). **NEW direct test `test_search_results_panel.py`** (mock `SearchThread`). | low | none | new test + existing API/E2E via shim |
| D4 | Browse panel → `desktop/browse_panel.py`; reading desk → `desktop/reading_desk_panel.py` (split `_browse_rd_*`). | medium | **D3** (shared `browse_text` widget) | `test_browse_state.py`, `test_browse_synthetic.py`, `test_local_browse_panel.py`, `test_wr01_open_local_browse_page_ast.py` |
| D5 | Lists tab → `desktop/lists_tab.py` (+ `_ListsSyncCoordinator` helper). | medium | core `ListsManager` (already extracted Phase 123) | `test_add_to_list_dialog_ui_context.py`, `test_user_lists_*.py` |

### Recipe & invariants (locked)
- **copy-not-move → retarget lazy imports → minimal `# noqa: F401` re-export shim** in `genizah_app.py`; one cohesive cluster = one atomic commit. **Shim deletion happens in Phase 127** (not here).
- **desktop/ shims are TEMPORARY** — deleted in Phase 127 (contrast: `genizah_core.py` facade is permanent). Phase 126 leaves `genizah_app.py` re-exporting each panel class.
- **GUARD-04 (this phase):** `genizah_app.py` re-exports each panel class so all current `from genizah_app import …` callers keep working. Verify with a base-vs-HEAD name diff.
- **GUARD-01 (desktop side):** no `desktop/` module imports `genizah_app` at module level (lazy function-body imports only, `# noqa: PLC0415`). The dedicated AST guard `test_no_back_edges_desktop.py` is installed in Phase 127, but keep the invariant from the first extraction.
- **GUARD-02:** zero behavior change; full suite green; 6-env pytest baseline unchanged.
- **pyqtSignal-bearing worker classes stay at module level** in their new `desktop/` home.
- **Never repo-wide `ruff --fix`** (strips `# noqa: F401` shims) — per-file ruff review on each extraction commit.
- **gui-test split is LOAD-BEARING here.** Run `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen`, never `-n auto`; add `test_search_results_panel.py` to conftest `_GUI_TEST_FILES`.
- **Coupling pattern (v7.9 idiom):** panels receive injected dependencies and expose signals back; `GenizahGUI` keeps thin coordinator APIs. Never import `desktop`/`genizah_app` into `shared/`.

### Claude's Discretion
- The exact boundary design (injected-dependency + exposed-signal + thin-GenizahGUI-API) per cluster. **If research surfaces a genuine user-facing choice, pause then.** (None surfaced — see Open Questions.)

### Deferred Ideas (OUT OF SCOPE)
- **E2 — Composition/Parallels/Lab tab** → `desktop/composition_tab.py`: infeasible until a `CompositionState` dataclass refactor lands (DEFER-02/DEFER-03).
- **E3 — Startup + session/history remainder**: ~50 `self.*` tab couplings (DEFER-04).
- **E1 — `desktop/update_ui.py`** + all shim deletions + `test_no_back_edges_desktop.py` AST guard + final full-suite sign-off: **Phase 127**.
- Any web (`web/`) change; any behavior change.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| DESK-01 | Settings/Help/Tabular dialogs → `desktop/settings_dialogs.py` | §Cluster D1 — exact class lines + `apply/cancel_settings` API + D-07b snapshot strip code located |
| DESK-02 | Table/header/scroll widget classes → `desktop/ui_widgets.py` | §Cluster D1 — `ShelfmarkTableWidgetItem`/`CheckBoxHeader`/`HiddenScrollArea` lines + `ListsTreeWidget` candidate |
| DESK-03 | Catalog Browse-by-Identification tab → `desktop/catalog_browse.py` | §Cluster D2 — `create_catalog_browse_tab` + 38 `_catalog_*` methods + `_CatalogRefreshWorker` + module-level `_CATALOG_FILTER_SETS` cache |
| DESK-04 | Search-results lifecycle → `desktop/search_results_panel.py` | §Cluster D3 — `SearchResultsPanel(QWidget)` boundary; 109-attr coupling map; `SearchThread` already in `gui_threads.py` |
| DESK-05 | Browse panel → `desktop/browse_panel.py` | §Cluster D4 — `create_browse_tab` + 61 `browse_*`/`_browse_*` methods + `browse_text` shared widget (D3-before-D4 reason confirmed) |
| DESK-06 | Reading desk → `desktop/reading_desk_panel.py` | §Cluster D4 — 12 `_browse_rd_*` methods + `shared/reading_desk_model.py` (already extracted) |
| DESK-07 | Lists tab + cloud-sync → `desktop/lists_tab.py` | §Cluster D5 — `create_lists_tab` + 47 `lists_*` methods + `_ListsSyncCoordinator` (cloud-sync gate + class-level debounce state lines 13406-13407) |
| GUARD-02 | Zero behavior change; full suite green | §Validation Architecture; §Common Pitfalls |
| GUARD-03 | Source-scanning tests retargeted before deletion | §GUARD-03 Source-Scanning Test Audit — only 6 of 10 named tests actually scan `genizah_app`; 4 are web-side false positives |
| GUARD-04 | `genizah_app.py` re-exports each panel class; all callers green | §GUARD-04 Importer Audit — actual `from genizah_app import` callers import FUNCTIONS not panel classes |
</phase_requirements>

## Summary

Phase 126 extracts five desktop UI clusters out of the 28,033-line `genizah_app.py` (the file is dominated by one god-class, `GenizahGUI`, spanning lines 3357→EOF with **603 methods**). The proven v7.9 recipe — copy-not-move, retarget lazy imports, add a `# noqa: F401` re-export shim, delete in the *next* phase — is in active use in this repo (`desktop/puzzle.py`, `desktop/viewers.py`, `desktop/my_library_tab.py`, `desktop/join_workbench.py`, etc.). The single best template is **`desktop/my_library_tab.py`**: a self-contained `QWidget` tab that takes a `parent` GenizahGUI reference, exposes core engines through deferred `@property` accessors (`search_engine` reads `parent.searcher`), holds its own module-level `pyqtSignal` worker classes, and contains **zero** `genizah_app` imports.

**The crux finding — coupling is heavily asymmetric across the five clusters, and SEED-020's risk ratings reflect TEST COVERAGE, not structural coupling.** Two clusters are genuinely self-contained tab factories (D2 catalog: 100 distinct `self.*`, all `self._catalog_*`-namespaced; D5 lists: 161 `self.*`, all `lists_*`/`_lists_*`-namespaced) and map cleanly to `QWidget` panels with a back-ref to GenizahGUI for cross-tab navigation. **D3 (search results) is the structurally hardest cluster despite its "low" rating** — `on_search_finished` alone touches **109 distinct `self.*` names** including ~30 sibling widgets (`results_table`, `search_progress`, `chk_search_header`, `status_label`, `results_stack`) and ~40 cross-cluster method calls. D1 dialogs are simple top-level classes (the v7.9 sweet spot). D4 browse shares the `browse_text` widget with D3 (hence D3-before-D4).

**Primary recommendation:** Extract D1 (dialogs/widgets — pure top-level classes, exactly mirroring v7.9) and D2 (catalog — clean namespaced tab factory) FIRST as the de-risk spine. For D3/D4/D5, the cluster logic lives as `GenizahGUI` *methods*, not separable classes — so the extraction is **NOT** a literal copy of methods into a free-standing class. Use the `MyLibraryTab` model: extract a `*Panel(QWidget)` that **owns its widgets and state**, takes `parent`-injected core deps via deferred properties, exposes `pyqtSignal`s for cross-tab events, and leaves GenizahGUI a thin coordinator. The panel methods become panel-instance methods (`self.` rebinds from GenizahGUI to the panel); GenizahGUI keeps delegating wrappers where other clusters still call in. Because the move is copy-not-move and deletion is Phase 127, **every source-scanning GUARD-03 test stays green automatically in 126** (the implementation is still in `genizah_app.py`); the retarget work in 126 is purely *additive* (scan both locations), and the *flip* to the new location happens at deletion in 127.

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Settings/Help/Tabular dialogs (D1) | Desktop UI (`desktop/settings_dialogs.py`) | — | Modal dialogs; pure presentation + config read/write via module-level `load_app_config`/`save_app_config` |
| Table/header/scroll widgets (D1) | Desktop UI (`desktop/ui_widgets.py`) | — | Reusable QWidget subclasses, no business logic |
| Catalog browse tab (D2) | Desktop UI (`desktop/catalog_browse.py`) | Shared services (`shared/fjms_service`) | Tab presents FJMS catalog data; queries run in module-level `_CatalogRefreshWorker` (QThread) |
| Search-results lifecycle (D3) | Desktop UI (`desktop/search_results_panel.py`) | Core engine (`shared/search_engine.SearchEngine` injected) | Renders + filters results; search executes in `gui_threads.SearchThread`; enrichment via `meta_mgr` |
| Browse panel (D4) | Desktop UI (`desktop/browse_panel.py`) | Core + shared services | Folio navigation + transcription render; PGP/FJMS/NLI enrichment via injected `meta_mgr` |
| Reading desk (D4) | Desktop UI (`desktop/reading_desk_panel.py`) | Shared model (`shared/reading_desk_model.py` — already extracted) | Multi-entry side-by-side view; state in `ReadingDeskState` dataclass |
| Lists tab + cloud sync (D5) | Desktop UI (`desktop/lists_tab.py`) | Core (`shared/lists_manager.ListsManager` injected) + Supabase (`corrections_client`) | Tab CRUD over `lists_mgr`; `_ListsSyncCoordinator` gates cloud writes |

**Tier-correctness note for the planner:** No capability in this phase belongs in `shared/`. Every extracted module is desktop-tier (`desktop/`). The injected dependencies (`searcher`, `meta_mgr`, `lab_engine`, `lists_mgr`, `corrections_client`) are constructed by GenizahGUI and reach the panels by `parent`-reference or constructor injection — the panels NEVER construct engines and NEVER import `shared/` engine modules to construct them (they receive instances). This direction (`desktop/panel → injected shared instance`) has no back-edge and needs no `shared/` change.

## Standard Stack

This is a pure internal refactor — **no new packages are installed.** The phase operates entirely within the existing toolchain.

### Core (verified installed)
| Tool | Version | Purpose | Why Standard |
|------|---------|---------|--------------|
| Python | 3.11.9 | Runtime | Project baseline (3.10+ per CLAUDE.md) [VERIFIED: `python --version`] |
| PyQt6 | 6.10.2 (Qt 6.10.0) | Desktop UI framework | All extracted classes are `QWidget`/`QDialog`/`QThread` subclasses [VERIFIED: `python -c "import PyQt6.QtCore"`] |
| ruff | 0.15.10 | Per-file lint (shim integrity) | Project gate; **never repo-wide `--fix`** [VERIFIED: `python -m ruff --version`] |
| pytest | 9.0.2 | Test runner | Existing suite; gui-test marker split in `tests/conftest.py` [VERIFIED: pyc filenames `cpython-311-pytest-9.0.2`] |

### Supporting (existing, in play)
| Module | Purpose | When to Use |
|--------|---------|-------------|
| `gui_threads.py` | Module-level `pyqtSignal` worker QThreads (`SearchThread`, `LabSearchThread`, `CompositionThread`, `RefinementReplayThread`, `IndexerThread`) | D3's `SearchThread` is ALREADY here — the new `test_search_results_panel.py` mocks it; no worker extraction needed for D3 [VERIFIED: `gui_threads.py:81`] |
| `shared/reading_desk_model.py` | `ReadingDeskEntry` + `ReadingDeskState` dataclasses | D6 reading desk state already lives here [VERIFIED: `shared/reading_desk_model.py:16,33`] |
| `shared/lists_manager.py` | `ListsManager` (extracted Phase 123) | D5 panel injects this; `self.lists_mgr = ListsManager(self.meta_mgr)` at `genizah_app.py:3560` [VERIFIED: grep] |
| `desktop/widgets/` | Already-extracted line-number text-edit + widgets package | D1 `ui_widgets.py` is the sibling; check for name collisions |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| `*Panel(QWidget)` owning widgets (MyLibraryTab model) | `*Mixin` class GenizahGUI multiple-inherits | **REJECTED** — no mixin pattern exists in this codebase today (`grep -i mixin` on `genizah_app.py` = 0 hits); introducing one is a larger blast radius than the v7.9 panel pattern and risks MRO/`self.*` surprises across 600 methods. The QWidget-panel + back-ref model is the proven local idiom. |
| Constructor injection of `searcher`/`meta_mgr` | Deferred `@property` reading `parent.searcher` | Use **deferred property** for engines (they're assigned async in `on_startup_finished`, AFTER panel construction — see `MyLibraryTab.search_engine` property at `my_library_tab.py:1161`). Use **constructor injection** only for deps available at construction time. D3's CONTEXT says "inject searcher/meta_mgr" — reconcile: inject the GenizahGUI parent, expose engines via property (matches the live MyLibraryTab pattern; engines are None at tab-build time). |

**Installation:** None. No `## Package Legitimacy Audit` section is needed (zero external packages).

## Architecture Patterns

### System Architecture Diagram

```
                         GenizahGUI(QMainWindow)  [genizah_app.py:3357]
                         ── constructs core deps (async, on_startup_finished) ──
                         self.searcher / meta_mgr / var_mgr / lab_engine /
                         self.lists_mgr / corrections_client / indexer
                                          │
            ┌─────────────────────────────┼──────────────────────────────────┐
            │ parent-ref injection         │ thin coordinator API              │ pyqtSignal (panel→GUI)
            ▼                              ▼                                   ▲
   ┌──────────────────┐          ┌──────────────────┐               ┌──────────────────┐
   │ Top-level dialogs│          │  *Panel(QWidget) │               │ cross-tab events │
   │ (D1 — v7.9 idiom)│          │  tabs (D2/D3/D4/  │  ───emit───►  │ (open-in-browse, │
   │ SettingsDialog,  │          │  D5)              │               │  switch-tab,     │
   │ HelpDialog,      │          │ owns widgets +   │               │  add-to-list)    │
   │ TabularQuery…    │          │ state + module-  │               └──────────────────┘
   │ takes parent ──► │          │ level workers    │
   │ self.main_win    │          └────────┬─────────┘
   └──────────────────┘                   │ injected instances (NOT constructed here)
                                          ▼
                    ┌────────────────────────────────────────────────┐
                    │ shared/ (engines, services) — NO back-edge      │
                    │ SearchEngine, LabEngine, ListsManager,          │
                    │ MetadataManager, fjms_service, reading_desk_model│
                    └────────────────────────────────────────────────┘
                                          ▲
                    ┌─────────────────────┴──────────────────────┐
                    │ module-level pyqtSignal workers (stay top-  │
                    │ level): gui_threads.SearchThread (D3),      │
                    │ _CatalogRefreshWorker (D2, moves with tab)  │
                    └─────────────────────────────────────────────┘
```

A reader traces D2 catalog: user clicks domain → `_catalog_start_async_refresh` spawns `_CatalogRefreshWorker(QThread)` → worker queries `shared/fjms_service` off-thread → `done` signal → `_catalog_on_async_refresh_done` renders into the panel's own `catalog_results_table`. "Search in these results" emits a cross-tab event → GenizahGUI sets `pre_search_filters` + `_set_active_tab(0)`.

### Recommended Module Layout
```
desktop/
├── settings_dialogs.py      # D1: SettingsDialog, SearchSettingsDialog, HelpDialog,
│                            #     TabularQueryBuilderDialog, LabScoringDialog (+ LabPanel?)
├── ui_widgets.py            # D1: ShelfmarkTableWidgetItem, CheckBoxHeader,
│                            #     HiddenScrollArea, ListsTreeWidget
├── catalog_browse.py        # D2: CatalogBrowsePanel(QWidget) + _CatalogRefreshWorker
│                            #     + module-level _CATALOG_FILTER_SETS cache + helpers
├── search_results_panel.py  # D3: SearchResultsPanel(QWidget) — owns results_table etc.
├── browse_panel.py          # D4: BrowsePanel(QWidget) — owns browse_text etc.
├── reading_desk_panel.py    # D4: ReadingDeskPanel (or mixin onto BrowsePanel)
└── lists_tab.py             # D5: ListsPanel(QWidget) + _ListsSyncCoordinator
```

### Pattern 1: Parent-ref + deferred engine property (THE local idiom)
**What:** Panel takes `parent` (the GenizahGUI), stashes `self._parent_window = parent`, and exposes core engines via `@property` that reads the parent lazily (engines are None at tab-build time, assigned later in `on_startup_finished`).
**When to use:** D2/D3/D4/D5 panels.
```python
# Source: desktop/my_library_tab.py:1161 (live codebase)
class MyLibraryTab(QWidget):
    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self._parent_window = parent
        # ... build own widgets ...

    @property
    def search_engine(self):
        if self._parent_window is None:
            return None
        return getattr(self._parent_window, "searcher", None)
```

### Pattern 2: Module-level pyqtSignal worker (NEVER nested)
**What:** QThread subclasses with `pyqtSignal` class attributes must be defined at module level, not inside a method/class.
**When to use:** `_CatalogRefreshWorker` (D2) moves WITH the catalog module as a module-level class.
```python
# Source: genizah_app.py:1453 (existing _CatalogRefreshWorker)
class _CatalogRefreshWorker(QThread):
    """Must be module-level (not nested inside a method) so pyqtSignal works
    reliably in PyQt6."""
    done = pyqtSignal(object)
```
Note: D3's worker (`SearchThread`) is ALREADY in `gui_threads.py:81` — no D3 worker extraction.

### Pattern 3: Constructor injection for construction-time deps (ResultDialog model)
**What:** When a dependency IS available at construction, inject it directly.
```python
# Source: desktop/result_dialog.py:48
class ResultDialog(QDialog):
    def __init__(self, parent, all_results, current_index, meta_mgr, searcher):
        ...
# Source: desktop/join_workbench.py:4900
class JoinWorkbenchWindow(QDialog):
    def __init__(self, parent, app):
        self.meta_mgr = app.meta_mgr   # engines already built when window opens
        self.searcher = app.searcher
```

### Pattern 4: thin GenizahGUI coordinator API (D1's `apply_settings`/`cancel_settings`)
**What:** When extracting a dialog that today calls back into GenizahGUI via many `self.main_win.<method>` paths, give GenizahGUI a small, named API surface the dialog calls — instead of the dialog reaching into arbitrary GUI internals.
**When to use:** D1 `SettingsDialog` (CONTEXT mandates `apply_settings`/`cancel_settings`). Today `SettingsDialog` wires `combo_language.currentIndexChanged → self.main_win._on_language_combo_changed` (`genizah_app.py:2326`) — keep those callbacks behind a thin GUI method named in CONTEXT.

### Anti-Patterns to Avoid
- **Module-level `import genizah_app` in any `desktop/` module** — GUARD-01 violation. Use lazy function-body imports (`# noqa: PLC0415`). MyLibraryTab has ZERO `genizah_app` imports (verified).
- **Repo-wide `ruff --fix`** — strips the `# noqa: F401` re-export shims. Per-file review only.
- **Nesting a `pyqtSignal` worker inside a method** — breaks signal binding in PyQt6 (the `_CatalogRefreshWorker` docstring states this explicitly).
- **Constructing engines inside a panel** — panels RECEIVE instances; they never build `SearchEngine`/`MetadataManager` (would create a back-edge and double-construct).
- **Deleting the genizah_app.py implementation in Phase 126** — deletion is Phase 127. Copy-not-move keeps source-scanning tests green.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Cross-module test green-keeping | Manual symbol checklist | base-vs-HEAD NAME-level test diff + `from genizah_app import` name diff (the 124 lesson) | Count-based "0 new failures" is untrustworthy — Phase 124 mis-reported 3 real defects as "pre-existing" [VERIFIED: STATE.md line 44] |
| Worker thread for catalog DB query | New threading scaffold | Move existing `_CatalogRefreshWorker` as module-level class | Already correct; just relocate |
| Search worker for the new D3 test | New mock thread | Mock `gui_threads.SearchThread` | Already module-level with `results_signal`/`progress_signal`/`error_signal`/`perf_signal` [VERIFIED: gui_threads.py:81-89] |
| Reading-desk state object | New dataclass | `shared/reading_desk_model.ReadingDeskState` | Already extracted [VERIFIED: shared/reading_desk_model.py] |
| Config snapshot/restore on Settings cancel | Re-implement | Preserve the existing D-07b strip-then-restore (see Runtime State Inventory) | The strip is intentional and load-bearing |

**Key insight:** This phase builds nothing new. Every "implementation" already exists; the only engineering is drawing the panel↔GenizahGUI boundary so the moved code's `self.*` rebinds correctly and no back-edge is introduced.

## Cluster-by-Cluster Coupling Map (THE CRUX)

> Line ranges are from the live file at base `aa215b37` (`wc -l genizah_app.py` = 28,033). Per the standing lesson, **grep — do not trust line numbers**; they will drift as commits land. The class-level map below is anchored by `grep -n "^class "`.

### Top-level class inventory (lines verified by `grep -n "^class "` + range computation)
| Class | Lines | Cluster | Notes |
|-------|-------|---------|-------|
| `UpdateNotificationBar` | 182–240 | **127 (E1)** | NOT this phase |
| `WhatsNewBar` | 241–292 | **127 (E1)** | NOT this phase |
| `WhatsNewDialog` | 293–361 | **127 (E1)** | NOT this phase |
| `UpdateProgressDialog` | 362–595 | **127 (E1)** | NOT this phase |
| `LabScoringDialog` | 596–696 | D1 (settings_dialogs) | Lab weights dialog |
| `SearchSettingsDialog` | 697–852 | D1 (settings_dialogs) | |
| `LabPanel` | 853–1091 | D1 (settings_dialogs OR composition-deferred?) | **OPEN Q — see Open Questions** |
| `ShelfmarkTableWidgetItem` | 1092–1098 | D2 (ui_widgets) | 6-line sort-key item |
| `CheckBoxHeader` | 1099–1335 | D2 (ui_widgets) | column-checkbox header |
| `HiddenScrollArea` | 1336–1452 | D2 (ui_widgets) | |
| `_CatalogRefreshWorker` | 1453–1519 | **D2 (catalog_browse)** | module-level pyqtSignal worker — moves with catalog |
| `HelpDialog` | 1520–1582 | D1 (settings_dialogs) | |
| `ListsTreeWidget` | 1583–1604 | D2 (ui_widgets) OR D5 | 21-line tree subclass — used by lists tab |
| `TabularQueryBuilderDialog` | 1605–2217 | D1 (settings_dialogs) | 612 lines; `test_tabular_builder_rtl` pins it |
| `SettingsDialog` | 2218–3356 | **D1 (settings_dialogs)** | 1138 lines; **D-07b strip at 2251-2271** |
| `GenizahGUI` | 3357–EOF | (the god-class — D2/D3/D4/D5 method clusters live inside) | 603 methods |

**Note on SEED-020's D1 class list:** SEED-020 §1 lists `LabPanel` under D1 (`desktop/settings_dialogs.py` + `ui_widgets.py`). But `LabPanel` (`853-1091`) is a composition/Lab-scoring panel that couples to `set_engine` parent-traversal (SEED-020 §7 C-6 flags it as part of the DEFERRED composition cluster: *"`LabPanel.set_engine` stops parent-traversing"* is an E2 prerequisite). **Planner must resolve:** keep `LabPanel` in `genizah_app.py` (defer to E2) OR move it to `ui_widgets.py`. Recommend DEFER to E2 with composition tab — see Open Questions Q1.

### Core injected dependencies (assigned on GenizahGUI, read by panels)
All assigned in `GenizahGUI.__init__` / `on_startup_finished`:
- `self.searcher` (SearchEngine) — line 3379 init=None, 3539 assigned [VERIFIED]
- `self.meta_mgr` (MetadataManager) — 3377 / 3537
- `self.var_mgr` (VariantManager) — 3378 / 3538
- `self.lab_engine` (LabEngine) — 3381 / 3567
- `self.lists_mgr` (ListsManager) — 3382 / 3560 (`ListsManager(self.meta_mgr)`)
- `self.indexer` (Indexer) — 3380 / 3540
- `self.corrections_client` — 3386 (`get_corrections_client()`)

These are None at tab-build time (assigned async in `on_startup_finished`). **Use deferred `@property`, not constructor injection** for the engines (matches `MyLibraryTab.search_engine`). `corrections_client` IS available at construction.

### Cross-tab navigation surface (panel → GenizahGUI thin API)
- `self._set_active_tab(idx)` — `genizah_app.py:4130` — switches `self.tabs` (QTabWidget at 3961). Tab order: 0=Search, 1=Composition, 2=Browse, 3=Catalog(Browse-by-ID), 4=Lists, 5=Community, 6=My Library [VERIFIED: genizah_app.py:3968-3975].
- `self.pre_search_filters` / `self.pre_search_restrict_sys_ids` — set by catalog "search in these results", consumed by search panel.
- `self._update_filter_chip_bar()` — search-panel method called by catalog.

**Boundary design:** each panel takes `parent` (GenizahGUI); cross-tab actions either (a) call a thin named GenizahGUI method (`_set_active_tab`, `apply_pre_search_filters`) or (b) emit a `pyqtSignal` the GUI connects. Prefer signals for events, named methods for queries/commands already public.

---

### Cluster D1 — Dialogs + Widgets (medium risk, no dep)
**Classes (top-level, the v7.9 sweet spot):**
- `desktop/settings_dialogs.py`: `SettingsDialog` (2218-3356), `SearchSettingsDialog` (697-852), `HelpDialog` (1520-1582), `TabularQueryBuilderDialog` (1605-2217), `LabScoringDialog` (596-696).
- `desktop/ui_widgets.py`: `ShelfmarkTableWidgetItem` (1092-1098), `CheckBoxHeader` (1099-1335), `HiddenScrollArea` (1336-1452), `ListsTreeWidget` (1583-1604).

**Coupling (`SettingsDialog`):**
- Reads/writes: module-level `load_app_config()` / `save_app_config()` (defined in `genizah_app.py`; the dialog calls them un-prefixed → must lazy-import them from `genizah_app` inside the new module, OR they move too). [VERIFIED: genizah_app.py:2269,2298]
- `self.main_win = parent` (the GenizahGUI). Wires callbacks: `combo_language.currentIndexChanged → self.main_win._on_language_combo_changed` (2326) and many more `self.main_win.*` in `_build_general_tab`.
- `tr`, `CURRENT_LANG` — from `genizah_core` (lazy import, as `MyLibraryTab` does: `from genizah_core import Config, tr, CURRENT_LANG`).
- **D-07b telemetry snapshot stripping (load-bearing)** — see Runtime State Inventory below.

**Boundary design:** `SettingsDialog(parent)` keeps `self.main_win = parent`; introduce the CONTEXT-mandated thin `GenizahGUI.apply_settings()` / `cancel_settings()` so the dialog's OK/Cancel paths call a named API rather than reaching into GUI internals. `load_app_config`/`save_app_config` stay module-level in `genizah_app.py` and are lazy-imported (they are config helpers, not panel code; do not move in this phase).

**Worker classes carrying `pyqtSignal`:** none in D1.

---

### Cluster D2 — Catalog Browse-by-Identification (low risk, no dep) — RECOMMENDED FIRST
**Build:** `create_catalog_browse_tab` (11772) returns a `QWidget` assigned to `self.catalog_browse_tab` (3965), added as tab index 3 (3971).
**Methods (38):** all `_catalog_*` / `create_catalog_browse_tab` (lines 11772–12951): `_catalog_refresh`, `_catalog_start_async_refresh`, `_catalog_on_async_refresh_done`, `_catalog_populate_tree`, `_catalog_render_tree`, `_catalog_view_result_by_row`, `_catalog_search_in_results`, `_catalog_parallels_in_results`, `_catalog_cycle_pgp_filter`, `_catalog_cycle_editions_filter`, `_catalog_build_browse_filters`, etc. [VERIFIED: grep method list]
**Module-level worker:** `_CatalogRefreshWorker` (1453-1519, `done = pyqtSignal(object)`) — moves with the module, stays module-level.
**Module-level state/functions (referenced by tests):** `_CATALOG_FILTER_SETS = {'value': None}` (1415) + `_CATALOG_FILTER_SETS_LOCK` (1416) + `_get_catalog_filter_sets()` (1419) + `reset_catalog_filter_sets()` (1445). `test_catalog_availability_filter.py` does `import genizah_app; genizah_app._get_catalog_filter_sets()` / `genizah_app._CATALOG_FILTER_SETS` / `genizah_app.reset_catalog_filter_sets` [VERIFIED]. **These are module-level helpers, not panel methods** — keep them in `genizah_app.py` with a re-export shim if they move, so the `import genizah_app; genizah_app.X` path stays green. Safest: keep `_CATALOG_FILTER_SETS*` and the two functions in `genizah_app.py` (they are app-global caches), and have `catalog_browse.py` lazy-import them — OR move them and add `from desktop.catalog_browse import _get_catalog_filter_sets, reset_catalog_filter_sets, _CATALOG_FILTER_SETS  # noqa: F401` in `genizah_app.py`. **Critical:** a module-level mutable like `_CATALOG_FILTER_SETS` re-exported by `from X import name` creates a SECOND binding — mutations to `genizah_app._CATALOG_FILTER_SETS['value']` and `desktop.catalog_browse._CATALOG_FILTER_SETS['value']` must stay the SAME object. Since it's a dict (mutable container), `from … import _CATALOG_FILTER_SETS` aliases the same dict object — mutating `['value']` is shared; **reassigning** the name is NOT. The test only mutates `['value']` and calls the functions, so a same-object import shim is safe. Verify with an identity test (`genizah_app._CATALOG_FILTER_SETS is desktop.catalog_browse._CATALOG_FILTER_SETS`).
**Coupling:** core deps `searcher`, `meta_mgr` (read). Cross-tab: `pre_search_filters`, `pre_search_restrict_sys_ids`, `_update_filter_chip_bar`, `_set_active_tab` (4 calls). Opens `ResultDialog` (already in `desktop/result_dialog.py`). 100 distinct `self.*`, all `self._catalog_*`-namespaced (clean).
**Boundary design:** `CatalogBrowsePanel(QWidget)` owns all `_catalog_*` widgets/state; takes `parent`; deferred `searcher`/`meta_mgr` properties; cross-tab via `parent._set_active_tab` + `parent.pre_search_filters = …` (or a thin `parent.apply_catalog_search_filters(filters, tab)` method).
**GUARD-03 tests:** `test_seed023_catalog_filters.py` scans `web.pages.catalog_browse` via `inspect.getsource(cb)` — **WEB-side, NOT genizah_app** (false positive in CONTEXT's list; no retarget needed). `test_catalog_availability_filter.py` is `import genizah_app` runtime (no source scan) — stays green via the module-level shim above. Both are in `_GUI_TEST_FILES` already (`test_catalog_availability_filter.py` listed; `test_seed023…` is not — confirm it doesn't construct Qt).

---

### Cluster D3 — Search-results lifecycle (LOW per SEED-020, STRUCTURALLY HARDEST) — depends on nothing, sequence BEFORE D4
**Methods (the search-results lifecycle, scattered across GenizahGUI):** `on_search_finished` (18506-~18900), `_on_search_progress` (18240), `set_results_loading` (7210), `_apply_results_table_filters` (19333), `_apply_local_filter`/`_apply_local_optout_filter` (19116/19141), `_render_view_all_batch`/`_append_next_view_all_batch` (20854/20893), `_collect_sorted_results` (20170), `show_full_text_for_result` (20236), `open_result_in_browse_from_table` (20265), `search_add_selected_to_list`/`search_add_row_to_list` (20347/20374), the `_local_filter_*`/`_results_filter_*` family, `_toggle_all_terms_filter`, `_undo_zero_result_refine`, `_update_search_row_list_indicator`. [VERIFIED: grep]
**Widgets owned (created in `create_search_tab` @ 6591):** `results_table` (7056, 13 cols), `results_stack`, `results_placeholder`, `table_container`, `search_progress`, `status_label`, `chk_search_header`, `query_input`, `mode_combo`, `btn_domain_filter`, `lbl_search_export`, `export_buttons`, `refinement_strip`, `refine_badge`. [VERIFIED]
**Coupling magnitude:** `on_search_finished` alone references **109 distinct `self.*` names** (full list in Code Examples). Of these: ~14 column constants (`COL_*`), ~30 sibling widgets/state, ~40 method calls into the SAME cluster, plus cross-cluster calls (`open_result_in_browse_from_table` → D4 browse; `start_metadata_loading`, `_launch_enrichment_workers` → enrichment; `_add_regular_search_to_history`, `_schedule_session_save` → session/E3-deferred). `searcher` + `meta_mgr` read.
**Worker classes carrying `pyqtSignal`:** `SearchThread` — **already module-level in `gui_threads.py:81`** (`results_signal`/`progress_signal`/`error_signal`/`perf_signal`). No worker move needed.
**Boundary design (the hard one):** `SearchResultsPanel(QWidget)` owns `results_table` + the results widgets + the filter state (`results_filters`, `_printed_filter_state`, `_local_filter_*`, refinement chain). Inject `parent` (deferred `searcher`/`meta_mgr` properties). EXPOSE signals: `result_open_requested(dict)` (→ GUI opens in browse, the D3→D4 cross-edge), `add_to_list_requested(list)`, `composition_requested(dict)`. The methods that touch session/history (`_add_regular_search_to_history`, `_schedule_session_save`) and tab state belong to E3 (deferred) — for D3, leave those as GenizahGUI calls the panel makes via `parent.<method>` (lazy), because the session/restore code is out of scope (DEFER-04). **Risk flag for the planner:** because 40+ method calls cross between the search-results methods and other GenizahGUI methods, the cleanest 126 move may extract `SearchResultsPanel` as a panel that still delegates a handful of GenizahGUI calls via `self._parent_window.<method>` — accept that a fully-decoupled panel is an E3/DEFER-04 outcome, not a 126 goal. The 126 goal is: the search-results widgets + render/filter logic LIVE in `desktop/search_results_panel.py`, imported back into `genizah_app.py`.
**NEW test:** `tests/test_search_results_panel.py` — construct `SearchResultsPanel` headless, mock `gui_threads.SearchThread`, assert render/filter behavior. **MUST add to `_GUI_TEST_FILES`** in conftest (it constructs a QWidget + dispatches events). Run `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen`.
**GUARD-03:** none of the listed D3 source-scan tests; new test only.

---

### Cluster D4 — Browse panel + Reading desk (medium risk, DEPENDS ON D3)
**Why D3-before-D4 (CONFIRMED):** `browse_text` (the transcription `QTextEdit`) is created in `create_browse_tab` (`self.browse_text = QTextEdit()` @ 8155) but is ALSO written by search-result snippet highlighting paths. `apply_line_numbered_text(self.browse_text, …)` and `apply_find_highlight(self.browse_text, …)` are shared. The module docstring at line 122 states "typically `self.browse_text` via apply_line_numbered_text." Extracting D3 first establishes who owns the result→browse hand-off (`open_result_in_browse_from_table` @ 20265 → `open_result_in_browse` @ 20446 lives in the browse area). [VERIFIED]
**Browse methods (61):** `create_browse_tab` (7615), `browse_load` (25316), `browse_navigate` (25622), `browse_render_page` (25668), `_browse_change_version`/`_browse_load_version`/`_browse_display_version_text` (4805/4821/4913), `_populate_pgp_combo` (4981), `on_browse_enriched_loaded` (8783), `_start_browse_enrichment` (8415), `_build_browse_enriched_html` (9523), `toggle_browse_view_all`/`browse_load_all` (11274/11536), `browse_search_parallels` (11454), `_open_local_browse`/`_open_local_browse_page` (20648/20989), `fetch_browse_thumbnail` (26098), etc. Plus `browse_thumb_resolved = pyqtSignal(str, object)` (a GenizahGUI class-level signal @ 3359 used by browse thumbnail resolution).
**Reading desk methods (12 `_browse_rd_*`):** `_browse_enter_reading_desk` (10280), `_browse_rd_enrich_entry` (10388), `_browse_rd_render` (10862), `_browse_rd_render_images` (10991), `_browse_rd_setup_sync_scroll` (11147), `_browse_rd_add_entry` (10634), `_browse_rd_remove_entry` (11207), etc. State: `self.browse_reading_desk_state = ReadingDeskState()` (8212, 10471) — model already in `shared/reading_desk_model.py`.
**Coupling:** `meta_mgr`, `searcher` read; PGP/FJMS/NLI enrichment via `meta_mgr`; opens `FjmsCatalogDialog`/`ResultDialog` (already extracted). `browse_thumb_resolved` pyqtSignal must move WITH the browse panel (it's on GenizahGUI today; if the panel becomes the signal owner, rewire connections).
**Boundary design:** `BrowsePanel(QWidget)` owns `browse_text` + browse widgets + reading-desk widgets; deferred `meta_mgr`/`searcher`. Split reading desk into `reading_desk_panel.py` (either a sub-widget the BrowsePanel embeds, or a mixin — recommend a sub-widget given the no-mixin convention). The `browse_thumb_resolved` pyqtSignal moves onto the panel class.
**GUARD-03 source-scan tests (ALL scan genizah_app.py — stay green in 126 via copy-not-move; retarget additively):**
- `test_browse_synthetic.py` — scans `genizah_app.py` for `PNX_MANUSCRIPTS{sys_id}` + `PNX_MANUSCRIPTS{self.current_browse_sid}` (lines 316-317) AND many web files. **The genizah_app.py patterns stay present in 126** (copy-not-move). When the browse code moves to `desktop/browse_panel.py`, ADD `("desktop/browse_panel.py", r"PNX_MANUSCRIPTS…", N)` entries; flip the genizah_app.py entries at deletion in 127.
- `test_local_browse_panel.py` — AST-scans `genizah_app.py` for `GenizahGUI._open_local_browse` + `_get_local_full_text_for_sys_id` (lines 43-100). Stays green in 126; retarget at 127.
- `test_wr01_open_local_browse_page_ast.py` — AST-counts `_open_local_browse_page` definitions in `genizah_app.py` (line 24). **GUARD-03 named in ROADMAP.** Stays green in 126; flip at 127.
- `test_browse_state.py` — imports `web.pages.browse_state` — **WEB-side, NOT genizah_app** (false positive in CONTEXT's list; no retarget).
- Also affected (not in CONTEXT list but verified to scan browse methods): `test_desktop_folio_navigation.py` (GUARD-03 named in ROADMAP — reads `genizah_app.py` source @ 32-33, checks browse methods), `test_desktop_pending_corrections.py` (`_browse_load_version` @ 105), `test_fgp_chooser_integration.py` (`on_browse_enriched_loaded` + `_browse_refresh_pgp_for_page` @ 145,155), `test_view_all_cap.py` (`_VIEW_ALL_PAGE_CAP` + `_render_view_all_batch` — GUARD-03 named in ROADMAP), `test_view_all_incremental.py` (`_render_view_all_batch`/`_append_next_view_all_batch`).

---

### Cluster D5 — Lists tab + cloud sync (medium risk, depends on core ListsManager — already extracted Phase 123)
**Build:** `create_lists_tab` (13010) returns the QWidget → `self.lists_tab` (3966) → tab index 4 (3972).
**Methods (47 `lists_*`/`_lists_*`):** sidebar/tree (`lists_refresh_sidebar` 13476, `lists_refresh_items` 13602, `lists_handle_tree_reorder` 13564), CRUD (`lists_create_new_list` 14061, `lists_delete_current_list` 14094, `lists_merge_lists` 14125, `lists_cleanup_duplicates` 14160), item ops (`lists_move_selected_items`, `lists_add_tag_to_selected`, `lists_remove_selected_items`), preview (`_lists_load_preview` 13926, `_lists_load_preview_image` 13960), export (`_export_as_text`/`_json`/`_excel`/`_word` 14951-15033), `show_add_to_list_menu` (15132). Plus community-tab populators (`_populate_discoveries_list` 15569, `_populate_joins_list` 16106, etc.) — **these are Community tab, may be out of D5 scope; confirm cluster boundary with planner.**
**Cloud-sync coordination (the `_ListsSyncCoordinator` target):**
- `_enable_lists_cloud_sync` (4239), `_show_lists_sync_dialog` (4318), `_disable_lists_cloud_sync` (4484), `_lists_auto_sync` (13409).
- **Class-level debounce state on GenizahGUI:** `_auto_sync_pending = False` (13406), `_auto_sync_last = 0` (13407) — read/written via `self.__class__._auto_sync_*` in `_lists_auto_sync`. **These MUST move into `_ListsSyncCoordinator`** (as instance or class state on the coordinator), else the debounce breaks. [VERIFIED]
- Cloud-write gate: `_lists_auto_sync` checks `self.lists_mgr.is_sync_available()` then runs `self.lists_mgr.sync_to_cloud()` in a daemon thread with a 30s timeout + a `socket.gethostbyname('…supabase.co')` network pre-check. `_enable_lists_cloud_sync` resolves the user UUID from `self.corrections_client` and calls `self.lists_mgr.enable_cloud_sync(uuid, supabase_client=…)`. [VERIFIED lines 4239-4316, 13409-13474]
**Coupling:** core `lists_mgr` (ListsManager from `shared/lists_manager.py` — injected; `ListsManager(self.meta_mgr)` @ 3560), `corrections_client`, `meta_mgr`, `searcher` read. Cross-tab: `_set_active_tab`. 161 distinct `self.*`, all `lists_*`/`_lists_*`-namespaced.
**Boundary design:** `ListsPanel(QWidget)` owns the lists widgets + the `lists_*` methods; deferred `lists_mgr`/`corrections_client`/`meta_mgr`. Extract `_ListsSyncCoordinator` as a helper class (NOT a QWidget — a plain coordinator) that owns the debounce state (`_auto_sync_pending`/`_auto_sync_last`) + the three sync methods, taking `lists_mgr` + `corrections_client` by injection. The panel/GUI calls `coordinator.auto_sync()` after mutations.
**GUARD-03 tests:** `test_add_to_list_dialog_ui_context.py` imports `web.components.add_to_list_dialog` — **WEB-side, NOT genizah_app** (false positive in CONTEXT's list; no retarget). `test_user_lists_*.py` (3 files) — **ALL web-side** (`web.user_lists`/`web.state`/`web.auth_state`; `ListsManager` via the `genizah_core` facade) — **no genizah_app retarget needed** [VERIFIED: grep]. `test_recently_viewed_bugs.py` references `GenizahGUI`. `test_recently_viewed_bugs.py` + `test_recovery_scan_runs_cleanup.py` reference `GenizahGUI` — confirm whether runtime-construct (stay green via shim) or source-scan (retarget).

## GUARD-03 Source-Scanning Test Audit (verified)

**CRITICAL FINDING — 4 of the 10 CONTEXT-listed "GUARD-03 retarget tests" do NOT scan or import `genizah_app` at all (they are web-side):**

| CONTEXT-listed test | Actually scans/imports | Retarget needed in 126/127? |
|---------------------|------------------------|------------------------------|
| `test_telemetry_consent_ux.py` | `import genizah_app` runtime + `genizah_app.SettingsDialog` (`.__new__`) — D1 | RUNTIME (stays green via D1 shim); already in `_GUI_TEST_FILES` |
| `test_tabular_builder_rtl.py` | reads `genizah_app.py` source, AST-finds `TabularQueryBuilderDialog` (line 15,67) — **GUARD-03 named in ROADMAP** — D1 | SOURCE-SCAN: green in 126 (copy-not-move); retarget additively (scan `desktop/settings_dialogs.py` too); flip at 127 |
| `test_seed023_catalog_filters.py` | `inspect.getsource(web.pages.catalog_browse)` — **WEB** | **NO** (false positive) |
| `test_catalog_availability_filter.py` | `import genizah_app` runtime (`_get_catalog_filter_sets`, `_CATALOG_FILTER_SETS`) — D2 | RUNTIME (stays green via D2 module-level shim); in `_GUI_TEST_FILES` |
| `test_browse_state.py` | `from web.pages.browse_state import …` — **WEB** | **NO** (false positive) |
| `test_browse_synthetic.py` | scans `genizah_app.py` (`PNX_MANUSCRIPTS{sys_id}` @ 316-317) + many web files — D4 | SOURCE-SCAN: green in 126; add `desktop/browse_panel.py` entries; flip genizah_app entries at 127 |
| `test_local_browse_panel.py` | AST-scans `genizah_app.py` for `_open_local_browse`/`_get_local_full_text_for_sys_id` — D4 | SOURCE-SCAN: green in 126; retarget at 127 |
| `test_wr01_open_local_browse_page_ast.py` | AST-counts `_open_local_browse_page` in `genizah_app.py` — **GUARD-03 named in ROADMAP** — D4 | SOURCE-SCAN: green in 126; flip at 127 |
| `test_add_to_list_dialog_ui_context.py` | `from web.components.add_to_list_dialog import …` — **WEB** | **NO** (false positive) |
| `test_user_lists_cache_isolation.py` / `_data_threading.py` / `_refresh_data_returns.py` | all import `web.user_lists` / `web.state` / `web.auth_state` + `genizah_core import ListsManager` (facade) — **WEB-side, ALL THREE** [VERIFIED: grep] | **NO** (false positives — none scan or construct `genizah_app`) |

**Additional genizah_app.py source-scan tests NOT in the CONTEXT list but verified to pin methods that move (planner MUST include in the additive-retarget set):**
`test_desktop_folio_navigation.py` (ROADMAP GUARD-03), `test_view_all_cap.py` (ROADMAP GUARD-03), `test_desktop_pending_corrections.py`, `test_fgp_chooser_integration.py`, `test_view_all_incremental.py`, `test_local_filter_cascade.py`, `test_local_nav_codex_fix7.py`/`fix8.py`, `test_my_library_tab.py`, `test_synthetic_round_trip.py`, `test_no_dynamic_telemetry_strings.py` (REPO_ROOT/`genizah_app.py`), `test_privacy_disclosure_strings.py`, `test_telemetry_selftest.py`.

**The copy-not-move safety property:** in Phase 126 the implementation stays in `genizah_app.py`, so EVERY source-scanning test above stays GREEN with no edits required. The 126 retarget work is *additive* (extend the scan to also accept the new `desktop/` location), and the *flip* (require new location, forbid old) happens at deletion in Phase 127. This matches how `test_view_all_cap.py` was handled in prior phases (it source-text + AST checks `genizah_app.py`). **Do NOT** prematurely flip any source-scan test in 126 — that would fail because the old code is intentionally still present.

## GUARD-04 Importer Audit (`from genizah_app import` callers)

**CRITICAL FINDING — the actual `from genizah_app import` callers import module-level FUNCTIONS, not panel classes.** The panel CLASSES are not imported by name anywhere (they are instantiated INSIDE GenizahGUI). [VERIFIED: grep]

| Caller | Imports | Affected by 126? |
|--------|---------|-------------------|
| `desktop/join_workbench.py:4135` | `_build_search_results_xlsx_bytes` (lazy) | Export helper — NOT moved in 126 (D3 scope is render/filter, not the xlsx builder). Stays in genizah_app.py. |
| `tests/test_desktop_xlsx_multi_sheet.py` (×5) | `_build_search_results_xlsx_bytes` | Same — unaffected |
| `tests/test_export_xlsx_cross_parity.py:108` | `_build_search_results_xlsx_bytes` | unaffected |
| `tests/test_expux_expanded_context.py` | `_format_txt_genizah_block`, `_format_txt_local_block` | export helpers — unaffected |
| `tests/test_joins_lab_modes_export.py:106` | `_build_search_results_xlsx_bytes` | unaffected |
| `tests/test_local_export_*.py` | `_build_search_results_xlsx_bytes` + others | unaffected |
| `tests/test_local_nav_page_chunk.py` | `_aggregate_local_pages_with_separators` | browse-local helper — verify if it moves with D4; if so add re-export shim |
| `tests/test_smoke_round2_export_gaps.py:112` | `_build_search_results_xlsx_bytes` | unaffected |

**`import genizah_app` (module-style) callers:** `test_audit_2026_06_23_guards.py` (uses `genizah_app.GenizahGUI`), `test_catalog_availability_filter.py` (D2 module-level funcs), `test_telemetry_consent_ux.py` (`genizah_app.SettingsDialog`), `test_local_nav_codex_fix7/8.py`, `test_schema_rebuild_worker.py`, `test_session_restore_ask_fix10.py`, `test_telemetry_phase114.py`. These access `genizah_app.<Name>` at runtime — **all stay green** as long as the re-export shim binds the moved names back into the `genizah_app` namespace.

**GUARD-04 discipline (the 124 lesson):** for EACH cluster commit, run a base-vs-HEAD diff of the importable name set:
```bash
# capture base, then HEAD, then diff — names present at base must be present at HEAD
python -c "import genizah_app, inspect; print('\n'.join(sorted(n for n in dir(genizah_app) if not n.startswith('__'))))"
```
The re-export shim must keep `genizah_app.SettingsDialog`, `genizah_app._get_catalog_filter_sets`, `genizah_app._CATALOG_FILTER_SETS` (same object), `genizah_app.GenizahGUI`, and every panel class importable. Do the NAME-level diff yourself — do not trust the executor's failure count (Phase 124 mis-reported 3 defects as "pre-existing").

## Runtime State Inventory

> This is a refactor phase — included per protocol. The state below is CODE state that must be preserved by the move; there is no stored-data migration (zero behavior change).

| Category | Items Found | Action Required |
|----------|-------------|------------------|
| Stored data | None — no datastore keys change. Lists live in `lists_mgr` (joins.db / Supabase) but the schema/keys are untouched. | None — verified by: zero-behavior-change constraint; `lists_mgr` is injected unchanged. |
| Live service config | None — no external service config embeds a moved symbol name. | None — verified: panels reference services by instance, not by name string. |
| OS-registered state | None — no Task Scheduler / launchd / pm2 entry references these module/class names. | None — verified: desktop is a frozen EXE; no name-based OS registration. |
| Secrets/env vars | None changed. `WEB_PUZZLE_ENABLED`, telemetry keys (`TELEMETRY_ENABLED_KEY` etc.) read by `desktop.telemetry` — unchanged. | None. |
| Build artifacts | `GenizahSearchPro.spec` / `CompileScriptGenizah.iss` reference `genizah_app.py` as the entry point — UNCHANGED (genizah_app.py remains the entry, now importing from new desktop modules). New `desktop/*.py` files are auto-collected by PyInstaller's `desktop` package collection IF a `collect_submodules('desktop')` or explicit hidden-imports cover them. **Action: planner must verify the new desktop modules are picked up by the .spec** (the v7.9 modules already are; confirm the pattern). | Verify .spec hidden-imports / `collect_submodules` covers new `desktop/*` modules before any release (not in this phase's scope, but flag). |

**LOAD-BEARING CODE STATE to preserve (NOT data — but behavior-critical):**

1. **D-07b telemetry snapshot stripping (`SettingsDialog.__init__`, lines 2251-2271):**
```python
# genizah_app.py:2251-2271 — DO NOT "fix" back to a full dict()
from desktop.telemetry import (  # noqa: PLC0415
    TELEMETRY_ENABLED_KEY, FIRST_RUN_SHOWN_KEY as _FRSKEY,
    TELEMETRY_INSTALL_ID_KEY, CONSENT_TIMESTAMP_KEY,
    CONSENT_APP_VERSION_KEY, CONSENT_UI_VERSION_KEY, IDENTIFIED_USER_KEY,
)
_TELEMETRY_SNAPSHOT_EXCLUDE = frozenset({...7 keys...})
self._config_snapshot = {
    k: v for k, v in load_app_config().items()
    if k not in _TELEMETRY_SNAPSHOT_EXCLUDE
}
# _on_cancel: save_app_config(self._config_snapshot)
```
The strip exists because `save_app_config` is additive-merge (`cfg.update(new_data)` — keys absent from new_data are left untouched), so the consent keys must be OMITTED from the snapshot or Cancel would overwrite a freshly-set consent. When `SettingsDialog` moves to `desktop/settings_dialogs.py`, this exact logic moves verbatim — `load_app_config`/`save_app_config` get lazy-imported from `genizah_app`. `test_telemetry_consent_ux.py` constructs `SettingsDialog.__new__` and pins this. [VERIFIED]

2. **`_ListsSyncCoordinator` cloud-sync gate (D5):** `_lists_auto_sync` (13409) — debounce via class-level `_auto_sync_pending`/`_auto_sync_last` (13406-13407), `is_sync_available()` gate, daemon-thread `sync_to_cloud()` with 30s timeout + supabase host pre-resolve. `_enable_lists_cloud_sync` (4239) — UUID resolution from `corrections_client` + `lists_mgr.enable_cloud_sync(uuid, supabase_client=…)`. The debounce class-state must travel into the coordinator. [VERIFIED]

3. **`browse_thumb_resolved = pyqtSignal(str, object)`** — GenizahGUI class-level signal (3359) used by browse thumbnail resolution; if browse panel owns it, rewire `.connect()`/`.emit()` sites.

## Common Pitfalls

### Pitfall 1: Prematurely flipping a source-scanning test in 126
**What goes wrong:** A GUARD-03 source-scan test is changed to require the new `desktop/` location and forbid `genizah_app.py` — but in 126 the code is still in BOTH (copy-not-move), and at the *additive* stage it's only in genizah_app. The flip fails the suite.
**Why it happens:** Conflating the additive phase (126) with the deletion phase (127).
**How to avoid:** In 126, make source-scan tests accept EITHER location (parametrize/OR the assertion). Flip to new-only in 127 at deletion.
**Warning signs:** A source-scan test red after a copy commit even though behavior is unchanged.

### Pitfall 2: `from genizah_app import _CATALOG_FILTER_SETS` reassignment breaking the shared cache
**What goes wrong:** If the catalog module REASSIGNS `_CATALOG_FILTER_SETS = {…}` (rebinds the name) instead of mutating `['value']`, the `genizah_app` alias points at a stale object and `test_catalog_availability_filter.py` (which checks both the function path and the module attribute) sees divergence.
**Why it happens:** Module-level mutable re-exported by `from … import name` aliases the SAME object only until rebound.
**How to avoid:** Keep the dict mutated-in-place (`['value'] = …`), never rebound. Add an identity test: `genizah_app._CATALOG_FILTER_SETS is desktop.catalog_browse._CATALOG_FILTER_SETS`. Safest: leave `_CATALOG_FILTER_SETS*` + the two functions in `genizah_app.py` and lazy-import into the panel.
**Warning signs:** `test_catalog_availability_filter.py::test_cache` fails on the second call assertion.

### Pitfall 3: Constructing a panel BEFORE engines exist, then engine being None
**What goes wrong:** Panel uses `self.searcher` directly in `__init__` → AttributeError/None (engines are assigned async in `on_startup_finished`, after tabs are built).
**Why it happens:** Tabs build in `GenizahGUI.__init__` (3962-3975); `self.searcher` is None until `on_startup_finished`.
**How to avoid:** Deferred `@property search_engine` reading `getattr(self._parent_window, 'searcher', None)` (the MyLibraryTab pattern), and None-guard at call time.
**Warning signs:** Crash on app launch in a fresh process; passes in tests that pre-set `searcher`.

### Pitfall 4: Nesting `_CatalogRefreshWorker` inside the panel class
**What goes wrong:** `pyqtSignal` doesn't bind reliably when the QThread class is defined inside a method/class.
**Why it happens:** PyQt6 signal metaclass needs the class at module import.
**How to avoid:** `_CatalogRefreshWorker` stays a MODULE-LEVEL class in `desktop/catalog_browse.py` (its own docstring already warns this).
**Warning signs:** Catalog refresh silently never emits `done`.

### Pitfall 5: Repo-wide `ruff --fix` strips the `# noqa: F401` shims
**What goes wrong:** The re-export shims look like unused imports; `--fix` deletes them; every `from genizah_app import <Panel>` and `genizah_app.<Panel>` caller breaks.
**How to avoid:** Per-file ruff review on each extraction commit only. Never `ruff check --fix .`.
**Warning signs:** Sudden ImportError storm across test collection.

### Pitfall 6: gui-test forgetting to register the new test → CI segfault
**What goes wrong:** `test_search_results_panel.py` constructs a QWidget + dispatches events; if not in `_GUI_TEST_FILES`, it runs in the bulk `-m "not gui"` process and tips the suite into SIGSEGV after ~3000 prior tests.
**How to avoid:** Add `"test_search_results_panel.py"` to `_GUI_TEST_FILES` in `tests/conftest.py` (line 92). Run gui tests as a fresh-process `-m gui` job.
**Warning signs:** CI exit 139 in the `tests` job, not the `gui-tests` job.

## Code Examples

### D3 coupling reality — `on_search_finished` self.* surface (109 names, verified)
```
COL_ACTIONS COL_CHECKBOX COL_DOMAIN COL_IMG COL_LIBRARY COL_PGP COL_PRINTED COL_SHELF
COL_SNIPPET COL_SRC COL_SYS_ID COL_TITLE COL_TRANSCRIPTION MODE_RESPONSA
_add_regular_search_to_history _all_terms_filter _apply_all_terms_filter_and_rerender
_apply_local_filter _apply_local_optout_filter _apply_results_table_filters
_clear_refinement_chain _comp_history_action _create_action_button _domain_display_name
_domain_exclusions _domain_name_map _emit_search_telemetry _has_result_domains
_launch_enrichment_workers _local_filter_inactive_chip_visible _lookup_local_filepath
_manual_transcription_sys_ids _notify_search_complete _pgp_transcription_sys_ids
_prime_local_filepath_cache _printed_filter_state _printed_sys_ids _refine_mode
_refinement_scope_sig _refinement_stale _result_domain_counts _result_domain_map
_results_filter_text_for_row _schedule_session_save _set_active_tab _show_local_filter_chip
_update_local_filter_visibility_search _update_refinement_strip _update_search_row_list_indicator
_update_search_within_btn _zero_result_back_btn _zero_result_refine btn_domain_filter
chk_search_header export_buttons last_results last_search_query lbl_search_export
meta_mgr query_input refine_badge refine_cancel_btn refinement_strip result_row_by_sys_id
results_filters results_loaded results_placeholder results_stack results_table
search_progress search_start_time searcher shelfmark_items_by_sid show_full_text_for_result
start_metadata_loading status_label statusBar table_container title_items_by_sid  ... (+others)
```
This is why D3 is the structurally hardest cluster: the panel boundary must claim ~30 widgets + filter state and re-expose ~6 cross-tab events as signals, while a handful of session/history calls (`_add_regular_search_to_history`, `_schedule_session_save`) stay GenizahGUI-side (DEFER-04).

### The proven extraction template (MyLibraryTab — zero genizah_app imports)
```python
# desktop/search_results_panel.py (NEW — sketch following my_library_tab.py)
from PyQt6.QtCore import pyqtSignal
from PyQt6.QtWidgets import QWidget
from genizah_core import tr, CURRENT_LANG  # lazy idiom OK; genizah_core is shared

class SearchResultsPanel(QWidget):
    result_open_requested = pyqtSignal(dict)     # → GUI opens in Browse (D3→D4)
    add_to_list_requested = pyqtSignal(list)
    def __init__(self, parent=None):
        super().__init__(parent)
        self._parent_window = parent
        self._build_ui()           # owns results_table, search_progress, ...
    @property
    def searcher(self):
        return getattr(self._parent_window, "searcher", None)
    @property
    def meta_mgr(self):
        return getattr(self._parent_window, "meta_mgr", None)
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| One 28K-line `genizah_app.py` god-file | Decomposed `desktop/*` modules behind re-export shims (v7.9 recipe) | v7.9 (Phases 67-76); continued v8.3.0 | The recipe is PROVEN in-repo; Phase 126 applies it to the remaining 5 clusters |
| Class-attribute NLI breaker, etc. | Module-level singletons + injection | Phase 98+, Phase 122-125 | Panels receive injected engine instances, never construct |
| Core logic in genizah_core.py | `shared/` modules + permanent facade | Phases 122-125 (DONE) | D5's `ListsManager` is now `shared/lists_manager.py` (injected into the panel) |

**Deprecated/outdated:**
- "standalone backend server" / port 8000 — removed Jan 2026 (per CLAUDE.md); irrelevant to this phase.
- The mixin extraction approach — never adopted in this codebase; do not introduce it.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `LabPanel` (853-1091) should DEFER to E2 (composition) rather than move to D1, because `LabPanel.set_engine` parent-traverses (`main = self.parent()` @ ~896) AND calls `self.engine.rebuild_lab_index` + reads `self.settings.comp_*` — confirming the SEED-020 §7 C-6 composition coupling [VERIFIED: grep on lines 853-1091]. (No longer ASSUMED — kept here only because the final cluster assignment is a planner/Codex pre-flight call.) | Cluster D1 / Open Q1 | If moved to D1 anyway, D1 over-reaches into composition-coupled code |
| A2 | Community-tab populators (`_populate_discoveries_list`, `_populate_joins_list`, etc., 15569-16246) are NOT part of D5 lists scope (they're the separate Community tab) | Cluster D5 | If they ARE in scope, D5 is larger; low risk — easily split |
| A3 | `_build_search_results_xlsx_bytes` / `_format_txt_*` / `_aggregate_local_pages_with_separators` export helpers stay in `genizah_app.py` (D3 scope = render/filter lifecycle, not export) | GUARD-04 audit | If they must move, add re-export shims so the 8+ test importers + `join_workbench.py` stay green |
| A4 | Engines are None at tab-build time → deferred property required (not constructor injection) for searcher/meta_mgr | Standard Stack / Pattern 1 | Verified against MyLibraryTab + GenizahGUI.__init__ order; very low risk |
| A5 | (RESOLVED — was: `test_user_lists_*.py` mostly web-side) — all 3 `test_user_lists_*.py` confirmed WEB-side, no retarget [VERIFIED: grep] | GUARD-03 audit | None — verified |

## Open Questions

1. **`LabPanel` cluster assignment.**
   - What we know: SEED-020 §1 lists `LabPanel` under D1 (`ui_widgets.py`/`settings_dialogs.py`); SEED-020 §7 C-6 + §5 Q says `LabPanel.set_engine` parent-traversal is an E2 (composition) prerequisite and composition is DEFERRED.
   - What's unclear: Whether `LabPanel` moves in 126 (D1) or stays for the deferred composition extraction.
   - Recommendation: DEFER `LabPanel` to E2 (leave in `genizah_app.py`); move only `LabScoringDialog`, `SettingsDialog`, `SearchSettingsDialog`, `HelpDialog`, `TabularQueryBuilderDialog` to `settings_dialogs.py`. Flag for Codex PLAN pre-flight.

2. **Community-tab populators in D5?**
   - What we know: `create_lists_tab` and the `_populate_*_list` community methods are adjacent (13010-16246); D5 is "Lists tab + cloud-sync."
   - What's unclear: Whether the Community tab populators are in D5 or out of scope.
   - Recommendation: Scope D5 to the Personal Lists tab (tab index 4) + cloud sync; leave the Community tab (index 5) for a later cluster. Confirm with planner.

3. **`_CATALOG_FILTER_SETS` + helpers: keep-in-place vs move-with-shim?**
   - What we know: `test_catalog_availability_filter.py` mutates `genizah_app._CATALOG_FILTER_SETS['value']` and calls the two module-level functions.
   - Recommendation: Keep `_CATALOG_FILTER_SETS*` + `_get_catalog_filter_sets`/`reset_catalog_filter_sets` in `genizah_app.py` (app-global caches); have `desktop/catalog_browse.py` lazy-import them. Avoids the mutable-rebind aliasing hazard entirely. If moved, add a same-object identity test.

No genuine USER-FACING choice surfaced — all open questions are technical sub-decisions delegated to research/planning + Codex pre-flight (per CONTEXT).

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Python | All | ✓ | 3.11.9 | — |
| PyQt6 | All panel classes | ✓ | 6.10.2 (Qt 6.10.0) | — |
| ruff | shim integrity gate | ✓ | 0.15.10 | — |
| pytest | suite + gui split | ✓ | 9.0.2 | — |
| `gui_threads.SearchThread` | D3 new test mock | ✓ | module-level @ gui_threads.py:81 | — |
| `shared/reading_desk_model.py` | D6 reading desk | ✓ | already extracted | — |
| `shared/lists_manager.py` | D5 injected ListsManager | ✓ | extracted Phase 123 | — |

**Missing dependencies with no fallback:** None.
**Missing dependencies with fallback:** None.

## Validation Architecture

> nyquist_validation: included (config key absent → treated as enabled).

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest 9.0.2 |
| Config file | `tests/conftest.py` (+ repo `pytest.ini`/`pyproject` markers `gui`, `render_smoke`, `scale`) |
| Quick run command | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/<cluster-test>.py -x` |
| Full suite command | bulk: `python -m pytest tests/ -m "not gui and not render_smoke"`; gui slice: `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/ -m "gui or render_smoke"` |

### Phase Requirements → Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| DESK-01 | SettingsDialog importable + D-07b strip preserved | gui (runtime construct) | `pytest tests/test_telemetry_consent_ux.py -x` | ✅ (in `_GUI_TEST_FILES`) |
| DESK-01 | TabularQueryBuilderDialog RTL | source-scan AST | `pytest tests/test_tabular_builder_rtl.py -x` | ✅ (additive-retarget) |
| DESK-03 | Catalog filter cache | gui (runtime) | `pytest tests/test_catalog_availability_filter.py -x` | ✅ (in `_GUI_TEST_FILES`) |
| DESK-03 | Catalog availability filters | logic | `pytest tests/test_seed023_catalog_filters.py -x` | ✅ (web-side — no retarget) |
| DESK-04 | SearchResultsPanel direct (mock SearchThread) | gui (NEW) | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/test_search_results_panel.py -x` | ❌ Wave 0 — NEW + add to `_GUI_TEST_FILES` |
| DESK-05 | Browse panel synthetic + local | source-scan | `pytest tests/test_browse_synthetic.py tests/test_local_browse_panel.py tests/test_wr01_open_local_browse_page_ast.py -x` | ✅ (additive-retarget) |
| DESK-06 | Reading desk render | (covered by browse tests + folio nav) | `pytest tests/test_desktop_folio_navigation.py -x` | ✅ (additive-retarget) |
| DESK-07 | Lists tab + recently-viewed | runtime (`GenizahGUI`) | `pytest tests/test_recently_viewed_bugs.py tests/test_user_lists_cache_isolation.py -x` | ✅ (verify per file) |
| GUARD-02 | Full suite green at each commit | full suite | both commands above | ✅ |
| GUARD-04 | `genizah_app.*` names importable | name diff | `python -c "import genizah_app; ..."` base-vs-HEAD | manual gate |

### Sampling Rate
- **Per task commit:** the cluster's targeted test(s) + per-file ruff + a base-vs-HEAD `dir(genizah_app)` name diff.
- **Per wave (cluster) merge:** bulk suite `-m "not gui and not render_smoke"` + gui slice `-m "gui or render_smoke"`.
- **Phase gate:** full suite green (bulk + gui + render_smoke) + base-vs-HEAD name-level test diff == 6-env baseline (red-at-base env-only `test_search_api_v2::…real_index[exact|fuzzy|responsa|shelfmark|title|variants]`) before `/gsd:verify-work`.

### Wave 0 Gaps
- [ ] `tests/test_search_results_panel.py` — NEW direct-module test for `SearchResultsPanel` (mock `gui_threads.SearchThread`); covers DESK-04. **MUST** add filename to `_GUI_TEST_FILES` in `tests/conftest.py:92`.
- [ ] No framework install needed (pytest/PyQt6/ruff all present).
- [ ] Additive-retarget (not new files): extend `test_tabular_builder_rtl.py`, `test_browse_synthetic.py`, `test_local_browse_panel.py`, `test_wr01_open_local_browse_page_ast.py`, `test_desktop_folio_navigation.py`, `test_view_all_cap.py`, `test_view_all_incremental.py`, `test_fgp_chooser_integration.py`, `test_desktop_pending_corrections.py`, `test_local_filter_cascade.py` to accept the new `desktop/` location (OR-assertion) — flip to new-only in Phase 127.

## Security Domain

> `security_enforcement` not explicitly false → included. This is a pure internal refactor (zero behavior change), so the security surface is limited to NOT regressing existing controls.

### Applicable ASVS Categories
| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | indirect | D5 cloud sync resolves user UUID from `corrections_client` (Supabase auth) — preserve unchanged |
| V3 Session Management | no | desktop app; no web session |
| V4 Access Control | indirect | Supabase RLS for list sync — `enable_cloud_sync(uuid, supabase_client=…)` passes the authenticated client; preserve verbatim |
| V5 Input Validation | no | no new inputs (refactor) |
| V6 Cryptography | indirect | telemetry consent keys + install-id (D-07b) — DO NOT regress the snapshot-strip that protects consent state; never log/transmit search text or My-Library content (existing invariant) |

### Known Threat Patterns for {PyQt6 desktop + Supabase sync}
| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Cancel-overwrites-consent (D-07b) | Tampering (config) | Preserve the telemetry-key strip in `SettingsDialog._config_snapshot` — moving it intact is the control |
| Cloud-sync without auth | Elevation | Preserve `is_sync_available()` gate + authenticated `supabase_client` pass-through in `_enable_lists_cloud_sync` |
| Telemetry leaking content | Information disclosure | No telemetry-string changes in scope; `test_no_dynamic_telemetry_strings.py` + `test_privacy_disclosure_strings.py` (genizah_app.py scanners) must stay green |

## Sources

### Primary (HIGH confidence — codebase-verified this session)
- `genizah_app.py` @ base `aa215b37` — `grep -n "^class "` (16 top-level classes), `wc -l` (28,033), method/coupling enumeration via Python AST/regex scan
- `desktop/my_library_tab.py:148,719-918,1068,1161` — the template parent-ref + deferred-property + module-level-worker pattern
- `desktop/result_dialog.py:48`, `desktop/join_workbench.py:4900,4135` — constructor-injection variants + the one `from genizah_app import` lazy call
- `shared/search_engine.py:406,441,791` — `_my_library_tab_ref` injected `is_searchable` gate (the cross-engine coupling analog)
- `gui_threads.py:81-89` — `SearchThread` (D3 worker, already module-level)
- `shared/reading_desk_model.py:16,33` — reading-desk dataclasses (already extracted)
- `tests/conftest.py:55-96,143-158` — `collect_ignore_glob`, `_GUI_TEST_FILES`, marker auto-apply
- GUARD-03 test files inspected directly: `test_tabular_builder_rtl.py`, `test_browse_synthetic.py`, `test_local_browse_panel.py`, `test_wr01_open_local_browse_page_ast.py`, `test_catalog_availability_filter.py`, `test_seed023_catalog_filters.py`, `test_browse_state.py`, `test_add_to_list_dialog_ui_context.py`, `test_view_all_cap.py`, `test_telemetry_consent_ux.py`
- `.planning/seeds/SEED-020-decomposition-map.md` (§Desktop D1-D5, §7 C-1..C-6), `.planning/REQUIREMENTS.md`, `.planning/ROADMAP.md` (Phase 126), `.planning/STATE.md`, `.planning/phases/126-desktop-panels/126-CONTEXT.md`
- Tool versions: `python --version` (3.11.9), `PyQt6.QtCore.PYQT_VERSION_STR` (6.10.2), `ruff --version` (0.15.10), pyc filenames (pytest 9.0.2)

### Secondary (MEDIUM)
- CLAUDE.md project conventions (Hebrew RTL, shared-service layer, no repo-wide ruff --fix), MEMORY.md lessons (`feedback_full_suite_testing_windows`, `feedback_godfile_extraction_import_lesson`, `feedback_codex_preflight_before_plan_complete`)

### Tertiary (LOW)
- None — all claims are codebase-verified or cited from planning docs.

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — no new packages; all tools version-verified installed.
- Architecture / coupling map: HIGH — line ranges, class list, and per-cluster `self.*` counts derived by direct AST/regex scan of the live file; template pattern read from live `my_library_tab.py`.
- GUARD-03/GUARD-04 audit: HIGH — every named test file inspected; 4 false-positives (web-side) and 12 additional genizah_app-scanning tests surfaced.
- Pitfalls: HIGH — each tied to a verified code site or a documented prior-phase lesson.
- Open questions (LabPanel cluster, Community scope): MEDIUM — flagged for planner + Codex PLAN pre-flight.

**Research date:** 2026-06-26
**Valid until:** 2026-07-26 (stable refactor domain; line numbers will DRIFT as commits land — re-grep before relying on any specific line).
