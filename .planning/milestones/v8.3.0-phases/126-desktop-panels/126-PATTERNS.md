# Phase 126: Desktop Panels - Pattern Map

**Mapped:** 2026-06-26
**Files analyzed:** 18 (7 new `desktop/` modules + 1 modified `genizah_app.py` shim host + 1 modified `tests/conftest.py` + 1 new test + ~8 additively-retargeted GUARD-03 tests)
**Analogs found:** 7 / 7 (every new module maps to a live in-repo extraction template; zero "no analog")

> **Read this first (planner):** This is a pure copy→shim→(delete-in-127) refactor with ZERO behavior change. There is exactly ONE primary analog — **`desktop/my_library_tab.py`** (`MyLibraryTab`) — and the secondary analog **`desktop/result_dialog.py`** (`ResultDialog`). Every D-cluster panel mirrors one of these two. The code being "extracted" already exists as `GenizahGUI` methods inside `genizah_app.py`; the engineering is drawing the panel↔GenizahGUI boundary so the moved `self.*` rebinds to the panel and no `desktop/`→`genizah_app` module-level back-edge is introduced. **Line numbers WILL drift — re-grep `^class ` and method names before each move; never trust the ranges below as literal cut points.**

## File Classification

| New / Modified File | Role | Data Flow | Closest Analog | Match Quality |
|---------------------|------|-----------|----------------|---------------|
| `desktop/settings_dialogs.py` (NEW, D1) | UI dialogs (modal) | request-response (config read/write) | `desktop/result_dialog.py` (top-level `QDialog`, v7.9 idiom) | role-match (dialog→dialog) |
| `desktop/ui_widgets.py` (NEW, D1) | UI widget subclasses | transform (sort-key / header / scroll) | `desktop/widgets/` package (existing extracted widgets) | exact (widget→widget) |
| `desktop/catalog_browse.py` (NEW, D2) | UI panel (`QWidget` tab) + module-level worker | event-driven (worker query → render) | `desktop/my_library_tab.py` (`MyLibraryTab` + `LocalIndexerWorker`) | exact (tab+worker→tab+worker) |
| `desktop/search_results_panel.py` (NEW, D3) | UI panel (`QWidget`) | event-driven (SearchThread → render/filter) | `desktop/my_library_tab.py` (`MyLibraryTab`) | role-match (hardest cluster) |
| `desktop/browse_panel.py` (NEW, D4) | UI panel (`QWidget`) | request-response + streaming (folio nav + enrichment) | `desktop/my_library_tab.py` (`MyLibraryTab`) | role-match |
| `desktop/reading_desk_panel.py` (NEW, D4) | UI sub-panel (`QWidget`) | transform (multi-entry side-by-side render) | `desktop/my_library_tab.py` + `shared/reading_desk_model.py` | role-match |
| `desktop/lists_tab.py` (NEW, D5) | UI panel (`QWidget`) + plain coordinator helper | CRUD (list ops) + pub-sub (cloud sync) | `desktop/my_library_tab.py` (panel) + `_lists_auto_sync` (coordinator) | exact (tab→tab) |
| `genizah_app.py` (MODIFIED) | re-export shim host | n/a | lines 67–76 existing `from desktop.* import …` block | exact (shim→shim) |
| `tests/conftest.py` (MODIFIED) | test config (`_GUI_TEST_FILES`) | n/a | `tests/conftest.py:92` existing set | exact |
| `tests/test_search_results_panel.py` (NEW, D3) | gui test (mock `SearchThread`) | n/a | `tests/test_telemetry_consent_ux.py` (runtime-construct gui test) | role-match |
| GUARD-03 source-scan tests (MODIFIED, additive) | source-scan / AST guard | n/a | `test_view_all_cap.py` (OR-location pattern) | exact |

---

## Shared Patterns

> These five patterns are extracted from the live primary analog and apply across MULTIPLE new modules. Each plan's action section should reference them by number rather than re-describing.

### SP-1: Module header + lazy `genizah_core` import (NO `genizah_app` import)
**Source:** `desktop/my_library_tab.py:24-71` and `desktop/result_dialog.py:1-40`
**Apply to:** every new `desktop/*.py` module.
The module imports PyQt6 + `shared/*` + `genizah_core` symbols at module top. It NEVER imports `genizah_app` at module level (GUARD-01). When a panel needs a `genizah_app` module-level function (e.g. `load_app_config`, `save_app_config`), it lazy-imports inside the method body with `# noqa: PLC0415`.
```python
# desktop/my_library_tab.py:63-71
from shared.local_indexer import (
    LocalIndexer, migrate_legacy_local_db, _SUPPORTED_EXTENSIONS, is_office_temp_file,
)
from genizah_core import Config, tr, CURRENT_LANG   # shared facade — OK at module level

logger = logging.getLogger(__name__)
```
```python
# desktop/result_dialog.py:14-17 — the canonical shared-symbol import block
from genizah_core import (
    CURRENT_LANG, get_library_display, get_logger, load_app_config, save_app_config, tr,
)
```
> **D1 note:** `SettingsDialog` calls `load_app_config()`/`save_app_config()` un-prefixed today (they are `genizah_app.py` module functions @ 2269/2298). RESEARCH §Cluster D1 + Open Q3 say: KEEP them in `genizah_app.py` and lazy-import (or import via the `genizah_core` facade if exposed there — `result_dialog.py:16` already imports `load_app_config, save_app_config` from `genizah_core`, so the facade route is the precedent — verify the names resolve through `genizah_core` before choosing).

### SP-2: Parent-ref + deferred `@property` engine accessor (THE local idiom)
**Source:** `desktop/my_library_tab.py:1077-1091` (`__init__` stashes `self._parent_window = parent`) and `:1161-1181` (deferred properties).
**Apply to:** D2 catalog, D3 search-results, D4 browse, D5 lists panels — anything that reads `searcher`/`meta_mgr`/`lab_engine`/`lists_mgr` (all None at tab-build time, assigned later in `on_startup_finished`).
```python
# desktop/my_library_tab.py:1077-1091
def __init__(self, parent: Optional[QWidget] = None) -> None:
    super().__init__(parent)
    self.is_searchable: bool = False
    # parent GenizahGUI exposes engines via self.searcher, assigned async in
    # on_startup_finished(); read them via deferred property, NOT in __init__.
    self._parent_window = parent
    ...
    self._build_ui()      # owns its widgets FIRST
    self._init_indexer()  # then deps
```
```python
# desktop/my_library_tab.py:1161-1181 — deferred property (None-safe)
@property
def search_engine(self):
    if self._parent_window is None:
        return None
    return getattr(self._parent_window, "searcher", None)

@property
def lab_engine(self):
    if self._parent_window is None:
        return None
    return getattr(self._parent_window, "lab_engine", None)
```
> Mirrors the engine-side `shared/search_engine.py::_my_library_tab_ref` injected gate (RESEARCH Sources). **Pitfall 3:** NEVER touch `self.searcher`/`self.meta_mgr` inside `__init__` — they are None until `on_startup_finished`; always read via the property and None-guard at call time.
> **Construction-time exception:** `corrections_client` IS available at construction → use direct constructor injection for it (the `ResultDialog` model, SP-5), not a deferred property.

### SP-3: Module-level `pyqtSignal` worker (NEVER nested)
**Source:** `desktop/my_library_tab.py:705-724` (`LocalIndexerWorker`) and `genizah_app.py:1453-1460` (`_CatalogRefreshWorker`).
**Apply to:** D2 — `_CatalogRefreshWorker` moves WITH `catalog_browse.py` as a **module-level** class (its own docstring states why). D3 needs NO worker move (`SearchThread` already module-level in `gui_threads.py:81`).
```python
# desktop/my_library_tab.py:705-724 — signals are CLASS attributes on a module-level QThread
class LocalIndexerWorker(QThread):
    progress_updated = pyqtSignal(int, int, str)
    file_finished = pyqtSignal(str, str, int, str)
    finished_signal = pyqtSignal(dict)
    error_signal = pyqtSignal(str)
    status_updated = pyqtSignal(str)
    def __init__(self, indexer: LocalIndexer, operation_kind: str = 'incremental_add') -> None:
        super().__init__()
        ...
```
```python
# genizah_app.py:1453-1460 — the D2 worker that MUST stay module-level (verbatim move)
class _CatalogRefreshWorker(QThread):
    """Background worker for catalog browse DB queries (authors/works/results).
    Must be module-level (not nested inside a method) so pyqtSignal works
    reliably in PyQt6."""
    done = pyqtSignal(object)  # dict with keys: authors?, works?, data
```
> **Pitfall 4:** nesting the QThread inside the panel class breaks signal binding (catalog refresh silently never emits `done`).

### SP-4: The re-export shim in `genizah_app.py` (GUARD-04)
**Source:** `genizah_app.py:67-76` — the existing block of v7.9 extraction shims.
**Apply to:** one `# noqa: F401` import line per extracted cluster, added to this block. Copy-not-move, so the implementation also still lives in `genizah_app.py` until Phase 127.
```python
# genizah_app.py:67-76 (existing — APPEND the new panel imports here)
from desktop.image_loader import ImageLoaderThread
from desktop.result_dialog import ResultDialog
from desktop.dialogs_scholarly import FjmsBibliographyDialog, FjmsCatalogDialog, FjmsMeasurementsDialog, NliBibliographyDialog  # noqa: F401
from desktop.dialogs_filter import ExcludeDialog, DomainFilterDialog, PreSearchFilterDialog  # noqa: F401
from desktop.viewers import ZoomableScrollArea, FullscreenImageWindow, ManuscriptViewerWidget, _make_scrollable_row, _generate_oxford_dynamic_url  # noqa: F401
from desktop.puzzle import PuzzleFragmentItem, PuzzleCanvasView, PuzzleExportThread, PuzzlePublishThread, PuzzleCanvasWindow  # noqa: F401
from desktop.vs_cache import DesktopVSCache, VSFetchThread, VSDownloadThread  # noqa: F401
from desktop.my_library_tab import MyLibraryTab  # Phase 95 — 7th tab
```
> New lines for Phase 126 (shape, exact class names per cluster below):
> ```python
> from desktop.settings_dialogs import SettingsDialog, SearchSettingsDialog, HelpDialog, TabularQueryBuilderDialog, LabScoringDialog  # noqa: F401  Phase 126 D1
> from desktop.ui_widgets import ShelfmarkTableWidgetItem, CheckBoxHeader, HiddenScrollArea, ListsTreeWidget  # noqa: F401  Phase 126 D1
> from desktop.catalog_browse import CatalogBrowsePanel, _CatalogRefreshWorker  # noqa: F401  Phase 126 D2
> from desktop.search_results_panel import SearchResultsPanel  # noqa: F401  Phase 126 D3
> from desktop.browse_panel import BrowsePanel  # noqa: F401  Phase 126 D4
> from desktop.reading_desk_panel import ReadingDeskPanel  # noqa: F401  Phase 126 D4
> from desktop.lists_tab import ListsPanel, _ListsSyncCoordinator  # noqa: F401  Phase 126 D5
> ```
> **GUARD-04 gate (the Phase-124 lesson — do NOT trust the executor's failure count):** for EACH cluster commit, run the base-vs-HEAD name diff yourself:
> ```bash
> python -c "import genizah_app; print('\n'.join(sorted(n for n in dir(genizah_app) if not n.startswith('__'))))"
> ```
> Every name present at base `aa215b37` MUST be present at HEAD. `genizah_app.SettingsDialog`, `genizah_app._get_catalog_filter_sets`, `genizah_app._CATALOG_FILTER_SETS` (SAME object), `genizah_app.GenizahGUI`, and every panel class stay importable. **Never repo-wide `ruff --fix`** (Pitfall 5 — strips the `# noqa: F401` shims). Per-file ruff review only.

### SP-5: Constructor injection for construction-time deps (ResultDialog model)
**Source:** `desktop/result_dialog.py:42-72`
**Apply to:** D5's `_ListsSyncCoordinator` (a plain non-QWidget helper that takes `lists_mgr` + `corrections_client` by injection); and any dependency genuinely available at construction time.
```python
# desktop/result_dialog.py:42-71
class ResultDialog(QDialog):
    metadata_loaded = pyqtSignal(int, dict)
    thumb_resolved = pyqtSignal(str, object)
    def __init__(self, parent, all_results, current_index, meta_mgr, searcher):
        super().__init__(parent)
        self._app = parent
        ...
        self.meta_mgr = meta_mgr      # injected — already built when dialog opens
        self.searcher = searcher
        self.thumb_resolved.connect(self._on_thumb_resolved)
```
> Note the dialog's two `pyqtSignal`s (`metadata_loaded`, `thumb_resolved`) are CLASS attributes on the QDialog defined at module level — same constraint as SP-3, applied to a dialog. This is the template for D4's `browse_thumb_resolved` pyqtSignal moving onto `BrowsePanel`.

---

## Pattern Assignments

### `desktop/settings_dialogs.py` + `desktop/ui_widgets.py` (D1 — dialogs + widgets, request-response/transform)

**Analog:** `desktop/result_dialog.py` (dialogs, top-level `QDialog`) + `desktop/widgets/` package (widget subclasses).

**Classes to move (re-grep `^class ` before cutting — ranges from RESEARCH @ base `aa215b37`, WILL drift):**
- `settings_dialogs.py`: `SettingsDialog` (~2218-3356), `SearchSettingsDialog` (~697-852), `HelpDialog` (~1520-1582), `TabularQueryBuilderDialog` (~1605-2217), `LabScoringDialog` (~596-696).
- `ui_widgets.py`: `ShelfmarkTableWidgetItem` (~1092-1098), `CheckBoxHeader` (~1099-1335), `HiddenScrollArea` (~1336-1452), `ListsTreeWidget` (~1583-1604).

**DEFER — do NOT move `LabPanel` (~853-1091) in D1.** Confirmed via `genizah_app.py:867-872, 896-899`: `LabPanel.set_engine` reads `engine.settings` + calls `self._mark_rebuild_required()` (composition coupling), and `init_ui` parent-traverses `main = self.parent()` for `open_help_center`. Both are E2 (composition tab, DEFERRED) prerequisites. Leave `LabPanel` in `genizah_app.py`. (RESEARCH Open Q1 / Assumption A1.)
```python
# genizah_app.py:867-872 — the composition coupling that pins LabPanel to E2
def set_engine(self, engine):
    self.lab_engine = engine
    self.settings = engine.settings          # composition settings
    self.refresh_values()
    self.enable_controls(True)
    self._mark_rebuild_required()
```

**Imports pattern:** SP-1 (lazy `load_app_config`/`save_app_config`; `tr`/`CURRENT_LANG`/`Config` from `genizah_core`).
**Coupling:** SP-4 (thin GenizahGUI API). Today `SettingsDialog` keeps `self.main_win = parent` and wires `combo_language.currentIndexChanged → self.main_win._on_language_combo_changed` (~2326). CONTEXT mandates introducing `GenizahGUI.apply_settings()` / `cancel_settings()` — give the OK/Cancel paths a named API instead of reaching into arbitrary GUI internals.

**LOAD-BEARING — D-07b telemetry snapshot strip (MOVE VERBATIM):** `genizah_app.py:2251-2271` + `_on_cancel` @ 2296-2299. The strip exists because `save_app_config` is additive-merge — Cancel must NOT overwrite freshly-set consent. Do NOT "fix" back to a full `dict()`.
```python
# genizah_app.py:2258-2271 — moves verbatim into desktop/settings_dialogs.py
from desktop.telemetry import (  # noqa: PLC0415
    TELEMETRY_ENABLED_KEY, FIRST_RUN_SHOWN_KEY as _FRSKEY,
    TELEMETRY_INSTALL_ID_KEY, CONSENT_TIMESTAMP_KEY,
    CONSENT_APP_VERSION_KEY, CONSENT_UI_VERSION_KEY, IDENTIFIED_USER_KEY,
)
_TELEMETRY_SNAPSHOT_EXCLUDE = frozenset({
    TELEMETRY_ENABLED_KEY, _FRSKEY, TELEMETRY_INSTALL_ID_KEY,
    CONSENT_TIMESTAMP_KEY, CONSENT_APP_VERSION_KEY,
    CONSENT_UI_VERSION_KEY, IDENTIFIED_USER_KEY,
})
self._config_snapshot = {
    k: v for k, v in load_app_config().items()
    if k not in _TELEMETRY_SNAPSHOT_EXCLUDE
}
# _on_cancel (2296-2299): save_app_config(self._config_snapshot); self.reject()
```
**Worker classes with `pyqtSignal`:** none in D1.
**Shim:** SP-4 (two new import lines for the dialog + widget classes).
**GUARD-03 tests:** `test_telemetry_consent_ux.py` — RUNTIME (`genizah_app.SettingsDialog.__new__`, pins the strip); stays green via shim; already in `_GUI_TEST_FILES`. `test_tabular_builder_rtl.py` — SOURCE-SCAN (AST-finds `TabularQueryBuilderDialog`); green in 126 (copy-not-move); additively-retarget to also accept `desktop/settings_dialogs.py`.

---

### `desktop/catalog_browse.py` (D2 — catalog tab + worker, event-driven) — RECOMMENDED FIRST (de-risk spine)

**Analog:** `desktop/my_library_tab.py` (clean `QWidget` tab factory + module-level worker — exact match).

**Build + methods (re-grep — RESEARCH ranges):** `create_catalog_browse_tab` (~11772) → `self.catalog_browse_tab`; 38 `_catalog_*` methods (~11772-12951): `_catalog_refresh`, `_catalog_start_async_refresh`, `_catalog_on_async_refresh_done`, `_catalog_populate_tree`, `_catalog_render_tree`, `_catalog_view_result_by_row`, `_catalog_search_in_results`, `_catalog_parallels_in_results`, `_catalog_cycle_pgp_filter`, `_catalog_cycle_editions_filter`, `_catalog_build_browse_filters`, etc. 100 distinct `self.*`, ALL `self._catalog_*`-namespaced (clean).

**Module-level worker (SP-3 — VERBATIM move):** `_CatalogRefreshWorker` (`genizah_app.py:1453-1517`, `done = pyqtSignal(object)`) moves WITH this module, stays module-level.

**Module-level cache + helpers — KEEP IN PLACE (RESEARCH Open Q3 recommendation, avoids the aliasing hazard):** `_CATALOG_FILTER_SETS` (~1415), `_CATALOG_FILTER_SETS_LOCK` (~1416), `_get_catalog_filter_sets()` (~1419-1442), `reset_catalog_filter_sets()` (~1445-1450) stay in `genizah_app.py`; `catalog_browse.py` lazy-imports them. `test_catalog_availability_filter.py` does `genizah_app._CATALOG_FILTER_SETS['value']` / `genizah_app._get_catalog_filter_sets()` at runtime.
```python
# genizah_app.py:1415-1442 — KEEP HERE (app-global cache). catalog_browse.py lazy-imports.
_CATALOG_FILTER_SETS = {'value': None}
_CATALOG_FILTER_SETS_LOCK = threading.Lock()
def _get_catalog_filter_sets():
    cached = _CATALOG_FILTER_SETS['value']
    if cached is not None:
        return cached
    with _CATALOG_FILTER_SETS_LOCK:
        ...
        _CATALOG_FILTER_SETS['value'] = cached  # mutate ['value'] in place — NEVER rebind the dict
    return cached
```
> **Pitfall 2:** if it ever moves, `from … import _CATALOG_FILTER_SETS` aliases the SAME dict only until rebound; the test mutates `['value']` so a same-object shim is safe, but reassigning the name diverges the aliases. Keeping it in place sidesteps this entirely. If moved anyway, add an identity test `genizah_app._CATALOG_FILTER_SETS is desktop.catalog_browse._CATALOG_FILTER_SETS`.

**Coupling:** SP-2 (deferred `searcher`/`meta_mgr`). Cross-tab: `parent.pre_search_filters`, `parent.pre_search_restrict_sys_ids`, `parent._update_filter_chip_bar()`, `parent._set_active_tab(0)` (4 calls). Opens `ResultDialog` (already in `desktop/result_dialog.py`). Boundary: `CatalogBrowsePanel(QWidget)` owns `_catalog_*` widgets; cross-tab via a thin `parent` method or `parent.pre_search_filters = …`.
**Shim:** SP-4. **GUARD-03:** `test_seed023_catalog_filters.py` is WEB-side (`web.pages.catalog_browse`) — NO retarget (false positive). `test_catalog_availability_filter.py` — RUNTIME; stays green via the keep-in-place cache; already in `_GUI_TEST_FILES`.

---

### `desktop/search_results_panel.py` (D3 — search-results lifecycle, event-driven) — STRUCTURALLY HARDEST; sequence BEFORE D4

**Analog:** `desktop/my_library_tab.py` (`MyLibraryTab` — owns widgets + state, deferred engine properties, signals out). The CONTEXT "inject searcher/meta_mgr" reconciles to: inject the GenizahGUI `parent`, expose `searcher`/`meta_mgr` via SP-2 deferred properties (engines are None at tab-build time).

**No worker to move:** `SearchThread` is ALREADY module-level in `gui_threads.py:81-89` — `results_signal(list)`, `progress_signal(int,int)`, `error_signal(str)`, `perf_signal(float,int)`. The new test mocks it.

**Methods (scattered across GenizahGUI — re-grep):** `on_search_finished` (~18506), `_on_search_progress` (~18240), `set_results_loading` (~7210), `_apply_results_table_filters` (~19333), `_apply_local_filter`/`_apply_local_optout_filter` (~19116/19141), `_render_view_all_batch`/`_append_next_view_all_batch` (~20854/20893), `_collect_sorted_results` (~20170), `show_full_text_for_result` (~20236), `open_result_in_browse_from_table` (~20265), `search_add_selected_to_list`/`search_add_row_to_list` (~20347/20374), the `_local_filter_*`/`_results_filter_*` family, `_toggle_all_terms_filter`, `_undo_zero_result_refine`, `_update_search_row_list_indicator`.

**Widgets owned (created in `create_search_tab` ~6591):** `results_table` (~7056, 13 cols), `results_stack`, `results_placeholder`, `table_container`, `search_progress`, `status_label`, `chk_search_header`, `query_input`, `mode_combo`, `btn_domain_filter`, `lbl_search_export`, `export_buttons`, `refinement_strip`, `refine_badge`.

**Coupling magnitude (the hard part):** `on_search_finished` alone touches **109 distinct `self.*` names** — ~14 `COL_*` constants, ~30 sibling widgets/state, ~40 method calls within the cluster, plus cross-cluster calls (`open_result_in_browse_from_table`→D4; `start_metadata_loading`/`_launch_enrichment_workers`→enrichment; `_add_regular_search_to_history`/`_schedule_session_save`→E3-DEFERRED).

**Boundary design (sketch, following the MyLibraryTab model):**
```python
# desktop/search_results_panel.py (NEW — SP-2 idiom)
from PyQt6.QtCore import pyqtSignal
from PyQt6.QtWidgets import QWidget
from genizah_core import tr, CURRENT_LANG  # SP-1
class SearchResultsPanel(QWidget):
    result_open_requested = pyqtSignal(dict)     # → GUI opens in Browse (the D3→D4 edge)
    add_to_list_requested = pyqtSignal(list)
    composition_requested = pyqtSignal(dict)
    def __init__(self, parent=None):
        super().__init__(parent)
        self._parent_window = parent
        self._build_ui()        # owns results_table, search_progress, status_label, ...
    @property
    def searcher(self):
        return getattr(self._parent_window, "searcher", None)
    @property
    def meta_mgr(self):
        return getattr(self._parent_window, "meta_mgr", None)
```
> **Risk flag for the planner (accept this scope):** because 40+ method calls cross between search-results methods and other GenizahGUI methods, the cleanest 126 outcome is a `SearchResultsPanel` that still DELEGATES a handful of session/history calls (`_add_regular_search_to_history`, `_schedule_session_save`) via `self._parent_window.<method>` (lazy). Those belong to E3/DEFER-04 — a fully-decoupled panel is NOT a 126 goal. The 126 goal: the search-results WIDGETS + render/filter logic LIVE in `desktop/search_results_panel.py`, re-imported into `genizah_app.py`.

**NEW test:** `tests/test_search_results_panel.py` — construct `SearchResultsPanel` headless, MOCK `gui_threads.SearchThread`, assert render/filter behavior. **MUST add `"test_search_results_panel.py"` to `_GUI_TEST_FILES` in `tests/conftest.py:92`** (Pitfall 6 — else CI exit 139). Model on `test_telemetry_consent_ux.py` (runtime-construct gui test). Run `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen`.
**Shim:** SP-4. **GUARD-03:** none of the source-scan tests pin D3 methods; new test only.

---

### `desktop/browse_panel.py` + `desktop/reading_desk_panel.py` (D4 — browse + reading desk, request-response/streaming) — DEPENDS ON D3

**Analog:** `desktop/my_library_tab.py` (panel) + `shared/reading_desk_model.py` (`ReadingDeskState` — already extracted, do NOT rebuild).

**Why D3-before-D4 (CONFIRMED):** `browse_text` (`self.browse_text = QTextEdit()` ~8155) is shared — written by search-result snippet highlighting (`apply_line_numbered_text(self.browse_text, …)`, `apply_find_highlight(self.browse_text, …)`). D3 establishes who owns the result→browse hand-off (`open_result_in_browse_from_table` ~20265 → `open_result_in_browse` ~20446).

**Browse methods (61, re-grep):** `create_browse_tab` (~7615), `browse_load` (~25316), `browse_navigate` (~25622), `browse_render_page` (~25668), `_browse_change_version`/`_browse_load_version`/`_browse_display_version_text` (~4805/4821/4913), `_populate_pgp_combo` (~4981), `on_browse_enriched_loaded` (~8783), `_start_browse_enrichment` (~8415), `_build_browse_enriched_html` (~9523), `toggle_browse_view_all`/`browse_load_all` (~11274/11536), `browse_search_parallels` (~11454), `_open_local_browse`/`_open_local_browse_page` (~20648/20989), `fetch_browse_thumbnail` (~26098).

**Reading desk methods (12 `_browse_rd_*`):** `_browse_enter_reading_desk` (~10280), `_browse_rd_enrich_entry` (~10388), `_browse_rd_render` (~10862), `_browse_rd_render_images` (~10991), `_browse_rd_setup_sync_scroll` (~11147), `_browse_rd_add_entry` (~10634), `_browse_rd_remove_entry` (~11207). State: `self.browse_reading_desk_state = ReadingDeskState()` (~8212, 10471).

**LOAD-BEARING — `browse_thumb_resolved = pyqtSignal(str, object)`** is a GenizahGUI CLASS-level signal (~3359) used by thumbnail resolution. If `BrowsePanel` owns it, move it onto the panel class (SP-5 — class-attr pyqtSignal on the QWidget) and rewire `.connect()`/`.emit()` sites. Apply the `ResultDialog.thumb_resolved` precedent (`result_dialog.py:46,72`).

**Coupling:** SP-2 (`meta_mgr`/`searcher`); enrichment via `meta_mgr`; opens `FjmsCatalogDialog`/`ResultDialog` (already extracted). Boundary: `BrowsePanel(QWidget)` owns `browse_text` + browse widgets; reading desk as a SUB-WIDGET `ReadingDeskPanel` the BrowsePanel embeds (NOT a mixin — no mixin pattern exists in this repo; RESEARCH Alternatives).
**Shim:** SP-4 (two new import lines).
**GUARD-03 (ALL source-scan; green in 126 via copy-not-move; ADDITIVELY-retarget to ALSO accept `desktop/browse_panel.py`; flip to new-only in 127):**
- `test_browse_synthetic.py` — scans `genizah_app.py` for `PNX_MANUSCRIPTS{sys_id}` / `PNX_MANUSCRIPTS{self.current_browse_sid}` (~316-317).
- `test_local_browse_panel.py` — AST-scans for `GenizahGUI._open_local_browse` + `_get_local_full_text_for_sys_id`.
- `test_wr01_open_local_browse_page_ast.py` — AST-counts `_open_local_browse_page` (ROADMAP GUARD-03).
- `test_browse_state.py` — WEB-side (`web.pages.browse_state`) — NO retarget (false positive).
- Additionally verified to pin browse methods (include in additive set): `test_desktop_folio_navigation.py` (ROADMAP GUARD-03), `test_desktop_pending_corrections.py`, `test_fgp_chooser_integration.py`, `test_view_all_cap.py` (ROADMAP GUARD-03), `test_view_all_incremental.py`, `test_local_filter_cascade.py`, `test_local_nav_codex_fix7.py`/`fix8.py`.

---

### `desktop/lists_tab.py` (D5 — lists tab + cloud sync, CRUD/pub-sub) — depends on core `ListsManager` (already extracted Phase 123)

**Analog:** `desktop/my_library_tab.py` (panel) + `genizah_app.py:13406-13474` `_lists_auto_sync` (coordinator logic).

**Build + methods (re-grep):** `create_lists_tab` (~13010) → `self.lists_tab` (tab index 4); 47 `lists_*`/`_lists_*` methods: sidebar/tree (`lists_refresh_sidebar` ~13476, `lists_refresh_items` ~13602, `lists_handle_tree_reorder` ~13564), CRUD (`lists_create_new_list` ~14061, `lists_delete_current_list` ~14094, `lists_merge_lists` ~14125, `lists_cleanup_duplicates` ~14160), item ops, preview (`_lists_load_preview` ~13926), export (`_export_as_text`/`_json`/`_excel`/`_word` ~14951-15033), `show_add_to_list_menu` (~15132). 161 distinct `self.*`, ALL `lists_*`/`_lists_*`-namespaced.

> **SCOPE D5 to the Personal Lists tab (index 4) + cloud sync.** Community-tab populators (`_populate_discoveries_list` ~15569, `_populate_joins_list` ~16106, etc.) are the SEPARATE Community tab (index 5) — leave them in `genizah_app.py` (RESEARCH Open Q2 / Assumption A2). Confirm cluster boundary with Codex PLAN pre-flight.

**LOAD-BEARING — `_ListsSyncCoordinator` (NEW plain helper, NOT a QWidget; SP-5 injection):** owns the cloud-sync gate + the debounce state that MUST travel with it. Today the debounce is GenizahGUI CLASS-level state read/written via `self.__class__._auto_sync_*`:
```python
# genizah_app.py:13406-13436 — class-level debounce state + gate (MOVE into the coordinator)
_auto_sync_pending = False
_auto_sync_last = 0
def _lists_auto_sync(self):
    if not self.lists_mgr: ...
    if not hasattr(self.lists_mgr, 'is_sync_available') or not self.lists_mgr.is_sync_available():
        return
    now = time.time()
    if now - self.__class__._auto_sync_last < 2:   # debounce — MUST stay coherent
        return
    if self.__class__._auto_sync_pending:
        return
    self.__class__._auto_sync_pending = True
    self.__class__._auto_sync_last = now
    # ... daemon-thread sync_to_cloud() with 30s timeout + supabase host pre-resolve (13441-13471)
```
> The `_auto_sync_pending`/`_auto_sync_last` debounce state must become coordinator instance/class state — else the debounce breaks. `_enable_lists_cloud_sync` (~4239) resolves the user UUID from `corrections_client` + calls `lists_mgr.enable_cloud_sync(uuid, supabase_client=…)` — preserve the `is_sync_available()` gate + authenticated client pass-through VERBATIM (Security V2/V4). Coordinator takes `lists_mgr` + `corrections_client` by injection (SP-5); panel/GUI calls `coordinator.auto_sync()` after mutations.

**Coupling:** SP-2 (`lists_mgr` = `ListsManager(self.meta_mgr)` @ 3560, `corrections_client`, `meta_mgr`, `searcher`). Cross-tab: `_set_active_tab`. Boundary: `ListsPanel(QWidget)` owns the `lists_*` methods + widgets.
**Shim:** SP-4 (`ListsPanel`, `_ListsSyncCoordinator`).
**GUARD-03:** `test_add_to_list_dialog_ui_context.py` — WEB (`web.components.add_to_list_dialog`) — NO retarget (false positive). `test_user_lists_cache_isolation.py`/`_data_threading.py`/`_refresh_data_returns.py` — ALL WEB-side (`web.user_lists`/`web.state`/`web.auth_state` + `ListsManager` via the `genizah_core` facade) — NO retarget. `test_recently_viewed_bugs.py` + `test_recovery_scan_runs_cleanup.py` reference `GenizahGUI` — confirm runtime-construct (stays green via shim) vs source-scan before relying on it.

---

## No Analog Found

None. Every new module maps to a live in-repo extraction template (`MyLibraryTab` and/or `ResultDialog`). This is a copy→shim refactor of code that already exists — there is no greenfield component requiring RESEARCH.md fallback patterns.

---

## Cross-Cutting Reminders (apply to ALL clusters)

| Concern | Source / Rule | Apply to |
|---------|---------------|----------|
| Copy-not-move; delete in 127 | RESEARCH §Summary | every cluster — implementation stays in `genizah_app.py` so all source-scan tests stay green in 126 |
| Never premature-flip a source-scan test | Pitfall 1 | D1 (`test_tabular_builder_rtl`), D4 (browse scanners) — make assertions accept EITHER location in 126 |
| Per-file ruff only, never `--fix .` | Pitfall 5 | every commit (preserves `# noqa: F401` shims) |
| gui-test registration | Pitfall 6 / conftest.py:92 | D3 — add `test_search_results_panel.py` to `_GUI_TEST_FILES` |
| base-vs-HEAD name diff (the 124 lesson) | SP-4 / RESEARCH GUARD-04 | every cluster commit — do the NAME diff yourself, don't trust the failure count |
| One cohesive cluster = one atomic commit | CONTEXT recipe | D1/D2/D3/D4/D5 each |
| No `desktop/`→`genizah_app` module-level import | GUARD-01 / SP-1 | every new module (lazy `# noqa: PLC0415` only) |

## Metadata

**Analog search scope:** `desktop/` (my_library_tab.py, result_dialog.py, widgets/), `gui_threads.py`, `shared/` (search_engine.py, reading_desk_model.py, lists_manager.py), `genizah_app.py` @ base `aa215b37`, `tests/conftest.py`.
**Files scanned:** 8 read in depth (my_library_tab.py ×4 ranges, result_dialog.py, genizah_app.py ×5 ranges, gui_threads.py, conftest.py) + RESEARCH.md/CONTEXT.md full.
**Pattern extraction date:** 2026-06-26
**Line-number caveat:** all `genizah_app.py` line numbers are from RESEARCH @ base `aa215b37` and WILL drift — re-grep `^class ` and method names before any cut.
