# Phase 126 — Desktop Panels: CONTEXT

> **⚠ RE-SCOPED 2026-06-26 (user decision):** Phase 126 now ships **D1 ONLY** (the clean top-level
> dialog + widget CLASS extractions → `desktop/settings_dialogs.py` + `desktop/ui_widgets.py`, via
> MOVE-and-shim). The four METHOD-based panels below (D2 catalog tab, D3 search-results, D4
> browse/reading-desk, D5 lists) are **DEFERRED to SEED-028** — the Codex PLAN pre-flight (2 rounds)
> proved them too densely cross-called in GenizahGUI to move-and-shim safely without a prerequisite
> widget-ownership refactor. The D1–D5 detail below is retained as the historical scope + SEED-028
> starting point; **only D1 (126-01) is in-scope for execution.** See `126-PREFLIGHT-CODEX.md`.

**Status:** Discuss-phase **SKIPPED** (no genuine user-facing gray areas). Recorded per the
standing v8.3.0 autonomous directive ("skip discuss if no genuine user-facing gray areas — assess +
justify + write a short CONTEXT recording the skip"). [[feedback_no_auto_discuss]] still holds: this
is a *skip*, not an auto-answer of a discuss.

## Why discuss is skipped
Phase 126 is a **pure internal refactor, zero behavior change** — extract seven desktop UI panel
clusters from `genizah_app.py` into `desktop/` modules via the proven v7.9 copy→shim→(next-phase)
delete recipe. The architecture is fully locked upstream:
- **ROADMAP** Phase 126 entry (7 target modules, 5 success criteria, D3-before-D4 sequencing,
  pyqtSignal-workers-stay-module-level, the GUARD-03 test-file list, the new direct test).
- **SEED-020 §Desktop (D1–D5)** + §"Order of operations" + §Risk register — the authoritative map.
There are no product/UX choices for the user to make; the one real unknown (GenizahGUI↔panel
coupling) is a **research/planning** investigation + a Codex PLAN pre-flight concern, and the user has
delegated such technical sub-decisions. If research surfaces a genuine user-facing choice, pause then.

## Locked decisions (from ROADMAP + SEED-020 — do NOT re-litigate)

### Scope = D1–D5 (seven panels, five plan-clusters)
| Plan | Cluster → module(s) | Risk | Dep | GUARD-03 retarget tests |
|------|---------------------|------|-----|--------------------------|
| D1 | Settings/Help/Tabular dialogs → `desktop/settings_dialogs.py` **+** table/header/scroll widgets → `desktop/ui_widgets.py` (split widgets from dialogs). Give `GenizahGUI` a clean `apply_settings`/`cancel_settings` API. **D-07b telemetry snapshot stripping is load-bearing — preserve.** | medium | none | `test_telemetry_consent_ux.py`, `test_tabular_builder_rtl.py` |
| D2 | Catalog "Browse-by-Identification" tab → `desktop/catalog_browse.py`. `_CatalogRefreshWorker` **stays module-level** (pyqtSignal). | low | none | `test_seed023_catalog_filters.py`, `test_catalog_availability_filter.py` |
| D3 | Search results lifecycle → `desktop/search_results_panel.py` (`SearchResultsPanel(QWidget)`; **inject** `searcher`/`meta_mgr`; **expose** signals). **NEW direct test `test_search_results_panel.py`** (mock `SearchThread`). | low | none | new test + existing API/E2E via shim |
| D4 | Browse panel → `desktop/browse_panel.py`; reading desk → `desktop/reading_desk_panel.py` (split `_browse_rd_*`). | medium | **D3** (shared `browse_text` widget) | `test_browse_state.py`, `test_browse_synthetic.py`, `test_local_browse_panel.py`, `test_wr01_open_local_browse_page_ast.py` |
| D5 | Lists tab → `desktop/lists_tab.py` (+ `_ListsSyncCoordinator` helper). | medium | core `ListsManager` (already extracted Phase 123) | `test_add_to_list_dialog_ui_context.py`, `test_user_lists_*.py` |

### Recipe & invariants
- **copy-not-move → retarget lazy imports → minimal `# noqa: F401` re-export shim** in `genizah_app.py`; one cohesive cluster = one atomic commit. Shim **deletion happens in Phase 127** (not here).
- **desktop/ shims are TEMPORARY** — deleted in Phase 127 (contrast: `genizah_core.py` facade is permanent). So Phase 126 leaves `genizah_app.py` re-exporting each panel class.
- **GUARD-04 (this phase):** `genizah_app.py` re-exports each panel class so all current `from genizah_app import …` callers keep working — **16+ test files + `desktop/join_workbench.py`** + others. Verify with a base-vs-HEAD name diff (same discipline as the genizah_core facade).
- **GUARD-01 (desktop side):** no `desktop/` module imports `genizah_app` at **module level** (lazy function-body imports only, `# noqa: PLC0415`). The dedicated AST guard `test_no_back_edges_desktop.py` is installed in **Phase 127**, but keep the invariant from the first extraction.
- **GUARD-02:** zero behavior change; full suite green; **6-env pytest baseline unchanged** (`test_search_api_v2::…real_index[exact|fuzzy|responsa|shelfmark|title|variants]` — red at base, env-only).
- **pyqtSignal-bearing worker classes stay at module level** in their new `desktop/` home (`_CatalogRefreshWorker`, etc.) — pyqtSignal needs a class attribute on a QObject defined at import.
- **Never repo-wide `ruff --fix`** (strips the `# noqa: F401` shims) — per-file ruff review on each extraction commit ([[feedback_no_auto_reindex_in_init]] sibling discipline).
- **gui-test split is LOAD-BEARING here** (these ARE the GUI panels): run `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen`, never `-n auto`; add `test_search_results_panel.py` (and any new dialog/panel construction tests) to conftest `_GUI_TEST_FILES`; the `-m "gui or render_smoke"` slice is a real gate, not an afterthought. [[feedback_full_suite_testing_windows]]
- **Coupling pattern** (the v7.9 idiom, mirrors the engine `_my_library_tab_ref` injection): panels receive injected dependencies (`searcher`/`meta_mgr`/signals) and expose signals back; `GenizahGUI` keeps thin coordinator APIs. Research maps the exact `self.*` / signal / parent-traversal couplings per cluster and designs the boundary; never import `desktop`/`genizah_app` into `shared/`.

## Research directives (the planning-critical investigation)
1. **Map GenizahGUI↔panel coupling per cluster** — enumerate the `self.*` attributes, `pyqtSignal`s, and `self.parent()`/parent-traversal each panel reads/writes; design the injected-dependency + exposed-signal + GenizahGUI-API boundary so no cluster needs `genizah_app` at module level.
2. **Pin the exact class lists + current line ranges** in `genizah_app.py` for each of D1–D5 (grep, do not trust line numbers — god file). Confirm which worker classes carry `pyqtSignal` (must stay module-level).
3. **Confirm the GUARD-03 source-scanning test files** that hard-code `genizah_app`/path literals and must be retargeted *with* (not after) each move (the v7.9/124/125 lesson — count-based "0 new failures" is untrustworthy; do the base-vs-HEAD NAME-level test diff + the `from genizah_app import` name diff yourself).
4. **D-07b telemetry snapshot stripping** (D1) and the `_ListsSyncCoordinator` cloud-sync gate (D5) — confirm the exact load-bearing code so the extraction preserves it.

## Out of scope (deferred — do NOT attempt here)
- **E2 — Composition/Parallels/Lab tab** → `desktop/composition_tab.py`: **infeasible** until a `CompositionState` dataclass refactor lands first (DEFER-02/DEFER-03).
- **E3 — Startup + session/history remainder**: ~50 `self.*` tab couplings; **deferred** (DEFER-04).
- **E1 — `desktop/update_ui.py`** + all shim deletions + the `test_no_back_edges_desktop.py` AST guard + final full-suite sign-off: **Phase 127**, not here.
- Any web (`web/`) change; any behavior change.

## Base & drill
- **Base commit:** `aa215b37` (Phase 125 closeout).
- **Full drill:** research → pattern-map → plan(opus) → gsd-plan-checker → **Codex PLAN pre-flight** → execute wave-by-wave + source-integrity gate per wave → **Codex CODE review 3-round** + base-vs-HEAD name+facade diff → gsd-verifier → advance to 127. BOTH Codex gates must clear.
