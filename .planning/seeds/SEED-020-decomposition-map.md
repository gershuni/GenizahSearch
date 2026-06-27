---
status: shipped
---

# SEED-020 — Decomposition Strategy: Splitting the Two GenizahSearch God-Files

> **Status:** strategy / map (planning seed). Run SEED-020 as its **own milestone, LAST**, because it rewrites files that nearly every other workstream touches.
> **Validated against the live tree (2026-06-25):** `genizah_app.py` = **28,033** lines, `genizah_core.py` = **12,506** lines. The v7.9-extracted modules (`desktop/puzzle.py`, `desktop/viewers.py`, `desktop/dialogs_scholarly.py`, `desktop/dialogs_filter.py`, `desktop/vs_cache.py`) already exist — the proven recipe is in active use in this repo.

---

## 0. The one decision that shapes everything: import direction

A repo-grep settled the central question for the **core** split:

| Fact (verified) | Consequence |
|---|---|
| `genizah_core.py` imports from `web/`/`desktop/`: **none** | Core has zero back-edges today — keep it that way. |
| `genizah_core.py` module-level `shared.*` imports: **only 2** (`shared.nli_circuit_breaker`, `shared.search_tokenizer`) | These are leaf modules. The dependency direction is `genizah_core → shared-leaf`. |
| `shared/*` modules that import `genizah_core`: **7** — incl. `shared/session_persistence.py` doing **`from genizah_core import Config`** at module level | There is an *established, tolerated* direction `shared-mid-tier → genizah_core` (for `Config` + helpers). |

**Implication — the cycle pivot is `Config`.** Any new `shared/` module that holds extracted core logic and *also* needs `Config` must either (a) receive `Config` by injection, (b) import it lazily inside methods, or (c) wait until `Config` itself moves to `shared/config.py`. A module-level `from genizah_core import Config` inside a *new* core-logic module that `genizah_core` then re-imports = **import cycle**. This is the single biggest hazard and it dictates the core ordering below.

The **desktop** split has no such hazard: `genizah_app.py` is desktop-only, importers use lazy (use-site) imports, and the v7.9 precedent already proved zero back-edges are achievable with the AST-guard discipline.

---

## 1. Recommended target module layout

### Core (`genizah_core.py` → `shared/`)
Both apps import `genizah_core`; extracted core logic goes to `shared/` and the god-file keeps a **re-export shim** (`from shared.X import Y  # noqa: F401`) for 1–2 phases so all current `from genizah_core import …` callers keep working unchanged.

| Cluster (from map) | New module | Why this split |
|---|---|---|
| Responsa parsing/expansion + `VariantManager` + `CodicologicalManager` | `shared/responsa.py` (optionally `shared/variants.py` + `shared/codicological.py` if size warrants) | Map risk **low**; ~3,271 lines of existing tests; pure functions, no `meta_mgr`/`var_mgr` coupling, no cycle. **De-risk spine.** |
| `JoinsManager`, `ListsManager`, browse-map utils | `shared/joins_manager.py` + `shared/lists_manager.py` (+ `shared/browse_map_utils.py` for `normalize_shelfmark`/`natural_sort_key`/`dedupe_browse_map`/`get_library_display`) | Map risk **low**; the two managers have **zero intra-coupling**; web (`user_lists.py`) and desktop already wrap them. |
| `Config` | `shared/config.py` | **Enabling refactor** — must precede the metadata/engine moves to break the `Config` cycle. Static namespace, no mutable state. |
| `_BoundedLRUCache` + `MetadataManager` (+ `CodicologicalManager` if not split out above) | `shared/metadata_manager.py` | Map risk **medium**; depends on `Config` (do after `shared/config.py`). |
| `Indexer` | `shared/indexer.py` | Pairs with `MetadataManager` (`Indexer.__init__(meta_mgr)`); Tantivy schema lives here. |
| `SearchEngine` | `shared/search_engine.py` | **Highest value, highest core risk.** Needs `meta_mgr`+`var_mgr` injection. Pre-split candidates *after* the class moves: `LineBreakSearcher` (~238 lines), `CompositionSearcher` (~509 lines). |
| `LabSettings`, `LabEngine` | `shared/lab_settings.py` + `shared/lab_engine.py` | Couples to `SearchEngine` via the LOCAL-LAB mirror (CR-01/CR-02). Extract **with/after** `SearchEngine`. |

### Desktop (`genizah_app.py` → `desktop/`)
Mirrors v7.9 exactly (modules already live in `desktop/`).

| Cluster (from map) | New module | Map feasibility / risk |
|---|---|---|
| Settings/Help/Tabular dialogs + table/header widgets (`SettingsDialog`, `SearchSettingsDialog`, `HelpDialog`, `TabularQueryBuilderDialog`, `ShelfmarkTableWidgetItem`, `CheckBoxHeader`, `HiddenScrollArea`, `LabPanel`) | `desktop/settings_dialogs.py` + `desktop/ui_widgets.py` (split widgets from dialogs) | feasible, **medium** |
| Catalog "Browse-by-Identification" tab | `desktop/catalog_browse.py` | feasible, **low** |
| Search results lifecycle | `desktop/search_results_panel.py` (`SearchResultsPanel(QWidget)`) | feasible, **low** |
| Browse panel + reading desk | `desktop/browse_panel.py` + `desktop/reading_desk_panel.py` (split the `_browse_rd_*` sub-cluster) | feasible, **medium** |
| Lists tab + cloud sync | `desktop/lists_tab.py` (+ a `_ListsSyncCoordinator` helper) | feasible, **medium** |
| Composition + Parallels + Lab UI | `desktop/composition_tab.py` (+ a `CompositionState` dataclass) | **infeasible as-is** — needs state-object refactor first |
| Update UI / startup / session-history | `desktop/update_ui.py` (clean sub-cluster) + remainder **deferred** | **infeasible as-is** — ~50 `self.*` couplings |

---

## 2. Ordered extraction plan (de-risk spine first)

Each step = **one cohesive unit, behind tests, one atomic commit** (per the proven recipe; deletion of the shim is a *later* commit in the next phase, paired with the AST guard).

### CORE — Phase A (leaf wins, no cycles, no engine coupling)

| # | from → to | risk | depends on | test anchor |
|---|---|---|---|---|
| A1 | `VariantManager`+`CodicologicalManager`+all `expand_*`/`parse_responsa_*` → `shared/responsa.py` | **low** | none | `test_responsa_*.py` (6 files, ~3,271 lines) — already import via `genizah_core`; shim keeps them green |
| A2 | `JoinsManager` → `shared/joins_manager.py` | **low** | A-utils (below) | `test_*_joins_*.py` (20+ integration), add a direct unit test for the new module |
| A3 | `ListsManager` → `shared/lists_manager.py` | **low** | A-utils | `test_user_lists_cache_isolation.py`, `test_recently_viewed_bugs.py` |
| A4 | browse-map utils (`normalize_shelfmark`, `natural_sort_key`, `dedupe_browse_map`, `get_library_display`, `_load_ie_volume_map`) → `shared/browse_map_utils.py` | **low** | none | new direct unit tests; existing browse tests via shim |

> A4 may be done **first** if A2/A3 reference these helpers (they do, as pure functions). Keep helpers in the god-file as a shim re-export until A2/A3 land.

### CORE — Phase B (metadata/indexer — gated by the Config refactor)

| # | from → to | risk | depends on | test anchor |
|---|---|---|---|---|
| B0 | **`Config` → `shared/config.py`** (enabling refactor; god-file re-exports `Config`) | **medium** | none | full suite + `session_persistence` import (already `from genizah_core import Config` → switch to `shared.config`, keep shim) |
| B1 | `_BoundedLRUCache` + `MetadataManager` (+ `CodicologicalManager` if not in A1) → `shared/metadata_manager.py` | **medium** | B0 | `test_browse_synthetic.py`, `test_audit_followup_2026_05_29.py`, `test_api_nli_breaker_integration.py` |
| B2 | `Indexer` → `shared/indexer.py` | **medium** | B1 | `build_index.py` smoke + add `Indexer.create_index` coverage (currently thin) |

### CORE — Phase C (engines — hardest core work, LAST in core)

| # | from → to | risk | depends on | test anchor |
|---|---|---|---|---|
| C0 | Refactor `SearchEngine.__init__` to accept `meta_mgr`+`var_mgr` as **injectable** (already the constructor shape — formalize + document); move `_LAST_RESPONSA_DOWNGRADE` thread-local handling to a clean channel | medium | B1 | `test_search_api.py`, `test_corpus_scope_routing.py`, `test_cross_side_contract.py` |
| C1 | `SearchEngine` → `shared/search_engine.py` (BrowseMap class-level cache → instance/injected; SEED-006 compat gates noted as tech-debt) | **high** | C0, B1, B2 | full search suite (broad) — reuse the existing `SearchEngine(meta_mgr, var_mgr)` bootstrap fixture |
| C2 | `LabSettings`+`LabEngine` → `shared/lab_settings.py`+`shared/lab_engine.py`; reconcile the SearchEngine↔LabEngine LOCAL-LAB mirror (CR-01/CR-02, `_lab_weights_hash_override`) | **high** | C1 | `test_comp_corpus_scope.py`, `test_lab_composition_chunk_hits.py`, `test_local_lab_invalidation.py` |
| C3 *(optional, post-move)* | within `shared/search_engine.py`: peel `LineBreakSearcher` (~238 ln) and `CompositionSearcher` (~509 ln) into sub-modules/mixins | medium | C1 | `test_boundary_search.py`, `test_composition_search.py` |

> **Pin SEED-011 here:** the composition double-prep dedup (`corpus_scope='all'` builds 222 vs 111 queries) touches the exact `search_composition_logic` / Lab composition code being moved in C1/C2. Do SEED-011 **before or together with C1** to avoid reworking moved code.

### DESKTOP — Phase D (leaf/medium panels)

| # | from → to | risk | depends on | test anchor |
|---|---|---|---|---|
| D1 | dialogs+widgets → `desktop/settings_dialogs.py` + `desktop/ui_widgets.py` (give `GenizahGUI` a clean `apply_settings`/`cancel_settings` API; D-07b telemetry snapshot stripping is load-bearing) | medium | none | `test_telemetry_consent_ux.py`, `test_tabular_builder_rtl.py` |
| D2 | catalog → `desktop/catalog_browse.py` (`_CatalogRefreshWorker` stays module-level for `pyqtSignal`) | **low** | none | `test_seed023_catalog_filters.py`, `test_catalog_availability_filter.py` |
| D3 | search results → `desktop/search_results_panel.py` (`SearchResultsPanel(QWidget)`; inject `searcher`/`meta_mgr`; expose signals) | **low** | none | add `test_search_results_panel.py` (mock `SearchThread`); existing API/E2E via shim |
| D4 | browse → `desktop/browse_panel.py`; split reading desk → `desktop/reading_desk_panel.py` | medium | D3 (shared `browse_text` widget) | `test_browse_state.py`, `test_browse_synthetic.py`, `test_local_browse_panel.py`, `test_wr01_*_ast.py` |
| D5 | lists tab → `desktop/lists_tab.py` (+ `_ListsSyncCoordinator`) | medium | A3 (core `ListsManager`) | `test_add_to_list_dialog_ui_context.py`, `test_user_lists_*` |

### DESKTOP — Phase E (hard / partial / deferred)

| # | from → to | risk | note | test anchor |
|---|---|---|---|---|
| E1 | Update UI sub-cluster (`UpdateNotificationBar`, `WhatsNewBar`, `WhatsNewDialog`, `UpdateProgressDialog`, sidecar download methods) → `desktop/update_ui.py` | medium | the **only** cleanly-extractable part of the startup/session cluster | new behavioral tests + existing sidecar tests |
| E2 | Composition tab → `desktop/composition_tab.py` | **high / blocked** | extraction marked **infeasible** until a `CompositionState` dataclass collects the ~30 `comp_*` fields and `LabPanel.set_engine` stops parent-traversing; remove dead VS remnants (D-11) first | `test_comp_*`, `test_boundary_search.py` |
| E3 | Startup + session/history remainder | **high / defer** | ~50 `self.*` couplings into search/comp/browse tab widgets; session-restore calls `on_search_finished`/`display_comp_results`. **Recommend deferring** the non-Update remainder past SEED-020, or doing it only after D3/D4/D5 expose the tab state behind setters | `test_local_optout_persistence.py`, `test_history_no_result_snapshots.py` |

---

## 3. Conflict / risk map

| Hazard | Where | Mitigation |
|---|---|---|
| **Import cycle via `Config`** | New `shared/` core modules need `Config`; `session_persistence` already imports `Config` from `genizah_core` | Extract `Config → shared/config.py` (B0) **before** B1/C1; or inject `Config` until then. Never `from genizah_core import Config` at module level inside a module the core re-imports. |
| **`SearchEngine` ↔ `LabEngine` LOCAL-LAB mirroring** | SearchEngine duplicates `reload_local_lab_index` + calls `LabEngine._check_local_lab_freshness` via `getattr` (CR-01/CR-02, `_lab_weights_hash_override`) | Move both engines in the same phase (C1→C2); keep the `getattr` graceful-guard until both land. |
| **`meta_mgr`/`var_mgr` dependency** | `SearchEngine`, `Indexer`, `LabEngine` all take them | Formalize constructor injection (C0); test bootstrap already does `SearchEngine(meta_mgr, var_mgr)`. |
| **`_my_library_tab_ref` weakref to desktop GUI** | `SearchEngine` Phase-97 R-01 gate couples core→desktop UI | Keep as an injected optional interface (duck-typed `is_searchable`); do **not** import desktop into shared. |
| **ruff `--fix` gutting re-export shims** | repo-wide auto-fix removes `# noqa: F401` re-exports | Per-file ruff review on extraction commits; never whole-repo `--fix`. |
| **Source-scanning tests read the god-file** | `test_desktop_folio_navigation.py` greps `genizah_app.py` source | Parametrize fixtures to read **both** locations during the additive phase; flip to new path at deletion. |
| **Deletion before migration stable** | removing shim too early breaks `from genizah_app import …` callers (16+ test files, `desktop/join_workbench.py`) | Delete shim + add AST guard in the *next* phase only after a clean back-edge grep. |
| **Desktop tab widgets shared across clusters** | `self.browse_text` shared by browse + search snippet highlight | Sequence D3 before D4; pass widget by injection, not attribute reach-through. |
| **`pyqtSignal` classes must stay module-level** | `_CatalogRefreshWorker`, workers in `gui_threads.py` | Extract worker classes as module-level (not nested); they already live in `gui_threads.py` in many cases. |

---

## 4. Quick wins vs hardest

**Quick wins (do first — high cohesion, low risk, strong existing tests):**
- `shared/responsa.py` (A1) — ~3,271 test lines, pure functions, **zero** coupling. Biggest single-commit win in core.
- `shared/joins_manager.py` + `shared/lists_manager.py` (A2/A3) — no intra-coupling, already wrapped by web+desktop.
- `desktop/catalog_browse.py` (D2) — risk **low**, two dedicated test files already exercise the logic.
- `desktop/search_results_panel.py` (D3) — risk **low**, self-contained, enables a new desktop unit-test suite.

**Hardest (sequence last within their app):**
- `shared/search_engine.py` (C1) + Lab engines (C2) — 500-line `execute_search`, dual-corpus RRF, BrowseMap global cache, engine cross-mirroring. The load-bearing core work.
- `desktop/composition_tab.py` (E2) — **infeasible without** a `CompositionState` dataclass refactor first.
- Startup + session/history remainder (E3) — **infeasible**; ~50 `self.*` tab couplings. Recommend deferring beyond SEED-020.

---

## 5. Open questions needing a human decision

1. **Config relocation scope.** Move all of `Config` to `shared/config.py`, or only the constants the new modules need (leave path-resolution methods in `genizah_core`)? Full move is cleaner but touches more callers.
2. **Granularity of `shared/responsa.py`.** One module, or split `VariantManager`/`CodicologicalManager`/expansion-functions into three? The map says they are independent subsystems — three modules is cleaner but is 3 commits vs 1.
3. **How aggressively to pre-split `SearchEngine`** (C3) — peel `LineBreakSearcher`/`CompositionSearcher` now, or ship a single `shared/search_engine.py` and defer the sub-split to a later quick task?
4. **E2/E3 in scope for SEED-020 at all?** Composition tab needs a `CompositionState` refactor and the startup/session remainder is structurally infeasible. Recommendation: ship **E1 (Update UI)** only and **defer E2/E3** — but confirm, since leaving them keeps `genizah_app.py` large.
5. **Shim lifetime.** v7.9 used 1–2 phases. Given SEED-020 is the last milestone, do we want shims to persist permanently (cheap, harmless) or force a clean deletion pass at the end?
6. **SEED-011 sequencing.** Fold the composition dedup into C1/C2 (recommended) or run it as a separate pre-SEED-020 quick task so the two milestones stay independent?

---

## 6. Condensed phase grouping (theme-grouped, per user preference)

Five phases, de-risk spine thin (Phase 1), the rest merged by theme:

- **Phase 1 — Core leaf de-risk spine** *(A1–A4)*: `shared/responsa.py`, `shared/joins_manager.py`, `shared/lists_manager.py`, `shared/browse_map_utils.py`. Lowest risk, strongest tests; proves the shim+AST-guard pipeline on core before touching engines.
- **Phase 2 — Core metadata/index** *(B0–B2)*: `shared/config.py` (enabler) → `shared/metadata_manager.py` → `shared/indexer.py`.
- **Phase 3 — Core engines** *(C0–C3, + SEED-011)*: `shared/search_engine.py` + `shared/lab_engine.py`/`shared/lab_settings.py`; fold in the composition double-prep dedup. The hardest core phase.
- **Phase 4 — Desktop panels** *(D1–D5)*: settings/widgets, catalog, search-results, browse+reading-desk, lists. Mirrors v7.9 cleanly.
- **Phase 5 — Desktop hard/cleanup** *(E1; E2/E3 pending Q4)*: extract `desktop/update_ui.py`; do `CompositionState` refactor + composition-tab extraction **only if** Q4 says in-scope; otherwise close SEED-020 with E2/E3 deferred. Final pass: remove all shims, install/verify the permanent AST back-edge guards (`test_no_back_edges_core.py`, `test_no_back_edges_desktop.py`).

**Per-commit discipline throughout (proven recipe):** copy-not-move → retarget lazy imports → add minimal `# noqa: F401` re-export shim → (next phase) delete cluster + delete shim + add AST guard in **one** atomic commit; per-file ruff review; parametrized source-scanning fixtures; pre/post back-edge greps for `from genizah_core import` / `from genizah_app import`.

---

## 7. Codex review corrections (2026-06-25) — verdict: SOUND WITH CHANGES

A standalone Codex review pressure-tested the strategy against the live code. The import-direction
premise is **confirmed**; the corrections below are **authoritative over §0–§6** where they conflict.

### C-1 (HIGH) — `Config` is **Phase 0**, before the leaf spine (fixes the §2 ordering bug)
The doc wrongly assumed Phase A leaf modules have no `Config` coupling. They DO, so `Config →
shared/config.py` must come **first**, ahead of everything:
- `VariantManager` → `Config.VARIANT_GEN_LIMIT` (`genizah_core.py:3175, 3240`)
- `CodicologicalManager` → `Config.OXFORD_DB` (`:3360`)
- responsa explosion guard → `Config.MAX_EXPANDED_TERMS` (`:6965`)
- `JoinsManager` / `ListsManager` bind `Config.INDEX_DIR` **at class-definition time** (`:10812, :11345`)

Keep `genizah_core.Config` as a **re-export of the same class object** (permanent compat facade).
Import-topology nuance: the shared→core importers are **6 files, not 7**; two are module-level —
`shared/session_persistence.py:32` (`Config`) and `shared/exclusion_service.py:17` (`normalize_shelfmark`).

### C-2 (HIGH) — split the over-broad `shared/responsa.py` into THREE modules
`VariantManager`, `CodicologicalManager`, and responsa parsing are NOT one pure subsystem
(`CodicologicalManager.load()` takes `csv_bank` *from MetadataManager* — `genizah_core.py:3349`).
Target: **`shared/variants.py` + `shared/codicological.py` + `shared/responsa.py`** (3 commits, not 1).

### C-3 (MEDIUM) — engine DI is right but the hazard set is bigger
DI of `meta_mgr`/`var_mgr` is already the constructor shape (`:701`, `:7188`). Real Phase-3 landmines:
duplicated LOCAL-LAB handling (`:781` + `:7361`), the `BrowseMap` **class-level** cache (`:7895`),
the SEED-006 compat gates (`:6697`), and **existing `shared/local_indexer.py` lazy back-edges into core
helpers** (`:3154`, `:3826`) — these must be retargeted to the new shared modules, not left pointing at
the facade.

### C-4 (MEDIUM) — `_my_library_tab_ref` spans BOTH engines
Not just `SearchEngine.attach_my_library_tab()` (`:7223`) — `LabEngine.lab_composition_search()` also
checks it (`:1740`). Model it as an **injected optional "local-search-gate" interface** for both engines;
never import desktop into shared.

### C-5 (MEDIUM) — retarget MORE source-scanning tests before any deletion
The §3 list undercounts. Also source-scan/hash the god-files: `test_desktop_folio_navigation.py:32`,
`test_wr01_open_local_browse_page_ast.py:24`, `test_tabular_builder_rtl.py:15`, `test_view_all_cap.py:14`,
and `test_shelfmark_bridge.py:85` (hashes `normalize_shelfmark` source). Retarget all of these in the
additive phase; flip at deletion.

### C-6 (MEDIUM/LOW) — deferrals confirmed; update_ui needs tests
`CompositionState`-first is **not optional** (comp state scattered: `:3396, :7216, :23218, :23367, :26911,
:27148`) → E2/E3 stay **out of SEED-020**. `update_ui.py` widget classes are isolated (`:182, :362`) but the
GUI sidecar reset/download coordination (`:26118, :26218`) needs **new direct tests**.

### Adjudicated answers to the 6 open questions (§5)
1. **Config:** full move first; `genizah_core.Config` = re-export of the same object. *(→ Phase 0)*
2. **Responsa granularity:** split into 3 modules (variants / codicological / responsa).
3. **SearchEngine pre-split:** move mostly intact first; peel `LineBreakSearcher`/`CompositionSearcher`
   **after** direct-module tests are green.
4. **E2/E3 scope:** OUT of SEED-020. Ship `update_ui.py`; make `CompositionState` a **separate prerequisite seed**.
5. **Shim lifetime:** `genizah_core.py` = **permanent** compat facade; do NOT permanently keep large
   `genizah_app.py` implementation shims (force a clean deletion pass for desktop).
6. **SEED-011:** run as a **separate pre-Phase-3 task**, before moving SearchEngine/LabEngine composition code.

### Revised phase order (supersedes §6)
**Phase 0 — `shared/config.py`** (enabler) → **Phase 1 — core leaf modules** (variants, codicological,
responsa, joins/lists, browse-map utils) → **Phase 2 — metadata/indexer** → **Phase 3 — engines**
(SEED-011 first, then SearchEngine + LabEngine) → **Phase 4 — desktop panels** → **Phase 5 — `update_ui.py`
only** (E2/E3 deferred; `CompositionState` is its own prerequisite seed).
