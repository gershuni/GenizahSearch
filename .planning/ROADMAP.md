# Roadmap: GenizahSearch

## Milestones

- **v1 External Data Integration** -- Phases 1-7 (shipped 2026-02-07)
- **v5.6.0 Desktop Parity & PGP Integration** -- Phases 8-12 (shipped 2026-02-09)
- **v5.7.0 Responsa Search** -- Phases 14-17 (shipped 2026-02-10)
- **v5.7.2 Cleanup, Normalization & Sections** -- Phases 18-21 (shipped 2026-02-11)
- **v5.7.3 Pending Corrections Visibility** -- Phases 22-24 (shipped 2026-02-11)
- **v5.8.0 FJMS Integration** -- Phases 25-28 (shipped 2026-02-15)
- **v5.9.0 Multi-Source Image & Metadata Integration** -- Phases 29-34 (shipped 2026-02-16)
- **v6.0.0 Local Data Architecture** -- Phases 35-40 (shipped 2026-02-22)
- **v6.1.0 Catalog Browse & Navigation** -- Phase 41 (shipped 2026-02-27)
- **v6.5.0 Search UX & Filtered Search** -- Phases 42-46 (shipped 2026-03-14)
- **v7.0.0 Fragment Puzzle** -- Phases 47-52 (shipped 2026-03-17)
- **v7.1.0 FIST Gap Fill** -- Phase 53 (shipped 2026-03-19)
- **v7.6 Search Refinement & Scholarly Joins** -- Phases 54-57 (shipped 2026-03-31)
- **v7.7 Volume-Aware Browse** -- Phases 58-61 (shipped 2026-04-01)
- **v7.8 Structural Foundation** -- Phases 63-66 (shipped 2026-04-15)
- **v7.9 Decomposition** -- Phases 67-76 (complete 2026-04-17)
- **v7.10 Search API** -- Phases 77-83 (shipped 2026-05-05)
- **v7.11 CUDL Coverage & Synthetic Inventories** -- Phases 84-86 (shipped 2026-05-12)
- **v7.12 Multitenant Architecture (Path B)** -- Phases 87-92 + 92.1 + 92.2 + promoted 999.1/999.4 (shipped 2026-05-18)
- **v7.13 Research-Grade Downloads & PGP Filter** -- Phases 93-94 (shipped 2026-05-21)
- **v7.14 My Library — Local Document Search** -- Phases 95-98 (shipped 2026-05-24; closed 2026-05-27)
- **v7.15 My Library Visual** -- Phases 99-101 (shipped 2026-05-28). See `milestones/v7.15-ROADMAP.md`
- **v7.16 Hebrew PDF Text Quality** -- Phase 102 + no-phase quality work (shipped 2026-06-01). See `milestones/v7.16-ROADMAP.md`
- **v8.0.0 Dicta Rebrand & Joins Lab** -- BRAND (no-phase) + Phases 103, 105 (folded from v7.17; Phase 104 → EXP-F3) + Phases 106-110 Joins Lab (shipped 2026-06-09; closed 2026-06-11). Component B (JSA-01/02/03 + JWB-05) + web Joins Lab UI deferred post-v8.0.0. See `milestones/v8.0.0-ROADMAP.md`
- **v8.1.0 Desktop Telemetry** -- Phases 111-116 (shipped 2026-06-16; closed 2026-06-16). See `milestones/v8.1.0-ROADMAP.md`
- ✅ **v8.2.0 Web Joins Lab, FGP Transcriptions & Hebrew Search** -- Phases 117-121 (shipped 2026-06-23, both apps)
- 🚧 **v8.3.0 God-File Decomposition** -- Phases 122-127 (in progress)

## Phases

<details>
<summary>✅ v8.2.0 Web Joins Lab, FGP Transcriptions & Hebrew Search (Phases 117-121) — SHIPPED 2026-06-23, both apps</summary>

See: .planning/milestones/v8.2.0-ROADMAP.md

5 phases (117-121). Ported the desktop Joins Lab (Component A) to the web at `/joins-lab` at full parity — anchor pane + line-by-line builders for both leaf sides + deduped candidate grid/table + side-by-side Compare + Visual Similarity toggle + Add-as-Join/Puzzle/list; bilingual EN/HE + RTL, no login, server-side per-session state via `safe_storage`. Bundled beyond scope: FGP transcriptions go-live (both apps), SEED-006 Hebrew/Judeo-Arabic search, Responsa-operators-over-My-Library (desktop). Phase dirs archived to `.planning/milestones/v8.2.0-phases/`.

</details>

<details>
<summary>✅ v8.1.0 Desktop Telemetry (Phases 111-116) — SHIPPED 2026-06-16, closed 2026-06-16</summary>

See: .planning/milestones/v8.1.0-ROADMAP.md

6 phases (111-116), 20 plans, 32 tasks. Opt-in, privacy-preserving desktop telemetry for "Dicta Genizah Search Pro" — anonymous usage analytics, crash reports, and per-session performance summaries flow to the shared web PostHog project (id 134161, EU), identity-aligned with the web app (logged-in users → same Supabase `user.id`), split by `platform=desktop`. Default OFF until the user consents via a bilingual first-run dialog; never transmits search content or My Library data. Also bundled: desktop "Public API & AI Tools" advertising and web Search API enhancements (quick task 260616-p9x) + the `platform=web` super-property.

</details>

<details>
<summary>✅ v8.0.0 Dicta Rebrand & Joins Lab (Phases 103, 105 + 106-110) — SHIPPED 2026-06-09, closed 2026-06-11</summary>

See: .planning/milestones/v8.0.0-ROADMAP.md

7 phases — 103 + 105 (folded from the v7.17 cycle) + 106-110 (Joins Lab Component A). 25 requirements satisfied (BRAND 2 + LEXP 7 + EXPUX 4 + JWB 9 + COMP-LOC 2 + EXP-F3 1). Desktop Joins Lab: shared core (`shared/joins_lab.py`) + anchor pane + line-by-line query builders for both sides of the leaf + deduped candidate grid/table + side-by-side Compare + pairwise→group join model + Visual Similarity toggle. Component B (JSA-01/02/03 + JWB-05) and web Joins Lab UI deferred.

</details>

---

### 🚧 v8.3.0 God-File Decomposition (Phases 122-127) — In Progress

**Milestone goal:** Split the two god-files — `genizah_app.py` (~28k lines, desktop) and `genizah_core.py` (~12.5k lines, shared by both apps) — into cohesive modules using the proven v7.9 extract-behind-tests / one-atomic-commit-per-cluster recipe. Zero behavior change. Pure internal maintainability work — no user-facing change, no GitHub Release (label-only version bump). Strategy in `.planning/seeds/SEED-020-decomposition-map.md` (§7 "Codex review corrections" authoritative).

**Hard constraints across all phases (GUARD invariants):**

- GUARD-01: No module-level import back-edges — no cycle through any extracted `shared/` module. AST/import guard installed in Phase 122 and enforced at every phase boundary.
- GUARD-02: Zero behavior change — full existing pytest suite (search / browse / responsa / joins / lists / composition parity, web + desktop import paths) passes at every phase boundary.
- GUARD-03: Every source-scanning / AST test that reads `genizah_core.py` or `genizah_app.py` is retargeted to the new module location before the original implementation is deleted (5 named files: `test_desktop_folio_navigation.py`, `test_wr01_open_local_browse_page_ast.py`, `test_tabular_builder_rtl.py`, `test_view_all_cap.py`, `test_shelfmark_bridge.py`). Retarget during the additive phase; flip at deletion.
- GUARD-04: `genizah_core.py` remains a permanent compatibility facade (re-export shims preserved); `genizah_app.py` implementation shims are removed in a clean final deletion pass.
- Per-commit discipline: copy-not-move → retarget importers → add `# noqa: F401` re-export shim → (next phase) delete original + add AST guard, one atomic commit per cluster; per-file ruff review only (never repo-wide `ruff --fix`).

## Summary Checklist

- [x] **Phase 122: Config Enabler** - Extract `Config` to `shared/config.py`; install back-edge AST guard (GUARD-01). Breaks the import-cycle pivot that blocks all subsequent core moves.
 (completed 2026-06-25)

- [x] **Phase 123: Core Leaf Modules** - Extract seven low-risk, well-tested clusters: `shared/variants.py`, `shared/codicological.py`, `shared/responsa.py`, `shared/joins_manager.py`, `shared/lists_manager.py`, `shared/browse_map_utils.py`, `shared/text_normalize.py`. Proves the shim+guard pipeline on core before touching engines. (completed 2026-06-25)
- [ ] **Phase 124: Core Metadata & Index** - Extract `shared/metadata_manager.py` (+ `_BoundedLRUCache`) and `shared/indexer.py`; retarget the `shared/local_indexer.py` lazy back-edges into `genizah_core` helpers.
- [ ] **Phase 125: Core Engines** - SEED-011 composition dedup first (125a), then extract `shared/search_engine.py` (DI + BrowseMap cache + SEED-006 gates + `_LAST_RESPONSA_DOWNGRADE` preserved), `shared/lab_settings.py`, `shared/lab_engine.py` (LOCAL-LAB mirror preserved), and model `_my_library_tab_ref` as an injected optional interface for both engines.
- [ ] **Phase 126: Desktop Panels** - Extract seven desktop panel clusters to `desktop/`: `settings_dialogs.py`, `ui_widgets.py`, `catalog_browse.py`, `search_results_panel.py`, `browse_panel.py`, `reading_desk_panel.py`, `lists_tab.py`.
- [ ] **Phase 127: Update UI & Final Cleanup** - Extract `desktop/update_ui.py` + new direct behavioral tests for sidecar reset/download coordination; remove all desktop shims from `genizah_app.py`; confirm `genizah_core.py` permanent facade; full-suite-green sign-off.

## Phase Details

### Phase 122: Config Enabler

**Goal**: `Config` lives in `shared/config.py`; all existing callers continue working via the `genizah_core.Config` re-export facade; and a permanent AST guard (GUARD-01) is installed to catch any future module-level back-edges from extracted `shared/` modules back into `genizah_core`.
**Depends on**: Phase 121 (v8.2.0 complete — no active code changes in flight)
**Requirements**: CONFIG-01, GUARD-01, GUARD-02, GUARD-03, GUARD-04
**Success Criteria** (what must be TRUE):

  1. `shared/config.py` exists and defines the `Config` class; `genizah_core.Config` is a re-export of the same class object (not a copy); a test imports both and asserts `shared.config.Config is genizah_core.Config`.
  2. All existing callers of `from genizah_core import Config` (including `shared/session_persistence.py:32`) continue to work without modification — the full existing pytest suite passes.
  3. A permanent CI test (`tests/test_no_back_edges_core.py`) is installed and green: it asserts no extracted `shared/` module imports `genizah_core` at module level (AST scan); it is parametrized so adding a new `shared/` module automatically enters the scan.
  4. Per-file ruff review on the extraction commit shows zero unintended F401 removals; the `# noqa: F401` shim in `genizah_core.py` is present and ruff-clean.

**Plans**: 1 plan

- [x] 122-01-PLAN.md — Extract Config to shared/config.py (same-object facade) + session_persistence retarget + GUARD-01 back-edge guard / CONFIG-01 identity test

### Phase 123: Core Leaf Modules

**Goal**: Seven low-risk, well-tested core clusters are extracted to `shared/` behind re-export shims: `shared/variants.py` (`VariantManager`), `shared/codicological.py` (`CodicologicalManager`), `shared/responsa.py` (responsa parsing/expansion), `shared/joins_manager.py` (`JoinsManager`), `shared/lists_manager.py` (`ListsManager`), `shared/browse_map_utils.py` (browse-map + shelfmark utilities), and `shared/text_normalize.py` (`strip_nikud`, `strip_search_diacritics`, normalization constants). The lazy back-edges in `shared/local_indexer.py` that point at these helpers via `genizah_core` are retargeted to the new modules.
**Depends on**: Phase 122
**Requirements**: CORE-01, CORE-02, CORE-03, CORE-04, CORE-05, CORE-06, CORE-07, GUARD-02, GUARD-03, GUARD-04
**Success Criteria** (what must be TRUE):

  1. `from shared.responsa import ...`, `from shared.variants import VariantManager`, and `from shared.codicological import CodicologicalManager` all resolve; `from genizah_core import ...` for each of these names also resolves via the re-export shims; both paths produce the same class/function objects.
  2. The full responsa test suite (`tests/test_responsa_*.py`, ~3,271 test lines) passes — importing through either `genizah_core` or the new `shared.responsa` module.
  3. `shared/local_indexer.py` no longer imports these helpers via `genizah_core` at module level; the retargeted imports point directly at the new `shared/` modules; the GUARD-01 back-edge CI test remains green.
  4. All five source-scanning / AST tests that read `genizah_core.py` or `genizah_app.py` (`test_desktop_folio_navigation.py`, `test_wr01_open_local_browse_page_ast.py`, `test_tabular_builder_rtl.py`, `test_view_all_cap.py`, `test_shelfmark_bridge.py`) are retargeted to both locations during the additive phase and remain green — no deletion of originals until Phase 127.
  5. Per-file ruff review on each extraction commit shows no unintended stripping of `# noqa: F401` re-export shims in `genizah_core.py`; the full existing pytest suite passes at every cluster commit boundary.

**Plans**: 1 plan (7 sequential waves, one atomic commit per cluster — D-02 leaf-first ordering)

Plans:

- [x] 123-01-PLAN.md — Extract 7 core leaf clusters (browse_map_utils -> text_normalize -> variants -> responsa -> codicological -> joins_manager -> lists_manager) behind permanent same-object re-export shims; D-01 back-edge retargets; GUARD-01 registry 1->8; D-03 identity/smoke tests; snapshot regen

### Phase 124: Core Metadata & Index

**Goal**: `MetadataManager` (and `_BoundedLRUCache`) are extracted to `shared/metadata_manager.py`, and `Indexer` is extracted to `shared/indexer.py`. These depend on `shared/config.py` (Phase 122) and are prerequisites for the engine moves in Phase 125.
**Depends on**: Phase 122, Phase 123
**Requirements**: CORE-08, CORE-09, GUARD-02, GUARD-03, GUARD-04
**Success Criteria** (what must be TRUE):

  1. `from shared.metadata_manager import MetadataManager` and `from shared.indexer import Indexer` both resolve; `from genizah_core import MetadataManager` and `from genizah_core import Indexer` also resolve via re-export shims; both paths yield the same class objects.
  2. `_BoundedLRUCache` is co-located in `shared/metadata_manager.py` (not left as an orphan in `genizah_core.py`); the existing browse/API/NLI-breaker integration tests that exercise `MetadataManager` pass unchanged (`tests/test_browse_synthetic.py`, `tests/test_audit_followup_2026_05_29.py`, `tests/test_api_nli_breaker_integration.py`).
  3. `build_index.py` continues to resolve `Indexer.create_index` (smoke-importable); any existing direct `Indexer` coverage tests pass.
  4. No new module-level back-edge from `shared/metadata_manager.py` or `shared/indexer.py` into `genizah_core`; the GUARD-01 CI test remains green; per-file ruff review shows shims intact.

**Plans**: TBD

### Phase 125: Core Engines

**Goal**: The hardest core phase. SEED-011 composition double-prep dedup lands first (125a, before the engine code moves). Then `SearchEngine` is extracted intact to `shared/search_engine.py` with formalized `meta_mgr`/`var_mgr` dependency injection, and the three critical hazards explicitly preserved: the BrowseMap class-level cache migration, the SEED-006 `content_search` compat gates, and the `_LAST_RESPONSA_DOWNGRADE` thread-local downgrade channel. `LabSettings` and `LabEngine` follow in the same phase, with the SearchEngine↔LabEngine LOCAL-LAB mirror (`_lab_weights_hash_override`, CR-01/CR-02) preserved intact. The `_my_library_tab_ref` coupling is modeled as an injected optional "local-search-gate" interface on both engines so no `shared/` module imports desktop code.
**Depends on**: Phase 124
**Requirements**: PREP-01, CORE-10, CORE-11, CORE-12, CORE-13, GUARD-02, GUARD-03, GUARD-04
**Success Criteria** (what must be TRUE):

  1. SEED-011 composition dedup is in place before any engine code moves: `corpus_scope='all'` composition no longer builds 2× the query set; the relevant composition tests pass.
  2. `from shared.search_engine import SearchEngine`, `from shared.lab_engine import LabEngine`, and `from shared.lab_settings import LabSettings` all resolve; `from genizah_core import SearchEngine` (and the other names) resolve via re-export shims; both paths yield the same class objects.
  3. The full search test suite passes — including `tests/test_search_api.py`, `tests/test_corpus_scope_routing.py`, `tests/test_cross_side_contract.py`, `tests/test_comp_corpus_scope.py`, `tests/test_lab_composition_chunk_hits.py`, and `tests/test_local_lab_invalidation.py` — with no behavior change in any search mode (keyword / Responsa / composition / parallels / Local / ALL).
  4. The BrowseMap class-level cache, the SEED-006 `content_search` compat gates, and the `_LAST_RESPONSA_DOWNGRADE` thread-local channel all work identically after the move — no cache invalidation, no compat gate regression, no Responsa downgrade loss.
  5. `LabEngine.lab_composition_search()` and `SearchEngine.attach_my_library_tab()` accept the injected optional local-search-gate interface and function correctly; no `shared/` module imports `desktop/` or `genizah_app`; the GUARD-01 back-edge CI test remains green.

**Plans**: TBD

### Phase 126: Desktop Panels

**Goal**: Seven desktop panel clusters are extracted from `genizah_app.py` to `desktop/` modules: `desktop/settings_dialogs.py` (Settings/Help/Tabular-builder dialogs, D-07b telemetry snapshot stripping preserved), `desktop/ui_widgets.py` (table/header/scroll widget classes), `desktop/catalog_browse.py` (catalog Browse-by-Identification tab), `desktop/search_results_panel.py` (`SearchResultsPanel(QWidget)`), `desktop/browse_panel.py` (browse panel), `desktop/reading_desk_panel.py` (reading desk), and `desktop/lists_tab.py` (lists tab + cloud-sync coordination). The v7.9 proven recipe (copy-not-move; shim; delete+guard next phase) applies. `pyqtSignal` worker classes stay at module level. `D3` (search results) is sequenced before `D4` (browse) because `browse_text` is shared.
**Depends on**: Phase 125
**Requirements**: DESK-01, DESK-02, DESK-03, DESK-04, DESK-05, DESK-06, DESK-07, GUARD-02, GUARD-03, GUARD-04
**Success Criteria** (what must be TRUE):

  1. Each of the seven `desktop/` modules exists and imports cleanly in isolation (no import of `genizah_app` at module level); `genizah_app.py` re-exports each panel class so all current `from genizah_app import ...` call sites (test files + `desktop/join_workbench.py` + 16+ other files) continue to work unchanged.
  2. The existing desktop panel test suites pass via the re-export shims: `tests/test_telemetry_consent_ux.py`, `tests/test_tabular_builder_rtl.py`, `tests/test_seed023_catalog_filters.py`, `tests/test_catalog_availability_filter.py`, `tests/test_browse_state.py`, `tests/test_browse_synthetic.py`, `tests/test_local_browse_panel.py`, `tests/test_wr01_open_local_browse_page_ast.py`, `tests/test_add_to_list_dialog_ui_context.py`, `tests/test_user_lists_*.py`.
  3. A new `tests/test_search_results_panel.py` (mock `SearchThread`) exercises `SearchResultsPanel` directly, imported from `desktop/search_results_panel.py` — the first panel to have a direct-module test.
  4. `pyqtSignal`-bearing worker classes (e.g. `_CatalogRefreshWorker`) remain at module level in their new `desktop/` home; the desktop app starts and the affected tabs are fully functional (desktop smoke-import + headless PyQt6 construction test green).
  5. The full existing pytest suite passes; per-file ruff review on each extraction commit shows no unintended shim stripping in `genizah_app.py`.

**Plans**: TBD

### Phase 127: Update UI & Final Cleanup

**Goal**: The last extractable desktop cluster — `desktop/update_ui.py` (notification bar, What's-New bar/dialog, update progress dialog, sidecar reset/download coordination) — lands with new direct behavioral tests for the sidecar reset/download coordination methods. Then: all implementation shims are removed from `genizah_app.py` (it keeps only thin `import ... as ...` re-exports for the deleted clusters); `genizah_core.py` permanent facade is confirmed intact; the GUARD-01 back-edge test and a new `tests/test_no_back_edges_desktop.py` guard are both green; the full pytest suite passes as the final sign-off.
**Depends on**: Phase 126
**Requirements**: DESK-08, GUARD-02, GUARD-03, GUARD-04
**Success Criteria** (what must be TRUE):

  1. `desktop/update_ui.py` exists and imports cleanly; `UpdateNotificationBar`, `WhatsNewBar`, `WhatsNewDialog`, and `UpdateProgressDialog` are importable from it; new direct behavioral tests covering the GUI sidecar reset/download coordination methods pass (SEED-020 §7 C-6 requirement).
  2. All `genizah_app.py` implementation shims (the desktop panel cluster code that was copied but not yet deleted in Phase 126) are removed in one clean commit; `genizah_app.py` contains only thin re-export lines for each extracted cluster; the file shrinks by at least 70% from its pre-milestone 28,033 lines.
  3. `genizah_core.py` permanent re-export facade is confirmed: `from genizah_core import Config`, `from genizah_core import SearchEngine`, and all other extracted names continue to resolve; `tests/test_genizah_core_facade.py` (new or updated) asserts the facade exports the same objects as the `shared/` modules.
  4. Both back-edge guards are green: `tests/test_no_back_edges_core.py` (GUARD-01, installed Phase 122) confirms no `shared/` module imports `genizah_core` at module level; `tests/test_no_back_edges_desktop.py` (new, this phase) confirms no `desktop/` module imports `genizah_app` at module level.
  5. The full existing pytest suite (all categories: search, browse, responsa, joins, lists, composition parity, web + desktop import paths) is green — the milestone's final zero-behavior-change sign-off.

**Plans**: TBD

## Progress

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 117. Vertical Spine | 6/6 | Complete | 2026-06-18 |
| 118. Joins, Entry & Full Builders | 6/6 | Complete | 2026-06-19 |
| 119. Candidates, Compare & Visual Similarity | 11/11 | Complete | 2026-06-19 |
| 120. Actions & Persistence | 7/8 | Complete | 2026-06-21 |
| 121. i18n Polish | 3/3 | Complete | 2026-06-21 |
| 122. Config Enabler | 1/1 | Complete    | 2026-06-25 |
| 123. Core Leaf Modules | 1/1 | Complete   | 2026-06-25 |
| 124. Core Metadata & Index | 0/TBD | Not started | - |
| 125. Core Engines | 0/TBD | Not started | - |
| 126. Desktop Panels | 0/TBD | Not started | - |
| 127. Update UI & Final Cleanup | 0/TBD | Not started | - |
