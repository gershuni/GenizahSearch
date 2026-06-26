# Requirements: GenizahSearch — v8.3.0 God-File Decomposition

**Defined:** 2026-06-25
**Core Value:** Researchers can find what they need in the Genizah corpus. (This milestone protects that value by making the two god-files maintainable — **zero behavior change**.)

> Strategy / dependency analysis / risk map: `.planning/seeds/SEED-020-decomposition-map.md` (§7 "Codex review corrections" authoritative). Requirements refined per the Codex requirements pre-flight (2026-06-25, verdict READY WITH EDITS — all 9 edits folded in).
> Each requirement is an **extraction outcome**, verified by: the new module exists, imports work in BOTH directions (web + desktop), the relevant test suite passes via the re-export facade, and no behavior changes.

## v1 Requirements (milestone v8.3.0 scope)

### Invariants (GUARD) — cross-cutting, enforced at EVERY phase boundary (122–127)

- [x] **GUARD-01**: No module-level import back-edges — no `shared/` module extracted this milestone is imported at module level by a `genizah_core` symbol that it in turn imports (no cycle). A permanent AST/import guard test enforces this from Phase 0 onward.
- [x] **GUARD-02**: Zero behavior change — the full existing pytest suite (search / browse / responsa / joins / lists / composition parity, web + desktop import paths) passes at every phase boundary.
- [x] **GUARD-03**: Every source-scanning / AST test that reads `genizah_core.py` or `genizah_app.py` is retargeted to the new module location **before** the original implementation is deleted — explicitly including `test_desktop_folio_navigation.py`, `test_wr01_open_local_browse_page_ast.py`, `test_tabular_builder_rtl.py`, `test_view_all_cap.py`, and `test_shelfmark_bridge.py` (which hashes `normalize_shelfmark` source) — retargeted during the additive phase, flipped at deletion.
- [x] **GUARD-04**: `genizah_core.py` remains a permanent compatibility facade (re-export shims preserved); the `genizah_app.py` implementation shims are removed in a clean final deletion pass.

### Config enabler (CONFIG) — Phase 122 (Phase 0)

- [x] **CONFIG-01**: `Config` is defined in `shared/config.py`; `genizah_core.Config` re-exports the same class object; all existing `from genizah_core import Config` callers (incl. `shared/session_persistence.py`) work unchanged.

### Core extractions (CORE) — `genizah_core.py` → `shared/`

- [x] **CORE-01**: Responsa parsing/expansion logic extracted to `shared/responsa.py`; the responsa test suites pass via the facade.
- [x] **CORE-02**: `VariantManager` extracted to `shared/variants.py`.
- [x] **CORE-03**: `CodicologicalManager` extracted to `shared/codicological.py`.
- [x] **CORE-04**: `JoinsManager` extracted to `shared/joins_manager.py`.
- [x] **CORE-05**: `ListsManager` extracted to `shared/lists_manager.py`.
- [x] **CORE-06**: Browse-map + shelfmark utilities (`normalize_shelfmark`, `natural_sort_key`, `dedupe_browse_map`, `get_library_display`, IE-volume helpers) extracted to `shared/browse_map_utils.py`.
- [x] **CORE-07**: Search/text normalization helpers (`strip_nikud`, `strip_search_diacritics`, and their normalization constants) extracted to `shared/text_normalize.py`; the lazy back-edge imports in `shared/local_indexer.py` (and any other shared importers of these core helpers) are retargeted to the new module so no module-level core back-edge remains. *(closes SEED-020 §7 C-3)*
- [x] **CORE-08**: `MetadataManager` (+ `_BoundedLRUCache`) extracted to `shared/metadata_manager.py`.
- [x] **CORE-09**: `Indexer` extracted to `shared/indexer.py`.
- [ ] **CORE-10**: `SearchEngine` extracted (intact) to `shared/search_engine.py` with `meta_mgr`/`var_mgr` passed by dependency injection; the BrowseMap class-level cache migration, the SEED-006 `content_search` compat gates, and the `_LAST_RESPONSA_DOWNGRADE` thread-local downgrade channel are explicitly preserved with behavior unchanged. *(SEED-020 §7 C-3 hazards)*
- [x] **CORE-11**: `LabSettings` extracted to `shared/lab_settings.py`.
- [ ] **CORE-12**: `LabEngine` extracted to `shared/lab_engine.py`; the SearchEngine↔LabEngine LOCAL-LAB mirror (CR-01/CR-02, `_lab_weights_hash_override`) preserved.
- [ ] **CORE-13**: `_my_library_tab_ref` modeled as an injected optional "local-search-gate" interface consumed by BOTH `SearchEngine.attach_my_library_tab()` and `LabEngine.lab_composition_search()`; no `shared/` → desktop import. *(closes SEED-020 §7 C-4)*

### Composition dedup prerequisite (PREP) — Phase 125, FIRST (125a), before the engine move

- [x] **PREP-01**: SEED-011 composition double-prep dedup lands **before** `SearchEngine`/`LabEngine` composition code is moved (so the dedup is not reworked post-move).

### Desktop extractions (DESK) — `genizah_app.py` → `desktop/`

- [ ] **DESK-01**: Settings / Help / Tabular-builder dialogs extracted to `desktop/settings_dialogs.py`.
- [ ] **DESK-02**: Table / header / scroll widget classes extracted to `desktop/ui_widgets.py`.
- [ ] **DESK-03**: Catalog "Browse-by-Identification" tab extracted to `desktop/catalog_browse.py`.
- [ ] **DESK-04**: Search-results lifecycle extracted to `desktop/search_results_panel.py`.
- [ ] **DESK-05**: Browse panel extracted to `desktop/browse_panel.py`.
- [ ] **DESK-06**: Reading desk extracted to `desktop/reading_desk_panel.py`.
- [ ] **DESK-07**: Lists tab + cloud-sync coordination extracted to `desktop/lists_tab.py`.
- [ ] **DESK-08**: Update-UI sub-cluster (notification / What's-New / progress dialogs + sidecar reset/download coordination) extracted to `desktop/update_ui.py`, with **new direct behavioral tests** for the sidecar reset/download coordination methods plus the existing sidecar tests. *(SEED-020 §7 C-6)*

## v2 Requirements (deferred — NOT in this roadmap)

### Future decomposition (DEFER)

- **DEFER-01**: `SearchEngine` internal sub-split — peel `LineBreakSearcher` (~238 ln) and `CompositionSearcher` (~509 ln) out of `shared/search_engine.py` (only after CORE-10 ships and its direct-module tests are green).
- **DEFER-02**: `CompositionState` dataclass refactor of `genizah_app.py`'s scattered `comp_*` fields — the **prerequisite** for any desktop composition-tab extraction. Own seed.
- **DEFER-03**: Desktop composition-tab extraction → `desktop/composition_tab.py` (blocked on DEFER-02).
- **DEFER-04**: Desktop startup/session remainder extraction (~50 `self.*` tab couplings) — structurally infeasible until DESK-04/05/06/07 expose tab state behind setters.

## Out of Scope

| Feature | Reason |
|---------|--------|
| Any user-facing behavior change | This is a pure refactor — zero behavior change is GUARD-02 |
| New features / performance work | Out of a decomposition milestone (SEED-011 is the one exception, as a Phase-3 prerequisite, not a feature) |
| Desktop composition-tab + startup/session extraction | Needs `CompositionState` refactor first (DEFER-02/03/04) |
| `SearchEngine` internal sub-split | Deferred to DEFER-01 after the class moves intact (CORE-10) |
| LAB side-index punctuation / SEED-006 compat-gate **redesign** | Pre-existing tech-debt; the gates are **preserved** (CORE-10), not redesigned |
| GitHub Release | Internal-only milestone (no user-facing change) |

## Traceability

GSD phase numbering continues from v8.2.0 (ended Phase 121) → this milestone is **Phases 122–127**. Finalized by the roadmapper.

| Requirement | Phase | Status |
|-------------|-------|--------|
| GUARD-01 | 122 (then enforced 122–127) | Complete |
| GUARD-02 | 122–127 (every boundary) | Complete |
| GUARD-03 | 122–127 (every boundary) | Complete |
| GUARD-04 | 122–127 (every boundary) | Complete |
| CONFIG-01 | 122 | Complete |
| CORE-01 | 123 | Complete |
| CORE-02 | 123 | Complete |
| CORE-03 | 123 | Complete |
| CORE-04 | 123 | Complete |
| CORE-05 | 123 | Complete |
| CORE-06 | 123 | Complete |
| CORE-07 | 123 | Complete |
| CORE-08 | 124 | Complete |
| CORE-09 | 124 | Complete |
| PREP-01 | 125 (125a — first) | Complete |
| CORE-10 | 125 | Pending |
| CORE-11 | 125 | Complete |
| CORE-12 | 125 | Pending |
| CORE-13 | 125 | Pending |
| DESK-01 | 126 | Pending |
| DESK-02 | 126 | Pending |
| DESK-03 | 126 | Pending |
| DESK-04 | 126 | Pending |
| DESK-05 | 126 | Pending |
| DESK-06 | 126 | Pending |
| DESK-07 | 126 | Pending |
| DESK-08 | 127 | Pending |

**Coverage:**

- v1 requirements: 27 total (4 GUARD + 1 CONFIG + 13 CORE + 1 PREP + 8 DESK)
- Mapped to phases: 27 (GUARD-02/03/04 are cross-cutting, verified at every phase boundary)
- Unmapped: 0 ✓

---
*Requirements defined: 2026-06-25*
*Last updated: 2026-06-25 after Codex requirements pre-flight (READY WITH EDITS, 9 edits applied)*
