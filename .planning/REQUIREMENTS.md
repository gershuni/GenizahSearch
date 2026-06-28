# Requirements: GenizahSearch — v8.3.0 God-File Decomposition + Search & Browse UX

**Defined:** 2026-06-25 (decomposition); extended 2026-06-27 (SEED-025 + SEED-026 features for the public 8.3.0 release)
**Core Value:** Researchers can find what they need in the Genizah corpus. The decomposition strand protects that value by making the two god-files maintainable (**zero behavior change**); the feature strand advances it with two user-facing search/browse additions that give the public 8.3.0 release real substance on **both apps**.

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
- [x] **CORE-10**: `SearchEngine` extracted (intact) to `shared/search_engine.py` with `meta_mgr`/`var_mgr` passed by dependency injection; the BrowseMap class-level cache migration, the SEED-006 `content_search` compat gates, and the `_LAST_RESPONSA_DOWNGRADE` thread-local downgrade channel are explicitly preserved with behavior unchanged. *(SEED-020 §7 C-3 hazards)*
- [x] **CORE-11**: `LabSettings` extracted to `shared/lab_settings.py`.
- [x] **CORE-12**: `LabEngine` extracted to `shared/lab_engine.py`; the SearchEngine↔LabEngine LOCAL-LAB mirror (CR-01/CR-02, `_lab_weights_hash_override`) preserved.
- [x] **CORE-13**: `_my_library_tab_ref` modeled as an injected optional "local-search-gate" interface consumed by BOTH `SearchEngine.attach_my_library_tab()` and `LabEngine.lab_composition_search()`; no `shared/` → desktop import. *(closes SEED-020 §7 C-4)*

### Composition dedup prerequisite (PREP) — Phase 125, FIRST (125a), before the engine move

- [x] **PREP-01**: SEED-011 composition double-prep dedup lands **before** `SearchEngine`/`LabEngine` composition code is moved (so the dedup is not reworked post-move).

### Desktop extractions (DESK) — `genizah_app.py` → `desktop/`

- [x] **DESK-01**: Settings / Help / Tabular-builder dialogs extracted to `desktop/settings_dialogs.py`.
- [x] **DESK-02**: Table / header / scroll widget classes extracted to `desktop/ui_widgets.py`.
- [~] **DESK-03**: Catalog "Browse-by-Identification" tab extracted to `desktop/catalog_browse.py`. **DEFERRED → SEED-028** (method-based, densely cross-called; needs widget-ownership refactor first — Codex PLAN pre-flight, user decision 2026-06-26).
- [~] **DESK-04**: Search-results lifecycle extracted to `desktop/search_results_panel.py`. **DEFERRED → SEED-028** (`on_search_finished` touches 109 `self.*`).
- [~] **DESK-05**: Browse panel extracted to `desktop/browse_panel.py`. **DEFERRED → SEED-028**.
- [~] **DESK-06**: Reading desk extracted to `desktop/reading_desk_panel.py`. **DEFERRED → SEED-028**.
- [~] **DESK-07**: Lists tab + cloud-sync coordination extracted to `desktop/lists_tab.py`. **DEFERRED → SEED-028**.
- [x] **DESK-08**: Update-UI sub-cluster (notification / What's-New / progress dialogs + sidecar reset/download coordination) extracted to `desktop/update_ui.py`, with **new direct behavioral tests** for the sidecar reset/download coordination methods plus the existing sidecar tests. *(SEED-020 §7 C-6)*

### Search Results Space-Scroll (SCROLL) — Phase 128 (SEED-025)

- [x] **SCROLL-01** (web): On `/search`, Space page-scrolls the results container (Shift+Space scrolls up) when no actionable result control holds focus; when a result checkbox / expand / open-detail control (or an open dialog) holds focus, Space performs that action and is NOT stolen; no `preventDefault` on controls that legitimately consume Space (a11y intact). State of the actionable-suppression set is enumerated + tested.
- [x] **SCROLL-02** (desktop): In the results table, Space routes to page-down (Shift+Space page-up) of the results scroll area when no item is in a checkable/actionable focus state; otherwise Space toggles/activates the focused item as today.

### Library Filter (LIBFILTER) — Phase 129 (SEED-026)

- [x] **LIBFILTER-01** (web search): A library **multi-select** on `/search` results filters by `library_code` over the FULL result set BEFORE the `[:200]` render cap (empty = all); persists via the `web/safe_storage.py` chokepoint (Phase 87 invariant, CI allowlist `[]`); removable chips; i18n EN/HE labels (`LIBRARY_CODES`/`LIBRARY_CODES_HE`, no English leak under Hebrew).
- [x] **LIBFILTER-02** (web Browse-by-Identification): A `library_codes` arg pushed into `shared/fjms_service.get_browse_results` applies BEFORE `COUNT(DISTINCT AlmaId)` + `LIMIT/OFFSET` so `total`/pagination are correct over the full filtered set; additive/backward-compatible (None/empty = no-op); composes with the SEED-023 PGP/Editions filters; persists via `safe_storage`.
- [x] **LIBFILTER-03** (desktop parity): The desktop catalog Browse-by-Identification view gains the same library filter; existing desktop search-results library/shelfmark filtering is untouched.

### Release (REL) — both apps

- [ ] **REL-01**: v8.3.0 ships to **both apps** — web deploy + desktop installer + GitHub Release (installer asset) + bilingual What's New highlighting the two visible features (Space-scroll, library filter). The decomposition is invisible plumbing in the build. App version bumps 8.2.2 → 8.3.0 via `scripts/bump_version.py`.

## v2 Requirements (deferred — NOT in this roadmap)

### Future decomposition (DEFER)

- **DEFER-01**: `SearchEngine` internal sub-split — peel `LineBreakSearcher` (~238 ln) and `CompositionSearcher` (~509 ln) out of `shared/search_engine.py` (only after CORE-10 ships and its direct-module tests are green).
- **DEFER-02**: `CompositionState` dataclass refactor of `genizah_app.py`'s scattered `comp_*` fields — the **prerequisite** for any desktop composition-tab extraction. Own seed.
- **DEFER-03**: Desktop composition-tab extraction → `desktop/composition_tab.py` (blocked on DEFER-02).
- **DEFER-04**: Desktop startup/session remainder extraction (~50 `self.*` tab couplings) — structurally infeasible until DESK-04/05/06/07 expose tab state behind setters.
- **DEFER-05** (SEED-028, added 2026-06-26): Method-based desktop panel extraction — DESK-03 (catalog tab), DESK-04 (search-results), DESK-05 (browse), DESK-06 (reading-desk), DESK-07 (lists). Deferred from Phase 126 after the Codex PLAN pre-flight proved them too densely cross-called to move-and-shim safely; needs a widget-ownership/state refactor first (pairs with DEFER-02/03/04).

## Out of Scope

| Feature | Reason |
|---------|--------|
| User-facing behavior change *in the decomposition strand (122-127)* | The refactor itself is zero-behavior-change (GUARD-02). New behavior is confined to the deliberately-added feature phases 128-129. |
| Feature scope beyond SEED-025 + SEED-026 | Only these two SEEDs were chosen for 8.3.0; other dormant seeds (001/003/005/012/026-followups) stay deferred. |
| Desktop composition-tab + startup/session extraction | Needs `CompositionState` refactor first (DEFER-02/03/04) |
| `SearchEngine` internal sub-split | Deferred to DEFER-01 after the class moves intact (CORE-10) |
| LAB side-index punctuation / SEED-006 compat-gate **redesign** | Pre-existing tech-debt; the gates are **preserved** (CORE-10), not redesigned |
| API library-filter param (`/api/search`, `/api/browse`) | Natural follow-up to SEED-026; out of scope here (note for a later add) |

> **Note (re-scope 2026-06-27):** the original "no GitHub Release / internal-only" out-of-scope line is **REMOVED** — v8.3.0 now ships publicly to both apps (REL-01).

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
| CORE-10 | 125 | Complete |
| CORE-11 | 125 | Complete |
| CORE-12 | 125 | Complete |
| CORE-13 | 125 | Complete |
| DESK-01 | 126 | Complete |
| DESK-02 | 126 | Complete |
| DESK-03 | SEED-028 | Deferred |
| DESK-04 | SEED-028 | Deferred |
| DESK-05 | SEED-028 | Deferred |
| DESK-06 | SEED-028 | Deferred |
| DESK-07 | SEED-028 | Deferred |
| DESK-08 | 127 | Complete |
| SCROLL-01 | 128 | Not started |
| SCROLL-02 | 128 | Not started |
| LIBFILTER-01 | 129 | Not started |
| LIBFILTER-02 | 129 | Not started |
| LIBFILTER-03 | 129 | Not started |
| REL-01 | release | Not started |

**Coverage:**

- Decomposition requirements: 27 (4 GUARD + 1 CONFIG + 13 CORE + 1 PREP + 8 DESK) — Phases 122-127, complete (22 done + 5 DESK deferred → SEED-028).
- Feature requirements: 6 (2 SCROLL + 3 LIBFILTER + 1 REL) — Phases 128-129 + release, not started.
- Total v8.3.0 requirements: 33; mapped to phases/release: 33; unmapped: 0 ✓

---
*Requirements defined: 2026-06-25 (decomposition)*
*Last updated: 2026-06-27 — extended with SEED-025 + SEED-026 features for the public both-apps 8.3.0 release*
