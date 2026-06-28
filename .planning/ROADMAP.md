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
- 🚧 **v8.3.0 God-File Decomposition + Search & Browse UX** -- Phases 122-129 (decomposition done 2026-06-26; + SEED-025 Space-scroll & SEED-026 Library filter → public both-apps 8.3.0 release — in progress)

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

### 🚧 v8.3.0 God-File Decomposition + Search & Browse UX (Phases 122-129) — In Progress

**Milestone goal:** Two strands shipped together as a **public both-apps 8.3.0 release**. (1) **Decomposition (Phases 122-127, DONE):** split the two god-files — `genizah_app.py` (~28k lines, desktop) and `genizah_core.py` (~12.5k lines, shared by both apps) — into cohesive `shared/`+`desktop/` modules behind permanent re-export facades, **zero behavior change** (rides along as invisible plumbing). (2) **Search & Browse UX (Phases 128-129, NEW):** two user-facing additions that give the version real substance on BOTH apps — **SEED-025** Space-key page-scroll of search results, and **SEED-026** a library filter on web search + Browse-by-Identification (+ desktop parity). Decomposition strategy in `.planning/seeds/SEED-020-decomposition-map.md` (§7 authoritative); feature specs in `.planning/seeds/SEED-025-*.md` + `SEED-026-*.md`.

**Re-scope note (2026-06-27):** v8.3.0 was originally scoped internal-only / no-release. User decision: ship it publicly as 8.3.0 (8.2.2 → 8.3.0, no skipped number) by folding in SEED-025 + SEED-026 at **full both-apps parity** — so desktop earns the version bump with visible features, not just the refactor. The decomposition's zero-behavior-change invariant (GUARD-02) is unchanged; the new feature phases are additive.

**Hard constraints across all phases (GUARD invariants):**

- GUARD-01: No module-level import back-edges — no cycle through any extracted `shared/` module. AST/import guard installed in Phase 122 and enforced at every phase boundary.
- GUARD-02: Zero behavior change — full existing pytest suite (search / browse / responsa / joins / lists / composition parity, web + desktop import paths) passes at every phase boundary.
- GUARD-03: Every source-scanning / AST test that reads `genizah_core.py` or `genizah_app.py` is retargeted to the new module location before the original implementation is deleted (5 named files: `test_desktop_folio_navigation.py`, `test_wr01_open_local_browse_page_ast.py`, `test_tabular_builder_rtl.py`, `test_view_all_cap.py`, `test_shelfmark_bridge.py`). Retarget during the additive phase; flip at deletion.
- GUARD-04: `genizah_core.py` remains a permanent compatibility facade (re-export shims preserved); `genizah_app.py` implementation shims are removed in a clean final deletion pass.
- Per-commit discipline: copy-not-move → retarget importers → add `# noqa: F401` re-export shim → (next phase) delete original + add AST guard, one atomic commit per cluster; per-file ruff review only (never repo-wide `ruff --fix`).

## Summary Checklist

- [x] **Phase 122: Config Enabler** - Extract `Config` to `shared/config.py`; install back-edge AST guard (GUARD-01). Breaks the import-cycle pivot that blocks all subsequent core moves.
 (completed 2026-06-25)

- [x] **Phase 123: Core Leaf Modules** - Extract seven low-risk, well-tested clusters: `shared/variants.py`, `shared/codicological.py`, `shared/responsa.py`, `shared/joins_manager.py`, `shared/lists_manager.py`, `shared/browse_map_utils.py`, `shared/text_normalize.py`. Proves the shim+guard pipeline on core before touching engines.
 (completed 2026-06-25)

- [x] **Phase 124: Core Metadata & Index** - Extract `shared/metadata_manager.py` (+ `_BoundedLRUCache`) and `shared/indexer.py`; retarget the `shared/local_indexer.py` lazy back-edges into `genizah_core` helpers.
 (completed 2026-06-26)

- [x] **Phase 125: Core Engines** - SEED-011 composition dedup first (125a), then extract `shared/search_engine.py` (DI + BrowseMap cache + SEED-006 gates + `_LAST_RESPONSA_DOWNGRADE` preserved), `shared/lab_settings.py`, `shared/lab_engine.py` (LOCAL-LAB mirror preserved), and model `_my_library_tab_ref` as an injected optional interface for both engines. (completed 2026-06-26)
- [x] **Phase 126: Desktop Panels (RE-SCOPED 2026-06-26 → D1 only)** - Extract the clean top-level CLASS clusters to `desktop/`: `settings_dialogs.py` (dialogs) + `ui_widgets.py` (table/header/scroll widgets). The four METHOD-based panels (catalog tab, search-results, browse/reading-desk, lists) are DEFERRED to **SEED-028** — the Codex PLAN pre-flight proved them too entangled (dense `self.method()`/`self.widget` cross-refs; D3 `on_search_finished` alone touches 109 `self.*`) for a safe zero-behavior-change move-and-shim without a prerequisite widget-ownership refactor (like E2's CompositionState). User decision 2026-06-26.
 (completed 2026-06-26)

- [x] **Phase 127: Update UI & Final Cleanup** - Extract `desktop/update_ui.py` + new direct behavioral tests for sidecar reset/download coordination; remove the Phase-126 (D1) desktop shims from `genizah_app.py`; install the `desktop/` back-edge guard; confirm `genizah_core.py` permanent facade; full-suite-green sign-off. (genizah_app.py shrinks only modestly this milestone — the bulk awaits SEED-028.) (completed 2026-06-26)

- [x] **Phase 128: Search Results Space-Scroll (SEED-025)** - Space page-scrolls the search-results area when no result control holds an actionable focus (checkbox / expand / open detail); Shift+Space scrolls up; never steals the keystroke from a focused control. Web (NiceGUI keydown handler + focus guard on the results scroll container) + desktop (PyQt6 results table page-down/up routing). Small, self-contained. (completed 2026-06-27)

- [ ] **Phase 129: Library Filter — Search + Browse-by-Identification (SEED-026)** - Library multi-select filter on web `/search` results (applied over the FULL pre-`[:200]` set, persisted via `safe_storage`, i18n EN/HE) and a `library_codes` filter pushed DOWN into `shared/fjms_service.get_browse_results` for Browse-by-Identification (correct `total`/pagination, composes with the SEED-023 PGP/Editions filters). Desktop parity: catalog Browse-by-Identification library filter (desktop search-results already filters by library/shelfmark). Reuses the SEED-023 push-down template; **Codex-review-before-code gate** per the seed.

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

**Plans**: 1 plan (2 sequential commits — metadata_manager first, then indexer; both touch genizah_core.py)

Plans:

- [x] 124-01-PLAN.md — Extract MetadataManager (+ _BoundedLRUCache + 8-item pre-cluster) to shared/metadata_manager.py and Indexer to shared/indexer.py behind permanent same-object re-export shims; inline _tr()/_strip_brackets for indexer (GUARD-01); GUARD-03 enrich_metadata fixture retarget; GUARD-01 registry 8->10; identity/smoke tests

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

**Plans**: 4 plans (4 sequential waves — all touch genizah_core.py, so they serialize; ordering is load-bearing: SEED-011 dedup → LabSettings → LabEngine → SearchEngine)

Plans:

- [x] 125-01-PLAN.md — SEED-011 composition double-prep dedup (PREP-01): _ChunkPlan (two-query: genizah + diacritic-folded LOCAL) + _LabChunkPlan (shared fingerprint, index-local source boost); behavior-preserving; new invocation-count guard test; GUARD-01 registry pre-grown 10→13 (skip-until-exists)
- [x] 125-02-PLAN.md — Extract LabSettings → shared/lab_settings.py (CORE-11; stdlib-only, no tantivy) behind a permanent same-object re-export shim; identity tests
- [x] 125-03-PLAN.md — Extract LabEngine → shared/lab_engine.py (CORE-12 + CORE-13 LabEngine side); preserve CR-01/CR-02 LOCAL-LAB mirror + _my_library_tab_ref getattr gate (no shared→desktop import); lazy text_to_fingerprint/_LabChunkPlan imports; GUARD-03 LabEngine source-scan retargets
- [x] 125-04-PLAN.md — Extract SearchEngine + pre-cluster → shared/search_engine.py (CORE-10 + CORE-13 SearchEngine side); preserve 3 hazards (BrowseMap class-cache, SEED-006 content_search gates, _LAST_RESPONSA_DOWNGRADE thread-local); full 20-name facade shim; duck-typed is_searchable gate; GUARD-03 SearchEngine retargets; GUARD-01 registry final at 13

### Phase 126: Desktop Panels

**Goal** (RE-SCOPED 2026-06-26 → D1 only): Extract the two clean top-level CLASS clusters from `genizah_app.py` to `desktop/` modules: `desktop/settings_dialogs.py` (Settings/SearchSettings/LabScoring/Tabular-builder dialogs, D-07b telemetry snapshot stripping preserved) and `desktop/ui_widgets.py` (table/header/scroll widget classes). MOVE-and-shim recipe (mirror genizah_core 122–125): delete the original class from `genizah_app.py`, replace with a `# noqa: F401` re-export shim so `genizah_app.X is desktop.Y.X` holds. `LabPanel` deferred to E2. The four METHOD-based panels (D2 catalog tab, D3 search-results, D4 browse/reading-desk, D5 lists) are DEFERRED to **SEED-028** — the Codex PLAN pre-flight (2 rounds) proved they are densely cross-called methods on `GenizahGUI` (e.g. `on_search_finished` touches 109 `self.*`; `_catalog_*`/browse/lists methods called from many sites that stay in `GenizahGUI`), unsafe to move-and-shim under a zero-behavior-change mandate without a prerequisite widget-ownership/state refactor (like E2's CompositionState). User decision 2026-06-26.
**Depends on**: Phase 125
**Requirements**: DESK-01, DESK-02, GUARD-02, GUARD-03, GUARD-04 (DESK-03..07 → deferred to SEED-028)
**Success Criteria** (what must be TRUE):

  1. `desktop/settings_dialogs.py` and `desktop/ui_widgets.py` exist and import cleanly in isolation (no module-level `import genizah_app`); `genizah_app.py` re-exports the moved classes so all current `from genizah_app import ...` / `genizah_app.X` call sites continue to work unchanged, with `genizah_app.X is desktop.Y.X` identity (move-and-shim — original deleted, shim not shadowed).
  2. The existing D1 test suites pass via the shims: `tests/test_telemetry_consent_ux.py` (D-07b consent snapshot strip identical after the move), `tests/test_tabular_builder_rtl.py` (additive GUARD-03 retarget).
  3. The full existing pytest suite (bulk + gui slice) passes — 6-env `test_search_api_v2::…real_index[*]` baseline is the accepted GREEN baseline; per-file ruff review on each extraction commit shows no unintended shim stripping; base-vs-HEAD `dir(genizah_app)` NAME diff (not failure count) confirms no dropped names.
  4. Headless PyQt6 construction/import smoke green; the dialogs/widgets work identically after the move.

**Plans**: 1 plan (126-01, Wave 1, D1). The D2–D5 method-based plans were drafted (Codex-r1-corrected) and preserved at `deferred-method-panels/126-02..05-PLAN.md` as the starting point for SEED-028.

Plans:

- [x] 126-01-PLAN.md (Wave 1, D1) — Extract Settings/SearchSettings/LabScoring/Tabular dialogs -> desktop/settings_dialogs.py + table/header/scroll widgets -> desktop/ui_widgets.py (MOVE-and-shim, identity); D-07b telemetry snapshot strip verbatim; GenizahGUI apply/cancel_settings API; LabPanel DEFERRED to E2
- [DEFERRED → SEED-028] D2 catalog tab, D3 search-results, D4 browse/reading-desk, D5 lists — method-based; need a widget-ownership refactor first. Draft plans preserved in `deferred-method-panels/`.

### Phase 127: Update UI & Final Cleanup

**Goal**: The last extractable desktop cluster — `desktop/update_ui.py` (notification bar, What's-New bar/dialog, update progress dialog, sidecar reset/download coordination) — lands with new direct behavioral tests for the sidecar reset/download coordination methods. Then: all implementation shims are removed from `genizah_app.py` (it keeps only thin `import ... as ...` re-exports for the deleted clusters); `genizah_core.py` permanent facade is confirmed intact; the GUARD-01 back-edge test and a new `tests/test_no_back_edges_desktop.py` guard are both green; the full pytest suite passes as the final sign-off.
**Depends on**: Phase 126
**Requirements**: DESK-08, GUARD-02, GUARD-03, GUARD-04
**Success Criteria** (what must be TRUE):

  1. `desktop/update_ui.py` exists and imports cleanly; `UpdateNotificationBar`, `WhatsNewBar`, `WhatsNewDialog`, and `UpdateProgressDialog` are importable from it; new direct behavioral tests covering the GUI sidecar reset/download coordination methods pass (SEED-020 §7 C-6 requirement).
  2. The Phase-126 (D1) desktop re-export shims are retired in one clean commit — callers retargeted from `genizah_app` to `desktop.settings_dialogs`/`desktop.ui_widgets`, the shim lines removed, plus the `update_ui` shim from this phase. (NOTE re-scope 2026-06-26: the original ≥70% `genizah_app.py` shrink is NO LONGER a v8.3.0 target — the bulk of the file is the four method-based panels deferred to SEED-028; this milestone delivers the D1 classes + update_ui only.)
  3. `genizah_core.py` permanent re-export facade is confirmed: `from genizah_core import Config`, `from genizah_core import SearchEngine`, and all other extracted names continue to resolve; `tests/test_genizah_core_facade.py` (new or updated) asserts the facade exports the same objects as the `shared/` modules.
  4. Both back-edge guards are green: `tests/test_no_back_edges_core.py` (GUARD-01, installed Phase 122) confirms no `shared/` module imports `genizah_core` at module level; `tests/test_no_back_edges_desktop.py` (new, this phase) confirms no `desktop/` module imports `genizah_app` at module level.
  5. The full existing pytest suite (all categories: search, browse, responsa, joins, lists, composition parity, web + desktop import paths) is green — the milestone's final zero-behavior-change sign-off.

**Plans**: 3 plans (3 sequential waves — each post-Wave-0 wave edits the genizah_app.py shim block, so they serialize)

- [x] 127-01-PLAN.md (Wave 1) — Wave-0 scaffolds: NEW `tests/test_no_back_edges_desktop.py` (GUARD-04 AST guard, 19 desktop modules incl. pre-registered `update_ui.py`), NEW `tests/test_genizah_core_facade.py` (SC#3 permanent-facade identity, 20 names), NEW `tests/test_update_ui_coordination.py` (DESK-08 behavioral tests for the sidecar coordination methods IN PLACE on GenizahGUI)
- [x] 127-02-PLAN.md (Wave 2) — Extract `desktop/update_ui.py`: MOVE-and-shim the 4 update-UI classes (`UpdateNotificationBar`/`WhatsNewBar`/`WhatsNewDialog`/`UpdateProgressDialog`), delete originals from `genizah_app.py`, add a no-noqa re-export shim (classes are used); back-edge guard now enforces `update_ui.py`. Coordination methods stay on GenizahGUI (research crux verdict)
- [x] 127-03-PLAN.md (Wave 3) — Final cleanup + sign-off: retire the Phase-126 D1 noqa suffix (genizah_app.py:77-78, imports kept), retarget `test_telemetry_consent_ux.py` to `desktop.settings_dialogs`, hard-flip the EN disclosure test in `test_privacy_disclosure_strings.py`, confirm the PERMANENT genizah_core facade, full-suite (bulk 6-env baseline + gui green) milestone sign-off

### Phase 128: Search Results Space-Scroll (SEED-025)

**Goal**: Pressing **Space** page-scrolls the search-results area (Shift+Space scrolls up) **only when no result control holds an actionable focus** — a focused checkbox, expand/collapse control, open-detail trigger, or open dialog keeps Space doing that action. When focus is on a non-actionable element, Space falls through to scroll the results container by ~one viewport instead of being swallowed or no-op. Web + desktop parity.
**Depends on**: Phase 127 (decomposition complete; desktop results panel is post-126/127)
**Requirements**: SCROLL-01, SCROLL-02, GUARD-02
**Success Criteria** (what must be TRUE):

  1. Web: with focus on a non-actionable element, Space scrolls the `/search` results container by one viewport and Shift+Space scrolls up; with focus on a result checkbox / expand control / open detail, Space performs that control's action (keystroke NOT stolen); never `preventDefault` on a control that legitimately wants Space (a11y intact).
  2. Desktop: in the results table, when no item is in a checkable/actionable focus state, Space routes to page-down (Shift+Space page-up) of the results scroll area; otherwise Space toggles/activates the focused item as today.
  3. The actionable-focus suppression set is explicitly enumerated (per SEED-025 open-question #2) and tested; everything outside it scrolls.
  4. Behavioral tests cover both the scroll path and the don't-steal path on each app; the full existing suite stays green.

**Plans**: 2 plans

Plans:

- [x] 128-01-PLAN.md (Wave 1) — Web Space-scroll: client-side keydown handler injected via ui.run_javascript scrolls .results-scroll-area / .q-scrollarea__container on Space (Shift+Space up), suppressed for INPUT/BUTTON/TEXTAREA/SELECT/role=button/contentEditable/open .q-dialog; double-install guard; + full tests/test_space_scroll.py scaffold + conftest gui registration (SCROLL-01, GUARD-02)
- [x] 128-02-PLAN.md (Wave 2, depends 128-01) — Desktop Space-scroll: QAbstractSlider import + Key_Space branch in GenizahGUI.eventFilter routing non-checkbox-column Space to verticalScrollBar().triggerAction(SliderPageStepAdd/Sub), checkbox-column Space falls through to Qt toggle; turns the 3 desktop gui tests green (SCROLL-02, GUARD-02)

### Phase 129: Library Filter — Search + Browse-by-Identification (SEED-026)

**Goal**: Add a **library filter** keyed on `library_code` (canonical list `LIBRARY_CODES`; Hebrew labels `LIBRARY_CODES_HE`) to: (1) web `/search` results as a **multi-select** applied over the FULL result set BEFORE the `[:200]` render cap (empty = all), persisted via `safe_storage`, removable chips, i18n EN/HE; (2) **Browse-by-Identification** (catalog) as a `library_codes` arg pushed DOWN into `shared/fjms_service.get_browse_results` BEFORE the `COUNT(DISTINCT AlmaId)` + `LIMIT/OFFSET` so `total`/pagination stay correct and it composes with the SEED-023 PGP/Editions filters; (3) desktop parity — a library filter on the desktop catalog Browse-by-Identification view (desktop search-results already filters by library/shelfmark, so that side is parity-only).
**Depends on**: Phase 127; reuses the SEED-023 push-down + chip + `safe_storage` template
**Requirements**: LIBFILTER-01, LIBFILTER-02, LIBFILTER-03, GUARD-02
**Success Criteria** (what must be TRUE):

  1. Web search: selecting one/several libraries narrows results to those `library_code`s over the FULL pre-`[:200]` set (not just the visible 200); empty = all; selection persists via `safe_storage` (Phase 87 invariant, CI allowlist `[]`); no English leak under Hebrew if names are shown.
  2. Browse-by-Identification: the new `library_codes` arg to `get_browse_results` changes `total` correctly over the full filtered set, paginates correctly, is additive/backward-compatible (None/empty = no-op), and composes with the SEED-023 PGP/Editions filters.
  3. Desktop catalog Browse-by-Identification gains the same library filter at parity; existing desktop search-results library/shelfmark filtering is untouched.
  4. **Codex-review-before-code gate** satisfied (project seed-review gate per SEED-026 + [[feedback_audit_to_cloud_pipeline]]); the design crux (how catalog rows map to `library_code` via `AlmaId==sys_id`) is resolved before implementation.
  5. Tests green (search full-set narrowing + persistence; catalog total/pagination/compose; `get_browse_results` additive arg); ruff clean; existing PGP/printed + SEED-023 filters unbroken.

**Plans**: 4 plans (2 waves) — Codex design-crux review CLEARED (APPROVE WITH CHANGES; see 129-CODEX-CRUX-REVIEW.md) before planning, satisfying Success Criterion #4. Wave 1 = shared push-down core (01) + web search filter (02), disjoint files, parallel. Wave 2 = web catalog UI (03) + desktop parity (04), both depend on 01's get_browse_results signature, disjoint files, parallel.

Plans:

**Wave 1**

- [x] 129-01-PLAN.md (Wave 1) — Shared push-down core: extend `shared/fjms_service.get_browse_results` with additive `library_codes`/`library_sys_ids` + `_browse_filter_library` TEMP table (content-derived token, Codex Change 1) + shared `resolve_library_sys_ids` helper + selected-but-empty fail-open (Codex Change 2); LIBFILTER-02 service tests (LIBFILTER-02, GUARD-02)
- [ ] 129-02-PLAN.md (Wave 1) — Web `/search` library multi-select: `_apply_library_filter` over the FULL pre-`[:200]` set + facet counts (hide 0-match) + removable chips + `search_library_filter` via safe_storage; EN/HE labels (LIBFILTER-01, GUARD-02)

**Wave 2** *(blocked on Wave 1 completion)*

- [ ] 129-03-PLAN.md (Wave 2, depends 129-01) — Web Browse-by-Identification: `catalog_library_filter` state + dropdown checklist + per-code chips; resolve sys_ids off the event loop in `_fetch_results_blocking` and push `library_codes`/`library_sys_ids` into get_browse_results (composes with SEED-023 PGP/Editions) (LIBFILTER-02, GUARD-02)
- [ ] 129-04-PLAN.md (Wave 2, depends 129-01) — Desktop catalog parity: `_catalog_library_filter` state + checkable widget beside SEED-023 buttons + worker `library_filter` param resolved on the QThread + chips; gui-marked `test_libfilter_desktop.py`; OQ-1 reachability documented (LIBFILTER-03, GUARD-02)

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
| 124. Core Metadata & Index | 1/1 | Complete   | 2026-06-26 |
| 125. Core Engines | 4/4 | Complete   | 2026-06-26 |
| 126. Desktop Panels | 1/1 | Complete   | 2026-06-26 |
| 127. Update UI & Final Cleanup | 3/3 | Complete   | 2026-06-26 |
| 128. Search Results Space-Scroll (SEED-025) | 2/2 | Complete   | 2026-06-27 |
| 129. Library Filter (SEED-026) | 1/4 | In Progress|  |
