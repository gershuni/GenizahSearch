---
gsd_state_version: 1.0
milestone: v8.3.0
milestone_name: God-File Decomposition
status: Phase 126 planned (5 plans, D1-D5 sequential waves) — ready to execute
stopped_at: Phase 126 planned — 5 PLANs (126-01..05), one cluster per sequential wave; awaiting Codex PLAN pre-flight + execute
last_updated: "2026-06-26T15:00:00.000Z"
last_activity: 2026-06-26
progress:
  total_phases: 6
  completed_phases: 4
  total_plans: 7
  completed_plans: 7
  percent: 67
---

# Project State

## Project Reference

See: .planning/PROJECT.md

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Phase 126 — Desktop Panels (next; Phase 125 complete)

## Current Position

Phase: 126 (Desktop Panels) — PLANNED (5 plans, D1-D5, sequential waves 1-5). NEXT: Codex PLAN pre-flight, then execute.
Plan: 0 of 5 executed.

PHASE 126 PLAN (5 plans, ONE cluster per sequential wave — every cluster edits the genizah_app.py shim
  block, so they serialize; USE_WORKTREES=false; D3 before D4 for shared browse_text):
  W1 126-01 D1 settings_dialogs + ui_widgets (D-07b strip verbatim; GenizahGUI apply/cancel_settings; LabPanel DEFERRED to E2)
  W2 126-02 D2 catalog_browse (CatalogBrowsePanel + module-level _CatalogRefreshWorker; _CATALOG_FILTER_SETS kept-in-place)
  W3 126-03 D3 search_results_panel (hardest — 109 self.* in on_search_finished; session/history DELEGATED=E3-defer; NEW test_search_results_panel.py + conftest _GUI_TEST_FILES)
  W4 126-04 D4 browse_panel + reading_desk_panel (browse_thumb_resolved signal moved onto BrowsePanel; reading desk = sub-widget; 8 browse source-scan tests additively retargeted OR-location)
  W5 126-05 D5 lists_tab (ListsPanel + _ListsSyncCoordinator owning the _auto_sync_pending/_last debounce + authenticated supabase_client gate verbatim; Personal Lists tab only — Community populators stay)
  GUARD-03 uses RESEARCH's CORRECTED list (4 CONTEXT-listed tests are web-side false positives; 8 browse + 1 tabular genuinely retarget, additively). copy-not-move; never repo-wide ruff --fix; base-vs-HEAD dir(genizah_app) NAME diff per commit (NOT count). Base = aa215b37.

PHASE 125 (engines) — COMPLETE 2026-06-26. genizah_core.py 6064→755 ln (permanent 20-name
  same-object facade). 4 waves: 125-01 SEED-011 dedup (_ChunkPlan/_LabChunkPlan), 125-02 LabSettings,
  125-03 LabEngine, 125-04 SearchEngine → shared/. Each wave source-integrity-gated (facade identity,
  AST method-completeness, BOM, bulk name-level diff == 6-env baseline).
  **Codex CODE review (Gate ②) converged APPROVE in 3 rounds:** R1 = 2 BLOCKERs (SEED-011 pre-pass
  moved per-chunk prep OUTSIDE the corpus/index gates → scoped/no-index over-build) → fixed 658ada3c
  +2 regression tests; R2 = B1/B2 confirmed fixed, found 2 HIGH (the r1 fix split build-vs-consume
  gates → TOCTOU that could drop LOCAL/LAB hits on a mid-search rebuild) + 1 LOW (trailing ws) → fixed
  eb802521 (build⟺consume share one snapshot; git diff --check clean); R3 = APPROVE, zero findings.
  **gsd-verifier PASS 5/5 SC, 8/8 reqs** (125-VERIFICATION.md) — facade identity 20/20, SearchEngine
  methods 52=52, GUARD-01 clean, CORE-10 hazards intact, GUARD-02 bulk == 6-env baseline (4853 passed),
  gui+render_smoke 60 passed. HEAD = eb802521 (+ a closeout commit for 2 cosmetic test-text nits).
  124 recap: post-exec review caught 3 defects the executor misreported as "pre-existing" (count-based)
  — _parse_cudl_label facade drop (fc3ce883), path-string-registry GUARD-03 miss (e4abf248), tantivy
  import-order (741f7b24). Codex R1→R2 APPROVE; verifier PASS 7/7.

  NEXT — Phase 126 (Desktop Panels): split genizah_app.py UI panels (settings_dialogs, ui_widgets,
  catalog_browse, search_results_panel, browse_panel, reading_desk_panel, lists_tab) into desktop/.
  Base = eb802521. Run under the SAME FULL DRILL below. (gui-test split matters here — these ARE the
  GUI panels; add new dialog tests to conftest _GUI_TEST_FILES; gui+render_smoke slice is load-bearing.)

  FULL DRILL for phases 126-127 (TWO Codex touchpoints — pre-flight + post-exec):
  discuss(skip-if-no-gray-areas) → research → pattern-map → plan(opus) → gsd-plan-checker loop
  → **Codex PLAN PRE-FLIGHT (review plan/research vs live codebase for plan↔code drift; must
  clear before execute)** → execute → Codex CODE review 3-round convergence + base-vs-HEAD
  facade-name diff + base-vs-HEAD name-level test comparison → gsd-verifier → auto-advance.
  Standing directive: run 125-127 autonomously, skip-discuss-when-unneeded, BOTH Codex gates
  must clear before proceeding. (User reminder 2026-06-26: do NOT skip the Codex plan
  pre-flight — see [[feedback_codex_preflight_before_plan_complete]].)
  LESSON from 124: the executor's "0 new failures" was count-based, not name-based — ALWAYS
  do the base-vs-HEAD NAME-level test comparison + facade-name diff yourself; don't trust the
  executor's failure count.
Last activity: 2026-06-26

Progress: [███████░░░] 67% (4 of 6 phases — 122, 123, 124, 125 complete)

## Accumulated Context

### Key Decisions (v8.3.0)

- **Phase 122 first (GUARD-01 + CONFIG-01):** Config cycle pivot must precede ALL other core moves (C-1 from SEED-020 §7 — VariantManager, CodicologicalManager, responsa explosion guard, JoinsManager, and ListsManager all reference Config at class-definition time).
- **Three modules from responsa cluster (C-2):** `shared/variants.py` + `shared/codicological.py` + `shared/responsa.py` — not a single `shared/responsa.py`; CodicologicalManager.load() takes csv_bank from MetadataManager.
- **SEED-011 before engine move (Phase 125a):** Composition double-prep dedup must land before SearchEngine/LabEngine code is relocated to avoid reworking moved code.
- **`genizah_core.py` = permanent compat facade:** Never delete the re-export shims; `genizah_app.py` shims DO get deleted in Phase 127 final cleanup.
- **Never repo-wide `ruff --fix`:** Strips `# noqa: F401` shims; per-file ruff review only on every extraction commit.
- **GUARD-03 named files:** 5 source-scanning tests must be retargeted before deletion: `test_desktop_folio_navigation.py`, `test_wr01_open_local_browse_page_ast.py`, `test_tabular_builder_rtl.py`, `test_view_all_cap.py`, `test_shelfmark_bridge.py`.
- **`_my_library_tab_ref` = injected optional interface (C-4):** Spans both SearchEngine AND LabEngine; never import desktop into shared.

### Blockers/Concerns

None at roadmap creation.

### Pending Todos

None yet. Begin with `/gsd-discuss-phase 122`.

## Deferred Items

Items carried forward from v8.2.0 and earlier:

| Category | Item | Status | Deferred At |
|----------|------|--------|-------------|
| CONSENT-F1 | "Reset telemetry id" affordance in Settings | Future | v8.1.0 |
| ERR-01 | Handled/non-fatal error counting at high-value sites | Future | v8.1.0 |
| CRASH-F1 | "Send logs" flow for local faulthandler log | Future | v8.1.0 |
| WEB-F1 | Clean web `search_executed` query-text property | Future | v8.1.0 |
| FLAG-F1 | PostHog feature flags / remote config on desktop | Future | v8.1.0 |
| PST-F1 | Cloud cross-device sync of Joins Lab candidate lists / triage | Future | v8.2.0 |
| D-F12 | Regular Search ~8s wall-clock (profile-first) | Future | v8.1.0 |
| D-F18 | Context-menu LOCAL detection via `display` | Future | v8.0.0 |
| JSA-01/02/03 | Anchor parallels, corpus completion, torn-word (Component B) | Future | v8.0.0 |
| JWB-05 | Tear-side assist (Component B) | Future | v8.0.0 |
| DEFER-01 | SearchEngine internal sub-split (LineBreakSearcher/CompositionSearcher) | After CORE-10 ships | v8.3.0 |
| DEFER-02 | CompositionState dataclass refactor | Own seed | v8.3.0 |
| DEFER-03 | Desktop composition-tab extraction | Blocked on DEFER-02 | v8.3.0 |
| DEFER-04 | Desktop startup/session remainder extraction | Blocked on DESK-04/05/06/07 | v8.3.0 |

## Session Continuity

Last session: 2026-06-26T13:00:00.000Z
Stopped at: Phase 125 verified complete (Codex APPROVE 3-round; verifier PASS 5/5)
Resume file: None
Next step: `/gsd-discuss-phase 126` (or skip-discuss-if-empty per the standing autonomous directive)

## Performance Metrics

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| (none yet — milestone just started) | - | - | - |
| 122 | 1 | - | - |
| Phase 123 P01 | 63 | 7 tasks | 13 files |
| Phase 124-core-metadata-index P01 | 90 | 2 tasks | 8 files |
| Phase 125-core-engines P01 | 45m | 3 tasks | 4 files |
| Phase 125 P02 | 33m | 1 tasks | 3 files |
| Phase 125-core-engines P03 | 120 | 1 tasks | 5 files |
| Phase 125 P04 | 90m | 1 tasks | 14 files |

## Decisions

- [Phase ?]: Engine-side helpers stay in genizah_core.py — depend on Tantivy engine context
- [Phase ?]: Inline _tr() helper for tr()-dependent modules — lazy CURRENT_LANG import satisfies GUARD-01
- [Phase ?]: LabSettings extracted to shared/lab_settings.py; same-object facade shim in genizah_core
- [Phase ?]: Phase 125-03
- [Phase ?]: SearchEngine extracted to shared/search_engine.py with 20-name facade; 7 lazy imports break cycles to genizah_core
