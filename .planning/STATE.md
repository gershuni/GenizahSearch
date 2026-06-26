---
gsd_state_version: 1.0
milestone: v8.3.0
milestone_name: God-File Decomposition
status: Phase 126 (D1) EXECUTED — 126-01 complete; D2-D5 deferred to SEED-028; awaiting Codex CODE review + gsd-verifier
stopped_at: Completed 126-01-PLAN.md (D1 dialogs+widgets MOVE-and-shim; 9/9 identity, D-07b verbatim, GUARD-02/03/04 green)
last_updated: "2026-06-26T12:31:26.413Z"
last_activity: 2026-06-26
progress:
  total_phases: 6
  completed_phases: 5
  total_plans: 8
  completed_plans: 8
  percent: 83
---

# Project State

## Project Reference

See: .planning/PROJECT.md

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Phase 126 — Desktop Panels (next; Phase 125 complete)

## Current Position

Phase: 126 (Desktop Panels) — RE-SCOPED 2026-06-26 to **D1 ONLY**. NEXT: execute 126-01 (already Codex-pre-flight-clean).
Plan: 1 of 1 executed (only 126-01 remains in the phase; D2-D5 moved to deferred-method-panels/ → SEED-028).

RE-SCOPE RATIONALE (user decision 2026-06-26): the Codex PLAN pre-flight ran TWICE. R1 caught a BLOCKER
  (recipe must be MOVE-and-shim, not copy-keep-both — a kept original shadows the shim) + D3/D4 method
  reassignment + GUARD-03 list fixes → all fixed (commit 4793ab05). R2 then exposed the FUNDAMENTAL issue:
  the D2 catalog-tab + D3 search-results + D4 browse + D5 lists "panels" are densely cross-called
  GenizahGUI METHODS — moving them breaks the many self.<method>() callers that stay in GenizahGUI
  (export_results, filter dialogs, startup, session-restore, login/logout, _on_tab_changed,
  open_result_in_browse, etc.). on_search_finished alone touches 109 self.*. This is the same dense-
  coupling that got E2/E3 deferred. User chose: DEFER the entangled panels, keep only the clean class
  extraction. So Phase 126 = **D1** (top-level dialog + widget CLASSES → desktop/settings_dialogs.py +
  desktop/ui_widgets.py; pure move-and-shim, identity holds, NO method deletion / cross-caller breakage).
  D2-D5 → **SEED-028** (needs a widget-ownership/state refactor first, like E2's CompositionState); their
  Codex-r1-corrected draft plans preserved at .planning/phases/126-desktop-panels/deferred-method-panels/.
  126-01 had ZERO Codex r2 findings → pre-flight-clean. Base = aa215b37.
  EXECUTE: 126-01 only. MOVE-and-shim; never repo-wide ruff --fix; base-vs-HEAD dir(genizah_app) NAME diff
  (NOT count); GUARD-03 additive retarget of test_tabular_builder_rtl.py; bulk + gui slices; 6-env baseline.

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
| DEFER-05 | Method-based desktop panel extraction (DESK-03..07: catalog tab, search-results, browse, reading-desk, lists) → SEED-028 | Needs widget-ownership refactor first; draft plans in 126/deferred-method-panels/ | v8.3.0 (Phase 126 re-scope, 2026-06-26) |

## Session Continuity

Last session: 2026-06-26T12:29:54.282Z
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
| Phase 126-desktop-panels P01 | 55m | 3 tasks | 5 files |

## Decisions

- [Phase ?]: Engine-side helpers stay in genizah_core.py — depend on Tantivy engine context
- [Phase ?]: Inline _tr() helper for tr()-dependent modules — lazy CURRENT_LANG import satisfies GUARD-01
- [Phase ?]: LabSettings extracted to shared/lab_settings.py; same-object facade shim in genizah_core
- [Phase ?]: Phase 125-03
- [Phase ?]: SearchEngine extracted to shared/search_engine.py with 20-name facade; 7 lazy imports break cycles to genizah_core
- [Phase ?]: Phase 126 D1: MOVE-and-shim 5 dialogs to desktop/settings_dialogs.py + 4 widgets to desktop/ui_widgets.py; originals deleted, identity holds 9/9; D-07b strip verbatim; GenizahGUI.apply/cancel_settings added; LabPanel deferred to E2
