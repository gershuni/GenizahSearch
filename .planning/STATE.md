---
gsd_state_version: 1.0
milestone: v8.3.0
milestone_name: God-File Decomposition + Search & Browse UX
status: Phase complete — ready for verification
stopped_at: Phase 129 context gathered
last_updated: "2026-06-28T20:00:41.008Z"
last_activity: 2026-06-28
progress:
  total_phases: 8
  completed_phases: 5
  total_plans: 22
  completed_plans: 18
  percent: 63
---

# Project State

## Project Reference

See: .planning/PROJECT.md

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Phase 129 — library-filter-search-browse-by-identification-seed-026

## Current Position

✅ **Phase 128 (Search Results Space-Scroll, SEED-025) — CODE-COMPLETE 2026-06-28.** Both plans executed (128-01 web
client-side keydown JS + two-file test scaffold; 128-02 desktop pure `space_scroll_action` helper + `eventFilter`
Key_Space branch). Gates all cleared: Codex PLAN pre-flight APPROVE (4-round), Codex CODE review APPROVE (2-round,
no findings), gsd-verifier 8/8 must-haves (`human_needed` only for the 6 live-browser/desktop manual smokes — the
render-smoke gap), full bulk suite 4901 passed/0 failed (GUARD-02). SCROLL-01 + SCROLL-02 satisfied. Remaining:
6 manual UAT smokes (do at release-time web/desktop smoke). NEXT: Phase 129 (SEED-026 library filter) — carries a
Codex-review-BEFORE-code gate; then `/release` 8.3.0 (both apps) → `/gsd-complete-milestone`.

Phase: 129 (library-filter-search-browse-by-identification-seed-026) — EXECUTING
Plan: 4 of 4
🚧 **RE-SCOPE 2026-06-27 (user decision):** v8.3.0 is NO LONGER internal-only. It now ships **publicly to
both apps** as 8.3.0 (8.2.2 → 8.3.0, no skipped number) by folding in two user-facing search/browse features
at **full parity**, so desktop earns the version bump with visible features (not just the invisible refactor):
**Phase 128 = SEED-025** (Space-key scroll of search results, web + desktop) and **Phase 129 = SEED-026**
(library filter on web search + Browse-by-Identification + desktop catalog parity; Codex-review-before-code
gate). Sequence: build 128 → build 129 → `/release` 8.3.0 (web deploy + desktop installer + GitHub Release +
What's New) → `/gsd-complete-milestone`. The decomposition's zero-behavior-change invariant is unchanged; the
milestone close was DEFERRED (not run) — the pre-close backlog triage already ran (122→18 open, commit
6b928d3f). Next action: `/gsd-discuss-phase 128`. The decomposition record below is unchanged execution history.

---

✅ **v8.3.0 DECOMPOSITION STRAND COMPLETE — 6/6 phases (122-127), 2026-06-26.** Pure internal refactor, zero behavior
change (rides along as invisible plumbing in the 8.3.0 build). genizah_core.py 12.5K→755 ln behind a permanent 27-name same-object facade;
the 4 update-UI classes + 9 D1 dialog/widget classes relocated to desktop/ (identity 13/13). Two back-edge
guards installed (GUARD-01 core + GUARD-04 desktop). Final suite: bulk 4894/0, gui 60/0 (3× consecutive green).

PHASE 127 (Update UI & Final Cleanup; FINAL) COMPLETE + verified 2026-06-26 (HEAD da6aaa1a-region):
  desktop/update_ui.py (4 classes, MOVE-and-shim, byte-identical to base bodies — Codex-confirmed cmp=0);
  D1 noqa markers retired (imports kept, 9 classes used internally), test_telemetry_consent_ux.py retargeted
  to desktop.settings_dialogs, EN privacy disclosure test hard-flipped; NEW tests/test_no_back_edges_desktop.py
  (GUARD-04, 19 modules) + tests/test_genizah_core_facade.py (27-name permanent facade). Sidecar coordination
  methods STAYED on GenizahGUI (research crux verdict — entangled across 4 ownership domains; DESK-08 tests
  target them in place). Codex PLAN pre-flight CLEARED (3-round: caught the APP_VERSION-not-in-core import drift

  + the BATCH_SIZE delete-range overshoot before any code was written) + Codex CODE review CLEARED (APPROVE-
  WITH-NITS → APPROVE; byte-faithfulness restore). gsd-verifier PASS 5/5 (127-VERIFICATION.md). One non-blocking
  human item: interactive desktop launch (update-UI render + sidecar download flow).

DEFERRED (logged): DESK-03..07 method-based panels → SEED-028; DEFER-01/02/03/04. The ≥70% genizah_app.py
  shrink was dropped (its bulk is the deferred method-based panels). docs/CODE_INDEX.md refresh → SEED-027.
  Phase-dir archival to .planning/milestones/v8.3.0-phases/ defers to the next milestone open (established cadence).

PHASE 126 (D1) COMPLETE 2026-06-26 (HEAD 7a692319). 9 classes MOVE-and-shimmed → desktop/settings_dialogs.py
  (5 dialogs) + desktop/ui_widgets.py (4 widgets); genizah_app.py −1860 net; D-07b strip verbatim; apply/
  cancel_settings API added; LabPanel+_CatalogRefreshWorker+D2-D5 methods correctly LEFT (deferred → SEED-028).
  Source-integrity gate ALL PASS (identity 9/9, GUARD-01, no BOM, name-diff = 6 unused PyQt drops only).
  Bulk 4853 passed/0 new fail; gui 60 passed. Codex CODE review substance-APPROVE + 2 LOW (unicode escapes,
  loose test assertion) → fixed (7a692319). gsd-verifier PASS 5/5 (126-VERIFICATION.md): grep ^class of all 9
  names in genizah_app.py = ZERO (true MOVE), deferral recorded in ROADMAP+REQUIREMENTS.

NEXT — Phase 127 (Update UI & Final Cleanup; FINAL, closes v8.3.0): extract desktop/update_ui.py
  (UpdateNotificationBar, WhatsNewBar, WhatsNewDialog, UpdateProgressDialog + sidecar reset/download
  coordination) with NEW direct behavioral tests (DESK-08); RETIRE the D1 shims (retarget genizah_app→desktop
  callers, remove shim lines); install tests/test_no_back_edges_desktop.py AST guard; confirm genizah_core
  facade; full-suite sign-off. NOTE (re-scope): the ≥70% genizah_app.py shrink is NO LONGER a target (bulk
  deferred to SEED-028). Run under the FULL DRILL (discuss-skip → research → pattern-map → plan(opus) →
  checker → Codex PLAN pre-flight → execute → source-integrity gate + Codex CODE review → verifier). Base = HEAD 7a692319.

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
Last activity: 2026-06-28

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

Last session: 2026-06-28T20:00:41.001Z
Stopped at: Phase 129 context gathered
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
| Phase 127-update-ui-final-cleanup P02 | 25 | 2 tasks | 2 files |
| Phase 127-update-ui-final-cleanup P03 | 30m | 2 tasks | 3 files |
| Phase 128 P01 | 20min | 2 tasks | 4 files |
| Phase 128 P02 | 8min | 1 tasks | 1 files |
| Phase 129 P02 | 9min | 2 tasks | 3 files |
| Phase 129 P03 | 15 | 2 tasks | 1 files |
| Phase 129 P04 | 16m | 2 tasks | 3 files |
| Phase 129 P05 | 8min | 2 tasks | 3 files |

## Decisions

- [Phase ?]: Engine-side helpers stay in genizah_core.py — depend on Tantivy engine context
- [Phase ?]: Inline _tr() helper for tr()-dependent modules — lazy CURRENT_LANG import satisfies GUARD-01
- [Phase ?]: LabSettings extracted to shared/lab_settings.py; same-object facade shim in genizah_core
- [Phase ?]: Phase 125-03
- [Phase ?]: SearchEngine extracted to shared/search_engine.py with 20-name facade; 7 lazy imports break cycles to genizah_core
- [Phase ?]: Phase 126 D1: MOVE-and-shim 5 dialogs to desktop/settings_dialogs.py + 4 widgets to desktop/ui_widgets.py; originals deleted, identity holds 9/9; D-07b strip verbatim; GenizahGUI.apply/cancel_settings added; LabPanel deferred to E2
