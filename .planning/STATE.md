---
gsd_state_version: 1.0
milestone: v8.3.0
milestone_name: God-File Decomposition
status: Executing Phase 125
stopped_at: Completed 123-01-PLAN.md
last_updated: "2026-06-26T04:31:02.765Z"
last_activity: 2026-06-26 -- Phase 125 execution started
progress:
  total_phases: 6
  completed_phases: 3
  total_plans: 7
  completed_plans: 3
  percent: 43
---

# Project State

## Project Reference

See: .planning/PROJECT.md

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Phase 125 — Core Engines

## Current Position

Phase: 125 (Core Engines) — EXECUTING
Plan: 1 of 4
124 recap: post-exec review caught 3 defects the executor misreported as "pre-existing"
  (count-based check) — _parse_cudl_label facade drop (fc3ce883), path-string-registry
  GUARD-03 miss (e4abf248), tantivy import-order GUARD-02 change (741f7b24, Codex r1 HIGH).
  Codex CODE review converged R1→R2 APPROVE. GUARD-02 confirmed zero new failures via
  base-vs-HEAD NAME-level diff (8 pre-existing reds confirmed at base; gui+render_smoke green).

PHASE 125 (engines) — PLANS CLEAR, ENTERING EXECUTION. Discuss SKIPPED. Research+pattern-map+plan
  DONE. 4 plans (e95b5053; revised a2dd3662 + a 125-04 nit commit). Internal checker PASS (12/12).
  **Pre-125 Codex regression audit** (user "Ask Codex"): found+fixed a 2nd BOM (shared/responsa.py);
  122-124 verified faithful (facade identity 12/12, AST method-completeness 7/7); adopted a
  source-integrity gate (125-PREAUDIT-CODEX.md). **Codex PLAN pre-flight CONVERGED in 3 rounds**:
  r1 = 2 BLOCKER + 4 HIGH + 2 MED (wrong LabEngine boundary, undefined-name deps, SearchEngine↔LabEngine
  cycle, GUARD-03 gaps, SEED-011 test premise, LAB_LOGGER routing) → revised; r2 = 2 HIGH runtime
  LOGGER-monkeypatch retargets + 2 LOW → fixed in 125-04; r3 = **PLANS CLEAR** (1 LOW doc nit fixed).
  NEXT: execute wave-by-wave (125-01→04), source-integrity gate between waves, then Codex CODE
  review + base-vs-HEAD name-level diff + facade-name diff → verify.
  **KEY RESEARCH FINDING + FIX (done):** the 8 "pre-existing" red tests were NOT SEED-011
  forward-spec — they were a **Phase-123 regression**: commit 674d16b5 added a UTF-8 BOM to
  genizah_core.py → every AST/source-scan test hit `SyntaxError: U+FEFF`. Fixed by byte-level
  BOM strip (commit 29d51f4a); all 8 now green; zero behavior change. Bulk suite now CLEAN.
  (Phase-123 verify MISSED this; my 124 SUMMARY mis-attributed it — both corrected.)
  Plan shape (researcher): 125a = SEED-011 dedup ONLY (BOM already fixed) — NOTE the
  ChunkPlan needs TWO query strings (Genizah vs LOCAL diacritic-fold differ; SEED-006 M1);
  then 125b LabSettings → 125c LabEngine → 125d SearchEngine (~3,490 ln, most importers;
  preserve BrowseMap class-cache, SEED-006 content_search gates, _LAST_RESPONSA_DOWNGRADE
  thread-locals; 6 downgrade names need facade shims). GUARD-03: 5 source-scan test files.
  Tell the planner the BOM is ALREADY fixed (don't re-include in 125a).

  FULL DRILL for phases 125-127 (TWO Codex touchpoints — pre-flight + post-exec):
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
Last activity: 2026-06-26 -- Phase 125 execution started

Progress: [█████░░░░░] 50% (3 of 6 phases — 122, 123, 124 complete)

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

Last session: 2026-06-26T01:03:04.921Z
Stopped at: Completed 123-01-PLAN.md
Resume file: None
Next step: `/gsd-discuss-phase 122`

## Performance Metrics

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| (none yet — milestone just started) | - | - | - |
| 122 | 1 | - | - |
| Phase 123 P01 | 63 | 7 tasks | 13 files |
| Phase 124-core-metadata-index P01 | 90 | 2 tasks | 8 files |

## Decisions

- [Phase ?]: Engine-side helpers stay in genizah_core.py — depend on Tantivy engine context
- [Phase ?]: Inline _tr() helper for tr()-dependent modules — lazy CURRENT_LANG import satisfies GUARD-01
