---
gsd_state_version: 1.0
milestone: v8.3.0
milestone_name: God-File Decomposition
status: verifying
stopped_at: Completed 123-01-PLAN.md
last_updated: "2026-06-26T01:03:04.928Z"
last_activity: 2026-06-26
progress:
  total_phases: 6
  completed_phases: 3
  total_plans: 3
  completed_plans: 3
  percent: 50
---

# Project State

## Project Reference

See: .planning/PROJECT.md

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Phase 124 — core-metadata-index

## Current Position

Phase: 124 (core-metadata-index) — EXECUTED + Codex CODE review APPROVE; VERIFIER RUNNING
Plan: 1 of 1
Status: Executed (6 commits b63411c1..741f7b24). Post-exec review caught 3 defects the
  executor misreported as "pre-existing" — _parse_cudl_label facade drop (fc3ce883),
  path-string-registry GUARD-03 miss (e4abf248), tantivy import-order GUARD-02 change
  (741f7b24, Codex r1 HIGH). Codex CODE review converged R1->R2 APPROVE (0 findings).
  GUARD-02 confirmed zero new failures via base-vs-HEAD name-level diff (8 pre-existing
  reds confirmed at base; gui+render_smoke green). gsd-verifier running.
  Remaining for 124: verifier PASS → auto-advance to 125.

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
Last activity: 2026-06-26

Progress: [███░░░░░░░] 33% (2 of 6 phases — 122, 123 complete)

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
