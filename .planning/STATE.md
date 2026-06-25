---
gsd_state_version: 1.0
milestone: v8.3.0
milestone_name: God-File Decomposition
status: verifying
stopped_at: Phase 123 context gathered
last_updated: "2026-06-25T15:55:57.966Z"
last_activity: 2026-06-25
progress:
  total_phases: 6
  completed_phases: 1
  total_plans: 1
  completed_plans: 1
  percent: 17
---

# Project State

## Project Reference

See: .planning/PROJECT.md

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Phase 122 — config-enabler

## Current Position

Phase: 123
Plan: Not started
Status: Phase complete — ready for verification
Last activity: 2026-06-25

Progress: [░░░░░░░░░░] 0%

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

Last session: 2026-06-25T15:55:57.959Z
Stopped at: Phase 123 context gathered
Resume file: .planning/phases/123-core-leaf-modules/123-CONTEXT.md
Next step: `/gsd-discuss-phase 122`

## Performance Metrics

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| (none yet — milestone just started) | - | - | - |
| 122 | 1 | - | - |
