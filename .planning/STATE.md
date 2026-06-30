---
gsd_state_version: 1.0
milestone: v8.3.0
milestone_name: God-File Decomposition + Search & Browse UX
status: Awaiting next milestone
stopped_at: Phase 129 context gathered
last_updated: "2026-06-30T03:54:58.331Z"
last_activity: 2026-06-30 — Milestone v8.3.0 completed and archived
progress:
  total_phases: 8
  completed_phases: 6
  total_plans: 22
  completed_plans: 20
  percent: 75
---

# Project State

## Project Reference

See: .planning/PROJECT.md

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Phase 129 — library-filter-search-browse-by-identification-seed-026

## Current Position

Phase: Milestone v8.3.0 complete
Plan: —
Status: Awaiting next milestone
Last activity: 2026-06-30 — Milestone v8.3.0 completed and archived

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

### v8.3.0 milestone close (2026-06-30)

Items acknowledged and deferred at v8.3.0 milestone close on 2026-06-30:

| Category | Item | Status |
|----------|------|--------|
| debug | desktop-tabular-rtl | diagnosed |
| debug | joins-lab-image-resolution | fix_implemented_pending_uat |
| debug | puzzle-nli-tiny-images | unknown |
| debug | web-catalog-browse-columns-broken | investigating |
| quick_task | 260322-jtk-brown-bg-removal-open-issues-md | missing |
| todo | 2026-02-11-migrate-desktop-corrections-to-shared-service | pending |
| todo | 2026-03-07-server-side-search-email-notification | pending |
| todo | 2026-03-08-nli-marc-crawl-and-translate | pending |
| todo | 2026-03-09-unified-metadata-text-search | pending |
| todo | 2026-04-16-reading-desk-ux-fixes | pending |
| todo | (1 more pending todo) | pending |
| seed | SEED-001-server-iiif-image-cache | dormant |
| seed | SEED-003-optional-ocr-extension | dormant |
| seed | SEED-005-thin-installer-data-manager | dormant |
| seed | SEED-012-supabase-startup-hang-hardening | dormant |
| seed | SEED-028-method-based-desktop-panel-extraction | dormant |
| uat | Phase 129 129-HUMAN-UAT.md (3 live-render scenarios) | partial |
| verification | Phase 128 128-VERIFICATION.md (live render smoke) | human_needed |
| verification | Phase 129 129-VERIFICATION.md (live render smoke) | human_needed |

**Note:** SEED-025 (Space-scroll), SEED-026 (library filter), and SEED-027 (refresh CODE_INDEX) were flagged as "dormant" by the audit but were in fact **DELIVERED in v8.3.0** — not deferred. Their seed state files are stale; treat them as complete.

## Session Continuity

Last session: 2026-06-28T20:23:43.457Z
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
| Phase 129 P06 | 7min | 3 tasks | 3 files |
| Phase 129 P07 | 35 | 3 tasks | 4 files |

## Decisions

- [Phase ?]: Engine-side helpers stay in genizah_core.py — depend on Tantivy engine context
- [Phase ?]: Inline _tr() helper for tr()-dependent modules — lazy CURRENT_LANG import satisfies GUARD-01
- [Phase ?]: LabSettings extracted to shared/lab_settings.py; same-object facade shim in genizah_core
- [Phase ?]: Phase 125-03
- [Phase ?]: SearchEngine extracted to shared/search_engine.py with 20-name facade; 7 lazy imports break cycles to genizah_core
- [Phase ?]: Phase 126 D1: MOVE-and-shim 5 dialogs to desktop/settings_dialogs.py + 4 widgets to desktop/ui_widgets.py; originals deleted, identity holds 9/9; D-07b strip verbatim; GenizahGUI.apply/cancel_settings added; LabPanel deferred to E2
- [Phase ?]: GAP-E: catalog library filter uses checkbox dialog not ui.select; FINDING 1 all-unchecked guard
- [Phase ?]: GAP-F: consume_incoming_filters persists search_library_filter key; persist->reload lifecycle via load-before-consume ordering in search.py
- [Phase ?]: GAP-G closed
- [Phase ?]: FINDING 2 closed

## Operator Next Steps

- Start the next milestone with /gsd:new-milestone
