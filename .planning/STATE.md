---
gsd_state_version: 1.0
milestone: v8.4.0
milestone_name: Dual-Mode Library Filter
status: ready
stopped_at: Phase 131 complete (user-approved 2026-07-01)
last_updated: "2026-07-01T04:26:41.044Z"
last_activity: 2026-07-01
progress:
  total_phases: 3
  completed_phases: 2
  total_plans: 13
  completed_plans: 13
  percent: 67
---

# Project State

## Project Reference

See: .planning/PROJECT.md

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Phase 132 — public-api-dual-mode (v8.4.0)

## Current Position

Phase: 132 (public-api-dual-mode) — Not started
Plan: Not started
Status: Phase 131 COMPLETE (user-approved UAT 11/11 on 2026-07-01). Dual-mode library filter now at full desktop + web parity (catalog + /search + /parallels), incl. dynamic counts, sort, type-to-find, and searchable English codes. Phase 132 (public API dual-mode) not yet started.
Last activity: 2026-07-01 -- Phase 131 completed (10 plans: 131-01..05 + gaps 131-06..10)
Next step: /gsd-discuss-phase 132 (recommended) or /gsd-plan-phase 132. Optional: /gsd-secure-phase 131 (security_enforcement is on; no 131-SECURITY.md yet).

## Accumulated Context

### Key Decisions (v8.4.0 roadmap)

- **Three phases, theme-grouped (condensed roadmap):** Phase 130 (lead — web `/search` core model + persistence + migration + edge states + button/label), Phase 131 (the three parity surfaces: desktop catalog dialog + web Browse-by-Identification + web `/parallels`), Phase 132 (public API mode). One phase per requirement was explicitly rejected per the project's condensed-roadmap preference.
- **Phase 130 settles the shared (mode + set) shape FIRST:** Phases 131/132 mirror it. The mode toggle + persistence + migration + edge-state semantics must be locked before any parity surface or the API extends the model.
- **Show-only = allowlist, Hide = denylist:** consistent with the existing `domain_exclusions` / printed-filter exclusion semantics. The Hide intent must persist as NEW libraries surface on later searches (the core "hide RNL stays hidden" behavior).
- **Migration interpretation (DMF-05):** existing v8.3.0 `search_library_filter` allowlist values load as **Show-only with the existing set** — no error, no re-entry required.
- **Edge-state sentinels (DMF-06):** empty selection in Show-only = "show all" (must not collide with the all-unchecked sentinel); a fully-populated Hide set (everything hidden) handled predictably.
- **DMF-10 is a cross-cutting invariant, not a phase:** the `'LOCAL'` guard is folded into every web phase's success criteria. It tripped the v8.3.0 release-commit CI — sanitize against `'LOCAL'` (not just `LIBRARY_CODES`) on every new web filter path.
- **API exclude = complement (DMF-11):** `mode=exclude` resolves to sys_ids whose `library_code` is NOT in the given set, intersected into `restrict_sys_ids` (mirrors the UI). Omitted mode defaults to `include` (today's allowlist behavior) — backward-compatible.

### Blockers/Concerns

None at roadmap creation.

### Pending Todos

- Begin with `/gsd-discuss-phase 130` (or skip-discuss-if-empty per the standing autonomous directive).

## Deferred Items

Items carried forward from v8.3.0 and earlier:

| Category | Item | Status | Deferred At |
|----------|------|--------|-------------|
| DMF-future | Cross-device sync of the library-filter preference (currently device-local via safe_storage) | Future | v8.4.0 |
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

**Note:** SEED-025 (Space-scroll), SEED-026 (library filter), and SEED-027 (refresh CODE_INDEX) were flagged as "dormant" by the audit but were in fact **DELIVERED in v8.3.0** — not deferred. Their seed state files are stale; treat them as complete. v8.4.0 is the planned **evolution** of SEED-026 (dual-mode).

## Session Continuity

Last session: 2026-06-30T16:30:00.000Z
Stopped at: Phase 131 executed (131-01..05 + gaps 131-06..10) — held for human UAT
Resume file: None
Next step: Run the 11 items in 131-HUMAN-UAT.md against a live instance, then re-run `/gsd-execute-phase 131` (verifier re-runs) to complete the phase, or `/gsd-plan-phase 131 --gaps` if UAT surfaces issues.

## Performance Metrics

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| (none yet — milestone just started) | - | - | - |
| Phase 130 P01 | 5 | 2 tasks | 1 files |
| Phase 130 P02 | 8 | 3 tasks | 3 files |
| Phase 130 P03 | 8 | 3 tasks | 2 files |
| 130 | 3 | - | - |
| Phase 131 P02 | 20m | 3 tasks | 2 files |
| Phase 131 P03 | 15 | 2 tasks | 2 files |
| Phase 131 P05 | 60 | 2 tasks | 1 files |
| Phase 131 P07 | 20 | 3 tasks | 3 files |
| 131 | 10 | - | - |

## Decisions

- [Phase 130]: (lead) define the shared (mode + set) state shape on web `/search` — mode toggle + safe_storage persistence + legacy-allowlist migration + edge-state sentinels + button/label, all settled before parity surfaces extend it.
- [Phase ?]: library_mode defaults to 'hide': D-05 fresh-user default, Hide mode with empty set = show all
- [Phase ?]: clear_search_snapshot resets search_library_filter to {'mode':'hide','codes':[]}: D-09 dict shape settles (mode+set) persistence contract for Plan 02
- [Phase ?]: show-all normalized to neutral hide/[] on Apply (D-05/DMF)
- [Phase ?]: browse->search handoff stamps mode=show_only + persists dict shape (prevents misread as Hide-set)

## Operator Next Steps

- `/gsd-discuss-phase 130` then `/gsd-plan-phase 130`.
