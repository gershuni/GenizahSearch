---
gsd_state_version: 1.0
milestone: none
milestone_name: "(none — v8.4.0 + v8.4.1 CLOSED 2026-07-01; awaiting /gsd-new-milestone)"
status: no_active_milestone
stopped_at: "v8.4.0 + v8.4.1 SHIPPED & CLOSED 2026-07-01. v8.4.0 (Phases 130-131, both apps): web deployed + desktop installer published to GitHub Release v8.4.0 @ 16fcf7a1 (latest, verified state:uploaded). v8.4.1 (Phase 132, web): public-API library_filter_mode deployed + live-verified. Both milestones archived (v8.4.0-* + v8.4.1-*); phase dirs moved to milestones/; REQUIREMENTS.md removed (fresh next milestone). No active milestone — run /gsd-new-milestone."
last_updated: "2026-07-01T21:00:00.000Z"
last_activity: 2026-07-01
progress:
  total_phases: 0
  completed_phases: 0
  total_plans: 0
  completed_plans: 0
  percent: 0
---

# Project State

## Project Reference

See: .planning/PROJECT.md

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** None — v8.4.0 + v8.4.1 shipped & closed 2026-07-01. Awaiting `/gsd-new-milestone`.

## Current Position

**No active milestone.** v8.4.0 (Dual-Mode Library Filter, Phases 130-131) and v8.4.1 (Public
API Dual-Mode, Phase 132) both SHIPPED & CLOSED 2026-07-01.

- **v8.4.0 (both apps):** web deployed to production (`deploy.sh master-main`); desktop
  installer `GenizahSearchPro_V8.4.0_Setup.exe` (531,242,765 bytes) uploaded + published to
  GitHub Release `v8.4.0` @ `16fcf7a1` (marked latest; verified `state:uploaded`,
  `isDraft:false`).
- **v8.4.1 (web):** public-API `library_filter_mode` (DMF-11) deployed to genizahsearch.com
  on the 8.4.0 tree + live-verified on the 255K corpus (exclude CUL→0 CUL; include CUL→all
  CUL; bad mode→400). Web point-release — no `version.py` bump / git tag (desktop stays 8.4.0).
- **Milestone close DONE:** both archived (`v8.4.0-ROADMAP.md`/`-REQUIREMENTS.md` +
  `v8.4.1-ROADMAP.md`/`-REQUIREMENTS.md`); phase dirs moved to `milestones/v8.4.0-phases/`
  (130,131) + `milestones/v8.4.1-phases/` (132); ROADMAP collapsed; MILESTONES.md + PROJECT.md
  evolved; `.planning/REQUIREMENTS.md` removed (fresh next milestone). DMF-01..12 Validated;
  DMF-13 Partial (behaviorally safe, carried forward).

All commits PUSHED to origin/master-main. **Next: `/gsd-new-milestone`.**

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

### Quick Tasks Completed

| # | Description | Date | Commit | Status | Directory |
|---|-------------|------|--------|--------|-----------|
| 260714-9jc | FGP default demotion via coverage ratio (SEED-030) — show V0.8/HTR when the folio's FGP is a partial excerpt; whole-MS baseline for whole-doc rows; all 4 selector surfaces | 2026-07-14 | 6166fdf5 | Codex APPROVE (4 rounds); not yet released | [260714-9jc-fgp-default-demotion-via-coverage-ratio-](./quick/260714-9jc-fgp-default-demotion-via-coverage-ratio-/) |

### Pending Todos

- Begin with `/gsd-discuss-phase 130` (or skip-discuss-if-empty per the standing autonomous directive).

## Deferred Items

Items carried forward from v8.3.0 and earlier:

| Category | Item | Status | Deferred At |
|----------|------|--------|-------------|
| DMF-11 (Phase 132) | Public API dual-mode: `mode` (include/exclude) on `POST /api/search` + `/api/parallels` — complement resolution into `restrict_sys_ids`, docs + skill `api_contract.md`. Roadmap detail + 3-plan-shaped success criteria already written in ROADMAP.md Phase 132. | Deferred → v8.4.1 | v8.4.0 (2026-07-01) |
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

Last session: 2026-07-01
Stopped at: v8.4.0 + v8.4.1 milestone close COMPLETE — both archived (ROADMAP collapsed, MILESTONES.md + PROJECT.md evolved, REQUIREMENTS.md removed); v8.4.0 desktop installer published + verified; v8.4.1 web live-verified.
Resume file: None
Next step: `/gsd-new-milestone` to open the next cycle. No active milestone.

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
| Phase 132 P03 | 8 | 2 tasks | 2 files |
| 132 | 3 | - | - |

## Decisions

- [Phase 130]: (lead) define the shared (mode + set) state shape on web `/search` — mode toggle + safe_storage persistence + legacy-allowlist migration + edge-state sentinels + button/label, all settled before parity surfaces extend it.
- [Phase ?]: library_mode defaults to 'hide': D-05 fresh-user default, Hide mode with empty set = show all
- [Phase ?]: clear_search_snapshot resets search_library_filter to {'mode':'hide','codes':[]}: D-09 dict shape settles (mode+set) persistence contract for Plan 02
- [Phase ?]: show-all normalized to neutral hide/[] on Apply (D-05/DMF)
- [Phase ?]: browse->search handoff stamps mode=show_only + persists dict shape (prevents misread as Hide-set)
- [Phase 132 Plan 01]: library_filter_mode default=None (not 'include') — model_dump(exclude_none=True) drops it → omitted callers' echo stays byte-for-byte unchanged (Codex R1 HIGH); _intersect_library_filter normalises None→'include' internally

- [Phase 132 Plan 02]: resolve_library_complement_sys_ids is a separate module-level function in shared/fjms_service.py (not inlined) — mirrors naming convention, independently testable; mode read AFTER `if not libs` short-circuit so exclude+empty is a clean no-op

- [Phase 132 Plan 03]: Document behavioral default (include) not Pydantic internal (None) — callers see include when omitting the field; unknown_filter_key Error Codes entry clarified: code reserved in ERROR_CODES but Pydantic extra=forbid fires first returning invalid_request

## Operator Next Steps

- v8.4.0 + v8.4.1 CLOSED (2026-07-01). No active milestone — run `/gsd-new-milestone` to open the next cycle.
