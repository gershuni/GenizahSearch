---
gsd_state_version: 1.0
milestone: v9.0.0
milestone_name: Discovery — Same-Work Identification & Connection Atlas
status: executing
stopped_at: Completed 133-02-PLAN.md
last_updated: "2026-07-20T16:38:54.640Z"
last_activity: 2026-07-20
progress:
  total_phases: 7
  completed_phases: 0
  total_plans: 6
  completed_plans: 2
  percent: 0
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-07-20)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Phase 133 — visual-atlas-preview-early-quick-win

## Current Position

Phase: 133 (visual-atlas-preview-early-quick-win) — EXECUTING
Plan: 3 of 6
Status: Ready to execute
Last activity: 2026-07-20

Progress: [██░░░░░░░░] 17%

## Roadmap Summary (v9.0.0)

Condensed 7-phase roadmap: an early atlas quick win, then the REL-01 gate sequence (thin de-risk spine first):

| Phase | Goal | Requirements |
|-------|------|--------------|
| 133 Visual Atlas Preview (early quick win) | Static, canon-masked corpus-overview atlas on a standalone `/atlas` beta page; offline layout bake; FIRST deployable artifact under the REL-01 atlas-preview exception | ATLAS-01 |
| 134 Discovery Data Spine | Masked, versioned sidecar + async service + frozen-frame/budget artifacts | DATA-01..08, DATA-10, PERF-01 |
| 135 Precision Certificate & Confidence Bands | Data-driven band display + methods page + pre-registered tier-A measurement | BAND-01..05, CERT-01, CERT-02 |
| 136 Read Surfaces — Panel & Work→Witnesses | Browse connections panel + `/work/{id}` witness-map | PANEL-01..03, WORK-01, WORK-02 |
| 137 Community Judgments | Supabase migration + ✓/?/✗ voting layer (never affects bands) | JUDGE-01..05 |
| 138 Leads Queue | `/leads` R-B screening lane, uncertified, canon caveated | LEADS-01, LEADS-02 |
| 139 Atlas Drill-down, Homepage & Release Hardening | Server-bounded drill-down (absorbs the preview) + homepage band + SEO/i18n/RTL/a11y/obs + REL-01 flag-flip | ATLAS-02/03, SEO-01, I18N-01/02, A11Y-01/02, OBS-01/02, REL-01 |

## Accumulated Context

### Key Decisions (v9.0.0 roadmap)

- **OWNER REVISION (2026-07-20): Visual Atlas Preview is the milestone's FIRST deployable artifact** — new self-contained Phase 133 (ATLAS-01, offline bake from the research data via the `build_atlas_draft.py` prototype approach; no claim-model sidecar dependency), deployed early under the REL-01 ATLAS-PREVIEW EXCEPTION: no claim-level statements (no identifications/bands/numbers, cluster/shelfmark-level only), work labels only from reviewed neutral titles or omitted, asset-level masking scan, PERF/i18n basics, behind the feature flag. The full REL-01 gates still govern everything else.
- **7 theme-grouped phases (condensed):** one-phase-per-requirement rejected per the house condensed-roadmap preference.
- **REL-01 ordering is the spine (Phases 134-139):** claim model + masked schema → title map + sidecar + frozen-frame (both in 134) → certificate card draw (135) → read surfaces (136) → Supabase migration + security smoke → judgment UI (both in 137) → leads (138) → bounded atlas → public promotion (both in 139). No reorder across these gates.
- **CERT-01 is a parallel research track:** its frame freezes AFTER Phase 134 distillation stabilizes; cards drawn in Phase 135; owner grading runs in parallel with the Phase 136–138 UI build; the completed certificate gates the Phase 139 REL-01 public-promotion flag-flip.
- **Cross-cutting reqs homed in Phase 139** (I18N-01/02, A11Y-01/02, SEO-01, OBS-01/02, REL-01) as the comprehensive release-hardening gate — but translations/RTL/a11y are built into every UI surface from line one; 139 owns final verification.
- **Two hard blockers drive Phase 134:** M-source provenance masking (structural, at the sidecar-build boundary + permanent leak-vector CI scan) and event-loop safety (all sidecar/graph queries off the loop via the DiscoveryService, timeouts + concurrency cap).
- **UX discuss-phase precedes Phase 133/134 planning** and settles: ATLAS-02 graph primary object (before the Phase 133 layout bake), DATA-01 relation-vocabulary bilingual wording, BAND-04 per-surface disclaimer wording, final band-selection/row counts, neutral work-title curation workflow, atlas scope.

### Pending Todos

- Run the UX discuss-phase (atlas graph object / atlas scope / relation wording / disclaimer wording / band-selection / title curation), then `/gsd-plan-phase 133`.

### Blockers/Concerns

None at roadmap creation.

## Deferred Items

Carried forward from prior milestones (unchanged; see MILESTONES.md for full context):

| Category | Item | Status | Deferred At |
|----------|------|--------|-------------|
| FUT-01 | Text-reuse engine as `/parallels` (desktop: composition) backend | Future (v9-deferred by user) | v9.0.0 |
| FUT-02 | Public API endpoints for discovery (band-labeled, masked) + skill parity | Future | v9.0.0 |
| FUT-03 | Desktop parity for the discovery module | Future | v9.0.0 |
| FUT-04 | Refresh pipeline/cadence for the discovery snapshot | Future | v9.0.0 |
| FUT-05 | Live-interactive full-corpus WebGL atlas (sigma.js) + multi-hop | Future | v9.0.0 |
| FUT-06 | Public rendering of moderated free-text annotations | Future | v9.0.0 |
| FUT-07 | R-B / gen-2-at-scale certification; R-A independent audit (external gate) | Future | v9.0.0 |
| FUT-08 | New generalized discovery exports (xlsx/CSV) | Future | v9.0.0 |

Older cross-milestone deferrals (JSA/JWB Component B, DEFER-01..05 decomposition, D-F12, etc.) remain tracked in `docs/OPEN_ISSUES.md` and the v8.4.0 archive; not v9.0.0-relevant.

## Session Continuity

Last session: 2026-07-20T16:38:54.633Z
Stopped at: Completed 133-02-PLAN.md
Resume file: .planning/phases/133-visual-atlas-preview-early-quick-win/133-03-PLAN.md
Next step: UX discuss-phase, then `/gsd-plan-phase 133`.

## Performance Metrics

| Phase | Plan | Duration | Notes |
|-------|------|----------|-------|
| Phase 133 P01 | 55min | 1 tasks | 4 files |
| Phase 133 P02 | 40min | 3 tasks | 12 files |

## Decisions

- [Phase 133 P01]: scan_repo uses a fast literal-byte matcher (not the rich normalized/encoded matcher) to stay practical over a real working tree with large non-ignored untracked content — A full --scan-repo run against the actual working tree (~24GB unrelated ACL2026_papers/) exceeded 3 minutes with a shared rich matcher; splitting scan_repo (fast) from scan_asset (rich) matches the plan's own acceptance-criteria wording and completes in ~2 minutes
- [Phase 133 P01]: M-source leak in genizah_translations.py scrubbed via codename rename (M-source), not deletion, preserving the unwired Discovery Review deck glossary's structure
- [Phase 133]: [Phase 133 P02]: Small fixed/dynamic lookup tables (domain groups, library codes) live in manifest.json rather than the binary string heap; edge deltas are plain unsigned (no zigzag) via a group-reset rule; island-only clusters reuse the SAME force-layout/dust-ring code path as continuation clusters with MIN_CLUSTER=1
