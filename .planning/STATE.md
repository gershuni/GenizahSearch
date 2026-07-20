---
gsd_state_version: 1.0
milestone: v9.0.0
milestone_name: Discovery — Same-Work Identification & Connection Atlas (web)
status: planning
last_updated: "2026-07-20T00:00:00.000Z"
last_activity: 2026-07-20
progress:
  total_phases: 7
  completed_phases: 0
  total_plans: 0
  completed_plans: 0
  percent: 0
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-07-20)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Phase 133 — Visual Atlas Preview (early quick win; roadmap created, awaiting UX discuss-phase + planning)

## Current Position

Phase: 133 of 139 (Visual Atlas Preview — early quick win) — 7 phases (133-139)
Plan: — (not yet planned)
Status: Roadmap created (owner revision applied 2026-07-20: atlas preview promoted to first deployable artifact); ready for UX discuss-phase then plan-phase 133
Last activity: 2026-07-20 — v9.0.0 roadmap restructured to Phases 133-139 (Phase 133 = Visual Atlas Preview under the REL-01 atlas-preview exception), 40/40 requirements mapped

Progress: [░░░░░░░░░░] 0%

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

Last session: 2026-07-20
Stopped at: v9.0.0 roadmap restructured per owner revision — Phase 133 = Visual Atlas Preview (ATLAS-01, REL-01 atlas-preview exception); former phases shifted to 134-139; ROADMAP.md + REQUIREMENTS.md traceability (40/40) + STATE.md consistent.
Resume file: None
Next step: UX discuss-phase, then `/gsd-plan-phase 133`.
