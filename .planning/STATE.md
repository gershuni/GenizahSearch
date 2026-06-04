---
gsd_state_version: 1.0
milestone: v8.0.0
milestone_name: Joins Lab — Phases 106-110
status: verifying
stopped_at: Phase 107 context gathered
last_updated: "2026-06-04T03:37:20.353Z"
last_activity: 2026-06-03
progress:
  total_phases: 8
  completed_phases: 2
  total_plans: 7
  completed_plans: 8
  percent: 100
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-06-02)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Phase 106 — joins-lab-shared-core-pure-logic-service-adapters-unit-tests

## Current Position

Milestone: **v8.0.0 Dicta Rebrand & Joins Lab — STARTED 2026-06-02.** Folds the delivered v7.17 cycle (the "Dicta Genizah Search Pro" rebrand + LOCAL "My Library" export, Phases 103 + 105) into the flagship v8.0.0 release, and adds **Joins Lab** — an interactive **human-in-the-loop** join-hunting workbench (Component A) + optional search-support algorithms (Component B), both apps. NO automated join-finder.

Phase: 107
Plan: Not started
Status: Phase complete — ready for verification
Last activity: 2026-06-03

### Scope (locked 2026-06-02)

**Component A — Join Workbench (primary, both apps):** JWB-01 dedicated tab/page · JWB-02 "Find joins" entry from desktop ResultDialog + Browse (web+desktop) + open-by-shelfmark · JWB-03 pinned anchor (image + numbered transcription) · JWB-04 show existing/known joins (PGP+FJMS+user+community) · JWB-05 conservative `[`/`]` tear-side assist (only when clear) · JWB-06 seed search from anchor into the existing search module (variants/fuzzy/Responsa/regex), editable · JWB-07 collect candidates to a list · JWB-08 side-by-side compare · JWB-09 act on confirmed join (joins button + export + add-to-list; optional Puzzle).

**Component B — Search-support algorithms (secondary, independent, both apps):** JSA-01 parallels seeded from anchor · JSA-02 corpus-driven suggest-then-search completion (first/last N words) · JSA-03 `[`/`]`-aware torn-word completion.

**Deferred to Future (not v8.0.0):** JOINS-F1 relative-offset cross-line positional search (spike-gated) · JOINS-F2 Dicta/Sefaria citation-ID completion source · JOINS-F3 batch-export + persisted list re-import · JOINS-F4 auto-ranked finder (out) · EXP-F3 composition-report LOCAL export · PERF-F1 D-F12 search latency. One-click citations parked in `docs/FEATURE_IDEAS.md`.

### Next

1. ✅ **Genizah-scholar design-critique session — DONE (2026-06-03).** Ran as a throwaway-sketch
   exploration; conclusions in `REQUIREMENTS.md` § Design-Critique Conclusions & Amendments.
   Sketch preserved at tag `spike-002-joins-workbench` + `.planning/spikes/002-.../sketch/`.

2. ✅ **v8.0.0 Joins Lab roadmap — CREATED (2026-06-03).** Phases 106-110 in `ROADMAP.md`.
3. **`/gsd-discuss-phase 106`** (user-led — do NOT auto-answer). Resolve the 7 deferrals
   (REQUIREMENTS § Deferrals / discuss-phase questions) BEFORE the Phase 106 plan locks. The most
   load-bearing for 106's scope: #1 join-model richness (confirm pairwise→group stays for v8), #3
   builder depth (variation columns? raw-query preview? page-level Text START/END?), #6 JSA-02/03
   keep-spike-cut (affects whether Phase 110 grows). #2/#4/#5/#7 bear more on 108/109 and the web
   phase but should be acknowledged.

4. **`/gsd-plan-phase 106`** → execute → verify; repeat down the chain 107 → 108 → 109 → 110. Web
   Joins Lab UI is a LATER phase on the shared core (deferral #5).

**Version decision RESOLVED:** ship as **v8.0.0** (closes the open decision from 2026-06-01). The actual version-file bump (`scripts/bump_version.py 8.0.0`, plus `_TARGET_VERSION` in `tests/test_release_artifacts.py` which the bumper misses, plus a `## [8.0.0]` CHANGELOG section folding the `[Unreleased]` rebrand note) happens at `/release` time — NOT now. Rebrand gotchas in memory `project_desktop_app_rebrand.md`.

**Build prerequisite (Joins Lab):** the Tantivy index must carry `line_starts` / `line_ends` (older indexes raise a rebuild error at `genizah_core.py:8583`). Already satisfied on web + most desktop users; degrade gracefully for stragglers.

## Deferred Items

Items acknowledged and deferred at v7.16 milestone close on 2026-06-01 (`gsd-tools.cjs audit-open` reported 102 items; same historical accumulation as the v7.14/v7.15 closes — none are milestone blockers):

| Category | Count | Notes |
|----------|-------|-------|
| Debug sessions | 41 | Mostly diagnosed-not-closed entries predating v7.13. Includes `local-search-freeze-2026-05-31` — actually RESOLVED (OPEN_ISSUES D-F23); tracker entry stale. |
| UAT gaps | 1 | Phase 100 `100-HUMAN-UAT.md` — 0 pending scenarios (effectively done; flag not flipped). |
| Quick tasks | 53 | Historical backlog (oldest from 2026-02). Use `/gsd-cleanup` to triage between milestones. |
| Pending todos | 6 | Largest: server-side search with email notification; NLI MARC crawl; unified metadata text search; one-click scholarly citations (2026-06-01). |
| Unimplemented seeds | 2 | SEED-001 server-side IIIF image cache (dormant; blocked on NLI TOS); SEED-003 opt-in OCR extension for image-only / corrupt-text-layer PDFs (dormant). |

Carried forward to v8.0.0+ (logged in `docs/OPEN_ISSUES.md`): **D-F12** (regular Search ~8s wall-clock — profile-first), **D-F18** (context-menu LOCAL detection via `display`). **EXP-F3** (composition-report LOCAL export, gated on a LOCAL comp-search UI). D-F17 (LOCAL export shape) is now DELIVERED (Phases 103 + 105, folded into v8.0.0). Recommend a `/gsd-cleanup` pass on the historical backlog.

## Recently Closed Milestones

- **v7.17 (folded, NOT separately closed)** — the rebrand + LOCAL export work shipped under the v7.17 phase numbers (103, 105) but, per the 2026-06-02 decision, is **folded into v8.0.0** rather than tagged/closed as its own milestone. Phases 103/105 retained as delivered. No "v7.17" release tag will exist.
- **v7.16 Hebrew PDF Text Quality** — shipped 2026-06-01 (v7.16.0, desktop); 1 formal phase (102, 5 plans) + no-phase de-space/UAT/freeze work; tag `v7.16.0` @ `ccb87c90`. LOCAL Hebrew PDF text-layer extraction rewrite, file-management actions for LOCAL hits, three search/startup freeze fixes. See `.planning/milestones/v7.16-ROADMAP.md`.
- **v7.15 My Library Visual** — shipped 2026-05-28; 3 phases (99, 100, 101); 7 plans; 6/6 PDFIMG-*. PDF page image rendering in ResultDialog + Browse, RTL/bidi reflow fixes, "Re-index All" recovery button.
- **v7.14 My Library — Local Document Search** — shipped 2026-05-24 (v7.14.0), closed 2026-05-27; 6 phases (95, 96, 97, 97.2/97.3 inserted, 98); 37 plans. Desktop local document search + Phase 98 NLI resilience.
- **v7.13 Research-Grade Downloads & PGP Filter** — shipped 2026-05-21 (v7.13.0), closed 2026-05-27; 2 phases (93, 94); 5 plans; 14/14 requirements.
- **v7.12 Multitenant Architecture (Path B)** — shipped 2026-05-18; 10 phases; 28 plans; 49/49 requirements.

## Accumulated Context

### Roadmap Evolution

- v8.0.0 opened (2026-06-02): folds the delivered v7.17 cycle (rebrand + LOCAL export, Phases 103/105) into the flagship v8.0.0 release and adds **Joins Lab** (Spike 002, FEASIBLE / ~M). Two independent components — A: Join Workbench hub (primary); B: search-support algorithms (secondary). Both apps. Human-in-the-loop; the auto-ranked v7/v8 finder is explicitly OUT. **Roadmap deferred** until after a user-led Genizah-scholar design-critique session. Phase numbering will continue from 105.
- Phases 103-105 (v7.17 cycle, now folded into v8.0.0): Phase 103 Search-results LOCAL export (LEXP-01/03–08, COMPLETE & verified); Phase 104 → DEFERRED to EXP-F3 (no LOCAL comp-search UI); Phase 105 Export UX Polish (EXPUX-01..04, implemented — EXPUX-01 dialog UAT pending). Desktop rebrand → "Dicta Genizah Search Pro" delivered as pre-release polish (commit `6e0c312d` + follow-ups).
- Phase 102 (v7.16) EXECUTED & CLOSED (commit `494c0c49`) + POST-102 de-space quality pass as NO-PHASE edits (D-F13b/c/d). Existing LOCAL libraries need one manual "Re-index All" (`extraction_format_version` 2→3).

## Session Continuity

Last session: 2026-06-04T03:37:20.320Z
Stopped at: Phase 107 context gathered
Resume file: .planning/phases/107-desktop-join-workbench-anchor-entry-points-actions-join-model/107-CONTEXT.md
Next step: `/gsd-discuss-phase 106` (user-led — resolve the 7 deferrals; do NOT auto-answer), then `/gsd-plan-phase 106` → execute → verify, chaining 107→110. Web UI a later phase.
