---
gsd_state_version: 1.0
milestone: none
milestone_name: "(between milestones — v8.0.0 shipped & closed)"
status: milestone_complete
stopped_at: v8.0.0 closed 2026-06-11 (retroactive); awaiting /gsd-new-milestone
last_updated: "2026-06-11T00:00:00.000Z"
last_activity: 2026-06-11
progress:
  total_phases: 7
  completed_phases: 7
  total_plans: 31
  completed_plans: 35
  percent: 100
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-06-11 after v8.0.0 close)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Between milestones — v8.0.0 shipped & closed. Run `/gsd-new-milestone` to start the next cycle.

## Current Position

**Between milestones.** v8.0.0 Dicta Rebrand & Joins Lab **shipped 2026-06-09** (both apps; tag
`v8.0.0` @ `71e0912e`; GitHub Release with desktop installer) and was **closed 2026-06-11** via a
retroactive `/gsd-complete-milestone` (the `/release` flow tagged + shipped but did not run the GSD
close ritual). 7 phases (103, 105 + 106-110), 31 formal plans, 25 requirements satisfied.

Phase: — (none active)
Plan: — (none active)
Status: Milestone complete. Awaiting next milestone.
Last activity: 2026-06-11

### Next

`/gsd-new-milestone` — start the next cycle (questioning → research → requirements → roadmap). A fresh
`.planning/REQUIREMENTS.md` is created there (the v8.0.0 one was archived + removed at close).

**Deferred to the next milestone (user decision 2026-06-08 — scope, not gaps):** all of **Component B**
(JSA-01 anchor parallels seeding + JSA-02 corpus completion + JSA-03 torn-word completion + JWB-05
tear-side assist) and the **web Joins Lab UI** (desktop-first per the Codex productionize critique).

**Carried-forward candidates:** D-F12 (regular Search ~8s wall-clock, profile-first) · D-F18
(context-menu LOCAL detection) · `_show_vs_dialog`/JoinsDialog pick-machinery
physical deletion (soft-retired this cycle) · Phase 106/107 advisory code-review findings (WR-01/02) ·
JOINS-F1 relative-offset positional search (spike-gated) · JOINS-F2 Dicta/Sefaria citation-ID source ·
JOINS-F3 batch-export + persisted list re-import · JOINS-F4 auto-ranked finder (explicitly OUT). See
`.planning/milestones/v8.0.0-REQUIREMENTS.md` + `docs/OPEN_ISSUES.md` + `docs/FEATURE_IDEAS.md`.

**Build prerequisite (Joins Lab, still applies):** the Tantivy index must carry `line_starts` /
`line_ends` (older indexes raise a rebuild error at `genizah_core.py:8583`). Already satisfied on web +
most desktop users; degrade gracefully for stragglers.

## Deferred Items

Items acknowledged and deferred at **v8.0.0 milestone close on 2026-06-11** (`gsd-tools.cjs audit-open` reported **103 items** — the same historical accumulation as the v7.14/v7.15/v7.16 closes; none are v8.0.0 blockers):

| Category | Count | Notes |
|----------|-------|-------|
| Debug sessions | 41 | Mostly diagnosed-not-closed, predating v8. `local-search-freeze-2026-05-31` actually RESOLVED (OPEN_ISSUES D-F23); tracker entry stale. |
| "UAT gaps" | 2 | **False positives** — Phase 107 & 108 both `[passed]` with 0 pending scenarios (done; flag not flipped). |
| Quick tasks | 53 | Historical backlog (oldest 2026-02). `/gsd-cleanup` candidate. |
| Pending todos | 5 | server-side search+email; NLI MARC crawl; unified metadata text search; FIST gap-fill; migrate desktop corrections to shared service. |
| Unimplemented seeds | 2 | SEED-001 server-side IIIF cache (dormant, blocked on NLI TOS); SEED-003 opt-in OCR extension (dormant). |

Plus the v8.0.0 scope deferrals (NOT gaps — user decision 2026-06-08): all of **Component B** (JSA-01/02/03 + JWB-05) + the **web Joins Lab UI** → post-v8.0.0 milestone. Recommend a `/gsd-cleanup` pass on the historical backlog before the next milestone.

---

Items previously acknowledged and deferred at v7.16 milestone close on 2026-06-01 (`gsd-tools.cjs audit-open` reported 102 items; same historical accumulation as the v7.14/v7.15 closes — none are milestone blockers):

| Category | Count | Notes |
|----------|-------|-------|
| Debug sessions | 41 | Mostly diagnosed-not-closed entries predating v7.13. Includes `local-search-freeze-2026-05-31` — actually RESOLVED (OPEN_ISSUES D-F23); tracker entry stale. |
| UAT gaps | 1 | Phase 100 `100-HUMAN-UAT.md` — 0 pending scenarios (effectively done; flag not flipped). |
| Quick tasks | 53 | Historical backlog (oldest from 2026-02). Use `/gsd-cleanup` to triage between milestones. |
| Pending todos | 6 | Largest: server-side search with email notification; NLI MARC crawl; unified metadata text search; one-click scholarly citations (2026-06-01). |
| Unimplemented seeds | 2 | SEED-001 server-side IIIF image cache (dormant; blocked on NLI TOS); SEED-003 opt-in OCR extension for image-only / corrupt-text-layer PDFs (dormant). |

Carried forward to v8.0.0+ (logged in `docs/OPEN_ISSUES.md`): **D-F12** (regular Search ~8s wall-clock — profile-first), **D-F18** (context-menu LOCAL detection via `display`). **EXP-F3** (composition-report LOCAL export, gated on a LOCAL comp-search UI). D-F17 (LOCAL export shape) is now DELIVERED (Phases 103 + 105, folded into v8.0.0). Recommend a `/gsd-cleanup` pass on the historical backlog.

## Recently Closed Milestones

- **v8.0.0 Dicta Rebrand & Joins Lab** — shipped 2026-06-09 (both apps; tag `v8.0.0` @ `71e0912e`; GitHub Release with desktop installer), closed 2026-06-11 (retroactive GSD close — `/release` shipped + tagged but skipped the close ritual). 7 phases (103, 105 folded from v7.17 + 106-110 Joins Lab Component A), 31 formal plans, 25 requirements satisfied (BRAND 2 + LEXP 7 + EXPUX 4 + JWB 9 + COMP-LOC 2 + EXP-F3 1). 328 commits since `v7.16.0`; 266 files, +55,320/−785. Flagship "Dicta Genizah Search Pro" release: desktop rebrand (display-only) + LOCAL export + Joins Lab (human-in-the-loop join-hunting workbench, desktop) + Composition over the LOCAL corpus. Component B (JSA-01/02/03 + JWB-05) + web Joins Lab UI deferred to a post-v8.0.0 milestone. See `.planning/milestones/v8.0.0-ROADMAP.md` + `v8.0.0-REQUIREMENTS.md`.
- **v7.17 (folded, NOT separately closed)** — the rebrand + LOCAL export work shipped under the v7.17 phase numbers (103, 105) but, per the 2026-06-02 decision, is **folded into v8.0.0** rather than tagged/closed as its own milestone. Phases 103/105 retained as delivered. No "v7.17" release tag will exist.
- **v7.16 Hebrew PDF Text Quality** — shipped 2026-06-01 (v7.16.0, desktop); 1 formal phase (102, 5 plans) + no-phase de-space/UAT/freeze work; tag `v7.16.0` @ `ccb87c90`. LOCAL Hebrew PDF text-layer extraction rewrite, file-management actions for LOCAL hits, three search/startup freeze fixes. See `.planning/milestones/v7.16-ROADMAP.md`.
- **v7.15 My Library Visual** — shipped 2026-05-28; 3 phases (99, 100, 101); 7 plans; 6/6 PDFIMG-*. PDF page image rendering in ResultDialog + Browse, RTL/bidi reflow fixes, "Re-index All" recovery button.
- **v7.14 My Library — Local Document Search** — shipped 2026-05-24 (v7.14.0), closed 2026-05-27; 6 phases (95, 96, 97, 97.2/97.3 inserted, 98); 37 plans. Desktop local document search + Phase 98 NLI resilience.
- **v7.13 Research-Grade Downloads & PGP Filter** — shipped 2026-05-21 (v7.13.0), closed 2026-05-27; 2 phases (93, 94); 5 plans; 14/14 requirements.
- **v7.12 Multitenant Architecture (Path B)** — shipped 2026-05-18; 10 phases; 28 plans; 49/49 requirements.

## Accumulated Context

### Roadmap Evolution

- v8.0.0 opened (2026-06-02): folds the delivered v7.17 cycle (rebrand + LOCAL export, Phases 103/105) into the flagship v8.0.0 release and adds **Joins Lab** (Spike 002, FEASIBLE / ~M). Two independent components — A: Join Workbench hub (primary); B: search-support algorithms (secondary). Both apps. Human-in-the-loop; the auto-ranked v7/v8 finder is explicitly OUT. **Roadmap deferred** until after a user-led Genizah-scholar design-critique session. Phase numbering will continue from 105.
- Phases 103-105 (v7.17 cycle, now folded into v8.0.0): Phase 103 Search-results LOCAL export (LEXP-01/03–08, COMPLETE & verified); Phase 104 → DEFERRED to EXP-F3 (no LOCAL comp-search UI); Phase 105 Export UX Polish (EXPUX-01..04, implemented; EXPUX-01 + EXPUX-04 UI UAT approved 2026-06-11). Desktop rebrand → "Dicta Genizah Search Pro" delivered as pre-release polish (commit `6e0c312d` + follow-ups).
- Phase 102 (v7.16) EXECUTED & CLOSED (commit `494c0c49`) + POST-102 de-space quality pass as NO-PHASE edits (D-F13b/c/d). Existing LOCAL libraries need one manual "Re-index All" (`extraction_format_version` 2→3).
- Phase 110 Plan 02 (2026-06-08): composition engine `corpus_scope` landed. `search_composition_logic` + `lab_composition_search` accept `corpus_scope` as the LAST param (C3); fail-closed normalizer coerces unknown→`genizah` (C4); Genizah loop gated `!= 'local'`, LOCAL LAB loop/hook gated `!= 'genizah'`; per-run `local_lab_stale` + `corpus_scope` echoed on EVERY return dict incl. both early returns (A2 + Round-2 #4); stale≠no-index (M2). `SearchEngine._current_lab_weights_hash` now honors `_lab_weights_hash_override` (RF-4 — fixes all-scope LOCAL-LAB silent drop; Plan 03 injects the value). Composition merge stays score-interleaved — NO RRF (RF-2). Threads plumbed in gui_threads.py (default `'genizah'`). All 10 Wave-0 pure-engine tests green; 18 regression tests green; COMP-LOC-01/02 marked complete. Commits `80583a60`, `f44aa0ee`.

## Session Continuity

Last session: 2026-06-11 (v8.0.0 milestone close — retroactive)
Stopped at: v8.0.0 closed — MILESTONES.md entry written; ROADMAP + REQUIREMENTS archived to `.planning/milestones/v8.0.0-*`; PROJECT.md evolved; REQUIREMENTS.md removed (git rm); RETROSPECTIVE.md updated; `v8.0.0` tag already existed (created at `/release`).
Resume file: None
Next step: /gsd-new-milestone — start the next milestone cycle (a fresh `.planning/REQUIREMENTS.md` is created there). Consider a `/gsd-cleanup` pass on the 103-item historical backlog first.
