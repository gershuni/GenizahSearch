---
gsd_state_version: 1.0
milestone: v7.17
milestone_name: LOCAL Export Support
status: Defining requirements
stopped_at: v7.17 milestone started — defining requirements
last_updated: "2026-06-01T00:00:00.000Z"
progress:
  total_phases: 0
  completed_phases: 0
  total_plans: 0
  completed_plans: 0
  percent: 0
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-06-01)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** v7.17 LOCAL Export Support — defining requirements (closes D-F17)

## Current Position

Milestone: **v7.17 LOCAL Export Support — STARTED 2026-06-01** (closes D-F17). Adapt the desktop result-export flows so LOCAL ("My Library") hits export with local-meaningful columns (filename/folder/filepath/page/matched-text) instead of empty Genizah columns, across XLSX/CSV/TXT/DOCX on the Search-results (`export_results`) and Composition-report (`export_comp_report`) surfaces; mixed/ALL xlsx gains a dedicated "Local Documents" sheet; Genizah-only exports unchanged. Desktop-only (web has no LOCAL); JSON + Parallels out of scope.
Phase: none active yet — defining requirements, then roadmap (continues at **Phase 103**).
Status: Defining requirements.
Last activity: 2026-06-01 — Milestone v7.17 started (PROJECT.md updated, research skipped).
Next: requirements → roadmap → `/gsd-discuss-phase 103` (or `/gsd-plan-phase 103`).

## Deferred Items

Items acknowledged and deferred at v7.16 milestone close on 2026-06-01 (`gsd-tools.cjs audit-open` reported 102 items; same historical accumulation as the v7.14/v7.15 closes — none are v7.16-specific blockers):

| Category | Count | Notes |
|----------|-------|-------|
| Debug sessions | 41 | Mostly diagnosed-not-closed entries predating v7.13. Includes `local-search-freeze-2026-05-31` — actually RESOLVED this milestone (see OPEN_ISSUES D-F23); the tracker entry is just stale. |
| UAT gaps | 1 | Phase 100 `100-HUMAN-UAT.md` — 0 pending scenarios (effectively done; status flag not flipped). |
| Quick tasks | 53 | Historical backlog (oldest from 2026-02). Use `/gsd-cleanup` to triage between milestones. |
| Pending todos | 5 | Largest: server-side search with email notification; NLI MARC crawl; unified metadata text search. |
| Unimplemented seeds | 2 | SEED-001 server-side IIIF image cache (dormant; blocked on NLI TOS); SEED-003 opt-in OCR extension for image-only / corrupt-text-layer PDFs (dormant). |

Carried forward to the next milestone (logged in `docs/OPEN_ISSUES.md`): **D-F12** (regular Search ~constant 8s wall-clock — profile-first: instrument Tantivy candidate fetch → regex post-filter → enrichment → highlight build → return-to-UI; profile LOCAL-only / Genizah-unfiltered / Genizah-filtered; optimize the actual bottleneck — do NOT guess), **D-F17** (xlsx/Word/JSON export not adapted to LOCAL / ALL results), **D-F18** (context-menu LOCAL detection could normalize through `display`). Recommend a `/gsd-cleanup` pass on the historical backlog between milestones.

## Recently Closed Milestones

- **v7.16 Hebrew PDF Text Quality** — shipped 2026-06-01 (v7.16.0, desktop); 1 formal phase (102, 5 plans) + no-phase de-space/UAT/freeze work; tag `v7.16.0` @ `ccb87c90`. LOCAL Hebrew PDF text-layer extraction rewrite (rawdict per-glyph, RTL-gated, Otsu de-space, Mn nikud, `_ltr_damage_guard`), file-management actions for LOCAL hits, and three search/startup freeze fixes (778 MB history file, large-folder O(n²) startup, LAB-rebuild churn). See `.planning/milestones/v7.16-ROADMAP.md`.
- **v7.15 My Library Visual** — shipped 2026-05-28; 3 phases (99, 100, 101); 7 plans; 6/6 PDFIMG-* requirements. PDF page image rendering alongside LOCAL extracted text in ResultDialog + Browse, RTL/bidi reflow fixes, "Re-index All" recovery button. See `.planning/milestones/v7.15-ROADMAP.md`.
- **v7.14 My Library — Local Document Search** — shipped 2026-05-24 (v7.14.0), closed 2026-05-27; 6 phases (95, 96, 97, 97.2/97.3 inserted, 98); 37 plans. Desktop local document search + Phase 98 NLI resilience.
- **v7.13 Research-Grade Downloads & PGP Filter** — shipped 2026-05-21 (v7.13.0), closed 2026-05-27; 2 phases (93, 94); 5 plans; 14/14 requirements.
- **v7.12 Multitenant Architecture (Path B)** — shipped 2026-05-18; 10 phases; 28 plans; 49/49 requirements.

## Accumulated Context

### Roadmap Evolution

- Phase 102 added (2026-05-29): PDF Extraction Reorder — adopt Meiri glyph-level parser (closes D-F13 letter-spaced emphasis + D-F14 rawdict reorder). First piece of v7.16 work; appended to the roadmap after the shipped v7.15 (Phases 99-101) per user choice of a single inserted phase rather than a full new milestone. Not yet planned.
- Phase 102 RE-SCOPED (2026-05-29) after Spike 001 (`.planning/spikes/001-meiri-glyph-reorder-vs-current/`, verdict PARTIAL): now "LOCAL PDF Text-Layer Extraction Rewrite (RTL-gated reorder + letter-spacing de-collapse)". Spike findings: (1) Meiri's reorder helps Hebrew order/headers/brackets but NOT letter-spacing, and HURTS Latin → must be RTL-gated, no LTR regression; (2) the dominant text-layer bug is letter-spacing fragmentation (אוצר הגאונים 46%), fixable via rawdict per-line adaptive gap de-collapse (prototyped); (3) a LARGE share of the real library is image-only scans → OCR (D-F2) deferred as optional opt-in extension `SEED-003`; (4) new failure mode D-F16 corrupt text-layer encoding (Vilna Shabbat) → detect+flag in 102. Catalog F-A..F-G in spike README. Still not planned — next: discuss/plan Phase 102.
- Phase 102 EXECUTED & CLOSED (commit `494c0c49`, 5/5 plans, 18/18 verification). Then a POST-102 de-space quality pass landed as NO-PHASE edits (2026-05-31) because the first real-library UAT (Hillel) showed the per-line de-space still mis-handled several book classes: D-F13b rewrote the boundary metric from center-gap-vs-1.8×median to **edge-gap + per-line 1-D Otsu valley** (the 1.8×-median/0.45-floor first cut SHATTERED wide letters off justified words and MERGED tight-set books) and found the real production blocker was `_ltr_damage_guard` discarding the good de-space on RTL pages; D-F13c fixed a launch freeze (`startup_recovery` Pass B re-extracting a bulk pending backlog on the UI thread → `reextract_pending=False` defers it); D-F13d added a locally-gated zero-width space-glyph word boundary (N1 — tight headings/tables encode word-spaces as zero-width glyphs the gap test can't see; the "Otsu outlier" hypothesis was probe-DISPROVED) and an embedded-number bidi flip (N3 — `1977`→`7791`). N2 maqaf was already cured by D-F13b's Unicode-`Mn` mark test. Known residual: PDFs whose maqaf/space is absent from the text layer entirely (e.g. some `הקדמות-שילת` abbreviation-table cells `כתבי־יד`→`כתבייד`) are unrecoverable without OCR. All tracked in `docs/OPEN_ISSUES.md` (D-F13b/c/d) + `CHANGELOG.md [Unreleased]`; commits `733c02af`+`d4f61245`. Existing LOCAL libraries need one manual "Re-index All" (per-row `extraction_format_version` 2→3).

## Session Continuity

Last session: 2026-06-01 (`/gsd-new-milestone` — started v7.17 LOCAL Export Support)
Stopped at: milestone questioning complete (scope confirmed: LOCAL-appropriate columns; dedicated "Local Documents" sheet for mixed/ALL xlsx; XLSX/CSV/TXT/DOCX; Search-results + Composition-report surfaces; desktop-only; JSON + Parallels out of scope; research skipped). PROJECT.md + STATE.md updated. Defining requirements next.
Resume file: `.planning/PROJECT.md` (## Current Milestone: v7.17) + `docs/OPEN_ISSUES.md` (D-F17).
Next step: define `.planning/REQUIREMENTS.md` → spawn roadmapper for ROADMAP.md (continues at Phase 103) → `/gsd-discuss-phase 103`.
