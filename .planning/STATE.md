---
gsd_state_version: 1.0
milestone: v5.6.0
milestone_name: milestone
status: Milestone complete
stopped_at: Phase 102 context gathered
last_updated: "2026-05-29T16:52:52.821Z"
progress:
  total_phases: 2
  completed_phases: 2
  total_plans: 7
  completed_plans: 7
  percent: 100
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-05-28)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Phase 102 — pdf-extraction-reorder-adopt-meiri-glyph-level-parser-d-f13-

## Current Position

Phase: 102 — COMPLETE (executed & verified after this file last froze at "context gathered"; 5/5 plans, 18/18 verification — commit `494c0c49`). ROADMAP.md Phase 102 checkboxes are the source of truth.
Plan: n/a (102 closed)
Milestone: v7.15 My Library Visual — CLOSED 2026-05-28
Phases shipped: 99 (PDF Page Renderer), 100 (LOCAL PDF Image in ResultDialog + Browse), 101 (RTL fix + remnant cleanup + UAT follow-ons), 102 (PDF extraction reorder + letter-spacing de-collapse, closes D-F13/D-F14/D-F16)
Post-102 (no-phase quality edits, 2026-05-31): LOCAL PDF de-space rewrite + follow-ups — D-F13b (edge-gap + Unicode-Mn + per-line Otsu valley + `_ltr_damage_guard` RTL-trust), D-F13c (launch-freeze deferral), D-F13d (zero-width space-glyph word boundary N1 + number bidi N3; N2 maqaf via Mn). Tracked in `docs/OPEN_ISSUES.md` + `CHANGELOG.md [Unreleased]`; commits `733c02af` (code+tests+fixtures) + `d4f61245` (changelog). Deliberately NOT a GSD phase (edit+tests per user) — that is why there is no `103-*` phase folder.
Next: `/release` (folds the CHANGELOG `[Unreleased]` de-space section into a version bump + desktop installer/build/GitHub Release); or `/gsd-new-milestone` to start v7.16 (candidate: D-F12 search-latency investigation).

## Deferred Items

Items acknowledged and deferred at v7.15 milestone close on 2026-05-28 (`gsd-tools.cjs audit-open` reported 100 items):

| Category | Count | Notes |
|----------|-------|-------|
| Debug sessions | 40 | Same historical accumulation as v7.14 close; mostly diagnosed-not-closed entries predating v7.13. |
| UAT gaps | 1 | Phase 100 `100-HUMAN-UAT.md` — 0 pending scenarios (effectively done; status flag not flipped). |
| Quick tasks | 53 | Historical backlog (oldest from 2026-02). Use `/gsd-cleanup` to triage between milestones. |
| Pending todos | 5 | Largest: server-side search with email notification; NLI MARC crawl; unified metadata text search. |
| Unimplemented seeds | 1 | SEED-001 server-side IIIF image cache (dormant; blocked on NLI TOS). |

NEW deferred item from v7.15 UAT: D-F12 (regular Search ~constant 8s wall-clock investigation) logged in `docs/OPEN_ISSUES.md` for v7.16+ work. Recommended next-milestone approach: instrument hot path with 5-6 timing markers (Tantivy candidate fetch, regex post-filter, enrichment, highlight build, return-to-UI), profile 3 search shapes (LOCAL-only, Genizah unfiltered, Genizah filtered), then optimize the actual bottleneck — explicitly do NOT guess.

## Recently Closed Milestones

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

Last session: 2026-05-31 (post-102 de-space quality pass)
Stopped at: D-F13b/c/d de-space follow-ups complete, tested (134 local-PDF tests, ruff clean, check_docs green) and committed (`733c02af` code, `d4f61245` changelog); STATE/OPEN_ISSUES/CHANGELOG/CLAUDE docs reconciled.
Resume file: `docs/DESPACE_WIP_HANDOFF.md` (full de-space state snapshot) + `docs/OPEN_ISSUES.md` D-F13b/c/d.
Next step: Hillel to run "Re-index All" on launch to re-extract the LOCAL library under format-version 3. Then either `/release` (fold CHANGELOG `[Unreleased]` into a version bump) or `/gsd-new-milestone` for v7.16 (D-F12 search-latency investigation is the leading candidate). NOTE: v7.15.0 already shipped 2026-05-28 — the stale "version bump 7.15.0" next-step that was here is obsolete.
