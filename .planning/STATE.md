---
gsd_state_version: 1.0
milestone: v7.15
milestone_name: My Library Visual
status: awaiting-followup
stopped_at: Phase 101 plans complete + UAT-driven fixes shipped; verifier + roadmap-complete postponed pending PDF in-paragraph line-break work
last_updated: "2026-05-28T07:30:00.000Z"
last_activity: 2026-05-28 -- Phase 101 substantively done; user postponed verifier to next session
progress:
  total_phases: 3
  completed_phases: 2
  total_plans: 7
  completed_plans: 7
  percent: 100
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-05-27)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Phase 101 — local-pdf-text-extraction-rtl-fix-and-phase-100-remnant-clea (CODE COMPLETE; verifier + roadmap-complete postponed)

## Current Position

Phase: 101 (local-pdf-text-extraction-rtl-fix-and-phase-100-remnant-clea) — CODE COMPLETE, ceremony postponed
Plan: 2 of 2 (both have SUMMARY.md committed)
Status: Postponed by user — handling PDF in-paragraph line-break issue next session, THEN verify + complete
Last activity: 2026-05-28 -- Phase 101 substantively done; UAT-driven fixes shipped

Progress: [████████░░] v7.15 67% (Phase 101 code complete; verifier pending)

## Resume Next Session

**Resume work:** `/gsd-resume-work` will surface this. Steps for next session:

1. **Tackle the in-paragraph PDF line-break issue** (`docs/OPEN_ISSUES.md` 2026-05-28 row 222+):
   - Hillel's UAT screenshot of page 177: Hebrew paragraphs split into ~10 one-fragment lines because PyMuPDF preserves the source PDF's column-wrap line breaks.
   - DIFFERENT bug from the RTL word-order fix shipped today (S-1 directional-run is working — only granularity is wrong).
   - Fix path: paragraph reflow heuristics OR `get_text("dict")` block analysis. Detect line-end-without-sentence-final-punctuation + same-x-position next line → join with space.
   - Scope: probably a small follow-up phase (101.1 or 102) on top of Phase 101, OR a hotfix commit if the change is contained.

2. **Spawn gsd-verifier** for phase 101 goal achievement check (will likely return `passed` or `human_needed`).

3. **`gsd-sdk query phase.complete 101`** to mark complete in ROADMAP + STATE + REQUIREMENTS, commit completion.

4. **Decide on v7.15 release** — Phase 101 is the last v7.15 phase per the roadmap; ready to ship once verifier passes.

## Phase 101 Summary (for resume context)

- Wave 1 (plan 01) — RTL word-order fix in `shared/local_indexer.py::extract_pdf_pages` sort=True fallback via S-1 directional-run reversal helpers `_fix_sort_true_rtl_line` / `_fix_sort_true_rtl_page`. D-09 batch-order flake closed at conftest level. **D-04 auto-self-heal ROLLED BACK post-UAT** (commit `c771afd2`) — froze 12K-PDF library at launch; existing libraries need manual Reset + re-scan to pick up RTL fix.
- Wave 2 (plan 02) — WR-01 single-lookup collapse in `_open_local_browse_page` (AST-pinned), WR-02 `test_discard_scope_clears_pending`, OPEN_ISSUES.md updated.
- UAT-driven follow-on fixes:
  - `599db50e` build_lab_side_index 5-consecutive-failure bail (LAB log spam)
  - `21378680` build_lab_side_index pre-flight callback probe (10s freeze → microseconds)
  - `c1fef1f6` remove_folder batched commit + retry (os-error-5 storm)
  - `d5ed0e3a` i18n fix for remove-folder confirm dialog
- All tests green; ruff clean on tracked files; code review came back clean (0 critical, 0 warning, 3 info).
- Deferred to future phase: PDF in-paragraph line-break reflow, D-04 done-properly via LocalIndexerWorker.

## Phase Plan Estimates

| Phase | Name | Reqs | Plan slots (est) | Scope | CONTEXT.md status |
|-------|------|------|------------------|-------|-------------------|
| 99    | PDF Page Renderer | 3 (PDFIMG-01/02/06) | 1-2 | desktop only | not started |
| 100   | LOCAL PDF Image in ResultDialog + Browse | 3 (PDFIMG-03/04/05) | 2 | desktop only | not started |

**Total:** 6 requirements, ~3-4 plan slots (estimated), 2 phases. Both phases carry a UI hint (`yes`) — desktop PyQt6 surfaces.

Phase 100 depends on Phase 99 (the renderer/worker must exist before the two UI surfaces can be wired). Execute 99 → 100.

## Requirement Coverage

6/6 PDFIMG-* requirements mapped (100%, no orphans):

- Phase 99 (renderer/worker + graceful failure): PDFIMG-01, PDFIMG-02, PDFIMG-06
- Phase 100 (ResultDialog + Browse wiring + text-only gating): PDFIMG-03, PDFIMG-04, PDFIMG-05

## Key Constraints (from PROJECT.md)

- **Desktop-only** — web "My Library" does not exist; no web parity required.
- **Lazy/on-demand rendering** — never bulk-render the 10K×500-page corpus; render the currently viewed page only.
- **No on-disk image cache** — rendered page images are ephemeral; bounded LRU of open `fitz.Document` handles only.
- **Reuse the existing `ImageLoaderThread` QThread pattern** for the off-UI-thread worker.
- `sys_id` + `page_num` already flow to the UI; filepath via `get_filepath(sys_id)`; `fitz` page index = `page_num - 1`.
- Non-PDF LOCAL files (`.docx`/`.html`/`.xlsx`/`.csv`/`.txt`) stay text-only (extension-gated).
- Render failures degrade gracefully (placeholder + log) — no UI hang, no crash.
- **D-F2 (PDF OCR)** explicitly deferred — possible follow-up phase later.

## Deferred Items

Items acknowledged and deferred at the v7.13 + v7.14 milestone close on 2026-05-27 (`gsd-tools.cjs audit-open` reported 104 items):

| Category | Count | Notes |
|----------|-------|-------|
| Debug sessions | 40 | Historical accumulation predating v7.13 (mostly diagnosed-not-closed); spans many prior milestones. Includes 2 post-97.2 UAT brief/critique entries (work already shipped). |
| UAT gaps | 3 phases | Phase 95 (3 pending scenarios), Phase 96 96-06/96-08 (0 pending — effectively done). My Library shipped as v7.14.0; scenarios substantively exercised in live use. |
| Verification gaps | 2 | Phase 95 + Phase 97 `human_needed` flags. Substantively closed by the shipped v7.14.0 release + 97.x hotfix chain; status flag not flipped. |
| Quick tasks | 53 | Historical backlog (oldest from 2026-02). Use `/gsd-cleanup` to triage between milestones. |
| Pending todos | 5 | Largest: server-side search with email notification; NLI MARC crawl; unified metadata text search. |
| Unimplemented seeds | 1 | SEED-001 server-side IIIF image cache (dormant; blocked on NLI TOS). |

The v7.13/v7.14-specific items are all substantively closed by the shipped releases; only status-flag bookkeeping and the long historical backlog (predating v7.12) were deferred. Recommend a `/gsd-cleanup` pass before/during this milestone.

## Recently Closed Milestones

- **v7.14 My Library — Local Document Search** — shipped 2026-05-24 (v7.14.0), closed 2026-05-27; 6 phases (95, 96, 97, 97.2/97.3 inserted, 98); 37 plans. Desktop local document search + Phase 98 NLI resilience. See `.planning/milestones/v7.14-ROADMAP.md`.
- **v7.13 Research-Grade Downloads & PGP Filter** — shipped 2026-05-21 (v7.13.0), closed 2026-05-27; 2 phases (93, 94); 5 plans; 14/14 requirements. See `.planning/milestones/v7.13-ROADMAP.md` / `v7.13-REQUIREMENTS.md`.
- **v7.12 Multitenant Architecture (Path B)** — shipped 2026-05-18; 10 phases (87-92 + 92.1/92.2 inserted + 999.1/999.4 promoted); 28 plans; 49/49 requirements satisfied. See `.planning/milestones/v7.12-ROADMAP.md`.

> Note (2026-05-27): v7.13 and v7.14 both shipped as app releases earlier but the GSD close ritual was skipped at the time; both were reconciled together on 2026-05-27 (MILESTONES.md entries, archives, REQUIREMENTS.md deletion).

## Accumulated Context

### Roadmap Evolution

- v7.15 roadmap created (2026-05-27): 2 phases — Phase 99 (PDF Page Renderer: shared on-demand PyMuPDF renderer + off-thread worker + graceful failure handling; PDFIMG-01/02/06) and Phase 100 (LOCAL PDF Image in ResultDialog + Browse: wire renderer into both desktop surfaces, non-PDF files stay text-only; PDFIMG-03/04/05). Numbering continues from v7.14's last phase 98. Closes deferred item D-F3.
- Phase 101 added (2026-05-27): LOCAL PDF text extraction RTL fix + Phase 100 remnant cleanup — pre-release polish for v7.15. Scope: (1) RTL/bidi word-order reversal in LOCAL PDF text extraction (P3, OPEN_ISSUES.md, surfaced in Phase 100 UAT); (2) code-review WR-01 Browse double-lookup empty-pane; (3) WR-02 regression test for `_pending` after `discard_scope`; (4) test-isolation flake `test_txt_undecodable_marked_encoding_error`. Added after Phase 100 closed; user wants remnants cleared before release.

## Session Continuity

Last session: 2026-05-27T18:06:08.456Z
Stopped at: Phase 101 context gathered
Resume file: .planning/phases/101-local-pdf-text-extraction-rtl-fix-and-phase-100-remnant-clea/101-CONTEXT.md
Next step: `/gsd-plan-phase 99` — plan the shared PDF page renderer + off-thread worker. Phase 100 depends on Phase 99, so execute in order 99 → 100.
