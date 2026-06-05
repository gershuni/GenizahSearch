---
phase: 108-desktop-join-workbench-query-builders-candidates-compare
plan: "04"
subsystem: desktop-joins-lab
tags: [joins-lab, compare-dialog, candidate-pane, i18n, phase-gate, rr-2, rr-7, rr-12]
dependency_graph:
  requires:
    - phase: 108-03
      provides: "JoinCandidatePane.open_compare stub, _enqueue_image_for_pane with None-page guard, candidate_to_result_dict adapter, wb.mark/set_anchor/open_result_in_* public delegators"
    - phase: 107-join-workbench-shell
      provides: "_anchor_res raw DICT, filtered list (Candidates), triage dict, open_anchor_as_join extended"
  provides:
    - CompareDialog(QDialog) two-pane compare (Candidate-typed, None-page-guarded)
    - open_compare wired to CompareDialog (stub replaced)
    - 22 new tr() keys in genizah_translations.TRANSLATIONS (RR-4)
    - 108-VALIDATION.md frontmatter set nyquist_compliant + wave_0_complete (phase gate)
  affects:
    - desktop/join_workbench.py
    - genizah_translations.py
    - .planning/phases/108-desktop-join-workbench-query-builders-candidates-compare/108-VALIDATION.md
tech_stack:
  added: []
  patterns:
    - CompareDialog reads Candidate attributes directly (RR-2) — _fill_candidate uses c.shelfmark/.full_text/.highlight_pattern/.page/.via_other_side/.key/.sys_id
    - Anchor pane reads raw DICT via r_* helpers — _fill_anchor uses r_shelf/r_sid/r_text/page_of
    - c.page (Optional[int]) passed straight to _enqueue_image_for_pane — no page-1 arithmetic in CompareDialog (RR-12)
    - Per-page image via _enqueue_image_for_pane (_image_url_for_idx path), NOT get_thumbnail (RR-7)
    - All actions via Phase-107 public methods — open_result_in_browse/puzzle/list/as_join + mark + set_anchor (D-20, no _vs_ calls)
    - "other side matched" meta label when c.via_other_side (D-18/R-06) — no special-case page arithmetic needed
    - Anchor pane stays static on every paint() call (D-18)

key-files:
  created: []
  modified:
    - desktop/join_workbench.py (CompareDialog class + open_compare wired)
    - genizah_translations.py (Phase 108-04 TRANSLATIONS.update block, 22 new EN->HE entries)
    - .planning/phases/108-desktop-join-workbench-query-builders-candidates-compare/108-VALIDATION.md (nyquist_compliant + wave_0_complete flipped)

decisions:
  - "RR-2: _fill_anchor reads anchor DICT via r_* helpers; _fill_candidate reads Candidate attributes; two separate fill helpers prevent data-model confusion"
  - "RR-12: c.page (Optional[int]) passed straight through to _enqueue_image_for_pane which guards None — no page-1 arithmetic in CompareDialog avoids None arithmetic crash on VS-only candidates"
  - "D-18: anchor pane re-filled on every paint() call (static in effect); _fill_anchor called with self.wb._anchor_res on every step"
  - "Modeless (setModal(False)) child dialog with maximize hint — keeps workbench usable behind compare"
  - "Phase gate: two pre-existing known failures documented in OPEN_ISSUES.md are NOT regressions from Phase 108 — crash is the Windows access violation in _build_fl_id_index daemon threads, failure is the Hebrew About dialog disclosure string not in genizah_app.py source"

requirements-completed: [JWB-08, JWB-12]

duration: ~45min
completed: "2026-06-05"
---

# Phase 108 Plan 04: CompareDialog (JWB-08 side-by-side compare) + Phase Gate Summary

**Modeless two-pane CompareDialog with Candidate-typed reads (RR-2), None-page-safe image resolution (RR-12), per-page _enqueue_image_for_pane (RR-7), four actions + triage + re-anchor via public methods (D-20), and phase gate flipped.**

## Performance

- **Duration:** ~45 min
- **Started:** 2026-06-05T12:10:00Z
- **Completed:** 2026-06-05T12:35:22Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments

- `CompareDialog(QDialog)`: modeless 1320×870 two-pane compare dialog replaces the Plan-03 stub
- Candidate pane (_fill_candidate): reads Candidate attributes directly (c.shelfmark/.full_text/.highlight_pattern/.page/.via_other_side — RR-2); no r_sid(c)/r_text(c)/page_of(c) on a Candidate
- Anchor pane (_fill_anchor): reads raw result DICT via r_* helpers + page_of(res) (correct side of the RR-2 boundary)
- None-page guard: c.page passed STRAIGHT to _enqueue_image_for_pane — no page-1 arithmetic in the dialog (RR-12)
- Per-page matched image via _enqueue_image_for_pane / _image_url_for_idx path (NOT get_thumbnail — RR-7)
- "other side matched" label when c.via_other_side (D-18/R-06) with no special-case page arithmetic
- All four actions (Browse/Puzzle/List/Join) + Y/?/N triage + Re-anchor routed through Phase-107 public methods (D-20, zero _vs_ calls)
- Add-as-Join calls wb.open_result_as_join(candidate) which delegates to the extended public open_anchor_as_join (RR-3/D-17)
- 22 new tr() keys registered in genizah_translations.TRANSLATIONS (RR-4)
- Phase gate: all 316 Phase-108 requirement tests green; ruff clean on project source; 108-VALIDATION.md frontmatter flipped

## Task Commits

1. **Task 1: CompareDialog(QDialog) — two panes + wire open_compare** - `b57d17d0` (feat)
2. **Task 2: Phase gate — VALIDATION.md flags flipped** - `c0452913` (chore)

## Files Created/Modified

- `C:\Genizahsearch\desktop\join_workbench.py` — CompareDialog class added (~290 lines); JoinCandidatePane.open_compare stub replaced with CompareDialog(wb, idx).show()
- `C:\Genizahsearch\genizah_translations.py` — Phase 108-04 TRANSLATIONS.update block with 22 new EN->HE entries (< prev, next >, Y yes, ? maybe, N no, 📖 Browse, 🧩 Puzzle, 📋 Add to List, 🔗 Add as Join, ⚓ Re-anchor, other side matched, candidate, …, navigation accessible names)
- `C:\Genizahsearch\.planning\phases\108-desktop-join-workbench-query-builders-candidates-compare\108-VALIDATION.md` — nyquist_compliant: true, wave_0_complete: true

## Decisions Made

- RR-2 boundary: two separate fill helpers (_fill_anchor / _fill_candidate) with clearly different data-model contracts — prevents callers from accidentally calling r_*(c) on a Candidate
- RR-12 implementation: c.page (Optional[int]) is passed STRAIGHT to wb._enqueue_image_for_pane; the pump's `if page is None: page = 1` guard (already present from Plan-03) handles VS-only/None-page candidates; zero arithmetic on c.page inside CompareDialog
- Anchor pane re-fill on every paint(): simpler than checking whether anchor changed; ensures consistency when set_anchor fires
- Added "…" as a tr() placeholder for the image label (maps to Hebrew "…") — minor but needed for the i18n AST guard

## Deviations from Plan

None — plan executed exactly as written.

## Issues Encountered

**Phase gate — pre-existing test failures (NOT introduced by Phase 108):**

1. **Windows fatal exception: access violation** in `genizah_core._build_fl_id_index` daemon threads when `test_my_library_tab.py` + `test_my_library_tab_optout_cascade.py` run sequentially in the full suite. This is documented in `docs/OPEN_ISSUES.md` as a known pre-existing issue "Surfaced 2026-05-29 during Phase 102 full-suite regression gate." Individual test files pass; the crash is a thread-teardown interaction. Not introduced by Phase 108.

2. **test_about_dialog_contains_local_cache_disclosure_he**: checks for Hebrew "אינו מועלה" in `genizah_app.py`, but the Hebrew ABOUT_HTML is in `genizah_translations.py` (as `tr("ABOUT_HTML")`). The string is there in translations but the test looks in the wrong file. Pre-existing design issue, not introduced by Phase 108.

Both issues confirmed pre-existing by reverting to pre-Phase-108 code and observing identical failures.

## Threat Flags

No new security-relevant surface beyond what the plan's threat_model documents:
- T-108-08 (HTML injection in compare panes): `htmlify()` escapes via `html.escape()` — reused verbatim; no new HTML assembly path.
- T-108-09 (image load on UI thread): images go through the existing bounded 5-slot ImageLoaderThread pool (off-UI) via `_enqueue_image_for_pane` (RR-7/RR-12).

## Self-Check: PASSED

Files exist:
- `desktop/join_workbench.py` (CompareDialog class) — FOUND
- `genizah_translations.py` (Phase 108-04 block) — FOUND
- `108-VALIDATION.md` (nyquist_compliant: true, wave_0_complete: true) — FOUND

Commits exist:
- `b57d17d0` (Task 1: CompareDialog) — FOUND
- `c0452913` (Task 2: VALIDATION.md flags) — FOUND

Tests pass: 316 Phase-108 requirement tests green; 3342+ total tests green in targeted batches; ruff clean on project source.
