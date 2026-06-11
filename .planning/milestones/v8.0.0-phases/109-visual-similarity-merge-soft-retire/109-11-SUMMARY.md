---
phase: 109-visual-similarity-merge-soft-retire
plan: "11"
subsystem: desktop/join_workbench
tags: [triage, folio-nav, vs-hint, gap-closure, G-10, G-11, G-13, JWB-12]
dependency_graph:
  requires: ["109-08", "109-09", "109-10"]
  provides: [idempotent-triage-toggle, merged-folio-triage-row, vs-hint-line]
  affects: [desktop/join_workbench.py, tests/test_join_workbench_vs.py]
tech_stack:
  added: []
  patterns: [idempotent-toggle, merged-layout-row, hint-QLabel]
key_files:
  modified:
    - desktop/join_workbench.py
    - tests/test_join_workbench_vs.py
decisions:
  - "G-13 fallback key: bare tr('No look-alikes match this search') is retained as a noqa-suppressed assignment so test_empty_intersection_status_message stays green and the literal remains visible to the i18n scanner"
  - "G-11 merged row uses name 'row' (not 'trow') to avoid confusion with the old standalone trow; layout order folio LEFT / stretch / triage RIGHT"
  - "G-10.1 toggle uses triage.pop(sys_id, None) guarded by current-value check — safe even when sys_id absent"
metrics:
  duration: 202s
  completed: 2026-06-08
  tasks_completed: 3
  tasks_total: 3
  files_modified: 2
---

# Phase 109 Plan 11: Triage toggle, folio merge, VS hint Summary

Idempotent triage toggle (G-10.1), merged folio+triage row (G-11.1), and eye-prefixed VS hint line + combined empty-intersection message (G-13) — all three editing `desktop/join_workbench.py` in a single plan.

## Tasks Completed

| Task | Description | Commit | Status |
|------|-------------|--------|--------|
| 1 | Idempotent triage toggle in mark() (G-10.1) | 10d709cb | Done |
| 2 | Merge folio-nav into triage row (G-11.1) | 3e4f53bd | Done |
| 3 | Eye-prefixed VS hint + combined empty message (G-13) | e1ab3b6a | Done |

## What Was Built

### Task 1 — Idempotent triage toggle (G-10.1)

`mark(sys_id, val)` in `JoinWorkbenchWindow` now checks whether `triage.get(sys_id) == val` before setting. If equal (same state re-clicked), it pops the entry; otherwise it sets it. This makes the Y/?/N buttons work as toggles: clicking the same state twice clears the triage for that fragment, clicking a different state sets it.

New test: `test_triage_second_click_clears` — headless stub test covering set / re-click clears / different-state sets.

### Task 2 — Merged folio+triage row (G-11.1)

The separate `trow` (Y/?/N) and `folio_row` (▶ p.N ◀) blocks in `CandidateCard.__init__` were merged into a single `row = QHBoxLayout()`. Layout order: folio nav LEFT (`_folio_prev_btn`, `_folio_lbl`, `_folio_next_btn`), `addStretch()`, triage RIGHT (Y/?/N buttons). This saves one row of vertical space per card.

All widget references (`self._folio_prev_btn`, `self._folio_lbl`, `self._folio_next_btn`, `self._card_page`) and their connected handlers (`_card_folio_prev`, `_card_folio_next`) are fully preserved. RTL glyphs unchanged (PREV=▶, NEXT=◀).

New test: `test_folio_and_triage_share_one_row` — static source scan confirming the old `folio_row = QHBoxLayout()` is gone and the combined-row marker comment is present.

### Task 3 — VS hint line + combined empty-intersection message (G-13)

Two additions to `JoinCandidatePane`:

1. **`self.vs_hint` QLabel** constructed in `__init__` just before `rv.addWidget(self.grid_scroll, 1)`. Text: `"👁 " + tr("Turn off Visual Similarity to see more results")`. Subtly styled (10px, italic, `#64748b`). Initially `setVisible(False)`.

2. **`apply_filters` status block** updated:
   - On empty intersection: uses the combined message `tr("No look-alikes match this search — turn off Visual Similarity to see all results")`. Sets `vs_hint.setVisible(False)` (combined message already carries the "turn off" advice).
   - On normal results: sets `vs_hint.setVisible(bool(_vs_on) and bool(filtered))` — hint shown only when toggle ON and there are results to show.
   - The bare `tr("No look-alikes match this search")` key is retained as a `noqa: F841` suppressed assignment so `test_empty_intersection_status_message` stays green.

New test: `test_vs_hint_and_combined_empty_strings_present` — static source scan confirming both tr() keys are present, `self.vs_hint` exists, and `vs_hint.setVisible` is wired.

## Verification

- `python -m pytest tests/test_join_workbench_vs.py tests/test_join_workbench_i18n.py tests/test_join_workbench_no_private.py tests/test_visual_similarity_dialog.py tests/test_join_workbench_construct.py -q` → **44 passed**
- `python -m ruff check desktop/join_workbench.py` → **clean**

## Deviations from Plan

None — plan executed exactly as written.

The only minor implementation choice: the fallback bare key `tr("No look-alikes match this search")` is retained as a `noqa: F841` assignment rather than inside a comment, which keeps the literal visible to both the i18n scanner and the existing test without producing a ruff F841 error. This matches the plan's guidance ("Simplest safe form … BOTH literals must appear in apply_filters").

## Known Stubs

None — all functionality is wired and behaviorally correct.

## Threat Flags

None — desktop UI layout refinement only; no new I/O, network, auth, or input-parsing surface introduced.

## Self-Check: PASSED

- `desktop/join_workbench.py` — present and modified (mark() idempotent toggle, merged row, vs_hint QLabel + apply_filters update)
- `tests/test_join_workbench_vs.py` — present, 3 new tests added
- Commits 10d709cb, 3e4f53bd, e1ab3b6a — all present in git log
- 44 tests pass, ruff clean
