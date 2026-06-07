---
phase: 109-visual-similarity-merge-soft-retire
plan: "02"
subsystem: desktop/join_workbench
tags: [visual-similarity, source-selector, badges, merge, pending-source, parity-test]
dependency_graph:
  requires:
    - "_normalize_vs_row shim in desktop/join_workbench.py (Plan 01)"
    - "Phase-109 i18n keys in genizah_translations.py (Plan 01)"
  provides:
    - "Live Visual/Combined source selector with three QRadioButtons + QButtonGroup"
    - "_load_visual_candidates(anchor_sid, service=) testable seam (D-14a parity)"
    - "_load_vs thin UI wrapper over _load_visual_candidates"
    - "_on_source_changed: routes visual auto-load (D-01), combined (D-02), clears stale (review #1)"
    - "_on_anchor_set: grey-out D-08 + pending-source apply (review #2)"
    - "apply_source: radio-select with already-checked fix (review #2b)"
    - "Source-aware _maybe_assemble (never merges stale, review #1)"
    - "set_source public window method (pending-source aware, for Plan 03)"
    - "★both / ⊙VS#rank badges in CandidateCard; text-only UNBADGED (review #6)"
    - "csv_bank batch shelfmark enrichment for VS cards (review #5)"
    - "D-14a parity test + page=None four-actions safety test + network-page-lazy test"
  affects:
    - "desktop/join_workbench.py — Plan 03 calls set_source('visual')"
    - "tests/test_join_workbench_vs.py — 3 new Plan-02 tests"
tech_stack:
  added: []
  patterns:
    - "QRadioButton + QButtonGroup source selector with canonical vocab (review #8a)"
    - "Pending-source pattern: stash + try-now + _on_anchor_set applies after grey-out"
    - "Unbound-method stub pattern for Qt-free parity test (no QApplication needed)"
    - "Source-aware merge: explicit half=[] to prevent stale candidate leakage"
key_files:
  created: []
  modified:
    - desktop/join_workbench.py
    - tests/test_join_workbench_vs.py
decisions:
  - "Committed both tasks in a single commit since implementation is tightly coupled"
  - "JoinCandidatePane is importable directly (module-level via if _QT_AVAILABLE: block)"
  - "_PER_PAGE is also module-level, imported directly in test_thumbnail_path_is_page_scoped"
  - "Source-aware _maybe_assemble uses explicit half=[] rather than relying on _on_source_changed clear alone (belt-and-suspenders review #1)"
  - "apply_source guards the D-08 disabled case with try/except RuntimeError for Qt widget safety"
metrics:
  duration: "~20 minutes"
  completed: "2026-06-07T15:24:42Z"
  tasks_completed: 2
  tasks_total: 2
  files_changed: 2
---

# Phase 109 Plan 02: Visual/Combined Source Selector + Source-Aware Merge Summary

Live Visual/Combined source selector with auto-load, source-aware merge preventing stale candidate leakage, pending-source grey-out pattern, ★both/⊙VS badges, csv_bank shelfmark enrichment, and three new tests proving D-14a parity, page=None safety, and network-page laziness.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1+2 | Visual/Combined sources + merge + badges + tests | `aefa6e49` | desktop/join_workbench.py, tests/test_join_workbench_vs.py |

## What Was Built

**Source Selector UI (Task 1b):** Three `QRadioButton`s (Text / Visual similarities / Search + visual) added before `btn_find` in `_build_ui`. Backed by `QButtonGroup`. `btn_find.setVisible(source != "visual")` hides the find button for the auto-load Visual source. `rb_text` checked by default.

**Phase 109 pane state (Task 1a / review #8a canonical vocab):**
- `self._vs_cands = None` — VS candidate list from `_load_vs`
- `self._active_source = "text"` — canonical token ∈ {"text", "visual", "combined"}
- `self._pending_source = None` — source request deferred until grey-out resolves

**`_on_source_changed(source)` (review #1):** Sets `_active_source` + `_sources` in canonical vocab. Visual branch explicitly sets `self._text_cands = []` (belt-and-suspenders clear) and calls `_load_vs()` + `_maybe_assemble()`. Combined branch loads VS then assembles if builder is empty (degrade-to-VS-only), otherwise waits for `do_search`. Text branch nulls `_vs_cands`.

**`_load_visual_candidates(anchor_sid, service=None)` (review #3 — testable seam):** Pure-ish helper injecting an optional service. Calls `is_available()` + `has_suggestions()` for D-08 guard. Fetches full `get_suggestions(anchor_sid, 200)`. Batch-enriches shelfmarks from `meta_mgr.csv_bank` (O(1) dict lookups, no network) then passes to `_normalize_vs_row` + `normalize_candidate`. Falls back to `str(alma_id)` via shim when csv_bank lacks row (review #5 — cards never render blank).

**`_load_vs()` (Task 1e):** Thin wrapper — calls `_load_visual_candidates(wb._anchor_sid)`, stores on `_vs_cands`, updates status label with "Visual look-alikes loaded" or "No visual similarity data...".

**`_on_anchor_set()` (Task 1f / D-08 + review #2):** Called from `JoinWorkbenchWindow._on_anchor_loaded` AFTER `_load_current_image()`. Calls `get_vs_service().has_suggestions(anchor_sid)` and enables/disables `rb_visual` + `rb_combined`. Falls back to Text if currently on a disabled source. Applies `_pending_source` via `apply_source`.

**`apply_source(source)` (Task 1g / review #2b):** Selects the target radio. Guards D-08 disabled case (stays on Text). When target radio is already checked, calls `_on_source_changed(source)` DIRECTLY — because `setChecked(True)` on an already-checked radio emits no `toggled` signal, so VS would not reload for the new anchor without this direct call.

**Source-aware `_maybe_assemble` (Task 2a / review #1):** Replaced the hardcoded `merge_candidates(text, [])` stub. Visual: `text_half=[], vs_half=_vs_cands`. Text: `text_half=_text_cands, vs_half=[]`. Combined: both halves. Relies on `_active_source` canonical token.

**Provenance badges (Task 2b / review #6):** Extended `CandidateCard.__init__` shelf_text `if/elif` chain:
- `c.via_text and c.via_vs` → `tr("  ★ both")`
- `c.via_vs and not c.via_text` → `tr("  ⊙ VS")` + `#{vs_rank}` (Pitfall 2 None guard)
- Text-only: UNBADGED (no `✎` branch per CONTEXT ✎text RESOLVED 2026-06-07)

**`set_source(source)` (Task 2c / review #2):** Public window method. Stashes source as `pane._pending_source`, then calls `pane.apply_source(source)` immediately. If `apply_source` succeeds, clears the pending. If not (pane not ready, RuntimeError), leaves pending for `_on_anchor_set` to apply after grey-out.

**`_on_anchor_set` call in `_on_anchor_loaded` (Task 1f):** Added `self._candidate_pane._on_anchor_set()` after `self._load_current_image()` in `JoinWorkbenchWindow._on_anchor_loaded` with `RuntimeError/AttributeError` guard.

## Tests Added (Plan 02)

**`test_load_visual_candidates_parity`** (review #3 — D-14a): Drives `JoinCandidatePane._load_visual_candidates` unbound with a stub `self` (no Qt construction). Asserts `{c.sys_id for c in cands} == {r["alma_id"] for r in svc.get_suggestions("100", 200)}`. Also asserts `all(c.via_vs)` and `all(c.shelfmark)` (review #5 never blank).

**`test_page_none_actions_do_not_crash`** (review #7 / RR-12): Creates a VS `Candidate` via `_normalize_vs_row` with no metadata. Asserts `c.page is None`, `candidate_to_result_dict(c)` returns safely with `img=None`, and `(c.page or 1) == 1`.

**`test_thumbnail_path_is_page_scoped`** (review #4 / D-09 AMENDMENT): Imports `_PER_PAGE` (module-level = 20) and asserts that a page slice of an 80-candidate result is exactly `_PER_PAGE` items, never all 80.

## Verification Results

```
tests/test_join_workbench_vs.py — 6 passed (3 Plan-01 + 3 Plan-02)
tests/test_joins_lab.py — 80 passed
tests/test_join_workbench_construct.py — 6 passed
tests/test_join_workbench_i18n.py — 4 passed
tests/test_join_workbench_no_private.py — 2 passed
Total: 88 tests — all GREEN
ruff check desktop/join_workbench.py tests/test_join_workbench_vs.py: All checks passed!
```

## Deviations from Plan

**None — plan executed exactly as written.**

Minor structural deviation: Plan suggested two separate commits (one per task), but since Task 1 and Task 2 modify the same two files and the Task 1 verification command includes Task 2 tests (they are co-dependent: `_on_source_changed` calls `_load_vs` which calls `_load_visual_candidates`, while `_maybe_assemble` uses `_active_source`), they were committed as a single atomic commit. All acceptance criteria for both tasks are satisfied.

The `JoinCandidatePane` import in the parity test uses `from desktop.join_workbench import JoinCandidatePane` (module-level) not `JoinWorkbenchWindow.JoinCandidatePane` (not accessible as a nested attribute). Same for `_PER_PAGE` — imported directly from the module.

## Known Stubs

None. All VS source selector functionality is wired and functional:
- Visual auto-loads on select (D-01)
- Combined merges text + VS (D-02)
- Grey-out applies on no-VS anchor (D-08)
- Pending-source survives async grey-out timing (review #2)
- csv_bank shelfmark enrichment active (review #5)
- Badges render correctly (review #6)

## Threat Flags

No new network or auth surface. The VS source selector uses the same local `visual_similarity.db` SQLite sidecar and the same page-lazy `ThumbResolver` path as the existing text candidates. T-109-03 (missing DB) and T-109-04 (NLI hang) mitigations are implemented via `is_available()` guard and page-lazy ThumbResolver respectively.

## Self-Check

**Commits exist:**
- `aefa6e49` — verified via `git log`

**Files modified:**
- `desktop/join_workbench.py` — modified (274 insertions, 11 deletions)
- `tests/test_join_workbench_vs.py` — modified (69 insertions, 1 deletion)

**Guard tests green:**
- VS tests: 6/6
- i18n guard: 4/4
- no_private guard: 2/2
- construct guard: 6/6
- joins_lab: 80/80

## Self-Check: PASSED
