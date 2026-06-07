---
phase: 109-visual-similarity-merge-soft-retire
plan: "05"
subsystem: desktop/join_workbench
tags: [visual-similarity, toggle, intersection, badge, re-anchor, page-lazy, tdd]
dependency_graph:
  requires:
    - phase: 109-04
      provides: "Pre-seeded tr() keys for toggle tooltip + empty-state in genizah_translations.py"
    - phase: 109-02
      provides: "_load_visual_candidates testable seam (D-14a parity), _normalize_vs_row shim"
  provides:
    - "Single btn_vs_toggle button (checkable) replaces 3-radio source selector (G-04)"
    - "_vs_on bool + _vs_loaded_sid + _pending_vs boolean state machine"
    - "_ensure_vs_loaded_for_anchor: memoised per wb._anchor_sid (HIGH-1)"
    - "apply_source returns bool; queries has_suggestions not widget enabled-state (HIGH-3)"
    - "set_source: clears _pending_vs ONLY when apply_source returns True (BLOCKER A)"
    - "_maybe_assemble: toggle ON+empty=pure-VS; ON+term=intersection; OFF=text-with-badges"
    - "_empty_intersection flag + tr('No look-alikes match this search') in apply_filters (MEDIUM-1)"
    - "set_anchor: invalidates pane DATA + card widgets via render_results() (BLOCKER B / NEW-HIGH)"
    - "CandidateCard.load_vs_text(): page-lazy browse-text fetch for via_vs rows (G-02)"
  affects:
    - "desktop/join_workbench.py — Plan 06 pick-affordance will call set_source('visual')"
    - "tests/test_join_workbench_vs.py — 10 new Plan-05 tests"
tech_stack:
  added: []
  patterns:
    - "Boolean toggle state machine: _vs_on + _vs_loaded_sid + _pending_vs (replaces _active_source/_pending_source)"
    - "Memoised per-anchor VS load: _ensure_vs_loaded_for_anchor keyed to wb._anchor_sid"
    - "apply_source returns bool; set_source guards pending-clear on return value (BLOCKER A)"
    - "Intersection filter: [c for c in merge_candidates(text, vs) if c.via_text and c.via_vs]"
    - "Empty-state flag _empty_intersection -> MEDIUM-1 status message in apply_filters"
    - "Re-anchor invalidation: set_anchor zeroes pane data + calls render_results() (BLOCKER B)"
    - "Page-lazy VS card text: CandidateCard.load_vs_text() reuses _PageTextWorker(page=1) (G-02)"
key_files:
  created: []
  modified:
    - desktop/join_workbench.py
    - tests/test_join_workbench_vs.py
key_decisions:
  - "Test helper _make_candidate uses dataclasses.replace(c, via_text=True) rather than dedup_candidates (which requires anchor_sid; returns tuple)"
  - "load_vs_text lives on CandidateCard (not JoinCandidatePane) so the page-lazy guard can access self.c.via_vs + self.c.full_text directly"
  - "set_anchor invalidation placed BEFORE _start_anchor_load so pane.results == [] when _render_grid_page first runs on the new anchor"
  - "_sources attribute (legacy) removed from __init__ and do_search since no code reads it in the new toggle model"
  - "_pending_source string removed entirely and replaced by _pending_vs bool|None; comment in set_source updated to avoid any residual reference"
requirements-completed: [JWB-12]
duration: ~12min
completed: "2026-06-07T20:57:00Z"
---

# Phase 109 Plan 05: VS Toggle + Intersection Assemble + Page-Lazy Card Text Summary

**Single 'Visual Similarity' toggle replaces 3-radio model; intersection-when-term, badge-always, BLOCKER A/B closed, VS cards render transcription text page-lazily.**

## Performance

- **Duration:** ~12 min
- **Started:** 2026-06-07T20:48:00Z
- **Completed:** 2026-06-07T20:57:00Z
- **Tasks:** 3 (all TDD: RED commit → GREEN commit)
- **Files modified:** 2

## Accomplishments

- Replaced the 3-radio model (Text/Visual/Combined) with a single checkable `btn_vs_toggle` driving a boolean `_vs_on` state machine (G-04)
- Closed BLOCKER A: `set_source('visual')` now stashes `_pending_vs` and clears it ONLY when `apply_source()` returns True (HIGH-3); a reused no-VS window no longer swallows the request
- Closed BLOCKER B / NEW-HIGH: `set_anchor` invalidates pane `_text_cands`/`_vs_cands`/`_vs_loaded_sid`/`results`/`wb.filtered` AND calls `pane.render_results()` immediately to clear stale card widgets
- `_ensure_vs_loaded_for_anchor` memoises the current anchor's VS set per `wb._anchor_sid` so OFF-mode ★both badges are always computed against the CURRENT anchor (HIGH-1/HIGH-2)
- `_maybe_assemble` rewritten for toggle: ON+empty=pure-VS; ON+term=intersection (★both only); OFF=text-with-badges, VS-only excluded
- MEDIUM-1: `apply_filters` sets `tr("No look-alikes match this search")` when `_empty_intersection` is True and no candidates pass filters (G-03 anti-spinner)
- `CandidateCard.load_vs_text()` added: page-lazy browse-text fetch (page=1) via `_PageTextWorker`; called from `_render_grid_page` for current-page cards only (D-09 scope); done handler always sets `snip.setHtml` — never stuck on "loading…" (G-02)

## Task Commits

| # | Phase | Commit | Type | Description |
|---|-------|--------|------|-------------|
| 1 | RED | 9048bf2b | test | Task 1: toggle/ensure-vs-load/set_source-pending tests |
| 1 | GREEN | f407c97e | feat | Task 1: VS toggle + boolean state machine + guarded set_source |
| 2 | RED | 684a70c1 | test | Task 2: intersection/empty-state/re-anchor tests |
| 2 | GREEN | 2ba2dddf | feat | Task 2: intersection assemble + empty-state + re-anchor invalidation |
| 3 | RED | 77de838b | test | Task 3: VS card text fetch test (G-02) |
| 3 | GREEN | 0e765dc5 | feat | Task 3: VS card page-lazy text fetch (G-02) |

## Files Created/Modified

- `desktop/join_workbench.py` — Toggle state machine, _ensure_vs_loaded_for_anchor, apply_source returns bool, set_source guards pending-clear, _maybe_assemble intersection logic, _empty_intersection flag, apply_filters MEDIUM-1 branch, set_anchor BLOCKER B invalidation, CandidateCard.load_vs_text, _render_grid_page card.load_vs_text() call
- `tests/test_join_workbench_vs.py` — 10 new Plan-05 tests (3 Task-1 + 6 Task-2 + 1 Task-3)

## Decisions Made

- `_make_candidate` test helper uses `dataclasses.replace(c, via_text=True)` rather than `dedup_candidates` (which requires `anchor_sid` and returns a tuple)
- `load_vs_text` lives on CandidateCard (not JoinCandidatePane) so it can access `self.c.via_vs` and `self.c.full_text` directly without pane indirection
- `set_anchor` invalidation placed BEFORE `_start_anchor_load` so `wb.filtered == []` when `_render_grid_page` first runs on the new anchor
- `_sources` legacy attribute removed (was set but never read in the toggle model)
- The `_pending_source` string reference in the `set_source` docstring comment was also removed to satisfy the acceptance criterion (zero grep matches)

## Deviations from Plan

None — plan executed exactly as written. The concrete code from the plan was applied faithfully (BLOCKER A rewrite at step 8, BLOCKER B rewrite at Task 2 step 4).

Minor test-helper fix: `dedup_candidates` signature requires `anchor_sid` (positional) and returns a tuple; the test helper used `dataclasses.replace(c, via_text=True)` directly instead — equivalent result, accepted per TDD guidance ("if a Qt-free behavioral seam is impractical, choose the mechanism you test").

## Known Stubs

None. All functionality is wired:
- Toggle ON triggers real `_ensure_vs_loaded_for_anchor` + `_maybe_assemble`
- Intersection filter is the live `merge_candidates` contract
- `_empty_intersection` flag is checked in `apply_filters` on every render
- `load_vs_text` fires `_PageTextWorker` on real `searcher.get_browse_page`
- `set_anchor` invalidation is synchronous and calls the real `render_results()` path

## Threat Flags

No new network or auth surface. Threat register items T-109-03 / T-109-04 / T-109-11 / T-109-14 / T-109-15 from the plan are all mitigated:

| Flag | File | Description |
|------|------|-------------|
| T-109-11 resolved | desktop/join_workbench.py | G-03 comment + MEDIUM-1 empty-state message in apply_filters confirms empty intersection never spins |
| T-109-14 resolved | desktop/join_workbench.py | set_anchor invalidates data AND card widgets; _vs_loaded_sid keyed per anchor |
| T-109-15 resolved | desktop/join_workbench.py | _pending_vs cleared only when apply_source returns True |

## Self-Check: PASSED

- `desktop/join_workbench.py` modified: FOUND
- `tests/test_join_workbench_vs.py` modified: FOUND
- Task commits confirmed: 9048bf2b, f407c97e, 684a70c1, 2ba2dddf, 77de838b, 0e765dc5
- `python -m pytest tests/test_join_workbench_vs.py tests/test_join_workbench_i18n.py tests/test_join_workbench_no_private.py tests/test_join_workbench_construct.py tests/test_joins_lab.py -q` → 98 passed
- `python -m ruff check desktop/join_workbench.py` → All checks passed
- D-14a parity: `test_load_visual_candidates_parity` PASSED
- `rb_combined` absent: CONFIRMED
- `btn_vs_toggle` present: CONFIRMED
- `_pending_source` absent: CONFIRMED
- `_active_source` absent: CONFIRMED
- `tr("No look-alikes match this search")` present: CONFIRMED
- `def load_vs_text(self):` present: CONFIRMED
- `card.load_vs_text()` in `_render_grid_page`: CONFIRMED
