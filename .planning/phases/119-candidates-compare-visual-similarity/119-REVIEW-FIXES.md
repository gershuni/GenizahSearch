---
phase: 119-candidates-compare-visual-similarity
date: 2026-06-19
type: review-fix-summary
fixes:
  criticals_resolved: 5
  warnings_resolved: 4
  infos_resolved: 2
  commits:
    - 922d9e61
    - 79b405da
    - 52a94c1d
---

# Phase 119: Code Review Fix Summary

Applied 2026-06-19 on top of committed Phase 119 work (do not revert).
Fixes are layered over the existing commits; no prior commits were modified.

---

## CR-01: TriageState constructor now accepts backing dict

**File:** `web/components/candidate_grid.py`
**Commit:** `922d9e61`

`TriageState.__init__` now accepts an optional `backing: dict | None = None`
parameter. When a backing dict is provided, `self._data` IS that dict (same
object, not a copy), making `_triage` (page-level dict) and `TriageState._data`
point at the same object.

In `web/pages/joins_lab.py`, `_render_candidates_surface` now calls
`TriageState(backing=_triage)` instead of the invalid `TriageState(_triage)`.

**Backward-compatible:** `TriageState()` with no args still works (existing tests
call it that way).

**Side-effects fixed:** WR-01 and WR-02 are resolved at the source — since both
`_triage[sid]` and `ts.set(sid, v)` now write to the same dict, there is no
drift between the page-level triage dict and the TriageState object.

---

## CR-02: open_filter_dialog call-site fixed

**File:** `web/pages/joins_lab.py` (in `_on_filter_open`)
**Commit:** `79b405da`

Three separate mismatches repaired:

1. Removed spurious `candidates=_all_candidates` kwarg (not in real signature).
2. Removed spurious `anchor_sys_id=anchor_sid` kwarg (not in real signature).
3. Added missing `on_reset=_on_filter_reset` argument (was required).
4. Fixed `_on_filter_apply` signature: changed from `(new_filter_state: dict)`
   to `()` — the dialog mutates `filter_state` in place before calling `on_apply()`.

A new `_on_filter_reset` handler was added that resets page to 0 and re-renders.

---

## CR-03: Candidate fl_id kwarg removed

**File:** `web/pages/joins_lab.py` (in `_open_compare`)
**Commit:** `79b405da`

Removed `fl_id=anchor_fl_id` from the anchor `Candidate(...)` construction.
`Candidate` is a `frozen=True` dataclass with no `fl_id` field — this caused
`TypeError` on every Compare button click.

The `AnchorViewer` inside `create_compare_modal` resolves the folio from
`anchor_cand.sys_id` and `anchor_cand.page` independently; `fl_id` is an
anchor-pane concept stored in `_anchor_state` and not needed on the `Candidate`.

---

## CR-04: _card_refs made per-render (no longer module-global)

**File:** `web/components/candidate_grid.py`
**Commit:** `922d9e61`

The module-level `_card_refs: dict = {}` dict (which accumulated card DOM refs
across ALL user sessions, never cleared, causing cross-user DOM mutation and
unbounded memory growth) was removed.

In its place:
- `_make_restyle_fn(card_refs: dict)` factory returns a render-scoped restyle
  callable bound to a caller-provided dict.
- `create_candidate_grid` creates a fresh `_render_card_refs: dict = {}` on each
  call and passes it down to every `_create_candidate_card` invocation via the
  new `card_refs=` and `restyle_fn=` keyword parameters.
- `create_candidate_table` similarly accepts `restyle_fn=` for its bulk-triage bar.

The `on_restyle_ready` callback parameter was added to `create_candidate_grid` so
the page can store the render-scoped restyle fn and use it for Compare verdicts.

---

## CR-05: b_query is None guard in run_cross_side_core

**File:** `web/pages/joins_lab.py` (inside `run_cross_side_core` closure)
**Commit:** `79b405da`

Added an early-return guard:
```python
if b_query is None:
    from shared.joins_lab import MergeResult
    return MergeResult(
        candidates=tuple(_base_snapshot),
        note='b_query empty — all other-side rows were whitespace',
    )
```

`compose()` returns `(None, None, None)` when all other-side rows are
whitespace-only. The previous code passed `None` to `_merge_globals_web` which
does `ro['flex_spacing'] = ...` → `TypeError: 'NoneType' object does not support
item assignment`. The new guard treats this as "no other-side query" and returns
the base candidates unchanged.

---

## WR-01 / WR-02: Compare verdicts now restyle grid cards

**Files:** `web/pages/joins_lab.py`, `web/components/candidate_grid.py`
**Commit:** `79b405da` (joins_lab), `922d9e61` (candidate_grid)

`_on_triage_verdict` and `_on_compare_verdict` in `joins_lab.py` now call the
render-scoped restyle function (stored in `_triage_state_ref['restyle']`) after
recording a verdict. The restyle fn is set via the new `on_restyle_ready` callback
on `create_candidate_grid` — it fires with the render-local restyle fn immediately
after the grid is built.

Since the CR-01 backing-dict fix makes `_triage` and `TriageState._data` the same
object, WR-02 (stale active-fill on buttons) is also partially resolved at the
data level; full button re-render on verdict change is deferred to Phase 120.

---

## WR-03: compute_filtered triage filter simplified

**File:** `web/components/candidate_grid.py`
**Commit:** `922d9e61`

The three-branch triage filter in `compute_filtered` was replaced with a single
readable predicate:
```python
verdict_key = verdict.capitalize() if verdict else "Not triaged"
if verdict_key not in triage_states:
    continue
```

The original three-branch form was logically equivalent but had a `pass` branch
that created a dead-code-like early-exit pattern, making future edits risky.

---

## WR-04: VS-only F1 branch cancels in-flight search

**File:** `web/pages/joins_lab.py`
**Commit:** `79b405da`

Added `_cancel_current_search()` at the top of the VS-only empty-builder branch
(F1). Previously the branch bumped `_search_generation` (cooperative cancel) but
did not explicitly cancel the `asyncio.Task`. The explicit cancel frees the
`run.io_bound` worker thread immediately instead of waiting for it to observe the
`InterruptedError` on the next progress callback.

---

## IN-01: Dead isinstance branch in current_verdict lookup collapsed

**File:** `web/components/candidate_grid.py`
**Commit:** `922d9e61`

```python
# Before: two identical branches
if isinstance(triage, TriageState):
    current_verdict = triage.get(cand.sys_id)
else:
    current_verdict = triage.get(cand.sys_id)

# After: single line
current_verdict = triage.get(cand.sys_id) if triage else None
```

---

## IN-02: detect_self_match result captured

**File:** `web/pages/joins_lab.py`
**Commit:** `79b405da`

`detect_self_match` result is now captured as `_self_matched` with a clarifying
comment attributing the intentional non-surfacing to D-13 and noting Phase 120
will expose it as a UI badge. This prevents future accidental removal as dead code.

---

## New test: tests/test_joins_lab_render_contract.py

**File:** `tests/test_joins_lab_render_contract.py`
**Commit:** `52a94c1d`

22 introspection tests using `inspect.signature` and `dataclasses.fields`.
No live NiceGUI runtime needed. Catches the entire CR-01/02/03/04/05 class
of API-signature drift bugs structurally:

- `TriageState` constructor contract (CR-01 class)
- `open_filter_dialog` caller kwargs vs. real params (CR-02 class)
- `Candidate` fields vs. anchor construction kwargs (CR-03 class)
- Module-level `_card_refs` absence + `_make_restyle_fn` existence (CR-04 class)
- `compose()` return contract — 3-tuple, not None (CR-05 class)

All 22 tests pass; ruff clean.

---

_Fixes applied: 2026-06-19_
_Author: Claude (gsd-execute-phase)_
