---
phase: 119-candidates-compare-visual-similarity
reviewed: 2026-06-19T00:00:00Z
depth: standard
files_reviewed: 4
files_reviewed_list:
  - shared/joins_lab.py
  - web/components/candidate_grid.py
  - web/components/compare_modal.py
  - web/pages/joins_lab.py
findings:
  critical: 5
  warning: 4
  info: 2
  total: 11
status: issues_found
---

# Phase 119: Code Review Report

**Reviewed:** 2026-06-19
**Depth:** standard
**Files Reviewed:** 4
**Status:** issues_found

## Summary

Phase 119 adds the candidate surface (grid, table, filter dialog, pagination), Compare modal, and VS integration to the Joins Lab web page. The shared domain model (`shared/joins_lab.py`) and the Compare modal (`web/components/compare_modal.py`) are sound. The critical bugs are concentrated in the integration layer: `web/pages/joins_lab.py` passes the wrong signature to `open_filter_dialog` (breaks every filter open), instantiates `TriageState` with a spurious argument (crash on first search), and constructs a `Candidate` with a non-existent `fl_id` field (crash whenever Compare is opened). `web/components/candidate_grid.py` carries a process-global `_card_refs` dict that leaks card DOM references across all user sessions — a multitenant state-sharing bug.

---

## Critical Issues

### CR-01: `TriageState(_triage)` — wrong constructor call crashes every search render

**File:** `web/pages/joins_lab.py:641`
**Issue:** `TriageState.__init__(self)` accepts no arguments beyond `self`, but the call site passes the shared `_triage` dict as a positional argument. This raises `TypeError: __init__() takes 1 positional argument but 2 were given` on every call to `_render_candidates_surface()` — which fires after every successful search, filter change, page change, and enrichment completion. The page is essentially unusable after any search produces results.

`candidate_grid.py:166`:
```python
def __init__(self) -> None:
    self._data: dict = {}
```

`joins_lab.py:641`:
```python
triage_obj = TriageState(_triage)   # TypeError — takes no args
```

**Fix:**
```python
triage_obj = TriageState()
# Pre-populate from the existing _triage dict so verdicts survive re-renders:
for sid, verdict in _triage.items():
    try:
        triage_obj.set(sid, verdict)
    except ValueError:
        pass
_triage_state_ref['obj'] = triage_obj
```

---

### CR-02: `open_filter_dialog` API mismatch — every Filter button click crashes

**File:** `web/pages/joins_lab.py:672-679`
**Issue:** Three separate API mismatches between the `open_filter_dialog` signature in `candidate_grid.py` and every call site in `joins_lab.py`:

1. `candidate_grid.py:701-706` — signature is `(filter_state, enrichment, enrichment_ready, on_apply, on_reset)`. The call site at `joins_lab.py:672` passes `candidates=_all_candidates` and `anchor_sys_id=anchor_sid` which are **not accepted parameters** — `TypeError: open_filter_dialog() got unexpected keyword argument 'candidates'`.

2. The call site omits the required `on_reset` argument — `TypeError: open_filter_dialog() missing 1 required positional argument: 'on_reset'`.

3. The `on_apply` callback at `joins_lab.py:667-670` has signature `def _on_filter_apply(new_filter_state: dict) -> None:` — but `open_filter_dialog` calls `on_apply()` with **no arguments** (line 775). This would raise `TypeError: _on_filter_apply() missing 1 required positional argument: 'new_filter_state'`.

The filter dialog is completely non-functional; pressing the filter button raises an exception every time.

**Fix:**
```python
def _on_filter_apply() -> None:           # no arg — filter_state is mutated in place
    _current_page['value'] = 0
    _re_render_candidates_surface()

def _on_filter_reset() -> None:
    _current_page['value'] = 0
    _re_render_candidates_surface()

open_filter_dialog(
    filter_state=_filter_state,
    enrichment=_enrichment,
    enrichment_ready=_enrichment_ready['value'],
    on_apply=_on_filter_apply,
    on_reset=_on_filter_reset,
)
```

---

### CR-03: `Candidate(fl_id=...)` — non-existent field crashes Compare on every open

**File:** `web/pages/joins_lab.py:574-581`
**Issue:** `shared/joins_lab.Candidate` is a `frozen=True` dataclass with no `fl_id` field. The `_open_compare` function passes `fl_id=anchor_fl_id` as a keyword argument:

```python
anchor_cand = Candidate(
    sys_id=anchor_sid,
    page=anchor_page_num,
    uid=f'{anchor_sid}|anchor',
    fl_id=anchor_fl_id,       # TypeError: __init__() got unexpected keyword argument 'fl_id'
    volume_ie=anchor_vol,
    is_anchor_self=True,
)
```

This raises `TypeError` every time the Compare button is clicked. `fl_id` is an anchor-pane concept stored in `_anchor_state` but never included in the `Candidate` dataclass.

**Fix:** Remove the `fl_id` argument. The `AnchorViewer` inside `create_compare_modal` is constructed from `anchor_cand.sys_id` and `anchor_cand.page`; it resolves the folio independently. If the Compare modal's `AnchorViewer` needs `fl_id` in future, add the field to `Candidate` via a proper dataclass field.

```python
anchor_cand = Candidate(
    sys_id=anchor_sid,
    page=anchor_page_num,
    uid=f'{anchor_sid}|anchor',
    volume_ie=anchor_vol,
    is_anchor_self=True,
)
```

---

### CR-04: `_card_refs` is a module-level dict — leaks card DOM refs across all user sessions

**File:** `web/components/candidate_grid.py:483`
**Issue:** `_card_refs: dict = {}` is a module-level (process-global) dictionary. Every call to `_create_candidate_card` appends to it (`line 568: _card_refs.setdefault(cand.sys_id, []).append(card_el)`). In a multi-user NiceGUI web app, every user's card elements accumulate here indefinitely. Over time:

1. **Cross-user state contamination**: `_restyle_all(sys_id, triage)` iterates all refs for a sys_id including stale elements from other users' sessions. A triage verdict set by User A for sys_id X will attempt to restyle User B's card elements for that same sys_id — DOM mutations fire on the wrong client WebSocket connection.

2. **Memory leak**: refs are never removed. Card elements from navigated-away pages remain in the dict forever, growing without bound.

The Phase 87 multitenant invariant (`NEVER raw app.storage.user`) is satisfied, but the process-global dict creates the same cross-session contamination risk through a different mechanism.

**Fix:** Move `_card_refs` into a per-user context (e.g., a local dict passed as a closure into `_create_candidate_card`), or clear it at the start of each `create_candidate_grid` call and document clearly that the function is single-session. A simple fix that preserves the current API shape:

```python
# In create_candidate_grid, before rendering:
_card_refs.clear()   # guard: clears any stale refs from prior renders
```

The correct fix is to make `_card_refs` a per-render local dict passed through the call chain, since module-level state is fundamentally unsafe in a multi-user server.

---

### CR-05: `_merge_globals_web(b_ro, ...)` crashes when `compose()` returns `(None, None, None)`

**File:** `web/pages/joins_lab.py:1721-1725`
**Issue:** Inside `run_cross_side_core`, `compose(_other_sq_snap)` may return `(None, None, None)` when all rows in `_other_sq_snap` have whitespace-only terms (the SideQuery exists and has row objects, but every row's `.term.strip()` is empty — the gate at line 1704 only checks `other_side_sq.rows` for truthiness, not that any row has non-empty content). When `b_ro` is `None`, the next line:

```python
_merge_globals_web(b_ro, _global_opts_snap)   # b_ro is None → TypeError
```

raises `TypeError: 'NoneType' object does not support item assignment` inside the run.io_bound worker. The exception propagates as a generic `Exception` in the outer `except Exception` handler (line 1770), showing a misleading "Could not resolve the other side" warning.

**Fix:** Guard against the `None` return from `compose()` inside `run_cross_side_core`:

```python
def run_cross_side_core():
    b_query, b_ro, b_page_position = compose(_other_sq_snap)
    if b_query is None:
        # All rows had empty terms — treat as no other-side query
        from shared.joins_lab import MergeResult
        return MergeResult(candidates=tuple(_base_snapshot), note='b_query empty')
    _merge_globals_web(b_ro, _global_opts_snap)
    ...
```

---

## Warnings

### WR-01: `_on_triage_verdict` / `_on_compare_verdict` do not call `_restyle_all` — triage verdicts from Compare don't update card borders

**File:** `web/pages/joins_lab.py:534-558`
**Issue:** When a verdict is recorded via the Compare modal's verdict buttons, `_on_compare_verdict` updates `_triage` and calls `ts.set()` on the `TriageState` object. However, `_restyle_all` is never called. The triage border on the card in the grid behind the modal stays its old colour until the next full re-render. The grid card's own triage buttons call `_restyle_all` directly (via `_make_triage_handler` in `candidate_grid.py:655`), but the Compare path bypasses that.

**Fix:** Call `_restyle_all` after the `ts.set()` in both callbacks:
```python
def _on_compare_verdict(sys_id: str, verdict: str) -> None:
    _triage[sys_id] = verdict
    ts = _triage_state_ref.get('obj')
    if ts is not None:
        try:
            ts.set(sys_id, verdict)
        except Exception:
            pass
    from web.components.candidate_grid import _restyle_all
    _restyle_all(sys_id, _triage)
```

---

### WR-02: Triage button active-state is rendered at card-construction time and never updated on restyle

**File:** `web/components/candidate_grid.py:643-660`
**Issue:** The initial `current_verdict` is captured at card-render time and used to decide the button fill colour (active = filled background). `_restyle_all` updates only the card's border via `ref.style(...)`, it does NOT re-render or update the triage button fill colours. After a verdict changes (via Compare or any other path), the card border updates but the Y/?/N buttons still show the old active state. A user triaging through the Compare modal will see stale button colours in the grid.

There is no mechanism to retroactively update the button `style` attribute after initial render.

**Fix:** Either (a) re-render the entire card on verdict change (expensive), or (b) keep a ref per-button in the closure and update button style from `_restyle_all`/`_make_triage_handler`:

```python
# In _make_triage_handler — after _restyle_all:
# update button styles via stored refs
```

A minimal fix is to document this as accepted behaviour and note that the border update (triage colour) is the primary feedback mechanism — the button fill is a supplementary indicator only updated on next page render.

---

### WR-03: `compute_filtered` triage-state filter has unreachable branch — "Not triaged" logic is subtly broken

**File:** `web/components/candidate_grid.py:274-282`
**Issue:** The triage-state filter block:

```python
if triage_states and "All" not in triage_states:
    verdict = _get_verdict(c.sys_id)
    if "Not triaged" in triage_states and verdict is None:
        pass  # passes — not triaged matches "Not triaged"
    elif verdict is not None and verdict.capitalize() not in triage_states:
        continue
    elif verdict is None and "Not triaged" not in triage_states:
        continue
```

When the user selects BOTH "Not triaged" AND (say) "Yes" and a candidate has `verdict="yes"`:
- First branch: `"Not triaged" in triage_states and verdict is None` — False (verdict is not None)
- Second branch: `verdict is not None and verdict.capitalize() not in triage_states` — `"Yes" in triage_states` so this is False — does NOT continue
- Third branch: `verdict is None` — False

The candidate passes, which is correct. However for `verdict="no"`:
- Second branch: `"No" not in triage_states` — True → `continue` — correctly excluded.

The actual bug is when `verdict is None` (not triaged) and `"Not triaged" NOT in triage_states` but some other state IS selected: the first branch takes the `pass` path only when `"Not triaged" in triage_states` is True. So when `verdict is None` and `"Not triaged" not in triage_states`, control falls to the second branch which is `False` (verdict is None), then the third branch which correctly excludes. The logic is correct but convoluted — the `pass` branch on line 277 creates a dead-code-like early-exit pattern that makes the three branches mutually non-exclusive in a non-obvious way. A future edit could easily break it.

**Fix:** Simplify to a single readable predicate:
```python
if triage_states and "All" not in triage_states:
    verdict = _get_verdict(c.sys_id)
    verdict_key = verdict.capitalize() if verdict else "Not triaged"
    if verdict_key not in triage_states:
        continue
```

---

### WR-04: VS-only branch (F1) in `execute_joins_search` does not cancel any in-flight search before bumping generation

**File:** `web/pages/joins_lab.py:1489-1530`
**Issue:** The VS-only empty-builder branch at line 1489 bumps `_search_generation['value']` and sets `_is_running['value'] = True`, but does NOT call `_cancel_current_search()` first. If a standard search is in flight when the user clicks Run Search with an empty builder and VS ON, the in-flight task is not cancelled. The task reference in `_current_task['task']` stays pointing at the old task. When the old task finishes, the generation guard at the outer finally (line 1832) sees `my_gen == _search_generation['value']` is False (generation was bumped by F1), so it does NOT clear the button — but the F1 branch's own `finally` block (line 1527) checks `my_gen_f1 == _search_generation['value']`, which IS True, so it clears the button correctly. The in-flight task still runs to completion on a worker thread, wasting resources.

More importantly, the in-flight task's progress_cb will raise `InterruptedError` (because `_search_generation` was bumped), which is the cooperative cancel. So cooperative cancellation works, but the asyncio task is not explicitly cancelled.

**Fix:** Call `_cancel_current_search()` at the top of the F1 branch, and assign the VS fetch to `_current_task['task']` so a further click can cancel it:
```python
if _vs_on['value']:
    _cancel_current_search()
    # ... rest of F1 branch
```

---

## Info

### IN-01: `_create_candidate_card` triage dict branch is identical for both TriageState and dict

**File:** `web/components/candidate_grid.py:554-558`
**Issue:** The current-verdict lookup reads:
```python
if isinstance(triage, TriageState):
    current_verdict = triage.get(cand.sys_id)
else:
    current_verdict = triage.get(cand.sys_id)
```

Both branches call `.get(cand.sys_id)` with identical code. The `isinstance` check is dead code.

**Fix:** Collapse to a single line:
```python
current_verdict = triage.get(cand.sys_id) if triage else None
```

---

### IN-02: `detect_self_match` result is silently discarded — D-13 note should clarify intent

**File:** `web/pages/joins_lab.py:1804`
**Issue:** `detect_self_match(raw_results, anchor_sid_step9)` is called but the return value is not captured or used. The comment says `result is not surfaced (D-13)`. This is intentional per D-13, but any future developer may see the call with no assignment and remove it as dead code. The function has no side effects, so removing it would be a no-op — except it would silently break any future caller that expects the call to happen for telemetry or logging purposes.

**Fix:** Either capture and log the result for observability, or add a `# noqa: F841` / comment that makes the intent unmistakable:
```python
_self_matched = detect_self_match(raw_results, anchor_sid_step9)
# D-13: self-match is detected but not surfaced in Phase 119. Phase 120 exposes it as a UI badge.
```

---

_Reviewed: 2026-06-19_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
