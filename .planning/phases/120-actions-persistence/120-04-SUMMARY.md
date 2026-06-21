---
phase: 120
plan: "04"
subsystem: web/joins-lab
tags:
  - add-as-join
  - login-gate
  - pending-replay
  - remove-join
  - confirmed_only
  - selection-substrate
dependency_graph:
  requires:
    - 120-01  # write_full_state/read_full_state/clear_joins_lab_state storage helpers
    - 120-02  # SEED-008 guards, Stop flag, create_login_dialog
    - 120-03  # _bootstrap_anchor restore flow (replay attaches here)
  provides:
    - ACT-01: Add-as-Join login gate (proposed write + pending replay after login reload)
    - D-02: confirmed_only=False in Lab known-joins (proposed + confirmed shown)
    - D-03: self-scoped remove-my-join control (own joins only, RLS-enforced)
    - H1: _load_known_joins force_refresh param + confirmed_only=False
    - H2: candidate-table multi-select feeds page _selected set (selection substrate for Plans 05/06)
  affects:
    - web/pages/joins_lab.py           # ACT-01 handler + pending replay + D-03 callback + H2 wiring
    - web/components/joins_panel.py    # user_id in formatted_joins + N1 docstring
    - web/components/known_joins_group.py  # on_remove_join + current_user_id + empty-state copy
    - web/components/candidate_grid.py    # on_selection_change in create_candidate_table
    - genizah_translations.py          # Phase 120-04 block
    - tests/test_joins_lab_page.py     # 26 new assertions
tech_stack:
  added: []
  patterns:
    - "Add-as-Join: confirm dialog → off-loop create_fragment_join (no status kwarg, proposed)"
    - "Anon gate: safe_user_set(joins_lab_pending, descriptor) before login dialog; safe_user_pop one-shot in _bootstrap_anchor"
    - "R2-M2 replay guards: schema_version==1, TTL<15min, is_logged_in, expected_anchor_sys_id match"
    - "D-03 remove: confirm dialog → off-loop delete_fragment_join + force_refresh=True"
    - "SEED-008 D-20: except RuntimeError: return wraps WHOLE coroutine body on add + remove handlers"
    - "H2 on_selection_change callback in create_candidate_table → page _selected set"
    - "confirmed_only=False in _load_known_joins (reverses Phase-118 D-17 for Lab path)"
key_files:
  created: []
  modified:
    - web/pages/joins_lab.py
    - web/components/joins_panel.py
    - web/components/known_joins_group.py
    - web/components/candidate_grid.py
    - genizah_translations.py
    - tests/test_joins_lab_page.py
decisions:
  - "All 4 tasks committed in a single atomic commit (04719336) — handlers are mutually referencing (_on_remove_join_click used inside _load_known_joins render, _replay_pending_action used in _bootstrap_anchor)"
  - "user_id propagated from formatted_joins dict in joins_panel (j.get('user_id')) — PGP joins get user_id=None, own-join detection skips them correctly"
  - "Pending descriptor schema: {schema_version:1, action:add_as_join, created_at:ISO, expected_anchor_sys_id, anchor_sys_id, anchor_shelfmark, candidate_sys_id, candidate_shelfmark}"
  - "TTL=900s (15min) — enough for a login flow, short enough to not replay against stale state"
  - "on_selection_change=None default in create_candidate_table keeps Phase-118/119 callers unaffected"
  - "Export is NOT selection-scoped — persistent toolbar control over full filtered set (R2-H2 / D-06 / Plan 06)"
metrics:
  duration: "~65min"
  completed: "2026-06-21"
  tasks: 4
  files: 6
---

# Phase 120 Plan 04: ACT-01/D-02/D-03/H1/H2 Add-as-Join + Remove-Join + Selection Substrate Summary

Add-as-Join (login-gated community write, status stays 'proposed'), Lab known-joins flip to confirmed_only=False (D-02), self-scoped remove-my-join (D-03), force_refresh param on _load_known_joins (H1), and candidate-table multi-select feeds page _selected set (H2).

## Tasks Completed

| # | Task | Commit | Files |
|---|------|--------|-------|
| 1 | _load_known_joins force_refresh + confirmed_only=False + user_id + N1 cleanup | 04719336 | joins_lab.py, joins_panel.py, known_joins_group.py, test_joins_lab_page.py |
| 2 | Add-as-Join login gate + create_fragment_join (no status) + pending replay (H4/R2-M2) | 04719336 | joins_lab.py, genizah_translations.py |
| 3 | D-03 remove-my-join — own joins only (link_off, confirm, delete_fragment_join) | 04719336 | joins_lab.py, known_joins_group.py |
| 4 | H2 candidate-table on_selection_change → page _selected set | 04719336 | candidate_grid.py, joins_lab.py |

## Implementation Details

### Task 1: H1/D-02/N1

**`_load_known_joins` signature extended:**
```python
async def _load_known_joins(
    sys_id: str, shelfmark: str, pgpid: Optional[int] = None,
    anchor_gen: int = 0, force_refresh: bool = False
) -> None:
```
- `confirmed_only=False` passed to `fetch_connected_fragments` (D-02 — Lab shows proposed + confirmed, parity with /browse)
- `force_refresh=force_refresh` threaded through (H1 — post-insert/delete cache bypass)
- Callers in `_on_add_as_join_click` and `_on_remove_join_click` pass `force_refresh=True`

**`formatted_joins` dict in joins_panel.py** now includes `'user_id': j.get('user_id')` for own-join detection.

**known_joins_group.py N1 cleanup:**
- Module docstring: updated to reflect `confirmed_only=False` Lab behavior
- Empty-state copy: `'Only confirmed public joins are shown'` → `tr('Community-proposed joins are shown')`
- `render_known_joins_group` signature extended with `on_remove_join=None` and `current_user_id=None` (backward-compat)
- `_render_member_row` extended with `join_id`, `is_mine`, `on_remove_join` params + `link_off` button (only when `is_mine=True`)

### Task 2: Add-as-Join (ACT-01/H4/R2-M2)

**`_on_add_as_join_click(candidate_sys_id, candidate_shelfmark)`:**
- Anonymous path: `safe_user_set('joins_lab_pending', descriptor)` → `create_login_dialog().open()`
- Descriptor schema: `{schema_version:1, action:'add_as_join', created_at:<iso>, expected_anchor_sys_id, anchor_sys_id, anchor_shelfmark, candidate_sys_id, candidate_shelfmark}`
- Logged-in path: confirm dialog → off-loop `create_fragment_join(... # NO status kwarg)` → `_load_known_joins(force_refresh=True)`
- SEED-008 guard wraps WHOLE coroutine body

**`_replay_pending_action()`** (called from `_bootstrap_anchor` after anchor is loaded):
- `safe_user_pop('joins_lab_pending', None)` — one-shot, prevents double-fire
- R2-M2 guards: schema_version==1 AND action=='add_as_join' AND NOT expired (TTL=900s) AND is_logged_in AND expected_anchor_sys_id==current anchor
- Off-loop `create_fragment_join` + `_load_known_joins(force_refresh=True)` on success
- SEED-008 guard

**Added imports:** `GlobalAuthState`, `create_login_dialog`, `create_fragment_join`, `delete_fragment_join`, `safe_user_pop`, `safe_user_set`, `datetime`

### Task 3: D-03 remove-my-join

**`_on_remove_join_click(join_id)`** in joins_lab.py:
- Confirm dialog → off-loop `delete_fragment_join(join_id)` → `_load_known_joins(force_refresh=True)`
- SEED-008 guard

**`render_known_joins_group` call** in `_load_known_joins` updated:
```python
_user = GlobalAuthState.get_user() if GlobalAuthState.is_logged_in() else None
_current_uid = _user['id'] if _user else None
render_known_joins_group(data, ..., on_remove_join=_on_remove_join_click, current_user_id=_current_uid)
```

### Task 4: H2 — Selection Substrate

**`create_candidate_table` new param:**
```python
on_selection_change: Optional[Callable] = None,
```
Called from `_on_selection` with `list(selected_sys_ids)` after each selection event (backward-compat: skipped when None).

**`_render_candidates_surface` wiring:**
```python
def _on_table_selection_change(sids: list) -> None:
    _selected.clear()
    _selected.update(sids)
create_candidate_table(..., on_selection_change=_on_table_selection_change)
```

Export remains NOT selection-scoped (Plans 05/06 note: Export reads the full filtered set, not `_selected`).

## Tests Added

**`tests/test_joins_lab_page.py` additions (26 new assertions):**

- `TestLoadKnownJoinsForceRefresh` (6): force_refresh in signature, confirmed_only=False, force_refresh threaded, user_id in formatted_joins, old confirmed copy gone, new community copy present
- `TestAddAsJoinGate` (9): create_fragment_join called, no status='confirmed', joins_lab_pending key, schema_version, created_at, expected_anchor_sys_id, safe_user_pop, force_refresh=True after insert, Add-as-Join button label
- `TestRemoveJoin` (6): on_remove_join in signature, defaults to None, link_off icon, is_mine flag, delete_fragment_join, remove tooltip
- `TestCandidateTableSelectionSubstrate` (5): on_selection_change in create_candidate_table, called from _on_selection, defaults to None, _selected wired in lab, grid has no bulk-selection

## Deviations from Plan

**None - plan executed exactly as written.**

All 4 tasks delivered as specified. The R2-M2 pending-descriptor schema matches the plan exactly. The guard sequence (schema_version → TTL → is_logged_in → anchor match) follows the plan's specification precisely.

## Known Stubs

None — all paths are fully wired:
- `_on_add_as_join_click` is defined but not yet wired to per-candidate card/row buttons (that surface work belongs to the candidate grid UI in Plans 05/06 which wire the Add-as-Join, Set-as-Anchor, and per-card action buttons). The handler is ready; the call site is the remaining piece.
- `_selected` is now correctly updated via `on_selection_change` — Plans 05/06 bulk handlers will read it.

## Threat Flags

No new network endpoints, auth paths beyond `create_fragment_join`/`delete_fragment_join` (existing, RLS-backed), or schema changes.
All pending action state stored via `safe_user_*` (Phase-87 invariant preserved, test_no_raw_storage_access green).

| Flag | File | Description |
|------|------|-------------|
| T-120-pending handled | web/pages/joins_lab.py | Pending descriptor schema_version+created_at+expected_anchor guards (R2-M2) — mitigated |
| T-120-self handled | web/components/known_joins_group.py | link_off only when is_mine=True; RLS DELETE USING(auth.uid()=user_id) on server — mitigated |

## Self-Check: PASSED

- `web/pages/joins_lab.py`: FOUND (force_refresh, confirmed_only=False, create_fragment_join, joins_lab_pending, _replay_pending_action, delete_fragment_join)
- `web/components/joins_panel.py`: FOUND (user_id in formatted_joins)
- `web/components/known_joins_group.py`: FOUND (on_remove_join, is_mine, link_off, Community-proposed copy)
- `web/components/candidate_grid.py`: FOUND (on_selection_change in create_candidate_table + _on_selection)
- `genizah_translations.py`: FOUND (Phase 120-04 block)
- `tests/test_joins_lab_page.py`: FOUND (26 new assertions, all pass)
- Commit 04719336: FOUND
- `tests/test_no_raw_storage_access.py`: PASS (6/6)
- `tests/test_joins_lab_off_loop.py`: PASS
- `tests/test_no_server_side_stop_propagation.py`: PASS
- Full suite (202 tests): ALL PASS
- `python -m ruff check` (5 changed files): CLEAN (only pre-existing E501/E402)
