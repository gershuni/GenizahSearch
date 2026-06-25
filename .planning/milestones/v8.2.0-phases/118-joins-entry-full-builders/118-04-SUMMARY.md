---
phase: 118-joins-entry-full-builders
plan: "04"
subsystem: web/joins-lab
tags: [bld-02, bld-03, bld-04, anc-04, off-loop, security, multitenant, wave-2]
dependency_graph:
  requires:
    - 118-01 (RED test stubs — test_merge_globals_web.py, test_other_side_page_contract.py)
    - 118-02 (fetch_connected_fragments confirmed_only + known_joins_group renderer)
    - 118-03 (create_joins_builder factory widget)
  provides:
    - web/pages/joins_lab.py: _merge_globals_web (BLD-04) + cross-side off-loop block (BLD-02) + builder integration (BLD-03) + known-joins wiring (ANC-04)
  affects:
    - plans/118-05 (final plan of the phase — wires entry points FND-04/05)
tech_stack:
  added: []
  patterns:
    - _merge_globals_web: pure module-level helper, applied to BOTH anchor ro and other-side b_ro after compose()
    - run_cross_side_core: literal-named sync closure (CI guard) with compose + _merge_globals_web + apply_cross_side inside
    - Auto-collapse summary bar (D-14): builder hides on search; Edit re-expands
    - run.io_bound(fetch_connected_fragments, confirmed_only=True) for known-joins off-loop fetch
    - Correct await precedence: await coroutine first, then unpack (not subscript-before-await)
key_files:
  created: []
  modified:
    - web/pages/joins_lab.py
decisions:
  - "_merge_globals_web: pure function, not anchor-specific — called on both sides; ja excluded per D-10"
  - "compose + _merge_globals_web inside run_cross_side_core: b_ro lifecycle fully off the event loop (plan requirement + CI contract)"
  - "global_opts snapshot taken before run_cross_side_core closure so current toggle values are captured correctly"
  - "Auto-collapse summary bar: builder.container set_visibility(False) on search; ui.row summary_bar_container set_visibility(True)"
  - "shelfmark fallback in load_anchor: await run.io_bound(lambda: executor.get_meta_for_id(sys_id)) → unpack tuple (correct precedence)"
  - "_load_known_joins called via asyncio.ensure_future from load_anchor (non-blocking for anchor swap)"
  - "on_reanchor calls asyncio.ensure_future(load_anchor(...)) — does NOT clear builder state (D-16)"
  - "on_open_browse navigates to /browse?shelfmark=... (same tab, per plan spec)"
metrics:
  duration: "~25min"
  completed: "2026-06-18"
  tasks: 3
  files: 1
---

# Phase 118 Plan 04: Full Builder Integration Summary

**One-liner:** `_merge_globals_web` BLD-04 re-injection (both sides) + row-builder replaces textarea + cross-side `apply_cross_side` off the event loop + confirmed-only known-joins group in the sticky anchor pane.

---

## What Was Built

### All three tasks — single file, single commit `bd2579c0`

**`web/pages/joins_lab.py`** — extended from the Phase-117 spine with four distinct additions.

#### Task 1: `_merge_globals_web` helper + global toggles + builder integration + Text Position routing (BLD-04, BLD-03)

**`def _merge_globals_web(ro: dict, global_opts: dict) -> dict`** — pure module-level helper:
- Sets `ro['flex_spacing']` and `ro['bidirectional']` from `global_opts`
- Does NOT set `ja` (stays `False` per D-10 user decision)
- Does NOT touch `variants` (flows via `SideQuery.variants`)
- Docstring cites BLD-04 / D-11 / desktop parity (`desktop/join_workbench.py:2475-2489`)

**Builder integration (BLD-03):**
- Phase-117 textarea (`:333-340`) replaced by `create_joins_builder(allow_page_position=True)`
- `anchor_builder` handle dict wired to `execute_joins_search`
- Empty-builder guard: if `anchor_builder['is_empty']()` → `ui.notify` + early return (no run)
- Text Position routing: `'line_start'/'line_end'` → `direct_text_position` (bypasses SideQuery, no ValueError); `'start'/'end'` flows via `compose()` → `page_position`; `'anywhere'` → both `None`
- Mode: `mode_str = anchor_builder['get_mode']()` (replaces hardcoded `'exact'`)
- `_merge_globals_web(ro, _global_opts)` called after `compose(side)` before `run_search_core`

**Global toggles (D-10, UI-SPEC §3):**
- `ui.expansion(tr('Advanced search options'))` disclosure (collapsed by default)
- `ui.checkbox(tr('Flexible spacing'))` + `ui.checkbox(tr('Bidirectional'))` — both write into `_global_opts` dict
- NO Judeo-Arabic toggle (D-10)

**Auto-collapse summary bar (D-14, UI-SPEC §2):**
- On non-empty Run Search: `anchor_builder['container'].set_visibility(False)`, `summary_bar_container.set_visibility(True)` with `_summary_label` showing `anchor_builder['get_summary']()`
- Edit icon button calls `_expand_builder()` which reverses the visibility

#### Task 2: Other-side builder + cross-side off-loop search (BLD-02)

**Other-side controls (UI-SPEC §3):**
- `ui.checkbox(tr('Search the other side of the leaf'))` — OFF by default
- When checked: reveals `create_joins_builder(allow_page_position=False)` + combine select Narrow(AND)/Widen(OR) defaulting to AND
- `_other_side = {'enabled': False, 'builder': None, 'combine': 'AND'}` mutable state

**Cross-side block in `execute_joins_search`:**
- After `dedup_candidates(raw_results, anchor_sid)` produces `base_candidates`
- If `_other_side['enabled']` and other-side builder not empty:
  - Snapshots `_combine_mode_snap`, `_other_sq_snap`, `_base_snapshot`, `_global_opts_snap`
  - Defines `run_cross_side_core` (literal name — CI guard) with `compose` + `_merge_globals_web(b_ro, _global_opts_snap)` + `apply_cross_side` ALL INSIDE the closure
  - `run.io_bound(run_cross_side_core)` → `asyncio.wait_for(cross_coro, timeout=...)` → `await cross_task`
  - Stale-generation re-check after the second await
  - Soft-notice on failure: `tr('Could not resolve the other side of this leaf...')`

**Web page contract (BLD-02):**
- `apply_cross_side` uses `executor.get_browse_page(sid, p_num=N)` internally (p_num 1-based, NOT internal_index)
- `total_pages=0` from metadata-only manuscripts is handled gracefully inside `apply_cross_side` (the shared core degrades to no upper clamp when `total_pages` is 0)
- Contract verified by `test_cross_side_uses_p_num_and_handles_metadata_only` from Plan 01

#### Task 3: Known-joins group in the sticky anchor pane (ANC-04)

**`known_joins_container`** — `ui.column()` inside `anchor_pane` BELOW `anchor_viewer_container` (UI-SPEC §4)

**`async def _load_known_joins(sys_id, shelfmark, pgpid=None)`:**
- Shows spinner; dispatches `run.io_bound(fetch_connected_fragments, ..., confirmed_only=True, ...)` (ANC-05 confirmed-only path)
- On success: clears container, calls `render_known_joins_group(data, ...)` with `on_reanchor` + `on_open_browse` callbacks
- On exception: shows `tr('Could not load joins. Check your connection.')` (muted)

**Callbacks:**
- `on_reanchor(member_sys_id, member_shelfmark)` → `asyncio.ensure_future(load_anchor(member_sys_id, ...))` (does NOT clear builder state — D-16)
- `on_open_browse(member_shelfmark)` → `ui.navigate.to(f'/browse?shelfmark={quote(member_shelfmark, safe="")}')` (same tab)

**`load_anchor` extension:**
- After `write_anchor` persists the anchor, fetches shelfmark via `await run.io_bound(lambda: executor.get_meta_for_id(sys_id))`; correct await precedence: `meta_result = await ...` then `shelfmark, _ = meta_result` (NOT `await run.io_bound(...)[0]`)
- Calls `asyncio.ensure_future(_load_known_joins(sys_id, shelfmark))` — non-blocking fire-and-forget

---

## Test Results

| Suite | Tests | Status |
|-------|-------|--------|
| test_merge_globals_web.py | 5 (were RED → GREEN) | PASS |
| test_other_side_page_contract.py | 8 (always GREEN) | PASS |
| test_joins_lab_off_loop.py | 7 | PASS |
| test_no_raw_storage_access.py | 6 | PASS |
| test_known_joins_group.py | 5 | PASS |
| test_joins_anc05_rls.py | 5 | PASS |
| test_joins_lab_page.py | 36 | PASS |
| **Total** | **72** | **PASS** |

---

## Deviations from Plan

None — plan executed exactly as written.

One implementation note: the plan stated compose + `_merge_globals_web(b_ro)` should run "inside `run_cross_side_core`". This was implemented exactly — both `compose(_other_sq_snap)` and `_merge_globals_web(b_ro, _global_opts_snap)` execute inside the `run_cross_side_core` sync closure, so the full b_ro lifecycle happens off the event loop. A snapshot of `_global_opts` is taken on the event loop before the closure (pure dict copy), which satisfies the "inside closure" contract while avoiding a race with the UI toggle state.

---

## Known Stubs

None. All four feature clusters are fully functional:
- `_merge_globals_web`: pure helper, tested, applied to both sides
- Builder integration: create_joins_builder replaces textarea; empty-guard; Text Position routing
- Cross-side: full `apply_cross_side` path; AND/OR; stale-gen re-check
- Known-joins: `fetch_connected_fragments(confirmed_only=True)` + `render_known_joins_group` wired

---

## Threat Surface Scan

No new network endpoints, auth paths, or schema changes introduced.

| Threat | Check | Result |
|--------|-------|--------|
| T-118-01 (cross-user join leak) | confirmed_only=True in _load_known_joins | PASS — status='confirmed' filter + ':confirmed' cache key from Plan 02 |
| T-118-02 (per-user state) | test_no_raw_storage_access.py | PASS — zero raw app.storage.user access |
| T-118-06 (event-loop blocking) | test_joins_lab_off_loop.py | PASS — run_search_core + run_cross_side_core + fetch_connected_fragments all via run.io_bound |
| T-118-04 (builder term → engine) | compose() produces parameterized Tantivy/Responsa syntax | PASS — no SQL |
| T-118-SC (supply chain) | No packages added | PASS |

---

## Self-Check: PASSED

Files verified:
- web/pages/joins_lab.py — FOUND

Commits verified:
- bd2579c0: feat(118-04): integrate builder + _merge_globals_web + cross-side + known-joins (BLD-02/03/04, ANC-04) — FOUND
