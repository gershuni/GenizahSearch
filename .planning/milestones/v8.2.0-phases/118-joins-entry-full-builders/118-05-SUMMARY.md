---
phase: 118-joins-entry-full-builders
plan: "05"
subsystem: web/joins-lab
tags: [fnd-04, fnd-05, entry-points, search-results, browse, joins-panel, off-loop, security, wave-2]
dependency_graph:
  requires:
    - 118-01 (RED test stubs)
    - 118-02 (fetch_connected_fragments confirmed_only + joins dialog base)
    - 118-04 (/joins-lab?sys_id= routing target)
  provides:
    - web/components/joins_panel.py: find_joins_url param on create_joins_button/create_joins_dialog (FND-04/05, backward-compatible)
    - web/pages/search_results.py: joins icon on result cards + Quick View find_joins_url wiring (FND-04, D-20/D-21); off-loop count load
    - web/pages/browse.py: find_joins_url passed to create_joins_button (FND-05, D-19)
  affects: []
tech_stack:
  added: []
  patterns:
    - find_joins_url Optional[str]=None as last kwarg (backward-compatible extension pattern)
    - ui.navigate.to(url, new_tab=True) for internal SPA routes to Joins Lab (D-18)
    - async ui.timer(0.15, once=True) + run.io_bound for off-loop joins-presence hint (T-118-06)
    - Joins-present: green icon + dialog with "Find more joins"; joins-absent: neutral icon + straight to Lab
key_files:
  created: []
  modified:
    - web/components/joins_panel.py
    - web/pages/search_results.py
    - web/pages/browse.py
decisions:
  - "find_joins_url appended as LAST kwarg to both create_joins_button and create_joins_dialog — all existing call sites unaffected (backward-compatible)"
  - "create_joins_button load_count: when no joins and find_joins_url set, rebinds click via .on('click', ...) after lazy count load (D-19 no-joins path)"
  - "'Find more joins' button placed after 'View All Fragments', gated on find_joins_url AND total>1 (joins must exist for the dialog to be meaningful)"
  - "Card-level joins count loads via async def + run.io_bound inside ui.timer(0.15, once=True) — keeps synchronous Supabase+SQLite fetch off the event loop (T-118-06 / Codex MEDIUM)"
  - "Click handler captures has_joins in a mutable ref dict; on click: has_joins=True -> create_joins_dialog(find_joins_url=url); False -> ui.navigate.to(url, new_tab=True) (D-21)"
  - "Quick View (open_advanced_dialog) builds _qv_joins_url from sys_id + adv_state.volume_ie (already parsed at line ~968); passes find_joins_url to existing create_joins_button call (D-20)"
  - "browse.py: _joins_lab_url built from page.sys_id + page.volume_ie (multi-IE FND-08 contract); passed to existing create_joins_button call (D-19)"
metrics:
  duration: "~25min"
  completed: "2026-06-18"
  tasks: 3
  files: 3
---

# Phase 118 Plan 05: Find Joins Entry Points Summary

**One-liner:** Find-joins entry points (FND-04 + FND-05) wired via backward-compatible `find_joins_url` param on the existing joins button/dialog; /search cards get a neutral-to-green joins icon loaded off the event loop via `run.io_bound`.

---

## What Was Built

### Task 1: `find_joins_url` in `create_joins_button` / `create_joins_dialog` (commit `0c25d4eb`)

**`web/components/joins_panel.py`** — two backward-compatible signature extensions.

**`create_joins_button`** gains `find_joins_url: Optional[str] = None` (last kwarg):
- `load_count` extended: when no joins (`total_fragments <= 1`) AND `find_joins_url` provided, recolors button to neutral (`text-neutral-500`) + rebinds click via `.on('click', ...)` to `ui.navigate.to(find_joins_url, new_tab=True)` (D-19 no-joins path)
- When joins exist, keeps existing green recolor; `open_joins_panel` forwards `find_joins_url` into `create_joins_dialog`

**`create_joins_dialog`** gains `find_joins_url: Optional[str] = None` (last kwarg):
- "Find more joins" flat button (`icon='science'`) inserted after "View All Fragments", gated on `find_joins_url and total > 1`; tooltip `tr('Go to Joins Lab to find more joins')`; click closes dialog then opens Lab in new tab

All existing callers omit `find_joins_url` and get identical behavior.

### Task 2: Joins icon on `/search` result cards + Quick View (commit `58c6ec07`)

**`web/pages/search_results.py`**:

**Top-level imports added:** `from web.components.joins_panel import fetch_connected_fragments, create_joins_dialog`

**`create_result_card`** — after the Catalog Records button, when `sys_id` present:
- Builds `_joins_url = '/joins-lab?sys_id={sys_id}'` + `&volume_ie={_joins_ie_id}` when available (re-parses `raw_header` via `state.meta_mgr.parse_full_id_components`, try/except guarded)
- Neutral `icon='link'` button (`style='color: var(--neutral-400);'`) with tooltip `tr('Joins')`
- Click handler (D-21): `has_joins=True` → `create_joins_dialog(find_joins_url=url)`; `has_joins=False` → `ui.navigate.to(url, new_tab=True)`
- Off-loop count load (T-118-06): `async def _load_card_joins_count` dispatches `fetch_connected_fragments` via `await run.io_bound(...)` — synchronous Supabase+SQLite I/O never runs on the event loop; one-shot `ui.timer(0.15, once=True)` fires the coroutine; exceptions swallowed (icon stays neutral)

**Quick View** (D-20): `create_joins_button` call at `~:2023` now builds `_qv_joins_url` from `sys_id` + `adv_state.volume_ie` (already parsed at line ~968) and passes `find_joins_url=_qv_joins_url`.

### Task 3: `find_joins_url` on `/browse` (commit `05d29191`)

**`web/pages/browse.py`** — the existing `create_joins_button` call at line 3904:
- Builds `_joins_lab_url = '/joins-lab?sys_id={page.sys_id}'` with `&volume_ie={page.volume_ie}` when set (multi-IE FND-08 contract)
- Passes `find_joins_url=_joins_lab_url` to the existing call — no separate button (D-19 reuse)
- Behavior driven by Task 1: joins exist → dialog + "Find more joins"; none → neutral recolor + new tab

---

## Test Results

| Suite | Tests | Status |
|-------|-------|--------|
| test_joins_lab_page.py | 36 | PASS |
| test_known_joins_group.py | 5 | PASS |
| test_joins_anc05_rls.py | 5 | PASS |
| test_no_raw_storage_access.py | 6 | PASS |
| **Total** | **52** | **PASS** |

All three files parse without error (`ast.parse` verified).

---

## Deviations from Plan

None — plan executed exactly as written.

One implementation note: The plan's PATTERNS.md listed `asyncio.get_event_loop().call_later(0.15, _load_card_joins_count)` as the kick-off pattern for the card count loader. The plan task text specified `ui.timer(0.15, _load_card_joins_count, once=True)` instead (one-shot async timer — runs the coroutine off the render path). The task text takes precedence: `ui.timer(once=True)` is the correct NiceGUI pattern for one-shot async coroutines (the PATTERNS.md example was a `call_later` with a synchronous function, which is the WRONG pattern that the plan explicitly forbids). Used `ui.timer` with `once=True` as directed by the task action text, matching the acceptance criteria.

---

## Known Stubs

None. All three files are fully functional:
- `joins_panel.py`: "Find more joins" button opens live Lab URL; no-joins recolor redirects to Lab.
- `search_results.py`: Card icon loads live joins count via `run.io_bound`; click opens dialog or Lab.
- `browse.py`: `page.volume_ie` is a live attribute on the `BrowsePage` dataclass.

---

## Threat Surface Scan

No new network endpoints, auth paths, file access patterns, or schema changes introduced.

| Threat | Check | Result |
|--------|-------|--------|
| T-118-07 (deep-link URL state disclosure) | URL carries only sys_id + volume_ie; no builder/triage/candidate state | PASS |
| T-118-06 (event-loop blocking) | fetch_connected_fragments dispatched via run.io_bound inside async timer; no synchronous call on loop | PASS |
| T-118-02 (per-user state) | test_no_raw_storage_access.py green; zero raw app.storage.user access | PASS |
| T-118-08 (sys_id/volume_ie tampering) | Read-only identifiers; joins-lab route validates via Phase-117 cold-start resolver | ACCEPT |

---

## Self-Check: PASSED

Files verified to exist:
- web/components/joins_panel.py — FOUND (find_joins_url in both signatures, "Find more joins" button, no-joins recolor)
- web/pages/search_results.py — FOUND (joins icon on cards, run.io_bound count load, Quick View find_joins_url)
- web/pages/browse.py — FOUND (find_joins_url + volume_ie passed to create_joins_button)

Commits verified:
- 0c25d4eb: feat(118-05): add find_joins_url to create_joins_button/create_joins_dialog (D-19, backward-compatible) — FOUND
- 58c6ec07: feat(118-05): joins icon on /search result cards + find_joins_url in Quick View (FND-04, D-20/D-21) — FOUND
- 05d29191: feat(118-05): pass find_joins_url to create_joins_button on /browse (FND-05, D-19) — FOUND
