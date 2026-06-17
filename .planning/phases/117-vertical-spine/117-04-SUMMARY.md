---
phase: 117-vertical-spine
plan: "04"
subsystem: web-joins-lab
tags: [joins-lab, integration, route, search-off-loop, high-3, medium-4, cancellation, stale-generation, fnd-02, fnd-03, fnd-08, bld-01, bld-05, cnd-01]
dependency_graph:
  requires: [117-01, 117-02, 117-05, 117-06]
  provides:
    - web/pages/joins_lab.py  # create_joins_lab_page(), module-level helpers
    - /joins-lab route in web/main.py  # FND-02 + FND-08 URL contract
    - tests/test_joins_lab_page.py  # headless helper tests (43 tests)
  affects:
    - web/main.py  # sidebar nav + route
    - tests/test_joins_lab_off_loop.py  # now load-bearing (not skipping) — MEDIUM-4
tech_stack:
  added: []
  patterns:
    - "asyncio.wait_for(run.io_bound(sync_closure), timeout=N) — HIGH-3 timeout + off-loop (MEDIUM-4)"
    - "latest-wins asyncio.Task.cancel() + _search_generation stale-gen discard — HIGH-3"
    - "_make_progress_cb cooperative worker cancel (InterruptedError) — MEDIUM"
    - "decide_initial_anchor URL-wins-over-storage helper — D-13"
    - "lines_to_side_query textarea -> SideQuery(variants=False) — D-08/D-09"
    - "ui.timer(0.05, once=True) deferred bootstrap — NiceGUI-safe async deferred init"
key_files:
  created:
    - web/pages/joins_lab.py
    - tests/test_joins_lab_page.py
  modified:
    - web/main.py  # +42 lines: route + nav entry
decisions:
  - "Use get_service() (not a module-level 'service' import) for search_by_shelfmark — service is a GenizahService instance obtained via get_service(); import pattern matches browse.py"
  - "lines_to_side_query passes line.strip() to BuilderRow(term=) — filter and term both use stripped value (bug caught by test)"
  - "AnchorViewer instantiated without executor= — HIGH-1 honored; self-resolves rich BrowsePage"
  - "D-06 login gate: shows picker dialog for logged-in users (stub placeholder pointing to /lists), explicit login prompt for anonymous; code comment accurately documents the process-global ListsManager divergence (NOTE)"
metrics:
  duration: "~40min"
  completed_date: "2026-06-17"
  tasks_completed: 4
  files_created: 2
  files_modified: 1
requirements: [FND-02, FND-03, FND-08, BLD-01, BLD-05, CND-01]
---

# Phase 117 Plan 04: Integration — /joins-lab Vertical Spine Summary

Wire all Wave-1 pieces into the working `/joins-lab` vertical spine: route + sidebar nav, empty state, anchor load/resolve, off-loop search with timeout+cancellation+stale-generation, dedup, candidate grid, anchor persistence.

## What Was Built

### Task 1 — `/joins-lab` route + sidebar nav in `web/main.py`

- `@ui.page('/joins-lab', ...)` route with 5 URL params (`sys_id` primary, `shelfmark`/`fl_id`/`page`/`volume_ie` optional) + FND-08 docstring documenting that builder/triage/candidate state is NEVER in the URL.
- Sidebar nav entry `('/joins-lab', 'join_inner', tr('Joins Lab'), None)` placed between "My Lists" and the WEB_PUZZLE_ENABLED puzzle append.
- Route delegates to `create_joins_lab_page(...)` from `web.pages.joins_lab`.

### Tasks 2 + 3 — `web/pages/joins_lab.py`

**Module-level pure helpers (headlessly testable):**
- `decide_initial_anchor(initial_sys_id, initial_shelfmark, stored)` — URL-wins-over-storage (D-13); returns a typed dict or None.
- `lines_to_side_query(text)` — strips + maps non-empty textarea lines to `BuilderRow`s in a `SideQuery(variants=False, page_position=None)` (D-08/D-09).
- `_should_apply_results(my_gen, gen_ref)` — pure discard predicate; the PRIMARY guard for a cooperatively-cancelled run's partial results.
- `_make_progress_cb(my_gen, gen_ref)` — returns a callback that raises `InterruptedError` when superseded (MEDIUM cooperative worker cancel + parallels.py dual-protocol guard).

**Page factory `create_joins_lab_page(...)`:**
- Direction-aware two-column sticky layout (D-01/D-02): `flex-row-reverse` in RTL, anchor pane 380px sticky, work column `flex:1` (D-04: no hardcoded child structure for Phase 118/119).
- Empty state: "pin an anchor" panel with smart box + D-06 login-gated "Choose from my lists" button (code comment accurately documents the process-global `ListsManager` divergence per NOTE).
- `resolve_anchor_input()`: sys_id fast path (digits + starts with '99') + `service.search_by_shelfmark()` off-loop via `run.io_bound`.
- `load_anchor()`: sets `_anchor_state`, swaps UI, instantiates `AnchorViewer` (HIGH-1: no executor= arg), `await`s `update_content()`, writes anchor via `write_anchor()` (D-13).
- `_bootstrap_anchor()` deferred via `ui.timer(0.05, once=True)` — URL-wins decision, shelfmark resolution, stored restore (D-13 complete).
- Builder: `ui.textarea` (RTL, Hebrew font, aria-label) + `tr('Run Search')` button.
- `execute_joins_search()`: **HIGH-3 all three SC#3 legs**:
  1. TIMEOUT: `asyncio.wait_for(run.io_bound(run_search_core), timeout=120s)`
  2. CANCELLATION / LATEST-WINS: `prev.cancel()` on in-flight task before new run
  3. STALE-GENERATION: `_should_apply_results(my_gen, _search_generation)` discards partial results from a cooperatively-cancelled run
- `run_search_core` sync closure: `executor.execute_search(...)` ONLY here (MEDIUM-4, statically enforced).
- MEDIUM cooperative worker cancel: `_make_progress_cb(my_gen)` raises `InterruptedError` → core catches (genizah_core.py:9000) → aborts scan early → worker freed → partial results returned → discarded by `_should_apply_results`.
- `dedup_candidates(raw_results, anchor_sid)` + `create_candidate_grid(candidates)` (CND-01/CND-02).

### Task 4 — `tests/test_joins_lab_page.py` (43 headless tests)

| Test class | Tests | What's covered |
|---|---|---|
| `TestLinesToSideQuery` | 8 | blank-line drop, term strip, spine defaults (variants=False), empty input |
| `TestComposePipeline` | 3 | 3-row compose → non-None query_str; empty → None; 3-tuple return |
| `TestDedupCandidates` | 5 | same-page dedup, anchor exclusion, include_self flag |
| `TestDecideInitialAnchor` | 8 | URL sys_id wins, URL shelfmark path, stored restore, cold start |
| `TestShouldApplyResults` | 4 | current gen → True; superseded → False |
| `TestMakeProgressCb` | 6 | InterruptedError when superseded; no raise when current; dual-protocol string guard; mid-search gen bump |
| `TestEndToEndDiscard` | 2 | fake executor mimics core catch-and-return-partial; _should_apply_results=False proves discard |

`tests/test_joins_lab_off_loop.py` now PASSES (not skips) against the real `joins_lab.py` — MEDIUM-4 is live.

## Task Commits

| Task | Commit | Files |
|------|--------|-------|
| 1: /joins-lab route + nav | dbd134f5 | web/main.py |
| 2+3: joins_lab.py page | 92cc8c97 | web/pages/joins_lab.py |
| 4: tests + strip bug fix | 2e162007 | tests/test_joins_lab_page.py, web/pages/joins_lab.py |

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `lines_to_side_query` passed unstripped line to `BuilderRow(term=)`**
- **Found during:** Task 4 (test `test_terms_are_stripped` failed)
- **Issue:** `BuilderRow(term=line)` used the raw line; the `if line.strip()` filter ran but the term in the row still had leading/trailing spaces.
- **Fix:** Changed to `BuilderRow(term=line.strip())` so the term is always stripped.
- **Files modified:** `web/pages/joins_lab.py`
- **Commit:** 2e162007

**2. [Rule 3 - Blocking] `service` import pattern — `web.services` exports `get_service()`, not `service`**
- **Found during:** Task 4 test import
- **Issue:** Plan said `from web.services import service` but `web/services.py` exports `get_service()` (not a module-level `service` name).
- **Fix:** Changed to `from web.services import get_service` and call `get_service().search_by_shelfmark(...)` at call sites (matching the browse.py pattern at line 521).
- **Files modified:** `web/pages/joins_lab.py`
- **Commit:** 92cc8c97 (initial fix, then confirmed in 2e162007 ruff clean)

## Known Stubs

One intentional stub: the logged-in "Choose from my lists" handler shows a placeholder dialog pointing to `/lists` rather than a full list-picker UI. This is documented as Phase 120 scope (PST-01/02). The anonymous path (login prompt) is fully implemented per D-06.

## Threat Surface Scan

No new threat surface beyond what the plan's threat model covers:

| Threat | Status |
|--------|--------|
| T-117-01 (DoS via event-loop execute_search) | MITIGATED — execute_search only inside run_search_core; test_joins_lab_off_loop.py enforces statically (MEDIUM-4) |
| T-117-12 (rapid clicks / pathological query) | MITIGATED — asyncio.wait_for(120s) + latest-wins cancel + stale-gen discard (HIGH-3 all three SC#3 legs) + MEDIUM cooperative worker cancel |
| T-117-04 (per-session anchor bleed) | MITIGATED — all state via write_anchor/read_anchor; no raw app.storage.user |
| T-117-13 (progress_cb string TypeError) | MITIGATED — isinstance(arg1, str): return guard in _make_progress_cb |
| T-117-11 (arbitrary sys_id in URL) | MITIGATED — search_by_shelfmark returns [] → inline "not found"; AnchorViewer returns None-boundary → "not found" state |
| T-117-14 (shared local ListsManager surfaced to anonymous) | MITIGATED — D-06 login prompt; local store intentionally NOT exposed; code comment documents NOTE divergence accurately |

## Verification

```
pytest tests/test_joins_lab_page.py tests/test_joins_lab_off_loop.py tests/test_no_raw_storage_access.py -x -q
49 passed in 2.90s

python -c "import ast; ast.parse(open('web/main.py',encoding='utf-8').read()); ast.parse(open('web/pages/joins_lab.py',encoding='utf-8').read())"  → exits 0

grep -nE "app\.storage\.user|iiif\.nli\.org\.il" web/pages/joins_lab.py  → only docstring/comment lines (no code)
grep -nE "asyncio\.wait_for|CancelledError|\.cancel\(\)|raise InterruptedError" web/pages/joins_lab.py  → all present (HIGH-3 + MEDIUM)
grep -n "def _should_apply_results" web/pages/joins_lab.py  → module-level helper present
grep -n "run.io_bound" web/pages/joins_lab.py  → search dispatch present
```

## Self-Check: PASSED

Files exist:
- `web/pages/joins_lab.py` ✓
- `tests/test_joins_lab_page.py` ✓
- `web/main.py` updated ✓

Commits exist:
- `dbd134f5` ✓
- `92cc8c97` ✓
- `2e162007` ✓

Test run: 49 passed ✓
Ruff: all checks passed ✓

---
*Phase: 117-vertical-spine*
*Completed: 2026-06-17*
