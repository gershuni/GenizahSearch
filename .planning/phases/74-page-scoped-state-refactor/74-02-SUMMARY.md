---
phase: 74
plan: 02
subsystem: web
tags: [web, browse, bootstrap, refactor, WEBM-03]
requirements: [WEBM-03]
dependency-graph:
  requires:
    - "restore_browse_snapshot / clear_browse_snapshot (Plan 74-01)"
  provides:
    - "resolve_browse_bootstrap pure precedence helper (web/browse_bootstrap.py)"
    - "8 passing unit tests in tests/test_browse_bootstrap.py"
  affects:
    - web/pages/browse.py
tech-stack:
  added: []
  patterns:
    - "Pure precedence helper mirroring web/search_bootstrap.py shape"
    - "Dispatch-dict return contract (action + data) with scheduling left to caller"
key-files:
  created:
    - web/browse_bootstrap.py
  modified:
    - tests/test_browse_bootstrap.py
    - web/pages/browse.py
decisions:
  - "resolve_browse_bootstrap is pure - no app.storage / asyncio / NiceGUI imports (Pitfall 3)"
  - "Dispatch in browse.py preserves restore_position side effects (shelfmark + volume_ie validation) verbatim (Codex HIGH #6)"
  - "Stale-desk clear routes through clear_browse_snapshot() not direct pop (Codex HIGH #9)"
metrics:
  duration: "~10m executor time"
  tasks: 3
  files_touched: 3
  commits: 3
  completed: 2026-04-17
---

# Phase 74 Plan 02: Browse Bootstrap Extraction Summary

Extracted the browse-page precedence logic from `web/pages/browse.py:4446-4512` into a new pure helper `resolve_browse_bootstrap()` in `web/browse_bootstrap.py`, mirroring the `web/search_bootstrap.py` pattern. The helper is fully unit-tested (8 cases) and the call site in `create_browse_page()` is now a single resolver call + dispatch switch.

## What Was Built

### Task 1 - TDD RED (commit `1f3c5f9e`)

Replaced Plan 74-01 skip-stubs in `tests/test_browse_bootstrap.py` with 8 concrete tests (direct import, no try/skip):

1. `test_explicit_sys_id_beats_saved_position` (D-19 case a)
2. `test_blank_browse_restores_saved_position` (D-19 case b)
3. `test_reading_desk_restore_wins_over_position` (D-19 case c)
4. `test_explicit_sys_id_matching_desk_restores_desk` (language-switch defensive)
5. `test_no_context_no_action` (empty state defensive)
6. `test_fl_id_trumps_everything` (fl_id precedence)
7. `test_restore_position_passes_shelfmark_and_volume_ie` (Codex #6 side-effect)
8. `test_restore_position_handles_none_volume_ie` (Codex #6 side-effect)

Collection failed with `ModuleNotFoundError: web.browse_bootstrap` — confirmed TDD RED.

### Task 2 - GREEN (commit `f6330ee4`)

Created `web/browse_bootstrap.py` (108 lines) with `resolve_browse_bootstrap(*, initial_fl_id, initial_sys_id, initial_page, pending_shelfmark, saved_reading_desk, saved_position)` returning a dispatch dict with keys `action / p_num / fl_id / sys_id / shelfmark / volume_ie / restore_desk / clear_desk`.

Seven precedence branches encoded:
1. `initial_fl_id` → `action='fl_id'`
2. `initial_sys_id` matches a desk entry → `action='restore_desk'`
3. `initial_sys_id` no match → `action='sys_id'`, `clear_desk=True` if desk existed
4. `pending_shelfmark` → `action='shelfmark'`
5. Blank URL + desk → `action='restore_desk'`
6. Blank URL + position → `action='restore_position'`
7. Nothing saved → `action='none'`

Purity verified: `grep` returns 0 hits for `app.storage`, `asyncio`, or `nicegui` in the module source.

### Task 3 - Wire into browse.py (commit `36beb9fa`)

Added imports:
```python
from web.pages.browse_state import (
    BrowseState, _crossref_cache,
    persist_browse_snapshot, clear_browse_snapshot, restore_browse_snapshot,
)
from web.browse_bootstrap import resolve_browse_bootstrap
```

### Before/After Bootstrap Block

**Before** (browse.py:4446-4512, 67 lines): 3-level `if/elif/else` tree with inline `app.storage.user.get('reading_desk_state')`, direct `app.storage.user.pop(...)`, and the embedded language-switch / cross-page-navigation collision detection.

**After** (browse.py:4446-4509, 64 lines): `restore_browse_snapshot(state)` tuple read → `resolve_browse_bootstrap(...)` call → flat `if action == ...` dispatch routing each case to the appropriate `asyncio.ensure_future(load_page(...))` / `search_shelfmark()` / `_restore_reading_desk_state()` / `update_content()` call.

### Cat-2 ensure_future calls surviving in dispatch (for Plan 74-03 audit)

All Cat-2 calls preserved unchanged — each has a Cat-2 justification comment:

| Action branch | Call | Reason |
|---|---|---|
| `fl_id` | `asyncio.ensure_future(load_page(fl_id=...))` | Spinner must render before async |
| `sys_id` | `asyncio.ensure_future(load_page(p_num=...))` | Container mount before load |
| `shelfmark` | `asyncio.ensure_future(search_shelfmark())` | Bootstrap deferred init |
| `restore_position` | `asyncio.ensure_future(load_page(p_num=...))` | Container mount + spinner before load |
| `restore_desk` fallback | `asyncio.ensure_future(load_page(p_num=...))` | Same (safety net) |

These remain in browse.py per D-11 (Cat-2 keep explicit). Plan 74-03 focuses on Cat-1 cleanup elsewhere.

## Test Results

- `pytest tests/test_browse_bootstrap.py -x` — **8 passed** (all D-19 cases + 2 side-effect + fl_id + no-context)
- `pytest tests/test_browse_bootstrap.py tests/test_search_state.py tests/test_search_bootstrap.py -x` — **16 passed**
- `pytest tests/ --ignore=tests/e2e` — **1078 passed, 5 skipped** (baseline after 74-01 was 1070; +8 new browse_bootstrap tests)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Docstring contained forbidden substrings `app.storage` / `asyncio`**

- **Found during:** Task 2 purity verification
- **Issue:** The plan's acceptance criterion `assert 'app.storage' not in src` and `assert 'asyncio' not in src` checks ANY occurrence, including docstrings. The original docstring copied verbatim from the plan said "no app.storage.user reads, no NiceGUI calls" and triggered a false-positive violation.
- **Fix:** Rewrote that sentence to "Pure function: no storage reads, no NiceGUI calls, no async scheduling." Meaning preserved; the literal substrings avoided.
- **Files modified:** `web/browse_bootstrap.py` (docstring only)
- **Commit:** Folded into `f6330ee4` (same Task 2 commit)

**2. [Rule 2 - Critical functionality] Preserve `is_loading` + `update_content()` in sys_id dispatch**

- **Found during:** Task 3, reviewing the dispatch against the pre-refactor block
- **Issue:** The plan's sys_id dispatch snippet only called `asyncio.ensure_future(load_page(...))` but the live pre-refactor code at browse.py:4483-4485 also set `state.is_loading = True; update_content()` to render the spinner synchronously before the async load. Omitting these would change visual behavior (no spinner during the load).
- **Fix:** Added `state.is_loading = True; update_content()` to the `action == 'sys_id'` branch, matching the pre-refactor behavior exactly.
- **Files modified:** `web/pages/browse.py`
- **Commit:** Folded into `36beb9fa` (Task 3 commit)

**3. [Rule 2 - Critical functionality] restore_desk failure safety net**

- **Found during:** Task 3, handling the `action == 'restore_desk'` branch
- **Issue:** Pre-refactor code at browse.py:4467-4470 had a fallback: if `_restore_reading_desk_state()` failed during a language-switch (sys_id in desk entries), it fell through to `load_page(p_num=initial_page)`. The plan's dispatch snippet only called `update_content()` on desk failure, which would leave the explicit sys_id URL unloaded.
- **Fix:** Split the desk-fallback: when `initial_sys_id` is set and desk restore fails, fall through to `load_page`; otherwise (blank URL case) fall through to `update_content()`. Preserves both legacy behaviors.
- **Files modified:** `web/pages/browse.py`
- **Commit:** Folded into `36beb9fa`

## Verification Summary

- Pure function check: `web/browse_bootstrap.py` has zero `app.storage` / `asyncio` / `from nicegui` occurrences
- Helper called exactly once in `browse.py` (the bootstrap block in `create_browse_page()`)
- `restore_browse_snapshot` tuple feeds bootstrap inputs — no double-read of storage
- Stale desk clear uses `clear_browse_snapshot()` (not direct `app.storage.user.pop`)
- `restore_position` branch preserves `state.shelfmark_query` assignment + `get_volumes_for_sys_id` validation + `is_loading` + `update_content` from pre-refactor block
- Web smoke (D-22): deferred to user — Windows dev box test constraint per MEMORY.md

## Notes for Plan 74-03

- Five Cat-2 `asyncio.ensure_future` sites remain in the bootstrap dispatch (documented in table above). All have Cat-2 justification comments. Plan 74-03 should leave these and focus on Cat-1 handlers (lambda wrappers in `on_click=` etc.) elsewhere in browse.py / search.py / filter_panel.py.
- The `_restore_reading_desk_state()` local helper at browse.py:1056 still reads `app.storage.user.get('reading_desk_state')` directly — this is the full-hydration path kept intentionally per Codex MEDIUM #5 (the snapshot helper only exposes minimal shape for precedence resolution).

## Self-Check: PASSED

Verified commits exist in git log:
- `1f3c5f9e` test(74-02): add 8 precedence + side-effect tests for resolve_browse_bootstrap
- `f6330ee4` feat(74-02): add resolve_browse_bootstrap pure precedence helper
- `36beb9fa` refactor(74-02): route browse bootstrap through resolve_browse_bootstrap

Verified files exist:
- `web/browse_bootstrap.py` FOUND — contains `def resolve_browse_bootstrap(`
- `tests/test_browse_bootstrap.py` FOUND — 8 test functions, 0 `pytest.skip` occurrences
- `web/pages/browse.py` FOUND — contains `from web.browse_bootstrap import resolve_browse_bootstrap` and `resolve_browse_bootstrap(` call
