---
phase: 120
plan: "02"
subsystem: web/joins-lab
tags:
  - stability
  - ux
  - fire-and-forget-guard
  - stop-button
  - auth-dialog
dependency_graph:
  requires:
    - 120-01  # state helpers (write_full_state/read_full_state)
    - 119     # candidate surface, VS toggle, compare modal
  provides:
    - SEED-008 RuntimeError guard on all fire-and-forget UI coroutines
    - D-18 anonymous sign-in via in-page dialog (not navigation)
    - D-11 Stop-with-partials button (Run Search ↔ Stop swap)
  affects:
    - web/pages/joins_lab.py  # all changes
    - genizah_translations.py # Stop search / Stopping… strings
tech_stack:
  added: []
  patterns:
    - "try/except RuntimeError guard on NiceGUI fire-and-forget coroutines (SEED-008)"
    - "Stop-with-partials via stop_ref flag in _make_progress_cb (D-11)"
    - "create_login_dialog().open() for in-page auth overlay (D-18)"
key_files:
  modified:
    - web/pages/joins_lab.py
    - genizah_translations.py
    - tests/test_joins_lab.py
    - tests/render_smoke/test_joins_lab_render_smoke.py
decisions:
  - "D-20 M4 guard: outer try/except RuntimeError opens BEFORE PRE-await UI mutations in _load_known_joins — existing inner try/except Exception nested inside"
  - "D-11 stop flag: _stop_requested checked BEFORE generation check in progress_cb so InterruptedError fires while _should_apply_results still returns True"
  - "D-18: removed custom login_dialog builder + navigate.to('/settings') entirely — replaced with canonical create_login_dialog().open() from web.auth_state"
  - "VS-only F1 branch + _cancel_current_search also restored to swap Stop/Run Search visibility on any search exit path"
metrics:
  duration: "45min"
  completed: "2026-06-21"
  tasks: 3
  files: 4
---

# Phase 120 Plan 02: SEED-008 Guards, Sign-in Fix, Stop Button Summary

SEED-008 RuntimeError guards on all 3 fire-and-forget coroutines; D-18 anonymous sign-in via in-page overlay; D-11 Stop-with-partials swapping Run Search slot.

## Tasks Completed

| # | Task | Commit | Files |
|---|------|--------|-------|
| 1 | SEED-008 guard (_load_known_joins, _do_vs_fetch_and_update, _do_enrich_and_update) | a68a3119 | web/pages/joins_lab.py + tests |
| 2 | D-18 sign-in fix (create_login_dialog() replaces navigate.to('/settings')) | 7fb8356a | web/pages/joins_lab.py |
| 3 | D-11 Stop-with-partials (Stop button, _stop_requested, _make_progress_cb extension) | b16e5522 | web/pages/joins_lab.py + genizah_translations.py |

## Implementation Details

### Task 1: SEED-008 Guard (D-20)

Three fire-and-forget coroutines dispatched via `asyncio.ensure_future()` were
unguarded against `RuntimeError('slot has been deleted')` or `RuntimeError('deleted')`
raised when the client tab closes mid-execution.

**`_load_known_joins`** — M4 requirement: the outer `try/except RuntimeError` was added
BEFORE the PRE-await spinner clear/render (line ~1309). The existing inner
`try/except Exception` (guarding the IO-bound await) is preserved nested inside.

**`_do_vs_fetch_and_update`** — the full body (VS probe, loading state updates,
VS fetch, meta enrichment, `_re_render_candidates_surface`) is wrapped. The existing
inner try/except for `_fetch_vs_candidates` is preserved nested inside.

**`_do_enrich_and_update`** — the full body (enrichment await + `_re_render_candidates_surface`)
is wrapped.

Guard pattern (same as `joins_panel.py:512`):
```python
try:
    # ... full coroutine body ...
except RuntimeError:
    return  # client/tab deleted mid-fetch — benign teardown (SEED-008 D-20)
```

### Task 2: D-18 Sign-in Fix

The anonymous path in `_on_lists_btn_click` built a custom `login_dialog` with a
"Sign in" button calling `ui.navigate.to('/settings')` — navigating away and
discarding all Lab state.

Replaced with:
```python
from web.auth_state import create_login_dialog
create_login_dialog().open()
```

This opens the canonical in-page login overlay so the user stays on `/joins-lab`
with anchor, query, and triage state intact.

### Task 3: D-11 Stop-with-Partials

**`_stop_requested: dict = {'value': False}`** added to page state.

**`_make_progress_cb` extended** with `stop_ref: Optional[dict] = None`:
- Stop flag checked BEFORE generation check
- `InterruptedError('joins-lab search stopped by user')` raised when stop is set
- Generation unchanged → `_should_apply_results(my_gen, _search_generation)` returns True
- Partial results from the core are applied (not discarded)

**Stop button** (`icon=stop_circle`, `color=negative`, marker `stop_search_btn`):
- Created hidden in the same row as Run Search
- `_on_stop_click`: sets flag + transitions to "Stopping…" affordance
- Search start: resets flag, hides Run Search, shows Stop
- Search end (outer finally, VS-only finally, `_cancel_current_search`): hides Stop, shows Run Search, resets flag

**Translations added:**
- `"Stop search and show partial results"` → `"עצור חיפוש והצג תוצאות חלקיות"`
- `"Stopping…"` (Unicode ellipsis) → `"עוצר…"`

## Tests Added

**Unit tests (test_joins_lab.py):**
- `test_load_known_joins_client_deleted` — PRE-await RuntimeError guard (M4)
- `test_load_known_joins_client_deleted_post_await` — POST-await guard
- `test_seed008_guard_only_catches_runtime_error` — non-RuntimeError bubbles
- `test_should_apply_results_and_stop_requested_logic` — generation guard contract
- `test_make_progress_cb_stop_requested_raises_interrupted` — stop-flag pattern
- `test_signin_opens_dialog_not_navigate` — static assert no navigate.to('/settings')
- `test_stop_applies_partials` — Stop-with-partials contract

**Render-smoke tests (tests/render_smoke/test_joins_lab_render_smoke.py):**
- `test_client_deleted_guard_load_known_joins` — D-20 live render path
- `test_signin_button_opens_dialog_not_navigate` — D-18 static + render
- `test_stop_button_visible_during_search` — D-11 static + render

## Deviations from Plan

None — plan executed exactly as written.

## Known Stubs

None — all three tasks are fully wired.

## Threat Flags

No new network endpoints, auth paths, file access patterns, or schema changes introduced.
The D-18 fix uses `create_login_dialog()` from existing `web.auth_state` — no new trust
boundary surface.

## Self-Check: PASSED

- 120-02-SUMMARY.md: FOUND
- a68a3119 (SEED-008 guard): FOUND
- 7fb8356a (D-18 sign-in fix): FOUND
- b16e5522 (D-11 Stop-with-partials): FOUND
- 95/95 unit tests pass (test_joins_lab.py + test_joins_lab_off_loop.py + test_no_server_side_stop_propagation.py)
- ruff clean on all modified files
