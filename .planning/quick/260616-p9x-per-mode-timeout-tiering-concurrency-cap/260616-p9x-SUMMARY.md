---
status: complete
phase: quick-260616-p9x
plan: 01
subsystem: web-search-api
tags: [api, timeout, concurrency, fuzzy, search]
dependency_graph:
  requires: [web/search_api.py, shared/api_errors.py, shared/browse_service.py]
  provides: [per-mode-timeout-ladder, heavy-mode-semaphore, fuzzy-result-cap]
  affects: [POST /api/search, POST /api/parallels, skill client]
tech_stack:
  added: []
  patterns: [asyncio.Semaphore non-blocking via _value check, per-request env-read via _read_timeout]
key_files:
  created: []
  modified:
    - shared/api_errors.py
    - web/search_api.py
    - tests/test_search_api_v2.py
    - tests/test_parallels_api.py
    - tests/test_search_api.py
    - docs/SEARCH_API.md
    - skills/cairo-genizah-research/references/api_contract.md
    - skills/cairo-genizah-research/scripts/search.py
    - skills/cairo-genizah-research/scripts/parallels.py
    - CLAUDE.md
    - CHANGELOG.md
decisions:
  - "_acquire_heavy_slot uses direct _value decrement (not asyncio.wait_for(timeout=0)) — in Python 3.11 wait_for(timeout=0) always raises TimeoutError before the coroutine runs one step, even when slots are available"
  - "Pydantic Field le=2000 (FUZZY_HARD_MAX) instead of le=100 — per-mode ceiling enforced in handler; limit_too_high code replaces invalid_request for limit in (100, 2000] for non-fuzzy"
  - "asyncio.Semaphore rebuild only when desired != current capacity AND sem is fully idle (no held slots) — safe resize without stranding held slots"
  - "fuzzy default-limit detection via 'limit' absent from raw body dict — avoids mutating the Pydantic model"
metrics:
  duration: "~35 minutes"
  completed_date: "2026-06-16"
  task_count: 3
  file_count: 11
---

# Quick Task 260616-p9x: Per-Mode Timeout Tiering + Concurrency Cap + Fuzzy Result Cap

**One-liner:** Per-mode Search API timeout ladder (variants 60s / fuzzy 300s / parallels 300s), asyncio.Semaphore fast-fail 503 for heavy-mode burst protection, and raised fuzzy result cap (default 500) so recall-critical queries don't silently drop true hits.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1+2 | Per-mode timeout ladder + heavy-mode semaphore + fuzzy result cap | `e4f315af` | shared/api_errors.py, web/search_api.py, tests/test_search_api_v2.py, tests/test_parallels_api.py, tests/test_search_api.py |
| 3 | Documentation | `f1b5b2b2` | docs/SEARCH_API.md, skills/.../api_contract.md, skills/.../search.py, skills/.../parallels.py, CLAUDE.md, CHANGELOG.md |

## What Was Built

### Task 1: Per-mode timeout ladder + heavy-mode concurrency semaphore

**`_resolve_search_timeout(search_mode)`** — returns `(ceiling_float, env_var_name)`:
- `variants` → `SEARCH_API_VARIANTS_TIMEOUT` (default 60.0)
- `fuzzy` → `SEARCH_API_FUZZY_TIMEOUT` (default 300.0)
- all other modes → `SEARCH_API_CORE_TIMEOUT` (default 30.0, interactive baseline)

All reads go through `_read_timeout()` so they are re-evaluated per request.

**`_resolve_parallels_timeout()`** — reads `SEARCH_API_PARALLELS_TIMEOUT` (default 300.0) per request.

**`_HeavySemaphoreState` + `_acquire_heavy_slot()`** — module-level asyncio.Semaphore singleton gating variants/fuzzy/parallels requests. Key design decisions:
- Non-blocking acquire uses direct `sem._value -= 1` check on the event-loop thread (asyncio is single-threaded, no lock needed). `asyncio.wait_for(timeout=0)` was tried and rejected — in Python 3.11 it ALWAYS raises TimeoutError before the coroutine runs even one step, regardless of slot availability.
- Semaphore can be rebuilt when `SEARCH_API_HEAVY_CONCURRENCY` changes AND the semaphore is fully idle (all slots free), so live tuning is safe.
- Slot released in `finally` in both `search_endpoint` and `parallels_endpoint` — timeout/exception cannot strand a slot.
- When no slot is available: raises `APIError('heavy_search_busy', ..., http_status=503, headers={'Retry-After': '5'})`.

**`ERROR_CODES`** gains `'heavy_search_busy'` with inline comment.

**`search_endpoint`** updated:
- `_resolve_search_timeout(req.search_mode)` replaces single `_read_timeout('SEARCH_API_CORE_TIMEOUT', ...)`.
- Heavy-mode gate: if `req.search_mode in HEAVY_SEARCH_MODES`, acquire slot before `run_in_executor`, release in `finally`.
- 504 message now names the ceiling and mode: `f'search did not complete within {core_timeout}s (search_mode={req.search_mode})'`.

**`parallels_endpoint`** updated: the `fetch_parallels_results(...)` call is now wrapped in `_acquire_heavy_slot()` + `asyncio.wait_for(..., timeout=_resolve_parallels_timeout())` with `finally` slot release.

### Task 2: Raise fuzzy result cap

- Pydantic `SearchRequest.limit` field: `le=100` → `le=2000` (FUZZY_HARD_MAX), so fuzzy requests with limit up to 2000 pass validation. Per-mode ceiling enforced in handler.
- `_resolve_fuzzy_max_limit()` — reads `SEARCH_API_FUZZY_MAX_LIMIT` per request, clamped `[1, 2000]`, default 500.
- Handler limit check replaced: `effective_max = _resolve_fuzzy_max_limit() if fuzzy else MAX_LIMIT`. Raises `limit_too_high` (not `invalid_request`) for limit > effective_max.
- Fuzzy recall default: when `search_mode='fuzzy'` AND `limit` absent from raw request body, effective limit widens to `min(_resolve_fuzzy_max_limit(), 250)` before `results[:effective_limit]`.
- `request_echo['limit_effective']` reflects the applied cap for fuzzy.
- `MAX_LIMIT` stays 100; `test_max_limit_lowered_to_100` still passes.

### Task 3: Documentation

- `docs/SEARCH_API.md`: updated fuzzy search_mode + limit rows, error table (new `heavy_search_busy` row, updated `core_timeout` and `limit_too_high` rows), 6 new env-var rows, new "Heavy-Search Tier" subsection, "Deferred Follow-Ups" section.
- `skills/.../api_contract.md`: updated fuzzy note (300s ceiling), limit ceiling (500 for fuzzy), added `heavy_search_busy` to error list.
- `skills/.../search.py`: client `timeout` default 30s → 320s; module docstring + `--limit` help updated.
- `skills/.../parallels.py`: client `timeout` default 60s → 320s.
- `CLAUDE.md`: 5 new env knobs; `SEARCH_API_CORE_TIMEOUT` comment reworded; P9X entry in Recently Changed.
- `CHANGELOG.md`: `[Unreleased]` bullet with tiering, semaphore, cap raise, deferred notes.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] asyncio.wait_for(timeout=0) unusable for non-blocking acquire**
- **Found during:** Task 1 implementation (GREEN phase)
- **Issue:** Plan specified `await asyncio.wait_for(sem.acquire(), timeout=0)` as the non-blocking acquire pattern. In Python 3.11, this ALWAYS raises asyncio.TimeoutError before the coroutine runs a single step, even when the semaphore has free slots. Verified empirically (`asyncio.run` test showed TIMEOUT ERROR with sem._value=2).
- **Fix:** Direct `sem._value > 0` check + `sem._value -= 1` decrement on the event-loop thread. This is safe because asyncio is single-threaded; no lock needed. The plan's docstring note ("comment that this runs on the event loop thread and never blocks it") is preserved.
- **Files modified:** `web/search_api.py`, `tests/test_search_api_v2.py`, `tests/test_parallels_api.py`
- **Commit:** `e4f315af`

**2. [Rule 1 - Bug] Existing tests expected `invalid_request` for limit 101-500 (non-fuzzy)**
- **Found during:** Task 2 GREEN phase verification
- **Issue:** `test_limit_above_max_rejected[101/200/500]` and `test_limit_too_high` expected `invalid_request` (Pydantic Field le=100 rejection). After relaxing to `le=2000`, limits 101-500 pass Pydantic and reach the handler, which correctly raises `limit_too_high`. The behavior (400, reject) is unchanged; only the error code changed.
- **Fix:** Updated `test_limit_above_max_rejected` (renamed parametrize range to [101,200,500]) and `test_limit_too_high` to expect `limit_too_high`. Added `test_limit_above_fuzzy_hard_max_rejected_by_pydantic` to pin the Pydantic-level rejection for limit>2000.
- **Files modified:** `tests/test_search_api_v2.py`, `tests/test_search_api.py`
- **Commit:** `e4f315af`

**3. [Rule 2 - Auto-add] parallels.py skill client timeout also needed updating**
- **Found during:** Task 3 execution
- **Issue:** Plan specified updating `search.py` timeout default (30s→320s). `parallels.py` had a 60s default, which is also below the server's new 300s parallels ceiling. Plan noted "if present and identical in shape — adjust only if present"; the file was present and the issue was identical.
- **Fix:** Updated `parallels.py` client `timeout` default 60s→320s.
- **Files modified:** `skills/cairo-genizah-research/scripts/parallels.py`
- **Commit:** `f1b5b2b2`

## Verification Results

```
pytest tests/test_search_api_v2.py tests/test_parallels_api.py tests/test_search_api.py tests/test_api_hardening.py
218 passed, 7 skipped, 1 warning

PYTHONUTF8=1 python scripts/check_docs.py
All blocking checks passed! Documentation is healthy.

python -m ruff check web/search_api.py shared/api_errors.py skills/.../search.py skills/.../parallels.py
All checks passed!

No import-time caching of new knobs (AST check on module-level assignments)
PASS
```

Key existing tests confirmed green:
- `test_max_limit_lowered_to_100` (MAX_LIMIT==100)
- `test_search_core_timeout_returns_504` (exact mode uses baseline 30s)
- `test_max_limit_unchanged` (new name for same assertion)

## Known Stubs

None — all new knobs are wired end-to-end.

## Self-Check: PASSED

- `e4f315af` exists in git log: confirmed
- `f1b5b2b2` exists in git log: confirmed
- All named source files exist: confirmed
- Test suite green: 218 passed, 0 failed
