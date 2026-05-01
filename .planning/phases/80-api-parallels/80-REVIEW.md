---
phase: 80-api-parallels
reviewed: 2026-05-01T00:00:00Z
depth: standard
files_reviewed: 4
files_reviewed_list:
  - shared/api_errors.py
  - shared/parallels_service.py
  - web/search_api.py
  - tests/test_parallels_api.py
findings:
  critical: 0
  warning: 0
  info: 4
  total: 4
status: clean
---

# Phase 80: Code Review Report

**Reviewed:** 2026-05-01
**Depth:** standard
**Files Reviewed:** 4 source files (CLAUDE.md env-var docs not reviewed as source)
**Status:** clean (no blockers / highs / mediums; 4 informational notes)

## Summary

Phase 80 (`POST /api/parallels`) cleanly inherits the hardening pattern established in Phase 78 (`/api/search`) and consolidated by Phase 79's `wrap_endpoint` decorator (R2-#6). The endpoint reuses the decorator verbatim, so try/except/finally, envelope rewriting, PostHog capture, and IP resolution are not re-implemented. New surface (`ParallelsRequest`, `_parallels_rate_limiter`, `COMPOSITION_LENGTH_CAP`, `composition_required` / `composition_too_long` errors, `truncated_to_200` warning, `shared/parallels_service.py`) all conform to the existing taxonomy.

All twelve specific concerns called out in the review prompt were checked and pass:

1. **Statelessness D-20** — clean. `parallels_endpoint` body and `shared/parallels_service.py` touch no `state.last_results`, `state.parallels_results`, `state.current_search_query`, `app.storage`, or `request.cookies`. The only `state` reads are the read-only process-singleton `state.searcher` and `state.meta_mgr`.
2. **Concern #2 lock (no global exception handlers)** — clean. `init_search_api` registers routes only; no `target_app.add_exception_handler(...)` or `@target_app.exception_handler(...)` calls anywhere in `web/search_api.py`. Envelope rewriting stays inside `wrap_endpoint`.
3. **Three identity-distinct RateLimiter instances** — `web/search_api.py:64,71,78` instantiate three `RateLimiter(default_limit=30)` objects bound to `_rate_limiter`, `_browse_rate_limiter`, `_parallels_rate_limiter`. Independence is asserted by `test_parallels_rate_limit_independence` (burst on parallels does not affect search/browse buckets).
4. **Length-cap enforcement order** — `web/search_api.py:769-783` strips and length-caps text BEFORE filter resolution (step 4) and BEFORE the service-layer `fetch_parallels_results` call (step 6). Composition cannot reach `search_composition_logic` without passing `0 < len(text.strip()) <= 20000`.
5. **Filter pipeline mirrors Phase 78 verbatim** — `web/search_api.py:792-805` is line-for-line equivalent to `web/search_api.py:462-481` (search endpoint), including the `from shared import fjms_service as _fjms_module` late-bind so test fixtures can monkeypatch `validate_filter_values` / `get_filter_sys_ids`.
6. **Composition text never logged; PostHog payload field set is fixed** — `web/api_hardening.py:572-621` (`capture_api_event`) emits only a fixed dict: `endpoint, mode, latency_bucket, status_code, error_code, result_count_bucket`. No path in `parallels_endpoint` adds the composition text to logs or PostHog. `captured_state` carries only `mode` (the locked enum) and `result_count` (an int).
7. **Lab Engine path not reachable** — `ParallelsRequest.mode` is `Literal['exact', 'variants', 'fuzzy']` (`web/search_api.py:185`); Pydantic rejects any other value at parse time with `invalid_request`. The string is forwarded verbatim to `search_composition_logic(mode=...)`, which has no special-case for `'lab'` in this code path.
8. **Pydantic `extra='forbid'` and locked enums** — both `ParallelsRequest` (line 181) and the existing `FiltersModel` (line 102) set `model_config = ConfigDict(extra='forbid')`. `chunk_size` is bounded `Field(default=5, ge=2, le=20)`; `mode` and `boundary_mode` are `Literal[...]`. Verified by `test_parallels_extra_field_rejected`, `test_parallels_unknown_mode`, `test_parallels_unknown_boundary_mode`, `test_parallels_chunk_size_too_low/high`.
9. **Group-cap correctness** — `shared/parallels_service.py:97-148` (`_cap_main_results_by_group`) groups via the same `_group_parallels_by_sys_id` helper the serializer uses (so cap-time and envelope-time groupings cannot diverge), sorts desc by `aggregate_score`, takes top 200, flattens. Edge cases handled: empty input (returns `[], False`), groups <= cap (returns input unchanged, `False`), groups > cap (`True` flag). The handler appends `'truncated_to_200'` to `warnings_list` (`web/search_api.py:835-836`) per D-07 contract.
10. **async/await correctness; `run_in_executor` for sync core** — `_run_sync` (`shared/parallels_service.py:82-94`) wraps the blocking `search_composition_logic` call. The handler awaits `fetch_parallels_results`, which awaits `_run_sync`. The R-09 docstring acknowledges that `asyncio.wait_for` would not actually cancel the executor thread; v7.10 deliberately omits a fan-out timeout (rate limiter is the load shield).
11. **Error envelope shape consistency** — all error paths raise `APIError(code, message, http_status=...)` from `shared.api_errors`. The decorator's `_build_envelope_response` is the sole producer; envelope shape is asserted by `test_parallels_error_envelope_shape`.
12. **Race conditions / unbounded resource use / exception leakage** — no shared mutable state introduced. Service module is pure-function async over a process-singleton `SearchEngine`. Exceptions in the executor surface as awaited exceptions; the decorator's `except Exception` clause prevents leakage and produces a 500 envelope.

Test suite is healthy: 1384 passed / 10 skipped after Phase 80 (1345 baseline + 39 new), per the prompt. The new test file covers happy paths across `mode x boundary_mode` (6), validation including length-cap boundaries at exactly-cap / cap+1 / whitespace-stripped (10), filter rejection, mode gate, three-bucket independence, group cap at 250 → 200 + warning, filtered-results uncapped at 250, statelessness, and a regex-based assertion that the handler body has no hand-rolled `capture_api_event` / `t0 = time.monotonic()` boilerplate.

No blocker, high, or medium issues found. Four informational notes follow.

## Info

### IN-01: Docstring inconsistency between cap aggregation and grouper aggregation

**File:** `shared/parallels_service.py:113-114, 130-139`
**Issue:** The `_cap_main_results_by_group` docstring states "sorts groups desc by aggregate_score (sum of per-row final_score within the group)". The defensive recompute in `_agg` (line 137) also prefers `final_score`. However, `_group_parallels_by_sys_id` (`shared/search_serializer.py:667`) actually sums `item.get('score', 0.0)`, not `final_score`. In practice the defensive path never fires (every group dict from the grouper has `aggregate_score` set as a float), so behavior is correct — but the docstring claim and the fallback both say `final_score` while the realised value is `score`. Minor documentation drift; no functional impact.
**Fix:** Either (a) update docstring + `_agg` fallback to say `score` to match the grouper, or (b) push a `final_score`-aware version into `_group_parallels_by_sys_id` so cap-time and envelope-time aggregates agree if anyone ever inspects `aggregate_score` from outside. (a) is the smaller change and matches existing v7.10 behavior.

### IN-02: `asyncio.get_event_loop()` is deprecated inside coroutines (Python 3.10+)

**File:** `shared/parallels_service.py:89`
**Issue:** `loop = asyncio.get_event_loop()` is called from inside an `async def` (`_run_sync`). Since Python 3.10, `get_event_loop()` emits `DeprecationWarning` when there is no running loop, and 3.12+ may eventually error. Inside a coroutine, the modern idiom is `asyncio.get_running_loop()`, which is also faster (no fallback path).
**Fix:**
```python
loop = asyncio.get_running_loop()
```
The semantics are identical for this call site (it's only ever invoked from within an awaited coroutine).

### IN-03: `_run_sync` kwargs branch is dead code

**File:** `shared/parallels_service.py:90-93`
**Issue:** `_run_sync` supports `**kwargs` via `functools.partial`, but the only caller (`fetch_parallels_results._sync_call`) is invoked as `_run_sync(_sync_call)` with no kwargs. The branch is reachable only by future callers. Not a defect — it's defensive — but it slightly inflates the API surface and the `from functools import partial` import is unused on every actual call.
**Fix:** Either (a) drop the kwargs branch and the local `partial` import for now (YAGNI), or (b) leave it and add a unit test that exercises the kwargs path so regressions surface. Recommend (a) for v7.10 and reintroduce when a second caller appears.

### IN-04: `chunk_size` validation message lumped under generic `invalid_request`

**File:** `web/search_api.py:184` (Pydantic `Field(default=5, ge=2, le=20)`)
**Issue:** Out-of-bounds `chunk_size` produces `code: invalid_request` with a generic Pydantic message rather than a dedicated taxonomy code (e.g. `chunk_size_out_of_range`). Tests `test_parallels_chunk_size_too_low/high` assert `invalid_request`, so this is the intended contract — but skill consumers branching on `code` cannot distinguish chunk-size errors from missing-text or extra-field errors without parsing `message`. Phase 78 made the same trade-off for `limit` vs `limit_too_high`, where `limit_too_high` got its own code because it was a likely user-facing error. Composition-mode UIs typically hard-code `chunk_size` so per-user errors are rare; v7.10 trade-off is reasonable.
**Fix:** No change needed for v7.10. If Phase 81's skill consumer wants finer error categorization, add a dedicated `chunk_size_out_of_range` code to `ERROR_CODES` and replace the Pydantic `Field(ge=..., le=...)` with explicit handler-side validation that raises `APIError('chunk_size_out_of_range', ...)`. Note this would be a contract change (test rewrite required).

---

_Reviewed: 2026-05-01_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
