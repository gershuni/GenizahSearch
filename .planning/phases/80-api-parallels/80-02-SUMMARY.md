---
phase: 80-api-parallels
plan: 02
subsystem: api
tags: [api, parallels, service-layer, statelessness]
requires: [shared/search_serializer._group_parallels_by_sys_id, web.state.state.searcher, genizah_core.SearchEngine.search_composition_logic]
provides: [shared.parallels_service.ParallelsResultBundle, shared.parallels_service.fetch_parallels_results, shared.parallels_service.PARALLELS_GROUP_CAP]
affects: []
tech_added: []
patterns: [pure-data async fan-out, late-import discipline, run_in_executor for sync CPU-bound core]
key_files_created: [shared/parallels_service.py]
key_files_modified: []
decisions:
  - "Mirrored Phase 79 shared/browse_service.py extraction precedent (D-11)"
  - "max_freq=None substituted with float('inf') sentinel so search_composition_logic length comparison disables high-freq filtering"
  - "_cap_main_results_by_group late-imports the serializer grouping helper so service cap and envelope group identically"
  - "filtered_results explicitly NOT capped in v7.10 (documented twice)"
  - "Boundary defaults echoed in returned boundary_options for skill observability"
metrics:
  duration_minutes: 8
  tasks_completed: 1
  files_created: 1
  files_modified: 0
  lines_added: 250
completed: 2026-05-01
---

# Phase 80 Plan 02: Service-layer Extraction (shared/parallels_service.py) Summary

Created `shared/parallels_service.py` (~250 lines) -- a pure-data async fan-out for `POST /api/parallels` that mirrors Phase 79`s `shared/browse_service.py` extraction precedent (D-11). The service exposes a `ParallelsResultBundle` dataclass and an `async fetch_parallels_results(...)` function with a keyword-only signature `{text, chunk_size, mode, max_freq, boundary_mode, restrict_sys_ids}`, applies the D-07 200-group cap to `main_results` only, and returns raw rows for the route handler (Plan 80-03) to serialize via `shared.search_serializer`.

## What was built

- **ParallelsResultBundle dataclass** with fields `main_results`, `filtered_results`, `boundary_options`, `truncated_to_200: bool = False`.
- **fetch_parallels_results(...) async function**, keyword-only signature exactly: `text`, `chunk_size`, `mode`, `max_freq`, `boundary_mode`, `restrict_sys_ids`. No `uid`, `filters`, `limit`, `filter_text`, or full boundary-options surface (D-02/D-03/D-07/CONTEXT deferred ideas).
- **PARALLELS_GROUP_CAP = 200** module-level constant (no env override in v7.10).
- **_cap_main_results_by_group helper** that late-imports the serializer`s grouping helper plus `web.state` so cap and envelope group identically; sorts groups desc by aggregate_score; takes top 200; flattens kept groups back to row order.
- **_run_sync helper** wrapping `loop.run_in_executor` (with `functools.partial` kwargs path) -- same pattern as Phase 79.
- **Sentinel substitution**: `max_freq=None` to `float('inf')` before passing to the core fn, so the `len(hits) > max_freq` comparison disables high-freq filtering instead of raising on None.
- **Boundary echo**: boundary_options returned with all 5 boundary parameters (mode + 4 core defaults) so the skill consumer can observe what was used.

## D-07 group cap behavior

- Raw main result group count <= 200: return main_results unchanged, `truncated_to_200=False`.
- Raw main result group count > 200: group by sys_id (via serializer helper), sort by aggregate_score desc, keep top 200 groups, flatten to row list, `truncated_to_200=True`.
- Plan 80-03`s handler is responsible for appending `truncated_to_200` to `warnings[]` when the flag is True.
- filtered_results is **explicitly NOT capped** in v7.10. Documented in both module docstring and fetch body comment as an explicit decision. Rationale: filtered_results is driven by user`s max_freq threshold and is typically small.

## Statelessness contract verified

Grep contract enforced:
- last-results / parallels-results / current-search-query / app-storage / request-cookies tokens: 0 matches.
- nicegui imports / web.pages / web.components imports: 0 matches.
- serializer dispatch token: 0 matches (handler is sole caller).
- Lab Engine token: 0 matches (D-02 -- out of scope).

Service reaches through to the process-singleton SearchEngine and meta_mgr (via late imports from web.state) -- no per-session UI state touched.

## Why run_in_executor?

`SearchEngine.search_composition_logic` is synchronous and CPU-bound (chunk hashing + Tantivy seeks + variant expansion + boundary scoring). Running it inline on the event loop would block all other requests for the duration. The `_run_sync` wrapper offloads to the default thread executor, identical to Phase 79`s `_fetch_core` pattern. Per CONTEXT R-09 (inherited): no per-call timeout -- the rate limiter is the v7.10 load shield; Phase 81+ may add an explicit composition timeout if observed needed.

## Deviations from Plan

**[Rule 3 - Blocking issue] Reworded docstrings to satisfy grep contracts**

The plan`s source-code template inlined the literal token strings (`state.last_results`, `state.parallels_results`, `state.current_search_query`, `app.storage`, `request.cookies`, `serialize_parallels_payload`, `lab_composition_search`, `import nicegui`) inside the module docstring as part of the statelessness narrative. The plan`s own acceptance criteria require those exact substrings to grep to **zero** in the file. Template-as-written failed the grep contract.

- **Fix**: paraphrased the docstring narrative to describe constraints without naming the forbidden tokens. State-list collapsed to hyphen-separated phrases ("last-results caches", "browser-storage", "request-cookies"). Serializer reference reworded to "the parallels payload serializer in shared.search_serializer". Lab Engine token replaced with "(out of scope; see D-02)". "does NOT import nicegui" replaced with "no UI-framework dependency".
- **Files modified**: `shared/parallels_service.py` (docstrings only -- no behavior change).
- **Verification**: all grep contracts pass post-fix; functional verification block prints `OK`; Phase 77/78/79 suites GREEN.
- **Commit**: `c910b8f6` (single commit covering create + docstring rewording, applied atomically).

## Verification

- Plan`s Task 1 verify Python block prints `OK` (public exports, dataclass fields, keyword-only signature, forbidden-param absence, cap behavior at 200/250, integration smoke with fake searcher).
- All grep acceptance criteria pass.
- Phase 77/78/79 test suites: tests/test_search_api.py tests/test_api_hardening.py tests/test_api_legacy_unchanged.py tests/test_browse_api.py tests/test_search_serializer.py: **146 passed, 1 skipped**.

## Commits

- `c910b8f6` -- `feat(80-02): add shared/parallels_service.py for /api/parallels` (1 file changed, 250 insertions)

## Self-Check: PASSED

- File exists: `shared/parallels_service.py` (250 lines)
- Commit exists: `c910b8f6`
- Phase 77/78/79 tests: 146 passed, 1 skipped
- Grep contracts: ALL OK
- Functional smoke: FUNCTIONAL OK
