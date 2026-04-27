---
phase: 77-serializer-json-export
plan: 01
subsystem: search-api
tags: [search-api, serializer, state-bugfix, test-scaffolding, tdd-red, envelope-echo]

# Dependency graph
requires:
  - phase: 75 (web-decomp)
    provides: page-scoped search_state and p_state already isolated, making the global-state mirror clean to introduce
provides:
  - 5 new AppState fields (current_search_mode, current_search_gap, last_filters_applied, last_search_warnings, parallels_search_meta) populated at execute-time
  - State-population blocks at 3 search-execute paths (happy 4077, cancelled 4026, history-restore 3740) and 3 parallels paths (success 2182, history 1894, reset 1944)
  - 22 RED tests in tests/test_search_serializer.py defining the contract for shared/search_serializer.py
  - Side-effect bugfix: state.current_search_query latent bug (declared but never assigned) now fixed at all 3 search paths — Excel/Word filenames will produce meaningful filenames
affects: [77-02 (lab_composition_search chunk_hits), 77-03 (search_serializer GREEN), 77-04 (api download handlers), 78-* (api/search uses same envelope shape)]

# Tech tracking
tech-stack:
  added: []  # No new libraries; pure refactoring of existing AppState
  patterns:
    - "Global-state mirror for stateful FastAPI handlers (extends existing state.last_results pattern)"
    - "10-key filter dict shape mirroring search.py:4232-4242 live snapshot for envelope replay"
    - "Wave 0 RED test scaffolding: write failing tests before implementation module exists"

key-files:
  created:
    - tests/test_search_serializer.py
  modified:
    - web/state.py
    - web/pages/search.py
    - web/pages/parallels.py

key-decisions:
  - "Filter dict shape locked to 10 keys matching live snapshots (HIGH-02 fix — smaller 6-key dict was incomplete and would not survive replay)"
  - "Search history restore (3740) extends to populate global state.last_results AND envelope-echo fields, not just search_state.results — restored exports are identical-shape to live exports (HIGH-01)"
  - "Parallels history restore (1894) uses state_snapshot['source_text'] + params dict as canonical source, NOT inferred from p_state.results[0]['source_ctx'] (HIGH-03 — result rows lose chunk_size/mode/filters fidelity)"
  - "warnings array communicates path-of-origin: ['partial-results'] for cancelled-path, ['restored-from-history'] for history restores; [] for happy path"
  - "RED test scaffolding deliberately fails with ModuleNotFoundError for shared.search_serializer — Plan 03 will turn them GREEN by creating the module"

patterns-established:
  - "Page-scoped state → global-state mirror pattern: assign at every execute-time path (not just happy path) so stateful download handlers never see stale envelope-echo data"
  - "Wave 0 TDD pattern: tests live in tests/ before the implementation module exists; pytest collection passes (file is syntactically valid) but each test fails import"
  - "Defensive list() copies on mutable filter values prevent mutation-after-search bugs"

requirements-completed: [EXPORT-01, EXPORT-02, EXPORT-03, EXPORT-04]

# Metrics
duration: 8min
completed: 2026-04-27
---

# Phase 77 Plan 01: Serializer Foundation Summary

**Five new AppState fields populated at six execute-time sites + 22-test RED scaffold for shared.search_serializer; latent state.current_search_query bug fixed as a side effect at all three search-execute paths.**

## Performance

- **Duration:** ~8 min (16:41:32Z → 16:49:18Z)
- **Started:** 2026-04-27T16:41:32Z
- **Completed:** 2026-04-27T16:49:18Z
- **Tasks:** 3
- **Files modified:** 4

## Accomplishments

- **5 new AppState fields** (`current_search_mode`, `current_search_gap`, `last_filters_applied`, `last_search_warnings`, `parallels_search_meta`) added to `web/state.py` with documented defaults
- **6 state-population sites** populated at execute-time:
  - search.py: happy path (4077), cancelled-partial (4026), history restore (3740)
  - parallels.py: success path (2182), history restore (1894), reset clears (1944)
- **22 RED tests** in `tests/test_search_serializer.py` covering EXPORT-01..04 + D-03/04/05/07/09/10/11/13 + locator-round-trip readiness, all failing with `ModuleNotFoundError: No module named 'shared.search_serializer'` (Plan 03 GREEN target)
- **Latent bug fixed** as a side effect: `state.current_search_query` was declared at `web/state.py:27` but never assigned — Excel/Word filenames will now produce meaningful filenames at all three search-execute paths instead of defaulting to `genizah.xlsx`
- **No regression**: full pytest suite still 1162 passed, 8 skipped (matches baseline), excluding the intentionally-RED `tests/test_search_serializer.py`

## Task Commits

Each task was committed atomically:

1. **Task 1: Extend AppState with 5 fields** — `cdd91928` (feat)
2. **Task 2: Populate envelope-echo state at 6 sites** — `2c5e94d5` (feat)
3. **Task 3: Wave 0 RED test scaffolding (22 tests)** — `d64ccb2b` (test)

## Files Created/Modified

- `web/state.py` — Added 5 new AppState fields with type hints and inline rationale comments referencing 77-RESEARCH §Pitfall 2 and the page-scoped → global-state mirror pattern
- `web/pages/search.py` — Three state-population blocks: happy path (~4077), cancelled-partial (~4026), search-history restore (~3740). Each mirrors the 10-key live filter snapshot at search.py:4232-4242. History restore uses `entry.get('query', '')` and `params.get(...)` from the snapshot rather than reading from `query_input.value` (which has not yet been re-rendered at restore time).
- `web/pages/parallels.py` — Three state-population sites: success path (~2182) using captured locals (`captured_chunk_size`, `captured_freq_threshold`, `captured_mode`, `text_input.value`), history restore (~1894) using `state_snapshot.get('source_text', '')` + `params` dict (HIGH-03 fix), and `_reset_parallels` (~1944) clearing `state.parallels_search_meta = None`
- `tests/test_search_serializer.py` (NEW, 554 lines) — 22 RED tests + 8 fixtures (`mock_meta_mgr`, `sample_search_results`, `oxford_only_hit`, `sample_parallels_results`, `sample_parallels_filtered`, `metadata_only_hit`, autouse `_disable_fjms`)

## Decisions Made

- **Filter dict shape:** Locked to 10 keys verbatim matching the live snapshot at `web/pages/search.py:4232-4242`. Earlier plan revisions used 6 keys; HIGH-02 review feedback caught that this would lose `include_mode`, text filters, and `material_exclude` and break envelope replay.
- **Defensive list() copies:** All list-typed filter values are copied at population time so subsequent UI mutation (filter tweak between search and export) doesn't corrupt the envelope-echo snapshot.
- **Warnings as origin signal:** `warnings = []` for happy path, `['partial-results']` for cancelled-path, `['restored-from-history']` for both search and parallels restores. Plan 03's serializer can surface these to consumers; downstream phases may add more signals.
- **History-restore fidelity:** The search-history restore now also writes to `state.last_results`, which makes restored exports byte-identical-shape to live exports. Same pattern applied to parallels.
- **No `time.sleep` in test scaffold:** HIGH-06 review feedback insisted the filename-uniqueness unit test must NOT sleep. The test calls `build_search_filename()` twice consecutively and asserts they differ — this forces Plan 03 to use millisecond resolution (or counter/random suffix), not second resolution. The original docstring referenced `time.sleep(1.0)` literally; reworded to "a one-second wait" to keep the file's grep -c "time.sleep" at exactly 0.

## Deviations from Plan

None — plan executed exactly as written.

The only minor edit beyond the plan's literal action was rewording one docstring sentence in `tests/test_search_serializer.py` from `time.sleep(1.0)` to "a one-second wait" so that `grep -c "time\.sleep" tests/test_search_serializer.py` returns 0 per the strict acceptance criterion. The behavioral content of the test is unchanged.

## Issues Encountered

None. All edits applied cleanly with no anchor-line drift. The plan's line references (search.py 4077/4026/3740, parallels.py 2182/1894/1942-1943) all matched the live file state exactly.

## User Setup Required

None — no external service configuration required.

## Next Phase Readiness

**Plan 02 (lab_composition_search chunk_hits) is now unblocked:**
- The parallels success-path state population in this plan does not yet read `chunk_hits` from result rows — Plan 02 extends `lab_composition_search` to populate that field per uid (D-13 Path A locked).
- Test fixtures `sample_parallels_results` already include `chunk_hits` per the Path A contract; Plan 03 will consume them when it implements `serialize_parallels_payload`.

**Plan 03 (shared/search_serializer.py GREEN) is unblocked:**
- 22 failing tests define the exact contract: schema_version=1, envelope shape, locator dict with `{sys_id, volume_ie, p_num}`, image_url server-relative or null (Oxford-only must be null per HIGH-07), domains as plural list, score rounded to 4 decimals, parallels grouped by manuscript with matches[] and SUM-aggregated score, single `_serialize_item` shared by both serializers, filename uniqueness without sleep.

**Plan 04 (api download handlers) is unblocked:**
- All envelope-echo state the JSON download handlers need is now populated at execute-time. Adding handlers in `web/api.py` becomes a pure read-from-state-singleton operation — no new state-management work required.

**Cross-plan invariant established:**
- The `state.current_search_query` latent bug is fixed at every search-execute path, so Excel/Word filename improvements are an immediate ride-along benefit independent of the JSON export work. No regression risk to existing handlers.

## Self-Check: PASSED

- File `web/state.py` modified — verified: 5 new fields present at lines 33–36 + 44 (`grep -n` confirms each).
- File `web/pages/search.py` modified — verified: `current_search_query = clean_query` count 2, `current_search_query = entry.get` count 1, `last_filters_applied = ` count 3, `'partial-results'` count 1, `'restored-from-history'` count 1.
- File `web/pages/parallels.py` modified — verified: `parallels_search_meta = {` count 2, `parallels_search_meta = None` count 1, `'restored-from-history'` count 1, `state_snapshot.get('source_text'` count 2 (existing line 1837 + new line 1894).
- File `tests/test_search_serializer.py` created — verified: 22 test functions, 21 `from shared.search_serializer import` lines, Oxford-only test present, filename-uniqueness test present, `time.sleep` count 0.
- Commits exist: `cdd91928` (Task 1), `2c5e94d5` (Task 2), `d64ccb2b` (Task 3) — verified via `git log --oneline HEAD~3..HEAD`.
- pytest collection on new file succeeds: `22 tests collected in 0.04s`.
- pytest baseline excluding new file: `1162 passed, 8 skipped` — matches pre-plan baseline exactly.

---
*Phase: 77-serializer-json-export*
*Completed: 2026-04-27*
