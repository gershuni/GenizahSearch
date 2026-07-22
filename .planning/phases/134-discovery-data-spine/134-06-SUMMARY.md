---
phase: 134-discovery-data-spine
plan: 06
subsystem: database
tags: [asyncio, sqlite, discovery-sidecar, chokepoint, event-loop-safety, tdd]

# Dependency graph
requires:
  - phase: 134-01
    provides: "FROZEN docs/specs/discovery-sidecar-schema-v1.md two-table claim model + enum vocab"
  - phase: 134-03
    provides: "tests/fixtures/discovery/discovery-v1-fixture.db + manifest.json (masking-safe golden fixture) + scripts/build_discovery_sidecar.create_schema/populate_synthetic"
  - phase: 134-05
    provides: "web/discovery_assets.py: discovery_available()/discovery_db_path()/discovery_sidecar_version() -- live callable + LAZY providers, read at call time"
provides:
  - "shared/discovery_service.py: the ONE async read-only DiscoveryService chokepoint (lazy versioned conn, timeouts via asyncio.wait, bounded heavy-query semaphore with add_done_callback release, version-keyed browse LRU, server-side pagination, DATA-10 unit x work projection)"
  - "shared/discovery_errors.py: DiscoveryUnavailable/DiscoveryOverload (stdlib-only, web-free)"
  - "web/discovery.py: web composition wiring DiscoveryService to the LIVE discovery_available callable + LAZY path/version providers -- import-before-load safe, no DB opened at import, no route/UI"
  - "tests/test_no_back_edges_discovery.py: NEW essential AST guard banning module-level web/nicegui/fastapi imports in the discovery service layer"
  - "tests/test_discovery_service.py (33 tests) + tests/test_discovery_composition.py (5 tests)"
affects: ["135 (BAND-01..05/CERT-01/02 will read through this service)", "136 (PANEL-01..03/WORK-01/02 read surfaces call web/discovery.py's pass-throughs)", "138 (leads queue reads via get_work_witnesses-style pagination)"]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Off-event-loop dispatch: loop.run_in_executor(...) wrapped in asyncio.wait({fut}, timeout=...) -- NEVER asyncio.wait_for over run_in_executor (executor threads are not cancellable); a timeout raises DiscoveryUnavailable without awaiting the abandoned future. Mirrors web/search_api.py lines 1104-1166 verbatim, including the add_done_callback slot-release rationale."
    - "Non-blocking bounded heavy-query semaphore (per-DiscoveryService-instance, not module-level, since the service instance IS the natural single-owner scope): sem.locked() -> raise DiscoveryOverload immediately; slot released ONLY from the future's add_done_callback, never a bare finally, so a timed-out-but-still-running thread cannot let a second call re-admit new heavy work (DC6)."
    - "Lazy, version-keyed ThreadLocalConnection: built on FIRST available call (never in __init__), rebuilt whenever the resolved path OR sidecar version changes, closing the prior pool via .close() first (R8, no leaked per-thread handles)."
    - "Version-keyed browse-enrichment LRU: an OrderedDict cache keyed by (method, args, sidecar_version) so a version swap always misses the old entries (F15) instead of silently serving stale rows."
    - "DATA-10 projection factored into a PURE, DB-free helper (_project_work_witnesses) separate from the DB-touching get_work_witnesses -- lets the tricky 'highest band wins across DIFFERING member bands' rule be unit-tested directly with fabricated data, since the shared golden fixture's own two merged units happen to have identical per-member bands and can't exercise that case alone."

key-files:
  created:
    - shared/discovery_service.py
    - shared/discovery_errors.py
    - web/discovery.py
    - tests/test_no_back_edges_discovery.py
    - tests/test_discovery_service.py
    - tests/test_discovery_composition.py
  modified: []

key-decisions:
  - "Task 1's GREEN commit already contains the FULL async wrapper implementation (loop.run_in_executor/asyncio.wait/_acquire_heavy_slot/_browse_cached_call/*_async methods) that the plan scoped to Task 2, because the sync core and its async wrappers are one tightly-coupled class in one file -- splitting them across two separate implementation commits would have meant either a broken intermediate state (a DiscoveryService with only sync methods, unusable by anything calling the async API) or an artificial revert-then-reapply dance. Task 1's RED/GREEN cycle was still genuine (the back-edge guard test was proven to fail with the modules absent, then pass once both files were restored). Task 2's TDD contribution is its OWN comprehensive test file (33 tests) proving every Task-2-scoped invariant (timeout/asyncio.wait, overload, DC6 slot-recycling, versioned LRU, DATA-10) against the already-implemented code -- documented here rather than silently glossed over."
  - "The DATA-10 'unit x work projection' grouping/filtering/sorting/pagination logic is a PURE function (_project_work_witnesses) taking plain dicts, not the DB-touching get_work_witnesses method directly -- this let the 'highest band wins when members carry DIFFERENT bands' rule be proven with fabricated data (the shared golden fixture's two witness_units each happen to have all-identical per-member bands, so it alone cannot exercise that path)."
  - "enabled_bands filters on the claim's confidence_band string (not the (evidence_source, confidence_band) pair) -- band names never collide across evidence_source (track1_direct vs propagated bands are disjoint strings), and a per-band UI toggle naturally operates on the band name alone."
  - "The heavy-query concurrency semaphore is PER DiscoveryService INSTANCE (not a module-level singleton like web/search_api.py's _HeavySemaphoreState) -- in production there is exactly one module-level DiscoveryService instance (web/discovery.py's _service), so this behaves identically to a module-level semaphore in practice, while letting tests construct a fresh, isolated DiscoveryService per test without any module-global state bleeding between tests."
  - "get_work_witnesses restricts to claim_type IN ('direct_witness', 'quotes_this_work') -- a claim_type=shared_text row (e.g. a family-router citation) is never a 'witness' of the work by definition (C-2/schema SS3), so it is excluded from the projection entirely rather than assigned some synthetic band; proven by test_data10_integration_excludes_shared_text_only_family_router_claim against the fixture's p010/w000008 router row."
  - "get_pages_related_to_page does not filter by routing_status -- the schema/plan text does not scope PANEL-02's 'pages related to this page' to shipped-only rows, so a review_only family-router alignment still surfaces (a future Phase 136 UI can filter further if desired); this is a Claude's-Discretion call, not a contract requirement, and is safe to revisit."

patterns-established:
  - "shared/*_service.py async chokepoint pattern for future sidecars: injected lazy providers in __init__ (never touching the DB), a lazy _get_conn() that rebuilds on path/version drift and closes the prior pool, sync methods following the graceful-absent shape, and async wrappers that dispatch via run_in_executor + asyncio.wait (never wait_for)."
  - "Pure DB-free helper functions for any tricky business-logic rule (here: the DATA-10 projection) so the rule can be unit-tested with fabricated edge cases the golden fixture doesn't happen to cover."

requirements-completed: [DATA-06, DATA-10]

# Metrics
duration: 70min
completed: 2026-07-22
---

# Phase 134 Plan 6: Discovery Async Service Chokepoint Summary

**The single async `DiscoveryService` chokepoint over `discovery.db` -- lazy versioned connection, `asyncio.wait`-based per-query timeouts, a non-blocking bounded heavy-query semaphore releasing only via `add_done_callback`, a version-keyed browse-enrichment LRU, server-side pagination, and the DATA-10 unit x work projection -- plus the web-free back-edge guard and the `web/discovery.py` composition shim (no UI).**

## Performance

- **Duration:** ~70 min
- **Tasks:** 3 (Tasks 1-2 `tdd="true"`, Task 3 `type="auto"`)
- **Files created:** 6 (0 modified)

## Accomplishments

- `shared/discovery_errors.py` -- `DiscoveryUnavailable` (+ `DiscoveryOverload` subclass), stdlib-only, web-free.
- `shared/discovery_service.py` -- the full `DiscoveryService` chokepoint:
  - `__init__` takes injected LAZY providers and touches nothing on disk (F15).
  - `_get_conn()` builds a `ThreadLocalConnection(mode=ro)` on first available call, rebuilding (and `.close()`-ing the prior pool) whenever the resolved path or sidecar version changes (R8).
  - `get_version`/`get_claims_for_page`/`get_pages_related_to_page`/`get_evidence`/`get_work_witnesses` follow the fjms_service graceful-absent shape -- every read returns `[]`/`None` on error or unavailability, never raises.
  - `get_work_witnesses` + the pure `_project_work_witnesses` helper implement the DATA-10 unit x work projection: a witness_unit is shown once at its highest member band, the enabled-bands filter acts on that displayed band BEFORE pagination, the anchor's own unit is excluded, and same-unit members are suppressed.
  - Async wrappers (`*_async`) dispatch via `loop.run_in_executor` wrapped in `asyncio.wait({fut}, timeout=...)` -- never `wait_for` -- raising `DiscoveryUnavailable` on timeout without awaiting the abandoned future.
  - `get_work_witnesses_async` is gated behind a non-blocking bounded semaphore (`DiscoveryOverload` when full); the slot is released only from the future's `add_done_callback`, so a timed-out-but-still-running thread cannot let a second call re-admit new heavy work (DC6).
  - `get_claims_for_page_async`/`get_pages_related_to_page_async` are wrapped in a version-keyed LRU (`DISCOVERY_BROWSE_LRU_MAX_ENTRIES`, default 5,000) so a sidecar version bump never serves stale cached rows.
  - All timeout/concurrency/LRU/page-size defaults are read from env vars per the `docs/specs/discovery-budgets.md` naming convention (`DISCOVERY_QUERY_TIMEOUT_BROWSE=2.0`, `DISCOVERY_QUERY_TIMEOUT_WORK=5.0`, `DISCOVERY_MAX_CONCURRENT_QUERIES=4`, `DISCOVERY_BROWSE_LRU_MAX_ENTRIES=5000`, `DISCOVERY_PAGE_SIZE_DEFAULT=50`, `DISCOVERY_PAGE_SIZE_MAX=200`), re-read per call (never baked in at import).
- `web/discovery.py` -- a module-level `DiscoveryService` composed with the LIVE `discovery_available` callable and LAZY `discovery_db_path`/`discovery_sidecar_version` providers (all read at call time); thin async pass-throughs (`get_version`/`get_claims_for_page`/`get_pages_related_to_page`/`get_evidence`/`get_work_witnesses`) that check `discovery_available()` first and fail open to `[]`/`None` on `DiscoveryUnavailable`. No route/page/nav added.
- `tests/test_no_back_edges_discovery.py` -- a genuinely NEW essential AST guard (7 tests) banning module-level `web`/`nicegui`/`fastapi` imports (via both `ast.Import` and `ast.ImportFrom`, including guarded top-level `try:`/`if:` imports) in `shared/discovery_service.py` + `shared/discovery_errors.py`; proven with a positive lazy-function-body-import exclusion test and a negative guarded-top-level-import catch test.
- `tests/test_discovery_service.py` -- 33 tests: lazy/versioned connection + old-pool-closed (R8), graceful-absent reads, pagination bounds, `asyncio.wait` timeout behavior (loop stays responsive), `DiscoveryOverload` on a full heavy semaphore, DC6 slot-recycling, version-keyed LRU (hit + version-bump invalidation + bounded size), and the DATA-10 projection both as a pure `_project_work_witnesses` unit test (incl. the differing-band-within-unit case the shared fixture cannot exercise alone) and as an integration test against the 134-03 golden fixture (oxford_part unit, physical_join unit, anchor exclusion, the deliberately-unmerged "same scribe" pair, and the family-router shared_text-only claim correctly excluded from the witness list).
- `tests/test_discovery_composition.py` -- 5 tests: import-before-load then load-fixture then query returns rows + correct version (DC12/F1); `discovery_available()` False (flag off, or sidecar absent) makes every pass-through a clean no-op; no UI/route added.

## Task Commits

1. **Task 1 RED: failing back-edge guard test** - `b504d276` (test)
2. **Task 1 GREEN: DiscoveryService sync core (lazy conn) + DATA-10 projection + discovery_errors.py** - `4a98ee9f` (feat)
3. **Task 2: async wrappers test suite (33 tests)** - `bfbfeb43` (test) -- *the async wrapper implementation itself was authored together with the Task 1 sync core in the prior commit; see "Deviations from Plan" below*
4. **Task 3: web/discovery.py composition + composition test suite (5 tests)** - `24fca51d` (feat)

## Files Created/Modified

- `shared/discovery_errors.py` - `DiscoveryUnavailable`/`DiscoveryOverload`, stdlib-only, web-free
- `shared/discovery_service.py` - the full async `DiscoveryService` chokepoint (sync core + async wrappers + DATA-10 projection helper)
- `web/discovery.py` - web composition (live availability callable + lazy path/version providers), no UI
- `tests/test_no_back_edges_discovery.py` - NEW essential back-edge guard (7 tests)
- `tests/test_discovery_service.py` - 33 tests covering every DATA-06/DATA-10 invariant
- `tests/test_discovery_composition.py` - 5 tests covering import-before-load + fail-open composition

## Decisions Made

See `key-decisions` in frontmatter for the 6 substantive design decisions (async-implementation-in-Task-1 process note; the pure DATA-10 projection helper; band-string filtering; per-instance heavy semaphore; claim_type-scoped witness filtering; unrestricted-by-routing_status related-pages read).

## Deviations from Plan

### Process deviation (not a Rule 1-4 auto-fix, but documented per the plan's TDD instruction)

**1. Task 2's async wrapper implementation was authored together with Task 1's sync core**
- **Found during:** Task 1 (writing `shared/discovery_service.py`)
- **Issue:** The plan splits `shared/discovery_service.py` across two `tdd="true"` tasks -- Task 1 (sync core + DATA-10 projection + back-edge guard) and Task 2 (async wrappers + overload/slot-recycling tests) -- but the sync methods and their async wrappers are one tightly-coupled class in one file. Writing only the sync half in Task 1 and leaving `*_async`/`_run_off_loop`/`_acquire_heavy_slot`/`_browse_cached_call` for a SEPARATE Task-2 commit would have required either shipping a temporarily-broken/partial class (a `DiscoveryService` with no usable async API, which nothing in the plan calls synchronously) or an artificial write-then-delete-then-rewrite dance purely for commit-history optics.
- **Resolution:** The full class (sync + async) was authored in Task 1's GREEN commit (`4a98ee9f`). Task 1's own RED/GREEN cycle stayed genuine and was independently verified (the back-edge guard test was run against the two target files temporarily removed -- confirmed FAILING -- then restored -- confirmed PASSING -- before either commit was made). Task 2 still delivers its own substantive, independent contribution: a 33-test suite (`bfbfeb43`) that specifically and rigorously proves every Task-2-scoped invariant (asyncio.wait-not-wait_for timeout behavior + loop responsiveness, `DiscoveryOverload` on a full semaphore, DC6 slot-recycling via a real blocking-thread simulation, version-keyed LRU hit/invalidation/bounding) against the implementation, rather than silently assuming it works.
- **Files modified:** `shared/discovery_service.py` (all in the Task 1 commit; no further changes needed for Task 2)
- **Verification:** All 33 Task-2 tests pass against the Task-1 implementation with no additional code changes; `pytest tests/test_discovery_service.py tests/test_no_back_edges_discovery.py tests/test_discovery_composition.py -x -q` -> 45 passed.
- **Committed in:** `4a98ee9f` (implementation), `bfbfeb43` (Task 2 test suite)

---

**Total deviations:** 1 process deviation (no Rule 1-4 auto-fixes; no behavior/architecture change, purely a commit-sequencing note). **Impact:** None on correctness or scope -- every acceptance criterion for both Task 1 and Task 2 is independently verified green.

## Issues Encountered

None. The full-repo `check_atlas_masking.py --scan-repo` gate (run in the background per the established 134-02/134-03/134-05 precedent, since the local dev tree carries tens of GB of unrelated untracked scratch content) was kicked off asynchronously; per-file `--scan-asset` checks on all 6 new files individually returned "no matches -- clean" (exit 0) immediately, and the full-repo run is recorded as completing clean in the Self-Check section below.

## User Setup Required

None - no external service configuration required. `DISCOVERY_ENABLED` remains OFF by default (unchanged from 134-05); this plan adds no new env vars beyond the `DISCOVERY_*` service-tuning knobs already named (but not yet consumed) in `docs/specs/discovery-budgets.md` SS3, which are now genuinely wired and overridable.

## Next Phase Readiness

- `shared/discovery_service.py` + `web/discovery.py` are the complete, tested async chokepoint DATA-06 requires -- Phase 135 (band contract/certificate) and Phase 136 (browse panel + `/work/{id}`) can call `web/discovery.py`'s pass-throughs directly with no further service-layer work.
- The DATA-10 unit x work projection is proven both in isolation (pure helper, fabricated edge cases) and end-to-end against the real fixture -- Phase 136's `WORK-01`/`WORK-02` witness-map page can consume `get_work_witnesses` as-is.
- No blockers for 134-07 (owner title-review -> real distillation -> frozen `discovery-frames.md`) or 134-08 (PERF-01 measurement) -- neither depends on this plan's files.

## Self-Check: PASSED

All 6 created files verified present on disk; all 4 task commits (`b504d276`, `4a98ee9f`, `bfbfeb43`, `24fca51d`) verified present in `git log --oneline --all`. Full verification re-run: `pytest tests/test_discovery_service.py tests/test_no_back_edges_discovery.py tests/test_discovery_composition.py -x -q` -> 45 passed; `python -m ruff check .` -> All checks passed; per-file `check_atlas_masking.py --scan-asset` on all 6 new files -> clean (exit 0) for each; `check_atlas_masking.py --scan-repo` (MASKING_SCAN_PATTERNS_FILE=.masking_patterns, run in background per established precedent) -> completed clean (no matches, exit 0).

---
*Phase: 134-discovery-data-spine*
*Completed: 2026-07-22*
