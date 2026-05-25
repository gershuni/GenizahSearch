---
phase: 98-nli-resilience-circuit-breaker-and-bounded-timeout-hardening
plan: 05
subsystem: resilience
tags: [resilience, genizah-core, breaker-migration, dead-code-removal, nli, circuit-breaker, retry-loop]

# Dependency graph
requires:
  - phase: 98-nli-resilience-circuit-breaker-and-bounded-timeout-hardening/02
    provides: shared.nli_circuit_breaker module (module-level singleton with is_open / record_failure / record_success / env-driven timeout constants)
provides:
  - "MetadataManager.fetch_iiif_manifest (shared-breaker-wired, typed failures, env timeouts)"
  - "MetadataManager.fetch_marc_data (shared-breaker-wired, typed failures, env timeouts)"
  - "MetadataManager._fetch_single_worker (D-22 new wiring + Codex Issue 3 per-iteration recheck)"
  - "MetadataManager._fetch_fl_ids (D-23 new wiring)"
  - "tests/test_genizah_core_nli_breaker_migration.py (23 tests, 4 classes — source audit + behavioral integration)"
affects: [98-06, web-api-integration, desktop-installer]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - class-attribute → module-level breaker migration (single source of truth across consumers)
    - synthetic-sys_id early-return BEFORE breaker check (Phase 85 D-14 invariant preserved)
    - per-sys_id negative cache + global breaker as complementary layers (D-07 semantics)
    - per-iteration retry-loop breaker recheck (short-circuits second retry when first opens breaker)
    - typed failures (timeout / connection_error / 5xx / 429) at every call site for PostHog dimensionality

key-files:
  created:
    - tests/test_genizah_core_nli_breaker_migration.py
  modified:
    - genizah_core.py
    - tests/test_browse_synthetic.py

key-decisions:
  - "Deleted legacy class-attribute breaker on MetadataManager (4 class attrs + 3 classmethods at lines 3938-3961) — RESEARCH Pitfall 5 fully mitigated, no deprecated stub left behind"
  - "Module-level import of shared.nli_circuit_breaker (aliased to legacy method names) instead of function-local deferred imports — grep-visible single source of truth"
  - "Per-sys_id negative caches (_iiif_manifest_fail_cache, _marc_fail_cache, _NLI_FAIL_TTL=60) PRESERVED — D-07 semantics intact"
  - "Codex REVIEW Issue 3 — added per-iteration breaker recheck inside _fetch_single_worker's `for attempt in range(2):` loop, so a first-attempt failure that opens the breaker prevents the second retry from burning another timeout"
  - "Phase 85 D-14 invariant preserved — synthetic-sys_id check BEFORE breaker check in both new call sites (_fetch_single_worker, _fetch_fl_ids)"

patterns-established:
  - "Migration pattern: module-level `from shared.X import name as _legacy_alias` keeps call-site syntax mechanical while making the import grep-visible"
  - "Test pattern: source-audit tests (TestLegacyBreakerRemoved, TestSharedBreakerWiring) provide static invariants; behavioral tests (TestBreakerBehaviorIntegration) exercise the runtime path"
  - "Retry-loop pattern: pre-loop guard + per-iteration recheck (defense in depth against threshold tripping mid-loop)"

requirements-completed: [D-03, D-04, D-13, D-22, D-23]

# Metrics
duration: 10min
completed: 2026-05-25
---

# Phase 98 Plan 05: genizah_core.py Breaker Migration Summary

**Migrated genizah_core.py off its class-attribute NLI circuit breaker onto the shared.nli_circuit_breaker module-level singleton; wired 2 new call sites (D-22 _fetch_single_worker, D-23 _fetch_fl_ids); deleted the legacy class-attribute breaker entirely (RESEARCH Pitfall 5 mitigated); added per-iteration retry-loop recheck (Codex REVIEW Issue 3).**

## Performance

- **Duration:** ~10 min
- **Started:** 2026-05-25T14:35:25Z
- **Completed:** 2026-05-25T14:45:19Z
- **Tasks:** 2 completed (1 source migration + 1 test creation)
- **Files modified:** 3 (genizah_core.py, tests/test_browse_synthetic.py, tests/test_genizah_core_nli_breaker_migration.py [created])

## Accomplishments

- All 6 NLI call sites in `genizah_core.py` are now wired to `shared.nli_circuit_breaker`: 4 migrated (`fetch_iiif_manifest`, `fetch_marc_data`) and 2 newly wired (D-22 `_fetch_single_worker`, D-23 `_fetch_fl_ids`).
- Legacy class-attribute breaker on `MetadataManager` (4 class attributes + 3 classmethods, lines 3938-3961) fully **deleted** — verified by both `grep` and `hasattr()` audits. RESEARCH Pitfall 5 ("class-attribute breaker not fully removed") mitigated.
- D-04 bug (`time.time()` vs `time.monotonic()` in breaker code) fixed by virtue of switching to the shared module which uses `time.monotonic()`.
- Codex REVIEW Issue 3 addressed: `_fetch_single_worker`'s `for attempt in range(2):` retry loop now rechecks `is_open()` at the top of each iteration. If the first attempt's failure opens the breaker, the second retry short-circuits immediately rather than burning another ~3s timeout.
- Per-sys_id negative caches (`_iiif_manifest_fail_cache`, `_marc_fail_cache`, `_NLI_FAIL_TTL`) **preserved** — D-07 semantics (404 / parse errors use per-sys_id cache; timeouts / 5xx / 429 / connection_error use the breaker) intact.
- 23 new tests + 1 fixed legacy test, all passing; combined Wave 3 slice (`test_nli_circuit_breaker.py` + `test_genizah_core_nli_breaker_migration.py` + `test_browse_synthetic.py`) = 85 tests, 0 failures, 3.9s.

## Task Commits

Each task was committed atomically:

1. **Task 1: Migrate genizah_core.py to shared NLI circuit breaker** — `0bf3cccc` (feat)
2. **Task 2: Migration tests + fix legacy class-attr ref in test_browse_synthetic** — `21985a29` (test)

_Note: Plan-level TDD gate is satisfied implicitly — Task 1 is `feat`, Task 2 is `test`; tests passed on first run against the migrated source._

## Files Created/Modified

- `genizah_core.py` — +175 / −70 lines. Module-level import of `shared.nli_circuit_breaker` (aliased to legacy method names). Class-attribute breaker deleted from `MetadataManager` (4 attrs + 3 classmethods). All 4 existing call sites in `fetch_iiif_manifest` / `fetch_marc_data` migrated with typed failures (`timeout` / `connection_error` / `5xx` / `429`) and env-driven timeout tuples. 2 new call sites (`_fetch_single_worker`, `_fetch_fl_ids`) wired with the same pattern. `_fetch_single_worker` retry loop has per-iteration breaker recheck (Codex Issue 3). Comment above per-sys_id negative caches updated to note the breaker has moved.
- `tests/test_genizah_core_nli_breaker_migration.py` — **CREATED**, 23 tests across 4 classes:
  - `TestLegacyBreakerRemoved` (6 tests) — RESEARCH Pitfall 5 invariants: no `self._nli_*` / `cls._nli_*` calls, no class-attribute state, no `_NLI_CIRCUIT_THRESHOLD = 3` definition, `hasattr(MetadataManager, '_nli_*') is False`.
  - `TestPerSysIdNegativeCachesPreserved` (3 tests) — D-07 cache layer intact.
  - `TestSharedBreakerWiring` (6 tests) — source audit: import present, state shared with `shared.nli_circuit_breaker` (alias is the same object), all 4 path strings present, all 4 failure types present, env-driven timeouts used.
  - `TestBreakerBehaviorIntegration` (8 tests) — runtime: breaker-open short-circuits `fetch_iiif_manifest` / `fetch_marc_data` / `_fetch_single_worker` / `_fetch_fl_ids` to defaults without network; 5xx response trips the breaker; 404 does NOT trip; timeout increments counter by 1; Codex Issue 3 — first-attempt timeout that opens the breaker causes second retry to short-circuit (assert `session.get` called exactly 1×, not 2×).
- `tests/test_browse_synthetic.py` — fixed `test_fetch_iiif_manifest_real_alma_attempts_call` to trip the shared breaker via `record_failure()` instead of poking the now-deleted `MetadataManager._nli_circuit_open_until` class attribute.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Updated stale legacy class-attribute reference in pre-existing test**

- **Found during:** Task 2 (running `pytest tests/test_browse_synthetic.py` to confirm no regression)
- **Issue:** `tests/test_browse_synthetic.py::TestFetchIiifManifestGuard::test_fetch_iiif_manifest_real_alma_attempts_call` reached directly into `MetadataManager._nli_circuit_open_until` and `MetadataManager._nli_consecutive_failures` — class attributes that Task 1 deleted. `AttributeError` on first run.
- **Fix:** Rewrote the test to trip the shared breaker via `shared.nli_circuit_breaker.record_failure('timeout', 'test_preload')` × `NLI_CIRCUIT_THRESHOLD` times, then `assert br.is_open()`. The autouse fixture in `tests/conftest.py` resets state between tests, so no manual save/restore is needed. Same semantic: open breaker → `fetch_iiif_manifest` returns the empty default without calling `_make_session`.
- **Files modified:** `tests/test_browse_synthetic.py`
- **Commit:** `21985a29` (folded into Task 2 commit)

No other deviations. All 85 tests in the breaker + migration + synthetic slice pass; legacy class-attribute references appear ONLY in docs/planning files (intentional historical context) and in the new test file's negative-assertion strings.

## Acceptance Criteria

All Task 1 + Task 2 acceptance criteria from PLAN.md verified:

- `grep -F "from shared.nli_circuit_breaker import" genizah_core.py` → **1 match** (module-level)
- All 6 imported names present: `is_open as _nli_circuit_is_open`, `record_failure as _nli_record_failure`, `record_success as _nli_record_success`, `NLI_CONNECT_TIMEOUT`, `NLI_IIIF_READ_TIMEOUT`, `NLI_MARC_READ_TIMEOUT` — ✓
- **Legacy breaker fully removed (RESEARCH Pitfall 5):**
  - `grep -E "self\._nli_circuit_is_open|self\._nli_record_failure|self\._nli_record_success" genizah_core.py` → **0 matches** ✓
  - `grep -E "cls\._nli_*" genizah_core.py` → **0 matches** ✓
  - `grep -E "_NLI_CIRCUIT_THRESHOLD\s*=\s*3|_NLI_CIRCUIT_WINDOW\s*=\s*60" genizah_core.py` → **0 matches** ✓
  - `hasattr(MetadataManager, '_nli_circuit_is_open')` → False ✓
  - `hasattr(MetadataManager, '_nli_consecutive_failures')` → False ✓
- **Per-sys_id caches preserved:** `_iiif_manifest_fail_cache`, `_marc_fail_cache`, `_NLI_FAIL_TTL = 60` all present ✓
- **6 call sites + per-iteration recheck = 5 guards (4 functions + 1 in-loop recheck):** `grep "if _nli_circuit_is_open()" genizah_core.py` → 5 matches ✓
- **Typed failures:** `_nli_record_failure(failure_type=...)` → 16 matches (4 sites × 4 failure types) ✓
- **Typed successes:** `_nli_record_success(path=...)` → 4 matches (1 per site) ✓
- **Env-driven timeouts:** `timeout=(NLI_CONNECT_TIMEOUT, NLI_IIIF_READ_TIMEOUT)` → 1; `timeout=(NLI_CONNECT_TIMEOUT, NLI_MARC_READ_TIMEOUT)` → 3 ✓
- **No GenizahCore references introduced** (class is `MetadataManager`) ✓
- **File compiles + imports:** `python -m py_compile genizah_core.py` exits 0; `from genizah_core import MetadataManager` exits 0 ✓
- **Tests pass:** `pytest tests/test_genizah_core_nli_breaker_migration.py -x` → 23/23 ✓; combined Wave 3 slice (`test_nli_circuit_breaker.py` + `test_genizah_core_nli_breaker_migration.py` + `test_browse_synthetic.py`) → 85/85 ✓

## Threat Mitigations Applied (from PLAN.md threat_model)

- **T-98-05-01 (DoS via 22s retry loop):** Mitigated — D-22 breaker check before loop entry returns instantly; each retry now uses `NLI_MARC_READ_TIMEOUT` (3s) instead of `timeout=10`. Codex Issue 3 per-iteration recheck closes the residual gap.
- **T-98-05-02 (Two parallel breakers drift):** Mitigated — legacy class-attribute breaker fully DELETED; `hasattr()` + grep audits enforce.
- **T-98-05-03 (404/parse flood breaker):** Mitigated — D-07 paths (404, `ET.ParseError`, non-network Exception) populate per-sys_id negative cache only, do NOT call `_nli_record_failure`. Test `test_404_does_not_trip_breaker_via_fetch_marc` pins.
- **T-98-05-04 (Subclass overrides):** Accepted per plan. No subclasses found in current codebase; CHANGELOG entry in Plan 98-06 will document the API change.
- **T-98-05-05 (PostHog path info disclosure):** Mitigated — all `path=...` values are static literals (`'fetch_iiif_manifest'`, `'fetch_marc_data'`, `'_fetch_single_worker'`, `'_fetch_fl_ids'`), no user data.
- **T-98-05-06 (Synthetic consumes breaker):** Mitigated — `is_synthetic_sys_id` early-return precedes the breaker check in all 6 sites. Phase 85 D-14 invariant preserved.

## Decisions Closed by This Plan

- **D-03 (migration to shared module):** COMPLETE — all 4 existing call sites migrated.
- **D-04 (`time.time()` vs `time.monotonic()` breaker bug):** COMPLETE — legacy buggy code deleted; shared module uses `time.monotonic()`.
- **D-13 (uniform call-site protocol):** PARTIAL — applied within genizah_core.py; full uniformity across web/api.py and puzzle paths is delivered by parallel plans 98-03 and 98-04 (same Wave 3).
- **D-22 (`_fetch_single_worker` breaker wiring):** COMPLETE — pre-loop guard + per-iteration recheck + typed failures.
- **D-23 (`_fetch_fl_ids` breaker wiring):** COMPLETE — guard + typed failures.

## Self-Check: PASSED

Verification of the SUMMARY's claims:

- `tests/test_genizah_core_nli_breaker_migration.py` exists ✓
- Commits `0bf3cccc` and `21985a29` exist on the current branch (`git log --oneline -3`) ✓
- `python -m py_compile genizah_core.py` exits 0 ✓
- `from genizah_core import MetadataManager` exits 0, legacy attrs absent ✓
- 85 tests pass across the Wave 3 slice ✓
