---
phase: 79-api-browse-drill-down
plan: 04
subsystem: testing
tags: [api, browse, fastapi, testclient, pytest, locator]
requirements: [API-03, API-04, API-05, API-06, HARDEN-01, HARDEN-04, HARDEN-05]
dependency_graph:
  requires:
    - "Plan 79-01: shared/search_serializer.serialize_browse_payload + ERROR_CODES (locator_conflict, manuscript_page_not_found, core_timeout)"
    - "Plan 79-02: shared/browse_service.fetch_browse_bundle (no uid param per R-PR-04)"
    - "Plan 79-03: web/search_api.BrowseRequest, NormalizedLocator, _browse_rate_limiter, browse_endpoint route"
    - "Phase 78: tests/test_search_api.py fixture pattern (bare FastAPI app + TestClient)"
  provides:
    - "tests/test_browse_api.py: 38 tests covering D-24 surface (locator forms, conflicts, multi-IE default, manuscript-not-found, transcription truncation, enrichment timeout, statelessness, rate-limit independence, error envelope, post-resolution uid mismatch, image picker, real-HTTP round-trip)"
    - "tests/test_api_legacy_unchanged.py: D-25 spot check for /api/nli_image_by_sysid"
    - "Pre-existing wrap_endpoint signature regression fix in web/api_hardening.py"
  affects:
    - "Phase 79 verification (gsd-verifier): full pytest must pass after this plan"
    - "Phase 81 skill consumer: /api/browse contract is now test-anchored"
tech_stack:
  added: []
  patterns:
    - "Fixture-driven WebDataService mock injection: monkeypatch get_browse_page / get_browse_page_by_fl on the singleton service to return BrowsePage instances (R-PR-07: real shape, not synthetic dicts)"
    - "silent_sidecars fixture: monkeypatch shared.browse_service._pgp_sync/_fjms_sync/_nli_sync to None for lean bundle assertions"
    - "PRIMARY round-trip test does real HTTP POST /api/search → GET /api/browse against a TestClient on a bare FastAPI app (R-PR-06); serializer-direct unit test kept as separate secondary test"
    - "Image library-aware picker test verifies image.url prefix per library_code AND image.sources[] kind/fl_id/folio_label shape (D-13/R-05)"
    - "R-PR-01 (D-14 reopened): NO test asserts image.url == null OR warnings: ['image_unavailable'] — image.url is best-effort, emitted unconditionally"
key_files:
  created:
    - "tests/test_browse_api.py (1002 lines)"
  modified:
    - "tests/test_api_legacy_unchanged.py (+38 lines for D-25 spot check)"
    - "web/api_hardening.py (-3 lines: drop *args/**kwargs from wrap_endpoint inner _wrapped — FastAPI signature inspection was binding them as required query params)"
key_decisions:
  - "wrap_endpoint inner _wrapped no longer accepts *args, **kwargs. FastAPI inspects the signature and treats variadic params as required query params named 'args' and 'kwargs' — this caused every /api/browse request to return 422. No current consumer needs the variadic forwarding (search_endpoint does not use the decorator; browse_endpoint passes through with just request + captured_state)."
  - "test_browse_uid_only_path_resolves was reworked to capture from BOTH get_browse_page AND get_browse_page_by_fl. When uid carries an FL component (e.g., IE99_P3_FL12345), _fetch_core routes via get_browse_page_by_fl (fl_id is the most specific pin). The R-PR-04 contract is satisfied as long as the parsed components from uid (fl_id='FL12345' or p_num=3+volume_ie='IE99' depending on routing) are forwarded — never the raw uid string."
patterns_established:
  - "When testing a FastAPI route that funnels through fl_id-vs-component routing logic, capture from ALL service entry points and assert on whichever was hit; do not hard-code the routing branch."
  - "Phase 78 fixture pattern (bare FastAPI app + init_search_api + TestClient) generalizes cleanly to Phase 79 — no new fixture infra required."
requirements_completed: [API-03, API-04, API-05, API-06, HARDEN-01, HARDEN-04, HARDEN-05]
duration: 30min
completed: 2026-04-30
---

# Phase 79 Plan 04: /api/browse Test Surface Summary

**Locked in 38 D-24 tests for GET /api/browse including a real-HTTP search→browse round-trip, plus a D-25 legacy spot check for /api/nli_image_by_sysid; uncovered and fixed a wrap_endpoint signature regression that was producing 422 on every browse request.**

## Performance

- **Duration:** ~30 min (executor partial run + main-context recovery + signature fix)
- **Started:** 2026-04-30 (executor `a923a3aec675a950f`)
- **Completed:** 2026-04-30 (commit `1fe494ef`)
- **Tasks:** 1/1 (single-task plan, recovered after executor budget exhaustion)
- **Files modified:** 3 (1 created, 2 modified)

## Accomplishments

### Tests added — `tests/test_browse_api.py` (1002 lines, 38 tests)

D-24 coverage as defined in 79-04-PLAN.md:

- **Happy paths** — uid, p_num+volume_ie, p_num alone, fl_id (4 tests)
- **Locator validation** — missing sys_id, missing locator, conflict (uid vs volume_ie / p_num / fl_id), malformed uid, p_num positive int (8 tests)
- **Multi-IE behavior** — default warning + post-resolution uid mismatch → 404
- **Image** — library-aware picker URL prefix, sources[] shape, emitted unconditionally on proxy failure (R-PR-01: NO image_unavailable warning)
- **Transcription** — text_cap bounds + truncation warning (D-11/R-08)
- **Enrichment** — per-source timeout warning, per-source exception warning, core timeout → 504
- **Statelessness** — two identical requests differ only in `generated_at` (D-22)
- **Rate-limit** — 31-burst on /api/browse → 30+1×429, /api/search bucket unaffected (D-18)
- **Mode gates** — disabled → 503, localhost-only with loopback → success
- **Error envelope** — shape parity with Phase 78 contract
- **R-PR-06 PRIMARY round-trip** — real HTTP POST /api/search → GET /api/browse via TestClient (env-gated; skips when fixture corpus absent)
- **R-PR-07 real WebDataService shape** — monkeypatched fakes return real BrowsePage instances, not synthetic dicts

### Tests added — `tests/test_api_legacy_unchanged.py` (+38 lines)

`test_legacy_nli_image_by_sysid_unchanged`: D-25 spot check that /api/nli_image_by_sysid does not get a Phase 78 envelope rewritten onto it after init_search_api runs.

### Bug fixed — `web/api_hardening.py` wrap_endpoint signature regression

The Phase 78 R2-#6 fix made wrap_endpoint own the try/except/finally + envelope rewriting, with the inner `_wrapped(request: Request, *args, **kwargs)` forwarding to the handler. browse_endpoint is the first real consumer (search_endpoint hand-rolls). FastAPI's signature inspection saw the variadic params and emitted required query params named "args" and "kwargs" → every /api/browse request returned 422 with `{"type":"missing","loc":["query","args"]}`. Dropped the variadic forwarding; the decorator now exposes `(request: Request)` to FastAPI and calls `handler(request, captured_state=captured_state)`. Both 108 Phase 77/78 tests and 37 Phase 79 tests are GREEN.

## Test Discipline

- 1336 passed, 9 skipped (vs 1298 + 8 baseline → +38 new + 1 expected env-gated skip)
- 0 failed, 0 unexpected skips
- 0 regressions across Phase 77/78 suites
- check_docs.py: GREEN

## Outstanding

None. Phase 79 ready for verification.
