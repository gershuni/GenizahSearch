---
phase: 79-api-browse-drill-down
verified: 2026-05-01T01:45:15Z
status: passed
score: 4/4 must-haves verified (all 4 SCs + 7 requirements + 5 STATE.md must-haves)
overrides_applied: 0
re_verification:
  previous_status: none
  previous_score: n/a
  gaps_closed: []
  gaps_remaining: []
  regressions: []
---

# Phase 79: /api/browse Drill-Down — Verification Report

**Phase Goal:** Ship POST→GET drill-down `/api/browse` endpoint that returns a hydrated locator (uid + p_num + volume_ie + fl_id), shelfmark/title/library metadata, transcription text (capped), image URL with library-aware picker, and partial enrichment from PGP/FJMS/NLI sidecars — all stateless, rate-limited as a separate bucket from /api/search, mode-gated, and behind the Phase 78 error envelope.
**Verified:** 2026-05-01T01:45:15Z
**Status:** PASSED
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths (ROADMAP Success Criteria)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| SC-1 | `GET /api/browse?uid=…` (preferred) or `?sys_id=…&volume_ie=…&page=…` (fallback) returns a fixed-shape JSON body with text, PGP/FJMS/NLI metadata subset, image URLs, with page-indexing convention explicit in the response | VERIFIED | `web/search_api.py:562-679` defines GET handler; `serialize_browse_payload` returns envelope including `'page_indexing': '1-based'` (line 622), `text`, `text_source`, `metadata.{pgp,fjms,nli}`, `image`, `locator`. BrowseRequest accepts both `uid` and `sys_id+volume_ie+p_num` forms. |
| SC-2 | Locator copied verbatim from /api/search → /api/browse round-trip works for multi-IE and single-IE | VERIFIED | `tests/test_browse_api.py` includes `test_browse_real_round_trip_search_to_browse` (R-PR-06 PRIMARY) doing real HTTP POST /api/search → GET /api/browse against TestClient; multi-IE warning surfaced via `volume_ie_defaulted` (search_api.py:649-660) and tested. |
| SC-3 | Endpoint is stateless: identical query strings produce identical bodies regardless of session/refinement state. Image best-effort (no probe). Source-enrichment failures degrade gracefully → null + warning, not whole-response failure | VERIFIED | Handler reads ONLY `request.query_params` — no `app.storage.user`/cookies/session reads (search_api.py:585). `serialize_browse_payload` docstring R-PR-01 confirms image emitted unconditionally without probe (search_serializer.py:555-557). `_wrap_with_timeout` (browse_service.py:250-270) catches per-source timeout/exception → emits warning + returns None for that source; bundle still serialized. Statelessness test passes (D-22). |
| SC-4 | Rate limiting, mode gating, error envelope, PostHog observability inherited from Phase 78 apply identically; existing /api/* routes unchanged | VERIFIED | `_browse_rate_limiter = RateLimiter(default_limit=30)` is a SEPARATE instance from `_rate_limiter` (search_api.py:62, 69 — verified at runtime: `_browse_rate_limiter is not _rate_limiter`). Mode gate via `enforce_mode_gate(request)` (search_api.py:601). Error envelope via `@wrap_endpoint` decorator (search_api.py:563). PostHog event via decorator's finally clause. `tests/test_api_legacy_unchanged.py:151` D-25 spot check confirms `/api/nli_image_by_sysid` is unaffected. |

**Score:** 4/4 success criteria verified.

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `shared/api_errors.py` | 3 new ERROR_CODES | VERIFIED | locator_conflict, manuscript_page_not_found, core_timeout all present (lines 38-40); confirmed via `from shared.api_errors import ERROR_CODES` runtime check. |
| `shared/search_serializer.py` | serialize_browse_payload, R-PR-01 (no image_unavailable warning), R-PR-09 (no requested_uid/requested_fl_id) | VERIFIED | Function at line 540, keyword-only signature `(*, page, pgp, fjms, nli, text_cap=4000, warnings=None)` — runtime inspect confirms `requested_uid`/`requested_fl_id` NOT in params. R-PR-01 documented in docstring (line 555-557). |
| `shared/browse_service.py` | BrowseEnrichmentBundle + fetch_browse_bundle (R-PR-02 WebDataService, R-PR-04 no uid, R-PR-05 single warning emitter) | VERIFIED | Class at line 66, function at line 277 with signature `(*, sys_id, p_num, volume_ie, fl_id)` — no uid param confirmed at runtime. `_fetch_core` uses WebDataService (line 101+). `_wrap_with_timeout` (line 250) is sole emitter of `enrichment_timeout`/`enrichment_failed` warnings; inner sync helpers have NO try/except. |
| `web/search_api.py` | BrowseRequest, NormalizedLocator, _browse_rate_limiter, browse_endpoint | VERIFIED | BrowseRequest at line 130, NormalizedLocator at line 149, `_browse_rate_limiter = RateLimiter(default_limit=30)` at line 69 (distinct from `_rate_limiter` at line 62), `browse_endpoint` route at line 562. |
| `web/api_hardening.py` | wrap_endpoint signature regression fix | VERIFIED | `_wrapped(request: Request)` at line 365 — no `*args/**kwargs`. Handler called as `await handler(request, captured_state=captured_state)` (line 372-375). |
| `tests/test_browse_api.py` | 38 D-24 tests | VERIFIED | `grep -c "^def test_\|^async def test_"` returns 38. |
| `tests/test_api_legacy_unchanged.py` | D-25 spot check | VERIFIED | `test_legacy_nli_image_by_sysid_unchanged` at line 151. |
| `CLAUDE.md` | env vars (BROWSE_TIMEOUT, BROWSE_CORE_TIMEOUT, BROWSE_TEXT_CAP) | VERIFIED | Lines 149-151 document all three. |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|----|--------|---------|
| web/main.py | search_api.init_search_api | import + call | WIRED | main.py:158, 172 |
| web/search_api.browse_endpoint | shared.browse_service.fetch_browse_bundle | import + await | WIRED | search_api.py:55, 617 |
| web/search_api.browse_endpoint | shared.search_serializer.serialize_browse_payload | import + call | WIRED | search_api.py:56, 667 |
| web/search_api.browse_endpoint | shared.api_errors (APIError + ERROR_CODES) | import + raise | WIRED | search_api.py imports + multiple raise sites (593, 626, 642) |
| browse_endpoint | _browse_rate_limiter.check | call | WIRED | search_api.py:605 (separate from _rate_limiter.check at line 384) |
| shared.browse_service._fetch_core | web.services.WebDataService | get_service() | WIRED | browse_service.py:9, 101+ |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|---------------|--------|---------------------|--------|
| browse_endpoint envelope | bundle.page | WebDataService.get_browse_page / get_browse_page_by_fl | Yes (BrowsePage dataclass with hydrated text/shelfmark/library_code/uid/volume_ie/p_num) | FLOWING |
| browse_endpoint envelope | bundle.pgp/fjms/nli | _pgp_sync/_fjms_sync/_nli_sync via WebDataService.PGP/FJMS/NLI services | Yes (real sidecar queries; documents.csv/fjms_enrichment.db/nli_crossref.db) | FLOWING |
| serialize_browse_payload locator | page attributes (uid/sys_id/volume_ie/p_num/fl_id) | resolved BrowsePage | Yes — getattr with `or None` fallbacks | FLOWING |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| Imports succeed (browse module wiring) | `python -c "from shared.browse_service import fetch_browse_bundle, BrowseEnrichmentBundle; from web.search_api import BrowseRequest, NormalizedLocator, _browse_rate_limiter, _rate_limiter"` | imports OK | PASS |
| 3 new ERROR_CODES present | `'locator_conflict','manuscript_page_not_found','core_timeout' in ERROR_CODES` | True/True/True | PASS |
| Separate rate-limit bucket | `_browse_rate_limiter is not _rate_limiter` | True | PASS |
| R-PR-09 — serialize_browse_payload signature | `inspect.signature` params | `[page, pgp, fjms, nli, text_cap, warnings]` (no requested_uid/requested_fl_id) | PASS |
| R-PR-04 — fetch_browse_bundle signature | `inspect.signature` params | `[sys_id, p_num, volume_ie, fl_id]` (no uid) | PASS |
| Phase 79-specific tests green | `pytest tests/test_search_api.py tests/test_search_serializer.py tests/test_browse_api.py tests/test_api_legacy_unchanged.py` | 107 passed, 1 skipped | PASS |
| Full suite green | `pytest tests/` | 1340 passed, 9 skipped, 0 failed | PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|-------------|-------------|--------|----------|
| API-03 | 79-01..04 | GET /api/browse resolves manuscript page by uid (preferred) or sys_id+volume_ie+page | SATISFIED | browse_endpoint accepts both forms; _validate_locator normalizes; serialize_browse_payload produces fixed-shape envelope with page_indexing='1-based'. |
| API-04 | 79-01,03,04 | Input validation + consistent error envelope | SATISFIED | BrowseRequest Pydantic model, _validate_locator raises APIError; @wrap_endpoint converts all errors to envelope shape; tested in test_browse_api.py error envelope tests. |
| API-05 | 79-01,03,04 | Drill-down locator on every result item (uid preferred; fallback {sys_id,volume_ie,p_num}) | SATISFIED | locator block in browse envelope (search_serializer.py:583-589) echoes uid+sys_id+volume_ie+p_num+fl_id from resolved BrowsePage; round-trip test exercises real /api/search → /api/browse. |
| API-06 | 79-03,04 | Stateless / request-driven | SATISFIED | Handler reads only request.query_params; no app.storage.user/cookies/state.* reads (search_api.py:585+); D-22 statelessness test asserts identical responses across two consecutive requests differ only in `generated_at`. |
| HARDEN-01 | 79-03,04 | Per-IP rate limit; existing /api/* unaffected | SATISFIED | _browse_rate_limiter (separate instance, default 30 req/min); D-18 test asserts 31-burst on /api/browse → 30+1×429 while /api/search bucket unaffected; D-25 spot-check confirms /api/nli_image_by_sysid unchanged. |
| HARDEN-04 | 79-03,04 | SEARCH_API_MODE env-var gating | SATISFIED | enforce_mode_gate(request) called at handler step 1 (search_api.py:601); test_browse_api.py covers disabled→503 and localhost-only→success. |
| HARDEN-05 | 79-03,04 | PostHog event per request (no payload contents) | SATISFIED | @wrap_endpoint owns capture_api_event in finally clause (api_hardening.py:391+); endpoint_name='browse' tag pinned. |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|

None. Grep for TODO/FIXME/XXX/HACK/placeholder/"not yet implemented" on the new files (`shared/browse_service.py`, `shared/api_errors.py`, `web/search_api.py`) returned no matches.

### Human Verification Required

None. All success criteria are testable programmatically and were exercised by the 38 D-24 tests + the real-HTTP round-trip test (env-gated, runs when fixture corpus is present). The phase gate ("locator round-trip test against single-IE and multi-IE manuscripts") is anchored in `test_browse_real_round_trip_search_to_browse` and `test_browse_multi_ie_default_warning`.

### Gaps Summary

No gaps. Every artifact named in the phase scope exists in the codebase, is substantive, is wired into the FastAPI app via `init_search_api()` in `web/main.py`, has data flowing through it (real WebDataService + sidecar reads), and is exercised by the 38-test suite plus the legacy spot-check. The two cross-AI plan-review constraints that materially shape the contract — R-PR-01 (image best-effort, no `image_unavailable` warning) and R-PR-09 (no `requested_uid`/`requested_fl_id` echo params on the serializer) — are confirmed at the signature level via runtime `inspect`. The wrap_endpoint signature regression flagged in 79-04 SUMMARY (FastAPI binding `*args`/`**kwargs` as required query params and 422-ing every browse request) is fixed: `_wrapped(request: Request)` has no variadics in `web/api_hardening.py:365`. Pytest baseline of 1336/9 declared in the SUMMARY is met or exceeded — current run shows 1340 passed / 9 skipped, 0 failed.

---

_Verified: 2026-05-01T01:45:15Z_
_Verifier: Claude (gsd-verifier)_
