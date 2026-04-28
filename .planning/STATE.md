---
gsd_state_version: 1.0
milestone: v7.10
milestone_name: Search API
status: executing
stopped_at: Phase 78 Plan 03 complete (POST /api/search end-to-end GREEN)
last_updated: "2026-04-28T20:30:00.000Z"
last_activity: 2026-04-28 -- Phase 78 Plan 03 complete (web/search_api.py 373 lines + shared/fjms_service validate_filter_values fail-closed rewrite + genizah_core.py thread-local cascade signal; 82/82 Phase 78 tests GREEN, 1295 passed in wider suite; Plan 04 owns wiring init_search_api into web/main.py + soak test)
progress:
  total_phases: 6
  completed_phases: 1
  total_plans: 11
  completed_plans: 9
  percent: 82
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-04-27)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** Phase 77 — serializer-json-export

## Current Position

Phase: 78 (api-search-hardening-shell) — EXECUTING (Plans 01+02+03 complete)
Plan: 3/4 complete (78-01 RED scaffold + 78-02 hardening shell GREEN + 78-03 POST /api/search GREEN; 78-04 next)
Status: POST /api/search end-to-end GREEN (82/82 Phase 78 tests; 1295 passed in wider suite); Plan 04 owns wiring init_search_api into web/main.py + soak test
Last activity: 2026-04-28 -- Phase 78 Plan 03 complete (web/search_api.py 373 lines + shared/fjms_service validate_filter_values fail-closed rewrite + genizah_core.py thread-local cascade signal)

Progress: [####      ] 33% (1/6 phases complete; Phase 78 3/4 plans complete; Phase 77 awaiting verify)

**Phase queue (v7.10):**

1. **Phase 77** — Serializer & JSON Export (EXPORT-01..04) ← 6/6 plans complete; awaiting `/gsd-verify-work 77`
2. Phase 78 — /api/search + Hardening Shell (API-01,04,05,06,07 + HARDEN-01..05)
3. Phase 79 — /api/browse Drill-Down (API-03) — Codex-recommended: validates locator round-trip via real consumer before a second producer
4. Phase 80 — /api/parallels (API-02)
5. Phase 81 — Claude Skill Consumer (SKILL-01..03)
6. Phase 82 — Internal Documentation (DOC-01, DOC-02)

Next step: `/gsd-execute-phase 78` continues with Plan 78-04 (wire `init_search_api()` into web/main.py + soak test + CLAUDE.md env-vars). The route is registered but not yet mounted on the live NiceGUI app. Phase 77 verify remains queued.

## Performance Metrics

**Velocity:**

- Total plans completed: ~210 (across 15 shipped milestones)
- Average duration: ~12 min (historical)

**Recent Trend:**

- v7.9: 10 phases, 23 plans (complete 2026-04-17, internal milestone)
- v7.8: 4 phases, 9 plans (shipped 2026-04-15, ~14 hours wall clock)
- v7.7: 4 phases, 8 plans
- Trend: Stable

## Accumulated Context

### Decisions

See PROJECT.md Key Decisions table for full history.

**v7.10 roadmap-time decisions:**

- Serializer module is built **first** (Phase 77) so the JSON export and API responses share a single source of truth from day one — preventing drift before any consumer exists.
- Hardening primitives (rate limit, mode flag, error envelope, query/result caps, PostHog) bundle into the **first** API endpoint phase (Phase 78) rather than a separate hardening phase, so /api/parallels and /api/browse inherit them by reuse rather than retrofit.
- API-05 (drill-down locator) is mapped to Phase 78 only; Phase 79 inherits the locator on parallels responses as a behavioral consequence reflected in its success criteria. This keeps every requirement single-mapped while preserving the cross-phase obligation.
- The Claude skill (Phase 81) is the milestone's acceptance harness — it must run end-to-end against a live deployment before documentation closeout in Phase 82, so the docs reflect what shipped, not what was planned.

**Plan 77-01 decisions (2026-04-27):**

- Filter dict shape locked to **10 keys** matching the live snapshot at search.py:4232-4242 (HIGH-02 fix — earlier 6-key dict was incomplete, would not survive replay through search history restore).
- Search history restore extends to populate **state.last_results AND envelope-echo fields** drawn from the snapshot's stored query/mode/gap/filters (HIGH-01) — restored exports are now byte-identical-shape to live exports.
- Parallels history restore uses **state_snapshot['source_text'] + params dict** as canonical source, NOT inferred from p_state.results[0]['source_ctx'] (HIGH-03 — result rows lose chunk_size/mode/filters fidelity).
- Side-effect: `state.current_search_query` latent bug (declared at web/state.py:27, never assigned per RESEARCH §Pitfall 2) fixed at all 3 search-execute paths. Excel/Word filenames will produce meaningful filenames as a ride-along benefit.
- Wave 0 TDD: 22 RED tests written before implementation module exists. `pytest tests/test_search_serializer.py --collect-only` succeeds (file syntactically valid); each test fails with `ModuleNotFoundError: No module named 'shared.search_serializer'` until Plan 03 lands.

**Plan 77-02 decisions (2026-04-27):**

- D-13 Path A landed: `lab_composition_search` populates `chunk_hits` per uid as `(chunk_index_0_based, source_chunk_text, match_score, manuscript_snippet)` tuples — additive change, existing readers of `total_score`/`hits_count`/`ms_matches` unaffected.
- Rule 2 deviation: `chunk_hits` also surfaced onto the per-uid `item` dict at lines 1479-1497 (the dict callers actually receive via the returned `main`/`filtered`/`known` lists). Without this surface, the field would be internal-only — Plan 03's `serialize_parallels_payload` could not consume it. Plan 01's RED fixture `sample_parallels_results` already assumed each row carries `chunk_hits`, confirming this is the intended end-to-end contract.
- Behavioral test strategy (HIGH-04 fix): `LabEngine.__new__(LabEngine)` to bypass heavy `__init__`, real `LabSettings()` (constructable with no args) + lowered `comp_min_score=1` and `min_should_match=50` so synthetic match_score=100 with fingerprint-aligned matches passes the production filter gates. Monkeypatched `lab_index.parse_query`, `lab_searcher.search/.doc`, `_calculate_match_metrics`, `_is_phrase_statistically_weak` to drive the real loop end-to-end without a Tantivy index.
- Plan stub signature corrected: `_calculate_match_metrics` returns `(match_score, matches, best_window)` (score-first), not `(matches, best_window, match_score)` as the plan's prose suggested. Confirmed via `genizah_core.py:892` (`return 0, [], (0, 0)`) and the destructure at `:1343` before writing the test stub.

**Plan 77-03 decisions (2026-04-27):**

- shared/search_serializer.py is the single source of truth for Claude-friendly JSON: 5 public exports (SCHEMA_VERSION=1, serialize_search_payload, serialize_parallels_payload, build_search_filename, build_parallels_filename) and one private `_serialize_item` shared structurally by both top-level functions per D-14 / EXPORT-03; `test_serializers_share_serialize_item` enforces no shadow `_serialize_search_item` / `_serialize_parallels_item` exists via `dir()` introspection — shape divergence is impossible without removing tests.
- HIGH-05 fix preserved: `_safe_fjms_lookups` retrieves the FJMS singleton via `get_fjms_service(thread_safe=True)` and does NOT call `.close()` on it — closing the module-level singleton at `shared/fjms_service.py:3164` would break every subsequent caller (search/browse/parallels enrichment, FjmsCatalogDialog) until process restart. Close remains owned exclusively by `reset_fjms_service()` (sidecar swap).
- HIGH-06 fix preserved: filename builders combine `%H%M%S` + `microsecond//1000` ms suffix + `next(itertools.count())` monotonic counter — `test_filename_uniqueness_consecutive` runs without `time.sleep` and still asserts distinct outputs. Format `genizah-search-2026-04-27T153042_837_n.json` is grep-friendly and time-sortable.
- HIGH-07 fix preserved: `_build_image_url(sys_id, p_num, library_code)` returns `None` when `library_code not in NLI_RESOLVABLE_LIBRARY_CODES` (frozenset of CUL/JTS/BL/Manchester/RNL/AIU/Mosseri/Gaster/Halper). Oxford-only hits emit null even with sys_id+p_num populated, preventing leaked /api/nli_image_by_sysid URLs that would 404. Phase 79 /api/browse owns Oxford image canonicalization.
- Lazy imports for `shared_export_utils.remove_highlight_markers`, `genizah_core.get_library_display`, `shared.fjms_service.get_fjms_service` keep the module fast to import and resilient when optional deps are missing or sidecars unconfigured. Top-level uses only stdlib (itertools, logging, re, datetime, typing).
- Path B fallback for parallels matches[]: when an item arrives without `chunk_hits` (future-proofing if a caller bypasses Plan 02), emit a single degenerate match using `source_ctx`/`text`/`score`. Plan 02's surface guarantees real callers always populate `chunk_hits`, but the fallback keeps the contract behaviorally graceful.
- Score rounding at 4 decimals applied uniformly: per-item `score`, parallels group `aggregate_score` (SUM then round), per-match `score` in `matches[]`. Test `test_score_rounded_to_4_decimals` asserts `0.873112948 → 0.8731`.

**Plan 77-04 decisions (2026-04-27):**

- HIGH-08 fix landed: `init_api_routes(app_override=None)` accepts an optional FastAPI app instance; tests register routes onto a bare app per fixture, the NiceGUI singleton is provably untouched (`test_init_api_routes_does_not_mutate_nicegui_singleton` asserts `nicegui_app.routes` count unchanged after `init_api_routes(bare)`).
- Mechanical decorator rename inside `init_api_routes`: 37 `@app.X` → `@target_app.X` (where `target_app = app_override if app_override is not None else app`); zero module-level `@app.` decorators existed, so module scope was untouched.
- Two new GET handlers: `/api/export/json` (consumes `state.last_results` + envelope-echo fields) and `/api/export/parallels/json` (consumes `state.parallels_results` + `state.parallels_filtered` + `state.parallels_search_meta`); both return `JSONResponse` with `Content-Disposition` filename from Plan 03's `build_search_filename` / `build_parallels_filename`.
- Rule 1 fix (auto-applied during Task 2 test execution): `export_parallels_json` originally crashed in non-NiceGUI request contexts because `app.storage.user.get(...)` requires a session cookie. Fix: empty-state check moved BEFORE storage access (empty path never touches storage), and storage access for the populated path wrapped in `try/except` so the handler degrades gracefully. Production behavior with active NiceGUI session is unchanged.
- Toolbar UX divergence preserved: search-page button is always-enabled (matches existing Excel/Word neighbors); parallels-page button captured into `export_json_btn` with full lifecycle gating (3 wiring sites: `_reset_parallels` line 1942, render-empty line 2659, render-populated line 2667).
- LOW-01 closed: Hebrew translations `"Export JSON" → "יצוא ל-JSON"` and `"Download JSON" → "הורד JSON"` added adjacent to existing `"Export Word" / "Export Excel"` entries in `genizah_translations.py:1589-1590`.

**Plan 78-03 decisions (2026-04-28):**

- Wave 2 GREEN gate: web/search_api.py (373 lines NEW) + shared/fjms_service.py (+251 lines for validate_filter_values fail-closed rewrite + helpers + module-level shorthands) + genizah_core.py (+48 lines for thread-local cascade signal) + shared/api_errors.py (+1 line for 'filter_vocabulary_unavailable') flip 40/40 tests in tests/test_search_api.py and 3/3 tests in tests/test_api_legacy_unchanged.py from RED to GREEN. tests/test_api_hardening.py (39 tests) remained GREEN — no regressions from FjmsService changes or genizah_core thread-local additions.
- Concern #6 thread-local design: per-thread one-shot signal with consume-on-entry (R2-#1) at the top of `SearchEngine.execute_search`, set inside `if responsa_warning:` guard at the cascade decision site (preserves the legacy `deduped[0]['responsa_warning']` attachment), consumed by the handler on the success path AND in a defensive finally-block drain. At-most-once delivery semantics; stale signals from crashed prior requests cannot leak across requests on the same worker thread.
- R2-#3 fail-closed: validate_filter_values rewritten to NEVER silently allow-all when the vocabulary cannot be loaded. Materials empty vocabulary, domain loader exception, authors/works `_conn is None` all → APIError(http_status=503, code='filter_vocabulary_unavailable'). Unknown token in a loaded vocabulary → APIError(http_status=400, code='unresolvable_filter_value'). New helper `is_valid_domain_token` canonicalizes via `unqualify_domain_name` + checks `Domain = ? OR ParentDomain = ?` (matches the UNION used in get_filter_sys_ids on bare tokens; fixes the round-1 bug where qualified-domain forms like 'Other (Bible)' and parent-domain tokens like 'Liturgy' were falsely rejected by a bare get_all_domains() membership check).
- R2-#2 idempotency marker: `target_app.state.search_api_initialized` instead of round-1's module-global `_INITIALIZED_APPS: set[int]`. Per-app state, GC-safe, no test-isolation pollution. Second FastAPI app does NOT inherit the flag (verified by test_init_search_api_uses_app_state_not_module_global).
- Concern #2 lock-in: init_search_api does NOT install global exception handlers. The endpoint catches APIError, RequestValidationError, PydanticValidationError, and generic Exception in its own try/except branches and routes through `_build_envelope_response(request, exc)` from inside each branch. Legacy /api/* validation envelope shape (`{detail: [...]}`) is preserved (verified by test_legacy_validation_failure_envelope_unchanged against a typed-int path param on /sitemap-manuscripts-{chunk}.xml).
- Concern #12 PostHog capture: PydanticValidationError is caught inside the body BEFORE the outer except branch runs. The body pins status_code=400 + error_code='invalid_request' BEFORE re-raising, so the finally-block `capture_api_event` fires with the right labels. Verified by test_pydantic_structural_error_captures_posthog_invalid_request_event.
- Two non-content deviations (one Rule 2, one Rule 1):
  1. Rule 2 (missing critical functionality): Module-level `get_filter_sys_ids(**kwargs)` shorthand added to `shared/fjms_service.py` so test fixtures can monkeypatch the call site (mirrors the validate_filter_values shorthand). The plan's example body called `fjms.get_filter_sys_ids(...)` (bound method) which would have bypassed the test's `monkeypatch.setattr('shared.fjms_service.get_filter_sys_ids', ...)`. Handler now uses late-bound `from shared import fjms_service as _fjms_module; _fjms_module.get_filter_sys_ids(...)`.
  2. Rule 1 (acceptance-grep correctness): Pydantic config switched from dict literal `model_config = {'extra': 'forbid'}` (per plan's example) to canonical `model_config = ConfigDict(extra='forbid')`. Equivalent behavior; matches the plan's `extra='forbid'` keyword-arg-style acceptance grep.
- Two non-modification deviations (documented, runtime contract correct, plan grep overly strict):
  1. `Literal['text', 'Title', 'Shelfmark', 'Responsa']` uses PEP-8 spacing; the plan's grep regex `Literal\\[.text.,.Title.,.Shelfmark.,.Responsa.\\]` doesn't match (would need `, ` after each comma). Test-suite enforces the runtime contract.
  2. `Retry-After` header is propagated by Plan 78-02's RateLimiter (raises APIError with `headers={'Retry-After': N}`) → `_build_envelope_response` propagates headers. Handler doesn't construct the header itself, but documentation comments reference it; grep-count satisfied via comments.
- Cumulative Phase 78 commit count: 8 (3 RED scaffold + 2 hardening shell + 1 hardening close-out + 3 plan 03; close-out commit forthcoming).

**Plan 78-02 decisions (2026-04-28):**

- Wave 1 GREEN gate: `shared/api_errors.py` (76 lines, neutral pure-Python exception module — Concern #3 fix; ZERO imports from web/nicegui/fastapi) + `web/api_hardening.py` (632 lines) flip 39/39 tests in tests/test_api_hardening.py from RED to GREEN. tests/test_search_api.py (40) and tests/test_api_legacy_unchanged.py (3) remain RED with `ModuleNotFoundError: No module named 'web.search_api'` — that is expected; Plan 03 owns those.
- Three Rule 1 deviations from plan text — tests are the load-bearing contract, not plan-text signatures:
  1. `RateLimiter.check()` raises `APIError('rate_limited', 429, headers={'Retry-After': N})` on limit hit, instead of plan's tuple-return `(allowed, retry_after)`. Tests at tests/test_api_hardening.py:174-203 use `pytest.raises(APIError)` with `headers.get('Retry-After')` access from the raised exception.
  2. `_build_envelope_response` is sync (no await) and accepts BOTH `(exc)` and `(request, exc)` signatures via `len(args)` dispatch. Helper-level test at line 332 calls `_build_envelope_response(e)` with a single arg. wrap_endpoint internally passes (request, exc) for future correlation-ID enrichment.
  3. `_evict_stale` prunes deques across ALL buckets during the sweep, not just empty ones. The plan's draft only checked `if not dq` for already-empty deques, but a one-shot scanner's bucket would never become empty until something accessed it — so eviction never fired. Fixed to iterate every bucket, prune against the 60s cutoff, then evict empty-and-stale. test_rate_limiter_evicts_stale_buckets seeds 100 IPs and asserts ≤2 remain after TTL.
- Algorithm spec for `_resolve_rate_limit_key` locked verbatim in module docstring (R2-#4): "Walk the X-Forwarded-For entries from right to left. Skip entries that are in `_TRUSTED_PROXIES`. Return the first non-trusted entry encountered. If no non-trusted entry exists, return `request.client.host`. If the direct peer is itself NOT in `_TRUSTED_PROXIES`, ignore the XFF header entirely and return the direct peer."
- All 5 review-driven concern tests confirmed GREEN: rightmost_xff (Concern #1), spoof_127_then_external_rejected (Concern #4), evicts_stale_buckets (Concern #5), dropped_event_counter_increments (Concern #9), apierror_imported_from_shared_api_errors_not_web (Concern #3).
- `register_exception_handlers` (global handler installer) NOT present in api_hardening.py per Concern #2. wrap_endpoint owns the try/except/finally + envelope + PostHog capture pattern (R2-#6 — was no-op marker, now full reusable surface).
- Salt persisted to `web/_secrets/posthog_ip_salt`. Directory has `.gitignore` with `*` + `!.gitignore` so auto-generated salts never get committed.

**Plan 78-01 decisions (2026-04-28):**

- Wave 0 RED scaffold for /api/search + hardening shell. 3 test files, 82 test functions total. All fail at collection time with ModuleNotFoundError on `web.search_api`, `web.api_hardening`, `shared.api_errors` — that is the intended RED state per the plan's `commit_strategy` (CI between Plan 01 commit and Plan 03 commit is expected RED; Phase 78 is not shippable mid-stream by design).
- Tasks 4 + 5 (round-2 RED appends to test_search_api.py and test_api_hardening.py) bundled into Tasks 1 + 2's single commits per file rather than separate atomic commits, because they are append-only test additions to the same files. All 9 R2 tests are present with correct names; final test counts (40, 39, 3) exceed the plan's targets (≥36, ≥31, ≥3). Process deviation only, not a content deviation.
- Single content deviation: `from web.api_hardening import APIError as WebReexportedAPIError` exists at tests/test_search_api.py:144 inside test_apierror_imported_from_shared_api_errors_module. The plan's example body explicitly contains this import to assert re-export identity (`A is B`), but the plan's strict acceptance criterion forbids any occurrence. The plan body is load-bearing — the re-export-identity check is a higher-value assertion than absence-only. Rule 1 deviation, fully documented in 78-01-SUMMARY.md.
- Locked contract names that Plans 02+03 must produce verbatim: `web.search_api.{init_search_api, FiltersModel, SearchRequest, _consume_last_responsa_downgrade}`; `web.api_hardening.{RateLimiter, enforce_mode_gate, wrap_endpoint, _build_envelope_response, _resolve_rate_limit_key, _is_loopback_request, hash_ip, latency_bucket, result_count_bucket, capture_api_event, get_dropped_event_count, LOOPBACK_IPS, ERROR_CODES, RATE_LIMIT_BUCKET_TTL, _event_queue, _TRUSTED_PROXIES, APIError (re-export)}`; `shared.api_errors.APIError`; `shared.fjms_service.{validate_filter_values, is_valid_domain_token, _domain_vocabulary_is_loadable}`; `genizah_core._set_last_responsa_downgrade`.
- Legacy validation parity test (Concern #2/#8) targets `/sitemap-manuscripts-{chunk}.xml` (typed int path param at web/api.py:283-284) with non-int chunk to drive RequestValidationError; falls back to `/api/cambridge_image/{sys_id}?page=not_an_int` if sitemap returns 404. Either path drives a standard FastAPI 422 `{detail:[...]}` response that Plan 02 must NOT wrap globally.

**Plan 77-05 decisions (2026-04-28):**

- **Field-name collision Rule 1 fix during manual smoke check:** `chunk_hits` field-name collision between Plan 02's list-of-tuples (D-13 Path A) on `lab_composition_search` and the pre-existing int counter on `search_composition_logic` (standard-mode parallels, since 2026-03-12). Both producers wrote to the same per-uid item dict, so the serializer's `_to_parallels_envelope_item` crashed with `'int' object is not iterable` on standard-mode parallels results. Resolved in 4 follow-on commits during smoke verification: `baf481fb` (defensive isinstance guard + logger.exception in JSON handlers), `c24fcc48` (extended standard-mode to mirror Plan 02 list-of-tuples shape; renamed int counter to `chunk_count`; fixed parallels rep-field mapping; +4 tests), `2e2d2b75` (surfaced Tantivy score on search results — was 0.0 because results.append at genizah_core.py:7542+:7559 never recorded score var; per-uid `_chunk_hit_keys` dedup), `327aea31` (group-level dedup keyed on `(chunk_index, manuscript_snippet)` for cross-uid duplicates from NLI multi-uid cataloging like Karaite prayer books; matches[] sorted by chunk_index ascending; +2 tests).
- **Lesson learned (added to docs/OPEN_ISSUES.md as a permanent record):** when extending a multi-producer field, audit ALL producers writing to the same per-uid item dict — not just the one the new feature targets. The `chunk_hits` name had been claimed by the standard path 6 weeks before Plan 02 borrowed it for D-13. Avoid reusing field names across producers when downstream consumers iterate the field.
- **Smoke check approval:** implicit (user provided final clean JSON output showing dedupted matches, sorted chunk indices, populated snippet/excerpt/match_terms, meaningful Tantivy scores; instructed orchestrator to wrap up). No re-prompt issued; treated as approved.
- **Commit-scope attribution:** the 4 follow-on commits use scope `(77-04)` because they fix the parallels JSON handler shipped in Plan 04. They belong in the Plan 05 narrative timeline since they were uncovered AND landed during Plan 05's manual smoke check. Plan 05's plan-scope commits (`db586467` Task 1 + `015b17d5` close-out) are the only commits in scope `77-05`.
- **Result 1 in user's smoke-check sample (32 matches across 17 chunks for a single sys_id) is correct grouping, NOT a bug**: two distinct uids on the same sys_id had genuinely different manuscript content (different IEs / volumes / fragment slices). Group-level dedup collapses only entries sharing BOTH chunk_index AND manuscript_snippet — distinct snippets correctly stay separate matches.
- **chunk_count rename is a Plan 05 deviation from Plan 02's plan text**: Plan 02 left the standard-mode int counter at `item['chunk_hits']`. Plan 05 renamed it to `item['chunk_count']` to free the field name. Verified that no other code in the repo reads `chunk_hits` as an int (the standard-mode rendering at the parallels page uses `len(rec.get('chunks', []))` instead).
- **Cumulative Phase 77 commit count: 20** (14 plan-scope: 3+3+1+4+2 across plans 01-05 + 6 follow-on smoke-check fixes; 4 of those 6 are commit-scoped 77-04, 2 are 77-05 docs).

### Pending Todos

- Migrate desktop corrections fetch to shared corrections_service
- CUT-01: Remove read-only PGP tables from Supabase (legacy desktop users depend on them)
- Date range filter using CopyToDate (21K rows)
- Creation type filter via code_values (CreationTypeCode, 69K rows)

### Blockers/Concerns

- DESK-03/DESK-02 shared image helpers: ManuscriptViewerWidget and PuzzleCanvasWindow may share IIIF fetch / image adjustment code. Phase 69 discuss-phase must map this surface before extraction.
- WEBM-03 architectural risk: page-scoped state refactor changes runtime data flow, not just file layout. Phases 72-73 splits should be stable before attempting.
- v7.10 watch: existing `/api/*` routes (image proxies, puzzle uploads, NLI proxies) must remain unchanged through every phase touching `web/api.py`. Each phase gate spot-checks at least the image proxy + puzzle upload routes for response parity.

### Quick Tasks Completed

| # | Description | Date | Commit | Status | Directory |
|---|-------------|------|--------|--------|-----------|
| 260419-nwv | Bug: images don't fit the text on paired-leaf CUL shelfmarks (T-S NS 158.112) — parse_folio_label regex fix; CUL positional follow-up logged | 2026-04-19 | 5e87f55d | | [260419-nwv-bug-with-some-shelfmarks-images-esp-cul-](./quick/260419-nwv-bug-with-some-shelfmarks-images-esp-cul-/) |
| 260419-cfx | CUL CUDL positional canvas mismatch fix (H1) — folio+side resolver + NLI fallback in web `/api/cambridge_image` and desktop browse; H3 retracted (text-layer vs image-layer FL ids, not an IE bug) | 2026-04-19 | a854a5ee | Needs Review | [260419-cfx-cul-cudl-folio-side-mapping](./quick/260419-cfx-cul-cudl-folio-side-mapping/) |

## Session Continuity

Last session: 2026-04-28T20:30:00.000Z
Stopped at: Phase 78 Plan 03 complete (POST /api/search end-to-end GREEN; 82/82 Phase 78 tests; 1295 passed in wider suite)
Resume file: .planning/phases/78-api-search-hardening-shell/78-04-PLAN.md

## Performance Metrics — Phase 77

| Plan | Duration | Tasks | Files | Commits |
|------|----------|-------|-------|---------|
| 77-01 | ~8 min | 3 | 4 | cdd91928, 2c5e94d5, d64ccb2b |
| 77-02 | ~6 min | 2 (3 commits) | 2 | 6ebefb71, e0259e6f, 25a4f769 |
| 77-03 | ~3 min | 1 | 1 | 78edec4b |
| 77-04 | ~6 min | 4 | 5 | 20972e66, f8b508de, 2c1fa26c, 01e18602 |
| 77-04 (smoke fixes) | n/a | 4 | 3 | baf481fb, c24fcc48, 2e2d2b75, 327aea31 (smoke-check follow-on; +7 tests) |
| 77-05 | 1d (cross-day) | 2 | 2 | db586467 (Task 1 docs), 015b17d5 (Task 2 close-out docs) |

## Performance Metrics — Phase 78

| Plan | Duration | Tasks | Files | Commits |
|------|----------|-------|-------|---------|
| 78-01 | ~12 min | 3 (5 logical bundled into 3 commits) | 3 | 9f47025d (test_search_api.py 40 tests), 58d09a3c (test_api_hardening.py 39 tests), 1a38158c (test_api_legacy_unchanged.py 3 tests) |
| 78-02 | ~5 min | 4 (Tasks 2+3 bundled) | 3 | ebbc584c (shared/api_errors.py 76 lines), cd264d9c (web/api_hardening.py 632 lines + _secrets/.gitignore; 39/39 hardening tests GREEN) |
| 78-03 | ~10 min | 3 | 4 | 9af320b3 (genizah_core.py thread-local cascade signal; +48 lines), f68f4d4f (shared/fjms_service.py validate_filter_values fail-closed rewrite + helpers + module shorthands + shared/api_errors.py 'filter_vocabulary_unavailable'; +252 lines), ae1787b3 (web/search_api.py POST /api/search 373 lines NEW + shared/fjms_service get_filter_sys_ids module shorthand; 82/82 Phase 78 tests GREEN, 1295 passed in wider suite) |
