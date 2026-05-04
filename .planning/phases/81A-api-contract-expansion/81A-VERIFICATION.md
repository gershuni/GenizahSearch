---
phase: 81A-api-contract-expansion
verified: 2026-05-04T00:00:00Z
status: passed
score: 8/8 must-haves verified
overrides_applied: 0
---

# Phase 81A: Minimal API Contract Expansion — Verification Report

**Phase Goal:** Replace `/api/search`'s conflated `mode` field with a UI-aligned `search_mode` enum + Responsa-only `responsa_options` flag bag; add `request` echo block to `/api/search` and `/api/parallels` envelopes; lower `limit` ceiling 200 → 100; preserve all Phase 78/79/80 hardening.

**Verified:** 2026-05-04
**Status:** PASSED
**Re-verification:** No — initial verification.

## Requirements Coverage

Per-requirement evidence against `web/search_api.py`, `shared/search_serializer.py`, `shared/api_errors.py`, `web/api_hardening.py`, `genizah_core.py` and the test suite (180 passed / 6 skipped on the four 81A-relevant test files; full repo green at 1465 / 15 skipped).

| Req | Verdict | Evidence |
| --- | ------- | -------- |
| **API-EXPAND-01** — UI-aligned `search_mode` enum, old `mode` hard-rejected with cutover hint | **PASS** | `web/search_api.py:128-149` defines `SearchRequest` with `model_config = ConfigDict(extra='forbid')` and `search_mode: Literal['exact','variants','responsa','title','shelfmark']` (5 values, regex dropped per D-09 — REQUIREMENTS.md note acknowledged). Hard-cutover detection at `web/search_api.py:498-507` raises `APIError('invalid_request', "unknown field 'mode' — use search_mode instead")`. Tests: `tests/test_search_api_v2.py:469-479` (`test_old_mode_field_rejected_with_helpful_message` asserts both 'mode' and 'search_mode' appear in message), `:512-516` (`test_search_mode_regex_rejected`). Commit `192d900d feat(81A-01)`. |
| **API-EXPAND-02** — `responsa_options` only valid with `search_mode='responsa'`; field names mirror desktop UI; internal `variant_mode` not exposed | **PASS** | `ResponsaOptions` model at `web/search_api.py:112-125` with `extra='forbid'` and the four UI-mirrored bool fields (`variants`, `ja`, `flex_spacing`, `bidirectional`). Internal `variant_mode` derived server-side at `web/search_api.py:607` (`'variants' if opts.variants else 'exact'`) and never echoed. Coupling validator at `:151-160` raises `invalid_combination` for non-responsa mode + non-None `responsa_options`. Tests: `tests/test_search_api_v2.py:385-398` (parametrized over the four non-responsa modes), `:490-498` (`test_responsa_options_extra_field_rejected` confirms `variant_mode` rejected as input), `:501-509` (`test_responsa_options_variants_extended_rejected`). |
| **API-EXPAND-03** — All search_mode values produce non-empty results on at least one fixture; all 4 responsa flags produce measurable behavioral change | **PASS** | Five-value enum (regex dropped per D-09; REQUIREMENTS.md text predates D-09 cascade). Real-index round-trip: `tests/test_search_api_v2.py:296-314` `test_search_mode_real_index_returns_at_least_one_result` parametrized over all 5 modes. Behavioral differentiation: `:260-272` exact-vs-variants behavioral difference; `:317-380` per-flag passthrough tests for `variants` (toggle flips internal `variant_mode`), `ja`, `flex_spacing`, `bidirectional`. Internal mapping enforced at `:705-709`. |
| **API-EXPAND-04** — Cross-field validation matrix returns 400 `invalid_combination` for (a) responsa_options + non-responsa mode; (b) gap≠0 + title/shelfmark; (c) regex + responsa_options [N/A after D-09] | **PASS** | `@model_validator(mode='after')` at `web/search_api.py:151-171` covers (a) and (b) with messages naming both offending fields. Clause (c) is N/A per D-09 (regex dropped from enum; CONTEXT line 53 documents the cascade). Tests: `tests/test_search_api_v2.py:385-411` (parametrized over modes × gap values for both rules); `:712-714` confirms `'invalid_combination'` is registered in `shared/api_errors.py:26`. |
| **API-EXPAND-05** — `limit` ceiling 100; `query` cap 1000 unchanged; regex pattern cap N/A after D-09 | **PASS** | `web/search_api.py:148` `limit: int = Field(default=50, ge=1, le=100)`; `:177` `MAX_LIMIT = 100`; defensive runtime check at `:561-566`. `QUERY_LENGTH_CAP = 1000` at `:175` unchanged from Phase 78. Regex pattern cap intentionally not added per D-09 (CONTEXT line 51). Tests: `tests/test_search_api_v2.py:442-466` cover limit boundaries (above-max, below-min, at-max, at-min); `:430-440` cover query cap; `:717-…` (`test_max_limit_lowered_to_100`). |
| **API-EXPAND-06** — `/api/search` envelope adds `request` echo: search_mode echoed verbatim (never silently downgraded), `responsa_options`, `responsa_options_effective`, `gap`, `limit`, `limit_effective`, `filters`. Cascade case shows divergence + `tr()` warnings | **PASS** | Echo construction at `web/search_api.py:670-704`; serializer integration at `shared/search_serializer.py:369,421-422` (`request_echo` kwarg embedded under `request` key). Cascade meta channel: `genizah_core.py:67-122` (thread-local `_LAST_RESPONSA_DOWNGRADE_META` + getter/setter), populated at `:7701-7706` from cascade decision site, drained at `web/search_api.py:642-643` (success path) and `:760-770` (defensive finally drain). Tests: `tests/test_search_api_v2.py:524-616` cover presence on all 5 modes, no-cascade equality, search_mode-never-downgraded invariant, cascade divergence, `limit_effective` cap, filters passthrough. Round-trip serializer tests `tests/test_search_serializer.py:716-820`. |
| **API-EXPAND-07** — `/api/parallels` envelope gains `request` echo; existing `mode: exact|variants|fuzzy` field name preserved (D-07); Phase 80 tests pass | **PASS** | `web/search_api.py:1015-1029` constructs `parallels_echo` with exactly 6 keys (`mode`, `chunk_size`, `max_freq`, `boundary_options`, `limit_effective`, `filters`); no `responsa_options`, no `gap`, no `search_mode`. Serializer wiring at `shared/search_serializer.py:812,873-874`. Test `tests/test_parallels_api.py:768-787` (`test_parallels_envelope_contains_request_echo`) asserts the exact 6-key set and verifies `'search_mode' not in echo` and `'responsa_options' not in echo`. All 64 Phase 80 tests in `tests/test_parallels_api.py` pass unchanged. |
| **API-EXPAND-08** — Phase 78/79/80 hardening preserved (rate limit, mode gate, error envelope, statelessness, per-bucket independence). PostHog event gains `search_mode_value` + `responsa_options_count` | **PASS** | Three independent rate-limit buckets retained at `web/search_api.py:64,71,78` (search/browse/parallels). `enforce_mode_gate(request)` and `_resolve_rate_limit_key` calls retained on all three endpoints (`:531,538`, `:816,820`, `:940,944`). Error envelope routed through `_build_envelope_response` (`:710,716,722`). PostHog property extension at `web/api_hardening.py:599-600,626-627` (kwargs added to `capture_api_event`) and `:414-415` (decorator pulls from `captured_state`). Provisional capture pattern at `web/search_api.py:457-528` ensures `search_mode_value` survives `invalid_combination` rejections (Codex MEDIUM-3). Browse + parallels handlers explicitly pin `search_mode_value=None, responsa_options_count=0` at `:795-796,936-937` for uniform property shape. Tests: `tests/test_search_api_v2.py:619-704` cover all PostHog property paths (success, invalid_combination retains mode, structural Pydantic rejection, unknown-field). Phase 78/79/80 hardening regressions covered by migrated `tests/test_search_api.py` (786 lines, all `mode` → `search_mode`) plus `test_parallels_api.py`. |

## Anti-Patterns / Spot Checks

- **Old `mode` field cutover:** Hard-rejected (no shim, no deprecation), per D-13. Cutover-hint message present in 400 body (`web/search_api.py:498-507`).
- **Provisional PostHog capture:** Pre-Pydantic snapshot of `search_mode` from raw body at `web/search_api.py:481-486` ensures cross-field validation rejections retain telemetry. Verified by `test_posthog_search_mode_value_present_on_invalid_combination` and `test_posthog_search_mode_value_null_on_pydantic_rejection`.
- **Statelessness (D-20):** No reads of `state.last_results`, `state.current_search_query`, `app.storage.user`, or `request.cookies` on either modified path (`web/search_api.py:610,984` mark the assertions; full handler bodies confirmed clean).
- **Thread-local hygiene:** Both legacy string channel and new structured-meta channel drained on success (`:638,642-643`) and defensively in `finally` (`:754-770`). No leak between requests on the same worker thread.
- **`extra='forbid'` everywhere:** `SearchRequest`, `ResponsaOptions`, `FiltersModel`, `BrowseRequest`, `ParallelsRequest` all set `model_config = ConfigDict(extra='forbid')`.
- **Test suite green:** 180 passed / 6 skipped on `tests/test_search_api_v2.py + test_search_api.py + test_parallels_api.py + test_search_serializer.py`. Full repo: 1465 passed / 15 skipped (per task context).

## Behavioral Spot-Checks

| Behavior | Command | Result | Status |
| -------- | ------- | ------ | ------ |
| 81A test files green | `pytest tests/test_search_api_v2.py tests/test_search_api.py tests/test_parallels_api.py tests/test_search_serializer.py -q` | 180 passed, 6 skipped | PASS |
| `invalid_combination` registered in taxonomy | `grep invalid_combination shared/api_errors.py` | line 26 | PASS |
| MAX_LIMIT lowered | `grep "MAX_LIMIT" web/search_api.py` | `MAX_LIMIT = 100` (line 177) + `Field(le=100)` (line 148) | PASS |
| Five-value enum (no regex) | inspect `SearchRequest.search_mode` `Literal[...]` | exact, variants, responsa, title, shelfmark | PASS |
| Commits exist | `git log --oneline --grep=81A` | 192d900d (01), 50195c8c (02), faa9de48 (03), 0420aa3b/4e81a56f/ca79ec6b/a7d28d12 (04), 47a94855 (05) | PASS |

## Human Verification Required

None. The phase ships an internal API surface; all acceptance behaviors are deterministic and covered by unit + integration tests. Visual/UX verification is out of scope.

## Gaps Summary

No gaps found. Each requirement (API-EXPAND-01..08) is satisfied by concrete code at the cited file:line pairs and exercised by named tests. The intentional drop of `regex` from the v7.10 enum (D-09) cascades cleanly through API-EXPAND-01/03/04/05; CONTEXT lines 47-56 document the deliberate deviation from the original AC2/AC4/AC5 wording in REQUIREMENTS.md.

---

## Overall Verdict: **PASS**

All 8 acceptance requirements verified end-to-end (model definition → endpoint wiring → serializer envelope → test coverage → PostHog observability). Hardening from Phases 78/79/80 preserved. Test suite green (1465 / 15 skipped). Ready to ship as part of v7.10.

_Verified: 2026-05-04_
_Verifier: Claude (gsd-verifier)_
