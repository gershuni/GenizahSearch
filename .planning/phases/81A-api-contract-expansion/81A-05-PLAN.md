---
phase: 81A-api-contract-expansion
plan: 05
type: execute
wave: 4
depends_on:
  - 81A-01
  - 81A-02
  - 81A-03
files_modified:
  - tests/test_search_api_v2.py
autonomous: true
requirements:
  - API-EXPAND-01
  - API-EXPAND-02
  - API-EXPAND-03
  - API-EXPAND-04
  - API-EXPAND-05
  - API-EXPAND-06
requirements_addressed:
  - API-EXPAND-01
  - API-EXPAND-02
  - API-EXPAND-03
  - API-EXPAND-04
  - API-EXPAND-05
  - API-EXPAND-06
tags:
  - tests
  - matrix
  - acceptance
must_haves:
  truths:
    - "tests/test_search_api_v2.py is a NEW file owning the search_mode × responsa_options × invalid-combination matrix per D-12."
    - "All 5 search_mode values (exact, variants, responsa, title, shelfmark) are exercised with at least one fixture query yielding a non-empty result OR a documented empty-but-200 result (AC2)."
    - "All 4 responsa_options flags (variants, ja, flex_spacing, bidirectional) have at least one test asserting the flag's effect is observable in the response (AC3)."
    - "The validation matrix from 81A-CONTEXT.md is verified case-by-case: invalid combinations return 400 invalid_combination; out-of-range limits return 422; old `mode` returns 400 invalid_request."
    - "The `request` echo block is asserted on every successful test case (presence + correct key set)."
    - "PostHog properties `search_mode_value` and `responsa_options_count` are asserted via a mocked or capture-based check on at least one test per endpoint variant."
    - "Total ~40-50 test cases (per D-12)."
  artifacts:
    - path: "tests/test_search_api_v2.py"
      provides: "search_mode × responsa_options × invalid-combination matrix"
      contains: "search_mode"
      min_lines: 350
  key_links:
    - from: "tests/test_search_api_v2.py"
      to: "web/search_api.py"
      via: "FastAPI TestClient hitting /api/search and /api/parallels"
      pattern: "/api/search"
---

<objective>
New test file `tests/test_search_api_v2.py` covering the search_mode × responsa_options × invalid-combination matrix per D-12. Owns AC2 (5 search_mode values produce results), AC3 (4 responsa_options flags have measurable effect), AC4 (invalid combinations return 400 invalid_combination), AC5 (limit/query bounds), AC6 (request echo correctness in cascade case), and the PostHog property additions from D-08.

Purpose: This is the acceptance harness for 81A. If this file exits 0, 81A is done.

Output: New test file with ~40–50 test cases organized by the validation matrix from 81A-CONTEXT.md.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@CLAUDE.md
@.planning/phases/81A-api-contract-expansion/81A-CONTEXT.md
@.planning/phases/81A-api-contract-expansion/81A-01-SUMMARY.md
@.planning/phases/81A-api-contract-expansion/81A-02-SUMMARY.md
@.planning/phases/81A-api-contract-expansion/81A-03-SUMMARY.md
@.planning/phases/81A-api-contract-expansion/81A-04-SUMMARY.md

<interfaces>
<!-- Existing test fixture patterns the executor will reuse. -->

From tests/test_search_api.py (post-Plan-04) — copy-paste these fixture patterns:
- Look for the `client` pytest fixture that yields a TestClient bound to the search API app.
- Look for any monkeypatch fixtures used for stubbing `state.searcher.execute_search` (some Phase 78 tests use a fake searcher to avoid loading the real Tantivy index).
- Look for the PostHog event-capture fixture (Phase 78 tests used `monkeypatch.setattr('web.api_hardening._event_queue', ...)` to intercept events).

From tests/test_search_api_soak.py — patterns for capturing emitted events.

From tests/test_search_serializer.py (post-Plan-04) — patterns for asserting envelope shape.

Validation matrix to translate into tests (from 81A-CONTEXT.md):

| Rule | Status | Test |
|------|--------|------|
| search_mode='responsa' + responsa_options=None | ✓ | test_responsa_none_options_legal |
| search_mode='responsa' + non-empty responsa_options | ✓ | test_responsa_with_options_legal |
| search_mode='exact'/'variants'/'title'/'shelfmark' + non-None responsa_options | ✗ 400 invalid_combination | test_responsa_options_with_X_mode_rejected (4 sub-cases) |
| search_mode='title'/'shelfmark' + non-zero gap | ✗ 400 invalid_combination | test_gap_with_metadata_mode_rejected (2 sub-cases × multiple gap values) |
| query empty after .strip() | ✗ 400 query_required | test_empty_query_rejected |
| len(query) > 1000 | ✗ 400 query_too_long | test_query_too_long_rejected |
| limit > 100 | ✗ 422 | test_limit_above_max_rejected |
| limit < 1 | ✗ 422 | test_limit_below_min_rejected |
| filters.* unknown key | ✗ 400 unknown_filter_key | test_unknown_filter_key_rejected (regression) |
| filters.* unresolvable value | ✗ 400 unresolvable_filter_value | test_unresolvable_filter_value_rejected (regression — Phase 78 D-17 emits this code, NOT `invalid_filter_value` which is not in ERROR_CODES) |
| Old `mode` field present | ✗ 400 invalid_request | test_old_mode_field_rejected (also in test_search_api.py; mirror here for matrix completeness) |

Fixture queries for AC2 (planner's discretion per CONTEXT — pick concrete strings):
- `search_mode='exact'`     → query="ברכת המזון" (Birkat Hamazon — known to yield results per CLAUDE.md context)
- `search_mode='variants'`  → query="ברכת המזון" (same query, different mode — exercises core variant pipeline)
- `search_mode='responsa'`  → query="שאלה" (basic Responsa term — yields results)
- `search_mode='title'`     → query="ברכת המזון" (csv_bank metadata search on titles)
- `search_mode='shelfmark'` → query="T-S 12.123" (a known shelfmark)

These queries are best-effort fixtures; if any returns 0 results in the test environment (no index loaded), the test should mark as `skip` with a clear reason — but on the production-like CI environment, all 5 should return ≥1 result. If the test environment uses a stub searcher (Phase 78 pattern), use the stub's known fixture data instead.
</interfaces>
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: Create tests/test_search_api_v2.py with the full matrix (~40-50 test cases)</name>
  <files>tests/test_search_api_v2.py</files>
  <read_first>
    - tests/test_search_api.py (post-Plan-04 — fixture patterns, especially the `client` fixture and any `state.searcher` stub)
    - tests/test_search_api_soak.py (PostHog event-capture pattern)
    - tests/test_parallels_api.py (post-Plan-04 — request echo assertion pattern)
    - .planning/phases/81A-api-contract-expansion/81A-CONTEXT.md (validation matrix table)
    - web/search_api.py (post-Plan-01/02/03 — confirm field names and error codes)
    - shared/api_errors.py (confirm `invalid_combination` is in ERROR_CODES per Plan 01)
  </read_first>
  <behavior>
    Test cases organized into the following pytest classes / sections:

    **Section 1 — search_mode value coverage (AC2):**
    - test_search_mode_exact_returns_envelope: POST with search_mode='exact', valid query → 200 envelope, request.search_mode=='exact', request.responsa_options is None.
    - test_search_mode_variants_returns_envelope: same with 'variants'.
    - test_search_mode_exact_vs_variants_behavioral_difference: post the SAME query string twice — once with search_mode='exact', once with 'variants'. Use a stub searcher that records the internal `mode` argument it receives. Assert: exact-call received `mode='exact'`, variants-call received `mode='variants'`. (Per Blocker 2 from revision 1: the API MUST produce a measurable behavioral distinction — wired via the internal mode mapping, consumed by `var_mgr.get_variants(term, mode)` at genizah_core.py:6467. NOT collapsed to a single internal `'text'` mode.)
    - test_search_mode_responsa_default_options_returns_envelope: search_mode='responsa', no responsa_options → 200, request.responsa_options=={variants:F,ja:F,flex_spacing:F,bidirectional:F}, request.responsa_options_effective is the same (no cascade).
    - test_search_mode_title_returns_envelope: search_mode='title' → 200, request.responsa_options is None.
    - test_search_mode_shelfmark_returns_envelope: search_mode='shelfmark' → 200, request.responsa_options is None.

    **Section 2 — responsa_options flag effect (AC3):**
    - For each of the 4 flags (variants, ja, flex_spacing, bidirectional), at least one test asserts that toggling the flag produces a different result count, different result-set shape, OR a different `responsa_options_effective` (when cascade fires for variants/ja). At minimum, assert the request echo carries the flag verbatim. Stub-based tests are acceptable: monkeypatch `state.searcher.execute_search` to record the `responsa_options` it received and assert it carried the user-supplied flag value.
    - test_responsa_options_variants_passed_to_engine: search_mode='responsa', responsa_options={variants:True,...} → engine receives responsa_options['variants']==True AND responsa_options['variant_mode']=='variants'.
    - test_responsa_options_variants_false_engine_gets_exact_mode: variants:False → engine receives responsa_options['variant_mode']=='exact'.
    - test_responsa_options_ja_passed_to_engine: ja:True → engine receives responsa_options['ja']==True.
    - test_responsa_options_flex_spacing_passed_to_engine: flex_spacing:True → engine receives that.
    - test_responsa_options_bidirectional_passed_to_engine: bidirectional:True → engine receives that.

    **Section 3 — Invalid combination matrix (AC4):**
    - test_responsa_options_with_exact_mode_rejected: search_mode='exact', responsa_options={variants:T} → 400, code='invalid_combination', message contains both 'responsa_options' and 'search_mode'.
    - test_responsa_options_with_variants_mode_rejected: same with 'variants'.
    - test_responsa_options_with_title_mode_rejected: same with 'title'.
    - test_responsa_options_with_shelfmark_mode_rejected: same with 'shelfmark'.
    - test_gap_with_title_mode_rejected: search_mode='title', gap=5 → 400, code='invalid_combination', message contains both 'gap' and 'title'.
    - test_gap_with_shelfmark_mode_rejected: same with 'shelfmark'.
    - test_gap_zero_with_title_mode_legal: search_mode='title', gap=0 → 200 (gap=0 is the default, not an explicit non-zero).

    **Section 4 — Bounds (AC5):**
    - test_query_empty_after_strip_rejected: query='   ' → 400 query_required.
    - test_query_too_long_rejected: query=('x'*1001) → 400 query_too_long.
    - test_query_at_cap_legal: query=('x'*1000), search_mode='exact' → 200 (boundary case).
    - test_limit_above_max_rejected: limit=101 → 422.
    - test_limit_below_min_rejected: limit=0 → 422.
    - test_limit_at_max_legal: limit=100 → 200.
    - test_limit_at_min_legal: limit=1 → 200.
    - test_negative_limit_rejected: limit=-5 → 422.

    **Section 5 — Hard cutover (AC1, D-13):**
    - test_old_mode_field_rejected_with_helpful_message (mirror of Plan 04 test for matrix completeness): {'query':'x','mode':'text'} → 400 invalid_request, "unknown field 'mode'" in message.
    - test_extra_unknown_field_rejected: {'query':'x','search_mode':'exact','foo':'bar'} → 400 invalid_request (extra='forbid').
    - test_responsa_options_extra_field_rejected: search_mode='responsa', responsa_options={variants:T, variant_mode:'variants'} → 400 invalid_request (variant_mode is not a valid ResponsaOptions field).
    - test_responsa_options_variants_extended_rejected: responsa_options={variants_extended:T} → 400 invalid_request.

    **Section 6 — request echo correctness (AC6):**
    - test_request_echo_present_on_all_5_modes (parametrized over the 5 search_modes): every successful response has `request` block with the 7-key shape.
    - test_request_echo_responsa_no_cascade: Responsa with no cascade → responsa_options == responsa_options_effective.
    - test_request_echo_search_mode_never_downgraded: a Responsa request that triggers cascade → request.search_mode is still 'responsa' (D-04 — never downgraded).
    - test_request_echo_responsa_cascade_diverges (using a stub or contrived input that forces the cascade): request.responsa_options.ja=True, request.responsa_options_effective.ja=False, warnings[] contains the JA tr() string. NOTE: forcing the cascade requires either (a) a query that exceeds the cascade limit naturally, or (b) monkeypatching `_consume_last_responsa_downgrade_meta` to return a divergent dict. Use approach (b) since it's deterministic and unit-testable. Set `_LAST_RESPONSA_DOWNGRADE_META.value` directly via `genizah_core._set_last_responsa_downgrade_meta({'variants':True,'ja':False,...})` AND set the legacy string channel via `_set_last_responsa_downgrade("Judeo-Arabic expansion disabled")` BEFORE the request fires — using a monkeypatch on a stub `execute_search` to set both signals after running.
    - test_request_echo_limit_effective_reflects_cap: limit=50 → request.limit_effective==50; limit=10 → request.limit_effective==10.
    - test_request_echo_filters_passthrough: filters={domains:['liturgy']} → request.filters=={'domains':['liturgy']}.

    **Section 7 — PostHog properties (AC for D-08):**
    - test_posthog_event_carries_search_mode_value: capture the emitted event (using the same pattern as test_search_api_soak.py — monkeypatch `web.api_hardening._event_queue` or capture via a fake `posthog.capture`). Assert `properties['search_mode_value'] == 'exact'` for an exact-mode call.
    - test_posthog_event_carries_responsa_options_count_zero_for_non_responsa: assert event has `responsa_options_count == 0`.
    - test_posthog_event_carries_responsa_options_count_three_for_three_flags: search_mode='responsa', responsa_options={variants:T,ja:T,flex_spacing:T,bidirectional:F} → event.properties['responsa_options_count']==3.
    - test_posthog_event_search_mode_value_null_on_pydantic_rejection: send {'mode':'text'} (rejected) → event.properties['search_mode_value'] is None, responsa_options_count is 0.

    Total: ~30 test functions; with parametrize multiplications (limit values, search_mode values, responsa_options flag values) the assertion count lands in the 40-50 range per D-12.
  </behavior>
  <action>
    Create the file from scratch. Header docstring should reference 81A-CONTEXT.md and explicitly note the regex absence (D-09). Fixture imports should match the patterns in `tests/test_search_api.py`. If the existing tests use module-scope fixtures (a `client` fixture and a fake searcher), import or replicate them.

    **Recommended structure:**

    ```python
    """81A — search_mode × responsa_options × invalid-combination matrix.

    Per D-12, this NEW file owns the matrix tests. Existing
    tests/test_search_api.py was rewritten in-place (Plan 04) to migrate
    Phase 78 hardening tests from `mode` → `search_mode`; this file owns
    the new contract surface tests.

    Regex is intentionally absent (D-09 — deferred to v7.11). The 5 valid
    search_mode values are: exact, variants, responsa, title, shelfmark.
    """
    import json
    import pytest
    from fastapi.testclient import TestClient

    # ... fixtures ...

    SEARCH_MODES = ['exact', 'variants', 'responsa', 'title', 'shelfmark']
    QUERIES_PER_MODE = {
        'exact':     'ברכת המזון',
        'variants':  'ברכת המזון',
        'responsa':  'שאלה',
        'title':     'ברכת המזון',
        'shelfmark': 'T-S 12.123',
    }

    REQUEST_ECHO_KEYS = {
        'search_mode', 'responsa_options', 'responsa_options_effective',
        'gap', 'limit', 'limit_effective', 'filters',
    }

    # ... test classes / functions ...
    ```

    **Stub the searcher when needed.** Several tests (especially Section 7 PostHog tests, Section 6 cascade-divergence test, Section 2 flag-pass-through tests) need a deterministic searcher. Use `monkeypatch.setattr('web.state.state.searcher', FakeSearcher())` where `FakeSearcher.execute_search` is a function that:
    1. Records its arguments to a mutable list.
    2. Returns a fixed list of result dicts.
    3. Optionally sets the cascade meta thread-local to simulate a downgrade.

    **PostHog event capture.** Look at `tests/test_search_api_soak.py` for the existing pattern. Likely it monkeypatches `web.api_hardening._event_queue` with a `queue.Queue` and reads from it after the request. Reuse that pattern.

    **Cascade-divergence test (Section 6).** Use a fake searcher that, when called, calls `genizah_core._set_last_responsa_downgrade("Judeo-Arabic expansion disabled")` and `genizah_core._set_last_responsa_downgrade_meta({'variants': True, 'ja': False, 'flex_spacing': False, 'bidirectional': False})` then returns []. The handler will read both thread-locals and embed them in the envelope.

    **Parametrize where it reduces duplication:**
    - `@pytest.mark.parametrize('mode', SEARCH_MODES)` for the AC2 envelope-shape assertions.
    - `@pytest.mark.parametrize('mode', ['exact', 'variants', 'title', 'shelfmark'])` for the responsa_options-coupling rejection tests.
    - `@pytest.mark.parametrize('limit', [101, 200, 500, 9999])` for the above-max limit rejection tests.
    - `@pytest.mark.parametrize('limit', [0, -1, -100])` for the below-min tests.

    **Skip patterns.** If the test environment cannot exercise the real Tantivy index, mark the AC2 envelope-shape tests with `pytest.mark.skipif(not has_index(), reason='no Tantivy index in test env')` and add a sibling test using the stub searcher to exercise the same envelope-shape assertions deterministically. Both kinds may coexist; the CI configuration determines which run.

    **Verify request echo on every successful test.** Add a helper:

    ```python
    def assert_search_envelope_shape(envelope, *, expected_search_mode):
        assert envelope.get('schema_version') == 1
        assert envelope.get('source') == 'search'
        assert 'request' in envelope, f'response missing `request` echo: {envelope}'
        echo = envelope['request']
        assert set(echo.keys()) == REQUEST_ECHO_KEYS, f'unexpected echo keys: {set(echo.keys())}'
        assert echo['search_mode'] == expected_search_mode
    ```

    Use this helper in every successful-200 test.
  </action>
  <verify>
    <automated>pytest tests/test_search_api_v2.py -x --tb=short -q</automated>
    <automated>python -c "txt = open('tests/test_search_api_v2.py').read(); assert txt.count('def test_') >= 25, f'expected >=25 test functions, got {txt.count(chr(0x64)+chr(0x65)+chr(0x66)+chr(0x20)+chr(0x74)+chr(0x65)+chr(0x73)+chr(0x74)+chr(0x5f))}'; print('OK')"</automated>
    <automated>grep -c "search_mode" tests/test_search_api_v2.py</automated>
    <automated>grep -c "responsa_options_effective" tests/test_search_api_v2.py</automated>
    <automated>grep -c "invalid_combination" tests/test_search_api_v2.py</automated>
    <automated>grep -c "search_mode_value" tests/test_search_api_v2.py</automated>
    <automated>grep -c "responsa_options_count" tests/test_search_api_v2.py</automated>
    <automated>grep -c "regex" tests/test_search_api_v2.py</automated>
  </verify>
  <acceptance_criteria>
    - `tests/test_search_api_v2.py` exists, ≥350 lines.
    - `pytest tests/test_search_api_v2.py -x` exits 0.
    - File contains ≥25 `def test_` functions (parametrize multiplies this to 40-50 actual cases).
    - All 5 search_mode values are exercised in successful-200 tests with assert_search_envelope_shape.
    - Each of the 4 responsa_options flags has at least one test asserting it is passed to the search engine.
    - The 4 invalid-combination tests (responsa_options + non-responsa mode × 4) all assert HTTP 400 + code='invalid_combination'.
    - The 2 gap-with-metadata-mode tests assert HTTP 400 + code='invalid_combination'.
    - The limit ceiling/floor tests assert HTTP 422.
    - The query length tests assert HTTP 400 + correct error code.
    - The old-mode rejection test mirrors Plan 04's assertion (presence + helpful message).
    - The request-echo-cascade-divergence test asserts request.responsa_options.ja=True AND request.responsa_options_effective.ja=False AND warnings[] contains a JA-disabled string.
    - The PostHog tests assert search_mode_value and responsa_options_count are present in emitted events with the correct values for each branch.
    - `regex` does NOT appear in any test as a search_mode VALUE — but may appear in a comment noting it is deferred per D-09 (one-or-two count is fine; matrix doesn't test regex).
  </acceptance_criteria>
  <done>
    81A's acceptance harness is green. AC1-AC8 from 81A-CONTEXT.md are verified end-to-end. Phase 81A is shippable.
  </done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| TestClient → FastAPI app | Same as production trust posture; tests verify the trust boundary works as designed. |
| Stub searcher → handler thread-locals | Test stubs may set thread-locals to simulate cascade — must drain on test teardown to avoid leaking into other tests on the same thread. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-81A05-01 | Tampering | thread-local cross-test leak | mitigate | Use a pytest fixture with autouse=True that drains both `_consume_last_responsa_downgrade()` and `_consume_last_responsa_downgrade_meta()` after each test (yield-then-drain). Phase 78 R2-#1 precedent. |
| T-81A05-02 | Repudiation | flaky cascade tests | mitigate | Cascade-divergence test uses deterministic monkeypatch (set the thread-local directly), NOT a real cascade-triggering query. |
| T-81A05-03 | DoS | matrix runtime | accept | ~50 test cases at sub-second each = <30s total. |
| T-81A05-04 | Information Disclosure | test query strings | accept | Hebrew query strings are public scholarly terms (Birkat Hamazon, basic Responsa terms); not user data. |
</threat_model>

<verification>
- `pytest tests/test_search_api_v2.py -x --tb=short` exits 0.
- All grep checks pass.
- Full regression: `pytest tests/ -x` exits 0 (Phase 81A complete).
</verification>

<success_criteria>
The matrix harness exits green. 81A is shippable. The v7.10 skill (81B) can be built against the locked contract. ROADMAP.md SC-1..SC-6 for Phase 81A are met.
</success_criteria>

<output>
Create `.planning/phases/81A-api-contract-expansion/81A-05-SUMMARY.md` listing: section breakdown of test_search_api_v2.py, total test-function count, fixture pattern reused (client + fake searcher + event-queue capture), the cascade-divergence simulation approach (monkeypatched thread-locals), and final pytest exit code from `pytest tests/test_search_api_v2.py` and `pytest tests/`.
</output>
