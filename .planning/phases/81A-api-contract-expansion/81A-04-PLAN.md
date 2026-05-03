---
phase: 81A-api-contract-expansion
plan: 04
type: execute
wave: 4
depends_on:
  - 81A-01
  - 81A-02
  - 81A-03
files_modified:
  - tests/test_search_api.py
  - tests/test_search_serializer.py
  - tests/test_parallels_api.py
autonomous: true
requirements:
  - API-EXPAND-07
  - API-EXPAND-08
requirements_addressed:
  - API-EXPAND-07
  - API-EXPAND-08
tags:
  - tests
  - regression
must_haves:
  truths:
    - "Existing Phase 78 hardening regression coverage (rate limit, mode gate, error envelope, PostHog capture, statelessness, filter validation) is preserved verbatim — only the request payload field name changes from `mode` to `search_mode` and old-mode values map to the new enum."
    - "tests/test_search_api.py exits 0 against the post-Plan-01/02/03 codebase."
    - "tests/test_search_serializer.py contains at least one test case asserting the `request` echo block is present in the search envelope when `request_echo` is passed, and absent when None (Phase 77 download back-compat)."
    - "tests/test_search_serializer.py contains at least one test case asserting the `request` echo block is present in the parallels envelope with the parallels-specific shape (mode, NOT search_mode)."
    - "tests/test_parallels_api.py contains at least one test case asserting the `/api/parallels` envelope contains a `request` block, and that all Phase 80 test cases still pass with the additive change. [API-EXPAND-07 verification]"
    - "Git history of tests/test_search_api.py is preserved (rewritten in-place, NOT deleted+recreated) per D-12."
  artifacts:
    - path: "tests/test_search_api.py"
      provides: "Phase 78-era hardening tests migrated to search_mode field"
      contains: "search_mode"
    - path: "tests/test_search_serializer.py"
      provides: "request echo round-trip cases for both search and parallels envelopes"
      contains: "request_echo"
    - path: "tests/test_parallels_api.py"
      provides: "request echo presence assertion + Phase 80 regression"
      contains: "request"
  key_links:
    - from: "tests/test_search_api.py"
      to: "web/search_api.py SearchRequest"
      via: "request payloads use {'search_mode': '...'} not {'mode': '...'}"
      pattern: "search_mode"
---

<objective>
Migrate Phase 78-era hardening tests to the new `search_mode` field name (D-12 — rewrite `tests/test_search_api.py` IN-PLACE so git history is preserved). Extend `tests/test_search_serializer.py` with `request` echo round-trip cases for both serializers. Extend `tests/test_parallels_api.py` with a `request` echo presence assertion alongside the existing Phase 80 regression coverage (API-EXPAND-07 verification — Plan 02 owns the implementation).

**Wave 4 (per revision 1).** Plans 02 and 03 modify the same finally block region in different sub-regions and must run sequentially (Plan 02 → Wave 2, Plan 03 → Wave 3). Plan 04 (tests) and Plan 05 (matrix) can then run in parallel in Wave 4 since they touch disjoint test files (`tests/test_search_api.py` + `tests/test_search_serializer.py` + `tests/test_parallels_api.py` for Plan 04 vs `tests/test_search_api_v2.py` for Plan 05).

Purpose: Inherits Phase 78/79/80 hardening coverage (AC7) and adds regression for the parallels echo (AC8). The new matrix tests live in Plan 05; this plan handles the in-place migration only.

Output: All three test files compile and pass against the post-Plan-01/02/03 codebase.
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

<interfaces>
<!-- Mapping the executor needs from old to new request shape. -->

Old payload shape (Phase 78):
```python
{'query': 'foo', 'mode': 'text'}                              # → search_mode='exact'
{'query': 'foo', 'mode': 'text', 'gap': 2}                    # → search_mode='exact', gap=2
{'query': 'foo', 'mode': 'Title'}                             # → search_mode='title'
{'query': 'foo', 'mode': 'Shelfmark'}                         # → search_mode='shelfmark'
{'query': 'foo', 'mode': 'Responsa'}                          # → search_mode='responsa'
```

Old → New mapping for in-place test rewrites:
- `mode='text'`     → `search_mode='exact'`
- `mode='Title'`    → `search_mode='title'`
- `mode='Shelfmark'` → `search_mode='shelfmark'`
- `mode='Responsa'` → `search_mode='responsa'`

The internal Phase 78 `mode` value space (capitalized) is now PURELY an internal value of `state.searcher.execute_search`; tests should NOT use it in API payloads.

Tests that previously asserted on the response envelope's `mode` field: this top-level field STILL exists for back-compat (serialize_search_payload still emits `'mode': mode or 'text'`). The new `request` block additionally carries `search_mode`. Tests can assert on either. **NOTE:** Per Plan 01 revision, the top-level `mode` echo field will now reflect the internal mode value ('exact' / 'variants' / 'Title' / 'Shelfmark' / 'Responsa') because `serialize_search_payload(mode=internal_mode, ...)` is called with the translated value. Tests asserting `body['mode'] == 'text'` will break — update to assert on `body['request']['search_mode']` instead, OR update the expected internal value (e.g., 'exact' or 'variants').

Old `mode` field rejection test: a NEW test must verify that `{'query':'x','mode':'text'}` returns 400 with error code `invalid_request` and the error message contains the literal substring `unknown field 'mode'`.

Tests that asserted limit > 200 returned 400: change to limit > 100 returns 422 (Pydantic Field constraint, NOT 400).

Filter-validation tests (Warning 5): The actual error code emitted by Phase 78 D-17 is `unresolvable_filter_value` (or `unknown_filter_key` for unknown field names). NOT `invalid_filter_value` (that string is referenced in CONTEXT validation matrix but does NOT exist in `shared/api_errors.py` ERROR_CODES — verified at revision time). Tests must continue to assert against the actual emitted codes (`unresolvable_filter_value`, `unknown_filter_key`, `filter_vocabulary_unavailable`) — these are unchanged by Phase 81A.
</interfaces>
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: Rewrite tests/test_search_api.py in-place — migrate `mode` → `search_mode` and add old-mode rejection test</name>
  <files>tests/test_search_api.py</files>
  <read_first>
    - tests/test_search_api.py (read fully — it is large; understand every test before mutating)
    - .planning/phases/81A-api-contract-expansion/81A-CONTEXT.md (validation matrix)
    - web/search_api.py (post Plan 01/02/03 — confirm field names)
    - shared/api_errors.py (confirm actual filter-validation codes: `unresolvable_filter_value`, `unknown_filter_key`, `filter_vocabulary_unavailable`)
  </read_first>
  <behavior>
    - Every test in tests/test_search_api.py that POSTs to /api/search uses the new `search_mode` field (not `mode`).
    - The old field name `'mode'` does NOT appear as a top-level key in any request payload (use grep to verify).
    - One new test asserts that POSTing `{'query':'x','mode':'text'}` returns HTTP 400 with `body['error']['code']=='invalid_request'` and `"unknown field 'mode'"` in `body['error']['message']`.
    - Tests for the limit ceiling are updated: `limit=201` (or any > 100) returns HTTP 422 (Pydantic validation), not 400 'limit_too_high'.
    - Existing rate-limit, mode-gate, statelessness, filter-vocab, PostHog-capture, and downgrade-warning tests pass with the renamed field.
    - Filter-validation tests continue to assert `unresolvable_filter_value` / `unknown_filter_key` (NOT a non-existent `invalid_filter_value`).
    - All other test bodies (assertions on count, total, results, warnings, error envelope shape) are unchanged.
    - Git history is preserved — the file is EDITED (Edit tool), not deleted and recreated.
  </behavior>
  <action>
    **Step A — Inventory.** Read `tests/test_search_api.py` fully. Make a list of every payload literal that contains `'mode':` as a request body key. There will likely be 30–60 such occurrences.

    **Step B — Mechanical rewrite.** Use the Edit tool to perform the replacements. Because `'mode':` may also appear in non-request contexts (assertions, comments), use surgical replacements based on context. Recommended approach: replace each occurrence individually using Edit with enough surrounding context to disambiguate.

    Translation table:
    - `'mode': 'text'`      → `'search_mode': 'exact'`
    - `'mode': 'Title'`     → `'search_mode': 'title'`
    - `'mode': 'Shelfmark'` → `'search_mode': 'shelfmark'`
    - `'mode': 'Responsa'`  → `'search_mode': 'responsa'`
    - `"mode": "text"`      → `"search_mode": "exact"`  (double-quote variants)
    - etc.

    For Responsa tests that test specific cascade behavior, also add `'responsa_options': {'variants': True, 'ja': True, 'flex_spacing': False, 'bidirectional': False}` to the payload IF the original test expected variants and ja to be active. If a Responsa test was just exercising the basic Responsa path with default flags, leave responsa_options omitted (the post-Plan-01 handler builds an all-False ResponsaOptions when omitted).

    **Important consideration on Responsa tests:** Phase 78's hard-coded responsa_options dict was `{variants: True, ja: True, flex_spacing: False, bidirectional: False}`. After Plan 01, the API default is all-False. Tests that previously relied on implicit variants/ja being on must explicitly send `responsa_options: {variants: true, ja: true, ...}` to preserve their behavioral expectations.

    **Step C — Update limit-ceiling tests.** Find any test that POSTs with `limit > 200` and asserts a 400 `limit_too_high` envelope. Update to:
    - For `limit > 100` (e.g., `limit=101`): expect HTTP 422 (Pydantic ValidationError) with `body['error']['code'] == 'invalid_request'`. If a Phase 78 test asserted code='limit_too_high', change it to 'invalid_request'.
    - For `limit < 1` (e.g., `limit=0`): same — expect 422 / invalid_request via Pydantic.

    **Step D — Add new old-mode rejection test.** At a sensible location (end of file, or grouped with other Pydantic-validation tests), add:

    ```python
    def test_old_mode_field_rejected_with_helpful_message(client):
        """81A D-13 — sending the old `mode` field returns 400 invalid_request
        with a message that names both the old and new field names."""
        resp = client.post('/api/search', json={'query': 'foo', 'mode': 'text'})
        assert resp.status_code == 400
        body = resp.json()
        assert body['error']['code'] == 'invalid_request'
        # Both `mode` and `search_mode` must appear so skill authors can find the migration path.
        msg = body['error']['message']
        assert "mode" in msg and "search_mode" in msg
        # Specifically, the cutover string from the handler:
        assert "unknown field 'mode'" in msg
    ```

    Use the existing `client` fixture pattern from the file (read the existing fixtures before writing this test).

    **Step E — Sanity check.** After all edits, run:
    - `grep -E "'mode':" tests/test_search_api.py` should return ZERO lines (all migrated).
    - `grep -E "'search_mode':" tests/test_search_api.py` should return MANY lines (every old test).
    - `grep "unknown field 'mode'" tests/test_search_api.py` should return at least 1 line (the new test).

    **Step F — Run tests and apply targeted repairs from the bounded fix list (Warning 7 fix).** Execute `pytest tests/test_search_api.py -x --tb=short`. Any remaining mismatches MUST belong to one of the following bounded categories — DO NOT make open-ended fixes:

    1. **Responsa default-flag drift.** Tests that asserted Phase 78's hard-coded `variants:True, ja:True` defaults now see all-False because the API default is `ResponsaOptions()` (all-False). Repair: add explicit `'responsa_options': {'variants': True, 'ja': True, 'flex_spacing': False, 'bidirectional': False}` to the payload to restore behavioral expectations.
    2. **Limit-ceiling code change.** Tests that asserted `limit > 200` → 400 `limit_too_high`. Repair: assert `limit > 100` → 422 with `body['error']['code'] == 'invalid_request'` (Pydantic Field constraint, not the inner APIError path).
    3. **Old-mode payload construction.** Stale `{'mode': 'text'/'Title'/'Shelfmark'/'Responsa'}` payloads not caught by the mechanical rewrite. Repair: re-grep and apply the translation table from Step B.
    4. **Top-level envelope `mode` field assertions.** Tests asserting `body['mode'] == 'text'`/`'Title'`/`'Shelfmark'`/`'Responsa'` see post-Plan-01 internal values like `'exact'`/`'variants'`/`'Title'`/`'Shelfmark'`/`'Responsa'`. Repair: either (a) update the expected value to the new internal mapping (`'exact'` for `search_mode='exact'`, `'variants'` for `search_mode='variants'`, capitalized for the others), OR (b) switch the assertion to `body['request']['search_mode']` which carries the API enum value verbatim.
    5. **Filter-validation code assertions.** If any test asserts a non-existent `invalid_filter_value` error code, repair to the actual emitted code (`unresolvable_filter_value` or `unknown_filter_key`). Phase 81A does not change Phase 78 D-17 filter-validation behavior.

    Any failure that does NOT fit one of these five categories is unexpected — surface it in the SUMMARY rather than guessing.
  </action>
  <verify>
    <automated>pytest tests/test_search_api.py -x --tb=short</automated>
    <automated>python -c "import re; txt = open('tests/test_search_api.py').read(); assert re.search(r\"['\\\"]mode['\\\"]\\s*:\\s*['\\\"](text|Title|Shelfmark|Responsa)['\\\"]\", txt) is None, 'old mode field still present in payloads'; print('OK')"</automated>
    <automated>grep -c "search_mode" tests/test_search_api.py</automated>
    <automated>grep -c "unknown field 'mode'" tests/test_search_api.py</automated>
    <automated>grep -c "invalid_filter_value" tests/test_search_api.py</automated>
  </verify>
  <acceptance_criteria>
    - `pytest tests/test_search_api.py -x` exits 0.
    - No payload literal in the file contains `'mode': 'text'` / `'mode': 'Title'` / etc. (grep verifies).
    - At least one test asserts the old-mode rejection with code='invalid_request' and message containing "unknown field 'mode'".
    - Limit-ceiling tests assert HTTP 422 (not 400) for `limit > 100`.
    - Responsa tests that previously relied on Phase 78's hard-coded `variants:True, ja:True` defaults now explicitly send those flags via `responsa_options`.
    - No test references a non-existent `invalid_filter_value` code (grep returns 0). Filter-validation assertions use `unresolvable_filter_value` / `unknown_filter_key` / `filter_vocabulary_unavailable` (the actual codes emitted by Phase 78 D-17).
    - Step F repairs are limited to the 5 bounded categories listed (Responsa defaults, limit ceiling, payload field name, envelope mode field, filter-validation codes). No open-ended changes.
    - The file was modified via Edit (not Write+delete) — `git log --follow tests/test_search_api.py` shows continuous history (manual check).
  </acceptance_criteria>
  <done>
    Phase 78 hardening regression coverage is preserved at the new contract. The matrix coverage is added in Plan 05.
  </done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Extend tests/test_search_serializer.py with request echo round-trip cases (search + parallels envelopes)</name>
  <files>tests/test_search_serializer.py</files>
  <read_first>
    - tests/test_search_serializer.py (read fully — understand existing fixtures and patterns)
    - shared/search_serializer.py (post-Plan-02 — confirm `request_echo` keyword arg present in both serializers)
  </read_first>
  <behavior>
    - Test 1: serialize_search_payload called WITHOUT `request_echo` returns an envelope without a top-level `request` key (Phase 77 download back-compat).
    - Test 2: serialize_search_payload called with a 7-key request_echo dict embeds it under `envelope['request']` verbatim.
    - Test 3: serialize_parallels_payload called WITHOUT `request_echo` returns an envelope without `request` key.
    - Test 4: serialize_parallels_payload called WITH a 6-key parallels_echo dict embeds it under `envelope['request']` verbatim. The dict must NOT contain `search_mode` or `responsa_options` keys (D-07).
    - Test 5: search echo round-trip with non-Responsa search_mode → echo's responsa_options and responsa_options_effective are both None.
    - Test 6: search echo round-trip with Responsa search_mode + cascade divergence → echo's responsa_options.ja = True, echo's responsa_options_effective.ja = False (constructed manually in the test fixture; this is a unit test for the serializer, not an end-to-end test of the cascade).
  </behavior>
  <action>
    Append new test functions to `tests/test_search_serializer.py`. Use existing fixture patterns. Skeleton:

    ```python
    def test_serialize_search_payload_omits_request_block_when_no_echo():
        """81A — back-compat: Phase 77 download path doesn't pass request_echo,
        so the envelope must NOT contain a `request` key."""
        from shared.search_serializer import serialize_search_payload
        env = serialize_search_payload(
            results=[],
            meta_mgr=None,
            query='foo',
            mode='text',
        )
        assert 'request' not in env

    def test_serialize_search_payload_embeds_request_echo_verbatim():
        """81A — when request_echo is supplied, it is embedded under `request`
        with the exact key set: search_mode, responsa_options,
        responsa_options_effective, gap, limit, limit_effective, filters."""
        from shared.search_serializer import serialize_search_payload
        echo = {
            'search_mode': 'exact',
            'responsa_options': None,
            'responsa_options_effective': None,
            'gap': 0,
            'limit': 50,
            'limit_effective': 50,
            'filters': None,
        }
        env = serialize_search_payload(
            results=[],
            meta_mgr=None,
            query='foo',
            mode='text',
            request_echo=echo,
        )
        assert env['request'] == echo
        assert set(env['request'].keys()) == {
            'search_mode', 'responsa_options', 'responsa_options_effective',
            'gap', 'limit', 'limit_effective', 'filters',
        }

    def test_serialize_search_payload_responsa_cascade_divergence():
        """81A AC6 — Responsa cascade case: requested ja=True, effective ja=False
        is preserved verbatim by the serializer (cascade detection is upstream)."""
        from shared.search_serializer import serialize_search_payload
        echo = {
            'search_mode': 'responsa',
            'responsa_options':           {'variants': True, 'ja': True,  'flex_spacing': False, 'bidirectional': False},
            'responsa_options_effective': {'variants': True, 'ja': False, 'flex_spacing': False, 'bidirectional': False},
            'gap': 0,
            'limit': 50,
            'limit_effective': 50,
            'filters': None,
        }
        env = serialize_search_payload(
            results=[],
            meta_mgr=None,
            query='foo',
            mode='Responsa',
            request_echo=echo,
        )
        assert env['request']['responsa_options']['ja'] is True
        assert env['request']['responsa_options_effective']['ja'] is False
        # search_mode is always identical to the input (D-04 — never downgraded).
        assert env['request']['search_mode'] == 'responsa'

    def test_serialize_parallels_payload_omits_request_block_when_no_echo():
        from shared.search_serializer import serialize_parallels_payload
        env = serialize_parallels_payload(
            main_results=[],
            filtered_results=[],
            meta_mgr=None,
            source_text='hello',
        )
        assert 'request' not in env

    def test_serialize_parallels_payload_embeds_request_echo_verbatim():
        """81A D-07 — parallels echo retains `mode` field name; does NOT have
        `search_mode` or `responsa_options`."""
        from shared.search_serializer import serialize_parallels_payload
        echo = {
            'mode': 'variants',
            'chunk_size': 5,
            'max_freq': None,
            'boundary_options': {'boundary_mode': 'full'},
            'limit_effective': 0,
            'filters': None,
        }
        env = serialize_parallels_payload(
            main_results=[],
            filtered_results=[],
            meta_mgr=None,
            source_text='hello',
            request_echo=echo,
        )
        assert env['request'] == echo
        assert 'search_mode' not in env['request']
        assert 'responsa_options' not in env['request']
        assert env['request']['mode'] == 'variants'
    ```

    Match the existing file's import / fixture style.
  </action>
  <verify>
    <automated>pytest tests/test_search_serializer.py -x --tb=short</automated>
    <automated>grep -c "request_echo" tests/test_search_serializer.py</automated>
    <automated>grep -c "responsa_options_effective" tests/test_search_serializer.py</automated>
  </verify>
  <acceptance_criteria>
    - `pytest tests/test_search_serializer.py -x` exits 0.
    - File contains tests for: search-no-echo, search-with-echo-7-keys, search-cascade-divergence, parallels-no-echo, parallels-with-echo-6-keys.
    - Cascade-divergence test asserts both that responsa_options.ja=True (input) AND responsa_options_effective.ja=False (post-cascade) AND search_mode='responsa' (D-04 unchanged).
    - Parallels-with-echo test asserts `search_mode` and `responsa_options` are absent from the parallels echo.
  </acceptance_criteria>
  <done>
    Serializer-level round-trip is verified for both endpoints. Plan 05 covers end-to-end matrix testing through the live FastAPI client.
  </done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: Extend tests/test_parallels_api.py with `request` echo presence assertion + Phase 80 regression</name>
  <files>tests/test_parallels_api.py</files>
  <read_first>
    - tests/test_parallels_api.py (read fully — understand the fixture for the parallels endpoint)
    - web/search_api.py parallels_endpoint (post Plan 02 — confirm `request_echo` is built and passed)
    - .planning/phases/81A-api-contract-expansion/81A-CONTEXT.md (D-07)
  </read_first>
  <behavior>
    - All existing Phase 80 tests in this file continue to pass (additive-only change to the endpoint).
    - One new test asserts that a successful POST /api/parallels response envelope contains a top-level `request` key.
    - The same test asserts the `request` dict has keys: `mode`, `chunk_size`, `max_freq`, `boundary_options`, `limit_effective`, `filters` — and DOES NOT contain `search_mode` or `responsa_options`.
    - The existing per-bucket-rate-limit-independence test (`test_parallels_rate_limit_independence` per Phase 80 docs in CLAUDE.md) still passes.
  </behavior>
  <action>
    Append a new test function. Use the existing client fixture and existing successful-call payload pattern:

    ```python
    def test_parallels_envelope_contains_request_echo(client, parallels_test_payload):
        """81A AC8 / D-07 — /api/parallels envelope gains a `request` echo block.
        Field name is `mode` (NOT `search_mode`) per D-07; no `responsa_options`.
        """
        resp = client.post('/api/parallels', json=parallels_test_payload)
        assert resp.status_code == 200
        body = resp.json()
        assert 'request' in body
        echo = body['request']
        assert set(echo.keys()) == {
            'mode', 'chunk_size', 'max_freq', 'boundary_options',
            'limit_effective', 'filters',
        }
        # D-07: parallels keeps `mode`, not `search_mode`.
        assert 'search_mode' not in echo
        assert 'responsa_options' not in echo
        # Echo's `mode` matches what the client sent.
        assert echo['mode'] == parallels_test_payload.get('mode', 'exact')
    ```

    If the file does not have a `parallels_test_payload` fixture, either adapt to the existing payload-construction pattern or define a minimal one inline.
  </action>
  <verify>
    <automated>pytest tests/test_parallels_api.py -x --tb=short</automated>
    <automated>grep -c "test_parallels_envelope_contains_request_echo" tests/test_parallels_api.py</automated>
  </verify>
  <acceptance_criteria>
    - `pytest tests/test_parallels_api.py -x` exits 0.
    - One new test exists named `test_parallels_envelope_contains_request_echo` (or equivalent) asserting the request echo presence and key set.
    - The Phase 80 regression suite (rate-limit independence, truncated_to_200 warning, mode enum acceptance, etc.) still passes unchanged.
  </acceptance_criteria>
  <done>
    AC8 verified at the API integration level: /api/parallels envelope contains the parallels-shaped `request` echo and Phase 80 regressions are intact.
  </done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Test fixtures → live FastAPI app | Tests instantiate the test client which exercises the full Pydantic + handler stack; same trust posture as production. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-81A04-01 | Tampering | test fixture rewrite | mitigate | Edit-in-place preserves git history; reviewer sees field rename diff cleanly. |
| T-81A04-02 | Repudiation | hidden coverage gaps | mitigate | grep verifies no `'mode': '...'` payload literals remain; positive grep confirms all migrated. |
| T-81A04-03 | DoS | test runtime | accept | Existing test suite runs in <60s; additive cases add ~1s. |
</threat_model>

<verification>
- All three pytest invocations exit 0.
- All grep checks return expected counts.
</verification>

<success_criteria>
Phase 78/80 hardening regression coverage is preserved at the new contract; the `request` echo block is verified at the serializer level (search + parallels) and at the integration level (parallels endpoint). Plan 05 owns the matrix tests.
</success_criteria>

<output>
Create `.planning/phases/81A-api-contract-expansion/81A-04-SUMMARY.md` listing: number of `mode→search_mode` payload replacements made, the new old-mode rejection test, the limit-ceiling assertion changes, the 5 new serializer round-trip tests, and the 1 new parallels-echo test. Confirm pytest exit 0 across all three files. Note that Step F repairs were limited to the 5 bounded categories.
</output>
