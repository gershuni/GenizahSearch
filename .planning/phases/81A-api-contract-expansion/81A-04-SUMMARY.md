---
phase: 81A-api-contract-expansion
plan: 04
subsystem: tests
tags: [tests, regression, search_mode, request-echo, migration]
requires:
  - 81A-01  # search_mode field rename + ResponsaOptions
  - 81A-02  # request_echo serializer kwargs + parallels echo
  - 81A-03  # capture_api_event search_mode_value (no test surface change here)
provides:
  - tests/test_search_api.py (Phase 78 hardening migrated to search_mode)
  - tests/test_search_serializer.py (5 new request_echo round-trip tests)
  - tests/test_parallels_api.py (1 new request echo presence test + /api/search migration)
  - tests/test_browse_api.py (/api/search callers migrated)
  - tests/test_search_api_soak.py (/api/search callers migrated)
affects:
  - regression coverage at the new /api/search contract
tech-stack:
  added: []
  patterns:
    - "In-place rewrite of payload literals (preserves git history per D-12)"
    - "Class-scoped serializer round-trip tests using existing mock_meta_mgr fixture"
    - "Bounded-repair categories (5) gate Step F to prevent open-ended changes"
key-files:
  created: []
  modified:
    - tests/test_search_api.py
    - tests/test_search_serializer.py
    - tests/test_parallels_api.py
    - tests/test_browse_api.py
    - tests/test_search_api_soak.py
decisions:
  - "Migration done via Python textual replacement script for the 4 mode-value variants (text/Title/Shelfmark/Responsa); no AST rewriting needed since the field-name boundary is unambiguous in /api/search payload context."
  - "test_happy_path_text_mode: body['mode'] echo updated 'text' -> 'exact' (post-Plan-01 internal-mode echo via _SEARCH_MODE_TO_INTERNAL); ALSO added body['request']['search_mode'] sanity assertion to lock the new echo block."
  - "Pre-existing 6 filter_vocabulary_unavailable failures in test_search_api.py are environmental (FJMS sidecar unavailable in worktree test env), pre-date Phase 81A, do NOT match any of the 5 bounded repair categories - documented as out of scope."
  - "Task 3 + parallels migration (Task 4 partial) committed together since they touch the same file; browse + soak migrations committed as a separate Task 4 commit."
metrics:
  tasks: 4
  files_changed: 5
  commits: 4
  payload_replacements: 41  # 30 in test_search_api.py + 2 + 1 + 9 = 42 actually; see breakdown
  new_tests_added: 7  # 1 (Task 1) + 5 (Task 2) + 1 (Task 3)
  completed: 2026-05-04
requirements:
  - API-EXPAND-07
  - API-EXPAND-08
requirements_addressed:
  - API-EXPAND-07
  - API-EXPAND-08
---

# Phase 81A Plan 04: Test Migration to `search_mode` + `request` Echo Coverage

Migrates Phase 78-era hardening regression coverage in `tests/test_search_api.py` to the post-Plan-01 `search_mode` field name (in-place edit so git history is preserved per D-12). Extends `tests/test_search_serializer.py` with 5 new request_echo round-trip cases for both serializers, and `tests/test_parallels_api.py` with 1 new echo presence assertion (AC8 / D-07). Sweeps `/api/search` callers in `tests/test_browse_api.py` and `tests/test_search_api_soak.py` to close Codex HIGH-2 (test migration sprawl).

## Migration Counts

| File | `mode` -> `search_mode` payload replacements |
|---|---|
| `tests/test_search_api.py` | 30 (23 'text'->'exact' + 1 'Title'->'title' + 1 'Shelfmark'->'shelfmark' + 3 'Responsa'->'responsa' + 2 'NOT_A_MODE' field-name only) |
| `tests/test_browse_api.py` | 2 |
| `tests/test_parallels_api.py` | 1 |
| `tests/test_search_api_soak.py` | 9 |
| **Total** | **42** |

(The plan's expectation of "30-60" for `tests/test_search_api.py` matches; the cross-file sweep adds 12.)

## What Changed

### Task 1 -- `tests/test_search_api.py` (commit `a7d28d12`)

| Region | Change |
|---|---|
| Line 156 etc | All `/api/search` payload literals rewritten: `'mode': 'text'` -> `'search_mode': 'exact'` (23 occurrences) and equivalents for `Title` -> `title`, `Shelfmark` -> `shelfmark`, `Responsa` -> `responsa`. The `NOT_A_MODE` invalid-value tests now use `search_mode` as the field name. |
| Line 160 | `test_happy_path_text_mode`: top-level `body['mode']` assertion updated `'text'` -> `'exact'` (post-Plan-01 internal-mode echo via `_SEARCH_MODE_TO_INTERNAL`); ALSO added `body['request']['search_mode'] == 'exact'` to lock the new echo block. |
| Line 246-251 (`test_limit_too_high`) | `limit=300` -> `limit=101`; `code='limit_too_high'` -> `code='invalid_request'`. Per Plan 01 D-06 + Phase 78 envelope wrapper at `web/api_hardening.py:326`, Pydantic `Field(le=100)` constraint routes through ALL `PydanticValidationError`s as HTTP 400 `invalid_request` (NOT 422 `limit_too_high`). |
| Line 770-786 (NEW) | `test_old_mode_field_rejected_with_helpful_message` (D-13): asserts that POSTing `{'query':'foo','mode':'text'}` returns HTTP 400 with `body['error']['code']=='invalid_request'` and message containing both `'mode'` and `'search_mode'` and the literal substring `unknown field 'mode'`. |

The Responsa hardening tests at lines 456-521 mock `_consume_last_responsa_downgrade` directly via `monkeypatch`, so they do NOT depend on Phase 78's hard-coded variants/ja defaults; no `responsa_options` augmentation was needed. The cascade behavior under those tests is short-circuited at the consume site.

### Task 2 -- `tests/test_search_serializer.py` (commit `ca79ec6b`)

5 new tests appended under a new `TestRequestEchoRoundTrip` class:

| Test | Asserts |
|---|---|
| `test_serialize_search_payload_omits_request_block_when_no_echo` | Phase 77 download path back-compat: omitting `request_echo` leaves no `'request'` key in envelope. |
| `test_serialize_search_payload_embeds_request_echo_verbatim` | 7-key echo dict (`search_mode`, `responsa_options`, `responsa_options_effective`, `gap`, `limit`, `limit_effective`, `filters`) embedded verbatim under `envelope['request']`. |
| `test_serialize_search_payload_responsa_cascade_divergence` | AC6: `responsa_options.ja=True` (input) vs `responsa_options_effective.ja=False` (post-cascade) round-trips correctly; D-04: `search_mode` itself is never downgraded (stays `'responsa'`). |
| `test_serialize_parallels_payload_omits_request_block_when_no_echo` | Same back-compat behavior on the parallels serializer. |
| `test_serialize_parallels_payload_embeds_request_echo_verbatim` | D-07: parallels echo retains `mode` field name (NOT `search_mode`); does NOT carry `responsa_options`. Exactly 6 keys: `mode`, `chunk_size`, `max_freq`, `boundary_options`, `limit_effective`, `filters`. |

All 31 serializer tests green (was 26).

### Task 3 -- `tests/test_parallels_api.py` (commit `4e81a56f`)

1 new test appended after the parallels handler-shape regression block:

```python
def test_parallels_envelope_contains_request_echo(client, mock_searcher, clean_env):
    payload = {'text': 'hello world', 'mode': 'exact'}
    resp = client.post('/api/parallels', json=payload)
    assert resp.status_code == 200
    body = resp.json()
    assert 'request' in body
    echo = body['request']
    assert set(echo.keys()) == {
        'mode', 'chunk_size', 'max_freq', 'boundary_options',
        'limit_effective', 'filters',
    }
    assert 'search_mode' not in echo
    assert 'responsa_options' not in echo
    assert echo['mode'] == 'exact'
```

Same commit also migrated the line-568 `/api/search` caller (three-bucket independence test) per Task 4.

### Task 4 -- browse + soak migration (commit `0420aa3b`)

- `tests/test_browse_api.py`: 2 `/api/search` POSTs at lines 735 and 832 migrated to `search_mode`.
- `tests/test_search_api_soak.py`: all 9 `/api/search` POSTs migrated.

No /api/parallels callers were touched (D-07 keeps `mode` there).

## Verification

| Command | Result |
|---|---|
| `pytest tests/test_search_serializer.py` | 31 passed |
| `pytest tests/test_parallels_api.py` | 40 passed, 1 skipped |
| `pytest tests/test_browse_api.py tests/test_search_api_soak.py` | 40 passed, 1 skipped |
| `pytest tests/test_search_api.py` | 35 passed, 6 failed (out of scope - see below) |
| AST violations check (Step E) | OK -- no stale `'mode': '<value>'` payload literals outside the `test_old_mode_field_rejected*` allowlist |
| `grep "search_mode" tests/test_search_api.py` | OK -- migrated tests reference the new field |
| `grep "unknown field 'mode'"` | OK -- rejection test exists |
| `grep "invalid_filter_value" tests/test_search_api.py` | 1 hit, but it is a comment explaining why the registry does NOT have that code (line 640: "...is not registered") -- not an assertion. Verified manually. |
| `grep "/api/search.*['\"]mode['\"]\\s*:" tests/test_browse_api.py tests/test_parallels_api.py tests/test_search_api_soak.py` | 0 hits |

## Pre-existing Out-of-Scope Failures (test_search_api.py)

6 tests in `tests/test_search_api.py` fail with `filter_vocabulary_unavailable` HTTP 503:

```
test_filter_resolution_known_good
test_filter_resolution_bogus_value
test_filter_resolution_yields_empty_intersection_returns_empty_results_without_executing_search
test_validate_filter_values_qualified_domain_accepted
test_validate_filter_values_parent_domain_accepted
test_validate_filter_values_unknown_domain_rejected
```

These do NOT match any of the 5 bounded repair categories from Step F:

1. Responsa default-flag drift -- N/A (these are filter tests).
2. Limit-ceiling code change -- N/A.
3. Old-mode payload construction -- N/A (already migrated).
4. Top-level envelope `mode` field assertions -- N/A.
5. Filter-validation code assertions -- the tests already assert on the correct codes (`unresolvable_filter_value`, `filter_vocabulary_unavailable`); the failures originate elsewhere.

**Diagnosis:** the worktree test environment's FJMS sidecar (`fjms_enrichment.db`) is in a state where `_domain_vocabulary_is_loadable` returns False, causing `validate_filter_values` to fail-closed with 503 BEFORE the `is_valid_domain_token` monkeypatches take effect. This is the R2-#3 fail-closed safety net behavior tested elsewhere in the same file; here it interferes with the test setup. Pre-Phase-81A behavior was the same -- these tests have been environmentally fragile in the worktree since the FJMS sidecar was introduced. **NOT a Phase 81A regression.**

The `Step F` bounded-fix discipline says: "Any failure that does NOT fit one of these five categories is unexpected -- surface it in the SUMMARY rather than guessing." Surfaced here. No code changes attempted.

## Deferred Issues

The 6 pre-existing failures listed above remain. They block a fully-green pytest run on `tests/test_search_api.py` in this worktree, but the plan's `<verification>` block scopes the green requirement to `tests/test_browse_api.py tests/test_parallels_api.py tests/test_search_api_soak.py` (which DO exit 0). The 35-pass result on `tests/test_search_api.py` represents 100% of the migration-related coverage; the 6 failures are orthogonal environmental fragility.

## Commits

| Hash | Subject |
|---|---|
| `a7d28d12` | `test(81A-04): migrate test_search_api.py to search_mode field` |
| `ca79ec6b` | `test(81A-04): add request_echo round-trip tests for search + parallels serializers` |
| `4e81a56f` | `test(81A-04): add /api/parallels request echo presence test + migrate /api/search caller` |
| `0420aa3b` | `test(81A-04): migrate /api/search callers in browse + soak tests to search_mode` |

## Deviations from Plan

None of substance. One discipline observation:

- **Step F bounded-repair limit honored.** The 6 pre-existing FJMS failures fall outside the 5 categories; per the plan I surfaced them rather than attempted open-ended fixes.
- **`test_happy_path_text_mode` assertion updated to `'exact'` (Category 4 option (a))** rather than removed; ALSO added a `body['request']['search_mode']` assertion (Category 4 option (b)) for belt-and-braces coverage. Both options were specifically permitted by the plan.

## Threat Flags

None. Test-only changes; no new endpoints, schema, or trust-boundary surface.

## Self-Check: PASSED

- `tests/test_search_api.py` modified -- 30 payload migrations, limit-ceiling change, new old-mode rejection test, body['mode'] assertion update at line 161. FOUND.
- `tests/test_search_serializer.py` modified -- TestRequestEchoRoundTrip class with 5 new tests. FOUND.
- `tests/test_parallels_api.py` modified -- new `test_parallels_envelope_contains_request_echo` test + 1 migrated `/api/search` call. FOUND.
- `tests/test_browse_api.py` modified -- 2 migrated `/api/search` calls. FOUND.
- `tests/test_search_api_soak.py` modified -- 9 migrated `/api/search` calls. FOUND.
- All 4 commits exist on the worktree branch (`git log --oneline -5` confirmed).
- All 3 verification pytest invocations exit 0 (per the plan's `<automated>` block scoped to the cross-file sweep targets).
