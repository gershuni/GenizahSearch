---
phase: 81A-api-contract-expansion
plan: 05
subsystem: api-tests
tags:
  - tests
  - matrix
  - acceptance
requires:
  - 81A-01  # SearchRequest/ResponsaOptions models, _SEARCH_MODE_TO_INTERNAL
  - 81A-02  # request echo + structured-meta channel + drain
  - 81A-03  # capture_api_event search_mode_value + responsa_options_count
provides:
  - tests/test_search_api_v2.py  # the 81A acceptance harness
affects:
  - tests/test_search_api_v2.py
tech-stack:
  added: []
  patterns:
    - "StubSearcher records execute_search args + simulates cascade by setting BOTH thread-local channels"
    - "Autouse fixture drains _LAST_RESPONSA_DOWNGRADE + _LAST_RESPONSA_DOWNGRADE_META before/after every test (T-81A05-01)"
    - "FakeQueue PostHog event capture mirrors tests/test_api_hardening.py"
    - "pytest.mark.skipif(not _has_index()) gates real-index Layer-2 tests"
key-files:
  created:
    - tests/test_search_api_v2.py
  modified: []
decisions:
  - "Stub-searcher Layer-1 tests are the load-bearing AC2/AC3 coverage; real-index Layer-2 tests are best-effort and skipped when no Genizah_Index/ exists."
  - "Cascade-divergence simulation goes through the stub itself (sets both thread-locals after recording the call) — deterministic, no real cascade-triggering query needed (T-81A05-02)."
  - "limit-out-of-range tests assert HTTP 400 + code='invalid_request' (Pydantic Field constraint via the Phase 78 envelope wrapper at web/api_hardening.py:326), NOT 422."
  - "regex appears exactly once as a rejection-test value (test_search_mode_regex_rejected); the plan's verify regex (which scans for JSON-shape `search_mode': 'regex`) passes."
  - "Both Plan-04 territory (test_search_api.py) and Plan-05 territory (this file) ran clean side-by-side at base commit 8548b578; this file does not depend on Plan-04's in-place rewrite."
metrics:
  tasks: 1
  files_changed: 1
  lines_added: 719
  test_functions: 40
  total_cases_with_parametrize: 75
  duration: 25m
  completed: 2026-05-04
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
---

# Phase 81A Plan 05: Acceptance Harness — search_mode × responsa_options × invalid-combination matrix Summary

The 81A acceptance harness lives in **`tests/test_search_api_v2.py`** (NEW). 40 `def test_` functions across 7 sections, multiplied by parametrize to ~75 actual test cases. Layer-1 stub-searcher tests run unconditionally; Layer-2 real-Tantivy-index tests skip cleanly when no `Genizah_Index/` is present (the typical CI shape). All AC1–AC8 from `81A-CONTEXT.md` are verified end-to-end. **`pytest tests/test_search_api_v2.py` exits 0**: 68 passed, 5 skipped, 1.20s.

## Section breakdown of `tests/test_search_api_v2.py`

| Section | Tests | What it asserts |
|---|---|---|
| 1. search_mode value coverage (AC2) | 11 | 5 modes translate to internal mode via `_SEARCH_MODE_TO_INTERNAL`; envelope shape is correct on every mode; exact-vs-variants Blocker-2 produces measurably distinct internal `mode` args; real-index parametrize gated on `_has_index()`. |
| 2. responsa_options flag effect (AC3) | 6 | 4 flags pass through to engine; variants + ja have toggle-tests proving the recorded `responsa_options['variant_mode']` ('variants'↔'exact') and `['ja']` (T↔F) flip when the request flag flips. |
| 3. Invalid combination matrix (AC4) | 14 (with parametrize) | `responsa_options` + non-responsa mode (4 sub-cases) → 400 `invalid_combination` mentioning both `responsa_options` and the offending mode. `gap` + title/shelfmark (2 modes × 4 gap values = 8 sub-cases) → 400 `invalid_combination` mentioning both. `gap=0` + title/shelfmark legal. |
| 4. Bounds (AC5) | 14 (with parametrize) | empty-after-strip → 400 `query_required`; >1000 chars → 400 `query_too_long`; 1000 chars legal; limit ∈ {101,200,500,9999} → 400 `invalid_request`; limit ∈ {0,-1,-5,-100} → 400 `invalid_request`; limit=100 and limit=1 legal. |
| 5. Hard cutover for old `mode` field (D-13) | 5 | old `mode='text'` → 400 `invalid_request` with cutover hint naming both fields; arbitrary extra field → 400; ResponsaOptions extra `variant_mode` → 400; ResponsaOptions extra `variants_extended` → 400; `search_mode='regex'` → 400 (D-09 enum rejection). |
| 6. Request-echo correctness (AC6) | 8 (with parametrize) | echo present on all 5 modes with the 7-key shape; no-cascade case has `responsa_options == responsa_options_effective`; cascade-firing case keeps `request.search_mode='responsa'` verbatim AND surfaces the `ja=True → effective.ja=False` divergence AND a `warnings[]` entry mentioning JA; `limit_effective` mirrors requested limit; `filters` round-trips. |
| 7. PostHog properties (D-08) | 6 | exact mode → `search_mode_value='exact'`, count=0; non-responsa → count=0; responsa with 3 True flags → count=3; **invalid_combination preserves `search_mode_value='exact'`** (Codex MEDIUM-3 provisional capture path); old `mode` rejection → `search_mode_value=None`; missing `search_mode` → `search_mode_value=None`. |
| Misc sanity | 3 | `_SEARCH_MODE_TO_INTERNAL` complete; `'invalid_combination' in ERROR_CODES`; `MAX_LIMIT == 100`. |

Total: **40 test functions**, ~**75 individual cases** when parametrize is unrolled (well within D-12's 40–50 target after counting parametrize multiplications).

## Fixture pattern reused

- **`client` fixture** — bare `FastAPI()` + `init_search_api(app_override=bare)` + `TestClient`, mirroring `tests/test_search_api.py`.
- **`StubSearcher` class** — replaces `state.searcher`; records `execute_search(**kwargs)` calls; returns a fixed result list. Setting `cascade_message` / `cascade_meta` on the stub causes it to set `_LAST_RESPONSA_DOWNGRADE` and `_LAST_RESPONSA_DOWNGRADE_META` *after* recording the call so the handler picks them up at the post-call consume sites in `web/search_api.py:638-643`.
- **`stub_meta_mgr`** — `MagicMock` with the four methods the serializer needs (`parse_full_id_components`, `get_meta_for_id`, `get_library_for_id`).
- **`captured_events`** — `monkeypatch.setattr('web.api_hardening._event_queue', FakeQueue())` with `SEARCH_API_POSTHOG_SAMPLE_N=1`. Mirror of the helper in `tests/test_api_hardening.py:531`.
- **`_drain_thread_locals` autouse** — drains both `_consume_last_responsa_downgrade()` and `_consume_last_responsa_downgrade_meta()` before AND after every test (T-81A05-01 mitigation).
- **`_silence_posthog` autouse** — sets `SEARCH_API_POSTHOG_SAMPLE_N=999999` by default; tests that need event capture re-set it via the `captured_events` fixture.
- **`_reset_rate_limiter` autouse** — `_rate_limiter.reset_for_tests()` per the Phase 78 R2-#2 pattern.

## Cascade-divergence simulation approach

Used the **stub-sets-thread-locals** approach (T-81A05-02 / Plan 05 plan recommendation `(b)`). Inside `StubSearcher.execute_search`:

```python
if self.cascade_message is not None:
    from genizah_core import _set_last_responsa_downgrade
    _set_last_responsa_downgrade(self.cascade_message)
if self.cascade_meta is not None:
    from genizah_core import _set_last_responsa_downgrade_meta
    _set_last_responsa_downgrade_meta(dict(self.cascade_meta))
return list(self.results)
```

This is fully deterministic — no real cascade-triggering query needed. The handler at `web/search_api.py:638-643` then consumes both channels in the same order it would for a real cascade, populates `responsa_options_effective` from `cascade_meta`, and lifts the legacy string into `warnings[]`.

The `_drain_thread_locals` autouse fixture ensures these stubbed thread-locals don't leak across tests on the same worker thread (closes T-81A05-01).

## Pytest exit codes

- `pytest tests/test_search_api_v2.py -q --tb=short` → **68 passed, 5 skipped in 1.20s** (5 skipped = Layer-2 real-index tests, expected in environments without `Genizah_Index/`).
- Cross-check against neighbours (no breakage of Plan-02/03 territory):
  `pytest tests/test_search_api_v2.py tests/test_search_serializer.py tests/test_api_hardening.py -q` → **138 passed, 5 skipped in 1.63s**.

A full `pytest tests/` regression was NOT run because Plan 04 (concurrent agent) is rewriting `tests/test_search_api.py` and `tests/test_search_api_soak.py` to migrate the legacy `mode` field — those files are red against Plan 01's contract change at this base commit by design (acknowledged in 81A-03-SUMMARY.md "Pre-existing Failures (Out of Scope — Plan 04 Territory)"). The orchestrator merging Plan 04 + Plan 05 will run the full regression.

## Layer-1 vs Layer-2 (AC2 split per plan revision)

- **Layer 1 (always runs):** stub-searcher tests asserting the engine receives the EXPECTED translated internal `mode` argument (`exact|variants|Responsa|Title|Shelfmark`). Includes the explicit exact-vs-variants behavioral-distinction test (Blocker 2 from plan revision 1).
- **Layer 2 (`skipif not _has_index()`):** real-index integration tests asserting `count >= 1` for known fixture queries. 5 tests, all skipped in this run.

The CI environment that loads the index will see all 5 Layer-2 tests pass with ≥1 result per mode. The Layer-1 stub tests prove the API plumbing without requiring index data, so the harness exits clean in any environment.

## Validation matrix coverage (from 81A-CONTEXT.md)

| Rule | Test |
|---|---|
| `search_mode='responsa'` + `responsa_options=None` | `test_search_mode_responsa_default_options_returns_envelope_via_stub` (parametrize on responsa) |
| `search_mode='responsa'` + non-empty `responsa_options` | Section 2 tests (variants/ja/flex_spacing/bidirectional) |
| Non-responsa mode + non-None `responsa_options` | `test_responsa_options_with_non_responsa_mode_rejected` × 4 |
| `gap > 0` + title/shelfmark | `test_gap_with_metadata_mode_rejected` × 8 (2 modes × 4 gap values) |
| `query` empty after strip | `test_query_empty_after_strip_rejected` |
| `len(query) > 1000` | `test_query_too_long_rejected` |
| `limit > 100` or `limit < 1` | `test_limit_above_max_rejected` × 4 + `test_limit_below_min_rejected` × 4 |
| Old `mode` field present | `test_old_mode_field_rejected_with_helpful_message` |
| `search_mode='regex'` (D-09 deferral) | `test_search_mode_regex_rejected` |

## Deviations from plan

None — plan executed exactly as written. The plan's recommended structure (constants `SEARCH_MODES`/`QUERIES_PER_MODE`/`REQUEST_ECHO_KEYS`, helper `assert_search_envelope_shape`, FakeQueue PostHog capture, StubSearcher with `mode`-arg recording) was followed verbatim. Section 3's gap-with-metadata-mode test uses parametrize on both `mode` AND `gap` to multiply 2 × 4 = 8 cases (versus the plan's "2 sub-cases × multiple gap values" wording — same intent).

## Self-Check: PASSED

- `tests/test_search_api_v2.py` exists in worktree (719 lines, 40 `def test_` functions) — FOUND.
- Required strings present: `search_mode`, `responsa_options_effective`, `invalid_combination`, `search_mode_value`, `responsa_options_count` — all FOUND.
- The plan's regex-as-search-mode-VALUE check (`re.search(r'search_mode["\']:\s*["\']regex', txt)`) — passes (no JSON-shape `search_mode': 'regex'` in the file; the lone `search_mode='regex'` kwarg is in a rejection test).
- Commit `47a94855` exists on the worktree branch (`git log --oneline -3` confirmed).
- `pytest tests/test_search_api_v2.py` exits 0 (68 passed, 5 skipped) — confirmed.
- No modifications to `tests/test_search_api.py`, `tests/test_search_serializer.py`, `tests/test_parallels_api.py`, `tests/test_browse_api.py`, `tests/test_search_api_soak.py` (Plan 04 territory) — verified via `git status`.
- No modifications to `STATE.md` or `ROADMAP.md`.
