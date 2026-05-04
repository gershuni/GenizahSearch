---
phase: 81A-api-contract-expansion
plan: 03
subsystem: search-api
tags:
  - api
  - posthog
  - observability
requires:
  - 81A-01  # search_mode + ResponsaOptions on /api/search
  - 81A-02  # request echo + structured-meta drain finally block
provides:
  - capture_api_event(search_mode_value, responsa_options_count)
  - wrap_endpoint plumbing for the two new properties via captured_state
  - PostHog uniform event shape across /api/search, /api/browse, /api/parallels
affects:
  - web/api_hardening.py
  - web/search_api.py
tech-stack:
  added: []
  patterns:
    - "Provisional-from-raw-body PostHog capture (Codex MEDIUM-3) — preserve telemetry across cross-field validator rejections"
    - "Uniform event shape across endpoints — property always present, None/0 when not applicable"
key-files:
  modified:
    - web/api_hardening.py
    - web/search_api.py
    - tests/test_api_hardening.py
  created: []
decisions:
  - "Always include search_mode_value + responsa_options_count in props (not conditionally); None/0 when not applicable; uniform shape simplifies PostHog dashboard queries."
  - "Provisional capture of search_mode from the raw JSON body BEFORE Pydantic construction; overwrite after successful Pydantic validation (Codex MEDIUM-3). Cross-field rejections raised by @model_validator(mode='after') therefore retain telemetry on the offending mode value, while structural rejections (missing field, wrong type) leave it at None."
  - "Browse and Parallels handlers explicitly set captured_state['search_mode_value']=None and captured_state['responsa_options_count']=0 in addition to the wrap_endpoint default — defensive duplication; clearer for auditing the contract per endpoint."
  - "Test additions live in tests/test_api_hardening.py alongside the existing capture_api_event tests; soak tests under tests/test_search_api_soak.py are pre-existing red against Plan 01's removal of the legacy `mode` field and will be migrated by Plan 04 Task 4 — not in scope here."
metrics:
  duration: 18m
  completed: 2026-05-04
requirements:
  - API-EXPAND-04
requirements_addressed:
  - API-EXPAND-04
---

# Phase 81A Plan 03: PostHog `search_api_request` Event Expansion Summary

Per-request PostHog `search_api_request` events now carry two additional, always-present properties — `search_mode_value` (str | null) and `responsa_options_count` (int) — so D-08 dashboards can track adoption of each `search_mode`, frequency of all-False vs cascade-disabled Responsa flags, and rejection rate of the legacy `mode` field. Codex MEDIUM-3 provisional-capture-from-raw-body preserves telemetry on `invalid_combination` cross-field rejections.

## What Shipped

### `web/api_hardening.py`

- `capture_api_event` signature gained two keyword-only parameters with back-compat defaults:
  ```python
  search_mode_value: Optional[str] = None
  responsa_options_count: int = 0
  ```
  Both land in `props` on every emitted event (always present, never conditional). The integer is coerced via `int(responsa_options_count or 0)` so falsy values normalize to 0.
- `wrap_endpoint`'s `captured_state` initial dict now seeds `search_mode_value: None` and `responsa_options_count: 0`. The decorator's finally-block `capture_api_event(...)` call plumbs both through via `captured_state.get('search_mode_value')` / `.get('responsa_options_count', 0)`.

### `web/search_api.py`

- `search_endpoint` adds two local state variables alongside `validated_mode`:
  ```python
  posthog_search_mode_value: Optional[str] = None
  posthog_responsa_options_count: int = 0
  ```
- **Provisional capture site** (Codex MEDIUM-3) sits BEFORE Pydantic construction, immediately after `body = await request.json()`. When `body` is a dict and `body['search_mode']` is a string, `posthog_search_mode_value` is set to that raw value. `responsa_options_count` stays 0 provisionally — only counted after Pydantic confirms the `responsa_options` shape.
- **Post-validation overwrite** lives next to the `validated_mode = req.search_mode` assignment. Identical content for valid mode strings, but explicit; and gives a single site to compute the count from the parsed `ResponsaOptions`:
  ```python
  if req.search_mode == 'responsa' and req.responsa_options is not None:
      posthog_responsa_options_count = sum([
          bool(opts.variants), bool(opts.ja),
          bool(opts.flex_spacing), bool(opts.bidirectional),
      ])
  else:
      posthog_responsa_options_count = 0
  ```
- The `finally` block's `capture_api_event(...)` call now passes the two new keyword args. Plan 02's adjacent string-drain (`_consume_last_responsa_downgrade`) and meta-drain (`_consume_last_responsa_downgrade_meta`) blocks were not modified — confirmed by visual diff and grep.
- `browse_endpoint` and `parallels_endpoint` each add an explicit pair:
  ```python
  captured_state['search_mode_value'] = None
  captured_state['responsa_options_count'] = 0
  ```
  immediately after the existing `captured_state['mode'] = ...` line. Defensive duplication (the wrap_endpoint default already covers this), but auditable per endpoint.

### `tests/test_api_hardening.py`

Five new tests under a "Phase 81A Plan 03" section:

| Test | Asserts |
|------|---------|
| `test_capture_api_event_signature_has_new_kwargs` | Signature exposes the two new keyword-only params with default None / 0. |
| `test_capture_api_event_back_compat_omits_new_kwargs` | Old call sites that omit the kwargs still emit; both keys present in props as None / 0. |
| `test_capture_api_event_propagates_search_mode_value` | `search_mode_value='exact'` lands in props verbatim. |
| `test_capture_api_event_propagates_responsa_options_count` | `responsa_options_count=3` lands in props as int 3. |
| `test_capture_api_event_responsa_options_count_coerces_to_int` | None/falsy coerce to 0; type is int. |

A shared `_capture_one_event` helper monkeypatches `_event_queue` with a `FakeQueue` and returns the single captured event dict.

## Verification

- `python -m py_compile web/api_hardening.py web/search_api.py` exits 0.
- 5 new + 3 pre-existing capture_api_event tests in `tests/test_api_hardening.py` all green:
  ```
  tests/test_api_hardening.py::test_capture_api_event_non_blocking PASSED
  tests/test_api_hardening.py::test_capture_api_event_sampling PASSED
  tests/test_api_hardening.py::test_capture_api_event_does_not_log_query_or_filters PASSED
  tests/test_api_hardening.py::test_capture_api_event_signature_has_new_kwargs PASSED
  tests/test_api_hardening.py::test_capture_api_event_back_compat_omits_new_kwargs PASSED
  tests/test_api_hardening.py::test_capture_api_event_propagates_search_mode_value PASSED
  tests/test_api_hardening.py::test_capture_api_event_propagates_responsa_options_count PASSED
  tests/test_api_hardening.py::test_capture_api_event_responsa_options_count_coerces_to_int PASSED
  ```
- Signature inspection check passes:
  ```
  >>> import inspect
  >>> from web.api_hardening import capture_api_event
  >>> sig = inspect.signature(capture_api_event)
  >>> 'search_mode_value' in sig.parameters and 'responsa_options_count' in sig.parameters
  True
  ```

## Pre-existing Failures (Out of Scope — Plan 04 Territory)

`tests/test_search_api.py` and `tests/test_search_api_soak.py` have ~24 pre-existing failures that originate from Plan 01's removal of the legacy `mode` field (now `search_mode`). The plan acceptance criteria explicitly note "tests in [the soak] file are migrated to `search_mode` by Plan 04 Task 4". I confirmed these failures pre-date this plan by stashing my changes and re-running the same tests — same failures appear before and after. They are NOT introduced by Plan 03.

## Deviations from Plan

None — plan executed exactly as written. The TDD RED→GREEN cycle followed the plan's `tdd="true"` directive: write failing tests first (commit `a15e789f`), then implement (commit `faa9de48`).

## TDD Gate Compliance

- RED gate: `test(81A-03): add failing tests for capture_api_event search_mode_value + responsa_options_count` — commit `a15e789f`.
- GREEN gate: `feat(81A-03): extend capture_api_event with search_mode_value + responsa_options_count` — commit `faa9de48`.
- REFACTOR: not needed; no cleanup pass required.

## Downstream Hooks (Plan 05)

Plan 05 Section 7 is expected to add an end-to-end test asserting the property presence on the cross-field-rejection path: `test_posthog_search_mode_value_present_on_invalid_combination`. The provisional-capture-before-Pydantic site in `search_endpoint` is the load-bearing implementation that test will exercise — when `@model_validator(mode='after')` raises `APIError('invalid_combination')`, the captured event will carry `search_mode_value` equal to whatever raw string the client sent (e.g. `'exact'` for an `exact + responsa_options` combo).

## Self-Check: PASSED

- `web/api_hardening.py` modified (signature change, props additions, wrap_endpoint plumbing) — FOUND.
- `web/search_api.py` modified (provisional capture, overwrite, finally call, browse/parallels explicit set) — FOUND.
- `tests/test_api_hardening.py` modified (5 new tests + helper) — FOUND.
- Commit `a15e789f` (RED) — FOUND in `git log --oneline`.
- Commit `faa9de48` (GREEN) — FOUND in `git log --oneline`.
- Plan 02's string-drain + meta-drain finally regions untouched — verified by re-reading `web/search_api.py:707-728`.
