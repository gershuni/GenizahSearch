---
phase: 81A-api-contract-expansion
plan: 03
type: execute
wave: 3
depends_on:
  - 81A-01
  - 81A-02
files_modified:
  - web/search_api.py
  - web/api_hardening.py
autonomous: true
requirements:
  - API-EXPAND-04
requirements_addressed:
  - API-EXPAND-04
tags:
  - api
  - posthog
  - observability
must_haves:
  truths:
    - "Every PostHog `search_api_request` event for endpoint=search carries a `search_mode_value` property containing the literal string the client sent (one of: exact, variants, responsa, title, shelfmark)."
    - "Every PostHog `search_api_request` event for endpoint=search carries a `responsa_options_count` integer property (0 when `responsa_options` is None or `search_mode != 'responsa'`; otherwise the count of True flags in the four ResponsaOptions booleans)."
    - "Events for endpoint=browse and endpoint=parallels do NOT include `search_mode_value` (or it is null) and have `responsa_options_count = 0`."
    - "On Pydantic-rejected requests (invalid_request) where `search_mode` cannot be validated, `search_mode_value` is null and `responsa_options_count` is 0."
  artifacts:
    - path: "web/api_hardening.py"
      provides: "capture_api_event extended with search_mode_value + responsa_options_count props"
      contains: "search_mode_value"
  key_links:
    - from: "web/search_api.py search_endpoint finally block"
      to: "web/api_hardening.py capture_api_event"
      via: "two new keyword args (search_mode_value, responsa_options_count) computed once-per-request"
      pattern: "search_mode_value"
---

<objective>
Extend the existing per-request PostHog `search_api_request` event with two new properties: `search_mode_value` (str — the literal `search_mode` enum string, one of `exact|variants|responsa|title|shelfmark`, or null when validation failed before assignment) and `responsa_options_count` (int — count of True flags in the validated ResponsaOptions, 0 otherwise).

**Wave 3 (per revision 1).** Plan 02 (Wave 2) modifies the same finally block (defensive meta-drain insertion). Plan 03 must run AFTER Plan 02 to avoid file-write conflicts on `web/search_api.py`'s finally block. Plan 03 modifies the `capture_api_event(...)` call region only; Plan 02 modifies the drain region only. Sequential execution prevents merge conflicts.

Purpose: Per D-08, observability of the new contract. PostHog dashboards will be able to track adoption of each `search_mode`, frequency of Responsa with all-False options vs cascade-disabled, and rejection rate of the old `mode` field.

Output: `web/api_hardening.py` `capture_api_event` accepts two new keyword args. `web/search_api.py` search_endpoint computes them once per request and passes them through. `parallels_endpoint` and `browse_endpoint` pass `search_mode_value=None, responsa_options_count=0` (so the event shape is uniform across endpoints).
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

<interfaces>
<!-- Existing exports -->

From web/api_hardening.py (line ~572 — current capture_api_event signature):
```python
def capture_api_event(
    *,
    endpoint: str,
    mode: Optional[str],
    latency_seconds: float,
    result_count: Optional[int],
    status_code: int,
    error_code: Optional[str],
    client_ip: str,
) -> None:
    # Builds props = {endpoint, mode, latency_bucket, status_code, error_code [, result_count_bucket]}
    # Enqueues to a best-effort queue; never blocks; never raises.
```

From web/search_api.py search_endpoint finally block (lines ~573-615 post-Plan-02):
```python
finally:
    try:
        elapsed = time.monotonic() - t0
        capture_api_event(
            endpoint=endpoint_name,
            mode=validated_mode,        # ← post-Plan-01 this is req.search_mode
            latency_seconds=elapsed,
            result_count=result_count,
            status_code=status_code,
            error_code=error_code,
            client_ip=client_ip,
        )
    except Exception:
        logger.warning('capture_api_event failed in finally block')
    # Plan 02 drain regions (string + meta) — DO NOT MODIFY, that is Plan 02's region:
    try:
        _consume_last_responsa_downgrade()
    except Exception:
        logger.warning('thread-local downgrade drain failed in finally')
    try:
        from genizah_core import _consume_last_responsa_downgrade_meta as _drain_meta
        _drain_meta()
    except Exception:
        logger.warning('thread-local downgrade-meta drain failed in finally')
```

From parallels_endpoint (uses @wrap_endpoint decorator, line 725) and browse_endpoint (line 606):
The decorator (in web/api_hardening.py — search for `def wrap_endpoint`) calls `capture_api_event` from its own finally block based on `captured_state['mode']`. The two new keyword args must be plumbed through the decorator too, OR the decorator just passes the new args as defaults (None / 0).
</interfaces>
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: Extend capture_api_event signature with search_mode_value + responsa_options_count, plumb through wrap_endpoint, wire from search_endpoint</name>
  <files>web/api_hardening.py, web/search_api.py</files>
  <read_first>
    - web/api_hardening.py — find `def capture_api_event` (~line 572) and `def wrap_endpoint` (search for it; it's the decorator used by browse/parallels)
    - web/search_api.py finally block in search_endpoint (~line 573-615 post-Plan-02; the meta-drain block from Plan 02 is present — do not touch it)
    - web/search_api.py parallels_endpoint and browse_endpoint (which use @wrap_endpoint)
    - .planning/phases/81A-api-contract-expansion/81A-CONTEXT.md (D-08)
  </read_first>
  <behavior>
    - capture_api_event(..., search_mode_value='exact', responsa_options_count=0) emits a PostHog event whose `properties` dict contains those two new keys with those values.
    - capture_api_event(..., search_mode_value=None, responsa_options_count=0) emits a PostHog event with `search_mode_value=None` and `responsa_options_count=0`.
    - capture_api_event called WITHOUT the two new args (existing call sites that haven't been updated) defaults them to None / 0 and still emits successfully.
    - For a successful /api/search call with search_mode='responsa' + responsa_options={variants:T,ja:T,flex_spacing:F,bidirectional:T} → event has search_mode_value='responsa', responsa_options_count=3.
    - For a successful /api/search call with search_mode='exact' → event has search_mode_value='exact', responsa_options_count=0.
    - For a Pydantic-rejected /api/search request (e.g. unknown field 'mode'): the finally block fires with search_mode_value=None (because `req` was never bound), responsa_options_count=0.
    - For /api/browse and /api/parallels: events have search_mode_value=None, responsa_options_count=0.
  </behavior>
  <action>
    **Step A — Extend capture_api_event signature.** In `web/api_hardening.py`, modify `capture_api_event` (line ~572) to accept two new keyword-only args with defaults that preserve back-compat:

    ```python
    def capture_api_event(
        *,
        endpoint: str,
        mode: Optional[str],
        latency_seconds: float,
        result_count: Optional[int],
        status_code: int,
        error_code: Optional[str],
        client_ip: str,
        # Phase 81A D-08 additions:
        search_mode_value: Optional[str] = None,
        responsa_options_count: int = 0,
    ) -> None:
    ```

    Inside the function, after the existing `props` dict is built:

    ```python
    props: dict = {
        'endpoint': endpoint,
        'mode': mode,
        'latency_bucket': latency_bucket(latency_seconds),
        'status_code': status_code,
        'error_code': error_code,
    }
    if result_count is not None:
        props['result_count_bucket'] = result_count_bucket(result_count)
    # Phase 81A D-08 additions — always present (None/0 when not applicable).
    props['search_mode_value'] = search_mode_value
    props['responsa_options_count'] = int(responsa_options_count or 0)
    ```

    **Step B — Compute the two values in search_endpoint.** In `web/search_api.py` `search_endpoint`, add two state variables alongside `validated_mode` (around line 391) and update them as the request is parsed:

    Find:
    ```python
    validated_mode: Optional[str] = None
    ```

    Replace with:
    ```python
    validated_mode: Optional[str] = None
    posthog_search_mode_value: Optional[str] = None
    posthog_responsa_options_count: int = 0
    ```

    After successful Pydantic validation (around line 417 where `validated_mode = req.search_mode` was set in Plan 01), also set:

    ```python
    validated_mode = req.search_mode
    posthog_search_mode_value = req.search_mode
    if req.search_mode == 'responsa' and req.responsa_options is not None:
        opts = req.responsa_options
        posthog_responsa_options_count = sum([
            bool(opts.variants),
            bool(opts.ja),
            bool(opts.flex_spacing),
            bool(opts.bidirectional),
        ])
    elif req.search_mode == 'responsa':
        # responsa_options omitted → defaults all-False → count 0
        posthog_responsa_options_count = 0
    else:
        posthog_responsa_options_count = 0
    ```

    Then update the `finally` block's `capture_api_event` call (the existing call at lines ~580-588 — DO NOT touch the surrounding string-drain or meta-drain blocks added by Plan 02):

    ```python
    capture_api_event(
        endpoint=endpoint_name,
        mode=validated_mode,
        latency_seconds=elapsed,
        result_count=result_count,
        status_code=status_code,
        error_code=error_code,
        client_ip=client_ip,
        search_mode_value=posthog_search_mode_value,
        responsa_options_count=posthog_responsa_options_count,
    )
    ```

    **Step C — Plumb through wrap_endpoint decorator** (used by browse and parallels). Find `def wrap_endpoint` in `web/api_hardening.py`. The decorator's finally block calls `capture_api_event(..., mode=captured_state.get('mode'), ...)`. Update it to also pass the two new args from `captured_state`:

    ```python
    capture_api_event(
        endpoint=endpoint_name,
        mode=captured_state.get('mode'),
        latency_seconds=elapsed,
        result_count=captured_state.get('result_count'),
        status_code=status_code,
        error_code=error_code,
        client_ip=client_ip,
        search_mode_value=captured_state.get('search_mode_value'),
        responsa_options_count=captured_state.get('responsa_options_count', 0),
    )
    ```

    For browse and parallels endpoints in `web/search_api.py`, the existing `captured_state['mode'] = ...` lines need not be changed; the decorator now reads two more keys with sensible defaults (None / 0). For strict explicitness, add `captured_state['search_mode_value'] = None` and `captured_state['responsa_options_count'] = 0` in both endpoints (search for `captured_state['mode'] = None` in browse_endpoint at line ~624 and `captured_state['mode'] = req.mode` in parallels_endpoint at line ~760).

    Choose: **explicit set in each endpoint** (clearer intent, easier to audit), so add those two lines to both `browse_endpoint` and `parallels_endpoint`.

    **Step D — Verify the kwargs land.** Read `capture_api_event` once more after the change to confirm the two new properties end up in the `props` dict and thus in the emitted PostHog event.
  </action>
  <verify>
    <automated>python -m py_compile web/api_hardening.py web/search_api.py</automated>
    <automated>python -c "import inspect; from web.api_hardening import capture_api_event; sig = inspect.signature(capture_api_event); assert 'search_mode_value' in sig.parameters and 'responsa_options_count' in sig.parameters; print('OK')"</automated>
    <automated>grep -c "search_mode_value" web/api_hardening.py</automated>
    <automated>grep -c "responsa_options_count" web/api_hardening.py</automated>
    <automated>grep -c "posthog_search_mode_value" web/search_api.py</automated>
    <automated>grep -c "posthog_responsa_options_count" web/search_api.py</automated>
  </verify>
  <acceptance_criteria>
    - `capture_api_event` signature includes keyword-only `search_mode_value: Optional[str] = None` and `responsa_options_count: int = 0`.
    - The function builds a `props` dict that contains both new keys on every emitted event (not conditionally).
    - `wrap_endpoint`'s finally block reads `captured_state.get('search_mode_value')` and `captured_state.get('responsa_options_count', 0)`.
    - `search_endpoint` computes `posthog_search_mode_value = req.search_mode` after Pydantic validation succeeds.
    - `search_endpoint` computes `posthog_responsa_options_count` correctly: `sum of True booleans in the four ResponsaOptions flags` when search_mode='responsa', else 0.
    - `browse_endpoint` sets `captured_state['search_mode_value'] = None` and `captured_state['responsa_options_count'] = 0` (explicit).
    - `parallels_endpoint` sets `captured_state['search_mode_value'] = None` and `captured_state['responsa_options_count'] = 0` (explicit).
    - Plan 03's edit to `web/search_api.py`'s finally block is LIMITED to the `capture_api_event(...)` call region — does NOT touch the Plan 02 string-drain or meta-drain blocks.
    - All three endpoints' Pydantic-rejection paths produce events where `search_mode_value` is None and `responsa_options_count` is 0.
    - `python -m py_compile` succeeds for both files.
    - Existing PostHog soak/stress tests in `tests/test_search_api_soak.py` still pass (the additive signature is back-compat).
  </acceptance_criteria>
  <done>
    PostHog events from /api/search carry the two new properties with correct values. /api/browse and /api/parallels carry None/0 (explicit). Plan 05 adds tests asserting the property presence + correctness.
  </done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| /api/search handler → PostHog queue | One-way; queue drained by background thread; never blocks request. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-81A03-01 | Information Disclosure | search_mode_value property | accept | The literal `search_mode` enum value is low-cardinality public information (one of 5 strings). Not user data. |
| T-81A03-02 | Information Disclosure | responsa_options_count property | accept | Single integer in [0, 4]. No content; no shape leak beyond "user enabled N flags." |
| T-81A03-03 | DoS / latency | event capture path | mitigate | Existing best-effort queue (Concern #9) + sampling (`_should_sample`) preserved. New props add ~30 bytes per event; negligible. |
| T-81A03-04 | Repudiation | failed-validation events | mitigate | search_mode_value is None on the Pydantic-rejection path (req unbound). The `error_code='invalid_request'` already labels the event correctly; the new fields are aux. |
</threat_model>

<verification>
- `python -m py_compile web/api_hardening.py web/search_api.py` exits 0.
- The signature inspection check (Task 1 verify) passes.
- `pytest tests/test_search_api_soak.py -x` exits 0.
</verification>

<success_criteria>
PostHog events emitted from all three endpoints have a uniform shape including the two new properties. Search events carry the validated search_mode + the responsa_options_count; non-search events carry None/0. Plan 05 verifies via tests.
</success_criteria>

<output>
Create `.planning/phases/81A-api-contract-expansion/81A-03-SUMMARY.md` listing the signature change, the two new local variables in `search_endpoint`, the explicit set in `browse_endpoint`/`parallels_endpoint`, the wrap_endpoint decorator update, and confirmation that the finally-block edit was limited to the capture_api_event call (Plan 02's drain regions untouched).
</output>
