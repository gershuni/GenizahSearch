---
phase: 81A-api-contract-expansion
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - web/search_api.py
  - shared/api_errors.py
autonomous: true
requirements:
  - API-EXPAND-01
  - API-EXPAND-02
  - API-EXPAND-03
  - API-EXPAND-05
requirements_addressed:
  - API-EXPAND-01
  - API-EXPAND-02
  - API-EXPAND-03
  - API-EXPAND-05
tags:
  - api
  - pydantic
  - validation
must_haves:
  truths:
    - "Client cannot POST /api/search with old `mode` field — receives 400 invalid_request with message naming both `mode` and `search_mode`."
    - "Client can POST /api/search with `search_mode` ∈ {exact, variants, responsa, title, shelfmark} and the request validates."
    - "Client posting `search_mode='regex'` receives 400 `invalid_request` (Pydantic enum-constraint routed through `web/api_hardening.py:326` envelope wrapper, which returns HTTP 400 for ALL PydanticValidationErrors — regex dropped per D-09)."
    - "Client posting non-responsa mode together with non-null `responsa_options` receives 400 invalid_combination."
    - "Client posting `search_mode='title'` or `'shelfmark'` together with non-zero `gap` receives 400 invalid_combination."
    - "Client posting `limit > 100` or `limit < 1` receives HTTP 400 with `body['error']['code']=='invalid_request'` (Pydantic Field constraint routed through Phase 78 envelope wrapper at `web/api_hardening.py:326`); default limit is 50."
    - "ResponsaOptions accepts only the four boolean fields (variants, ja, flex_spacing, bidirectional); any extra field is rejected by extra='forbid'."
    - "`search_mode='exact'` and `search_mode='variants'` produce DIFFERENT result sets (or different scoring/count) — they map to internal `mode='exact'` vs `mode='variants'` respectively, which `var_mgr.get_variants(term, mode)` and `build_tantivy_query(terms, mode)` consume to drive variant expansion (genizah_core.py:6467, 7473)."
    - "`shared/api_errors.py` ERROR_CODES contains `'invalid_combination'`."
  artifacts:
    - path: "web/search_api.py"
      provides: "ResponsaOptions BaseModel + rewritten SearchRequest with search_mode + cross-field model_validators"
      contains: "class ResponsaOptions"
    - path: "web/search_api.py"
      provides: "MAX_LIMIT lowered to 100"
      contains: "MAX_LIMIT = 100"
    - path: "shared/api_errors.py"
      provides: "invalid_combination added to ERROR_CODES"
      contains: "invalid_combination"
  key_links:
    - from: "web/search_api.py SearchRequest model"
      to: "web/search_api.py search_endpoint handler"
      via: "req.search_mode and req.responsa_options drive the responsa_options dict passed to state.searcher.execute_search"
      pattern: "responsa_options"
    - from: "web/search_api.py SearchRequest"
      to: "shared.api_errors.APIError"
      via: "model_validator raises APIError(invalid_combination, http_status=400)"
      pattern: "invalid_combination"
    - from: "web/search_api.py search_endpoint"
      to: "genizah_core.py SearchEngine.execute_search → build_tantivy_query (line 7473) and _get_or_compute_variants (line 7471)"
      via: "internal_mode='exact' vs 'variants' is consumed by var_mgr.get_variants(term, mode, limit=200) at line 6467 to expand variants"
      pattern: "get_variants"
---

<objective>
Replace the conflated `mode` field on `/api/search`'s `SearchRequest` with a UI-aligned `search_mode` enum (5 values) and add a new `ResponsaOptions` Pydantic model. Add two cross-field `@model_validator(mode='after')` validators (responsa-mode coupling, gap-mode coupling). Lower the `limit` ceiling from 200 → 100. Map the new fields onto the existing internal `responsa_options` dict so `state.searcher.execute_search` and the legacy `mode` value space are preserved unchanged.

**Variants wiring (per revision 1, addressing Blocker 2):** `search_mode='exact'` and `search_mode='variants'` MUST produce a measurable behavioral difference — they map to internal `mode='exact'` and `mode='variants'` respectively (NOT both to `'text'`). The variant pipeline is wired automatically because `genizah_core.py:7471` (`_get_or_compute_variants(terms, mode)`) and `genizah_core.py:7473` (`build_tantivy_query(terms, mode)` → `var_mgr.get_variants(term, mode, limit=200)` at line 6467) consume the `mode` argument as the variant-tier knob. This mirrors the desktop UI's variants checkbox semantics.

**Pydantic error status code (verified against live code 2026-05-03):** `web/api_hardening.py:299-326` `_build_envelope_response` returns HTTP **400** with `code='invalid_request'` for ALL `PydanticValidationError` and `RequestValidationError` instances — including `Field(le=100, ge=1)` violations and `Literal[...]` enum violations. This is the Phase 78 envelope contract; 81A does NOT modify it. All assertions in this plan, Plan 04, and Plan 05 use HTTP 400 (not 422) for Pydantic constraint failures.

Purpose: This is the foundation of Phase 81A. Plans 02–05 cannot run until the new request shape compiles and validates. Locks the API contract that the v7.10 Claude skill (81B) will consume.

Output: `web/search_api.py` modified — `SearchRequest` rewritten in-place, `ResponsaOptions` added, `MAX_LIMIT` constant lowered, search_endpoint handler updated to translate `search_mode` → internal `mode` value (`'exact'|'variants'|'Title'|'Shelfmark'|'Responsa'`) and to build the responsa_options dict from `req.responsa_options` rather than the hard-coded literals it currently uses. `shared/api_errors.py` modified — `invalid_combination` added to `ERROR_CODES`.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@CLAUDE.md
@.planning/STATE.md
@.planning/ROADMAP.md
@.planning/REQUIREMENTS.md
@.planning/phases/81A-api-contract-expansion/81A-CONTEXT.md
@.planning/phases/81B-claude-skill-consumer/81-RESCOPE.md

<interfaces>
<!-- Existing relevant types/exports the executor will use. NO codebase exploration needed. -->

From web/search_api.py current state (verified at revision time):
```python
# Line 112-120 — current SearchRequest (being replaced):
class SearchRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')
    query: str
    mode: Literal['text', 'Title', 'Shelfmark', 'Responsa']
    gap: int = 0
    limit: int = 50
    filters: Optional[FiltersModel] = None

# Line 124-126 — current constants:
QUERY_LENGTH_CAP = 1000
DEFAULT_LIMIT = 50
MAX_LIMIT = 200            # ← lower to 100

# Line 391 — search_endpoint local at function entry:
validated_mode: Optional[str] = None

# Line 409-416 — Pydantic-error except block:
except PydanticValidationError:
    status_code = 400
    error_code = 'invalid_request'
    raise

# Line 417 — sets validated_mode for PostHog:
validated_mode = req.mode

# Line 484-493 — current Responsa branch (hard-coded options):
responsa_options = None
if req.mode == 'Responsa':
    responsa_options = {
        'responsa_mode': True,
        'variants': True,         # ← currently hard-coded
        'ja': True,                # ← currently hard-coded
        'flex_spacing': False,
        'bidirectional': False,
        'variant_mode': 'variants',
    }

# Line 502-511 — execute_search call site:
results = state.searcher.execute_search(
    query_str=query,
    mode=req.mode,                    # ← change to mode=internal_mode
    gap=req.gap,
    progress_callback=None,
    exclude_words=None,
    responsa_options=responsa_options,
    restrict_sys_ids=restrict_sys_ids,
    text_position=None,
) or []

# Line 549 — serialize_search_payload mode arg (also passes req.mode currently):
mode=req.mode,
```

From web/api_hardening.py:299-326 (verified at revision time — the envelope wrapper that determines status code for Pydantic errors):
```python
if isinstance(exc, (RequestValidationError, PydanticValidationError)):
    # ...build body with code='invalid_request', message, fields...
    return JSONResponse(status_code=400, content=body)
```
**This means: every `Field(le=100, ge=1)` violation, every `Literal[...]` enum violation, every `extra='forbid'` rejection returns HTTP 400 with `body['error']['code']=='invalid_request'`.** No 422. 81A does not modify this contract.

From shared/api_errors.py (current state — verified at revision time):
```python
# Line 24-44 — ERROR_CODES frozenset DOES NOT contain 'invalid_combination'.
# Plan 01 MUST add it (declarative — no conditional).
ERROR_CODES = frozenset({
    'invalid_request', 'invalid_mode', 'query_required', 'query_too_long',
    'limit_too_high', 'unknown_filter_key', 'unresolvable_filter_value',
    'filter_vocabulary_unavailable', 'rate_limited', 'disabled',
    'localhost_only', 'internal_error', 'locator_conflict',
    'manuscript_page_not_found', 'core_timeout',
    'composition_required', 'composition_too_long',
})
# 'invalid_combination' MUST be added.
```

From genizah_core.py — variant pipeline call sites that consume the internal `mode` argument:
```python
# Line 7249 — execute_search signature:
def execute_search(self, query_str, mode, gap, progress_callback=None,
                   exclude_words=None, responsa_options=None,
                   restrict_sys_ids: set = None, text_position: str = None):

# Line 7256 — metadata-mode branch:
if mode in ['Title', 'Shelfmark']:
    return self._execute_metadata_search(...)

# Line 7470-7473 — non-Responsa text path: `mode` IS the variant-tier knob:
if mode != 'Regex':
    self._get_or_compute_variants(terms, mode)
t_query_str = self.build_tantivy_query(terms, mode)
regex = self.build_regex_pattern(terms, mode, gap)

# Line 6467 — get_variants(term, mode) — this is where 'exact' vs 'variants' diverges:
all_vars = self.var_mgr.get_variants(term, mode, limit=200)

# Line 6371 — build_tantivy_query signature:
def build_tantivy_query(self, terms, mode, responsa_components=None, responsa_options=None):
```

The desktop UI (CLAUDE.md memory: `genizah_app.py:15788-15797`) drives this same `mode` argument with `'exact'` or `'variants'` based on the variants checkbox. **Therefore mapping `search_mode='exact' → 'exact'` and `search_mode='variants' → 'variants'` is the exact desktop UI semantics.**

From genizah_app.py:15788-15797 (desktop UI — name parity reference, per CLAUDE.md memory):
```python
self.responsa_variants_check    # → responsa_options.variants
self.responsa_ja_check          # → responsa_options.ja
self.responsa_flex_spacing_check  # → responsa_options.flex_spacing
self.responsa_bidirectional_check  # → responsa_options.bidirectional
# variant_mode = 'variants' if variants_check else 'exact'
```
</interfaces>
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: Add ResponsaOptions model + rewrite SearchRequest with search_mode + cross-field validators + lower MAX_LIMIT + add invalid_combination to ERROR_CODES + wire variants at API layer</name>
  <files>web/search_api.py, shared/api_errors.py</files>
  <read_first>
    - .planning/phases/81A-api-contract-expansion/81A-CONTEXT.md (validation matrix table — copy verbatim; note the limit-bound row asserts 400 invalid_request, NOT 422)
    - .planning/phases/81B-claude-skill-consumer/81-RESCOPE.md §3.1, §3.3, §3.6
    - web/search_api.py (read fully — current SearchRequest at line 112-120, MAX_LIMIT at line 126, validated_mode at line 391, Pydantic-except at 409-416, validated_mode assignment at 417, Responsa branch at 484-493, execute_search call at 502-511)
    - web/api_hardening.py:280-330 (`_build_envelope_response` — confirms HTTP 400 for all PydanticValidationErrors)
    - shared/api_errors.py (ERROR_CODES at lines 24-44 — 'invalid_combination' is ABSENT and MUST be added)
    - genizah_core.py:7249-7473 (variants/metadata branching — confirms internal `mode` = variant tier for non-Responsa text)
  </read_first>
  <behavior>
    - SearchRequest with valid `search_mode='exact'`, `query='foo'` → validates, no error.
    - SearchRequest with `search_mode='responsa'`, `responsa_options={variants:true, ja:false, flex_spacing:false, bidirectional:false}` → validates.
    - SearchRequest with `search_mode='exact'`, `responsa_options={variants:true,...}` → raises APIError(code='invalid_combination', http_status=400) with message containing both `responsa_options` AND `search_mode`.
    - SearchRequest with `search_mode='title'`, `gap=2` → raises APIError(code='invalid_combination', http_status=400) with message containing both `gap` AND `title`.
    - SearchRequest with `search_mode='shelfmark'`, `gap=5` → same as above (mentions `shelfmark`).
    - SearchRequest with `search_mode='regex'` → raises Pydantic ValidationError (Literal enum constraint — regex is NOT in the 5 values). The endpoint wrapper at `web/api_hardening.py:326` converts this to HTTP 400 with `code='invalid_request'`.
    - SearchRequest with `mode='text'` (old field) → raises Pydantic ValidationError (extra='forbid'); the search_endpoint catches it and emits 400 invalid_request with message containing `unknown field 'mode'` and `search_mode`.
    - SearchRequest with `limit=101` → ValidationError (Field constraint le=100); endpoint wrapper returns HTTP 400 with `code='invalid_request'`. `limit=0` → ValidationError (ge=1) → same HTTP 400 invalid_request.
    - SearchRequest defaults: `gap=0`, `limit=50`, `filters=None`, `responsa_options=None`.
    - ResponsaOptions defaults: all four flags = False; `extra='forbid'` rejects any unknown key (e.g. `variant_mode`, `variants_extended`).
    - **Variants wiring (Blocker 2 fix):** `search_mode='exact'` produces `mode='exact'` passed to `state.searcher.execute_search`; `search_mode='variants'` produces `mode='variants'`. These are the EXACT internal values the desktop UI passes (per `genizah_app.py:15796`).
    - `shared/api_errors.py` ERROR_CODES contains `'invalid_combination'`.
  </behavior>
  <action>
    Modify `web/search_api.py` and `shared/api_errors.py` as follows. Use the Edit tool, not Write.

    **Step A — Add ResponsaOptions model.** Insert directly above `class SearchRequest` (around current line 112):

    ```python
    class ResponsaOptions(BaseModel):
        """Phase 81A D-02 — Responsa-only options. Field names mirror the desktop
        UI checkboxes exactly (genizah_app.py:15788-15797).

        D-03: extra='forbid' — extended/maximum variant tiers, variant_mode, and
        any other field name are rejected. D-11: variants is a plain bool; the
        internal variant_mode is derived server-side ('variants' if True else 'exact').
        """
        model_config = ConfigDict(extra='forbid')

        variants: bool = False
        ja: bool = False
        flex_spacing: bool = False
        bidirectional: bool = False
    ```

    **Step B — Rewrite SearchRequest in-place** (replace the existing class body at lines 112-120):

    ```python
    class SearchRequest(BaseModel):
        """Phase 81A — UI-aligned search_mode + Responsa-only options.

        Replaces the Phase 78 `mode` field with `search_mode` (D-01, D-13). Old
        `mode` field is hard-rejected by extra='forbid' — see search_endpoint for
        the 400 invalid_request envelope with the explicit `unknown field 'mode'`
        message.

        Validation matrix (81A-CONTEXT.md):
        - Any non-responsa search_mode + non-None responsa_options → 400 invalid_combination
        - search_mode in {'title', 'shelfmark'} + non-zero gap → 400 invalid_combination
        - limit must be in [1, 100] (Pydantic Field constraint → 400 invalid_request via Phase 78 envelope wrapper)
        - regex is intentionally NOT in the enum (D-09; deferred to v7.11)
        """
        model_config = ConfigDict(extra='forbid')

        query: str
        search_mode: Literal['exact', 'variants', 'responsa', 'title', 'shelfmark']
        responsa_options: Optional[ResponsaOptions] = None
        gap: int = 0
        limit: int = Field(default=50, ge=1, le=100)
        filters: Optional[FiltersModel] = None

        @model_validator(mode='after')
        def _check_responsa_options_coupling(self):
            if self.search_mode != 'responsa' and self.responsa_options is not None:
                raise APIError(
                    'invalid_combination',
                    f"responsa_options is only valid when search_mode='responsa' "
                    f"(got search_mode={self.search_mode!r})",
                    http_status=400,
                )
            return self

        @model_validator(mode='after')
        def _check_gap_metadata_coupling(self):
            if self.search_mode in ('title', 'shelfmark') and self.gap and self.gap != 0:
                raise APIError(
                    'invalid_combination',
                    f"gap has no effect with metadata-only search modes "
                    f"(search_mode={self.search_mode!r}, gap={self.gap})",
                    http_status=400,
                )
            return self
    ```

    Add the import for `model_validator` at the top of the file. Find the existing pydantic import line and add `model_validator` to it:
    ```python
    from pydantic import BaseModel, ConfigDict, Field, ValidationError as PydanticValidationError
    ```
    Replace with:
    ```python
    from pydantic import BaseModel, ConfigDict, Field, ValidationError as PydanticValidationError, model_validator
    ```

    **Step C — Lower MAX_LIMIT.** Find at line 126:
    ```python
    MAX_LIMIT = 200
    ```
    Replace with:
    ```python
    MAX_LIMIT = 100  # 81A D-06 — lowered from 200 (also enforced via Pydantic Field(le=100))
    ```

    **Step D — Add `_SEARCH_MODE_TO_INTERNAL` mapping near MAX_LIMIT** (Blocker 2 wiring — variants is a separate internal mode, NOT collapsed to 'text'):

    Right after the new `MAX_LIMIT = 100` line, add:

    ```python
    # 81A — translate API search_mode → internal mode value space consumed by
    # SearchEngine.execute_search (genizah_core.py:7249). For non-Responsa text
    # searches, the internal `mode` argument is THE variant-tier knob:
    # genizah_core.py:6467 calls var_mgr.get_variants(term, mode, limit=200),
    # so 'exact' → no variant expansion, 'variants' → 30-pair variant expansion.
    # Mirrors desktop UI semantics (genizah_app.py:15796 toggles 'variants' vs 'exact').
    _SEARCH_MODE_TO_INTERNAL = {
        'exact':     'exact',
        'variants':  'variants',
        'responsa':  'Responsa',
        'title':     'Title',
        'shelfmark': 'Shelfmark',
    }
    ```

    **Step E — Update search_endpoint** to consume the new fields. Multiple sub-steps:

    **E.1.** Line 417 — change:
    ```python
    validated_mode = req.mode
    ```
    to:
    ```python
    validated_mode = req.search_mode
    ```
    (drives the existing `mode` PostHog property; Plan 03 adds `search_mode_value`.)

    **E.2.** Lines 484-493 — replace the hard-coded Responsa branch with a derivation from `req.responsa_options`:

    ```python
    # 5. Build responsa_options if responsa search_mode (per req.responsa_options).
    responsa_options = None
    if req.search_mode == 'responsa':
        # Default: all-False ResponsaOptions when client omitted the field.
        opts = req.responsa_options or ResponsaOptions()
        # D-11: derive internal variant_mode from the boolean flag exactly as
        # the desktop UI does (genizah_app.py:15796).
        responsa_options = {
            'responsa_mode': True,
            'variants': opts.variants,
            'ja': opts.ja,
            'flex_spacing': opts.flex_spacing,
            'bidirectional': opts.bidirectional,
            'variant_mode': 'variants' if opts.variants else 'exact',
        }
    ```

    **E.3.** Lines 502-511 — change the `execute_search` call so `mode=` uses the translated internal value, NOT `req.mode` (which no longer exists). Also update the serialize call at line 549. Insert just BEFORE the `state.searcher.execute_search(...)` call:

    ```python
    internal_mode = _SEARCH_MODE_TO_INTERNAL[req.search_mode]
    ```

    Then change:
    - Line 504: `mode=req.mode,` → `mode=internal_mode,`
    - Line 549 (the `serialize_search_payload(...)` call): `mode=req.mode,` → `mode=internal_mode,`

    **Variants behavioral wiring (Blocker 2 fix):** Because `_SEARCH_MODE_TO_INTERNAL['exact']='exact'` and `['variants']='variants'`, and because `genizah_core.py:6467` calls `var_mgr.get_variants(term, mode, limit=200)`, the two API values produce different result sets: `'exact'` returns no variant expansion (only the literal term variants Tantivy considers an exact match), `'variants'` returns the 30-pair variant expansion (per `_MODE_PAIRS_COUNT['variants']=30` at genizah_core.py:2475). This is verified by Plan 05 AC2 (which now asserts a measurable difference between the two values on at least one fixture query — see Plan 05 Section 1).

    **Step F — Update the old `mode` field error message.** The Pydantic `extra='forbid'` already rejects unknown keys, but the default Pydantic message is generic. Override by intercepting in the existing PydanticValidationError except block at lines 409-416:

    Replace:
    ```python
    except PydanticValidationError:
        status_code = 400
        error_code = 'invalid_request'
        raise
    ```

    With:

    ```python
    except PydanticValidationError as exc:
        status_code = 400
        error_code = 'invalid_request'
        # 81A D-13: when the rejected field is the old `mode`, surface the
        # cutover hint explicitly so skill authors copy-pasting old payloads
        # see the migration path.
        for err in exc.errors():
            if err.get('type') == 'extra_forbidden' and err.get('loc') == ('mode',):
                raise APIError(
                    'invalid_request',
                    "unknown field 'mode' — use search_mode instead",
                    http_status=400,
                )
        raise
    ```

    **Step G — Add `'invalid_combination'` to `shared/api_errors.py` ERROR_CODES.** Open `shared/api_errors.py`. The `ERROR_CODES` frozenset is at lines 24-44. `'invalid_combination'` is confirmed absent. Add it as a single new entry alongside `'invalid_request'`:

    Find:
    ```python
    ERROR_CODES = frozenset({
        'invalid_request',
        'invalid_mode',
    ```

    Replace with:
    ```python
    ERROR_CODES = frozenset({
        'invalid_request',
        'invalid_combination',  # 81A D-03 — cross-field validation rejection (responsa_options/mode coupling, gap/metadata-mode coupling).
        'invalid_mode',
    ```

    Do NOT add `regex_pattern_too_long` (D-09 drops regex).
  </action>
  <verify>
    <automated>python -c "from web.search_api import SearchRequest, ResponsaOptions, MAX_LIMIT, _SEARCH_MODE_TO_INTERNAL; assert MAX_LIMIT == 100; assert _SEARCH_MODE_TO_INTERNAL['exact'] == 'exact' and _SEARCH_MODE_TO_INTERNAL['variants'] == 'variants'; r = SearchRequest(query='x', search_mode='exact'); assert r.search_mode == 'exact' and r.limit == 50; ro = ResponsaOptions(); assert ro.variants is False and ro.ja is False and ro.flex_spacing is False and ro.bidirectional is False; print('OK')"</automated>
    <automated>python -c "import typing; from web.search_api import SearchRequest; assert set(typing.get_args(SearchRequest.model_fields['search_mode'].annotation)) == {'exact','variants','responsa','title','shelfmark'}; print('OK')"</automated>
    <automated>grep -q "class ResponsaOptions" web/search_api.py && echo found-ResponsaOptions || (echo missing-ResponsaOptions; exit 1)</automated>
    <automated>grep -q "model_validator" web/search_api.py && echo found-model_validator || (echo missing-model_validator; exit 1)</automated>
    <automated>grep -q "MAX_LIMIT = 100" web/search_api.py && echo found-MAX_LIMIT || (echo missing-MAX_LIMIT; exit 1)</automated>
    <automated>grep -q "unknown field 'mode'" web/search_api.py && echo found-cutover-msg || (echo missing-cutover-msg; exit 1)</automated>
    <automated>grep -q "_SEARCH_MODE_TO_INTERNAL" web/search_api.py && echo found-mapping || (echo missing-mapping; exit 1)</automated>
    <automated>grep -q "'invalid_combination'" shared/api_errors.py && echo found-invalid_combination || (echo missing-invalid_combination; exit 1)</automated>
    <automated>python -c "from shared.api_errors import ERROR_CODES; assert 'invalid_combination' in ERROR_CODES; print('OK')"</automated>
  </verify>
  <acceptance_criteria>
    - `web/search_api.py` contains `class ResponsaOptions(BaseModel):` with exactly four fields (variants, ja, flex_spacing, bidirectional), each `bool = False`, and `model_config = ConfigDict(extra='forbid')`.
    - `SearchRequest` declares `search_mode` as a Literal with the exact 5-value set `{'exact','variants','responsa','title','shelfmark'}` (verified via `typing.get_args` rather than quote-style-dependent grep).
    - `SearchRequest` no longer declares a `mode:` field.
    - Two `@model_validator(mode='after')` decorators present in SearchRequest body.
    - `MAX_LIMIT = 100` (`grep -q` confirms presence).
    - `SearchRequest.limit` field uses `Field(default=50, ge=1, le=100)`.
    - `_SEARCH_MODE_TO_INTERNAL` mapping defined as a module-level constant containing all 5 keys, with `'exact' → 'exact'` and `'variants' → 'variants'` (NOT both → 'text'; this is the Blocker 2 fix that makes the two values behaviorally distinct via genizah_core.py:6467).
    - search_endpoint passes `mode=internal_mode` (not `mode=req.mode`) to `state.searcher.execute_search`.
    - search_endpoint Responsa branch builds `responsa_options` dict from `req.responsa_options or ResponsaOptions()` (NOT hard-coded True/True/False/False).
    - PydanticValidationError except block contains a check for `loc == ('mode',)` that raises `APIError('invalid_request', "unknown field 'mode' — use search_mode instead", ...)`.
    - `shared/api_errors.py` ERROR_CODES contains `'invalid_combination'` (added declaratively, not conditionally — confirmed absent at revision time).
    - `python -c "from web.search_api import SearchRequest, ResponsaOptions"` exits 0.
    - `python -m py_compile web/search_api.py shared/api_errors.py` exits 0.
  </acceptance_criteria>
  <done>
    Pydantic models compile and import cleanly; the validation matrix from 81A-CONTEXT.md is enforced at model-construction time. The endpoint handler reaches `state.searcher.execute_search` with the correct internal mode (`'exact'` vs `'variants'` are NOT collapsed) and a non-hard-coded responsa_options dict. `'invalid_combination'` is in the ERROR_CODES taxonomy.
  </done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| HTTP client → /api/search | Untrusted JSON crosses this boundary; Pydantic is the first validation layer. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-81A01-01 | Tampering | SearchRequest body | mitigate | `extra='forbid'` on both SearchRequest and ResponsaOptions rejects unknown fields. Cross-field validators reject illegal combinations before reaching the search engine. |
| T-81A01-02 | Denial of Service | limit field | mitigate | `Field(le=100)` caps result-set size at the API layer (was 200 in Phase 78). Pydantic rejects out-of-range before any DB query. |
| T-81A01-03 | Information Disclosure | Error message | accept | Cutover error message names the old field `mode` explicitly (D-13). Internal API; no external clients; the disclosure is intentional debug aid for skill authors. |
| T-81A01-04 | Repudiation | search_mode value | mitigate | Plan 03 logs `search_mode_value` to PostHog so every request is auditable. |
| T-81A01-05 | Elevation of Privilege | responsa_options field names | mitigate | `extra='forbid'` ensures no client can sneak in `variant_mode='variants_maximum'` or other deferred flags. The four allowed flags are explicit booleans only (D-11). |
</threat_model>

<verification>
- `python -m py_compile web/search_api.py shared/api_errors.py` exits 0.
- `python -c "from web.search_api import SearchRequest, ResponsaOptions, MAX_LIMIT, _SEARCH_MODE_TO_INTERNAL; assert MAX_LIMIT == 100; assert set(_SEARCH_MODE_TO_INTERNAL.keys()) == {'exact','variants','responsa','title','shelfmark'}; assert _SEARCH_MODE_TO_INTERNAL['exact']=='exact' and _SEARCH_MODE_TO_INTERNAL['variants']=='variants'; print('OK')"` exits 0.
- `python -c "from shared.api_errors import ERROR_CODES; assert 'invalid_combination' in ERROR_CODES; print('OK')"` exits 0.
- `pytest tests/test_search_api.py -x` may fail in this plan because old tests use `mode=`; this is expected and Plan 04 fixes them. Do NOT block on it.
</verification>

<success_criteria>
The new request shape compiles, validates, and routes through to `state.searcher.execute_search` with internal-mode translation. `'exact'` and `'variants'` map to behaviorally-distinct internal values consumed by the variant pipeline at genizah_core.py:6467. Responsa mode honors `req.responsa_options` instead of hard-coded literals. The four 81A-CONTEXT.md validation-matrix rows that pertain to model construction (responsa_options coupling, gap-metadata coupling, limit ceiling, old `mode` rejection) are enforced. `'invalid_combination'` is in ERROR_CODES. Plans 02–05 can build on this.
</success_criteria>

<output>
Create `.planning/phases/81A-api-contract-expansion/81A-01-SUMMARY.md` summarizing: which lines of `web/search_api.py` changed, which Pydantic fields/validators are now in place, the explicit error string for the `mode` cutover, the updated MAX_LIMIT, the `_SEARCH_MODE_TO_INTERNAL` mapping (especially that `'exact' → 'exact'` and `'variants' → 'variants'` — Blocker 2 fix per revision 1), and the addition of `'invalid_combination'` to `shared/api_errors.py` ERROR_CODES. Note in the summary that Plan 04 will rewrite tests to use `search_mode` (the old `mode=` test cases are expected to fail until then).
</output>
