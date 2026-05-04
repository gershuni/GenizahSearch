---
phase: 81A-api-contract-expansion
plan: 01
subsystem: api
tags: [api, pydantic, validation, search_mode, responsa]
requires: []
provides:
  - SearchRequest.search_mode (5-value Literal)
  - ResponsaOptions Pydantic model
  - _SEARCH_MODE_TO_INTERNAL mapping
  - shared.api_errors.ERROR_CODES['invalid_combination']
affects:
  - /api/search request contract (breaking: `mode` -> `search_mode`)
tech-stack:
  added: []
  patterns:
    - "@model_validator(mode='after') for cross-field validation"
    - "extra='forbid' on both SearchRequest and ResponsaOptions"
    - "Pydantic Field(ge=1, le=100) belt-and-braces with Phase 78 envelope wrapper"
key-files:
  created: []
  modified:
    - web/search_api.py
    - shared/api_errors.py
decisions:
  - "search_mode='exact' and search_mode='variants' map to internal 'exact' / 'variants' (not both to 'text') so var_mgr.get_variants(term, mode) at genizah_core.py:6467 produces behaviorally distinct result sets (Blocker 2 fix per revision 1)"
  - "Cross-field validators raise APIError(invalid_combination) directly (not ValueError) so the endpoint's APIError handler routes through _build_envelope_response with the semantic code; Pydantic does NOT wrap APIError into ValidationError"
  - "Old 'mode' field cutover hint surfaced in PydanticValidationError except block via loc==('mode',) detection -> APIError('invalid_request', \"unknown field 'mode' -- use search_mode instead\")"
metrics:
  tasks: 1
  files_changed: 2
  lines_added: 97
  lines_removed: 16
  completed: 2026-05-04
---

# Phase 81A Plan 01: API Contract -- search_mode + ResponsaOptions Summary

Replaces the conflated `mode` field on POST `/api/search` with a UI-aligned 5-value `search_mode` enum and a separate `ResponsaOptions` model; adds cross-field validation, lowers the limit ceiling 200 -> 100, and adds `invalid_combination` to the error-code taxonomy. The new shape compiles, imports cleanly, and routes through to `state.searcher.execute_search` with internal-mode translation that preserves desktop UI variant semantics.

## What Changed

### `web/search_api.py`

| Region | Change |
|---|---|
| Line 41 | Added `model_validator` to the pydantic import |
| Lines 112-127 | Added `class ResponsaOptions(BaseModel)` with 4 bool fields (`variants`, `ja`, `flex_spacing`, `bidirectional`), all default False, `extra='forbid'` |
| Lines 129-173 | Rewrote `class SearchRequest` -- `query: str`; `search_mode: Literal['exact','variants','responsa','title','shelfmark']`; `responsa_options: Optional[ResponsaOptions]=None`; `gap: int=0`; `limit: int = Field(default=50, ge=1, le=100)`; `filters: Optional[FiltersModel]=None`. Two `@model_validator(mode='after')` decorators: `_check_responsa_options_coupling` and `_check_gap_metadata_coupling`, both raising `APIError('invalid_combination', ..., http_status=400)` |
| Line 178 | `MAX_LIMIT = 100` (was 200) -- 81A D-06 |
| Lines 180-191 | New `_SEARCH_MODE_TO_INTERNAL` module-level mapping. **Critical Blocker-2 fix:** `'exact' -> 'exact'`, `'variants' -> 'variants'` (NOT both -> `'text'`). The internal `mode` argument is the variant-tier knob consumed by `var_mgr.get_variants(term, mode, limit=200)` at `genizah_core.py:6467`, so the two API values produce measurably different result sets. `'responsa'->'Responsa'`, `'title'->'Title'`, `'shelfmark'->'Shelfmark'` |
| Lines ~409-426 | PydanticValidationError except block extended: iterates `exc.errors()` and, on `type=='extra_forbidden'` with `loc==('mode',)`, raises `APIError('invalid_request', "unknown field 'mode' -- use search_mode instead", http_status=400)` -- the explicit cutover hint for skill authors |
| Line 428 | `validated_mode = req.search_mode` (was `req.mode`) |
| Lines ~495-509 | Responsa branch now derives `responsa_options` dict from `req.responsa_options or ResponsaOptions()` instead of hard-coding `variants=True, ja=True, flex_spacing=False, bidirectional=False`. `variant_mode` is `'variants' if opts.variants else 'exact'`, mirroring desktop UI semantics (`genizah_app.py:15796`) |
| Lines ~513-524 | `internal_mode = _SEARCH_MODE_TO_INTERNAL[req.search_mode]` computed before the `state.searcher.execute_search(...)` call; `mode=internal_mode` (was `mode=req.mode`) |
| Line ~561 | `serialize_search_payload(..., mode=_SEARCH_MODE_TO_INTERNAL[req.search_mode], ...)` (was `mode=req.mode`) |

### `shared/api_errors.py`

| Region | Change |
|---|---|
| Line 26 | Added `'invalid_combination'` to the `ERROR_CODES` frozenset, with comment "81A D-03 -- cross-field validation rejection (responsa_options/mode coupling, gap/metadata-mode coupling)" |

## Validation Matrix Enforced

| Input | Outcome |
|---|---|
| `search_mode='exact'`, `query='x'` | Validates; defaults `gap=0`, `limit=50`, `filters=None`, `responsa_options=None` |
| `search_mode='responsa'`, `responsa_options={variants:true, ja:false, ...}` | Validates |
| `search_mode='exact'`, `responsa_options={...}` | `APIError('invalid_combination', ..., 400)` mentioning both `responsa_options` and `search_mode` |
| `search_mode='title'`, `gap=2` | `APIError('invalid_combination', ..., 400)` mentioning both `gap` and `title` |
| `search_mode='shelfmark'`, `gap=5` | Same as above (mentions `shelfmark`) |
| `search_mode='regex'` | Pydantic `ValidationError` (Literal enum) -> envelope wrapper returns HTTP 400 `invalid_request` |
| `mode='text'` (old field) | Pydantic `ValidationError` (`extra='forbid'`); endpoint detects `loc==('mode',)` and raises `APIError('invalid_request', "unknown field 'mode' -- use search_mode instead", 400)` |
| `limit=101` or `limit=0` | Pydantic `ValidationError` (Field constraint) -> envelope returns HTTP 400 `invalid_request` |
| `ResponsaOptions(variant_mode='...')` or any extra key | Pydantic `ValidationError` (`extra='forbid'`) |

## Variants Wiring (Blocker 2)

`_SEARCH_MODE_TO_INTERNAL` is the linchpin. Because `genizah_core.py:7470-7473` runs `_get_or_compute_variants(terms, mode)` and `build_tantivy_query(terms, mode)` for non-Responsa text searches, and because `var_mgr.get_variants(term, mode, limit=200)` at `genizah_core.py:6467` is the variant-tier knob, mapping `'exact'->'exact'` and `'variants'->'variants'` makes the two API values produce different result sets (the `'variants'` value triggers the 30-pair variant expansion per `_MODE_PAIRS_COUNT['variants']=30`). This mirrors desktop UI behavior at `genizah_app.py:15796`. Plan 05 AC2 will assert the measurable behavioral difference on a fixture query.

## Behavioral Verification

All 11 behavior checks ran green inside the worktree against the new code:

```
T1 OK   -- valid responsa with options
T2 OK   -- non-responsa + responsa_options -> APIError(invalid_combination)
T3 OK   -- search_mode=title + gap=2 -> APIError(invalid_combination)
T3b OK  -- search_mode=shelfmark + gap=5 -> APIError mentions shelfmark
T4 OK   -- search_mode=regex -> Pydantic ValidationError
T5 OK   -- limit=101 -> ValidationError
T6 OK   -- limit=0 -> ValidationError
T7 OK   -- old mode='text' -> ValidationError
T8 OK   -- ResponsaOptions(variant_mode='...') -> ValidationError
T9 OK   -- defaults: gap=0, limit=50, filters=None, responsa_options=None
T10 OK  -- search_mode=title + gap=0 (no gap) validates
```

Compile-level checks:
- `python -m py_compile web/search_api.py shared/api_errors.py` -> exit 0
- `from web.search_api import SearchRequest, ResponsaOptions, MAX_LIMIT, _SEARCH_MODE_TO_INTERNAL` -> exit 0
- `MAX_LIMIT == 100` confirmed
- `_SEARCH_MODE_TO_INTERNAL['exact']=='exact'` and `['variants']=='variants'` confirmed
- `set(typing.get_args(SearchRequest.model_fields['search_mode'].annotation)) == {'exact','variants','responsa','title','shelfmark'}` confirmed
- `'invalid_combination' in shared.api_errors.ERROR_CODES` confirmed

## Note on APIError vs ValidationError

The two `@model_validator(mode='after')` validators raise `APIError(...)` **directly** (not wrapped in `ValueError`/`ValidationError`). Pydantic only wraps `ValueError`/`AssertionError` from validators -- `APIError` propagates as-is, which is exactly what the endpoint needs: the existing `except APIError` branch in `search_endpoint` (lines 557+) routes the error through `_build_envelope_response` with the semantic `'invalid_combination'` code intact. Previous Phase 78 patterns (e.g. `validate_filter_values`) use the same direct-raise approach.

## Deviations from Plan

None -- plan executed exactly as written. The plan specified `raise APIError(...)` from validators (not `raise ValueError(APIError(...))`); this works because Pydantic propagates non-`ValueError` exceptions unchanged.

## Tests

`pytest tests/test_search_api.py -x` was NOT run because the existing tests use `mode=` (the Phase 78 field). Plan 04 explicitly rewrites those tests to use `search_mode`. Per the plan's `<verification>` block: "may fail in this plan because old tests use `mode=`; this is expected and Plan 04 fixes them. Do NOT block on it."

## Self-Check: PASSED

- `web/search_api.py` (in worktree) -- modified, contains `class ResponsaOptions`, `model_validator`, `MAX_LIMIT = 100`, `_SEARCH_MODE_TO_INTERNAL`, `unknown field 'mode'` cutover hint
- `shared/api_errors.py` (in worktree) -- modified, contains `'invalid_combination'`
- Commit `192d900d` exists in worktree branch (`git log` confirmed)
- All compile/import/behavior checks green
