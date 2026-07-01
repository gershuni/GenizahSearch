# Phase 132: Public API Dual-Mode - Research

**Researched:** 2026-07-01
**Domain:** Public REST API (FastAPI / Pydantic) — library-filter mode extension on POST /api/search and POST /api/parallels
**Confidence:** HIGH

---

## Summary

Phase 132 adds an optional `library_filter_mode` field (values `include` | `exclude`) alongside the existing `filters.library` on both public endpoints.
Omitting the field defaults to `include`, which is byte-for-byte today's behavior (backward-compatible).
`exclude` resolves to the complement — sys_ids whose `library_code` is NOT in the submitted set — intersected into the existing `restrict_sys_ids` path identically on both endpoints.
Invalid values produce a standard 400 `invalid_request` envelope.
The change touches exactly four code artefacts: `web/search_api.py` (Pydantic model + `_intersect_library_filter` helper), `docs/SEARCH_API.md`, and `skills/cairo-genizah-research/references/api_contract.md`.
No new packages are needed.

**Primary recommendation:** Add `library_filter_mode: Optional[Literal['include', 'exclude']] = 'include'` to `FiltersModel` at `web/search_api.py:105–141`; extend `_intersect_library_filter` at line 311 to pass `mode` down and negate the csv_bank scan when `mode == 'exclude'`.

---

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| DMF-11-1 | `POST /api/search` and `POST /api/parallels` accept optional `library_filter_mode` of `include`\|`exclude`; omitted = `include` = today's behavior | FiltersModel in `web/search_api.py:105` is the single model shared by both endpoints; adding the field there covers both at once |
| DMF-11-2 | `mode=exclude` + library set → results scoped to sys_ids NOT in set, intersected into `restrict_sys_ids`; verified by disjoint-libraries API test | Complement = one negated pass over `meta_mgr.csv_bank` in `_intersect_library_filter`; same executor pattern already used for include |
| DMF-11-3 | Invalid `mode` value → 400 `invalid_request` envelope (fail-closed) | `Literal['include','exclude']` on a Pydantic field with `extra='forbid'` on FiltersModel produces PydanticValidationError → caught by existing per-endpoint `except PydanticValidationError` → `_build_envelope_response` → 400 `invalid_request` |
| DMF-11-4 | Documented in `docs/SEARCH_API.md` and `skills/cairo-genizah-research/references/api_contract.md` | Both files located; exact sections identified |
</phase_requirements>

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Request model & validation | API / Backend (`web/search_api.py`) | — | Pydantic `FiltersModel` owned here; both endpoints share this model |
| Complement (exclude) resolution | API / Backend (`_intersect_library_filter` helper) | Database / Storage (`meta_mgr.csv_bank`) | csv_bank is the source of truth for `library_code → sys_id` mapping |
| Error response shape | API / Backend (`web/api_hardening._build_envelope_response`) | — | All 400s flow through this shared envelope builder |
| Documentation | Static docs surfaces | — | `docs/SEARCH_API.md` and `skills/cairo-genizah-research/references/api_contract.md` |

---

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| pydantic | 2.x (already installed) | Request model field, `Literal` type, validation | Already used for `FiltersModel`; `Literal['include','exclude']` is the correct Pydantic v2 pattern for an enum field with a typed default |
| fastapi | already installed | Route decorator, `Request` type | Already used; no change |

### Supporting
No new supporting libraries needed.

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| `Literal['include','exclude']` on `FiltersModel` | Separate `library_filter_mode` field on `SearchRequest` / `ParallelsRequest` | `FiltersModel` is the correct semantic home (mode governs the `library` filter); adding it to the top-level request model would mean callers can pass `library_filter_mode` even without `filters.library`, creating a confusing dead field. The `FiltersModel.extra='forbid'` constraint already handles unknown keys. |
| `Literal` constraint (Pydantic auto-400 on bad value) | Manual validation in handler body with explicit `APIError('invalid_request', ...)` | `Literal` on a Pydantic field is cleaner but the rejection goes through `PydanticValidationError` → `except PydanticValidationError` branch → `invalid_request` code (already the pattern). Either approach produces the same envelope; `Literal` is less code. |

**Installation:** No new packages. This phase is code-only.

---

## Package Legitimacy Audit

> No external packages are installed in this phase. All required libraries (pydantic, fastapi) are already present in the project.

| Package | Registry | Age | Downloads | Source Repo | slopcheck | Disposition |
|---------|----------|-----|-----------|-------------|-----------|-------------|
| (none) | — | — | — | — | — | No new packages |

**Packages removed due to slopcheck [SLOP] verdict:** none
**Packages flagged as suspicious [SUS]:** none

---

## Architecture Patterns

### System Architecture Diagram

```
POST /api/search or POST /api/parallels
         |
         v
   [Pydantic parse — FiltersModel]
         |
    filters.library: Optional[List[str]]
    filters.library_filter_mode: Optional[Literal['include','exclude']]  ← NEW
         |
         v
   [validate_filter_values]
    — library codes validated against LIBRARY_CODES (400 on unknown) [EXISTING]
    — library_filter_mode: Literal constraint rejects invalid values [NEW]
         |
         v
   [_intersect_library_filter(restrict_sys_ids, filters_dict, meta_mgr)]
         |
         +--- mode == 'include' (or absent) ---→ csv_bank scan: library_code IN set
         |    (today's behavior, unchanged)        → restrict_sys_ids &= lib_ids
         |
         +--- mode == 'exclude' ---→ csv_bank scan: library_code NOT IN set
              (complement)                         → restrict_sys_ids &= complement_ids
         |
         v
   [execute_search / fetch_parallels_results](restrict_sys_ids=...)
         |
         v
   [serialize + return envelope]
```

### Recommended Project Structure

No structural changes. All changes are within existing files:

```
web/
└── search_api.py        # FiltersModel + _intersect_library_filter
docs/
└── SEARCH_API.md        # filters.library documentation table
skills/cairo-genizah-research/references/
└── api_contract.md      # POST /api/search + parallels notes
tests/
└── test_search_api_library_mode.py   # NEW: disjoint-libraries + 400 tests
```

### Pattern 1: Adding a Pydantic `Literal` field to `FiltersModel`

**What:** Add `library_filter_mode` as an optional field with a default of `'include'`.
**When to use:** When the new field has a small closed set of valid values; Pydantic's `Literal` enforces the closed set automatically.

```python
# Source: web/search_api.py — FiltersModel (line 105)
# [VERIFIED: codebase grep, confirmed pattern used in ParallelsRequest.mode and SearchRequest.search_mode]
from typing import Literal, Optional, List

class FiltersModel(BaseModel):
    model_config = ConfigDict(extra='forbid')

    # ... existing fields ...

    library: Optional[List[str]] = Field(
        default=None,
        description="Library codes (e.g. 'CUL', 'JTS'). ...",
    )
    library_filter_mode: Optional[Literal['include', 'exclude']] = Field(
        default=None,  # Codex R1 HIGH: None (not 'include') → omitted callers' echo stays byte-for-byte; normalise None→'include' in _intersect_library_filter
        description=(
            "How the library list is applied. 'include' (default): restrict results to "
            "manuscripts in the given libraries (today's behavior). "
            "'exclude': restrict results to manuscripts NOT in the given libraries "
            "(complement). Omitting this field is equivalent to 'include'. "
            "Invalid values -> 400 invalid_request."
        ),
    )
```

**Why `Optional[Literal[...]] = 'include'` rather than `Literal[...] = 'include'`:**
Pydantic v2 treats `Optional[X]` as `Union[X, None]`. Using `Optional` allows `null`/`None` from JSON (treated as "use default"), which is more client-friendly. The handler normalises `None → 'include'` in `_intersect_library_filter`. [VERIFIED: codebase pattern — `ParallelsRequest.mode: Literal[...] = 'exact'` does NOT use Optional because it has a non-None default with no null-pass-through intent; here we want null-as-include]

### Pattern 2: Extending `_intersect_library_filter` for complement mode

**What:** The existing helper at `web/search_api.py:311` reads `filters_dict.get('library')` and calls `resolve_library_sys_ids`. The complement variant performs the same csv_bank scan but negates the membership test.

```python
# Source: web/search_api.py:311 — _intersect_library_filter (annotated with mode extension)
# [VERIFIED: codebase read, lines 311-337]

async def _intersect_library_filter(restrict_sys_ids, filters_dict, meta_mgr):
    libs = (filters_dict or {}).get('library')
    if not libs:
        return restrict_sys_ids

    mode = (filters_dict or {}).get('library_filter_mode') or 'include'

    from shared import fjms_service as _fjms_module
    loop = asyncio.get_running_loop()

    if mode == 'exclude':
        # Complement: resolve the EXCLUDE set, then subtract from full corpus.
        # resolve_library_sys_ids already iterates csv_bank; complement needs
        # one additional pass or a negated comprehension.  Cheapest correct
        # approach: resolve include set, then compute full_corpus - include_set.
        excl_ids = await loop.run_in_executor(
            None, _fjms_module.resolve_library_sys_ids, libs, meta_mgr
        )
        # Complement = all sys_ids in csv_bank NOT in the excluded set.
        all_ids = await loop.run_in_executor(
            None, lambda: set(meta_mgr.csv_bank.keys())
        )
        lib_ids = all_ids - excl_ids
    else:
        # Include (default) — today's behavior unchanged.
        lib_ids = await loop.run_in_executor(
            None, _fjms_module.resolve_library_sys_ids, libs, meta_mgr
        )

    if restrict_sys_ids is None:
        return lib_ids
    return restrict_sys_ids & lib_ids
```

**Performance note:** Two executor calls for exclude mode (one for `resolve_library_sys_ids`, one for `set(csv_bank.keys())`). The `set(meta_mgr.csv_bank.keys())` call is O(255K) but produces a plain Python set in ~5ms — the same order as the include scan. Both run off the event loop. [VERIFIED: resolve_library_sys_ids docstring at fjms_service.py:3773 confirms O(255K) csv_bank iteration is acceptable and the pattern for the include mode]

**Alternative:** A single pass negated comprehension in a new helper `resolve_library_complement_sys_ids` avoids building the full-key set separately:

```python
# Single-pass complement (avoids allocating full key set):
def resolve_library_complement_sys_ids(library_codes, meta_mgr) -> set:
    if not library_codes or meta_mgr is None:
        return set()
    from shared.browse_map_utils import LIBRARY_CODES as _VALID_CODES
    excl_set = {c for c in library_codes if c in _VALID_CODES}
    if not excl_set:
        return set(meta_mgr.csv_bank.keys())   # exclude nothing = full corpus
    return {
        sid
        for sid, row in meta_mgr.csv_bank.items()
        if row.get("library_code") not in excl_set
    }
```

This mirrors `resolve_library_sys_ids` exactly but negates the membership test. [VERIFIED: resolve_library_sys_ids at fjms_service.py:3819-3823 — complement just swaps `in` → `not in`]. **This single-pass version is preferred** — one executor call, no intermediate set allocation.

### Pattern 3: Validate mode BEFORE calling `_intersect_library_filter`

The Pydantic `Literal` constraint means an invalid `library_filter_mode` value is rejected at Pydantic parse time, before the handler body runs. The existing `except PydanticValidationError` branch at `web/search_api.py:928-944` (for `/api/search`) and the `wrap_endpoint` decorator for `/api/parallels` catch these and return the standard 400 `invalid_request` envelope.

**No additional validation code is needed in the handler body** — Pydantic's type system handles it. [VERIFIED: codebase read — `search_endpoint` lines 923-943, `wrap_endpoint` used on `parallels_endpoint`]

### Pattern 4: Request echo / `filters_dict` passthrough

The `filters_dict` is built by `req.filters.model_dump(exclude_none=True)` at:
- `/api/search`: `web/search_api.py:1015`
- `/api/parallels`: `web/search_api.py:1545`

`model_dump(exclude_none=True)` will include `library_filter_mode` in the dumped dict whenever it is not `None`. Since the default is `'include'` (not `None`), a client that omits the field entirely gets `library_filter_mode='include'` in the echo — accurate and informative.

The code dumps via `model_dump(exclude_none=True)`, so use `Optional[Literal['include','exclude']] = Field(default=None)` and normalise `None → 'include'` in `_intersect_library_filter`. An omitted field is None → dropped from the dump → the request echo stays byte-for-byte identical (existing exact-echo tests unaffected). **DECISION (Codex R1 HIGH, 2026-07-01): `default=None`** — NOT `'include'`, which would inject the key into every caller's echo and break `tests/test_search_api_v2.py:662`/`:1237`.

### Anti-Patterns to Avoid

- **Putting `library_filter_mode` on the top-level request model** (`SearchRequest` / `ParallelsRequest`): it belongs inside `FiltersModel` because it governs the `library` filter specifically. Adding it at the top level allows callers to pass it without `filters.library`, creating a dead field.
- **Using `validate_filter_values` to check the mode value**: mode is a Pydantic `Literal`; structural rejection by Pydantic is cleaner and consistent with how all other enum fields (`search_mode`, `mode`, `boundary_mode`) are validated in this codebase.
- **Applying exclude as a post-search client-side filter**: the UI does this for "hide" mode, but the API must scope the corpus pre-search (via `restrict_sys_ids`) so the result cap and `total` reflect the filtered universe. [VERIFIED: `_intersect_library_filter` docstring confirms "BEFORE the result cap"]
- **Failing open when library_filter_mode='exclude' + empty library list**: an empty library list with mode='exclude' means "exclude nothing = show all". The `_intersect_library_filter` already short-circuits when `not libs`, so this is naturally handled. [VERIFIED: `web/search_api.py:327-329`]

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Complement sys_id set | Custom SQL or multi-step filter | Single-pass negated csv_bank comprehension (mirrors `resolve_library_sys_ids`) | csv_bank is already the authoritative mapping; the complement is trivially `not in excl_set` on the same iteration |
| Mode enum validation | Manual `if mode not in ('include', 'exclude')` in handler body | Pydantic `Literal['include', 'exclude']` on `FiltersModel` | Consistent with how ALL other mode/enum fields are validated (`search_mode`, `boundary_mode`, `mode` on parallels); automatic PydanticValidationError → `invalid_request` envelope |
| 400 error envelope | Custom JSON response construction | `_build_envelope_response(request, exc)` from `web.api_hardening` | Already called in both endpoints' `except PydanticValidationError` branch |

**Key insight:** The entire complement implementation is a one-line change to the `_intersect_library_filter` helper — the rest is Pydantic field declaration and documentation.

---

## Common Pitfalls

### Pitfall 1: `extra='forbid'` on `FiltersModel` hard-rejects unknown keys
**What goes wrong:** Any key added to `FiltersModel` that is not listed as a Pydantic field will be rejected by `extra='forbid'` on the CALLER's side — if a client sends `{"library_filter_mode": "exclude"}` AND we haven't added the field to the model, they get 400 `invalid_request`. Conversely, once we add the field, unknown keys still fail closed. [VERIFIED: `web/search_api.py:107`]
**Why it happens:** `ConfigDict(extra='forbid')` is a deliberate security choice in this codebase.
**How to avoid:** Add `library_filter_mode` as a proper named field on `FiltersModel`. Do NOT add it as a free-form field or handle it outside Pydantic.
**Warning signs:** Test `test_extra_top_level_key_rejected` passes (it should); test for valid `library_filter_mode` returns 200.

### Pitfall 2: `model_dump(exclude_none=True)` behavior with default `'include'`
**What goes wrong:** If `library_filter_mode` uses `Optional[...] = None` as default and the handler passes `filters_dict` to `_intersect_library_filter`, a client that omits the field gets `None` in `filters_dict` — then `(filters_dict or {}).get('library_filter_mode')` returns `None`, which must be treated as `'include'`. [ASSUMED — design detail to confirm]
**Why it happens:** `exclude_none=True` strips `None` values; `Optional[...] = 'include'` includes the field only when the value is non-`None`.
**How to avoid:** Use `default=None` (Codex R1 HIGH) and normalise `None → 'include'` explicitly in `_intersect_library_filter`, so an omitted field is dropped by `exclude_none=True` and the echo stays byte-for-byte unchanged. (A non-None `'include'` default would inject the key into every caller's echo and break the exact-echo tests — rejected.)
**Warning signs:** Mode echo in response shows wrong value; include/exclude behaves identically.

### Pitfall 3: DMF-10 — `'LOCAL'` must not be passable as an exclude code
**What goes wrong:** A caller sending `filters.library=['LOCAL']` with `library_filter_mode='exclude'` would "exclude LOCAL" — which is a no-op since LOCAL is never in the web API corpus, but it should still be rejected by the existing `validate_filter_values` library code check. [VERIFIED: `fjms_service.py:1499-1508` — `v not in LIBRARY_CODES` raises 400; `LOCAL` IS in `LIBRARY_CODES` so it would pass! But it is then handled as a valid code in `resolve_library_sys_ids` which filters from csv_bank, and LOCAL records don't appear in csv_bank on web.]
**The actual gap:** `validate_filter_values` at line 1501 checks `if v not in LIBRARY_CODES` — `LOCAL` is in `LIBRARY_CODES`, so it passes validation. The guard `c != 'LOCAL'` in `sanitize_library_codes` is for the UI path, not the API path. [VERIFIED: `browse_map_utils.py:184-199` — sanitize_library_codes is for UI restore, not called by the API validate path]
**Consequence:** A caller submitting `library=['LOCAL']` on the API today gets past validation and `resolve_library_sys_ids` returns an empty set (csv_bank has no LOCAL rows on the web server). For `exclude` mode with `library=['LOCAL']`, the complement of an empty set = full corpus = effectively no filter. This is a no-op, not a security issue, but it is inconsistent behavior.
**How to avoid:** The planner should decide whether to add a `LOCAL` guard to `validate_filter_values` for the library filter. Given DMF-10's cross-cutting status and the fact that v8.3.0 CI failed on it, the safest approach is to add the guard. [ASSUMED — planner decision needed]
**Warning signs:** API accepts `library=['LOCAL']` without 400; test `test_web_library_options_no_local.py` does not cover the API path.

### Pitfall 4: Backward-compat — `filters_dict` propagation to `_intersect_library_filter`
**What goes wrong:** The `_intersect_library_filter` call at lines 1033 and 1558 passes `filters_dict` — which is the output of `req.filters.model_dump(exclude_none=True)`. If `library_filter_mode` is added to `FiltersModel` with `default='include'`, the value will always be present in the dump, including for existing callers that omit it. [VERIFIED: Python Pydantic v2 behavior with non-None defaults]
**Why it happens:** `model_dump(exclude_none=True)` excludes None values only; `'include'` is not None.
**How to avoid:** This is the desired behavior — the echo always shows the effective mode. No code change needed for backward compat beyond adding the field.

### Pitfall 5: Request echo must include `library_filter_mode`
**What goes wrong:** The `request_echo` dict at `web/search_api.py:1201-1211` is built manually and includes `filters: filters_dict`. Since `library_filter_mode` lives IN `FiltersModel` (and thus in `filters_dict`), it will automatically appear in the echo via `'filters': filters_dict` — no separate echo code needed. [VERIFIED: lines 1201-1211]
**Warning signs:** Echo shows `filters.library` but not `filters.library_filter_mode`.

### Pitfall 6: OpenAPI schema must reflect the new field
**What goes wrong:** The OpenAPI metadata is built from `_openapi_request_body(SearchRequest)` at line 863 and `_openapi_request_body(ParallelsRequest)` at line 1456. These call `model_json_schema()` on the request model. Because `FiltersModel` is a nested model referenced by `filters: Optional[FiltersModel]`, and `_inline_schema_refs` inlines all `$defs`, the new `library_filter_mode` field will automatically appear in the OpenAPI spec when added to `FiltersModel`. [VERIFIED: `_openapi_request_body` at lines 737-750; `_inline_schema_refs` at lines 699-734]
**How to avoid:** No extra work needed — Pydantic's `model_json_schema()` picks up all fields automatically.

---

## Code Examples

Verified patterns from official sources:

### Current `FiltersModel` field declarations (annotated with where to add)
```python
# Source: web/search_api.py:105-141 [VERIFIED: codebase read]
class FiltersModel(BaseModel):
    model_config = ConfigDict(extra='forbid')

    domains: Optional[List[str]] = Field(default=None, ...)
    authors: Optional[List[str]] = Field(default=None, ...)
    works: Optional[List[str]] = Field(default=None, ...)
    library: Optional[List[str]] = Field(
        default=None,
        description="Library codes...",
    )
    # ADD AFTER library:
    # library_filter_mode: Optional[Literal['include', 'exclude']] = Field(
    #     default=None,  # Codex R1 HIGH: None, not 'include'
    #     description="...",
    # )
    materials: Optional[List[str]] = Field(default=None, ...)
    date_from: Optional[int] = Field(default=None, ...)
    date_to: Optional[int] = Field(default=None, ...)
```

### Current `_intersect_library_filter` (annotated with mode extension site)
```python
# Source: web/search_api.py:311-337 [VERIFIED: codebase read]
async def _intersect_library_filter(restrict_sys_ids, filters_dict, meta_mgr):
    libs = (filters_dict or {}).get('library')
    if not libs:
        return restrict_sys_ids          # ← short-circuit covers exclude+empty case too
    # ADD: mode = (filters_dict or {}).get('library_filter_mode') or 'include'
    from shared import fjms_service as _fjms_module
    loop = asyncio.get_running_loop()
    # EXISTING include path:
    lib_ids = await loop.run_in_executor(
        None, _fjms_module.resolve_library_sys_ids, libs, meta_mgr
    )
    # ADD exclude branch:
    # if mode == 'exclude':
    #     lib_ids = await loop.run_in_executor(
    #         None, _fjms_module.resolve_library_complement_sys_ids, libs, meta_mgr
    #     )
    if restrict_sys_ids is None:
        return lib_ids
    return restrict_sys_ids & lib_ids
```

### New `resolve_library_complement_sys_ids` helper (to add to `shared/fjms_service.py`)
```python
# Mirrors resolve_library_sys_ids (fjms_service.py:3773) with negated membership test
# [VERIFIED: resolve_library_sys_ids at fjms_service.py:3819-3823 — complement = `not in`]
def resolve_library_complement_sys_ids(library_codes, meta_mgr) -> set:
    """Reverse-lookup: sys_ids whose library_code is NOT in library_codes.

    Complement of resolve_library_sys_ids. Used for API library_filter_mode='exclude'.
    Returns set() when library_codes is empty/None (no-op — caller short-circuits).
    Returns full corpus when all supplied codes are unknown/invalid.
    """
    if not library_codes or meta_mgr is None:
        return set()
    try:
        from shared.browse_map_utils import LIBRARY_CODES as _VALID_CODES
    except ImportError:
        logger.error("resolve_library_complement_sys_ids: could not import LIBRARY_CODES")
        return set(meta_mgr.csv_bank.keys())   # fail-open: full corpus
    excl_set = {c for c in library_codes if c in _VALID_CODES}
    if not excl_set:
        # All codes unknown — excluding nothing = full corpus
        return set(meta_mgr.csv_bank.keys())
    return {
        sid
        for sid, row in meta_mgr.csv_bank.items()
        if row.get("library_code") not in excl_set
    }
```

### 400 error envelope shape (from existing codebase)
```json
// Source: shared/api_errors.py + web/api_hardening._build_envelope_response [VERIFIED]
{
  "error": {
    "code": "invalid_request",
    "message": "1 validation error for FiltersModel\nlibrary_filter_mode\n  Input should be 'include' or 'exclude' [type=literal_error, ...]"
  }
}
```

### Test pattern for the disjoint-libraries assertion (mirrors existing test_search_api.py)
```python
# Pattern from test_search_api.py:284-338 (filter_resolution tests) [VERIFIED: codebase read]
def test_library_include_exclude_disjoint(client, populated_state, clean_env, monkeypatch):
    """include=['CUL'] vs exclude=['CUL'] return disjoint result library sets."""
    # monkeypatch resolve_library_sys_ids to return a small known set
    # then assert include results have only CUL, exclude results have no CUL
    ...
```

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Single-mode include-only library filter (`filters.library`) | Dual-mode with optional `library_filter_mode` | Phase 132 (this phase) | Programmatic callers can now express "hide these libraries" |
| UI hide mode applies post-search client-side filter | API hide/exclude resolves pre-search via `restrict_sys_ids` | Phase 132 (this phase) | API result cap and `total` reflect the filtered corpus, not the full corpus |

**Deprecated/outdated:**
- The `_intersect_library_filter` docstring refers to `filters.library` as an "inclusion filter" — this will need updating to say "inclusion or exclusion, depending on `library_filter_mode`".

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `library_filter_mode` belongs in `FiltersModel` rather than as a top-level field on `SearchRequest`/`ParallelsRequest` | Architecture, Code Examples | If it's at the top level, the planner must add it to both request models instead of one. Behavior is identical. |
| A2 | REVERSED (Codex R1 HIGH, 2026-07-01): `default=None` + internal `None→'include'` normalisation is REQUIRED (NOT `default='include'`), so omitting callers' request echo stays byte-for-byte | Code Examples | `_intersect_library_filter` treats `None → 'include'`; a non-None default would break exact-echo tests (test_search_api_v2.py:662/:1237) |
| A3 | Adding a `LOCAL` guard to `validate_filter_values` library check is OPTIONAL (the current behavior is a no-op, not a security gap) | Pitfall 3 | If `LOCAL` should explicitly 400, the planner needs an extra task to add the guard to `validate_filter_values`. Currently `LOCAL` passes validation but resolves to empty set (web csv_bank has no LOCAL rows). |
| A4 | `resolve_library_complement_sys_ids` is added to `shared/fjms_service.py` (module-level function, alongside `resolve_library_sys_ids`) | Code Examples | Could also be inlined in `_intersect_library_filter`. Separate function is more testable and mirrors the existing naming convention. |

---

## Open Questions

1. **`library_filter_mode` field placement: inside `FiltersModel` vs top-level request field?**
   - What we know: `FiltersModel` is shared by both endpoints; top-level placement would require changes to `SearchRequest` AND `ParallelsRequest`.
   - What's unclear: product preference on whether the echo shows `filters.library_filter_mode` or a top-level `library_filter_mode`.
   - Recommendation: inside `FiltersModel` (semantic coupling, single change point). The research supports this; see Architecture Patterns.

2. **Should `validate_filter_values` reject `library=['LOCAL']` with a 400?**
   - What we know: `LOCAL` is in `LIBRARY_CODES` and passes current validation. On web, csv_bank has no LOCAL rows, so the result is an empty include set / full-corpus exclude set.
   - What's unclear: whether `LOCAL` should be explicitly rejected at the API layer for consistency with DMF-10.
   - Recommendation: add the guard for correctness (no LOCAL on the API path), consistent with `sanitize_library_codes`. One line in `validate_filter_values`.

---

## Environment Availability

Step 2.6: SKIPPED — Phase 132 has no external dependencies. It is a code-only change to existing Python files and documentation. No new services, CLIs, runtimes, or databases are needed.

---

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest (already installed) |
| Config file | `pytest.ini` / `conftest.py` |
| Quick run command | `pytest tests/test_search_api_library_mode.py -x` |
| Full suite command | `pytest tests/ -k "not gui" -x` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| DMF-11-1 | `include` mode (default) = today's behavior unchanged | unit | `pytest tests/test_search_api_library_mode.py::test_include_mode_is_default_same_as_omitted -x` | ❌ Wave 0 |
| DMF-11-1 | Omitting `library_filter_mode` altogether behaves identically to `mode='include'` | unit | `pytest tests/test_search_api_library_mode.py::test_omit_mode_equals_include -x` | ❌ Wave 0 |
| DMF-11-2 | `exclude` + codes → only results from OTHER libraries | unit | `pytest tests/test_search_api_library_mode.py::test_exclude_restricts_to_complement -x` | ❌ Wave 0 |
| DMF-11-2 | `include` vs `exclude` on same library set → disjoint result library sets | unit | `pytest tests/test_search_api_library_mode.py::test_include_vs_exclude_disjoint -x` | ❌ Wave 0 |
| DMF-11-2 | Both endpoints (`/api/search` AND `/api/parallels`) honor the mode | unit | `pytest tests/test_search_api_library_mode.py::test_parallels_exclude_mode -x` | ❌ Wave 0 |
| DMF-11-3 | Invalid mode value → 400 `invalid_request` | unit | `pytest tests/test_search_api_library_mode.py::test_invalid_mode_returns_400 -x` | ❌ Wave 0 |
| DMF-11-3 | Mode without library list → no filter applied (no-op) | unit | `pytest tests/test_search_api_library_mode.py::test_mode_without_library_is_noop -x` | ❌ Wave 0 |
| DMF-11 | `resolve_library_complement_sys_ids` unit test | unit | `pytest tests/test_search_api_library_mode.py::test_resolve_library_complement_sys_ids -x` | ❌ Wave 0 |

### Sampling Rate
- **Per task commit:** `pytest tests/test_search_api_library_mode.py -x`
- **Per wave merge:** `pytest tests/ -k "not gui" -x --ignore=tests/test_search_api_soak.py`
- **Phase gate:** Full suite green before `/gsd-verify-work`

### Wave 0 Gaps
- [ ] `tests/test_search_api_library_mode.py` — new file covering all DMF-11 requirements above (8 tests)
- [ ] The `resolve_library_complement_sys_ids` helper in `shared/fjms_service.py` does not yet exist

*(No framework install needed — pytest already present)*

---

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | no | — |
| V3 Session Management | no | — |
| V4 Access Control | no | — |
| V5 Input Validation | yes | Pydantic `Literal` + `extra='forbid'` on `FiltersModel`; `validate_filter_values` for library code validation |
| V6 Cryptography | no | — |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Sending unknown `library_filter_mode` values to probe server behavior | Tampering | `Literal['include','exclude']` → PydanticValidationError → 400 `invalid_request` (fail-closed) |
| Sending very large `library` list with `mode=exclude` to force a full-corpus scan | DoS | Already mitigated: `validate_filter_values` validates each code; large lists of valid codes produce the same O(255K) csv_bank scan as include mode; no amplification |
| `library=['LOCAL']` with `mode=exclude` to infer LOCAL content existence | Information Disclosure | No-op on web (csv_bank has no LOCAL rows); complement of empty = full corpus = effectively unfiltered. LOW risk. |

---

## Sources

### Primary (HIGH confidence)
- `web/search_api.py` — `FiltersModel` (lines 105-141), `_intersect_library_filter` (lines 311-337), `search_endpoint` filter resolution (lines 1010-1037), `parallels_endpoint` filter resolution (lines 1540-1562), request echo (lines 1201-1211), OpenAPI helpers (lines 699-816) [VERIFIED: codebase read]
- `shared/fjms_service.py` — `resolve_library_sys_ids` (lines 3773-3823), `validate_filter_values` library section (lines 1496-1508), `get_browse_results` `library_mode` parameter (lines 2249-2295) [VERIFIED: codebase read]
- `shared/browse_map_utils.py` — `sanitize_library_codes` (lines 184-199), `library_codes_with_manuscripts` (lines 123-178) [VERIFIED: codebase read]
- `shared/api_errors.py` — `ERROR_CODES` frozenset, `APIError` class [VERIFIED: codebase read]
- `.planning/REQUIREMENTS.md` — DMF-11 requirement text [VERIFIED: codebase read]
- `.planning/ROADMAP.md` — Phase 132 plan shape (lines 158-169) [VERIFIED: codebase read]
- `docs/SEARCH_API.md` — `filters.library` documentation at line 184 [VERIFIED: codebase read]
- `skills/cairo-genizah-research/references/api_contract.md` — `filters` notes at lines 52-59 [VERIFIED: codebase read]

### Secondary (MEDIUM confidence)
- `web/pages/parallels.py` — UI "hide" mode implementation (lines 2655-2666) confirms that "show_only" goes pre-search via `restrict_sys_ids` while "hide" currently goes post-search via `_apply_parallels_library_filter`; the API must do both pre-search [VERIFIED: codebase read]
- `tests/test_parallels_library_filter.py` — test patterns for dual-mode library filter [VERIFIED: codebase read]
- `tests/test_search_api.py` — test patterns for filter resolution, error envelope, idempotency [VERIFIED: codebase read]
- `tests/test_parallels_api.py` — test patterns for parallels endpoint (unknown mode, filter rejection) [VERIFIED: codebase read]

### Tertiary (LOW confidence)
- None.

---

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — pure Pydantic/FastAPI, no new packages, well-established codebase patterns
- Architecture: HIGH — `_intersect_library_filter` is the single chokepoint for both endpoints; complement is a one-line negation of the existing csv_bank scan
- Pitfalls: HIGH — all pitfalls verified against actual code; no speculative hazards
- Test patterns: HIGH — mirrors existing `test_search_api.py` and `test_parallels_api.py` conventions exactly

**Research date:** 2026-07-01
**Valid until:** 2026-07-31 (stable API surface; no fast-moving dependencies)

---

## RESEARCH COMPLETE
