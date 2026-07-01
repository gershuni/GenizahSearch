# 132-SECURITY.md — Phase 132 Security Audit

**Phase:** 132 — Public API Dual-Mode (library_filter_mode on POST /api/search + POST /api/parallels)
**Audited:** 2026-07-01
**ASVS Level:** 1
**block_on:** high
**Result:** SECURED — 5/5 threats CLOSED, 0 OPEN

---

## Threat Verification

| Threat ID | Category | Disposition | Status | Evidence |
|-----------|----------|-------------|--------|----------|
| T-132-01 | Tampering — library_filter_mode value | mitigate | CLOSED | `web/search_api.py:107` `model_config = ConfigDict(extra='forbid')` + `web/search_api.py:131` `library_filter_mode: Optional[Literal['include','exclude']] = Field(default=None, ...)`. Any non-enum value triggers PydanticValidationError → 400 `invalid_request` on both endpoints. Pinned by `tests/test_search_api_library_mode.py:399-419` (`test_invalid_mode_returns_400`), which asserts HTTP 400 + `error.code == 'invalid_request'` on BOTH `/api/search` and `/api/parallels`. |
| T-132-02 | Denial of Service — large library list + mode=exclude | accept | CLOSED | Accepted risk. Rationale is sound: `shared/fjms_service.py:3879-3883` confirms a single O(N) `csv_bank` pass with `not in excl_set` — identical cost profile to the include path at `shared/fjms_service.py:3823-3827`. Both paths run off the event loop via `loop.run_in_executor(None, ...)` at `web/search_api.py:354-361`. Each code is validated upstream by `validate_filter_values`. No amplification vector. |
| T-132-03 | Information Disclosure — library=['LOCAL'] + mode=exclude | accept | CLOSED | Accepted risk (user decision D2, 2026-07-01). Rationale confirmed: no LOCAL rows exist in the web server's `csv_bank`; complement of an empty exclude set returns the full corpus (`shared/fjms_service.py:3869-3877` — `excl_set` empty → `set(meta_mgr.csv_bank.keys())`), which is the same as unfiltered. No LOCAL-specific handling added per user direction. No sensitive data disclosed. |
| T-132-04 | Information Disclosure — public docs describing exclude semantics | accept | CLOSED | Accepted risk. Confirmed both public doc surfaces are updated: `docs/SEARCH_API.md:184` documents `library_filter_mode` with values, default, exclude/complement semantics, both endpoints, and 400-on-invalid. `skills/cairo-genizah-research/references/api_contract.md:54-61` carries consistent wording. Documents only public, validated filter behavior; no sensitive corpus data disclosed. |
| T-132-SC | Tampering — supply chain (pip installs) | mitigate | CLOSED | `git diff 16fcf7a1..HEAD -- requirements*.txt pyproject.toml setup.py` produces zero output. No new packages installed across any of the three plans. Pydantic and FastAPI were already present. |

---

## Unregistered Flags

**None.** All three SUMMARY.md files (`132-01-SUMMARY.md`, `132-02-SUMMARY.md`, `132-03-SUMMARY.md`) report "Threat Flags: None." No new network endpoints, auth paths, or trust boundaries were introduced.

---

## Verification Details

### T-132-01 (mitigate) — Pydantic Literal + extra='forbid'

Both controls verified in implementation:

1. `web/search_api.py:107` — `model_config = ConfigDict(extra='forbid')` on `FiltersModel`  
   Rejects any unknown key, including any future attempted bypass, before the handler body runs.

2. `web/search_api.py:131-142` — `library_filter_mode: Optional[Literal['include','exclude']] = Field(default=None, ...)`  
   Accepts only the two declared enum values; any other string (including `None`-typed field with a value) raises `PydanticValidationError`.

3. Test coverage at `tests/test_search_api_library_mode.py:399-419` — `test_invalid_mode_returns_400` posts `'sideways'` and asserts `HTTP 400` + `error.code == 'invalid_request'` on BOTH `/api/search` and `/api/parallels`. No handler-body validation code was added; the control is structural.

### T-132-SC (mitigate) — No new packages

`git diff 16fcf7a1..HEAD` over `requirements*.txt`, `pyproject.toml`, and `setup.py` returns empty. Confirmed zero new packages across the three plans.

---

## Backward-Compatibility Note (informational, not a threat)

`library_filter_mode` defaults to `None` (not `'include'`). Callers that omit the field receive `model_dump(exclude_none=True)` output with the key absent, keeping the request echo byte-for-byte identical to pre-Phase-132 behavior. This is pinned by `tests/test_search_api_library_mode.py:195-227` (`test_omit_mode_equals_include`).

---

*Auditor: claude-sonnet-4-6 via gsd-security-auditor*  
*Phase directory: `.planning/phases/132-public-api-dual-mode-api-search-api-parallels/`*
