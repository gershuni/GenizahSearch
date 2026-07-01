---
phase: 132-public-api-dual-mode-api-search-api-parallels
verified: 2026-07-01T14:00:00Z
status: passed
score: 4/4
overrides_applied: 0
live_smoke:
  performed: 2026-07-01 (v8.4.1 deployed to genizahsearch.com at commit c624aa83)
  exclude: "POST /api/search query='אשר' library=['CUL'] mode=exclude → total 28007, 50-sample = 0 CUL (Oxford/RNL/JTS/Lutzki/AIU/HUC) — PASS"
  include: "same query mode=include → total 8554, 50-sample = all CUL — PASS"
  invalid: "mode='sideways' → HTTP 400 error.code=invalid_request — PASS"
  disjoint: "include(8554) + exclude(28007) < baseline(40029); ~3468 gap = matching docs whose sys_id is not in the library csv_bank (synthetic/unmapped), scoped out in both directions like the existing include path — consistent + correct"
---

# Phase 132: Public API Dual-Mode Verification Report

**Phase Goal:** Programmatic callers can express "hide these libraries" as well as "only these" — `POST /api/search` and `POST /api/parallels` accept an optional `filters.library_filter_mode` (`include | exclude`) alongside `filters.library`, backward-compatibly.
**Verified:** 2026-07-01T14:00:00Z
**Status:** passed (live prod smoke confirmed 2026-07-01 — see `live_smoke` in frontmatter)
**Re-verification:** No — initial verification; live smoke closed the human_needed item

---

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | Both endpoints accept optional `library_filter_mode` (include\|exclude); omitted = byte-for-byte today's behavior | VERIFIED | `FiltersModel.library_filter_mode: Optional[Literal['include','exclude']] = Field(default=None)` at `web/search_api.py:131`. Default `None`; both endpoints call `req.filters.model_dump(exclude_none=True)` (lines 1042, 1572) which drops the key when omitted. `test_omit_mode_equals_include` asserts echo contains NO `library_filter_mode` key — PASSES. |
| 2 | `mode=exclude` + a set → results scoped to complement (library_code NOT in set), intersected into `restrict_sys_ids` on BOTH endpoints; include vs exclude on same set are disjoint | VERIFIED | `_intersect_library_filter` reads `mode = (filters_dict or {}).get('library_filter_mode') or 'include'`, branches on `'exclude'` to call `resolve_library_complement_sys_ids` via `run_in_executor` (`web/search_api.py:354-357`). Both call sites at lines 1060 and 1585 pass `filters_dict`. `resolve_library_complement_sys_ids` is a single O(N) pass with `not in excl_set` (`shared/fjms_service.py:3875-3879`). Tests `test_exclude_restricts_to_complement`, `test_include_vs_exclude_disjoint`, `test_parallels_exclude_mode`, `test_intersect_helper_exclude_branch` all PASS. |
| 3 | Invalid `mode` value → 400 `invalid_request` on both endpoints (fail-closed) | VERIFIED | `FiltersModel` has `model_config = ConfigDict(extra='forbid')` (line 107) and `library_filter_mode` is `Optional[Literal['include','exclude']]` (line 131). Pydantic rejects any out-of-Literal value with `PydanticValidationError` caught by the existing envelope handlers. `test_invalid_mode_returns_400` asserts HTTP 400 + `error.code == 'invalid_request'` on BOTH endpoints — PASSES. |
| 4 | Documented in `docs/SEARCH_API.md` and `skills/cairo-genizah-research/references/api_contract.md` (omitted-mode default + exclude/complement semantics) | VERIFIED | `docs/SEARCH_API.md:184` documents `library_filter_mode` with values, default `include`, omitted≡include, exclude=complement, both endpoints, 400 on invalid. `skills/cairo-genizah-research/references/api_contract.md` contains 2 occurrences: field name + description with include/exclude semantics, both endpoints, 400 note. Docs guard `tests/test_search_api_docs.py` — 8/8 PASS. `check_docs.py` — no new blocking issues. |

**Score: 4/4 truths verified**

---

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `tests/test_search_api_library_mode.py` | Wave 0 test scaffold — 9 tests covering all DMF-11 behaviors on both endpoints | VERIFIED | Exists (commit `0da02e11`). 9 tests collected, 9/9 PASS. Covers both `/api/search` and `/api/parallels`. No LOCAL handling. `grep -c "def test_" == 9`. |
| `web/search_api.py` | `library_filter_mode` field on `FiltersModel` + exclude branch in `_intersect_library_filter` | VERIFIED | Field declared at line 131 (`Optional[Literal['include','exclude']] = Field(default=None)`). Exclude branch at lines 354-357 calls complement helper via `run_in_executor`. 4 occurrences of `library_filter_mode` in file (field decl, `library` description update, helper docstring, read in `_intersect_library_filter`). Commit `dc0c5033`. |
| `shared/fjms_service.py` | `resolve_library_complement_sys_ids` single-pass negation helper | VERIFIED | Function at line 3826, docstrings + implementation spanning lines 3826-3879. Single O(N) csv_bank pass with `not in excl_set`. Empty/None codes → `set()`; all-unknown codes → full corpus (fail-open); ImportError → full corpus + log. Exact complement invariant verified by `test_resolve_library_complement_sys_ids`. Commit `ae232d49`. |
| `docs/SEARCH_API.md` | `library_filter_mode` row/note in the request-fields section | VERIFIED | Line 184: documents field with values, default, omitted≡include, exclude=complement, both endpoints, 400 on invalid, Phase 132 DMF-11 citation. |
| `skills/cairo-genizah-research/references/api_contract.md` | `library_filter_mode` note in the filters section | VERIFIED | 2 occurrences at lines 54-61: field name alongside `library`, include/exclude semantics, both endpoints, 400 on invalid, Phase 132 DMF-11 citation. `library` description updated from "inclusion filter" to "inclusion or exclusion filter controlled by `library_filter_mode`". |

---

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `FiltersModel.library_filter_mode` | `SearchRequest.filters` AND `ParallelsRequest.filters` | Shared nested model (D1 — single field covers both endpoints) | WIRED | Both `SearchRequest.filters: Optional[FiltersModel]` and `ParallelsRequest.filters: Optional[FiltersModel]` reference the same class. One field addition covers both endpoints. Verified by `test_parallels_exclude_mode` passing. |
| `web.search_api._intersect_library_filter` | `shared.fjms_service.resolve_library_complement_sys_ids` | `run_in_executor` on exclude branch (lines 354-357), late-bound via `_fjms_module` attribute | WIRED | `mode = (filters_dict or {}).get('library_filter_mode') or 'include'`; when `mode == 'exclude'`: `await loop.run_in_executor(None, _fjms_module.resolve_library_complement_sys_ids, libs, meta_mgr)`. Both endpoints call `_intersect_library_filter` at lines 1060 and 1585 passing `filters_dict`. `test_intersect_helper_exclude_branch` confirms the branch routes to the complement helper and NOT the include helper — PASS. |
| `model_dump(exclude_none=True)` | backward-compat echo | Called at lines 1042 (`/api/search`) and 1572 (`/api/parallels`) | WIRED | `library_filter_mode` defaults to `None`; `exclude_none=True` drops it from `filters_dict` when omitted. `test_omit_mode_equals_include` asserts echo has NO `library_filter_mode` key — PASS. |

---

### Data-Flow Trace (Level 4)

Not applicable — this phase adds an API field (`library_filter_mode`) to a filtering path, not a dynamic-data rendering component. The data flow is: HTTP body → Pydantic model → `model_dump(exclude_none=True)` → `filters_dict` → `_intersect_library_filter` → `resolve_library_complement_sys_ids` (off event loop) → `restrict_sys_ids`. The complement helper itself is a deterministic set computation over `meta_mgr.csv_bank`. No hollow-prop or hollow-wiring patterns apply.

---

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| All 9 DMF-11 tests pass | `pytest tests/test_search_api_library_mode.py -v` | 9 passed in 0.43s | PASS |
| Backward-compat regression: existing search/library tests unaffected | `pytest tests/test_search_api_v2.py tests/test_parallels_api.py tests/test_parallels_library_filter.py -q` | 108+59 = 167 passed, 7 skipped | PASS |
| Docs guard unaffected | `pytest tests/test_search_api_docs.py -q` | 8 passed | PASS |
| Ruff clean on modified files | `ruff check web/search_api.py shared/fjms_service.py` | All checks passed | PASS |
| `library_filter_mode` field present in `FiltersModel` (not at top-level) | `grep -c "library_filter_mode" web/search_api.py` | 4 | PASS |
| Complement helper present in `shared/fjms_service.py` | `grep -c "def resolve_library_complement_sys_ids" shared/fjms_service.py` | 1 | PASS |

---

### Probe Execution

Not applicable — no `scripts/*/tests/probe-*.sh` files declared for this phase. Step 7c: SKIPPED (no probes declared; docs-only plan 03 + test-scaffold + API-field plans have no probe pattern).

---

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| DMF-11 | 132-01, 132-02, 132-03 | `POST /api/search` and `POST /api/parallels` accept optional library-filter mode (include/exclude); backward-compatible; exclude = complement intersected into `restrict_sys_ids`; documented in `docs/SEARCH_API.md` + `api_contract.md` | SATISFIED | All 4 success criteria verified above. DMF-11 marked `[x]` in `.planning/REQUIREMENTS.md:34` with status "Complete" in the phase mapping table. |

---

### Anti-Patterns Found

Scan of modified files (`web/search_api.py`, `shared/fjms_service.py`, `tests/test_search_api_library_mode.py`, `docs/SEARCH_API.md`, `skills/cairo-genizah-research/references/api_contract.md`):

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| (none) | — | No TBD/FIXME/XXX/TODO/HACK/PLACEHOLDER or stub patterns found in modified files | — | — |

No debt markers found. No stub implementations detected. The `_intersect_library_filter` exclude branch delegates to a real complement helper (not a `return set()` stub). The complement helper iterates `meta_mgr.csv_bank` with `not in excl_set` — real O(N) computation, not hardcoded empty.

---

### Human Verification Required

One item requires live-deployment verification. All automated checks pass; this item cannot be verified without the real corpus (`meta_mgr.csv_bank` with ~255K rows) and the deployed web endpoint.

#### 1. Live Production Smoke Test — `mode=exclude`

**Test:** After v8.4.1 deploys to `genizahsearch.com`, run:
```bash
curl -s -X POST https://genizahsearch.com/api/search \
  -H "Content-Type: application/json" \
  -d '{"query": "Torah", "search_mode": "exact", "limit": 5, "filters": {"library": ["CUL"], "library_filter_mode": "exclude"}}' \
  | python -m json.tool
```
**Expected:** HTTP 200; all items in `results[].display.library_code` are NOT `"CUL"` (e.g., JTS, Oxford, RNL, BL, etc.).
**Why human:** Automated tests use a `StubSearcher` with a 4-row `csv_bank` mock. The actual corpus has ~255K rows. This smoke confirms `resolve_library_complement_sys_ids` produces a non-empty real complement and it flows through the full request-response pipeline on deployed infrastructure.

**Note:** The VALIDATION.md also records this as the sole manual-only verification item for DMF-11.

---

### Gaps Summary

No gaps. All four success criteria are code-verified and all 9 test-pinned behaviors pass. Human verification is gated on deployment (`v8.4.1`) which is outside this phase's scope.

**One external review is noted (not a code gap):** The Codex cross-AI code-diff review (mentioned in the RESEARCH/planning cycle as deferred due to offline connectivity at execution time) has not yet been conducted. The Codex R1 pre-flight feedback was already incorporated into the implementation (commit `1f7ca3a0` — `default=None` backward-compat + doc error-code fix). No further code gaps were found in manual verification. If the project convention requires a final Codex code-diff review before closing, schedule it against commits `ae232d49 dc0c5033 4af733de 031e9f8d`.

---

_Verified: 2026-07-01T14:00:00Z_
_Verifier: Claude (gsd-verifier)_
