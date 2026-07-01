---
phase: 132-public-api-dual-mode-api-search-api-parallels
plan: "03"
subsystem: public-api
tags: [docs, library-filter, api, dmf-11]
dependency_graph:
  requires:
    - phase: 132-02
      provides: FiltersModel.library_filter_mode field (shipped; now documented)
  provides:
    - docs/SEARCH_API.md with library_filter_mode documented
    - skills/cairo-genizah-research/references/api_contract.md with library_filter_mode documented
  affects:
    - docs/SEARCH_API.md
    - skills/cairo-genizah-research/references/api_contract.md
tech-stack:
  added: []
  patterns:
    - "Additive doc row in request-fields table using existing pipe-separated sub-key style"
key-files:
  created: []
  modified:
    - docs/SEARCH_API.md
    - skills/cairo-genizah-research/references/api_contract.md
key-decisions:
  - "Document the behavioral default (include) not the Pydantic internal (None) — callers see include behavior when they omit the field, that is what the doc says"
  - "unknown_filter_key Error Codes table entry clarified: the code is reserved in ERROR_CODES but Pydantic extra=forbid fires first and returns invalid_request — not a code removal, a clarification"
  - "filters row in SEARCH_API.md kept as a single table cell (same style as before); library_filter_mode inlined as a sub-key note rather than a separate table row to match the existing library/domains/authors/works pattern"
requirements-completed: [DMF-11]
duration: 8min
completed: 2026-07-01
---

# Phase 132 Plan 03: Wave 3 Docs Summary

**Document `filters.library_filter_mode` on both public API surfaces so callers know the field exists, defaults to `include`, and that `exclude` = complement. Fixes stale `unknown_filter_key` wording to `invalid_request`.**

## Performance

- **Duration:** 8 min
- **Started:** 2026-07-01T13:10:00Z
- **Completed:** 2026-07-01T13:18:00Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments

- `docs/SEARCH_API.md` request-fields table `filters` row updated: `library_filter_mode` documented as optional enum `include` (default, omitted-equivalent) | `exclude` (complement — manuscripts NOT in the set), intersected BEFORE the result cap, applying to both `POST /api/search` and `POST /api/parallels`, with 400 `invalid_request` on invalid value (Phase 132 DMF-11)
- `docs/SEARCH_API.md` Error Codes table: stale `unknown_filter_key` description corrected — the code is reserved in `ERROR_CODES` for contractual stability, but structural unknown-key validation fires via Pydantic `extra='forbid'` and returns `invalid_request` (Codex R1 LOW, verified against `web/api_hardening.py:306-326` and `tests/test_search_api.py::test_unknown_filter_key_returns_invalid_request`)
- `skills/cairo-genizah-research/references/api_contract.md` `filters` bullet updated: `library_filter_mode` added alongside `library`, with consistent wording (include default, exclude complement, both endpoints, 400 on invalid); `library` description updated from "inclusion filter" to "inclusion or exclusion filter controlled by `library_filter_mode`"
- `test_search_api_docs.py` — 8/8 GREEN (unaffected); `check_docs.py` — no new issues

## Task Commits

1. **Task 1: Document library_filter_mode in docs/SEARCH_API.md** — `4af733de` (docs)
2. **Task 2: Document library_filter_mode in skill api_contract.md** — `031e9f8d` (docs)

## Files Created/Modified

- `docs/SEARCH_API.md` — filters row updated (library_filter_mode sub-key added; unknown_filter_key wording corrected in Error Codes table)
- `skills/cairo-genizah-research/references/api_contract.md` — filters bullet updated (library_filter_mode added; library description updated)

## Decisions Made

- Document the behavioral default (`include`) rather than the Pydantic internal (`None`): callers observe `include` behavior when the field is omitted; the internal `None→'include'` normalization is an implementation detail.
- `unknown_filter_key` in Error Codes table: clarified rather than removed. The code exists in `ERROR_CODES` (public, contractual); but Pydantic fires first for structural unknown-key cases. The table entry now accurately reflects this distinction.
- `library_filter_mode` inlined as a sub-key in the `filters` row (not a separate table row), matching the existing style for `library`, `domains`, `authors`, etc.

## Deviations from Plan

None — plan executed exactly as written.

## Issues Encountered

None.

## Known Stubs

None. Both doc surfaces are fully updated.

## Threat Flags

None — docs-only plan; no new network endpoints, auth paths, or trust boundaries.

## Self-Check: PASSED

- [x] `grep -c "library_filter_mode" docs/SEARCH_API.md` == 1 (FOUND)
- [x] `grep -c "library_filter_mode" skills/cairo-genizah-research/references/api_contract.md` == 2 (FOUND)
- [x] `python -m pytest tests/test_search_api_docs.py -x` — 8 passed
- [x] `PYTHONUTF8=1 python scripts/check_docs.py` — no new blocking issues
- [x] Commit `4af733de` exists (Task 1)
- [x] Commit `031e9f8d` exists (Task 2)
- [x] unknown_filter_key Error Codes entry corrected (Codex R1 LOW resolved)
- [x] Both doc surfaces agree: default `include`, omitted≡include, `exclude`=complement, both endpoints, 400 on invalid

---
*Phase: 132-public-api-dual-mode-api-search-api-parallels*
*Completed: 2026-07-01*
