---
phase: 80-api-parallels
plan: 01
subsystem: api
tags: [api, parallels, error-codes, foundations]
requires: []
provides:
  - "shared.api_errors.ERROR_CODES['composition_required']"
  - "shared.api_errors.ERROR_CODES['composition_too_long']"
  - "shared.api_errors.WARNING_CODES['truncated_to_200']"
  - "CLAUDE.md env-var documentation noting /api/parallels"
affects:
  - shared/api_errors.py
  - CLAUDE.md
tech-stack:
  added: []
  patterns:
    - "Frozenset taxonomy extension following Phase 79 (/api/browse) precedent"
key-files:
  created: []
  modified:
    - shared/api_errors.py
    - CLAUDE.md
decisions:
  - "Inline two ERROR_CODES additions inside the existing frozenset block, after the Phase 79 entries, with `# Phase 80 (/api/parallels) additions:` comment header (mirrors Phase 79 pattern)."
  - "Restructure WARNING_CODES from inline single-element frozenset to multi-line form so the new entry has a proper home and a comment header — preserves the existing `# Surfaced in top-level warnings: []` block comment above."
  - "CLAUDE.md edit is purely documentation: append `; applies to /api/search, /api/browse, /api/parallels` to MODE and POSTHOG_SAMPLE_N lines; for RATE_LIMIT add explicit independent-bucket note pointing to D-05."
  - "No new env vars introduced. Phase 80 reuses Phase 78's three knobs verbatim per CONTEXT D-05/D-09."
metrics:
  duration: "~5 min"
  completed: "2026-05-01"
  tasks_completed: 2
  files_modified: 2
---

# Phase 80 Plan 01: Foundations (api_errors + CLAUDE.md docs) Summary

Extended `shared/api_errors.py` with 2 new ERROR_CODES (`composition_required`, `composition_too_long`) and 1 new WARNING_CODES entry (`truncated_to_200`) per CONTEXT D-06/D-07; updated `CLAUDE.md` Environment Variables block so SEARCH_API_MODE / SEARCH_API_RATE_LIMIT / SEARCH_API_POSTHOG_SAMPLE_N explicitly list `/api/parallels` alongside `/api/search` and `/api/browse`.

## What Was Built

### Task 1: api_errors.py taxonomy extension
- `ERROR_CODES` frozenset: 15 → 17 entries. New entries `composition_required` and `composition_too_long` with inline comments referencing D-06.
- `WARNING_CODES` frozenset: 1 → 2 entries. Restructured from inline single-element form to multi-line form to add `truncated_to_200` with D-07 reference; existing block comment `# Surfaced in top-level warnings: []` preserved.
- All 15 prior ERROR_CODES (Phase 78 + Phase 79) intact.
- `query_downgraded` warning code intact.
- APIError construction with new codes does NOT trigger the `unknown code` logger.warning branch — confirmed by automated verify.

### Task 2: CLAUDE.md env-var documentation
- `SEARCH_API_MODE` line: appended `; applies to /api/search, /api/browse, /api/parallels`.
- `SEARCH_API_RATE_LIMIT` line: appended `; SHARED ceiling across /api/search, /api/browse, /api/parallels but each endpoint has its own independent bucket — see Phase 80 D-05`.
- `SEARCH_API_POSTHOG_SAMPLE_N` line: appended `; applies to /api/search, /api/browse, /api/parallels`.
- All three Phase 79 browse-specific env vars (`SEARCH_API_BROWSE_TIMEOUT`, `SEARCH_API_BROWSE_CORE_TIMEOUT`, `SEARCH_API_BROWSE_TEXT_CAP`) preserved unchanged.
- No new `SEARCH_API_PARALLELS_*` env var introduced (verified via grep).

## Verification

- Automated verify block from Task 1 prints `OK`: ERROR_CODES count=17, WARNING_CODES count=2, all expected entries present, APIError('composition_too_long', ...) emits no unknown-code warning.
- Automated verify block from Task 2 prints `OK`: all three env-var lines mention `/api/parallels`; rate-limit line includes D-05/independent-bucket clarification; no `SEARCH_API_PARALLELS_*` introduced.
- Phase 77/78/79 baseline regression suite (`tests/test_search_api.py tests/test_api_hardening.py tests/test_api_legacy_unchanged.py tests/test_browse_api.py tests/test_search_serializer.py`): **146 passed / 1 skipped**, GREEN.
- `python -X utf8 scripts/check_docs.py`: **All checks passed! Documentation is healthy.**

## Deviations from Plan

None — plan executed exactly as written. The check_docs.py invocation required `python -X utf8` flag instead of `PYTHONIOENCODING=utf-8` env var (PowerShell session quirk on Windows; the plan acknowledged the Phase 79 cp1255 workaround). Same effect, no behavior change.

## Commits

- `7194590e feat(80-01): add parallels error/warning codes to api_errors`
- `76f30186 docs(80-01): note /api/parallels shares mode/rate/posthog env vars`

## Downstream Impact

Plans 02, 03, 04 can now safely:
- `from shared.api_errors import APIError`
- `raise APIError('composition_required', ...)` / `raise APIError('composition_too_long', ...)` without triggering the unknown-code `logger.warning` branch.
- Append `'truncated_to_200'` to envelope `warnings[]` array.

Phase 81 skill consumer can branch on the new `code` strings as part of the public API contract.

## Self-Check: PASSED

- shared/api_errors.py modified: confirmed (commit 7194590e)
- CLAUDE.md modified: confirmed (commit 76f30186)
- Both commits present in git log.
- Phase 78/79 test baselines intact (146 passed / 1 skipped).
