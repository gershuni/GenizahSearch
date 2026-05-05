---
phase: 82
plan: 02
subsystem: internal-documentation
tags: [documentation, api-contract, search-helper, v7.10, DOC-01]
requires:
  - .planning/phases/82-internal-documentation/82-CONTRACT-AUDIT.md
  - .planning/phases/82-internal-documentation/82-01-SUMMARY.md
  - skills/cairo-genizah-research/references/api_contract.md
  - web/search_api.py
  - shared/api_errors.py
provides:
  - docs/SEARCH_API.md
affects:
  - .planning/phases/82-internal-documentation/82-04-PLAN.md (acceptance walkthrough reads this file)
tech-stack:
  added: []
  patterns:
    - internal-only API documentation page (NOT linked from README.md or public site)
    - audit-doc-as-single-source-of-truth authoring (Plan 02 reads only the Plan 01 audit, not the live source)
key-files:
  created:
    - docs/SEARCH_API.md
  modified: []
decisions:
  - "Wrote 15 H2 sections (1 above the plan minimum of 14) — kept 'Rate Limiting & Buckets' and 'Statelessness Contract' as separate top-level sections rather than nesting under Environment Variables, matching the audit's structure and giving each its own anchor for cross-linking from Plan 04's acceptance walkthrough."
  - "Disclaimer block placed immediately after the H1+date as the very first H2, with the literal phrase 'no stability promise' in the heading text — guarantees the (case-insensitive) disclaimer phrase appears in the file regardless of body copy."
  - "DOCUMENTATION_INDEX.md was deliberately NOT modified — the plan marks the index entry as OPTIONAL and the file is internal-only; skipping the index entry keeps the page out of any indirect public exposure path."
metrics:
  duration: ~30 minutes
  completed: 2026-05-05
  tasks: 1
  files_changed: 1
---

# Phase 82 Plan 02: docs/SEARCH_API.md Summary

Single 628-line internal documentation page (`docs/SEARCH_API.md`) capturing the as-shipped v7.10 search-helper API surface — request/response shapes for POST /api/search, GET /api/browse, and POST /api/parallels, error envelope and 18-code catalogue, top-level warnings vocabulary, environment variables, rate-limit topology, the parallels.mode-vs-search.search_mode intentional naming inconsistency, and a worked drill-down locator round-trip example — authored entirely from Plan 01's `82-CONTRACT-AUDIT.md` audit doc without re-reading the live source.

## Tasks Completed

| Task | Name | Commit | Files |
| ---- | ---- | ------ | ----- |
| 1 | Author docs/SEARCH_API.md from the audit doc | 183093e0 | docs/SEARCH_API.md |

## What Was Built

`docs/SEARCH_API.md` — 628 lines, 15 H2 sections, 10 fenced JSON code blocks. Section structure (in order):

1. `## ⚠ Internal Helper — No Stability Promise` — disclaimer block (literal "no stability promise" phrase appears in the heading itself).
2. `## Overview` — three endpoints, who consumes them, shared hardening shell.
3. `## Endpoint: POST /api/search` — non-Responsa + Responsa request examples, field tables (`SearchRequest`, `responsa_options`, cross-field rejections), full response example, response-item field table, **7-key request echo with worked Responsa cascade case showing `responsa_options.ja=true → responsa_options_effective.ja=false` divergence and accompanying `query_downgraded` warning**.
4. `## Endpoint: GET /api/browse` — three resolution paths (uid alone / sys_id+p_num+volume_ie / sys_id+fl_id), `?text_cap=N` bounds, locator-conflict raise table, full response example, top-level field table, `text_source` enum verbatim (`pgp_transcription | snippet | none`), best-effort image URL contract (R-PR-01 / D-14, no upstream probing), per-source enrichment failure modes.
5. `## Endpoint: POST /api/parallels` — request example + field table including `mode` (NOT `search_mode`), full response example, always-present `filtered: []` (Phase 80 D-04), 200-group cap with `truncated_to_200` warning, **6-key request echo with explicit note that `search_mode`, `gap`, `responsa_options` are NOT echoed**.
6. `## Naming Inconsistency: parallels.mode vs search.search_mode` — dedicated H2 explicitly citing Phase 81A D-07 as the source of the v7.10 debt and noting the disjoint enum value spaces.
7. `## Drill-Down Locator Round-Trip` — worked example pasting a `/api/search` result locator into a `/api/browse?uid=...` request.
8. `## Error Envelope` — uniform `{"error": {"code", "message"}}` shape; HTTP 429 carries `Retry-After`; envelope applies ONLY to the three search-helper endpoints.
9. `## Error Codes` — full 18-code table from `shared/api_errors.py:L24-L45` with HTTP status and typical raise condition.
10. `## Warnings Array` — top-level only (HARDEN-03), 5-code vocabulary table, worked Responsa cascade case showing both signal channels (warnings entry + echo divergence) emitted together.
11. `## Environment Variables` — 9-row table covering 7 server-side vars + 2 skill-side vars (GENIZAH_API_BASE, GENIZAH_SKILL_REQ_PER_MIN).
12. `## Rate Limiting & Buckets` — three independent buckets, ~3× per-IP allowance for cross-endpoint workloads, mode-gate semantics.
13. `## Statelessness Contract` — ZERO references to `state.last_results` / `app.storage` / `request.cookies`.
14. `## What This API Is NOT` — 5-bullet anti-misuse list.
15. `## See Also` — relative-link cross-references to skill api_contract.md, CLAUDE.md, web/search_api.py, shared/api_errors.py, shared/search_serializer.py.

## Verification

All 33 acceptance grep checks PASS:

| Check | Required | Actual |
| --- | --- | --- |
| File exists | yes | yes |
| Line count | ≥ 250 | 628 |
| `grep -ic "no stability promise"` | ≥ 1 | 1 |
| `grep -c "internal"` | ≥ 3 | 4 |
| `grep -c "/api/search"` | ≥ 3 | 12 |
| `grep -c "/api/browse"` | ≥ 3 | 14 |
| `grep -c "/api/parallels"` | ≥ 3 | 10 |
| `grep -c "search_mode"` | ≥ 5 | 24 |
| `grep -c "responsa_options"` | ≥ 3 | 22 |
| `grep -c "responsa_options_effective"` | ≥ 2 | 7 |
| `grep -c "pgp_transcription"` | ≥ 1 | 3 |
| `grep -c "text_source"` | ≥ 2 | 3 |
| `grep -c "locator_conflict"` | ≥ 1 | 5 |
| `grep -c "manuscript_page_not_found"` | ≥ 1 | 3 |
| `grep -c "core_timeout"` | ≥ 1 | 2 |
| `grep -c "invalid_combination"` | ≥ 1 | 4 |
| `grep -c "filter_vocabulary_unavailable"` | ≥ 1 | 1 |
| `grep -c "rate_limited"` | ≥ 1 | 2 |
| `grep -c "Retry-After"` | ≥ 1 | 3 |
| `grep -c "SEARCH_API_MODE"` | ≥ 1 | 5 |
| `grep -c "SEARCH_API_RATE_LIMIT"` | ≥ 1 | 4 |
| `grep -c "SEARCH_API_BROWSE_TIMEOUT"` | ≥ 1 | 2 |
| `grep -c "GENIZAH_SKILL_REQ_PER_MIN"` | ≥ 1 | 1 |
| `grep -c "GENIZAH_API_BASE"` | ≥ 1 | 1 |
| naming-inconsistency pattern (case-insensitive) | ≥ 1 | 14 |
| `grep -c "^## "` (H2 sections) | ≥ 13 | 15 |
| `grep -c "^# "` (H1 title) | ≥ 1 | 1 |
| `grep -c "warnings"` | ≥ 3 | 11 |
| `grep -c "filtered"` | ≥ 1 | 5 |
| `grep -c "limit_effective"` | ≥ 1 | 6 |
| `grep -c "image_unavailable\|head_probe"` | == 0 | 0 |
| README.md links to SEARCH_API.md | == 0 | 0 |
| JSON fenced code blocks | ≥ 3 | 10 |

Plan's `<automated>` Python verification one-liner also passed (single combined assert chain).

## Decisions Made

- **15 H2 sections shipped (plan minimum 13, plan-spec lists 14).** Kept "Rate Limiting & Buckets" and "Statelessness Contract" as separate top-level sections rather than nesting under Environment Variables — matches the audit's section split (audit §12 + §13) and gives each topic its own anchor for cross-linking from the Plan 04 walkthrough.
- **Disclaimer phrase 'no stability promise' is in the heading itself.** Belt-and-suspenders approach: even if body copy is later edited, the heading text guarantees the case-insensitive grep check stays green.
- **DOCUMENTATION_INDEX.md was NOT modified.** The plan marks the index entry as OPTIONAL ("if you skip it, that's also acceptable per DOC-01"). Skipping it keeps the file off any indirect public exposure path; if a future maintainer wants the index entry, it's a one-line PR.

## Deviations from Plan

None — plan executed exactly as written. The audit doc was sufficient as the sole input; no need to consult `web/search_api.py` directly or invoke deviation rules 1-4. JSON examples were syntax-checked by mental review (each block is valid JSON literal: balanced braces, correct quoting, proper comma placement, no trailing commas).

## Authentication Gates

None — pure documentation work, no external calls or auth required.

## Known Stubs

None. The page is content-complete and standalone: a reader unfamiliar with v7.10 can use it to construct a valid `/api/search` request, read the response, copy the locator into `/api/browse`, predict the error envelope from sending `responsa_options` with `search_mode: "exact"` (400 `invalid_combination`), and find the env vars to flip on the server. This satisfies the SC3 falsification test for the phase.

## Self-Check: PASSED

- File `docs/SEARCH_API.md` exists (628 lines).
- Commit `183093e0` exists in `git log` on the worktree branch (verified via `git rev-parse --short HEAD`).
- All 33 acceptance grep checks pass (per Verification table above).
- Plan's `<automated>` Python verification one-liner passes (`OK` printed).
- README.md does NOT link to SEARCH_API.md (verified).
