---
phase: 82
plan: 01
subsystem: internal-documentation
tags: [audit, api-contract, search-helper, v7.10]
requires:
  - .planning/phases/78-api-search-hardening-shell/78-04-SUMMARY.md
  - .planning/phases/79-api-browse-drill-down/79-03-SUMMARY.md
  - .planning/phases/80-api-parallels/80-03-SUMMARY.md
  - .planning/phases/81A-api-contract-expansion/81A-01-SUMMARY.md
  - .planning/phases/81A-api-contract-expansion/81A-02-SUMMARY.md
  - .planning/phases/81B-claude-skill-consumer/81B-04-SUMMARY.md
  - web/search_api.py
  - shared/api_errors.py
  - skills/cairo-genizah-research/references/api_contract.md
provides:
  - .planning/phases/82-internal-documentation/82-CONTRACT-AUDIT.md
affects:
  - .planning/phases/82-internal-documentation/82-02-PLAN.md (consumer — writes docs/SEARCH_API.md from this audit)
  - .planning/phases/82-internal-documentation/82-03-PLAN.md (consumer — adds GENIZAH_* env vars to CLAUDE.md per §11.3)
tech-stack:
  added: []
  patterns:
    - line-cited contract audit (single sheet of truth for documentation writers)
key-files:
  created:
    - .planning/phases/82-internal-documentation/82-CONTRACT-AUDIT.md
  modified: []
decisions:
  - "Confirmed all 7 server-side env vars (SEARCH_API_*, POSTHOG_IP_SALT) already present in CLAUDE.md L137-L151 — Plan 03 only needs to add the 2 skill-side vars (GENIZAH_API_BASE, GENIZAH_SKILL_REQ_PER_MIN)."
  - "Recorded the /api/parallels.mode vs /api/search.search_mode naming inconsistency in §6.1 with explicit citation of Phase 81A D-07 as the source of the 'rename deferred to v7.11' decision."
  - "Catalogued 18 ERROR_CODES against shared/api_errors.py:L24-L45 — no missing codes found beyond the 12 the plan minimum-required."
metrics:
  duration: ~25 minutes
  completed: 2026-05-05
  tasks: 1
  files_changed: 1
---

# Phase 82 Plan 01: Search-Helper Contract Audit Summary

Internal contract audit (`.planning/phases/82-internal-documentation/82-CONTRACT-AUDIT.md`, 516 lines, 16 H2 sections) capturing the as-shipped v7.10 surface of POST /api/search, GET /api/browse, and POST /api/parallels — request/response shapes, error codes, warnings, env vars, rate-limit topology, and the parallels-mode-vs-search_mode naming inconsistency — with explicit line-number citations into web/search_api.py and shared/api_errors.py so Plan 02 can write the user-facing docs/SEARCH_API.md without re-reading source.

## Tasks Completed

| Task | Name | Commit | Files |
| ---- | ---- | ------ | ----- |
| 1 | Read live source + write 82-CONTRACT-AUDIT.md | 025f49f1 | .planning/phases/82-internal-documentation/82-CONTRACT-AUDIT.md |

## What Was Built

A single 516-line internal scratch doc with the following section structure (verbatim per plan spec):

1. Endpoints Inventory — table of three handlers with rate-limiter instances and source-file lines.
2. POST /api/search — Request — SearchRequest + ResponsaOptions + FiltersModel field tables; cross-field rejection matrix (responsa_options coupling, gap/metadata coupling, regex-not-in-enum, old-mode-field hint per 81A D-13).
3. POST /api/search — Response — envelope skeleton, per-item shape, **7-key request echo** with the `responsa_options` vs `responsa_options_effective` cascade nuance documented.
4. GET /api/browse — Request — three locator resolution paths (uid alone / sys_id+p_num+volume_ie / sys_id+fl_id), text_cap bounds + env fallback, locator_conflict raise conditions, post-resolution uid verification (D-03b).
5. GET /api/browse — Response — locked envelope shape, `text_source` enum (pgp_transcription | snippet | none), browse-emitted warnings (volume_ie_defaulted, enrichment_timeout, enrichment_failed), R-PR-01/D-14 (image URLs are best-effort, no upstream probing).
6. POST /api/parallels — Request — ParallelsRequest field table including a dedicated **§6.1 "Naming inconsistency: `mode` vs `search_mode`"** subsection citing Phase 81A D-07.
7. POST /api/parallels — Response — envelope including the always-present `filtered: []` (Phase 80 D-04) and **6-key request echo** with explicit note that `search_mode`, `gap`, and `responsa_options` are absent.
8. Error Envelope — uniform `{"error": {"code", "message"}}` shape; HTTP 429 carries Retry-After.
9. Error Code Catalogue — 18 codes from shared/api_errors.py:L24-L45 with HTTP status, raise condition, originating phase, and source citation.
10. Warnings Vocabulary — 5 warning codes (query_downgraded, volume_ie_defaulted, enrichment_timeout, enrichment_failed, truncated_to_200) with emission sites; clarifies HARDEN-03 (warnings are top-level, never per-item).
11. Environment Variables — three sub-sections: server-side (already in CLAUDE.md), skill-side (NOT in CLAUDE.md), and explicit DOC-02 delta list for Plan 03.
12. Rate Limiting Architecture — three independent buckets sharing `SEARCH_API_RATE_LIMIT` ceiling.
13. Statelessness Contract — handlers have ZERO references to state.last_results / app.storage / request.cookies.
14. Mode Gate — SEARCH_API_MODE values + scope clarification (only the three search-helper endpoints; legacy /api/* unaffected).
15. Drill-Down Locator Round-Trip — how to feed search/parallels result locators verbatim into /api/browse.
16. Source-File Reference Index — quick-lookup table for Plan 02 citation hygiene.

## Verification

All 21 acceptance grep checks PASS:

| Check | Required | Actual |
| --- | --- | --- |
| File exists | yes | yes |
| Line count | ≥ 200 | 516 |
| H2 section count | ≥ 16 | 16 |
| `search_mode` count | ≥ 5 | 13 |
| `responsa_options` count | ≥ 5 | 9 |
| `responsa_options_effective` count | ≥ 2 | 3 |
| `pgp_transcription` count | ≥ 1 | 2 |
| `locator_conflict` count | ≥ 1 | 3 |
| `manuscript_page_not_found` count | ≥ 1 | 2 |
| `core_timeout` count | ≥ 1 | 1 |
| `invalid_combination` count | ≥ 1 | 4 |
| `filter_vocabulary_unavailable` count | ≥ 1 | 1 |
| `rate_limited` count | ≥ 1 | 2 |
| `GENIZAH_SKILL_REQ_PER_MIN` count | ≥ 1 | 3 |
| `GENIZAH_API_BASE` count | ≥ 1 | 3 |
| `SEARCH_API_MODE` count | ≥ 1 | 4 |
| `SEARCH_API_RATE_LIMIT` count | ≥ 1 | 4 |
| `Naming inconsistency` count | ≥ 1 | 1 |
| `web/search_api.py` references | ≥ 5 | 93 |
| `shared/api_errors.py` references | ≥ 1 | 27 |
| `D-07` count | ≥ 1 | 4 |

## Decisions Made

- **No server-side env-var additions needed for Plan 03.** All seven (SEARCH_API_MODE, SEARCH_API_RATE_LIMIT, POSTHOG_IP_SALT, SEARCH_API_POSTHOG_SAMPLE_N, SEARCH_API_BROWSE_TIMEOUT, SEARCH_API_BROWSE_CORE_TIMEOUT, SEARCH_API_BROWSE_TEXT_CAP) already present in CLAUDE.md L137-L151. Plan 03's CLAUDE.md edit is reduced to adding only `GENIZAH_API_BASE` and `GENIZAH_SKILL_REQ_PER_MIN` — both skill-side, both currently undocumented in CLAUDE.md.
- **`/api/parallels.mode` rename to `search_mode` is intentionally deferred** per Phase 81A D-07. Audit §6.1 records this as a deliberate v7.10 contract feature with the rename slated for v7.11. The two enum value spaces are also semantically disjoint (parallels: exact|variants|fuzzy; search: exact|variants|responsa|title|shelfmark) so consumers cannot blindly assume field-name equivalence.
- **18 ERROR_CODES catalogued in §9** — full enumeration of `shared/api_errors.py:L24-L45`, exceeding the plan's minimum-12. No missing codes were found beyond what the plan listed.

## Deviations from Plan

None — plan executed exactly as written. The plan's `<read_first>` enumeration was sufficient; no deviation rules (1-4) triggered.

## Known Stubs

None. Audit doc is content-complete and standalone.

## Self-Check: PASSED

- File `.planning/phases/82-internal-documentation/82-CONTRACT-AUDIT.md` exists (516 lines).
- Commit `025f49f1` exists in `git log` on the worktree branch.
- All 21 acceptance grep checks pass (per Verification table above).
