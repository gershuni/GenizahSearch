---
phase: 83-public-release
plan: 02
subsystem: docs
tags: [public-api, documentation, reframe]
requires:
  - 83-01 (Wave 0 RED tests for docs reframe)
provides:
  - Public-facing SEARCH_API.md (Stability + Quick Start + Attribution + Changelog)
  - GREEN status for 5 of 8 test_search_api_docs.py assertions
affects:
  - docs/SEARCH_API.md
tech-stack:
  added: []
  patterns:
    - "Verbatim D-02 stability statement in public contract docs"
key-files:
  created: []
  modified:
    - docs/SEARCH_API.md
decisions:
  - "Removed contradictory 'What This API Is NOT' bullets that asserted 'Not a public API' — replaced with scope-clarifying bullets (no auth keys, read-only, no bulk export, no async jobs) consistent with new public framing (Rule 1 deviation: contradiction with newly added Stability section)"
metrics:
  duration: 10m
  completed: 2026-05-05
  tasks: 1
  files: 1
  commits: 1
---

# Phase 83 Plan 02: Public API Docs Reframe Summary

Reframed `docs/SEARCH_API.md` from internal-helper-only to public research-automation API: removed the warning banner, added verbatim D-02 stability statement, added Quick Start with runnable curl + JSON shape excerpts, added Attribution & Citation crediting MiDRASH/PGP/FJMS/NLI, added Changelog seeded with v7.10 entry. All 663 lines of locked contract material (endpoint shapes, error codes, warnings, env vars, statelessness contract, locator round-trip) preserved intact.

## What Changed

- **Header → Stability section** (new): Verbatim D-02 statement — "We aim to keep this contract stable. Breaking changes ... only ship on major website-version releases ... announced in `CHANGELOG.md` and the Changelog section below. Additive changes ... may ship at any time." Plus links to `/api/docs` (Swagger) and `/api/openapi.json`.
- **Quick Start section** (new): Three runnable `curl` examples (search/browse/parallels) each followed by a JSON shape excerpt showing `schema_version`, `request`, and `results`/result payload. Trailing error-envelope example showing `{"error": {"code": "...", "message": "..."}}` shape.
- **Attribution & Citation section** (new): Credits MiDRASH (Zenodo DOI), Princeton Geniza Project, Friedberg JMS/FGP, NLI; links to `../README.md#credits--data` for full credits. Single `../` (Codex HIGH fix).
- **Overview paragraph** (rewritten): Dropped "consumed primarily by the cairo-genizah-research Claude skill, by the deployment soak/smoke harness, and by occasional maintainer scripts" → "let a research consumer execute ... A reference consumer is the cairo-genizah-research Claude skill, which demonstrates the full search → browse → rank workflow." Link uses `../skills/cairo-genizah-research/SKILL.md` (single `../`).
- **"What This API Is NOT" section** (rewritten — Rule 1 deviation): Original bullets explicitly contradicted the new Stability section ("Not a public API. No keys, no SLAs, no semver guarantees... External usage is not invited."). Replaced with scope-clarifying bullets that reflect actual v7.10 posture: not authenticated (today), not browse-page parity, not a write API, not a bulk-export interface, not a long-running job runner.
- **Changelog section** (new, file-bottom): `### v7.10 (2026-05-05) — Initial public release` with bullets enumerating: endpoint promotion, OpenAPI/Swagger, stability commitment, attribution section. Notes that future breaking changes will be announced in `CHANGELOG.md`.
- **Banner removed**: `## ⚠ Internal Helper — No Stability Promise` and the follow-up "prefer the skill" paragraph deleted.

## Preserved Contract Sections (verified post-edit)

All 82-CONTRACT-AUDIT.md-locked sections survive byte-for-byte:

| Section | Line | Status |
|---------|------|--------|
| `## Endpoint: POST /api/search` | 135 | preserved |
| `## Endpoint: GET /api/browse` | 343 | preserved |
| `## Endpoint: POST /api/parallels` | 446 | preserved |
| `## Naming Inconsistency: parallels.mode vs search.search_mode` | 554 | preserved |
| `## Drill-Down Locator Round-Trip` | 567 | preserved |
| `## Error Envelope` | 605 | preserved |
| `## Error Codes` | 629 | preserved |
| `## Warnings Array` | 657 | preserved |
| `## Environment Variables` | 694 | preserved |
| `## Rate Limiting & Buckets` | 714 | preserved (mode gate documented within) |
| `## Statelessness Contract` | 741 | preserved |
| `## See Also` | 757 | preserved |

## Verification

```
$ pytest tests/test_search_api_docs.py::test_stability_statement_present \
                tests/test_search_api_docs.py::test_no_internal_helper_banner \
                tests/test_search_api_docs.py::test_quick_start_section_present \
                tests/test_search_api_docs.py::test_attribution_section_present \
                tests/test_search_api_docs.py::test_changelog_section_present -v

5 passed in 0.04s
```

Acceptance-criteria greps (all pass):
- `Internal Helper`: 0 ✅
- `We aim to keep this contract stable`: 1 ✅
- `## Quick Start`: 1 ✅
- `## Attribution`: 1 ✅
- `## Changelog`: 1 ✅
- `../../` (no double-dot-dot): 0 ✅ (Codex HIGH fix)
- `../README.md`: 1 ✅
- `schema_version`: 8 (≥4 required) ✅
- `## Endpoint: POST /api/search`: 1 ✅
- `## Error Codes`: 1 ✅ (file uses this name; "## Error Code Catalogue" in plan was a wording inconsistency — Quick Start anchor link updated to match)
- File length: 776 lines (>700 required) ✅

Note: the remaining 3 RED tests in `test_search_api_docs.py` (`test_readme_has_api_section`, `test_readme_api_links_to_search_api_md`, `test_skill_md_references_public_docs`) intentionally remain RED — they belong to Plan 04 (README + skill update).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Rewrote "What This API Is NOT" section**
- **Found during:** Task 1 final review (post-edit consistency check)
- **Issue:** The original section contained five bullets directly contradicting the newly added Stability section: "Not a public API. No keys, no SLAs, no semver guarantees... Internal helper for v7.10 first-party tooling; the contract may change without warning... Not linked from `README.md`... External usage is not invited..." Leaving it would have made the document internally inconsistent — a reader would see both "We aim to keep this contract stable" and "Not a public API. No semver guarantees" within the same file.
- **Fix:** Rewrote bullets to reflect actual v7.10 posture (anonymous + rate-limited; no write/bulk-export/async-job APIs). The section now scopes the API rather than disclaiming it.
- **Files modified:** docs/SEARCH_API.md (lines 749-755 in final file)
- **Commit:** a67cc5dd

**2. [Rule 1 - Wording] Quick Start anchor link target**
- **Found during:** Task 1 (writing the Quick Start error-block)
- **Issue:** The plan's prescribed Quick Start text linked to `[Error Code Catalogue](#error-code-catalogue)`, but the existing preserved section is named `## Error Codes` (matching what's actually in the file — the plan's "Error Code Catalogue" was a wording mismatch with the contract).
- **Fix:** Quick Start link updated to `[Error Codes](#error-codes)` so the in-doc anchor actually resolves.
- **Files modified:** docs/SEARCH_API.md (Quick Start error-response paragraph)
- **Commit:** a67cc5dd

No architectural deviations. No checkpoints hit.

## Threat Flags

None — no new endpoints, no schema changes, no auth surface introduced. Edit is documentation-only and preserves all locked contract material.

## Self-Check: PASSED

Verified post-write:
- `docs/SEARCH_API.md` exists and contains all 5 new/edited sections (greps in Verification block above all return expected counts).
- Commit `a67cc5dd` exists in `git log --oneline` and matches commit message format `docs(83-02): ...`.
- All 5 targeted pytest assertions GREEN.
- No `../../` patterns remain (Codex HIGH concern resolved).
- Contract-audit-locked sections all present.
