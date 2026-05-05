---
phase: 83-public-release
plan: 04
subsystem: docs
tags: [docs, readme, skill, api]
requires: [83-01, 83-02]
provides:
  - README.md ## API section
  - SKILL.md public docs reference
affects:
  - README.md
  - skills/cairo-genizah-research/SKILL.md
tech-stack:
  added: []
  patterns: []
key-files:
  created: []
  modified:
    - README.md
    - skills/cairo-genizah-research/SKILL.md
decisions:
  - API section trimmed to 3 sentences (Codex LOW concern; was 4)
  - Rate-limit phrasing tied to "default public deployment" since SEARCH_API_RATE_LIMIT is operator-configurable
  - SKILL.md API doc reference placed inside Configuration table block as a follow-on bold paragraph
metrics:
  duration: ~5 min
  completed: 2026-05-05
---

# Phase 83 Plan 04: README + SKILL Public Docs References Summary

Doc-only updates pointing readers from the repo README and the cairo-genizah-research skill to the public API reference at `docs/SEARCH_API.md`.

## Tasks

| Task | Name                                        | Commit     | Files                                          |
| ---- | ------------------------------------------- | ---------- | ---------------------------------------------- |
| 1    | Add concise "## API" section to README.md   | `1388669b` | `README.md`                                    |
| 2    | Update SKILL.md to reference public docs    | `1b1aac37` | `skills/cairo-genizah-research/SKILL.md`       |

## What Changed

### README.md

Inserted a 3-sentence `## API` section between `## Additional Capabilities` and `## Getting Started`. Lists all three public endpoints (`POST /api/search`, `GET /api/browse`, `POST /api/parallels`), describes anonymity + rate-limit posture, and links to `docs/SEARCH_API.md` plus the live Swagger UI at `/api/docs`. Existing sections (Getting Started, Credits & Data, Documentation, version header) untouched.

Per Codex LOW concerns from `83-REVIEWS.md`:
- Trimmed prior 4-sentence draft to 3 sentences
- Rate-limit phrasing reads "30 req/min per endpoint per IP in the default public deployment" — tying the number to the configurable default rather than promising it as an immutable contract

### SKILL.md

Added a single-line bold paragraph after the Configuration table:

```
**API Documentation:** Full public API reference at [`docs/SEARCH_API.md`](../../docs/SEARCH_API.md) · Interactive: [genizahsearch.com/api/docs](https://genizahsearch.com/api/docs)
```

Relative path `../../docs/SEARCH_API.md` is correct because the skill file sits two levels deep at `skills/cairo-genizah-research/SKILL.md`. No skill code, no workflow logic, and no frontmatter touched.

## Verification

- `pytest tests/test_search_api_docs.py::test_readme_has_api_section` — PASSED
- `pytest tests/test_search_api_docs.py::test_readme_api_links_to_search_api_md` — PASSED
- `pytest tests/test_search_api_docs.py::test_skill_md_references_public_docs` — PASSED

Sentence-count check on README.md API section returned `sentences=3` (within 2–5 acceptance bound).

The remaining 5 tests in `tests/test_search_api_docs.py` (stability statement, internal-helper banner removal, Quick Start, Attribution, Changelog sections in `docs/SEARCH_API.md`) belong to Plan 83-02 and are out of scope for this plan; they will land via Wave 1.

## Threat Surface Scan

No new security-relevant surface introduced. Both threat-model entries from the plan (T-83-README-01 tampering, T-83-README-02 information disclosure on rate-limit phrasing) are mitigated:
- README structure preserved — `## Getting Started` and `## Credits & Data` headings still present at exactly 1 occurrence each
- Rate-limit phrasing tied to "default public deployment" (T-83-README-02 fix)

## Deviations from Plan

None — plan executed exactly as written.

## Self-Check: PASSED

- README.md modified: FOUND
- skills/cairo-genizah-research/SKILL.md modified: FOUND
- Commit 1388669b: FOUND
- Commit 1b1aac37: FOUND
