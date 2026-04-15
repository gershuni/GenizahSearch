# Phase 66: Documentation Update - Context

**Gathered:** 2026-04-15
**Status:** Ready for planning

<domain>
## Phase Boundary

Update project documentation to accurately reflect the current codebase state after all v7.8 structural changes (CI, auth migration, repo hygiene). No code changes — documentation only. Requirements: DOCS-01 through DOCS-04.

</domain>

<decisions>
## Implementation Decisions

### Claude's Discretion

User chose to skip discussion — all implementation decisions are at Claude's discretion. The requirements (DOCS-01 through DOCS-04) are clear-cut and self-documenting.

Guidance for downstream agents:

- **CODE_INDEX.md (DOCS-01):** Regenerate or update the auto-generated index to reflect new files (`web/framework_patches.py`) and any line number shifts from v7.8 changes. Last updated 2026-03-26 — needs refresh.
- **OPEN_ISSUES.md (DOCS-02):** Add structural debt items from the Phase 65 code review (`65-REVIEW.md`). Mark resolved items with date. Group by category (silent handlers, monkey-patches, gitignore). Include only items that are actionable or informational — don't duplicate the full review.
- **check_docs.py (DOCS-03):** Already passes green as of 2026-04-15. Maintain green state after all doc changes. Note: the script has a `UnicodeEncodeError` on Windows cp1255 when emoji output hits the console — works fine with `PYTHONIOENCODING=utf-8`. This is a pre-existing issue, not a Phase 66 concern.
- **DEVELOPER_GUIDE.md (DOCS-04):** Document CI workflow (GitHub Actions), ruff configuration (scoped ruleset), and dependency upgrade process (two-file pinning). Keep it practical — quick-reference style with commands, not tutorial prose. The guide already has a dependency section from Phase 63; update/extend it.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Code Review Findings
- `.planning/phases/65-repo-hygiene/65-REVIEW.md` — Code review findings that need OPEN_ISSUES.md entries

### Current Documentation State
- `docs/CODE_INDEX.md` — Last updated 2026-03-26, needs refresh for v7.8 changes
- `docs/OPEN_ISSUES.md` — Active tracker, already has v7.8 entries from earlier phases
- `docs/guides/DEVELOPER_GUIDE.md` — Already has CI and dependency sections from Phase 63

### v7.8 Artifacts
- `.planning/phases/63-ci-dependency-pinning/63-CONTEXT.md` — CI and pinning decisions
- `.planning/phases/64-auth-migration/64-CONTEXT.md` — Auth migration decisions
- `.planning/phases/65-repo-hygiene/65-CONTEXT.md` — Hygiene decisions
- `.github/workflows/ci.yml` — The CI workflow to document
- `pyproject.toml` — Ruff config to document
- `web/framework_patches.py` — New file to add to CODE_INDEX

### Validation
- `scripts/check_docs.py` — Must pass green after all changes

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `scripts/check_docs.py` — Automated doc health checker (critical docs, outdated terms, freshness, links)
- `docs/CODE_INDEX.md` — Auto-generated format with class/function/line-number structure

### Established Patterns
- CODE_INDEX uses `**Function** name (Line N)` and `**Class** name (Line N)` format with indented methods
- OPEN_ISSUES uses table format with status column (Open/Fixed with date)
- DEVELOPER_GUIDE uses command-block quick-reference style

### Integration Points
- check_docs.py validates: critical doc existence, outdated terminology, freshness (<90 days), internal links
- CLAUDE.md references docs that must stay consistent

</code_context>

<specifics>
## Specific Ideas

No specific requirements — open to standard approaches. User explicitly chose "skip discussion" indicating confidence in the requirements as written.

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope.

</deferred>

---

*Phase: 66-documentation-update*
*Context gathered: 2026-04-15*
