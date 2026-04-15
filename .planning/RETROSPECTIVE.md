# Project Retrospective

*A living document updated after each milestone. Lessons feed forward into future planning.*

## Milestone: v6.5.0 — Search UX & Filtered Search

**Shipped:** 2026-03-14
**Phases:** 5 | **Plans:** 26

### What Was Built
- Search UX overhaul with elapsed timer, ETA, partial results on cancel, 3-state printed filter
- Session persistence with full state restore including 5K+ exclusions, search/composition history
- Bidirectional filtered search by domain/author/work/date/material across all modes
- ~580K Dicta translations (libraries, PGP, FJMS catalog, descriptions, running titles) with translation toggle
- Translation QA infrastructure with 10-heuristic checks and 12,827 automated data fixes

### What Worked
- **User-feedback-driven scoping**: The power user letter (17 requests) gave clear priorities and concrete acceptance criteria
- **UAT gap closure pattern**: Phases 42's 6 gap closure plans (42-04 through 42-09) caught real issues that would have shipped as bugs
- **Batch translation pipeline**: Checkpointing + resume + SIGINT handling made 580K translations reliable with zero failures
- **Translation QA heuristics**: Automated detection of hallucinations, script mismatches, and anomalies prevented bad translations from reaching users
- **Quick task workflow**: 5 quick tasks (15-19) handled urgent fixes without disrupting phase flow

### What Was Inefficient
- **REQUIREMENTS.md scoping**: Requirements were written as "v7.0.0" covering both v6.5.0 and v7.0.0 work, creating confusion during milestone completion about which requirements belonged where
- **Translation toggle scope creep**: Initially added translated-match badges to main search results, then had to remove them (46-05) — should have been clearer about translation display scope upfront
- **Session restore bugs**: Three quick tasks (18-19) and three commits for session restore fixes after the feature was "complete" — indicates insufficient edge case testing

### Patterns Established
- **3-state filter toggle**: all/hide/only pattern for binary properties (printed, domain exclusion)
- **restrict_sys_ids pattern**: Pre-search filtering via sys_id set, applied BEFORE regex for performance
- **Dicta scholarly few-shot**: 16-pair specialized prompts for genizah terminology (JA transliteration, etc.)
- **Translation QC pipeline**: Batch translate → QC audit → stratified sampling → data fixes → user report button
- **Browse-to-search navigation**: incoming_filters via storage/state for cross-page filter carry-over

### Key Lessons
1. **Write requirements scoped to exactly one milestone** — broader requirements docs create confusion at completion time
2. **Translation display scope should be decided before building** — adding then removing features wastes effort
3. **Session persistence needs adversarial testing** — state restore with complex UI (browse tabs, composition summaries, filtered results) has many edge cases

## Milestone: v7.8 — Structural Foundation

**Shipped:** 2026-04-15
**Phases:** 4 | **Plans:** 9
**Git:** 64 commits, 173 files, +6,269 / -828 lines | **Wall clock:** ~14 hours (2026-04-14 → 2026-04-15)
**Requirements:** 12/12 satisfied

### What Was Built
- GitHub Actions CI with Ubuntu + Windows matrix running ruff + check_docs.py + pytest on every push/PR
- Two-file dependency pinning: 14 direct (requirements.txt) + 115 transitive (requirements-lock.txt), all `==`
- Supabase auth migrated off deprecated gotrue: supabase_auth.errors imports, PKCE-only OAuth callback, dead implicit-flow endpoint removed
- NiceGUI monkey-patches isolated in web/framework_patches.py with per-patch packaging.version version guards
- 205+ silent exception handlers across 76 first-party files audited: each logs or has justification comment
- .gitignore extended from 50 to 126 lines with root-anchored patterns; untracked root files 67 → 1
- Documentation refresh: CODE_INDEX v7.8 sections, OPEN_ISSUES code review tracking, DEVELOPER_GUIDE CI/ruff/deps workflow

### What Worked
- **CI first, risky changes after**: Ordering Phase 63 (CI) before Phase 64 (auth) meant the auth migration had an immediate regression safety net
- **Scoped ruff ruleset**: E9/F401/F811/F821 caught real bugs (an F821 undefined name that would have crashed at runtime) without triggering a legacy-code side quest
- **Two-file dependency pinning**: Direct deps stay editable, lock file stays reproducible — cross-platform caveat documented without trying to solve it
- **Inline justification comments over logging conversion**: Preserved intentional suppression semantics, kept changes grep-reviewable, zero behavioral risk
- **Production OAuth testing**: PKCE code_verifier requires same-process callback, so localhost testing was impossible — pragmatic choice to verify on production
- **Independent per-patch version guards**: Using packaging.version.Version() instead of string comparison — future-proofs against 3.10 vs 3.8 sort ordering
- **Shipped in 14 hours**: Infrastructure-heavy milestone with no user-visible changes executed quickly because the scope was disciplined

### What Was Inefficient
- **Ruff auto-fix regression**: `ruff check --fix` gutted backward-compatibility re-exports across 5 shim modules — had to restore each with explicit `X as X` syntax. Auto-fix should have been reviewed per-file, not run whole-repo
- **SUMMARY.md frontmatter inconsistency**: 5 of 9 summaries had empty `requirements-completed:` fields despite VERIFICATION.md explicitly tracking satisfaction — discovered at milestone close, backfilled manually
- **REQUIREMENTS.md checkboxes stale**: 8 of 12 requirement checkboxes and traceability rows still showed "Pending" at close despite phases being complete — workflow didn't auto-update them after phase completion
- **gsd-tools audit-open CLI bug**: `ReferenceError: output is not defined` at gsd-tools.cjs:786 — blocked automated pre-close audit, had to audit manually
- **No Nyquist VALIDATION.md in Phases 65/66**: Infrastructure phases skipped validation contracts; acceptable but visible gap

### Patterns Established
- **Scoped linter introduction**: Start with syntax/import hygiene only (E9/F401/F811/F821), explicit "expand over time" note in config
- **Two-file pinning workflow**: Edit requirements.txt, regenerate requirements-lock.txt via pip freeze, commit both
- **Inline justification for silent handlers**: `pass  # <reason>` preserves behavior + adds auditability
- **Root-anchored .gitignore with exemption block**: Explicit `!intentional-asset.json` entries prevent silent ignoring of tracked files
- **Per-patch version guards**: Each monkey-patch gets its own `_NV > _V('x.y.z')` — patches can be retired independently as upstream fixes land
- **Pragmatic production verification**: When localhost testing is impossible (OAuth PKCE), note the constraint and verify on production with user confirmation

### Key Lessons
1. **Run `ruff check --fix` with review, not wholesale** — autofix is aggressive about "unused" imports and will break re-export shims
2. **Automate requirements tracking updates at phase completion** — SUMMARY frontmatter and REQUIREMENTS.md checkboxes drift and become stale by milestone close
3. **CI safety net first is non-negotiable for risky refactoring milestones** — auth migration was 10x safer because ruff/pytest/check_docs were already gating every change
4. **Scoped changes ship faster than sprawling ones** — this milestone stayed on the narrow "structural debt without behavior change" target and delivered in ~14 hours

### Cost Observations
- Model mix: primarily opus (planner + executor) per project config
- Sessions: ~1-2 focused sessions across 14 hours wall clock
- Notable: smallest milestone by plan count (9) but highest-leverage — unblocks v7.9 Decomposition with CI safety net

---

## Cross-Milestone Trends

| Milestone | Phases | Plans | Days | Plans/Day | Key Theme |
|-----------|--------|-------|------|-----------|-----------|
| v1 | 9 | 18 | 3 | 6.0 | PGP integration |
| v5.6.0 | 5 | 25 | 2 | 12.5 | Desktop parity |
| v5.7.0 | 4 | 14 | 2 | 7.0 | Responsa search |
| v5.7.2 | 4 | 11 | 1 | 11.0 | Cleanup |
| v5.7.3 | 3 | 3 | 1 | 3.0 | Corrections |
| v5.8.0 | 4 | 12 | 3 | 4.0 | FJMS integration |
| v5.9.0 | 6 | 22 | 6 | 3.7 | Multi-source images |
| v6.0.0 | 6 | 21 | 6 | 3.5 | Local data architecture |
| v6.1.0 | 1 | 4 | 1 | 4.0 | Catalog browse |
| v6.5.0 | 5 | 26 | 15 | 1.7 | Search UX + translations |
| v7.8 | 4 | 9 | 1 | 9.0 | Structural foundation / CI |

**Observations:**
- v6.5.0 had the lowest plans/day ratio — reflects the large batch translation work (multi-day server jobs) and bug-fix tail
- Gap closure plans (UAT-driven) continue to be valuable but add ~30% overhead to phase count
- Quick tasks are an effective escape valve for urgent fixes during milestone execution
- v7.8 shipped fastest per-plan (~14 hours wall clock for 9 plans) — scoped to structural changes with CI safety net as the first deliverable
