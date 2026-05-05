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

## Milestone: v7.10 — Search API

**Shipped:** 2026-05-05
**Phases:** 8 (77, 78, 79, 80, 81A, 81B, 82, 83) | **Plans:** 37

### What Was Built
Public HTTP/JSON research-automation API: `/api/search`, `/api/browse`, `/api/parallels`. Single serializer module owns the response shape. Hardening shell (rate limit, mode gate, error envelope, PostHog) shared across all three endpoints. Reference Anthropic Skill `cairo-genizah-research` drives the API end-to-end with file-locked token-bucket throttling and browse-honesty annotations. OpenAPI sub-mounted at `/api/openapi.json` + Swagger at `/api/docs`, scoped to the 3 search-helper endpoints. `docs/SEARCH_API.md` reframed from internal-only to public-facing with Stability + Quick Start + Attribution + Changelog.

### What Worked
1. **Serializer-first sequencing (Phase 77 before any HTTP endpoint)** — the JSON contract was locked and exercised via toolbar exports before `/api/search` consumed it. Caught a `chunk_hits` field-name collision during smoke check before it reached the API surface.
2. **Hardening shell built once in Phase 78, inherited by 79 + 80** — rate limiter, mode gate, error envelope, PostHog capture, and `wrap_endpoint` decorator wrote once and reused. Phases 79 and 80 added their endpoints in 4 plans each rather than reimplementing cross-cutting concerns.
3. **Live acceptance run as the milestone gate (81B Plan 5)** — user-observed end-to-end skill run against production beat any static-analysis subagent for integration validation.
4. **Phase 81 mid-milestone rescope into 81A + 81B + deferred 81C** — live testing after Phase 80 surfaced API expressivity gaps before the skill phase sunk cost into the wrong contract; 81-RESCOPE.md captured the decision atomically.
5. **OpenAPI sub-mount over surgical handler refactor (Plan 83-03 Option B)** — `openapi_extra=` decorators preserved Phase 78/79/80 handler signatures byte-identical, no behavior change risk.

### What Was Inefficient
1. **Phase 82 + Phase 83 shipped without canonical VERIFICATION.md** — both were evidenced inline (82-04-SUMMARY cold-reader walkthrough; 83-SECURITY Post-Deploy checklist + STATE.md) but missing the artifact required by `/gsd-audit-milestone`. Surfaced at milestone-close audit, not blocking but admin tax.
2. **PUBLIC-01..PUBLIC-08 lived in ROADMAP.md but never landed in REQUIREMENTS.md** — orphan-in-reverse: Phase 83 plans referenced them, were satisfied in flight, but never got traceability rows. Backfilled at close.
3. **Pre-existing 120-item open artifact backlog** (38 debug sessions, 18 UAT gaps, 8 unresolved verifications, 50 quick tasks, 5 todos) is unrelated to v7.10 but blocked the close-audit from running clean. Needs a /gsd-cleanup pass between milestones.

### Patterns Established
1. **Wave 0 RED-test scaffold per phase** — Phases 78–83 all opened with a Wave 0 plan landing failing tests scoped to the phase's must-haves. Subsequent plans flip them GREEN. Already a convention, but this milestone made it universal.
2. **Security-audit-as-document** (`83-SECURITY.md`): mitigation table at file:line + operator-runnable Post-Deploy Verification checklist + verdict. New format for retroactive security reviews.
3. **Sub-mount for scoped FastAPI metadata** — when you need OpenAPI/Swagger on *some* routes but not others (image proxies, etc.), sub-app + `app.mount('/api', sub)` beats trying to filter the parent app's spec.
4. **Web-only release pattern formalized**: NO git tag, NO GitHub Release object — desktop polls `/releases/latest` and would prompt every desktop user to update for a no-installer page. Documented in 83-05-SUMMARY decisions.

### Key Lessons
1. **Verification artifacts are administrative but load-bearing for audit.** When a phase's verification lives in SUMMARY narrative + sibling docs (SECURITY, VALIDATION, ACCEPTANCE-RUN), the canonical `{phase}-VERIFICATION.md` should still be authored, even if it just transcribes pointers.
2. **Requirements traceability needs a forcing function before milestone close.** PUBLIC-01..08 satisfied themselves into the codebase but never satisfied themselves into REQUIREMENTS.md. Consider an `/gsd-add-requirement` step when ROADMAP introduces new REQ-IDs.
3. **Mid-milestone rescopes work IF the rationale is captured atomically.** 81-RESCOPE.md (rev 3, APPROVED 2026-05-02) was the load-bearing artifact that let 81A + 81B + deferred 81C feel like a deliberate decomposition rather than scope creep.
4. **A live user-observed acceptance run is stronger than any subagent integration check** for a deployed-and-running API. Use it.

### Cost Observations
- Model mix: opus (planner + executor) per project config; sonnet for review subagents
- Sessions: ~6–8 focused sessions across 9 days wall clock (2026-04-27 → 2026-05-05)
- Notable: highest plan count since v6.5.0 (37 plans for 8 phases — per-phase plans tightly scoped, ~4–5 plans each); reference Anthropic Skill is the first deliverable that lives outside genizahsearch.com proper

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
| v7.10 | 8 | 37 | 9 | 4.1 | Public Search API + reference Skill |

**Observations:**
- v6.5.0 had the lowest plans/day ratio — reflects the large batch translation work (multi-day server jobs) and bug-fix tail
- Gap closure plans (UAT-driven) continue to be valuable but add ~30% overhead to phase count
- Quick tasks are an effective escape valve for urgent fixes during milestone execution
- v7.8 shipped fastest per-plan (~14 hours wall clock for 9 plans) — scoped to structural changes with CI safety net as the first deliverable
