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

## Milestone: v7.12 — Multitenant Architecture (Path B)

**Shipped:** 2026-05-18
**Phases:** 10 (87-92 + 92.1/92.2 inserted + 999.1/999.4 promoted) | **Plans:** 28
**Git:** 277 commits, +10,587 / -1,604 Python LOC, +7,034 / -386 test LOC | **Wall clock:** 6 days (2026-05-13 → 2026-05-18)
**Requirements:** 49/49 satisfied (38 v7.12 core + 11 promoted backlog)

### What Was Built

Refactored GenizahSearch's web layer off the desktop-inherited single-user mental model. 131 raw `app.storage.user` accesses migrated through a single chokepoint adapter (`web/safe_storage.py`); allowlist driven to zero. 10 per-user `AppState` mirror fields physically deleted (state separation by deletion, not migration). `UserListsManager` singleton + 10s TTL plumbing deleted; per-request instantiation. Process-wide auth client cache (4 globals + 2 helpers) deleted; request-scoped auth via local header mutation; refresh locks keyed by `_session_uuid`; NO `auth.set_session()` mid-flight (Codex constraint at `gotrue_client.py:713`). `sign_out` uses `throwaway.auth.admin.sign_out(jwt, "global")` for real server-side revocation. Two emergency sub-phases shipped during execution: Phase 92.1 (reader-client RLS retrofit) after SWEEP-05 smoke run 1 failed at R0; Phase 92.2 (`/lists` 19.3x perf fix via task-scoped `WeakKeyDictionary` memo) after evening UAT measured 36s warm load. `docs/guides/MULTITENANT.md` shipped as architecture reference. Two backlog phases (FOLIO-01 + LINE-NUM-01..10) promoted into archive because they shipped in same window.

### What Worked

1. **Foundations-first ordering (Phase 87 before everything else)** — landing the chokepoint + AST lint scanner + allowlist FIRST meant every subsequent phase had a stable invariant to work against. Phases 88-92 could focus on deletion-not-migration without worrying about new raw accesses sneaking in.
2. **State separation by DELETION, not migration** — dual-write through `AppState` mirrors would have invited regression every refactor. Physically deleting the 10 fields + installing AST scanner regression guard made cross-user export leak structurally impossible. The pattern repeated 4 times (export state, lists cache, auth client cache, raw storage access) — each time deletion beat migration.
3. **Cross-AI plan review (Gemini + Codex) BEFORE execution** — caught 7 critical items the internal plan-checker missed. The Phase 92 SWEEP-01 widening from 1 surface to 5 (Gemini D-03 CRITICAL) was load-bearing — without it, the `app.storage.browser` cookie scope and `web/analytics.py` JS injection paths would not have been audited. The Phase 91 stale `auth_profile` security leak (Codex HIGH) prevented a role-confusion bug from shipping.
4. **Encoding cross-AI findings as locked decisions in CONTEXT.md BEFORE planning** — Gemini/Codex flag → D-XX decision baked into CONTEXT.md → plan executes against locked decisions. No re-litigation during execution. Phase 92 had 9 such D-XX entries; Phase 92.1 had 17.
5. **Two emergency sub-phase insertions worked cleanly** — Phase 92.1 (RLS reader regression) and Phase 92.2 (lists perf regression) were both inserted mid-milestone with full plan + cross-AI review + CI guards in 24-hour cycles. The pattern: smoke test FAILS → ROADMAP gets a new INSERTED row → `/gsd-plan-phase` with reviews → `/gsd-execute-phase` → re-run smoke. Sub-phase numbering (92.1, 92.2) made the insertion semantics unambiguous.
6. **HARD GATE forensic baseline for Phase 92.2 perf fix** — Plan 92.2-01 was instrumentation-only and committed a baseline JSON forensic artifact (3 warm samples 35.5/34.2/45.8s, `query_count=client_build_count=26`). Plan 92.2-02 shipped the fix and committed a postfix JSON (3 warm samples 1.9/1.9/2.2s, `query_count=2 client_build_count=1`). HARD GATE `all_warm_total_wall_clock_ms_lte_5000` passed by 2.3x margin. The before/after artifacts make the fix self-auditing.
7. **Per-phase atomic CI guard discipline** — Phase 90 D-15/D-16/D-17 three guards installed in a SINGLE commit alongside the 4-global deletion. Phase 91 separated AUTHW-05 (7 tests in one file) from AUTHW-06 (6 tests in another file) for clean blast radius. Phase 92.1 added the AST scanner + behavioral regression + dialog-context regression as three separate files. Each guard is isolated and traceable.
8. **`_session_uuid` as stable cache key** — tokens rotate; UUIDs don't. Refresh-only locking keyed by `_session_uuid` (Test B: `max_concurrent == 2` for distinct UUIDs) survived every subsequent design pressure. The single architectural primitive that everything else built on.

### What Was Inefficient

1. **Phase 90 D-09/D-10 missed the reader-client RLS reachability problem** — the singleton-anonymous-only invariant correctly stopped the SIGNED_IN-event-listener leak but left 12 reader functions using the anonymous singleton. Pre-Phase-90 the Supabase event listener auto-authenticated the singleton; Phase 90 correctly closed that channel but missed the readers. The bug surfaced 4 days later in SWEEP-05 smoke run 1 and required an emergency Phase 92.1 insertion. Better cross-checking the planning artifact (`.planning/phases/90-.../90-DISCUSSION-LOG.md:147`) against the live RLS policies in `docs/guides/SUPABASE_GUIDE.md:429-432` would have caught the false assumption "`get_user_lists` reads work anonymously."
2. **Phase 92.1 reader migration introduced a 36s perf regression** — moving 12 readers to per-request `get_user_client()` made every reader build a fresh authenticated `Client` instance. Required emergency Phase 92.2 insertion. The `~4 + L + 2*S` Supabase fanout was visible in Codex's repo evidence (`web/components/project_tree.py:60 + :301`) but not surfaced during plan review because the perf cost wasn't load-bearing for the RLS-reachability fix itself.
3. **REQUIREMENTS.md checkbox drift mid-phase** — Phase 92.1 verifier flagged that READER-01..05 checkboxes remained `[ ]` despite the requirements being satisfied; only READER-06 was flipped by the SDK post-ship sync. Pattern same as v7.8: SDK sync doesn't catch all states. Manual flip needed at close.
4. **Gemini 429 quota mid-review** — Phase 91 round 2 lost Gemini coverage; Phase 92 lost Codex coverage. Each round had only one AI reviewer. The symmetric loss pattern (different AI each time) preserved overall coverage but added uncertainty per individual phase.
5. **Pre-existing 96-item open artifact backlog** — same audit-debt drag as v7.10. The 2 v7.12-specific verification gaps were buried under 94 historical items. `gsd-tools.cjs audit-open` doesn't distinguish "stale from prior milestone" from "actionable in this milestone."

### Patterns Established

1. **Chokepoint adapter + AST lint scanner pattern** — pick one critical resource (storage, auth, RLS reachability), build a thin adapter module with the safe API, install an AST scanner that bans direct access outside an allowlist YAML, drive allowlist to zero by phased migration. Repeatable for any singleton-class problem.
2. **Deletion-not-migration discipline** — when refactoring away a pattern, physically delete the old surface in the same commit that installs the regression guard. Dual-write windows invite re-introduction. Phase 88 (AppState fields), Phase 89 (lists singleton), Phase 90 (auth caches), Phase 91 (allowlist itself) all followed this.
3. **Cross-AI plan review encoded as locked CONTEXT.md decisions** — Gemini/Codex round 1 → 7+ findings encoded as D-02..D-10 → plan executes against locked decisions, no re-litigation. Round 2 catches NEW items, encoded as NEW-H/NEW-M/NEW-L → applied via `/gsd-plan-phase --reviews` before execution.
4. **Emergency sub-phase insertion (92.1, 92.2)** — when a smoke test fails mid-milestone, insert a decimal-numbered sub-phase rather than reworking the parent phase. Preserves the parent's "shipped" semantics while making the insertion auditable in ROADMAP.
5. **HARD GATE forensic baseline + postfix artifact** — for perf regressions, commit a baseline JSON before the fix and a postfix JSON after. Multi-sample aggregation rule locked in CONTEXT.md as MUST-FIX. The artifacts make the fix self-auditing across reviews.
6. **Symmetric N-key rollback for multi-write boundaries** — `set_auth` writes 2 keys atomically with symmetric rollback; `do_login` and `_oauth_complete_login` add defensive 3-key caller-level cleanup with stale-pre-seed regression tests.
7. **No git tag for web-only milestones** (refined from v7.10) — but v7.12 ships alongside desktop changes (LINE-NUM-07/08 + v7.11.2 patch), so the tag is deferred to `/release` which bundles both. The pattern: tag only when you have something for desktop users to install.
8. **5-surface multitenant audit (Gemini D-03 widening)** — `app.storage.user` is necessary but not sufficient. Multitenant leaks live wherever global state touches disk or external APIs: `app.storage.browser` (cookies), `app.storage.client` (NiceGUI client slot), persistent SQLite files (joins.db), JS injection paths (analytics.py). Audit all 5 surfaces.

### Key Lessons

1. **Plan-time assumptions about RLS reachability MUST be checked against live `SUPABASE_GUIDE.md` evidence.** Phase 90's "`get_user_lists` reads work anonymously" assumption cost 4 days + emergency Phase 92.1. The fix is to grep `SUPABASE_GUIDE.md` for the actual SELECT policy on every BANNED_TABLES entry before the discussion log locks.
2. **Perf-sensitive paths need pre-merge instrumentation, not post-ship measurement.** Phase 92.1's reader migration shipped without `/lists` perf measurement; the regression surfaced in evening UAT 12 hours later. Better: add ContextVar-based query counters in the migration commit itself, run a baseline + verify path before merging.
3. **Cross-AI review is load-bearing for architectural milestones.** The Phase 92 5-surface audit widening (Gemini D-03 CRITICAL) and Phase 91 stale auth_profile leak (Codex HIGH) were not caught by internal plan-checker. Budget 2 review rounds per phase as a baseline; encode findings as locked CONTEXT.md decisions; revision-iterate via `/gsd-plan-phase --reviews` before execution.
4. **Atomic CI guard installation alongside code deletion** prevents the post-deletion regression window. Phase 90's three guards (D-15/D-16/D-17) shipped in the same commit as the 4-global deletion. Phase 91 separated AUTHW-05 from AUTHW-06 across two single-test-file commits. Each guard is one commit, one file, one purpose.
5. **`_session_uuid` is the right primitive.** Token-keyed locks rotate when tokens rotate (orphaning the lock); UUID-keyed locks are stable across refresh. Codex's `gotrue_client.py:713` finding (NO `set_session()` mid-flight because it's networked) made this constraint visible early; everything else built on it.
6. **Deletion-not-migration discipline propagates correctly through CI.** Phase 88's static AST scanner (`tests/test_no_deleted_state_references.py`) walks alias imports + chained attribute access; same pattern in Phase 90 D-15. Once a deletion is permanent, the scanner makes re-introduction impossible without explicit allowlist.

### Cost Observations

- Model mix: opus (planner + executor + verifier); sonnet for review subagents
- Sessions: ~10-12 focused sessions across 6 days wall clock
- Notable: highest single-milestone plan count to date (28 plans vs v7.10's 37 across 9 days). Plans/day = 4.7 — within v7.10's neighborhood despite the two emergency sub-phase insertions. Per-phase plan count averaged 2.8 (lower than v7.10's 4.6) because v7.12 phases were more vertically integrated (one chokepoint per phase) vs v7.10's horizontal-by-endpoint shape.
- Two emergency sub-phases added ~25% to phase count + ~30% to wall clock. Worth it: both shipped clean fixes with full cross-AI review + atomic CI guards in 24-hour cycles. The decimal-numbering convention (92.1, 92.2) made the insertion auditable.

---

## Milestone: v7.13 — Research-Grade Downloads & PGP Filter

**Shipped:** 2026-05-21 (closed retroactively 2026-05-27)
**Phases:** 2 (93, 94) | **Plans:** 5
**Git:** 102 commits (v7.12.0..v7.13.0) | **Wall clock:** 3 days (2026-05-19 → 2026-05-21)
**Requirements:** 14/14 satisfied (5 PGP-FILTER + 9 EXPORT-META; PGP-FILTER-03 superseded)

### What Was Built
- Web-only post-search 3-state PGP filter on `/search` (`Filter PGP`/`Has PGP`/`No PGP`), persisted via the Phase 87 `safe_storage` chokepoint
- 4-sheet bilingual research-grade xlsx workbook on BOTH apps (`Search Results` + `Manuscripts` + `Bibliography` + `Credits and Info`) via shared `shared/export_dossier.py` helpers
- Web JSON gains 3 additive per-item flags (`has_pgp`/`is_printed`/`domains`) with envelope `schema_version` unchanged

### What Worked
- **Promote-from-backlog discipline** — both phases came from 999.2/999.3 with locked CONTEXT, so planning was fast
- **Shared dossier module across web + desktop** — `shared/export_dossier.py` made the workbook structurally identical on both apps; cross-parity pinned by `tests/test_export_xlsx_cross_parity.py`
- **Same-day smoke-verification rounds** — Hillel approved 6 rounds of UX patches in-session, far faster than async review cycles
- **Mirroring an existing pattern** — the PGP filter cloned the proven `printed_filter` 3-state idiom with an AST cascade guard

### What Was Inefficient
- **CONTEXT D-04 reversed mid-flight (2026-05-20)** for the bilingual row-content layer — the English-only decision was overturned once smoke testing showed Hebrew users needed Hebrew sheets; better to have discussed bilingual scope upfront
- **Desktop-parity scope (EXPORT-META-09) added after the original backlog plan** — the SUPERSEDED `94-01-PLAN` had to be re-planned from scratch
- **The milestone close ritual was skipped entirely** — `/release` tagged v7.13.0 but MILESTONES.md + requirements archival never ran; reconciled 6 days later

### Patterns Established
- **Shared export-dossier helpers** as the single source of workbook structure for both apps
- **Bilingual export** — `lang='he'` produces Hebrew sheet titles/headers/metadata with English fallback; `lang='en'` the reverse
- **Soft-scope columns** — D-13 `IIIF Manifest` shipped header-only, then lifted post-closeout (Phase 94.1) once a zero-network proxy-URL approach was found

### Key Lessons
1. **Decide bilingual vs single-language export scope before building** — the D-04 reversal re-touched the whole row-content layer
2. **Run the milestone-close ritual at release time, not weeks later** — skipping it left REQUIREMENTS.md stranded on v7.13 and no MILESTONES entry until the v7.14 reconciliation

---

## Milestone: v7.14 — My Library (Local Document Search)

**Shipped:** 2026-05-24 (public v7.14.0); closed 2026-05-27
**Phases:** 6 (95, 96, 97, 97.2 INSERTED, 97.3 INSERTED, 98) | **Plans:** 37
**Git:** 355 commits (v7.13.0..HEAD, incl. bundled data) | **Wall clock:** 7 days (2026-05-21 → 2026-05-27)

### What Was Built
- Desktop "My Library" 7th tab indexing user folders (`.docx`/`.pdf`/`.txt`/`.html`/`.xlsx`/`.csv`) into a separate Tantivy side-index merged into Search/Composition/Parallels via RRF k=60 POST-dedup
- Three cloud-write gates at the TOP of serializer/corrections/lists-sync — personal corpora never reach web/API/Supabase
- Scale to 13K files / 43 GB: SQLite v1→v2 + zstd `cached_text` + atomic Tantivy rebuild (WAL+FULL) + crash recovery + Reset My Library
- Phase 98 (web infra): shared NLI circuit breaker across all 10 NLI/IIIF fetch sites; 45s → ~9s worst-case blocking

### What Worked
- **Codex P0 sequencing** — recovery foundation (atomic rebuild + durable cache) landed in Phase 97 Wave A *before* the 50K/50GB ceiling lift; the order paid off when 97.2's startup cascade hit
- **Three cloud-write gates at function TOP (Codex D-30)** — fail-closed against accidental cloud exposure; structurally simple to audit
- **RRF k=60 POST-`_deduplicate()` (Codex D-08)** — LOCAL hits dedup correctly against the Genizah corpus instead of double-counting
- **Decimal sub-phase insertions (97.2, 97.3)** — the now-proven pattern from v7.12 handled two emergency UAT cascades cleanly with their own plans + Codex critiques
- **"Workable limited release over another phase with more bugs"** — the user's explicit scoping constraint kept 97.3 tight (fix the 4 UAT-reproduced failures + truly-free adjacent fixes, defer the rest)
- **Replace-not-augment for the NLI breaker** — Phase 98 deleted the buggy `time.time` class-attribute breaker rather than layering on it

### What Was Inefficient
- **Three post-ship hotfix cascades (97, 97.2, 97.3) after v7.14.0** — the MVP shipped, then a 100K-file Dropbox repro surfaced a 5-bug startup cascade, then post-97.2 UAT surfaced a mega-folder UI-thread freeze. My Library's real-world scale (mega folders, mid-scan quits, network drives) wasn't exercised before the public release
- **UI-thread sync recursive walk** (Phase 96 D-F1) froze the app on mega folders — a latent defect that only surfaced under post-97.2 UAT; should have been workerized when the opt-out tree was first built
- **Recovery-modal recurrence** required a second fix (`1859b8ac`+`528906e4`) after the first 97.2 attempt — the clean-shutdown sweep lived in a child widget's `closeEvent` that never fires on app exit
- **Milestone-close ritual skipped again** — same as v7.13; both reconciled together on 2026-05-27

### Patterns Established
- **Namespace-isolated synthetic sys_ids for LOCAL** — separate from the "starts with 99" NLI/PGP/CUDL synthetic space; LOCAL never collides
- **Cloud-write gate at function TOP** — the gate is the first statement, before any client acquisition, so it cannot be bypassed by a later code path
- **Atomic Tantivy rebuild with durability bracket** — close reader → rebuild to temp → `os.rename` → reopen; WAL+FULL around the SQLite mutation; guards against the Windows handle-retention `os error 5`
- **`shared/posthog_server.py` factored so `shared/` no longer imports `web/`** — telemetry queue+daemon reusable by both layers
- **Detect-then-fallback extraction** — PDF one-word-per-line detection (0.70 single-word-ratio threshold) → `get_text("text", sort=True)` fallback

### Key Lessons
1. **Exercise real-world scale before the public release, not after.** Three hotfix cascades (97/97.2/97.3) all traced to mega-folder / mid-scan-quit / network-drive conditions that the MVP never tested. A pre-release run against Seewald's actual 13K-file / 43 GB tree would have caught the freeze and the recovery cascade.
2. **`closeEvent` on a child widget is dead code on app exit.** The clean-shutdown sweep must live on the top-level window (`GenizahGUI.closeEvent`), not a tab widget — child widgets never receive `closeEvent` when the app is killed.
3. **Sequence recovery/durability foundations before scale lifts.** Codex's P0 ordering (recovery in Phase 97 Wave A before the ceiling lift) was the single decision that made the 97.2 cascade fixable rather than data-losing.
4. **Run the close ritual at `/release` time.** Two milestones in a row shipped without it; the drift (REQUIREMENTS.md stranded on v7.13, no MILESTONES entries, ROADMAP phases misfiled under Backlog) compounded until a single reconciliation pass was needed.

### Cost Observations
- Model mix: opus (planner + executor); Codex CLI for cross-AI critique at every sub-decision
- Notable: highest plan count to date for a single milestone (37), driven by the 9+9 plans of Phases 95+96 plus two emergency decimal sub-phases and a parallel web-resilience phase. The My Library chain (95→97.3) is one feature hardened over 5 phases — the cost of productizing an external prototype to real scale.

## Milestone: v7.16 — Hebrew PDF Text Quality

**Shipped:** 2026-06-01
**Phases:** 1 formal (102) + no-phase quality work | **Plans:** 5 (Phase 102)

> Note: v7.15 was shipped 2026-05-28 but its retrospective entry was skipped at close (drift — see Key Lesson 3 below); only the trends-table row was backfilled.

### What Was Built
- LOCAL ("My Library") Hebrew PDF text-layer extractor rewritten onto a `rawdict` per-glyph foundation: RTL-gated reorder, Unicode-`Mn` nikud/maqaf classification, per-line 1-D Otsu word-gap de-space, `_ltr_damage_guard` RTL-trust fix, corrupt_encoding detection (Phase 102, 5 plans)
- De-space follow-ups from real-library UAT (D-F13b/c/d): edge-gap+Otsu metric, launch-freeze deferral, zero-width space-glyph boundary, embedded-number bidi
- LOCAL UAT extraction fixes (HTML `&nbsp` / `.xlsx` formula-only / UTF-16 `.csv`) + folder opt-out cascade (BLOCKER) + file-management actions for LOCAL hits
- Three search/startup freeze fixes (D-F23): 778 MB `search_history.json`, large-folder O(n²) startup, LAB-rebuild churn

### What Worked
- **Measure on real data, don't guess**: the freeze root causes were found by timing the actual operations against the real quarantined SQLite/data and a headless PyQt probe — which DISPROVED two plausible hypotheses (autotristate O(n²), search-path slowness) before any fix was written
- **Parallel Claude + Codex investigation**: running Codex (`codex exec`) in the background while investigating independently, then comparing, converged on the real root cause with high confidence (Codex flagged the unprofiled post-checkpoint history write as #1 suspect)
- **Real-corpus spike before the rewrite**: Spike 001 reshaped Phase 102 (RTL-gate the reorder; de-space is the dominant bug) and prevented an LTR regression
- **Edit+test no-phase track for quality follow-ups**: the de-space/UAT/freeze work moved fast as edit+test cycles rather than full GSD phases — appropriate for a one-developer quality pass

### What Was Inefficient
- **A profiler that ends at a checkpoint has a blind spot**: the in-app `GENIZAH_PROFILE_SEARCH` profiler always showed the search path fast because the freeze (the history write) ran *after* the last checkpoint — burned time before pivoting to Codex
- **The first de-space cut shipped a wrong metric**: center-gap-vs-1.8×-median shattered wide letters and merged tight-set books; needed a full rewrite to edge-gap + per-line Otsu after real-library UAT. A wider fixture set up front (tight-set + justified + letter-spaced classes) would have caught it
- **One feature, five doc surfaces**: each fix touched OPEN_ISSUES + CHANGELOG + CLAUDE + STATE + memory — heavy bookkeeping for desktop-only quality work

### Patterns Established
- **Background Codex + self, then converge**: for hard diagnosis, run an independent cross-AI investigation in parallel and compare conclusions
- **Quarantined-DB forensics**: the reset-quarantine SQLite copy preserves the pre-reset library for measuring real query costs offline
- **Remove debugging scaffolding before release**: gated profiling earns its keep during a hunt, then gets cut once the root cause proves unrelated to the instrumented span

### Key Lessons
1. **Check file sizes early.** `(Get-Item search_history.json).Length` = 778 MB was the instant confirmation once Codex pointed at history persistence — a one-command check that would have short-circuited days of search-path theorizing.
2. **Never bulk-mutate row state from `__init__` / the UI thread.** The history write, the startup opt-out refresh, the LAB rebuild, and the prior startup_recovery re-extract were all UI-thread freezes — every one moved to a background worker or eliminated (see `feedback_no_auto_reindex_in_init`).
3. **`/release` still skips the milestone-close ritual.** v7.15 skipped the retrospective entirely (missing from this file until v7.16); run the close immediately after shipping — this is the fourth milestone in a row to note the same drift.

### Cost Observations
- Model mix: opus (release + close); Codex CLI (gpt-5.5) for the parallel freeze diagnosis
- Notable: only 5 counted plans (one phase) but 4 days wall-clock — most of the milestone was no-phase edit+test quality work (de-space follow-ups, UAT fixes, three freeze fixes) that plan-count metrics don't capture

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
| v7.12 | 10 | 28 | 6 | 4.7 | Multitenant architecture (Path B) |
| v7.13 | 2 | 5 | 3 | 1.7 | Research-grade exports + PGP filter |
| v7.14 | 6 | 37 | 7 | 5.3 | My Library local document search |
| v7.15 | 3 | 7 | 2 | 3.5 | My Library visual (PDF page image) |
| v7.16 | 1* | 5 | 4 | 1.3 | Hebrew PDF text quality + freeze fixes |

*v7.16: 1 formal phase (102); most of the milestone was no-phase edit+test quality work (de-space follow-ups, UAT fixes, three search/startup freeze fixes) not captured by plan count.

**Observations:**
- v6.5.0 had the lowest plans/day ratio — reflects the large batch translation work (multi-day server jobs) and bug-fix tail
- Gap closure plans (UAT-driven) continue to be valuable but add ~30% overhead to phase count
- Quick tasks are an effective escape valve for urgent fixes during milestone execution
- v7.8 shipped fastest per-plan (~14 hours wall clock for 9 plans) — scoped to structural changes with CI safety net as the first deliverable
- v7.12 had the highest phase count (10) due to two emergency sub-phase insertions (92.1, 92.2) and two promoted backlog phases (999.1, 999.4). Per-phase plan count (2.8) was lower than v7.10 (4.6) reflecting vertically-integrated phase shape (one chokepoint per phase) vs horizontal-by-endpoint shape. Pattern: architectural milestones benefit from fewer-plans-per-phase + cross-AI review depth, not more plans.
