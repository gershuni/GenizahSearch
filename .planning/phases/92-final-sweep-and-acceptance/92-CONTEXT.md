# Phase 92: Final Sweep and Acceptance — Context

**Gathered:** 2026-05-15
**Status:** Ready for planning

<domain>
## Phase Boundary

Close the v7.12 Multitenant Architecture (Path B) milestone by producing **verification evidence**, executing a **human cross-user smoke test**, auditing **state surfaces outside `app.storage.user`** (Gemini-flagged: `joins.db`, PostHog, `app.storage.browser/client`), and writing the canonical architecture reference (`docs/guides/MULTITENANT.md`) so the next contributor can extend the chokepoint without reading 30K lines of Codex transcripts. Phase 91 took the Phase 87 allowlist to 0 entries (`allowed_raw_access: []`) and the lint scanner (`tests/test_no_raw_storage_access.py`) now enforces zero raw `app.storage.user` accesses anywhere under `web/` — an independent AST scan run during this discuss-phase confirms 0 raw accesses outside `web/safe_storage.py`. SWEEP-01 / SWEEP-02 / SWEEP-03 therefore collapse from "discovery" to "produce evidence + audit non-`app.storage.user` surfaces Gemini flagged + tag the 4 Codex review transcripts."

**Architectural framing (Gemini round-1 CRITICAL catch):** **Multitenant leaks happen wherever global state touches disk or external APIs — not just at `app.storage.user`.** Phases 87-91 hardened the `app.storage.user` chokepoint. Phase 92 expands the audit scope to: (a) `shared/puzzle_service.py` / `joins.db` SQLite singleton (process-wide; check whether per-user `created_by`/`owner_id` exists or whether community-share semantics make ownership N/A); (b) `web/analytics.py:posthog_capture` (verified Python-side JS-injection-only — no server state; document in MULTITENANT.md §8 as "verified non-leaking by inspection"); (c) `app.storage.browser` (Phase 91 noted `create_login_dialog` uses it for "Remember me" — verify scope is intentional + bounded); (d) `app.storage.client` (NiceGUI connection-scoped storage — verify no PII leakage). The Phase 87 lint scanner does NOT cover (a)–(d); SWEEP-01 is widened to manual+grep audit of those four surfaces in addition to the now-trivial `app.storage.user` verification.

**SWEEP-04 methodology pivot (Gemini round-1 HIGH catch):** Replace chronological "walk each transcript top to bottom" with **thematic walk by unique issue with lifecycle map**. The 4 Codex transcripts (`codex_post_711` 1c/1h/2m/3l, `codex_critical_high` 2c/1h/0m/0l, `codex_3rdpass` 2c/1h/0m/1l/1n, `codex_4thpass` 1c/2h/2m/1l/1n = 6c+5h+4m+5l+3n = 23 unique-or-overlapping findings) have re-flagged-after-fix-attempt history (e.g., composite-key-RMW pattern was flagged in pre-v7.12 review rounds and re-surfaced in Phase 91 discuss-phase round 1). Linear walk would mark Issue A "addressed" in transcript 1 and miss Issue A-prime in transcript 4. SWEEP-04 audit memo (`92-SWEEP-04-TRANSCRIPT-AUDIT.md`) is keyed on **unique issue ID** (Claude's choice — e.g., `LISTS-CACHE-RMW-RACE`) with columns: first-flagged-transcript | re-flagged-transcripts | final-disposition | resolution-commit-hash | resolution-phase-plan.

**Smoke test scenario expansion (Gemini round-1 HIGH catch):** Beyond the baseline two-user concurrent search→browse→lists→xlsx (REQUIREMENTS.md SWEEP-05 wording), the smoke checklist adds: **(R1)** User A signs out mid-flight while User B is downloading xlsx (validates `throwaway.auth.admin.sign_out(jwt, "global")` from Phase 90 doesn't nuke shared anonymous-client state for B); **(R2)** Force a token refresh on User A (wait for natural expiry OR temporarily lower JWT TTL in a side branch) while User B heavily interacts with lists/parallels (validates per-`_session_uuid` refresh lock from Phase 90 D-17 doesn't serialize across distinct sessions). The (c) `joins.db` concurrent-puzzle-write scenario is added only if SWEEP-01 puzzle audit reveals per-user state in joins.db — otherwise N/A (community-share semantics).

**Scope (after Phase 91 took allowlist to 0):**
- **SWEEP-01:** Audit `web/` for residual raw `app.storage.user` accesses (de-facto satisfied by Phase 87 lint scanner; capture independent AST snapshot as evidence). **Widened per Gemini CRITICAL** to include `app.storage.browser/client`, `shared/puzzle_service.py`/`joins.db`, `web/analytics.py` (PostHog) audits.
- **SWEEP-02:** Confirm `parallels.py:3520` (deferred-restore callback) and `text_editor.py` (auto-save) are migrated. Independent verification during this discuss-phase: both confirmed using `safe_user_get/set` (Phase 87 work complete).
- **SWEEP-03:** Allowlist re-audit. Current state: `allowed_raw_access: []` with preamble comment. Verify `tests/test_no_raw_storage_access.py:test_allowlist_well_formed` accepts empty list (Phase 91 D-07 fix).
- **SWEEP-04:** Thematic-walk audit of 4 Codex transcripts. Each unique issue → addressed (with **git commit hash + phase pointer**) OR waived (with rationale). Output: `92-SWEEP-04-TRANSCRIPT-AUDIT.md`.
- **SWEEP-05:** Human-driven cross-user smoke test with **baseline + R1 logout-race + R2 refresh-race + conditional R3 puzzle-race**. Pre-filled checklist at `92-SWEEP-05-SMOKE.md`. Plan 92-01 commits the scaffold; user runs smoke; user commits the filled-in checklist back; Plan 92-02 starts only after smoke PASS commit.
- **SWEEP-06:** `docs/guides/MULTITENANT.md` written. Architecture reference + "how to add a new per-user state value" tutorial per locked decision. **Plus**: bright-red warning callout in §7 about `set_auth(user, profile=None)` clears-profile semantics (Gemini HIGH catch on Principle-of-Least-Astonishment violation).

**Out of scope:**
- **Production code changes** — Phase 92 is verification + docs only. The migration is complete; the lint scanner is the live enforcement layer.
- **New lint rules** — `tests/test_no_raw_storage_access.py` already enforces the empty-allowlist invariant. No new AST scanners needed.
- **Cross-process safety** — single Uvicorn process today. Carryover from REQUIREMENTS.md "Out of Scope."
- **Desktop-app multitenancy** — desktop is single-user by design. Web-only milestone closure.
- **`v7.12.0` release tagging / GitHub release** — Phase 92 unblocks `deploy.sh` (server is detached at `v7.11.1` commit `242664d3`). Tag + release decisions are post-Phase-92 release-management, not closeout.
- **Lessons-learned appendix in MULTITENANT.md** — explicitly rejected per locked decision (history preserved in `_tmp/codex_*.txt` + `.planning/phases/87-92/*-CONTEXT.md`).
- **Composite-key consolidation `_auth_block`** — rejected in Phase 91 (Codex F1 RMW race surface); not reopened in Phase 92.

</domain>

<decisions>
## Implementation Decisions

### Area 1: External Codex Red-Team Round (the only gray area selected)

- **D-01 (Single round, fallback dispatched):** User selected "One round only" per the locked v7.12 pattern. Codex CLI was **quota-blocked until 2026-05-19** when dispatched; **Gemini CLI succeeded as the symmetric fallback** (inverse of Phase 91 round 2 where Codex worked and Gemini 429'd). Gemini round-1 verdict: **1 CRITICAL + 3 HIGH + 2 MEDIUM + 1 LOW → REFACTOR**. All findings encoded as locked decisions below. No round-2 dispatched — surface after these refinements is mechanical (verification + docs).

- **D-02 (Plan decomposition):** **Two plans** per user-locked decision, BUT with an explicit human-gated commit boundary between them (Gemini MEDIUM):
  - **Plan 92-01:** SWEEP-01 (widened) + SWEEP-02 + SWEEP-03 + SWEEP-04 audit memos + SWEEP-05 smoke **scaffold commit**.
  - **[HUMAN INTERVENTION]:** Hillel runs the smoke test, fills in `92-SWEEP-05-SMOKE.md` checkboxes, commits the PASS-confirmed checklist back.
  - **Plan 92-02:** `docs/guides/MULTITENANT.md` write — **gated on smoke checklist commit showing PASS**. Plan 92-02 cannot start until 92-SWEEP-05-SMOKE.md is committed with all checkboxes checked. Gemini's argument: "Do not write docs for an architecture that hasn't passed its smoke test."

### Area 2: SWEEP-01 Scope Widening (Gemini CRITICAL)

- **D-03 (Audit surfaces beyond `app.storage.user`):** SWEEP-01 produces `92-SWEEP-01-AUDIT.md` covering **5 surfaces**, not 1:

  | Surface | Audit method | Expected state |
  |---------|--------------|----------------|
  | `app.storage.user` in `web/` | AST walker + grep (belt-and-suspenders) | Zero raw accesses outside `web/safe_storage.py` (verified during discuss-phase) |
  | `app.storage.browser` | grep `web/` for `app.storage.browser` | Document each call site with intent (`create_login_dialog` "Remember me" is the known site per Phase 91 NEW-H2 deviation) |
  | `app.storage.client` | grep `web/` for `app.storage.client` | Document each call site with intent. If any per-user data found, flag as a follow-up phase issue |
  | `shared/puzzle_service.py` + `joins.db` | Read schema; check for `user_id` / `created_by` / `owner_id` columns; check `PuzzleService` for module-level singleton + process-wide write surface | Community-share semantics OR explicit per-user RLS. Independent ground-truth check during discuss-phase: schema has `id, title, notes, fragments_json, thumbnail_b64` — no user-ownership column, so community-share semantics apply. Per-user puzzle ownership lives in Supabase (cloud DB with RLS). Document this finding |
  | `web/analytics.py:posthog_capture` | Read source; verify no server-side per-user state | Ground-truth check during discuss-phase: function is `ui.run_javascript` injection only; no Python-side caching of `distinct_id`. Document as "verified non-leaking by inspection" |

  **Why widened:** Gemini correctly noted that multitenant leaks happen wherever global state touches disk or external APIs — the Phase 87 lint scanner only covers `app.storage.user.*`. The 4 additional surfaces are out-of-scanner-scope and need human audit before milestone closure.

- **D-04 (joins.db community-share posture documented):** The local `joins.db` SQLite sidecar at `joins_data/joins.db` has **no per-user ownership columns** (`id, title, notes, fragments_json, thumbnail_b64` only). It is process-wide writable via the `PuzzleService` singleton pattern. **This is intentional** — joins.db is the desktop/offline copy of community-share puzzle data; per-user ownership lives in Supabase (RLS-protected). Document in `92-SWEEP-01-AUDIT.md` under "Audited surfaces" with this rationale. The R3 concurrent-puzzle-write smoke scenario stays **conditional** on SWEEP-01 finding contradictory evidence (e.g., a per-user joins.db field I missed during the discuss-phase 100-line skim).

### Area 3: SWEEP-04 Thematic-Walk Methodology (Gemini HIGH)

- **D-05 (Issue-keyed audit, not transcript-keyed):** `92-SWEEP-04-TRANSCRIPT-AUDIT.md` structure:

  ```markdown
  ## Issue: <SHORT-ID> — <one-line description>

  - **First flagged:** <transcript>, <round-N>, severity <CRITICAL|HIGH|MEDIUM|LOW|NIT>
  - **Re-flagged (if any):** <list of {transcript, round, severity}>
  - **Final disposition:** addressed | waived
  - **Resolution commit:** <git short hash> (`<phase>` Plan `<plan>`)
  - **Resolution rationale:** <one paragraph: what was changed, why this fully closes vs. partially closes>
  - **Waiver rationale (if waived):** <explicit reason: defer / not-applicable / accepted-risk>
  ```

  Issue IDs are short slugs Claude chooses during plan execution (e.g., `LISTS-CACHE-RMW`, `CLIENT-CACHE-RESURRECT`, `PERSIST-VALUE-UNSAFE`, `OAUTH-CALLBACK-ATOMICITY`). The audit memo opens with a **cross-transcript matrix** (issue × transcript with severity cells) so a reader can spot re-flag patterns at a glance.

- **D-06 (Initial inventory baseline — produced during discuss-phase):** Counts pulled during discuss-phase as the starting baseline for SWEEP-04:
  - `codex_post_711_review_response.txt`: 1 critical, 1 high, 2 medium, 3 low, 0 nits → recall
  - `codex_critical_high_review_response.txt`: 2 critical, 1 high, 0 medium, 0 low, 0 nits → recall
  - `codex_3rdpass_review_response.txt`: 2 critical, 1 high, 0 medium, 1 low, 1 nit → recall
  - `codex_4thpass_review_response.txt`: 1 critical, 2 high, 2 medium, 1 low, 1 nit → not deployable
  - **Total: 6 critical + 5 high + 4 medium + 5 low + 3 nits = 23 findings** (some likely duplicates across transcripts — thematic dedup is the SWEEP-04 task itself).

### Area 4: SWEEP-04 Evidence — Git Commit Hashes (Gemini MEDIUM)

- **D-07 (Commit hashes are mandatory; phase pointers are companion not substitute):** Each "addressed" disposition in SWEEP-04 audit memo MUST include the actual git short commit hash that closed the finding, in addition to the `Phase N Plan N-N` pointer. Gemini's rationale: "`Phase 91, Plan 91-02` is completely meaningless to a developer looking at this repo in 2 years. `.planning` files are ephemeral process artifacts; `git log` is forever." Format: `Resolution: 091c70f4 (Phase 90 Plan 90-02)` — git hash first because it's the durable identifier. Use `git log --oneline --grep="Phase NN"` to find candidates; verify by reading the commit body.

### Area 5: SWEEP-05 Smoke Test Expansion (Gemini HIGH)

- **D-08 (Pre-filled checklist with 4 scenarios — R0 baseline + R1 logout race + R2 refresh race + conditional R3 puzzle race):** `92-SWEEP-05-SMOKE.md` shape:

  ```markdown
  ## R0 — Baseline cross-user isolation
  - [ ] User A logs in (account: gershuni+a@gmail.com)
  - [ ] User B logs in in second browser (account: gershuni+b@gmail.com), confirms different `_session_uuid` from A
  - [ ] A and B run different searches concurrently
  - [ ] Each navigates to browse on a hit
  - [ ] Each adds the hit to a personal list
  - [ ] Each downloads xlsx of search results
  - [ ] A's xlsx contains only A's results (sample 5 rows + filename)
  - [ ] B's xlsx contains only B's results (sample 5 rows + filename)
  - [ ] A's `/lists` page shows only A's lists
  - [ ] B's `/lists` page shows only B's lists
  - [ ] Evidence column (screenshots / paste of first 5 rows)

  ## R1 — Logout-mid-flight race (Phase 90 throwaway.auth.admin.sign_out validation)
  - [ ] B starts an xlsx download (heavy search, hold the export)
  - [ ] A clicks Logout while B's export is in flight
  - [ ] B's xlsx completes normally and contains only B's data
  - [ ] B's session is unaffected (token refresh still works after; B can run another search)
  - [ ] A's logout actually revoked server-side (verify by attempting an authenticated API call with A's old access_token — should 401)
  - [ ] Evidence column

  ## R2 — Token refresh race (Phase 90 _session_uuid lock validation)
  - [ ] Set up: lower JWT TTL on a side Supabase project OR wait until A's access_token is within 1 min of expiry
  - [ ] A's tab is heavily interacting with /parallels (longer requests) while access_token expires mid-flight
  - [ ] B is downloading xlsx in parallel
  - [ ] A's requests succeed (refresh fires; B is unaffected)
  - [ ] B's xlsx completes with only B's data
  - [ ] No "set_session() called mid-flight" log warnings on the server (Phase 90 AUTHC-02 invariant)
  - [ ] Evidence column

  ## R3 — Concurrent puzzle write (CONDITIONAL — only if SWEEP-01 reveals per-user puzzle ownership in joins.db)
  - [ ] Skip if SWEEP-01 confirmed joins.db is community-share with no per-user columns
  - [ ] Otherwise: A and B both add/edit puzzle documents at /puzzle simultaneously
  - [ ] Each sees only their own changes
  - [ ] Evidence column

  ## Final disposition
  - [ ] R0 PASS / FAIL: __________
  - [ ] R1 PASS / FAIL: __________
  - [ ] R2 PASS / FAIL: __________
  - [ ] R3 PASS / FAIL / N/A: __________
  - [ ] Overall: PASS / FAIL — __________
  - [ ] Tester: Hillel Gershuni, date: __________
  ```

  Plan 92-01 commits this file pre-filled but unchecked. Plan 92-02 is **gated** on a follow-up commit checking all boxes with `Overall: PASS`. **Server start is manual (per `feedback_no_background_webserver.md` — no Bash-spawned web server on Windows).** Use `python -m web.main` in a dedicated terminal that Hillel controls.

### Area 6: MULTITENANT.md §7 Warning Box (Gemini HIGH)

- **D-09 (Bright warning callout for `profile=None` semantics):** §7 "Adding a new per-user state value" tutorial MUST include a callout (markdown blockquote with `> ⚠️ **WARNING**` prefix to make it visually distinctive in any markdown renderer) about `set_auth(user, profile: Optional[Dict] = None)`:

  ```markdown
  > ⚠️ **WARNING — `profile=None` clears, not "no change"**
  >
  > `GlobalAuthState.set_auth(user, profile=None)` **deletes** the
  > `auth_profile` storage key (i.e., clears any stale profile data).
  > This violates the Pythonic Principle of Least Astonishment, where
  > `kwarg=None` usually means "no change." We chose `None`-clears
  > semantics in Phase 91 to close a Codex HIGH catch: a stale
  > `auth_profile` after a partial-write rollback could leak admin/editor
  > role to a logged-out user because `GlobalAuthState.get_role()`
  > reads `auth_profile` independently of `auth_user`.
  >
  > **If you want "no change," do not pass `profile=` at all** — the
  > default is `None` AND it intentionally clears any pre-existing stale
  > profile from a prior session. To replace the profile, pass the new
  > dict explicitly.
  >
  > See `tests/test_auth_callback_resilience.py:T-F` for the regression
  > test that locks this semantic in place.
  ```

  Gemini's rationale: a new contributor passing `profile=None` thinking "no change" would silently nuke their own logged-in user's profile cache. The visual prominence of the callout is the cheapest mitigation.

### Area 7: SWEEP-01 `app.storage.browser` / `client` Audit (Gemini LOW)

- **D-10 (Document existing usage, do not migrate):** `app.storage.browser` is used in `web/auth_state.py:create_login_dialog` for "Remember me" cookie persistence (Phase 91 Rule-1 deviation noted at NEW-H2). This is a **NiceGUI-managed cookie-backed store**, not the same surface as `app.storage.user`. SWEEP-01 documents:
  - All `app.storage.browser` call sites in `web/`
  - Whether each carries PII or just preference flags (Remember me = boolean preference, not PII)
  - Whether any leakage scenario exists (cookies are browser-scoped, not Python-process-scoped — no cross-user-in-same-process leak risk by design)
  - `app.storage.client` (NiceGUI connection-scoped) call sites — verify they exist (if any) and document scope

  **Do NOT widen the Phase 87 lint scanner to cover these.** They are different storage surfaces with different leak semantics. Documenting their usage in SWEEP-01 audit memo + MULTITENANT.md §8 enforcement section is sufficient.

### Claude's Discretion

- **Issue ID slug naming convention for SWEEP-04** — Claude picks short kebab-case slugs (e.g., `LISTS-CACHE-RMW-RACE`, `CLIENT-CACHE-RESURRECT`). The audit memo's cross-transcript matrix uses these slugs as row keys.
- **Order of audits within Plan 92-01** — SWEEP-01 (5-surface audit) first to establish ground truth, then SWEEP-02/SWEEP-03 (mechanical re-verification), then SWEEP-04 (the thoughtful thematic walk), then SWEEP-05 scaffold last (since R3 conditional depends on SWEEP-01 finding).
- **Whether SWEEP-04 audit memo dedupes the 23-finding inventory inline or in an appendix** — Claude picks: top-level table is unique-issue-keyed (deduped), with a per-transcript appendix showing the raw count if needed for traceability.
- **Word count target for MULTITENANT.md** — Claude picks: aim for ~2,000-3,000 words total, with §7 tutorial as the longest section. Pure reference + tutorial; no padding.
- **Whether to include a `MULTITENANT.md` skeleton check in Plan 92-02** (e.g., a CI test that asserts the doc contains §1..§8 anchors) — Claude picks: NO. The doc is reference-grade content; an anchor-presence test adds maintenance overhead with marginal value. Future contributors editing the doc will naturally preserve structure.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase 92 Locked Requirements
- `.planning/REQUIREMENTS.md` §"Final Sweep + Acceptance — Phase 92" — SWEEP-01 through SWEEP-06. Note that SWEEP-01/02/03 are **verification rather than discovery** post-Phase-91 (Phase 87 lint scanner is the live enforcement layer; AST scan independently confirmed zero raw accesses).
- `.planning/ROADMAP.md` §"Phase 92: Final Sweep and Acceptance" — 5 success criteria. SC #1/#2 reduce to "produce evidence the invariant holds + audit non-`app.storage.user` surfaces per D-03"; SC #5 (MULTITENANT.md) is the bulk of new work.

### Phase 87-91 Foundations (load-bearing for Phase 92)
- `web/safe_storage.py` — Phase 87 chokepoint. The single API for per-user state. Phase 92's MULTITENANT.md §7 tutorial cites this as the only safe API.
- `tests/test_no_raw_storage_access.py` — Phase 87 lint scanner. Permanent CI guard. Phase 92 documents this as the enforcement layer in MULTITENANT.md §8 (no new tests added).
- `.planning/phase87_storage_allowlist.yaml` — Post-Phase-91 state: `allowed_raw_access: []`. SWEEP-03 verifies this state + the empty-list comment.
- `tests/test_auth_callback_resilience.py` (Phase 91 AUTHW-05) — T-F locks the `set_auth(profile=None)` clears-stale semantics. MULTITENANT.md §7 warning callout cross-references this test.

### Codex Review Transcripts (SWEEP-04 input)
- `_tmp/codex_post_711_review_response.txt` — Round 1, 1c/1h/2m/3l/0n, recall verdict (pre-v7.12). Findings drove Phase 88 export-state separation + Phase 89 lists cache rewrite.
- `_tmp/codex_critical_high_review_response.txt` — Round 2, 2c/1h/0m/0l/0n, recall verdict. Findings drove Phase 87 chokepoint adoption + Phase 88 mirror deletion.
- `_tmp/codex_3rdpass_review_response.txt` — Round 3, 2c/1h/0m/1l/1n, recall verdict. Findings drove Phase 87 raw-access migrations + Phase 90 client-cache deletion.
- `_tmp/codex_4thpass_review_response.txt` — Round 4, 1c/2h/2m/1l/1n, "not deployable" verdict. Findings drove Phase 87 (parallels.py:3520 + text_editor.py auto-save) + Phase 90 set_session() prohibition + Phase 91 atomic auth writes.

### Phase 92 Round-1 External Review (Gemini)
- `_tmp/codex_phase92_discuss_review_prompt.md` — Claude's round-1 proposal sent to external review.
- `_tmp/gemini_phase92_discuss_review_response.txt` — Gemini round-1 verdict: 1 CRITICAL + 3 HIGH + 2 MEDIUM + 1 LOW → REFACTOR. All findings encoded as locked decisions D-02 through D-10 above.
- `_tmp/codex_phase92_err.txt` — Codex CLI quota-blocked until 2026-05-19 (`You've hit your usage limit`); Gemini was the symmetric fallback per the locked v7.12 external-review pattern.

### Source files audited (read-only for Phase 92)
- `web/auth_state.py` — Phase 91 migration target. SWEEP-01 verifies no raw access remains (independent AST scan during discuss-phase confirms).
- `web/main.py:complete_login` + `_oauth_complete_login` — Phase 91 migration target. SWEEP-02 spot-check.
- `web/pages/parallels.py:3510-3530` — Phase 87-06 deferred-restore callback. Independent grep during discuss-phase confirms `safe_user_get` at lines 3513-3516 (SWEEP-02 satisfied pre-emptively).
- `web/components/text_editor.py` — Phase 87-03 auto-save. Independent grep during discuss-phase confirms `safe_user_set` at lines 51, 67 + import at line 17 (SWEEP-02 satisfied pre-emptively).
- `shared/puzzle_service.py:1-100` — D-03/D-04 audit input. Independent read during discuss-phase confirms no `user_id`/`owner_id` columns. SWEEP-01 R3 conditional path.
- `web/analytics.py` — D-03 audit input. Independent read during discuss-phase confirms `posthog_capture` is JS injection only (no Python server-side state). Document in SWEEP-01 + MULTITENANT.md §8.

### Phase 88/89/90/91 Patterns (templates Phase 92 mirrors)
- `.planning/phases/87-foundations-session-uuid-and-safe-storage-chokepoint/87-08-ACCEPTANCE-AND-DOCS-PLAN.md` — closest precedent for Phase 92 Plan 92-01 (Task 1 was docs refresh, Task 2 was human smoke-check checkpoint). Phase 92 swaps the order (audit first, smoke last) and explicitly gates Plan 92-02 on smoke PASS.
- `.planning/phases/91-atomic-auth-state-writes/91-03-PLAN.md` — closeout-docs-only plan precedent. Phase 92 Plan 92-02 is similarly docs-only.
- All `.planning/phases/87-91/*-CONTEXT.md` files — read during Phase 92 Plan 92-02 to extract architectural rationale + Codex catch history for MULTITENANT.md content. The CONTEXT.md files preserve "why" history; MULTITENANT.md distills the steady-state architecture without dragging history into the contributor-facing doc.

### Hard constraints (carried from milestone)
- `feedback_no_background_webserver.md` — Smoke test server is human-driven; no `run_in_background` web server from Bash on Windows.
- `feedback_review_workflow.md` — Codex + Gemini CLIs are the external-review tooling; skip Claude CLI (we are Claude). Phase 92 used Gemini after Codex 429'd — symmetric inverse of Phase 91 round 2.
- `feedback_deploy_db_sync.md` — Server is at `v7.11.1` commit `242664d3`; `deploy.sh` BLOCKED until Phase 92 ships. Phase 92 completion unblocks `deploy.sh` AND requires deploy posture (scp DBs FIRST, then push code) per the 2026-05-11 incident.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `web/safe_storage.py` — THE chokepoint. Phase 92 doesn't touch it; only documents it in MULTITENANT.md §2.
- `tests/test_no_raw_storage_access.py` — Permanent CI guard. Phase 92 doesn't extend it (LOW-1 LOW-priority Gemini catch on `app.storage.browser/client` is documented, not lint-enforced).
- Phase 87/88/89/90/91 CONTEXT.md files — source material for MULTITENANT.md content. SWEEP-06 work is largely distillation.
- AST walker pattern from Phase 88 D-07 / Phase 90 D-15 / Phase 91 D-09 — Plan 92-01 SWEEP-01 reuses the AST walker shape for the independent audit snapshot (no new pattern invention).

### Established Patterns
- **Verification-not-discovery for SWEEP-01/02/03** — Phase 87-91 already did the migration work; Phase 92 produces evidence files documenting the state. AST scan + grep + manual non-`app.storage.user` surface audits = ~3-5 evidence files committed to `.planning/phases/92-*/`.
- **Atomic-commit discipline (Phase 89/90/91 pattern)** — Plan 92-01 commits all audit memos + smoke scaffold in one commit. Plan 92-02 commits MULTITENANT.md in another commit. The human-smoke commit between them is yours, not the executor's.
- **External-review symmetric fallback** — Phase 91 round 2 used Codex (Gemini 429'd); Phase 92 round 1 used Gemini (Codex 429'd). The CONTEXT.md cites whichever CLI actually returned a response, with the failure mode of the other recorded in `_tmp/`.

### Integration Points
- **`deploy.sh`** — Unblocked by Phase 92 milestone closure (verification + smoke + docs). Out-of-scope-but-imminent post-Phase-92 work.
- **`docs/OPEN_ISSUES.md`** — Phase 92 Plan 92-02 commits a closeout entry stating v7.12 Path B is complete; Phase 87 lint scanner remains the live enforcement layer. The MEM leak entry remains as a separate P1 followup (not part of v7.12 scope).
- **`docs/DOCUMENTATION_INDEX.md`** — Plan 92-02 adds `docs/guides/MULTITENANT.md` to the guides section of the index. Mechanical doc-index update.
- **`CLAUDE.md` "Recently Changed" section** — Plan 92-02 adds a Phase 92 / v7.12 closure entry. Compact one-paragraph entry citing milestone shipped + MULTITENANT.md as the reference doc.

### Why Gemini's CRITICAL Catch Matters (High-Value Insight)

My original proposal scoped SWEEP-01 narrowly to "audit `web/` for residual raw `app.storage.user` accesses." Gemini caught that this is a **scoping error**: the multitenant safety property is "no per-user state leaks across sessions sharing one Python process," which is broader than "no raw `app.storage.user` accesses." The Phase 87 lint scanner enforces a NECESSARY-but-not-SUFFICIENT subset of the property.

The four out-of-scanner surfaces Gemini called out:
1. **`shared/puzzle_service.py:joins.db`** — process-wide SQLite singleton. If it had per-user data (it doesn't; community-share), it'd be a leak surface invisible to the lint scanner.
2. **`web/analytics.py` PostHog** — server-side analytics client. If it cached `distinct_id` server-side (it doesn't; JS injection), it'd attribute all subsequent events to the first user.
3. **`app.storage.browser`** — known site at `create_login_dialog` (Phase 91 deviation). Cookies are browser-scoped, not Python-process-scoped, so no in-process leak — but worth documenting in MULTITENANT.md §8 so future contributors don't assume Phase 87 covers it.
4. **`app.storage.client`** — NiceGUI connection-scoped. Need to audit if any code uses it for per-user state.

My independent ground-truth checks during the discuss-phase preemptively closed (1) and (2) — they're verified non-leaking by inspection. (3) and (4) are documentation tasks. **The risk Gemini surfaced is real, but the actual implementation surface is verification + documentation, not migration.** This is why the verdict was REFACTOR (adjust scope) not RECALL (deliverables are still mostly verification + docs).

This is the milestone-closing equivalent of the Phase 91 F1 catch (composite-key RMW race): the original framing missed a class of issues; the fix is widening scope, not rewriting.

</code_context>

<specifics>
## Specific Ideas

- **User direction (locked across Phases 88/89/90/91/92):** Out of 4 gray areas presented (audit shape, smoke test, MULTITENANT.md scope, Codex red-team), the user selected ONLY "External Codex red-team round on the plan." Same exclusive-delegation pattern as the four prior v7.12 phases. The pattern is locked for the milestone.

- **Codex CLI quota outage as a milestone-closure event:** Codex CLI returned `You've hit your usage limit. To get more access now, send a request to your admin or try again at May 19th, 2026 1:25 PM.` Gemini CLI succeeded (`gemini@0.42.0`). This is the symmetric inverse of Phase 91 round 2 (Codex worked / Gemini 429'd). Documenting both states in canonical_refs preserves the audit trail.

- **Gemini's CRITICAL catch reframes Phase 92 scope correctly:** My original "verify zero raw `app.storage.user`" framing collapsed Phase 92 to a near-no-op (lint scanner already enforces it). Gemini's "widen to 5 surfaces" framing restores real audit work for SWEEP-01 — but it's still all VERIFICATION (no migrations, no production-code changes). The work is `wc-l`-sized: ~5 audit memos + 1 smoke checklist + 1 MULTITENANT.md doc + 1 OPEN_ISSUES/CLAUDE.md/DOCUMENTATION_INDEX closeout touch.

- **Gemini's 3 HIGH findings each have a non-obvious failure mode:**
  - **Cross-transcript correlation:** A linear transcript walk would have marked the lists-cache-RMW finding closed in round-1 + round-3 and missed that round-4 re-flagged a different angle of the same root cause. The thematic walk forces Claude to dedupe by root issue before declaring resolution.
  - **Token refresh + logout race in smoke:** Without R1/R2, the smoke test would pass even if Phase 90's `_session_uuid`-keyed refresh lock had a bug. R1/R2 exercise the actual concurrency primitives.
  - **`profile=None` clears-stale-profile semantics:** Without the §7 warning callout, a new contributor extending the auth surface would call `set_auth(user, profile=None)` thinking "no change" and silently nuke their session's profile cache. The warning is the cheapest UX mitigation.

- **Gemini's MEDIUM commit-hash-vs-phase-pointer call is forward-looking:** `.planning/` files may be archived or restructured over years; `git log` is permanent. Using git short hashes in SWEEP-04 evidence column is the right durability call.

- **Phase 92's strategic position in v7.12:** After Phase 92 ships, `deploy.sh` unblocks. The server moves from `v7.11.1` (commit `242664d3`) forward to the Phase 92 closeout commit. The MULTITENANT.md doc + the live lint scanner together close the milestone for the next contributor. Web-only milestone — no desktop work, no tag, no GitHub Release (per `feedback_no_github_release_for_web_only.md`).

- **Survival post-Phase-92** (Gemini's equivalent to Phase 91 "Surviving threats"):
  - **`app.storage.browser` / `client`** — documented but not lint-enforced. New contributors could add per-user data to these surfaces inadvertently. **Mitigation:** MULTITENANT.md §8 explicitly cites these as "outside Phase 87 scanner; audit manually on any new code touching them." Acceptable.
  - **`joins.db` ownership semantics could change** — if a future phase adds per-user puzzle ownership to local joins.db (today it's community-share), the same multitenant safety questions arise. **Mitigation:** MULTITENANT.md §6 deletion-not-migration discipline section calls out joins.db explicitly. Future contributor's PR would re-trigger the audit.
  - **External APIs (Supabase, PostHog) cache PII server-side** — Supabase clients are throwaway-per-request (Phase 90); PostHog is JS-only (verified). New API integrations could regress this. **Mitigation:** MULTITENANT.md §4 cites the Codex `set_session()` finding + throwaway-client discipline.

</specifics>

<deferred>
## Deferred Ideas

- **Round-2 Codex review post-2026-05-19** — Codex quota refreshes May 19. If a residual issue surfaces after Plan 92-01 ships, a post-plan Codex round could be dispatched. Phase 92's locked-decision "one round only" stands; only re-open if a concrete blocker appears.

- **Widening Phase 87 lint scanner to cover `app.storage.browser` / `client`** — Tempting but **deferred** (LOW-priority Gemini catch). These have different leak semantics (cookies are browser-scoped, client storage is connection-scoped). A future scanner extension could enforce "no PII in `app.storage.browser`," but the AST shape would need a new pattern matcher. Defer until a concrete PII leak surfaces.

- **Per-user data warehouse / RLS audit on joins.db** — If joins.db ever gains per-user puzzle ownership (currently community-share), it would need an audit equivalent to Phase 88/89's state-separation discipline. Defer until product direction adds per-user puzzle storage local-side (today's user-puzzle ownership lives entirely in Supabase RLS-protected tables).

- **`MULTITENANT.md` as live executable doc** — Could ship with embedded code snippets that get type-checked / lint-checked on CI. Adds maintenance overhead; doc churn would slow contribution. Defer until the doc starts bitrotting in practice.

- **Server-side smoke automation** — Could write a Playwright/Selenium harness that automates R0/R1/R2/R3. Conflicts with `feedback_no_background_webserver.md` (no Bash-spawned web server). Defer until tooling matures or feedback changes.

- **Lessons-learned appendix** — Explicitly rejected per locked decision. `_tmp/codex_*.txt` + `.planning/phases/87-92/*-CONTEXT.md` already preserve milestone history. A contributor wanting "why did this happen?" reads CONTEXT.md; a contributor wanting "what's the current state?" reads MULTITENANT.md. Clean separation.

- **Cross-process safety for horizontal scaling** — Out of scope per REQUIREMENTS.md; if production traffic justifies multi-Uvicorn-process scaling, a new milestone (v7.13?) handles cross-process state. Not a Phase 92 deliverable.

- **v7.12.0 release tagging strategy** — Phase 92 closure unblocks `deploy.sh`; the release-management decision (tag? GitHub release? web-only-no-tag per `feedback_no_github_release_for_web_only.md`?) is separate post-milestone work. Web-only Path B has no desktop installer, so no GitHub release.

- **Round-3+ Codex / Gemini review** — Phase 92 is the closing phase; further external review beyond round-1 is not contemplated. If Plan 92-01 + Plan 92-02 ship clean, the milestone closes.

</deferred>

---

*Phase: 92-final-sweep-and-acceptance*
*Context gathered: 2026-05-15*
*Workflow note: This CONTEXT.md captures decisions refined by **one round** of Gemini external review (round 1 dispatched as the symmetric fallback after Codex CLI returned `usage limit` until 2026-05-19; mirrors Phase 91 round 2's "Gemini 429 → Codex only" inverse). Gemini round-1 verdict: 1 CRITICAL + 3 HIGH + 2 MEDIUM + 1 LOW → **REFACTOR**. All findings encoded as locked decisions D-02 through D-10. The CRITICAL catch (multitenant leaks happen wherever global state touches disk or external APIs — not just `app.storage.user`) widened SWEEP-01 from 1 surface to 5; independent ground-truth checks during discuss-phase preemptively resolved (puzzle joins.db = community-share, no user-ownership) and (PostHog = JS injection, no Python state). HIGH catches drove thematic-walk methodology for SWEEP-04 (D-05), R1 logout-race + R2 refresh-race smoke scenarios (D-08), and the §7 `profile=None` warning callout (D-09). MEDIUM catches drove git-commit-hash evidence (D-07) and explicit human-gating between Plan 92-01 and Plan 92-02 (D-02). No round-2 dispatched — surface after refinements is verification + docs (no production code), low residual risk.*
