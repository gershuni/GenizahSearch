---
phase: 92
reviewers: [gemini, codex]
reviewed_at: 2026-05-16T22:28:00Z
plans_reviewed: [92-01-PLAN.md, 92-02-PLAN.md]
runtime_env: claude-code-cli (CLAUDE_CODE_ENTRYPOINT=cli — claude self-review skipped for independence)
---

# Cross-AI Plan Review — Phase 92

## Gemini Review

# Phase 92 Plan Review: Final Sweep and Acceptance

## 1. Summary
The implementation plans for Phase 92 are exceptionally well-structured, focusing strictly on verification, evidence production, and canonical documentation without modifying production code. The strategy successfully incorporates all high-value findings from the Gemini round-1 review, particularly the widening of the audit scope to 5 surfaces and the thematic "unique-issue" walk for the Codex review transcripts. The human-gating mechanism between plans is robustly implemented via an automated pre-flight script that verifies both file state and git history. These plans provide a high-confidence path to closing the v7.12 Multitenant Architecture milestone.

## 2. Strengths
- **Comprehensive Audit Scope (D-03):** Plan 92-01 Task 2 explicitly covers the 5 surfaces (user/browser/client storage, `joins.db`, and PostHog) identified as potential leak vectors, moving beyond the narrow focus on `app.storage.user`.
- **Robust Gating (D-02):** Plan 92-02 Task 0 uses a sophisticated Python script to enforce the human-gating commit. It checks for specific "checked" markdown boxes (`[x]`) and verifies `git log` to ensure the smoke test results were actually committed by the user.
- **Thematic Methodology (D-05):** The issue-keyed audit in `92-SWEEP-04-TRANSCRIPT-AUDIT.md` (Task 3) prevents the "false resolution" trap of a chronological walk and provides a clear lifecycle map of every finding.
- **Evidence Durability (D-07):** The requirement for git short commit hashes in the transcript audit ensures the documentation remains useful for future developers who may not have access to ephemeral process artifacts.
- **Scaffold Completeness (D-08):** The smoke test checklist includes critical concurrency races (logout-mid-flight and token-refresh) that exercise the specific primitives implemented in Phase 90.
- **Principle of Least Astonishment (D-09):** The bright-red warning callout regarding `profile=None` semantics in `MULTITENANT.md` §7 is well-placed to prevent future contributor errors.

## 3. Concerns

- **Transcript Token Volume (Task 3):** 
  - **Severity: MEDIUM**
  - **Risk:** Reading 4 long Codex transcripts in full to identify 23 unique issues may be turn-intensive or push context limits.
  - **Mitigation:** The plan already suggests using a heuristic keyword search (`python -c` in Task 3 Step 1) to identify severity blocks, which is a good optimization.

- **MULTITENANT.md Word Count:** 
  - **Severity: LOW**
  - **Risk:** Generating a ~2500-3000 word document in a single `write_file` call may be slow or hit output truncation limits for some agents.
  - **Mitigation:** The agent should be prepared to use `replace` or multiple turns if the initial write is truncated, though modern large-context models usually handle 15-20KB of output.

- **R3 Conditional Logic:**
  - **Severity: LOW**
  - **Risk:** There is a slight dependency between the Surface 4 verdict in Task 2 and the scaffold marking in Task 4.
  - **Mitigation:** Plan 92-01 Task 4 explicitly instructs the executor to consult the Surface 4 verdict before deciding the R3 path, which is correct.

## 4. Suggestions
- **Automated Commit Verification:** In Plan 92-01 Task 5, the post-commit verification (`git diff --name-only`) is a great practice. It should specifically assert that the allowlist YAML was NOT changed, as Phase 91 already took it to zero.
- **Session UUID Visibility:** In the smoke test (R0), suggesting the user check `.nicegui/storage-user-*.json` files is an excellent "ground truth" check for Windows local development, as it avoids browser DevTools complexity.
- **Internal Link Check:** Ensure `python scripts/check_docs.py` is run *after* `MULTITENANT.md` is added to the index, as Task 2 correctly plans.

## 5. Risk Assessment
- **Overall Risk: LOW**
- **Justification:** The plans involve zero production code changes, significantly reducing the risk of regressions. The heavy emphasis on automated AST scanning and deterministic behavioral tests (from prior phases) combined with the widened human smoke test provides a solid safety net. The gating between plans ensures that no documentation is published for a state that hasn't been verified by a human operator.

---
**Verdict: Approved.** Proceed with Plan 92-01.

---

## Codex Review

## Summary

Both plans are strong in intent: they respect Phase 92 as verification + documentation only, carry forward the Gemini-driven scope widening, and encode the human smoke-test boundary between 92-01 and 92-02. The main risks are not architectural, but procedural: a few acceptance checks can pass while the underlying requirement is not actually satisfied, and both plans contain internal scope contradictions around summary files. I would revise those before execution.

## Strengths

- The 5-surface SWEEP-01 expansion is correctly reflected: `app.storage.user`, `app.storage.browser`, `app.storage.client`, `joins.db`, and PostHog are all covered.
- SWEEP-04 uses the right issue-keyed methodology rather than a transcript-by-transcript walk.
- D-07 is well handled in principle: git short hashes are required and verified.
- The smoke plan covers the important concurrency classes: baseline isolation, logout race, token refresh race, and conditional puzzle write.
- Plan 92-02 correctly gates documentation on human smoke PASS.
- The MULTITENANT.md outline is useful and contributor-oriented, especially §7 and §8.

## Concerns

- **HIGH: Plan 92-01 and 92-02 both contradict their own strict scope.**  
  Each plan's `<output>` says to create `92-01-SUMMARY.md` / `92-02-SUMMARY.md`, but the files_modified lists and post-commit checks require exactly 4 and exactly 6 files respectively. Execution will either fail scope checks or omit the requested summaries.

- **HIGH: SWEEP-04 acceptance can pass without accounting for all 23 findings.**  
  Requiring `>=8` issue sections and the raw count table does not prove every raw Codex finding was mapped to an addressed/waived issue. This is weaker than SWEEP-04's "no issue left silently unaddressed" requirement.

- **HIGH: Plan 92-02 gate can be fooled by template text.**  
  Regex like `Overall:\s*PASS` will match a checked template line such as `- [x] Overall: PASS / FAIL -- _________`. Same for `R0 PASS / FAIL`. It does not prove the tester selected PASS or filled evidence.

- **HIGH: Plan 92-02 gate allows R3 FAIL.**  
  The regex accepts `R3 FAIL`; if `Overall: PASS` is present, the gate can proceed despite a failed conditional scenario. R3 should allow only `PASS` or `N/A`.

- **HIGH: The smoke gate does not prove the PASS checklist was committed separately.**  
  The action prints recent commits but relaxes to current file state. That weakens D-02's locked human-gating commit boundary.

- **HIGH: SWEEP-01 puzzle audit should inspect the actual SQLite DB, not only source schema.**  
  `CREATE TABLE IF NOT EXISTS` in `shared/puzzle_service.py` does not prove the live `joins_data/joins.db` schema lacks user columns. Add `PRAGMA table_info(...)` against the actual DB.

- **MEDIUM: Plan 92-01 says it closes SWEEP-05, but it only creates the scaffold.**  
  SWEEP-05 is fully closed only after Hillel executes the smoke and commits PASS. The plan should say it prepares SWEEP-05 and the human commit closes it.

- **MEDIUM: The `app.storage.browser/client` audit uses literal grep only.**  
  That can miss aliases like `nicegui_app.storage.browser`. For consistency, use the same AST alias detection pattern as the `app.storage.user` scan.

- **MEDIUM: Roadmap SC #1 explicitly asks for grep evidence.**  
  The AST scan is stronger, but the plan should also archive a literal `rg "app\.storage\.user" web/` result, excluding/identifying `web/safe_storage.py`, to match the stated criterion.

- **MEDIUM: R2 smoke scenario may be hard to execute honestly.**  
  "Lower JWT TTL on a side Supabase project OR wait until expiry" is operationally vague. If R2 is a hard gate, provide deterministic steps.

- **MEDIUM: Fixed `2026-05-15` dates may now be stale.**  
  Current execution context is 2026-05-16. If executed now, artifact dates, "Last Updated," and milestone closeout dates should reflect the actual run date or explicitly say the milestone date is historical.

- **MEDIUM: D-09 warning text appears logically inconsistent.**  
  It says "If you want no change, do not pass `profile=` at all," but the signature default is `profile=None`, and `None` clears. That is not "no change." This should be clarified before being made canonical.

## Suggestions

- Add `92-01-SUMMARY.md` and `92-02-SUMMARY.md` to `files_modified`, artifact checks, and commit scope, or remove those output requirements.
- For SWEEP-04, require a raw-finding appendix with one row per finding: transcript, severity, short title, mapped issue slug, disposition. Verify count equals 23.
- Strengthen the smoke gate:
  - Assert no uncommitted changes to `92-SWEEP-05-SMOKE.md`.
  - Assert at least two commits touched the file.
  - Require `R0: PASS`, `R1: PASS`, `R2: PASS`, and `R3: PASS|N/A` after the colon, not just checked template labels.
  - Reject any `FAIL` in final disposition.
- Add actual SQLite checks for `joins_data/joins.db`, for example `PRAGMA table_info(join_documents)` and `PRAGMA table_info(join_document_fragments)`.
- Add alias-aware AST scans for `app.storage.browser` and `app.storage.client`.
- Add a literal grep artifact for `app.storage.user` to satisfy the roadmap wording.
- Rewrite the §7 warning to say: "There is no no-change profile mode in `set_auth`; omitting `profile` also clears because the default is `None`."
- Make all closeout dates either dynamic at execution time or explicitly historical.

## Risk Assessment

**Overall risk: MEDIUM.**

The technical direction is sound and unlikely to require production code changes. The main risk is that the evidence trail can falsely pass: SWEEP-04 can miss findings, the smoke gate can accept ambiguous checked template rows, and the `joins.db` audit may inspect source instead of the live database. Fixing those is straightforward and stays within the verification/docs-only boundary.

---

## Consensus Summary

### Agreed Strengths
- 5-surface SWEEP-01 scope widening per D-03 is correctly implemented (both reviewers).
- Thematic issue-keyed SWEEP-04 walk (D-05) is the right methodology (both).
- Smoke scaffold covers the right concurrency classes — R1 logout race + R2 refresh race + R3 conditional puzzle (both).
- Plan 92-02 correctly gates documentation on human smoke PASS (both).
- §7 WARNING callout in MULTITENANT.md is well-placed (Gemini); D-07 git short hashes durable (both).

### Agreed Concerns
- **Smoke gate robustness (HIGH, raised by Codex only):** Gemini called the gate "robust"; Codex found 3 attack vectors — template-text false positives (`[x] Overall: PASS / FAIL` matches the unfilled template), R3 FAIL acceptable while Overall PASS, and missing assertion that the PASS checklist was committed as a separate commit by the human. Gemini's review didn't notice these regex weaknesses. **Codex is correct.** The Python verification script must:
  - Parse the checklist body proper, not just regex over the file.
  - Require all `R*: PASS` answers AFTER the colon (not just labels in checkbox-template lines like `[x] R0 PASS / FAIL: ____`).
  - Reject any `FAIL` selection in final disposition.
  - Assert at least 2 commits touched `92-SWEEP-05-SMOKE.md` AND no uncommitted changes remain.
  - Force `R3: PASS|N/A` (never `R3: FAIL` accepted).

### Divergent Views
- **Gating mechanism quality:** Gemini said "robust" (LOW risk); Codex said "can be fooled by template text" (HIGH risk). **Codex's specific examples (regex matches the unfilled template line `[x] Overall: PASS / FAIL -- _____`) are concrete and verifiable. Apply Codex's strengthening.**
- **Overall risk:** Gemini = LOW; Codex = MEDIUM. The divergence is procedural-rigor (Codex) vs architectural-soundness (Gemini). Both can be true: the architecture is sound (Gemini) but the evidence trail has procedural fragility (Codex). **Both must be addressed.**

### Codex-Only Concerns Worth Acting On (Gemini didn't surface these)

1. **SUMMARY.md scope contradiction (HIGH):** Each plan's `<output>` block says to create a SUMMARY.md, but the `files_modified` field and the post-commit check explicitly list exactly 4 (92-01) / exactly 6 (92-02) files which EXCLUDE the SUMMARY.md. The executor will face a contradiction. **Fix:** add `92-01-SUMMARY.md` (and `92-02-SUMMARY.md`) to `files_modified` AND the post-commit assertion sets, OR remove the `<output>` SUMMARY requirements.

2. **SWEEP-04 ≥8-issue check is weaker than the requirement (HIGH):** The phase requirement says "no issue left silently unaddressed" (all 23 findings). A `>=8 issue sections` acceptance criterion can pass while leaving 15 raw findings unmapped. **Fix:** require a raw-findings appendix with one row per finding (transcript, severity, short title, mapped issue slug, disposition); verify `count == 23` (or whatever the actual deduplicated raw count is after thematic walking).

3. **joins.db live-schema check, not just source schema (HIGH):** `CREATE TABLE IF NOT EXISTS` in source doesn't prove the running DB has no user_id columns (column-add migrations could exist outside of the source CREATE). **Fix:** add `PRAGMA table_info(join_documents)` and `PRAGMA table_info(join_document_fragments)` against `joins_data/joins.db` to the SWEEP-01 Task 2 Surface 4 audit; archive the output.

4. **app.storage.browser/client should use AST alias detection (MEDIUM):** Literal grep misses aliases like `from nicegui import app as nicegui_app; nicegui_app.storage.browser.get(...)`. Same code pattern as the existing user-scope AST walker; trivial reuse.

5. **Literal `rg "app\.storage\.user" web/` evidence (MEDIUM):** Roadmap SC #1 explicitly states grep evidence. The plan has AST evidence (stronger) but should ALSO archive the literal-grep output to satisfy the SC's literal text.

6. **R2 smoke scenario operationally vague (MEDIUM):** "Lower JWT TTL on a side Supabase project OR wait until expiry" is ambiguous. The smoke scaffold should include a deterministic R2 procedure (e.g., specific steps with example dashboard URLs/screenshots or a code snippet for JWT TTL override in dev).

7. **2026-05-15 dates are now stale (MEDIUM):** Current run date is 2026-05-16. Audit memo dates, MULTITENANT.md "Last Updated," and Phase 92 closeout entry dates should resolve to the actual execution date OR be explicitly marked historical.

8. **D-09 §7 warning text is logically inconsistent (MEDIUM):** The callout says "If you want 'no change,' do not pass `profile=` at all — the default is `None` AND it intentionally clears." But omitting `profile=` defaults to `None` which CLEARS. So "do not pass it" still clears — that's not "no change." **The truth is: `set_auth` has no "leave existing profile" mode.** Rewrite the callout's prescriptive sentence to: "**There is no 'no change' mode for `profile` in `set_auth`** — omitting it AND passing `profile=None` both clear `auth_profile`. To replace the profile, pass the new dict explicitly. To preserve an existing profile across calls, do not call `set_auth` at all (call `set_user(...)` if you have a narrower update path, or read+pass the existing dict back)."

## Recommendation

Codex's HIGH findings are substantive and actionable without redesigning the phase. Recommend running `/gsd-plan-phase 92 --reviews` to incorporate the 8 numbered concerns above (4 HIGH + 4 MEDIUM that align with the phase scope). Gemini's 3 LOW suggestions (allowlist negative assertion, NiceGUI storage-user JSON ground-truth hint, check_docs ordering) are nice-to-haves and can be folded into the same revision pass.
