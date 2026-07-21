# Deferred Items — Phase 134

Out-of-scope discoveries logged during plan execution (not fixed inline per
the executor's scope-boundary rule, except where noted).

## 134-01 Task 1 — pre-existing masking leak in an untracked scratch log (fixed as a blocking exception, systemic risk deferred)

**Found during:** 134-01 Task 1, while running the required
`check_atlas_masking.py --scan-repo` gate.

**Issue:** `tmp/codex_134_confirm.log` (an untracked Codex CLI session
transcript from the phase-134 plan-review process, pre-dating this
execution session) contained one literal restricted-corpus-name mask hit —
a printed Python f-string source snippet from an unrelated research script
that used the real corpus name as a display label. `tmp/` is **not**
gitignored, so this and the many sibling `tmp/codex_*`/`tmp/*.log` files
sit as untracked-but-visible-to-git-status content.

**Action taken (exception to the scope-boundary "do not fix unrelated
files" rule):** this single leak directly blocked the Task 1 `--scan-repo`
gate that this plan's own verification step mandates, and CLAUDE.md treats
the M-source/R-source masking rule as a hard constraint — so it was
redacted in place (the real name replaced with the "M-source" codename)
rather than left red. No git history is affected (the file was never
committed). Re-ran the gate after the fix: exit 0.

**Deferred (systemic, NOT fixed here):**
- `tmp/` is not in `.gitignore` — any future `git add -A` (prohibited by
  this project's own commit protocol, but a real footgun for a
  less-careful session) could commit a similar leak from a Codex/Gemini CLI
  transcript. Recommend either gitignoring `tmp/**/*.log` or running the
  masking scan as a documented step at the end of every Codex-CLI-assisted
  planning/review session (mirrors the existing "M-source codename rule"
  and "R-source codename rule" memory entries about scrubbing uncommitted
  leftovers before commit).
- No systematic sweep of the OTHER `tmp/codex_*` / `tmp/*.log` files was
  performed — only the one instance that broke this plan's own gate was
  addressed. A dedicated `/gsd-quick` or manual `--scan-repo` pass over the
  full `tmp/` tree is recommended before any of those files are ever
  staged.

## 134-02 — pre-existing masking leaks recur in untracked `tmp/` Codex-review scratch files (NOT fixed; systemic risk from 134-01 has recurred a 2nd time)

**Found during:** 134-02's own required verification step
(`check_atlas_masking.py --scan-repo`), run to confirm this plan's new/modified
committed files (`scripts/check_atlas_masking.py`, `tests/test_masking_sqlite.py`,
`docs/specs/discovery-budgets.md`, `.gitignore`) are masking-clean.

**Issue:** the full `--scan-repo` invocation reported 19 hits, ALL inside two
UNTRACKED Codex-CLI review-transcript files that pre-date this execution
session and are unrelated to this plan's task list: `tmp/CODEX-REVIEW-134-replan-r2.md`
and `tmp/CODEX-REVIEW-134-replan-r3.md` (both are `tmp/` review artifacts from
the 134-CONTEXT.md re-plan/owner-gate Codex-review rounds). `tmp/` is still
**not** gitignored (the exact gap the 134-01 entry above already flagged), so
these sit as untracked-but-visible-to-`git status` content.

**Scope-boundary determination:** these two files are (a) not part of this
plan's `files_modified` list, (b) large multi-hundred-KB/MB review transcripts
whose flagged byte offsets sit inside free-text critique prose (not a
small, easily-isolated literal like the 134-01 f-string case), and (c) never
committed (no git-history exposure). Per the executor's scope-boundary rule
("only auto-fix issues directly caused by the current task's changes"; Rule
priority favors NOT touching unrelated files when the fix is non-trivial and
outside task scope), these were **NOT edited/redacted** in this session — an
in-place redaction of large freeform review text, done without directly
viewing the matched restricted string (by design -- the executor never reads
`.masking_patterns` or the flagged byte spans directly, to avoid pulling
restricted content into its own context), carries real risk of an incomplete
or corrupting edit.

**Confirmed clean instead:** this plan's own 4 deliverables were verified
individually clean via `--scan-asset` on each file (not just inferred from the
combined `--scan-repo` report):
`scripts/check_atlas_masking.py`, `tests/test_masking_sqlite.py`,
`docs/specs/discovery-budgets.md`, `.gitignore` — all `no matches -- clean`.

**Recommended action (systemic, escalates the 134-01 recommendation --
2nd occurrence now confirmed):**
- Gitignore `tmp/**/*.md` and `tmp/**/*.log` (or the whole `/tmp/` tree) so
  Codex/Gemini CLI review transcripts can never accidentally enter a
  `git add -A` (already prohibited by this project's commit protocol, but a
  real footgun for a less-careful session or a future contributor).
- OR: run `check_atlas_masking.py --scan-repo` (or a `tmp/`-scoped
  `--scan-asset tmp/` pass) as a documented step at the end of every
  Codex-CLI-assisted planning/review session, and delete/redact flagged
  scratch files before the next session starts.
- The owner should manually redact or delete
  `tmp/CODEX-REVIEW-134-replan-r2.md` / `tmp/CODEX-REVIEW-134-replan-r3.md`
  (or move them outside the repo tree) at their convenience -- they were left
  untouched by this session for the reasons above, not because they are safe.

## Future work — catalog/title/FGP-identity propagation (owner-flagged 2026-07-21, NOT built)

A THIRD work-identification mechanism the SEED-029 spike never investigated,
surfaced by the owner at the 134-01 contract-correction gate: use a MS-to-MS
(Track-2) connection + an EXTERNAL catalog/title/FGP identity on a connected
fragment to identify works that are NOT in the reference corpus (potentially
very prolific — most Genizah works). Q2-as-built only propagates identities
that originate in Track-1 (work must be in the reference corpus); this pathway
does not. RESERVED in the v1 contract as a future `evidence_source =
catalog_propagated` (added via a versioned rebuild, never a v1 migration —
see 134-CONTEXT C-9). Documented in the `project_seed029_catalog_identity_propagation`
memory. Candidate future spike/phase (v9 milestone 135-139); does NOT ship in 134.
