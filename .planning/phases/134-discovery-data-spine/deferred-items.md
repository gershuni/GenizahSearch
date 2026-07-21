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
