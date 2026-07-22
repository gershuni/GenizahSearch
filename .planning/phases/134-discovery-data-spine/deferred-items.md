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

## 134-04 — evidence_id collision: shared_text vs family-router same-span (auto-fixed defensively; FROZEN recipe gap flagged for a future dated amendment)

**Found during:** 134-04 Task 2/3, in a real dev-box smoke build of
`finalize_build` run against the actual gitignored research corpus
(`fullcorpus_v2.db` + the real Q2/E1 collections; open-corpus-only approved
set, 625 works / 231,604 claims / 252,091 candidate evidence rows before
dedup). This is NOT reachable from the committed synthetic fixture or the
`tests/test_discovery_build.py` unit fixtures on their own — it only
surfaced once real data was run through the pipeline, which is exactly why
this plan attempted a real end-to-end smoke build in addition to the
required synthetic-fixture-safe unit tests.

**Issue:** 115 of 252,091 candidate evidence rows (0.046%) collided on
`evidence_id` — a plain `q2_shared_text.jsonl` row and a family-router
(`q2_collection_tafsir_targum.jsonl`/`q2_collection_with_arabic.jsonl`) row
for the SAME `(cpage, work_id)` independently resolved to the IDENTICAL
`(work_id, a_page_id, sys_id, evidence_kind=shared_text,
evidence_source=propagated, confidence_band=not_evaluated, span_start,
span_end, other_page_id)` tuple — the exact frozen input to
`discovery_ids.evidence_id()`. The FROZEN evidence_id recipe
(`docs/specs/discovery-sidecar-schema-v1.md` SS2) has no "which source
collection" discriminator by design (it was never meant to distinguish two
DIFFERENT collections landing on the same resolved primary span), so this
is a real-data gap in the frozen contract, not a bug in this plan's
implementation of that contract. Left unhandled, the `UNIQUE(claim_id,
evidence_id)` constraint on `discovery_evidence` rejects the second insert
and crashes the build.

**Action taken (Rule 1/3 boundary — auto-fixed the BUILD-SIDE symptom,
never touched the FROZEN `discovery_ids.py` recipe):** `assemble_claims_and_evidence`
now deduplicates on `evidence_id` (the table's actual PK) before insert:
when two evidence specs collide, it deterministically keeps the `shipped`
row over a `review_only` one (never let a co-citation-only signal silently
displace a shipped recall-widening row), else the first-seen row. The
collision count is returned (`evidence_id_collisions` in `finalize_build`'s
stats / the real run printed `evidence_id_collisions=115`) so it stays
visible, never silent. A regression test
(`test_evidence_id_collision_shared_text_vs_family_router_prefers_shipped`)
pins this behavior against a small synthetic case. Re-ran the real build
after the fix: succeeded (625 works / 231,604 claims / 251,976 deduped
evidence rows / 5,547 witness units), passed `verify_discovery_sidecar.py`
clean, and passed the BLOCKING `check_atlas_masking.py --scan-sqlite` gate
with the real `.masking_patterns` file (0 hits).

**Deferred (schema-level, NOT resolved here — the frozen recipe itself is
untouched):**
- Whether the 134-07 real re-distill (or an earlier schema-amendment plan)
  should extend the FROZEN `evidence_id()` recipe with an explicit
  discriminator (e.g. folding `router_bucket`/collection-source into the
  hash input) so a shared_text-vs-family-router collision never needs a
  build-side dedup heuristic at all. Any such change requires a NEW dated
  amendment section in `docs/specs/discovery-sidecar-schema-v1.md` per its
  own closing note ("any correction requires a new dated amendment
  section... never a silent edit") — explicitly NOT done in this plan.
- Whether "prefer shipped over review_only" is the right precedence in
  EVERY case, or whether the owner would rather see both signals
  represented some other way (e.g. via a future non-PK-colliding evidence
  model). Flagging for 134-07 owner review alongside the neutral-title
  curation gate.

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
