---
phase: 136-read-surfaces-connections-panel-work-witnesses
plan: 04
subsystem: discovery
tags: [novelty, shade-enum, llm-gate, funnel, grading-harness, sqlite, gemini]

# Dependency graph
requires:
  - phase: 136-03
    provides: the ten-value shade enum rulings (E/E'/F/G/H/I/J) + the owner-labelled 101-case ground-truth file (discovery_data/novelty_hardcase_labels-v1.json) and its recorded content hash
provides:
  - shared/discovery_novelty.py -- the ten-value shade enum + pinned LLM contract + masked provenance + verdict->column mapping + alias-aware identity key
  - docs/specs/discovery-novelty-v1.md -- the canonical, single-cited novelty contract
  - scripts/discovery_novelty_funnel.py -- the funnel-first heuristic pass + checkpointed model arm + owner-label grading harness
  - A documented, environment-accurate DEFERRAL of the ruling-I re-measurement and the production run (136-NOVELTY-RUN.md), distinguishing an execution-environment gap from an owner decision
affects: [136-12, 136-13, 136-15, 136-16, 136-17, 136-18]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Self-hashing prompt/input-normalization pins: PROMPT_SHA256/INPUT_NORMALIZATION_SHA256 computed at import time from the literal template/spec strings, never a hand-copied hex digest"
    - "Allowlist masking (never a denylist): masked_provenance_label can only ever return one of a small set of pre-written strings, structurally incapable of echoing its input"
    - "Funnel-first heuristic pass: a mechanical name-match test applied uniformly across every checked source's own free text, never deciding anything beyond confirms/unresolved -- richer shades are the model's job alone over the residual"
    - "Checkpointed, resumable model arm: JSONL checkpoint flushed after every call, keyed by (sys_id, ref_work_id), so a killed-and-restarted run never re-bills completed work"
    - "Grading harness fail-closed denominator guard, manually mutation-tested (guard removed, dedicated tests observed to fail with DID NOT RAISE, guard restored) rather than trusted on a bare pytest.raises(Exception) assertion"

key-files:
  created:
    - shared/discovery_novelty.py
    - docs/specs/discovery-novelty-v1.md
    - scripts/discovery_novelty_funnel.py
    - tests/test_discovery_novelty_contract.py
    - .planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-NOVELTY-RUN.md
  modified: []

key-decisions:
  - "The heuristic (mechanical) funnel pass can ONLY ever resolve to confirms or leave a row unresolved (residual) -- it never itself decides diverges_work/diverges_part/refines_granularity/aid_more_specific/alias_merge/container_predicts/extends, all of which require judgment beyond string matching and are reserved for the model over the residual"
  - "Mere source PRESENCE (a bibliography row exists; a PGP description exists) is never decisive by itself -- an actual textual name-match against the source's own free text is required, fixing Codex findings 1/6 (published_full/bare-PGP over-demotion) at the design level, not merely by citation"
  - "Task 3's real re-measurement and production run could NOT execute in this isolated parallel-worktree environment (the owner-labelled ground truth file and every real checked-source sidecar are gitignored artifacts absent from this worktree; no LLM provider credentials are configured here) -- documented as an environment gap, explicitly distinguished from an owner decision or a declined authorization, using the plan's own permitted 'explicitly marked deferred, with a stated reason' path"
  - "NOVEL-01/NOVEL-02 are NOT marked complete in this plan -- the contract, the funnel and the grading harness are built and tested, but no real claim in the shipped asset yet carries a computed novelty_status; that ships via 136-12's build wiring, gated on the still-open ruling-I re-measurement"

requirements-completed: []

# Metrics
duration: ~2h (single continuous session)
completed: 2026-08-03
---

# Phase 136 Plan 04: Novelty Axis -- Ten-Value Shade Enum, Pinned LLM Contract, Funnel & Grading Harness Summary

**Built the novelty contract module (ten-value shade enum, masked provenance, alias-aware identity, self-hashing pinned LLM contract), its canonical spec doc, and a committed funnel-first heuristic-pass + checkpointed-model-arm + owner-label grading harness that structurally avoids the reference implementation's Codex-flagged over-demotion defects -- but could not execute Task 3's real re-measurement or production run in this isolated environment, which lacks the owner label file, every real sidecar, and any LLM credentials; that gap is documented, not worked around.**

## Performance

- **Duration:** ~2h (single continuous session, 2026-08-03)
- **Tasks:** 3 of 3 (all `type="auto"`, no checkpoints in this plan by design)
- **Files modified:** 5 (4 created, all in Tasks 1-2; 1 created in Task 3)

## Accomplishments

- **`shared/discovery_novelty.py`** -- the ten-value shade enum (`NOVELTY_STATUSES`, per owner rulings E/E'/F/G/H), fail-closed `DEFAULT_STATUS = 'not_checked'`, the candidate/hidden-by-default/source-label-eligible predicates, the `divergence_correctness` sibling vocabulary and its applicability gate, the pure `novelty_columns_for` verdict->column mapping, D-25/NOVEL-02 masked provenance (an allowlist, never a denylist), D-23d's alias-aware `novelty_work_key`, and the pinned LLM contract (`gemini-3.6-flash`/effort-low, a self-hashing prompt template stating ruling G's rule verbatim and eliciting ruling H's container-predicts relationship, an explicit cache-key spec, and structured abstention mapping to `not_checked`).
- **`docs/specs/discovery-novelty-v1.md`** -- the canonical, single-cited contract doc: the checked-source-set implementation-status table (per `136-NOVELTY-PRIOR-ART.md` section 2), the funnel-first architecture, the LLM contract, ruling I's exact re-measurement scoping (never citing the ~$27 figure as accuracy evidence), the masking rule, the owner-labels-only grading rule, and a "known limitations of the ground-truth set" section.
- **`scripts/discovery_novelty_funnel.py`** -- a NEW committed script implementing the funnel-first architecture (ruling J): the mechanical pass (`run_heuristic_pass`/`run_heuristic_funnel`) can only ever resolve to `confirms` or leave a candidate in the residual, fixing Codex findings 1/2/3/4/5/6 at the design level (never merely by citation); a checkpointed, resumable model arm (`run_model_arm`); and the owner-label grading harness (`grade_against_owner_labels`/`load_owner_labels`) enforcing all three of Task 2's hard rules -- owner-provenance-only grading, skipped-case accounting, and the two novelty error directions reported separately.
- **93 passing tests** (`tests/test_discovery_novelty_contract.py`), including a schema-doc drift guard (extracts the live SQL `CHECK` constraint and asserts equality with the module's frozenset), an adversarial masking-input table, a fixture for every named Codex finding, ruling G's real worked-case parallel (case 87's alias spelling), the checkpoint/resume mechanism, and the grading harness's fail-closed denominator guard -- manually mutation-tested during implementation (see "Issues Encountered").
- **`136-NOVELTY-RUN.md`** -- an honest, environment-accurate record: Task 3's real re-measurement and production run could not execute here (see below), documented as its own clearly-labelled deferral rather than silently skipped or faked.

## Task Commits

1. **Task 1: shared/discovery_novelty.py + docs/specs/discovery-novelty-v1.md + tests** - `9c285c44` (feat)
2. **Task 2: scripts/discovery_novelty_funnel.py + grading-harness tests** - `c44a6c48` (feat)
3. **Task 3: 136-NOVELTY-RUN.md (deferral record)** - `98419056` (docs)

**Plan metadata:** pending (this commit, per the standard final-commit protocol -- SUMMARY.md only; STATE.md/ROADMAP.md are the orchestrator's to update per this plan's own dispatch instructions).

## Files Created/Modified

- `shared/discovery_novelty.py` (542 lines) -- the novelty contract module.
- `docs/specs/discovery-novelty-v1.md` -- the canonical spec doc.
- `scripts/discovery_novelty_funnel.py` -- the funnel runner + grading harness.
- `tests/test_discovery_novelty_contract.py` -- 93 tests across both tasks.
- `.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-NOVELTY-RUN.md` -- Task 3's deferral record.

## Decisions Made

- **The heuristic pass is deliberately narrow in what it can decide.** It resolves ONLY `confirms` (a genuine textual name-match against ANY checked source's free text) or leaves a candidate unresolved (residual). It never itself concludes `diverges_work`/`diverges_part`/`refines_granularity`/`aid_more_specific`/`alias_merge`/`container_predicts`/`extends` -- all of these require judgment beyond mechanical string matching and remain the model's job alone, over the residual only (ruling J). This also resolves Codex findings 1 and 6 (over-demotion on bare source presence) at the design level: presence of a `published_full` bibliography row or a bare PGP description, without an actual textual match, leaves a row unresolved rather than falsely demoting it.
- **Ruling G's free-text test is applied uniformly, not merely to the catalogue.** The SAME normalized-substring test decides `confirms` across catalogue/bibliography/PGP/FGP/M-source-shelfmark text alike -- so a structured field pointing elsewhere never, by itself, causes divergence; the free text is always checked first. A fixture directly mirrors the real worked case in `136-GATE1-DECISIONS.md` section G (case 87: an alias spelling present only in the catalogue's free text) and confirms it resolves to `confirms`, never a divergence shade.
- **Task 3's real run could not execute in this environment -- documented as such, not faked or silently skipped.** Before writing `136-NOVELTY-RUN.md`, the environment was checked directly: `discovery_data/novelty_hardcase_labels-v1.json` (written by plan 136-03 Task 4) does not exist in this worktree (git worktrees never share gitignored files -- only git-tracked content -- and `discovery_data/` is gitignored project-wide); the real checked-source sidecars (`fist_data/fjms_enrichment.db`, `pgp_data/pgp.db`, `fgp_data/fgp_transcriptions.db`) are likewise absent (confirmed directly: `fist_data/`/`pgp_data/` here contain only small non-DB artifacts, no `.db` file exists at the repo root); and no LLM provider credentials are configured in this execution environment. This is a Rule-4-shaped judgment call in the sense that fabricating any of the above would have been worse than reporting the gap -- but it is not really a Rule 1-4 deviation at all, since the plan's own Task 3 acceptance criteria explicitly anticipate and permit this exact outcome ("explicitly marked deferred, with a stated reason"). `136-NOVELTY-RUN.md` documents this distinction explicitly (an execution-environment gap, never an owner decision or a declined authorization) and lays out the concrete path a future execution with real data/credentials must follow.
- **NOVEL-01/NOVEL-02 are NOT marked complete**, mirroring 136-03's own posture on PANEL-01/PANEL-02: this plan builds and tests the contract, the funnel and the grading harness, but no real claim in the shipped asset yet carries a computed `novelty_status` -- that ships via 136-12's build wiring, itself gated on ruling I's still-open re-measurement.

## Deviations from Plan

**None (Rule 1-3).** The plan's own Task 3 acceptance criteria explicitly anticipate the exact outcome this execution reached (an unpopulated, explicitly-deferred production-run section with a stated reason) -- following that path is not a deviation from the plan, it is the plan's own designed fallback. No auto-fixes were applied to work around the missing data/credentials (fabricating sidecar data or guessing at credentials would themselves have been Rule-4-shaped architectural/scope decisions requiring a human, not something to silently paper over).

**Total deviations:** 0 auto-fixed. **Impact on plan:** Tasks 1 and 2 are fully built and tested exactly as specified. Task 3 is honestly documented as blocked by this execution environment rather than by anything within the plan's own design or the owner's authorization.

## Issues Encountered

- **Two literal-string acceptance criteria required a docstring rewrite.** Task 1's verify script asserts `'same_work_spike' not in shared/discovery_novelty.py` and Task 2's verify script asserts the same for `scripts/discovery_novelty_funnel.py`; an initial draft's own docstrings mentioned the gitignored research-tree name in prose (explaining it is read-only reference material). Rephrased both without the literal substring, verified via direct `grep -c` (both now `0`).
- **The mutation-test exercise was performed for real, not merely simulated.** Per Task 2's own instruction ("A guard whose test still passes without the guard is not a guard"), the explicit denominator guard in `grade_against_owner_labels` was temporarily commented out, `tests/test_discovery_novelty_contract.py -k zero_owner_provenance` was re-run, and BOTH dedicated tests failed with `Failed: DID NOT RAISE <class '...NoOwnerProvenanceLabels'>` -- confirming the guard is load-bearing and the tests are not vacuously satisfied by any exception. The guard was then restored and the full 93-test suite re-confirmed green before continuing.
- **`MASKING_SCAN_PATTERNS_FILE` was sourced from the primary checkout, not this worktree.** `.masking_patterns` is itself a gitignored artifact and (like `discovery_data/`) does not exist in this worktree's filesystem; the env var was pointed at the primary checkout's copy (`C:/Genizahsearch/.masking_patterns`) for the read-only masking scans this plan's verification requires. This does not affect the scan's correctness (it is the identical pattern file, just physically absent from this parallel worktree's own directory tree) and is unrelated to the Task 3 data-availability gap above (that gap is about production SIDECAR data and LLM credentials, not the masking pattern file).

## User Setup Required

None for Tasks 1-2 (no external service configuration required to build or test the contract, funnel, or grading harness). Task 3's eventual real production run will need: (1) `discovery_data/novelty_hardcase_labels-v1.json` present (already exists in the primary checkout, per plan 136-03); (2) the real `fist_data/fjms_enrichment.db`/`pgp_data/pgp.db`/`fgp_data/fgp_transcriptions.db`/`discovery-v1-*.db` sidecars present; (3) LLM provider credentials for `gemini-3.6-flash` configured in whatever environment runs it -- ideally not an isolated parallel worktree, or one where these have been deliberately provisioned.

## Next Phase Readiness

- **136-12 (novelty ingestion / build wiring) is NOT blocked by this plan's own scope**, but IS blocked, by ruling I's own design, from authorizing the FULL production run until the re-measurement `136-NOVELTY-RUN.md` section 2 describes is actually performed and recorded. The contract module, the funnel runner and the grading harness are all built, tested, and ready for 136-12 to call.
- **The ruling-I re-measurement and the production run remain genuinely open work**, not merely deferred paperwork -- `136-NOVELTY-RUN.md` section 5 states the exact five steps a future execution (with the label file, the real sidecars, and real credentials all present) must follow. This should be flagged to whichever session or environment next has access to those three things.
- **The checkpoint/resume mechanism (`run_model_arm`) and the grading harness are fully exercised by fixtures**, so 136-12's build wiring (and the eventual real run) can call them with confidence that the mechanism itself is correct -- only the real-data exercise remains outstanding.
- **No STATE.md/ROADMAP.md edits were made** by this plan, per its own dispatch instructions (the orchestrator owns those writes after the wave completes).

---

## ADDENDUM (2026-08-03, primary checkout, later session) -- Task 3's ruling-I re-measurement EXECUTED for real; production run HALTED at the gate, not deferred by environment

A later session, dispatched directly (not the isolated worktree above), ran on `C:\Genizahsearch`
where the label file, all three real sidecars, and a working `OPENROUTER_API_KEY` were all directly
verified present. It executed `136-NOVELTY-RUN.md` §5's five steps for real:

- **Ruling-I re-measurement: DONE, real cost $0.322146.** 60 real `NoveltyCandidate` rows (Class 6 +
  Arm 1) built from the real sidecars; the real heuristic funnel resolved 1/60 mechanically (to
  `confirms` -- but the owner's real label is `diverges_work`, a genuine, observed instance of ruling
  J's predicted permanent mechanical false-known); the real pinned model (`gemini-3.6-flash`,
  effort `low`, via OpenRouter, `usage.cost` read on every call) graded the 59-case residual. **Overall
  shade agreement: 47/60 = 78.3%** (vs. the prior 99-100% benchmark on the OLD five-way/one-title
  vocabulary); `divergence_correctness` sub-agreement only 8/28 (28.6%, at/below chance);
  `container_predicts` only 1/4 (25%). The pre-registered `alias_merge`-vs-granularity risk was
  UNTESTABLE (zero `alias_merge` labels in this 60-case pool).
- **Gate applied: this is a MATERIAL REGRESSION.** Per this plan's own instruction ("a material
  regression is the owner's decision to accept, not yours"), the session HALTED before the production
  run rather than proceeding automatically.
- **A free, zero-model-call full-corpus heuristic pass was ALSO run** (not gated -- costs nothing) to
  re-derive ruling J's own outstanding cost/scale note: of 65,200 real shipped `(sys_id, work_id)`
  pairs, 10,016 (15.4%) resolve for free and **55,184 (84.6%) are RESIDUAL** -- projecting the real
  measured per-call cost linearly gives **~$301** (roughly 11x the stale `~$27` estimate), and the
  observed serial latency projects to roughly one-to-two days of continuous API traffic without a
  concurrent/batched implementation.
- **NOVEL-01/NOVEL-02 remain NOT satisfied** -- no claim in the shipped asset carries a computed
  `novelty_status` as a result of this session; the block is now a measured, owner-facing decision
  gate (accept the regression / narrow the scope / harden and re-measure), not an environment gap.

Full numbers, per-shade confusion matrix, and the owner-decision options: `136-NOVELTY-RUN.md`
sections 2-3 (REPLACING the DEFERRED placeholders that section previously held; section 0's
environment-gap account is preserved unchanged for the historical record).

---
*Phase: 136-read-surfaces-connections-panel-work-witnesses*
*Completed: 2026-08-03*
