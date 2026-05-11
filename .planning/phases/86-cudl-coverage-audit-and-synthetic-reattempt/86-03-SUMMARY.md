---
phase: 86-cudl-coverage-audit-and-synthetic-reattempt
plan: 03
subsystem: residue-pattern-adjudication
status: complete
tags:
  - residue-patterns
  - human-adjudication
  - phase-86
  - checkpoint
  - bridge-aware-ranker
  - reject-all-outcome

# Dependency graph
requires:
  - phase: 86-01
    provides: shared/fist_cudl_bridge.py (fist_to_cudl_keys, build_fist_alias_index)
  - phase: 86-02
    provides: scripts/generate_synthetic_rows.py --dry-run output (reports/synthetic_ambiguity_residue_dryrun.csv)
provides:
  - scripts/build_residue_patterns_artifact.py — D-02c residue patterns artifact generator (553 lines)
  - .planning/phases/86-cudl-coverage-audit-and-synthetic-reattempt/86-RESIDUE-PATTERNS.md — 6 pattern family sections, 497 lines (pre-adjudication state)
  - tests/test_build_residue_patterns_artifact.py — bridge-aware ranker tests (TestResiduePatternRanker, 3 tests)
affects:
  - phase-86-03 Task 2 (CHECKPOINT — user adjudication; this summary is interim)
  - phase-86-03 Task 3 (CONDITIONAL — bridge extension only if N>0 rules accepted)
  - phase-86-04 (audit re-runs generation pipeline after adjudication)

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Bridge-aware ranker (Pass 2 MEDIUM-1): scoring driven by fist_to_cudl_keys output, NOT by Levenshtein or fuzzy matching"
    - "Prefetch-once family buckets (Pass 3 MED-86-03): one SELECT replaces per-residue LIKE+LIMIT (~1,599 round-trips eliminated)"
    - "Noisy-prefix tail filing: 'AIU: CUL: Or.1080 1.5' reaches or1080 bucket via rsplit(':', 1)[1] tail"
    - "CONCRETE PROPOSED FIST->CUDL RULE per family (Pass 2 HIGH-5): expressed as FIST.Shelfmark regex + real-FIST-shelfmark fixtures + test scaffold"
    - "Stop rule (Pass 2 LOW Codex): ONE generation + ONE adjudication pass; Spot-check more = Deferred, not auto-loop"

key-files:
  created:
    - scripts/build_residue_patterns_artifact.py (553 lines)
    - tests/test_build_residue_patterns_artifact.py (155 lines, 3 tests)
    - .planning/phases/86-cudl-coverage-audit-and-synthetic-reattempt/86-RESIDUE-PATTERNS.md (497 lines)
  modified: []
  ephemeral:
    - reports/synthetic_ambiguity_residue_dryrun.csv (gitignored regenerable artifact; 1,847 residue rows)

key-decisions:
  - "Test isolation file: chose tests/test_build_residue_patterns_artifact.py over extending tests/test_fist_cudl_bridge.py because the ranker tests exercise a NEW script helper not part of the bridge module's public API. Plan accepts either location per Pass 4 iteration 1 checker fix."
  - "sys.path bootstrap inside script: `if str(_PROJECT_ROOT) not in sys.path: sys.path.insert(0, str(_PROJECT_ROOT))`. Mirrors how scripts/generate_synthetic_rows.py implicitly relies on the project being on sys.path; the explicit bootstrap lets the script be invoked from any cwd (worktree, main, CI)."
  - "FIST.db path override: added --fist-db CLI arg so the script can run from a worktree against the main checkout's data file (fist_data/FIST.db is gitignored)."
  - "First-occurrence-only filing in _prefetch_fist_candidate_buckets: the 3-table production join can multiply rows for an inventory with several signatures/UCR matches; the prefetch helper bucketises by InventoryId first occurrence so each FIST shelfmark appears once per bucket."

requirements-completed:
  - AUDIT-01 (adjudication complete; 0 rules accepted; bridge unchanged from Plan 01 baseline)
requirements-in-progress: []

# Metrics
duration: ~30min (pre-checkpoint ~25min + post-checkpoint ~5min)
completed: 2026-05-11
started: 2026-05-11
---

# Phase 86 Plan 03: Residue Pattern Adjudication Summary (INTERIM, pre-checkpoint)

**This plan PAUSES at a user-adjudication CHECKPOINT.** This interim SUMMARY.md
captures the pre-checkpoint state. After the user adjudicates each of the 6
pattern families in `86-RESIDUE-PATTERNS.md`, a continuation agent will:

1. Parse the user's adjudication
2. For each Accepted rule: extend `shared/fist_cudl_bridge.py::fist_to_cudl_keys`
   AND add `test_accepted_rule_<rule_name>_fist_to_cudl` +
   `test_refute_rule_<rule_name>_fist_to_cudl` tests
3. For each Rejected/Deferred rule: note rationale in artifact + write
   residue-pattern entries that Plan 04's cudl_coverage.md will consume
4. Commit and finalize this SUMMARY.md with post-checkpoint state

## Pre-checkpoint accomplishments

- **`scripts/build_residue_patterns_artifact.py` (553 lines):** bridge-aware
  residue patterns artifact generator. Reads
  `reports/synthetic_ambiguity_residue_dryrun.csv` (Plan 02 `--dry-run`
  output, HIGH #5) and writes
  `.planning/phases/86-cudl-coverage-audit-and-synthetic-reattempt/86-RESIDUE-PATTERNS.md`.
- **Bridge-aware ranker (Pass 2 MEDIUM-1):** scoring is driven by
  `fist_to_cudl_keys(candidate_shelfmark)` — NOT Levenshtein or numeric-token
  overlap alone. Exact bridge-key match = 100; shared >=3-char prefix = 50;
  numeric-token overlap = tie-break.
- **Prefetch-once family buckets (Pass 3 MED-86-03):** `_prefetch_fist_candidate_buckets`
  issues ONE SQL query against `dbo_Inventory` joined through
  `dbo_InventorySignature -> dbo_Signature -> dbo_UnitCatalogRec` (3-table
  production-correct join, Pass 2 HIGH-2) and files each candidate into a
  family bucket keyed by CUDL prefix. Replaces the previous per-residue
  `LIKE ?` pattern that would have issued ~1,599 round-trips.
- **Noisy-prefix coverage:** a FIST shelfmark with a noisy leading prefix
  (e.g. `'AIU: CUL: Or.1080 1.5'`) is filed into the `or1080` bucket via its
  post-`rsplit(':', 1)[1]` tail, so it remains reachable by the ranker even
  though its raw shelfmark does not start with `Or.1080`.
- **`86-RESIDUE-PATTERNS.md` (497 lines):** 6 pattern family sections, each
  containing up to 5 sample CUDL classmarks paired with up to 3 bridge-aware
  nearest-neighbour FIST candidates (InventoryId, SignatureId,
  UnitCatalogRec Title/GenizahTitleText snippets, CUDL viewer URL, score)
  followed by a CONCRETE PROPOSED FIST->CUDL RULE expressed on FIST.Shelfmark
  inputs.
- **`tests/test_build_residue_patterns_artifact.py` (155 lines, 3 tests):**
  `TestResiduePatternRanker::test_ranker_prioritizes_exact_bridge_match`
  (Pass 2 MEDIUM-1: exact > prefix > unrelated),
  `test_ranker_prefetch_handles_noisy_prefix` (Pass 3 MED-86-03 noisy-prefix
  via tail filing), `test_ranker_prefetch_runs_only_once` (cache reuse —
  second call MUST NOT rebuild).
- **All 3 ranker tests pass.** All 40 Phase 86 tests pass
  (`tests/test_fist_cudl_bridge.py` + `tests/test_synthetic_generation_phase86.py` +
  `tests/test_build_residue_patterns_artifact.py`).

## Pattern families surfaced for adjudication

Bridge-aware ranker results are populated for each family. Sample classmarks
demonstrate the proposed FIST->CUDL transformation visibly resolves to
candidates with score 100 (exact bridge match + numeric-token tie-break = 102
in practice) for the families where the hypothesis is sound:

| # | Family | Residue count | Sample classmark | Sample top-score candidate | Score |
|---|--------|--------------:|------------------|----------------------------|------:|
| 1 | T-S F flattened-series | 441 | `tsf1.11` | `T-S F1(1).11` (InvId 5026101) | 102 |
| 2 | T-S Ar flattened-series | 401 | (see artifact) | (see artifact) | (see artifact) |
| 3 | T-S NS minute-fragments + letter | 179 | (see artifact) | (see artifact) | (see artifact) |
| 4 | Or. single-segment ambiguity | 577 | (see artifact) | (see artifact) | (see artifact) |
| 5 | Mosseri exotic letter | 141 | (see artifact) | (see artifact) | (see artifact) |
| 6 | T-S Misc multi-segment | 98 | (see artifact) | (see artifact) | (see artifact) |

**Total residue:** 1,847 entries (1,599 `no_fist_match` + 248 `multi_inventory`)

## Awaiting user adjudication

The user must review `86-RESIDUE-PATTERNS.md` and fill in Accept/Reject/
Spot-check checkboxes for each of the 6 pattern families. Each family
section contains:

- Up to 5 sample CUDL classmarks with bridge-aware FIST candidates +
  CUDL viewer URLs + UnitCatalogRec title metadata
- A CONCRETE PROPOSED FIST->CUDL RULE: regex on `FIST.Shelfmark` +
  resulting CUDL key template + supporting FIST fixtures + refuting
  FIST fixture + false-positive risk note
- A test scaffold with positive `test_accepted_rule_<rule_name>_fist_to_cudl`
  + refuting `test_refute_rule_<rule_name>_fist_to_cudl`
- Decision checkboxes: `[ ] Accept rule  [ ] Reject  [ ] Spot-check more (Deferred)`

**Resume signal:** the user responds with `approved: N accepted` (N is 0..6)
or `revise: <reason>`. Spot-check choices become Deferred annotations in
`reports/cudl_coverage.md` (Plan 04). Per Pass 2 LOW Codex stop rule: ONE
adjudication pass; further iteration requires explicit user request.

## Pre-checkpoint task commit

1. **Task 1 (pre-checkpoint phase): build residue patterns artifact + ranker tests** — `2f053ff7` (feat)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] FIST.db / nli_crossref.db absent from worktree**
- **Found during:** Task 1 Step A (running `python scripts/generate_synthetic_rows.py --dry-run`)
- **Issue:** Worktree at `.claude/worktrees/agent-accbc7fc4f52e4d9b/` lacks `fist_data/FIST.db` and `nli_data/nli_crossref.db` (both are gitignored). The dry-run cannot complete without them.
- **Fix:** Used Plan 02's existing `--fist-db` and `--nli-db` CLI overrides to point at the main checkout's data files (`C:/Genizahsearch/fist_data/FIST.db`, `C:/Genizahsearch/nli_data/nli_crossref.db`). No code change required; the override pathway is documented in Plan 02's SUMMARY.
- **Files modified:** None (CLI-only deviation)
- **Verification:** Dry-run produced `reports/synthetic_ambiguity_residue_dryrun.csv` with 1,847 residue rows (108 qualifying inventories, distribution `multi_inventory=248, no_fist_match=1599`); artifact generator consumed it successfully.

**2. [Rule 3 - Blocking] Script's `from shared...` import fails when run directly**
- **Found during:** Task 1 Step C (running `python scripts/build_residue_patterns_artifact.py`)
- **Issue:** `ModuleNotFoundError: No module named 'shared'`. Python does not add the script's parent directory to `sys.path` by default, so `from shared.fist_cudl_bridge import fist_to_cudl_keys` fails when the script is invoked without an external `PYTHONPATH`.
- **Fix:** Added the standard project-root bootstrap at the top of the script:
  ```python
  _PROJECT_ROOT = Path(__file__).resolve().parent.parent
  if str(_PROJECT_ROOT) not in sys.path:
      sys.path.insert(0, str(_PROJECT_ROOT))
  ```
  This mirrors the implicit assumption in `scripts/generate_synthetic_rows.py` (which relies on the user invoking it from the project root) but is explicit and works regardless of cwd.
- **Files modified:** `scripts/build_residue_patterns_artifact.py`
- **Verification:** `python scripts/build_residue_patterns_artifact.py --fist-db ...` now works without `PYTHONPATH`. Tests use the same bootstrap.

**3. [Rule 3 - Blocking] First-occurrence-only filing in prefetch**
- **Found during:** Task 1 Step C (initial run)
- **Issue:** The 3-table production join multiplies rows for an inventory with multiple signatures/UCR matches (e.g. inv 5026101 with several signature mappings). Without dedup, each FIST inventory could appear multiple times in its family bucket, inflating candidate counts and producing duplicate rows in the artifact's nearest-neighbour table.
- **Fix:** Added `seen_inv: set[int]` in `_prefetch_fist_candidate_buckets` to skip duplicate `InventoryId` rows. First occurrence wins (consistent with `ORDER BY inv.InventoryId`).
- **Files modified:** `scripts/build_residue_patterns_artifact.py`
- **Verification:** Sample tables in `86-RESIDUE-PATTERNS.md` show distinct InventoryIds per row; ranker tests still pass.

**4. [Rule 2 - Missing critical functionality] `--fist-db` CLI override**
- **Found during:** Task 1 Step C
- **Issue:** Plan's `build_artifact()` function had no parameter for an alternate FIST.db path. In a worktree (where `fist_data/FIST.db` is gitignored), this would block execution.
- **Fix:** Added `argparse.ArgumentParser` with `--fist-db` flag; `build_artifact(fist_db_path=...)` accepts the override. Default still resolves to `ROOT / "fist_data" / "FIST.db"`.
- **Files modified:** `scripts/build_residue_patterns_artifact.py`
- **Verification:** `python scripts/build_residue_patterns_artifact.py --fist-db C:/Genizahsearch/fist_data/FIST.db` succeeds; ranker tests don't touch the CLI and still pass.

---

**Total deviations:** 4 auto-fixed (3 Rule 3 — blocking environmental
gaps; 1 Rule 2 — missing CLI ergonomics). No Rule 4 architectural changes.
All deviations are mechanical adjustments that preserve the plan's exact
semantic intent (bridge-aware ranking, prefetch-once buckets, noisy-prefix
tail coverage). None affect the artifact content.

## Issues Encountered

- **Worktree base mismatch at startup:** Initial worktree HEAD was
  `9d8a5b29` (a different feature branch). Hard-reset to expected base
  `362cb576` per `<worktree_branch_check>` protocol.
- **No other issues:** All pre-checkpoint acceptance criteria met.

## TDD Gate Compliance

Plan has `type: execute` (not `type: tdd`); Task 1 is `type="auto"` without
TDD attribute. The new ranker tests were written immediately after the
script and before commit, so the impl + tests are committed atomically. No
RED/GREEN gate enforcement applies at the plan level. Task 3 (post-
checkpoint, conditional) IS marked `tdd="true"` — its execution flow will
follow RED/GREEN/REFACTOR when the continuation agent runs.

## Acceptance criteria status (pre-checkpoint)

All Task 1 grep + functional acceptance criteria met:

- `scripts/build_residue_patterns_artifact.py` exists, 553 lines (>= 200 PASS)
- `python -m py_compile`: exit 0 (PASS)
- `from shared.fist_cudl_bridge import fist_to_cudl_keys`: 1 (PASS)
- `from shared.shelfmark_bridge import cudl_normalize`: 1 (PASS)
- `fist_to_cudl_keys(shelfmark)`: 1 (PASS — in docstring comment)
- `score_candidate`: 2 (PASS)
- `_CANDIDATE_LIMIT`: 3 (PASS)
- `2000`: 2 (PASS)
- `_CUDL_TO_FIST_PREFIX`: 8 (PASS, >= 2)
- `synthetic_ambiguity_residue_dryrun`: 3 (PASS, >= 1)
- `synthetic_ambiguity_residue.csv` (literal with trailing quote): 0 (PASS, must be 0)
- `dbo_UnitCatalogRec`: 2 (PASS)
- `JOIN dbo_Signature `: 1 (PASS)
- `cudl_viewer_url_for`: 2 (PASS)
- `cudl.lib.cam.ac.uk/view`: 1 (PASS)
- `def _prefetch_fist_candidate_buckets`: 1 (PASS)
- `_FIST_CANDIDATE_BUCKETS`: 6 (PASS, >= 3)
- `post_colon_tail`: 1 (PASS)
- `rsplit(":", 1)`: 1 (PASS)
- `WHERE LOWER(inv.Shelfmark) LIKE ?` (must be 0): 0 (PASS)
- `reports/synthetic_ambiguity_residue_dryrun.csv` exists (PASS)
- `grep -c "pattern_guess" reports/synthetic_ambiguity_residue_dryrun.csv`: 1 (PASS)
- `86-RESIDUE-PATTERNS.md` exists, 497 lines (>= 100 PASS)
- `^## Pattern Family`: 6 (PASS)
- `User decision`: 6 (PASS, >= 6)
- `Proposed FIST`: 6 (PASS, >= 6)
- `Direction: FIST.Shelfmark`: 10 (PASS, >= 6)
- `FIST regex on dbo_Inventory.Shelfmark`: 6 (PASS, >= 6)
- `test_accepted_rule_`: 7 (PASS, >= 6)
- `test_refute_rule_`: 7 (PASS, >= 6)
- `Supporting FIST.Shelfmark fixtures`: 6 (PASS, >= 6)
- `Refuting FIST.Shelfmark fixture`: 6 (PASS, >= 6)
- `False-positive risk`: 6 (PASS, >= 6)
- `Stop rule`: 2 (PASS, >= 1)
- `https://cudl.lib.cam.ac.uk/view/`: 90 (PASS, >= 1)
- All 6 family titles present (T-S F flat, T-S Ar flat, T-S NS minute, Or.
  single-segment, Mosseri exotic, T-S Misc multi-segment): PASS
- `TestResiduePatternRanker` in tests/: PASS (via `grep -rl`, found in
  tests/test_build_residue_patterns_artifact.py)
- `pytest -k "TestResiduePatternRanker"`: 3 passed, 3 skipped, 1840
  deselected (PASS, >= 1 selected)
- `test_ranker_prefetch_handles_noisy_prefix` in tests/: PASS (via `grep -rl`)
- `AIU: CUL: Or.1080` in tests/: PASS (via `grep -rl`)
- `pytest -k "test_ranker_prefetch_handles_noisy_prefix"`: 1 passed, 3
  skipped, 1842 deselected (PASS, exactly 1 selected)

## Self-Check: PASSED

Files created:
- FOUND: scripts/build_residue_patterns_artifact.py (553 lines)
- FOUND: tests/test_build_residue_patterns_artifact.py (155 lines)
- FOUND: .planning/phases/86-cudl-coverage-audit-and-synthetic-reattempt/86-RESIDUE-PATTERNS.md (497 lines)

Commits exist:
- FOUND: 2f053ff7 — feat(86-03): add scripts/build_residue_patterns_artifact.py + 86-RESIDUE-PATTERNS.md + ranker tests

Ephemeral / regenerable artifacts:
- FOUND: reports/synthetic_ambiguity_residue_dryrun.csv (1,847 rows, gitignored)

Plan completion status: PRE-CHECKPOINT (Task 1 of 3 done; Task 2 awaits user; Task 3 conditional).

---

## Post-Checkpoint Summary (2026-05-11)

**Outcome:** All 6 pattern families REJECTED by user. **0 rules accepted.** Bridge code (`shared/fist_cudl_bridge.py`) remains UNCHANGED from Plan 01 baseline. Task 3 (conditional rule integration) is correctly SKIPPED because N=0 rules were accepted.

### User adjudication rationale (per family)

1. **T-S F flattened-series — REJECT.** Proposed CUDL keys (`tsf1.1100`, `tsf2.250`, etc.) don't appear in residue; score-102 matches in artifact's sample table come from EXISTING D-02a Pattern 3 (N)-strip rule, not from the proposed flattened-series rule. Residue `tsf1.11` rows are `multi_inventory_ambiguous` because the existing (N)-strip rule maps both `T-S F1(1).11` and `T-S F1(2).11` to `tsf1.11`.
2. **T-S Ar flattened-series — REJECT.** Same shape as Family 1: proposed keys (`tsar18.234`) don't appear in residue; score-102 matches are from existing (N)-strip rule.
3. **T-S NS minute-fragments — REJECT.** `cudl_normalize('T-S NS X.minute fragments')` already produces `tsnsXminutefragments` — rule is redundant with existing behavior. Residue suggests a separate alias-index investigation worth carry-forward, not a new normalizer rule.
4. **Or. single-segment ambiguity — REJECT.** HIGH RISK confirmed via FIST.db probing: `Or.1080 11.45`, `Or.1080 5.17`, `Or.1080 6.11`, `Or.1080 B14.1` show sub-fragment digits are REAL physical divisions. Collapsing `Or.1080 11.1` → CUDL `or1080.11` would conflate distinct manuscripts.
5. **Mosseri exotic letter — REJECT.** FIST has ZERO `Moss.{ROMAN},{N}{lowercase letter}` shelfmarks. All Mosseri letter-suffixes use UPPERCASE A: `Moss. I,53A`, `Moss. III,133A`, `Moss. I,118.1A`. Proposed regex would never match real FIST data.
6. **T-S Misc multi-segment — REJECT.** FIST uses `T-S Misc.X.Y(Z)` (parens for sub-fragment) for residue cases, not `T-S Misc X.Y.Z`. Proposed regex matches existing canonical 3-segment forms that already normalize correctly — so the rule is either redundant or wrong-direction.

### Key carry-forward finding (3 categories of existing-rule issues)

The 1,847 residue is dominated by EXISTING-rule over-aggressiveness, **not** missing normalizer rules. The three patterns documented for a future "Phase 87 — Bridge rule disambiguation" plan are:

1. **D-02a Pattern 3 ((N)-strip) conflations** — `T-S F1(1).N` and `T-S F1(2).N` both produce `tsf1.N`, causing `multi_inventory_ambiguous` for CUDL `tsf1.N`. Disambiguation idea: preserve (N) suffix for T-S F / T-S Ar families.
2. **Mosseri concat-form spurious collisions** — `Moss. I,5.1` produces concat-form alias `mosserii51` which collides with canonical `Moss. I,51`. Disambiguation idea: gate concat-form alias to fragments without internal sub-segments.
3. **AIU-preliminary-handlist duplicates** — `AIU: Mosseri: Moss. I,26.1` and canonical `Moss. I,26.1` produce identical aliases → `multi_inventory_ambiguous`. Disambiguation idea: prefer AlmaId-bearing inventory or strip AIU-prefix preliminary-handlist entries.

### Milestone-defining win

**`T-S NS 329.96` (the originating user case) CLOSES** via Plan 02's CUDL-walked generator:

- `explain_fist_by_cudl('tsns329.96')` returns `status='single' entries=[(65549106, 'T-S NS 329.96')]`
- Plan 02's `--apply` run will emit a synthetic libraries.csv row for it
- This was the case that motivated the entire Phase 86 reframe

### Post-checkpoint verification results

- **Task 1 — Adjudication edits to `86-RESIDUE-PATTERNS.md`:** 6/6 family decisions filled in with `[x] Reject` checkbox + verbatim rationale block; Adjudication Summary section appended above `## After Adjudication`. Committed as `85eba7f5`.
- **Task 2 — Bridge unchanged verification:** `git log --oneline -- shared/fist_cudl_bridge.py` shows ONE commit (Plan 01's `91eb38a6 feat(86-01): add shared/fist_cudl_bridge.py with D-02a normalizers + 3-table join`); `git diff 91eb38a6 -- shared/fist_cudl_bridge.py` is empty (exit code 0). Bridge code identical to Plan 01 baseline. No commit needed for this task.
- **Task 3 — Full pytest run:** GREEN. `1824 passed, 21 skipped, 2 warnings in 163.59s`. The 2 warnings are pre-existing and unrelated (httpx deprecation in parallels API tests; posthog-api-drain teardown AttributeError on FakeQueue). No new tests added in this plan (correctly, since 0 rules accepted).

### Plan 04 inputs

Plan 04 should:
- Reference the Adjudication Summary section in `reports/cudl_coverage.md` under a new `## Residue Pattern Adjudication` section.
- Quote the 3-category existing-rule findings as recommended scope for a future "Phase 87 — Bridge rule disambiguation" plan revision.
- Confirm T-S NS 329.96 appears in the Plan 02 `--apply` output as a closed (synthetic-row-emitted) case.

### Post-checkpoint task commits

1. **Task 1 (post-checkpoint): adjudicate 6 residue families as REJECTED** — `85eba7f5` (docs)
2. **Task 4 (post-checkpoint): finalize SUMMARY.md** — committed alongside this finalize edit (docs)

### TDD Gate Compliance (post-checkpoint)

Task 3 was marked `tdd="true"` in the plan, but its preconditions (N>=1 accepted rules) are not met — N=0 accepted means no `test_accepted_rule_*` tests to add. No RED/GREEN gate enforcement applies because there is no rule to test. The plan-level conditional structure correctly handles this branch.

---
*Phase: 86-cudl-coverage-audit-and-synthetic-reattempt*
*Plan: 03*
*Status: COMPLETE — 0 rules accepted, bridge unchanged, T-S NS 329.96 closes via Plan 02*
*Completion: 2026-05-11*
