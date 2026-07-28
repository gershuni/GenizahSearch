---
phase: 135-precision-certificate-confidence-bands
plan: 09
subsystem: discovery
tags: [cert01, precision-certificate, pre-registration, sqlite, statistics, masking]

# Dependency graph
requires:
  - phase: 135-08
    provides: the deployed v2 discovery.db sidecar (content_hash 33499c5b89f9…, frame_content_hash 53725098…) + discovery_data/manifest.json
provides:
  - "The IMMUTABLE CERT-01 pre-registration (cert01_prereg.json) freezing the shipped tier_a estimand (134,123 rows) against the deployed v2 sidecar"
  - "The pre-outcome operating-characteristics table (cert01_oc_table.md)"
  - "The drawn 220-card stratified discovery deck + 20 gold + 20+20 blinded diagnostic sample, bound via the separate cert01_deck_manifest.json"
  - "scripts/verify_cert01_grading.py -- the mechanical/estimand grading-STARTED validator (twelve checks)"
affects: [136-read-surfaces, 137-community-judgments, 138-leads-queue, 139-atlas-homepage-release]

tech-stack:
  added: []
  patterns:
    - "Frozen-recipe module shared by build/draw/verify (scripts/cert01_frame.py), mirroring the discovery_ids.py / build_discovery_sidecar.py precedent"
    - "Recompute-and-compare hash pinning (never trust a stored value) for every frozen input"
    - "Beta-Bernoulli simulated-ICC OC table over the real physMS cluster-size distribution"

key-files:
  created:
    - scripts/cert01_frame.py
    - scripts/cert01_freeze.py
    - scripts/cert01_draw_deck.py
    - scripts/verify_cert01_grading.py
    - tests/test_cert01_frame.py
    - tests/test_cert01_harness_adapter.py
    - tests/test_cert01_grading_validator.py
    - .planning/phases/135-precision-certificate-confidence-bands/cert01_prereg.json
    - .planning/phases/135-precision-certificate-confidence-bands/cert01_oc_table.md
    - .planning/phases/135-precision-certificate-confidence-bands/cert01_deck_manifest.json
  modified: []

key-decisions:
  - "same_work_spike/probe/scripts/cert01_frame_adapter.py (as named in the plan) was NOT created -- that whole tree was deliberately untracked by commit 5370c20f as part of the M-source masking-history remediation; new tracked estimand/hash/deck-draw/validator logic lives in scripts/ instead (scripts/cert01_frame.py, cert01_freeze.py, cert01_draw_deck.py), importing e1_deck.py/e1_confirm_sizing.py at RUNTIME via a sys.path bridge to the gitignored dev-box tree"
  - "Task 1's plan-specified verify command (--scan-repo --strict with no --scan-asset) is malformed (the flag requires BOTH); ran --scan-repo alone (clean) plus per-file --scan-asset on every new tracked file (all clean), matching the 135-02 precedent"
  - "OC table ICC realized via a Beta-Bernoulli simulation over the REAL 31,022-cluster physMS size distribution at three illustrative correlation levels (0.0/0.05/0.10, the e1_confirm_sizing.py self-test's own documented range) -- the true within-cluster verdict correlation cannot be measured pre-outcome; disclosed as an assumption in cert01_oc_table.md"
  - "gold_allocation.n = 20 (capped by the 174-card e1_adjudicated_a.jsonl pool); confirmation_allocation.n_drawn = 340 (the maximum finite size_confirmation n_drawn across the full pre-outcome OC grid) -- both frozen as COUNTS at freeze time per protocol §5.1, with the specific cluster-disjoint gold cards selected at draw time (Task 2)"
  - "Diagnostic-sample 'retained' candidates identified at PAGE granularity via discovery_routing_audit.decision='kept_tie' pages (the shipped v2 asset's kept_tie rows carry demoted_work_id=NULL, so the exact co-claim pair is not recoverable from the audit table alone) -- documented data-shape limitation, not a fabrication"

requirements-completed: []  # CERT-01 grading has NOT started yet (Task 3 checkpoint pending) -- premature to mark Complete
---

# Phase 135 Plan 09: CERT-01 Precision Certificate -- Frame Freeze + Deck Draw + Validator Summary

**Froze the shipped tier_a estimand (134,123 `(page, canonical_work_id)` rows) in an immutable, hash-pinned pre-registration against the deployed discovery-v2 sidecar, drew a 220-card stratified discovery deck + 20 gold + 20/20 blinded diagnostic sample bound by a separate deck manifest, and built a twelve-check mechanical validator that currently reports 11/12 PASS -- the sole failure is the honest, expected one: no verdict has been graded yet.**

## Performance

- **Duration:** ~170 min (single continuous session; exact start not separately timestamped)
- **Completed:** 2026-07-28
- **Tasks:** 2 of 3 fully executed (Task 1, Task 2); Task 3's buildable half (validator + tests) executed and committed, its human-grading half is the open checkpoint
- **Files created:** 10 tracked (+ 3 gitignored research artifacts)

## Accomplishments

- **Task 1 (frame freeze):** Computed the frozen protocol §1.2 dedup/ranking SQL directly against the deployed v2 sidecar (`discovery-v1-33499c5b…db`), yielding **134,123** shipped, display-deduplicated `tier_a` `(page_id, canonical_work_id)` rows (326 raw collisions correctly resolved by the precedence-lattice re-ranking across canonical merges). Computed `population_hash`, a dedicated `cluster_map_hash` (over the page->physMS `unit_key` mapping, 31,022 distinct clusters), read + recomputed-and-compared all four frozen input hashes (`canonical_merges_sha256`, `composition_dates_sha256`, `seftja_dates_sha256`) plus `db_content_hash` and `crosswalk_sha256` against the deployed sidecar `meta` table and `discovery_data/manifest.json` -- every one matched exactly. Wrote the immutable `cert01_prereg.json` (`report_id` = SHA-256 over the payload with its own field omitted) and the mandatory pre-outcome `cert01_oc_table.md` (36-row grid: true precision × ICC scenario × INS rate), computed via the REUSED `e1_confirm_sizing.py` primitives (`anova_icc`, `size_confirmation`, `expected_nonempty_components`, `wilson_lower_one_sided`, `binom_sf`) -- never re-derived.
- **Task 2 (deck draw):** Re-verified `population_hash`/`cluster_map_hash` against the frozen pre-registration before drawing (fail-loud on drift), then drew a 220-card SRSWOR-per-stratum discovery deck (`ja:high` 27, `ja:medium` 17, `msource:high` 26, `msource:medium` 17, `sefaria:high` 102, `sefaria:medium` 31), 20 cluster-disjoint gold repeat cards from the existing 174-card `e1_adjudicated_a.jsonl` pool, and a blinded 20-demoted + 20-retained `later_shared_text` diagnostic sample (protocol §8) with the hidden classifier tag stored in a SEPARATE gitignored side file, never in the grader-visible deck. Delegated to `e1_deck.components_of` for the drawn deck's physMS bipartite component structure (85 components over 220 cards). Wrote the separate, tracked `cert01_deck_manifest.json` (`prereg_report_id` + `deck_manifest_hash` + per-stratum/gold/confirmation counts); confirmed `cert01_prereg.json` stayed byte-identical (never mutated).
- **Task 3 (validator, buildable half):** Built `scripts/verify_cert01_grading.py` implementing all twelve forge-resistant checks named in the plan, each independently proven load-bearing by 30 tests in `tests/test_cert01_grading_validator.py` (a self-consistent golden fixture passes all twelve; each check's specific failure mode is reverted and shown to raise). Ran the validator against the REAL current artifacts: **11/12 checks PASS**; check 6 (grader attribution / >=1 verdict) correctly FAILS because the ledger is genuinely empty -- grading has not started.

## Task Commits

1. **Task 1: Freeze the CERT-01 frame** - `3e5772cc` (feat)
2. **Task 2: Draw the deck + diagnostic sample** - `d2bfd9ae` (feat)
3. **Task 3: Grading-STARTED validator + tests (buildable half)** - `ad793440` (feat)

_Task 3's human-grading half is the open checkpoint below -- no further commit until an owner/expert records a verdict._

## Files Created/Modified

- `scripts/cert01_frame.py` - Frozen estimand/hash primitives shared by all three call sites (the dedup/ranking SQL across canonical-merge collisions, page->physMS cluster mapping, cross-corpus stratum tie-break, `population_hash`/`cluster_map_hash`, self-referential `report_id`, input-hash recompute-and-compare, stratified card allocation)
- `scripts/cert01_freeze.py` - Task 1 CLI: computes the estimand + OC table, writes `cert01_prereg.json` + `cert01_oc_table.md`
- `scripts/cert01_draw_deck.py` - Task 2 CLI: draws the stratified deck + gold + diagnostic sample, writes `cert01_deck_manifest.json` + the gitignored deck/ledger artifacts
- `scripts/verify_cert01_grading.py` - Task 3 CLI: the twelve-check mechanical/estimand validator
- `tests/test_cert01_frame.py` - 19 tests (hash invariance, `report_id` self-reference, ranked-SQL collision dedup + w001239 drop-list, stratum tie-break, input-hash mismatch fail-loud)
- `tests/test_cert01_harness_adapter.py` - 8 tests (stratified-draw allocation/reproducibility, gold cluster-disjoint filtering, diagnostic-sample page identification, `e1_deck.components_of` delegation proof)
- `tests/test_cert01_grading_validator.py` - 30 tests (every one of the twelve checks proven load-bearing + wiring tests + a real-artifact end-to-end check)
- `.planning/phases/135-precision-certificate-confidence-bands/cert01_prereg.json` - The TRACKED, immutable pre-registration
- `.planning/phases/135-precision-certificate-confidence-bands/cert01_oc_table.md` - The TRACKED pre-outcome OC table
- `.planning/phases/135-precision-certificate-confidence-bands/cert01_deck_manifest.json` - The TRACKED, separate deck manifest

**Gitignored research artifacts (created on disk, never committed, per the protocol's own interface convention):**
- `same_work_spike/probe/data/cert01_deck_key.json` - the drawn deck (280 cards: 220 candidate + 20 gold + 20 demoted + 20 retained)
- `same_work_spike/probe/data/cert01_diagnostic_tag.json` - the hidden `later_shared_text` classifier tag, keyed by uid
- `same_work_spike/probe/review/cert01_deck_verdicts.json` - the verdict ledger, currently `[]` (empty -- awaiting the human checkpoint)

## Decisions Made

See frontmatter `key-decisions`. In short: (1) new tracked CERT-01 logic lives in `scripts/` rather than the plan's named `same_work_spike/probe/scripts/cert01_frame_adapter.py` path, because that whole research tree was deliberately untracked by a prior masking-remediation commit and this plan does not reverse that; (2) the plan's literal Task-1 verify command (`--scan-repo --strict`) is malformed and was run as `--scan-repo` + per-file `--scan-asset` instead (established 135-02 precedent); (3) the OC table's ICC scenarios are a disclosed simulation assumption, not a measured value (impossible pre-outcome); (4) gold/confirmation allocation counts were sized from the real available gold pool and the real OC grid's worst finite case respectively; (5) the diagnostic sample's "retained" population is identified at page granularity due to a genuine data-shape gap in the shipped `discovery_routing_audit` table for tied decisions.

## Deviations from Plan

### Auto-fixed / Documented Issues

**1. [Rule 4-adjacent, resolved by following the more recent hard constraint] `same_work_spike/probe/scripts/cert01_frame_adapter.py` relocated to `scripts/`**
- **Found during:** Task 2 planning (before any code was written)
- **Issue:** The plan's `files_modified` names a new tracked file inside `same_work_spike/probe/scripts/`. Git history shows that ENTIRE tree was deliberately untracked by commit `5370c20f` ("chore: untrack same_work_spike research tree (internal; gitignored)") as part of the M-source masking-history remediation (see memory `project_git_history_msource_exposure_accepted`). Adding a NEW tracked file there would reverse a deliberate, security-motivated decision made after this plan was authored.
- **Fix:** All new tracked CERT-01 logic (`cert01_frame.py`, `cert01_freeze.py`, `cert01_draw_deck.py`) lives under `scripts/` instead, matching the existing `build_discovery_sidecar.py`/`verify_discovery_sidecar.py`/`discovery_ids.py` convention. `e1_deck.py`/`e1_confirm_sizing.py` are still reused (never re-derived) via a runtime `sys.path` bridge to the gitignored dev-box copy -- exactly the reuse the plan's `<interfaces>` block specifies, just invoked from a tracked call site instead of an untracked one.
- **Files affected:** `scripts/cert01_frame.py`, `scripts/cert01_freeze.py`, `scripts/cert01_draw_deck.py` (new, in place of the plan-named path)
- **Verification:** masking scan clean on every new file; `--scan-repo` full gate clean.
- **Committed in:** `3e5772cc`, `d2bfd9ae`

**2. [Rule 1 - bug in the plan's own verify command] `--scan-repo --strict` requires `--scan-asset`**
- **Found during:** Task 1 verification
- **Issue:** `scripts/check_atlas_masking.py --scan-repo --strict` (as literally written in the plan's automated verify) exits with `ERROR: --strict requires BOTH --scan-repo and --scan-asset PATH.` -- a pre-existing malformed-command issue, same class as the one documented in the 135-02 SUMMARY.
- **Fix:** Ran `--scan-repo` alone (clean, full repo scan) plus `--scan-asset <file>` individually on every new tracked file (all clean).
- **Committed in:** N/A (verification-only, no code change)

---

**Total deviations:** 2 (1 file-location correction following a more recent hard constraint, 1 pre-existing malformed-verify-command workaround). No scope creep -- both preserve or strengthen the plan's own masking/verification intent.

## Issues Encountered

- The `discovery_routing_audit` table's `kept_tie` rows carry `demoted_work_id=NULL` in the shipped v2 asset (a tie decision demotes nobody, so the build never populated an "other side" id for that decision kind). This means the diagnostic sample's "retained" population (protocol §8) is identified at PAGE granularity (pages where SOME kept_tie comparison occurred) rather than at the exact `(page, work)` pair the original tie comparison involved. Documented in `scripts/cert01_draw_deck.py::build_diagnostic_sample`'s docstring; does not affect the CORE tier_a precision estimand (which never includes `later_shared_text`-demoted rows), only the separate, non-adjudication-evidence diagnostic classifier-validation exercise.
- The pre-outcome OC table's ICC input cannot be measured before any card is graded (a genuine statistical constraint, not a project-specific gap); resolved via a disclosed Beta-Bernoulli simulation over the real cluster-size distribution rather than an invented single number.

## Known Stubs

None. Every artifact this plan produces is a real, computed value (hashes, counts, allocations) against the actual deployed sidecar -- no placeholder/mock data reaches any tracked file.

## Self-Check

Verifying claimed files exist and claimed commits exist:

```
FOUND: scripts/cert01_frame.py
FOUND: scripts/cert01_freeze.py
FOUND: scripts/cert01_draw_deck.py
FOUND: scripts/verify_cert01_grading.py
FOUND: tests/test_cert01_frame.py
FOUND: tests/test_cert01_harness_adapter.py
FOUND: tests/test_cert01_grading_validator.py
FOUND: .planning/phases/135-precision-certificate-confidence-bands/cert01_prereg.json
FOUND: .planning/phases/135-precision-certificate-confidence-bands/cert01_oc_table.md
FOUND: .planning/phases/135-precision-certificate-confidence-bands/cert01_deck_manifest.json
FOUND commit: 3e5772cc
FOUND commit: d2bfd9ae
FOUND commit: ad793440
```

## Self-Check: PASSED

## Next Phase Readiness

**BLOCKED on the Task 3 human checkpoint.** `scripts/verify_cert01_grading.py` runs clean against the real artifacts today: 11 of 12 checks PASS; the sole failure is check 6 ("grader attribution present (>=1 verdict)"), because the ledger (`same_work_spike/probe/review/cert01_deck_verdicts.json`) is genuinely empty. This is the correct, honest state -- no verdict was fabricated.

**What the owner must do to close Phase 135:**
1. Grade at least one card from the drawn deck (`same_work_spike/probe/data/cert01_deck_key.json`, 280 cards: 220 candidate + 20 gold + 20 demoted + 20 retained diagnostic) using one of the allowed verdicts (`A`/`B`/`C`/`INS`), recording `{uid, verdict, grader, ...}` in `same_work_spike/probe/review/cert01_deck_verdicts.json`.
2. Re-run `python scripts/verify_cert01_grading.py` -- it should then exit 0 (all twelve checks pass).
3. Resume this plan (or hand off to a fresh execute-plan agent) with the resume signal `"grading-started"` once step 2 passes, so Phase 135 can close (D-02: grading STARTED, not completed).

Owner grading then continues in parallel through Phases 136-138 and gates only the Phase 139 REL-01 public-promotion flag-flip, per the roadmap's own sequencing (this does NOT block Phase 136 planning).

---
*Phase: 135-precision-certificate-confidence-bands*
*Completed: 2026-07-28 (Tasks 1-2 + Task 3 buildable half; Task 3 human grading is the open checkpoint)*
