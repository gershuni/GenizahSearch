---
phase: 135-precision-certificate-confidence-bands
plan: 04
subsystem: data
tags: [discovery-v2, bake-plan, chronological-demotion, band-precision, codex-review, owner-authority]

# Dependency graph
requires:
  - phase: 134-discovery-data-spine
    provides: "Frozen discovery sidecar schema (band_precision, discovery_claim/discovery_evidence two-table claim model), the SEED-029 owner-ratified census, and the D-19 date-coverage audit (DELTA=100y, launch-grade)"
provides:
  - "Rewritten, hash-bound docs/specs/discovery-v2-bake-plan.md: full 16-merge census + D-14 flip + w001239 drop, D-17 chronological co-claim demotion (DELTA=100y hardcoded), THREE hash-pinned build inputs (--canonical-merges, --composition-dates, --seftja-dates) each with a frozen exact-shape parser, an independent chrono_coverage_prebuild regression anchor with a now-frozen schema, the tested CERT-01 FAIL-branch reband to screening_rb with atomic legacy-precision invalidation (including the pre-existing collection-level measurement preserved), and 15 verifier gates"
  - "135-BAKEPLAN-CODEX-REVIEW.md: a transparent, hash-bound record of the owner-authorized gate close after 9 rounds of non-converging adversarial review, with every residual finding dispositioned to a named forward track"
  - "Unblocks 135-05+ (v2 build code) and Wave 2 (135-02, 135-05)"
affects: [135-05, 135-06, 135-07, 135-09, "v2.1 discovery-v2 rebuild"]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Hash-bound Codex-approval gate: reviewed-bytes SHA-256 recorded so a post-review edit re-opens the gate (used again here, but for an owner-authorized close rather than an APPROVE)"
    - "Owner-authorized gate closure as an explicit, documented deviation from an automated verify predicate, with every residual finding dispositioned (fixed / routed to a named future plan / accepted) rather than silently dropped"

key-files:
  created:
    - .planning/phases/135-precision-certificate-confidence-bands/135-BAKEPLAN-CODEX-REVIEW.md
    - .planning/phases/135-precision-certificate-confidence-bands/135-04-SUMMARY.md
  modified:
    - docs/specs/discovery-v2-bake-plan.md

key-decisions:
  - "OWNER AUTHORITY: closed the blocking Task-2 Codex gate without a literal final-line VERDICT: APPROVE, after 9 rounds of non-converging adversarial review (design-level BLOCKERs eliminated by round 6; zero BLOCKERs rounds 7-8; every Codex-requested check + the full 15-gate verifier inventory rated PRESENT by round 9)."
  - "Folded the two concrete real defects round 9 raised (HIGH-1: first-build migration was about to erase a pre-existing, valid collection-level band_precision measurement; HIGH-2: --chrono-coverage-anchor lacked a frozen JSON schema) before closing the gate."
  - "Round-9's lone BLOCKER (F5, fuzzy-date-midpoint chronology) is dispositioned as a bounded, already-owner-ratified property of the D-17 coarse rule (commit 84054c45, 'launch-grade') rather than fixed in this plan -- routed to a named 135-06/v2.1 conservative-interval-semantics refinement track."
  - "HIGH-3 (gate 13 iff-contract) and MEDIUM-1 (universal shipped-first display selection) routed to 135-06 as required verifier-hardening invariants."
  - "HIGH-4 (--precision-spec closed contract) deferred to 135-09+ as a measurement-time deliverable (fed post-135, after CERT-01 actually measures a band)."

patterns-established:
  - "When an adversarial review plateaus (findings churn at a roughly constant severity/count for several consecutive rounds with zero design-level BLOCKERs), the owner can authorize closing the gate on a documented, hash-bound, fully-dispositioned basis rather than iterating indefinitely toward diminishing returns."

requirements-completed: [CERT-01]

# Metrics
duration: ~30min
completed: 2026-07-24
---

# Phase 135 Plan 04: Discovery v2 Bake-Plan Rewrite + Owner-Authorized Codex Gate Close Summary

**Rewrote the v2 discovery-sidecar bake plan against the full 16-merge owner-ratified census, the D-17 chronological co-claim demotion rule, and three hash-pinned build inputs; closed the blocking Codex adversarial-review gate on OWNER AUTHORITY after 9 non-converging rounds, folding the two remaining real defects (collection-precision preservation + coverage-anchor schema) first.**

## Performance

- **Duration:** ~30 min (this closing plan; the underlying 9-round Codex review chain that preceded it ran over several hours in a prior session)
- **Started:** 2026-07-24T03:39:34Z (immediately following round-9 Codex completion)
- **Completed:** 2026-07-24 (see final commit timestamp)
- **Tasks:** 3 (fold round-9 defects; write hash-bound owner-authorized review doc; write SUMMARY + update tracking)
- **Files modified:** 3 (1 modified, 2 created)

## Accomplishments

- Task 1 (rewrite of `docs/specs/discovery-v2-bake-plan.md` against the full census/D-17/three-hash-pinned-inputs/reband design) was completed and Codex-reviewed across 9 rounds in a prior session; this plan folded the two concrete real defects that 9th round raised:
  - **HIGH-1:** the FIRST-build `measurement_status` migration was specified to null EVERY `band_precision` row unconditionally, which would have discarded the pre-existing `scope='collection'` (`propagated_witness_collection_v1`) row that already carries a genuine, complete, previously-computed CI (`precision=0.926`, `ci_low=0.875`, `ci_high=0.968`, `numerator=176`, `denominator=190`, per `discovery-sidecar-schema-v1.md` §1.6). The migration now explicitly preserves that one row unchanged and classifies it `measurement_status='measured_pass'` (consistent with `ci_low=0.875 >= 0.85` per gate 12) -- the underlying propagated-witness-collection population is never touched by this rewrite's census/D-17/reband changes, which act on `track1_direct` claims and the `tier_a`/`screening_rb` bands only.
  - **HIGH-2:** the required `--chrono-coverage-anchor` input (the independent pre-build regression baseline, Option A) had no frozen accepted shape -- a matching SHA-256 file pin proved only which bytes were read, not that they represented the claimed same-basis measurement. Froze an exact 5-key JSON schema (`pair_coverage`, `numerator`, `denominator`, `candidate_universe_id`, `methodology_version`, `measurement_basis`) with typed bounds, a mechanically-verified `candidate_universe_id` digest cross-check against the main build's own `discovery_routing_audit` population, and an exhaustive reject-condition list.
- Task 2 (the blocking Codex adversarial-review gate) is **CLOSED ON OWNER AUTHORITY** rather than via a literal final-line `VERDICT: APPROVE`. The owner authorized this after 9 rounds of adversarial, non-converging review: design-level BLOCKERs (impossible/self-contradictory contracts) were fully eliminated by round 6 (zero BLOCKERs in rounds 7 and 8); every one of Codex's own "requested checks" and the document's full 15-gate verifier inventory were rated **PRESENT** by round 9; and the review had visibly plateaued (HIGH/MEDIUM counts held at 4/2 for four consecutive rounds, with fixes closing targeted gaps while occasionally surfacing one new, narrower edge case -- e.g. round 9's HIGH-2 was itself introduced by the fix that resolved round 8's HIGH-4). The full per-round convergence table, the FINAL post-fix bake-plan SHA-256 this closure is bound to, and a disposition for every round-9 residual finding are recorded in `135-BAKEPLAN-CODEX-REVIEW.md`.
- Every round-9 residual finding is explicitly dispositioned (none silently dropped): 2 FIXED (HIGH-1, HIGH-2, above), 1 accepted as a bounded, already-owner-ratified property with a named refinement track (BLOCKER F5), 2 routed to 135-06 as required verifier/spec hardening (HIGH-3, MEDIUM-1), 1 deferred to 135-09+ as a measurement-time deliverable (HIGH-4), and 1 accepted as a spec-quality note with no build-safety impact (MEDIUM-2).

## Task Commits

Each task was committed atomically:

1. **Task 1: Fold round-9 real defects (preserve collection precision + freeze coverage-anchor schema)** - `c35b45db` (docs)
2. **Task 2: Owner-authorized close of bake-plan Codex gate (hash-bound, residuals dispositioned)** - `57abcac7` (docs)

**Plan metadata:** (this commit) - `docs(135-04): complete plan (SUMMARY + tracking; gate closed on owner authority)`

## Files Created/Modified

- `docs/specs/discovery-v2-bake-plan.md` - Fixed the FIRST-build `measurement_status` migration to preserve the pre-existing collection-level `band_precision` measurement, and froze the `--chrono-coverage-anchor` JSON schema with a mechanically-verified universe-identity binding
- `.planning/phases/135-precision-certificate-confidence-bands/135-BAKEPLAN-CODEX-REVIEW.md` - The hash-bound, owner-authorized gate-closure record: full per-round convergence table (R1 orig 3/9/2 through R9 1/4/2), the FINAL bake-plan SHA-256 this closure approves, and a disposition table for every round-9 residual
- `.planning/phases/135-precision-certificate-confidence-bands/135-04-SUMMARY.md` - This summary

## Decisions Made

- **Closed the Task-2 blocking gate on owner authority, not on a literal `VERDICT: APPROVE`.** This is the plan's single most consequential deviation and is documented in full below.
- Round-9's lone BLOCKER (F5) is a request to further harden an ALREADY-owner-ratified measurement decision (D-17's coarse `DELTA=100y` rule, commit `84054c45`, explicitly ratified "launch-grade"), not a broken contract -- it is recorded as a bounded, recoverable residual (a `review_only` outcome is never data loss, per D-17's own "demotion is a confidence tier, never suppression" invariant) with a named 135-06/v2.1 conservative-interval-semantics refinement track, rather than fixed inline in this plan.
- HIGH-3 (gate 13's conditional-not-iff wording) and MEDIUM-1 (the display-selector's conditional-not-universal specification) are verifier/spec-hardening work naturally scoped to 135-06 (the build/verifier implementation plan) -- the bake-plan DOCUMENT already specifies the correct underlying behavior; the residual is strengthening how the VERIFIER checks it.
- HIGH-4 (`--precision-spec`'s closed contract) is deferred to 135-09+ because that input is fed AFTER the v2 bake, once CERT-01 actually produces a measurement -- its exact contract is a measurement-time deliverable, not a v2-bake-plan blocker.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] First-build migration would have discarded a valid pre-existing measurement**
- **Found during:** Task 1 (folding Codex round-9 findings)
- **Issue:** The FIRST-build `measurement_status` population rule nulled every `band_precision` row unconditionally, including the pre-existing `scope='collection'` row that already carries a complete, valid, previously-computed CI (per `discovery-sidecar-schema-v1.md` §1.6) over a population this rewrite never touches.
- **Fix:** The migration now explicitly names and preserves that one row (fields unchanged), classifying it `measurement_status='measured_pass'` per gate 12's `ci_low >= 0.85` predicate.
- **Files modified:** `docs/specs/discovery-v2-bake-plan.md`
- **Verification:** Manual re-derivation against `discovery-sidecar-schema-v1.md` §1.6 (the cited source of truth) plus the semantic Task-1 verify check (16-merge/no-work_relations/etc. substring assertions) still passing.
- **Committed in:** `c35b45db`

**2. [Rule 1/2 - Bug/Missing Critical] `--chrono-coverage-anchor` had no frozen shape**
- **Found during:** Task 1 (folding Codex round-9 findings)
- **Issue:** A release-blocking regression comparison depended on an input with only a SHA-256 file pin and no exact schema -- the hash proved only which bytes were read, not that they represented the claimed measurement.
- **Fix:** Froze an exact 5-key JSON schema with typed bounds, an exhaustive reject-condition list, and a mechanically-verified `candidate_universe_id` digest cross-check against the main build's own candidate-universe population.
- **Files modified:** `docs/specs/discovery-v2-bake-plan.md`
- **Verification:** Manual re-derivation consistent with every other hash-pinned input's exact-shape convention already established elsewhere in this same document.
- **Committed in:** `c35b45db`

---

**Total deviations:** 2 auto-fixed (both Rule 1/2 -- correctness of the v2-bake-plan contract). Both are the two concrete defects the executor brief explicitly directed be folded before the gate close.
**Impact on plan:** Both fixes necessary for correctness of the frozen build contract; no scope creep beyond the two named defects.

### Central deviation: Task 2's automated verify predicate is intentionally NOT satisfied

The 135-04-PLAN.md Task 2 automated verify requires the review file's **final non-empty line** to be the exact literal string `VERDICT: APPROVE`. **This predicate is deliberately NOT met.** Per explicit executor-brief instruction, Codex was NOT run again in this closing plan; the gate was instead closed **on owner authority**, after 9 prior rounds of non-converging adversarial review (BLOCKERs eliminated by round 6; every requested check + gate rated PRESENT by round 9). This is documented transparently, in full, in `135-BAKEPLAN-CODEX-REVIEW.md` -- which explicitly states in its header that it is NOT an unqualified `VERDICT: APPROVE`, records the FINAL bake-plan SHA-256 this closure is bound to, tabulates the full round-by-round convergence history, and dispositions every one of round 9's residual findings (2 fixed, 1 accepted-as-already-ratified-and-bounded, 2 routed to 135-06, 1 deferred to 135-09+, 1 accepted-as-spec-quality). Downstream plans (135-05+) are unblocked on this owner-authorized basis, not on an automated-verify pass.

## Issues Encountered

None beyond the documented owner-authority deviation above.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- **135-05+ (v2 build code) is unblocked** -- the bake-plan document is stable, hash-bound, and its residuals are all named and owned by specific future plans rather than silently outstanding.
- **Wave 2 (135-02, 135-05) is unblocked** now that 135-04's leadoff Task-2 gate is closed.
- **Forward items for 135-06** (verifier/build-implementation plan): (1) make gate 13 (reband-precision-invalidation) an `iff` contract rather than conditional-on-marker-presence; (2) require universal shipped-first display selection regardless of the Pitfall-2 Track-1 investigation outcome; (3) a conservative-interval-semantics refinement to D-17 (demote only when the DELTA threshold holds across admissible interval endpoints, not point-estimate midpoints) plus a closed semantic-basis contract for the `basis` field -- both routed from round-9 BLOCKER F5, recorded as a v2.1-class refinement, not a launch blocker.
- **Forward item for 135-09+:** `--precision-spec`'s closed mechanical parser/verifier contract (exact JSON shape, closed keys, target-band/scope uniqueness, duplicate handling, immutable-input binding) is a measurement-time deliverable once CERT-01 actually produces a measurement.
- **Phase-136 forward item** (already recorded in the bake plan's own §10): the `later_shared_text` routing-reason surface needs a distinct display label when Phase 136's read surfaces expose demoted claims behind the "show more possible identifications" toggle.
- **Known residual risk (bounded, non-blocking):** D-17's coarse chronology rule can, in principle, demote a claim based on interval-midpoint arithmetic when both sides' dates are themselves interval-derived (century/range) rather than explicit years; this is bounded (recoverable via `review_only`, never data loss) and was already accepted by the owner as part of ratifying the coarse rule as launch-grade (commit `84054c45`).

---
*Phase: 135-precision-certificate-confidence-bands*
*Completed: 2026-07-24*
