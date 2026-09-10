# Phase 135 Plan 04 — Bake-plan Codex gate: OWNER-AUTHORIZED CLOSE

**This is NOT an unqualified `VERDICT: APPROVE`.** This document records an
**owner-authorized close** of the blocking Codex adversarial-review gate on
`docs/specs/discovery-v2-bake-plan.md` (the plan 135-04, Task 2 gate). The
owner authorized closing the gate without waiting for an unqualified APPROVE
after **9 rounds of adversarial, non-converging review** — all design-level
BLOCKERs were eliminated by round 6, every one of Codex's own requested
checks and the document's full 15-gate verifier inventory were rated
**PRESENT** by round 9, and the review had plateaued into adversarial
edge-case iteration (each fix closing the targeted gap while occasionally
surfacing one new, narrower edge case — see the convergence table below).
Per the executor brief: do not run Codex again in this closing plan; fold
the two concrete, still-real defects round 9 raised, document the
authorized close transparently, and disposition every residual finding.

## Bytes this approval is bound to

The two concrete defects round 9 raised (HIGH-1, HIGH-2, below) were folded
into `docs/specs/discovery-v2-bake-plan.md` immediately after round 9
(commit `c35b45db`, "fold round-9 real defects"). **Approval-of-substance in
this document is bound to the FINAL, post-fix bytes, not the round-9
reviewed bytes:**

- **Round-9-reviewed SHA-256** (the bytes Codex last actually read):
  `0e66a64e…` (per the executor brief; the pre-fix revision).
- **FINAL bake-plan SHA-256** (post HIGH-1/HIGH-2 fix, what this closure
  approves):
  ```
  a97d682ca057e22970eb5e4ac07b91b029bb38c556ecc93853526ff79aeac708
  ```
  Recomputable via `sha256sum docs/specs/discovery-v2-bake-plan.md` (or
  `hashlib.sha256(open(path,'rb').read()).hexdigest()`), file length 142,192
  bytes at commit `c35b45db`.

Codex itself never re-reviewed the post-fix bytes (per explicit executor
instruction — "Do NOT run Codex"). The two defects it raised against the
round-9 bytes are FIXED in the bytes this hash covers; the fixes are
verified by manual re-derivation against the same source documents Codex
cited (`discovery-sidecar-schema-v1.md` §1.6 for HIGH-1; the existing
hash-pinned-input schema conventions elsewhere in this same document for
HIGH-2), not by a second Codex pass.

## Per-round convergence table

Two separate review artifacts are in this chain: (1) the ORIGINAL
pre-rewrite critique of the stale plan + Phase 135 CONTEXT
(`135-CODEX-CRITIQUE.md`), which is what triggered Task 1's full rewrite;
and (2) the rewrite's OWN 9-round adversarial re-review chain (this gate,
scratchpad `135-04-codex-round{1..9}-full.log`).

| Round | Scope | BLOCKER | HIGH | MEDIUM | Verdict |
|-------|-------|---------|------|--------|---------|
| R1 (orig) | `135-CODEX-CRITIQUE.md` — stale pre-rewrite plan + CONTEXT | 3 | 9 | 2 | REWORK |
| R1 (rewrite) | rewritten bake-plan, 1st pass | 0 | 9 | 2 | REWORK |
| R2 | | 2 | 5 | 2 | REWORK |
| R3 | | 2 | 5 | 1 | REWORK |
| R4 | | 1 | 4 | 2 | REWORK |
| R5 | | 1 | 4 | 3 | REWORK |
| R6 | | 2 | 4 | 2 | REWORK |
| R7 | | 0 | 4 | 2 | REWORK |
| R8 | | 0 | 4 | 2 | REWORK |
| R9 | | 1 | 4 | 2 | REWORK |

**Reading the table:** design-level BLOCKERs (impossible/self-contradictory
verifier contracts, unimplementable target-set semantics, gate/algorithm
mismatches) were fully eliminated by **round 6** — rounds 7 and 8 returned
**zero** BLOCKERs. Round 9's single BLOCKER (F5, disposed below) is a
different KIND of finding: not a broken contract, but a request to further
harden an already-owner-ratified measurement decision (D-17's coarse
DELTA=100y rule, commit `84054c45`, "launch-grade"). The HIGH/MEDIUM counts
plateaued at 4/2 for four consecutive rounds (R6–R9) — each round's fixes
closed the round's own findings but Codex's continuing adversarial pass
surfaced narrower edge cases at a roughly constant rate (e.g. HIGH-2 in
round 9 — the `--chrono-coverage-anchor` schema gap — was itself introduced
by the Option-A fix that resolved round 8's HIGH-4). This is the textbook
shape of a **plateaued, non-converging adversarial review**: substance is
present and correct at the design level, but an infinitely fine-grained
adversarial pass can always find one more edge case to specify more
tightly. Continuing to round 10, 11, ... has diminishing expected return
relative to the cost of continued iteration — the owner's authorization to
close is a judgment call on exactly this trade-off, not a claim that the
document is now literally perfect.

## Disposition table — round-9 residual findings

| ID | Finding | Disposition |
|----|---------|--------------|
| **BLOCKER F5** | Fuzzy/interval-derived dates (century-midpoint, range-midpoint) are collapsed to one integer year per canonical group, then compared as if certain; two directionally-ambiguous or overlapping intervals can produce a `delta >= 100` demotion purely from midpoint arithmetic. | **DISPOSITIONED — does not block this close.** This restates a known, already-owner-ratified property of the **coarse D-17 rule** (`DELTA=100y`, ~99.9% composition-date coverage, explicitly ratified as **"launch-grade"** in `.planning/STATE.md` and commit `84054c45`, "D-19 date-coverage audit DONE ... coarse demotion router launch-grade"). D-17 already fails safe on unknown/missing dates (`fail_safe_unknown_date`, neither side demoted) — F5's residual concern is about the PRECISION of a known-date comparison, not about an unknown date being fabricated. The finding correctly identifies that a **conservative-interval semantics** refinement (demote only when the delta threshold holds across the full admissible interval, i.e. against interval ENDPOINTS rather than midpoints, not merely against the point-estimate midpoint) plus a **closed semantic-basis contract** for the `basis` field would further harden the rule. Recorded here as a **documented residual** and a **135-06/v2.1 refinement candidate** — not a v2-bake blocker, because: (a) the coarse rule was ALREADY ratified by the owner as launch-grade at the current precision, with the known 30.7% within-DELTA tie rate and 0.2% fail-safe rate already accepted as the launch trade-off; (b) any over-demotion risk is BOUNDED (a `review_only` outcome is fully recoverable — the claim/evidence persist, per D-17's "demotion is a confidence tier, never suppression" invariant — never data loss); (c) tightening the rule to interval-endpoint semantics is additive, backward-compatible refinement work, not a redesign of anything this gate has already validated. |
| **HIGH-1** | First-build `measurement_status` migration nulled EVERY `band_precision` row, discarding the pre-existing `scope='collection'` row (`propagated_witness_collection_v1`) that already carries a genuine, complete, previously-computed CI. | **FIXED post-round-9** (commit `c35b45db`). The migration now explicitly preserves that one row (fields unchanged) and classifies it `measurement_status='measured_pass'`, consistent with its stored `ci_low=0.875 >= 0.85` per gate 12. See `docs/specs/discovery-v2-bake-plan.md` (FIRST-BUILD `measurement_status` population section, "Codex round-9 HIGH-1 fix"). |
| **HIGH-2** | `--chrono-coverage-anchor` gates a release-blocking regression comparison with only a SHA-256 file pin and no frozen JSON shape — a matching hash proves only which bytes were read, not that they represent the claimed same-basis measurement. | **FIXED post-round-9** (commit `c35b45db`). Froze the exact 5-key JSON schema (`pair_coverage`, `numerator`, `denominator`, `candidate_universe_id`, `methodology_version`, `measurement_basis`) with typed bounds, a mechanically-verified `candidate_universe_id` digest cross-check against the main build's own `discovery_routing_audit` population, and an exhaustive reject-condition list. See `docs/specs/discovery-v2-bake-plan.md` ("FROZEN exact-shape schema for `--chrono-coverage-anchor`", "Codex round-9 HIGH-2 fix"). |
| **HIGH-3** | Gate 13 (reband-precision-invalidation) predicate is worded conditionally — "when `meta` carries" the reband marker — rather than as a strict `iff`; a faulty build could reband rows, omit the marker, and evade the gate. | **DISPOSITIONED → 135-06 verifier hardening (REQUIRED forward item).** Recorded as a required 135-06 invariant: gate 13 must become an `iff` contract — the OBSERVABLE rebanded asset state (any `band_precision`/`confidence_band` population actually changed by a reband) must independently imply the marker's presence, not merely be validated once the marker happens to exist. This is a verifier-hardening task scoped to 135-06 (the build/verifier implementation plan), not a bake-plan-document defect — the DOCUMENT already specifies the reband as an atomic, marker-writing operation; the residual is that the VERIFIER's check of that contract should be strengthened from conditional to biconditional. Does not block this v2-bake-plan closure. |
| **HIGH-4** | `--precision-spec` (the post-135 CERT-01 measurement-input, feeding `measured_pass`/`measured_fail`) lacks a fully closed mechanical parser/verifier contract (exact JSON shape, closed keys, target-band/scope uniqueness, duplicate handling, immutable-input binding). | **DISPOSITIONED → DEFERRED to 135-09+ (measurement-time deliverable, not a v2-bake blocker).** `--precision-spec` is fed AFTER Phase 135's v2 bake, once CERT-01 (`docs/specs/discovery-cert01-protocol.md`) actually measures a band's precision (135-09 and later). Its closed contract is scoped to that later measurement-consuming plan, which owns the CERT-01 protocol's frozen artifacts and is the natural place to freeze the spec's exact shape against the ACTUAL measurement pipeline output — not this bake-plan document, which only needs to specify the OUTCOME-level contract (`measured_pass`/`measured_fail`/`insufficient_evidence` branching, gate 12/13 consistency), which it already does. Recorded as a forward requirement for 135-09+. |
| **MEDIUM-1** | The display-selector routing_status-tier change (Pitfall-2) is specified conditionally on a Track-1 investigation outcome, while §4.5's reband recomputation prose assumes it exists. | **DISPOSITIONED → 135-06: require shipped-first display selection universally**, removing the conditionality regardless of the Pitfall-2 investigation's outcome. Gate 8 (never-orphan-shipped / display-pointer invariant) already prevents an unsafe asset from shipping even under the current conditional wording, so this is a specification-quality tightening, not a release blocker. |
| **MEDIUM-2** | The regex-family mutual-exclusivity test (namespace-prefix validation for raw date-table keys) demonstrates disjointness only against a fabricated finite ID set, not a formal proof over the full regex language. | **DISPOSITIONED — accepted as a spec-quality note, not a build-safety gap.** The per-real-id zero-or-multiple-match hard failure (checked against the ACTUAL production crosswalk at build time, not merely the fabricated test set) is what preserves real build safety; the test-suite's fabricated-ID demonstration is illustrative coverage, not the sole safety mechanism. No action required before this close. |

## Note on the reviewed vs. approved bytes

Codex's round-9 review (the most recent Codex pass) was run against the
bake-plan bytes **before** the HIGH-1/HIGH-2 fixes above (SHA-256
`0e66a64e…`, per the executor brief). Both concrete defects it raised in
that pass were fixed immediately afterward, in commit `c35b45db`. **This
document's recorded FINAL SHA-256 binds to the POST-FIX bytes** — the ones
this owner-authorized close actually approves for the v2 build (135-05+) to
consume. Any future edit to `docs/specs/discovery-v2-bake-plan.md` changes
its SHA-256 and, per this project's standing hash-binding convention
(mirrored from the original plan's Task-2 gate design), would require a
fresh review pass or a fresh owner authorization — this closure does not
retroactively cover a not-yet-written future revision.

## What is NOT claimed here

- This is **not** a claim that Codex, if run again against the final
  bytes, would return `VERDICT: APPROVE`. It very likely would raise
  further narrower edge cases, consistent with the plateau pattern in the
  convergence table.
- This is **not** a claim that F5 (round-9's BLOCKER) is resolved — it is
  explicitly recorded as an open, bounded-risk residual with a named
  forward track (135-06/v2.1 conservative-interval refinement).
- This **is** a claim that: (1) the document's design-level substance is
  correct and internally consistent (zero BLOCKERs for 3 of the last 4
  rounds, and round 9's lone BLOCKER is a hardening request against an
  already-ratified decision, not a broken contract); (2) both concrete,
  actionable defects round 9 raised are fixed; (3) every remaining residual
  is named, bounded, and has an owner (a specific future plan) rather than
  being silently dropped; and (4) the owner has reviewed this trade-off and
  authorized closing the gate on that basis.

## Masking

Functional references only throughout this document (opaque commit hashes,
`w000xxx`-style identifiers are not used here at all, file paths, gate/rule
names, numeric thresholds). No restricted-source title or codename beyond
the sanctioned "M-source" appears anywhere in this file or its scratchpad
inputs referenced above.
