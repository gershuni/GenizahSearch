# 135-09 — Owner Attestation — ⚠ SUPERSEDED, DO NOT CITE

**Status: SUPERSEDED 2026-07-28, within the hour, by an actual measurement.**
**See `135-09-CERT01-MEASUREMENT.md`.**

## What this document said

Earlier on 2026-07-28 the owner reviewed the rendered 280-card deck, judged the shipped
identifications broadly sound ("about 95% were what I would expect to find when I ask for
textual witness suggestions"), and declined to record per-card verdicts. This file recorded
that decision and closed Phase 135 on OWNER AUTHORITY with CERT-01 left UNMEASURED. It
also recorded that the verdict ledger was deliberately left empty rather than fabricated,
so the validator's honest 11/12 FAIL stood.

## Why it is superseded

The owner then reversed the decision and graded all 280 cards. The measurement exists:
`verify_cert01_grading.py` reports **12/12, exit 0**, and the pre-registered weighted
estimand is **0.9382, 95% CI [0.9084, 0.9644] — PASS** against the D-07 Strict floor.

The owner-authority close is therefore void. Phase 135 closes on the **real mechanical
D-02 signal**, not on a written attestation.

## Why it is kept rather than deleted

Two reasons worth preserving:

1. **The ~95% impression proved close but not identical to the measurement.** The
   attestation's own §3 predicted this: the deck rubric counts **A alone** as correct
   (a B — related work but not W — is a miss), so a usefulness impression is an UPPER
   BOUND on precision. Realized: 95% impression vs **0.900 unweighted** / 0.9382 weighted
   A-rate. The reasoning held up, which is why the caveat is worth remembering the next
   time an impression is offered in place of a measurement.
2. **It records that the ledger was never fabricated.** At the point where closing the
   phase looked like it required a verdict, none was invented. That restraint is what made
   the later real measurement meaningful instead of contaminated.

**Nothing in this file may be cited as evidence.** The ~95% is not a precision figure and
never was. Use `135-09-CERT01-MEASUREMENT.md`.

---

*Phase 135, plan 09 — superseded attestation, retained as decision trail only.*
