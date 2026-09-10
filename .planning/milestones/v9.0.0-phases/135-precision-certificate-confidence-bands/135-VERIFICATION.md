---
phase: 135
status: passed
verdict: closed-on-evidence
method: retrospective-attestation
verifier_run: false
attested: 2026-08-20
attested_by: "owner-directed roadmap re-map — NOT a gsd-verifier run"
caveat: "Phase work done and CERT-01 measured, but three outputs are unapplied and CERT-01's own status stays Pending — see Phase 139a."
---

# VERIFICATION — Phase 135: Precision Certificate & Confidence Bands

> **THIS IS A RETROSPECTIVE ATTESTATION, NOT A `gsd-verifier` RUN.**
> Written 2026-08-20 during the second roadmap re-map. No goal-backward verifier agent was
> spawned for this phase, no VERIFICATION agent analysed the codebase against the phase goal,
> and nothing here was produced by `/gsd-execute-phase`'s verification gate. What follows is the
> evidence the phase's closure actually rests on — production behaviour, commit references, and
> the plan summaries on disk — recorded so that the planning tooling stops reporting a phase as
> incomplete when the record and the live site both say otherwise.
>
> **Verdict: CLOSED ON EVIDENCE.** Treat it as an owner-level attestation, not a machine check.
> If a genuine verifier pass is ever wanted here, run `/gsd-execute-phase 135` — it resumes at
> the verification gates and does not re-run plans that already have a SUMMARY.

**Phase**: 135 — Precision Certificate & Confidence Bands
**Closed**: 2026-07-28
**Plans**: 9 of 9 have a SUMMARY.md
**Attested**: 2026-08-20
**Method**: retrospective, from production state + git history + on-disk plan summaries

---

## What the closure rests on

This is the strongest-evidenced phase in the milestone, and unusually it rests on a **measurement**
rather than on shipped behaviour.

| Claim | Evidence | Grade |
|---|---|---|
| CERT-01 measured PASS | Pre-registered weighted precision **0.9382, 95% CI [0.9084, 0.9644]** against the 0.85 Strict floor — `135-09-CERT01-MEASUREMENT.md` | measured |
| The grading was blind and validated | Owner graded all 280 cards catalogue-blind; validator 12/12; `135-09-OWNER-ATTESTATION.md`, `cert01_prereg.json`, `cert01_deck_manifest.json` on disk | measured |
| Public-scope subgroup | 0.9580 CI [0.9240, 0.9847] — **descriptive, not pre-registered**, and must not be quoted as a pre-registered result | measured |
| The four-band display contract ships | Band labels and honesty-safe vocabulary live in `shared/discovery_band_labels.py` / `discovery_display_strings.py`, in production since 2026-08-08 | source + production |
| Every plan produced a summary | 9 PLAN.md / 9 SUMMARY.md on disk | on-disk |

## What this attestation does NOT establish — and what is still owed

Three items were open at closure and are **still open**, now homed in Phase 139a:

1. `band_precision` has **not** been re-baked — `tier_a` carries no number (needs `--precision-spec` + deploy).
2. The CERT-02 outcome-specific copy is **unapplied**. Moot while the no-percentages ruling stands, but unrecorded.
3. The **per-stratum spread has not reached the BAND-05 methods page**: 1.000 (`ja`) down to **0.471**
   (`msource:medium`, n=17), with one work causing **45%** of all measured error and the top three
   causing 68%. The measurement document states plainly that publishing the weighted headline without
   the spread "would mislead anyone in the weak stratum."

**CERT-01's own status therefore remains `Pending`, not `Complete`**, and this attestation does not
change that. What is attested is that the phase's work was done and its measurement was taken — not
that its outputs have all been applied to the public surfaces.

The liturgical-containment false-positive class (one work = 45% of the error; D-17 structurally cannot
catch it) is a **discovery-v3** candidate and is now scoped in Phase 148.
