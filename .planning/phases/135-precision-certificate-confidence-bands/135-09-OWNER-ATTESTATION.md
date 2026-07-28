# 135-09 — Owner Attestation in Lieu of Per-Card Grading

**Date:** 2026-07-28
**Decision by:** owner (Hillel Gershuni), after reviewing the rendered 280-card deck
**Effect:** Phase 135 closes on OWNER AUTHORITY. The mechanical D-02 grading-STARTED
signal is **NOT** satisfied. CERT-01 remains **unmeasured**.

---

## 1. What the owner did and said

The owner opened the rendered deck (`same_work_spike/probe/review/cert01_deck.html`,
280 cards) and reviewed it in full. Two statements, recorded verbatim:

> "Most identifications I saw in the first dozens are correct."

> "I went quickly through the 280 cards. They are mostly ok."

> "about 95% were what I would expect to find when I ask for textual witness suggestions"

The owner then declined to record per-card verdicts, and directed that this
attestation be recorded instead so the milestone can move on.

## 2. What this attestation IS

A genuine, first-hand expert impression from the project's own domain expert, formed
over the actual frozen CERT-01 deck — not a guess, and not a proxy. It is meaningful
evidence that the shipped `tier_a` identifications are broadly sound, and it is the
reason the owner judged a formal measurement not worth its cost right now.

## 3. What this attestation is NOT — read before citing the 95%

**It is not the CERT-01 measurement, and the 95% must never be presented as a precision
figure.** Four independent reasons:

1. **Different question.** The deck's rubric is A / B / C / INS, where **A alone** counts
   as a correct identification: A = the page is a genuine witness of work W; B = a
   related work but NOT W; C = no shared work identity; INS = insufficient evidence.
   The owner's phrasing — "what I would expect to find when I ask for textual witness
   suggestions" — is a **usefulness** judgment. A B-type card (related work, not W) is a
   miss for precision but is plausibly still a useful suggestion. So ~95%-as-expected is
   an **upper bound** on, and not comparable to, A-only precision.
2. **No per-card record.** Nothing ties the number to specific uids, so it cannot be
   audited, recomputed, or re-graded, and the gold-repeat consistency check (20 cards)
   and the demoted/retained diagnostic (40 cards) were never scored.
3. **No confidence interval and no clustering.** The CERT-01 design computes a bound
   clustered by physical MS. A single global impression has no interval, so it cannot be
   compared to the Strict 0.85 floor at all.
4. **Not blinded in the protocol sense.** Reveals were not logged, and the 280 cards mix
   candidate, gold and diagnostic roles that the protocol requires be scored separately.

**Therefore:** this number is NOT written into `band_precision`, NOT rendered on any
public surface, and NOT eligible for the BAND-05 methods page. `tier_a` continues to
carry **no** measured precision (`band_precision` id=5), which is the correct and honest
state per CERT-02 ("tier-A shows no precision number until its CERT-01 certificate
lands"). The prohibited-wording rules stand: "certified" is never used.

## 4. Mechanical state — unchanged and honest

`scripts/verify_cert01_grading.py` still reports **11/12**, with check 6 ("grader
attribution present (>=1 verdict)") failing because
`same_work_spike/probe/review/cert01_deck_verdicts.json` is genuinely `[]`.

**The ledger was deliberately NOT populated.** Writing verdicts the owner did not record
per-card would be precisely threat T-135-09-05 (spoofing a forged grading-started signal)
that the twelve-check validator was built to prevent. The validator's honest FAIL is more
valuable than a green light bought by fabrication.

Everything else stands and re-verifies: the immutable pre-registration
(`cert01_prereg.json`, `report_id` recomputes), the separate deck manifest bound to it,
the pre-outcome OC table, the frozen 134,123-row estimand, and the four input hashes +
`crosswalk_sha256` + `cluster_map_hash` all pinned to the deployed v2 asset.

## 5. Consequence deferred to Phase 139 (not a problem today)

Nothing user-facing depends on this now — the discovery flag is OFF and `tier_a` shows no
number. The bill comes due at **REL-01**, which as written requires "the CERT-01
measurement is **graded to completion**" and that "tier-A goes public **WITH its measured
number**" before the flag flip.

So at Phase 139 the owner must pick one of:

| Option | What it means |
|---|---|
| **Grade then** | Run the deck properly before the flip. The deck, pre-registration and validator are all frozen and ready; nothing expires. |
| **Ship tier_a with no number** | Consistent with CERT-02's "no number until the certificate lands", but requires amending REL-01's "WITH its measured number" clause. |
| **Amend REL-01** | Owner-ratified change to what the release gate demands — recorded like the 135-04 gate closure was. |
| **Reband** | Move `tier_a` behind the BAND-03 toggle rather than measure it. |

**CERT-01 stays `Pending` in `REQUIREMENTS.md`.** Closing Phase 135 does not satisfy it.

## 6. Precedent

This mirrors the documented 135-04 pattern: a blocking gate closed on explicit owner
authority rather than on its literal mechanical signal, with the shortfall named in
writing instead of papered over. See the STATE.md Blockers entry for 135-04's Codex gate.

Also consistent with the owner's standing steer that the discovery gates are a
vibe-check rather than a certified experiment (memory
`feedback_discovery_vibe_not_experiment`) — with the caveat that a vibe-check yields a
disclaimer, never a published precision number.

---

*Phase 135, plan 09 — owner attestation; CERT-01 unmeasured; phase closed on owner authority.*
