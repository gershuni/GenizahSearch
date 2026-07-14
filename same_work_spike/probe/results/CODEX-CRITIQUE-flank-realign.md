# Codex critique — citation-vs-witness flank detector (MAPV2-15m, "Way 2")

Verdict: **A is necessary; C is not strong enough to be primary.** Use **A+B as
the primary target-work continuity detector**, **C only as independent
citation/other-work evidence**. D remains a baseline/reporting feature.

## 1. Method choice (three-part detector)
1. **Target continuity (primary):** relocate the page span in the reference and
   test whether the page reconverges to the same reference work after a bounded
   gap.
2. **Whole-page target coverage (supporting):** does target-work alignment
   explain substantial text outside the original maximal span?
3. **Competing-work flank (confirmatory):** if another non-equivalent work
   explains the page flanks, strengthen a citation verdict. Absence of
   competitors is NEUTRAL.

C is not primary: Track-1 competitors are incomplete/corpus-dependent; "no other
work matched" ≠ "same-work witness." C is excellent only when positive.

## 2. Algorithm — per (page_id, work_id, spans_json)
1. Normalize page + ref to letter streams; merge page spans with gaps <15.
2. **Relocate each page span in the target ref:** k=5 exact grams from the
   trimmed span core (fallback k=4 only if no credible k=5); ignore very
   high-freq grams within the work; vote by diagonal (ref_pos − page_pos, bin
   ~25); re-score top 8 diagonals by banded semi-global edit alignment of the
   full span vs the ref window; keep candidates with span normalized edit ≤0.38
   (≤0.42 for spans <80) and ≥2 anchor grams. Keep ALL close candidates; for
   citation demotion, require ALL credible relocations to be islands — if one
   shows continuation, do NOT demote.
3. **Monotone target chain** for multi-span: preserve page order + ref order;
   discordant page/ref gaps → ambiguity evidence, not citation. Evaluate flanks
   outside the first/last span of the best chain.
4. **Flank reconvergence test:** up to 220 letters outward each side (reverse
   the left); **allow an unmatched edge gap up to 60 letters on page OR ref —
   this is the key fix for the maximal-span boundary problem**; find the best
   aligned continuation block (preferred 80-120, min 60). Side labels:
   - continuation: block ≥60 & norm-edit ≤0.42 (weak to 0.48 with ≥2 anchors &
     gap ≤40)
   - ambiguous: 0.48-0.58
   - island: enough text & >0.58
   - short/edge: not enough text
5. **Whole-page target support:** adjusted coverage = original spans + recovered
   continuation blocks (union over page coords). Positive witness if page ≥200
   & adjusted coverage ≥0.55, or target-aligned text outside original spans
   ≥120. Low coverage ALONE is not citation.
6. **Competing-work flank (C):** from track1_candidates, other-work coverage in
   regions outside the target chain. Strong C = a non-equivalent, non-canonical
   competitor (p_same_work ≥0.65 or matched_letters ≥100) covering ≥90 letters
   outside the target span or ≥45% of a judged flank. Canonical competitors do
   NOT trigger demotion for a non-canonical target; a non-canon competitor
   explaining the flanks of a CANONICAL target IS strong citation evidence.

Decision rule:
- `target_continuation`: strong reconvergence OR strong whole-page support, no
  stronger contradictory C.
- `likely_citation_strong`: no target continuation AND ≥2 independent negatives
  (both sides island; or one island + strong C; or strong C both flanks + low
  target coverage on a long page).
- `mixed_multiwork`: target continuation + strong other-work → route separately,
  NOT simple citation.
- `abstain`: everything else.

## 3. Abstention (preserve the fragmentary tail)
Negative demotion requires enough text; else neutral. Abstain when: relocation
fails/ambiguous; page text outside the span chain <120; no side has ≥60 usable
page AND ref letters; only one side island w/o strong C; any ambiguous side;
edge with one weak flank; only canonical/common-source competitor. **Short
fragments: neutral, NEVER penalize** (positive continuation may still boost).

## 4. Score integration (advisory)
Keep `disc_score2` unchanged; add `flank_verdict`, `flank_strength`,
`flank_multiplier`, `flank_evidence_line`, and `disc_score2_flank = disc_score2
* flank_multiplier`. Multipliers: strong continuation 1.10, weak continuation
1.04, abstain/ambiguous/mixed 1.00, weak citation 0.75, strong citation 0.45.
NO hard filter — strong citation may route to "Other / citation review" but the
row stays visible with score + evidence.

## 5. Validation without leakage
Calibrate on the 132 gold ONLY (predefined labels: continuation=must-not-demote,
island=desired-demotion, edge/short=acceptable-abstain). Narrow grid-search:
flank min 50/60/80, gap 40/60/80, continuation thr 0.40/0.42/0.45/0.48, island
thr 0.56/0.58/0.62. Objective: max citation recall subject to ≤1 false
strong-demotion of a same-work witness; report Wilson intervals; report by
length/flank-availability strata. FREEZE thresholds + multipliers, then run the
100 held-out ONCE (report strong-citation precision, same-work false-demotion
rate, abstention rate, citation recall among non-abstain, top-slice rank churn).
If it fails, revise on 132 + new dev data, never by iterating on held-out.

## 6. Ranked pitfalls
1. Recension divergence mistaken for citation → bounded reconvergence, ambiguous
   band, no one-side demotion.
2. Short/edge fragments falsely demoted → strict abstention, neutral multiplier.
3. Canon inside legitimate works → canonical competitor never triggers C for a
   non-canonical target.
4. Commentary/lemma interleaving → classify mixed/lemma-context, not citation.
5. Palimpsest/composite pages → region-level evidence, mixed queue.
6. Duplicate passages inside one reference → keep multiple relocations, demote
   only if ALL are islands.
7. Same work-family split across work_ids → title/author equivalence buckets
   before treating Y as a competitor.
8. Circularity with the canon penalty → flank signal stays separate; no extra
   citation penalty for canon-only evidence.
9. Whole-page coverage bias vs damaged fragments → B is positive/supporting,
   never a standalone negative rule.
10. HTR-noise drift by script/genre → calibrate thresholds by length/noise
    strata, keep a wide ambiguous band.
