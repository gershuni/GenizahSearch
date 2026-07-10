# CAL-1 pilot — clean vs noise-injected calibration (2026-07-10)

Three arms, identical protocol (979 single-work pages, 13,706 crops, all-
candidate recording, chunk-shuffle decoys, work-granular holdout): **clean**
(`p_calibration_pilot.md`), **n10** (+10% empirical HTR noise,
`…_pilot-n10.md`), **n20** (+20%, `…_pilot-n20.md`). Noise sampled from the
measured confusion profile (`confusion_matrix.json`: 209 FGP-aligned pages,
micro-CER 20.1%, median 16.6%; י↔ו/ד↔ר/ב↔כ etc. + empirical ins/del mix).

## 1. Recall ceiling vs noise (share of crops whose true work forms ANY
## wide-cutoff candidate)

| len | clean | n20 |
|---|---|---|
| 40 | 97.1% | **77.2%** |
| 60 | 99.5% | 91.3% |
| 80 | 99.7% | 96.1% |
| 100 | 99.8% | 98.2% |
| 150+ | ~100% | 99.4–100% |

At upper-bound noise the k=5 gram model itself loses ~23% of 40-letter
fragments (no anchor survives). ≥100 letters the ceiling stays ≥98% even at
20% CER — **length, not noise, is the structural constraint above 100
letters.**

## 2. Operating points shift right with noise (max density for P ≥ 0.5)

| len | clean | n10 | n20 |
|---|---|---|---|
| 40 | 0.614 | 0.632 | 0.635 |
| 60 | 0.543 | 0.560 | 0.604 |
| 80 | 0.497 | 0.530 | 0.560 |
| 100 | 0.448 | 0.490 | 0.530 |
| 150 | 0.394 | 0.430 | 0.473 |
| 200 | 0.354 | 0.402 | 0.435 |
| 300 | 0.302 | 0.367 | 0.414 |

Same shape, deeper acceptance — the noisy curves correctly re-price densities
once noise is known to be present. **Deployment rule: pick the curve by page
provenance** — clean curve for FGP-transcribed pages, noisy curve (bracket by
measured/estimated page CER) for HTR pages.

## 3. Recall at the P ≥ 0.5 operating point (per arm, per length)

| len | clean recall | n20 recall | wrong-share in accepted pool (n20) |
|---|---|---|---|
| 40 | 0.807 | 0.526 | 0.434 |
| 60 | 0.902 | 0.828 | 0.351 |
| 80 | 0.941 | 0.890 | 0.264 |
| 100 | 0.938 | 0.926 | 0.243 |
| 150 | 0.968 | 0.953 | 0.185 |
| 200 | 0.969 | 0.958 | 0.149 |
| 300 | 0.967 | 0.977 | 0.123 |

vs the production census boundary: ~0% @40 / 28% @100 (FRAG-1). The
recall-first tier is real under both noise assumptions.

## 4. Holdout reliability — holds in all three arms

0.9-bucket → 0.936 / 0.936 / 0.933 empirical (clean / n10 / n20);
0.7-bucket → 0.704 / 0.708 / 0.724. The probability grade is trustworthy on
the synthetic-population terms of each arm.

## 5. The density_fail stress cards — noise closes only part of the gap

Hillel's 10/10-correct real cards (density 0.41–0.55, len 125–298),
predicted P by arm:

| len | dens | clean | n10 | n20 |
|---|---|---|---|---|
| 125 | 0.472 | 0.236 | 0.837 | **0.891** |
| 298 | 0.406 | 0.017 | 0.014 | **0.570** |
| 241 | 0.456 | 0.030 | 0.055 | 0.321 |
| 222 | 0.477 | 0.030 | 0.051 | 0.261 |
| 257 | 0.463 | 0.009 | 0.009 | 0.062 |
| 276 | 0.460 | 0.009 | 0.009 | 0.062 |
| 145 | 0.552 | 0.004 | 0.016 | 0.026 |
| 176 | 0.545 | 0.002 | 0.007 | 0.010 |
| 272 | 0.526 | 0.001 | 0.001 | 0.002 |

n20 rescues the head (4/9 ≥ 0.26) but not the tail. **Diagnosis:** at those
(length, density) points the synthetic pool is dominated by wrong candidates
because chance matches are ABUNDANT there — but a chance-dominated pool is
exactly where the *structure* of the candidate set separates real from
chance: Hillel's cards were all SINGLE coherent candidates (large margin, one
work aligning across the span), while chance hits scatter across many works.
(length, density) alone cannot see this; margin/singleton status can, and it
is known at deployment time.

## Consequences for FINAL CAL-1 / Map v2 (plan already Codex-cleared for
## richer features)

1. **Per-provenance curves:** clean model for FGP-text pages; noise-bracketed
   model for HTR pages (n10/n20 bracket; page-level CER estimate picks the
   interpolation when available).
2. **Add a margin feature** to the FINAL fit: P(correct | length, density,
   margin-band), margin = density gap to the best OTHER work (∞ = singleton).
   This is the rescue for the long-noisy-single-candidate regime — the
   density_fail class Hillel validated 10/10.
3. Tier-B default operating point proposal stands: **P ≥ 0.5** per the
   provenance-appropriate curve (recall 53–93% at 40–100 letters even under
   n20), with the full P-ranked list retained down to P ≥ 0.2 for
   human-triage depth. Hillel picks the final point on the FINAL curves.
4. The ~23% @40-letter ceiling loss under n20 is structural (no surviving
   anchors) — those fragments need FGP text (where available), lower-k
   matching, or idiom search; no threshold recovers them.
