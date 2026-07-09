# A5 — conformal + FDR thresholds via target-decoy null (probe.db dry run)

Corpus: 17,228 real pages + 4,307 chunk-shuffle decoys (CHUNK=25, seed=29); production candidate mode (k=5, DF<=100, two-hit, band=20).
Candidates: 16,894,135 — tested (real-real) 11,024,561, null (decoy-involved) 5,869,574 (decoy candidate volume = 34.7% of all).

## Null density distribution (per length bin)
| bin | n null | median | q05 | q01 |
|---|---|---|---|---|
| [25,50) | 5,292 | 0.778 | 0.651 | 0.565 |
| [50,100) | 4,619,298 | 0.773 | 0.697 | 0.635 |
| [100,200) | 291,781 | 0.757 | 0.684 | 0.592 |
| [200,400) | 376,957 | 0.778 | 0.734 | 0.681 |
| [400,800) | 356,183 | 0.791 | 0.759 | 0.735 |
| 800+ | 220,063 | 0.800 | 0.777 | 0.761 |

## FDR-bounded operating table (max accepted density per bin)
| bin | hand boundary | q=0.01 | q=0.05 |
|---|---|---|---|
| [25,50) | 0.300 | — | — |
| [50,100) | 0.300 | 0.374 | 0.485 |
| [100,200) | 0.386 | 0.396 | 0.443 |
| [200,400) | 0.418 | 0.469 | 0.540 |
| [400,800) | 0.418 | 0.572 | 0.634 |
| 800+ | 0.418 | 0.662 | 0.692 |

## Acceptance volume + incumbent-boundary FDR estimate
- tested accepted by HAND boundary: 10,999; decoy-side accepted: 18; population ratio 1.88 -> point estimate FDR = 0.31%. With 0 of 5,869,574 null acceptances, the one-sided 95% bound is <= 0.051% of hand-accepted pairs (rule-of-three x ratio). This bounds CHANCE-alignment errors only — it is a different (narrower) error class than human semantic grading.
- BH q=0.01: accepted 15,228 tested pairs (p-cut 1.37e-05)
- BH q=0.05: accepted 28,894 tested pairs (p-cut 1.30e-04)

## Tier-1 ground-truth recall (pairs present in probe corpus)
| family | GT pairs | as candidates | hand | q=0.01 | q=0.05 |
|---|---|---|---|---|---|
| joins | 36 | 36 | 35 | 36 | 36 |
| titles | 1866 | 1866 | 1832 | 1861 | 1863 |
| bh | 118 | 118 | 114 | 117 | 118 |

## Per-stratum null sensitivity (same-bucket null pairs; median/q05)
| bucket | [25,50) | [50,100) | [100,200) | [200,400) | [400,800) | 800+ |
|---|---|---|---|---|---|---|
| background | 0.783/0.667 | 0.773/0.697 | 0.759/0.681 | 0.780/0.733 | 0.792/0.761 | 0.799/0.777 |
| titles | 0.761/0.634 | 0.773/0.682 | 0.750/0.665 | 0.772/0.712 | 0.787/0.744 | 0.799/0.772 |
| bh | 0.776/0.578 | 0.708/0.581 | 0.685/0.558 | 0.730/0.636 | 0.770/0.712 | 0.793/0.769 |
| joins | 0.775/0.677 | 0.773/0.697 | 0.759/0.703 | 0.781/0.741 | 0.792/0.762 | 0.795/0.768 |

## Graded-pairs overlap
- pages from Hillel's 164 graded pairs present in probe corpus: 278 (grades were taken at rehearsal/full scale; full validation belongs to the wave-2 calibration)

## Caveats / wave-2 plan
- DF inflation: decoys add ~25% pages -> DF cap slightly stricter than production. Wave-2: two-run protocol (clean run for tested pairs, decoy run for the null), on liturgy.db then fullcorpus.db behind the compute queue.
- ~2% duplicate candidate rows not deduped (matches candidate_pairs vs candidate_unique_pairs at rehearsal scale).
- Single-chunk residual leak bounded by CHUNK=10 (span is mostly margin -> density >> any threshold).
- Exchangeability argument: decoy candidates traverse the identical seed/DF/two-hit/verify path as tested candidates; the null is conditioned on candidate-generation by construction.

Total runtime: 306s.