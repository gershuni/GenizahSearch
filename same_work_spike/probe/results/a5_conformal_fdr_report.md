# A5 — conformal + FDR thresholds via target-decoy null (probe.db dry run)

Corpus: 17,228 real pages + 4,307 chunk-shuffle decoys (CHUNK=10, seed=29); production candidate mode (k=5, DF<=100, two-hit, band=20).
Candidates: 16,832,350 — tested (real-real) 11,295,695, null (decoy-involved) 5,536,655 (decoy candidate volume = 32.9% of all).

## Null density distribution (per length bin)
| bin | n null | median | q05 | q01 |
|---|---|---|---|---|
| [25,50) | 5,427 | 0.778 | 0.676 | 0.629 |
| [50,100) | 4,251,279 | 0.776 | 0.712 | 0.682 |
| [100,200) | 298,680 | 0.760 | 0.706 | 0.675 |
| [200,400) | 389,200 | 0.780 | 0.739 | 0.716 |
| [400,800) | 366,760 | 0.792 | 0.762 | 0.745 |
| 800+ | 225,309 | 0.800 | 0.779 | 0.764 |

## FDR-bounded operating table (max accepted density per bin)
| bin | hand boundary | q=0.01 | q=0.05 |
|---|---|---|---|
| [25,50) | 0.300 | — | 0.545 |
| [50,100) | 0.300 | 0.596 | 0.635 |
| [100,200) | 0.386 | 0.576 | 0.621 |
| [200,400) | 0.418 | 0.648 | 0.677 |
| [400,800) | 0.418 | 0.690 | 0.711 |
| 800+ | 0.418 | 0.714 | 0.738 |

## Acceptance volume + incumbent-boundary FDR estimate
- tested accepted by HAND boundary: 10,951; decoy-side accepted: 0; population ratio 2.04 -> point estimate FDR = 0.00%. With 0 of 5,536,655 null acceptances, the one-sided 95% bound is <= 0.056% of hand-accepted pairs (rule-of-three x ratio). This bounds CHANCE-alignment errors only — it is a different (narrower) error class than human semantic grading.
- BH q=0.01: accepted 96,548 tested pairs (p-cut 8.54e-05)
- BH q=0.05: accepted 160,324 tested pairs (p-cut 7.07e-04)

## Tier-1 ground-truth recall (pairs present in probe corpus)
| family | GT pairs | as candidates | hand | q=0.01 | q=0.05 |
|---|---|---|---|---|---|
| joins | 36 | 36 | 35 | 36 | 36 |
| titles | 1866 | 1866 | 1829 | 1866 | 1866 |
| bh | 118 | 118 | 114 | 118 | 118 |

## Per-stratum null sensitivity (same-bucket null pairs; median/q05)
| bucket | [25,50) | [50,100) | [100,200) | [200,400) | [400,800) | 800+ |
|---|---|---|---|---|---|---|
| background | 0.783/0.688 | 0.776/0.713 | 0.761/0.708 | 0.781/0.741 | 0.793/0.764 | 0.800/0.778 |
| titles | 0.767/0.674 | 0.773/0.711 | 0.754/0.698 | 0.774/0.728 | 0.788/0.751 | 0.800/0.774 |
| bh | 0.773/0.647 | 0.742/0.667 | 0.720/0.653 | 0.750/0.693 | 0.777/0.735 | 0.787/0.776 |
| joins | — | 0.776/0.716 | 0.760/0.715 | 0.781/0.745 | 0.792/0.767 | 0.797/0.778 |

## Chunk-size sensitivity (Codex review HIGH-1) — orchestrator synthesis

The decoy design has two opposing biases: small chunks (CHUNK=10) destroy every
k-gram crossing a chunk boundary (decoys less phrase-like -> null too HIGH ->
thresholds too loose); large chunks (CHUNK=25) leave intact 25-letter runs of
REAL text in the decoys (genuine short matches counted as null -> null lower
tail too LOW -> thresholds too tight). The rerun at CHUNK=25
(`a5_conformal_fdr_report_c25.md`) shows the effect is material at q=0.01:

| bin | hand | q=0.01 (c10, optimistic) | q=0.01 (c25, conservative) |
|---|---|---|---|
| [50,100) | 0.300 | 0.596 | 0.374 |
| [100,200) | 0.386 | 0.576 | 0.396 |
| [200,400) | 0.418 | 0.648 | 0.469 |
| [400,800) | 0.418 | 0.690 | 0.572 |
| 800+ | 0.418 | 0.714 | 0.662 |

**Adopt the conservative envelope (c25).** Even under it, tier-1 recall at
q=0.01 is 36/36 joins, 1,861/1,866 titles, 117/118 BH (hand: 35/1,832/114) —
i.e. a modest, principled relaxation (~0.30-0.42 -> ~0.37-0.66 by length)
captures essentially all known ground truth at bounded chance-FDR. Notably the
c25 thresholds land almost exactly where the liturgy-connectivity evidence
pointed during the probe (density ~0.45): the two independent lines agree.
Wave-2 refinement: a leak-free decoy (e.g. chunk-shuffle with chunk-boundary
gram EXCLUSION on the real side too, or cross-page chimeras with provenance
tracking) can narrow the c10/c25 bracket.

## Graded-pairs overlap
- pages from Hillel's 164 graded pairs present in probe corpus: 278 (grades were taken at rehearsal/full scale; full validation belongs to the wave-2 calibration)

## Caveats / wave-2 plan
- DF inflation: decoys add ~25% pages -> DF cap slightly stricter than production. Wave-2: two-run protocol (clean run for tested pairs, decoy run for the null), on liturgy.db then fullcorpus.db behind the compute queue.
- ~2% duplicate candidate rows not deduped (matches candidate_pairs vs candidate_unique_pairs at rehearsal scale).
- Single-chunk residual leak bounded by CHUNK=10 (span is mostly margin -> density >> any threshold).
- Exchangeability argument: decoy candidates traverse the identical seed/DF/two-hit/verify path as tested candidates; the null is conditioned on candidate-generation by construction.

Total runtime: 309s.