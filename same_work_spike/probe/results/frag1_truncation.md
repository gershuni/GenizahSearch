# FRAG-1 -- synthetic-truncation grounding spike (fragment-ID pivot)

Generated 2026-07-09 04:27; total runtime 5.2 min. Reference: ref_corpus.pkl only (5,363 works); ref-side canonical masks: 503 works masked, mtime=2026-07-09 04:13. No full-corpus candidate-engine scan was run (mask_ref_canon.py owned the box's CPU throughout).

**Caveat (binding, per FRAG-ID-PLAN.md):** truncation crops are CLEAN contiguous slices of a page that was ALREADY a confident, single-span, high-coverage Track-1 testimony. This models a clean fragment cut short -- it does NOT model HTR damage, OCR garbling, or heavy textual variance within the fragment itself. Those need lower-k / confusion-weighted matching and are a SEPARATE axis (FRAG-2 extension: crop + inject HTR-confusion noise), not measured here.

## 1+2. recall(length) and mis-attribution(length) by synthetic truncation

Ground truth by construction: each crop is a verbatim slice of a page independently, confidently Track-1-labeled as work W (live row, >=300 matched letters, <=2 spans, best_density<=0.15, matched_letters/page_len>=0.85 -- i.e. the page IS essentially all W). Crop is queried against the reference index using track1_match.py's exact one-sided mechanics (K=5, band=20, min_anchors=2, accept_density 0.28/0.35 by alignment length). recall = crop recovers W among accepted IDs; mis-attribution = crop accepted as some OTHER work / crop accepted as anything (both at the SAME density boundary each candidate would use in production, at scale=1.0 = the current hand-tuned boundary).

| length | n crops | mean grams | mean ref hits | recall@1.0x | mis-attrib@1.0x |
|---|---|---|---|---|---|
| 40 | 1500 | 36.0 | 698.9 | 0.0% | 0.0% (0/0) |
| 60 | 1500 | 56.0 | 1090.6 | 0.8% | 20.0% (3/15) |
| 80 | 1500 | 76.0 | 1466.9 | 9.4% | 9.5% (14/148) |
| 100 | 1500 | 96.0 | 1852.1 | 27.9% | 8.0% (35/435) |
| 150 | 1500 | 146.0 | 2852.3 | 86.9% | 5.3% (69/1307) |
| 200 | 1500 | 196.0 | 3787.7 | 94.9% | 7.5% (108/1432) |
| 300 | 1500 | 296.0 | 5792.3 | 98.1% | 12.2% (180/1476) |

### recall/mis-attribution by category (default boundary, incl. JA)

| length | Bavli | Bible | JA | Maagarim | Mishnah | Tosefta | Yerushalmi |
|---|---|---|---|---|---|---|---|
| 40 | 0% (n=120) | 0% (n=600) | 0% (n=240) | 0% (n=450) | 0% (n=60) | 0% (n=6) | 0% (n=24) |
| 60 | 1% (n=120) | 1% (n=600) | 1% (n=240) | 0% (n=450) | 3% (n=60) | 17% (n=6) | 0% (n=24) |
| 80 | 11% (n=120) | 14% (n=600) | 3% (n=240) | 6% (n=450) | 8% (n=60) | 33% (n=6) | 17% (n=24) |
| 100 | 29% (n=120) | 33% (n=600) | 18% (n=240) | 25% (n=450) | 33% (n=60) | 50% (n=6) | 33% (n=24) |
| 150 | 76% (n=120) | 91% (n=600) | 81% (n=240) | 88% (n=450) | 85% (n=60) | 100% (n=6) | 83% (n=24) |
| 200 | 96% (n=120) | 95% (n=600) | 92% (n=240) | 97% (n=450) | 90% (n=60) | 83% (n=6) | 88% (n=24) |
| 300 | 95% (n=120) | 99% (n=600) | 98% (n=240) | 99% (n=450) | 95% (n=60) | 100% (n=6) | 96% (n=24) |

### density-boundary sweep (recall/mis-attribution tradeoff per length)

**length 40** (n=1500)

| scale x boundary | recall | mis-attribution | n any-id | n wrong-id |
|---|---|---|---|---|
| 0.5x | 0.0% | 0.0% | 0 | 0 |
| 0.6x | 0.0% | 0.0% | 0 | 0 |
| 0.7x | 0.0% | 0.0% | 0 | 0 |
| 0.8x | 0.0% | 0.0% | 0 | 0 |
| 0.9x | 0.0% | 0.0% | 0 | 0 |
| 1.0x | 0.0% | 0.0% | 0 | 0 |
| 1.1x | 0.0% | 0.0% | 0 | 0 |
| 1.2x | 0.0% | 0.0% | 0 | 0 |
| 1.3x | 0.1% | 0.0% | 1 | 0 |
| 1.5x | 0.7% | 8.3% | 12 | 1 |

**length 60** (n=1500)

| scale x boundary | recall | mis-attribution | n any-id | n wrong-id |
|---|---|---|---|---|
| 0.5x | 0.0% | 0.0% | 0 | 0 |
| 0.6x | 0.0% | 0.0% | 0 | 0 |
| 0.7x | 0.0% | 0.0% | 0 | 0 |
| 0.8x | 0.2% | 0.0% | 3 | 0 |
| 0.9x | 0.5% | 12.5% | 8 | 1 |
| 1.0x | 0.8% | 20.0% | 15 | 3 |
| 1.1x | 1.1% | 19.0% | 21 | 4 |
| 1.2x | 3.3% | 11.1% | 54 | 6 |
| 1.3x | 14.1% | 10.4% | 222 | 23 |
| 1.5x | 77.1% | 10.6% | 1176 | 125 |

**length 80** (n=1500)

| scale x boundary | recall | mis-attribution | n any-id | n wrong-id |
|---|---|---|---|---|
| 0.5x | 0.0% | 0.0% | 0 | 0 |
| 0.6x | 0.1% | 0.0% | 1 | 0 |
| 0.7x | 0.1% | 33.3% | 3 | 1 |
| 0.8x | 1.3% | 10.0% | 20 | 2 |
| 0.9x | 3.5% | 8.9% | 56 | 5 |
| 1.0x | 9.4% | 9.5% | 148 | 14 |
| 1.1x | 20.2% | 8.9% | 315 | 28 |
| 1.2x | 44.9% | 7.7% | 689 | 53 |
| 1.3x | 78.8% | 7.7% | 1192 | 92 |
| 1.5x | 95.4% | 14.4% | 1446 | 208 |

**length 100** (n=1500)

| scale x boundary | recall | mis-attribution | n any-id | n wrong-id |
|---|---|---|---|---|
| 0.5x | 0.3% | 20.0% | 5 | 1 |
| 0.6x | 0.7% | 9.1% | 11 | 1 |
| 0.7x | 1.9% | 6.5% | 31 | 2 |
| 0.8x | 5.5% | 8.9% | 90 | 8 |
| 0.9x | 12.5% | 8.6% | 198 | 17 |
| 1.0x | 27.9% | 8.0% | 435 | 35 |
| 1.1x | 60.1% | 7.3% | 920 | 67 |
| 1.2x | 83.6% | 8.6% | 1259 | 108 |
| 1.3x | 93.4% | 12.2% | 1408 | 172 |
| 1.5x | 97.1% | 21.2% | 1464 | 310 |

**length 150** (n=1500)

| scale x boundary | recall | mis-attribution | n any-id | n wrong-id |
|---|---|---|---|---|
| 0.5x | 1.8% | 3.6% | 28 | 1 |
| 0.6x | 5.0% | 1.3% | 76 | 1 |
| 0.7x | 13.0% | 2.0% | 198 | 4 |
| 0.8x | 34.7% | 2.5% | 524 | 13 |
| 0.9x | 68.5% | 3.0% | 1029 | 31 |
| 1.0x | 86.9% | 5.3% | 1307 | 69 |
| 1.1x | 93.0% | 8.8% | 1402 | 124 |
| 1.2x | 96.3% | 12.3% | 1450 | 179 |
| 1.3x | 97.2% | 18.4% | 1464 | 270 |
| 1.5x | 99.3% | 30.9% | 1493 | 462 |

**length 200** (n=1500)

| scale x boundary | recall | mis-attribution | n any-id | n wrong-id |
|---|---|---|---|---|
| 0.5x | 4.8% | 2.7% | 73 | 2 |
| 0.6x | 16.7% | 1.6% | 252 | 4 |
| 0.7x | 47.0% | 1.0% | 707 | 7 |
| 0.8x | 76.2% | 1.4% | 1147 | 16 |
| 0.9x | 89.5% | 3.5% | 1345 | 47 |
| 1.0x | 94.9% | 7.5% | 1432 | 108 |
| 1.1x | 97.0% | 11.7% | 1467 | 171 |
| 1.2x | 98.3% | 17.9% | 1483 | 265 |
| 1.3x | 98.9% | 25.0% | 1490 | 372 |
| 1.5x | 99.5% | 38.7% | 1497 | 580 |

**length 300** (n=1500)

| scale x boundary | recall | mis-attribution | n any-id | n wrong-id |
|---|---|---|---|---|
| 0.5x | 31.1% | 0.2% | 467 | 1 |
| 0.6x | 64.5% | 0.6% | 968 | 6 |
| 0.7x | 83.6% | 1.6% | 1257 | 20 |
| 0.8x | 92.7% | 3.2% | 1392 | 45 |
| 0.9x | 96.1% | 6.5% | 1442 | 94 |
| 1.0x | 98.1% | 12.2% | 1476 | 180 |
| 1.1x | 98.9% | 18.1% | 1488 | 270 |
| 1.2x | 99.3% | 24.6% | 1494 | 368 |
| 1.3x | 99.5% | 32.1% | 1497 | 480 |
| 1.5x | 99.6% | 47.0% | 1499 | 705 |

### knee / interpretation

Recall at the current (1.0x) boundary drops below 50% at length **40**. Below that, DF-immune querying alone (at the current boundary) is not reliable; loosening the boundary (scale>1.0) may recover more but check the mis-attribution column at that scale before trusting it as a census-grade identification.
Mis-attribution at 1.0x exceeds 10% at length **60** and below (among crops that get ANY identification) -- below that length, an accepted ID is not a safe census-grade testimony on its own; treat as CANDIDATE tier requiring the two-tier (census/review) split (A5), not direct census inclusion.

## 3. failure-mode stage attribution (n=300 real unidentified short pages, shadowed-filtered + canonmask-excluded)

Pages sampled from `pages` with norm_stream length <200, NOT a live track1_matches row, NOT a member of any accepted_pairs_canonmask pair (the true target/orphan population). Each re-run through the SAME query pipeline as the truncation experiment (own text as query against the reference index) and classified by the FIRST pipeline stage that fails.

| stage | count | share |
|---|---|---|
| no_grams | 0 | 0.0% |
| no_reference_covers_it | 0 | 0.0% |
| <2_anchors | 0 | 0.0% |
| no_diagonal_cluster | 0 | 0.0% |
| density_fail | 297 | 99.0% |
| ambiguous | 3 | 1.0% |
| would_now_pass | 0 | 0.0% |

`would_now_pass` is NOT a failure -- it means our re-run (using the CURRENT reference/mask config) finds an acceptable candidate the live `track1_matches` table does not carry. This can happen because `track1_matches` in fullcorpus.db predates the ref-side canonical masking added 2026-07-08 (its report has no 'canonical' mention and predates ref_canon_masks.json's mtime) -- i.e. partial census staleness, not a pipeline defect. It is excluded from the failure interpretation below but counted for transparency.

**Heuristic caveats (stated explicitly):** `no_reference_covers_it` is inferred from ZERO raw k-gram hits anywhere in the reference index despite having grams to try -- a strong but not certain signal (severe HTR garbling of a work that IS referenced could also produce zero hits, though unlikely to zero out every 5-gram). `ambiguous` requires 2+ distinct works both under a generous plausibility cutoff (0.45) within 0.05 density of each other -- a documented design choice, not a fixed constant from prior work.

### example cards

**density_fail** (297 total, showing up to 6):
- page `990053691230205171_IE168603151_P000001_FL168603153` (sys 990053691230205171, len 89): grams=85 hits=1391 best_cluster=3 n_candidates=0
- page `990052080400205171_IE167661313_P000002_FL167661316` (sys 990052080400205171, len 92): grams=88 hits=1791 best_cluster=6 n_candidates=0
- page `990001461650205171_IE49388101_P000004_FL49388121` (sys 990001461650205171, len 97): grams=93 hits=861 best_cluster=4 n_candidates=0
- page `990052162920205171_IE166763681_P000001_FL166763683` (sys 990052162920205171, len 91): grams=87 hits=2202 best_cluster=4 n_candidates=0
- page `990051819920205171_IE167275547_P000001_FL167275549` (sys 990051819920205171, len 88): grams=84 hits=2013 best_cluster=6 n_candidates=0
- page `990051135290205171_IE164033182_P000003_FL164033186` (sys 990051135290205171, len 93): grams=89 hits=1299 best_cluster=9 n_candidates=0

**ambiguous** (3 total, showing up to 6):
- page `990051496810205171_IE161348586_P000001_FL161348589` (sys 990051496810205171, len 98): grams=94 hits=1429 best_cluster=10 n_candidates=17
- page `990051577290205171_IE162837881_P000002_FL162837884` (sys 990051577290205171, len 142): grams=138 hits=3226 best_cluster=4 n_candidates=16
- page `990051659400205171_IE164271285_P000001_FL164271287` (sys 990051659400205171, len 187): grams=183 hits=2963 best_cluster=16 n_candidates=2

## 4. fragment-population census (fullcorpus.db, counts only)

### <100 letters (total 18,605 pages)

- track1_identified (live): 102
- canonmask_paired (Track-2, no work ID): 2,083
- **neither_target (target population)**: 16,420

By FJMS domain group:

| domain | track1_identified | canonmask_paired | neither_target | total |
|---|---|---|---|---|
| Other / Unidentified | 18 | 479 | 3,449 | 3,946 |
| Bible | 54 | 278 | 3,005 | 3,337 |
| Piyyut | 11 | 239 | 2,842 | 3,092 |
| Documents & Letters | 2 | 80 | 1,413 | 1,495 |
| Liturgy | 10 | 197 | 1,262 | 1,469 |
| Halakha | 5 | 174 | 1,215 | 1,394 |
| Exegesis & Tafsir | 0 | 160 | 804 | 964 |
| Talmud & Midrash | 0 | 28 | 821 | 849 |
| Thought & Kabbalah | 2 | 129 | 486 | 617 |
| Sciences & Medicine | 0 | 67 | 452 | 519 |
| Poetry | 0 | 137 | 322 | 459 |
| Philology | 0 | 62 | 235 | 297 |
| Belles Lettres | 0 | 53 | 114 | 167 |

### <200 letters (total 86,164 pages)

- track1_identified (live): 4,547
- canonmask_paired (Track-2, no work ID): 10,441
- **neither_target (target population)**: 71,176

By FJMS domain group:

| domain | track1_identified | canonmask_paired | neither_target | total |
|---|---|---|---|---|
| Piyyut | 816 | 2,467 | 14,281 | 17,564 |
| Other / Unidentified | 569 | 2,418 | 13,309 | 16,296 |
| Bible | 2,199 | 1,625 | 12,307 | 16,131 |
| Liturgy | 643 | 900 | 5,783 | 7,326 |
| Documents & Letters | 31 | 312 | 6,172 | 6,515 |
| Halakha | 125 | 665 | 5,392 | 6,182 |
| Talmud & Midrash | 85 | 189 | 3,902 | 4,176 |
| Exegesis & Tafsir | 18 | 497 | 2,926 | 3,441 |
| Thought & Kabbalah | 18 | 317 | 2,061 | 2,396 |
| Poetry | 34 | 503 | 1,720 | 2,257 |
| Sciences & Medicine | 5 | 185 | 1,990 | 2,180 |
| Philology | 1 | 214 | 986 | 1,201 |
| Belles Lettres | 3 | 149 | 347 | 499 |

## 5. pipeline design (grounded in 1-4)

**Per-length operating thresholds** (from the sweep above):

- **40 letters**: recall 0.0%, mis-attribution 0.0% -> mostly unrecoverable at 1.0x
- **60 letters**: recall 0.8%, mis-attribution 20.0% -> candidate-tier only (review, not census)
- **80 letters**: recall 9.4%, mis-attribution 9.5% -> candidate-tier only (review, not census)
- **100 letters**: recall 27.9%, mis-attribution 8.0% -> candidate-tier only (review, not census)
- **150 letters**: recall 86.9%, mis-attribution 5.3% -> census-safe at 1.0x
- **200 letters**: recall 94.9%, mis-attribution 7.5% -> census-safe at 1.0x
- **300 letters**: recall 98.1%, mis-attribution 12.2% -> candidate-tier only (review, not census)

**Where DF-immune querying suffices vs where A5's two-tier (census/candidate) gate is forced:** the mis-attribution knee (see section 1+2) marks the length below which an accepted identification is NOT safe to mint directly into the census -- those matches should route to the CANDIDATE/review tier (human-graded or conformal-FDR-bounded per A5) rather than being auto-accepted. Above the recall knee and below the mis-attribution knee, straightforward DF-immune track1_match-style querying at the current boundary is sufficient.

**Unrecoverable floor:** at the shortest tested length (40 letters), recall@1.0x = 0.0%. Below this, lexical matching against ANY reference is structurally limited by the k=5 gram model itself (a 40-letter crop yields only ~36 grams; a handful of transcription variants can eliminate all shared anchors) -- this is a genuine floor, not a tuning artifact, and fragments below it need either a different signal (paleography, codicological join, external catalog metadata) or human review, not a better acceptance threshold.

**Which lever fixes which failure-mode regime** (from section 3; percentages below are shares of the 300 GENUINE failures -- 0 `would_now_pass` cases excluded from this denominator, since they are not failures):
- `no_reference_covers_it` (0.0% of genuine failures) -> REF-1 reference expansion is the ONLY lever; no amount of threshold tuning helps a work that isn't in ref_corpus.pkl.
- `<2_anchors` / `no_diagonal_cluster` (0.0%) -> genuinely thin signal on short-but-real text; length-conditional thresholds cannot manufacture anchors that don't exist -- these need either a smaller k (more anchors per letter, more false positives) or acceptance of a recall floor.
- `density_fail` (99.0%) -> a real candidate exists but is too noisy/short to clear the bar; this is exactly what a length-conditional, less conservative boundary (A5 conformal/FDR) targets -- expect this bucket to shrink most from that work.
- `ambiguous` (1.0%) -> candidate tier or unrecoverable; by construction these need human disambiguation (shared formulaic language across 2+ works) and should never be auto-accepted regardless of threshold.

**Targeted short-bin human-grading request for Hillel:** the census shows 16,420 pages <100 letters and 71,176 pages <200 letters with NEITHER a Track-1 ID nor a Track-2 canonmask pair -- the target population. Grades on ~5 no_reference_covers_it, ~5 density_fail, and ~5 ambiguous example cards above (already drawn from the real unidentified pool, evidence cards ready) would validate the failure-mode heuristics before FRAG-2 commits compute to the full-scale run; and grading ~20 crop-recovered identifications at length ~60-100 (the recall knee zone) would be the real-data precision check the synthetic curve cannot fully replace (per the truncation caveat above -- clean crops, not HTR-damaged fragments).
