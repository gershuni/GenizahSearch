# FRAG-1 -- synthetic-truncation grounding spike (fragment-ID pivot)

Generated 2026-07-09 04:38; total runtime 0.7 min. Reference: ref_corpus.pkl only (5,363 works); ref-side canonical masks: 503 works masked, mtime=2026-07-09 04:13. No full-corpus candidate-engine scan was run (mask_ref_canon.py owned the box's CPU throughout).

**Caveat (binding, per FRAG-ID-PLAN.md):** truncation crops are CLEAN contiguous slices of a page that was ALREADY a confident, single-span, high-coverage Track-1 testimony. This models a clean fragment cut short -- it does NOT model HTR damage, OCR garbling, or heavy textual variance within the fragment itself. Those need lower-k / confusion-weighted matching and are a SEPARATE axis (FRAG-2 extension: crop + inject HTR-confusion noise), not measured here.

## 1+2. recall(length) and mis-attribution(length) by synthetic truncation

Ground truth by construction: each crop is a verbatim slice of a page independently, confidently Track-1-labeled as work W (live row, >=300 matched letters, <=2 spans, best_density<=0.15, matched_letters/page_len>=0.85 -- i.e. the page IS essentially all W). Crop is queried against the reference index using track1_match.py's exact one-sided mechanics (K=5, band=20, min_anchors=2, accept_density 0.28/0.35 by alignment length). recall = crop recovers W among accepted IDs; mis-attribution = crop accepted as some OTHER work / crop accepted as anything (both at the SAME density boundary each candidate would use in production, at scale=1.0 = the current hand-tuned boundary).

Two mis-attribution readings: **any-wrong** = a wrong work appears among the accepted set (relevant to a take-ALL-matches census that mints every accepted work as a witness); **top-wrong** = the single lowest-density (best) accepted work is wrong (relevant to a take-BEST-match census). They diverge at long crops, where the true work is nearly always recovered AND a spurious second work also clears the loose >=100-letter boundary -- so top-wrong is the more faithful precision proxy for a best-match census.

| length | n crops | mean grams | mean ref hits | recall@1.0x | any-wrong@1.0x | top-wrong@1.0x |
|---|---|---|---|---|---|---|
| 40 | 1500 | 36.0 | 698.9 | 0.0% | 0.0% (0/0) | 0.0% (0/0) |
| 60 | 1500 | 56.0 | 1090.6 | 0.8% | 20.0% (3/15) | 20.0% (3/15) |
| 80 | 1500 | 76.0 | 1466.9 | 9.4% | 9.5% (14/148) | 8.1% (12/148) |
| 100 | 1500 | 96.0 | 1852.1 | 27.9% | 8.0% (35/435) | 6.0% (26/435) |
| 150 | 1500 | 146.0 | 2852.3 | 86.9% | 5.3% (69/1307) | 1.1% (14/1307) |
| 200 | 1500 | 196.0 | 3787.7 | 94.9% | 7.5% (108/1432) | 1.4% (20/1432) |
| 300 | 1500 | 296.0 | 5792.3 | 98.1% | 12.2% (180/1476) | 0.7% (10/1476) |

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

| scale x boundary | recall | any-wrong | top-wrong | n any-id | n any-wrong | n top-wrong |
|---|---|---|---|---|---|---|
| 0.5x | 0.0% | 0.0% | 0.0% | 0 | 0 | 0 |
| 0.6x | 0.0% | 0.0% | 0.0% | 0 | 0 | 0 |
| 0.7x | 0.0% | 0.0% | 0.0% | 0 | 0 | 0 |
| 0.8x | 0.0% | 0.0% | 0.0% | 0 | 0 | 0 |
| 0.9x | 0.0% | 0.0% | 0.0% | 0 | 0 | 0 |
| 1.0x | 0.0% | 0.0% | 0.0% | 0 | 0 | 0 |
| 1.1x | 0.0% | 0.0% | 0.0% | 0 | 0 | 0 |
| 1.2x | 0.0% | 0.0% | 0.0% | 0 | 0 | 0 |
| 1.3x | 0.1% | 0.0% | 0.0% | 1 | 0 | 0 |
| 1.5x | 0.7% | 8.3% | 8.3% | 12 | 1 | 1 |

**length 60** (n=1500)

| scale x boundary | recall | any-wrong | top-wrong | n any-id | n any-wrong | n top-wrong |
|---|---|---|---|---|---|---|
| 0.5x | 0.0% | 0.0% | 0.0% | 0 | 0 | 0 |
| 0.6x | 0.0% | 0.0% | 0.0% | 0 | 0 | 0 |
| 0.7x | 0.0% | 0.0% | 0.0% | 0 | 0 | 0 |
| 0.8x | 0.2% | 0.0% | 0.0% | 3 | 0 | 0 |
| 0.9x | 0.5% | 12.5% | 12.5% | 8 | 1 | 1 |
| 1.0x | 0.8% | 20.0% | 20.0% | 15 | 3 | 3 |
| 1.1x | 1.1% | 19.0% | 19.0% | 21 | 4 | 4 |
| 1.2x | 3.3% | 11.1% | 11.1% | 54 | 6 | 6 |
| 1.3x | 14.1% | 10.4% | 8.1% | 222 | 23 | 18 |
| 1.5x | 77.1% | 10.6% | 6.0% | 1176 | 125 | 71 |

**length 80** (n=1500)

| scale x boundary | recall | any-wrong | top-wrong | n any-id | n any-wrong | n top-wrong |
|---|---|---|---|---|---|---|
| 0.5x | 0.0% | 0.0% | 0.0% | 0 | 0 | 0 |
| 0.6x | 0.1% | 0.0% | 0.0% | 1 | 0 | 0 |
| 0.7x | 0.1% | 33.3% | 33.3% | 3 | 1 | 1 |
| 0.8x | 1.3% | 10.0% | 5.0% | 20 | 2 | 1 |
| 0.9x | 3.5% | 8.9% | 7.1% | 56 | 5 | 4 |
| 1.0x | 9.4% | 9.5% | 8.1% | 148 | 14 | 12 |
| 1.1x | 20.2% | 8.9% | 6.3% | 315 | 28 | 20 |
| 1.2x | 44.9% | 7.7% | 4.8% | 689 | 53 | 33 |
| 1.3x | 78.8% | 7.7% | 3.8% | 1192 | 92 | 45 |
| 1.5x | 95.4% | 14.4% | 5.6% | 1446 | 208 | 81 |

**length 100** (n=1500)

| scale x boundary | recall | any-wrong | top-wrong | n any-id | n any-wrong | n top-wrong |
|---|---|---|---|---|---|---|
| 0.5x | 0.3% | 20.0% | 20.0% | 5 | 1 | 1 |
| 0.6x | 0.7% | 9.1% | 9.1% | 11 | 1 | 1 |
| 0.7x | 1.9% | 6.5% | 6.5% | 31 | 2 | 2 |
| 0.8x | 5.5% | 8.9% | 8.9% | 90 | 8 | 8 |
| 0.9x | 12.5% | 8.6% | 7.6% | 198 | 17 | 15 |
| 1.0x | 27.9% | 8.0% | 6.0% | 435 | 35 | 26 |
| 1.1x | 60.1% | 7.3% | 4.6% | 920 | 67 | 42 |
| 1.2x | 83.6% | 8.6% | 3.8% | 1259 | 108 | 48 |
| 1.3x | 93.4% | 12.2% | 4.5% | 1408 | 172 | 63 |
| 1.5x | 97.1% | 21.2% | 5.3% | 1464 | 310 | 77 |

**length 150** (n=1500)

| scale x boundary | recall | any-wrong | top-wrong | n any-id | n any-wrong | n top-wrong |
|---|---|---|---|---|---|---|
| 0.5x | 1.8% | 3.6% | 3.6% | 28 | 1 | 1 |
| 0.6x | 5.0% | 1.3% | 1.3% | 76 | 1 | 1 |
| 0.7x | 13.0% | 2.0% | 2.0% | 198 | 4 | 4 |
| 0.8x | 34.7% | 2.5% | 1.0% | 524 | 13 | 5 |
| 0.9x | 68.5% | 3.0% | 1.0% | 1029 | 31 | 10 |
| 1.0x | 86.9% | 5.3% | 1.1% | 1307 | 69 | 14 |
| 1.1x | 93.0% | 8.8% | 1.5% | 1402 | 124 | 21 |
| 1.2x | 96.3% | 12.3% | 1.6% | 1450 | 179 | 23 |
| 1.3x | 97.2% | 18.4% | 1.8% | 1464 | 270 | 26 |
| 1.5x | 99.3% | 30.9% | 2.1% | 1493 | 462 | 32 |

**length 200** (n=1500)

| scale x boundary | recall | any-wrong | top-wrong | n any-id | n any-wrong | n top-wrong |
|---|---|---|---|---|---|---|
| 0.5x | 4.8% | 2.7% | 1.4% | 73 | 2 | 1 |
| 0.6x | 16.7% | 1.6% | 1.2% | 252 | 4 | 3 |
| 0.7x | 47.0% | 1.0% | 0.6% | 707 | 7 | 4 |
| 0.8x | 76.2% | 1.4% | 0.7% | 1147 | 16 | 8 |
| 0.9x | 89.5% | 3.5% | 0.7% | 1345 | 47 | 10 |
| 1.0x | 94.9% | 7.5% | 1.4% | 1432 | 108 | 20 |
| 1.1x | 97.0% | 11.7% | 1.8% | 1467 | 171 | 26 |
| 1.2x | 98.3% | 17.9% | 1.9% | 1483 | 265 | 28 |
| 1.3x | 98.9% | 25.0% | 1.9% | 1490 | 372 | 28 |
| 1.5x | 99.5% | 38.7% | 1.9% | 1497 | 580 | 29 |

**length 300** (n=1500)

| scale x boundary | recall | any-wrong | top-wrong | n any-id | n any-wrong | n top-wrong |
|---|---|---|---|---|---|---|
| 0.5x | 31.1% | 0.2% | 0.2% | 467 | 1 | 1 |
| 0.6x | 64.5% | 0.6% | 0.1% | 968 | 6 | 1 |
| 0.7x | 83.6% | 1.6% | 0.4% | 1257 | 20 | 5 |
| 0.8x | 92.7% | 3.2% | 0.5% | 1392 | 45 | 7 |
| 0.9x | 96.1% | 6.5% | 0.5% | 1442 | 94 | 7 |
| 1.0x | 98.1% | 12.2% | 0.7% | 1476 | 180 | 10 |
| 1.1x | 98.9% | 18.1% | 0.8% | 1488 | 270 | 12 |
| 1.2x | 99.3% | 24.6% | 0.9% | 1494 | 368 | 13 |
| 1.3x | 99.5% | 32.1% | 0.9% | 1497 | 480 | 14 |
| 1.5x | 99.6% | 47.0% | 1.0% | 1499 | 705 | 15 |

### knee / interpretation

Recall at the current (1.0x) boundary drops below 50% at length **40**. Below that, DF-immune querying alone (at the current boundary) is not reliable; loosening the boundary (scale>1.0) may recover more but check the mis-attribution column at that scale before trusting it as a census-grade identification.
TOP-wrong mis-attribution at 1.0x exceeds 5% at length **60** and below (among crops that get ANY identification) -- below that length, even the single best accepted work is wrong >5% of the time, so an auto-accepted ID is not a safe census-grade testimony on its own; treat as CANDIDATE tier requiring the two-tier (census/review) split (A5), not direct census inclusion.

## 3. failure-mode stage attribution (n=300 real unidentified short pages, shadowed-filtered + canonmask-excluded)

Pages sampled from `pages` with norm_stream length <200, NOT a live track1_matches row, NOT a member of any accepted_pairs_canonmask pair (the true target/orphan population). Each re-run through the SAME query pipeline as the truncation experiment (own text as query against the reference index) and classified by the FIRST pipeline stage that fails.

| stage (first to gate) | count | share |
|---|---|---|
| no_grams | 0 | 0.0% |
| <2_anchors | 0 | 0.0% |
| no_diagonal_cluster | 0 | 0.0% |
| no_reference_covers_it | 247 | 82.3% |
| density_fail | 50 | 16.7% |
| ambiguous | 3 | 1.0% |
| would_now_pass | 0 | 0.0% |

**Dominant genuine failure mode: `no_reference_covers_it` (82.3%).** The headline is that these short orphan pages are NOT sitting just below the acceptance boundary -- they overwhelmingly fail because NO reference span aligns to them at even a generous 0.55 edit-density (they form 3-9-anchor diagonal clusters purely from chance 5-gram collisions -- unavoidable in Hebrew -- but nothing verifies). That means the binding lever for this population is **REF-1 reference expansion** (or genuine non-identifiability), NOT threshold loosening. A looser boundary (A5) only helps the `density_fail` slice, which is small (16.7%).

Best-achievable edit-density among the pages that DID verify a candidate at the 0.55 wide cutoff (density_fail + would_now_pass) -- shows how far the verifiers are from an accept: {'0.30-0.40': 8, '0.40-0.50': 23, '>0.50': 22}. (Pages in `no_reference_covers_it` verified NOTHING at 0.55, so they have no best-density -- they are not near any accept at all.)

`would_now_pass` is NOT a failure -- it means our re-run (using the CURRENT reference/mask config) finds an acceptable candidate the live `track1_matches` table does not carry. This can happen because `track1_matches` in fullcorpus.db predates the ref-side canonical masking added 2026-07-08 (its report has no 'canonical' mention and predates ref_canon_masks.json's mtime) -- i.e. partial census staleness, not a pipeline defect. It is excluded from the failure interpretation below but counted for transparency.

**Heuristic caveats (stated explicitly):** `no_reference_covers_it` here means a diagonal cluster formed but NO reference span verified at the generous 0.55 edit-density cutoff -- a strong signal the page is not a copy of any reference text, but it cannot fully distinguish 'genuinely no reference exists' from 'the reference exists but this witness is so textually variant / HTR-garbled that even 0.55 fails'. Those two need different levers (REF-1 vs lower-k/confusion-weighted matching) and disentangling them requires the human grades requested below. `density_fail` is the cleaner bucket: a real alignment verified at 0.55 but missed the production boundary. `ambiguous` requires 2+ distinct works both under a generous plausibility cutoff (0.45) within 0.05 density of each other -- a documented design choice, not a fixed constant from prior work.

### example cards

**no_reference_covers_it** (247 total, showing up to 6):
- page `990053691230205171_IE168603151_P000001_FL168603153` (sys 990053691230205171, len 89): grams=85 hits=1391 best_cluster=3 n_candidates=0
- page `990052080400205171_IE167661313_P000002_FL167661316` (sys 990052080400205171, len 92): grams=88 hits=1791 best_cluster=6 n_candidates=0
- page `990001461650205171_IE49388101_P000004_FL49388121` (sys 990001461650205171, len 97): grams=93 hits=861 best_cluster=4 n_candidates=0
- page `990052162920205171_IE166763681_P000001_FL166763683` (sys 990052162920205171, len 91): grams=87 hits=2202 best_cluster=4 n_candidates=0
- page `990051819920205171_IE167275547_P000001_FL167275549` (sys 990051819920205171, len 88): grams=84 hits=2013 best_cluster=6 n_candidates=0
- page `990051135290205171_IE164033182_P000003_FL164033186` (sys 990051135290205171, len 93): grams=89 hits=1299 best_cluster=9 n_candidates=0

**density_fail** (50 total, showing up to 6):
- page `990051852510205171_IE165632647_P000002_FL165632650` (sys 990051852510205171, len 84): grams=80 hits=1308 best_cluster=8 n_candidates=4 best_dens=0.308
- page `990051121050205171_IE166122697_P000002_FL166122700` (sys 990051121050205171, len 96): grams=92 hits=1193 best_cluster=4 n_candidates=7 best_dens=0.414
- page `990051851030205171_IE165630479_P000002_FL165630482` (sys 990051851030205171, len 87): grams=83 hits=2009 best_cluster=12 n_candidates=3 best_dens=0.402
- page `990051560670205171_IE162749765_P000002_FL162749768` (sys 990051560670205171, len 84): grams=80 hits=1500 best_cluster=11 n_candidates=2 best_dens=0.529
- page `990053220940205171_IE158655747_P000002_FL158655753` (sys 990053220940205171, len 93): grams=89 hits=2381 best_cluster=11 n_candidates=1 best_dens=0.514
- page `990051734920205171_IE164947898_P000002_FL164947901` (sys 990051734920205171, len 99): grams=95 hits=1564 best_cluster=14 n_candidates=4 best_dens=0.476

**ambiguous** (3 total, showing up to 6):
- page `990051496810205171_IE161348586_P000001_FL161348589` (sys 990051496810205171, len 98): grams=94 hits=1429 best_cluster=10 n_candidates=17 best_dens=0.405
- page `990051577290205171_IE162837881_P000002_FL162837884` (sys 990051577290205171, len 142): grams=138 hits=3226 best_cluster=4 n_candidates=16 best_dens=0.343
- page `990051659400205171_IE164271285_P000001_FL164271287` (sys 990051659400205171, len 187): grams=183 hits=2963 best_cluster=16 n_candidates=2 best_dens=0.353

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

- **40 letters**: recall 0.0%, any-wrong 0.0%, top-wrong 0.0% -> mostly unrecoverable at 1.0x
- **60 letters**: recall 0.8%, any-wrong 20.0%, top-wrong 20.0% -> candidate-tier only (review, not census)
- **80 letters**: recall 9.4%, any-wrong 9.5%, top-wrong 8.1% -> candidate-tier only (review, not census)
- **100 letters**: recall 27.9%, any-wrong 8.0%, top-wrong 6.0% -> candidate-tier only (review, not census)
- **150 letters**: recall 86.9%, any-wrong 5.3%, top-wrong 1.1% -> census-safe at 1.0x
- **200 letters**: recall 94.9%, any-wrong 7.5%, top-wrong 1.4% -> census-safe at 1.0x
- **300 letters**: recall 98.1%, any-wrong 12.2%, top-wrong 0.7% -> census-safe at 1.0x

**Where DF-immune querying suffices vs where A5's two-tier (census/candidate) gate is forced.** The two metrics separate the regimes cleanly under a take-BEST-match policy (accept only the single lowest-density work per page):
- **>=150 letters -- census-safe, no gate needed:** recall 87-98% AND top-wrong 0.7-1.4%. DF-immune track1_match-style querying at the current boundary is directly census-grade here. (Note the any-wrong rate climbs to 12% at 300 letters -- that is entirely spurious SECOND works clearing the loose >=100-letter boundary; a take-all-matches census would need the A5 gate to suppress them, but a take-best census does not.)
- **~100 letters -- candidate tier, but the gate is RECALL not precision:** top-wrong is only 6% (the IDs it produces are ~94% correct), but recall is 28% -- most 100-letter crops produce NO identification at all. So a 100-letter accepted ID is fairly trustworthy, but coverage is thin; route to candidate/review mainly because so few clear, and loosening the boundary (scale 1.1-1.2x lifts recall to 60-84%) trades in rising top-wrong -- the A5 length-conditional operating point lives exactly here.
- **<=80 letters -- recall-floored:** recall <=9% at the current boundary; the handful that do clear are unreliable (top-wrong 8-20%). Neither census nor a useful candidate stream; needs external signal or human review.

**Unrecoverable floor:** at the shortest tested length (40 letters), recall@1.0x = 0.0%. Below this, lexical matching against ANY reference is structurally limited by the k=5 gram model itself (a 40-letter crop yields only ~36 grams; a handful of transcription variants can eliminate all shared anchors) -- this is a genuine floor, not a tuning artifact, and fragments below it need either a different signal (paleography, codicological join, external catalog metadata) or human review, not a better acceptance threshold.

**Which lever fixes which failure-mode regime** (from section 3; percentages below are shares of the 300 GENUINE failures -- 0 `would_now_pass` cases excluded from this denominator, since they are not failures):
- `no_reference_covers_it` (82.3% of genuine failures) -> REF-1 reference expansion is the ONLY lever; no amount of threshold tuning helps a work that isn't in ref_corpus.pkl.
- `<2_anchors` / `no_diagonal_cluster` (0.0%) -> genuinely thin signal on short-but-real text; length-conditional thresholds cannot manufacture anchors that don't exist -- these need either a smaller k (more anchors per letter, more false positives) or acceptance of a recall floor.
- `density_fail` (16.7%) -> a real candidate exists but is too noisy/short to clear the bar; this is exactly what a length-conditional, less conservative boundary (A5 conformal/FDR) targets -- expect this bucket to shrink most from that work.
- `ambiguous` (1.0%) -> candidate tier or unrecoverable; by construction these need human disambiguation (shared formulaic language across 2+ works) and should never be auto-accepted regardless of threshold.

**Targeted short-bin human-grading request for Hillel:** the census shows 16,420 pages <100 letters and 71,176 pages <200 letters with NEITHER a Track-1 ID nor a Track-2 canonmask pair -- the target population. Grades on ~5 no_reference_covers_it, ~5 density_fail, and ~5 ambiguous example cards above (already drawn from the real unidentified pool, evidence cards ready) would validate the failure-mode heuristics before FRAG-2 commits compute to the full-scale run; and grading ~20 crop-recovered identifications at length ~60-100 (the recall knee zone) would be the real-data precision check the synthetic curve cannot fully replace (per the truncation caveat above -- clean crops, not HTR-damaged fragments).
