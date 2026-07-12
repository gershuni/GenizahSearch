# Track-2 WIDE tier report (full)

Generated 2026-07-11 01:34; source `C:\Genizahsearch\same_work_spike\probe\data\fullcorpus_v2.db`; git `v8.4.0-113-ga8d0e0ae-dirty`.

## Cohort sizes
- real pages: 667,411
- decoys: 24,000 {'D25': 12000, 'D12': 12000}
- leak-clean pool: 198,873 (468,538 excluded)

## Volume funnel
- candidate_pairs: 56,544,496
- rej_short: 0
- decoy_decoy: 138,057
- decoy_real: 186
- strict_pairs_seen: 1,335,473
- strict_purged_raw: 0
- wide_spilled_raw: 598,026
- wide_deduped: 598,026
- pruned low-value: 45623
- survivors (post score+prune): 552403

## Strict-pairs vs chain step-6
- strict-class real-real PAIRS seen this run: 1,335,473 (pair-level; a pair with any strict segment is excluded from the sidecar entirely)
- chain step-6 `accepted_pairs_canonmask` rows: 1,361,749
- delta: -1.93% (small DF perturbation from injected decoys is expected)

## Normalization derivation
Opportunity normalization -- expected null real-real per bucket =
`decoy_real_count * n_real / (2 * n_dec_cohort)`, conservative
envelope = MAX over the two chunk cohorts. Factors this run:

- D25: factor = 667,411 / (2 * 12,000) = 27.81
- D12: factor = 667,411 / (2 * 12,000) = 27.81

The `/2` is the C(n_real,2) vs n_dec*n_real opportunity asymmetry:
real-real pairs are unordered (both real sides), decoy-real pairs
are already distinct unordered pairs (only the decoy side spent).
See the module docstring for the full step-by-step.

## Null estimation policy
- bucket-level local FDR used AS-IS only when the envelope's supporting decoy count >= 25; below that: max(bucket, same-stratum marginal, global), each with a `+1` upper bound on the decoy count (no bucket gets fdr=0 / p_local=1.0 from absence of evidence), THEN the monotone majorant (non-decreasing in dens, non-increasing in alen).
- decoy-real null counting is SEGMENT-level while the real side is pair-best + strict-pair-excluded: segments-per-pair multiplicity >= 1 only INFLATES the null -> conservative.
- q_value = global BH over per-row empirical null-tail p-values, p = (1 + null mass with alen'>=alen, dens'<=dens in the row's stratum) / (1 + stratum null mass).

## Null buckets (envelope; top by observed_real)
| alen_bin | dens_bin | stratum | support | expected_null | observed_real | local_fdr | p_local |
|---|---|---|---|---|---|---|---|
| [50,70) | [0.40,0.45) | 3 | 5 | 139.04 | 61,091 | 0.0085 | 0.9915 |
| [50,70) | [0.35,0.40) | 3 | 5 | 139.04 | 40,473 | 0.0085 | 0.9915 |
| [50,70) | [0.40,0.45) | 2 | 2 | 55.62 | 36,871 | 0.0487 | 0.9513 |
| [70,100) | [0.40,0.45) | 3 | 1 | 27.81 | 34,107 | 0.0085 | 0.9915 |
| [50,70) | [0.30,0.35) | 3 | 0 | 0.00 | 32,498 | 0.0084 | 0.9916 |
| [50,70) | [0.35,0.40) | 2 | 4 | 111.24 | 27,540 | 0.0115 | 0.9885 |
| [70,100) | [0.35,0.40) | 3 | 0 | 0.00 | 25,485 | 0.0085 | 0.9915 |
| [50,70) | [0.30,0.35) | 2 | 1 | 27.81 | 24,637 | 0.0084 | 0.9916 |
| [100,150) | [0.40,0.45) | 3 | 0 | 0.00 | 24,180 | 0.0085 | 0.9915 |
| [70,100) | [0.40,0.45) | 2 | 1 | 27.81 | 20,860 | 0.0487 | 0.9513 |
| [70,100) | [0.30,0.35) | 3 | 0 | 0.00 | 20,145 | 0.0084 | 0.9916 |
| [50,70) | [0.40,0.45) | 1 | 4 | 111.24 | 17,046 | 1.0000 | 0.0000 |
| [70,100) | [0.35,0.40) | 2 | 0 | 0.00 | 17,042 | 0.0115 | 0.9885 |
| [100,150) | [0.40,0.45) | 2 | 0 | 0.00 | 16,520 | 0.0487 | 0.9513 |
| [50,70) | [0.40,0.45) | 0 | 76 | 2113.47 | 15,785 | 1.0000 | 0.0000 |
| [150,250) | [0.40,0.45) | 3 | 0 | 0.00 | 15,415 | 0.0085 | 0.9915 |
| [70,100) | [0.30,0.35) | 2 | 0 | 0.00 | 14,140 | 0.0084 | 0.9916 |
| [50,70) | [0.35,0.40) | 0 | 25 | 695.22 | 13,598 | 0.2075 | 0.7925 |
| [50,70) | [0.30,0.35) | 0 | 20 | 556.18 | 12,805 | 0.0695 | 0.9305 |
| [50,70) | [0.35,0.40) | 1 | 1 | 27.81 | 12,514 | 0.0211 | 0.9789 |
| [150,250) | [0.40,0.45) | 2 | 0 | 0.00 | 11,192 | 0.0487 | 0.9513 |
| [50,70) | [0.30,0.35) | 1 | 0 | 0.00 | 11,120 | 0.0084 | 0.9916 |
| [70,100) | [0.40,0.45) | 1 | 4 | 111.24 | 10,095 | 1.0000 | 0.0000 |
| [100,150) | [0.40,0.45) | 1 | 0 | 0.00 | 9,059 | 1.0000 | 0.0000 |
| [70,100) | [0.35,0.40) | 1 | 1 | 27.81 | 8,542 | 0.0211 | 0.9789 |

## Reliability caveats
- `p_local_bucket` is BUCKET-LEVEL empirical precision (1 - local FDR), NOT a per-pair probability (Codex design #3). It ignores anchor count, coverage, length asymmetry, shelf/dup signals.
- The chunk-shuffle null assumes decoy-real chance rate == unrelated real-real chance rate; the CHUNK=12/25 envelope is the conservative mitigation of the unsettled chunk size (design #4).
- Verification runs on UNMASKED streams, so canonical text can re-enter a window near an anchor; `mask_ov_a/mask_ov_b` let consumers drop rows whose evidence is mostly masked (design #5).
- Decoys inflate DF by the injected fraction, mildly tightening the DF<=100 cap vs a pure production run (reported delta above).
- `dup_shelf`/`dup_lines` flag same-object joins (design R4): these are FINDS for a scholar, kept and flagged, not dropped.
## Addendum 2026-07-11 — human review (Hillel) + microfilm masking (MAPV2-10)

**Human review of the review page (`review/track2_wide_small_fragments.html`):**

- **The parallels are excellent** (Hillel, verbatim).
- The 13 "שאר הזוגות המובילים (אי/קצה)" pairs are, on inspection, **citations**:
  Arabic Targum / Arabic commentary that includes the biblical verse itself;
  two works citing the same verse; two halachic works with a shared formula;
  sometimes the same book (e.g. Torah) where one fragment juxtaposes other
  verses read together because of cut pages/columns; and some are indeed good
  parallels of the same poetic passage. This confirms the section's framing
  (island/edge flank = shared-citation suspect), and the note is now embedded
  under that section header in the HTML.
- **Defect found: NLI microfilm title-card pages** ("בית הספרים הלאומי
  והאוניברסיטאי" / "כל הזכויות שמורות" film leaders) read by different HTR
  modes match EACH OTHER and pollute the pair map. Directive: these pages
  should not be mapped at all.

**Fix (MAPV2-10):** `scripts/mask_microfilm_pages.py` fuzzy-detects the
boilerplate (3 needles over norm_stream: בית הספרים הלאומי והאוניברסיטאי @78,
כל הזכויות שמורות @84, המכון לתצלומי כתבי היד העבריים @80) and writes
`data/microfilm_title_pages.json`. Consumers: `build_track2_wide_deck.py`
(pair exclude + over-fetch so sections stay full), `track2_wide_run.py`
(pages dropped at load on future runs — engine input AND decoy pool),
`mapv2_deck.py` (`guard_microfilm` page exclude). Numbers below reflect the
post-mask rebuild of the review page only; the track2_wide.db table itself
still contains the pairs (they are filtered at view/run level).

**Measured impact (2026-07-11):** detector scanned 667,411 pages in ~6 min;
flagged **5,426** title-card/stamp pages (by needle: bet_hasfarim 5,394,
makhon_tatslumim 28, rights 4; flagged-page stream length median 105,
p95 200). Position check: 97% sit at P000003 — the film-leader frame
(P3=5,247, P1-P8 minor, P9+=35; the later-page flags are real NLI ownership
stamps read mid-page, almost all on modern printed books — excluded too, by
design). Wide-tier damage: **24,759 of 552,403 pairs (4.5%) touch a flagged
page**, 24,192 of them passing the review-page filters — stamp-vs-stamp
matches were crowding the small-fragment strata. Track-1 exposure: **zero**
(0 deck cards, 0 track1_matches, 0 track1_candidates rows on flagged pages —
the boilerplate never matched a literary reference; the mapv2_deck guard is
prophylactic). Post-mask rebuild: all three sections fill to 20/12/13; five
title-card pairs dropped out of the top view.
