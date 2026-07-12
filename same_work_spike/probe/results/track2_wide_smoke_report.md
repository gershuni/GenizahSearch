# Track-2 WIDE tier report (smoke)

Generated 2026-07-10 23:55; source `C:\Genizahsearch\same_work_spike\probe\data\mapv2_smoke.db`; git `v8.4.0-113-ga8d0e0ae-dirty`.

## Cohort sizes
- real pages: 139,694
- decoys: 1,200 {'D25': 600, 'D12': 600}
- leak-clean pool: 55,237 (84,457 excluded)

## Volume funnel
- candidate_pairs: 33,745,608
- rej_short: 0
- decoy_decoy: 3,404
- decoy_real: 3
- strict_pairs_seen: 731,737
- strict_purged_raw: 0
- wide_spilled_raw: 584,850
- wide_deduped: 584,850
- pruned low-value: 47476
- survivors (post score+prune): 537374

## Strict-pairs vs chain step-6
- strict-class real-real PAIRS seen this run: 731,737 (pair-level; a pair with any strict segment is excluded from the sidecar entirely)
- chain step-6 `accepted_pairs_canonmask` rows: 735,700
- delta: -0.54% (small DF perturbation from injected decoys is expected)

## Normalization derivation
Opportunity normalization -- expected null real-real per bucket =
`decoy_real_count * n_real / (2 * n_dec_cohort)`, conservative
envelope = MAX over the two chunk cohorts. Factors this run:

- D25: factor = 139,694 / (2 * 600) = 116.41
- D12: factor = 139,694 / (2 * 600) = 116.41

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
| [50,70) | [0.40,0.45) | 2 | 0 | 0.00 | 38,802 | 0.1238 | 0.8762 |
| [70,100) | [0.40,0.45) | 2 | 0 | 0.00 | 37,799 | 0.1238 | 0.8762 |
| [70,100) | [0.35,0.40) | 2 | 1 | 116.41 | 30,274 | 0.0328 | 0.9672 |
| [50,70) | [0.35,0.40) | 2 | 1 | 116.41 | 28,911 | 0.0328 | 0.9672 |
| [100,150) | [0.40,0.45) | 2 | 0 | 0.00 | 28,278 | 0.1238 | 0.8762 |
| [50,70) | [0.40,0.45) | 3 | 0 | 0.00 | 24,263 | 0.1140 | 0.8860 |
| [50,70) | [0.30,0.35) | 2 | 0 | 0.00 | 23,692 | 0.0050 | 0.9950 |
| [70,100) | [0.30,0.35) | 2 | 0 | 0.00 | 23,313 | 0.0050 | 0.9950 |
| [70,100) | [0.40,0.45) | 3 | 0 | 0.00 | 22,988 | 0.1140 | 0.8860 |
| [70,100) | [0.40,0.45) | 1 | 0 | 0.00 | 21,815 | 1.0000 | 0.0000 |
| [100,150) | [0.40,0.45) | 1 | 0 | 0.00 | 19,827 | 1.0000 | 0.0000 |
| [50,70) | [0.40,0.45) | 1 | 1 | 116.41 | 19,358 | 1.0000 | 0.0000 |
| [70,100) | [0.35,0.40) | 3 | 0 | 0.00 | 17,897 | 0.0807 | 0.9193 |
| [150,250) | [0.40,0.45) | 2 | 0 | 0.00 | 17,370 | 0.1238 | 0.8762 |
| [50,70) | [0.35,0.40) | 3 | 0 | 0.00 | 16,817 | 0.0807 | 0.9193 |
| [70,100) | [0.35,0.40) | 1 | 0 | 0.00 | 16,554 | 0.0396 | 0.9604 |
| [150,250) | [0.40,0.45) | 1 | 0 | 0.00 | 15,023 | 1.0000 | 0.0000 |
| [100,150) | [0.40,0.45) | 3 | 0 | 0.00 | 14,817 | 0.1140 | 0.8860 |
| [50,70) | [0.35,0.40) | 1 | 0 | 0.00 | 13,905 | 0.0396 | 0.9604 |
| [50,70) | [0.30,0.35) | 3 | 0 | 0.00 | 13,383 | 0.0090 | 0.9910 |
| [70,100) | [0.30,0.35) | 3 | 0 | 0.00 | 12,976 | 0.0090 | 0.9910 |
| [70,100) | [0.30,0.35) | 1 | 0 | 0.00 | 12,366 | 0.0094 | 0.9906 |
| [50,70) | [0.30,0.35) | 1 | 0 | 0.00 | 11,898 | 0.0098 | 0.9902 |
| [50,70) | [0.40,0.45) | 0 | 0 | 0.00 | 10,414 | 1.0000 | 0.0000 |
| [70,100) | [0.40,0.45) | 0 | 0 | 0.00 | 9,168 | 1.0000 | 0.0000 |

## Reliability caveats
- `p_local_bucket` is BUCKET-LEVEL empirical precision (1 - local FDR), NOT a per-pair probability (Codex design #3). It ignores anchor count, coverage, length asymmetry, shelf/dup signals.
- The chunk-shuffle null assumes decoy-real chance rate == unrelated real-real chance rate; the CHUNK=12/25 envelope is the conservative mitigation of the unsettled chunk size (design #4).
- Verification runs on UNMASKED streams, so canonical text can re-enter a window near an anchor; `mask_ov_a/mask_ov_b` let consumers drop rows whose evidence is mostly masked (design #5).
- Decoys inflate DF by the injected fraction, mildly tightening the DF<=100 cap vs a pure production run (reported delta above).
- `dup_shelf`/`dup_lines` flag same-object joins (design R4): these are FINDS for a scholar, kept and flagged, not dropped.