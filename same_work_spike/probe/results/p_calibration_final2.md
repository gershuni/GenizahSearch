# CAL-1 **FINAL2 (alen-bin refit)** — P(same-work | density, length) [refit-from-rows]

Generated 2026-07-10 20:25; **refit from `../data/cal1_rows_final.json`** (no corpus/reference rebuild) in 0.4 min. train 759,018 / holdout 93,718 rows, 75 held-out works. Truth relation = version-group.

**Isotonic re-fit by ALEN bin** — audit §3 root-cause remedy. Every margin-band curve and the pooled fallback are fit by the row's nearest ALEN bin (edges [40,60,80,100,150,200,300]) — the SAME feature `p_deploy` / `PModel` deploy on (fit / validate / deploy now share one feature). The two small-margin bands (m_003_010, m_0_003) pool alen bins whose cell has < 8 distinct (effective) works, borrowing a band-pooled curve fit on the union. Singleton null + decoy arm reused verbatim from the crop-len `final` model (unchanged by the re-fit). Byte-compatible with `mapv2_track1_run.PModel`.

## Margin-band fit inventory (rows / effective works per alen bin)

| band | alen | rows | eff works | fitted |
|---|---|---|---|---|
| m_ge_010 | 40 | 2 | 1 | — |
| m_ge_010 | 60 | 94 | 35 | yes |
| m_ge_010 | 80 | 359 | 150 | yes |
| m_ge_010 | 100 | 1345 | 285 | yes |
| m_ge_010 | 150 | 2103 | 292 | yes |
| m_ge_010 | 200 | 1945 | 297 | yes |
| m_ge_010 | 300 | 2301 | 295 | yes |
| m_003_010 | 40 | 8 | 6 | yes |
| m_003_010 | 60 | 121 | 31 | yes |
| m_003_010 | 80 | 276 | 82 | yes |
| m_003_010 | 100 | 890 | 214 | yes |
| m_003_010 | 150 | 462 | 81 | yes |
| m_003_010 | 200 | 201 | 33 | yes |
| m_003_010 | 300 | 146 | 26 | yes |
| m_0_003 | 40 | 0 | 0 | yes |
| m_0_003 | 60 | 149 | 46 | yes |
| m_0_003 | 80 | 253 | 45 | yes |
| m_0_003 | 100 | 403 | 83 | yes |
| m_0_003 | 150 | 152 | 35 | yes |
| m_0_003 | 200 | 52 | 20 | yes |
| m_0_003 | 300 | 34 | 14 | yes |
| not_best | 40 | 1327 | 236 | yes |
| not_best | 60 | 512601 | 301 | yes |
| not_best | 80 | 166734 | 301 | yes |
| not_best | 100 | 55425 | 301 | yes |
| not_best | 150 | 8119 | 293 | yes |
| not_best | 200 | 2774 | 252 | yes |
| not_best | 300 | 732 | 124 | yes |

### Holdout reliability of the deployment-composed lookup (predicted-P bucket)

| pred bucket | n | empirical |
|---|---|---|
| 0.0-0.1 | 92215 | 0.003 |
| 0.1-0.2 | 93 | 0.179 |
| 0.2-0.3 | 7 | 0.0 |
| 0.3-0.4 | 4 | 0.0 |
| 0.4-0.5 | 18 | 0.396 |
| 0.5-0.6 | 17 | 0.349 |
| 0.6-0.7 | 1 | 0.0 |
| 0.7-0.8 | 29 | 0.748 |
| 0.8-0.9 | 49 | 0.742 |
| 0.9-1.0 | 1285 | 0.987 |

### Self-validation — holdout reliability grid (margin band × ALEN bin) through the DEPLOY path

Work-weighted (1 unit per (alen-bin, work)); `gap = pred − empirical`. `final gap` re-scores the SAME holdout rows through the crop-len-fit `final` model for a like-for-like comparison. **`Δ|gap|`** = `|final2 gap| − |final gap|` (negative = final2 improved); a bucket flagged **WORSE** regressed by > 0.05.

| band | alen | n | final2 pred | empirical | final2 gap | final pred | final gap | Δ\|gap\| | flag |
|---|---|---|---|---|---|---|---|---|---|
| singleton | 60 | 1 | 0.883 | 1.000 | -0.117 | 0.883 | -0.117 | +0.000 |  |
| m_ge_010 | 40 | 1 | 1.000 | 1.000 | +0.000 | 1.000 | +0.000 | +0.000 |  |
| m_ge_010 | 60 | 5 | 0.945 | 0.730 | +0.215 | 1.000 | +0.271 | -0.055 |  |
| m_ge_010 | 80 | 57 | 0.997 | 0.971 | +0.025 | 0.995 | +0.024 | +0.002 |  |
| m_ge_010 | 100 | 199 | 0.998 | 0.989 | +0.009 | 0.987 | -0.002 | +0.006 |  |
| m_ge_010 | 150 | 307 | 1.000 | 1.000 | -0.000 | 0.997 | -0.004 | -0.004 |  |
| m_ge_010 | 200 | 239 | 0.999 | 1.000 | -0.002 | 1.000 | +0.000 | +0.002 |  |
| m_ge_010 | 300 | 301 | 1.000 | 1.000 | +0.000 | 1.000 | +0.000 | +0.000 |  |
| m_003_010 | 60 | 15 | 0.368 | 0.481 | -0.113 | 0.659 | +0.178 | -0.065 |  |
| m_003_010 | 80 | 28 | 0.780 | 0.759 | +0.021 | 0.908 | +0.149 | -0.128 |  |
| m_003_010 | 100 | 99 | 0.966 | 0.973 | -0.007 | 0.738 | -0.235 | -0.228 |  |
| m_003_010 | 150 | 38 | 0.956 | 0.964 | -0.008 | 0.704 | -0.260 | -0.252 |  |
| m_003_010 | 200 | 19 | 0.903 | 0.746 | +0.157 | 0.824 | +0.077 | +0.079 | **WORSE** |
| m_003_010 | 300 | 17 | 1.000 | 0.727 | +0.274 | 0.867 | +0.140 | +0.134 | **WORSE** |
| m_0_003 | 40 | 1 | 0.633 | 0.000 | +0.633 | 0.694 | +0.694 | -0.060 |  |
| m_0_003 | 60 | 11 | 0.110 | 0.000 | +0.110 | 0.478 | +0.478 | -0.368 |  |
| m_0_003 | 80 | 18 | 0.466 | 0.185 | +0.281 | 0.335 | +0.150 | +0.131 | **WORSE** |
| m_0_003 | 100 | 29 | 0.761 | 0.748 | +0.013 | 0.205 | -0.542 | -0.529 |  |
| m_0_003 | 150 | 11 | 0.884 | 0.646 | +0.238 | 0.668 | +0.023 | +0.216 | **WORSE** |
| m_0_003 | 200 | 9 | 0.801 | 0.794 | +0.006 | 0.016 | -0.778 | -0.772 |  |
| m_0_003 | 300 | 3 | 1.000 | 1.000 | +0.000 | 0.731 | -0.269 | -0.269 |  |
| not_best | 40 | 182 | 0.000 | 0.000 | +0.000 | 0.012 | +0.012 | -0.012 |  |
| not_best | 60 | 65144 | 0.000 | 0.000 | -0.000 | 0.001 | +0.001 | -0.001 |  |
| not_best | 80 | 19184 | 0.000 | 0.001 | -0.000 | 0.001 | +0.001 | -0.000 |  |
| not_best | 100 | 6571 | 0.004 | 0.004 | +0.001 | 0.002 | -0.001 | -0.000 |  |
| not_best | 150 | 890 | 0.016 | 0.015 | +0.001 | 0.009 | -0.006 | -0.004 |  |
| not_best | 200 | 280 | 0.027 | 0.015 | +0.012 | 0.004 | -0.011 | +0.001 |  |
| not_best | 300 | 59 | 0.046 | 0.091 | -0.045 | 0.017 | -0.074 | -0.029 |  |

**Mean |gap| over 28 shared buckets: final2 0.082 vs final 0.161.** Improved: 17; regressed by >0.05: 4.

**Buckets that regressed:**
- `m_0_003`/alen-150: |gap| 0.023 → 0.238 (Δ +0.216); pred 0.668→0.884, empirical 0.646 (n=11)
- `m_003_010`/alen-300: |gap| 0.140 → 0.274 (Δ +0.134); pred 0.867→1.000, empirical 0.727 (n=17)
- `m_0_003`/alen-80: |gap| 0.150 → 0.281 (Δ +0.131); pred 0.335→0.466, empirical 0.185 (n=18)
- `m_003_010`/alen-200: |gap| 0.077 → 0.157 (Δ +0.079); pred 0.824→0.903, empirical 0.746 (n=19)
