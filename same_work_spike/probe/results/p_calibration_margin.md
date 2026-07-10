# CAL-1 margin refit — (length, density, margin-band) model

Generated 2026-07-10 11:36 (711s, no re-querying — margins computed post-hoc from the pilot candidate rows). margin = best-OTHER-work density − this candidate's density on the same crop; 'singleton' = no other work verified at the 0.75 wide cutoff.

## Why: the singleton signal (raw correctness by band × density slice)

**pilot** (rows per band: {'0.03<=m<0.10': 2465, 'm<=0': 907888, 'm>=0.10': 9959, '0<m<0.03': 1168, 'singleton': 12})

| band | n | 0.0-0.3 | 0.3-0.4 | 0.4-0.5 | 0.5-0.6 |
|---|---|---|---|---|---|
| singleton | 12 | — | — | 1.0 (n=1) | 0.889 (n=9) |
| m>=0.10 | 9,959 | 0.999 (n=4700) | 0.995 (n=2443) | 0.99 (n=2028) | 0.992 (n=760) |
| 0.03<=m<0.10 | 2,465 | 0.915 (n=282) | 0.845 (n=419) | 0.908 (n=600) | 0.905 (n=831) |
| 0<m<0.03 | 1,168 | 0.538 (n=78) | 0.564 (n=163) | 0.572 (n=304) | 0.567 (n=409) |
| m<=0 | 907,888 | 0.125 (n=264) | 0.06 (n=1759) | 0.028 (n=6921) | 0.009 (n=29258) |

**pilot-n10** (rows per band: {'0.03<=m<0.10': 2451, 'm<=0': 780283, 'm>=0.10': 9875, '0<m<0.03': 1230, 'singleton': 5})

| band | n | 0.0-0.3 | 0.3-0.4 | 0.4-0.5 | 0.5-0.6 |
|---|---|---|---|---|---|
| singleton | 5 | — | — | — | 1.0 (n=1) |
| m>=0.10 | 9,875 | 0.998 (n=2915) | 0.997 (n=3267) | 0.993 (n=2493) | 0.993 (n=1163) |
| 0.03<=m<0.10 | 2,451 | 0.871 (n=210) | 0.869 (n=451) | 0.879 (n=547) | 0.911 (n=744) |
| 0<m<0.03 | 1,230 | 0.558 (n=43) | 0.585 (n=147) | 0.547 (n=265) | 0.539 (n=349) |
| m<=0 | 780,283 | 0.211 (n=109) | 0.098 (n=1025) | 0.042 (n=4647) | 0.012 (n=19080) |

**pilot-n20** (rows per band: {'0.03<=m<0.10': 2404, 'm<=0': 683418, 'm>=0.10': 9584, '0<m<0.03': 1458, 'singleton': 14})

| band | n | 0.0-0.3 | 0.3-0.4 | 0.4-0.5 | 0.5-0.6 |
|---|---|---|---|---|---|
| singleton | 14 | — | 1.0 (n=1) | — | — |
| m>=0.10 | 9,584 | 0.996 (n=1153) | 0.997 (n=3899) | 0.998 (n=2912) | 0.99 (n=1568) |
| 0.03<=m<0.10 | 2,404 | 0.853 (n=68) | 0.897 (n=456) | 0.879 (n=488) | 0.923 (n=684) |
| 0<m<0.03 | 1,458 | 0.6 (n=15) | 0.613 (n=150) | 0.591 (n=225) | 0.575 (n=273) |
| m<=0 | 683,418 | 0.368 (n=19) | 0.147 (n=457) | 0.059 (n=2745) | 0.017 (n=11369) |

## Holdout reliability (work-granular split, per-work weighted)

**pilot** (holdout rows scored: 97,234)

| pred bucket | n | empirical |
|---|---|---|
| 0.0-0.1 | 95596 | 0.001 |
| 0.1-0.2 | 160 | 0.171 |
| 0.2-0.3 | 34 | 0.264 |
| 0.3-0.4 | 2 | 1.0 |
| 0.4-0.5 | 7 | 0.033 |
| 0.5-0.6 | 58 | 0.652 |
| 0.6-0.7 | 21 | 0.471 |
| 0.7-0.8 | 6 | 0.565 |
| 0.8-0.9 | 65 | 0.878 |
| 0.9-1.0 | 1285 | 0.978 |

**pilot-n10** (holdout rows scored: 82,083)

| pred bucket | n | empirical |
|---|---|---|
| 0.0-0.1 | 80469 | 0.001 |
| 0.1-0.2 | 137 | 0.137 |
| 0.2-0.3 | 31 | 0.326 |
| 0.3-0.4 | 26 | 0.201 |
| 0.4-0.5 | 7 | 0.77 |
| 0.5-0.6 | 59 | 0.589 |
| 0.6-0.7 | 27 | 0.626 |
| 0.7-0.8 | 18 | 0.931 |
| 0.8-0.9 | 55 | 0.811 |
| 0.9-1.0 | 1254 | 0.988 |

**pilot-n20** (holdout rows scored: 72,734)

| pred bucket | n | empirical |
|---|---|---|
| 0.0-0.1 | 71177 | 0.001 |
| 0.1-0.2 | 58 | 0.175 |
| 0.2-0.3 | 106 | 0.151 |
| 0.3-0.4 | 10 | 0.228 |
| 0.4-0.5 | 6 | 0.382 |
| 0.5-0.6 | 50 | 0.495 |
| 0.6-0.7 | 40 | 0.62 |
| 0.7-0.8 | 30 | 0.713 |
| 0.8-0.9 | 3 | 0.514 |
| 0.9-1.0 | 1254 | 0.982 |

## Stress test — Hillel's density_fail cards (10/10 graded correct; ALL singletons), singleton-band predicted P per arm

| len | density | pilot | pilot-n10 | pilot-n20 | (len,dens)-only best arm |
|---|---|---|---|---|---|
| 145 | 0.552 | — | — | — | 0.026 |
| 176 | 0.545 | — | — | — | 0.01 |
| 272 | 0.526 | — | — | — | 0.002 |
| 222 | 0.477 | — | — | — | 0.261 |
| 125 | 0.472 | — | — | — | 0.891 |
| 257 | 0.463 | — | — | — | 0.062 |
| 276 | 0.460 | — | — | — | 0.062 |
| 241 | 0.456 | — | — | — | 0.321 |
| 298 | 0.406 | — | — | — | 0.57 |

Success criterion: singleton-band P materially above the (len,dens)-only predictions for these human-verified-correct cards. If it holds, the FINAL CAL-1 model ships as P(correct | length, density, margin-band) with per-provenance arms.
