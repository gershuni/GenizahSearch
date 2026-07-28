# CERT-01 Pre-Outcome Operating-Characteristics (OC) Table

**Computed BEFORE any card is drawn** (protocol §6, RESEARCH.md Pitfall 8). Expectations-setting only -- the deck is drawn regardless of what this table says.

Frame: `tier_a` shipped estimand, 134,123 `(page, canonical_work_id)` rows over 31,022 physMS clusters. Discovery deck size: ~220 cards (within the protocol's 200-250 band). Strict floor: 0.85. Single gate, k=1, no multiple-testing correction (protocol §2).

**Methodology note (documented assumption):** the true within-cluster verdict correlation (ICC) cannot be measured before any card is graded. Three illustrative correlation scenarios (rho = 0.0 null / 0.05 / 0.10, spanning `e1_confirm_sizing.py`'s own self-test's documented plausible range) are realized via a Beta-Bernoulli simulation over the REAL frame's physMS cluster-size distribution (`cluster_sizes()`), then fed to the REUSED `anova_icc`/`size_confirmation`/`expected_nonempty_components`/ `wilson_lower_one_sided`/`binom_sf` functions (never re-derived). The CLUSTER GEOMETRY is real; only the correlation coefficient itself is an assumption, disclosed as such.

| rho (ICC scenario) | true p | INS rate | ICC realized | deff | n_eff | joint pass prob. | confirmation sizing | conditional confirm. pass prob. |
|---|---|---|---|---|---|---|---|---|
| 0.00 | 0.80 | 0.00 | 0.0021 | 1.000 | 220 | 0.000 | screening (discovery lower bound below locked threshold) | n/a |
| 0.00 | 0.80 | 0.10 | 0.0021 | 1.000 | 198 | 0.000 | screening (discovery lower bound below locked threshold) | n/a |
| 0.00 | 0.80 | 0.20 | 0.0021 | 1.000 | 176 | 0.000 | screening (discovery lower bound below locked threshold) | n/a |
| 0.00 | 0.85 | 0.00 | 0.0035 | 1.000 | 220 | 0.050 | screening (no n <= 5000 reaches 80% power) | n/a |
| 0.00 | 0.85 | 0.10 | 0.0035 | 1.000 | 198 | 0.047 | screening (no n <= 5000 reaches 80% power) | n/a |
| 0.00 | 0.85 | 0.20 | 0.0035 | 1.000 | 176 | 0.043 | screening (no n <= 5000 reaches 80% power) | n/a |
| 0.00 | 0.90 | 0.00 | 0.0010 | 1.000 | 220 | 0.720 | n_drawn=270 | ~0.80 (by construction) |
| 0.00 | 0.90 | 0.10 | 0.0010 | 1.000 | 198 | 0.666 | n_drawn=300 | ~0.80 (by construction) |
| 0.00 | 0.90 | 0.20 | 0.0010 | 1.000 | 176 | 0.602 | n_drawn=340 | ~0.80 (by construction) |
| 0.00 | 0.95 | 0.00 | 0.0000 | 1.000 | 220 | 1.000 | n_drawn=60 | ~0.80 (by construction) |
| 0.00 | 0.95 | 0.10 | 0.0000 | 1.000 | 198 | 1.000 | n_drawn=70 | ~0.80 (by construction) |
| 0.00 | 0.95 | 0.20 | 0.0000 | 1.000 | 176 | 0.999 | n_drawn=70 | ~0.80 (by construction) |
| 0.05 | 0.80 | 0.00 | 0.0500 | 1.002 | 219 | 0.000 | screening (discovery lower bound below locked threshold) | n/a |
| 0.05 | 0.80 | 0.10 | 0.0500 | 1.002 | 198 | 0.000 | screening (discovery lower bound below locked threshold) | n/a |
| 0.05 | 0.80 | 0.20 | 0.0500 | 1.002 | 176 | 0.000 | screening (discovery lower bound below locked threshold) | n/a |
| 0.05 | 0.85 | 0.00 | 0.0502 | 1.002 | 219 | 0.053 | screening (no n <= 5000 reaches 80% power) | n/a |
| 0.05 | 0.85 | 0.10 | 0.0502 | 1.002 | 198 | 0.047 | screening (no n <= 5000 reaches 80% power) | n/a |
| 0.05 | 0.85 | 0.20 | 0.0502 | 1.002 | 176 | 0.043 | screening (no n <= 5000 reaches 80% power) | n/a |
| 0.05 | 0.90 | 0.00 | 0.0568 | 1.003 | 219 | 0.728 | n_drawn=270 | ~0.80 (by construction) |
| 0.05 | 0.90 | 0.10 | 0.0568 | 1.002 | 198 | 0.666 | n_drawn=300 | ~0.80 (by construction) |
| 0.05 | 0.90 | 0.20 | 0.0568 | 1.002 | 176 | 0.602 | n_drawn=340 | ~0.80 (by construction) |
| 0.05 | 0.95 | 0.00 | 0.0602 | 1.003 | 219 | 1.000 | n_drawn=60 | ~0.80 (by construction) |
| 0.05 | 0.95 | 0.10 | 0.0602 | 1.003 | 197 | 1.000 | n_drawn=70 | ~0.80 (by construction) |
| 0.05 | 0.95 | 0.20 | 0.0602 | 1.002 | 176 | 0.999 | n_drawn=80 | ~0.80 (by construction) |
| 0.10 | 0.80 | 0.00 | 0.1094 | 1.005 | 219 | 0.000 | screening (discovery lower bound below locked threshold) | n/a |
| 0.10 | 0.80 | 0.10 | 0.1094 | 1.005 | 197 | 0.000 | screening (discovery lower bound below locked threshold) | n/a |
| 0.10 | 0.80 | 0.20 | 0.1094 | 1.004 | 175 | 0.000 | screening (discovery lower bound below locked threshold) | n/a |
| 0.10 | 0.85 | 0.00 | 0.0984 | 1.005 | 219 | 0.053 | screening (no n <= 5000 reaches 80% power) | n/a |
| 0.10 | 0.85 | 0.10 | 0.0984 | 1.004 | 197 | 0.049 | screening (no n <= 5000 reaches 80% power) | n/a |
| 0.10 | 0.85 | 0.20 | 0.0984 | 1.004 | 175 | 0.045 | screening (no n <= 5000 reaches 80% power) | n/a |
| 0.10 | 0.90 | 0.00 | 0.0923 | 1.004 | 219 | 0.728 | n_drawn=270 | ~0.80 (by construction) |
| 0.10 | 0.90 | 0.10 | 0.0923 | 1.004 | 197 | 0.675 | n_drawn=300 | ~0.80 (by construction) |
| 0.10 | 0.90 | 0.20 | 0.0923 | 1.004 | 175 | 0.612 | n_drawn=340 | ~0.80 (by construction) |
| 0.10 | 0.95 | 0.00 | 0.1000 | 1.005 | 219 | 1.000 | n_drawn=60 | ~0.80 (by construction) |
| 0.10 | 0.95 | 0.10 | 0.1000 | 1.004 | 197 | 1.000 | n_drawn=70 | ~0.80 (by construction) |
| 0.10 | 0.95 | 0.20 | 0.1000 | 1.004 | 175 | 0.999 | n_drawn=80 | ~0.80 (by construction) |

**Pre-reserved confirmation-draw size (frozen at freeze time):** 340 -- the MAXIMUM finite `size_confirmation` `n_drawn` observed across the whole grid above (conservative: whatever the real discovery outcome turns out to be, within the workable region, a single physically-sequestered reserve of this size covers it without drawing more cards after discovery results land -- protocol §7).

**Reading this table (Pitfall 8):** at `p` exactly AT the Strict floor (0.85), the joint pass probability is ~alpha (~0.04-0.05) BY CONSTRUCTION -- a one-sided lower-bound test of a true value sitting exactly on its own threshold clears only rarely. At `p=0.80` (below floor), the joint pass probability is 0 and `size_confirmation` correctly reports screening (`discovery lower bound below locked threshold`). At `p=0.90`/`0.95` (comfortably above), joint pass probability is substantial to near-certain. This matches the protocol's own framing (`PLAN-e1-round2.md`): Strict at ~200-250 cards is a materially harder target than Broad -- a low pass probability at `p` near the floor is a KNOWN, DISCLOSED risk, not a reason to skip the OC step.
