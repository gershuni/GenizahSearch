# FINAL CAL-1 re-audit — P(same-work) calibration

**Auditor:** statistics agent · **Date:** 2026-07-10
**Artifacts audited:** `data/p_calibration_final.json` (model), `data/cal1_rows_final.json`
(852,736 labeled rows + 12,813 decoy rows), `results/p_calibration_final.md`,
`scripts/cal1_calibration.py::p_deploy`, `scripts/mapv2_track1_run.py::PModel.p`.
Model meta: stage=`final`, truth=`version-group`, 916 pages → 12,824 crops, seed 20260710,
CHUNK=25 decoys, generated 2026-07-10 17:08.

---

## VERDICT: SOUND-WITH-CAPS

The model is an **excellent ranker and a safe accept/reject gate**, but its **P values are
not trustworthy as probabilities in the 0.2–0.8 "maybe" range**, and its **singleton band is
under-validated and optimistic**. Deploy the run as-is for tier-A (P never touches tier A) and
for coarse tier-B accept/reject; apply the caps below in the **deck builder** before any human
review, and correct the "isotonic per length" framing (see §3).

**Integrity check passed:** my independent re-implementation of `p_deploy` reproduces the
model's stored `reliability_margin` table **bucket-for-bucket** (0.0–0.1 → 0.002 on n=92,088;
0.9–1.0 → 0.981 on n=1,151; every intermediate bucket matches). All numbers below are therefore
faithful to the deployed lookup.

### The 5 numbers that matter
1. **AUC = 0.9909** overall (12,746 pos / 839,990 neg); **0.958** within hard cases (alen<80).
   Ranking is strong; the problem is calibration, not separation.
2. **Extremes are well-calibrated:** P<0.1 bucket → **0.2%** empirical (834,921 rows);
   P≥0.9 bucket → **98.5%** empirical (9,961 rows). These two buckets hold ~99% of all mass.
3. **Mid-range is badly miscalibrated:** the small-margin bands miss by **+0.18 to +0.70**
   (e.g. `m_0_003`/alen-100 predicts 0.31, empirical 0.80; `m_0_003`/alen-200 predicts 0.11,
   empirical 0.81) and **−0.27 to −0.41** the other way (`m_0_003`/alen-60 predicts 0.49,
   empirical 0.08). Non-monotone in length.
4. **The length feature has collapsed:** aligned length `alen` clusters at **median ~66 for
   EVERY crop length** (len-40 crops → alen mean 69.6; len-300 crops → alen mean 77.7). The
   model fits 7 length curves but deploys ~2 (alen-60 receives 68% of rows, alen-80 22%); the
   len-40 curve is selected for **0.5%** of len-40 rows.
5. **Singleton band = 11 labeled rows total.** Decoy null gives P≥0.90 for singletons out to
   density ~0.45–0.50 at every length, yet **chance matches themselves reach density ~0.50**
   (decoy p05=0.50 at alen-40). Unvalidated and optimistic in the 0.3–0.55 density range.

---

## Findings ranked by severity

| # | Severity | Finding |
|---|----------|---------|
| 1 | **HIGH** | Mid-range P (0.2–0.8) is miscalibrated by up to ±0.4–0.7 in the small-margin bands (`m_003_010`, `m_0_003`) — the discovery-relevant "maybe" zone. Treat P as an **ordinal grade**, not a probability, in this range. |
| 2 | **HIGH** | Length-feature collapse: `alen` ≈ 66 regardless of crop length; isotonic is **fit by crop-len** but **deployed/validated by alen** → within-bin population shift and 2–3 curves doing all the work. Root cause of #1. **Re-fit isotonic by alen bin.** |
| 3 | **HIGH** | Singleton band under-validated (11 labeled rows) and optimistic (P≥0.9 to density ~0.45); decoy null is contaminated by identity shuffles at short alen (CHUNK=25 on a 40-char crop is 50% identity). **Cap it.** |
| 4 | **MED-HIGH** | `RARITY_MAX=60` still hardcoded in `build_smoke_preview2.py` (pilot BLOCKER, not yet fixed). Must become a quantile of the full-corpus tier-A witness distribution — §6 gives the exact procedure. |
| 5 | **MED** | Competitor/margin definition **differs** between calibration (all candidate works of a single-span crop) and deployment (span-overlap ≥ 0.5 only). Identical on single-span crops, **divergent and untested on multi-work pages** — where the margin band is decided. |
| 6 | **MED** | Small-margin bands have tiny *effective* (per-work-weighted) n; `MARGIN_FIT_FLOOR=20` counts raw rows only. Isotonic overfits → the non-monotone-in-length curves in #1. |
| 7 | **PASS** | Bucketing-parity BLOCKER (Codex #2) **fix held**: `p_deploy` and `PModel.p` are logically identical and both key on `alen`; holdout reliability is computed through `p_deploy`, so validation and deployment use the same bin. Verified line-by-line (§5). |
| 8 | **LOW** | Pooled per-length model is effectively dead code at deploy (all 4 non-singleton bands fitted for all 7 lengths → fallback never taken). Harmless. `not_best`/alen-300 mildly under-predicts (0.012 vs 0.083 empirical) — safe direction. |

---

## §1 Discrimination (AUC)

Computed on predicted P vs `correct`, over all 852,736 rows (Mann-Whitney, average-rank tie
handling):

| Slice | AUC | positives | negatives | base rate |
|---|---|---|---|---|
| Overall | **0.9909** | 12,746 | 839,990 | 1.49% |
| Hard cases (alen < 80) | **0.9582** | 432 | 670,913 | 0.06% |
| alen ≤ 60 | 0.9982 | 11 | 3,907 | (unstable, 11 pos) |

Consistent with the pilot audit's 0.990 overall. Discrimination is strong; **the model orders
same-work above wrong-work reliably.** The audit's concerns are all about the *values*, not the
*ordering*. (Note: alen<80 is ~99.9% negatives — the giant pile of chance `not_best` matches at
alen~60 — so 0.958 there is still healthy separation.)

---

## §2 Reliability (predicted P vs empirical same-work rate)

Per-work weighted (each source work contributes weight 1 per length bin, matching the fit),
computed on **all rows** (the dump carries no train/holdout flag; the holdout-only version
matches the model's stored table exactly and is shown for cross-check).

**By predicted-P bucket (deployment-composed lookup):**

| bucket | n (raw) | pred mid | empirical | gap |
|---|---|---|---|---|
| 0.0–0.1 | 834,921 | 0.05 | **0.002** | −0.048 ✓ |
| 0.1–0.2 | 2,713 | 0.15 | 0.149 | −0.001 ✓ |
| 0.2–0.3 | 2,269 | 0.25 | 0.025 | **−0.225** |
| 0.3–0.4 | 20 | 0.35 | 0.001 | −0.349 (n small) |
| 0.4–0.5 | 32 | 0.45 | 0.303 | −0.147 (n small) |
| 0.5–0.6 | 861 | 0.55 | 0.526 | −0.024 ✓ |
| 0.6–0.7 | 483 | 0.65 | 0.463 | **−0.187** |
| 0.7–0.8 | 1,025 | 0.75 | 0.980 | **+0.230** |
| 0.8–0.9 | 451 | 0.85 | 0.695 | **−0.155** |
| 0.9–1.0 | 9,961 | 0.95 | **0.985** | +0.035 ✓ |

The two extreme buckets (99% of mass) are well-calibrated. The interior buckets — the "maybe"
grades a discovery deck lives on — swing wildly and even **non-monotonically** (0.7–0.8 over-performs
its own bucket at 0.98 while 0.8–0.9 under-performs at 0.70). Interior buckets hold only ~0.6% of
mass, so each is a noisy estimate, but the pattern is systematic.

**By (margin band × alen bin)** — buckets flagged `***` are off by >0.05 with n≥30 raw rows:

| band | alen bin | n (raw) | mean pred | empirical (wtd) | gap | flag |
|---|---|---|---|---|---|---|
| m_ge_010 | 60 | 99 | 1.000 | 0.956 | −0.043 | |
| m_ge_010 | 80 | 416 | 0.994 | 0.982 | −0.012 | |
| m_ge_010 | 100 | 1,544 | 0.988 | 0.996 | +0.008 | |
| m_ge_010 | 150 | 2,410 | 0.997 | 1.000 | +0.003 | |
| m_ge_010 | 200 | 2,184 | 1.000 | 0.998 | −0.002 | |
| m_ge_010 | 300 | 2,602 | 1.000 | 1.000 | +0.000 | |
| m_003_010 | 60 | 136 | 0.718 | 0.444 | **−0.274** | *** |
| m_003_010 | 80 | 304 | 0.904 | 0.804 | **−0.100** | *** |
| m_003_010 | 100 | 989 | 0.800 | 0.977 | **+0.177** | *** |
| m_003_010 | 150 | 500 | 0.711 | 0.952 | **+0.241** | *** |
| m_003_010 | 200 | 220 | 0.833 | 0.869 | +0.036 | |
| m_003_010 | 300 | 163 | 0.860 | 0.907 | +0.047 | |
| m_0_003 | 60 | 160 | 0.493 | 0.081 | **−0.412** | *** |
| m_0_003 | 80 | 271 | 0.278 | 0.431 | **+0.153** | *** |
| m_0_003 | 100 | 432 | 0.314 | 0.798 | **+0.484** | *** |
| m_0_003 | 150 | 163 | 0.674 | 0.856 | **+0.182** | *** |
| m_0_003 | 200 | 61 | 0.109 | 0.809 | **+0.699** | *** |
| m_0_003 | 300 | 37 | 0.731 | 1.000 | **+0.269** | *** |
| not_best | 40 | 1,509 | 0.016 | 0.000 | −0.016 | |
| not_best | 60 | 577,745 | 0.001 | 0.000 | −0.001 | |
| not_best | 80 | 185,918 | 0.003 | 0.001 | −0.003 | |
| not_best | 100 | 61,996 | 0.005 | 0.009 | +0.004 | |
| not_best | 150 | 9,009 | 0.012 | 0.031 | +0.019 | |
| not_best | 200 | 3,054 | 0.006 | 0.022 | +0.016 | |
| not_best | 300 | 791 | 0.012 | 0.083 | **+0.072** | *** |

**Read:** `m_ge_010` (clear winner, margin ≥ 0.10) and `not_best` are excellent — these are the
bands that carry the accept/reject decision, and they are trustworthy. **Every flagged bucket is
in the two small-margin bands** `m_003_010` and `m_0_003` — the close-call candidates. Their
curves are non-monotone in length (m_0_003 alen-60 → 0.49 but alen-100 → 0.31) because they are
fit on very few *effective* works (see §7).

---

## §3 The length feature has collapsed (root cause of §2)

`alen` (the hull's aligned length) is **not** the crop length. The seed-and-extend hull is
limited by HTR noise to the longest cleanly-alignable run (~66 letters), so it saturates near 66
**independent of how long the crop is**:

| crop len | alen mean | alen median | alen p95 | % of rows whose nearest alen-bin ≠ crop-len |
|---|---|---|---|---|
| 40 | 69.6 | 66 | 90 | **99.5%** |
| 60 | 71.2 | 66 | 96 | 24.5% |
| 80 | 72.3 | 66 | 98 | 81.0% |
| 100 | 73.4 | 66 | 99 | 93.5% |
| 150 | 75.2 | 67 | 103 | 98.7% |
| 200 | 76.0 | 67 | 103 | 99.1% |
| 300 | 77.7 | 67 | 106 | 99.0% |

Consequences:
- The isotonic is **fit by `r['len']`** (crop length; `cal1_calibration.py` lines 388, 446) but
  **deployed and validated by `alen`** (`p_deploy`/`PModel.p`). Within each nominal length bin,
  the fit population and the deploy population differ.
- At deploy, `alen` maps overwhelmingly to bins 60 (68% of rows) and 80 (22%); the **len-40 curve
  is essentially never used** (0.5% of len-40 rows), and the len-150/200/300 curves receive a thin
  tail. The report's "isotonic per length (7 curves)" is misleading — deployment behaves like a
  ~single-length (alen≈60–80) calibration with margin-band stratification.
- This is *not* the Codex BLOCKER-2 bug (that was "validate one bin, deploy another" — fixed, see
  §5). It is a second-order residue: the fix aligned *validation* with *deployment* (both alen) but
  left the *fit* keyed on crop-len. **Recommended fix: re-fit the isotonic by alen bin** (and pool
  lengths for the small-margin bands, §7). This is the cleanest single remedy for §2.

Deploy/calibration `alen` distributions are themselves broadly comparable (both are HTR-noise-
limited hull lengths), so this is a *calibration-quality* issue, not a distribution-shift bug — but
it fully explains the interior miscalibration.

---

## §4 Singleton band — inflation and decoy-null validity

**Labeled anchor mass is essentially nil:** only **11** of 852,736 rows are labeled singletons
(10 at crop-len 40, 1 at 80). The band cannot be validated from labeled data by construction — a
random crop of a well-referenced work almost always chance-matches other works — so it rests
entirely on the decoy null `P = 1 − DecoyBestCDF(alen, dens)`.

**Decoy MASS is ample** (this clarifies the "chance singletons by len {40: 9}" meta line, which is
a *different* quantity): the null CDF is built from `decoy_best` = every decoy with any candidate,
≈**1,821–1,832 points per length**. The `{40: 9}` figure is the count of decoys matching *exactly
one* work — the singleton *rate*, not the CDF denominator. So bin 40 has enough decoy **mass**.

**The problem is null VALIDITY at short alen, not mass.** CHUNK=25 shuffle on a 40-char crop makes
only 2 chunks → 50% of decoys are byte-identical to the crop. Empirically the decoy best-density
distribution and true short matches **overlap heavily**:

| alen bin | decoy best-dens p05 | p25 | p50 | frac < 0.30 |
|---|---|---|---|---|
| 40 | 0.500 | 0.568 | 0.597 | 0.000 |
| 60 | 0.419 | 0.504 | 0.558 | 0.004 |
| 80 | 0.394 | 0.480 | 0.537 | 0.004 |
| 100 | 0.359 | 0.468 | 0.526 | 0.013 |

Chance matches routinely reach density **~0.50** at every length. Meanwhile true len-40 matches
*also* sit at density ~0.51–0.60 (the len-40 isotonic: d≤0.514→P0.866, d≤0.590→P0.740,
d≤0.621→P0.540). Signal and null coincide at short length — the intrinsic reason short fragments
are hard.

**Deployed singleton P is optimistic across the overlap zone:**

| alen | d0.20 | d0.30 | d0.35 | d0.40 | d0.45 | d0.50 | d0.55 |
|---|---|---|---|---|---|---|---|
| 40 | 1.00 | 1.00 | 0.98 | 0.98 | 0.97 | **0.95** | 0.83 |
| 60 | 0.98 | 0.98 | 0.98 | 0.95 | 0.90 | **0.77** | 0.55 |
| 80 | 1.00 | 0.98 | 0.98 | 0.93 | 0.83 | 0.67 | 0.42 |
| 100 | 0.98 | 0.98 | 0.95 | 0.88 | 0.80 | 0.62 | 0.37 |

A singleton at density 0.50 and alen 40 is assigned **P = 0.95**, yet ~5% of pure-chance shuffles
already reach that density and true len-40 matches at 0.50 are only ~80% correct even *with*
competitors present. The theorized inflation (+0.05 to +0.15 from the pilot) is real in the
**0.30–0.55 density band**, where the null is thin/contaminated and there are zero labeled anchors.
(Caveat, stated honestly: the 10 labeled len-40 singletons sat at *high* density ~0.6 and were 80%
correct while the model gave mean P 0.48 — i.e. at the *high*-density end the null is if anything
*conservative*. The band is simply unvalidated in both directions; the actionable risk for a
discovery deck is the false-high P at low-moderate density, so cap the upside.)

---

## §5 Bucketing parity — Codex BLOCKER-2 fix VERIFIED HELD

Line-by-line comparison of `cal1_calibration.py::p_deploy` and `mapv2_track1_run.py::PModel.p`:

| element | calibration `p_deploy` | runner `PModel.p` | match |
|---|---|---|---|
| length feature | `alen = row.get('alen') or row['len']` | `alen` argument (from `best` hull) | ✓ **alen, not len** |
| bin selection | `min(bins, key=lambda x: abs(x-alen))` | `min(bins, key=lambda L: abs(L-alen))` | ✓ identical |
| band edges | singleton/`≤0`→not_best/`≥.10`/`≥.03`/else | same thresholds (`margin_band`) | ✓ identical |
| knot lookup | `for max_d,p,_n: if dens≤max_d: return p; else last` | `_knot_lookup` — identical | ✓ |
| singleton null | `for d0,f: if dens≤d0: frac=f; break; else last; 1−frac` | identical | ✓ |
| fallback | pooled per-length isotonic by alen | pooled per-length isotonic by alen | ✓ |

Holdout reliability inside the fit (`margin_reliability`) is computed **through `p_deploy`**, so
validation and deployment key on the same feature. The BLOCKER-2 bug ("bucket the holdout by crop
len while deploying by alen") **is fixed and holds.** My replication reproducing the stored
`reliability_margin` bucket-for-bucket is independent confirmation.

**One residual parity gap (Finding 5, MEDIUM — not a bucketing bug):** the *margin/competitor*
feeding the band is computed differently. Calibration treats **all** distinct candidate works of a
crop as competitors (a crop is one contiguous span, so they all overlap). The runner counts only
works whose best hull **spatially overlaps** this work's best hull by ≥ `OVERLAP_FRAC=0.5`
(`assign_page`). On single-span calibration crops these coincide; on **multi-work pages** (two
works on disjoint halves) they diverge — the runner will call both "singleton"/high-margin where
calibration would have seen competitors. The band distribution the runner emits is therefore not
the one the model was calibrated on for multi-work pages. Untested; worth a targeted check on a
sample of multi-work tier-B pages.

---

## §6 Rarity gate — exact re-derivation procedure

`build_smoke_preview2.py` line 98 still hardcodes `RARITY_MAX = 60`, applied as
`len(a_ms[wid]) > RARITY_MAX → drop`, where `a_ms[wid]` = **distinct tier-A `sys_id`s
(manuscript witnesses)** for the work. It must become a quantile of the full-corpus tier-A
witness distribution.

**Procedure (run when the writer releases `fullcorpus_v2.db`):**

```sql
-- 1. per-work tier-A witness count (matches a_ms semantics exactly)
CREATE TEMP TABLE work_wit AS
  SELECT work_id, COUNT(DISTINCT sys_id) AS n_wit
  FROM track1_matches            -- tier-A only; candidates table is tier B
  GROUP BY work_id;

-- 2. the ordered distribution over WORKS (not rows)
SELECT n_wit FROM work_wit ORDER BY n_wit;   -- feed to numpy.percentile
```
```python
import numpy as np
n = np.array([r[0] for r in con.execute("SELECT n_wit FROM work_wit")])
RARITY_MAX = int(round(np.percentile(n, 92)))   # primary recommendation
```

**Which quantile and why — 92nd percentile.** I calibrated the pilot value against the (unlocked)
`liturgy.db` tier-A distribution (2,563 works, 81,679 rows). There, `RARITY_MAX=60` keeps
**92.7%** of works and excludes the top **7.3%** — i.e. **60 ≈ the 92nd–93rd percentile** of the
liturgy witness distribution (which is p50=2, p90=38, p95=83, max=4,512). A count does not
transfer across subcorpora of different size/composition; **a percentile does**. Setting
`RARITY_MAX` to the **p92 of the full corpus** preserves the exact selectivity Hillel implicitly
accepted on liturgy. The gate's real targets — Bible, "ספר אהבה"/siddur (2,321 wit) and other
canonical agglomerates — sit beyond p99, so any threshold in **p90–p98** catches them; the choice
only affects genuine mid-frequency works. Use **p90** for a tighter discovery deck, **p95** to
retain more mid-frequency works.

**Fallback default (until the SQL can be run):** use **`RARITY_MAX = 100`**, not 60. The full
corpus (~62K works, 1.34M pairs) has a heavier canonical tail than the liturgy subcorpus, so the
liturgy-tuned 60 will over-exclude real mid-frequency works; 100 ≈ liturgy p96–97 and still
removes only the agglomerated tail. Mark it provisional and replace with the computed p92 as soon
as the DB is free.

---

## §7 Other statistical red flags

- **Isotonic fallback (pooled model):** all 4 non-singleton bands are fitted for all 7 lengths
  (`margin_fit_counts` all ≥ 20), so `p_deploy`'s pooled fallback is **never taken** at deploy
  (only the separately-handled singleton path bypasses the bands). The pooled `model{}` block is
  effectively dead weight in the deployed artifact — harmless, but don't rely on it as a safety net.
- **`MARGIN_FIT_FLOOR = 20` counts RAW rows, not effective works.** Cells clear 20 raw rows but the
  fit is per-work-weighted, so effective n is far smaller (e.g. `m_0_003`/alen-300 = 25 rows from
  perhaps <10 works → an isotonic on ~10 effective points). This is why the small-margin curves are
  jumpy and non-monotone in length (§2). Raise the floor to an *effective* (weighted / distinct-work)
  count of ~30, or **pool lengths** within the small-margin bands.
- **Severe class imbalance:** overall base rate 1.49% correct; `not_best` is 98.5% of all rows at
  0.10% correct. AUC is robust to this, but the isotonic in the minority bands is driven by a
  handful of positive works — compounding the small-effective-n problem.
- **Tiny-n interior reliability buckets:** the 0.3–0.5 P buckets have n=20 and n=32 raw rows; treat
  their empirical estimates as indicative only.
- **Wide-recall ceiling at short length:** even at the 0.75 verification cutoff, only **97.2%** of
  true works form any candidate at crop-len 40 (99.1% at 60). Short fragments are intrinsically
  lossy before P is even applied.

---

## §8 Concrete recommended parameters for the deck builder

### (a) Singleton P cap
Principled (uses the decoy percentiles this model already stores):
```
# decoy best-density percentiles per alen bin (from this audit):
DECOY_P50 = {40:0.597, 60:0.558, 80:0.537, 100:0.526, 150:0.519, 200:0.517, 300:0.522}
DECOY_P10 = {40:0.500, 60:0.430, 80:0.410, 100:0.380, 150:0.400, 200:0.400, 300:0.400}  # ~p05–p10

def singleton_P_capped(alen, dens, p_raw):
    L = nearest_bin(alen)
    if dens >= DECOY_P50[L]:            # at/above the median chance match → not discovery-grade
        return 0.0
    cap = 0.80 if alen < 80 else 0.90   # shorter singletons are the least trustworthy
    if dens > DECOY_P10[L]:             # allow >0.80 only below the chance floor
        cap = min(cap, 0.80)
    return min(p_raw, cap)
```
Simple version if the deck can't reach the null internals:
- **Never assign a singleton P > 0.80 when alen < 80.**
- **Hard-floor any singleton with density ≥ 0.50 to "weak" (P treated as < 0.5)** regardless of alen.

### (b) Display cap / relabel
- **Below alen 80, do NOT show a numeric P for singleton or `m_0_003` candidates** — show the band
  label + `(alen a, density d)` and route to manual review. (11 labeled singletons; `m_0_003` gaps
  up to +0.70.)
- **In the 0.2–0.8 P range for `m_003_010`/`m_0_003`, present a coarse grade, not the number:**
  `≥0.90 = "strong"`, `0.50–0.90 = "review"`, `<0.50 = "weak"`. The ranking is trustworthy (AUC
  0.99); the decimal is not.

### (c) Rarity gate (§6)
- Replace `RARITY_MAX = 60` with `RARITY_MAX = round(np.percentile(work_wit_fullcorpus, 92))`.
- Fallback until the DB is free: **`RARITY_MAX = 100`** (provisional).

### (d) Upstream calibration fix (next re-fit — highest leverage)
- **Fit the isotonic by `alen` bin, not crop-len** (`cal1_calibration.py` lines 388, 446), so fit /
  validate / deploy all share one feature. Consider **pooling lengths** within `m_003_010` and
  `m_0_003` (too few effective works to stratify by length).
- Correct the report language: it is not "isotonic per crop-length"; at deploy it is an alen-keyed
  lookup dominated by the alen-60/80 curves.

### (e) Deployment posture (no need to stop the running job)
P never enters the tier-A / census path (`track1_matches`), and tier-B rows are P-stamped for human
review with the `P_MIN_STORE=0.05` "no silent caps" floor intact. The current run is safe to
complete; the caps above apply in the **deck builder** that reads `track1_candidates`, not in the
runner.
