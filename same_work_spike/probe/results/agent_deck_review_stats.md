# Stats/methodology audit — tier-B P calibration + deck guards (SEED-029 / FRAG-2)

**Auditor:** stats agent (adversarial pass, numbers + method only; Hebrew content graded by the parallel agent).
**Date:** 2026-07-10 night. **Scope read-only:** `data/p_calibration_final.json`,
`results/p_calibration_final.md`, `data/cal1_rows_final.json`, `data/cal1_rows_pilot-n10/n20.json`,
`data/mapv2_smoke.db` (ro), `scripts/cal1_calibration.py`, `scripts/mapv2_track1_run.py`,
`scripts/build_smoke_preview2.py`, overnight logs. `fullcorpus_v2.db` NOT touched.
All numbers below were re-derived by scratch scripts (holdout split replicated from
`RNG_SEED=20260710`; guard pipeline replicated on a random 1,500-page sample = 16,706 tier-B rows;
~935 Bible alignments run).

## Verdict in one paragraph

The margin-band machinery is solid and its high-P claims survive adversarial noise-injection
(good); **P works as a coarse ranker (holdout AUC 0.99 overall, 0.88 on deck-relevant rows)**
and the deck strata are honest as *coarse bins* on synthetic holdout (P≥0.8 → 97.3% correct,
0.5–0.8 → 85.9%). The three exposure points are: (1) the **singleton band — ~58% of the deck's
top stratum — is validated by nothing** (1 holdout row) and its "P" is a p-value complement,
not a posterior; (2) **BIBLE_ALIGN_MIN=70 sits on a smooth continuum, not a valley** — 28% of
aligned survivors are in the 60–70 near-miss band, including ~30% of the P≥0.8 stratum;
(3) **RARITY_MAX=60 is mid-slope and its denominator is the liturgy smoke** — reused as-is on
the full corpus tomorrow it will silently cut a different (larger) set of works.

---

## Findings, ranked

### BLOCKER-1 (for tomorrow's real deck): RARITY_MAX=60 denominator is subcorpus-bound

The gate counts strict-tier witnesses in **`mapv2_smoke.db::track1_matches` (liturgy subcorpus,
139,694 pages)**. Tomorrow's deck builder runs over fullcorpus_v2 (667,411 pages, ~4.8×);
witness counts are strictly larger there, so the same "60" excludes a much bigger, arbitrary
set of works — a silent recall loss against the recall-first directive.

Measured on the smoke db (2,568 works with ≥1 strict witness):
- No valley anywhere near 60: works per witness-count value run 1–6 smoothly through 40–110
  (41–60: 48 works, 61–80: 47, 81–100: 23). The cut is mid-slope.
- Row impact: RARITY_MAX=60 cuts 251,273 tier-B rows (38.2%); 70 cuts 227,506 (34.6%);
  100 cuts 184,542 (28.1%). True agglomerates live far above (13 works at 301–700 witnesses,
  4 works >700, e.g. ספר אהבה 2,321).
- Edge arbitrariness: פסיקתא רבתי (wit=49, **5,964** tier-B rows) survives; מאור האפלה
  (wit=61, 3,124 rows) dies; תלמוד ירושלמי תענית (58) survives; בבלי ברכות (83) dies.

**Fix (cheap, do before the real deck):** re-derive the gate on fullcorpus_v2 tier A — either a
quantile rule (exclude top-K works by witness count; K≈120 reproduces today's intent) or a rate
(witnesses per 10K corpus pages). Do not carry the literal 60.

### HIGH-1: the singleton band — the deck's dominant band — is empirically unvalidated, and its P is not a probability of correctness

- Deck composition (my 1,500-page guard replication, matching the preview_v3 log): survivors
  = 730/16,706 rows (4.4%); bands: **singleton 440 (60%)**, m_0_003 180, m_003_010 96,
  m_ge_010 14. Of the P≥0.8 survivors (251), **146 (58%) are singletons**.
- Validation coverage: the FINAL holdout contains **1 singleton row** (synthetic crops almost
  never produce singletons — the known FRAG2-PLAN finding); the noise arms contain 5/14. So
  `reliability_margin` says nothing about this band; the 0.9–1.0 bucket's reassuring 0.981 is
  ~96% m_ge_010 rows (n=1,109 of 1,151).
- Semantics: `P = 1 − DecoyBestCDF(len, dens)` is the complement of a chance-null CDF — a
  p-value complement, **not** P(correct | evidence). Without a prior×power term it overstates
  correctness exactly where true matches are rare (the discovery regime). Illustration: at
  len 100, CDF(0.45)=0.19 → claimed P=0.81; if only ~20% of such observed singletons were true,
  actual precision ≈ 0.2/(0.2+0.8·0.19) ≈ 0.57.
- Look-elsewhere (quantified from the decoy rows themselves): the null is indexed by the row's
  **alen** (40–300), but the deployment unit is a whole page — median stream length of pages
  carrying P≥0.8 singletons is **891 letters** (p90 = 1,805). The decoys show chance grows with
  query length then plateaus: CDF(best ≤ 0.45) = 0.017 (L40), 0.086 (L60), 0.157 (L80),
  0.192 (L100), 0.158 (L300). Looking up a short-alen match against its alen bin instead of a
  page-scale bin under-counts chance by ~1.5–2.2× in the P 0.8–0.9 region → **P inflated by
  roughly +0.05–0.15 absolute** there. (Partially offsetting, unquantified: chunk-shuffle decoys
  retain intact 25-letter true-work chunks, which can inflate the null → conservative. Net
  direction unknown.)
- Real-data hint (the only one): Hillel's 9 graded density_fail cards (all correct) get deployed
  singleton-null P of 0.27–0.92 (my recomputation: 0.72, 0.92, 0.80, 0.72, 0.78, 0.47, 0.80,
  0.27, 0.30) — under-confident on that selected set; but that set was selection-biased.

**Fix:** (a) on cards, render singleton-band scores as what they are — a chance-based score
("סיכוי שצירוף מקרים ייראה כך: X%") or cap the displayed P at 0.9 for singletons; (b) the fresh
blinded grading deck MUST be stratified **by band × P**, not P alone — the singleton band is the
one thing it needs to measure; (c) medium-term, replace 1−CDF with an empirical-Bayes localFDR
once the blinded grades exist.

### HIGH-2: BIBLE_ALIGN_MIN=70 — the "72 vs 61" separation does not exist at scale

The threshold was set from ~18 points (verse cards 72–97, non-verse 0–61). On **935** surviving
slices I aligned against the same Bible stream, the score distribution is a smooth continuum:

| <40 | 40–55 | 55–60 | 60–65 | 65–70 | 70–75 | 75–85 | 85+ |
|---|---|---|---|---|---|---|---|
| 0 | 314 | 152 | 133 | 131 | 79 | 96 | 30 |

- **264 rows (28.2% of aligned) sit in the 60–70 near-miss band**; 76 of them carry P≥0.8 —
  i.e. **~30% of the deck's top stratum** (extrapolated ≈ 2,400–3,000 (page,work) rows at smoke
  scale) is within 10 points of the verse guard.
- Threshold sensitivity: 65 demotes 35.9% of aligned rows, 70 demotes 21.9%, 75 demotes 13.5%.
  There is no natural break; every choice trades verse leak vs evidence loss blindly.

**Fix:** before the real deck, have ~30 rows in the 55–75 band ground-truthed (content agent /
Hillel — cheap); until then, don't hard-demote/hard-keep across 70: mark the 60–75 band with a
"חשד לפסוק" chip on the card (keep it in the deck, visible), and hard-demote only ≥75.

### MEDIUM-1: persistent 0.7–0.8 vs 0.8–0.9 reliability inversion (~−0.15)

Across all three arms the 0.8–0.9 bucket underperforms its label while 0.7–0.8 overperforms:
clean holdout 0.963 vs **0.694** (n=115/46); pilot-n10 0.977 vs 0.781; pilot-n20 0.984 vs 0.852.
Localized to the m_003_010 band: the len-60 curve's big block (p=0.80, n=433) is overconfident
(bucket emp 0.67–0.85), while the len-100/150 blocks (p=0.75–0.77) are underconfident (emp
0.96–0.98). **Fix:** merge the deck's 0.7/0.8 boundary (one stratum 0.7–0.9), or refit
m_003_010 pooled across lengths; do not present 0.85 as better than 0.75 within this band.

### MEDIUM-2: mid-range absolute P is noise; thin buckets

Holdout buckets 0.2–0.5 hold n=1–92 rows with empirical 0.0–0.22 (0.2–0.3: pred 0.25 → **emp
0.009**, n=92). 0.5–0.7: emp 0.43–0.49 on n=51/23. As a ranking this is survivable (the deck's
strata skip 0.2–0.3 entirely — my survivor recount has ZERO rows at P 0.2–0.3, so the "ספק
0.2–0.5" stratum is really 0.4–0.5); as absolute probability the mid-range is not defensible.
Curiosity in the same table: P<0.02 rows are 73.5% correct (n=34) — single-row p=0 isotonic tail
blocks misfire (under-confidence). Volume harmless; don't quote mid-range P decimals.

### MEDIUM-3: "P 1.00" on cards comes from n=1 isotonic blocks and a 1/1832-resolution null

m_ge_010 curves are chains of n=1 blocks at p=1.0 (len100: 1,173/1,174 training rows sit in
p≥0.999 blocks; len300: all 1,513); singleton max P = 1 − 1/1832 = 0.99945 → renders as
"P 1.00". The deck sorts descending, so the FIRST cards a reviewer sees claim certainty the
data cannot support (rule of three at n=1,832 alone caps claims at ~0.998). **Fix:** display
cap at 0.99 (or a Wilson lower bound per block); one line in the deck builder.

### MEDIUM-4: the report's stress-test table doesn't test the deployed model

`load_graded_external` scores Hillel's cards through the **pooled** curve (`p_lookup`), not the
deployment-composed `p_deploy`. The table therefore shows density_fail cards at P 0.000–0.339 —
alarming and wrong twice over: the deployed singleton null actually gives them 0.27–0.92
(better), and the one deployed path with zero other validation (singleton) is exactly the path
the table never exercises. **Fix:** rerun the stress table through `p_deploy` before showing
the report to Codex.

### MEDIUM-5: promised per-category divergence check is missing; Bavli undersampled

FRAG2-PLAN step 1.4 (Codex HIGH fold-in) promised per-cat empirical precision at the pooled
operating points. Neither `cal1_calibration.py` nor either report contains any per-cat
analysis. Sampling shortfalls compound it: Bavli got 17/80 target pages, Tosefta 3/4. The deck
mixes cats. ~20-line addition to the reliability function.

### LOW-1: smoke db `p_same_work` is pilot-scored — documented, but the stored histograms mislead

Confirmed from `track1_smoke.log` (`meta=pilot ... margin bands=[]`): the smoke run stamped P
from the pilot **pooled** curves; the FINAL json postdates it by 17 min. Under pilot scoring
209K not_best rows carry P≥0.8 (e.g. alen 66–87, dens 0.38–0.52 → stored 0.82–0.93, vs ≤0.63
under the FINAL not_best curves). Anyone reading `track1_candidates` or the smoke report's "P
histogram (stored tier B)" line must rescore first. The overnight fullcorpus run loads the
FINAL model (verified: `mapv2-2-track1-v2.log` line 2, `meta=final`), so this dies with the
smoke db.

### LOW-2: three competitor/margin definitions in flight

(1) calibration: all distinct-work candidates of the crop at cutoff 0.75; (2) runner: best-hull
vs best-hull overlap ≥ 0.5×shorter at cutoff 0.55; (3) preview: competitor span-UNION vs best
span, ≥ 0.5×**blen** (best-span denominator — a short competitor fully inside the best span
counts for the runner but not for the preview). Net deployment direction: fewer competitors →
more singletons and wider margins → higher-P bands than the calibration population at equal
evidence (optimistic, feeds HIGH-1). **Fix:** one shared `competitors()` function imported by
the real deck builder and any refit; document the calibration↔deployment residual.

### LOW-3: artifact bloat

The model json (46MB) and md (509K lines) keep every PAVA block (pooled len300: 203,443 knots,
mostly n=1). Lookup-correct but unreviewable; merge blocks below a min-n for the report copy.

---

## What is methodologically solid (say it to Codex first)

1. **Design integrity is real, not aspirational** — verified in code: crop-inside-verified-span
   labels, no top-K censoring, work-granular 80/20 holdout (75 held-out works), per-work equal
   weighting, decoys through the identical query path, PILOT/FINAL separation honored, FINAL
   run on the frozen v2 state with version-group truth. Storage floor drops are counted
   (no silent caps). Runner↔calibration lookup parity is code-true (`p_deploy` mirrors
   `PModel.p` including the alen bucketing; alen>300 affects only 643/657,205 rows).
2. **P is a good ranker.** Holdout AUC 0.9902 (all rows); 0.8756 on the deck-relevant subset
   (not_best excluded; caveat: 1,332/1,408 positive). Deck strata as coarse bins: P≥0.8 →
   0.973 (n=1,196), 0.5–0.8 → 0.859 (n=163).
3. **The optimism-bias caveat is now quantified — and the margin bands pass.** Scoring the
   10%/20% noise-injected pilot crops with the clean-fit FINAL model: 0.9–1.0 bucket → 0.988 /
   0.989; 0.7–0.8 → 0.977 / 0.984; 0.8–0.9 → 0.781 / 0.852 (same inversion as clean, not
   worse). m_ge_010 is rock solid everywhere: 0.99–1.00 at n=1.1K/9.9K/9.6K. HTR-grade noise
   does not break the band curves' top end. (Does NOT cover the singleton band — see HIGH-1.)
4. **Singleton null resolution is adequate** (its *validity* is HIGH-1, its *precision* is
   fine): 1,832 decoys per length bin; ~100% produced a candidate at cutoff 0.75 so the CDF
   denominator is full; 60-knot downsampling gives P steps ≈0.017 and rounds conservatively
   (next-knot lookup overestimates CDF); binomial SE at the P=0.8 point ≈ ±0.01.
5. **The guards do not flatten the spectrum.** Survivors 4.4% of tier-B rows (730/16,706
   sample; preview log agrees: ~3.9% at 328K rows processed — rarity ~36%, bible-cover ~35%,
   recomputed not_best ~23%, verse guard ~1.6%). Survivor P histogram keeps two lobes:
   0.5–0.6 bulge (287) and 0.9 peak (163), dip at 0.7 — matched by the completed v2 preview at
   (ms,work) level (0.5: 5,623 / 0.6: 6,100 / 0.7: 2,791 / 0.8: 4,006 / 0.9: 7,697).
6. **No single-work flooding:** 251 P≥0.8 survivor rows spread over 191 distinct works
   (max 5 per work before the deck's PER_WORK_CAP=3 even applies).

## Concrete threshold recommendations

| knob | now | recommendation |
|---|---|---|
| RARITY_MAX | 60 (smoke counts) | re-derive on fullcorpus_v2 tier A; quantile/top-K rule, not a literal count |
| BIBLE_ALIGN_MIN | 70 hard demote | 75 hard demote + "verse-suspect" chip for 60–75; ground-truth 30 cards in 55–75 first |
| singleton P display | raw 1−CDF, up to "1.00" | cap 0.9 (or relabel as chance-score); blinded deck stratified band×P |
| P display cap (all bands) | up to 1.00 | 0.99 cap / Wilson floor |
| deck strata | 0.8 / 0.5 / 0.2 cuts | merge 0.7–0.9 into one stratum OR relabel 0.8–0.9 (empirical ~0.7–0.85, MEDIUM-1); "0.2–0.5" is de facto 0.4–0.5 |
| stress-test table | pooled lookup | rerun through `p_deploy` before Codex sees it |

*Scratch audit scripts lived in the session scratchpad (read-only vs all data; sqlite opened
`mode=ro`; fullcorpus_v2.db untouched).*
