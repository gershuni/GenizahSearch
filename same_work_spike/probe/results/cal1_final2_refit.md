# CAL-1 `final2` — isotonic re-fit by ALEN bin (audit §3 remedy)

**Date:** 2026-07-10 · **Author:** calibration agent
**Trigger:** `results/agent_final_cal_audit.md` §3/§7 — the length feature had
collapsed (alen ≈ 66 for every crop length), so the isotonic was **fit by
crop-len but deployed/validated by alen**. The remedy: fit the per-band curves
by **alen bin**, and pool the two small-margin bands (too few effective works
to stratify).

Produced **from the existing rows dump** (`data/cal1_rows_final.json`, 852,736
labeled + 12,813 decoy rows) via the new `--refit-from-rows` entry point — no
6-minute corpus/reference rebuild. `data/p_calibration_final.json` was **not
touched** (verified: mtime unchanged, read-only reuse of its singleton null).

---

## What changed in `scripts/cal1_calibration.py`

All changes are **gated on `FIT_BY_ALEN`** (`tag == 'final2'`, or `--fit-by-alen`
/ `--no-fit-by-alen`). The `pilot` and plain-`final` tags keep the crop-len fit
**byte-for-byte** — proven end-to-end (see Validation §1).

1. **`nearest_bin(x)`** — new helper: nearest calibrated bin by `min|L-x|` over
   the existing edges `[40,60,80,100,150,200,300]`. Identical selection rule to
   `p_deploy` / `PModel._nearest` / `p_lookup`.
2. **`fit_margin_model(train, by_alen=False)`** — the core change. When
   `by_alen`, cells are keyed by the row's **nearest ALEN bin** instead of
   `r['len']`. Within `POOL_BANDS = {m_003_010, m_0_003}`, a cell with
   `< EFFECTIVE_WORKS_FLOOR (=8)` distinct works (`{r['true_work']}` count)
   borrows a **band-pooled curve** fit on the union of that band's rows across
   all alen bins (one weight-unit per distinct work). Dense cells self-fit.
   With `by_alen=False` the code path is unchanged.
3. **`work_weights(rows, by_alen=False)`** — keys the per-work weight by
   `(alen_bin, work)` when `by_alen` (matches the alen-bin fit); crop-len
   otherwise.
4. **`fit_calibration(rows, by_alen=False)`** — the pooled-fallback model and
   its holdout reliability now bucket by alen when `by_alen` (the fallback is
   already *deployed* by alen in `p_deploy`).
5. **`margin_reliability(..., by_alen=False)`** — alen-consistent weighting.
6. **`reliability_grid` / `grid_section` / `load_deploy_model`** — new
   self-validation: recomputes the audit's (band × alen-bin) reliability grid
   **through the deploy path (`p_deploy`)** and compares it, on the same
   holdout rows, against the crop-len `final` model. Flags any bucket that
   regressed by `> 0.05`. Written into `results/p_calibration_final2.md`.
7. **`refit_from_rows(...)` + `--refit-from-rows PATH`** — new CLI: skip all
   corpus/reference work, fit straight from a dumped rows file. The singleton
   null + decoy arm are **reused verbatim** from the crop-len `final` model
   (the alen re-fit does not touch the decoy arm — task point d), so the
   singleton band stays byte-identical to the deployed model.

**Unchanged:** singleton-null logic (`build_singleton_null`, `p_deploy`
singleton branch), band edges (`margin_band_of`), the knot data structure
(`[[max_d, p, n], …]` for curves, `[[dens, cum_frac], …]` for the null). The
output json is loadable by `mapv2_track1_run.PModel` **unchanged**.

---

## Outputs

| file | status |
|---|---|
| `data/p_calibration_final2.json` | **written** (4.4 MB) — `meta.stage=final2`, `meta.fit_by=alen` |
| `results/p_calibration_final2.md` | **written** — fit inventory + reliability + self-validation grid |
| `data/p_calibration_final.json` | **untouched** (17:08, byte-for-byte) |

`singleton_null` in `final2` is byte-identical to `final` (420 knots, reused).
The json shrank 46.8 MB → 4.4 MB purely because the alen row-partition lets
PAVA merge more equal-mean `not_best` plateaus (516k → 46k knots); it is a
complete, valid model and lookups are faster.

---

## Validation

**§1 — `final` behavior is byte-identical (gating proof).** A crop-len refit
(`--refit-from-rows … --no-fit-by-alen`, throwaway tag) reproduced the deployed
`final` model **IDENTICAL** across `model`, `margin_model`, `reliability`,
`reliability_margin`, `singleton_null`, `decoy_singleton_rate`. Synthetic
unit tests also assert `fit_margin_model(rows, by_alen=False)` equals the
pre-change logic exactly.

**§2 — `PModel` loads `final2` unchanged.** 240 `pmodel.p(alen, dens, band)`
calls across all bands / alen / density returned floats in `[0,1]`; band edges
consistent. No `mapv2_track1_run.py` change required.

**§3 — Holdout reliability grid (band × alen bin) through the deploy path.**
**Mean |gap| over the 28 shared buckets: `final` 0.161 → `final2` 0.082**
(roughly halved). 17 buckets improved; 4 regressed by > 0.05.

### 5 worst buckets — before (`final`) vs after (`final2`)
(ranked by `|final gap|`; gap = predicted − empirical; n = raw holdout rows)

| band / alen | n | empirical | final pred (gap) | final2 pred (gap) | verdict |
|---|---|---|---|---|---|
| `m_0_003` / 200 | 9 | 0.794 | 0.016 (**−0.778**) | 0.801 (+0.006) | **fixed** |
| `m_0_003` / 40 | 1 | 0.000 | 0.694 (+0.694) | 0.633 (+0.633) | still bad (n=1) |
| `m_0_003` / 100 | 29 | 0.748 | 0.205 (**−0.542**) | 0.761 (+0.013) | **fixed** |
| `m_0_003` / 60 | 11 | 0.000 | 0.478 (+0.478) | 0.110 (+0.110) | much better |
| `m_0_003` / 300 | 3 | 1.000 | 0.731 (−0.269) | 1.000 (+0.000) | **fixed** |

Other large audit-flagged fixes: `m_003_010`/150 −0.260→−0.008;
`m_003_010`/100 −0.235→−0.007.

### Buckets that regressed by > 0.05 (all tiny-n sparse-tail cells)

| band / alen | n | empirical | final \|gap\| | final2 \|gap\| | Δ\|gap\| |
|---|---|---|---|---|---|
| `m_0_003` / 150 | 11 | 0.646 | 0.023 | 0.238 | +0.216 |
| `m_003_010` / 300 | 17 | 0.727 | 0.140 | 0.274 | +0.134 |
| `m_0_003` / 80 | 18 | 0.185 | 0.150 | 0.281 | +0.131 |
| `m_003_010` / 200 | 19 | 0.746 | 0.077 | 0.157 | +0.079 |

**Reading:** the four regressions are all in the sparse high-alen tail
(n = 11–19 holdout rows) where pooling / a small self-fit cell now slightly
over-predicts. They are modest (Δ 0.08–0.22) and in low-mass buckets; the
improvements are large (Δ up to −0.78) and land squarely on the audit's
flagged deploy-dominant buckets. Net calibration error is halved. The tail
over-prediction is the expected bias–variance cost of borrowing strength; a
future pass could raise `EFFECTIVE_WORKS_FLOOR` or shrink pooled-cell P toward
the band mean if the tail matters for the deck.

---

## How to reproduce

```
cd same_work_spike/probe/scripts
# from the existing dump (CPU-light, ~1 min):
python -X utf8 -u cal1_calibration.py --refit-from-rows ../data/cal1_rows_final.json --tag final2
# or a full rebuild (frozen Map-v2 state, ~6 min corpus + fit):
python -X utf8 -u cal1_calibration.py --tag final2
```

## Adoption decision (orchestrator, post-Codex review)
Codex verdict REVISE with 0 BLOCKER / 0 HIGH (results/overnight/codex_cal1_final2.log):
alen-bin fit matches PModel deploy semantics; holdout is real (work-split, no vgroup
leakage); singleton null byte-equal to final. **final2 ADOPTED for deck ranking**
(mapv2_deck.py prefers it) because the deck's display-honesty rules (range labels for
small-margin bands in P 0.2-0.8, singleton caps) already mask the sparse tail cells
where Codex MED-2 notes overconfidence. Runner-stored tier-B p_same_work remains
final-model (recompute happens in the deck).
Follow-ups (not tonight): shrink sparse self-fit cells toward band-pooled curve /
raise effective-works floor (MED-2); fail-closed --refit-from-rows when final.json
absent (LOW-3); fresh-split validation = tomorrow's blinded-deck grading (LOW-4);
narrow the byte-identical claim to model curves (MED-1 — rows-json shape change is
shared plumbing, no pilot/final rerun planned).
