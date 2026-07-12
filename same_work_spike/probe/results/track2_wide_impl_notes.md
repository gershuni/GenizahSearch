# track2_wide_run.py — implementation notes

Deliverable for MAPV2-6 (Track-2 WIDE tier). Script:
`scripts/track2_wide_run.py`. Spec: `_tmp/track2_wide_spec.md` (revised after
the Codex design critique `results/overnight/codex_track2_wide_design_r1.log`).
A Codex CODE review (round 1, `results/overnight/codex_track2_wide_code_r1.log`,
verdict REVISE, 7 findings) was applied in full — see "Code-review round 1"
below. This note records the load-bearing decisions, the normalization
derivation, and every deviation from the spec with its reason.

## Normalization factor (the headline number)

Opportunity-normalized expected null real-real count per bucket:

```
expected_null_real_real[bucket] = decoy_real_count[bucket] * n_real / (2 * n_dec_cohort)
```

Derivation (also in the module docstring and the run report):

- Decoys travel the SAME engine (same index, same DF_DROP=100 gram band, same
  two-hit candidate generation, same MARGIN-padded verify), so the null is
  exchangeable with the tested real-real pairs BY CONSTRUCTION. Only the number
  of *pairing opportunities* differs.
- Unordered opportunities: real-real `N_rr = C(n_real,2) = n_real*(n_real-1)/2`;
  decoy-real `N_dr = n_dec * n_real` (decoys are a disjoint set, every
  {decoy,real} combination is exactly one unordered pair — no symmetry factor).
- Per-opportunity chance rate `r` is equal for decoy-real and unrelated
  real-real (both = two noisy streams, no shared long-range structure). So per
  bucket `observed_decoy_real = r*N_dr` and `expected_null_real_real = r*N_rr`,
  hence `expected = observed_decoy_real * N_rr/N_dr = observed_decoy_real *
  (n_real-1)/(2*n_dec) ~= observed_decoy_real * n_real/(2*n_dec)`.
- **The `/2` is the C(n_real,2) vs n_dec*n_real asymmetry**: a real-real pair
  draws its null opportunity from BOTH real sides (unordered → halved), a
  decoy-real opportunity is already one distinct unordered pair (only the decoy
  side is "spent"). This is exactly the spec's stated intuition — no
  disagreement, no re-derivation needed.

Factor values: FULL `n_real≈667,411`, `n_dec=12,000` → **≈27.8**; smoke
`n_real≈139,694`, `n_dec=600` → **≈116.4**.

Conservative envelope: expected_null is computed per cohort (CHUNK=25 and
CHUNK=12) and the per-bucket MAX is taken. CHUNK=12 breaks more long-range
structure → looser null → more decoy pairs → higher expected_null → higher FDR
estimate, so the envelope is conservative w.r.t. the unsettled chunk size.

## Code-review round 1 (Codex REVISE, all 7 findings applied)

1. **BLOCKER — wide-only pair semantics now ENFORCED.** The verify loop keeps
   a `strict_pairs` set (packed `ia<<20|ib` int keys). On a strict-class
   real-real segment: the key is recorded, any pair-best entry is popped, and
   later wide segments of that pair are suppressed. Already-spilled raw rows
   are purged by SQL join against a persisted `track2_strict_pairs` table
   BEFORE the pair-best dedup (`strict_purged_raw` reported in the funnel).
   `strict_pairs_seen` is now PAIR-level, directly comparable to the chain's
   `accepted_pairs_canonmask` row count.
   **Decoy-real null decision (explicit):** the null stays **SEGMENT-level** —
   every wide-not-strict decoy-real segment is counted, including weaker
   segments of decoy pairs that also have a strict-class segment. Relative to
   the pair-best + strict-excluded real side, segments-per-pair multiplicity
   ≥ 1 can only INFLATE the null → higher expected_null → higher FDR →
   **conservative**. Chosen over pair-level decoy tracking to avoid a second
   multi-million-entry pair dict on the hot path; the bias direction is safe.
2. **BLOCKER — null mass + zero-decoy overconfidence.** `DECOY_N_FULL` raised
   6,000 → **12,000 per cohort** (24,000 total ≈ +3.6% pages). Hierarchical
   fallback in `compute_null_model`: the bucket-level estimate is used AS-IS
   only when the envelope's supporting decoy count ≥ `K_SUPPORT=25` draws
   (rel. sampling error ~20%); below that the bucket gets
   `max(bucket, same-stratum marginal, global)`, where EVERY level is computed
   with a `+1` upper bound on the decoy count (a crude one-sided
   Poisson/Garwood-style bound: 0 draws are treated as ≤ 1). Consequence: **no
   bucket can get local_fdr = 0 / p_local_bucket = 1.0 from absence of
   evidence.** The monotone majorant is applied AFTER the fallback.
3. **HIGH — q_value is now a real BH q-value.** Per-row empirical null-tail
   p-value from the decoy envelope over the "as-or-more-significant" ordering
   (alen' ≥ alen AND dens' ≤ dens, within the row's minlen stratum), +1
   smoothed: `p_tail = (1 + tail_null_mass) / (1 + stratum_null_mass)`
   (computed as a 2-D reverse/forward cumsum over the bucket grid). Global BH
   is then computed EXACTLY on bucket groups (p_tail is bucket-constant, so
   ranks are cumulative row counts in p order; reverse-cummin enforces the
   step-up). The old `expected/observed`-ratio-as-p is gone.
4. **HIGH — resume idempotence.** The deduped pair-best set is written to an
   IMMUTABLE `track2_wide_base` at `spill_done` (satisfies "retain raw or an
   immutable base"; raw is dropped right after to avoid 2x disk). The observed
   per-bucket histogram is persisted to `track2_obs_buckets` (pre-prune).
   Scoring reads ONLY base + persisted tables, writes `track2_wide_new`, and
   swaps it in atomically (`DROP` + `RENAME` inside one transaction). A crash
   anywhere in scoring leaves base intact → `--resume` recomputes cleanly.
   `write_report` reads ONLY persisted tables/meta (never recomputes from
   survivors). Base + strict-pairs tables are dropped only in the final
   `cleanup` phase (after the report).
5. **HIGH — scale hazards.** All tables carry `alen_bin/dens_bin/stratum`
   columns; scoring is a ~200-row `track2_bucket_scores` table joined in SQL —
   NO fetchall of millions of rows, no Python-side per-row UPDATE list. The
   raw→base dedup is backed by an index on `(page_a, page_b, alen DESC,
   dens ASC)`. VACUUM is conditional on `shutil.disk_usage`: runs only if
   free > 1.2× db size + 2 GB, else skipped with a meta note.
6. **MED — guards.** Full mode hard-aborts (before any heavy work) unless ALL
   of: chain step `6-track2` done, `accepted_pairs_canonmask` table exists,
   `track1_matches.shadowed_by`, `track1_candidates.p_same_work`, and
   `pages.provenance` exist. `--resume` refuses if source path/mtime, decoy
   target, engine params, or cohort chunk sizes differ from what the sidecar
   meta recorded (`check_resume_compat`).
7. **LOW — exact cohort sizes.** Largest-remainder (Hamilton) allocation per
   stratum, with shortfall refill from adjacent strata (distance-ordered),
   cohorts disjoint by construction. Smoke now yields exactly 600+600.

## Phase structure + resume

Phase markers in `track2_wide_meta` (`phase_<name>` → timestamp):
`engine_done` → `spill_done` → `null_done` → `scored` → `enriched` → `cleanup`.
`--resume` skips any phase already marked (after `check_resume_compat`
validates the run parameters). The real resume boundary is `spill_done`: once
base + obs histogram + decoy tables are persisted, every later phase recomputes
from persisted state only ("engine finishes, scoring crashes" is recoverable,
per the spec's checkpoint clause). `engine_done` is informational — the
engine's in-RAM candidate arrays can't be checkpointed mid-verify, so a crash
between `engine_done` and `spill_done` re-runs the engine+verify block.

## Monotone smoothing

`monotone_majorant`: conservative one-sided pool-adjacent-violators — the
smallest function ≥ the raw local-FDR that is non-decreasing in dens and
non-increasing in alen, per stratum. Implemented as `np.maximum.accumulate` up
the dens axis then down the alen axis; a single pass is exact because the
alen-cummax is a max over dens-monotone slices, which stays dens-monotone. Only
ever RAISES the FDR estimate — never overstates precision.

## Deviations from the spec (with reasons)

1. **Null binning excludes the strict-density sub-region.** Decoy-real null
   draws are counted only when wide-class AND NOT strict-class, exactly
   matching the stored real rows (wide-AND-not-strict pairs). Reason:
   `observed_real` excludes strict pairs, so an apples-to-apples local FDR
   needs the null to exclude the strict sub-region too. Does not change the
   normalization factor.
2. **Decoy-real null is SEGMENT-level** while the real side is pair-best —
   explicitly conservative (see code-review item 1 above).
3. **`track2_null_buckets` carries an `ENVELOPE` pseudo-cohort row** per bucket
   (decoy_n=-1) with the envelope expected_null + final smoothed local_fdr +
   support, so consumers read the final model without recombining cohort rows.
4. **The spec's "DELETE + VACUUM" is realized as prune-at-insert + atomic
   swap + conditional VACUUM** (same result set: rows with
   `p_local_bucket < 0.05 AND q_value > 0.5` never enter the final table) —
   required by the resume-idempotence fix (base stays immutable) and cheaper
   than DELETE churn.
5. **Enrichment re-loads survivor page text from the source DB** (second
   read-only pass) rather than holding all streams in RAM through scoring.
6. **Source DB opened strictly read-only** (`file:...?mode=ro`) in BOTH modes;
   in FULL mode the chain-state guard is checked BEFORE any connect to
   `fullcorpus_v2.db`.
7. **Smoke keeps 600+600 decoys** (spec's smoke definition); the 12,000/cohort
   applies to the FULL run only. At smoke scale the null is expected to be
   nearly empty — the fallback machinery (not the decoy volume) is what the
   smoke validates.

## Smoke evidence

**Round 1** (pre-code-review build, 2026-07-10 20:29): completed end-to-end in
14.1 min at BelowNormal — 33.7M candidates, 609,667 wide rows, 3 decoy-real
draws, all phases + `--resume` + full-mode guard verified. Log:
`results/overnight/track2_wide_smoke.log`.

**Round 2** (post-code-review build, fresh sidecar, 2026-07-10 21:04):
completed end-to-end in **13.1 min** at BelowNormal, exit 0, all SIX phases
marked (`engine_done` 21:07 → `cleanup` 21:17). Log:
`results/overnight/track2_wide_smoke_r2.log`; sidecar
`data/track2_wide_smoke.db`; report `results/track2_wide_smoke_report.md`;
stats `results/track2_wide_smoke_stats.json`.

- Exactly **600+600 decoys** (largest-remainder allocation; round 1 had 601+601).
- 33,745,608 candidates; verify 149s.
- `strict_pairs_seen` (now PAIR-level) = **731,737** vs chain step-6 735,700 →
  **-0.54% delta** (round 1's segment-tinged count was +2.75% — the pair-level
  fix landed the comparison where it should be).
- Wide-only: 584,850 spilled → 584,850 base pair-best (vs 609,667 in round 1 —
  the strict-pair suppression removed ~25K wide segments of strict pairs).
  Strict purge deleted 0 already-spilled rows (at smoke scale nothing flushed
  mid-run, so `best.pop` caught everything in-dict; the SQL purge is exercised
  and armed for the full run where mid-run flushes occur).
- Null: 3 decoy-real draws again; `buckets with support>=25: 0` → ALL buckets
  ride the hierarchical fallback. **p_local_bucket range 0.0–0.995007; ZERO
  rows at 1.0** (round 1: 589,888 rows at 1.0 from absence of evidence).
- q_value is a real BH q (range 0.0216–1.0 from the tail p-values; only 4
  distinct p_tail values at smoke scale because the 3 draws sit in 1 stratum —
  coarse but honest).
- **Pruning engaged**: 47,476 low-value rows pruned (p_local<0.05 AND q>0.5),
  survivors 537,374 (round 1 pruned 0 — the overconfident 1.0s shielded all).
- Enrichment: {island: 398,069, edge: 78,795, continuation: 47,900,
  ambig: 12,610}. Conditional VACUUM ran (0.4 GB db, 65.5 GB free).
- Post-cleanup: `track2_wide_base`/`track2_strict_pairs`/`track2_wide_raw`
  gone; `track2_obs_buckets` retains the pre-prune 584,850 histogram; a
  `--smoke --resume` after completion passes `check_resume_compat`, skips all
  phases, and regenerates the report purely from persisted tables.
- `py_compile` clean; `ruff check` clean.

## Residual concerns for the FULL run

- Even at 12,000/cohort the per-bucket null will be sparse in the high-alen
  bins; most buckets will ride the stratum/global fallback + majorant. That is
  by design (conservative), but the report's `buckets_with_support` count
  should be checked before trusting bucket-resolution differences.
- The strict-pairs set (~3.4M packed ints at full scale) adds ~200-300 MB RAM
  on top of the engine's peak; fine on the 64 GB box but keep an eye on it if
  the corpus grows.
- `line_agreement` enrichment cost scales with survivor count; if the full run
  keeps >5M survivors, enrichment (not the engine) becomes the long pole.
- VACUUM may be skipped on a tight disk (meta key `vacuum_skipped`); the
  sidecar then carries dead pages until a manual VACUUM.
