# MAPV2-6 implementation spec — track2_wide_run.py (REVISED per Codex design critique R1)

Codex critique: results/overnight/codex_track2_wide_design_r1.log (REVISE — 2 BLOCKER,
4 HIGH, 2 MED + 3 answered questions). This spec incorporates every finding. The original
brief (context): _tmp/codex-track2-wide-brief.md.

## Single-pass exchangeable design (kills BLOCKER 1)
ONE script, ONE full-corpus engine pass with decoy pages injected inline. The null and
the wide tier come from the SAME index, SAME DF statistics, SAME machinery — exchangeable
by construction. No separate calibration run.

### Inputs / guards (Codex #8)
- data/fullcorpus_v2.db must have: track1_matches WITH shadowed_by column,
  accepted_pairs_canonmask (chain step 6 done), pages with provenance column.
- data/mapv2_chain_state.json must list step "6-track2" (verify the exact step key from
  scripts/mapv2_overnight.py) in done — else sys.exit(1).
- Record in the sidecar meta table: source db path+mtime, masks logic version,
  engine params, decoy cohort sizes, chunk sizes, git-describe if cheap.

### Decoy construction (Codex Q1, Q2, #4)
- Decoy source pool = real pages that are maximally UNLIKELY to have true copies:
  no row in accepted_pairs_canonmask (either side), no row in track1_matches (live OR
  shadowed), no track1_candidates row with p_same_work >= 0.5. Build the pool with SQL.
- Stratify pool by stream length quartile strata (<=150 / 150-300 / 300-600 / >600,
  measured on the MASKED-REDUCED stream, see below). Sample 6,000 pages for the
  CHUNK=25 cohort + 6,000 DIFFERENT pages for the CHUNK=12 cohort (sensitivity
  envelope), proportional to the real length distribution.
- Decoy stream: take the source page's normalized stream, REMOVE its track1 canonical
  masked spans first (concatenate unmasked segments — mirrors what the engine
  effectively indexes for real pages), then chunk-shuffle with the cohort's CHUNK using
  a seeded rng (seed derived from page_id hash, reproducible).
- Decoy keeps the SOURCE page's sys_id (the engine's same-sys drop then kills
  decoy-vs-its-own-manuscript pairs = the trivial leak), gets page_id
  'D25_<orig>' / 'D12_<orig>', and carries its stratum + cohort in side arrays.

### Engine pass
- Same params as scripts/rehearsal_run.py strict: K=5, BAND=20, DF_DROP=100,
  MIN_ANCHORS=2, maskcanon masks on REAL pages (decoys pre-stripped instead),
  engine_np.build_candidates over real+decoy streams together, spill_dir as usual.

### Verify loop (Codex #5, #6, #7)
For each candidate pair, compute the same MARGIN-padded window and Levenshtein with
score_cutoff = 0.45*alen+1 (identical to strict verify). Then:
- strict-class = density <= accept_density(alen) and span >= MIN_SPAN 25 (the strict
  boundary). wide-class = alen >= 35 and density <= 0.45.
- pair involves a decoy page: increment decoy counters
  [cohort][alen_bin][dens_bin][minlen_stratum] (alen bins [35,50,70,100,150,250,400+],
  dens bins 0.05 steps to 0.45, minlen stratum of the smaller REAL side — for
  decoy-real pairs use the real side's stratum... no: stratum of min(len) side as the
  null must condition on the SMALLER page). Keep also a reservoir sample (<=2,000 rows
  per cohort) of decoy pair records for diagnostics. Decoy-vs-decoy pairs: discard.
- real-real pair: if strict-class -> count it (n_strict_seen) but DO NOT store (strict
  lives in the chain's accepted_pairs_canonmask; sidecar is wide-ONLY — pair semantics
  documented in the meta table) (Codex #7). If wide-class and not strict-class ->
  keep pair-best record (by (aligned_len, -density) like strict) in a dict, but SPILL
  to SQLite in batches: when the dict exceeds 2,000,000 entries, flush ALL current
  entries to sidecar table and clear (dedup happens post-hoc in SQL: keep best per
  pair). NO truncation, NO cap-bias (Codex #6). Sidecar = data/track2_wide.db,
  table track2_wide_raw(page_a, page_b, sys_a, sys_b, a0,a1,b0,b1, n_anchors,
  alen INT, dens REAL, minlen INT, mask_ov_a REAL, mask_ov_b REAL).
  mask_ov_x = fraction of the verified window [x0,x1) overlapping that page's
  canonical track1 spans (interval overlap vs the masks dict) — verification runs on
  UNMASKED streams so canonical text can re-enter through the window; the flag lets
  consumers drop rows whose evidence is mostly masked content (Codex #5).
- Volume note: expect ~10-30M wide rows spilled; INSERTs in executemany batches of
  200K with one transaction per batch; final SQL dedup into track2_wide(pair-best)
  via ROW_NUMBER() OVER (PARTITION BY page_a,page_b ORDER BY alen DESC, dens ASC).

### Null + scoring (Codex BLOCKER 2, #3, Q3)
- Opportunity normalization: each decoy page has the same pairing opportunity as a real
  page in the same index. Expected null real-real count per bucket =
  decoy_real_count[bucket] * (n_real_pages / n_decoy_pages_cohort) / 2
  (the /2 because real-real pairs draw null opportunity from both sides while
  decoy-real pairs only from the decoy side — document this derivation in the report;
  if you disagree with the factor, derive it properly and write the derivation).
- local_fdr[bucket] = min(1, expected_null / observed_real_wide). Use the CONSERVATIVE
  envelope: null = max over the two chunk cohorts per bucket. Smooth: enforce
  monotonicity (fdr non-decreasing in dens within (alen,stratum); non-increasing in
  alen within (dens,stratum)) via pool-adjacent-violators in the safe direction.
- p_local = 1 - local_fdr. THIS IS BUCKET-LEVEL PRECISION, not a pair probability —
  name columns p_local_bucket / q_value and put a comment in the schema (Codex #3).
- Global BH q-value over ALL real wide rows: per-row null tail probability from the
  envelope null, then Benjamini-Hochberg globally (not per bucket) (Codex Q3). Store
  q_value on each row.
- After stamping: DELETE rows with p_local_bucket < 0.05 AND q_value > 0.5; VACUUM.
- Flank/dup enrichment (flank_dist logic + dup_shelf/dup_lines from
  scripts/rehearsal_run.py / scripts/stage0.py) computed ONLY for surviving rows,
  writing flank_dist, flank_class, dup_shelf, dup_lines columns.

### Outputs
- data/track2_wide.db: track2_wide (final), track2_wide_meta, null tables
  (track2_null_buckets: cohort, alen_bin, dens_bin, stratum, decoy_n, expected_null,
  observed_real, local_fdr).
- results/track2_wide_report.md: cohort sizes, null bucket tables, volume funnel,
  strict-seen count vs chain step-6 count (should be close — small DF perturbation
  from decoys is expected and must be REPORTED as a delta %), the normalization
  derivation, and reliability caveats.
- results/track2_wide_stats.json: machine-readable stats.

### Runtime/RAM budget
64 GB machine, expect ~40 GB free at run time. Strict full pass = ~1h; this adds
decoys (+2%) and the spill I/O. Target < 3h. Streams held in RAM as strict does.
Checkpoint: if the engine pass finishes but scoring crashes, the spilled raw table
must be reusable — write a phase marker into track2_wide_meta after each phase
(engine_done / spill_done / null_done / scored / enriched) and make the script
resumable from the last marker (--resume).

## Deliverable
scripts/track2_wide_run.py implementing ALL of the above + py_compile clean +
a SMOKE path: --smoke runs the whole pipeline on data/mapv2_smoke.db with 600+600
decoys and relaxed guards (no step-6 requirement; use its accepted_pairs_canonmask
if present, else skip the strict-exclusion join) so the logic can be validated
tonight-ish on the busy machine (BelowNormal priority, single-threaded numpy ok).
DO NOT run anything heavy without BelowNormal priority; do NOT touch
data/fullcorpus_v2.db at all (guard the full mode behind the chain-state check).
