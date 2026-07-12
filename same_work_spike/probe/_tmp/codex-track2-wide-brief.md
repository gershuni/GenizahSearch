# Codex design critique — MAPV2-6: Track-2 probability spectrum for small/fragmented pages

## Goal (user requirement, verbatim intent)
Track-2 finds same-work witness PAIRS inside the Genizah (page-vs-page, HTR both sides,
no reference text). The strict pass (rehearsal_run.py, sloped accept boundary
0.30/<100, 0.386/<200, 0.418 else, MIN_SPAN=25) optimizes precision. The scholar wants a
SECOND, less strict tier "since it may be revealing", with probabilities — and it must
serve SMALL/FRAGMENTARY pages (192,659 of 667,411 pages have <= 500 raw chars), which the
strict boundary treats harshly (density cap 0.30 for alen<100 — one noisy HTR line kills
a genuine small-fragment match).

## Constraints and facts
- Engine: engine_np.build_candidates (char-5-gram, band 20, min_anchors 2, df_drop 100)
  is all-vs-all over the page list, ~20 min for the full corpus. Verify pass computes
  Levenshtein with score_cutoff = 0.45*alen + 1, so densities up to 0.45 are measured
  FOR FREE before the strict boundary rejects. Full strict run = ~1h wall.
- Full-corpus strict pass (step 6 of the running chain) writes accepted_pairs_canonmask
  in fullcorpus_v2.db (canonical Track-1 spans masked out of the gram index).
- Previous full run stats: 55M candidate unique pairs, 53M pair-segments rejected by
  density, 3.4M accepted. So the naive "keep everything under 0.45" wide tier is ~tens
  of millions of rows — must be bounded.
- The Track-1 tier-B design (approved): store candidates with calibrated P >= 0.05 only.
- CAL-1's P model does NOT transfer (it's query-vs-clean-reference, one-sided noise).
- Existing machinery: probe_conformal_fdr.py (chunk-shuffle decoys, CHUNK-controlled;
  make_decoy shuffles a stream in 25-char chunks, destroying long-range structure while
  preserving local gram statistics).

## Proposed design (critique this)
Two new scripts, run AFTER the overnight chain completes (machine free):

### 1. track2_null_calibration.py (~30-60 min)
- Sample S = 60,000 real pages stratified by stream length
  (<=150 / 150-300 / 300-600 / >600), plus D = 12,000 decoys built by chunk-shuffling
  (CHUNK=25) a DIFFERENT random sample of pages (length-stratified the same way).
- Run engine_np.build_candidates over S+D together (one index), maskcanon masks applied
  (same as strict pass). Verify with WIDE acceptance: alen >= 35, density <= 0.45.
- Null model: for each bucket (alen in [35,50,70,100,150,250,400+] x density in 0.05
  steps x min-side-len in the 4 strata), compute decoy pair RATE per decoy page
  (decoy-vs-real pairs only; decoy-vs-decoy discarded). Real rate from real-vs-real.
  Empirical local FDR per bucket = decoy_rate / real_rate (capped 1.0);
  P_genuine = 1 - FDR. Smooth by monotone regression along density within each
  (alen, minlen) slice (P must be non-increasing in density, non-decreasing in alen).
- Output: data/track2_p_model.json (+ reliability table in results/).
- Question for reviewer: is decoy-vs-real the right null? Chunk-shuffle keeps gram
  frequencies but breaks syntax — is 25 chars the right chunk for a PAIR engine where
  BOTH sides are noisy HTR? Should decoys also inherit the real page's exact length?

### 2. track2_wide_run.py (~1.5-2.5h)
- Same as rehearsal_run.py (maskcanon) but verify keeps a pair-best record when:
  (a) NOT strictly accepted (strict rows already live in accepted_pairs_canonmask), and
  (b) alen >= 35, density <= 0.45, and
  (c) P_genuine(bucket) >= 0.05 per the null model — this is the volume bound.
- Compute flank_dist/flank_class + dup detectors for kept rows (same as strict).
- Write to a SEPARATE sidecar db data/track2_wide.db :: track2_candidates
  (17 strict cols + p_genuine REAL + tier TEXT='wide'), so fullcorpus_v2.db stays
  the chain's artifact. Stats json to results/.
- Product (later, not in this review): small-fragment discovery deck merging Track-1
  tier-B (page-vs-reference) and Track-2 wide (page-vs-page) candidates for pages
  <= 600 stream letters, ranked by P, flank_class shown as evidence chip.

## Risks I already see (tell me if mitigations are adequate)
- R1 volume: if P>=0.05 buckets still admit ~10M+ rows, RAM for the pair-best dict
  explodes. Mitigation: hard cap 8M entries + overflow counter in stats; if
  overflowed, raise P floor to 0.10 and rerun (cheap, 2.5h).
- R2 null leakage: decoys built from pages that have TRUE copies elsewhere in the
  corpus retain enough contiguous text (25-char chunks!) to genuinely match their
  copy's pages at alen 35-60 — inflating the null and deflating P for everyone.
  Mitigation: exclude from decoy sourcing any page participating in v1 strict pairs
  or Track-1 tier A. Is that enough, or should CHUNK be smaller (12)?
- R3 multiple testing across ~55M candidate pairs: local FDR per bucket is honest
  about rates, but should the product ALSO carry a q-value (BH within bucket)?
- R4 dup_shelf pairs (same physical object, re-joined fragments) are FINDS for a
  Genizah scholar (joins!), not noise. Plan: keep, flag, and let the deck separate
  "possible join (same shelfmark family)" from "same work, different manuscript".

## Deliverable of this review
Numbered findings (BLOCKER/HIGH/MEDIUM/LOW) on the DESIGN (no code exists yet), answers
to the three embedded questions, then a final line: APPROVE or REVISE.
