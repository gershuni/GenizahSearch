# Codex pre-flight review — MAPV2-15m plan. Verdict: REVISE (all folded)

Codex read the plan + prior design critique + the ACTUAL code and caught 6
plan↔code drift items. All fixed in MAPV2-15m-PLAN.md "Revisions" section.

## Findings (required changes)
1. **C evidence must query `track1_matches` UNION `track1_candidates`.**
   `track1_candidates` is TIER-B only; tier-A matches live in `track1_matches`
   and are skipped from candidates → the competing-work signal would miss real
   tier-A competitors. Handle tier-A rows lacking `p_same_work`.
2. **`discovery_scored_gated.jsonl` has NO `spans_json`.** `discovery_score.py`
   writes only its `keep` fields (spans excluded). The flank pass must REJOIN
   page text + spans from `track1_matches`/`track1_candidates` in the DB.
3. **Span-merge misstated.** `classify_canonical_edges.merge_intervals()` merges
   only OVERLAPping intervals, not gap<15; and persisted Track-1 `spans_json` is
   ALREADY merged at gap≤30. Use a local gap-aware merge and treat stored spans
   as already-coarse.
4. **Gold count = 127 human-graded (not 132).** 5 ungraded cards: 14,15,16,17,24.
   The distribution (discovery26/witness33/shared30/known18/citation12/tsarich8)
   sums to 127. Call it "127 human-graded dev cards from the 132-card deck."
5. **`score_validation.w()` is not import-clean** (module loads files + runs at
   import). Refactor `w()` into an import-safe helper OR reimplement the weight
   load in `calibrate_flank.py`.
6. **Display helpers narrower than assumed.** `snippet(text,spans)` returns the
   single LONGEST span's slice; `RefText.passage(wid,page_slice,pad=40)` needs
   the slice. Evidence generation must be multi-span/recovered-block aware, not
   rely on snippet()'s longest-span slice.

## Answers to the 5 open questions
1. Full-work relocation is sound, but DON'T call `gram_anchors()` naively in the
   hot loop for long works — cache per-work k-gram positions via `_gram_codes`,
   cap very-frequent in-work grams, verify only the top diagonal windows.
2. Banded aligner: enumerate leading edge shifts `delta ∈ [-G,G]`, run banded
   Levenshtein around the SHIFTED diagonal; band = `ceil(thr*L)+slack`
   (edit-cutoff based, not gap+slack); keep trailing ends free; score endpoints
   with block length ≥ min_len.
3. `shared` is NOT auto-safe as citation — it is safe only as
   non-witness/non-discovery, and only after a spot-check: **hand-check ALL 30
   dev `shared` cards** before trusting the derived island label. `known` mostly
   won't reach production (running `bucket2=='discovery'`) but keep it in dev as
   a false-demotion safety class.
4. Don't withhold strong demotion by grade class (production has no grade). For
   formula/norel: report separately, require the SAME strong-evidence rule (no
   continuation + two-sided island or strong competitor), and DON'T tune
   thresholds from the held-out formula/norel cases.
5. Leakage risk moderate, not fatal: do NOT use the old `flank_class`/
   `flank_dist` as features; freeze on the human-graded dev set only; run the
   100 held-out once.
