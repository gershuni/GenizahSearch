# Codex review R2 — mapv2_deck.py (real full-corpus deck builder)

## What happened since R1
Your R1 review (of scripts/build_smoke_preview2.py — findings in
results/overnight/codex_deck_guard_r1.log) returned REVISE with 7 findings.
A NEW script scripts/mapv2_deck.py was written to be the production deck
builder. It claims to fix all 7:
  #1 rarity derived at runtime: quantile (deck_params.json: 0.92, bounds
     [30,400]) of live tier-A witness counts, recorded in the report.
  #2 streaming page-grouped pass over both tables ORDER BY page_id (no global
     page_rows dict); canonical guard + Bible-align guard chunked (4,000
     slices/chunk) with an NDJSON checkpoint keyed (page_id, work_id), resume
     by default, --fresh to discard.
  #3 Bible coverage over the row's span UNION.
  #4 flagged rows (merge_page) + not_best rows enter page CONTEXT, excluded
     from display stream.
  #5 (sys_id, work_id) already in LIVE tier A (shadowed_by IS NULL) dropped
     from the discovery stream, counted as known_tierA_pair.
  #6 span-union margin recompute takes competitor density from the
     competitor's OVERLAPPING spans (per-span densities from spans_json),
     fallback to its global best_density.
  #7 argparse CLI (--db/--outdir/--label/--fresh); pnum() failure omits the
     page URL param; no hardcoded corpus counts.

Additional layers (from tonight's Opus stats audit, results/agent_final_cal_audit.md):
  - P recomputed via PModel from stored (alen, dens) + re-derived band;
    singleton caps ([[80, 0.80]] + density>=0.52 -> cap 0.30); display
    honesty: singleton alen<80 shows band label, small-margin bands in
    P 0.2-0.8 show a range bucket instead of a decimal; display cap 0.99.
  - deck_params.json (data/) overrides all parameters.
  - flank-contrast chip per DISPLAYED card (page flanks vs claimed work's
    edition stream, equal-length clip >= 60, thresholds 0.52/0.58) — evidence
    chip only, never a filter.
  - blinded grading deck: ~60 cards stratified (band x P quintile), P/band
    hidden, key json saved separately.

## Producer schema (context)
- track1_matches(page_id, sys_id, work_id, cat, genre, author, title, mesirah,
  matched_letters, best_density, n_spans, spans_json[, shadowed_by added by
  chain step 3]) — spans_json = [[p0,p1,dens],...] merged spans.
- track1_candidates(..., best_alen, best_density, margin, n_competitors,
  margin_band, p_same_work, flag, n_spans, spans_json).
- pages(page_id, sys_id, buckets, n_chars, text, provenance, ...).
- Runs on data/fullcorpus_v2.db AFTER the overnight chain finishes
  (~250-300K tier-A rows, ~1.5-1.9M tier-B rows, 667K pages).

## Review focus
1. Correctness of the two-cursor page-grouped merge loop (ordering guarantees,
   pages present in only one cursor, last-page flush, ties).
2. Checkpoint semantics: is (page_id, work_id) a sufficient key given a row
   can appear once per (page, work)? Resume correctness if the process died
   mid-chunk (partial NDJSON line, flush timing).
3. Guard-chunk logic: test_pos indexing vs results alignment; rows NOT tested
   (Bible-claimed / guard-exempt) getting verdict 0; the per-chunk verdict
   dict.
4. Memory at full-corpus scale: survivors list (~100-250K tuples), pcache
   clear-at-8000 policy, wstreams dict (~2GB, held through render), guard_ref
   index, cursor memory for ORDER BY on 1.9M rows.
5. Statistical honesty of the display rules as implemented (do the caps apply
   before strata placement? does the blinded deck leak P anywhere?).
6. Anything that would make the deck LIE to the scholar (wrong URL, wrong
   snippet offsets via norm_stream, ref passage from the wrong work).

Files: scripts/mapv2_deck.py (under review), scripts/build_smoke_preview2.py
(imports: query_batch_trimmed/snippet/RefText/merge_iv/ov_len — reviewed in
R1), scripts/mapv2_track1_run.py (PModel/margin_band), data/deck_params.json.

## Output
Numbered findings with severity (BLOCKER/HIGH/MEDIUM/LOW), file+line/function,
concrete fix each. Final verdict line: APPROVE or REVISE.
