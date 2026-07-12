# Codex review brief — MAPV2 preview-deck guard chain (build_smoke_preview2.py)

## What this is
`C:\Genizahsearch\same_work_spike\probe\scripts\build_smoke_preview2.py` builds an HTML
"discovery deck" from `track1_candidates` (tier B — probability-graded same-work candidates
for Cairo Genizah manuscript pages against a 5,421-work reference corpus). The deck's job:
show a scholar high-probability NEW witness candidates, while filtering out **citation
leaks** — pages that merely QUOTE a canonical text (Bible verse, Talmud dictum, statutory
prayer) and get claimed as witnesses of some work that quotes the same canon.

This smoke version ran on a 139,694-page liturgy subcorpus (mapv2_smoke.db). It is about to
become the base of the REAL full-corpus deck (667,411 pages; tier B will be ~1.5M rows).
We need a hard review BEFORE that promotion.

## Guard chain (v6, current file state)
1. `not_best` margin-band rows excluded (dominated by a better match on same page).
2. Rarity gate: drop rows whose work's total tier-A witness count > RARITY_MAX=60
   (KNOWN BLOCKER: 60 was hand-picked on the liturgy subcorpus; must be re-derived as a
   quantile of the full-corpus tier-A distribution — check how the code would behave).
3. Bible-span-coverage gate: if >=0.70 of the matched span-union is inside spans that also
   match Bible, drop (cheap gram-level pass).
4. Span-union margin gate.
5. Canonical-rendering guard (the heavy one, `query_batch_trimmed`): re-queries each
   surviving slice against a GUARD REFERENCE = works with cat in
   ('Bible','Targum','Liturgy','Mishnah','Bavli','Yerushalmi','Tosefta') + JA tafsir
   (151 works, 15.5M letters). Uses TRIMMED hulls (no +-30 padding), window>=18,
   density<=0.35, per-work UNION coverage >=0.45 => the slice is "canonical content", drop.
   EXEMPTION: rows whose CLAIMED work is itself in the guard reference skip the whole
   guard (`r[3] not in guard_ids` = only non-guard claims are tested) — a genuine Bavli
   witness must not be killed for containing Bavli.
6. Stage-2 for survivors claimed as non-Bible/non-Targum: whole-slice
   `partial_ratio_alignment` vs the Bible stream, score>=60 => drop (catches verse quotes
   the gram guard misses due to HTR noise).

Measured on graded ground truth: v5 recipe = 85% leak catch, zero genuine-discovery kills,
top-25 band precision 72% (v3 was 55%, a broken v4 was 32%). v6 adds the rabbinic canon
to the guard reference to kill Bavli/Mishnah-dicta leaks (2 known cards).

## Numbers from the running v6 build (liturgy smoke)
- tier-B rows 657,205; after cheap guards survivors 37,863
  (guard_rarity dropped 251,273; guard_bible 219,643; not_best 148,426)
- canonical guard: 35,422 slices queried, dropped 18,540, kept 19,323

## What to review (ranked)
1. CORRECTNESS of the guard chain: order-of-application bugs, double-counting, rows
   escaping via code paths that skip a guard unintentionally, the exemption semantics
   (does `r[3] not in guard_ids` exempt exactly claimed-work-in-guard rows and nothing
   else?), off-by-one in span/stream offset handling (norm_stream offsets vs raw text).
2. SCALE hazards for the full corpus: ~1.5M tier-B rows, 667K pages. Memory blowups
   (per-row text fetch, dict growth), quadratic passes, the guard query cost
   (35K slices took ~minutes on liturgy; full corpus maybe 10-20x). What must be batched,
   capped, or made resumable?
3. STATISTICAL soundness of gates: RARITY_MAX=60 hardcoded (flagged); Bible-cover 0.70;
   coverage 0.45; align>=60 — any of these coupling badly with the exemption or with
   each other? Any gate that silently drops the very thing the deck exists to find
   (single-witness works, fragmentary pages)?
4. HTML/report layer: RTL correctness (bdi/dir usage), side-by-side panes, escaping,
   deck size limits (cards capped?), link URLs.
5. Anything that breaks when the DB is fullcorpus_v2.db instead of mapv2_smoke.db
   (schema identical, but provenance mix htr/fgp/pgp and much larger works table).

## Context files (read as needed)
- scripts/build_smoke_preview2.py  (the file under review)
- scripts/mapv2_track1_run.py      (producer of track1_candidates; 18-col schema:
  page_id,sys_id,work_id,cat,genre,author,title,mesirah,matched_letters,best_alen,
  best_density,margin,n_competitors,margin_band,p_same_work,flag,n_spans,spans_json)
- scripts/cal1_calibration.py      (P model producer)
- scripts/normalize.py             (norm_stream)
- data/p_calibration_final.json    (deployed P model)

## Output format
Numbered findings, each with severity (BLOCKER / HIGH / MEDIUM / LOW), file+function,
and a concrete fix. End with a final verdict line: APPROVE or REVISE.
