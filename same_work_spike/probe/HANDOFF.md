# SEED-029 — Session handoff (written 2026-07-08 ~00:30, Fable session)

**State: the full-corpus run is executing unattended overnight.** This file is the
morning entry point. Definitive method doc: `METHOD.md`; rehearsal numbers:
`REHEARSAL-RESULTS.md`; grades: `results/track1_id_grades_round1.md`.

## What is running right now (background, dev box)

1. **Track-1 full pass** — `track1_match.py data/fullcorpus.db full`
   (667,411 pages vs Maagarim+JA). Started ~21:15, ~4–5h expected. Writes
   `fullcorpus.db::track1_matches` + `results/track1_full_report.md` +
   `results/track1_full_stats.json` (the completion marker).
2. **Overnight orchestrator** — `overnight_full_run.py`, polling for that stats
   file, then chaining (sequentially, logs in `results/overnight/<step>.log`,
   progress in `results/OVERNIGHT-RUN-LOG.md`):
   parity ram → parity compare → testimonies full → review page full →
   **Track-2 canonmask full run** (hours) → map → atlas → graph →
   page-chains (`chain_pages.py` — multi-page continuous parallels,
   `results/chains_full.md`; validated on 100K: 54 chains, all
   catalog-plausible, incl. a generic "קובץ בהלכה קראית" identified as
   ספר המצוות ללוי בן יפת by a 2-page chain).

## Morning checklist

1. Read `results/OVERNIGHT-RUN-LOG.md` top to bottom. Every step should say `OK`.
   - `2-parity-compare` must print `PARITY OK — 40,549,024 segments` (in
     `results/overnight/2-parity-compare.log`). If it FAILED, the spill engine
     has a bug → do NOT trust step 5 output; debug `engine_np._spill_path`
     against `parity_spill.py`.
   - If the chain aborted at step 0: Track-1 crashed — check the last lines of
     the Track-1 console (task output) and re-run
     `python track1_match.py <fullcorpus.db> full`, then re-run the orchestrator.
2. Expected full-corpus artifacts (tag `full`):
   - `results/track1_full_report.md` — identification census over 667K pages
   - `results/track1_full_testimonies.{md,csv}` + `review/track1_full_testimonies.html`
     — testimony/citation split; **tier `new?` (no bib) = the discovery queue**,
     `new?known` = already discussed/published (FJMS bibliography demotion)
   - `review/track1_full_id_review.html` — 400-card evidence review page
   - `results/rehearsal_full_{stats.json,map.md,clusters.csv}` — the canonmask map
   - `review/rehearsal_full_atlas.html`, `review/rehearsal_full_graph.html`
3. Deliver to Hillel: the testimonies HTML, the id-review HTML, the graph.
4. Commit the results/review artifacts (data DBs stay gitignored).

## Hillel's review state (2026-07-07 night)

- Graded 61 cards of review v1: **58 correct, 3 boundary citations, 0 wrong**
  (`results/track1_id_grades_round1.md`). Grades JSON preserved at
  `review/grades_track1_id_round1_2026-07-07.json`.
- His two corrections are implemented (`42f8dc3a`): translation/acronym-aware
  title matching + FJMS bibliography demotion of already-known `new?` claims.
- He may grade more of v2 overnight/morning — if a new grades JSON appears in
  Downloads, copy it into `review/` and fold into the analysis md.

## Known trade-offs / open items (do not re-litigate architecture)

- **Thresholds**: testimony ≥0.45 / citation <0.15 page coverage kept as-is;
  the 3 citation grades clustered on ONE work (`Ytext86000`) at 0.43–0.50 —
  per-work quotation profiles are a possible refinement, not a blocker.
- **Verify stage** of the full Track-2 run holds ~250–300M candidate segments
  in RAM (~10 GB) — fine alone, do NOT run other heavy jobs concurrently.
- **Graph/atlas at full scale** are `critical=False` in the orchestrator; if
  they failed, the map data (step 6) is still complete — rebuild interactively.
- **Page-chain extension** IS implemented as post-processing
  (`chain_pages.py`, orchestrator step 9) — chains consecutive-P-number
  accepted pairs whose spans reach the shared page boundary. Not yet
  integrated into the map layers (a chain could upgrade its MS pair to
  continuation-class regardless of flank noise) — candidate refinement.
- **Disk**: spill peaks ~45 GB in `data/spill/` (auto-deleted); box had 82 GB
  free at launch. `fullcorpus.db` ~1.5 GB, gitignored, regenerable in ~2 min
  (`extract_full.py`).
- Maagarim needs the `\\?\` long-path prefix on open (done in code). AlmaId ==
  sys_id for all FJMS lookups (`fist_data/fjms_enrichment.db` — root copy is
  a 0-byte stub, don't use it).

## Next work after the full map (priority order, per Hillel)

1. Review-driven calibration: fold round-2 grades into thresholds/triage.
2. The `new?` discovery queue at full scale → a clean deliverable list
   (work × MS × evidence + bib status) for scholarly follow-up.
3. Page-chain extension (edge-class spans across consecutive pages).
4. Residue mining: high-witness unidentified units (piyyut/Karaite continent)
   ranked by witness count = "frequent but unedited works" worklist.
5. METHOD.md → share with Avi Shmidman (MiDRASH coordination).
