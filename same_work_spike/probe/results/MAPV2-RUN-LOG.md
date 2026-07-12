
## MAPV2 overnight chain — 2026-07-10 17:02
- 17:02:34 preflight: 667,411 v2 pages, 14,932 FGP-substituted; masks 58,762 B; ref 171 MB
- 17:02:34 START 1-final-cal1: cal1_calibration.py --tag final
- 17:08:18 OK    1-final-cal1 (6 min)
- 17:08:18 START 2-track1-v2: mapv2_track1_run.py C:\Genizahsearch\same_work_spike\probe\data\fullcorpus_v2.db v2 C:\Genizahsearch\same_work_spike\probe\data\p_calibration_final.json
- 20:24:52 OK    2-track1-v2 (197 min)
- 20:24:52 START 3-shadow: track1_shadow.py C:\Genizahsearch\same_work_spike\probe\data\fullcorpus_v2.db fullv2
- 20:24:56 OK    3-shadow (0 min)
- 20:24:56 START 4-testimonies: track1_testimonies.py C:\Genizahsearch\same_work_spike\probe\data\fullcorpus_v2.db fullv2
- 20:28:58 OK    4-testimonies (4 min)
- 20:28:58 START 5-review: build_track1_review.py C:\Genizahsearch\same_work_spike\probe\data\fullcorpus_v2.db fullv2
- 20:30:26 OK    5-review (1 min)
- 20:30:26 START 6-track2: rehearsal_run.py C:\Genizahsearch\same_work_spike\probe\data\fullcorpus_v2.db fullv2 maskcanon

## MAPV2 morning chain — 2026-07-10 20:59
- 21:20:09 OK    6-track2 (50 min)
- 21:20:09 START 7-map: rehearsal_map.py C:\Genizahsearch\same_work_spike\probe\data\fullcorpus_v2.db fullv2 accepted_pairs_canonmask
- 21:20:21 OK    7-map (0 min)
- 21:20:21 START 7b-atlas: build_rehearsal_atlas.py C:\Genizahsearch\same_work_spike\probe\data\fullcorpus_v2.db fullv2 120 accepted_pairs_canonmask
- 21:20:34 OK    7b-atlas (0 min)
- 21:20:34 START 8-graph: build_reuse_graph.py C:\Genizahsearch\same_work_spike\probe\data\fullcorpus_v2.db fullv2 accepted_pairs_canonmask
- 21:27:39 OK    8-graph (7 min)
- 21:27:39 START 9-chains: chain_pages.py C:\Genizahsearch\same_work_spike\probe\data\fullcorpus_v2.db fullv2 accepted_pairs_canonmask
- 21:29:14 OK    9-chains (2 min)
- 21:29:14 START 10-units: passage_units.py C:\Genizahsearch\same_work_spike\probe\data\fullcorpus_v2.db fullv2 accepted_pairs_canonmask
- 21:32:01 OK    10-units (3 min)
- 21:32:01 START 11-motifs: motif_pilot.py C:\Genizahsearch\same_work_spike\probe\data\fullcorpus_v2.db accepted_pairs_canonmask pilot
- 21:32:30 OK    11-motifs (0 min)
- 21:32:30 START 12-motif-query: motif_query.py C:\Genizahsearch\same_work_spike\probe\data\fullcorpus_v2.db 3 100
- 23:08:34 OK    12-motif-query (96 min)
- 23:08:34 START 13-work-query: work_query.py C:\Genizahsearch\same_work_spike\probe\data\fullcorpus_v2.db fullv2 --census-db C:\Genizahsearch\same_work_spike\probe\data\fullcorpus_v2.db --ref C:\Genizahsearch\same_work_spike\probe\data\ref_corpus_v2.pkl --masks C:\Genizahsearch\same_work_spike\probe\data\ref_canon_masks_v2.json
- 23:47:05 OK    13-work-query (39 min)
- 23:47:05 MAPV2 CHAIN COMPLETE — tier A census + tier B candidates + Track-2 map rebuilt on the v2 state; products (delta report, discovery deck, blinded deck) run next, interactively
- 23:47:48 overnight chain complete (14 steps) — starting morning products
- 23:47:48 START M1-delta: mapv2_delta_report.py
- 23:47:55 FAIL  M1-delta (exit 1) — chain stopped; see C:\Genizahsearch\same_work_spike\probe\results\overnight\morning-M1-delta.log

## MAPV2 morning chain — 2026-07-10 23:53
- 23:53:29 overnight chain complete (14 steps) — starting morning products
- 23:53:29 SKIP  M1-delta (already done)
- 23:53:29 START M2-deck: mapv2_deck.py --outdir C:\Genizahsearch\same_work_spike\probe\review\full_deck --label הקורפוס המלא (667 אלף עמודים)
- 00:50:47 OK    M2-deck
- 00:50:47 morning chain COMPLETE
