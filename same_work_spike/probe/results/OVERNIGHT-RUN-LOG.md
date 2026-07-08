
## Overnight full-corpus run — 2026-07-07 20:58
- 20:58:39 waiting for Track-1 full pass (track1_full_stats.json)…

## Overnight full-corpus run — 2026-07-07 21:05
- 21:05:01 waiting for Track-1 full pass (track1_full_stats.json)…
- 21:35:08   still waiting (30 min)
- 21:40:09 Track-1 done (waited 35 min)
- 21:40:09 START 1-parity-ram: parity_spill.py ram

## Overnight full-corpus run — 2026-07-07 21:43
- 21:43:35 waiting for Track-1 full pass (track1_full_stats.json)…
- 21:43:35 Track-1 done (waited 0 min)
- 21:43:35 START 1-parity-ram: parity_spill.py ram
- 21:49:37 OK    1-parity-ram (6 min)
- 21:49:37 START 2-parity-compare: parity_spill.py compare
- 21:50:09 OK    2-parity-compare (1 min)
- 21:50:10 START 3-testimonies: track1_testimonies.py C:\Genizahsearch\same_work_spike\probe\data\fullcorpus.db full
- 21:52:25 OK    3-testimonies (2 min)
- 21:52:25 START 4-review: build_track1_review.py C:\Genizahsearch\same_work_spike\probe\data\fullcorpus.db full
- 21:53:22 OK    4-review (1 min)
- 21:53:22 START 5-track2-canonmask: rehearsal_run.py C:\Genizahsearch\same_work_spike\probe\data\fullcorpus.db full maskcanon
- 22:08:55 FAIL  5-track2-canonmask exit=1 (16 min) — see C:\Genizahsearch\same_work_spike\probe\results\overnight\5-track2-canonmask.log
- 22:08:55 ABORT — critical step failed; chain stopped

## Overnight full-corpus run — 2026-07-07 22:09
- 22:09:57 waiting for Track-1 full pass (track1_full_stats.json)…
- 22:09:57 Track-1 done (waited 0 min)
- 22:09:57 START 1-parity-ram: parity_spill.py ram
- 22:15:47 OK    1-parity-ram (6 min)
- 22:15:47 START 2-parity-compare: parity_spill.py compare
- 22:16:19 OK    2-parity-compare (1 min)
- 22:16:19 START 3-testimonies: track1_testimonies.py C:\Genizahsearch\same_work_spike\probe\data\fullcorpus.db full
- 22:18:35 OK    3-testimonies (2 min)
- 22:18:35 START 4-review: build_track1_review.py C:\Genizahsearch\same_work_spike\probe\data\fullcorpus.db full
- 22:19:31 OK    4-review (1 min)
- 22:19:31 START 5-track2-canonmask: rehearsal_run.py C:\Genizahsearch\same_work_spike\probe\data\fullcorpus.db full maskcanon
- 22:54:30 OK    5-track2-canonmask (35 min)
- 22:54:30 START 6-map: rehearsal_map.py C:\Genizahsearch\same_work_spike\probe\data\fullcorpus.db full accepted_pairs_canonmask
- 22:54:36 OK    6-map (0 min)
- 22:54:36 START 7-atlas: build_rehearsal_atlas.py C:\Genizahsearch\same_work_spike\probe\data\fullcorpus.db full 120 accepted_pairs_canonmask
- 22:54:45 OK    7-atlas (0 min)
- 22:54:45 START 8-graph: build_reuse_graph.py C:\Genizahsearch\same_work_spike\probe\data\fullcorpus.db full accepted_pairs_canonmask
- 22:59:07 OK    8-graph (4 min)
- 22:59:07 START 9-chains: chain_pages.py C:\Genizahsearch\same_work_spike\probe\data\fullcorpus.db full accepted_pairs_canonmask
- 22:59:49 OK    9-chains (1 min)
- 22:59:49 CHAIN COMPLETE — all artifacts under results/ + review/ (tags *_full*)
