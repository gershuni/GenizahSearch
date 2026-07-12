# Stage-0 report — fullcorpus_v2.db (transcription-preferred search text)

- generated: 2026-07-10 17:01:38 (elapsed 40.7 min)
- gate: score >= 70.0, both streams >= 200 letters, COVERAGE >= 0.8 of the HTR stream when the transcription is shorter (partial-draft guard), window-crop when longer than 1.3x (multi-page guard), ratio cap 12.0, greedy 1:1 per sys_id across both sources
- sources: FGP doc_relation='Digital Edition' only (translations excluded); PGP documents.transcription (has_transcription=1, sys_id via document_fragments)

## Corpus
- total pages: **667411**
- sys_ids with FGP edition rows: **24668**; with PGP transcriptions: **7126**; union: **27356** (of which in fullcorpus: **23445**)
- pages substituted: **18982** — by source: {'fgp': 14932, 'pgp': 4050}
- windows cropped (multi-page transcriptions): 5419

## Gate-failure histogram (pages in transcription-bearing, in-corpus sys_ids that were NOT substituted)
- short (page stream < 200): 8731
- no_candidate (no transcription with stream >= 200): 8617
- partial_coverage (transcription covers < 0.8 of the page — Hillel's partial-draft class): 10033
- ratio (transcription > 12.0x page): 3873
- low_score (best score < 70.0): 37623
- lost_greedy (had a passing pair, lost 1:1 assignment): 2763

## Score distribution of substitutions (by decade)
- 70-79: 2642
- 80-89: 6731
- 90-99: 9598
- 100-100: 11

## Letters / chars
- norm-stream letters over substituted pages — HTR: 12284727, transcription: 12676340 (delta +391613)
- corpus raw chars (SUM n_chars) — before: 731306937, after: 734013928 (delta +2706991)

## fgp_disagree (FGP rows > n_pages — two-page-merge signal)
- total flagged sys_ids: 5959
- flagged AND in fullcorpus (n_pages > 0): 2581

## 10 sample substituted pages

| page_id | source | id | score |
|---|---|---|---|
| 990000465700205171_IE19215902_P000001_FL19215904 | fgp | 36206 | 96.56 |
| 990000555760205171_IE36804917_P000002_FL36804923 | fgp | 43168 | 95.61 |
| 990000555780205171_IE36804914_P000004_FL36804933 | fgp | 43206 | 97.4 |
| 990000555790205171_IE36804940_P000004_FL36804961 | fgp | 43167 | 94.66 |
| 990000571710205171_IE48358011_P000010_FL48358048 | pgp | 38544 | 84.78 |
| 990000571720205171_IE48368950_P000005_FL48369000 | pgp | 38609 | 93.44 |
| 990000635610205171_IE41905554_P000033_FL41906172 | fgp | 43425 | 82.4 |
| 990000635680205171_IE46973883_P000041_FL46973925 | fgp | 44039 | 78.48 |
| 990000635790205171_IE39905712_P000012_FL39905725 | fgp | 43382 | 91.3 |
| 990000635820205171_IE48049385_P000041_FL48049699 | fgp | 44220 | 70.79 |
