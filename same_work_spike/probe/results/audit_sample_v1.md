# MAPV2-15b — locked audit sample v1  (FROZEN)

- raw frame (non-shadowed, matched_letters≥40): **218,680**
- sampled (frozen): **467**   hash `879e3b5c7b54d82d`
- **Do not tune against this sample.** Post-stratify by the frame cell sizes in the JSON manifest for corpus-wide rates; split leakage-safe by `component_key` (+ `page_text_hash`).

## sample composition

- genre bucket: piyyut:70, talmud_midrash:60, rabbinic:45, judeo_arabic:45, bible:45, other:36, sefarad:36, targum:34, geonic:33, letters:32, karaite:31
- match size: s:126, m:126, xs:110, l:105
- stitch status: singleton:185, weak_two_work:146, chained:136
- scope regime: single_work:257, miscellany:110, homogeneous_anthology:69, ambiguous:31
- title_class: different_specific:319, generic_or_absent:81, same_work:45, known_quoter:21, name_variant:1
- bib_class: bib_empty:238, bib_mentions:72, bib_partial:62, known_bib:53, published_full:24, known_bib_genre:18
- resolution: ms_scope_ambiguous:279, global_ms_likely:142, page_resolved_known:46
- dedup duplicates flagged: 0

## stitch strata (Stage-0 error denominator)

- chained pages in frame: 5190
