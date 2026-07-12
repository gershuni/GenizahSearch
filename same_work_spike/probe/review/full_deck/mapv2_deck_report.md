# MAPV2 deck build report

- DB: C:\Genizahsearch\same_work_spike\probe\data\fullcorpus_v2.db
- pages with candidate rows: 364,900
- tier A live: 87,547 (ms,work); works: 3,665
- rarity gate: q92 of witness counts = 45, bounded -> **45**
- singleton caps: [[80, 0.799]]; display cap 0.99

## Funnel

- skip_not_best: 845,482
- guard_rarity: 344,283
- band_singleton: 67,035
- guard_bible: 32,909
- guard_canonical_rendering: 29,935
- not_best_union: 10,417
- band_m_0_003: 9,595
- guard_canon_citation: 9,551
- guard_verse_align: 7,743
- known_tierA_pair: 7,018
- band_m_003_010: 6,529
- guard_modern_era: 5,214
- skip_flag: 1,259
- band_m_ge_010: 972
- guard_cite_formula: 919
- guard_substitution_risk: 270
- known_sibling_targum: 49
- known_sibling_vgroup: 0
- survivors after cheap guards: 77,949
- kept after canonical+verse guards: 40,271
- (ms, work) aggregated: 33,684

## Title-gate router (v11)

- generic_or_absent: 7,257
- same_work: 1,040
- name_variant: 39
- known_quoter: 1,881
- different_specific: 23,467
- reversed-citation candidates surfaced: 0
- cite-gate v11: dropped 919, exempted-aligned 41

## P histogram (post-guard, aggregated)
0.1: 823 · 0.2: 178 · 0.3: 12,960 · 0.4: 567 · 0.5: 2,706 · 0.6: 3,856 · 0.7: 5,273 · 0.8: 3,324 · 0.9: 3,997

- total time: 94s
