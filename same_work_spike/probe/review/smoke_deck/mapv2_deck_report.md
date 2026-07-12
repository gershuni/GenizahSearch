# MAPV2 deck build report

- DB: C:\Genizahsearch\same_work_spike\probe\data\mapv2_smoke.db
- pages with candidate rows: 70,245
- tier A live: 43,697 (ms,work); works: 2,568
- rarity gate: q92 of witness counts = 41, bounded -> **41**
- singleton caps: [[80, 0.799]]; display cap 0.99

## Funnel

- skip_not_best: 552,755
- guard_rarity: 52,885
- guard_canonical_rendering: 15,096
- band_singleton: 14,373
- guard_bible: 12,950
- band_m_0_003: 8,394
- band_m_003_010: 5,156
- not_best_union: 4,282
- known_tierA_pair: 3,012
- guard_canon_citation: 2,673
- guard_verse_align: 2,606
- band_m_ge_010: 725
- known_sibling_vgroup: 0
- survivors after cheap guards: 28,648
- kept after canonical+verse guards: 10,946
- (ms, work) aggregated: 10,143

## P histogram (post-guard, aggregated)
0.1: 575 · 0.3: 2,687 · 0.4: 316 · 0.5: 923 · 0.6: 750 · 0.7: 1,472 · 0.8: 1,402 · 0.9: 2,018

- total time: 917s
