# A2 work-query second pass -- tag 'dryrun' (liturgy.db)

- query works: 3 (fullcorpus.db df_damage cohort: short-work <2,000L/>=10MS rate<60% UNION >=20-MS edited-cat rate<30%)
- reference letters indexed: 1,885; ref-canon-masks applied to 0/3 works; segments 3, postings 1,740
- runtime: 65s total, 21.8s/work avg (139,694 target-db pages scanned)
- hit rows: 248; works with >=1 hit: 3/3
- cohort pages in target-db scope: 83; paired before (Track-2 same-work pairs): 56 (67%); paired after (reference-mediated): 62 (75%)
- added (MS,work) memberships beyond the live track1_matches census: 9 (across 3/3 works)

## Per-work before/after (sorted by rate-after, worst first)
| work | reasons | pages(target-db) | rate-before(full-census) | rate-before(local) | rate-after | hit-pages | added-MS |
|---|---|---|---|---|---|---|---|
| [Maagarim] מחבר לא ידוע — תפילה ליום כיפור | short | 50 | 15% | 54% | 70% | 81 | 1 |
| [Maagarim] מחבר לא ידוע — קדושת היום בערבית לשבת | short | 9 | 27% | 67% | 78% | 75 | 3 |
| [Maagarim] מחבר לא ידוע — קדושת היום בשחרית לשבת [שריד מ | short | 24 | 54% | 96% | 83% | 77 | 5 |