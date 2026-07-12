# A2 work-query second pass -- tag 'fullv2' (fullcorpus_v2.db)

- query works: 38 (fullcorpus.db df_damage cohort: short-work <2,000L/>=10MS rate<60% UNION >=20-MS edited-cat rate<30%)
- reference letters indexed: 8,262,980; ref-canon-masks applied to 26/38 works; segments 2,311, postings 1,681,670; distinct-work DF cap=3 (dropped 292,222 generic gram-codes)
- runtime: 690s total, 18.2s/work avg (667,411 target-db pages scanned)
- hit rows: 36,875; works with >=1 hit: 38/38
- cohort pages in target-db scope: 2,924
- **metric (b) df_damage-compatible pairing** -- before (Track-2 same-work accepted pairs): 675 (23%); after (reference-mediated, >=200L both sides, distinct sys, dup-clean): 2,774 (95%)
- added (MS,work) memberships beyond the live track1_matches census: 12,834 (across 38/38 works)

Metric (a) = raw reference-hit coverage (share of census pages the work-query re-hit at all); metric (b) = the honest pairing rate directly comparable to df_damage.py's before-column.

## Per-work before/after (sorted by rate-after (b), worst first)
| work | reasons | pages(target-db) | rate-before(full-census) | rate-before(local) | rate-after(b) | coverage(a) | hit-pages | added-MS |
|---|---|---|---|---|---|---|---|---|
| [Maagarim] מחבר לא ידוע — והזהיר | big | 81 | 19% | 19% | 80% | 96% | 1019 | 545 |
| [Maagarim] מחבר לא ידוע — תפילת פסוקים לאחר ערבית | short,big | 46 | 24% | 24% | 85% | 100% | 134 | 35 |
| [Maagarim] מחבר לא ידוע — איכה רבה | big | 27 | 7% | 7% | 85% | 96% | 727 | 400 |
| [Maagarim] מחבר לא ידוע — חתימה לקדושת היום בתפילות יום  | short | 62 | 56% | 56% | 85% | 100% | 266 | 111 |
| [Maagarim] מחבר לא ידוע — ספרי דברים | big | 49 | 27% | 27% | 86% | 100% | 525 | 313 |
| [Maagarim] מחבר לא ידוע — סדר עולם רבה | big | 89 | 29% | 29% | 90% | 100% | 2679 | 946 |
| [Maagarim] מחבר לא ידוע — הגדה של פסח, ברכת החתימה | short | 10 | 20% | 20% | 90% | 100% | 110 | 45 |
| [JA] סעדיה בן דוד, מדרש הבאור ב | big | 195 | 30% | 30% | 90% | 97% | 1879 | 928 |
| [Maagarim] מחבר לא ידוע — שיר השירים רבה | big | 52 | 19% | 19% | 90% | 98% | 531 | 346 |
| [Maagarim] מחבר לא ידוע — ויקרא רבה | big | 42 | 19% | 19% | 90% | 100% | 443 | 267 |
| [Maagarim] מחבר לא ידוע — קידוש לרגלים | short,big | 34 | 24% | 24% | 91% | 100% | 340 | 181 |
| [JA] ראב"ש, מלכים א פירוש | big | 139 | 19% | 19% | 92% | 100% | 1498 | 426 |
| [Maagarim] מחבר לא ידוע — מתיבות | big | 28 | 21% | 21% | 93% | 100% | 157 | 87 |
| [Maagarim] מחבר לא ידוע — תנחומא | big | 252 | 29% | 29% | 94% | 99% | 6052 | 2212 |
| [JA] רמב"ם, פרוש המשנה ג. סדר נשים | big | 96 | 21% | 21% | 95% | 100% | 250 | 111 |
| [Maagarim] מחבר לא ידוע — פסיקתא דרב כהנא | big | 78 | 29% | 29% | 95% | 99% | 1327 | 771 |
| [JA] סעדיה בן דוד, מדרש הבאור א | big | 91 | 16% | 16% | 96% | 100% | 1044 | 642 |
| [Maagarim] רב אחאי משבחא — שאילתות | big | 296 | 29% | 29% | 96% | 99% | 1678 | 760 |
| [Maagarim] נטרונאי גאון בר׳ הילאי — תשובות, ברודי | big | 61 | 20% | 20% | 97% | 98% | 249 | 116 |
| [Maagarim] יוסף בן אביתור — רהיטים וסדרי פסוקים ליו״כ | big | 34 | 24% | 24% | 97% | 97% | 44 | 5 |
| [Maagarim] רב סעדיה גאון — יוצרות לשבתות השנה, ויקרא | big | 38 | 26% | 26% | 97% | 100% | 297 | 153 |
| [JA] רמב"ם, פרוש המשנה ה. סדר קדשים | big | 87 | 29% | 29% | 98% | 100% | 579 | 306 |
| [Maagarim] מחבר לא ידוע — יוסיפון | big | 65 | 22% | 22% | 98% | 100% | 112 | 28 |
| [JA] רמב"ם, פרוש המשנה ו. סדר טהרות | big | 66 | 9% | 9% | 98% | 100% | 402 | 188 |
| [Maagarim] מתרגם לא ידוע — ספר מצוות ללוי בן יפת הלוי, ת | big | 289 | 12% | 12% | 99% | 100% | 1018 | 467 |
| [JA] רי"צ גיאת, קהלת פירוש (ספר הפרישות) | big | 42 | 5% | 5% | 100% | 100% | 153 | 87 |
| [Maagarim] אלעזר החזן — קדושתות י״ח לראשי חדשים | big | 56 | 5% | 5% | 100% | 100% | 65 | 1 |
| [Maagarim] יניי — קדושתות לשבתות השנה, דברים | big | 34 | 6% | 6% | 100% | 100% | 222 | 156 |
| [Maagarim] מחבר לא ידוע — מדרש ״אגור״ | big | 29 | 7% | 7% | 100% | 100% | 152 | 93 |
| [Maagarim] יניי — קדושתות לשבתות השנה, ויקרא | big | 40 | 15% | 15% | 100% | 100% | 292 | 164 |
| [JA] רשב"ח, עשר שאלות לרשב"ח | big | 84 | 15% | 15% | 100% | 100% | 87 | 2 |
| [Maagarim] שלמה סולימן — יוצרות לשבתות השנה, דברים | big | 38 | 21% | 21% | 100% | 100% | 53 | 4 |
| [Maagarim] מחבר לא ידוע — תפילת עמידה, ברכות אחרונות | big | 27 | 22% | 22% | 100% | 100% | 506 | 197 |
| [Maagarim] יניי — קדושתות לשבתות השנה, במדבר | big | 34 | 26% | 26% | 100% | 100% | 433 | 286 |
| [Maagarim] חננאל בן חושיאל — פירוש לתלמוד, סנהדרין (טקסט | big | 52 | 27% | 27% | 100% | 100% | 507 | 343 |
| [Maagarim] יניי — קדושתות לשבתות השנה, שמות | big | 46 | 28% | 28% | 100% | 100% | 334 | 195 |
| [JA] אבן עקנין, התגלות הסודות | big | 88 | 30% | 30% | 100% | 100% | 1167 | 747 |
| [Maagarim] מחבר לא ידוע — קדושת היום במוסף לרגלים | short | 47 | 49% | 49% | 100% | 100% | 420 | 170 |