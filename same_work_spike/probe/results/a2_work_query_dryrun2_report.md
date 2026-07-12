# A2 work-query second pass -- tag 'dryrun2' (liturgy.db)

- query works: 49 (fullcorpus.db df_damage cohort: short-work <2,000L/>=10MS rate<60% UNION >=20-MS edited-cat rate<30%)
- reference letters indexed: 8,170,967; ref-canon-masks applied to 11/49 works; segments 2,293, postings 7,871,820; distinct-work DF cap=24 (dropped 2,644 generic gram-codes)
- runtime: 5760s total, 117.5s/work avg (139,694 target-db pages scanned)
- hit rows: 2,528; works with >=1 hit: 46/49
- cohort pages in target-db scope: 1,112
- **metric (b) df_damage-compatible pairing** -- before (Track-2 same-work accepted pairs): 492 (44%); after (reference-mediated, >=200L both sides, distinct sys, dup-clean): 794 (71%)
- added (MS,work) memberships beyond the live track1_matches census: 201 (across 36/49 works)

Metric (a) = raw reference-hit coverage (share of census pages the work-query re-hit at all); metric (b) = the honest pairing rate directly comparable to df_damage.py's before-column.

## Per-work before/after (sorted by rate-after (b), worst first)
| work | reasons | pages(target-db) | rate-before(full-census) | rate-before(local) | rate-after(b) | coverage(a) | hit-pages | added-MS |
|---|---|---|---|---|---|---|---|---|
| [Maagarim] מחבר לא ידוע — מדרש ״אגור״ | big | 0 | 7% | n/a | n/a | n/a | 4 | 0 |
| [Maagarim] נסים גאון בן יעקב — מגילת סתרים | big | 0 | 10% | n/a | n/a | n/a | 0 | 0 |
| [Maagarim] מחבר לא ידוע — מתיבות | big | 0 | 22% | n/a | n/a | n/a | 0 | 0 |
| [JA] רי"צ גיאת, קהלת פירוש (ספר הפרישות) | big | 2 | 5% | 0% | 0% | 100% | 3 | 0 |
| [Maagarim] מחבר לא ידוע — ויקרא רבה | big | 1 | 10% | 0% | 0% | 0% | 14 | 2 |
| [JA] רמב"ם, פרוש המשנה ג. סדר נשים | big | 2 | 21% | 0% | 0% | 0% | 0 | 0 |
| [Maagarim] מחבר לא ידוע — יוסיפון | big | 1 | 22% | 0% | 0% | 100% | 1 | 0 |
| [JA] נתנאל בן ישעיה, מאור האפלה | big | 2 | 29% | 0% | 0% | 100% | 41 | 5 |
| [Maagarim] מחבר לא ידוע — בקשה בתפילת השחר | short,big | 33 | 30% | 9% | 0% | 0% | 1 | 0 |
| [Maagarim] מחבר לא ידוע — ספרי דברים | big | 1 | 30% | 0% | 0% | 0% | 5 | 0 |
| [Maagarim] נטרונאי גאון בר׳ הילאי — תשובות, ברודי | big | 5 | 18% | 40% | 20% | 20% | 16 | 1 |
| [Maagarim] מחבר לא ידוע — קידוש לרגלים | short,big | 37 | 30% | 78% | 22% | 22% | 45 | 8 |
| [Maagarim] מחבר לא ידוע — הלכות ״ראו״ | big | 11 | 29% | 18% | 27% | 27% | 12 | 6 |
| [Maagarim] מחבר לא ידוע — מסכת אבות דרבי נתן, נוסח ב | big | 14 | 12% | 0% | 29% | 50% | 11 | 1 |
| [Maagarim] מחבר לא ידוע — והזהיר | big | 21 | 18% | 0% | 29% | 29% | 21 | 2 |
| [Maagarim] מחבר לא ידוע — קדושת היום בשחרית לשבת [שריד מ | short | 24 | 54% | 96% | 38% | 42% | 72 | 5 |
| [Maagarim] מחבר לא ידוע — הגדה של פסח, ברכת החתימה | short | 9 | 20% | 89% | 44% | 89% | 36 | 3 |
| [JA] רמב"ם, פרוש המשנה ה. סדר קדשים | big | 4 | 26% | 75% | 50% | 50% | 9 | 3 |
| [Maagarim] חננאל בן חושיאל — פירוש לתלמוד, סנהדרין (טקסט | big | 7 | 25% | 0% | 57% | 57% | 9 | 3 |
| [Maagarim] מחבר לא ידוע — הגדה של פסח, חלק א (מה נשתנה ו | big | 103 | 26% | 94% | 58% | 64% | 126 | 13 |
| [Maagarim] מרדכי זאב פיירברג — לאן؟ | big | 36 | 5% | 33% | 58% | 58% | 63 | 9 |
| [Maagarim] מחבר לא ידוע — סדר עולם רבה | big | 51 | 30% | 57% | 59% | 59% | 73 | 6 |
| [Maagarim] מחבר לא ידוע — וידוי לסליחות וליום הכיפורים,  | big | 102 | 26% | 61% | 60% | 73% | 227 | 23 |
| [Maagarim] מחבר לא ידוע — מגילת המקדש | big | 15 | 18% | 0% | 60% | 60% | 17 | 2 |
| [JA] סעדיה בן דוד, מדרש הבאור א | big | 6 | 15% | 0% | 67% | 67% | 35 | 5 |
| [JA] אבן עקנין, התגלות הסודות | big | 6 | 29% | 0% | 67% | 67% | 17 | 5 |
| [Maagarim] מתרגם לא ידוע — ספר מצוות ללוי בן יפת הלוי, ת | big | 7 | 13% | 0% | 71% | 71% | 8 | 0 |
| [JA] ראב"ש, מלכים א פירוש | big | 14 | 18% | 0% | 79% | 79% | 21 | 2 |
| [Maagarim] מחבר לא ידוע — תפילה ליום כיפור | short | 50 | 15% | 54% | 80% | 82% | 105 | 1 |
| [Maagarim] מחבר לא ידוע — פיוט לאחר הסליחות במוסף ליום ה | short | 16 | 33% | 100% | 81% | 81% | 169 | 4 |
| [Maagarim] יוסף בן אביתור — רהיטים וסדרי פסוקים ליו״כ | big | 32 | 24% | 25% | 84% | 84% | 43 | 1 |
| [Maagarim] מחבר לא ידוע — תפילת פסוקים לאחר ערבית | short,big | 55 | 14% | 91% | 87% | 89% | 149 | 5 |
| [Maagarim] מחבר לא ידוע — איכה רבה | big | 8 | 8% | 0% | 88% | 88% | 20 | 7 |
| [Maagarim] מחבר לא ידוע — שבעתא למוסף לראש חודש | short | 43 | 25% | 56% | 88% | 88% | 42 | 0 |
| [Maagarim] מחבר לא ידוע — קדושת היום בערבית לשבת | short | 9 | 27% | 67% | 89% | 89% | 108 | 5 |
| [Maagarim] מחבר לא ידוע — תפילת בית המדרש | short | 9 | 38% | 78% | 89% | 100% | 14 | 1 |
| [JA] סעדיה בן דוד, מדרש הבאור ב | big | 10 | 28% | 30% | 90% | 90% | 49 | 9 |
| [Maagarim] יניי — קדושתות לשבתות השנה, שמות | big | 44 | 27% | 34% | 91% | 91% | 57 | 6 |
| [Maagarim] יניי — קדושתות לשבתות השנה, ויקרא | big | 40 | 15% | 15% | 92% | 92% | 68 | 1 |
| [Maagarim] יניי — קדושתות לשבתות השנה, דברים | big | 34 | 6% | 6% | 94% | 94% | 95 | 12 |
| [Maagarim] יניי — קדושתות לשבתות השנה, במדבר | big | 34 | 26% | 29% | 94% | 94% | 82 | 3 |
| [Maagarim] רב סעדיה גאון — צלותא | short,big | 73 | 21% | 44% | 95% | 95% | 117 | 8 |
| [Maagarim] אלעזר החזן — קדושתות י״ח לראשי חדשים | big | 56 | 5% | 5% | 98% | 100% | 63 | 0 |
| [JA] רמב"ם, פרוש המשנה ו. סדר טהרות | big | 1 | 9% | 0% | 100% | 100% | 4 | 0 |
| [JA] רשב"ח, עשר שאלות לרשב"ח | big | 4 | 15% | 0% | 100% | 100% | 4 | 0 |
| [Maagarim] מחבר לא ידוע — שיר השירים רבה | big | 2 | 18% | 0% | 100% | 100% | 5 | 1 |
| [Maagarim] שלמה סולימן — יוצרות לשבתות השנה, דברים | big | 38 | 21% | 16% | 100% | 100% | 46 | 1 |
| [Maagarim] רב סעדיה גאון — יוצרות לשבתות השנה, ויקרא | big | 38 | 26% | 18% | 100% | 100% | 82 | 7 |
| [Maagarim] חננאל בן חושיאל — פירוש לתלמוד, פסחים (טקסט ב | big | 2 | 28% | 0% | 100% | 100% | 131 | 25 |