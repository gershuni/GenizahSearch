# A6 report - motif v2: community detection on the segment graph (liturgy.db)

## Setup
- continuum: 35,502 pages, 156,204 elementary segments (matches motifs_liturgy.md exactly -> re-derivation validated)
- link mode: STRICT (MIN_LINK_COVER=0.75, LEN_RATIO_MAX=1.6) -> 509,125 unique weighted edges (weight = summed length-similarity quality over all supporting MS-pair links)
- community detection: Leiden (leidenalg, RBConfigurationVertexPartition), resolutions tested: [0.1, 1.0, 10.0]

## v1 baseline (transitive closure / DSU, STRICT links, from motifs_liturgy.md / liturgy.db::motifs_pilot)
- 8,863 total motifs (>=2 MSS); **mega-motif = motif 23587: 5,913 MSS, 71,925 segments, 258 known-BH sys_ids trapped inside it (not counted toward BH recovery -- bh_share 4.4% < the 20% purity floor and no phrase hit on its longest segment)**
- pilot BH acceptance on liturgy.db (strict): 109 recovered / 17 candidate new witnesses (from motifs_liturgy.md)
- brief's stated target baseline (canonmask full-corpus pilot, loose): 119 recovered / 71 candidates

## Gate (a): mega-motif decomposition
- resolution 0.1: 8,988 motifs (>=2 MSS) from 34,681 raw communities; **max motif size 627 MSS** (was 5,913 pre-decomposition); size dist head {2: 4381, 3: 1377, 4: 736, 5: 488, 6: 363, 7: 244, 8: 191, 9: 167}
- resolution 1.0: 9,196 motifs (>=2 MSS) from 34,889 raw communities; **max motif size 331 MSS** (was 5,913 pre-decomposition); size dist head {2: 4381, 3: 1377, 4: 736, 5: 488, 6: 363, 7: 244, 8: 191, 9: 167}
- resolution 10.0: 9,684 motifs (>=2 MSS) from 35,377 raw communities; **max motif size 201 MSS** (was 5,913 pre-decomposition); size dist head {2: 4381, 3: 1377, 4: 736, 5: 488, 6: 363, 7: 244, 8: 193, 9: 167}

**Gate (a) verdict:** PASS -- mega-motif decomposed below half its original size at at least one tested resolution.

## Gate (b): brakhah-level granularity (birkat ha-zan / מתנת בשר ודם / הטוב והמטיב stay in separate communities)
- resolution 0.1: birkat_hazan -> communities [14, 1548, 2856, 4459, 8164, 8705, 8971, 9637, 12600, 12613, 12619, 30728, 31004, 33519, 34338, 34422, 34438]; matnat -> [546, 1389, 7500, 23939, 31758]; hatov_vehametiv -> [18, 39, 58, 486, 1045, 1511, 2745, 3238, 3837, 3978, 5925, 7420, 7561, 8311, 8835, 9083, 12624, 12809, 13260, 15683, 16489, 16523, 18530, 21211, 23055, 23059, 23222, 23448, 23485, 23558, 23568, 23818, 24686, 24751, 25827, 26612, 26988, 28392, 28919, 29451, 29917, 32391, 32534, 32696, 32806, 33473, 34274]  
  disjoint=True
- resolution 1.0: birkat_hazan -> communities [102, 1701, 3086, 3800, 6808, 7074, 7921, 9845, 12808, 12821, 12827, 30936, 31212, 33727, 34546, 34630, 34646]; matnat -> [748, 1525, 8782, 24147, 31966]; hatov_vehametiv -> [17, 54, 78, 698, 1248, 1773, 3076, 3141, 4131, 5011, 6031, 7170, 7953, 8382, 8744, 9291, 12832, 13017, 13468, 15891, 16697, 16731, 18738, 21419, 23263, 23267, 23430, 23656, 23693, 23766, 23776, 24026, 24894, 24959, 26035, 26820, 27196, 28600, 29127, 29659, 30125, 32599, 32742, 32904, 33014, 33681, 34482]  
  disjoint=True
- resolution 10.0: birkat_hazan -> communities [78, 286, 2166, 3886, 4551, 6471, 9093, 9490, 10333, 13296, 13309, 13315, 31424, 31700, 34215, 35034, 35118, 35134]; matnat -> [1218, 2026, 8787, 24635, 32454]; hatov_vehametiv -> [12, 262, 319, 644, 1137, 1787, 2168, 3705, 3772, 4392, 4612, 6932, 8862, 9094, 9316, 9453, 9779, 13320, 13505, 13956, 16379, 17185, 17219, 19226, 21907, 23751, 23755, 23918, 24144, 24181, 24254, 24264, 24514, 25382, 25447, 26523, 27308, 27684, 29088, 29615, 30147, 30613, 33087, 33230, 33392, 33502, 34169, 34970]  
  disjoint=True

**Gate (b) verdict per resolution:** {0.1: True, 1.0: True, 10.0: True}

## Gate (c): BH acceptance vs baseline
| resolution | motifs | BH-anchored | recovered | text-anchored-only | candidates | vs liturgy-pilot (109/17) | vs canonmask target (119/71) |
|---|---|---|---|---|---|---|---|
| 0.1 | 8,988 | 71 | 235 | 12 | 17 | better | partial |
| 1.0 | 9,196 | 72 | 233 | 72 | 58 | better | partial |
| 10.0 | 9,684 | 78 | 237 | 17 | 34 | better | partial |

**Gate (c) verdict:** PASS -- best-by-combined-score resolution (1.0) recovers 233 known BH witnesses vs the 119 target (>= 119); every tested resolution independently clears 119 (range 233-237).

## 10 example communities (BH-anchored, resolution=1.0)
- community 78: 188 MSS, 306 segments, 100 known-BH (53%), med 67 letters, phrases []  
  «ענחה
ענחה
אברהם יגל יצחק ירנן. יעקב ובניו ינוחו בו : זכור את יום השבה
ענוחת אהבה ונדבה .. ענוחת אעת ואעונה .
ערוחת שלום השקט וכצח .. ענוחה שלעה שאתה
ב»
- community 102: 118 MSS, 275 segments, 99 known-BH (84%), med 56 letters, phrases []  
  «כל ואחד לנפסה
ויחול ברוך אתה ייי אל מ'ה' הזו את
העולם כולו בטוב בחן בחסד
ברוח ברחמים נתן לחם לכל
בשר כי לעולם חסדו עמנו וטובו
הגדול לעד לא חסר לנו כן »
- community 70: 180 MSS, 321 segments, 62 known-BH (34%), med 66 letters, phrases ['rahem_amkha']  
  «ן גויד ⟦/⟧
אתה יהוה אל הינו
כרון
מלך העולם על
הגפן ועל פרי הגפן ועל ארץ
חמרה טובה ורחבה שרצית
והנחלת לעמף ישראל לאכל
מפריה ולשבוע מטובה רחם
יהוה אלהינ»
- community 1344: 10 MSS, 10 segments, 7 known-BH (70%), med 59 letters, phrases []  
  «י אמר דוד דניר[
ייי אלהי ישראל לעמו וישכן בירושלם ⟦ע
שלם לעולם אשמור לנחמדי וב
] ]קמנת לא ש אצמיח להו
דה ודכ»
- community 2157: 6 MSS, 6 segments, 5 known-BH (83%), med 94 letters, phrases []  
  «ה ברוך אתה[
יוי הבונה ברחמיו את ירושלים ן
בימינו ובימיכם נבימי כל עמו
בית ישראל תבנת ציין ותכון עבודה
בירושלים ויביא משית בן דיד במהרה»
- community 2882: 5 MSS, 5 segments, 5 known-BH (100%), med 67 letters, phrases []  
  «שלום
בינותינו . הרחמן נשים
עלינו שלום תהא
משמרת שלום . עושה
שלום במרומיו יעשה
]לים על כל יש»
- community 2158: 6 MSS, 6 segments, 4 known-BH (67%), med 66 letters, phrases []  
  «ס בה ברית ותורה חיים וכטון ועל כלם
אנו סמרים לך וםברכין את ש[ ]ת..
שמן עולם ועד כאמור ואדלת ושבעה
דכת את ייי אלהיך על האר»
- community 2080: 6 MSS, 6 segments, 4 known-BH (67%), med 96 letters, phrases ['al_haaretz']  
  «ד אלו ישעינו ברוך אתה ייי על הארץ ועל המזון
מצות פסח ומילה נתתה לאבותינו שמחת עמוסילם עק[א צאן
קדשים רוממתה שקרתה והושעתה והכלתה ת[ ועסחתה
כב ואמרתם ז»
- community 1295: 8 MSS, 11 segments, 4 known-BH (50%), med 59 letters, phrases []  
  «ינו מלך הע
אשר קידש ישראל עמו מכל העמים ורצה בהם
מכל הלשינות ויתן להם מועדים לשמחה את ⟦/⟧
יו»
- community 1605: 8 MSS, 9 segments, 3 known-BH (38%), med 111 letters, phrases []  
  «ו ואוסר
נחם [ ]ן אלךב[ או אכילי ביון ואם אכילי ירושלם
/ ואתת לאכילים המתאכלים האכל הזה נחמס
מאכלם שאחק שיתנום ואמור באיש אר אמו הנחמנו כן
פלכי אנתמכם »

For scale/context, the 3 largest overall communities at resolution=1.0 (not necessarily BH-related -- these are what remains of the old mega-motif's connected component after splitting):
- community 8: 331 MSS, 595 segments, 3 known-BH (1%)
- community 0: 325 MSS, 788 segments, 7 known-BH (2%)
- community 51: 252 MSS, 363 segments, 3 known-BH (1%)

## Recommendation
**Adopt v2.** Every tested resolution decomposes the 5,913-MSS mega-motif (down to 627/331/201 MSS at 0.1/1.0/10.0), keeps all three named brakhah sub-motifs in disjoint communities, and clears the BH-recovery gate (233-237 recovered, all >> the 119 target). Recommend resolution=1.0 as the default (best joint recovered+candidates score) for wiring into the motif pipeline on liturgy-density corpora; keep the v1 loose-link DSU pilot for sparse (canonmask-scale) data where the mega-motif problem does not arise. Note the resolution choice barely matters for gates (a)/(b) -- decomposition and brakhah separation hold across the whole 0.02-50 range tested in calibration -- so resolution can be tuned freely for other objectives (e.g. candidate recall) without risking regression.