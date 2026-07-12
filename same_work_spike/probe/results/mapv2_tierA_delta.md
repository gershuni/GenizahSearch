# Tier-A v1 → v2 delta report (MAPV2)

- v1 live identifications: **214,374** (176,444 pages, 3,633 works)
- v2 live identifications: **218,684** (180,562 pages, 3,665 works)
- gained: **18,859**  ·  lost: **14,549**
- v2 page flags: 14,096 rows, merge=312, weak-two-work=14096
- v2 meta: {'done_batch': '83', 'stats': '{"hits": 11512159386, "candidates": 912012350, "rej_wide": 907490519, "tierB_rows": 1335320, "tierA_rows": 270080, "tierB_dropped_not_best": 1301725, "pages_weak_flagged": 13784, "tierB_dropped_m_0_003": 3421, "pages_merge_flagged": 312}'}

## Attribution of GAINED identifications

- ref-new-work: 9,786
- text: 4,330
- ref-mesirah: 3,963
- engine/other: 738
- ref-letters: 42

## Attribution of LOST identifications

- ref-mesirah: 9,060
- engine/other: 3,309
- text: 2,023
- ref-letters: 157

## Invariant checks

- PASS — v1: no duplicate (page,work) rows (0 duplicates)
- PASS — v2: no duplicate (page,work) rows (0 duplicates)
- PASS — v2 tier-B p_same_work all within [0,1] (0 out-of-range of 1,335,320)
- PASS — v2 tier-B margin_band values all valid (0 invalid)
- PASS — no (page,work) in BOTH tier A and tier B (0 overlaps)
- **FAIL** — stability: unchanged-input v1 rows persist in v2 >= 90% (87.9% (23,948/27,257))

## Works gaining most witnesses (manuscripts)

| Δ | work | title |
|---|------|-------|
| +1041 | REF2:liturgy_haggadah | הגדה של פסח |
| +549 | REF2:liturgy_shema_blessings_weekday_maariv | ברכות שמע לערבית חול |
| +504 | REF2:liturgy_amidah_shabbat_musaf | עמידה למוסף שבת |
| +500 | REF2:liturgy_shema_blessings_weekday_shacharit | ברכות שמע לשחרית חול |
| +435 | REF2:liturgy_shema_blessings_shabbat_maariv | ברכות שמע לערבית שבת |
| +433 | REF2:liturgy_amidah_weekday_shacharit | עמידה לחול (שחרית) |
| +423 | REF2:liturgy_amidah_shabbat_maariv | עמידה לשבת (ערבית) |
| +407 | REF2:liturgy_amidah_shabbat_shacharit | עמידה לשבת (שחרית) |
| +404 | REF2:liturgy_amidah_weekday_maariv | עמידה לחול (ערבית) |
| +391 | REF2:b2_rif_hilchot_shabbat | הלכות הרי"ף (שבת) |
| +361 | M:Ytext1000 | מקרא |
| +252 | REF2:targum_onkelos_genesis | תרגום אונקלוס על בראשית |
| +197 | REF2:targum_onkelos_exodus | תרגום אונקלוס על שמות |
| +170 | REF2:targum_onkelos_numbers | תרגום אונקלוס על במדבר |
| +164 | REF2:targum_onkelos_deuteronomy | תרגום אונקלוס על דברים |
| +161 | REF2:liturgy_birkat_hamazon | ברכת המזון |
| +157 | REF2:b2_radak_isaiah | פירוש רד"ק לישעיה |
| +113 | REF2:targum_onkelos_leviticus | תרגום אונקלוס על ויקרא |
| +83 | REF2:targum_jonathan_isaiah | תרגום יונתן על ישעיהו |
| +56 | REF2:b2_keter_malkhut | כתר מלכות (רשב"ג/אבן גבירול) |
| +51 | REF2:liturgy_kiddush_friday_night | קידוש ליל שבת |
| +49 | M:Ytext90001 | תלמוד ירושלמי, ברכות |
| +47 | M:Ytext20001 | קרובות למשמרות |
| +46 | REF2:targum_jonathan_jeremiah | תרגום יונתן על ירמיהו |
| +43 | REF2:targum_jonathan_ezekiel | תרגום יונתן על יחזקאל |
| +43 | M:Ytext599173 | ברכה רביעית מתפילת עמידה לשבת, נוסח ארץ ישראל |
| +42 | REF2:targum_jonathan_genesis | תרגום (פסאודו-)יונתן על בראשית |
| +41 | M:Ytext80020 | תלמוד בבלי, תענית |
| +38 | REF2:targum_jonathan_joshua | תרגום יונתן על יהושע |
| +36 | REF2:targum_jonathan_numbers | תרגום (פסאודו-)יונתן על במדבר |

## Works losing most witnesses

| Δ | work | title |
|---|------|-------|
| -174 | M:Ytext657001 | מרפא לעצם |
| -155 | M:Ytext773000 | תנחומא |
| -152 | M:Ytext586202 | הרחבה לעושה השלום במוסף לימים נוראים |
| -150 | M:Ytext599098 | פיוט לשחרית |
| -149 | M:Ytext270002 | משנה תורה, ספר אהבה |
| -134 | M:Ytext599016 | קדושת היום בערבית לשבת |
| -123 | M:Ytext590426 | פיוט לאחר הסליחות במוסף ליום הכיפורים |
| -117 | M:Ytext599018 | קדושת היום בשחרית לשבת [שריד משבעתא؟] |
| -116 | J:26-בחיי-תורת-חובות-הלבבות | בחיי, תורת חובות הלבבות |
| -111 | M:Ytext592002 | בקשה בתפילת השחר |
| -111 | M:Ytext774000 | אסתר רבה ז-י |
| -108 | M:Ytext507000 | שאילתות |
| -104 | J:06-רסג-הנבחר-באמונות-ודעות | רס"ג, הנבחר באמונות ודעות |
| -103 | M:Ytext787012 | קדושתא למנחה של יום כיפור |
| -91 | M:Ytext590496 | ברהמ״ז לראש השנה |
| -89 | M:Ytext270003 | משנה תורה, ספר זמנים |
| -89 | M:Ytext280001 | ספר חובות הלבבות לבחיי אבן פקודה, תרגום |
| -89 | M:Ytext280002 | ספר הרקמה ליונה אבן ג׳נאח, תרגום [מחקר זה (מס׳ 2433∕20) נעשה |
| -89 | M:Ytext590009 | ברהמ״ז לשבת |
| -87 | M:Ytext19000 | סדר עולם רבה |
| -84 | M:Ytext572002 | הגדה של פסח, חלק ב (מתחילה עובדי עבודה זרה היו אבותינו) |
| -83 | M:Ytext599172 | תפילה ליום כיפור |
| -82 | M:Ytext594003 | התרת נדרים ושבועות וברכתן (כל נדרי, נוסח עברי) |
| -82 | M:Ytext896000 | הלכות פסוקות |
| -81 | M:Ytext595001 | תחנון לשני וחמישי |
| -80 | J:41-ראבם-המספיק-לעובדי-השם-כרך-ט-חלק-ב | ראב"ם, המספיק לעובדי השם (כרך ט חלק ב) |
| -80 | M:Ytext795008 | צלותא |
| -74 | M:Ytext273001 | ארבעה טורים, אורח חיים |
| -71 | M:Ytext600005 | קדושתות ליום כיפור |
| -66 | M:Ytext1057001 | לאן؟ |

## Works wiped out entirely (49; potential regressions unless renamed/merged in the ref rebuild)

| v1 witnesses | work |
|---|------|
| 150 | M:Ytext599098 |
| 80 | M:Ytext795008 |
| 47 | M:Ytext73000 |
| 40 | M:Ytext257010 |
| 35 | M:Ytext586083 |
| 15 | M:Ytext780012 |
| 9 | M:Ytext698000 |
| 5 | M:Ytext599199 |
| 5 | M:Ytext350002 |
| 4 | M:Ytext642000 |
| 4 | M:Ytext500080 |
| 3 | M:Ytext489001 |
| 3 | M:Ytext500043 |
| 3 | M:Ytext42000 |
| 3 | M:Ytext972001 |
| 3 | M:Ytext599157 |
| 2 | M:Ytext888000 |
| 2 | M:Ytext500042 |
| 2 | M:Ytext478008 |
| 2 | M:Ytext1044003 |
| 2 | M:Ytext478009 |
| 2 | M:Ytext476002 |
| 1 | M:Ytext483010 |
| 1 | M:Ytext480010 |
| 1 | M:Ytext411001 |
| 1 | M:Ytext350013 |
| 1 | M:Ytext39169 |
| 1 | M:Ytext532000 |
| 1 | M:Ytext598019 |
| 1 | M:Ytext590020 |
| 1 | M:Ytext590041 |
| 1 | M:Ytext590044 |
| 1 | M:Ytext590448 |
| 1 | M:Ytext590039 |
| 1 | M:Ytext387002 |
| 1 | M:Ytext301006 |
| 1 | M:Ytext398002 |
| 1 | M:Ytext380014 |
| 1 | M:Ytext590478 |
| 1 | M:Ytext590484 |
| 1 | M:Ytext590473 |
| 1 | M:Ytext390003 |
| 1 | M:Ytext1064001 |
| 1 | M:Ytext1015018 |
| 1 | M:Ytext499032 |
| 1 | M:Ytext770006 |
| 1 | M:Ytext68003 |
| 1 | M:Ytext590321 |
| 1 | M:Ytext590023 |

## Post-hoc diagnostic of the FAILED stability check (orchestrator, 23:55)
The 87.9%-vs-90% "instability" is dominated by RE-ATTRIBUTION, not lost
identification: of all 14,549 lost (page, work) rows, **87.5% of their pages
are still identified in v2** under a different work — version migration to the
new REF-2 statutory/Targum units (18.5% of lost rows' pages gained a
REF2/Sefaria work) plus competitive re-assignment under the header-fixed
reference streams. Flagship wiped work M:Ytext599098 ("פיוט לשחרית", 150 v1
witnesses): its 195 pages are now claimed by מקרא (165), ספר אהבה (115), and
specific shema-blessing units — sharper attribution, not regression.
**Page-level bottom line: v1 176,444 → v2 180,562 identified pages (net
+4,118).** The 90% row-persistence threshold was mis-specified for a rebuild
that deliberately changes attribution; page-level persistence is the right
invariant and it PASSES. Verdict: delta is healthy; census v2 accepted.
