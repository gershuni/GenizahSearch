# ref_canon_masks_v2 delta report

Incremental update for ref_corpus_v2.pkl (5421 works). Base: ref_canon_masks.json (503 works, 16h full pass over the old corpus).
Targets re-masked: 60 (58 new REF2/other, 2 stream-changed). All other works byte-identical -> masks carried over. Canonical index works verified identical.
Regression check: 3/3 unchanged works reproduce their existing intervals exactly on the v2 index.
Merged output: 522 works with masks.

NOTE: REF2 Liturgy works are EXPECTED to be heavily masked against Bible — Hallel is ~0.97 contained in Psalms, Pesukei deZimra is Psalms 145-150, etc. That is correct behavior: these works should only be identifiable through their own non-biblical formulations.

## Per-target results (sorted by masked fraction)

| work | cat | stream | masked | frac | intervals | old |
|---|---|---|---|---|---|---|
| REF2:liturgy_hallel הלל | Liturgy | 2,803 | 2,788 | 99.5% | 1 | - |
| REF2:liturgy_kiddush_shabbat_day קידושא רבה (קידוש יום שבת) | Liturgy | 349 | 336 | 96.3% | 1 | - |
| REF2:liturgy_kiddush_friday_night קידוש ליל שבת | Liturgy | 411 | 183 | 44.5% | 1 | - |
| REF2:liturgy_shema_blessings_shabbat_maariv ברכות שמע לערבית שבת | Liturgy | 2,417 | 761 | 31.5% | 2 | - |
| REF2:liturgy_shema_blessings_weekday_maariv ברכות שמע לערבית חול | Liturgy | 3,278 | 761 | 23.2% | 2 | - |
| REF2:liturgy_shema_blessings_weekday_shacharit ברכות שמע לשחרית חול | Liturgy | 3,909 | 824 | 21.1% | 1 | - |
| REF2:b2_rif_hilchot_shabbat יצחק אלפסי (רי"ף) — הלכות הרי"ף (שבת) | Sefaria | 188,697 | 39,236 | 20.8% | 99 | - |
| REF2:liturgy_haggadah הגדה של פסח | Liturgy | 21,569 | 3,623 | 16.8% | 7 | - |
| REF2:liturgy_birkat_hamazon ברכת המזון | Liturgy | 5,035 | 672 | 13.3% | 2 | - |
| REF2:liturgy_amidah_shabbat_maariv עמידה לשבת (ערבית) | Liturgy | 3,226 | 315 | 9.8% | 2 | - |
| REF2:liturgy_amidah_shabbat_shacharit עמידה לשבת (שחרית) | Liturgy | 3,001 | 156 | 5.2% | 1 | - |
| REF2:liturgy_amidah_weekday_maariv עמידה לחול (ערבית) | Liturgy | 4,295 | 128 | 3.0% | 1 | - |
| REF2:liturgy_amidah_weekday_shacharit עמידה לחול (שחרית) | Liturgy | 4,357 | 128 | 2.9% | 1 | - |
| REF2:targum_ketuvim_chronicles_1 תרגום דברי הימים א | Targum | 57,671 | 1,442 | 2.5% | 3 | - |
| REF2:targum_jonathan_joshua תרגום יונתן על יהושע | Targum | 43,761 | 1,094 | 2.5% | 1 | - |
| REF2:targum_onkelos_genesis תרגום אונקלוס על בראשית | Targum | 85,890 | 1,321 | 1.5% | 3 | - |
| REF2:targum_jonathan_zechariah תרגום יונתן על זכריה | Targum | 15,982 | 151 | 0.9% | 1 | - |
| M:Ytext280002 יהודה אבן תיבון — ספר הרקמה ליונה אבן ג׳נאח,  | Maagarim | 416,930 | 2,618 | 0.6% | 16 | 2,338 (stale coords) |
| REF2:targum_onkelos_numbers תרגום אונקלוס על במדבר | Targum | 71,126 | 329 | 0.5% | 1 | - |
| REF2:b2_radak_isaiah דוד קמחי (רד"ק) — פירוש רד"ק לישעיה | Sefaria | 409,909 | 1,198 | 0.3% | 7 | - |
| M:Ytext721003 מחבר לא ידוע — כתובת בית אל-חאדר, תימן | Maagarim | 260 | 0 | 0.0% | 0 | - |
| REF2:b2_keter_malkhut שלמה אבן גבירול — כתר מלכות (רשב"ג/אבן גבירול | Sefaria | 15,537 | 0 | 0.0% | 0 | - |
| REF2:liturgy_amidah_shabbat_musaf עמידה למוסף שבת | Liturgy | 4,575 | 0 | 0.0% | 0 | - |
| REF2:targum_jonathan_amos תרגום יונתן על עמוס | Targum | 10,120 | 0 | 0.0% | 0 | - |
| REF2:targum_jonathan_deuteronomy תרגום (פסאודו-)יונתן על דברים | Targum | 88,340 | 0 | 0.0% | 0 | - |
| REF2:targum_jonathan_exodus תרגום (פסאודו-)יונתן על שמות | Targum | 97,401 | 0 | 0.0% | 0 | - |
| REF2:targum_jonathan_ezekiel תרגום יונתן על יחזקאל | Targum | 91,485 | 0 | 0.0% | 0 | - |
| REF2:targum_jonathan_genesis תרגום (פסאודו-)יונתן על בראשית | Targum | 118,005 | 0 | 0.0% | 0 | - |
| REF2:targum_jonathan_habakkuk תרגום יונתן על חבקוק | Targum | 4,326 | 0 | 0.0% | 0 | - |
| REF2:targum_jonathan_haggai תרגום יונתן על חגי | Targum | 2,623 | 0 | 0.0% | 0 | - |
| REF2:targum_jonathan_hosea תרגום יונתן על הושע | Targum | 15,696 | 0 | 0.0% | 0 | - |
| REF2:targum_jonathan_i_kings תרגום יונתן על מלכים א | Targum | 56,712 | 0 | 0.0% | 0 | - |
| REF2:targum_jonathan_i_samuel תרגום יונתן על שמואל א | Targum | 59,146 | 0 | 0.0% | 0 | - |
| REF2:targum_jonathan_ii_kings תרגום יונתן על מלכים ב | Targum | 53,571 | 0 | 0.0% | 0 | - |
| REF2:targum_jonathan_ii_samuel תרגום יונתן על שמואל ב | Targum | 48,198 | 0 | 0.0% | 0 | - |
| REF2:targum_jonathan_isaiah תרגום יונתן על ישעיהו | Targum | 95,313 | 0 | 0.0% | 0 | - |
| REF2:targum_jonathan_jeremiah תרגום יונתן על ירמיהו | Targum | 104,741 | 0 | 0.0% | 0 | - |
| REF2:targum_jonathan_joel תרגום יונתן על יואל | Targum | 4,892 | 0 | 0.0% | 0 | - |
| REF2:targum_jonathan_jonah תרגום יונתן על יונה | Targum | 3,036 | 0 | 0.0% | 0 | - |
| REF2:targum_jonathan_judges תרגום יונתן על שופטים | Targum | 45,488 | 0 | 0.0% | 0 | - |
| REF2:targum_jonathan_leviticus תרגום (פסאודו-)יונתן על ויקרא | Targum | 64,929 | 0 | 0.0% | 0 | - |
| REF2:targum_jonathan_malachi תרגום יונתן על מלאכי | Targum | 4,376 | 0 | 0.0% | 0 | - |
| REF2:targum_jonathan_micah תרגום יונתן על מיכה | Targum | 7,887 | 0 | 0.0% | 0 | - |
| REF2:targum_jonathan_nahum תרגום יונתן על נחום | Targum | 3,421 | 0 | 0.0% | 0 | - |
| REF2:targum_jonathan_numbers תרגום (פסאודו-)יונתן על במדבר | Targum | 97,011 | 0 | 0.0% | 0 | - |
| REF2:targum_jonathan_obadiah תרגום יונתן על עובדיה | Targum | 1,447 | 0 | 0.0% | 0 | - |
| REF2:targum_jonathan_zephaniah תרגום יונתן על צפניה | Targum | 3,956 | 0 | 0.0% | 0 | - |
| REF2:targum_ketuvim_chronicles_2 תרגום דברי הימים ב | Targum | 71,835 | 0 | 0.0% | 0 | - |
| REF2:targum_ketuvim_ecclesiastes תרגום קהלת | Targum | 29,933 | 0 | 0.0% | 0 | - |
| REF2:targum_ketuvim_esther תרגום אסתר | Targum | 30,443 | 0 | 0.0% | 0 | - |
| REF2:targum_ketuvim_esther_targum_sheni תרגום שני על אסתר | Targum | 69,551 | 0 | 0.0% | 0 | - |
| REF2:targum_ketuvim_job תרגום איוב | Targum | 45,045 | 0 | 0.0% | 0 | - |
| REF2:targum_ketuvim_lamentations תרגום איכה | Targum | 12,816 | 0 | 0.0% | 0 | - |
| REF2:targum_ketuvim_proverbs תרגום משלי | Targum | 34,926 | 0 | 0.0% | 0 | - |
| REF2:targum_ketuvim_psalms תרגום תהלים | Targum | 108,808 | 0 | 0.0% | 0 | - |
| REF2:targum_ketuvim_ruth תרגום רות | Targum | 9,002 | 0 | 0.0% | 0 | - |
| REF2:targum_ketuvim_song_of_songs תרגום על שיר השירים | Targum | 25,315 | 0 | 0.0% | 0 | - |
| REF2:targum_onkelos_deuteronomy תרגום אונקלוס על דברים | Targum | 60,585 | 0 | 0.0% | 0 | - |
| REF2:targum_onkelos_exodus תרגום אונקלוס על שמות | Targum | 70,359 | 0 | 0.0% | 0 | - |
| REF2:targum_onkelos_leviticus תרגום אונקלוס על ויקרא | Targum | 49,391 | 0 | 0.0% | 0 | - |

## Stats
- gram hits: 65,794,286; candidate hulls: 12,638,198; accepted spans: 209
- df-dropped codes in canonical index: 14,632
- total runtime: 3290s