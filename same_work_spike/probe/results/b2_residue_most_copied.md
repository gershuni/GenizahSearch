# B2 — residue mining: most-copied unidentified texts

Generated 2026-07-08 23:30. Substrate: `fullcorpus.db` tables `passage_units_accepted_pairs_canonmask` (81,365 units) / `passage_unit_members_accepted_pairs_canonmask` (412,471 rows) — the fresh rebuild with live Track-1 (competitive span assignment) labels. Read-only, light SQLite/CSV reads, no engine runs.

## Method

1. **Residue census** = units with `labeled=0` (no confident Track-1 label), continuum unit 367274 (18,676 MSS) excluded per the brief. **73,101 units.** No other `suspect`-style flag exists in the `passage_units`/`passage_unit_members` schema (checked `PRAGMA table_info`) — only the continuum unit needed quarantine.
2. Ranked by **distinct witness-MS count** (`n_ms`), top **200** taken.
3. **Auto-label attempt** per unit, reusing `frag_tail_catalog_check.py` (B3)'s scoring machinery UNMODIFIED (`is_informative_title`, `content_weight`, `labels_equiv` — itself built on `track1_bib.title_bucket2`'s acronym/translation equivalence tables; `frag_tail_catalog_check.py`/`track1_bib.py` are NEVER edited): one candidate label per distinct sys_id (its own low-confidence Track-1 label if the unit carries one for that member, else its best informative catalog title from libraries.csv/FJMS).
4. **Two additive, non-invasive fixes were required before the reused scorer produced trustworthy results at THIS scale** (B3 was tuned for 2-4 OLD members per motif and a Bible/Talmud-commentary corpus; this residue has 10-150+ candidates per unit and is liturgy/poetry/RNL-miscellany-heavy — full development trail with concrete before/after cases is in the script's comments):
   - **(a) clip + empirical DF-strip.** RNL/Bodleian catalog fields for composite prayer-book manuscripts enumerate MANY bound items in one string (unit 59088's sys_id 990053433000... catalog title alone lists 7 different Rosh-ha-Shanah piyyutim). The matching input is clipped to each candidate's leading 10 content tokens, then further stripped of tokens whose document frequency across this residue's OWN candidate pool exceeds 2% (an empirically-derived, data-driven generalization of B3's hand-curated `EXTRA_GENERIC` for THIS corpus's liturgy/poetry/archival-classification vocabulary — top stripped tokens: פיוט, קטע, בן, מקרא, קטעים, פרוש, תרגום, קראים, טקסט, כתאב, תפילה, על, ערבי, וברכות, מנהג; 46 tokens total, see run log). Without this, 175/200 units falsely 'AUTO-LABELED' by generic liturgical-genre overlap alone (verified by hand on unit 59088: candidates visibly include BOTH "דיואן ר' יהודה הלוי" (Yehuda ha-Levi's Diwan) and "סדור מנהג קראים" (a Karaite siddur) — unrelated works).
   - **(b) non-transitive ANCHOR clustering, not `UF`.** At n=50-150+ candidates, B3's transitive union-find chain-collapses unrelated works through a handful of false pairwise edges (spot-checked: pre-fix, unit 59088's 84 candidates merged ~90% into one component under `UF`). Switched to a one-hop STAR: the ANCHOR = the candidate with the most direct (non-transitive) `labels_equiv` edges; cluster = anchor + its direct neighbors only. The representative label is the ANCHOR ITSELF, not a re-pick favoring Track-1-sourced members of the cluster — an earlier version of this script did that re-pick and it surfaced a genuine bug (unit 9140: a single spurious edge from a shared-GIVEN-NAME collision — `יצחק` is both Rashi's patronymic, שלמה בן יצחק, and Rif's given name, יצחק אלפסי, so `track1_bib`'s author-acronym equivalence table linked them — displayed "Rashi's Torah commentary" when the actual, best-supported (15/26 direct edges) identity was "הלכות הרי"ף"/Alfasi's Halakhot on Shabbat, which the underlying text verified as correct). **This exact false-edge mechanism (rare common-name collisions in the acronym-equivalence table) is a real, NOT fully fixed residual limitation of reusing `labels_equiv` unmodified at this scale — documented, not patched, consistent with the brief's reuse-don't-reinvent instruction.**
5. **Convergence rule: AUTO-LABELED iff >=2 candidates exist and the anchor cluster is a STRICT MAJORITY (>50%) with >=2 members** — 'a meaningful fraction of members' catalogs converge on the same work'. SUGGESTIVE = some catalog signal but no majority convergence. NO-CATALOG = zero informative candidates anywhere in the unit.

## Headline split

- **(a) Auto-labeled by catalog** (Track-1 reference-gap: the work exists in catalogs/bibliography but not in Track-1's reference corpus): **90** / 200 of the top units.
- **(b) Truly unidentified** (discovery queue: no catalog convergence, incl. SUGGESTIVE + NO-CATALOG): **110** (110 SUGGESTIVE + 0 NO-CATALOG).

## The 70%-RNL Karaite-liturgy hypothesis — full-scale check

Computed over the FULL 73,101-unit residue (not just the top 200): 39,084 distinct witness MSS, 266,871 member-page-rows (one row per unit×page a MS participates in — a MS that recurs across many different residue units contributes one row per unit).

- **By distinct witness MS: RNL = 24.5%** (9,585 / 39,084). CUL is actually the plurality library by this measure (15,982, 40.9%).
- **By member-page-row (occurrence count): RNL = 75.1%** (200,323 / 266,871) — THIS is where the ~70% figure lives.
- **Verdict: the 70% signal is real but is a concentration effect, not a distinct-manuscript effect.** Only 9,585 distinct RNL MSS are involved (24.5% of witnesses), but a handful of them recur across hundreds of DIFFERENT residue units each — e.g. sys_id 990001538710205171 (`Ms. EVR ARAB I 2064`, catalogued "תרגום ופרוש ערבי לתורה לישועה בן יהודה (דברים)" — Yeshua ben Yehuda's Judeo-Arabic Torah commentary) alone touches 535 distinct residue units. The top RNL contributors checked by hand are large Judeo-Arabic Karaite Bible-commentary/philological codices (al-Qirqisani's כתאב אלאנואר ואלמראקב, ibn Janah's ספר השרשים/כתאב אלאצול, Yosef ben Noah's Torah commentary, מדרש דוד) — running discursive commentary prose whose surrounding Arabic argument never matches the reference corpus (only its embedded Bible citations do), so it fragments into hundreds of small residue units per codex. Genuinely liturgical Karaite items also appear in the top-30 by witness count (units 666840 "פיוטי מאורה ואהבה", 695000 "קדושת היום במוסף לרגלים") but the RNL page-row mass is dominated by **commentary/philology, not liturgy specifically** — the hypothesis should be re-stated as 'RNL's uncatalogued Karaite philological corpus', not 'Karaite liturgy'.

## Library distribution, full residue (by distinct MS)

- CUL: 15,982 (40.9%)
- RNL: 9,585 (24.5%)
- JTS: 4,835 (12.4%)
- Oxford: 3,556 (9.1%)
- BL: 2,377 (6.1%)
- AIU: 737 (1.9%)
- Mosseri: 400 (1.0%)
- Strasbourg: 371 (0.9%)
- Manchester: 313 (0.8%)
- Katz: 197 (0.5%)
- HAS: 172 (0.4%)
- HUC: 141 (0.4%)

## Library distribution, full residue (by member-page-row)

- RNL: 200,323 (75.1%)
- CUL: 33,625 (12.6%)
- JTS: 9,971 (3.7%)
- BL: 7,228 (2.7%)
- Oxford: 5,906 (2.2%)
- AIU: 3,297 (1.2%)
- Mosseri: 986 (0.4%)
- Katz: 920 (0.3%)
- Manchester: 741 (0.3%)
- Strasbourg: 722 (0.3%)
- HUC: 555 (0.2%)
- HAS: 525 (0.2%)

## (a) Auto-labeled by catalog — the reference-gap list

Ranked by witness-MS count. `cluster/candidates` = majority cluster size / total candidates scored.

| unit | MSS | pages | med len | auto-label | source | cluster/cand | libs (by MS) |
|---|---|---|---|---|---|---|---|
| 1430332 | 97 | 628 | 588 | שרח אלאלפאץ (קטעים) : ; לתורה ולנביאים. | catalog | 56/93 | RNL:63, Oxford:20, CUL:7, JTS:3 |
| 2201742 | 62 | 148 | 400 | היסטוריוגרפיה ותאורים גיאוגרפיים;סיפורת וספרות יפה;ספרות יפה. ; קצת חנ | catalog | 51/59 | RNL:18, Oxford:14, JTS:13, CUL:10 |
| 1648364 | 54 | 296 | 680 | דקדוק;כתאב אלאפעאל ד'ואת חרוף אללין;כתאב אלמסתלחק;מילונים. ; ספר שרשים | catalog | 33/50 | RNL:36, CUL:11, JTS:4, HUC:2 |
| 640680 | 53 | 119 | 280 | פיוטים לסוכות ולשמחת תורה, כמנהג קראים. | catalog | 13/25 | RNL:45, CUL:6, Oxford:1, BL:1 |
| 604138 | 50 | 161 | 256 | פיוטים לסוכות ולשמחת תורה, כמנהג קראים. | catalog | 15/24 | RNL:45, CUL:2, JTS:2, Oxford:1 |
| 1157648 | 48 | 162 | 610 | תרגום ערבי לכתובים לרס"ג (תהלים א). | catalog | 31/42 | RNL:40, BL:5, InstFrance:2, JTS:1 |
| 1046324 | 48 | 56 | 834 | תעודות אישיות ושטרות. ; שטר בעניין "יעקב חסון... שקיבל מיד... מאיר ן'  | catalog | 19/30 | CUL:12, AIU:11, BL:9, JTS:7 |
| 499884 | 47 | 80 | 219 | הלכה;הלכה- גאונים. ; הלכות שחיטה בערבית-יהודית בדיקת הסכין; שחיטת הסימ | catalog | 31/46 | CUL:22, BL:7, JTS:5, Oxford:4 |
| 1194766 | 45 | 344 | 817 | תרגום ופרוש ערבי לנביאים (ישעיה) וכתובים (דניאל). | catalog | 24/36 | RNL:39, JTS:4, CUL:2 |
| 29572 | 44 | 103 | 404 | תחכמוני (בלתי שלם). | catalog | 17/26 | RNL:22, CUL:12, JTS:10 |
| 1123696 | 43 | 181 | 675 | פרוש נביאים לרד"ק (ישעיה). | catalog | 21/33 | RNL:31, CUL:6, JTS:3, BL:2 |
| 2254424 | 42 | 62 | 220 | הלכות הרי"ף; ספרות הלכתית ופרשנות תלמודית. ; Isaac Al-Fasi, Hilkhot ha | catalog | 25/42 | CUL:26, RNL:4, BL:4, Mosseri:2 |
| 555602 | 42 | 88 | 274 | שירת חול. ; Saadiah b. Joseph al-Fayyumi (Saadiah Gaon), Occasional pr | catalog | 17/21 | CUL:32, JTS:6, RNL:2, Sassoon:1 |
| 1628540 | 42 | 312 | 774 | חבור בדקדוק עברי : ; בערבית יהודית. | catalog | 21/41 | RNL:39, CUL:2, Mosseri:1 |
| 178254 | 41 | 120 | 485 | תרגום ופרוש ערבי לנביאים (ישעיה) וכתובים (דניאל). | catalog | 25/33 | RNL:37, CUL:4 |
| 601440 | 39 | 41 | 80 | מחזור מנהג קראים לשבת וליום כפור. | catalog | 9/17 | RNL:30, CUL:6, JTS:3 |
| 821214 | 39 | 97 | 298 | קינה לתשעה באב. | catalog | 20/28 | RNL:37, CUL:2 |
| 1544162 | 38 | 212 | 736 | תרגום ופרוש ערבי לתורה לישועה בן יהודה : ; הנוסח הקצר. | catalog | 11/18 | RNL:36, CUL:1, Oxford:1 |
| 1117104 | 38 | 64 | 483 | הפטרות. ; Haftarot: Jeremiah 14:1 – 2; Joshua 19:51 – 20:6[beg. & end  | catalog | 16/25 | RNL:29, Oxford:6, JTS:2, CUL:1 |
| 1597514 | 38 | 70 | 312 | פרשנות מקרא;שירת חול. ; Biblical Exegesis: Song of Songs 7:4 – 8:3 ; פ | catalog | 23/34 | RNL:16, CUL:14, JTS:2, Oxford:2 |
| 577308 | 37 | 93 | 564 | פיוט;שירת חול. ; Secular Poetry ; שלמה אבן גבירול, כתר מלכות | catalog | 14/24 | CUL:22, RNL:7, JTS:3, Katz:3 |
| 1552648 | 37 | 95 | 370 | שירים בעניני מוסר ודרשה. | catalog | 18/31 | RNL:37 |
| 575452 | 36 | 44 | 239 | תפלות ופיוטים וסליחות לשבת ומועד. | catalog | 6/10 | CUL:16, RNL:10, JTS:5, Strasbourg:2 |
| 1047252 | 35 | 277 | 619 | גלוסאר למקרא;חכמת הלשון;תפסיר אלאלפאט' אלצעבה. ; Philology ; Translati | catalog | 27/34 | RNL:28, CUL:4, AIU:2, JTS:1 |
| 1508444 | 35 | 108 | 593 | רס"ג, תהלים תרגום | track1 | 21/32 | RNL:30, CUL:3, JTS:1, Manchester:1 |
| 1638640 | 34 | 96 | 509 | גלוסאר למשנה;חכמת הלשון;כתאב אלאפעאל ד'ואת חרוף אללין. ; Bifolium 1: J | catalog | 22/33 | RNL:19, CUL:13, JTS:1, HAS:1 |
| 90144 | 34 | 436 | 689 | שרח אלאלפאץ (איוב, קטע). | catalog | 24/28 | RNL:34 |
| 576476 | 34 | 44 | 281 | פיוט. ; Piyyut ; פיוט (סליחות ליום הכיפורים): "הולכים בגיא צלמות" (משה | catalog | 4/5 | CUL:20, JTS:8, RNL:4, Oxford:1 |
| 1117174 | 33 | 46 | 304 | נביאים (יהושע כא:ו-כד:לב) : ; עם ניקוד. | catalog | 19/27 | RNL:24, CUL:6, JTS:1, Oxford:1 |
| 48276 | 33 | 151 | 366 | ספר הענק למשה ן' עזרא. | catalog | 21/30 | RNL:28, CUL:2, JTS:2, AIU:1 |
| 1145408 | 33 | 54 | 454 | תרגום ופרוש ערבי לנביאים (יחזקאל). | catalog | 12/23 | RNL:32, Oxford:1 |
| 529026 | 33 | 51 | 266 | נביאים וכתובים (הפטרות, רות, קטעים). | catalog | 20/25 | RNL:16, CUL:14, JTS:3 |
| 1116826 | 32 | 73 | 468 | תרגום ופרוש נביאים ליפת בן עלי (יהושע, קטעים). | catalog | 15/24 | RNL:24, CUL:6, JTS:1, Oxford:1 |
| 1430770 | 32 | 151 | 462 | שרח אלאלפאץ (קטע) : ; על שמות. | catalog | 24/31 | RNL:24, Oxford:4, CUL:3, JTS:1 |
| 2386688 | 32 | 38 | 93 | מעשה בית דין;נספחות לחיבור;רשימות;תעודות אישיות ושטרות. ; מעשה בית דין | catalog | 25/31 | CUL:15, Oxford:11, BL:3, RNL:1 |
| 852590 | 32 | 55 | 288 | Piyyut (Selihot for Yom Ha-Kippurim) ; פיוט (סליחות ליום הכיפורים) | catalog | 11/19 | CUL:17, Oxford:5, RNL:4, JTS:4 |
| 31936 | 31 | 395 | 1010 | תחכמוני (בלתי שלם). | catalog | 14/21 | RNL:24, CUL:4, JTS:3 |
| 108752 | 31 | 127 | 682 | מקאלה פי אלעריות [שלמה בן דוד הנשיא] | catalog | 18/31 | RNL:30, JTS:1 |
| 827890 | 31 | 42 | 206 | שירים ופיוטים למאורעות בחיי האדם, למועדים ולהזדמנויות שונות. | catalog | 7/13 | RNL:22, CUL:4, JTS:3, Oxford:2 |
| 1511646 | 31 | 99 | 573 | תרגום ופרוש ערבי לרס"ג לכתובים (תהלים, צד-צז; קטו-קטז) : ; עם כל המקרא | catalog | 24/30 | RNL:24, BL:3, CUL:2, JTS:2 |
| 13746 | 30 | 41 | 217 | תורה (בראשית כז:ז-ל:ז) : ; עם ניקוד וטעמים, מסורה קטנה וגדולה. | catalog | 19/21 | RNL:14, CUL:7, JTS:5, Oxford:2 |
| 141994 | 30 | 112 | 592 | כתאב אלתמייז [יוסף אלבציר];ספרות הגות (פילוסופיה, תיאולוגיה, מוסר);פיל | catalog | 17/27 | RNL:18, CUL:4, BL:4, JTS:3 |
| 402050 | 30 | 35 | 205 | פיוט. ; פיוט קינות לט" באב: "בליל זה הפקדתו ואת פשעי הריב ומאס הנקרב ו | catalog | 10/12 | CUL:13, JTS:8, RNL:2, Oxford:2 |
| 1638848 | 30 | 160 | 663 | חבור בדקדוק עברי. | catalog | 20/30 | RNL:29, CUL:1 |
| 662494 | 29 | 48 | 290 | הלכות הרי"ף;פירושי תלמוד בבלי. ; Isaac Al-Fasi, Hilkhot ha-Rif: Shabba | catalog | 25/28 | CUL:11, Oxford:8, RNL:4, Toronto:2 |
| 1199396 | 29 | 95 | 556 | תרגום ופרוש ערבי לכתובים (משלי, עזרא-נחמיה). | catalog | 17/23 | RNL:22, CUL:5, JTS:1, AIU:1 |
| 1141178 | 29 | 60 | 579 | הפטרות. ; Haftarot: Amos 2:14[forוישב יעקב]; Ezechiel 16:2 – 14; 28:24 | catalog | 20/26 | RNL:18, CUL:5, AIU:2, JTS:2 |
| 9140 | 28 | 34 | 325 | הלכות הרי"ף. ; Isaac Al-Fasi, Hilkhot ha-Rif: Shabbat 1 ; יצחק אלפסי,  | catalog | 18/26 | CUL:14, Oxford:6, RNL:5, Toronto:2 |
| 48518 | 28 | 127 | 414 | ספר הענק למשה ן' עזרא. | catalog | 17/24 | RNL:23, CUL:3, JTS:2 |
| 87088 | 28 | 248 | 687 | שרח אלאלפאץ (קטע) : ; על ירמיה. | catalog | 17/28 | RNL:26, CUL:2 |
| 330714 | 27 | 31 | 253 | כתובים (משלי ח:ד-כז:כב, קטעים) : ; עם ניקוד וטעמים, מסורה קטנה וגדולה. | catalog | 10/16 | RNL:14, CUL:9, JTS:2, Wallach:1 |
| 1578430 | 27 | 74 | 476 | חבור בדקדוק עברי בערבית (קטע). | catalog | 12/23 | RNL:18, BL:5, CUL:3, Westminster:1 |
| 425792 | 27 | 66 | 606 | פרוש התורה בערבית לאבו אלפרג' (בראשית-שמות). | catalog | 17/25 | RNL:23, CUL:3, Oxford:1 |
| 499528 | 27 | 51 | 489 | אוסף חבורים בערבית בעניני תיאולוגיה. | catalog | 9/16 | RNL:9, BL:7, CUL:6, AIU:4 |
| 480854 | 27 | 45 | 255 | הלכה;פיוט. ; אזהרות עם פירוש בערבית-יהודית ? | catalog | 10/17 | CUL:11, JTS:5, RNL:4, Oxford:3 |
| 1300216 | 27 | 73 | 350 | תוכן עשרת הדברות בחרוזים. | catalog | 9/17 | RNL:24, JTS:2, CUL:1 |
| 783666 | 26 | 47 | 305 | תרגום ופרוש ערבי לכתובים (קהלת). | catalog | 12/20 | RNL:19, CUL:5, BL:1, Oxford:1 |
| 84920 | 26 | 140 | 614 | גלוסאר למקרא;חכמת הלשון;תפסיר אלאלפאט' אלצעבה. ; Philology ; Translati | catalog | 22/26 | RNL:25, CUL:1 |
| 1431182 | 26 | 94 | 431 | שרח אלאלפאץ (דברים ונביאים, קטעים). | catalog | 16/23 | RNL:22, JTS:2, CUL:2 |
| 459768 | 25 | 39 | 468 | הלכות הרי"ף. ; Isaac Al-Fasi, Hilkhot ha-Rif: Shabbat 2:11 a – b[ספרד] | catalog | 15/22 | RNL:7, CUL:7, Oxford:5, JTS:4 |
| 20986 | 25 | 44 | 495 | כתאב ג'אמע אלצלואת ואלתסאביח; כתאב פי וג'וב אלצלוה; ספרות הלכתית ופרשנ | catalog | 21/25 | CUL:16, Oxford:4, BL:3, RNL:1 |
| 60792 | 25 | 56 | 372 | גלוסארים. ; גלוסר עברי-ערבי לצימודים מספר "ענק" למשה אבן עזרא (שני קטע | catalog | 15/20 | RNL:17, BL:3, JTS:3, CUL:2 |
| 1833848 | 25 | 113 | 598 | פרוש כתובים לסלמון בן ירוחם (קהלת). | catalog | 21/25 | RNL:22, CUL:2, BL:1 |
| 1580528 | 25 | 66 | 599 | מקרא [טקסט]; פרשנות מקרא. ; סלמון בן ירוחם, פרשנות מקרא: קהלת ז:ג; ז:כ | catalog | 19/25 | RNL:22, CUL:3 |
| 1452982 | 25 | 98 | 661 | פרוש כתובים לסלמון בן ירוחם (קהלת). | catalog | 16/23 | RNL:22, JTS:2, BL:1 |
| 822492 | 25 | 59 | 236 | פיוטים כמנהג קראים לשמחת תורה. | catalog | 8/11 | RNL:19, CUL:5, Oxford:1 |
| 2244628 | 25 | 29 | 149 | נספחות לחיבור;תעודות אישיות ושטרות. ; תעודות אישיות ושטרות: שובר ששילם | catalog | 15/22 | CUL:18, JTS:2, RNL:1, NLI:1 |
| 1158012 | 25 | 119 | 528 | פרוש כתובים ליפת בן עלי (תהלים). | catalog | 16/20 | RNL:24, BL:1 |
| 1117008 | 24 | 35 | 279 | תרגום ופרוש נביאים ליפת בן עלי (יהושע, קטעים). | catalog | 12/22 | RNL:20, JTS:2, CUL:2 |
| 478512 | 24 | 57 | 431 | זהר (ספר דברים, פרשת האזינו, רצב-רצט) . ; זוהר: דברים האזינו | catalog | 10/11 | BL:9, CUL:7, Oxford:7, Manchester:1 |
| 497364 | 24 | 55 | 428 | פרוש כתובים לסלמון בן ירוחם (קהלת). | catalog | 14/21 | RNL:17, BL:3, CUL:3, JTS:1 |
| 567122 | 24 | 34 | 251 | פיוט. ; פיוטים (קינות ?) 5 ע"ב: גירהא לר' יהודה זצ"ל: ירושלים האנחי וד | catalog | 7/10 | CUL:11, RNL:9, JTS:2, Warsaw:1 |
| 1117058 | 23 | 34 | 394 | ראב"ש, יהושע פירוש | track1 | 14/20 | RNL:17, CUL:3, JTS:2, Oxford:1 |
| 88366 | 23 | 152 | 458 | שרח אלאלפאץ (תהלים, קטע). | catalog | 19/21 | RNL:23 |
| 104286 | 23 | 89 | 708 | ספר מצוות [יפת בן דוד אבן צגיר] | catalog | 15/22 | RNL:23 |
| 176402 | 23 | 23 | 111 | פיוט;תוספות של סידור;תפילה וברכות;תפילות קבע. ; Liturgical additions:  | catalog | 8/14 | CUL:11, RNL:6, Strasbourg:4, JTS:2 |
| 1633746 | 23 | 139 | 604 | שרח אלאלפאץ (תרי עשר ודברי הימים, קטעים). | catalog | 13/23 | RNL:22, Oxford:1 |
| 2141254 | 23 | 34 | 763 | תרגום אונקלוס;תרגומים ארמיים. ; Targum Onqelos: Leviticus 5:17 – 6:3 ; | catalog | 8/14 | CUL:19, RNL:1, Katz:1, JTS:1 |
| 29674 | 22 | 76 | 655 | תחכמוני (בלתי שלם). | catalog | 9/15 | RNL:13, CUL:3, Oxford:3, JTS:2 |
| 1452788 | 22 | 80 | 587 | תרגום ופרוש ערבי לכתובים (קהלת). | catalog | 13/18 | RNL:19, BL:1, JTS:1, CUL:1 |
| 559102 | 22 | 32 | 231 | הלכות הרי"ף;תלמוד בבלי. ; Isaac Al-Fasi, Hilkhot ha-Rif: Hullin 10 a – | catalog | 12/21 | CUL:11, JTS:5, BL:3, RNL:2 |
| 510474 | 22 | 28 | 187 | תפילה וברכות;תפילות קבע. ; תפילות קבע: שחרית חול תחנון "לבבי הרחיבו ממ | catalog | 4/5 | CUL:14, HUC:3, BL:2, JTS:1 |
| 668212 | 22 | 26 | 66 | פיוטים כמנהג קראים לסוכות. | catalog | 6/9 | RNL:19, CUL:2, Oxford:1 |
| 2422280 | 22 | 25 | 166 | פיוט;תפילה וברכות;תפילות קבע. ; Common Prayers: Shaharit Weekday Amida | catalog | 15/17 | CUL:14, JTS:6, RNL:1, NLI:1 |
| 50224 | 21 | 23 | 243 | מחבר לא ידוע — תלמוד בבלי, שבת | track1 | 11/17 | CUL:8, JTS:6, RNL:5, Oxford:1 |
| 1685960 | 21 | 58 | 535 | שרח אלאלפאץ (יחזקאל, קטע). | catalog | 14/21 | RNL:15, JTS:5, CUL:1 |
| 338750 | 21 | 69 | 1439 | שאלות ותשובות מאת שמואל בן משה המערבי. | catalog | 8/14 | RNL:21 |
| 556882 | 21 | 35 | 263 | הלכות הרי"ף;תלמוד בבלי [טקסט]. ; Isaac Al-Fasi, Hilkhot ha-Rif: Shabba | catalog | 19/21 | CUL:11, Oxford:3, RNL:2, AIU:2 |
| 1127012 | 21 | 27 | 143 | תרגום ופרוש ערבי לנביאים (ישעיה) וכתובים (דניאל). | catalog | 8/15 | RNL:18, CUL:3 |
| 1141908 | 21 | 43 | 436 | פרוש נביאים ליפת בן סגיר הקראי (יחזקאל). | catalog | 10/19 | RNL:21 |

## (b) Truly unidentified — the headline discovery list (top 60 by witness count)

| unit | MSS | pages | med len | verdict | weak T1 hint | catalog signal | libs (by MS) |
|---|---|---|---|---|---|---|---|
| 1405798 | 167 | 641 | 436 | SUGGESTIVE | — | 28/69 (מכתבים;סיפורת וספרות יפה;פיוט;שירת חול. ) | RNL:152, JTS:7, CUL:5, Oxford:2 |
| 59088 | 130 | 301 | 396 | SUGGESTIVE | — | 10/51 (פיוט. ; Piyyut ; פיוט: "[אברך את ד"]" (ר) | CUL:60, RNL:24, JTS:21, Oxford:9 |
| 567892 | 115 | 201 | 304 | SUGGESTIVE | — | 9/31 (פיוט. ; פיוט (בקשה ליום הכיפורים): "שמע ) | CUL:56, RNL:27, JTS:11, Oxford:7 |
| 490112 | 109 | 164 | 254 | SUGGESTIVE | — | 9/45 (דיואן ר' אברהם אבן עזרא.) | CUL:47, RNL:38, JTS:11, BL:6 |
| 666840 | 76 | 104 | 153 | SUGGESTIVE | יוסף אלברדאני — פיוטי מאורה ואהבה | 7/30 (מחזור מנהג קראים לימי חג וצום.) | RNL:65, CUL:9, JTS:2 |
| 568942 | 75 | 132 | 300 | SUGGESTIVE | — | 10/24 (פיוט;שירת חול. ; Secular Poetry: Dirges ) | CUL:41, RNL:16, JTS:10, BL:4 |
| 592464 | 60 | 180 | 293 | SUGGESTIVE | — | 7/23 (פיוטים למועדים מאת קראים ורבנים.) | RNL:55, CUL:3, JTS:2 |
| 303006 | 57 | 135 | 451 | SUGGESTIVE | — | 17/37 (מקרא ותרגומים;תפסיר ערבי;תפסיר רס"ג. ; S) | RNL:35, CUL:14, BL:6, Westminster:1 |
| 645786 | 54 | 79 | 233 | SUGGESTIVE | — | 5/19 (דיואן ר' יהודה הלוי (קטע).) | CUL:27, RNL:18, JTS:7, Oxford:2 |
| 107568 | 53 | 246 | 687 | SUGGESTIVE | מתרגם לא ידוע — ספר מצוות ללוי בן יפת הלוי, תרגום | 14/48 (כתאב אלאנואר ואלמראקב (קטע) : ; על ענין ) | RNL:49, CUL:3, AIU:1 |
| 603914 | 52 | 88 | 198 | SUGGESTIVE | — | 4/13 (פיוט;תוספות של סידור;תפילה וברכות. ; תוס) | CUL:28, RNL:13, JTS:9, Freer:1 |
| 2203182 | 52 | 113 | 332 | SUGGESTIVE | — | 4/13 (שירת חול. ; שיר הבנוי סביב סיפור יוסף) | RNL:48, JTS:4 |
| 695000 | 49 | 76 | 165 | SUGGESTIVE | מחבר לא ידוע — קדושת היום במוסף לרגלים | 11/25 (מחזור מנהג קראים לפסח (קטע).) | RNL:47, JTS:1, Manchester:1 |
| 130844 | 46 | 182 | 601 | SUGGESTIVE | — | 13/35 (ספר מצוות [יפת בן דוד אבן צגיר]) | RNL:46 |
| 1038702 | 46 | 58 | 216 | SUGGESTIVE | — | 3/14 (תרגום ופרוש ערבי לתורה לישועה בן יהודה () | RNL:24, CUL:13, AIU:4, JTS:2 |
| 1314378 | 45 | 129 | 416 | SUGGESTIVE | — | 7/25 (חבור במוסר (קטע) : ; בערבית יהודית.) | RNL:44, CUL:1 |
| 1143026 | 44 | 144 | 711 | SUGGESTIVE | — | 14/44 (חכמת הלשון;כתאב אלאפעאל ד'ואת חרוף אללין) | RNL:36, CUL:7, JTS:1 |
| 286716 | 41 | 54 | 195 | SUGGESTIVE | — | 6/16 (תרגום אונקלוס;תרגומים ארמיים. ; Targum O) | RNL:21, CUL:13, JTS:2, BL:2 |
| 81134 | 41 | 57 | 217 | SUGGESTIVE | — | 6/22 (ספר המצוות ללוי בן יפת : ; על נר שבת.) | RNL:38, CUL:3 |
| 569748 | 41 | 55 | 166 | SUGGESTIVE | — | 4/14 (פיוטים למועדים מאת קראים ורבנים.) | RNL:20, CUL:10, BL:3, JTS:3 |
| 146950 | 40 | 91 | 403 | SUGGESTIVE | מחבר לא ידוע — ברכת נישואין לאלמן ואלמנה | 8/36 (ספר מצוות [יפת בן דוד אבן צגיר]) | RNL:32, CUL:4, Oxford:2, AIU:1 |
| 494912 | 40 | 57 | 272 | SUGGESTIVE | — | 2/15 (פיוט. ; Piyyut ; פיוט: "יזכרו פלאך צבא מ) | RNL:17, CUL:13, BL:4, JTS:3 |
| 567674 | 40 | 74 | 375 | SUGGESTIVE | — | 4/13 (פיוט. ; Piyyut (Shevah): "בשם י"י ים סוף) | RNL:19, CUL:13, JTS:4, Oxford:2 |
| 592148 | 40 | 83 | 264 | SUGGESTIVE | — | 4/13 (פיוטים ותפלות כמנהג קראים.) | RNL:37, JTS:2, CUL:1 |
| 1082536 | 39 | 70 | 340 | SUGGESTIVE | רמב"ם, ספר המצוות | 2/14 (תרגום ופרוש ערבי לתורה לישועה בן יהודה () | RNL:23, CUL:9, AIU:3, JTS:2 |
| 1200272 | 37 | 45 | 142 | SUGGESTIVE | דניאל אלקומסי — פירוש למקרא, קטעים, קהלת | 4/9 (נביאים וכתובים (קטעים) : ; עם ניקוד וטעמ) | CUL:17, RNL:15, BL:1, Strasbourg:1 |
| 34690 | 36 | 104 | 1062 | SUGGESTIVE | יהודה אבן תיבון — ספר הרקמה ליונה אבן ג׳נאח, תרגום | 12/35 (ספר השרשים (הקדמה-או; אזן-איש). ; David ) | RNL:28, CUL:3, AIU:3, Strasbourg:1 |
| 60816 | 36 | 90 | 406 | SUGGESTIVE | — | 17/36 (גלוסארים. ; גלוסר עברי-ערבי לצימודים מספ) | RNL:29, CUL:2, JTS:2, BL:2 |
| 740590 | 36 | 40 | 109 | SUGGESTIVE | — | 4/18 (פיוטים כמנהג קראים לסוכות.) | RNL:30, CUL:6 |
| 307522 | 36 | 58 | 212 | SUGGESTIVE | — | 4/14 (מחזור מנהג קראים לימי חג וצום.) | RNL:36 |
| 493286 | 36 | 49 | 224 | SUGGESTIVE | — | 5/13 (תפלות ופיוטים וסליחות לשבת ומועד.) | CUL:14, RNL:8, JTS:6, BL:4 |
| 1139430 | 35 | 84 | 652 | SUGGESTIVE | — | 14/32 (תרגום ופרוש ערבי לנביאים (יחזקאל).) | RNL:29, CUL:3, JTS:1, ASL:1 |
| 162366 | 35 | 97 | 482 | SUGGESTIVE | — | 4/31 (הלכה;הלכה- ראשונים ואחרונים;סידור שלמה מ) | RNL:30, CUL:3, HAS:1, JTS:1 |
| 800350 | 34 | 55 | 293 | SUGGESTIVE | — | 2/10 (תורה (בראשית לה-במדבר לד) : ; עם ניקוד ו) | RNL:21, CUL:10, Allony:1, JTS:1 |
| 96118 | 34 | 43 | 255 | SUGGESTIVE | — | 8/20 (תרגום ופרוש ערבי לנביאים (ירמיה מו-סוף).) | RNL:28, CUL:3, JTS:1, Oxford:1 |
| 149924 | 34 | 180 | 693 | SUGGESTIVE | — | 11/30 (מכ'תצר כתאב אלמרשד [אליהו בן אהרן אבן עב) | RNL:34 |
| 522514 | 34 | 77 | 442 | SUGGESTIVE | מחבר לא ידוע — תנחומא | 3/11 (קצור פרוש אבו אלפרג' הרון אבו אלפרג' לתו) | RNL:26, CUL:7, Mosseri:1 |
| 61118 | 33 | 65 | 231 | SUGGESTIVE | — | 7/14 (פיוט. ; שלמה אבן גבירול, פיוט (אזהרות, ש) | CUL:24, RNL:5, AIU:1, Katz:1 |
| 328936 | 32 | 39 | 186 | SUGGESTIVE | — | 6/21 (נביאים ראשונים (קטעים) : ; עם ניקוד וטעמ) | RNL:19, CUL:8, Oxford:2, AIU:1 |
| 1062944 | 32 | 95 | 629 | SUGGESTIVE | מחבר לא ידוע — תנחומא | 4/23 (פרוש התורה ונביאים ראשונים.) | RNL:26, BL:2, CUL:2, AIU:1 |
| 144144 | 32 | 40 | 251 | SUGGESTIVE | — | 4/12 (זמירות לשבת.) | RNL:14, CUL:11, BL:3, Mosseri:1 |
| 563898 | 32 | 36 | 190 | SUGGESTIVE | — | 2/10 (פיוט. ; Piyyut ; פיוט: "- - - - שלום אין) | RNL:13, CUL:12, JTS:5, Mosseri:1 |
| 757124 | 32 | 41 | 99 | SUGGESTIVE | — | 6/16 (הלל לשבת שלפני פסח, מנהג הקראים.) | RNL:28, Oxford:2, JTS:1, CUL:1 |
| 233714 | 31 | 79 | 444 | SUGGESTIVE | רשב"ח, פירוש התורה לרשב"ח | 5/22 (תפסיר תורה צוה לנו.) | RNL:21, CUL:6, JTS:2, Oxford:1 |
| 1068102 | 30 | 49 | 374 | SUGGESTIVE | רס"ג, שמות פירוש | 7/21 (תרגום ופרוש ערבי לתורה לישועה בן יהודה () | RNL:22, CUL:4, JTS:2, Katz:1 |
| 329972 | 29 | 34 | 236 | SUGGESTIVE | — | 9/20 (נביאים (שופטים-מלכים, קטעים).) | RNL:14, CUL:12, JTS:3 |
| 1575784 | 29 | 64 | 511 | SUGGESTIVE | — | 7/28 (שרח אלאלפאט' אלמתג'אנסה פי אלענק לר' משה) | RNL:27, CUL:1, HAS:1 |
| 106378 | 29 | 86 | 613 | SUGGESTIVE | — | 12/27 (ספר מצוות [יפת בן דוד אבן צגיר]) | RNL:28, JTS:1 |
| 161642 | 29 | 82 | 600 | SUGGESTIVE | — | 5/23 (ספר המצוות ליפת בן עלי : ; על עניני הלוח) | RNL:29 |
| 327908 | 29 | 38 | 301 | SUGGESTIVE | מחבר לא ידוע — סדר עולם רבה | 11/22 (תרגום ופרוש ערבי לכתובים (שיר השירים, רו) | RNL:13, CUL:11, JTS:2, Manchester:1 |
| 568028 | 29 | 43 | 277 | SUGGESTIVE | נתנאל בן פיומי, גן השכלים | 3/9 (פיוט. ; Piyyut ; פיוט: "[יום נכספו נפשים) | RNL:12, CUL:8, JTS:7, BL:2 |
| 633294 | 29 | 47 | 266 | SUGGESTIVE | — | 4/15 (פיוטים ושירים לשבת, למועדים ולהזדמנויות ) | RNL:22, CUL:3, Oxford:2, HUC:1 |
| 156426 | 28 | 89 | 600 | SUGGESTIVE | דאוד אלמקמץ, עשרים מאמרים | 11/28 (כתאב אלאנואר ואלמראקב (קטע) : ; על ענין ) | RNL:25, CUL:2, JTS:1 |
| 1082678 | 28 | 42 | 333 | SUGGESTIVE | — | 3/11 (תרגום ופרוש ערבי לתורה לישועה בן יהודה () | RNL:20, CUL:7, Strasbourg:1 |
| 1094720 | 28 | 54 | 596 | SUGGESTIVE | — | 6/20 (מקרא [טקסט];תרגום אונקלוס;תרגומים ארמיים) | RNL:22, CUL:3, JTS:2, Mosseri:1 |
| 11508 | 27 | 34 | 228 | SUGGESTIVE | בחיי, תורת חובות הלבבות | 6/16 (משנה [טקסט];ספרות הלכתית ופרשנות תלמודית) | CUL:13, RNL:10, BL:2, Oxford:1 |
| 1552276 | 27 | 78 | 482 | SUGGESTIVE | — | 7/21 (תרגום ופרוש ערבי לתורה (שמות).) | RNL:27 |
| 524740 | 27 | 52 | 335 | SUGGESTIVE | — | 3/14 (תרגום ופרוש ערבי לתורה לישועה בן יהודה.) | RNL:17, CUL:7, AIU:2, JTS:1 |
| 800720 | 27 | 40 | 190 | SUGGESTIVE | — | 6/13 (משה בן נחמן, פירוש הרמב"ן לתורה: ויקרא ב) | RNL:13, CUL:9, JTS:2, AIU:2 |
| 569802 | 27 | 41 | 248 | SUGGESTIVE | — | 3/10 (שירים ופיוטים למאורעות בחיי האדם, למועדי) | RNL:10, CUL:9, JTS:4, HUC:2 |

## Evidence cards — top 20 truly-unidentified units (text snippets)

### 1. unit 1405798 — 167 MSS, 641 pages, med 436 letters — SUGGESTIVE
- catalog candidates: 69 candidates ({'catalog': 69}): שרוט אלדבאחה [אליהו בן אהרן אבן עבד אלולי] [catalog:990000853270205171]; קבץ. [catalog:990001430270205171]; גירסה ערבית לספור אסתר. [catalog:990001438060205171]; כלי נגינה שהוזכרו בתהלים. [catalog:990001438070205171]; מגלת אסתר בשירה. [catalog:990001531610205171]; קצת אסתר (קטע). [catalog:990001556370205171] … +63 more
- libraries (by MS): RNL:152, JTS:7, CUL:5, Oxford:2, Mosseri:1
- [Ms. EVR ARAB II 2014 (RNL) p.5](https://genizahsearch.com/browse?sys_id=990001612340205171&page=5) — cat title: *ספור על אסתר.*
  > אכתיארי אדא הוא חפט רוחי מן אלאצ̇רארי אדא כנת אסרפת עלו אלמות בגיר הדא מצי ואלמלך אזרשידי אקאם המן להו וזירי . וכאן המן רדי שרירי מנזלה צאחב חיילא כאפר כנזירי תם רפע אלמלך קדרהו ושאני וצארס אלסלטאני וקד חכם פי אלדיואנא וחול הו אלחג̇אב ואלגלמאנא ואן טלע פי קנצא וצידי וחול הו אלראיאת ואלבנודי תם תגיה סאיר אלגנודו. ומעהם אלבאזאת ואלפהודי ואן עבר במגלסא והם קעודי קאמולה בלהם סגודי' ומרדכי אן ראה קד קד…
- [Ms. EVR ARAB II 2686 (RNL) p.5](https://genizahsearch.com/browse?sys_id=990001618680205171&page=5) — cat title: *ספור על אסתר.*
  > י מו יקאסי אלאחזאן ואלהמומו סבראן לא יצחא מן אלגמומו ואלדמנ מן ניונהו הגימו ראיתהו יא כתבאכי אלעין ופי צדרהי ידק באלידין יחולהו כג מליחא זין עליה מו מן אלתיאב אלכשין קד לבהו מן אלתיאב אלהודו ואלקום טוב לילה מו קעודו ישכון לרבהמו אלמעבודו וימרגי קראמו אלכדודו פאנכדת תקול להו פי אלחאלא אנש הוא אלדי חוגה לדלפעלא ואפתרא עליך באלמחאלה פכברעי בצחיח אלכבארא מא אלדי קד כאן ואיש הוא אלדי גרא ומן עלי אלניס …

### 2. unit 59088 — 130 MSS, 301 pages, med 396 letters — SUGGESTIVE
- catalog candidates: 51 candidates ({'catalog': 51}): קובץ. [catalog:990000852000205171]; קובץ חבורי יוסף בן תנחום הירושלמי. [catalog:990000852110205171]; דיואן ר' יהודה הלוי. [catalog:990001428370205171]; פיוטים ותפלות כמנהג קראים. [catalog:990001434610205171]; דיואן ר' יהודה הלוי. [catalog:990001435030205171]; דיואן ר' יהודה הלוי. [catalog:990001435100205171] … +45 more
- libraries (by MS): CUL:60, RNL:24, JTS:21, Oxford:9, BL:8
- [Ms. EVR II A 105/01 (RNL) p.65](https://genizahsearch.com/browse?sys_id=990000852000205171&page=65) — cat title: *קובץ.*
  > מלכת אלכן לאזארכת הנל אלר יתאפק א ההצות פעתיק אחר זתותק זה אומר כבה וזה חזמר בבה אתה כעושה חלוכה שים משלו כך אין בהז לשכוך שיחוך התאות ותקביק נגאות ואתה תבקשלק נדולות אל תבקש כי את הכתוך החקש השלקפה להאיב ומצח להחם ראש תשא עעיק לשמים ולפקבין תסור ופרם ותכלתה אך דבר שפתים לשונך לכה ותפלתק לא זכה כנחנן הולה ובק בן תהלה בלשוכך תעלה ובקבך תבלי עשה מלאבת ה' רראה הסר מעלי המן שריך הגטוב רצח ומלוך שחוח ו…
- [Ms. EVR II A 100/01 (RNL) p.67](https://genizahsearch.com/browse?sys_id=990000852110205171&page=67) — cat title: *קובץ חבורי יוסף בן תנחום הירושלמי.*
  > שרלפניי נשוחת מתיך לא שמת ואחויך לא כגורות חזקונתם וקוצא מן החפשה והית נבדך לך לשלל בשובביך לקוטה רעד כתי מחניך פטחים על שתי הבעיפים נרפים זם כרפים השב יצרך אחותות והלך קחורנית וגער בשנון והר חיקהו וביצר הרש ואלתיקהו דהעגאת אשר תשאה לוכי לפתשך בא לבנות את אשר כליבך ועומה על ימינה לשטנץ בקתך הוא ישכי וממותבאל יום חושב מניו כגבור משביל לא ישוב הקם אתה תקוום והוא יפרנם עד יפשינוח ערום עסוב הזמן ותעצנ…

### 3. unit 567892 — 115 MSS, 201 pages, med 304 letters — SUGGESTIVE
- catalog candidates: 31 candidates ({'catalog': 31}): תפלות על דרך הקבלה. [catalog:990000617200205171]; בית זבול (קטע) : ; מפתח לפסוקי המקרא בתלמוד ובמדרשים. [catalog:990001104920205171]; דיואן ר' יהודה הלוי. [catalog:990001428370205171]; פיוטים כמנהג קראים. [catalog:990001433510205171]; תפלות ופיוטים. [catalog:990001433780205171]; תפלות ופיוטים וסליחות לשבת ומועד. [catalog:990001434030205171] … +25 more
- libraries (by MS): CUL:56, RNL:27, JTS:11, Oxford:7, BL:6
- [Ms. EVR II A 206 (RNL) p.48](https://genizahsearch.com/browse?sys_id=990001435030205171&page=48) — cat title: *דיואן ר' יהודה הלוי.*
  > דקח על ערן בות אלהים אחשם דוכב שמים לעזרת כמהים רמים יהודוהו ויעידו אבוהים רם על כל גויים יוי ברכי שוכנח בתי חומר ויסודה בשחקים את שם הנקדש בשבע כחיות הצדיקים שמורים מכלטוף ומכל חטא מנוקים שם יתנו צדקות ⟦יוי תמינ ברכי תלויה בימין עליון ונצורה כאישון את שם דעים ברוך בלב ומבורך בלשון תיכן נשמות עד האור הראשון תחלת דבריו יחיד השיבנו ↑ ומבור דלות העלינו האומר לחרם ולא יזרח באורך נהלינו שמך וכבודך אלנס…
- [Moss. II,225 (Mosseri) p.1](https://genizahsearch.com/browse?sys_id=990002016490205171&page=1) — cat title: *סדר סליחות.*
  > ברכי השלוחה לחכם לבות בני אדם תתשם השולחך למחית בשר ודםו המקדירם בהלקחך ושב וליסורים ועליך יזרח נסיב ברכי זכה בעד מחשדי גופ מאירה את שם זיהר העולם נאר ונורא וזקף שערי צדקויקיא דה השער וים יבדכר ברכיחיה בהרב מתה נצורהו את וסחי העולמים לאזר בגבו היזול ]מרחם מכבדי תורהה חן וכבוד ייי ברכר נברכי טהורה בעצם השמים צבאם אתשם טוב לקויו אשר לכבודו בראשי טרב יבינו הבינם וקראסיט עמו וראו כי טוב וטו בדכרו ברכי…

### 4. unit 490112 — 109 MSS, 164 pages, med 254 letters — SUGGESTIVE
- catalog candidates: 45 candidates ({'catalog': 45}): בית זבול (קטע) : ; מפתח לפסוקי המקרא בתלמוד ובמדרשים. [catalog:990001104920205171]; שירים ופזמונים : ; בעברית ובערבית. [catalog:990001399220205171]; דיואן. [catalog:990001428890205171]; קבץ פיוטים (קטע). [catalog:990001432790205171]; קבץ פיוטים (קטע). [catalog:990001432820205171]; דיואן ר' אברהם אבן עזרא. [catalog:990001434490205171] … +39 more
- libraries (by MS): CUL:47, RNL:38, JTS:11, BL:6, Oxford:4
- [Ms. EVR II A 206 (RNL) p.37](https://genizahsearch.com/browse?sys_id=990001435030205171&page=37) — cat title: *דיואן ר' יהודה הלוי.*
  > כי בכל צורת יצורים יש לאל חותם וטבע זה לעומת זה סדורים זהרי שבע לשבע כאשר שבע מאירות שם לשבע המנורות שם לעינים מאירות לחזות את המאורות ולה אהבה ידיד עליון שמע הגיון כאשר אשמיעך כחין מלין בדת תפלץ בתורת שעשועיך וארבעה לראש קבעה ונצרם לרגנייך ושליד יהי אחד ליד שמאלך מקבעך בחוחם על לבך כחותם על זרועיך הדר תסלין היוח אצולין ממותר לפי קדושים ותעמידם ותפרידם היות לארבעה ראשים וראשי שין מפורשין מרובעין מ…
- [Ms. EVR IV 1 (RNL) p.197](https://genizahsearch.com/browse?sys_id=990001966850205171&page=197) — cat title: *סדור מנהג אשכנז המזרחי לכל השנה.*
  > ת . אופך . לשמיכי יהו' לשון חזות אישור אשר יחזה פלאך כפי כוחו ישר' שיחו ולא כפי מוראיך . לך מעגל וכל גלגל . והוא לא ישאך . כל נוצר . ואם בבצר . מתחת לכסאך ומיפעלך יעיד לך לנגד כל ברואיך . ועדותם כי בראתם . לא ידעו איכה כראשו נים באחרונים יחד באימה שנים .. הוא אלאי/⟧ האהים . ואדוני האדונים . הזמנים . משתנים . והודו לא ישתנה הכיקרה שתים עטרה ומעלית לשבעה בונה . ובחכמה . כסיל וכימה . שבעה עולים בקנה …

### 5. unit 666840 — 76 MSS, 104 pages, med 153 letters — SUGGESTIVE
- weak Track-1 hint (low): *יוסף אלברדאני — פיוטי מאורה ואהבה*
- catalog candidates: 30 candidates ({'catalog': 29, 'track1': 1}): כתובים : ; עם ניקוד וטעמים, מסורה קטנה וגדולה. [catalog:990000991250205171]; תשבחות. [catalog:990001445480205171]; תפלה מנהג קראים לשבת פורים (קטע). [catalog:990001449580205171]; תפלות מנהג קראים לפסח ולשבועות. [catalog:990001455400205171]; מחזור מנהג קראים לסוכות. [catalog:990001456320205171]; תפלות מנהג קראים לימי צום. [catalog:990001456900205171] … +24 more
- libraries (by MS): RNL:65, CUL:9, JTS:2
- [The National Library of Russia Box O.63 (RNL) p.120](https://genizahsearch.com/browse?sys_id=997011946568005171&page=120) — cat title: *נביאים (יחזקאל יח:טז-יח:כה) : ; עם ניקוד וטעמים, מסורה קטנה וגדולה.*
  > אחדי כי עם בציון ישב בירו שלים בכה לא תבכה חנון וחנך לקול צעקיך כשמעתו ענך : והיה ביום ההוא אענה נאום ליג אענה את השמים והם יענו את הארץ : והארץ תענה את הרא ואת היצהר והם יענו את יזרעאל: וזרעתיה לי בארץ ורחמתי את לא רוחמה ואמרתי ללא עמי עמי אתה והוא יאמר אלהי: לאלהעונה אותו ביום צרתי ויהי עומדי בדרך אשר הלכתי: העניים והאביונים מבקשים מים ואין לשונם בצמא נשתה אני ייג אענם אלהי ישראל לא אעזבם: והיה …
- [Ms. EVR II A 2483 (RNL) p.26](https://genizahsearch.com/browse?sys_id=990001460080205171&page=26) — cat title: *מחזור מנהג קראים לראש השנה.*
  > מתל כל קבת אלי אן תנתהי מן אלשירה וקול מטרק כיעם בציון ישב בירושלם בכה לא תבכה חנון יחנך לקול צעקיך כשמעתו ענך והיה ביום ההוא אענה נאום יוי אענה את השמים והם יענו את הארץ . והארץ תענה את הרג̇ו ואת התירוש ואת היצהר והם יענו את הארץ : והארץ תענה את הראן ואת התי א ואת היצהר והם יענו את יזרעע: וזרעתיה לי בארץ וריחמתי את לא רוחמה ואמרת יעלא עמי עמי אתה והוא יאמר אלהי לאל העונה אותי ביום צרתי ויהי עמרי …

### 6. unit 568942 — 75 MSS, 132 pages, med 300 letters — SUGGESTIVE
- catalog candidates: 24 candidates ({'catalog': 24}): דיואן ר' יהודה הלוי. [catalog:990001428370205171]; דיואן. [catalog:990001428380205171]; קינות לתשעה באב. [catalog:990001428920205171]; דיואן ר' יהודה הלוי. [catalog:990001435030205171]; דיואן ר' יהודה הלוי. [catalog:990001435080205171]; קינות לתשעה באב. [catalog:990001441310205171] … +18 more
- libraries (by MS): CUL:41, RNL:16, JTS:10, BL:4, Mosseri:1
- [Ms. 4078.33 (Strasbourg) p.1](https://genizahsearch.com/browse?sys_id=990053953080205171&page=1) — cat title: *פיוט;שירת חול. ; Piyyut ; פיוט*
  > ה ואם תנים יהנוה בנותציון יקוננוה ואתם האזינוה היא השידה אשו מאז שדטה הינה היא וקוננוה . גלה כבוד מאוהליו ושמש זרחה ונודד . יילילו שירות היכליו כי מחמדם שורך . שבר זדע לוי כליו וכף יספוק ויהנודד . וכפוד דוד יענה מאליו היכה ישבה בדד . חסף אסף הו הו ואויה ויכה ירך בכפו . וידותון בתאניה ואניה למקדש חרב ספו . . ובני קרח יוסיפו בכיה על יום השך נשפו .. ובני משה בנהי נהיה תיכה יעיב באפו . איך היתה העזרה …
- [Ms. EVR II A 206 (RNL) p.67](https://genizahsearch.com/browse?sys_id=990001435030205171&page=67) — cat title: *דיואן ר' יהודה הלוי.*
  > ואליהי מקום לזעקתי דפא הלך לעטאל ⟦במ⟧ במלאכותו כי הסרו בריתי ויצמדו לבעלים ואלאים עזבו וביתו העם והמלך ושריו ושרי' יהודה לעומתו ורוח אלהים אז לבשה זכריה והחזיק בתומתו ויעל מעל לעם וכה אמר בענותו למה עברתם פי יוי ולא תצלחו בזולתו ואם ארום תקשרו עלי אני לא אעזוב צדקתי ⟦פא ויקשרו עליו קשה ואין איש משים על לבבו וכל איש בשרירואי לסקלו כמצית המלך וכיבו והיא מתחת יד הורגיו צועק ליואש במנאוכו יואש המלך זכ…

### 7. unit 592464 — 60 MSS, 180 pages, med 293 letters — SUGGESTIVE
- catalog candidates: 23 candidates ({'catalog': 23}): פיוטים למועדים מאת קראים ורבנים. [catalog:990001428390205171]; הוראות ותשבחות של קראים. [catalog:990001429560205171]; פיוטים ותפלות כמנהג קראים. [catalog:990001434610205171]; פיוטים מאת פייטנים קראים. [catalog:990001443740205171]; פיוטים מאת פייטנים קראים ורבנים. [catalog:990001443780205171]; פיוטים מאת פייטנים קראים ורבנים. [catalog:990001443800205171] … +17 more
- libraries (by MS): RNL:55, CUL:3, JTS:2
- [Ms. EVR II A 925 (RNL) p.13](https://genizahsearch.com/browse?sys_id=990001443740205171&page=13) — cat title: *פיוטים מאת פייטנים קראים.*
  > על יהם מלוך צור ושכלל דביר לאיתן ונעקד ו ותמים גביר ואתה יי לעקב אביר סלחנא ועו' וחאט העביר ומהר בעוזך חשיפת ארון למשמווגם אהרן פחז וקרב פחזוי יחירים עניים מיחדים לך ודורשים סליחה . . ואתה ייי ברוב גדלך הלח נא והושע נות חכלך ורחם שארית בני שומרון . למישמו וגם בהרן . ע'ה פחזו וקרב טזו התרחק ואריה ורוב אורבים וצואנך בתוכם אזי יושבים . ואתה ייי ולך אוהבים סלחנא וחנס והך אויבים וכלם בחרב עלי צוראן למש…
- [Ms. EVR II A 199/03 (RNL) p.62](https://genizahsearch.com/browse?sys_id=990001434610205171&page=62) — cat title: *פיוטים ותפלות כמנהג קראים.*
  > תפארה נכתרו כדינו ויכתוב משה התורה . מפי דר מעונו . פי בזכות פא יקרו מלספר מופתוו הם למאור עצומים נביא נורעים אותותיו בין כל היקומים אתחנן בטוב מרותוו לפני רם על רמים בזכותו יחיש לי עזרה עת אנעק באזנו .. ישמע נא קולי עת אקרא יפרני למענו כג בזכות פי נשלמה גירהא לר' שמואל סני נע' לחן רחום וחנון ילי .ו' אהללאל ממראה עינים נעלם . אדיר טורא הוצרים כלם . אמת מי הקשה ליו וושלם . אלהי עולם צ אלהי ברעתו פי…

### 8. unit 303006 — 57 MSS, 135 pages, med 451 letters — SUGGESTIVE
- catalog candidates: 37 candidates ({'catalog': 37}): מקאלה פי אלעריות [שלמה בן דוד הנשיא] [catalog:990000854780205171]; מקאלה פי אלעריות [שלמה בן דוד הנשיא] [catalog:990000854790205171]; תורה : ; עם ניקוד וטעמים, מסורה קטנה וגדולה. [catalog:990000989500205171]; כתאב אלמרשד [שמואל בן משה המערבי] [catalog:990001023110205171]; כתאב אלמרשד [שמואל בן משה המערבי] [catalog:990001023120205171]; כתאב אלמרשד [שמואל בן משה המערבי] [catalog:990001227420205171] … +31 more
- libraries (by MS): RNL:35, CUL:14, BL:6, Westminster:1, Schoeyen:1
- [Ms. EVR ARAB II 3359 (RNL) p.47](https://genizahsearch.com/browse?sys_id=990001624370205171&page=47) — cat title: *פרוש התורה (דברים, קטעים).*
  > המא מתפרדין וקאל לא תכשל ולם יקאל ולא תבשל לאנה מעני גיר מעני אלפצל ואנמא דכרה ליערף אן אפלה חראם מתל אכל אלשי אלחראם וקד קאל פי מוצ̇ע אכר לא תבשל גדי וג' בעקב קו' ראשית בכורי אדמתך פנקול אנה ישיר בה אלי אעטא אלבכור ללכהן פי אליום אלתאמן ולא יוכרה ולא יקול אני לא אעטיה ללכהן מהזולא בל אכלוה מע אמה חתי ידגן ויסמן וחיניד אעטיה . פקאל את לא ננצגה בל בן אמה יעני לא תכליה חתי ידאן פי לבן אמה והאהנא יפס…
- [Or. 5562D (BL) p.29](https://genizahsearch.com/browse?sys_id=990001233460205171&page=29) — cat title: *פרושים ותרגומים למקרא בערבית.*
  > ן אלגריב אלדכינעימא יאכל שואלה אלם אחללת ג̇צ̇בי פיה וקטעתה מן בין קומה כי לאן נפס אלבשריון אלדם מקא לדלך געלתה לכם עלי אלמדבא ליס תגברבה ען נפוסכם לאן אלדם כדאך סת עריע אללה עלקוי לבני אם כל אנסאן מנכם לאיאכסי מא חתי אלגריב ואלדכו ללא יאכל דמא ואיש ואי אנטאן מן בני אם ומן אלגריב אלדכל פי מא בינהם אצאדצירא מן אלוחש ואלטאיר מן אלוחש ואלטאיר אלדי וכלא חללא פליספך דמה וואריה אלתראב כי לאן נפום גמיע אל…

### 9. unit 645786 — 54 MSS, 79 pages, med 233 letters — SUGGESTIVE
- catalog candidates: 19 candidates ({'catalog': 19}): דיואן ר' יהודה הלוי (קטע). [catalog:990001432460205171]; קבץ פיוטים (קטע). [catalog:990001432820205171]; דיואן ר' יהודה הלוי. [catalog:990001435030205171]; דיואן ר' יהודה הלוי (קטע). [catalog:990001435070205171]; דיואן ר' יהודה הלוי. [catalog:990001441130205171]; שירים : ; שירים לחג וחתונה. [catalog:990001444120205171] … +13 more
- libraries (by MS): CUL:27, RNL:18, JTS:7, Oxford:2
- [Cambridge University Library Ms. T-S H 15.20 (CUL) p.1](https://genizahsearch.com/browse?sys_id=990051196710205171&page=1) — cat title: *פיוט. ; Piyyut ; פיוט: "[ערכו כיום רעיוני] ...-- והניפי שיר ומנעמיו" (רי"ה)*
  > בידו נפש כל חי פד יד אלקיכלדם ונעלה. ראש לכל סבה ועלילה ולחזותו עין כלה ומבשרי ללבינגלה אחזה שדי מנתחי פד המכונ[ ][ ] ערומה באטש משכלת חכמה וממאור נר [ ]מה זהרה לטוהר נעימה בה שנות ימי גם ירחי פז וענה [ ]ודה תאוה וכל נדודה כל יום אקוה יודי היותה קרבו צוה לער[ ]ואחוה מעשה יוצרה משבחי פד דורל[ ]רו יעידון מעשיו כי יכלון ויאכדון והוא לבדו ישאר וידון למתים בסתר יזידון ומזבול השקיף על ארחי פי לתך הם לעב…
- [Ms. EVR II A 614 (RNL) p.39](https://genizahsearch.com/browse?sys_id=990001441130205171&page=39) — cat title: *דיואן ר' יהודה הלוי.*
  > מאמ'ר כלי חומר אלי יוצרו מה[ שתיהו פגשתיהו למגדל עוז וצור הבהיר כאור מזהיר באין מסק ולא מכסה ישתבח יתפאר ויתרומם ויתנשא הדר כבודך ועוז ידך מספרים השמים בעת על תם ועת פנותם ועת מחותם אפים ומלאכים נהלכים ⟦נהלם ס בתוך אבני אשו מים יבנידות ויודות בורא ניב שפתים כי תסבול לא תבול בלי זרוע וידים תחתיות ועליוה והחיות והכסא ישתבח ומי ימלל כבוד מחולל שחקים באמונתו חי עולם אשר נעלם בגבהי רום מעונתו וברצותו ב…

### 10. unit 107568 — 53 MSS, 246 pages, med 687 letters — SUGGESTIVE
- weak Track-1 hint (low): *מתרגם לא ידוע — ספר מצוות ללוי בן יפת הלוי, תרגום*
- catalog candidates: 48 candidates ({'catalog': 46, 'track1': 2}): כתאב אלאסתבצאר : ; בערבית. [catalog:990000635780205171]; קובץ קראי. [catalog:990000850900205171]; מתרגם לא ידוע — ספר מצוות ללוי בן יפת הלוי, תרגום [track1:990000851150205171]; מקאלה פי אלעריות [שלמה בן דוד הנשיא] [catalog:990000854790205171]; כתאב אלאסתבצאר (קטע). [catalog:990000854850205171]; כלאם פי וג'וב אתכאד רווס אלשהור עלי אלרויא אלהלאל [catalog:990000855070205171] … +42 more
- libraries (by MS): RNL:49, CUL:3, AIU:1
- [Ms. EVR ARAB I 832 (RNL) p.83](https://genizahsearch.com/browse?sys_id=990000871090205171&page=83) — cat title: *כתאב אלאנואר ואלמראקב.*
  > שבו אחים אנמא אראד אכוה מן אלמשאחה ואעתלו פי דלך במא קדמנא דכרה מן קולה ערות אשת אחיך וג׳ ואן תחרים גמיע אלערוות תחרימא מוכדא אעני פי אלחלאה ואלמות גמיעא פאדא כאן דלך כדלך וכאן אלכתאב לא תגוז עליה אלמנאקצ̇ה וגב אן יכון אראד אכוה מן אלקבילה אד כאן אלכתאב קד אסמי אלאסראיל אכוה ואחתג בעצ̇הם איצ̇א באן קאל אן אלכתאב חרם מרה אלאך בקולה ערות אשת אחיך וחרם מדה אלאב בקולה ערות אשת אביך וחרם מרה אלאבן בקולה…
- [Ms. EVR ARAB I 3911 (RNL) p.170](https://genizahsearch.com/browse?sys_id=990000854790205171&page=170) — cat title: *אלמקאלה פי אלעריות.*
  > ז ואחד והו לא מחאלה מחאל פאדי תערי ען אלחיז פינבג אן יעלם אלמראד בקרצה ומא גרי מג̇ראהא לא ⟦/ מן אללפט̇ פלדלך קלנא אן קולהא ואנחנו יחרו אלדי יושבים מכתצר מנה עלם אלמראד בקולהא ואן זר אתנו במא יק' יקתרן בה מן קולהא בבית עלי אן תסלימנא למן כאלפנא אן ישיבת יחדו מפהום והו אלשרכה פי סכני אלמאוי חסב אסתדלאל צאחב כתאב אלאנואר בקולה וישב ישראל ויהודה לבטח או קול אלרבאטן ישיבה בעולם ותסלימנא איצ̇א אן אחים א…

### 11. unit 603914 — 52 MSS, 88 pages, med 198 letters — SUGGESTIVE
- catalog candidates: 13 candidates ({'catalog': 13}): דיואן. [catalog:990001428380205171]; תפלות ופיוטים וסליחות לשבת ומועד. [catalog:990001434030205171]; קינות על נפטרים ושירים. [catalog:990001444050205171]; פיוטים לחתונה, לברית מילה ולמועדים. [catalog:990001445140205171]; פיוטים וסליחות. [catalog:990001445550205171]; פיוטים כמנהג קראים (קטעים). [catalog:990001463760205171] … +7 more
- libraries (by MS): CUL:28, RNL:13, JTS:9, Freer:1, Mosseri:1
- [Ms. EVR II A 2402 (RNL) p.79](https://genizahsearch.com/browse?sys_id=990001459240205171&page=79) — cat title: *פיוטים.*
  > אחרו יה ממעון ⟦ש גיך וממכון היכליך צפה ברוב רחמיך למתחנין למוליך חסדיך יקדמוני ותשמע מיבוליה קוע תחנוני בשועי אליך יה כיום באשמורת שמעה קול הקורא ואם מלא מאשמים ולבו סורר ומורה יכול בארץ עד מקום אלו מורה ושלח לך בחמלתך לצדקה המורה ורום חנך עליו ממרום יי ערה ותחום עינך בנשאו את עיניו אליך טי מעלה צעקתי בצר לי תעלעד שמי תכלה ואם רבו חטאתי לאין קצה ולא פמלה זכור נאום נעקד לאביו לעורר עליו הן האש והעצ…
- [Ms. EVR II A 1089 (RNL) p.15](https://genizahsearch.com/browse?sys_id=990001445550205171&page=15) — cat title: *פיוטים וסליחות.*
  > כאשמורת לשפלך תחנות עניים ועמלם ראה אלאמונות דיין אלמנות ואבי יתומים ז וא פי מסריך הפלא[ ]ם יעלאל ומקדש עוז תכונן ומזבח אריאלי וקץ סתום תגלה בבוא הגואלי אלהי ישראלי הבה תמים ואל יקרב הרשום יהבכתב אמתי היות מלך וכהן שניהם להעמיתי ופלא כל יפרה וצעיר להצמית נא והוכה ומתי אין לו רמים וא' כלה נא מארום צבא והמון וכם שבפו מנחלי רת אמון . ואו על משפטו בארמון נא בצדקת אבהמון בא בימים ואל⟧ נא כשוב עמך אל יכ…

### 12. unit 2203182 — 52 MSS, 113 pages, med 332 letters — SUGGESTIVE
- catalog candidates: 13 candidates ({'catalog': 13}): ספורים ושירים על יוסף. [catalog:990001605240205171]; קצת יוסף. [catalog:990001610720205171]; קצת יוסף. [catalog:990001612000205171]; קצת יוסף. [catalog:990001612420205171]; ספור על יוסף : ; בצורת שיר. [catalog:990001613550205171]; ספור על יוסף : ; בצורת שיר. [catalog:990001614330205171] … +7 more
- libraries (by MS): RNL:48, JTS:4
- [Ms. EVR ARAB II 1321 (RNL) p.6](https://genizahsearch.com/browse?sys_id=990001605250205171&page=6) — cat title: *ספור על יוסף (קטע).*
  > אכונא מנא קאל יוסף אנקבר שפטנון אנבצר יא תרא איש אלכבר פי צדורהום דקו באלטוב וכל ואחד מנהום מלרוב. פעמנו אלצבי באלתחקיק בעד אן כאן אבה נשאענהום וראחו גלה באלתצדיק מני צדק כיף יקשעהום פלמא אתו ללצדיק ואכדו אלהראיאת מנהום חין נמר אכוה אבן אמו ואבוה ואליה קד מוה אשת על פי קלבו מלרוב וצאר דמעו פוקכדו פיסכוב וקאל ללדי פי אלמטל אדבחלי אנס מע כרפאן ואג̇מע אלצמיע פי קאעה והיו אלטאואם ללציפאן למא אקבל עליה…
- [Ms. EVR ARAB II 1708 (RNL) p.5](https://genizahsearch.com/browse?sys_id=990001609220205171&page=5) — cat title: *ספור על יוסף.*
  > עאלם באלאשיא מא אתעד מן עליה אתוכל וכבב לפרעון ראיא פיהא קד רהשי ותאמל תבעה בקראת מצריה ידעוני/ קרט אלמחפל וכבעה אכר בעדהם חצר שנעין אלמנטר אלחתן ענהם אנצוב ופיי אלראיא צאר מרעוב אם פר פה פנה למא אצבח מן מנאמו מראופץ נאדא לגמיעיי אלעאלם אבצרו לי חכים אן מערוף בשהח. אלמנאמאת עאלם בעקל אן ראיח יקולו צחיח ונט̇קו פציח רכבוה עלי אחתן מרכוב פהר . אלמנאס אלמצרטב יב. תב למא לאק עליה אלתפתיר קאל אנת אחד פי…

### 13. unit 695000 — 49 MSS, 76 pages, med 165 letters — SUGGESTIVE
- weak Track-1 hint (low): *מחבר לא ידוע — קדושת היום במוסף לרגלים*
- catalog candidates: 25 candidates ({'catalog': 24, 'track1': 1}): תורה : ; עם ניקוד וטעמים, מסורה קטנה וגדולה. [catalog:990000953740205171]; תורה : ; עם ניקוד וטעמים, מסורה קטנה וגדולה. [catalog:990000986220205171]; תורה (שמות לד:יד-לה:ל). [catalog:990001013370205171]; סדור מנהג קראים : ; כמנהג הקראים בדמשק. [catalog:990001428980205171]; מחבר לא ידוע — קדושת היום במוסף לרגלים [track1:990001442790205171]; תפלות מנהג קראים לערב פסח ושחרית של פסח. [catalog:990001455020205171] … +19 more
- libraries (by MS): RNL:47, JTS:1, Manchester:1
- [Ms. EVR II A 2905 (RNL) p.37](https://genizahsearch.com/browse?sys_id=990001464460205171&page=37) — cat title: *מחזור מנהג קראים (קטעים).*
  > צויתיך למועד חדש האביב פי בו וצאת ממצרים ולא יראו פני תקם : שלוש פעמים בשנה יראה כל זכורך אלפני האדון ילי : את חג המצות תשמור שבעת ימים תאכל מצות אשר צויתיך למועד חדש האביב כי בחכש האביב יצאת ממצרים שלאשו פעמים בשנה יראה לאכורך את פני האדון ולי אלהי ישראל כי אוריש גוים מפניך והרחב את ג̇בולך ולא יחמוד איש את ארצך בעלותך לראות את פני יוי אלריך שלוש פעמים בשנה . שלוש פעמים בשנה יראה כל זכורך את פני י…
- [Ms. EVR II A 857 (RNL) p.10](https://genizahsearch.com/browse?sys_id=990001442990205171&page=10) — cat title: *סדור מנהג קראים.*
  > אביב כי בו יצאת ממצרים ולא יראו פני ריקם שלש פעמים בשני יראה כל זכורך אל פני האדון יגי את חג המצות תשמור שבעת ימים תאכל מצות אשר צוותיך למועד חדש האביב פי בחדש האביב יצאת ממצרים שלש פעמים בשנה י יראה כל אבורך אלפני האדון יגי אלהי ישראל כי אוריש גוים מפניך והרחבתי את גבולך ולא יחמוד אין את ארצך בעלותך לראות את פני ייי אלהיך שלש פעמים בשנה: שלש פעמים בשנה וראה כל זכורך את פני יגי אלהוך במקום אשר יבח…

### 14. unit 130844 — 46 MSS, 182 pages, med 601 letters — SUGGESTIVE
- catalog candidates: 35 candidates ({'catalog': 35}): פרוש התורה לדוד בן בעז. [catalog:990000854620205171]; ספר מצוות [יפת בן דוד אבן צגיר] [catalog:990000854810205171]; ספר מצוות [יפת בן דוד אבן צגיר] [catalog:990000855570205171]; ספר מצוות [יפת בן דוד אבן צגיר] [catalog:990000855590205171]; ספר מצוות [יפת בן דוד אבן צגיר] [catalog:990000855640205171]; ספר המצוות ליפת בן עלי. [catalog:990000855840205171] … +29 more
- libraries (by MS): RNL:46
- [Ms. EVR ARAB I 716 (RNL) p.29](https://genizahsearch.com/browse?sys_id=990000855590205171&page=29) — cat title: *ספר המצוות ליפת אבן צגיר.*
  > ולא יטמיגירה פאן כאקאלגאלהעלי אלמשכב אלדי ללנדה מתצולא בשי אכר פדלך אלשי אלדי אתצל בה טמיאר הו וא סטו ביוהא יבין אלמשכב לאן אלנדה ומשכבה ומושבה ומרכבה יטמו בואסטה כמא הו מעלום מן קו' וכל הצאע במשכבה יכבס באדיו ונחזו דלך . פלדלה קאל יואם על המשכב הוא או על הכלי אשר היא יושבת עליו לאן הואראגב אלואלנוגע אללקדם דכרה פערף אן לא פדקבין אלנוגע אלש יעמקדם דכרה עלי אלמשכב ועלי אלכלי אלנגסי באלנדה ואין כונה…
- [Ms. EVR ARAB I 4408 (RNL) p.145](https://genizahsearch.com/browse?sys_id=990001561350205171&page=145) — cat title: *פרוש התורה לעלי בן סולימאן.*
  > לו ואגואה במעוי פאפ אלעקיו כאן בערת[ ]המא צאגעהא עלי אלונהין פליחצל חכם ]ניה ויכון תקדירה ותהי הקר צדתה תפשרח ]מו אכף נרתה אלתי יתעדא מנהא אליהפביין אנההא אן ילזה אלטמא סבעה איאם ואן ינאם מצועה קאל ]טעל אלסולין ליס פי דלך מא ידל עלי אן אללדם אלאול מזייה עלי אלתאנן ומא ואלאהמא פאלקול אלקול יקתצי אנה פ לא ינגם מן צאנעהא אלא בחית יחצל מן דם חיצהא   עליה ואלתאני יקתצי אנה יננס ואן לם ימסרם נרתה נסמה ו…

### 15. unit 1038702 — 46 MSS, 58 pages, med 216 letters — SUGGESTIVE
- catalog candidates: 14 candidates ({'catalog': 14}): כפאיה אלעאבדין. [catalog:990000862360205171]; תשובה בעניין חזקה. [catalog:990001508000205171]; תרגום ופרוש ערבי לתורה לישועה בן יהודה (ויקרא). [catalog:990001538720205171]; תרגום ופרוש ערבי לתורה לישועה בן יהודה (ויקרא). [catalog:990001538980205171]; תרגום ופרוש ערבי לתורה (ויקרא, קטע). [catalog:990001564680205171]; תורה (שמות-דברים). [catalog:990001566320205171] … +8 more
- libraries (by MS): RNL:24, CUL:13, AIU:4, JTS:2, Toronto:2
- [Ms. EVR ARAB I 59 (RNL) p.50](https://genizahsearch.com/browse?sys_id=990001522330205171&page=50) — cat title: *פרוש התורה ליפת בן עלי (ויקרא, קטעים).*
  > בל עלי חאלה תם יכרג פי אליוכל והדא אלשרט איצא עב פי ישראל אשתה ישראל וקד אכתצר ען דכרה פי פצל וכי ימוך אחיך עמך ונמכר לך ולם ידכר פי הדא אלפצל תלך אלמדכראת פי פצל ופי ימוך אחיך עמך ונמכר לך אעני לא תעבד בו עבדת עבד כשביור כתושב יהיה עמך בל דבר ויצא ביובל הוא ובניו עמו ודבר איצא לא ירדנו בפרך לעיניך פמכן אנה לא ימכנה אן ישתרא עלי אלאד אנה לא יעמל אעמאל אלעביד בל ישתרט עליה באשיא בעצ̇הא אלדכורה פי א…
- [Ms. EVR ARAB I 122 (RNL) p.199](https://genizahsearch.com/browse?sys_id=990001522790205171&page=199) — cat title: *פרוש התורה (קטעים).*
  > בל עלי חאלה תם יכרג̇ פי אליובל והדא אלשרט איצ̇א יג̇ב פי ישראלי אדא ישתרי ישראל [ וקד אכתצר ען דכרה פי פצל וכי ימוך אחיך שיר ונמכר לך ולם ידכר פי הדא אלפצל תלך אל מדכוראת פי פצל וכי ימוך אחיך עמך ונמכר לך אעני לא תע ברבו עברות עבוד כשכיר כתושב כל דכר ויצא ביובל הוא ובניו עמו ודכר איצא לא ירדנו בפרד לעיניך פימכן אנה לא ימכנה אן ישתרט עליף אלגר ואנה לא ויעמל א' אעמאל אלעביד כל ישתרט עליה באשיא בעצהא …

### 16. unit 1314378 — 45 MSS, 129 pages, med 416 letters — SUGGESTIVE
- catalog candidates: 25 candidates ({'catalog': 25}): מכתצר כתאב אלאנואר. [catalog:990000865930205171]; שרוט אלדבאחה [ישראל בן שמואל המערבי] [catalog:990000865990205171]; שרוט אלדבאחה [ישראל בן שמואל המערבי] [catalog:990000989740205171]; מסאיל וג'אואב (קטע). [catalog:990001525790205171]; חבור במוסר (קטע). [catalog:990001526650205171]; ספר המצוות (קטע). [catalog:990001527720205171] … +19 more
- libraries (by MS): RNL:44, CUL:1
- [Ms. EVR ARAB II 1005.7 (RNL) p.6](https://genizahsearch.com/browse?sys_id=990041043680205171&page=6) — cat title: *חבור במוסר (קטע) : ; בערבית יהודית.*
  > פתתל אנום קראמה אמואג אלנאר מתל תיאר אלבחאר ותצטף רבואת אלמלאיכה צפוף בין ירה וקוף פינצב כרסי אלעדל ויג̇לס עליה אלמלאך אלמוכל באלחכם פאול מא יקול אספו לי חסידיו כרתי בריתו עלי זבח : פתקבל בין ידיה יס()רי ארץ̇ עמורי תבל [/ אלאג̇רא אלמקדסין אלמטהרין מטהרין אלתחיר ואלתמגיד אבותינו אברהם ויצ̇חק ויעקב עאלם ומעהם אלסייד אלאפצל אלמכרם ואלאמאם אאכגר חגה אללה עלי כלקה לסאן שריעתה קטב חכמה אלחכמא שמתו עקל א…
- [Ms. EVR ARAB II 2664 (RNL) p.5](https://genizahsearch.com/browse?sys_id=990001618460205171&page=5) — cat title: *חבור תיאולוגי.*
  > תוני באהל אלסבת וא מלאיכתי פינאדו אין מן חפט אלסבת אין מן חפט סבות אללה ואשיאדה אין מן מאת עלי אלתוחיד פיחצ̇רו ג̇מיע אלדי פארקו אלדניא והם עלי דין משה בן עמרם עקש והם מזפופין בש מנאופה ופלע . רצא עליהם פענד מא יקרבו מן אלחצ̇רה [א קרסיה יכרגו אל אבות ושלתיו משה עליהם הש ילקוהם ויבשרוהם ויקולו להם אשריכם אשריכם וטוב לכם העשו פדו כבי לכם מן אלכיראת פיערצ̇ו ואחד ואחד ומע פל ואחד ידכלו אלאבות ואלסי משה…

### 17. unit 1143026 — 44 MSS, 144 pages, med 711 letters — SUGGESTIVE
- catalog candidates: 44 candidates ({'catalog': 44}): נביאים (קטעים) : ; עם ניקוד וטעמים, מסורה קטנה וגדולה. [catalog:990000571730205171]; כתאב אפעאל דואת אלמתלין [catalog:990000852800205171]; תרגום ופרוש נביאים ליפת בן עלי (יחזקאל, קטעים). [catalog:990001523260205171]; תרגום ופרוש נביאים ליפת בן עלי (יחזקאל ה-כז). [catalog:990001523280205171]; כתאב אלמסתלחק (קטע). [catalog:990001541220205171]; אלאפעאל דואת חרוף אללין (קטע). [catalog:990001541450205171] … +38 more
- libraries (by MS): RNL:36, CUL:7, JTS:1
- [Ms. EVR ARAB I 2440 (RNL) p.26](https://genizahsearch.com/browse?sys_id=990001542350205171&page=26) — cat title: *חבורים בדקדוק (קטעים).*
  > את אלמתלין ממא לא אפעאל להא ולא תצריך ] נבא פהמהם ען קולי פי צדר דלך אלכתאב אני לם אלזם נפסי אסתלחאק אלאסמא ]מעתלה ואלאסמא דואת אלמתלק אלתי לם ידכרהא אבו זכריא ממא לא תצריף להא אנמא אסתלחק ממא לם ידכרה אצלא מא וגדת לה פעלא ותצריפא אד הדא פא גזאה פי כתאביה אלא אנה נסי נפסה פי מואצע כתורה מנהמא פאדכל פיהמא אסמא נא ]פעאל להא מתלטריה ומסוה וצחיח סלע וקלת איצא פי גיר הדא אלמוצע מן צדר דלך אלכתאב ואמא א…
- [Ms. EVR ARAB I 4590 (RNL) p.9](https://genizahsearch.com/browse?sys_id=990001562850205171&page=9) — cat title: *רסאלה אלתנביה.*
  > עתלאל דאכל פי בעץ̇ אנואעה והאתאן אללפטתאן ומא גאנסהמא פממא לם יעתל פאוה אצלא ואמאא אסתלחקוה מן אלאסעאל אלתי פאוה יא פמתל בהתיחשם מתי הדים ולם יאבהו אלי קולו פי צחר דלך אלכתאב אגי לא אסתלחק מן אלאפעאל אלתי פאאתהא יא אלא מא כאן מעתלא ומא כאן אלאעתלאל לאזם לה פי תצריפה ואן לם יוגד פי אלמקרא מעתלא ובניה הדין אללפטתין גיר לחזם להא אלעל ואמא מא אסתלחקוה מן אלאפעאל אלדי עינהא אחד אחרף אלטלה פמתל מאם כי ג…

### 18. unit 286716 — 41 MSS, 54 pages, med 195 letters — SUGGESTIVE
- catalog candidates: 16 candidates ({'catalog': 16}): תרגום ופרוש ערבי לתורה לישועה בן יהודה. [catalog:990000853740205171]; תורה : ; עם ניקוד וטעמים, מסורה קטנה וגדולה. [catalog:990000989500205171]; תרגום ופרוש ערבי לתורה לישועה בן יהודה (במדבר). [catalog:990001538790205171]; תרגום ופרוש ערבי לתורה לישועה בן יהודה. [catalog:990001539220205171]; תרגום ופרוש ערבי לתורה. [catalog:990001563120205171]; תרגום ערבי לתורה. [catalog:990001564610205171] … +10 more
- libraries (by MS): RNL:21, CUL:13, JTS:2, BL:2, AIU:1
- [Cambridge University Library Ms. T-S NS 266.147 (CUL) p.2](https://genizahsearch.com/browse?sys_id=990051632920205171&page=2) — cat title: *(none)*
  > ל זרעיתהון לבית אבהתהון במניין שמהן מבר עסרין שנין ולעילא לגולגלתהון : במא דפקיד ייית משה ומגנון במדברא דסיני : והוו בני ראובן בוכרא דישראל תולדתהון לזרעית הון לבית אבהתהון במניין שמהן לגולגלתהון כל דבורא מבר עשרין שנין ולעילא כל נפיק חילא : מני נהון לשבטא דראובן ארבעין ושיתא אלפין וחמש מאה : לבני שמעון תולדתהון לזרעיתהון לבית אבהתהון מנינוהי במניין שמהן לגלגלתהון כל דבורא מבר עשרין שנין ולעילא כל…
- [Cambridge University Library Ms. T-S NS 266.185 (CUL) p.2](https://genizahsearch.com/browse?sys_id=990051633300205171&page=2) — cat title: *(none)*
  > תרגדם[ כמניין שמהן מבר עסרין שנין ולעילא לגולגלתהון ⟦/⟧ כמא דפקיד יי ית משה ומנבון במדברא דסיני ⟦/⟧ והוו בני ראובן בוכרא דישראל תולדתהון לזרעיתהון לבית אבהתהון במניין שמהן לגולגלתהון כל דבורא מבר עשרין שנין ולעילא כל נפיק הילא : מניינהון לשכטא דראובן ארבעין ושיתא אלפין וחמש מאה ⟦: לבני שמעון תולדתהון לזרעייתהון לבית אבהתהון מנינוהי בניניין שמהן לגלגלתהון כל דבורא מבר עשרין שנין ולעילא כל נפיק הילא…

### 19. unit 81134 — 41 MSS, 57 pages, med 217 letters — SUGGESTIVE
- catalog candidates: 22 candidates ({'catalog': 21, 'track1': 1}): ספר המצוות ללוי בן יפת. [catalog:990000854610205171]; ספר המצוות ללוי בן יפת (קטע). [catalog:990000860120205171]; ספר המצוות ללוי בן יפת : ; על נר שבת. [catalog:990000865500205171]; גן עדן. [catalog:990000877870205171]; סדור [ישעיה בן עזיה הכהן] [catalog:990000997210205171]; מחבר לא ידוע — קדושת היום במוסף לרגלים [track1:990001442790205171] … +16 more
- libraries (by MS): RNL:38, CUL:3
- [Ms. EVR ARAB I 930 (RNL) p.86](https://genizahsearch.com/browse?sys_id=990000854610205171&page=86) — cat title: *ספר המצוות ללוי בן יפת.*
  > אן יחצל לידי אכולה אסור ואלדיגרת אלעארה תנט̇יפה אלתיאב וגירהא פהו עלי סביל אלתנט̇יף לא לגהה אלנהי. וקד יגוז אנה דכלת אלשכהה עלי מן פעל דלך וט̇ן אנה מחט̇ורא. פאמא מן ירי אנה לא יגוז אלאנתסאע באלכמיר לקולה פיה ולא יאכל חמץ ויגוב לדלך נט̇איר פהו בעוד ⟦/⟧ לאנה קד קאל פי אלחיואן אלחראם לא יאכלו שקץהם וקאל שקץ הוא לא יאכל ויגוזלה ביעה ללגוים יעמל בה מא ישא. וליס פי קוה אללפט̇ מא יקתצ̇י אנה מן באע אלשי א…
- [Ms. EVR ARAB I 3920 (RNL) p.27](https://genizahsearch.com/browse?sys_id=990001556670205171&page=27) — cat title: *ספר המצוות ללוי בן יפת.*
  > ד בל אלאולי אנה ט' טעאם אלספר לאן אלמסאפר ליס יתמכן מן אלמקאם פי כמר ויכבז פי תנור בל יעגן ויכבזה מלה סרעה. וכדא אכבר ען אלכארגין אנהם בבזוה מלאת. וקד אטלקת הדה אללפט̇ה עלי אלספר לקולה ענה בדרך כחי וקאל דוד על' אלסל' והנה בעניי הכינותי לבות אלהיי פאן אייבן אן יעמלעאות פי אלסבעה אואם הו אולי. ישבה מא עמלה אלכארגין לקולה למען תזכר את יום צאתך מארץ מצרים כל ימי חייך: ובעץ̇ ועם אן כבז אלפטיר יסמא להם …

### 20. unit 569748 — 41 MSS, 55 pages, med 166 letters — SUGGESTIVE
- catalog candidates: 14 candidates ({'catalog': 14}): פיוטים למועדים מאת קראים ורבנים. [catalog:990001428390205171]; פיוטים, ברכה לחתן וסליחות. [catalog:990001434500205171]; פיוטים לתפלות השבתות המצויינות והמועדים ולהזדמנויות שונות. [catalog:990001443710205171]; פיוטים מאת פייטנים קראים ורבנים. [catalog:990001443770205171]; פיוטים כמנהג קראים. [catalog:990001444470205171]; שירים ופיוטים. [catalog:990001445710205171] … +8 more
- libraries (by MS): RNL:20, CUL:10, BL:3, JTS:3, Mosseri:2
- [Cambridge University Library Ms. T-S H 5.117 (CUL) p.1](https://genizahsearch.com/browse?sys_id=990051186940205171&page=1) — cat title: *פיוט. ; Piyyut*
  > בשמך רחמנא שער אשר נסגר קומה פתחיהו וצבי אשר ברח אלי שלחידם ליום בואך עדיי תלין בבין שרי ושם רוחך הטוב עלי תניחהם מזה דמות הודיך כלה יספיה אשר תאמרי אלי שלחת המהו ההוא יפה עין אדום וטוב רואי הודי ורעי זה קומה משחהו גדו שמעה אדו ⟧עולם שמעה תחינתי הכוז לפניך קטורת תפילתיו לבי בן חשק למהוד ולא ⟦יכפל יוכל להסתיה עדי ארקה חישקו בתי לה[ ] שדי אשר יקשיב להל ויעתר עד אן תהי רחוק מויותסתר: ליל ויום אעטוף א…
- [Cambridge University Library Ms. T-S NS 28.249 (CUL) p.1](https://genizahsearch.com/browse?sys_id=990051404470205171&page=1) — cat title: *(none)*
  > צור עד מתי השכח בניך . וקנא לשם קדשך : חמול על זרע אמוניך . כי סבלנו עול כפלים : צור מהר שלח לעמך את אליה נביאך . בשרם ממעונך . פעבור יונקי שרים : צור יה יתנדל שמך . ותשלח את משיחך . להושיע את עמך . כאבותינו במצרים : צורי אחר אמן אמן אמן שם נורא . אמן אמן אמן : אמן אל אדיר נורא ואיום לעמך הנה פריום . ויברך אתכם היום . ואמרו כל העם אמן : אמן אמן אמן אל אדיר ונורא יגדיל ויאדיר התורה . ויפתח לנו שערי…

## Manual sanity sample (10 units read by hand)

Read the actual page text (`pages`, via the same `norm_stream`/offset-projection helpers the pipeline itself uses) against the catalog evidence for 10 units spanning both verdicts and a range of witness counts. **Every one is a genuine shared passage** — no junk-leakage class found in this sample (cf. the already-fixed NLI-ownership-stamp junk class removed upstream in `passage_units.py`) — but the sample surfaced one *systemic*, reportable finding beyond the individual AUTO-LABELED texts:

1. **unit 9140** (28 MSS, AUTO-LABELED "הלכות הרי"ף" — Alfasi's Halakhot on Shabbat). Two Toronto MSS show near-IDENTICAL text ("מאימתי התחלת תספורת א'ר' אבין משיניח מעפורת שלספרים על ברכיו...") — textbook genuine duplication. Also the unit that exposed the representative-selection bug fixed above (see Method §4b).
2. **unit 2201742** (62 MSS, AUTO-LABELED "קצת חנה" — the Judeo-Arabic Tale of Hannah). CUL Ms. T-S Misc. 27.3.15 and RNL EVR ARAB II 1273 both open on the same narrative beat (a messenger scene: "...קלבו פאיית מכצור... יא ולד טיע לי..."), confirmed genuine parallel narrative prose.
3. **unit 1648364** (54 MSS, AUTO-LABELED a Hebrew-grammar/weak-verb treatise, "כתאב אלאפעאל דואת חרוף אללין" — this exact title also appears in B3's OWN report as a known verb-morphology treatise, independent cross-confirmation between the two spikes). Two RNL MSS share technical grammatical terminology ("...לאנך תקול צמת וצמתת באט̇האר אלתא...").
4. **unit 20986** (25 MSS, AUTO-LABELED a Judeo-Arabic prayer-book/Torah-reading-rules text; one member independently cataloged "סדור רס"ג" — Saadia Gaon's Siddur). CUL T-S H 18.20 and RNL Evr. Antonin B 184 carry VERBATIM-matching rules for which Torah portion to read when a festival coincides with Shabbat — a real, specific halakhic-liturgical text, absent from Track-1's reference corpus.
5. **unit 176402** (23 MSS, AUTO-LABELED "תוספות של סידור"/liturgical Psalm additions). Two RNL MSS (one cataloged "ספר המצוות ליפת אבן צגיר", one "סדור מנהג קראים") both quote Psalm 118 verbatim ("...כל גוים סבבוני... דחה דחיתני לנפול וה' עזרני...") — confirms the catalog convergence AND shows how a scriptural quotation embedded in two DIFFERENT larger works (a law-book and a prayer-book) still forms one legitimate reuse unit.
6. **unit 2141254** (23 MSS, AUTO-LABELED Targum Onqelos on Leviticus) — **the systemic finding.** Halper/Katz and CUL T-S NS 29.67 carry LITERAL Targum Onqelos Leviticus text ("...תרביא ניכסת קודשיא..."/"...קפא זייתי ית אשמית קדם דיי..."). This is canonical, well-known Aramaic Bible translation — yet it sits in the Track-1 residue. `passage_units.py`'s `CANON_CATS` (Track-1 label-propagation gate) is `{'Bible','Mishnah','Tosefta','Bavli','Yerushalmi'}` — **Targum is not in that set**, so Targum-category Track-1 spans (if Track-1 identifies Onqelos at all in its own pass) don't get the same-cautious 2-direct-label confidence treatment as other canonical categories, and evidently not enough direct Track-1 hits landed on these particular members to clear the `labeled=1` bar. **Some fraction of this 'unidentified residue' is canonical/classical text that Track-1 under-covers for structural reasons, not genuinely novel material** — a caveat for how 'discovery' should be read, distinct from true novelty. Recommend a follow-up: check whether ref_corpus.pkl includes Targum Onqelos at all, and if so why direct-label coverage is this thin.
7-8. **units 666840 (76 MSS) / 592464 (60 MSS)**, both SUGGESTIVE Karaite maḥzor material — RNL MSS verbatim-share Isaiah-paraphrase liturgical text ("...כי עם בציון ישב בירושלם בכה לא תבכה חנון..."); genuine, just short of majority catalog convergence (candidate titles split between "מחזור"/"פיוטים" variants that a stricter matcher correctly declines to force together).
9. **unit 303006** (57 MSS, SUGGESTIVE, Judeo-Arabic Torah/Deuteronomy commentary). RNL and BL snippets both read as genuine running Torah-commentary prose in the same idiom, though the two printed snippets land on different verses — a reminder that a unit's per-member snippet is illustrative evidence from that member's longest occurrence, not necessarily an aligned excerpt of the same exact sentence when the shared work is a long, continuous commentary.
10. **unit 50224** (21 MSS, AUTO-LABELED, Track-1-sourced weak label "תלמוד בבלי, שבת"). RNL and JTS (Adler 363) both carry genuine Bavli Shabbat menstrual-law discussion ("...ובמוך שבסנדלה... בעא מיניה ר' ירמיה מר' אבא...") — correctly identified even via the lower-confidence Track-1 route.

**Net: 10/10 sampled units are genuine shared passages** (no junk). The Targum-Onqelos case (#6) is the one class-level caveat worth carrying forward: not everything in this residue is novel/uncataloged — a slice is canonical text that Track-1's own coverage/confidence gates miss for structural reasons.
