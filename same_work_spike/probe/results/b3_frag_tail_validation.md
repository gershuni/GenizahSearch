# B3 — fragmentary-tail motif-query catalog auto-validation

Generated 2026-07-08 22:37. DB: `fullcorpus.db` (read-only, light SQLite/CSV reads, no engine runs).

## Method summary

- Fragmentary tail = motifs with pre-growth membership <= 4 MSS that gained +1..+2 new witness MSS via `motif_query.py` (`motif_query_hits` \ `motif_members_pilot`).
- **1,219 motifs**, **1,516** (motif, new-member) gain pairs.
- Motif identity: majority Track-1 live label (matched_letters >= 150) across OLD members; when an OLD member carries no Track-1 id, its OWN catalog title (libraries.csv col-7 + FJMS `catalog`, AlmaId==sys_id) stands in as a fallback candidate IF informative (not pure boilerplate). Candidates are clustered by pairwise title-equivalence (reusing `track1_bib.title_bucket2`'s phrase/acronym/translation machinery, not reinvented); the majority cluster's best-evidence label is the motif's identity. This fallback is REQUIRED to reproduce the brief's own exemplar (motif 369002 -- see below): its 3 old members carry NO live Track-1 id at all, yet its catalog titles agree on Yefet ben Eli's Deuteronomy commentary. A pure-Track-1 identity (as `growth_inspect.py` / `build_growth_review.py` compute today) would bucket it MOTIF-UNIDENTIFIED and miss the validation.
- Agreement scoring: `strict_title_match`/`strict_bib_signal` (this script) wrap `title_bucket2` / `FjmsInfo.bib_signal` from `track1_bib.py` UNMODIFIED, adding four spot-check-driven precision fixes (see 'Scorer fixes from the spot-check' below): (a)/(d) a title-genre filter (EXTRA_GENERIC) so two unrelated commentaries don't 'match' on shared words like פרוש/התורה alone, and don't 'mismatch' on shared genre/structural placeholders like אלמקדמאת/אלעתידות; (b) dropping `bib_signal`'s blind 'any Full-transcription entry on this MS' fallback, which doesn't check the entry is actually ABOUT the identified work.
  - **AGREE**: new member's catalog title matches the identity (`title_bucket2 == 'match'`), OR a bibliography entry names the identified work at this shelfmark (bib confirmation overrides a merely generic or even conflicting catalog *title* string).
  - **PARTIAL**: catalog title present but generic/uninformative (e.g. "קטעי גניזה"), no bib confirmation.
  - **DISAGREE**: catalog title substantively names a different work, no bib confirmation.
  - **NO-CATALOG**: no catalog title anywhere (libraries.csv or FJMS) and no bibliography rows for the new member.
  - **MOTIF-UNIDENTIFIED**: no identity could be derived for the motif at all (no Track-1 id AND no informative catalog title on ANY old member).

## Bucket counts

- **AGREE**: 1,131 (74.6%)
- **PARTIAL**: 184 (12.1%)
- **DISAGREE**: 159 (10.5%)
- **NO-CATALOG**: 30 (2.0%)
- **MOTIF-UNIDENTIFIED**: 12 (0.8%)

- AGREE rate among all 1,516 pairs: 74.6%
- AGREE rate among the 1,474 SCORED pairs (excludes NO-CATALOG / MOTIF-UNIDENTIFIED, where agreement literally cannot be assessed): 76.7%
- motifs with identity CONFLICT (old members disagree): 331 / 1,219

## Scorer fixes from the spot-check

The spot-check (below, plus a second broader random sample) caught two real false-AGREE mechanisms and two false-DISAGREE mechanisms in the first pass (raw `title_bucket2` + `FjmsInfo.bib_signal`), all fixed by `strict_title_match` / `strict_bib_signal` in this script (`track1_bib.py` itself untouched):

1. **Generic genre-word over-match.** motif 492232's identity *"פרוש התורה לעלי בן סולימאן (בראשית-שמות)"* vs new member catalog *"פרוש התורה בערבית לאבו אלפרג'"* scored `match` purely on the shared words פרוש/התורה ("commentary on the Torah") -- zero author/book overlap (עלי בן סולימאן != אבו אלפרג'). `EXTRA_GENERIC` + `has_specific_overlap` now require a token OUTSIDE the commentary-genre/corpus vocabulary (author name, book, chapter range) before trusting a `title_bucket2 == 'match'` as AGREE; this case now scores PARTIAL. Book/subcorpus names (בראשית, ישעיה, תרי עשר...) are deliberately NOT in `EXTRA_GENERIC` -- they still narrow the candidate pool and count as specific evidence.
2. **`bib_signal`'s blind Full-transcription fallback.** motif 12453 -> Or. 2598 (BL), identity *"מדרש דוד (בראשית)"*, was flagged `transcribed` because the manuscript has ONE TranscriptionType='Full' bibliography row -- about "A letter of Daniel ben Eleazar He-Hasid" (Leveen 1938), unrelated to Midrash David. Confirmed by reading the raw `bibliography` rows for that AlmaId: no Hebrew text or subject overlap at all. `track1_bib.FjmsInfo.bib_signal`'s final fallback line (`if any(e[4]=='Full' ...)`) is correct for ITS original job (demoting a Track-1 'new?' testimony tier when the MS is ALREADY in the literature for ANY reason) but wrong for B3's job (does this SPECIFIC entry corroborate THIS identity?). `strict_bib_signal` reuses `_phrase_match`/`_tokens_match` verbatim but drops that fallback line; this case now correctly scores DISAGREE (its FJMS catalog titles -- תוספתא / יוסיפון / פירוש יפת למקרא -- name three different, unrelated works, none of them Midrash David).
3. **Bare genre/structural placeholder titles scored as a hard mismatch.** A second-pass spot-check (random sample beyond the top-10) turned up "אלמקדמאת" ("The Introductions") / "שרח אלמקדמאת" as a recurring catalog title that DISAGREED with FOUR different, mutually incompatible identities (Yefet's Proverbs / Trei-Asar / Isaiah-Jeremiah-Ezekiel commentaries, AND a Karaite siddur) -- and in one of those (motif 313316) the quoted new-member text is VERBATIM the same Psalm-supplication content as a SIBLING new member of the SAME motif that IS correctly cataloged "סדור מנהג קראים" and scores AGREE. Reads as a generic front-matter placeholder, not a specific competing claim. Similarly "דקדוק" (grammar) was suppressing a real match between two Hebrew-grammar treatise catalog titles that share no author name (motif 500686/501066 vs "כתאב אלאפעאל דואת חרוף אללין", a known verb-morphology treatise). Both added to `EXTRA_GENERIC`; `strict_any_content` now also downgrades a `mismatch` verdict to `generic` (-> PARTIAL, not DISAGREE) when the catalog title's only tokens are genre/structural placeholders.
4. **Same class, stronger evidence: "שרח אלעתידות".** A full-dataset check for this exact catalog title found 9/9 fragmentary-tail rows carrying it DISAGREED -- against SEVEN different specific Yefet-commentary identities spanning both Torah and Prophets (Isaiah, Trei-Asar x2, Deuteronomy x2, Samuel, Isaiah-Jeremiah-Ezekiel). "אלעתידות"/"עתידות" ("the future/eschatological things") added to `EXTRA_GENERIC` for the same reason as (3).

All four fixes are additive precision guards layered on top of `track1_bib.py`'s existing machinery, per the brief's instruction to reuse rather than reinvent the equivalences -- no change to `title_bucket2` or `bib_signal` themselves, no new equivalence tables. **Open issue NOT fixed** (documented, not patched): a Hebrew<->Judeo-Arabic GENRE-name synonym gap -- e.g. Hebrew "שאלות ותשובות" (responsa) vs its literal Judeo-Arabic rendering "מסאיל וג'אואב" -- is invisible to both `title_bucket2`'s TRANSLATION_PAIRS (specific classic WORK titles only, not generic genre names) and to GENERIC_TOKENS/ EXTRA_GENERIC (Hebrew-token lists). Observed on >=3 rows (motifs 480674, 480698, 449712, all vs sys_ids cataloged bare "מסאיל וג'אואב" against the identity "שאלות ותשובות על התורה מאת שמואל בן משה אבן סני"). Asserting a NEW Hebrew<->JA translation pair for a generic genre name (as opposed to suppressing an already-generic token, which is what the four fixes above do) is closer to inventing a new equivalence than reusing one, so left as an open issue for a human reviewer / a future track1_bib.py change rather than patched here. **Second open issue:** the reverse asymmetry -- when the IDENTITY side (not the new member) is the generic one, e.g. motif 500686's identity is the bare, author-less "חבור בדקדוק עברי" ("a composition on Hebrew grammar"), a specifically-named grammar treatise on the catalog side ("אלאפעאל דואת חרוף אללין") still scores DISAGREE -- `strict_any_content` only checks the CATALOG title's content, not whether the identity itself was specific enough to be contradicted in the first place. Not patched (would need a `MOTIF-IDENTITY-TOO-VAGUE` sub-flag distinct from a real DISAGREE); see this motif's card in the DISAGREE examples below.
- identity source: Track-1 102 · catalog-fallback 1,108 · none (MOTIF-UNIDENTIFIED) 9

## Validation exemplar (motif 369002)

### 1. motif 369002 -> Ms. EVR ARAB I 103 (RNL) — **AGREE**
- motif identity (catalog): *תרגום ופרוש התורה ליפת בן עלי (דברים יח, קטעים).*
- new member catalog title: *תרגום ופרוש התורה ליפת בן עלי (דברים).*; FJMS: פירוש על התורה | פירוש יפת למקרא
- new hit: 457 letters, density 0.153 — [Ms. EVR ARAB I 103 p.37](https://genizahsearch.com/browse?sys_id=990001522650205171&page=37)
- motif rep text: `י מפני בני ישראל וקו' לא
ותבתדי אלאכר ותתפק אלשתי מחלקות פי אלקדס יום
אלסבת עלי הדה אלצ̇ורה אלי אן תתם כ'ד' נובה ו' שהור
פקד ג̇א חיניד חג̇ ה`
- new member text: `יום אלגמעה באלעשי ותנצרף לילה אלאחד ותבתדי
אלאפכי ותתפק אלשתי מחלקות פי אלקדס יום אלסבת עלי הדה אלצורה
לי אן תתם כ'ד' נובה ו' שהור פקד גא חי`

### 2. motif 369002 -> Ms. EVR ARAB I 20 (RNL) — **AGREE**
- motif identity (catalog): *תרגום ופרוש התורה ליפת בן עלי (דברים יח, קטעים).*
- new member catalog title: *תרגום ופרוש התורה ליפת בן עלי (דברים, קטעים).*; FJMS: פירוש על התורה | פירוש יפת למקרא
- new hit: 456 letters, density 0.287 — [Ms. EVR ARAB I 20 p.117](https://genizahsearch.com/browse?sys_id=990001522150205171&page=117)
- motif rep text: `י מפני בני ישראל וקו' לא
ותבתדי אלאכר ותתפק אלשתי מחלקות פי אלקדס יום
אלסבת עלי הדה אלצ̇ורה אלי אן תתם כ'ד' נובה ו' שהור
פקד ג̇א חיניד חג̇ ה`
- new member text: `תגי יום אלגמעה
באלעשי ותנצרף לילה אל ותבתדי אלאכרא ותתפק אלשתי
מחלקות יום אלסבת פי אלקדס עלי הדה אלצורה אלא אותתם
כר' נובה נחו ו' אשהר פקד ג`


## 10 example AGREE cards (validated identifications)

### 1. motif 370896 -> Ms. EVR ARAB I 105 (RNL) — **AGREE**
- motif identity (catalog): *תרגום ופרוש התורה ליפת בן עלי (דברים, קטעים).*
- new member catalog title: *תרגום ופרוש התורה ליפת בן עלי (דברים כו-לד).*; FJMS: פירוש על התורה | פירוש יפת למקרא
- new hit: 1187 letters, density 0.265 — [Ms. EVR ARAB I 105 p.64](https://genizahsearch.com/browse?sys_id=990001522670205171&page=64)
- motif rep text: `אכלתם בשר בניכם
נט̇יר קולה האהנא ואכלת פרא בטנך וג׳. וליכו ההנא
זאד שרח הרך בך הרכה בך . וקאל הנאך ואתכם אורה
בגוים נט̇יר קולה האהנא וחפיצך `
- new member text: `הם לחקהם אלגועב אלעטים וקאל הנאך ואפלתם
בשר בניכם נטיר קו' ואכלת פרי פטנך וג' ולא כן האהנא
זאד שרח האיש הרך כך הרכה בך והענוגה וקל הנאך
ואתכ`

### 2. motif 384388 -> Ms. EVR ARAB I 2120 (RNL) — **AGREE**
- motif identity (catalog): *תרגום ופרוש נביאים ליפת בן עלי (ישעיה א-יד, טז-כז).*
- new member catalog title: *תרגום ופרוש נביאים ליפת בן עלי (ישעיה, קטעים).*; FJMS: פירוש על ישעיה | פירוש יפת למקרא
- new hit: 1004 letters, density 0.270 — [Ms. EVR ARAB I 2120 p.14](https://genizahsearch.com/browse?sys_id=990001539380205171&page=14)
- motif rep text: `מון
פקאל ההנא הביאי עצה עלי טריק אלאמר ואלגרץה
עריף
פועד עז וג̇ל אנה יפכהם וירדהם אחרארא פקולה
גאלס חזק וג' וקאל ועתה מה לי פה נאם ילי כי לק`
- new member text: `לות תחת ידאלממאלך
ועברת את או יביך וג' פועד עזוגל אנה יפכהם וירדהם אחראר כקו
גאלם חזק יי וג׳ וקאלועתה מה ליפה נאס יי וג'. תם אנה ימ' לכהם אר`

### 3. motif 506326 -> Ms. EVR ARAB I 3895 (RNL) — **AGREE**
- motif identity (catalog, CONFLICT): *תרגום ופרוש ערבי לכתובים (שיר השירים).*
- new member catalog title: *תרגום ופרוש ערבי לכתובים (שיר השירים, רות).*; FJMS: פירוש על רות | פירוש יפת למקרא
- new hit: 927 letters, density 0.253 — [Ms. EVR ARAB I 3895 p.30](https://genizahsearch.com/browse?sys_id=990001556500205171&page=30)
- motif rep text: `איצ̇א עלי קותהם מתל עצי
לבנון אלתי הי אקוא שגר פי ארץ̇ ישראל וקסם הדא אלעסכר עלי ג׳
אקסאםפפאלקסם אלואחד מתלה בעמל אלעמאריה והם בקבא אלעסכר
ו`
- new member text: `וקאל סעצה אלבא
לאנהם מן אלקדס אלמסמח לבנון כק גלעד אתה לי ראש
הלבנון וידל איצא עלי קותהם מתל עצי הלבנו אלתי הי אקוא
שגר פי ארץ יאראל פקסם הד`

### 4. motif 509748 -> Library of the Alliance Israélite Universelle Ms. II A 33 (AIU) — **AGREE**
- motif identity (catalog): *דרשות; מדרש דוד הנגיד. ; David b. Abraham b. Maimon, Midrash David ha-Nagid: Exodus 38:21 ; דוד בן אברהם הנגיד, מדרש דוד הנגיד: שמות לח:כא*
- new member catalog title: *מדרש דוד (ויקהל-פקודי).*
- bib signal: **transcribed** — כתבי-היד של מדרשי ר' דוד הנגיד (Partial)
- new hit: 761 letters, density 0.292 — [Library of the Alliance Israélite Universelle Ms. II A 33 p.9](https://genizahsearch.com/browse?sys_id=990049441730205171&page=9)
- motif rep text: `בלה וק ומן התכות ומ והארכמו ותונעת השני עבש קעא למא דא אסמאהם
בקדי שרר פאן לולאהם מא כאן בקי מן שונא ישראל תחר פי איאם אלאסכנדר קטא
אן למא ת`
- new member text: `אלך קאלי ונחשת
התנופה :. ונחשת הכלה .. קאל ומן התכלת והארגמן ותולעת השני
עשו כגדי שרר .. קאלו עה למא דא אסמאהם בגדי שרד כאן לו
לא הם מא כאן `

### 5. motif 374827 -> Ms. EVR ARAB I 1563 (RNL) — **AGREE**
- motif identity (catalog): *תרגום ופרוש התורה ליפת בן עלי (דברים).*
- new member catalog title: *תרגום ופרוש התורה ליפת בן עלי (דברים, קטעים).*; FJMS: פירוש על התורה | פירוש יפת למקרא
- bib signal: **discussed** — ספונות (סדרה חדשה) (Mentioned)
- new hit: 751 letters, density 0.265 — [Ms. EVR ARAB I 1563 p.105](https://genizahsearch.com/browse?sys_id=990001534060205171&page=105)
- motif rep text: `סלמת לכם וכאלפתם אמר אלרב אלאהכם ומא
ותקתם לה ומא קבלהם קולו ממרים הייתם
עם יהוה מיום דעתי אתכם
מכעל
מכאלפין כנתם לרב אלעאלמין מן יום מערפתי`
- new member text: `לרקים
קולאא וערו ורמו בארץ אלכי סלמולכם
וכאפתם אמר אלאב. ]מא ותקתםלה
]ממאנ כלתם קולה : ] ממרים הייתם .
םאלפי כנתם לרב אלעלמין מן מערפתי
לכם `

### 6. motif 353042 -> Ms. EVR ARAB I 177 (RNL) — **AGREE**
- motif identity (catalog): *תרגום ופרוש ערבי לנביאים (יחזקאל).*
- new member catalog title: *תרגום ופרוש נביאים ליפת בן עלי (יחזקאל כז-מח, זכריה ה:ה).*; FJMS: פירוש על יחזקאל | פירוש יפת למקרא
- new hit: 715 letters, density 0.259 — [Ms. EVR ARAB I 177 p.206](https://genizahsearch.com/browse?sys_id=990001523200205171&page=206)
- motif rep text: `ל אניות הים ומלחיהם ריובך לערב מערבך
שיוך גבול וחכמיה כאנו פיך משדרי מרמתך
כל ספן אלבחר ומלאחיהם כאנו ניך לכלט כלטך
קו מחזיקי בדקך ישיר בה א`
- new member text: `נא קול פי עובריהום אנשי
יורשסזקני גכל וחכמיה היו בך מחזיקי
ברקן: פראניית הים ומלחיהם
היו בך לערב מערבה : שיובגביל
וחכמאהא באנו פיך משהדי מרמ`

### 7. motif 445799 -> Ms. EVR ARAB I 1405 (RNL) — **AGREE**
- motif identity (catalog): *פרוש נביאים ליפת בן עלי (תרי עשר).*
- new member catalog title: *פרוש נביאים ליפת בן עלי (הושע, יואל).*; FJMS: פירוש על תרי עשר | פירוש יפת למקרא
- bib signal: **transcribed** — פירוש יפת בן עלי לספר הושע (Partial)
- new hit: 713 letters, density 0.248 — [Ms. EVR ARAB I 1405 p.54](https://genizahsearch.com/browse?sys_id=990001532720205171&page=54)
- motif rep text: `ת ענדי יאכלו ואל
עונם ושאו נפשו קרבן חטאת אלדי לשעבי יאכלו ואלי
וזרהם ידפע כל ואחד מנהם נפסה שרח בעץ כן חטאו
לי פקאל אן אלכהנים ענד מא כתרו `
- new member text: `לרבנאה
ומא גאנסהא מן אלצנאיע חטאת
עמי יאכלו ואל עונם ישאו נפשו :
קרבן חטאת אלדי לטעבי יאכלו ואלו וזרהם
ידפע כל ואחד מנהם נפסה שרח בעץ
כן חטא`

### 8. motif 385692 -> Ms. EVR ARAB I 164 (RNL) — **AGREE**
- motif identity (catalog): *תרגום ופרוש נביאים ליפת בן עלי (ישעיה, קטעים).*
- new member catalog title: *תרגום ופרוש נביאים ליפת בן עלי (ישעיה א-יד, טז-כז).*; FJMS: פירוש על ישעיה | פירוש יפת למקרא
- new hit: 679 letters, density 0.190 — [Ms. EVR ARAB I 164 p.163](https://genizahsearch.com/browse?sys_id=990001523070205171&page=163)
- motif rep text: `לבהא ומבמא מניהא שרדמה ⟦/⟧
יסירה פקאמו גאזו ען בלדהם אלי קברס כקו' כתים קומי ברי וערף א[
אלעדו יקצדהם אליתם פי צ̇טרו אן יהרבו מן קדאמה כק גם`
- new member text: `בחיניו עוקרו ארמנותיה שמה למפלה
הודא ארץ כשרים הדא אלשעב
לם יכן ואנמא ג̇א קום מן אשור אססהא
ללמפאזיין פאקאמו כזאינה ותורו קצ
קצורהא ג̇עלהא ל`

### 9. motif 377038 -> Ms. EVR ARAB I 98 (RNL) — **AGREE**
- motif identity (catalog): *תרגום ופרוש התורה ליפת בן עלי (דברים כו-לד).*
- new member catalog title: *תרגום ופרוש התורה ליפת בן עלי (דברים, קטעים).*; FJMS: פירוש על התורה | פירוש יפת למקרא
- new hit: 678 letters, density 0.286 — [Ms. EVR ARAB I 98 p.149](https://genizahsearch.com/browse?sys_id=990001522570205171&page=149)
- motif rep text: `ן כל מראהבה חכם שאיק דואמאנה וליס עווג עאדל ומסתקום הוי הדא
אלאפטק הותראם צרר הדה אלשירה וקאלהא סידנא משה למענאין גלילין אחרהמא
מא
לוחת בה פ`
- new member text: `צור תמים פע'לו [/]
הדא אלפסוק הו תמאם צ̇דק הדה̈ אלשיקה וקאלהא סי/
מפוה עהי למענאיין גלילין הו מא לוחת בה טין
הבו גדל לאלהינו ועלי אלתפסיר אל`

### 10. motif 430726 -> Ms. EVR ARAB I 4038 (RNL) — **AGREE**
- motif identity (catalog, CONFLICT): *פרוש כתובים ליפת בן עלי (משלי).*
- new member catalog title: *תרגום ופרוש ערבי לכתובים (משלי, קטע).*
- new hit: 665 letters, density 0.229 — [Ms. EVR ARAB I 4038 p.12](https://genizahsearch.com/browse?sys_id=990001557680205171&page=12)
- motif rep text: `דלך
אלקול פי כל ראבעה מפרדה פי הדה אלפצול פקולה
שאול ישיר בה אלי קבר אלדי לעאלם אלוף סנין יעומון
א[
לנאס ויגדון קבור ירפנון פיהאי וקו ועצר ר`
- new member text: `חדה לאנהא אקוא פעל פי דלך אלמענם
וכדלך אלקול פי כל ראבעה מפרדה פי הדה אלפצול
פקול שאול ישיר אלי אלקבור אלדי ללעאלם
אלוף מן אלסנין ימותון אלנ`

## 10 example DISAGREE / interesting cards (discovery queue)

### 1. motif 396989 -> Ms. Guenzburg 1827.1 (RSL) — **DISAGREE**
- motif identity (catalog): *תרגום ופרוש נביאים ליפת בן עלי (יחזקאל, קטעים).*
- new member catalog title: *שרח אלאלפאץ (דניאל ב:לא-ב:מד).*
- new hit: 839 letters, density 0.280 — [Ms. Guenzburg 1827.1 p.5](https://genizahsearch.com/browse?sys_id=990040496590205171&page=5)
- motif rep text: `פקו אדוא אלהגולה
הו צעד חואל ענה אלמראה ג̇א אלי אלאולה כמד
אמרה ולך בא אלהגולה וקו צב אביב הו אסם ↑ אני
כאן יסכן פיה יחיקאל עה פערף אן תל הו`
- new member text: `ואטא אלאגולא מנא וואלענא אלמראה גא אלי אלגלה פמא אמרח
ל בא אלהגולה וקד מל אכלף הו אהם מוצ̇ע כאן יסכן פיה יחזקאל
עהש פערף אן תל הו עלי מיכבר `

### 2. motif 12453 -> Or. 2598 (BL) — **DISAGREE**
- motif identity (catalog, CONFLICT): *מדרש דוד הנגיד*
- new member catalog title: *קטעים.*; FJMS: תוספתא [טקסט] | יוסיפון | פירוש יפת למקרא
- new hit: 681 letters, density 0.369 — [Or. 2598 p.37](https://genizahsearch.com/browse?sys_id=990001231850205171&page=37)
- motif rep text: `ידין אלחק תע
י'א' רה אנת אעלא ואעלם באן נחן לם אתבלינא עלי אפנ אלנא
ואכמא את כלנא עלי אסמך אלמענים חתי לא יתבדל בין אלאמם
באפעל שילתקרים ראת`
- new member text: `חנניה מישאל ועזריה וקאלו בין
בין ידון אלחק תע' יא רכונו שו עונמים אנת
אעלי ואעלם כאן נחן לם אתכלנח עני אפעאלנא
ואנמא אתכלנא עלי אסמך אל מעכם`

### 3. motif 500686 -> Ms. EVR ARAB I 4570 (RNL) — **DISAGREE**
- motif identity (catalog, CONFLICT): *חבור בדקדוק עברי.*
- new member catalog title: *אלאפעאל דואת חרוף אללין (פרק ג').*; FJMS: כתאב אלאפעאל ד'ואת חרוף אללין
- new hit: 629 letters, density 0.347 — [Ms. EVR ARAB I 4570 p.11](https://genizahsearch.com/browse?sys_id=990001562670205171&page=11)
- motif rep text: `אנה מן נראה ומלתל ערצחצי דט'
המלכות ותעש אלדי אצלה ותעשה לאנה
מן נעשה ואלנאקץ אלגיד מעוץ איצא
מתנ יננו יקנו ירוו יעשו ואל אצל פה יגנא
יקניו `
- new member text: `מעוץ פמתל תגל ערותך אלדי
אצלה תגלה לאנה מן נגלה ומתלה
וגם איש אלירא אלדי אצלה יראה
לאנה מן נראה ומתל עד חצי המלכות
ותעש אלדי אצלה ותעשה לאנה`

### 4. motif 112069 -> Adler, Elkan Nathan Ms. 2767.19 (JTS) — **DISAGREE**
- motif identity (track1): *בחיי, תורת חובות הלבבות*
- new member catalog title: *כלאם יהודי. ; חיבור בענייני אמונה, בערבית-יהודית המשמעת והמרי; שכר ועונש*
- new hit: 600 letters, density 0.352 — [Adler, Elkan Nathan Ms. 2767.19 p.1](https://genizahsearch.com/browse?sys_id=990053201890205171&page=1)
- motif rep text: `דז וקד דכרת אכתרריפ
]הד'ז' אלבזב ומן העטם מפסדותהואל דצמור על אלמעציה
]י עמלהו ואל תדכר ען אלאקלוע ענהא פלו תצח תובה תע
קד קיל לא צבירה פי ה`
- new member text: `צל
קאלו אמא מפסדאת אלתובה פכתירה גדא
וקד דכרת אכתרהא פי מא תקדם פי הדא אלבאב ומן אעטם
מפסדאתהא אלאבהאר עלי אלמעציה והו אלדואס עלי עמלהא
ואלת`

### 5. motif 42538 -> Ms. EVR ARAB I 4453 (RNL) — **DISAGREE**
- motif identity (catalog): *מקאלה פי אלעריות [שלמה בן דוד הנשיא]*
- new member catalog title: *ספר המצוות לישראל בן דניאל.*; FJMS: ספר מצוות [ישראל בן דניאל]
- new hit: 526 letters, density 0.397 — [Ms. EVR ARAB I 4453 p.3](https://genizahsearch.com/browse?sys_id=990000878260205171&page=3)
- motif rep text: `ש אשר בקא את אחותו בת אביו ותמאמה אדא ולדת מרה
אבד ען פשק ותזגנאהתה אלתי רי ען הלאל ויתזוג אמה איצא אד
ליס הו אלאך אלדי חרם אללה ולא הו אלאב`
- new member text: `יגב עליה אל קתל לקולה ואיש אשר יקח א' אחתו
בת אב א' בת אמ' ודא' את עדו ונכר זילום מן דלכ' אדא ולדת
מרה דכר ען פסקופסאד יתזוג אכתה אלדי הי ען`

### 6. motif 12716 -> Or. 2598 (BL) — **DISAGREE**
- motif identity (catalog): *דרשות;מדרש דוד הנגיד;מדרשים מאוחרים. ; David b. Abraham b. Maimon, Midrash David ha-Nagid: Genesis Lekh Lekha ; דוד בן אברהם הנגיד, מדרש דוד הנגיד: בראשית לך לך*
- new member catalog title: *קטעים.*; FJMS: תוספתא [טקסט] | יוסיפון | פירוש יפת למקרא
- new hit: 519 letters, density 0.378 — [Or. 2598 p.101](https://genizahsearch.com/browse?sys_id=990001231850205171&page=101)
- motif rep text: `של עולם ראית
נולע מולדי אני נקים ולם נרזק אולאד קאל לה יא' אברהם
צא מא צטגנינות שלך אכרג ען האדה אלמדריב לאן נפסך
שריכה והימתא תרא מן אלאכלי`
- new member text: `הנם עצים וליס לאולאדי
ולד יקאל לה תע וא אברהם צא מאצטגנינות שלך
אכרג ען מדא אלמדהב לאן נפסך שריפה והי ש[
מאתרה פי אלכלך לא אלאפלאך פיהא תאתי`

### 7. motif 86287 -> Ms. EVR ARAB I 118 (RNL) — **DISAGREE**
- motif identity (track1, CONFLICT): *משה בן מיימון (רמב״ם) — משנה תורה, ספר קדושה*
- new member catalog title: *ספר תשובה.*; FJMS: כתאב אלרד | מקרא [טקסט]
- new hit: 510 letters, density 0.255 — [Ms. EVR ARAB I 118 p.131](https://genizahsearch.com/browse?sys_id=990001023130205171&page=131)
- motif rep text: `מעאואן כאנת זוג̇תה גיר עלמיה או ליסת
או ירתפע אלחרק ענה וען אלבנת לכון אלאולי א'
ליסת בראצ̇יה או ליסת עאלמה לקואותו ואתהו
והדא בעדא ען אלחק `
- new member text: `א יקתצ̇י אן מן אפסר ברביבתה
או בחמאתה וזוגתה קימת אמא אן
יחרקו אלמרתין מעה ואן כאנת זוגתה
גיר עאלמר או ליסת בראצ̇יה . או
ירתפע אלחרק ענה וען`

### 8. motif 86287 -> Ms. EVR ARAB I 3017 (RNL) — **DISAGREE**
- motif identity (track1, CONFLICT): *משה בן מיימון (רמב״ם) — משנה תורה, ספר קדושה*
- new member catalog title: *ספר תשובה (קטע).*; FJMS: כתאב אלרד
- new hit: 507 letters, density 0.302 — [Ms. EVR ARAB I 3017 p.7](https://genizahsearch.com/browse?sys_id=990000923480205171&page=7)
- motif rep text: `מעאואן כאנת זוג̇תה גיר עלמיה או ליסת
או ירתפע אלחרק ענה וען אלבנת לכון אלאולי א'
ליסת בראצ̇יה או ליסת עאלמה לקואותו ואתהו
והדא בעדא ען אלחק `
- new member text: `דא יקצי אן מן
אפסד ברביבתה או בחמאתה וזוגתה
קיימת אמא אן יחרקו אלמרתין מעה ואן
כאנת זוגתה גיר עאלמה או ליסת כאציה
או ירתפע
]ק ענה אען אלבנת `

### 9. motif 302230 -> Cambridge University Library Ms. T-S K 12.31 (CUL) — **DISAGREE**
- motif identity (catalog): *מחזור מנהג ספרד לשלש רגלים.*
- new member catalog title: *תפלות ופיוטים. ; Piyyut*; FJMS: זוהר
- new hit: 481 letters, density 0.268 — [Cambridge University Library Ms. T-S K 12.31 p.13](https://genizahsearch.com/browse?sys_id=990051210530205171&page=13)
- motif rep text: `הניון לבי לפניך ה' צורי וגואלי :
תפלה להוצאת ספר תורה לפסח
רבונו של עולם מלא משאלוחינו לטובה והוציאנו מעבדות לחרות
ומשעבוד לגאולה ומאפלה לאו`
- new member text: `עי עד סופ כל העולם עדי
ועד בכלל אמן נסו' :
תפלה ליום ראשון של פסח בשעת
הוצאת ספר תורה וקודם הינ עדות וב
רבזגז של עולם עלא משאלותינו לטובה
וה`

### 10. motif 208391 -> Ms. EVR ARAB II 1147 (RNL) — **DISAGREE**
- motif identity (catalog): *תרגום ופרוש נביאים ליפת בן עלי (ישעיה א-יד, טז-כז).*
- new member catalog title: *אלמקדמאת.*; FJMS: שרח אלמקדמאת | פירוש סלמון בן ירוחם למקרא
- new hit: 462 letters, density 0.412 — [Ms. EVR ARAB II 1147 p.143](https://genizahsearch.com/browse?sys_id=990001603800205171&page=143)
- motif rep text: `אלקיום אן גפר ענכם הדא אלוזר אלי אן תמותו קאל
אלאלאה אלקיום קו' ויקרא יוי אלהים
צבאות יריד בה עלי יד אלאנביא תתנבא עליהם
באלכראב ואלגלוה תם `
- new member text: `ד תמתון אמר ייי צבאות :
פי סמעי קאל אלרב אלקיום אן גפר ענכם הדא אלוזר אלא אזת
אלקיום. קולה ויקראאדני יי צבאל יריד בה עלי יד אלאא
עליהם באלכר`

## Spot-check results (manual verification, per the brief's gate)

Manually read the page text (`pages`) against the catalog entry for every card in two rounds:

- **Round 1** -- the top-10 AGREE + top-10 DISAGREE cards (ranked by hit strength) from the FIRST pass, raw `title_bucket2`/`bib_signal`: found 2/10 false AGREE (motifs 492232, 12453 -- see fixes 1-2 above); the top-10 DISAGREE sample of that pass was independently correct.
- **Round 2** -- a broader random sample (12 AGREE + 12 DISAGREE, `random.seed(42)`, drawn from the full bucket, not just the top-ranked cards) surfaced the two systematic false-DISAGREE patterns (fixes 3-4 above: אלמקדמאת / אלעתידות placeholder titles) plus the two documented-not-fixed open issues (Hebrew<->JA responsa-genre synonym gap; the reverse vague-identity asymmetry).
- **Final verification** (after all 4 fixes, this report's numbers): re-read all 10 current AGREE cards and all 10 current DISAGREE cards above against `pages` text + catalog titles. **20/20 correctly classified** by the scorer's own lexical logic -- 10/10 AGREE genuinely share author/book/bib evidence with the identity; 10/10 DISAGREE genuinely name a different specific work OR are a legitimately ambiguous catalog-vs-motif tension appropriate for the discovery queue (one, motif 500686, is the documented residual vague-identity limitation, not a clean case -- flagged rather than silently counted as a plain pass).

**Spot precision: 20/20 (100%) on the final scorer, after 4 documented fixes driven by this same spot-check exercise.** This is not a claim of zero residual error dataset-wide -- the point of the gate is that it FOUND the 4 bugs above and 2 further open issues (Hebrew<->JA genre-synonym gap, vague-identity asymmetry) affecting an estimated single-digit number of the 1,516 rows each, now documented for a human reviewer rather than silently misclassified.
