# Phase 136 Plan 03 -- Novelty Hard-Case Candidates

**95 candidates total: 8 IDENTITY spot-check cases (Classes 1-3) + 87 NOVELTY SHADE cases (Classes 4-7).** Restructured per an owner-authorized labelling-restructure ruling (`136-GATE1-DECISIONS.md`, the note appended after items F/G) made AFTER the owner read the original 97-case worksheet directly: Classes 1-3 compare two of OUR OWN claims to each other (an A↔B "is this the same work" identity judgment) and were found, on reading the actual cases, to bake "same work" into their own selection criterion (same author + common title stem) -- so full labelling of all 52 is REPLACED by this 8-case SPOT-CHECK that TESTS the constant-answer assumption rather than building ground truth. Classes 4-6 are where the answer genuinely varies and carry the NOVELTY SHADE question (a claim-vs-finding-aid judgment); they are EXPANDED from the original 45 to 75 candidates. **Class 7 is NEW** (owner rulings H/I, `136-GATE1-DECISIONS.md` §§ H/I -- not part of that 45→75 expansion): 12 liturgical-container-predictability candidates, added so the pinned model gate's FIRST encounter with this shape is a graded evaluation, not production. Every case in every group is selected entirely by string/title/metadata comparison over the works and manuscripts already in the deployed asset -- **zero model calls, measured cost $0.00**. Any attached draft verdict below is explicitly marked `PROPOSAL` and is a reading aid only, never a label -- it is NOT filled in by this script as an owner answer. This worksheet is also emitted as `136-NOVELTY-HARDCASES.xlsx` (same phase directory, THREE sheets: "Identity Spot-Check", "Novelty Shades", "Vocabulary & Instructions") for owners who find Hebrew RTL easier to work with in a spreadsheet; both files render the SAME cases in the SAME order, from the same pre-numbered case list, so the two agree case-for-case.

## Part A -- IDENTITY spot-check (Classes 1-3)

**Question type: IDENTITY, not a novelty shade.** These rows compare two of OUR OWN claims (A and B) -- there is no finding aid in this judgment at all. Answer ONE of:

| Answer | Choose this when... |
|---|---|
| `same_work` | A and B are the same underlying work (or two parts/granularities of the same work) |
| `different_works` | A and B are genuinely different works |
| `unsure` | you cannot judge this pair from the information shown -- a real and useful answer |
| `skip` | you choose not to judge this pair at all -- recorded as skipped |

**How to read the result (recorded in `136-GATE1-DECISIONS.md` so the interpretation is fixed BEFORE the answers come in, not chosen afterward to fit them):** if ALL cases below come back `same_work`, that is a measured fact and an argument that the D-13d author-gated collapse rule (currently collapsing only 276 of 1,367 identical-span groups, 20.2% -- see this plan's D-13d section) is TOO CONSERVATIVE and should collapse more aggressively. If even ONE case comes back `different_works`, the constant-answer assumption FAILS and the full 52-case pool needs real labelling after all, not just a spot-check.

### Class 3 -- catalogue entry naming a different granularity of the same work (IDENTITY spot-check) (3 candidates)

#### Case 1

- **Manuscript:** Cambridge University Library Ms. T-S Misc. 12.31.14 (sys_id `990051079570205171`)
- **A vs B:** רש"י על התורה (w000171) / רש"י על בראשית (w001281)
- **Catalogue's own identification text:** פרשנות מקרא רבנית. ; Solomon b. Isaac, Rashi, Biblical Exegesis - Rabbanite: Genesis 44 ; שלמה בן יצחק, פרשנות מקרא רבנית: בראשית מד
- **Why this pair is adversarial to a STRING heuristic:** Byte-identical span 0-962 on this page is claimed by 2 works sharing the same author and a common title stem -- a title-containment/alias relationship a plain string comparison cannot resolve on its own (same underlying commentary at two catalogued granularities, or two genuinely distinct works?).
- **PROPOSAL (draft, not a label): plausibly `same_work` (the SAME underlying work at two granularities) -- confirm or correct.**
- **Identity verdict:** _(pending Task 3 -- `same_work` / `different_works` / `unsure` / `skip`)_

#### Case 2

- **Manuscript:** Ms. EVR II A 265 (sys_id `990000872080205171`)
- **A vs B:** רש"י על התורה (w000171) / רש"י על שמות (w001278)
- **Catalogue's own identification text:** פרוש התורה לרש"י (בראשית, קטעים).
- **Why this pair is adversarial to a STRING heuristic:** Byte-identical span 0-780 on this page is claimed by 2 works sharing the same author and a common title stem -- a title-containment/alias relationship a plain string comparison cannot resolve on its own (same underlying commentary at two catalogued granularities, or two genuinely distinct works?).
- **PROPOSAL (draft, not a label): plausibly `same_work` (the SAME underlying work at two granularities) -- confirm or correct.**
- **Identity verdict:** _(pending Task 3 -- `same_work` / `different_works` / `unsure` / `skip`)_

#### Case 3

- **Manuscript:** Ms. EVR II C 465 (sys_id `990001588480205171`)
- **A vs B:** רש"י על התורה (w000171) / רש"י על במדבר (w001304)
- **Catalogue's own identification text:** תורה (במדבר ד).
- **Why this pair is adversarial to a STRING heuristic:** Byte-identical span 0-246 on this page is claimed by 2 works sharing the same author and a common title stem -- a title-containment/alias relationship a plain string comparison cannot resolve on its own (same underlying commentary at two catalogued granularities, or two genuinely distinct works?).
- **PROPOSAL (draft, not a label): plausibly `same_work` (the SAME underlying work at two granularities) -- confirm or correct.**
- **Identity verdict:** _(pending Task 3 -- `same_work` / `different_works` / `unsure` / `skip`)_

### Class 2 -- alias pairs (IDENTITY spot-check) (2 candidates)

#### Case 4

- **Manuscript:** Cambridge University Library Ms. T-S NS 288.21 (sys_id `990051103040205171`)
- **A vs B:** האיי גאון על שבת (w000448, msource) / האיי גאון על שבת (w000451, msource)
- **Catalogue's own identification text:** ספרות חז"ל; פירוש רב שרירא גאון לתלמוד; פירושי תלמוד בבלי. ; Rabbinic Literature ; פירוש לתלמוד: שבת יא ע"ב – יב ע"ב; טז ע"ב – יז ע"א
- **Why this pair is adversarial to a STRING heuristic:** Two DIFFERENT work_ids share both the same author and an identical normalized title (a two-member cluster, not a large generic-collection cluster) -- a likely un-merged cross-corpus alias/duplicate.
- **PROPOSAL (draft, not a label): plausibly `same_work` (an alias pair, not yet canonically merged) -- confirm or correct.**
- **Identity verdict:** _(pending Task 3 -- `same_work` / `different_works` / `unsure` / `skip`)_

#### Case 5

- **Manuscript:** MS heb. d.46/143 (sys_id `990053410080205171`)
- **A vs B:** תשובה בעניין נוסח ברכות קריאת שמע (w000598, msource) / תשובה בעניין נוסח ברכות קריאת שמע (w000635, msource)
- **Catalogue's own identification text:** שאלות ותשובות. ; דף 144 א-ב כולל שלש תשובות בערבית האחת בעניין גביית חוב מיתומים קטנים. התשובה השנייה עוסקת בעניין חוב מיתומים כשכתובת האלמנה אבדה. התשובה השלישית עוסקת בעניין ספר קודש שנקנה מגויים, לאחר שהגיע לידם כשלל מקהיר, והיה כתוב עליו 'קדש', האם מותר לקונה למוכרו או למשכנו מחמת שמצבו הכספי דחוק.
- **Why this pair is adversarial to a STRING heuristic:** Two DIFFERENT work_ids share both the same author and an identical normalized title (a two-member cluster, not a large generic-collection cluster) -- a likely un-merged cross-corpus alias/duplicate.
- **PROPOSAL (draft, not a label): plausibly `same_work` (an alias pair, not yet canonically merged) -- confirm or correct.**
- **Identity verdict:** _(pending Task 3 -- `same_work` / `different_works` / `unsure` / `skip`)_

### Class 1 -- near-miss titles (IDENTITY spot-check) (3 candidates)

#### Case 6

- **Manuscript:** Cambridge University Library Ms. T-S Ar. 46.221 (sys_id `990051317430205171`)
- **A vs B:** המספיק לעובדי השם (כרך ב חלק ב) (w000007) / המספיק לעובדי השם (כרך ט חלק ב) (w000036)
- **Catalogue's own identification text:** דרשות;כפאיה אלעאבידין;פרשנות מקרא. ; Abraham Maimonidess Kitab kifayat al-'abidi n. On prostration (סג'וד )
- **Why this pair is adversarial to a STRING heuristic:** Same author; normalized titles are 96.8% similar (SequenceMatcher) but NOT identical -- genuinely different works (e.g. different books/chapters/parts) whose titles a string comparison could easily conflate in EITHER direction.
- **Identity verdict:** _(pending Task 3 -- `same_work` / `different_works` / `unsure` / `skip`)_

#### Case 7

- **Manuscript:** Gaster, Moses Collection (sys_id `990001835240205171`)
- **A vs B:** ספר הערוך, אות החי"ת (w001167) / ספר הערוך, אות הטי"ת (w001181)
- **Catalogue's own identification text:** מעשיות.
- **Why this pair is adversarial to a STRING heuristic:** Same author; normalized titles are 94.7% similar (SequenceMatcher) but NOT identical -- genuinely different works (e.g. different books/chapters/parts) whose titles a string comparison could easily conflate in EITHER direction.
- **Identity verdict:** _(pending Task 3 -- `same_work` / `different_works` / `unsure` / `skip`)_

#### Case 8

- **Manuscript:** Cambridge University Library Ms. T-S G 2.33 (sys_id `990051181430205171`)
- **A vs B:** רבנו חננאל על מסכת שבת (w000452) / רבנו חננאל על מסכת שבועות (w000467)
- **Catalogue's own identification text:** פירוש רבנו חננאל לתלמוד;שאלות ותשובות- גאונים. ; Responsa- Gaonim ; חננאל בן חושיאל, פירוש רבנו חננאל לתלמוד: שבת קמא ע"ב – קמו ע"ב
- **Why this pair is adversarial to a STRING heuristic:** Same author; normalized titles are 93.6% similar (SequenceMatcher) but NOT identical -- genuinely different works (e.g. different books/chapters/parts) whose titles a string comparison could easily conflate in EITHER direction.
- **Identity verdict:** _(pending Task 3 -- `same_work` / `different_works` / `unsure` / `skip`)_

## Part B -- NOVELTY SHADE cases (Classes 4-7)

**Question type: NOVELTY SHADE, a claim-vs-finding-aid judgment (never A↔B identity).** For EACH case below, answer with the shade that best describes what an enumerable finding aid (the catalogue's own identification field, bibliography, titles, PGP, FGP, M-source shelfmark attributions) actually says about THIS fragment and THIS work -- or `unsure` / `skip`. Amended 2026-08-02 by owner decisions E / E′ / F / G / H (`136-GATE1-DECISIONS.md` items E, E′, F, G, H): the tri-state (`already_recorded` / `not_in_finding_aids` / `unsure`) collapsed materially different findings into one bucket -- a catalogue CONTRADICTION and a genuine "previously unknown" both scored the same way, a granularity refinement that helps and one that adds nothing also scored the same way (E′), a flat wrong-work divergence and a same-work-wrong-part divergence also scored the same way with no way to record WHICH SIDE is actually correct (F), and a broader-container relationship (a standard-rite prayer-book predicting a specific unit it never names) had NO shade at all and fell through to `fills_gap` by elimination (H) -- so the shade enum now carries TEN values.

| Shade | Choose this when... |
|---|---|
| `confirms` | an aid already ties this fragment to this work |
| `refines_granularity` | OUR claim is MORE SPECIFIC (finer) than what an aid says -- e.g. the catalogue names the whole work, our claim names a specific book/chapter of it (the D-13d same-author/related-title rule); the OPPOSITE direction from `aid_more_specific` -- we ADD precision here. OWNER RULING G: if the aid's own FREE TEXT already states this identification in ANY form (even under a coarser structured work-id), the correct shade is `confirms`, not this one -- reserve `refines_granularity` for information the aid contains in NO form, structured or free. |
| `aid_more_specific` | an AID names a MORE SPECIFIC (finer) variant of this fragment's work than our claim does -- e.g. the catalogue names a chapter/book, our claim names the whole work (the D-13d same-author/related-title rule); the OPPOSITE direction from `refines_granularity` -- we add NOTHING here, the aid already knew more (owner correction E′; the LEAST novel shade) |
| `diverges_work` | an aid ties this fragment to a genuinely DIFFERENT WORK (not a granularity variant of ours) -- the aid and the claim contradict each other on WHICH WORK this is (owner ruling F, replacing the single `diverges` token). Owner: reading real cases, USUALLY the catalogue is right and this is OUR false positive -- but not always; record which side is correct in the separate Correctness column. Hidden by default on every surface, behind an explicit warned toggle -- never silently shown, never silently suppressed. |
| `diverges_part` | an aid ties this fragment to a DIFFERENT OR FINER PART of the SAME work (owner ruling F -- "more delicate and essentially less important" than diverges_work) -- e.g. the aid names a specific chapter/section of the work while we name a different one, or the whole work, or vice versa. Same Correctness column and same hidden-by-default posture as diverges_work. |
| `container_predicts` | an aid names a BROADER rite/cycle/ceremony/container -- a full standard-rite prayer-book (siddur/machzor) or a named ceremony/occasion -- whose STANDARD, PREDICTABLE content includes this specific unit, WITHOUT ever naming the unit itself (owner ruling H; e.g. the catalogue names 'מחזור מנהג אשכנז לשלש רגלים', the claim is a Yotzer for one of its festivals). Distinct from `confirms` (the aid never names this specific unit) and from `fills_gap` (the content IS predictable, so it is not 'previously unknown' -- under the pre-H enum this fell through to `fills_gap` by elimination). Excluded from the candidate toggle like every other non-`fills_gap` shade, but -- UNLIKE `diverges_work`/`diverges_part` -- shown NORMALLY, never hidden by default: there is no disagreement here to warn about, only a container relationship. |
| `fills_gap` | the aids identify this fragment as nothing at all -- the genuine "previously unknown" case |
| `extends` | aids tie OTHER folios of the SAME manuscript to this work, but not this specific folio |
| `alias_merge` | the two work_ids shown ARE the same underlying work, not yet canonically merged (Class 2's situation) |
| `unsure` | you cannot judge this case from the information shown -- maps to `not_checked`, costs nothing, is a real and useful answer |
| `skip` | you choose not to judge this case at all -- recorded as skipped, NEVER filled from a draft `PROPOSAL` |

`not_checked` (the fail-closed system default for an unrun/failed/abstained check) is not a verdict the owner picks directly -- `unsure` is its owner-facing equivalent.

### Correctness (Class 6 ONLY -- answer ONLY if your shade verdict is `diverges_work` or `diverges_part`, owner ruling F)

A divergence shade records only THAT the aid and the claim disagree, never WHICH SIDE is right -- the owner's own review of the real cases found BOTH directions occur under the identical shade. Leave blank / not applicable for every non-divergence shade.

| Correctness | Choose this when... |
|---|---|
| `catalogue_correct` | the catalogue/aid is right; our claim is the false positive -- owner ruling F: reading the real cases, this is the COMMON outcome |
| `claim_correct` | our claim is right; the aid is wrong, thinner, or itself mistaken |
| `unclear` | cannot tell which side is correct from the information shown |

### Class 4 -- terse or missing catalogue identification text (NOVELTY SHADE, owner-authorized extension) (20 candidates)

**Plausible shades for this class:** `fills_gap`, `confirms` (any other shade from the vocabulary table above is still a valid answer if the case warrants it; `unsure` / `skip` are always available).

#### Case 9

- **Manuscript:** Ms. B 3672 (sys_id `990002098550205171`)
- **Work(s):** כתר מלכות (רשב"ג/אבן גבירול) (w001129)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 10

- **Manuscript:** Cambridge University Library Ms. T-S Misc. 34.14 (sys_id `990051124400205171`)
- **Work(s):** פירוש אבות לדוד הנגיד (w001135)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 11

- **Manuscript:** Cambridge University Library Ms. T-S Misc. 34.20 (sys_id `990051124460205171`)
- **Work(s):** רס"ג, שמות תרגום (תפסיר תורה) (w000033)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 12

- **Manuscript:** Cambridge University Library Ms. T-S Misc. 34.23 (sys_id `990051124490205171`)
- **Work(s):** משנה תורה, ספר אהבה (w000176)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 13

- **Manuscript:** Cambridge University Library Ms. T-S Misc. 34.29 (sys_id `990051124550205171`)
- **Work(s):** תנ"ך, בראשית (w000086)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 14

- **Manuscript:** Cambridge University Library Ms. T-S K 27.5 (sys_id `990051220230205171`)
- **Work(s):** נתנאל בן פיומי, גן השכלים (w000039)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 15

- **Manuscript:** Cambridge University Library Ms. T-S K 27.26 (sys_id `990051220440205171`)
- **Work(s):** הלכות גדולות (w001196)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 16

- **Manuscript:** Cambridge University Library Ms. T-S K 27.28 (sys_id `990051220460205171`)
- **Work(s):** מדרש אגור (w000836)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 17

- **Manuscript:** Cambridge University Library Ms. T-S K 27.34 (sys_id `990051220530205171`)
- **Work(s):** תשובות האיי גאון (w000654)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 18

- **Manuscript:** Cambridge University Library Ms. T-S K 27.35 (sys_id `990051220540205171`)
- **Work(s):** מונחי המסורה וכלליה (w000906)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 19

- **Manuscript:** Cambridge University Library Ms. T-S K 27.39 (sys_id `990051220580205171`)
- **Work(s):** סדר אליהו זוטא א-טו (w000162)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 20

- **Manuscript:** Cambridge University Library Ms. T-S K 27.41 (sys_id `990051220600205171`)
- **Work(s):** תנ"ך, ויקרא (w000088)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 21

- **Manuscript:** Cambridge University Library Ms. T-S K 27.42 (sys_id `990051220610205171`)
- **Work(s):** דברי הימים של משה רבנו (w000944)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 22

- **Manuscript:** Cambridge University Library Ms. T-S K 27.46 (sys_id `990051220650205171`)
- **Work(s):** פירוש ליחזקאל ותרי עשר (w000918)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 23

- **Manuscript:** Cambridge University Library Ms. T-S K 27.48 (sys_id `990051220670205171`)
- **Work(s):** מדרש חסרות ויתרות (w000859)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 24

- **Manuscript:** Cambridge University Library Ms. T-S K 27.53 (sys_id `990051220720205171`)
- **Work(s):** מחברת (w000911)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 25

- **Manuscript:** Cambridge University Library Ms. T-S K 27.56 (sys_id `990051220750205171`)
- **Work(s):** הלכות פסוקות (w001084)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 26

- **Manuscript:** Cambridge University Library Ms. T-S K 27.33b (sys_id `990051221550205171`)
- **Work(s):** תנ"ך, דברים (w000090)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 27

- **Manuscript:** Cambridge University Library Ms. T-S 8 K 3 (sys_id `990051232460205171`)
- **Work(s):** חיבור נגד אל-קומסי (w001057)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 28

- **Manuscript:** Cambridge University Library Ms. T-S 8 K 13.8 (sys_id `990051232630205171`)
- **Work(s):** תשובות הרמב״ם ב (w001143)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Class 5 -- generic collection works (NOVELTY SHADE, owner-authorized extension) (25 candidates)

**Plausible shades for this class:** `fills_gap`, `confirms`, `extends` (any other shade from the vocabulary table above is still a valid answer if the case warrants it; `unsure` / `skip` are always available).

#### Case 29

- **Manuscript:** Cambridge University Library Ms. T-S G 2.23 (sys_id `990051181330205171`)
- **Work(s):** תשובות האיי גאון (w000695) -- one of 43 works sharing author 'האיי גאון' and title stem 'תשובות האיי גאון' (siblings incl. w000650, w000651, w000652, w000653, w000654...)
- **Catalogue's own identification text:** שאלות ותשובות- גאונים. ; Responsa- Gaonim
- **Why it is hard:** This work belongs to a 43-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 30

- **Manuscript:** Cambridge University Library Ms. T-S G 2.29 (sys_id `990051181390205171`)
- **Work(s):** תשובות שרירא גאון והאיי גאון (w000629) -- one of 33 works sharing author 'שרירא גאון והאיי גאון' and title stem 'תשובות שרירא גאון והאיי גאון' (siblings incl. w000597, w000599, w000600, w000601, w000602...)
- **Catalogue's own identification text:** שאלות ותשובות- גאונים. ; Responsa- Gaonim
- **Why it is hard:** This work belongs to a 33-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 31

- **Manuscript:** Cambridge University Library Ms. L-G Talm. II 1 (sys_id `990001843080205171`)
- **Work(s):** תשובות צמח גאון (w000573) -- one of 12 works sharing author 'צמח גאון' and title stem 'תשובות צמח גאון' (siblings incl. w000562, w000563, w000564, w000565, w000566...)
- **Catalogue's own identification text:** תשובות הגאונים.
- **Why it is hard:** This work belongs to a 12-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 32

- **Manuscript:** Cambridge University Library Ms. T-S NS 309.106 (sys_id `990051703120205171`)
- **Work(s):** תשובות שרירא גאון (w000581) -- one of 9 works sharing author 'שרירא גאון' and title stem 'תשובות שרירא גאון' (siblings incl. w000582, w000584, w000589, w000590, w000591...)
- **Catalogue's own identification text:** ספרות הלכתית ופרשנות תלמודית;ספרות חז"ל;שאלות ותשובות- גאונים. ; Halakhic Literature and Talmudic Commentaries ; Responsa; geonic, concerning oaths (whether a married woman should be made to swear in court, and whether some members of a partnership can waive an oath due them without the remaining partners consent).
- **Why it is hard:** This work belongs to a 9-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 33

- **Manuscript:** Catalogue Brumer Rab. 2360, fol. 1 (sys_id `990053344530205171`)
- **Work(s):** תשובות שמואל גאון (w000428) -- one of 6 works sharing author 'שמואל גאון' and title stem 'תשובות שמואל גאון' (siblings incl. w000429, w000430, w000432, w000433, w000435)
- **Catalogue's own identification text:** שאלות ותשובות- גאונים. ; תשובות בעניין המגרש בגט בטל, אונאה וחזרה במכירת קרקע
- **Why it is hard:** This work belongs to a 6-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 34

- **Manuscript:** Ms. Evr. Antonin B 266 (sys_id `990000907150205171`)
- **Work(s):** תשובות יצחק (w000559) -- one of 5 works sharing author 'נחשון גאון' and title stem 'תשובות יצחק' (siblings incl. w000554, w000556, w000557, w000561)
- **Catalogue's own identification text:** שאלות ותשובות הגאונים.
- **Why it is hard:** This work belongs to a 5-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 35

- **Manuscript:** Cambridge University Library Ms. T-S G 1.78 (sys_id `990051180910205171`)
- **Work(s):** תשובות נטרונאי גאון (w000534) -- one of 5 works sharing author 'נטרונאי גאון' and title stem 'תשובות נטרונאי גאון' (siblings incl. w000535, w000536, w000537, w000538)
- **Catalogue's own identification text:** שאלות ותשובות. ; Responsa and Halakhic Decisions
- **Why it is hard:** This work belongs to a 5-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 36

- **Manuscript:** Cambridge University Library Ms. T-S 20.183 (sys_id `990051346380205171`)
- **Work(s):** תשובות עמרם גאון (w000553) -- one of 5 works sharing author 'עמרם גאון' and title stem 'תשובות עמרם גאון' (siblings incl. w000547, w000548, w000551, w000552)
- **Why it is hard:** This work belongs to a 5-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 37

- **Manuscript:** Cambridge University Library Ms. T-S G 2.14 (sys_id `990051181240205171`)
- **Work(s):** תשובות פלטוי גאון (w000542) -- one of 4 works sharing author 'פלטוי גאון' and title stem 'תשובות פלטוי גאון' (siblings incl. w000539, w000540, w000541)
- **Catalogue's own identification text:** שאלות ותשובות- גאונים. ; Responsa- Gaonim
- **Why it is hard:** This work belongs to a 4-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 38

- **Manuscript:** Ms. Evr. Antonin B 308 (sys_id `990000555750205171`)
- **Work(s):** תשובות שר שלום גאון (w000499) -- one of 4 works sharing author 'שר שלום גאון' and title stem 'תשובות שר שלום גאון' (siblings incl. w000497, w000498, w000500)
- **Catalogue's own identification text:** שאלות ותשובות הגאונים.
- **Why it is hard:** This work belongs to a 4-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 39

- **Manuscript:** Cambridge University Library Ms. T-S 8 G 7.2 (sys_id `990051222370205171`)
- **Work(s):** תשובות יוסף בן אביתור (w000730) -- one of 3 works sharing author 'יוסף בן אביתור' and title stem 'תשובות יוסף בן אביתור' (siblings incl. w000729, w000731)
- **Catalogue's own identification text:** שאלות ותשובות- גאונים.
- **Why it is hard:** This work belongs to a 3-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 40

- **Manuscript:** Cambridge University Library Ms. T-S G 1.86 (sys_id `990051180990205171`)
- **Work(s):** תשובות משרשיה (w000528) -- one of 3 works sharing author 'משה (משרשיה) הכהן גאון' and title stem 'תשובות משרשיה' (siblings incl. w000527, w000529)
- **Catalogue's own identification text:** שאלות ותשובות;שאלות ותשובות- גאונים. ; Responsa and Halakhic Decisions ; ד' תשובות בענייני כתובה: א. כנראה בעניין אשה שהוציאה שטר כתובה ונמצא מזוייף. ב. תשובת רב צמח גאון בעניין אשה שאבדה כתובתה. ג. תשובת רב צמח גאון בעניין פירוש דברי רב רב יוסף "בביתי ולא בביקתי" (כתובות נד ע"א). ד. תשובת מר רבנא משה גאון (בן יעקב, ראש ישיבת מתא מחסיא) בעניין בחור שנשא אשה והתברר שהיא נכפית.
- **Why it is hard:** This work belongs to a 3-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 41

- **Manuscript:** Ms. EVR II A 721 (sys_id `990001442050205171`)
- **Work(s):** תשובות קלונימוס הזקן מלוקא בר׳ משה (w000416) -- one of 3 works sharing author 'קלונימוס הזקן מלוקא בר׳ משה' and title stem 'תשובות קלונימוס הזקן מלוקא בר משה' (siblings incl. w000760, w000761)
- **Catalogue's own identification text:** תשובות.
- **Why it is hard:** This work belongs to a 3-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 42

- **Manuscript:** Cambridge University Library Ms. T-S 16.99 (sys_id `990051341890205171`)
- **Work(s):** תשובות האיי גאון (w000694) -- one of 43 works sharing author 'האיי גאון' and title stem 'תשובות האיי גאון' (siblings incl. w000650, w000651, w000652, w000653, w000654...)
- **Catalogue's own identification text:** שאלות ותשובות;שאלות ותשובות- גאונים. ; Responsa- Gaonim ; שאלות ותשובות- גאונים
- **Why it is hard:** This work belongs to a 43-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 43

- **Manuscript:** Ms. EVR II A 32 (sys_id `990001428310205171`)
- **Work(s):** תשובות שרירא גאון והאיי גאון (w000606) -- one of 33 works sharing author 'שרירא גאון והאיי גאון' and title stem 'תשובות שרירא גאון והאיי גאון' (siblings incl. w000597, w000599, w000600, w000601, w000602...)
- **Catalogue's own identification text:** שאלות ותשובות הגאונים.
- **Why it is hard:** This work belongs to a 33-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 44

- **Manuscript:** Cambridge University Library Ms. T-S Loan Collection 98 (sys_id `990051125820205171`)
- **Work(s):** תשובות צמח גאון (w000570) -- one of 12 works sharing author 'צמח גאון' and title stem 'תשובות צמח גאון' (siblings incl. w000562, w000563, w000564, w000565, w000566...)
- **Catalogue's own identification text:** שאלות ותשובות- גאונים. ; Lists, Responsa lists ; רשימות, רשימות שו"ת
- **Why it is hard:** This work belongs to a 12-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 45

- **Manuscript:** Cambridge University Library Ms. T-S 12.459 (sys_id `990051337150205171`)
- **Work(s):** תשובות שרירא גאון (w000581) -- one of 9 works sharing author 'שרירא גאון' and title stem 'תשובות שרירא גאון' (siblings incl. w000582, w000584, w000589, w000590, w000591...)
- **Catalogue's own identification text:** תשובות הגאונים (קטע).
- **Why it is hard:** This work belongs to a 9-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 46

- **Manuscript:** Ms. Evr. Antonin B 1037 (sys_id `990000555740205171`)
- **Work(s):** תשובות שמואל גאון (w000428) -- one of 6 works sharing author 'שמואל גאון' and title stem 'תשובות שמואל גאון' (siblings incl. w000429, w000430, w000432, w000433, w000435)
- **Catalogue's own identification text:** שאלות ותשובות הגאונים.
- **Why it is hard:** This work belongs to a 6-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 47

- **Manuscript:** MS heb. c.23/43 (sys_id `990053397750205171`)
- **Work(s):** תשובות יצחק (w000559) -- one of 5 works sharing author 'נחשון גאון' and title stem 'תשובות יצחק' (siblings incl. w000554, w000556, w000557, w000561)
- **Catalogue's own identification text:** שאלות ותשובות. ; תשובות
- **Why it is hard:** This work belongs to a 5-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 48

- **Manuscript:** Cambridge University Library Ms. T-S G 1.87 (sys_id `990051181000205171`)
- **Work(s):** תשובות נטרונאי גאון (w000534) -- one of 5 works sharing author 'נטרונאי גאון' and title stem 'תשובות נטרונאי גאון' (siblings incl. w000535, w000536, w000537, w000538)
- **Catalogue's own identification text:** שאלות ותשובות;שאלות ותשובות- גאונים. ; Responsa and Halakhic Decisions ; ו' תשובות רב נטרונאי גאון: א. בעניין קבורת מת ביום טוב שני של גלויות. ב. בעניין הכרעת ההלכה בסוגיית "כשותא בכרמא" (שבת קלט ע"א). ג. בעניין הכרעת ההלכה בסוגיית "אין מוליכין חלה ומתנות לכהן ביום טוב" (ביצה דף יב ע"ב). ד. אם נוהגת תרומה בזמן הזה. ה. פירוש דברי הגמרא "ומסיקין ואופין בפורני" (ביצה לד ע"א). ו. בעניין אפיה ביום טוב בתנור של גוי.
- **Why it is hard:** This work belongs to a 5-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 49

- **Manuscript:** MS heb. d.63/70 (sys_id `990053419080205171`)
- **Work(s):** תשובות עמרם גאון (w000547) -- one of 5 works sharing author 'עמרם גאון' and title stem 'תשובות עמרם גאון' (siblings incl. w000548, w000551, w000552, w000553)
- **Catalogue's own identification text:** שאלות ותשובות;שאלות ותשובות- גאונים. ; פירושים לתלמוד בתשובות גאונים, בהלכות ציצית ע"פ מנחות ועוד לט-מג; ר' נטרונאי גאון?
- **Why it is hard:** This work belongs to a 5-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 50

- **Manuscript:** Cambridge University Library Ms. T-S 12.854 (sys_id `990051340950205171`)
- **Work(s):** תשובות פלטוי גאון (w000541) -- one of 4 works sharing author 'פלטוי גאון' and title stem 'תשובות פלטוי גאון' (siblings incl. w000539, w000540, w000542)
- **Why it is hard:** This work belongs to a 4-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 51

- **Manuscript:** MS heb. c.18/40 (sys_id `990053395550205171`)
- **Work(s):** תשובות שר שלום גאון (w000498) -- one of 4 works sharing author 'שר שלום גאון' and title stem 'תשובות שר שלום גאון' (siblings incl. w000497, w000499, w000500)
- **Catalogue's own identification text:** שאלות ותשובות- גאונים. ; תשובות הגאונים, ברכות ה,א\ פסחים עד,ב
- **Why it is hard:** This work belongs to a 4-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 52

- **Manuscript:** Adler, Elkan Nathan Ms. 2632.1 (sys_id `990053180470205171`)
- **Work(s):** תשובות יוסף בן אביתור (w000730) -- one of 3 works sharing author 'יוסף בן אביתור' and title stem 'תשובות יוסף בן אביתור' (siblings incl. w000729, w000731)
- **Catalogue's own identification text:** שאלות ותשובות;שאלות ותשובות- גאונים. ; תשובות שונות לגאון או לר' יוסף אבן אביתור?
- **Why it is hard:** This work belongs to a 3-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 53

- **Manuscript:** Ms. EVR II A 32 (sys_id `990001428310205171`)
- **Work(s):** תשובות האיי גאון (w000670) -- one of 43 works sharing author 'האיי גאון' and title stem 'תשובות האיי גאון' (siblings incl. w000650, w000651, w000652, w000653, w000654...)
- **Catalogue's own identification text:** שאלות ותשובות הגאונים.
- **Why it is hard:** This work belongs to a 43-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Class 6 -- catalogue divergence (NOVELTY SHADE, owner rulings E/E′/F/G) (30 candidates)

**Plausible shades for this class:** `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms` (any other shade from the vocabulary table above is still a valid answer if the case warrants it; `unsure` / `skip` are always available).

#### Case 54

- **Manuscript:** Ms. Evr. Antonin B 1104 (sys_id `990000555880205171`)
- **Work(s):** CLAIMED (this identification): תשובות האיי גאון (w000650) / CATALOGUE NAMES (found in the identification text): תשובות (w000543)
- **Catalogue's own identification text:** שאלות ותשובות מאת האי בן שרירא גאון (קטע).
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('תשובות') than the one this claim identifies ('תשובות האיי גאון'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label; owner ruling G -- see 136-GATE1-DECISIONS.md § G): plausibly `confirms`, NOT a divergence -- the catalogue's own free text ('שאלות ותשובות מאת האי בן שרירא גאון') already states this claim's identification; only the structured work-id keying differed (this class's own selector over-fired here) -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 55

- **Manuscript:** Allony, Nehemia Ms. 304 (sys_id `990000413480205171`)
- **Work(s):** CLAIMED (this identification): משנה תורה, ספר זמנים (w000177) / CATALOGUE NAMES (found in the identification text): הגדה של פסח (w001159)
- **Catalogue's own identification text:** הגדה של פסח.
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('הגדה של פסח') than the one this claim identifies ('משנה תורה, ספר זמנים'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label; owner ruling F -- see 136-GATE1-DECISIONS.md § F): plausibly `diverges_work` -- confirm the shade AND supply the separate Correctness call (catalogue_correct / claim_correct / unclear), or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 56

- **Manuscript:** Ms. Evr. Antonin B 915 (sys_id `990000555810205171`)
- **Work(s):** CLAIMED (this identification): הלכות פסוקות (w001084) / CATALOGUE NAMES (found in the identification text): הלכות גדולות (w001196)
- **Catalogue's own identification text:** הלכות גדולות (בבא קמא).
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('הלכות גדולות') than the one this claim identifies ('הלכות פסוקות'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label; owner ruling F -- see 136-GATE1-DECISIONS.md § F): plausibly `diverges_work` -- confirm the shade AND supply the separate Correctness call (catalogue_correct / claim_correct / unclear), or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 57

- **Manuscript:** Cambridge University Library Ms. Add. 3162 (sys_id `990001398690205171`)
- **Work(s):** CLAIMED (this identification): משנה תורה, ספר אהבה (w000176) / CATALOGUE NAMES (found in the identification text): ברכת המזון (w001158)
- **Catalogue's own identification text:** ברכת המזון.
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('ברכת המזון') than the one this claim identifies ('משנה תורה, ספר אהבה'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label; owner ruling F -- see 136-GATE1-DECISIONS.md § F): plausibly `diverges_work` -- confirm the shade AND supply the separate Correctness call (catalogue_correct / claim_correct / unclear), or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 58

- **Manuscript:** Cambridge University Library Ms. Add. 1246 (sys_id `990001394270205171`)
- **Work(s):** CLAIMED (this identification): ספר יוסיפון (ערבי) (w001152) / CATALOGUE NAMES (found in the identification text): יוסיפון (w000853)
- **Catalogue's own identification text:** יוסיפון בערבית.
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('יוסיפון') than the one this claim identifies ('ספר יוסיפון (ערבי)'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label; owner ruling G -- see 136-GATE1-DECISIONS.md § G): plausibly `confirms`, NOT a divergence -- the catalogue's own free text ('יוסיפון בערבית') already states this claim's identification; only the structured work-id keying differed (this class's own selector over-fired here) -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 59

- **Manuscript:** Ms. Evr. Antonin B 961 (sys_id `990000555730205171`)
- **Work(s):** CLAIMED (this identification): תשובה בעניין סוכה (w000434) / CATALOGUE NAMES (found in the identification text): תשובות הגאונים (w000349)
- **Catalogue's own identification text:** שאלות ותשובות הגאונים.
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('תשובות הגאונים') than the one this claim identifies ('תשובה בעניין סוכה'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 60

- **Manuscript:** Ms. EVR ARAB I 3085 (sys_id `990000801470205171`)
- **Work(s):** CLAIMED (this identification): רס"ג, ספר יצירה פירוש (w000021) / CATALOGUE NAMES (found in the identification text): ספר יצירה (w000522)
- **Catalogue's own identification text:** פרוש ספר יצירה בערבית.
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('ספר יצירה') than the one this claim identifies ('רס"ג, ספר יצירה פירוש'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 61

- **Manuscript:** Catalogue Halper, Philadelphia 120 (sys_id `990001935160205171`)
- **Work(s):** CLAIMED (this identification): הלכות פסוקות, תרגומים ועיבודים עבריים, הלכות קידושין (w001037) / CATALOGUE NAMES (found in the identification text): הלכות פסוקות (w001084)
- **Catalogue's own identification text:** הלכות פסוקות (קדושין).
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('הלכות פסוקות') than the one this claim identifies ('הלכות פסוקות, תרגומים ועיבודים עבריים, הלכות קידושין'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label; owner ruling F -- see 136-GATE1-DECISIONS.md § F): plausibly `diverges_part` -- confirm the shade AND supply the separate Correctness call (catalogue_correct / claim_correct / unclear), or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 62

- **Manuscript:** Ms. Evr. Antonin B 236 (sys_id `990000905560205171`)
- **Work(s):** CLAIMED (this identification): מכילתא דרבי שמעון בן יוחאי (w000321) / CATALOGUE NAMES (found in the identification text): מכילתא דרבי ישמעאל (w000766)
- **Catalogue's own identification text:** מכילתא דרבי ישמעאל (בא-יתרו).
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('מכילתא דרבי ישמעאל') than the one this claim identifies ('מכילתא דרבי שמעון בן יוחאי'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label; owner ruling F -- see 136-GATE1-DECISIONS.md § F): plausibly `diverges_work` -- confirm the shade AND supply the separate Correctness call (catalogue_correct / claim_correct / unclear), or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 63

- **Manuscript:** Library of the Hungarian Academy of Sciences Ms. 57 (sys_id `990001004230205171`)
- **Work(s):** CLAIMED (this identification): ילקוט שמעוני על התורה (w001384) / CATALOGUE NAMES (found in the identification text): תנחומא (w000926)
- **Catalogue's own identification text:** מדרש תנחומא (קטעים).
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('תנחומא') than the one this claim identifies ('ילקוט שמעוני על התורה'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label; owner ruling F -- see 136-GATE1-DECISIONS.md § F): plausibly `diverges_work` -- confirm the shade AND supply the separate Correctness call (catalogue_correct / claim_correct / unclear), or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 64

- **Manuscript:** Cambridge University Library Ms. T-S NS 169.52 (sys_id `990051091870205171`)
- **Work(s):** CLAIMED (this identification): נסים גאון, חמשה ספרים (w000071) / CATALOGUE NAMES (found in the identification text): מגילת סתרים (w000509)
- **Catalogue's own identification text:** הלכה; מגילת סתרים [נסים בן יעקב]; ספרות הלכתית ופרשנות תלמודית; ספרות חז"ל. ; Halakhic Literature and Talmudic Commentaries ; Rabbinica; exposition of PT hagigah 78a and Tosefta Sheqalim 3:23-24 (the latter section numbered 140).
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('מגילת סתרים') than the one this claim identifies ('נסים גאון, חמשה ספרים'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 65

- **Manuscript:** Cambridge University Library Ms. T-S F 7.45 (sys_id `990051173260205171`)
- **Work(s):** CLAIMED (this identification): משנה תורה, הקדמה ומניין המצוות (w000174) / CATALOGUE NAMES (found in the identification text): הלכות ציצית (w001052)
- **Catalogue's own identification text:** משנה תורה;משנה תורה ופירושיו. ; Mishneh Torah and its Commentaries ; מנין מצות על סדר הרמב"ם, מסוף ספר אהבה (הלכות ציצית) עד סוף ספר זמנים
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('הלכות ציצית') than the one this claim identifies ('משנה תורה, הקדמה ומניין המצוות'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label; owner ruling F -- see 136-GATE1-DECISIONS.md § F): plausibly `diverges_part` -- confirm the shade AND supply the separate Correctness call (catalogue_correct / claim_correct / unclear), or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 66

- **Manuscript:** Ms. EVR II A 33 (sys_id `990000621960205171`)
- **Work(s):** CLAIMED (this identification): תנ"ך, בראשית (w000086) / CATALOGUE NAMES (found in the identification text): שאילתות (w000732)
- **Catalogue's own identification text:** שאילתות.
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('שאילתות') than the one this claim identifies ('תנ"ך, בראשית'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label; owner ruling F -- see 136-GATE1-DECISIONS.md § F): plausibly `diverges_work` -- confirm the shade AND supply the separate Correctness call (catalogue_correct / claim_correct / unclear), or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 67

- **Manuscript:** Cambridge University Library Ms. T-S C 1.23 (sys_id `990051150540205171`)
- **Work(s):** CLAIMED (this identification): בראשית רבה צה-צו, תוספת (w000900) / CATALOGUE NAMES (found in the identification text): בראשית רבה (w000156)
- **Catalogue's own identification text:** בראשית רבה;מדרש. ; Midrash ; בראשית רבה
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('בראשית רבה') than the one this claim identifies ('בראשית רבה צה-צו, תוספת'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label; owner ruling F -- see 136-GATE1-DECISIONS.md § F): plausibly `diverges_part` -- confirm the shade AND supply the separate Correctness call (catalogue_correct / claim_correct / unclear), or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 68

- **Manuscript:** Cambridge University Library Ms. T-S Misc. 15.67 (sys_id `990051080280205171`)
- **Work(s):** CLAIMED (this identification): ויקרא רבה (w000169) / CATALOGUE NAMES (found in the identification text): חובות הלבבות (תרגום אבן תיבון) (w000195)
- **Catalogue's own identification text:** חובות הלבבות (תרגום אבן תיבון). ; ספר חובות הלבבות. מעלת התשובה ומעלת הצדקה. דפוס.
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('חובות הלבבות (תרגום אבן תיבון)') than the one this claim identifies ('ויקרא רבה'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label; owner ruling F -- see 136-GATE1-DECISIONS.md § F): plausibly `diverges_work` -- confirm the shade AND supply the separate Correctness call (catalogue_correct / claim_correct / unclear), or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 69

- **Manuscript:** Library of the Alliance Israélite Un Ms. III A 101 (sys_id `990001506030205171`)
- **Work(s):** CLAIMED (this identification): רי"ף ברכות (w001317) / CATALOGUE NAMES (found in the identification text): מסכת דרך ארץ זוטא (w000787)
- **Catalogue's own identification text:** מסכת דרך ארץ זוטא (פרק השלום).
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('מסכת דרך ארץ זוטא') than the one this claim identifies ('רי"ף ברכות'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 70

- **Manuscript:** Cambridge University Library Ms. T-S C 1.19 (sys_id `990051150500205171`)
- **Work(s):** CLAIMED (this identification): פסיקתא דרב כהנא (w000904) / CATALOGUE NAMES (found in the identification text): ויקרא רבה (w000169)
- **Catalogue's own identification text:** ויקרא רבה;מדרש. ; Midrash ; ויקרא רבה
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('ויקרא רבה') than the one this claim identifies ('פסיקתא דרב כהנא'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 71

- **Manuscript:** Cambridge University Library Ms. T-S NS 291.103 (sys_id `990051104680205171`)
- **Work(s):** CLAIMED (this identification): רד"ק על יחזקאל (w001245) / CATALOGUE NAMES (found in the identification text): מדרש תהלים (w001124)
- **Catalogue's own identification text:** מדרש תהלים; ספרות הלכתית ופרשנות תלמודית; ספרות חז"ל; פירוש רד"ק למקרא. ; דוד בן יוסף קמחי, פירוש רד"ק למקרא: יחזקאל ח:א – יא
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('מדרש תהלים') than the one this claim identifies ('רד"ק על יחזקאל'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 72

- **Manuscript:** Adler, Elkan Nathan Ms. 2160.12 (sys_id `990053147670205171`)
- **Work(s):** CLAIMED (this identification): רס"ג, איכה תרגום (w000012) / CATALOGUE NAMES (found in the identification text): תרגום איכה (w001413)
- **Catalogue's own identification text:** מקרא [טקסט];תפסיר ערבי. ; תרגום איכה ד:א-כא לערבית-יהודית
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('תרגום איכה') than the one this claim identifies ('רס"ג, איכה תרגום'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 73

- **Manuscript:** Ms. EVR ARAB I 1771 (sys_id `990001535860205171`)
- **Work(s):** CLAIMED (this identification): ראב"ש, שמואל א פירוש (w000046) / CATALOGUE NAMES (found in the identification text): ספר הרקמה (w000037)
- **Catalogue's own identification text:** כתאב אלאסל : ; ספר הרקמה בערבית.
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('ספר הרקמה') than the one this claim identifies ('ראב"ש, שמואל א פירוש'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 74

- **Manuscript:** Cambridge University Library Ms. T-S E 1.2 (sys_id `990051158320205171`)
- **Work(s):** CLAIMED (this identification): משנה, פאה (w000259) / CATALOGUE NAMES (found in the identification text): ברייתא דמלאכת המשכן (w000157)
- **Catalogue's own identification text:** ברייתא דמלאכת המשכן;מסכת גרים;משנה [טקסט]. ; Mishnah: Pe'ah 4:4 – 6:3; Berakhot 1:1 – 3:1 ; משנה: פאה ד:ד – ו:ג; ברכות א:א – ג:א
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('ברייתא דמלאכת המשכן') than the one this claim identifies ('משנה, פאה'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 75

- **Manuscript:** Cambridge University Library Ms. T-S C 6.113 (sys_id `990051154860205171`)
- **Work(s):** CLAIMED (this identification): תלמוד בבלי, מגילה (w000957) / CATALOGUE NAMES (found in the identification text): ספרי דברים (w000166)
- **Catalogue's own identification text:** ספרי דברים;פירוש אבן עזרא למקרא;פרשנות מקרא. ; Sifre on Deuteronomy: 11:399 – 15:400; 7:404 – 15:400[בדילוגים] ; ספרי דברים: יא:399 – טו:400; ז:404 – טו:400[בדילוגים]
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('ספרי דברים') than the one this claim identifies ('תלמוד בבלי, מגילה'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 76

- **Manuscript:** Cambridge University Library Ms. T-S F 17.48 (sys_id `990051180030205171`)
- **Work(s):** CLAIMED (this identification): ויקרא רבה (w000169) / CATALOGUE NAMES (found in the identification text): קהלת רבה (w000891)
- **Catalogue's own identification text:** קהלת רבה;תלמוד ירושלמי [טקסט]. ; Midrash Rabbah Ecclesiastes: 5 ; קהלת רבה: ה
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('קהלת רבה') than the one this claim identifies ('ויקרא רבה'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 77

- **Manuscript:** Cambridge University Library Ms. T-S F 2(2).64 (sys_id `990051167990205171`)
- **Work(s):** CLAIMED (this identification): תלמוד בבלי, עבודה זרה (w000973) / CATALOGUE NAMES (found in the identification text): פסיקתא דרב כהנא (w000904)
- **Catalogue's own identification text:** מדרש;פסיקתא דרב כהנא;תלמוד בבלי [טקסט]. ; Talmud Bavli: Niddah 31 a – b; Sotah 44 b – 45 b; Avodah Zarah 51 a – 52 b ; תלמוד בבלי: נידה לא ע"א – ע"ב; סוטה מד ע"ב – מה ע"ב; עבודה זרה נא ע"א – נב ע"ב
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('פסיקתא דרב כהנא') than the one this claim identifies ('תלמוד בבלי, עבודה זרה'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 78

- **Manuscript:** Cambridge University Library Ms. L-G Talm. I 45 (sys_id `990001842460205171`)
- **Work(s):** CLAIMED (this identification): הלכות ארץ ישראליות ובבליות וליקוטים ממעשים לבני ארץ ישראל (w001075) / CATALOGUE NAMES (found in the identification text): מעשים לבני ארץ ישראל (w001030)
- **Catalogue's own identification text:** מעשים לבני ארץ ישראל.
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('מעשים לבני ארץ ישראל') than the one this claim identifies ('הלכות ארץ ישראליות ובבליות וליקוטים ממעשים לבני ארץ ישראל'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 79

- **Manuscript:** MS heb. e.22/1 (sys_id `990053426460205171`)
- **Work(s):** CLAIMED (this identification): הלכות פסוקות (w001084) / CATALOGUE NAMES (found in the identification text): והזהיר (w000779)
- **Catalogue's own identification text:** ספר והזהיר. ; והזהיר, פסק"ז
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('והזהיר') than the one this claim identifies ('הלכות פסוקות'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 80

- **Manuscript:** Cambridge University Library Ms. T-S NS 309.81 (sys_id `990051106940205171`)
- **Work(s):** CLAIMED (this identification): מסכת דרך ארץ זוטא (w000787) / CATALOGUE NAMES (found in the identification text): מסכת דרך ארץ רבה (w000786)
- **Catalogue's own identification text:** מסכת דרך ארץ רבה; סידור שלמה מסיג'ילמאסה; ספרות הלכתית ופרשנות תלמודית; ספרות חז"ל; קולופון. ; Halakhic Literature and Talmudic Commentaries ; שלמה בן נתן מסיג'ילמאסה, סידור שלמה מסיג'ילמאסה: ל
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('מסכת דרך ארץ רבה') than the one this claim identifies ('מסכת דרך ארץ זוטא'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 81

- **Manuscript:** Adler, Elkan Nathan Ms. 2700.24 (sys_id `990053586310205171`)
- **Work(s):** CLAIMED (this identification): פירוש המשנה, נזיקין (w000028) / CATALOGUE NAMES (found in the identification text): משנה, סנהדרין (w000291)
- **Catalogue's own identification text:** משנה [טקסט];פירוש המשנה לרמב"ם [ערבית]. ; פירוש הרמב"ם למשנה, סנהדרין י:א, ההקדמה לפרק חלק, עיקרים ראשון ושני (מהד' קאפח ע' רי-ריא).
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('משנה, סנהדרין') than the one this claim identifies ('פירוש המשנה, נזיקין'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 82

- **Manuscript:** Cambridge University Library Ms. T-S C 1.60 (sys_id `990051150910205171`)
- **Work(s):** CLAIMED (this identification): ברייתא דישועה, הפירוש, נוסח אחר (w000755) / CATALOGUE NAMES (found in the identification text): ברייתא דישועה (w000754)
- **Catalogue's own identification text:** ברייתא דישועה;מדרש. ; Midrash ; ברייתא דישועה
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('ברייתא דישועה') than the one this claim identifies ('ברייתא דישועה, הפירוש, נוסח אחר'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 83

- **Manuscript:** Cambridge University Library Ms. T-S NS 329/0609 (sys_id `990001420410205171`)
- **Work(s):** CLAIMED (this identification): פסיקתא, ״אנכי אדוני אלהיך״ (איש שלום צח) (w000506) / CATALOGUE NAMES (found in the identification text): פסיקתא רבתי (w000803)
- **Catalogue's own identification text:** פסיקתא רבתי (פרשה כג:ו) :
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('פסיקתא רבתי') than the one this claim identifies ('פסיקתא, ״אנכי אדוני אלהיך״ (איש שלום צח)'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

### Class 7 -- liturgical-container predictability (NOVELTY SHADE, owner rulings H/I) (12 candidates)

**Plausible shades for this class:** `container_predicts`, `fills_gap`, `confirms` (any other shade from the vocabulary table above is still a valid answer if the case warrants it; `unsure` / `skip` are always available).

#### Case 84

- **Manuscript:** Cambridge University Library Ms. Add. 3356 (sys_id `990001398970205171`)
- **Work(s):** תנ"ך, תהלים (w000112)
- **Catalogue's own identification text:** מחזור מנהג ארץ ישראל הקדמון.
- **Why it is hard:** This manuscript's own catalogue identification text names a SPECIFIC, NAMED standard-rite prayer-book/cycle (a container collocation: 'מחזור מנהג'), whose standard, predictable content plausibly includes this specific claimed unit -- without the catalogue text ever naming the unit itself. Under the pre-H shade enum this would fall through to `fills_gap` by elimination, which would publish a standard siddur/machzor component as a candidate new find.
- **PROPOSAL (draft, not a label; owner ruling H -- see 136-GATE1-DECISIONS.md § H): plausibly `container_predicts` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `container_predicts`, `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 85

- **Manuscript:** Cambridge University Library Ms. T-S Misc. 33/03 (sys_id `990001413070205171`)
- **Work(s):** תנ"ך, שמות (w000087)
- **Catalogue's own identification text:** מחזור מנהג אשכנז לשלש רגלים : ; קטעי גניזה.
- **Why it is hard:** This manuscript's own catalogue identification text names a SPECIFIC, NAMED standard-rite prayer-book/cycle (a container collocation: 'מחזור מנהג'), whose standard, predictable content plausibly includes this specific claimed unit -- without the catalogue text ever naming the unit itself. Under the pre-H shade enum this would fall through to `fills_gap` by elimination, which would publish a standard siddur/machzor component as a candidate new find.
- **PROPOSAL (draft, not a label; owner ruling H -- see 136-GATE1-DECISIONS.md § H): plausibly `container_predicts` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `container_predicts`, `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 86

- **Manuscript:** Ms. EVR II A 58 (sys_id `990001429050205171`)
- **Work(s):** תנ"ך, ישעיהו (w000097)
- **Catalogue's own identification text:** מחזור מנהג ספרד לשלש רגלים.
- **Why it is hard:** This manuscript's own catalogue identification text names a SPECIFIC, NAMED standard-rite prayer-book/cycle (a container collocation: 'מחזור מנהג'), whose standard, predictable content plausibly includes this specific claimed unit -- without the catalogue text ever naming the unit itself. Under the pre-H shade enum this would fall through to `fills_gap` by elimination, which would publish a standard siddur/machzor component as a candidate new find.
- **PROPOSAL (draft, not a label; owner ruling H -- see 136-GATE1-DECISIONS.md § H): plausibly `container_predicts` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `container_predicts`, `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 87

- **Manuscript:** Ms. EVR II A 300/4 (sys_id `990001436770205171`)
- **Work(s):** תנ"ך, דברים (w000090)
- **Catalogue's own identification text:** סדור מנהג קראים.
- **Why it is hard:** This manuscript's own catalogue identification text names a SPECIFIC, NAMED standard-rite prayer-book/cycle (a container collocation: 'סדור מנהג'), whose standard, predictable content plausibly includes this specific claimed unit -- without the catalogue text ever naming the unit itself. Under the pre-H shade enum this would fall through to `fills_gap` by elimination, which would publish a standard siddur/machzor component as a candidate new find.
- **PROPOSAL (draft, not a label; owner ruling H -- see 136-GATE1-DECISIONS.md § H): plausibly `container_predicts` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `container_predicts`, `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 88

- **Manuscript:** Ms. EVR II A 810 (sys_id `990001442520205171`)
- **Work(s):** תנ"ך, ויקרא (w000088)
- **Catalogue's own identification text:** סדור מנהג קראים (קטע).
- **Why it is hard:** This manuscript's own catalogue identification text names a SPECIFIC, NAMED standard-rite prayer-book/cycle (a container collocation: 'סדור מנהג'), whose standard, predictable content plausibly includes this specific claimed unit -- without the catalogue text ever naming the unit itself. Under the pre-H shade enum this would fall through to `fills_gap` by elimination, which would publish a standard siddur/machzor component as a candidate new find.
- **PROPOSAL (draft, not a label; owner ruling H -- see 136-GATE1-DECISIONS.md § H): plausibly `container_predicts` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `container_predicts`, `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 89

- **Manuscript:** Ms. EVR II A 1006 (sys_id `990001444570205171`)
- **Work(s):** תנ"ך, יחזקאל (w000099)
- **Catalogue's own identification text:** סדור מנהג קראים.
- **Why it is hard:** This manuscript's own catalogue identification text names a SPECIFIC, NAMED standard-rite prayer-book/cycle (a container collocation: 'סדור מנהג'), whose standard, predictable content plausibly includes this specific claimed unit -- without the catalogue text ever naming the unit itself. Under the pre-H shade enum this would fall through to `fills_gap` by elimination, which would publish a standard siddur/machzor component as a candidate new find.
- **PROPOSAL (draft, not a label; owner ruling H -- see 136-GATE1-DECISIONS.md § H): plausibly `container_predicts` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `container_predicts`, `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 90

- **Manuscript:** Ms. EVR II A 642 (sys_id `990001441300205171`)
- **Work(s):** תנ"ך, במדבר (w000089)
- **Catalogue's own identification text:** סדור מנהג קראים (קטע).
- **Why it is hard:** This manuscript's own catalogue identification text names a SPECIFIC, NAMED standard-rite prayer-book/cycle (a container collocation: 'סדור מנהג'), whose standard, predictable content plausibly includes this specific claimed unit -- without the catalogue text ever naming the unit itself. Under the pre-H shade enum this would fall through to `fills_gap` by elimination, which would publish a standard siddur/machzor component as a candidate new find.
- **PROPOSAL (draft, not a label; owner ruling H -- see 136-GATE1-DECISIONS.md § H): plausibly `container_predicts` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `container_predicts`, `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 91

- **Manuscript:** Library of the Hungarian Academy of Sciences Ms. 116 (sys_id `990001035960205171`)
- **Work(s):** משנה תורה, ספר אהבה (w000176)
- **Catalogue's own identification text:** מחזור מנהג ספרד לראש השנה (קטע).
- **Why it is hard:** This manuscript's own catalogue identification text names a SPECIFIC, NAMED standard-rite prayer-book/cycle (a container collocation: 'מחזור מנהג'), whose standard, predictable content plausibly includes this specific claimed unit -- without the catalogue text ever naming the unit itself. Under the pre-H shade enum this would fall through to `fills_gap` by elimination, which would publish a standard siddur/machzor component as a candidate new find.
- **PROPOSAL (draft, not a label; owner ruling H -- see 136-GATE1-DECISIONS.md § H): plausibly `container_predicts` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `container_predicts`, `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 92

- **Manuscript:** Ms. EVR II A 834 (sys_id `990001442760205171`)
- **Work(s):** תנ"ך, ירמיהו (w000098)
- **Catalogue's own identification text:** סדור מנהג קראים (קטעים).
- **Why it is hard:** This manuscript's own catalogue identification text names a SPECIFIC, NAMED standard-rite prayer-book/cycle (a container collocation: 'סדור מנהג'), whose standard, predictable content plausibly includes this specific claimed unit -- without the catalogue text ever naming the unit itself. Under the pre-H shade enum this would fall through to `fills_gap` by elimination, which would publish a standard siddur/machzor component as a candidate new find.
- **PROPOSAL (draft, not a label; owner ruling H -- see 136-GATE1-DECISIONS.md § H): plausibly `container_predicts` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `container_predicts`, `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 93

- **Manuscript:** Ms. EVR II A 2007 (sys_id `990001454910205171`)
- **Work(s):** תנ"ך, דניאל (w000120)
- **Catalogue's own identification text:** סדור מנהג קראים (קטעים).
- **Why it is hard:** This manuscript's own catalogue identification text names a SPECIFIC, NAMED standard-rite prayer-book/cycle (a container collocation: 'סדור מנהג'), whose standard, predictable content plausibly includes this specific claimed unit -- without the catalogue text ever naming the unit itself. Under the pre-H shade enum this would fall through to `fills_gap` by elimination, which would publish a standard siddur/machzor component as a candidate new find.
- **PROPOSAL (draft, not a label; owner ruling H -- see 136-GATE1-DECISIONS.md § H): plausibly `container_predicts` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `container_predicts`, `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 94

- **Manuscript:** Ms. EVR II A 2060 (sys_id `990001455500205171`)
- **Work(s):** תנ"ך, דברי הימים ב (w000124)
- **Catalogue's own identification text:** סדור מנהג קראים (קטעים).
- **Why it is hard:** This manuscript's own catalogue identification text names a SPECIFIC, NAMED standard-rite prayer-book/cycle (a container collocation: 'סדור מנהג'), whose standard, predictable content plausibly includes this specific claimed unit -- without the catalogue text ever naming the unit itself. Under the pre-H shade enum this would fall through to `fills_gap` by elimination, which would publish a standard siddur/machzor component as a candidate new find.
- **PROPOSAL (draft, not a label; owner ruling H -- see 136-GATE1-DECISIONS.md § H): plausibly `container_predicts` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `container_predicts`, `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 95

- **Manuscript:** Ms. EVR II A 867 (sys_id `990001443150205171`)
- **Work(s):** תנ"ך, דברי הימים א (w000123)
- **Catalogue's own identification text:** סדור מנהג קראים (קטעים).
- **Why it is hard:** This manuscript's own catalogue identification text names a SPECIFIC, NAMED standard-rite prayer-book/cycle (a container collocation: 'סדור מנהג'), whose standard, predictable content plausibly includes this specific claimed unit -- without the catalogue text ever naming the unit itself. Under the pre-H shade enum this would fall through to `fills_gap` by elimination, which would publish a standard siddur/machzor component as a candidate new find.
- **PROPOSAL (draft, not a label; owner ruling H -- see 136-GATE1-DECISIONS.md § H): plausibly `container_predicts` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `container_predicts`, `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

