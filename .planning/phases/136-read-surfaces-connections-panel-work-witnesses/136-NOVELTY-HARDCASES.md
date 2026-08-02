# Phase 136 Plan 03 -- Novelty Hard-Case Candidates

Candidates the novelty funnel's owner-labelled ground truth (plan 136-03 Task 3) will be drawn from. The original three classes D-23c names -- **near-miss titles**, **alias pairs**, and a **catalogue entry naming a different GRANULARITY of the same work** -- were selected adversarially to a STRING heuristic, not to an LLM. Classes 4 and 5 are an OWNER-AUTHORIZED scope extension (`136-GATE1-DECISIONS.md` item C), added so the measured novelty-funnel error rate is not flattered by cases an LLM finds easy: **terse or missing catalogue identification text** and **generic collection works** (responsa/piyyut/collection titles recurring across many distinct catalogued items, where "already recorded" is genuinely ill-defined rather than merely hard to string-match). **Class 6 -- catalogue divergence -- is a further owner-authorized extension (`136-GATE1-DECISIONS.md` item E)**: real shipped claims where an available finding aid ties the SAME fragment to a DIFFERENT work that is NOT a D-13d granularity variant -- the shade item E's ruling calls `diverges`, with ZERO representation in Classes 1-5. All six classes are selected entirely by string/title/metadata comparison over the works and manuscripts already in the deployed asset -- **zero model calls, measured cost $0.00**. Every existing case from the original 82 is kept unchanged; Class 6 is purely additive. Any attached draft verdict below is explicitly marked `PROPOSAL` and is a reading aid only, never a label -- it is NOT filled in by this script as an owner answer. **Correction E′ (`136-GATE1-DECISIONS.md` § E′, same day as decision E, a correction to it and not a new ruling)** splits decision E's `refines_granularity` shade by direction, adding `aid_more_specific` -- see the updated vocabulary table below. This worksheet is also emitted as `136-NOVELTY-HARDCASES.xlsx` (same phase directory) for owners who find Hebrew RTL easier to work with in a spreadsheet; both files render the SAME 97 cases in the SAME order, from the same pre-numbered case list, so the two agree case-for-case.

## Verdict vocabulary (amended 2026-08-02, owner decisions E / E′ -- see `136-GATE1-DECISIONS.md` items E and E′)

Novelty is no longer a tri-state (`already_recorded` / `not_in_finding_aids` / `unsure`). The owner ruled it into an EIGHT-shade enum (decision E's original seven, direction-split by correction E′ into eight) because the tri-state collapsed materially different findings into one bucket -- a catalogue CONTRADICTION and a genuine "previously unknown" both used to score the same way, and (per E′) a granularity refinement that helps and one that adds nothing also used to score the same way. For EACH case below, answer with the shade that best describes what an enumerable finding aid (the catalogue's own identification field, bibliography, titles, PGP, FGP, M-source shelfmark attributions) actually says about THIS fragment and THIS work -- or `unsure` / `skip`.

| Shade | Choose this when... |
|---|---|
| `confirms` | an aid already ties this fragment to this work |
| `refines_granularity` | OUR claim is MORE SPECIFIC (finer) than what an aid says -- e.g. the catalogue names the whole work, our claim names a specific book/chapter of it (the D-13d same-author/related-title rule); the OPPOSITE direction from `aid_more_specific` -- we ADD precision here |
| `aid_more_specific` | an AID names a MORE SPECIFIC (finer) variant of this fragment's work than our claim does -- e.g. the catalogue names a chapter/book, our claim names the whole work (the D-13d same-author/related-title rule); the OPPOSITE direction from `refines_granularity` -- we add NOTHING here, the aid already knew more (owner correction E′; the LEAST novel shade) |
| `diverges` | an aid ties this fragment to a DIFFERENT work that is NOT a granularity variant -- the aid and the claim contradict each other |
| `fills_gap` | the aids identify this fragment as nothing at all -- the genuine "previously unknown" case |
| `extends` | aids tie OTHER folios of the SAME manuscript to this work, but not this specific folio |
| `alias_merge` | the two work_ids shown ARE the same underlying work, not yet canonically merged (Class 2's situation) |
| `unsure` | you cannot judge this case from the information shown -- maps to `not_checked`, costs nothing, is a real and useful answer |
| `skip` | you choose not to judge this case at all -- recorded as skipped, NEVER filled from a draft `PROPOSAL` |

`not_checked` (the fail-closed system default for an unrun/failed/abstained check) is not a verdict the owner picks directly -- `unsure` is its owner-facing equivalent.

## Class 3 -- catalogue entry naming a different granularity of the same work (20 candidates)

**Plausible shades for this class:** `refines_granularity`, `aid_more_specific`, `confirms`, `diverges` (any other shade from the vocabulary table above is still a valid answer if the case warrants it; `unsure` / `skip` are always available).

### Case 1

- **Manuscript:** Cambridge University Library Ms. T-S Misc. 12.31.14 (sys_id `990051079570205171`)
- **Work(s):** רש"י על התורה (w000171) / רש"י על בראשית (w001281)
- **Catalogue's own identification text:** פרשנות מקרא רבנית. ; Solomon b. Isaac, Rashi, Biblical Exegesis - Rabbanite: Genesis 44 ; שלמה בן יצחק, פרשנות מקרא רבנית: בראשית מד
- **Why it is hard:** Byte-identical span 0-962 on this page is claimed by 2 works sharing the same author and a common title stem -- a title-containment/alias relationship a plain string comparison cannot resolve on its own (same underlying commentary at two catalogued granularities, or two genuinely distinct works?).
- **PROPOSAL (draft, not a label): plausibly the SAME underlying work at two granularities -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `refines_granularity`, `aid_more_specific`, `confirms`, `diverges`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 2

- **Manuscript:** Ms. EVR II A 610/02 (sys_id `990000851320205171`)
- **Work(s):** משנה תורה, הקדמה ומניין המצוות (w000174) / משנה תורה, ספר שופטים (w000188)
- **Catalogue's own identification text:** משנה תורה (ספר שופטים).
- **Why it is hard:** Byte-identical span 0-617 on this page is claimed by 2 works sharing the same author and a common title stem -- a title-containment/alias relationship a plain string comparison cannot resolve on its own (same underlying commentary at two catalogued granularities, or two genuinely distinct works?).
- **PROPOSAL (draft, not a label): plausibly the SAME underlying work at two granularities -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `refines_granularity`, `aid_more_specific`, `confirms`, `diverges`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 3

- **Manuscript:** Ms. EVR II A 118/01 (sys_id `990000852430205171`)
- **Work(s):** רש"י על התורה (w000171) / רש"י על שמות (w001278)
- **Catalogue's own identification text:** פרוש התורה לרש"י.
- **Why it is hard:** Byte-identical span 0-2500 on this page is claimed by 2 works sharing the same author and a common title stem -- a title-containment/alias relationship a plain string comparison cannot resolve on its own (same underlying commentary at two catalogued granularities, or two genuinely distinct works?).
- **PROPOSAL (draft, not a label): plausibly the SAME underlying work at two granularities -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `refines_granularity`, `aid_more_specific`, `confirms`, `diverges`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 4

- **Manuscript:** Ms. EVR II A 263/07 (sys_id `990000852510205171`)
- **Work(s):** רש"י על התורה (w000171) / רש"י על שמות (w001278)
- **Catalogue's own identification text:** פרוש התורה לרש"י (שמות כא:כו-כו:כד).
- **Why it is hard:** Byte-identical span 0-3022 on this page is claimed by 2 works sharing the same author and a common title stem -- a title-containment/alias relationship a plain string comparison cannot resolve on its own (same underlying commentary at two catalogued granularities, or two genuinely distinct works?).
- **PROPOSAL (draft, not a label): plausibly the SAME underlying work at two granularities -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `refines_granularity`, `aid_more_specific`, `confirms`, `diverges`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 5

- **Manuscript:** Ms. EVR II A 263/01 (sys_id `990000853660205171`)
- **Work(s):** רש"י על התורה (w000171) / רש"י על במדבר (w001304)
- **Catalogue's own identification text:** פרוש התורה לרש"י (במדבר).
- **Why it is hard:** Byte-identical span 0-1846 on this page is claimed by 2 works sharing the same author and a common title stem -- a title-containment/alias relationship a plain string comparison cannot resolve on its own (same underlying commentary at two catalogued granularities, or two genuinely distinct works?).
- **PROPOSAL (draft, not a label): plausibly the SAME underlying work at two granularities -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `refines_granularity`, `aid_more_specific`, `confirms`, `diverges`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 6

- **Manuscript:** Ms. EVR II A 246/02 (sys_id `990000853670205171`)
- **Work(s):** רש"י על התורה (w000171) / רש"י על שמות (w001278)
- **Catalogue's own identification text:** פרוש התורה לרש"י (בראשית כ-כד, שמות כ-לד, קטעים).
- **Why it is hard:** Byte-identical span 0-1087 on this page is claimed by 2 works sharing the same author and a common title stem -- a title-containment/alias relationship a plain string comparison cannot resolve on its own (same underlying commentary at two catalogued granularities, or two genuinely distinct works?).
- **PROPOSAL (draft, not a label): plausibly the SAME underlying work at two granularities -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `refines_granularity`, `aid_more_specific`, `confirms`, `diverges`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 7

- **Manuscript:** Ms. EVR II A 244/07 (sys_id `990000853680205171`)
- **Work(s):** רש"י על התורה (w000171) / רש"י על דברים (w001275)
- **Catalogue's own identification text:** פרוש התורה לרש"י (דברים כט:י-לב:יג, קטעים).
- **Why it is hard:** Byte-identical span 0-1198 on this page is claimed by 2 works sharing the same author and a common title stem -- a title-containment/alias relationship a plain string comparison cannot resolve on its own (same underlying commentary at two catalogued granularities, or two genuinely distinct works?).
- **PROPOSAL (draft, not a label): plausibly the SAME underlying work at two granularities -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `refines_granularity`, `aid_more_specific`, `confirms`, `diverges`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 8

- **Manuscript:** Ms. EVR II A 118/02 (sys_id `990000853710205171`)
- **Work(s):** רש"י על התורה (w000171) / רש"י על דברים (w001275)
- **Catalogue's own identification text:** פרוש התורה לרש"י (שמות, במדבר-דברים).
- **Why it is hard:** Byte-identical span 0-1591 on this page is claimed by 2 works sharing the same author and a common title stem -- a title-containment/alias relationship a plain string comparison cannot resolve on its own (same underlying commentary at two catalogued granularities, or two genuinely distinct works?).
- **PROPOSAL (draft, not a label): plausibly the SAME underlying work at two granularities -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `refines_granularity`, `aid_more_specific`, `confirms`, `diverges`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 9

- **Manuscript:** Ms. EVR II A 394.1 (sys_id `990000868890205171`)
- **Work(s):** רש"י על התורה (w000171) / רש"י על שמות (w001278)
- **Catalogue's own identification text:** פרוש התורה לרש"י (שמות כח:לה-ל:יג).
- **Why it is hard:** Byte-identical span 0-993 on this page is claimed by 2 works sharing the same author and a common title stem -- a title-containment/alias relationship a plain string comparison cannot resolve on its own (same underlying commentary at two catalogued granularities, or two genuinely distinct works?).
- **PROPOSAL (draft, not a label): plausibly the SAME underlying work at two granularities -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `refines_granularity`, `aid_more_specific`, `confirms`, `diverges`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 10

- **Manuscript:** Ms. EVR II A 363 (sys_id `990000869400205171`)
- **Work(s):** רש"י על התורה (w000171) / רש"י על ויקרא (w001299)
- **Catalogue's own identification text:** פרוש התורה לרש"י (שמות יט:יז-כ:ב; ויקרא יט:א-במדבר יא:כו).
- **Why it is hard:** Byte-identical span 0-1510 on this page is claimed by 2 works sharing the same author and a common title stem -- a title-containment/alias relationship a plain string comparison cannot resolve on its own (same underlying commentary at two catalogued granularities, or two genuinely distinct works?).
- **PROPOSAL (draft, not a label): plausibly the SAME underlying work at two granularities -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `refines_granularity`, `aid_more_specific`, `confirms`, `diverges`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 11

- **Manuscript:** Ms. EVR II A 265 (sys_id `990000872080205171`)
- **Work(s):** רש"י על התורה (w000171) / רש"י על שמות (w001278)
- **Catalogue's own identification text:** פרוש התורה לרש"י (בראשית, קטעים).
- **Why it is hard:** Byte-identical span 0-780 on this page is claimed by 2 works sharing the same author and a common title stem -- a title-containment/alias relationship a plain string comparison cannot resolve on its own (same underlying commentary at two catalogued granularities, or two genuinely distinct works?).
- **PROPOSAL (draft, not a label): plausibly the SAME underlying work at two granularities -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `refines_granularity`, `aid_more_specific`, `confirms`, `diverges`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 12

- **Manuscript:** Gaster, Moses Collection (sys_id `990001256610205171`)
- **Work(s):** משנה תורה, הקדמה ומניין המצוות (w000174) / משנה תורה, ספר עבודה (w000182)
- **Catalogue's own identification text:** משנה תורה (הקדמה, מניין המצוות לפי סדר הספרים, קטע).
- **Why it is hard:** Byte-identical span 0-436 on this page is claimed by 2 works sharing the same author and a common title stem -- a title-containment/alias relationship a plain string comparison cannot resolve on its own (same underlying commentary at two catalogued granularities, or two genuinely distinct works?).
- **PROPOSAL (draft, not a label): plausibly the SAME underlying work at two granularities -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `refines_granularity`, `aid_more_specific`, `confirms`, `diverges`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 13

- **Manuscript:** Hebrew Union College Library Ms. 1101-1195 (sys_id `990001273010205171`)
- **Work(s):** משנה תורה, הקדמה ומניין המצוות (w000174) / משנה תורה, ספר זרעים (w000181)
- **Catalogue's own identification text:** קטעי גניזה.
- **Why it is hard:** Byte-identical span 0-662 on this page is claimed by 2 works sharing the same author and a common title stem -- a title-containment/alias relationship a plain string comparison cannot resolve on its own (same underlying commentary at two catalogued granularities, or two genuinely distinct works?).
- **PROPOSAL (draft, not a label): plausibly the SAME underlying work at two granularities -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `refines_granularity`, `aid_more_specific`, `confirms`, `diverges`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 14

- **Manuscript:** Ms. EVR II A 2882 (sys_id `990001464220205171`)
- **Work(s):** רש"י על התורה (w000171) / רש"י על במדבר (w001304)
- **Catalogue's own identification text:** פרוש התורה (במדבר, קטע).
- **Why it is hard:** Byte-identical span 0-685 on this page is claimed by 2 works sharing the same author and a common title stem -- a title-containment/alias relationship a plain string comparison cannot resolve on its own (same underlying commentary at two catalogued granularities, or two genuinely distinct works?).
- **PROPOSAL (draft, not a label): plausibly the SAME underlying work at two granularities -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `refines_granularity`, `aid_more_specific`, `confirms`, `diverges`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 15

- **Manuscript:** Ms. EVR ARAB I 1747 (sys_id `990001535680205171`)
- **Work(s):** רס"ג, ישעיה תרגום (w000073) / רס״ג, ישעיהו פירוש (w001148)
- **Catalogue's own identification text:** פרוש נביאים (ישעיה, קטע).
- **Why it is hard:** Byte-identical span 0-1091 on this page is claimed by 2 works sharing the same author and a common title stem -- a title-containment/alias relationship a plain string comparison cannot resolve on its own (same underlying commentary at two catalogued granularities, or two genuinely distinct works?).
- **PROPOSAL (draft, not a label): plausibly the SAME underlying work at two granularities -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `refines_granularity`, `aid_more_specific`, `confirms`, `diverges`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 16

- **Manuscript:** Ms. EVR ARAB I 2121 (sys_id `990001539390205171`)
- **Work(s):** רס"ג, ישעיה תרגום (w000073) / רס״ג, ישעיהו פירוש (w001148)
- **Catalogue's own identification text:** פרוש נביאים (ישעיה, קטע).
- **Why it is hard:** Byte-identical span 13-463 on this page is claimed by 2 works sharing the same author and a common title stem -- a title-containment/alias relationship a plain string comparison cannot resolve on its own (same underlying commentary at two catalogued granularities, or two genuinely distinct works?).
- **PROPOSAL (draft, not a label): plausibly the SAME underlying work at two granularities -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `refines_granularity`, `aid_more_specific`, `confirms`, `diverges`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 17

- **Manuscript:** Ms. EVR ARAB I 3075 (sys_id `990001548960205171`)
- **Work(s):** רס"ג, ישעיה תרגום (w000073) / רס״ג, ישעיהו פירוש (w001148)
- **Catalogue's own identification text:** פרוש נביאים לרס"ג.
- **Why it is hard:** Byte-identical span 0-1326 on this page is claimed by 2 works sharing the same author and a common title stem -- a title-containment/alias relationship a plain string comparison cannot resolve on its own (same underlying commentary at two catalogued granularities, or two genuinely distinct works?).
- **PROPOSAL (draft, not a label): plausibly the SAME underlying work at two granularities -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `refines_granularity`, `aid_more_specific`, `confirms`, `diverges`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 18

- **Manuscript:** Ms. EVR ARAB I 4651 (sys_id `990001563390205171`)
- **Work(s):** רס"ג, שמות פירוש (w000054) / רס"ג, בראשית פירוש (w000060)
- **Catalogue's own identification text:** פרוש התורה (בראשית, קטע).
- **Why it is hard:** Byte-identical span 0-633 on this page is claimed by 2 works sharing the same author and a common title stem -- a title-containment/alias relationship a plain string comparison cannot resolve on its own (same underlying commentary at two catalogued granularities, or two genuinely distinct works?).
- **PROPOSAL (draft, not a label): plausibly the SAME underlying work at two granularities -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `refines_granularity`, `aid_more_specific`, `confirms`, `diverges`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 19

- **Manuscript:** Ms. EVR ARAB I 4767 (sys_id `990001564430205171`)
- **Work(s):** רס"ג, שמות תרגום (תפסיר תורה) (w000033) / רס"ג, שמות פירוש (w000054)
- **Catalogue's own identification text:** תרגום ופרוש ערבי לתורה (קטע).
- **Why it is hard:** Byte-identical span 625-1572 on this page is claimed by 2 works sharing the same author and a common title stem -- a title-containment/alias relationship a plain string comparison cannot resolve on its own (same underlying commentary at two catalogued granularities, or two genuinely distinct works?).
- **PROPOSAL (draft, not a label): plausibly the SAME underlying work at two granularities -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `refines_granularity`, `aid_more_specific`, `confirms`, `diverges`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 20

- **Manuscript:** Ms. EVR II C 465 (sys_id `990001588480205171`)
- **Work(s):** רש"י על התורה (w000171) / רש"י על במדבר (w001304)
- **Catalogue's own identification text:** תורה (במדבר ד).
- **Why it is hard:** Byte-identical span 0-246 on this page is claimed by 2 works sharing the same author and a common title stem -- a title-containment/alias relationship a plain string comparison cannot resolve on its own (same underlying commentary at two catalogued granularities, or two genuinely distinct works?).
- **PROPOSAL (draft, not a label): plausibly the SAME underlying work at two granularities -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `refines_granularity`, `aid_more_specific`, `confirms`, `diverges`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

## Class 2 -- alias pairs (12 candidates)

**Plausible shades for this class:** `alias_merge`, `confirms`, `fills_gap` (any other shade from the vocabulary table above is still a valid answer if the case warrants it; `unsure` / `skip` are always available).

### Case 21

- **Manuscript:** Cambridge University Library Ms. T-S NS 288.21 (sys_id `990051103040205171`)
- **Work(s):** האיי גאון על שבת (w000448, msource) / האיי גאון על שבת (w000451, msource)
- **Catalogue's own identification text:** ספרות חז"ל; פירוש רב שרירא גאון לתלמוד; פירושי תלמוד בבלי. ; Rabbinic Literature ; פירוש לתלמוד: שבת יא ע"ב – יב ע"ב; טז ע"ב – יז ע"א
- **Why it is hard:** Two DIFFERENT work_ids share both the same author and an identical normalized title (a two-member cluster, not a large generic-collection cluster) -- a likely un-merged cross-corpus alias/duplicate.
- **PROPOSAL (draft, not a label): plausibly an alias pair (same work, not yet canonically merged) -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `alias_merge`, `confirms`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 22

- **Manuscript:** (no shipped claim instance found for either work)
- **Work(s):** תשובות הילאי גאון (w000575, msource) / תשובות הילאי גאון (w000576, msource)
- **Why it is hard:** Two DIFFERENT work_ids share both the same author and an identical normalized title (a two-member cluster, not a large generic-collection cluster) -- a likely un-merged cross-corpus alias/duplicate.
- **PROPOSAL (draft, not a label): plausibly an alias pair (same work, not yet canonically merged) -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `alias_merge`, `confirms`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 23

- **Manuscript:** (no shipped claim instance found for either work)
- **Work(s):** תשובות חנוך בן משה (w000475, msource) / תשובות חנוך בן משה (w000477, msource)
- **Why it is hard:** Two DIFFERENT work_ids share both the same author and an identical normalized title (a two-member cluster, not a large generic-collection cluster) -- a likely un-merged cross-corpus alias/duplicate.
- **PROPOSAL (draft, not a label): plausibly an alias pair (same work, not yet canonically merged) -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `alias_merge`, `confirms`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 24

- **Manuscript:** sys_id `990038675800205171` (no shelfmark on file)
- **Work(s):** רבנו חננאל על מסכת ביצה (w000457, msource) / רבנו חננאל על מסכת ביצה (w001227, sefaria)
- **Catalogue's own identification text:** ספר העתים (קטע).
- **Why it is hard:** Two DIFFERENT work_ids share both the same author and an identical normalized title (a two-member cluster, not a large generic-collection cluster) -- a likely un-merged cross-corpus alias/duplicate.
- **PROPOSAL (draft, not a label): plausibly an alias pair (same work, not yet canonically merged) -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `alias_merge`, `confirms`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 25

- **Manuscript:** Cambridge University Library Ms. T-S NS 329.424 (sys_id `990051728630205171`)
- **Work(s):** רבנו חננאל על מסכת חגיגה (w000462, msource) / רבנו חננאל על מסכת חגיגה (w001228, sefaria)
- **Catalogue's own identification text:** הלכות הרי"ף;ספרות הלכתית ופרשנות תלמודית;ספרות חז"ל. ; Isaac Al-Fasi, Hilkhot ha-Rif: Mo'ed Qatan 1 ; יצחק אלפסי, הלכות הרי"ף: מועד קטן א
- **Why it is hard:** Two DIFFERENT work_ids share both the same author and an identical normalized title (a two-member cluster, not a large generic-collection cluster) -- a likely un-merged cross-corpus alias/duplicate.
- **PROPOSAL (draft, not a label): plausibly an alias pair (same work, not yet canonically merged) -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `alias_merge`, `confirms`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 26

- **Manuscript:** MS heb. d.45/23 (sys_id `990053408970205171`)
- **Work(s):** רבנו חננאל על מסכת מכות (w000466, msource) / רבנו חננאל על מסכת מכות (w001233, sefaria)
- **Catalogue's own identification text:** פירושי תלמוד בבלי. ; Talmudic Commentaries: Sanhedrin 90; Makkot ; פירושי תלמוד: סנהדרין צ; מכות
- **Why it is hard:** Two DIFFERENT work_ids share both the same author and an identical normalized title (a two-member cluster, not a large generic-collection cluster) -- a likely un-merged cross-corpus alias/duplicate.
- **PROPOSAL (draft, not a label): plausibly an alias pair (same work, not yet canonically merged) -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `alias_merge`, `confirms`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 27

- **Manuscript:** (no shipped claim instance found for either work)
- **Work(s):** תשובות משה בן חנוך (w000473, msource) / תשובות משה בן חנוך (w000474, msource)
- **Why it is hard:** Two DIFFERENT work_ids share both the same author and an identical normalized title (a two-member cluster, not a large generic-collection cluster) -- a likely un-merged cross-corpus alias/duplicate.
- **PROPOSAL (draft, not a label): plausibly an alias pair (same work, not yet canonically merged) -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `alias_merge`, `confirms`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 28

- **Manuscript:** (no shipped claim instance found for either work)
- **Work(s):** איגרת קראית אל אחד מנכבדי הקראים בפוסטאט (w000518, msource) / איגרת קראית אל אחד מנכבדי הקראים בפוסטאט (w000519, msource)
- **Why it is hard:** Two DIFFERENT work_ids share both the same author and an identical normalized title (a two-member cluster, not a large generic-collection cluster) -- a likely un-merged cross-corpus alias/duplicate.
- **PROPOSAL (draft, not a label): plausibly an alias pair (same work, not yet canonically merged) -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `alias_merge`, `confirms`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 29

- **Manuscript:** Cambridge University Library Ms. T-S Loan Collection 99 (sys_id `990051125830205171`)
- **Work(s):** תשובות משולם בר׳ קלונימוס מלוקא (w000920, msource) / תשובות משולם בר׳ קלונימוס מלוקא (w000921, msource)
- **Catalogue's own identification text:** שאלות ותשובות- גאונים. ; Lists, Responsa lists ; רשימות, רשימות שו"ת
- **Why it is hard:** Two DIFFERENT work_ids share both the same author and an identical normalized title (a two-member cluster, not a large generic-collection cluster) -- a likely un-merged cross-corpus alias/duplicate.
- **PROPOSAL (draft, not a label): plausibly an alias pair (same work, not yet canonically merged) -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `alias_merge`, `confirms`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 30

- **Manuscript:** MS heb. d.64/92 (sys_id `990053629300205171`)
- **Work(s):** נסים גאון על עירובין (w000469, msource) / נסים גאון על עירובין (w000471, msource)
- **Catalogue's own identification text:** חידושי הר"ן לתלמוד;פירושי תלמוד בבלי. ; נסים בן ראובן גירונדי, חידושי הר"ן לתלמוד: עירובין ב ע"ב
- **Why it is hard:** Two DIFFERENT work_ids share both the same author and an identical normalized title (a two-member cluster, not a large generic-collection cluster) -- a likely un-merged cross-corpus alias/duplicate.
- **PROPOSAL (draft, not a label): plausibly an alias pair (same work, not yet canonically merged) -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `alias_merge`, `confirms`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 31

- **Manuscript:** (no shipped claim instance found for either work)
- **Work(s):** תשובות סעדיה גאון (w001060, msource) / תשובות סעדיה גאון (w001061, msource)
- **Why it is hard:** Two DIFFERENT work_ids share both the same author and an identical normalized title (a two-member cluster, not a large generic-collection cluster) -- a likely un-merged cross-corpus alias/duplicate.
- **PROPOSAL (draft, not a label): plausibly an alias pair (same work, not yet canonically merged) -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `alias_merge`, `confirms`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 32

- **Manuscript:** MS heb. d.46/143 (sys_id `990053410080205171`)
- **Work(s):** תשובה בעניין נוסח ברכות קריאת שמע (w000598, msource) / תשובה בעניין נוסח ברכות קריאת שמע (w000635, msource)
- **Catalogue's own identification text:** שאלות ותשובות. ; דף 144 א-ב כולל שלש תשובות בערבית האחת בעניין גביית חוב מיתומים קטנים. התשובה השנייה עוסקת בעניין חוב מיתומים כשכתובת האלמנה אבדה. התשובה השלישית עוסקת בעניין ספר קודש שנקנה מגויים, לאחר שהגיע לידם כשלל מקהיר, והיה כתוב עליו 'קדש', האם מותר לקונה למוכרו או למשכנו מחמת שמצבו הכספי דחוק.
- **Why it is hard:** Two DIFFERENT work_ids share both the same author and an identical normalized title (a two-member cluster, not a large generic-collection cluster) -- a likely un-merged cross-corpus alias/duplicate.
- **PROPOSAL (draft, not a label): plausibly an alias pair (same work, not yet canonically merged) -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `alias_merge`, `confirms`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

## Class 1 -- near-miss titles (20 candidates)

**Plausible shades for this class:** `confirms`, `diverges`, `fills_gap` (any other shade from the vocabulary table above is still a valid answer if the case warrants it; `unsure` / `skip` are always available).

### Case 33

- **Manuscript:** Cambridge University Library Ms. T-S Ar. 46.221 (sys_id `990051317430205171`)
- **Work(s):** המספיק לעובדי השם (כרך ב חלק ב) (w000007) / המספיק לעובדי השם (כרך ט חלק ב) (w000036)
- **Catalogue's own identification text:** דרשות;כפאיה אלעאבידין;פרשנות מקרא. ; Abraham Maimonidess Kitab kifayat al-'abidi n. On prostration (סג'וד )
- **Why it is hard:** Same author; normalized titles are 96.8% similar (SequenceMatcher) but NOT identical -- genuinely different works (e.g. different books/chapters/parts) whose titles a string comparison could easily conflate in EITHER direction.
- **Owner verdict:** _(pending Task 3 -- `confirms`, `diverges`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 34

- **Manuscript:** Ms. EVR ARAB I 1717 (sys_id `990000862350205171`)
- **Work(s):** המספיק לעובדי השם (כרך ט חלק ב) (w000036) / המספיק לעובדי השם (כרך ט חלק א) (w000038)
- **Catalogue's own identification text:** כפאיה אלעאבדין.
- **Why it is hard:** Same author; normalized titles are 96.8% similar (SequenceMatcher) but NOT identical -- genuinely different works (e.g. different books/chapters/parts) whose titles a string comparison could easily conflate in EITHER direction.
- **Owner verdict:** _(pending Task 3 -- `confirms`, `diverges`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 35

- **Manuscript:** Ms. EVR II A 229 (sys_id `990000856980205171`)
- **Work(s):** רד"ק על הושע (w001248) / רד"ק על יהושע (w001258)
- **Catalogue's own identification text:** פרוש נביאים לרד"ק (הושע, עמוס, מיכה, קטעים).
- **Why it is hard:** Same author; normalized titles are 95.7% similar (SequenceMatcher) but NOT identical -- genuinely different works (e.g. different books/chapters/parts) whose titles a string comparison could easily conflate in EITHER direction.
- **Owner verdict:** _(pending Task 3 -- `confirms`, `diverges`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 36

- **Manuscript:** Ms. EVR II A 228 (sys_id `990000857150205171`)
- **Work(s):** רש"י על הושע (w001284) / רש"י על יהושע (w001296)
- **Catalogue's own identification text:** פרוש נביאים לרש"י (יחזקאל, תרי עשר, קטעים).
- **Why it is hard:** Same author; normalized titles are 95.7% similar (SequenceMatcher) but NOT identical -- genuinely different works (e.g. different books/chapters/parts) whose titles a string comparison could easily conflate in EITHER direction.
- **Owner verdict:** _(pending Task 3 -- `confirms`, `diverges`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 37

- **Manuscript:** Cambridge University Library Ms. T-S F 7.99 (sys_id `990051173750205171`)
- **Work(s):** משנה תורה, ספר משפטים (w000187) / משנה תורה, ספר שופטים (w000188)
- **Catalogue's own identification text:** משנה תורה;משנה תורה ופירושיו. ; Moses b. Maimon, Rambam, Mishneh Torah: Mishpatim ; משה בן מימון, משנה תורה: משפטים
- **Why it is hard:** Same author; normalized titles are 95.2% similar (SequenceMatcher) but NOT identical -- genuinely different works (e.g. different books/chapters/parts) whose titles a string comparison could easily conflate in EITHER direction.
- **Owner verdict:** _(pending Task 3 -- `confirms`, `diverges`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 38

- **Manuscript:** Ms. EVR ARAB I 4543 (sys_id `990001562410205171`)
- **Work(s):** ראב"ש, שמואל א פירוש (w000046) / ראב"ש, שמואל ב פירוש (w000047)
- **Catalogue's own identification text:** כתאב אלביאן תאליף (יהושע ושופטים).
- **Why it is hard:** Same author; normalized titles are 94.7% similar (SequenceMatcher) but NOT identical -- genuinely different works (e.g. different books/chapters/parts) whose titles a string comparison could easily conflate in EITHER direction.
- **Owner verdict:** _(pending Task 3 -- `confirms`, `diverges`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 39

- **Manuscript:** Ms. EVR II A 281/01 (sys_id `990000851650205171`)
- **Work(s):** ספר הערוך, אות העי"ן (w001165) / ספר הערוך, אות השי"ן (w001179)
- **Catalogue's own identification text:** ערוך (קטעים).
- **Why it is hard:** Same author; normalized titles are 94.7% similar (SequenceMatcher) but NOT identical -- genuinely different works (e.g. different books/chapters/parts) whose titles a string comparison could easily conflate in EITHER direction.
- **Owner verdict:** _(pending Task 3 -- `confirms`, `diverges`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 40

- **Manuscript:** Ms. EVR II A 281/01 (sys_id `990000851650205171`)
- **Work(s):** ספר הערוך, אות העי"ן (w001165) / ספר הערוך, אות הזי"ן (w001184)
- **Catalogue's own identification text:** ערוך (קטעים).
- **Why it is hard:** Same author; normalized titles are 94.7% similar (SequenceMatcher) but NOT identical -- genuinely different works (e.g. different books/chapters/parts) whose titles a string comparison could easily conflate in EITHER direction.
- **Owner verdict:** _(pending Task 3 -- `confirms`, `diverges`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 41

- **Manuscript:** MS heb. c.27/88 (sys_id `990053620660205171`)
- **Work(s):** ספר הערוך, אות הבי"ת (w001166) / ספר הערוך, אות החי"ת (w001167)
- **Catalogue's own identification text:** ספר הערוך;ספרות הלכתית ופרשנות תלמודית. ; ערוך, בלע-במות;בעמדם בצית; ר' נתן בן יחיאל מרומא
- **Why it is hard:** Same author; normalized titles are 94.7% similar (SequenceMatcher) but NOT identical -- genuinely different works (e.g. different books/chapters/parts) whose titles a string comparison could easily conflate in EITHER direction.
- **Owner verdict:** _(pending Task 3 -- `confirms`, `diverges`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 42

- **Manuscript:** MS heb. c.27/88 (sys_id `990053620660205171`)
- **Work(s):** ספר הערוך, אות הבי"ת (w001166) / ספר הערוך, אות הטי"ת (w001181)
- **Catalogue's own identification text:** ספר הערוך;ספרות הלכתית ופרשנות תלמודית. ; ערוך, בלע-במות;בעמדם בצית; ר' נתן בן יחיאל מרומא
- **Why it is hard:** Same author; normalized titles are 94.7% similar (SequenceMatcher) but NOT identical -- genuinely different works (e.g. different books/chapters/parts) whose titles a string comparison could easily conflate in EITHER direction.
- **Owner verdict:** _(pending Task 3 -- `confirms`, `diverges`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 43

- **Manuscript:** Gaster, Moses Collection (sys_id `990001835240205171`)
- **Work(s):** ספר הערוך, אות החי"ת (w001167) / ספר הערוך, אות הטי"ת (w001181)
- **Catalogue's own identification text:** מעשיות.
- **Why it is hard:** Same author; normalized titles are 94.7% similar (SequenceMatcher) but NOT identical -- genuinely different works (e.g. different books/chapters/parts) whose titles a string comparison could easily conflate in EITHER direction.
- **Owner verdict:** _(pending Task 3 -- `confirms`, `diverges`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 44

- **Manuscript:** Cambridge University Library Ms. T-S Misc. 16.117 (sys_id `990051081140205171`)
- **Work(s):** ספר הערוך, אות השי"ן (w001179) / ספר הערוך, אות הזי"ן (w001184)
- **Catalogue's own identification text:** מילונים. ; ספר 'הערוך' לר' נתן בן יחיאל. דפוס.
- **Why it is hard:** Same author; normalized titles are 94.7% similar (SequenceMatcher) but NOT identical -- genuinely different works (e.g. different books/chapters/parts) whose titles a string comparison could easily conflate in EITHER direction.
- **Owner verdict:** _(pending Task 3 -- `confirms`, `diverges`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 45

- **Manuscript:** Ms. EVR II A 329/03a (sys_id `990000851600205171`)
- **Work(s):** ספר הערוך, אות התי"ו (w001180) / ספר הערוך, אות היו"ד (w001183)
- **Catalogue's own identification text:** ערוך (אותיות תאל-תבליא).
- **Why it is hard:** Same author; normalized titles are 94.7% similar (SequenceMatcher) but NOT identical -- genuinely different works (e.g. different books/chapters/parts) whose titles a string comparison could easily conflate in EITHER direction.
- **Owner verdict:** _(pending Task 3 -- `confirms`, `diverges`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 46

- **Manuscript:** Ms. EVR II A 6 (sys_id `990000589160205171`)
- **Work(s):** רד"ק על דברי הימים א׳ (w001249) / רד"ק על דברי הימים ב' (w001252)
- **Catalogue's own identification text:** קובץ.
- **Why it is hard:** Same author; normalized titles are 94.7% similar (SequenceMatcher) but NOT identical -- genuinely different works (e.g. different books/chapters/parts) whose titles a string comparison could easily conflate in EITHER direction.
- **Owner verdict:** _(pending Task 3 -- `confirms`, `diverges`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 47

- **Manuscript:** Cambridge University Library Ms. T-S AS 194.373 (sys_id `990052328210205171`)
- **Work(s):** רש"י על דברי הימים א (w001285) / רש"י על דברי הימים ב (w001288)
- **Why it is hard:** Same author; normalized titles are 94.7% similar (SequenceMatcher) but NOT identical -- genuinely different works (e.g. different books/chapters/parts) whose titles a string comparison could easily conflate in EITHER direction.
- **Owner verdict:** _(pending Task 3 -- `confirms`, `diverges`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 48

- **Manuscript:** Ms. EVR II A 279 (sys_id `990000857390205171`)
- **Work(s):** ספר הערוך, אות הה"א (w001170) / ספר הערוך, אות הפ"א (w001176)
- **Catalogue's own identification text:** ערוך (אותיות פטפוט-תאטר).
- **Why it is hard:** Same author; normalized titles are 94.4% similar (SequenceMatcher) but NOT identical -- genuinely different works (e.g. different books/chapters/parts) whose titles a string comparison could easily conflate in EITHER direction.
- **Owner verdict:** _(pending Task 3 -- `confirms`, `diverges`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 49

- **Manuscript:** Ms. EVR II A 68 (sys_id `990000853170205171`)
- **Work(s):** מורה נבוכים, חלק א (w001193) / מורה נבוכים, חלק ב (w001194)
- **Catalogue's own identification text:** מורה נבוכים.
- **Why it is hard:** Same author; normalized titles are 94.4% similar (SequenceMatcher) but NOT identical -- genuinely different works (e.g. different books/chapters/parts) whose titles a string comparison could easily conflate in EITHER direction.
- **Owner verdict:** _(pending Task 3 -- `confirms`, `diverges`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 50

- **Manuscript:** Ms. EVR II A 68 (sys_id `990000853170205171`)
- **Work(s):** מורה נבוכים, חלק א (w001193) / מורה נבוכים, חלק ג (w001195)
- **Catalogue's own identification text:** מורה נבוכים.
- **Why it is hard:** Same author; normalized titles are 94.4% similar (SequenceMatcher) but NOT identical -- genuinely different works (e.g. different books/chapters/parts) whose titles a string comparison could easily conflate in EITHER direction.
- **Owner verdict:** _(pending Task 3 -- `confirms`, `diverges`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 51

- **Manuscript:** Ms. EVR II A 68 (sys_id `990000853170205171`)
- **Work(s):** מורה נבוכים, חלק ב (w001194) / מורה נבוכים, חלק ג (w001195)
- **Catalogue's own identification text:** מורה נבוכים.
- **Why it is hard:** Same author; normalized titles are 94.4% similar (SequenceMatcher) but NOT identical -- genuinely different works (e.g. different books/chapters/parts) whose titles a string comparison could easily conflate in EITHER direction.
- **Owner verdict:** _(pending Task 3 -- `confirms`, `diverges`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 52

- **Manuscript:** Cambridge University Library Ms. T-S G 2.33 (sys_id `990051181430205171`)
- **Work(s):** רבנו חננאל על מסכת שבת (w000452) / רבנו חננאל על מסכת שבועות (w000467)
- **Catalogue's own identification text:** פירוש רבנו חננאל לתלמוד;שאלות ותשובות- גאונים. ; Responsa- Gaonim ; חננאל בן חושיאל, פירוש רבנו חננאל לתלמוד: שבת קמא ע"ב – קמו ע"ב
- **Why it is hard:** Same author; normalized titles are 93.6% similar (SequenceMatcher) but NOT identical -- genuinely different works (e.g. different books/chapters/parts) whose titles a string comparison could easily conflate in EITHER direction.
- **Owner verdict:** _(pending Task 3 -- `confirms`, `diverges`, `fills_gap`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

## Class 4 -- terse or missing catalogue identification text (owner-authorized extension) (15 candidates)

**Plausible shades for this class:** `fills_gap`, `confirms` (any other shade from the vocabulary table above is still a valid answer if the case warrants it; `unsure` / `skip` are always available).

### Case 53

- **Manuscript:** Ms. B 3672 (sys_id `990002098550205171`)
- **Work(s):** כתר מלכות (רשב"ג/אבן גבירול) (w001129)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Owner verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 54

- **Manuscript:** Cambridge University Library Ms. T-S Misc. 34.14 (sys_id `990051124400205171`)
- **Work(s):** פירוש אבות לדוד הנגיד (w001135)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Owner verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 55

- **Manuscript:** Cambridge University Library Ms. T-S Misc. 34.20 (sys_id `990051124460205171`)
- **Work(s):** רס"ג, שמות תרגום (תפסיר תורה) (w000033)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Owner verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 56

- **Manuscript:** Cambridge University Library Ms. T-S Misc. 34.23 (sys_id `990051124490205171`)
- **Work(s):** משנה תורה, ספר אהבה (w000176)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Owner verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 57

- **Manuscript:** Cambridge University Library Ms. T-S Misc. 34.29 (sys_id `990051124550205171`)
- **Work(s):** תנ"ך, בראשית (w000086)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Owner verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 58

- **Manuscript:** Cambridge University Library Ms. T-S K 27.5 (sys_id `990051220230205171`)
- **Work(s):** נתנאל בן פיומי, גן השכלים (w000039)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Owner verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 59

- **Manuscript:** Cambridge University Library Ms. T-S K 27.26 (sys_id `990051220440205171`)
- **Work(s):** הלכות גדולות (w001196)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Owner verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 60

- **Manuscript:** Cambridge University Library Ms. T-S K 27.28 (sys_id `990051220460205171`)
- **Work(s):** מדרש אגור (w000836)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Owner verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 61

- **Manuscript:** Cambridge University Library Ms. T-S K 27.34 (sys_id `990051220530205171`)
- **Work(s):** תשובות האיי גאון (w000654)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Owner verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 62

- **Manuscript:** Cambridge University Library Ms. T-S K 27.35 (sys_id `990051220540205171`)
- **Work(s):** מונחי המסורה וכלליה (w000906)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Owner verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 63

- **Manuscript:** Cambridge University Library Ms. T-S K 27.39 (sys_id `990051220580205171`)
- **Work(s):** סדר אליהו זוטא א-טו (w000162)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Owner verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 64

- **Manuscript:** Cambridge University Library Ms. T-S K 27.41 (sys_id `990051220600205171`)
- **Work(s):** תנ"ך, ויקרא (w000088)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Owner verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 65

- **Manuscript:** Cambridge University Library Ms. T-S K 27.42 (sys_id `990051220610205171`)
- **Work(s):** דברי הימים של משה רבנו (w000944)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Owner verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 66

- **Manuscript:** Cambridge University Library Ms. T-S K 27.46 (sys_id `990051220650205171`)
- **Work(s):** פירוש ליחזקאל ותרי עשר (w000918)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Owner verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 67

- **Manuscript:** Cambridge University Library Ms. T-S K 27.48 (sys_id `990051220670205171`)
- **Work(s):** מדרש חסרות ויתרות (w000859)
- **Catalogue's own identification text:** _(none on file -- explicit marker of absence, not an omission)_
- **Why it is hard:** This manuscript's own catalogue identification field is EMPTY -- there is no catalogue text at all for a title comparison to work with, only the identified work's title itself.
- **Owner verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

## Class 5 -- generic collection works (owner-authorized extension) (15 candidates)

**Plausible shades for this class:** `fills_gap`, `confirms`, `extends` (any other shade from the vocabulary table above is still a valid answer if the case warrants it; `unsure` / `skip` are always available).

### Case 68

- **Manuscript:** Cambridge University Library Ms. T-S G 2.23 (sys_id `990051181330205171`)
- **Work(s):** תשובות האיי גאון (w000695) -- one of 43 works sharing author 'האיי גאון' and title stem 'תשובות האיי גאון' (siblings incl. w000650, w000651, w000652, w000653, w000654...)
- **Catalogue's own identification text:** שאלות ותשובות- גאונים. ; Responsa- Gaonim
- **Why it is hard:** This work belongs to a 43-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Owner verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 69

- **Manuscript:** Cambridge University Library Ms. T-S G 2.29 (sys_id `990051181390205171`)
- **Work(s):** תשובות שרירא גאון והאיי גאון (w000629) -- one of 33 works sharing author 'שרירא גאון והאיי גאון' and title stem 'תשובות שרירא גאון והאיי גאון' (siblings incl. w000597, w000599, w000600, w000601, w000602...)
- **Catalogue's own identification text:** שאלות ותשובות- גאונים. ; Responsa- Gaonim
- **Why it is hard:** This work belongs to a 33-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Owner verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 70

- **Manuscript:** Cambridge University Library Ms. L-G Talm. II 1 (sys_id `990001843080205171`)
- **Work(s):** תשובות צמח גאון (w000573) -- one of 12 works sharing author 'צמח גאון' and title stem 'תשובות צמח גאון' (siblings incl. w000562, w000563, w000564, w000565, w000566...)
- **Catalogue's own identification text:** תשובות הגאונים.
- **Why it is hard:** This work belongs to a 12-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Owner verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 71

- **Manuscript:** Cambridge University Library Ms. T-S NS 309.106 (sys_id `990051703120205171`)
- **Work(s):** תשובות שרירא גאון (w000581) -- one of 9 works sharing author 'שרירא גאון' and title stem 'תשובות שרירא גאון' (siblings incl. w000582, w000584, w000589, w000590, w000591...)
- **Catalogue's own identification text:** ספרות הלכתית ופרשנות תלמודית;ספרות חז"ל;שאלות ותשובות- גאונים. ; Halakhic Literature and Talmudic Commentaries ; Responsa; geonic, concerning oaths (whether a married woman should be made to swear in court, and whether some members of a partnership can waive an oath due them without the remaining partners consent).
- **Why it is hard:** This work belongs to a 9-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Owner verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 72

- **Manuscript:** Catalogue Brumer Rab. 2360, fol. 1 (sys_id `990053344530205171`)
- **Work(s):** תשובות שמואל גאון (w000428) -- one of 6 works sharing author 'שמואל גאון' and title stem 'תשובות שמואל גאון' (siblings incl. w000429, w000430, w000432, w000433, w000435)
- **Catalogue's own identification text:** שאלות ותשובות- גאונים. ; תשובות בעניין המגרש בגט בטל, אונאה וחזרה במכירת קרקע
- **Why it is hard:** This work belongs to a 6-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Owner verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 73

- **Manuscript:** Ms. Evr. Antonin B 266 (sys_id `990000907150205171`)
- **Work(s):** תשובות יצחק (w000559) -- one of 5 works sharing author 'נחשון גאון' and title stem 'תשובות יצחק' (siblings incl. w000554, w000556, w000557, w000561)
- **Catalogue's own identification text:** שאלות ותשובות הגאונים.
- **Why it is hard:** This work belongs to a 5-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Owner verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 74

- **Manuscript:** Cambridge University Library Ms. T-S G 1.78 (sys_id `990051180910205171`)
- **Work(s):** תשובות נטרונאי גאון (w000534) -- one of 5 works sharing author 'נטרונאי גאון' and title stem 'תשובות נטרונאי גאון' (siblings incl. w000535, w000536, w000537, w000538)
- **Catalogue's own identification text:** שאלות ותשובות. ; Responsa and Halakhic Decisions
- **Why it is hard:** This work belongs to a 5-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Owner verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 75

- **Manuscript:** Cambridge University Library Ms. T-S 20.183 (sys_id `990051346380205171`)
- **Work(s):** תשובות עמרם גאון (w000553) -- one of 5 works sharing author 'עמרם גאון' and title stem 'תשובות עמרם גאון' (siblings incl. w000547, w000548, w000551, w000552)
- **Why it is hard:** This work belongs to a 5-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Owner verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 76

- **Manuscript:** Cambridge University Library Ms. T-S G 2.14 (sys_id `990051181240205171`)
- **Work(s):** תשובות פלטוי גאון (w000542) -- one of 4 works sharing author 'פלטוי גאון' and title stem 'תשובות פלטוי גאון' (siblings incl. w000539, w000540, w000541)
- **Catalogue's own identification text:** שאלות ותשובות- גאונים. ; Responsa- Gaonim
- **Why it is hard:** This work belongs to a 4-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Owner verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 77

- **Manuscript:** Ms. Evr. Antonin B 308 (sys_id `990000555750205171`)
- **Work(s):** תשובות שר שלום גאון (w000499) -- one of 4 works sharing author 'שר שלום גאון' and title stem 'תשובות שר שלום גאון' (siblings incl. w000497, w000498, w000500)
- **Catalogue's own identification text:** שאלות ותשובות הגאונים.
- **Why it is hard:** This work belongs to a 4-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Owner verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 78

- **Manuscript:** Cambridge University Library Ms. T-S 8 G 7.2 (sys_id `990051222370205171`)
- **Work(s):** תשובות יוסף בן אביתור (w000730) -- one of 3 works sharing author 'יוסף בן אביתור' and title stem 'תשובות יוסף בן אביתור' (siblings incl. w000729, w000731)
- **Catalogue's own identification text:** שאלות ותשובות- גאונים.
- **Why it is hard:** This work belongs to a 3-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Owner verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 79

- **Manuscript:** Cambridge University Library Ms. T-S G 1.86 (sys_id `990051180990205171`)
- **Work(s):** תשובות משרשיה (w000528) -- one of 3 works sharing author 'משה (משרשיה) הכהן גאון' and title stem 'תשובות משרשיה' (siblings incl. w000527, w000529)
- **Catalogue's own identification text:** שאלות ותשובות;שאלות ותשובות- גאונים. ; Responsa and Halakhic Decisions ; ד' תשובות בענייני כתובה: א. כנראה בעניין אשה שהוציאה שטר כתובה ונמצא מזוייף. ב. תשובת רב צמח גאון בעניין אשה שאבדה כתובתה. ג. תשובת רב צמח גאון בעניין פירוש דברי רב רב יוסף "בביתי ולא בביקתי" (כתובות נד ע"א). ד. תשובת מר רבנא משה גאון (בן יעקב, ראש ישיבת מתא מחסיא) בעניין בחור שנשא אשה והתברר שהיא נכפית.
- **Why it is hard:** This work belongs to a 3-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Owner verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 80

- **Manuscript:** Ms. EVR II A 721 (sys_id `990001442050205171`)
- **Work(s):** תשובות קלונימוס הזקן מלוקא בר׳ משה (w000416) -- one of 3 works sharing author 'קלונימוס הזקן מלוקא בר׳ משה' and title stem 'תשובות קלונימוס הזקן מלוקא בר משה' (siblings incl. w000760, w000761)
- **Catalogue's own identification text:** תשובות.
- **Why it is hard:** This work belongs to a 3-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Owner verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 81

- **Manuscript:** Cambridge University Library Ms. T-S 16.99 (sys_id `990051341890205171`)
- **Work(s):** תשובות האיי גאון (w000694) -- one of 43 works sharing author 'האיי גאון' and title stem 'תשובות האיי גאון' (siblings incl. w000650, w000651, w000652, w000653, w000654...)
- **Catalogue's own identification text:** שאלות ותשובות;שאלות ותשובות- גאונים. ; Responsa- Gaonim ; שאלות ותשובות- גאונים
- **Why it is hard:** This work belongs to a 43-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Owner verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 82

- **Manuscript:** Ms. EVR II A 32 (sys_id `990001428310205171`)
- **Work(s):** תשובות שרירא גאון והאיי גאון (w000606) -- one of 33 works sharing author 'שרירא גאון והאיי גאון' and title stem 'תשובות שרירא גאון והאיי גאון' (siblings incl. w000597, w000599, w000600, w000601, w000602...)
- **Catalogue's own identification text:** שאלות ותשובות הגאונים.
- **Why it is hard:** This work belongs to a 33-member same-author/same-title-stem collection (a generic responsa/piyyut/collection title recurring across many distinct catalogued items) with >=2 distinct canonical_work_ids in the cluster -- whether THIS witness is 'already recorded' is genuinely ill-defined at the collection level, not merely hard for a string comparison to settle.
- **PROPOSAL (draft, not a label): a generic collection member -- confirm whether this specific witness/passage is already recorded, or correct.**
- **Owner verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `extends`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

## Class 6 -- catalogue divergence (owner decision E) (15 candidates)

**Plausible shades for this class:** `diverges`, `aid_more_specific`, `refines_granularity`, `confirms` (any other shade from the vocabulary table above is still a valid answer if the case warrants it; `unsure` / `skip` are always available).

### Case 83

- **Manuscript:** Ms. Evr. Antonin B 1104 (sys_id `990000555880205171`)
- **Work(s):** CLAIMED (this identification): תשובות האיי גאון (w000650) / CATALOGUE NAMES (found in the identification text): תשובות (w000543)
- **Catalogue's own identification text:** שאלות ותשובות מאת האי בן שרירא גאון (קטע).
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('תשובות') than the one this claim identifies ('תשובות האיי גאון'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence, the shade decision E calls `diverges`.
- **PROPOSAL (draft, not a label): plausibly `diverges` -- the catalogue and this claim name different works -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `diverges`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 84

- **Manuscript:** Allony, Nehemia Ms. 304 (sys_id `990000413480205171`)
- **Work(s):** CLAIMED (this identification): משנה תורה, ספר זמנים (w000177) / CATALOGUE NAMES (found in the identification text): הגדה של פסח (w001159)
- **Catalogue's own identification text:** הגדה של פסח.
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('הגדה של פסח') than the one this claim identifies ('משנה תורה, ספר זמנים'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence, the shade decision E calls `diverges`.
- **PROPOSAL (draft, not a label): plausibly `diverges` -- the catalogue and this claim name different works -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `diverges`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 85

- **Manuscript:** Ms. Evr. Antonin B 915 (sys_id `990000555810205171`)
- **Work(s):** CLAIMED (this identification): הלכות פסוקות (w001084) / CATALOGUE NAMES (found in the identification text): הלכות גדולות (w001196)
- **Catalogue's own identification text:** הלכות גדולות (בבא קמא).
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('הלכות גדולות') than the one this claim identifies ('הלכות פסוקות'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence, the shade decision E calls `diverges`.
- **PROPOSAL (draft, not a label): plausibly `diverges` -- the catalogue and this claim name different works -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `diverges`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 86

- **Manuscript:** Cambridge University Library Ms. Add. 3162 (sys_id `990001398690205171`)
- **Work(s):** CLAIMED (this identification): משנה תורה, ספר אהבה (w000176) / CATALOGUE NAMES (found in the identification text): ברכת המזון (w001158)
- **Catalogue's own identification text:** ברכת המזון.
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('ברכת המזון') than the one this claim identifies ('משנה תורה, ספר אהבה'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence, the shade decision E calls `diverges`.
- **PROPOSAL (draft, not a label): plausibly `diverges` -- the catalogue and this claim name different works -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `diverges`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 87

- **Manuscript:** Cambridge University Library Ms. Add. 1246 (sys_id `990001394270205171`)
- **Work(s):** CLAIMED (this identification): ספר יוסיפון (ערבי) (w001152) / CATALOGUE NAMES (found in the identification text): יוסיפון (w000853)
- **Catalogue's own identification text:** יוסיפון בערבית.
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('יוסיפון') than the one this claim identifies ('ספר יוסיפון (ערבי)'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence, the shade decision E calls `diverges`.
- **PROPOSAL (draft, not a label): plausibly `diverges` -- the catalogue and this claim name different works -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `diverges`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 88

- **Manuscript:** Ms. Evr. Antonin B 961 (sys_id `990000555730205171`)
- **Work(s):** CLAIMED (this identification): תשובה בעניין סוכה (w000434) / CATALOGUE NAMES (found in the identification text): תשובות הגאונים (w000349)
- **Catalogue's own identification text:** שאלות ותשובות הגאונים.
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('תשובות הגאונים') than the one this claim identifies ('תשובה בעניין סוכה'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence, the shade decision E calls `diverges`.
- **PROPOSAL (draft, not a label): plausibly `diverges` -- the catalogue and this claim name different works -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `diverges`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 89

- **Manuscript:** Ms. EVR ARAB I 3085 (sys_id `990000801470205171`)
- **Work(s):** CLAIMED (this identification): רס"ג, ספר יצירה פירוש (w000021) / CATALOGUE NAMES (found in the identification text): ספר יצירה (w000522)
- **Catalogue's own identification text:** פרוש ספר יצירה בערבית.
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('ספר יצירה') than the one this claim identifies ('רס"ג, ספר יצירה פירוש'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence, the shade decision E calls `diverges`.
- **PROPOSAL (draft, not a label): plausibly `diverges` -- the catalogue and this claim name different works -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `diverges`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 90

- **Manuscript:** Catalogue Halper, Philadelphia 120 (sys_id `990001935160205171`)
- **Work(s):** CLAIMED (this identification): הלכות פסוקות, תרגומים ועיבודים עבריים, הלכות קידושין (w001037) / CATALOGUE NAMES (found in the identification text): הלכות פסוקות (w001084)
- **Catalogue's own identification text:** הלכות פסוקות (קדושין).
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('הלכות פסוקות') than the one this claim identifies ('הלכות פסוקות, תרגומים ועיבודים עבריים, הלכות קידושין'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence, the shade decision E calls `diverges`.
- **PROPOSAL (draft, not a label): plausibly `diverges` -- the catalogue and this claim name different works -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `diverges`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 91

- **Manuscript:** Ms. Evr. Antonin B 236 (sys_id `990000905560205171`)
- **Work(s):** CLAIMED (this identification): מכילתא דרבי שמעון בן יוחאי (w000321) / CATALOGUE NAMES (found in the identification text): מכילתא דרבי ישמעאל (w000766)
- **Catalogue's own identification text:** מכילתא דרבי ישמעאל (בא-יתרו).
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('מכילתא דרבי ישמעאל') than the one this claim identifies ('מכילתא דרבי שמעון בן יוחאי'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence, the shade decision E calls `diverges`.
- **PROPOSAL (draft, not a label): plausibly `diverges` -- the catalogue and this claim name different works -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `diverges`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 92

- **Manuscript:** Library of the Hungarian Academy of Sciences Ms. 57 (sys_id `990001004230205171`)
- **Work(s):** CLAIMED (this identification): ילקוט שמעוני על התורה (w001384) / CATALOGUE NAMES (found in the identification text): תנחומא (w000926)
- **Catalogue's own identification text:** מדרש תנחומא (קטעים).
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('תנחומא') than the one this claim identifies ('ילקוט שמעוני על התורה'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence, the shade decision E calls `diverges`.
- **PROPOSAL (draft, not a label): plausibly `diverges` -- the catalogue and this claim name different works -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `diverges`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 93

- **Manuscript:** Cambridge University Library Ms. T-S NS 169.52 (sys_id `990051091870205171`)
- **Work(s):** CLAIMED (this identification): נסים גאון, חמשה ספרים (w000071) / CATALOGUE NAMES (found in the identification text): מגילת סתרים (w000509)
- **Catalogue's own identification text:** הלכה; מגילת סתרים [נסים בן יעקב]; ספרות הלכתית ופרשנות תלמודית; ספרות חז"ל. ; Halakhic Literature and Talmudic Commentaries ; Rabbinica; exposition of PT hagigah 78a and Tosefta Sheqalim 3:23-24 (the latter section numbered 140).
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('מגילת סתרים') than the one this claim identifies ('נסים גאון, חמשה ספרים'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence, the shade decision E calls `diverges`.
- **PROPOSAL (draft, not a label): plausibly `diverges` -- the catalogue and this claim name different works -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `diverges`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 94

- **Manuscript:** Cambridge University Library Ms. T-S F 7.45 (sys_id `990051173260205171`)
- **Work(s):** CLAIMED (this identification): משנה תורה, הקדמה ומניין המצוות (w000174) / CATALOGUE NAMES (found in the identification text): הלכות ציצית (w001052)
- **Catalogue's own identification text:** משנה תורה;משנה תורה ופירושיו. ; Mishneh Torah and its Commentaries ; מנין מצות על סדר הרמב"ם, מסוף ספר אהבה (הלכות ציצית) עד סוף ספר זמנים
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('הלכות ציצית') than the one this claim identifies ('משנה תורה, הקדמה ומניין המצוות'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence, the shade decision E calls `diverges`.
- **PROPOSAL (draft, not a label): plausibly `diverges` -- the catalogue and this claim name different works -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `diverges`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 95

- **Manuscript:** Ms. EVR II A 33 (sys_id `990000621960205171`)
- **Work(s):** CLAIMED (this identification): תנ"ך, בראשית (w000086) / CATALOGUE NAMES (found in the identification text): שאילתות (w000732)
- **Catalogue's own identification text:** שאילתות.
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('שאילתות') than the one this claim identifies ('תנ"ך, בראשית'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence, the shade decision E calls `diverges`.
- **PROPOSAL (draft, not a label): plausibly `diverges` -- the catalogue and this claim name different works -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `diverges`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 96

- **Manuscript:** Cambridge University Library Ms. T-S C 1.23 (sys_id `990051150540205171`)
- **Work(s):** CLAIMED (this identification): בראשית רבה צה-צו, תוספת (w000900) / CATALOGUE NAMES (found in the identification text): בראשית רבה (w000156)
- **Catalogue's own identification text:** בראשית רבה;מדרש. ; Midrash ; בראשית רבה
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('בראשית רבה') than the one this claim identifies ('בראשית רבה צה-צו, תוספת'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence, the shade decision E calls `diverges`.
- **PROPOSAL (draft, not a label): plausibly `diverges` -- the catalogue and this claim name different works -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `diverges`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

### Case 97

- **Manuscript:** Cambridge University Library Ms. T-S Misc. 15.67 (sys_id `990051080280205171`)
- **Work(s):** CLAIMED (this identification): ויקרא רבה (w000169) / CATALOGUE NAMES (found in the identification text): חובות הלבבות (תרגום אבן תיבון) (w000195)
- **Catalogue's own identification text:** חובות הלבבות (תרגום אבן תיבון). ; ספר חובות הלבבות. מעלת התשובה ומעלת הצדקה. דפוס.
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('חובות הלבבות (תרגום אבן תיבון)') than the one this claim identifies ('ויקרא רבה'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence, the shade decision E calls `diverges`.
- **PROPOSAL (draft, not a label): plausibly `diverges` -- the catalogue and this claim name different works -- confirm or correct.**
- **Owner verdict:** _(pending Task 3 -- `diverges`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

