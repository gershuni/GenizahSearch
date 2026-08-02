# Phase 136 Plan 03 -- Novelty Hard-Case Candidates

**101 candidates total: 8 IDENTITY spot-check cases (Classes 1-3, UNCHANGED) + 93 novelty-evaluation cases** (Class 6 catalogue divergence, RETAINED unchanged, plus owner ruling J's new three-arm, SOURCE-STRATIFIED sample). **Redesigned per owner ruling J** (`136-GATE1-DECISIONS.md` § J, 2026-08-02) after a read-only prior-art pass (`136-NOVELTY-PRIOR-ART.md` §§ 6-7) found that the FORMER Classes 4/5/7 all read EXACTLY ONE field (libraries.csv column 7) and therefore had ZERO representation of the bib/PGP/FGP failure modes Codex measured as most damaging in the prior heuristic funnel (`gen2_novelty_gate.py`): 3,688 `published_full` false-knowns, 2,014 PGP false-knowns (942 sole-source), FGP's 1,177-known/9,373-fail split. A gate could score perfectly on the pre-J pool and still reproduce every one of those defects in production.

**What changed, explicitly (per this continuation's own accounting instruction -- nothing is silently dropped):**

| Former pool item | Disposition | Why |
|---|---|---|
| Classes 1-3 (identity spot-check, 8 cases) | **KEPT, unchanged** | Tests a different assumption (A↔B same-work identity), already correctly sized |
| Class 6 (catalogue divergence, 30 cases incl. the owner's F/G annotations on 12 of the original 15) | **KEPT, unchanged** | Owner rulings F/G are substantive, dated characterizations of SPECIFIC real manuscripts (worked cases 83-97) -- dropping or reselecting this class would silently discard that owner-authorized work before its Task-3 confirmation ever ran. It also tests a different, already-owner-engaged axis (divergence SHAPE/correctness, deliberately left an uncorrected heuristic per ruling G) orthogonal to source coverage |
| Class 4 (terse/missing catalogue text) | **FOLDED IN** as Arm 1's `terse_catalogue` stratum | Ruling J's own instruction: fold in as a stratum, not a separate exercise; now checked against the REAL bib/PGP/FGP signal, not catalogue text alone |
| Class 7 (liturgical-container predictability, owner rulings H/I) | **FOLDED IN** as Arm 1's `container_predicts` stratum | Same instruction: "a container-only machzor title has text, fails name-match, and therefore lands in the residual" |
| Class 5 (generic collection works) | **DROPPED** | No owner ruling exists for any specific Class 5 case (only generic PROPOSALS, unlike Class 6). Its phenomenon -- whether a single witness of a same-author/same-title-stem collection is "already recorded" -- is a WORK-IDENTITY ambiguity, not a source-coverage gap; it does not correspond to any of the three arms and forcing it into one would blur what that arm measures. The underlying question remains valid and is flagged for a future, separately-scoped pass -- not silently discarded, just not carried by THIS redesign |

Every case in every group is still selected entirely by deterministic string/metadata/source-presence comparison over the works, manuscripts and finding-aid sidecars already on this machine (fist_data/fjms_enrichment.db, pgp_data/pgp.db, fgp_data/fgp_transcriptions.db, libraries.csv) -- **zero model calls, measured cost $0.00**. Any attached draft verdict is explicitly marked `PROPOSAL` and is a reading aid only, never a label. This worksheet is also emitted as `136-NOVELTY-HARDCASES.xlsx` (same phase directory, FIVE sheets: "Identity Spot-Check", "Novelty Shades", "Heuristic-Demoted", "No-Source-Text", "Vocabulary & Instructions") for owners who find Hebrew RTL easier to work with in a spreadsheet; both files render the SAME cases in the SAME order, from the same pre-numbered case list, so the two agree case-for-case.

## Sizing -- what each arm can and cannot answer, and why this size

**Total: 101** (8 identity + 93 novelty-evaluation, of which owner ruling J's own sizing instruction covers the 63 non-Class-6 cases -- kept under the ~100 novelty-case guidance with Class 6 counted separately as pre-existing, unchanged owner-engaged work).

- **Class 6 (catalogue divergence, 30 cases, unchanged):** answers "does the owner confirm the shade + correctness proposals already characterized on 12 of the original 15 real cases, and how does the selector's own measured over-fire rate (~50%) hold up across the expanded pool." It does NOT test source coverage -- a case here is selected purely on catalogue-text containment, same as before.
- **Arm 1 -- RESIDUAL, 30 cases across 7 strata (a FIXED per-stratum cap, not a proportional sample, where the population supports it):** answers "of the rows that WOULD reach the pinned model gate, does the model correctly classify a representative case from EACH source family and from the two folded-in shapes." It does NOT establish a base rate for how COMMON each stratum is in the full corpus -- the cap is a fixed ceiling per stratum, not a proportional sample, so this arm answers a per-stratum ACCURACY question, never a POPULATION-SIZE question.
- **Arm 2 -- HEURISTIC-DEMOTED, 25 cases across 3 strata (oversampling `published_full`-sole and PGP-sole demotions specifically, per Codex findings 1 and 6):** answers "of the rows the funnel marks known WITHOUT ever consulting a model, how many are FALSE-knowns -- lost findings that ruling J's funnel-first architecture can never recover." It does NOT give a project-wide false-known RATE (3,688 `published_full` and 2,014 PGP pairs exist corpus-wide per Codex's own measurement; this arm samples a small, oversampled slice of each, never the full population) -- a rate estimate would need a much larger, proportionally-stratified sample, which is explicitly NOT what this size buys.
- **Arm 3 -- NO-SOURCE-TEXT, 8 cases, NO verdict collected:** answers, qualitatively, "does this population look like genuinely untouched fragments, or does something in it look surprising/wrong." It is NOT a labelling exercise -- these rows ship as candidates automatically regardless of what the owner observes here, per ruling J's own design ("these ship as candidates with no verdict"); this section exists so the owner can eyeball the bypass rather than trust it blindly, not to produce a graded number.

**What this sizing does NOT cover, stated plainly:** none of the three arms measures a corpus-wide RATE (what fraction of all claims fall in each stratum) -- only a per-stratum, per-shape ACCURACY/correctness check on a small, capped, deterministically-selected representative. A future pass wanting base rates would need to run the real funnel (plan 136-04) over the full corpus and report its own per-stratum counts, not re-derive them from this labelling sample.

## Part A -- IDENTITY spot-check (Classes 1-3, UNCHANGED)

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

## Part B -- NOVELTY SHADE cases (Class 6, unchanged, + Arm 1 residual)

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

A divergence shade records only THAT the aid and the claim disagree, never WHICH SIDE is right -- the owner's own review of the real cases found BOTH directions occur under the identical shade. Leave blank / not applicable for every non-divergence shade, and for every Arm 1 residual row (Arm 1 excludes the manuscripts already selected for Class 6, so a residual row is never ALSO a divergence candidate).

| Correctness | Choose this when... |
|---|---|
| `catalogue_correct` | the catalogue/aid is right; our claim is the false positive -- owner ruling F: reading the real cases, this is the COMMON outcome |
| `claim_correct` | our claim is right; the aid is wrong, thinner, or itself mistaken |
| `unclear` | cannot tell which side is correct from the information shown |

### Class 6 -- catalogue divergence (NOVELTY SHADE, owner rulings E/E′/F/G -- RETAINED UNCHANGED by the ruling-J redesign) (30 candidates)

**Plausible shades for this class:** `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms` (any other shade from the vocabulary table above is still a valid answer if the case warrants it; `unsure` / `skip` are always available).

#### Case 9

- **Manuscript:** Ms. Evr. Antonin B 1104 (sys_id `990000555880205171`)
- **Work(s):** CLAIMED (this identification): תשובות האיי גאון (w000650) / CATALOGUE NAMES (found in the identification text): תשובות (w000543)
- **Catalogue's own identification text:** שאלות ותשובות מאת האי בן שרירא גאון (קטע).
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('תשובות') than the one this claim identifies ('תשובות האיי גאון'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label; owner ruling G -- see 136-GATE1-DECISIONS.md § G): plausibly `confirms`, NOT a divergence -- the catalogue's own free text ('שאלות ותשובות מאת האי בן שרירא גאון') already states this claim's identification; only the structured work-id keying differed (this class's own selector over-fired here) -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 10

- **Manuscript:** Allony, Nehemia Ms. 304 (sys_id `990000413480205171`)
- **Work(s):** CLAIMED (this identification): משנה תורה, ספר זמנים (w000177) / CATALOGUE NAMES (found in the identification text): הגדה של פסח (w001159)
- **Catalogue's own identification text:** הגדה של פסח.
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('הגדה של פסח') than the one this claim identifies ('משנה תורה, ספר זמנים'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label; owner ruling F -- see 136-GATE1-DECISIONS.md § F): plausibly `diverges_work` -- confirm the shade AND supply the separate Correctness call (catalogue_correct / claim_correct / unclear), or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 11

- **Manuscript:** Ms. Evr. Antonin B 915 (sys_id `990000555810205171`)
- **Work(s):** CLAIMED (this identification): הלכות פסוקות (w001084) / CATALOGUE NAMES (found in the identification text): הלכות גדולות (w001196)
- **Catalogue's own identification text:** הלכות גדולות (בבא קמא).
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('הלכות גדולות') than the one this claim identifies ('הלכות פסוקות'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label; owner ruling F -- see 136-GATE1-DECISIONS.md § F): plausibly `diverges_work` -- confirm the shade AND supply the separate Correctness call (catalogue_correct / claim_correct / unclear), or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 12

- **Manuscript:** Cambridge University Library Ms. Add. 3162 (sys_id `990001398690205171`)
- **Work(s):** CLAIMED (this identification): משנה תורה, ספר אהבה (w000176) / CATALOGUE NAMES (found in the identification text): ברכת המזון (w001158)
- **Catalogue's own identification text:** ברכת המזון.
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('ברכת המזון') than the one this claim identifies ('משנה תורה, ספר אהבה'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label; owner ruling F -- see 136-GATE1-DECISIONS.md § F): plausibly `diverges_work` -- confirm the shade AND supply the separate Correctness call (catalogue_correct / claim_correct / unclear), or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 13

- **Manuscript:** Cambridge University Library Ms. Add. 1246 (sys_id `990001394270205171`)
- **Work(s):** CLAIMED (this identification): ספר יוסיפון (ערבי) (w001152) / CATALOGUE NAMES (found in the identification text): יוסיפון (w000853)
- **Catalogue's own identification text:** יוסיפון בערבית.
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('יוסיפון') than the one this claim identifies ('ספר יוסיפון (ערבי)'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label; owner ruling G -- see 136-GATE1-DECISIONS.md § G): plausibly `confirms`, NOT a divergence -- the catalogue's own free text ('יוסיפון בערבית') already states this claim's identification; only the structured work-id keying differed (this class's own selector over-fired here) -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 14

- **Manuscript:** Ms. Evr. Antonin B 961 (sys_id `990000555730205171`)
- **Work(s):** CLAIMED (this identification): תשובה בעניין סוכה (w000434) / CATALOGUE NAMES (found in the identification text): תשובות הגאונים (w000349)
- **Catalogue's own identification text:** שאלות ותשובות הגאונים.
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('תשובות הגאונים') than the one this claim identifies ('תשובה בעניין סוכה'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 15

- **Manuscript:** Ms. EVR ARAB I 3085 (sys_id `990000801470205171`)
- **Work(s):** CLAIMED (this identification): רס"ג, ספר יצירה פירוש (w000021) / CATALOGUE NAMES (found in the identification text): ספר יצירה (w000522)
- **Catalogue's own identification text:** פרוש ספר יצירה בערבית.
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('ספר יצירה') than the one this claim identifies ('רס"ג, ספר יצירה פירוש'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 16

- **Manuscript:** Catalogue Halper, Philadelphia 120 (sys_id `990001935160205171`)
- **Work(s):** CLAIMED (this identification): הלכות פסוקות, תרגומים ועיבודים עבריים, הלכות קידושין (w001037) / CATALOGUE NAMES (found in the identification text): הלכות פסוקות (w001084)
- **Catalogue's own identification text:** הלכות פסוקות (קדושין).
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('הלכות פסוקות') than the one this claim identifies ('הלכות פסוקות, תרגומים ועיבודים עבריים, הלכות קידושין'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label; owner ruling F -- see 136-GATE1-DECISIONS.md § F): plausibly `diverges_part` -- confirm the shade AND supply the separate Correctness call (catalogue_correct / claim_correct / unclear), or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 17

- **Manuscript:** Ms. Evr. Antonin B 236 (sys_id `990000905560205171`)
- **Work(s):** CLAIMED (this identification): מכילתא דרבי שמעון בן יוחאי (w000321) / CATALOGUE NAMES (found in the identification text): מכילתא דרבי ישמעאל (w000766)
- **Catalogue's own identification text:** מכילתא דרבי ישמעאל (בא-יתרו).
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('מכילתא דרבי ישמעאל') than the one this claim identifies ('מכילתא דרבי שמעון בן יוחאי'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label; owner ruling F -- see 136-GATE1-DECISIONS.md § F): plausibly `diverges_work` -- confirm the shade AND supply the separate Correctness call (catalogue_correct / claim_correct / unclear), or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 18

- **Manuscript:** Library of the Hungarian Academy of Sciences Ms. 57 (sys_id `990001004230205171`)
- **Work(s):** CLAIMED (this identification): ילקוט שמעוני על התורה (w001384) / CATALOGUE NAMES (found in the identification text): תנחומא (w000926)
- **Catalogue's own identification text:** מדרש תנחומא (קטעים).
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('תנחומא') than the one this claim identifies ('ילקוט שמעוני על התורה'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label; owner ruling F -- see 136-GATE1-DECISIONS.md § F): plausibly `diverges_work` -- confirm the shade AND supply the separate Correctness call (catalogue_correct / claim_correct / unclear), or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 19

- **Manuscript:** Cambridge University Library Ms. T-S NS 169.52 (sys_id `990051091870205171`)
- **Work(s):** CLAIMED (this identification): נסים גאון, חמשה ספרים (w000071) / CATALOGUE NAMES (found in the identification text): מגילת סתרים (w000509)
- **Catalogue's own identification text:** הלכה; מגילת סתרים [נסים בן יעקב]; ספרות הלכתית ופרשנות תלמודית; ספרות חז"ל. ; Halakhic Literature and Talmudic Commentaries ; Rabbinica; exposition of PT hagigah 78a and Tosefta Sheqalim 3:23-24 (the latter section numbered 140).
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('מגילת סתרים') than the one this claim identifies ('נסים גאון, חמשה ספרים'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 20

- **Manuscript:** Cambridge University Library Ms. T-S F 7.45 (sys_id `990051173260205171`)
- **Work(s):** CLAIMED (this identification): משנה תורה, הקדמה ומניין המצוות (w000174) / CATALOGUE NAMES (found in the identification text): הלכות ציצית (w001052)
- **Catalogue's own identification text:** משנה תורה;משנה תורה ופירושיו. ; Mishneh Torah and its Commentaries ; מנין מצות על סדר הרמב"ם, מסוף ספר אהבה (הלכות ציצית) עד סוף ספר זמנים
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('הלכות ציצית') than the one this claim identifies ('משנה תורה, הקדמה ומניין המצוות'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label; owner ruling F -- see 136-GATE1-DECISIONS.md § F): plausibly `diverges_part` -- confirm the shade AND supply the separate Correctness call (catalogue_correct / claim_correct / unclear), or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 21

- **Manuscript:** Ms. EVR II A 33 (sys_id `990000621960205171`)
- **Work(s):** CLAIMED (this identification): תנ"ך, בראשית (w000086) / CATALOGUE NAMES (found in the identification text): שאילתות (w000732)
- **Catalogue's own identification text:** שאילתות.
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('שאילתות') than the one this claim identifies ('תנ"ך, בראשית'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label; owner ruling F -- see 136-GATE1-DECISIONS.md § F): plausibly `diverges_work` -- confirm the shade AND supply the separate Correctness call (catalogue_correct / claim_correct / unclear), or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 22

- **Manuscript:** Cambridge University Library Ms. T-S C 1.23 (sys_id `990051150540205171`)
- **Work(s):** CLAIMED (this identification): בראשית רבה צה-צו, תוספת (w000900) / CATALOGUE NAMES (found in the identification text): בראשית רבה (w000156)
- **Catalogue's own identification text:** בראשית רבה;מדרש. ; Midrash ; בראשית רבה
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('בראשית רבה') than the one this claim identifies ('בראשית רבה צה-צו, תוספת'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label; owner ruling F -- see 136-GATE1-DECISIONS.md § F): plausibly `diverges_part` -- confirm the shade AND supply the separate Correctness call (catalogue_correct / claim_correct / unclear), or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 23

- **Manuscript:** Cambridge University Library Ms. T-S Misc. 15.67 (sys_id `990051080280205171`)
- **Work(s):** CLAIMED (this identification): ויקרא רבה (w000169) / CATALOGUE NAMES (found in the identification text): חובות הלבבות (תרגום אבן תיבון) (w000195)
- **Catalogue's own identification text:** חובות הלבבות (תרגום אבן תיבון). ; ספר חובות הלבבות. מעלת התשובה ומעלת הצדקה. דפוס.
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('חובות הלבבות (תרגום אבן תיבון)') than the one this claim identifies ('ויקרא רבה'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label; owner ruling F -- see 136-GATE1-DECISIONS.md § F): plausibly `diverges_work` -- confirm the shade AND supply the separate Correctness call (catalogue_correct / claim_correct / unclear), or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 24

- **Manuscript:** Library of the Alliance Israélite Un Ms. III A 101 (sys_id `990001506030205171`)
- **Work(s):** CLAIMED (this identification): רי"ף ברכות (w001317) / CATALOGUE NAMES (found in the identification text): מסכת דרך ארץ זוטא (w000787)
- **Catalogue's own identification text:** מסכת דרך ארץ זוטא (פרק השלום).
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('מסכת דרך ארץ זוטא') than the one this claim identifies ('רי"ף ברכות'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 25

- **Manuscript:** Cambridge University Library Ms. T-S C 1.19 (sys_id `990051150500205171`)
- **Work(s):** CLAIMED (this identification): פסיקתא דרב כהנא (w000904) / CATALOGUE NAMES (found in the identification text): ויקרא רבה (w000169)
- **Catalogue's own identification text:** ויקרא רבה;מדרש. ; Midrash ; ויקרא רבה
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('ויקרא רבה') than the one this claim identifies ('פסיקתא דרב כהנא'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 26

- **Manuscript:** Cambridge University Library Ms. T-S NS 291.103 (sys_id `990051104680205171`)
- **Work(s):** CLAIMED (this identification): רד"ק על יחזקאל (w001245) / CATALOGUE NAMES (found in the identification text): מדרש תהלים (w001124)
- **Catalogue's own identification text:** מדרש תהלים; ספרות הלכתית ופרשנות תלמודית; ספרות חז"ל; פירוש רד"ק למקרא. ; דוד בן יוסף קמחי, פירוש רד"ק למקרא: יחזקאל ח:א – יא
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('מדרש תהלים') than the one this claim identifies ('רד"ק על יחזקאל'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 27

- **Manuscript:** Adler, Elkan Nathan Ms. 2160.12 (sys_id `990053147670205171`)
- **Work(s):** CLAIMED (this identification): רס"ג, איכה תרגום (w000012) / CATALOGUE NAMES (found in the identification text): תרגום איכה (w001413)
- **Catalogue's own identification text:** מקרא [טקסט];תפסיר ערבי. ; תרגום איכה ד:א-כא לערבית-יהודית
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('תרגום איכה') than the one this claim identifies ('רס"ג, איכה תרגום'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 28

- **Manuscript:** Ms. EVR ARAB I 1771 (sys_id `990001535860205171`)
- **Work(s):** CLAIMED (this identification): ראב"ש, שמואל א פירוש (w000046) / CATALOGUE NAMES (found in the identification text): ספר הרקמה (w000037)
- **Catalogue's own identification text:** כתאב אלאסל : ; ספר הרקמה בערבית.
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('ספר הרקמה') than the one this claim identifies ('ראב"ש, שמואל א פירוש'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 29

- **Manuscript:** Cambridge University Library Ms. T-S E 1.2 (sys_id `990051158320205171`)
- **Work(s):** CLAIMED (this identification): משנה, פאה (w000259) / CATALOGUE NAMES (found in the identification text): ברייתא דמלאכת המשכן (w000157)
- **Catalogue's own identification text:** ברייתא דמלאכת המשכן;מסכת גרים;משנה [טקסט]. ; Mishnah: Pe'ah 4:4 – 6:3; Berakhot 1:1 – 3:1 ; משנה: פאה ד:ד – ו:ג; ברכות א:א – ג:א
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('ברייתא דמלאכת המשכן') than the one this claim identifies ('משנה, פאה'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 30

- **Manuscript:** Cambridge University Library Ms. T-S C 6.113 (sys_id `990051154860205171`)
- **Work(s):** CLAIMED (this identification): תלמוד בבלי, מגילה (w000957) / CATALOGUE NAMES (found in the identification text): ספרי דברים (w000166)
- **Catalogue's own identification text:** ספרי דברים;פירוש אבן עזרא למקרא;פרשנות מקרא. ; Sifre on Deuteronomy: 11:399 – 15:400; 7:404 – 15:400[בדילוגים] ; ספרי דברים: יא:399 – טו:400; ז:404 – טו:400[בדילוגים]
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('ספרי דברים') than the one this claim identifies ('תלמוד בבלי, מגילה'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 31

- **Manuscript:** Cambridge University Library Ms. T-S F 17.48 (sys_id `990051180030205171`)
- **Work(s):** CLAIMED (this identification): ויקרא רבה (w000169) / CATALOGUE NAMES (found in the identification text): קהלת רבה (w000891)
- **Catalogue's own identification text:** קהלת רבה;תלמוד ירושלמי [טקסט]. ; Midrash Rabbah Ecclesiastes: 5 ; קהלת רבה: ה
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('קהלת רבה') than the one this claim identifies ('ויקרא רבה'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 32

- **Manuscript:** Cambridge University Library Ms. T-S F 2(2).64 (sys_id `990051167990205171`)
- **Work(s):** CLAIMED (this identification): תלמוד בבלי, עבודה זרה (w000973) / CATALOGUE NAMES (found in the identification text): פסיקתא דרב כהנא (w000904)
- **Catalogue's own identification text:** מדרש;פסיקתא דרב כהנא;תלמוד בבלי [טקסט]. ; Talmud Bavli: Niddah 31 a – b; Sotah 44 b – 45 b; Avodah Zarah 51 a – 52 b ; תלמוד בבלי: נידה לא ע"א – ע"ב; סוטה מד ע"ב – מה ע"ב; עבודה זרה נא ע"א – נב ע"ב
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('פסיקתא דרב כהנא') than the one this claim identifies ('תלמוד בבלי, עבודה זרה'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 33

- **Manuscript:** Cambridge University Library Ms. L-G Talm. I 45 (sys_id `990001842460205171`)
- **Work(s):** CLAIMED (this identification): הלכות ארץ ישראליות ובבליות וליקוטים ממעשים לבני ארץ ישראל (w001075) / CATALOGUE NAMES (found in the identification text): מעשים לבני ארץ ישראל (w001030)
- **Catalogue's own identification text:** מעשים לבני ארץ ישראל.
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('מעשים לבני ארץ ישראל') than the one this claim identifies ('הלכות ארץ ישראליות ובבליות וליקוטים ממעשים לבני ארץ ישראל'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 34

- **Manuscript:** MS heb. e.22/1 (sys_id `990053426460205171`)
- **Work(s):** CLAIMED (this identification): הלכות פסוקות (w001084) / CATALOGUE NAMES (found in the identification text): והזהיר (w000779)
- **Catalogue's own identification text:** ספר והזהיר. ; והזהיר, פסק"ז
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('והזהיר') than the one this claim identifies ('הלכות פסוקות'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 35

- **Manuscript:** Cambridge University Library Ms. T-S NS 309.81 (sys_id `990051106940205171`)
- **Work(s):** CLAIMED (this identification): מסכת דרך ארץ זוטא (w000787) / CATALOGUE NAMES (found in the identification text): מסכת דרך ארץ רבה (w000786)
- **Catalogue's own identification text:** מסכת דרך ארץ רבה; סידור שלמה מסיג'ילמאסה; ספרות הלכתית ופרשנות תלמודית; ספרות חז"ל; קולופון. ; Halakhic Literature and Talmudic Commentaries ; שלמה בן נתן מסיג'ילמאסה, סידור שלמה מסיג'ילמאסה: ל
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('מסכת דרך ארץ רבה') than the one this claim identifies ('מסכת דרך ארץ זוטא'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 36

- **Manuscript:** Adler, Elkan Nathan Ms. 2700.24 (sys_id `990053586310205171`)
- **Work(s):** CLAIMED (this identification): פירוש המשנה, נזיקין (w000028) / CATALOGUE NAMES (found in the identification text): משנה, סנהדרין (w000291)
- **Catalogue's own identification text:** משנה [טקסט];פירוש המשנה לרמב"ם [ערבית]. ; פירוש הרמב"ם למשנה, סנהדרין י:א, ההקדמה לפרק חלק, עיקרים ראשון ושני (מהד' קאפח ע' רי-ריא).
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('משנה, סנהדרין') than the one this claim identifies ('פירוש המשנה, נזיקין'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 37

- **Manuscript:** Cambridge University Library Ms. T-S C 1.60 (sys_id `990051150910205171`)
- **Work(s):** CLAIMED (this identification): ברייתא דישועה, הפירוש, נוסח אחר (w000755) / CATALOGUE NAMES (found in the identification text): ברייתא דישועה (w000754)
- **Catalogue's own identification text:** ברייתא דישועה;מדרש. ; Midrash ; ברייתא דישועה
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('ברייתא דישועה') than the one this claim identifies ('ברייתא דישועה, הפירוש, נוסח אחר'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

#### Case 38

- **Manuscript:** Cambridge University Library Ms. T-S NS 329/0609 (sys_id `990001420410205171`)
- **Work(s):** CLAIMED (this identification): פסיקתא, ״אנכי אדוני אלהיך״ (איש שלום צח) (w000506) / CATALOGUE NAMES (found in the identification text): פסיקתא רבתי (w000803)
- **Catalogue's own identification text:** פסיקתא רבתי (פרשה כג:ו) :
- **Why it is hard:** This manuscript's own catalogue identification text names a DIFFERENT work ('פסיקתא רבתי') than the one this claim identifies ('פסיקתא, ״אנכי אדוני אלהיך״ (איש שלום צח)'); the two are NOT a granularity variant under the D-13d author-gated rule (different author, or an unrelated title) -- a genuine catalogue/claim divergence CANDIDATE (owner ruling F splits the shade into `diverges_work` / `diverges_part` by scope; owner ruling G warns this selector can over-fire when the catalogue's free text actually already agrees with the claim under a different structured key -- see the Vocabulary sheet/table).
- **PROPOSAL (draft, not a label): plausibly `diverges_work` or `diverges_part` (scope not yet distinguished by owner rulings F/G for this specific case) -- the catalogue and this claim name different works or parts; per ruling G, first check whether the catalogue's own FREE TEXT already states this claim's identification under a different spelling/phrasing before confirming a divergence -- confirm the shade, the scope, and (if divergent) the Correctness call, or correct.**
- **Shade verdict:** _(pending Task 3 -- `diverges_work`, `diverges_part`, `aid_more_specific`, `refines_granularity`, `confirms`, any other shade from the vocabulary table above, or `unsure` / `skip`)_
- **Correctness (only if `diverges_work` / `diverges_part` above):** _(pending Task 3 -- `catalogue_correct` / `claim_correct` / `unclear`, or blank if not applicable)_

### Arm 1 -- RESIDUAL: rows that would reach the model (NOVELTY SHADE, source-stratified, owner ruling J -- folds in the former Classes 4 and 7) (30 candidates)

**Plausible shades for this class:** `fills_gap`, `confirms`, `container_predicts`, `refines_granularity`, `aid_more_specific` (any other shade from the vocabulary table above is still a valid answer if the case warrants it; `unsure` / `skip` are always available).

#### Case 39

- **Manuscript:** Ms. Evr. Antonin B 3 (sys_id `990000905190205171`)
- **Work(s):** משנה, אבות (w000296)
- **Catalogue's own identification text:** סדור מנהג רומניא לכל השנה (קטע). משנה [טקסט] Mishnah [Text] משנה [טקסט] Mishnah [Text] משנה [טקסט] Mishnah [Text]
- **Residual stratum:** `container_predicts`
- **Why it is hard:** Residual stratum `container_predicts`: this manuscript's own catalogue text names a specific, NAMED standard-rite container (a container noun immediately followed by מנהג) whose standard, predictable content plausibly includes this claimed unit, without the catalogue ever naming the unit itself -- folds in the former Class 7 (owner rulings H/I). Under owner ruling J's funnel-first architecture, this row WOULD reach the pinned model gate.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `container_predicts` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `container_predicts`, `refines_granularity`, `aid_more_specific`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 40

- **Manuscript:** Ms. Evr. Antonin B 3 (sys_id `990000905190205171`)
- **Work(s):** הלכות מסידור (w000345)
- **Catalogue's own identification text:** סדור מנהג רומניא לכל השנה (קטע). משנה [טקסט] Mishnah [Text] משנה [טקסט] Mishnah [Text] משנה [טקסט] Mishnah [Text]
- **Residual stratum:** `container_predicts`
- **Why it is hard:** Residual stratum `container_predicts`: this manuscript's own catalogue text names a specific, NAMED standard-rite container (a container noun immediately followed by מנהג) whose standard, predictable content plausibly includes this claimed unit, without the catalogue ever naming the unit itself -- folds in the former Class 7 (owner rulings H/I). Under owner ruling J's funnel-first architecture, this row WOULD reach the pinned model gate.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `container_predicts` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `container_predicts`, `refines_granularity`, `aid_more_specific`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 41

- **Manuscript:** Library of the Hungarian Academy of Sciences Ms. 116 (sys_id `990001035960205171`)
- **Work(s):** התגלות הסודות (w000052)
- **Catalogue's own identification text:** מחזור מנהג ספרד לראש השנה (קטע).
- **Residual stratum:** `container_predicts`
- **Why it is hard:** Residual stratum `container_predicts`: this manuscript's own catalogue text names a specific, NAMED standard-rite container (a container noun immediately followed by מנהג) whose standard, predictable content plausibly includes this claimed unit, without the catalogue ever naming the unit itself -- folds in the former Class 7 (owner rulings H/I). Under owner ruling J's funnel-first architecture, this row WOULD reach the pinned model gate.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `container_predicts` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `container_predicts`, `refines_granularity`, `aid_more_specific`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 42

- **Manuscript:** Library of the Hungarian Academy of Sciences Ms. 116 (sys_id `990001035960205171`)
- **Work(s):** תנ"ך, ישעיהו (w000097)
- **Catalogue's own identification text:** מחזור מנהג ספרד לראש השנה (קטע).
- **Residual stratum:** `container_predicts`
- **Why it is hard:** Residual stratum `container_predicts`: this manuscript's own catalogue text names a specific, NAMED standard-rite container (a container noun immediately followed by מנהג) whose standard, predictable content plausibly includes this claimed unit, without the catalogue ever naming the unit itself -- folds in the former Class 7 (owner rulings H/I). Under owner ruling J's funnel-first architecture, this row WOULD reach the pinned model gate.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `container_predicts` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `container_predicts`, `refines_granularity`, `aid_more_specific`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 43

- **Manuscript:** Library of the Hungarian Academy of Sciences Ms. 116 (sys_id `990001035960205171`)
- **Work(s):** משנה תורה, ספר אהבה (w000176)
- **Catalogue's own identification text:** מחזור מנהג ספרד לראש השנה (קטע).
- **Residual stratum:** `container_predicts`
- **Why it is hard:** Residual stratum `container_predicts`: this manuscript's own catalogue text names a specific, NAMED standard-rite container (a container noun immediately followed by מנהג) whose standard, predictable content plausibly includes this claimed unit, without the catalogue ever naming the unit itself -- folds in the former Class 7 (owner rulings H/I). Under owner ruling J's funnel-first architecture, this row WOULD reach the pinned model gate.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `container_predicts` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `container_predicts`, `refines_granularity`, `aid_more_specific`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 44

- **Manuscript:** Ms. 2069 (sys_id `990000617200205171`)
- **Work(s):** כתר מלכות (רשב"ג/אבן גבירול) (w001129)
- **Catalogue's own identification text:** תפלות על דרך הקבלה.
- **Residual stratum:** `terse_catalogue`
- **Why it is hard:** Residual stratum `terse_catalogue`: this manuscript's own catalogue identification field is empty or too short (<=20 characters) to compare against -- folds in the former Class 4 (terse/missing catalogue text). Under owner ruling J's funnel-first architecture, this row WOULD reach the pinned model gate.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `fills_gap` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `container_predicts`, `refines_granularity`, `aid_more_specific`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 45

- **Manuscript:** Ms. EVR II A 28 (sys_id `990000621930205171`)
- **Work(s):** נסים גאון, חמשה ספרים (w000071)
- **Catalogue's own identification text:** קובץ ברפואה.
- **Residual stratum:** `terse_catalogue`
- **Why it is hard:** Residual stratum `terse_catalogue`: this manuscript's own catalogue identification field is empty or too short (<=20 characters) to compare against -- folds in the former Class 4 (terse/missing catalogue text). Under owner ruling J's funnel-first architecture, this row WOULD reach the pinned model gate.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `fills_gap` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `container_predicts`, `refines_granularity`, `aid_more_specific`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 46

- **Manuscript:** Ms. EVR II A 28 (sys_id `990000621930205171`)
- **Work(s):** היכלות רבתי (w000170)
- **Catalogue's own identification text:** קובץ ברפואה.
- **Residual stratum:** `terse_catalogue`
- **Why it is hard:** Residual stratum `terse_catalogue`: this manuscript's own catalogue identification field is empty or too short (<=20 characters) to compare against -- folds in the former Class 4 (terse/missing catalogue text). Under owner ruling J's funnel-first architecture, this row WOULD reach the pinned model gate.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `fills_gap` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `container_predicts`, `refines_granularity`, `aid_more_specific`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 47

- **Manuscript:** Ms. EVR II A 28 (sys_id `990000621930205171`)
- **Work(s):** ספר יצירה (w000522)
- **Catalogue's own identification text:** קובץ ברפואה.
- **Residual stratum:** `terse_catalogue`
- **Why it is hard:** Residual stratum `terse_catalogue`: this manuscript's own catalogue identification field is empty or too short (<=20 characters) to compare against -- folds in the former Class 4 (terse/missing catalogue text). Under owner ruling J's funnel-first architecture, this row WOULD reach the pinned model gate.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `fills_gap` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `container_predicts`, `refines_granularity`, `aid_more_specific`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 48

- **Manuscript:** Ms. EVR II A 28 (sys_id `990000621930205171`)
- **Work(s):** מסכת היכלות (w000840)
- **Catalogue's own identification text:** קובץ ברפואה.
- **Residual stratum:** `terse_catalogue`
- **Why it is hard:** Residual stratum `terse_catalogue`: this manuscript's own catalogue identification field is empty or too short (<=20 characters) to compare against -- folds in the former Class 4 (terse/missing catalogue text). Under owner ruling J's funnel-first architecture, this row WOULD reach the pinned model gate.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `fills_gap` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `container_predicts`, `refines_granularity`, `aid_more_specific`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 49

- **Manuscript:** Ms. EVR II A 6 (sys_id `990000589160205171`)
- **Work(s):** נסים גאון, חמשה ספרים (w000071)
- **Catalogue's own identification text:** קובץ. זוהר
- **Residual stratum:** `bib_sole`
- **Why it is hard:** Residual stratum `bib_sole`: the Friedberg bibliography has text for this manuscript that does NOT name this specific claimed work (no other checked source has any text at all for this manuscript). Under owner ruling J's funnel-first architecture, this row WOULD reach the pinned model gate.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `fills_gap` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `container_predicts`, `refines_granularity`, `aid_more_specific`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 50

- **Manuscript:** Ms. EVR II A 6 (sys_id `990000589160205171`)
- **Work(s):** היכלות רבתי (w000170)
- **Catalogue's own identification text:** קובץ. זוהר
- **Residual stratum:** `bib_sole`
- **Why it is hard:** Residual stratum `bib_sole`: the Friedberg bibliography has text for this manuscript that does NOT name this specific claimed work (no other checked source has any text at all for this manuscript). Under owner ruling J's funnel-first architecture, this row WOULD reach the pinned model gate.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `fills_gap` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `container_predicts`, `refines_granularity`, `aid_more_specific`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 51

- **Manuscript:** Ms. EVR II A 6 (sys_id `990000589160205171`)
- **Work(s):** ספר יצירה (w000522)
- **Catalogue's own identification text:** קובץ. זוהר
- **Residual stratum:** `bib_sole`
- **Why it is hard:** Residual stratum `bib_sole`: the Friedberg bibliography has text for this manuscript that does NOT name this specific claimed work (no other checked source has any text at all for this manuscript). Under owner ruling J's funnel-first architecture, this row WOULD reach the pinned model gate.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `fills_gap` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `container_predicts`, `refines_granularity`, `aid_more_specific`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 52

- **Manuscript:** Ms. EVR II A 6 (sys_id `990000589160205171`)
- **Work(s):** מדרש עשרת הרוגי מלכות, נוסח ארוך (w000937)
- **Catalogue's own identification text:** קובץ. זוהר
- **Residual stratum:** `bib_sole`
- **Why it is hard:** Residual stratum `bib_sole`: the Friedberg bibliography has text for this manuscript that does NOT name this specific claimed work (no other checked source has any text at all for this manuscript). Under owner ruling J's funnel-first architecture, this row WOULD reach the pinned model gate.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `fills_gap` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `container_predicts`, `refines_granularity`, `aid_more_specific`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 53

- **Manuscript:** Ms. EVR II A 6 (sys_id `990000589160205171`)
- **Work(s):** רד"ק על דברי הימים א׳ (w001249)
- **Catalogue's own identification text:** קובץ. זוהר
- **Residual stratum:** `bib_sole`
- **Why it is hard:** Residual stratum `bib_sole`: the Friedberg bibliography has text for this manuscript that does NOT name this specific claimed work (no other checked source has any text at all for this manuscript). Under owner ruling J's funnel-first architecture, this row WOULD reach the pinned model gate.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `fills_gap` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `container_predicts`, `refines_granularity`, `aid_more_specific`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 54

- **Manuscript:** Ms. EVR ARAB I 1722 (sys_id `990001535460205171`)
- **Work(s):** פירוש המשנה, נשים (w000024)
- **Catalogue's own identification text:** ספר תשובה. כתאב אלרד
- **Residual stratum:** `fgp_sole`
- **Why it is hard:** Residual stratum `fgp_sole`: an FGP transcription's own title/author fields do NOT name this specific claimed work (no other checked source has any text at all). Under owner ruling J's funnel-first architecture, this row WOULD reach the pinned model gate.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `fills_gap` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `container_predicts`, `refines_granularity`, `aid_more_specific`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 55

- **Manuscript:** Ms. EVR ARAB I 1722 (sys_id `990001535460205171`)
- **Work(s):** פירוש המשנה, זרעי (w000026)
- **Catalogue's own identification text:** ספר תשובה. כתאב אלרד
- **Residual stratum:** `fgp_sole`
- **Why it is hard:** Residual stratum `fgp_sole`: an FGP transcription's own title/author fields do NOT name this specific claimed work (no other checked source has any text at all). Under owner ruling J's funnel-first architecture, this row WOULD reach the pinned model gate.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `fills_gap` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `container_predicts`, `refines_granularity`, `aid_more_specific`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 56

- **Manuscript:** Ms. EVR ARAB I 1722 (sys_id `990001535460205171`)
- **Work(s):** פירוש המשנה, מועד (w000027)
- **Catalogue's own identification text:** ספר תשובה. כתאב אלרד
- **Residual stratum:** `fgp_sole`
- **Why it is hard:** Residual stratum `fgp_sole`: an FGP transcription's own title/author fields do NOT name this specific claimed work (no other checked source has any text at all). Under owner ruling J's funnel-first architecture, this row WOULD reach the pinned model gate.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `fills_gap` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `container_predicts`, `refines_granularity`, `aid_more_specific`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 57

- **Manuscript:** Ms. EVR ARAB I 1722 (sys_id `990001535460205171`)
- **Work(s):** ראב"ש, שמואל א פירוש (w000046)
- **Catalogue's own identification text:** ספר תשובה. כתאב אלרד
- **Residual stratum:** `fgp_sole`
- **Why it is hard:** Residual stratum `fgp_sole`: an FGP transcription's own title/author fields do NOT name this specific claimed work (no other checked source has any text at all). Under owner ruling J's funnel-first architecture, this row WOULD reach the pinned model gate.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `fills_gap` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `container_predicts`, `refines_granularity`, `aid_more_specific`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 58

- **Manuscript:** Ms. EVR ARAB I 1722 (sys_id `990001535460205171`)
- **Work(s):** מדרש הבאור ב (w000050)
- **Catalogue's own identification text:** ספר תשובה. כתאב אלרד
- **Residual stratum:** `fgp_sole`
- **Why it is hard:** Residual stratum `fgp_sole`: an FGP transcription's own title/author fields do NOT name this specific claimed work (no other checked source has any text at all). Under owner ruling J's funnel-first architecture, this row WOULD reach the pinned model gate.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `fills_gap` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `container_predicts`, `refines_granularity`, `aid_more_specific`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 59

- **Manuscript:** Ms. 526 (sys_id `990000432020205171`)
- **Work(s):** תלמוד בבלי, תענית (w000956)
- **Catalogue's own identification text:** מעשיות מן התלמוד : ; מעשיות מן התלמוד בתרגום לערבית-יהודית.
- **Residual stratum:** `catalogue_sole`
- **Why it is hard:** Residual stratum `catalogue_sole`: this manuscript's own catalogue identification (NLI title and/or the FJMS catalog table) does NOT name this specific claimed work (no other checked source has any text at all). Under owner ruling J's funnel-first architecture, this row WOULD reach the pinned model gate.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `fills_gap` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `container_predicts`, `refines_granularity`, `aid_more_specific`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 60

- **Manuscript:** Ms. 526 (sys_id `990000432020205171`)
- **Work(s):** ילקוט שמעוני על נ"ך (w001383)
- **Catalogue's own identification text:** מעשיות מן התלמוד : ; מעשיות מן התלמוד בתרגום לערבית-יהודית.
- **Residual stratum:** `catalogue_sole`
- **Why it is hard:** Residual stratum `catalogue_sole`: this manuscript's own catalogue identification (NLI title and/or the FJMS catalog table) does NOT name this specific claimed work (no other checked source has any text at all). Under owner ruling J's funnel-first architecture, this row WOULD reach the pinned model gate.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `fills_gap` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `container_predicts`, `refines_granularity`, `aid_more_specific`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 61

- **Manuscript:** Allony, Nehemia Ms. 30g (sys_id `990000465650205171`)
- **Work(s):** תנ"ך, דברים (w000090)
- **Catalogue's own identification text:** תורה (דברים יד:ג-יד:כא, טז:ח-טז:יז).
- **Residual stratum:** `catalogue_sole`
- **Why it is hard:** Residual stratum `catalogue_sole`: this manuscript's own catalogue identification (NLI title and/or the FJMS catalog table) does NOT name this specific claimed work (no other checked source has any text at all). Under owner ruling J's funnel-first architecture, this row WOULD reach the pinned model gate.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `fills_gap` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `container_predicts`, `refines_granularity`, `aid_more_specific`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 62

- **Manuscript:** Ms. Evr. Antonin B 1093 (sys_id `990000555840205171`)
- **Work(s):** פרקי רבי אליעזר (w000807)
- **Catalogue's own identification text:** תורת האדם : ; קטע מענין האבל-ענין שבתות וימים טובים. תורת האדם תורת האדם תורת האדם תורת האדם
- **Residual stratum:** `catalogue_sole`
- **Why it is hard:** Residual stratum `catalogue_sole`: this manuscript's own catalogue identification (NLI title and/or the FJMS catalog table) does NOT name this specific claimed work (no other checked source has any text at all). Under owner ruling J's funnel-first architecture, this row WOULD reach the pinned model gate.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `fills_gap` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `container_predicts`, `refines_granularity`, `aid_more_specific`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 63

- **Manuscript:** Ms. EVR II A 313/22 (sys_id `990000622340205171`)
- **Work(s):** משנה תורה, ספר אהבה (w000176)
- **Catalogue's own identification text:** משנה תורה (ספר אהבה).
- **Residual stratum:** `catalogue_sole`
- **Why it is hard:** Residual stratum `catalogue_sole`: this manuscript's own catalogue identification (NLI title and/or the FJMS catalog table) does NOT name this specific claimed work (no other checked source has any text at all). Under owner ruling J's funnel-first architecture, this row WOULD reach the pinned model gate.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `fills_gap` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `container_predicts`, `refines_granularity`, `aid_more_specific`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 64

- **Manuscript:** Ms. 523 (sys_id `990000432000205171`)
- **Work(s):** משנה, מעשרות (w000264)
- **Catalogue's own identification text:** משנה סדר זרעים (קטעים) : ; קטע ממס' מעשר-שני, מעשרות, קטע ממס' חלה. עם טעמים וניקוד חלקי. משנה [טקסט] Mishnah [Text] משנה [טקסט] Mishnah [Text]
- **Residual stratum:** `multi_source`
- **Why it is hard:** Residual stratum `multi_source`: >=2 checked sources have text for this manuscript, but NONE of them names this specific claimed work. Under owner ruling J's funnel-first architecture, this row WOULD reach the pinned model gate.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `fills_gap` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `container_predicts`, `refines_granularity`, `aid_more_specific`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 65

- **Manuscript:** Ms. 523 (sys_id `990000432000205171`)
- **Work(s):** משנה, מעשר שני (w000265)
- **Catalogue's own identification text:** משנה סדר זרעים (קטעים) : ; קטע ממס' מעשר-שני, מעשרות, קטע ממס' חלה. עם טעמים וניקוד חלקי. משנה [טקסט] Mishnah [Text] משנה [טקסט] Mishnah [Text]
- **Residual stratum:** `multi_source`
- **Why it is hard:** Residual stratum `multi_source`: >=2 checked sources have text for this manuscript, but NONE of them names this specific claimed work. Under owner ruling J's funnel-first architecture, this row WOULD reach the pinned model gate.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `fills_gap` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `container_predicts`, `refines_granularity`, `aid_more_specific`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 66

- **Manuscript:** Ms. 523 (sys_id `990000432000205171`)
- **Work(s):** משנה, חלה (w000266)
- **Catalogue's own identification text:** משנה סדר זרעים (קטעים) : ; קטע ממס' מעשר-שני, מעשרות, קטע ממס' חלה. עם טעמים וניקוד חלקי. משנה [טקסט] Mishnah [Text] משנה [טקסט] Mishnah [Text]
- **Residual stratum:** `multi_source`
- **Why it is hard:** Residual stratum `multi_source`: >=2 checked sources have text for this manuscript, but NONE of them names this specific claimed work. Under owner ruling J's funnel-first architecture, this row WOULD reach the pinned model gate.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `fills_gap` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `container_predicts`, `refines_granularity`, `aid_more_specific`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 67

- **Manuscript:** Constantin von Tischendorf Collection Ms. 43 (sys_id `990000571680205171`)
- **Work(s):** תנ"ך, יהושע (w000091)
- **Catalogue's own identification text:** נביאים ראשונים (קטעים) : ; עם ניקוד וטעמים, מסורה קטנה וגדולה. מקרא [טקסט] Bible [Text]
- **Residual stratum:** `multi_source`
- **Why it is hard:** Residual stratum `multi_source`: >=2 checked sources have text for this manuscript, but NONE of them names this specific claimed work. Under owner ruling J's funnel-first architecture, this row WOULD reach the pinned model gate.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `fills_gap` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `container_predicts`, `refines_granularity`, `aid_more_specific`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

#### Case 68

- **Manuscript:** Constantin von Tischendorf Collection Ms. 43 (sys_id `990000571680205171`)
- **Work(s):** תנ"ך, שופטים (w000092)
- **Catalogue's own identification text:** נביאים ראשונים (קטעים) : ; עם ניקוד וטעמים, מסורה קטנה וגדולה. מקרא [טקסט] Bible [Text]
- **Residual stratum:** `multi_source`
- **Why it is hard:** Residual stratum `multi_source`: >=2 checked sources have text for this manuscript, but NONE of them names this specific claimed work. Under owner ruling J's funnel-first architecture, this row WOULD reach the pinned model gate.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `fills_gap` -- confirm or correct.**
- **Shade verdict:** _(pending Task 3 -- `fills_gap`, `confirms`, `container_predicts`, `refines_granularity`, `aid_more_specific`, any other shade from the vocabulary table above, or `unsure` / `skip`)_

## Part C -- HEURISTIC-DEMOTED cases (Arm 2, owner ruling J)

**Question type: DEMOTION CORRECTNESS.** These rows were marked "already recorded" by the CURRENT heuristic funnel's own decisive test (a bib `published_full` row, or a PGP document with any non-empty description/transcription) -- they NEVER reach the model at all under ruling J's funnel-first architecture. For EACH case, judge whether the demoting source genuinely names THIS specific claimed work, or only tripped the heuristic through generic presence:

| Answer | Choose this when... |
|---|---|
| `demotion_correct` | the demoting source genuinely already names/records THIS SPECIFIC work on THIS fragment -- the heuristic's demotion (marking it already known, never reaching the model) is right |
| `false_known` | the demoting source does NOT actually name this specific work -- only its GENERIC presence (e.g. a bibliography record, a PGP description/transcription on the fragment) tripped the heuristic. Per owner ruling J this is an UNRECOVERABLE lost finding: the funnel only ever demotes (discovery -> known, never the reverse) and the model never sees a heuristically-demoted row, so a false_known here is permanent unless this labelling catches it |
| `unsure` | you cannot judge this case from the information shown -- a real and useful answer |
| `skip` | you choose not to judge this case at all -- recorded as skipped |

#### Case 69

- **Manuscript:** Allony, Nehemia Ms. 113 (sys_id `990000465700205171`)
- **Claimed work:** הלכות גדולות (w001196)
- **Catalogue's own identification text:** כתאב אלנפקאת (קטע). כתאב אלנפקאת
- **Demotion stratum:** `published_full_sole`
- **Why this demotion is being checked:** Demotion stratum `published_full_sole`: the CURRENT heuristic treats ANY bibliography row with TranscriptionType='Full' as naming this claim, regardless of whether that row's own title/author actually matches (Codex finding 1) -- no OTHER checked source agrees.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `demotion_correct` -- confirm or correct to `false_known` if the demoting source does not actually name this specific work.**
- **Demotion verdict:** _(pending Task 3 -- `demotion_correct` / `false_known` / `unsure` / `skip`)_

#### Case 70

- **Manuscript:** Ms. Evr. Antonin B 1037 (sys_id `990000555740205171`)
- **Claimed work:** תשובות שמואל גאון (w000428)
- **Catalogue's own identification text:** שאלות ותשובות הגאונים.
- **Demotion stratum:** `published_full_sole`
- **Why this demotion is being checked:** Demotion stratum `published_full_sole`: the CURRENT heuristic treats ANY bibliography row with TranscriptionType='Full' as naming this claim, regardless of whether that row's own title/author actually matches (Codex finding 1) -- no OTHER checked source agrees.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `demotion_correct` -- confirm or correct to `false_known` if the demoting source does not actually name this specific work.**
- **Demotion verdict:** _(pending Task 3 -- `demotion_correct` / `false_known` / `unsure` / `skip`)_

#### Case 71

- **Manuscript:** Ms. Evr. Antonin B 292 (sys_id `990000555790205171`)
- **Claimed work:** במדבר רבה א-יד (w000496)
- **Catalogue's own identification text:** אבות דרבי נתן נוסח א : ; קטעים מפרקים א-ב. אבות דרבי נתן, נוסחא א Avot de-Rabbi Natan I אבות דרבי נתן, נוסחא א Avot de-Rabbi Natan I אבות דרבי נתן, נוסחא א Avot de-Rabbi Natan I אבות דרבי נתן, נוסחא א Avot de-Rabbi Natan I אבות דרבי נתן, נוסחא א Avot de-Rabbi Natan I אבות דרבי נתן, נוסחא א Avot de-Rabbi Natan I אבות דרבי נתן Avot de-Rabbi Natan אבות דרבי נתן Avot de-Rabbi Natan אבות דרבי נתן, נוסחא א Avot de-Rabbi Natan I
- **Demotion stratum:** `published_full_sole`
- **Why this demotion is being checked:** Demotion stratum `published_full_sole`: the CURRENT heuristic treats ANY bibliography row with TranscriptionType='Full' as naming this claim, regardless of whether that row's own title/author actually matches (Codex finding 1) -- no OTHER checked source agrees.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `demotion_correct` -- confirm or correct to `false_known` if the demoting source does not actually name this specific work.**
- **Demotion verdict:** _(pending Task 3 -- `demotion_correct` / `false_known` / `unsure` / `skip`)_

#### Case 72

- **Manuscript:** Ms. EVR ARAB I 1467 (sys_id `990000635630205171`)
- **Claimed work:** הלכות גדולות (w001196)
- **Catalogue's own identification text:** קובץ חבורי הלכה מאת שמואל בן חפני גאון בשפה הערבית. כתאב אלשפעה כתאב אחכאם שרע אלציצית כתאב אלבלוג כתאב אלבלוג ואל אדראך כתאב אחכאם שרע אלציצית כתאב אלשפעה
- **Demotion stratum:** `published_full_sole`
- **Why this demotion is being checked:** Demotion stratum `published_full_sole`: the CURRENT heuristic treats ANY bibliography row with TranscriptionType='Full' as naming this claim, regardless of whether that row's own title/author actually matches (Codex finding 1) -- no OTHER checked source agrees.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `demotion_correct` -- confirm or correct to `false_known` if the demoting source does not actually name this specific work.**
- **Demotion verdict:** _(pending Task 3 -- `demotion_correct` / `false_known` / `unsure` / `skip`)_

#### Case 73

- **Manuscript:** Ms. EVR ARAB I 1793 (sys_id `990000635770205171`)
- **Claimed work:** משנה, ראש השנה (w000276)
- **Catalogue's own identification text:** כתאב אלאסתבצאר : ; בערבית. כתאב אלאסתבצאר
- **Demotion stratum:** `published_full_sole`
- **Why this demotion is being checked:** Demotion stratum `published_full_sole`: the CURRENT heuristic treats ANY bibliography row with TranscriptionType='Full' as naming this claim, regardless of whether that row's own title/author actually matches (Codex finding 1) -- no OTHER checked source agrees.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `demotion_correct` -- confirm or correct to `false_known` if the demoting source does not actually name this specific work.**
- **Demotion verdict:** _(pending Task 3 -- `demotion_correct` / `false_known` / `unsure` / `skip`)_

#### Case 74

- **Manuscript:** Ms. EVR II A 85/14 (sys_id `990000847320205171`)
- **Claimed work:** אגרות הרמב״ם (שילת) (w001140)
- **Catalogue's own identification text:** מקמה (קטע).
- **Demotion stratum:** `published_full_sole`
- **Why this demotion is being checked:** Demotion stratum `published_full_sole`: the CURRENT heuristic treats ANY bibliography row with TranscriptionType='Full' as naming this claim, regardless of whether that row's own title/author actually matches (Codex finding 1) -- no OTHER checked source agrees.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `demotion_correct` -- confirm or correct to `false_known` if the demoting source does not actually name this specific work.**
- **Demotion verdict:** _(pending Task 3 -- `demotion_correct` / `false_known` / `unsure` / `skip`)_

#### Case 75

- **Manuscript:** Ms. EVR II A 380 (sys_id `990000848410205171`)
- **Claimed work:** הכוזרי (w000053)
- **Catalogue's own identification text:** קובץ בהגות. זוהר
- **Demotion stratum:** `published_full_sole`
- **Why this demotion is being checked:** Demotion stratum `published_full_sole`: the CURRENT heuristic treats ANY bibliography row with TranscriptionType='Full' as naming this claim, regardless of whether that row's own title/author actually matches (Codex finding 1) -- no OTHER checked source agrees.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `demotion_correct` -- confirm or correct to `false_known` if the demoting source does not actually name this specific work.**
- **Demotion verdict:** _(pending Task 3 -- `demotion_correct` / `false_known` / `unsure` / `skip`)_

#### Case 76

- **Manuscript:** Ms. EVR II A 380 (sys_id `990000848410205171`)
- **Claimed work:** ספר יצירה (w000522)
- **Catalogue's own identification text:** קובץ בהגות. זוהר
- **Demotion stratum:** `published_full_sole`
- **Why this demotion is being checked:** Demotion stratum `published_full_sole`: the CURRENT heuristic treats ANY bibliography row with TranscriptionType='Full' as naming this claim, regardless of whether that row's own title/author actually matches (Codex finding 1) -- no OTHER checked source agrees.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `demotion_correct` -- confirm or correct to `false_known` if the demoting source does not actually name this specific work.**
- **Demotion verdict:** _(pending Task 3 -- `demotion_correct` / `false_known` / `unsure` / `skip`)_

#### Case 77

- **Manuscript:** Ms. EVR ARAB I 4633 (sys_id `990000854530205171`)
- **Claimed work:** תנ"ך, בראשית (w000086)
- **Catalogue's own identification text:** פרוש התורה (בראשית). פירוש על התורה פירוש על התורה
- **Demotion stratum:** `published_full_sole`
- **Why this demotion is being checked:** Demotion stratum `published_full_sole`: the CURRENT heuristic treats ANY bibliography row with TranscriptionType='Full' as naming this claim, regardless of whether that row's own title/author actually matches (Codex finding 1) -- no OTHER checked source agrees.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `demotion_correct` -- confirm or correct to `false_known` if the demoting source does not actually name this specific work.**
- **Demotion verdict:** _(pending Task 3 -- `demotion_correct` / `false_known` / `unsure` / `skip`)_

#### Case 78

- **Manuscript:** Ms. EVR ARAB I 1687 (sys_id `990000854930205171`)
- **Claimed work:** משנה, ראש השנה (w000276)
- **Catalogue's own identification text:** כתאב אלאנואר ואלמראקב (קטעים). כתאב אלאנואר ואלמראקב כתאב אלאנואר ואלמראקב
- **Demotion stratum:** `published_full_sole`
- **Why this demotion is being checked:** Demotion stratum `published_full_sole`: the CURRENT heuristic treats ANY bibliography row with TranscriptionType='Full' as naming this claim, regardless of whether that row's own title/author actually matches (Codex finding 1) -- no OTHER checked source agrees.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `demotion_correct` -- confirm or correct to `false_known` if the demoting source does not actually name this specific work.**
- **Demotion verdict:** _(pending Task 3 -- `demotion_correct` / `false_known` / `unsure` / `skip`)_

#### Case 79

- **Manuscript:** Ms. EVR II B 159 (sys_id `990000571710205171`)
- **Claimed work:** תנ"ך, דברים (w000090)
- **Catalogue's own identification text:** תורה (דברים לא:י-לד:יב) : ; עם ניקוד וטעמים, מסורה קטנה וגדולה. מקרא [טקסט] Bible [Text]
- **Demotion stratum:** `pgp_sole`
- **Why this demotion is being checked:** Demotion stratum `pgp_sole`: the CURRENT heuristic treats ANY PGP document with a non-empty description or transcription as naming this claim, regardless of whether that text actually names this specific work (Codex finding 6) -- no OTHER checked source agrees.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `demotion_correct` -- confirm or correct to `false_known` if the demoting source does not actually name this specific work.**
- **Demotion verdict:** _(pending Task 3 -- `demotion_correct` / `false_known` / `unsure` / `skip`)_

#### Case 80

- **Manuscript:** Ms. EVR II B 25 (sys_id `990000571720205171`)
- **Claimed work:** תנ"ך, יהושע (w000091)
- **Catalogue's own identification text:** נביאים (קטעים). מקרא [טקסט] Bible [Text] מקרא [טקסט] Bible [Text]
- **Demotion stratum:** `pgp_sole`
- **Why this demotion is being checked:** Demotion stratum `pgp_sole`: the CURRENT heuristic treats ANY PGP document with a non-empty description or transcription as naming this claim, regardless of whether that text actually names this specific work (Codex finding 6) -- no OTHER checked source agrees.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `demotion_correct` -- confirm or correct to `false_known` if the demoting source does not actually name this specific work.**
- **Demotion verdict:** _(pending Task 3 -- `demotion_correct` / `false_known` / `unsure` / `skip`)_

#### Case 81

- **Manuscript:** Ms. EVR II B 25 (sys_id `990000571720205171`)
- **Claimed work:** תנ"ך, שופטים (w000092)
- **Catalogue's own identification text:** נביאים (קטעים). מקרא [טקסט] Bible [Text] מקרא [טקסט] Bible [Text]
- **Demotion stratum:** `pgp_sole`
- **Why this demotion is being checked:** Demotion stratum `pgp_sole`: the CURRENT heuristic treats ANY PGP document with a non-empty description or transcription as naming this claim, regardless of whether that text actually names this specific work (Codex finding 6) -- no OTHER checked source agrees.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `demotion_correct` -- confirm or correct to `false_known` if the demoting source does not actually name this specific work.**
- **Demotion verdict:** _(pending Task 3 -- `demotion_correct` / `false_known` / `unsure` / `skip`)_

#### Case 82

- **Manuscript:** Ms. EVR II B 25 (sys_id `990000571720205171`)
- **Claimed work:** תנ"ך, שמואל א (w000093)
- **Catalogue's own identification text:** נביאים (קטעים). מקרא [טקסט] Bible [Text] מקרא [טקסט] Bible [Text]
- **Demotion stratum:** `pgp_sole`
- **Why this demotion is being checked:** Demotion stratum `pgp_sole`: the CURRENT heuristic treats ANY PGP document with a non-empty description or transcription as naming this claim, regardless of whether that text actually names this specific work (Codex finding 6) -- no OTHER checked source agrees.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `demotion_correct` -- confirm or correct to `false_known` if the demoting source does not actually name this specific work.**
- **Demotion verdict:** _(pending Task 3 -- `demotion_correct` / `false_known` / `unsure` / `skip`)_

#### Case 83

- **Manuscript:** Ms. EVR II B 25 (sys_id `990000571720205171`)
- **Claimed work:** תנ"ך, שמואל ב (w000094)
- **Catalogue's own identification text:** נביאים (קטעים). מקרא [טקסט] Bible [Text] מקרא [טקסט] Bible [Text]
- **Demotion stratum:** `pgp_sole`
- **Why this demotion is being checked:** Demotion stratum `pgp_sole`: the CURRENT heuristic treats ANY PGP document with a non-empty description or transcription as naming this claim, regardless of whether that text actually names this specific work (Codex finding 6) -- no OTHER checked source agrees.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `demotion_correct` -- confirm or correct to `false_known` if the demoting source does not actually name this specific work.**
- **Demotion verdict:** _(pending Task 3 -- `demotion_correct` / `false_known` / `unsure` / `skip`)_

#### Case 84

- **Manuscript:** Ms. EVR II B 25 (sys_id `990000571720205171`)
- **Claimed work:** תנ"ך, מלכים א (w000095)
- **Catalogue's own identification text:** נביאים (קטעים). מקרא [טקסט] Bible [Text] מקרא [טקסט] Bible [Text]
- **Demotion stratum:** `pgp_sole`
- **Why this demotion is being checked:** Demotion stratum `pgp_sole`: the CURRENT heuristic treats ANY PGP document with a non-empty description or transcription as naming this claim, regardless of whether that text actually names this specific work (Codex finding 6) -- no OTHER checked source agrees.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `demotion_correct` -- confirm or correct to `false_known` if the demoting source does not actually name this specific work.**
- **Demotion verdict:** _(pending Task 3 -- `demotion_correct` / `false_known` / `unsure` / `skip`)_

#### Case 85

- **Manuscript:** Ms. EVR II B 25 (sys_id `990000571720205171`)
- **Claimed work:** תנ"ך, מלכים ב (w000096)
- **Catalogue's own identification text:** נביאים (קטעים). מקרא [טקסט] Bible [Text] מקרא [טקסט] Bible [Text]
- **Demotion stratum:** `pgp_sole`
- **Why this demotion is being checked:** Demotion stratum `pgp_sole`: the CURRENT heuristic treats ANY PGP document with a non-empty description or transcription as naming this claim, regardless of whether that text actually names this specific work (Codex finding 6) -- no OTHER checked source agrees.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `demotion_correct` -- confirm or correct to `false_known` if the demoting source does not actually name this specific work.**
- **Demotion verdict:** _(pending Task 3 -- `demotion_correct` / `false_known` / `unsure` / `skip`)_

#### Case 86

- **Manuscript:** Ms. EVR II B 25 (sys_id `990000571720205171`)
- **Claimed work:** תנ"ך, ישעיהו (w000097)
- **Catalogue's own identification text:** נביאים (קטעים). מקרא [טקסט] Bible [Text] מקרא [טקסט] Bible [Text]
- **Demotion stratum:** `pgp_sole`
- **Why this demotion is being checked:** Demotion stratum `pgp_sole`: the CURRENT heuristic treats ANY PGP document with a non-empty description or transcription as naming this claim, regardless of whether that text actually names this specific work (Codex finding 6) -- no OTHER checked source agrees.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `demotion_correct` -- confirm or correct to `false_known` if the demoting source does not actually name this specific work.**
- **Demotion verdict:** _(pending Task 3 -- `demotion_correct` / `false_known` / `unsure` / `skip`)_

#### Case 87

- **Manuscript:** Ms. EVR II B 25 (sys_id `990000571720205171`)
- **Claimed work:** תנ"ך, דברי הימים ב (w000124)
- **Catalogue's own identification text:** נביאים (קטעים). מקרא [טקסט] Bible [Text] מקרא [טקסט] Bible [Text]
- **Demotion stratum:** `pgp_sole`
- **Why this demotion is being checked:** Demotion stratum `pgp_sole`: the CURRENT heuristic treats ANY PGP document with a non-empty description or transcription as naming this claim, regardless of whether that text actually names this specific work (Codex finding 6) -- no OTHER checked source agrees.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `demotion_correct` -- confirm or correct to `false_known` if the demoting source does not actually name this specific work.**
- **Demotion verdict:** _(pending Task 3 -- `demotion_correct` / `false_known` / `unsure` / `skip`)_

#### Case 88

- **Manuscript:** Ms. EVR II B 1283 (sys_id `990000571850205171`)
- **Claimed work:** תנ"ך, ישעיהו (w000097)
- **Catalogue's own identification text:** נביאים אחרונים (קטעים). מקרא [טקסט] Bible [Text]
- **Demotion stratum:** `pgp_sole`
- **Why this demotion is being checked:** Demotion stratum `pgp_sole`: the CURRENT heuristic treats ANY PGP document with a non-empty description or transcription as naming this claim, regardless of whether that text actually names this specific work (Codex finding 6) -- no OTHER checked source agrees.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `demotion_correct` -- confirm or correct to `false_known` if the demoting source does not actually name this specific work.**
- **Demotion verdict:** _(pending Task 3 -- `demotion_correct` / `false_known` / `unsure` / `skip`)_

#### Case 89

- **Manuscript:** Ms. Evr. Antonin B 308 (sys_id `990000555750205171`)
- **Claimed work:** תשובות שר שלום גאון (w000499)
- **Catalogue's own identification text:** שאלות ותשובות הגאונים.
- **Demotion stratum:** `other_demotion`
- **Why this demotion is being checked:** Demotion stratum `other_demotion`: a genuine token-name-match (catalogue, FGP, or bib known_bib) demotes this claim -- included for comparison against the two oversampled over-broad categories above.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `demotion_correct` -- confirm or correct to `false_known` if the demoting source does not actually name this specific work.**
- **Demotion verdict:** _(pending Task 3 -- `demotion_correct` / `false_known` / `unsure` / `skip`)_

#### Case 90

- **Manuscript:** Ms. Evr. Antonin B 297 (sys_id `990000555760205171`)
- **Claimed work:** מסכת אבות דרבי נתן, נוסח ב (w000789)
- **Catalogue's own identification text:** אבות דרבי נתן נוסח ב : ; קטע מפרקים ד-ז. אבות דרבי נתן, נוסחא א Avot de-Rabbi Natan I אבות דרבי נתן, נוסחא ב Avot de-Rabbi Natan II אבות דרבי נתן Avot de-Rabbi Natan אבות דרבי נתן, נוסחא א Avot de-Rabbi Natan I אבות דרבי נתן, נוסחא ב Avot de-Rabbi Natan II אבות דרבי נתן, נוסחא ב Avot de-Rabbi Natan II אבות דרבי נתן Avot de-Rabbi Natan אבות דרבי נתן Avot de-Rabbi Natan אבות דרבי נתן, נוסחא ב Avot de-Rabbi Natan II
- **Demotion stratum:** `other_demotion`
- **Why this demotion is being checked:** Demotion stratum `other_demotion`: a genuine token-name-match (catalogue, FGP, or bib known_bib) demotes this claim -- included for comparison against the two oversampled over-broad categories above.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `demotion_correct` -- confirm or correct to `false_known` if the demoting source does not actually name this specific work.**
- **Demotion verdict:** _(pending Task 3 -- `demotion_correct` / `false_known` / `unsure` / `skip`)_

#### Case 91

- **Manuscript:** Ms. Evr. Antonin B 249 (sys_id `990000555770205171`)
- **Claimed work:** מסכת אבות דרבי נתן, נוסח ב (w000789)
- **Catalogue's own identification text:** אבות דרבי נתן נוסח ב : ; כולל קטעים מפרק מג, רובו של פרק מו ותחילת פרק מז. אבות דרבי נתן, נוסחא ב Avot de-Rabbi Natan II אבות דרבי נתן, נוסחא ב Avot de-Rabbi Natan II אבות דרבי נתן, נוסחא ב Avot de-Rabbi Natan II אבות דרבי נתן, נוסחא ב Avot de-Rabbi Natan II אבות דרבי נתן, נוסחא ב Avot de-Rabbi Natan II אבות דרבי נתן Avot de-Rabbi Natan אבות דרבי נתן Avot de-Rabbi Natan אבות דרבי נתן Avot de-Rabbi Natan אבות דרבי נתן, נוסחא ב Avot de-Rabbi Natan II
- **Demotion stratum:** `other_demotion`
- **Why this demotion is being checked:** Demotion stratum `other_demotion`: a genuine token-name-match (catalogue, FGP, or bib known_bib) demotes this claim -- included for comparison against the two oversampled over-broad categories above.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `demotion_correct` -- confirm or correct to `false_known` if the demoting source does not actually name this specific work.**
- **Demotion verdict:** _(pending Task 3 -- `demotion_correct` / `false_known` / `unsure` / `skip`)_

#### Case 92

- **Manuscript:** Ms. Evr. Antonin B 902 (sys_id `990000555800205171`)
- **Claimed work:** הלכות ראו (w000797)
- **Catalogue's own identification text:** הלכות ראו (אבל-מועד) : ; הוא התרגום העברי של הלכות פסוקות. הלכות פסוקות Halakhot Pesuqot הלכות פסוקות Halakhot Pesuqot
- **Demotion stratum:** `other_demotion`
- **Why this demotion is being checked:** Demotion stratum `other_demotion`: a genuine token-name-match (catalogue, FGP, or bib known_bib) demotes this claim -- included for comparison against the two oversampled over-broad categories above.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `demotion_correct` -- confirm or correct to `false_known` if the demoting source does not actually name this specific work.**
- **Demotion verdict:** _(pending Task 3 -- `demotion_correct` / `false_known` / `unsure` / `skip`)_

#### Case 93

- **Manuscript:** Ms. Evr. Antonin B 276 (sys_id `990000555830205171`)
- **Claimed work:** הלכות גדולות (w001196)
- **Catalogue's own identification text:** הלכות גדולות (קטע). הלכות גדולות Halakhot Gedolot הלכות גדולות Halakhot Gedolot הלכות גדולות Halakhot Gedolot הלכות גדולות Halakhot Gedolot
- **Demotion stratum:** `other_demotion`
- **Why this demotion is being checked:** Demotion stratum `other_demotion`: a genuine token-name-match (catalogue, FGP, or bib known_bib) demotes this claim -- included for comparison against the two oversampled over-broad categories above.
- **PROPOSAL (draft, not a label; owner ruling J -- see 136-GATE1-DECISIONS.md § J): plausibly `demotion_correct` -- confirm or correct to `false_known` if the demoting source does not actually name this specific work.**
- **Demotion verdict:** _(pending Task 3 -- `demotion_correct` / `false_known` / `unsure` / `skip`)_

## Part D -- NO-SOURCE-TEXT cases (Arm 3, owner ruling J -- NO VERDICT REQUIRED)

**Question type: none -- informational only.** None of the four checked-source families has ANY text at all for these manuscripts, so they ship as novelty candidates automatically, with no source to check them against. This section exists ONLY so the owner can eyeball whether that bypass looks safe -- there is nothing to confirm or correct, and no verdict is collected here.

#### Case 94

- **Manuscript:** Cambridge University Library Ms. T-S Misc. 34.20 (sys_id `990051124460205171`)
- **Claimed work:** רס"ג, שמות תרגום (תפסיר תורה) (w000033)
- **Why no verdict is collected:** None of the four checked-source families (bib, PGP, FGP, catalogue) has ANY text at all for this manuscript -- this row ships as a novelty candidate automatically, with no source to check it against.

#### Case 95

- **Manuscript:** Cambridge University Library Ms. T-S Misc. 34.23 (sys_id `990051124490205171`)
- **Claimed work:** משנה תורה, ספר אהבה (w000176)
- **Why no verdict is collected:** None of the four checked-source families (bib, PGP, FGP, catalogue) has ANY text at all for this manuscript -- this row ships as a novelty candidate automatically, with no source to check it against.

#### Case 96

- **Manuscript:** Cambridge University Library Ms. T-S Misc. 34.29 (sys_id `990051124550205171`)
- **Claimed work:** תנ"ך, בראשית (w000086)
- **Why no verdict is collected:** None of the four checked-source families (bib, PGP, FGP, catalogue) has ANY text at all for this manuscript -- this row ships as a novelty candidate automatically, with no source to check it against.

#### Case 97

- **Manuscript:** Cambridge University Library Ms. T-S K 27.26 (sys_id `990051220440205171`)
- **Claimed work:** הלכות גדולות (w001196)
- **Why no verdict is collected:** None of the four checked-source families (bib, PGP, FGP, catalogue) has ANY text at all for this manuscript -- this row ships as a novelty candidate automatically, with no source to check it against.

#### Case 98

- **Manuscript:** Cambridge University Library Ms. T-S K 27.28 (sys_id `990051220460205171`)
- **Claimed work:** מדרש אגור (w000836)
- **Why no verdict is collected:** None of the four checked-source families (bib, PGP, FGP, catalogue) has ANY text at all for this manuscript -- this row ships as a novelty candidate automatically, with no source to check it against.

#### Case 99

- **Manuscript:** Cambridge University Library Ms. T-S K 27.39 (sys_id `990051220580205171`)
- **Claimed work:** סדר אליהו זוטא א-טו (w000162)
- **Why no verdict is collected:** None of the four checked-source families (bib, PGP, FGP, catalogue) has ANY text at all for this manuscript -- this row ships as a novelty candidate automatically, with no source to check it against.

#### Case 100

- **Manuscript:** Cambridge University Library Ms. T-S K 27.41 (sys_id `990051220600205171`)
- **Claimed work:** תנ"ך, ויקרא (w000088)
- **Why no verdict is collected:** None of the four checked-source families (bib, PGP, FGP, catalogue) has ANY text at all for this manuscript -- this row ships as a novelty candidate automatically, with no source to check it against.

#### Case 101

- **Manuscript:** Cambridge University Library Ms. T-S K 27.42 (sys_id `990051220610205171`)
- **Claimed work:** דברי הימים של משה רבנו (w000944)
- **Why no verdict is collected:** None of the four checked-source families (bib, PGP, FGP, catalogue) has ANY text at all for this manuscript -- this row ships as a novelty candidate automatically, with no source to check it against.

