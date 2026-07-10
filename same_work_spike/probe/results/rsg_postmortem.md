# RSG postmortem — why a "Saadia Tafsir" cluster went unidentified

Generated 2026-07-10. Read-only diagnostic (SEED-029). Substrate: `fullcorpus.db`
(`track1_matches` with `shadowed_by` gate; `passage_units_accepted_pairs_canonmask`
residue clusters), `ref_corpus.pkl` (5,363 works). Query harness = the exact
`track1_match.build_ref_index` + `frag1_truncation.query_batch` mechanics
(K=5, band=20, min_anchors=2, one-sided accept_density 0.28/0.35, wide cutoff 0.55).
New analysis scripts only: `rsg_explore.py`, `rsg_members.py`, `rsg_query_test.py`,
`rsg_mechanism.py`. No pipeline script or DB was modified.

---

## Verdict (one line)

**None of the four hypotheses holds. The cluster is not Saadia's Tafsir at all.**
The RSG-dominated residue cluster (**unit 1430332**) is an anonymous Judeo-Arabic
**biblical GLOSSARY of difficult words** ("תפסיר / שרח אלאלפאט אלצעבה", the modal
genizah-title on 50 of its 97 witnesses) — a *different work* that is **genuinely
absent from the reference corpus**. The "תפסיר רס״ג" label is **manuscript-level
cataloger-title contamination** on a minority of composite witnesses (7/97), exactly
the `LIMITATION` `residue_naming.md` warns about. The work we *do* have — RSG's
continuous-prose Tafsir — is a different text, and it correctly did not match.

Map-v2 fix: **REF-1 reference expansion (add the glossary + the Karaite JA Bible
commentaries), NOT a tier-B threshold, NOT RSG re-verification, NOT an
interleaving-aware pass.** RSG itself is not the gap.

---

## Why this is airtight: the positive control

Before trusting any negative result, I confirmed the RSG reference + harness actually
work. I pulled 15 pages that **live Track-1 already labeled RSG** (`best_density ≤ 0.15`)
and re-queried them against an **RSG-only index** (the 27 JA works titled רס״ג/סעדיה,
**no masks**). **14/15 re-verify at density 0.13–0.27** (the one miss is RSG *Emunot
ve-Deot* re-hitting a sibling RSG segment at 0.36 — a within-RSG cross-book artifact,
not a failure). The RSG Tafsir/Targum of Genesis, Exodus, Numbers, Deuteronomy,
Isaiah, Psalms, Daniel and the megillot are all present and matchable. (Gap noted for
completeness: RSG **Leviticus translation, J:38, is absent** — the only Torah-translation
book missing.)

**So RSG is recoverable when it is actually present. The cluster failures below are
therefore real properties of the cluster text, not of the reference or the harness.**

---

## The four hypotheses, tested with numbers (unit 1430332, 30 sampled pages)

Two indexes were built: the **FULL** production index (5,363 works, 27,739 segs,
34.5M postings, 142,250 DF-capped codes, 574,324 grams canon-masked; 11/27 RSG works
carry canon masks) and an **RSG-ONLY** index (27 works, 990 segs, 3.27M postings,
**only 1,772 DF-capped codes** — the DF cap is effectively inert, and there is zero
cross-work competition).

| test | measured | verdict |
|---|---|---|
| pages with ANY RSG candidate (dens ≤ 0.55) vs **RSG-only** index | **0 / 30** | — |
| pages with ANY RSG candidate vs **FULL** index | **0 / 30** | — |
| pages whose best FULL-ref match would production-ACCEPT | **0 / 30** | — |
| best-overall FULL-ref match distribution | **{} (empty)** | matches *nothing* |
| member pages present in **live** `track1_matches` | **1 / 628 (0%)** | Track-1-invisible |

**1. Recension distance — REJECTED.** This predicts a clean-vs-clean RSG density just
above the boundary (≈0.30–0.45). Actual: there is **no RSG candidate at all even at the
generous 0.55 wide cutoff**, with the DF cap and all competing works removed. The
positive control shows genuine RSG witnesses land at 0.13–0.27, so the acceptance
boundary is nowhere near the problem.

**2. Verse-by-verse interleaving — REJECTED as stated (partial mechanism, wrong target).**
The fragmentation intuition is directionally right — the text *is* maximally fragmented —
but the premise is wrong. It is not "RSG tafsir interleaved with the Hebrew verse"; it
is a glossary of a *different* work. De-interleaving it would still not produce RSG
matches. See mechanism below.

**3. DF-cap self-suppression — REJECTED.** The RSG-only index drops only 1,772 codes
(vs 142,250 in the full index) and removes all competition; still **0/30**. And the
positive-control RSG pages verify fine against the FULL index too. RSG's own grams are
not being suppressed — there is simply no RSG text on these pages to suppress.

**4. Script / orthography mismatch — REJECTED.** This would show *moderate* density
(~0.4–0.5), not nothing. The positive control proves JA orthography matches at 0.13–0.27.
There is no near-miss to explain.

---

## The actual mechanism (`rsg_mechanism.py`, n=80 pages of unit 1430332 vs FULL ref)

Failure-stage histogram: **`no_reference_covers_it` = 79/80, `density_fail` = 1.**

Per-page internals (mean page length 775 letters):

| page len | grams | ref gram-hits | best diagonal cluster | verified candidates |
|---|---|---|---|---|
| 1110 | 1106 | 27,088 | 14 | 0 |
| 806 | 802 | 17,649 | 15 | 0 |
| 629 | 625 | 14,601 | 11 | 0 |
| 1133 | 1129 | 29,626 | 9 | 0 |

The pages are long and gram-rich and produce **tens of thousands of chance 5-gram
collisions** against the huge reference, and they even form small diagonal clusters
(9–15 anchors) — **but zero reference spans verify at 0.55.** This is frag1's
`no_reference_covers_it` regime verbatim: "a diagonal cluster formed from chance
collisions but no reference span aligns → the page is not a copy of any reference text."
Per frag1's own conclusion, **this regime's only lever is REF-1 reference expansion,
never threshold loosening.**

**Why even the Bible (which the glossary quotes) doesn't match:** the glossary structure
is `‹Arabic gloss› מן ‹Hebrew lemma›` alternating every 2–5 letters, and the lemmata are
scattered *hapax* words picked from all over Scripture — so there is never a contiguous
≥30-letter (MIN_SPAN) run of either pure biblical Hebrew or pure Arabic to seed-and-extend.
Contiguous-span matching is structurally defeated by the glossary form.

---

## Concrete page-vs-reference excerpts

**(A) The cluster text — unit 1430332, `Ms. EVR ARAB I 1391` p32 (RNL), catalogued
"תפסיר אלאלפאט אלעצבה":** a glossary, `gloss מן lemma`, not running tafsir:
> …ין בעלי אצחאב מן נקרב בעלהבית . הרית . עחד מן זאת בריתי . נשבה סבי מן וישב ממנו שכי
> וירק וגרד מן והרק חנית וסגר חויריו מחנכיה מן חגוך לנער . ויראף וכלב מן וירדפו מצרים…

**(B) What RSG's Tafsir actually looks like (reference `J:40`, RSG Deuteronomy
translation):** continuous Judeo-Arabic prose, no `מן`-lemma structure:
> …יאלסמאחתי בני אלגבאבר הראינאהם תם מ פסוק פקלת לכם לא תראהבוהם ולא תכאפוהם פסוק אללה
> רבכם אלסאיר נורה בין ידיכם הו איחארב ענכם כגמיע מא צנע במצר בחצרתכם…

Excerpt (A) shares **zero** verifiable span with (B) — and with everything else in the
5,363-work reference. The two are different genres of different works.

**(C) Positive control — genuine RSG witness `990000862330205171` (labeled RSG Genesis
commentary):** re-queried vs RSG-only index → **best-RSG density 0.130** (accepts). This
is what a real RSG page scores; the cluster pages score nothing.

**(D) A companion cluster — unit 303006, `Or. 5562C.22` (BL), MARC-catalogued
"…תפסיר רס״ג ; Tafsir Saadya Gaon":** running Karaite Deuteronomy commentary prose
(blood-prohibition exegesis) —
> …אחללת גצבי פיה וקטעתה מן בין קומה כי לאן נפס אלבשריון אלדם … געלתה לכם עלי אלמדבח…
> לא יאכל דמא … ומן אלגריב אלדכיל פי מא בינהם…

Its best RSG density is 0.526 (FULL) / no RSG candidate (RSG-only); its only *accepts*
come from **embedded biblical quotations matching the Bible reference (0.289)**, not from
the commentary itself. It is Yefet ben Eli's tafsir (per the modal MARC title
"תרגום ופרוש התורה ליפת בן עלי"), not RSG.

---

## The companion "RSG-tagged" clusters confirm the same disease

Same query test on the other three residue clusters the task flagged (30 pages each):

| unit | true content (modal MARC title) | accepts | RSG-only best-dens (n verified) | % pages in live Track-1 |
|---|---|---|---|---|
| 303006 | Yefet Deut commentary (תרגום ופרוש ליפת בן עלי) | 6/30 (all via embedded Bible/Talmud quotes: Bible 0.289, Bavli Yoma 0.341, RaShBaCh 0.324) | 0.475–0.549 (5/30) | 14% |
| 1157648 | Yefet Psalms commentary (תרגום ופרוש כתובים, תהלים) | 0/30 | 0.454–0.533 (2/30) | 2% |
| 1038702 | Karaite Leviticus (מקרא + אונקלוס + commentary) | 12/30 (all via embedded Leviticus **verses** matching Bible, 0.12–0.46, 25× Bible-winner) | 0.481–0.560 (7/30) | 47% |

Every RSG best-density sits at **0.45–0.56**, far above the 0.35 boundary — these are not
RSG. Where pages *do* accept, it is because they **quote Scripture/Talmud** (which is in
the reference) inside a **running commentary that is not** (Yefet ben Eli / Yeshua ben
Yehuda's Judeo-Arabic Bible commentaries). unit 1038702's 47% live-Track-1 rate is the
purest illustration: the running Leviticus *verses* match `מקרא [Bible]`, but the host
Karaite commentary — the thing the 46 witnesses actually share — never gets identified.
(And note the reference is missing RSG's Leviticus *translation*, J:38, so even a genuine
RSG-Lev page could not have matched here — though the content is commentary, not RSG,
regardless.)

---

## Map-v2 implication

1. **RSG is not the gap — this is a false alarm against RSG.** The reference already
   covers RSG's running Tafsir (positive control 14/15). The Map-v2 census should *not*
   spend a tier-B threshold, an RSG re-verification, or an interleaving-aware motif pass
   trying to "recover RSG" from these clusters — RSG isn't in them.

2. **The real gap is REF-1 reference expansion for two absent work-families:**
   (a) the anonymous **Judeo-Arabic difficult-words biblical glossary** ("תפסיר/שרח
   אלאלפאט אלצעבה"; unit 1430332, 97 witnesses) and (b) the **Karaite JA Bible
   commentaries** — **Yefet ben Eli** (Deut/Psalms/Leviticus: units 303006, 1157648,
   1194766, 1038702) and Yeshua ben Yehuda. Adding these editions is what converts these
   clusters from residue to identified.

3. **The glossary sub-class additionally needs a lemma/motif-query approach, not
   contiguous-span matching.** Its `gloss מן lemma` structure yields `no_reference_covers_it`
   against *any* running reference (0 spans verify despite 14K–30K gram-hits/page); it can
   only be identified by matching its individual biblical lemmata as short motifs
   (the motif-query engine), never by seed-and-extend against prose.

4. **Naming caveat for the residue reports:** name residue clusters by the **modal
   genizah-title** (which here already said "glossary / תפסיר אלאלפאט"), and treat the
   near-universal MARC titles as noisy co-evidence. Lumping "תפסיר אלאלפאט" together with
   the minority "תפסיר רס״ג" MS-tag is what manufactured the illusion that a
   work-we-have went unidentified. This aligns with — and sharpens —
   `residue_naming.md`'s own conclusion that these clear/competing clusters are
   "genuine reference-gap works recoverable by ADDING their references."
