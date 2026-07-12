# Independent tier-B deck review — stratified content grading (agent, 2026-07-10)

**What was graded.** An INDEPENDENT, seeded-random (seed 20260710) stratified sample of the
tier-B candidate stream in `data/mapv2_smoke.db` (657,205 rows, liturgy subcorpus), pushed
through the EXACT guard pipeline of `scripts/build_smoke_preview2.py` (rarity>60 → Bible-span
coverage ≥0.70 → span-union margin/not_best → verse-content `partial_ratio_alignment` vs the
full Bible stream ≥70), re-scored with `p_calibration_final.json`, aggregated per
(manuscript, work) with per-work cap 3 per stratum — i.e., the same units the deck shows,
but drawn at random rather than curated. 60 surviving cards graded by reading both texts
(page snippet reconstructed from spans + edition passage via `RefText`), plus 19
guard-demoted rows audited for over-kill. Grading scratch artifacts in the session
scratchpad (`sample_tierb.py`, `cards_for_grading.md`, `cards_raw.json`); nothing committed.

Pipeline numbers reproduced independently: rarity demoted 251,273; bible-cover 219,643;
not_best 148,426; cheap-guard survivors 37,863 rows → 31,140 (ms, work) units. The
expensive verse-content guard was applied lazily to sampled units only (statistically
identical to the deck's full application); it demoted ~18% of sampled units on top.

## Per-stratum precision (my verdicts)

| Stratum (final P) | n | correct-work | plausible-same-work | wrong-work | verse/liturgy leak | insufficient |
|---|---|---|---|---|---|---|
| P ≥ 0.8   | 20 | **9** (6 strong + 3 statutory-trivial) | 2 | 0 | **9** | 0 |
| 0.5–0.8   | 15 | 1 (statutory-trivial) | 2 | 0 | **11** | 1 |
| 0.2–0.5   | 15 | 1 | 2 | 1 | **11** | 0 |
| < 0.2     | 10 | 0 | 2 | 0 | **8** | 0 |
| **Total** | 60 | 11 | 8 | 1 | **39 (65%)** | 1 |

"Statutory-trivial" = the claimed work IS a statutory prayer (סוף ובא לציון, ברכת השינה,
ברכת השיר, ברכות נישואין) and the page indeed quotes it — technically correct, near-zero
discovery value. P ranking clearly carries signal (top band ≈ 55% correct+plausible vs
≈ 20% below), but the dominant failure mode — verse/liturgy-chain leakage — reaches all
the way to P = 1.00.

## Strongest positive examples (real discovery potential)

1. **CARD 14 — יהודה הלוי, ונשקפת בעד סתרי סתרים; 2 strict witnesses; P=0.833** — page
   `990001435100205171_IE49219947_P000022`: a damaged page correctly matched to the poem
   over 161 letters ("ונמשלה כאילה שלוחה... וחרדה על בנה ואקראה לה... ופנתה להלוך ויהי
   בחפזה"). Few-witness secular poetry identified from noisy HTR — exactly the deck's
   purpose.
2. **CARD 50 — יוסף אבן צדיק, עמוד כמעט צבי צבאות צבאים; 2 witnesses; P=0.433 (LOW
   stratum!)** — page `990053577910205171_IE150532999_P000002`: an adab/poetics anthology
   quoting two lines of the poem **with explicit attribution** ("לך אמר בנו צדיק בשירו...
   למחלת עינך נפשות ולא תשאיל..."). A genuine find sitting in the 0.2–0.5 band — evidence
   that even low strata are worth a scholar's skim once the leaks are gone.
3. **CARD 6 — יהודה אבן גיאת, ידידיך מאמש; 8 witnesses; P=0.90** — distinctive strophes
   match verbatim ("העתר נא ורצה לקרוא לעמוסיך... כי כבר רצה אלהים את מעשיך").
4. **CARD 4 / CARD 11 — יניי קדושתות ליו"כ (וכל מאמינים, 274 letters, P=0.966) and סליחת
   אנשי אמנה אבדו (P=0.996)** — long distinctive piyyut matches; the final model prices
   them correctly at the very top.
5. **CARD 10 — תרגום אונקלוס בראשית, interleaved Bible+Targum page, P=0.95** — the
   interleaved-Targum class works: the Targum side is correctly claimed even where the
   Hebrew lines belong to מקרא.

Also good: CARD 1 (פתיחה לאמירת י"ג מידות — matched through the liturgical framing, not
just the embedded verse), CARD 9 (Mishneh Torah sheniyot list quoted at length inside
another halachic work — a real reuse edge, see problem P6).

## Problem classes found (named)

**P1 — Multi-verse chain leak (THE dominant problem, 39/60 survivors incl. 9/20 at
P≥0.8).** Liturgy pages that are verse anthologies (selichot pesukim, tachanun, festival
verse chains, pesukei-dezimra sequences) match ANY ref work that quotes one of those
verses. The verse-content guard tests the matched slice against the *contiguous* Bible
stream; a chain of verses from different books/chapters — or one verse chain abbreviated
with וג' — aligns at only 52–69 and survives the ≥70 cutoff. Examples:
- page `990001457190205171_..._P000008` → **מרדכי זאב פיירברג, "לאן?" at P=1.00**
  (m_ge_010, 2 "strict witnesses"): the page is a Ps 44 selichot chain; Feierberg's novella
  quotes the same chain. Bible-align score 53.8 — sails under the guard.
- page `990001443130205171_..._P000014` → ספרי דברים at P=0.949: Micah 7:18–20 quoted with
  וג' skips (score 62.4).
- page `990001448220205171_..._P000015` → יניי at P=0.949 via Ps 148:14+147:20 (score 67.6);
  page `990001441670205171_..._P000050` → תשובות ראב"ם via Jer 32+Deut 30+Dan 9 (score 69.0
  — misses the cutoff by 1.0).
In my sample the 60–70 score band is ~100% leak for non-Targum works (25/60 cards sit
there), but a plain threshold drop to 60 would also kill correct Targum cards (Onkelos at
60.8) and still miss low-score chains (Feierberg 53.8). The fix must be chain-aware:
e.g., window the matched slice (~40-letter windows, each aligned against the Bible stream)
and demote when most windows are verse-covered — the direct-alignment analogue of the
existing span-coverage guard.

**P2 — Statutory-prayer quotation leak (verse guard is blind to it — not Bible).** Works
matched through a statutory blessing/prayer formula: shehecheyanu → ברכות ההודאה (P=0.902)
on a JA halachic treatise; אהבה רבה phrases → רס"ג בקשות (P=0.866); כל חמירא → **חיי אדם**
(P=0.65); generic kaddish formula → קדיש לכבוד גאון (P=0.801). Needs a statutory-formula
mask analogous to the Bible one (the liturgy pass's own tier-A prayer works could supply
the streams).

**P3 — Canonical-translation leak (Targum/tafsir of verses is unmasked).** The guards test
only the HEBREW Bible stream, so verse content in Aramaic or Arabic garb leaks: trilingual
Decalogue page → JA polemic המעשה בפולמוס הכומר (P=0.833, matched on the standard tafsir
wording); Zech 14 Bible+Targum page → מדרש חסרות ויתרות (score 67.5 — targum interleaving
dilutes the alignment); יי ימלוך verse+targum liturgy → קניין תורה. Note DEM 17 shows
Aramaic *Daniel* IS caught (it is in the Bible stream) — the gap is specifically
targum/tafsir renderings.

**P4 — Post-classical works reach high P + tier-A contamination signal.** Feierberg
(P=1.00, witA=2), Mendele ×2 works (witA=1 each), חיי אדם (witA=5), ספר הברית 1797
(witA=4, saved only by the verse guard at 79.1), and in the demoted pool: **רנ"ק מורה
נבוכי הזמן with 70 strict tier-A witnesses** (rarity-gated by a margin of 10!) and טור
או"ח with 429. The same verse-chain mechanism is evidently inflating the STRICT tier for
verse-quoting late works — worth a separate tier-A audit, and a cheap era/date sanity flag
on deck cards.

**P5 — Sibling-work confusion inside canonical families.** Page with verbatim ONKELOS
(Gen 8:7–8 "ושלח ית יונה מלותיה למחזי אם קלו מיא") labeled תרגום פסאודו-יונתן (P=0.45,
singleton — the true sibling never competed on that page); Qillar yotzrot cycle matched via
the cycle's recurring formula ("ביאר לציר... וידבר יי אל משה לאמר") to the wrong volume
(במדבר claimed, page is מצורע). Mid-P, so the model prices the doubt roughly right.

**P6 — Quotation-direction ambiguity.** The page IS the quoted text, the claim is the
quoting work (or vice versa): Shiur-Komah page claimed as the Sherira+Hai responsum ABOUT
Shiur Komah (P=0.7, witA=0); halachic compendium quoting MT's sheniyot list claimed as MT
itself. Real reuse edges, but "witness of work X" is the wrong caption — deck could label
these "quotes/quoted-by?" when the matched span is a known embedded quotation.

**P7 — Epistolary formula leak (P1/P2 subspecies).** Letters matched through verse-built
greeting/exordium conventions: אגרות שמואל בן עלי via "ואתם חזקו... / חזקו ויאמץ לבבכם"
closings (P=0.6); the Gaza community letter via its praise-verse exordium (P=0.499).

**Minor — calibration quantization.** Final-model P values are coarse knots (all 10 D-stratum
units share P=0.191; nothing at all falls in 0.2–0.4: unit histogram 0.0:34, 0.1:2202,
0.4:2687, 0.5:5623, 0.6:6100, 0.7:2791, 0.8:4006, 0.9:7697). The deck's 0.2–0.5 stratum is
effectively 0.4–0.5; strata boundaries should follow the knot structure.

## Over-kill audit (guard-demoted sample, n=19)

**No over-kill found.** All demotions checked were correct kills:
- rarity ×4 (מקרא 4523w, עמידה 216w, טור או"ח 429w, רנ"ק 70w) — all agglomerate/leak rows;
- bible-cover ×4 (all coverage 1.00; pages were psalm sequences / Malachi Bible text /
  verse-quotes inside piyyut) — in each the page↔work overlap was exactly the verse;
- not_best ×3 — the beating competitor was genuinely better (Bible at dens 0.19 vs 0.54;
  Bavli פסחים over ר"ח פסחים on a havdala formula);
- verse-content ×8 including the whole borderline band 70.1–79.1 — every one a true verse
  match; the guard even saved a would-be P=0.953 top-stratum card (בבלי סנהדרין claimed on
  a והוא-רחום liturgy page via Daniel 3:33). The borderline band is clean on the demote
  side; the problem is exclusively UNDER-kill below 70 (see P1).

## Bottom line

The machinery is sound — the four guards fire correctly with zero observed over-kill, the
final model prices long distinctive piyyut matches at the top, and the deck already
surfaces genuine discovery-grade identifications (2-witness Halevi and Ibn Tzaddik poems)
— but the tier-B deck as sampled is **not yet worth scholar review time below the top
band, and even the P≥0.8 stratum is ~45% verse/liturgy-chain noise**. One leak family
accounts for nearly all of it: verse/liturgy CHAINS (multi-book chains, וג'-abbreviated
quotes, targum/tafsir renderings, statutory formulas) that the single contiguous-window
Bible-alignment test cannot see, and it reaches P=1.00 (Feierberg). Priority fixes, in
leverage order: (1) chain-aware verse guard (windowed alignment coverage instead of one
partial_ratio window); (2) extend canonical masking to Targum/tafsir streams; (3) a
statutory-prayer formula mask + demote-or-flag works that ARE statutory prayers; (4) a
tier-A verse-leak audit for post-classical works (the strict census currently credits רנ"ק
with 70 witnesses and the טור with 429 inside the liturgy subcorpus). With (1)–(3) in
place, the observed correct+plausible core (≈30% overall, ≈55% at P≥0.8, plus real finds
even at P=0.43) suggests the re-filtered deck would clear the bar comfortably.

## v4 re-grade (2026-07-10 — batch canonical-rendering guard)

**What was checked.** (1) Re-graded all 25 P≥0.8 cards in `review/mapv2_smoke_preview_v4.html`
(same verdict labels). (2) Re-ran the EXACT v4 guard (`query_batch` over the
Bible+Targum+Liturgy+tafsir guard reference, `GUARD_QUERY_CUTOFF=0.45`, `GUARD_COVER_MIN=0.55`)
on all 68 of my previously graded slices (60 sample survivors + 8 old-verse-guard demotions),
read-only, with per-case coverage and gram-level diagnostics. Scratch scripts:
`v4_recheck.py`, `anchor_cover.py`, `trimmed_hull.py` (session scratchpad).

### 1. v4 P≥0.8 deck re-grade — precision DROPPED vs v3

| verdict | n / 25 | cards |
|---|---|---|
| correct-work | 5 | V4-4 (Rashi on Emor, 379 letters), V4-5 (סדר פסוקים למנחה ליו"כ — framing lines match, **1 witness, genuine find**), V4-6 (דרך ארץ זוטא), V4-7 (מעריב א' פסח), V4-9 (צידוק הדין) |
| correct-trivial (statutory) | 1 | V4-8 (פתיחה לפסוקי דזמרה = ברוך שאמר litany) |
| plausible-same-work | 2 | V4-2 (Ramban quoted at length inside a supercommentary — NLI: פרוש סודות התורה לרמב"ן), V4-11 (הבדלה verse-composition, sequence matches) |
| **verse/liturgy-chain leak** | **17** | V4-1, 3, 10, 12–25 minus the above |

**32% correct+plausible at P=1.00 — worse than the v3 top band (55%).** The v4 pipeline
REPLACED the old whole-slice partial_ratio≥70 guard instead of augmenting it, so contiguous
verse quotes the old guard used to catch are back in the deck: V4-1 is **בבלי ערכין claimed
on an Ezekiel Bible page** (Ezek 39:29–40:1 — the exact class Hillel flagged in preview v1)
and V4-10 is בבלי סנהדרין on a Karaite prayer page via 1 Kings 21:13. The top band is
dominated by RNL Karaite-siddur verse-anthology pages (EVR mss): 9 of the 17 leak cards are
duplicate slots of just 4 leak-pattern works (בקשה ×3 — all matched on the same
הושיעה-את-עמך + ברוך-יי-לעולם doxology pair; משיבת נפש ×2 via the Isa 40/46 תדמיוני chain;
מדרש משלי ×2 via Ex 4:11; בקשה לאחר ברכות השחר ×2 via Ps 143:11-12). Tier-A witness-count
inflation is visible on the cards themselves (the Palermo letter with "18 strict witnesses",
Ibn Aknin 35, משיבת נפש 47 — all epistolary/doxology verse chains).

### 2. Flagship leaks re-checked — NOT demoted

Direct guard re-run on my flagged cards (max per-guard-work summed hull letters / slice):

| card | leak type | slice | guard coverage | demoted? |
|---|---|---|---|---|
| S19 Feierberg "לאן?" (P=1.00) | Ps 44 verse chain | 145 | **0.00** | no |
| S22 חיי אדם (כל חמירא) | statutory formula | 106 | **0.00** | no |
| S05 ברכות ההודאה (שהחיינו) | statutory blessing | 77 | **0.00** | no |
| S12 רס"ג בקשות (אהבה רבה) | statutory blessing | 135 | **0.00** | no |
| S52 מדרש חסרות ויתרות | Zech+Targum interleave | 83 | **0.00** | no |
| S16 polemic (tafsir Decalogue) | tafsir | 91 | 1.11 (dens exactly 0.45) | **yes** |

Across all graded cases: the guard catches **1/39 of my sample leaks** and 3/8 of the old
guard's demotions (the three with ≥~70 contiguous clean verse letters) — i.e. v4 is a
regression even on the 8 cards v3 had already removed. Its only other hit is an
**over-kill**: S10, the CORRECT Onkelos-Genesis identification, self-matches the guard's
own Onkelos unit (coverage 1.09) and is demoted. The aggregate 3,273/37,863 (8.6%) drop is
therefore genuine under-kill, not a sampling artifact on my side: the graded leak share
implies ~50–65% of cheap-guard survivors (≈19–25K rows) should be leaving the deck.

### 3. Why the guard misses — diagnosed, with numbers

**Cause 1 (dominant): the ±MARGIN=30 hull padding poisons the density check.** Clusters DO
form on the right guard works (flagships: best_cluster 5–44 anchors; raw gram hits 512–3,771
per slice), but every hull is verified over the anchor extent padded ±30 on BOTH sides
(p0=minp-30, p1=maxp+K+30, same on the ref side). For a canonical quote of contiguous length
run, the two mismatched flanks alone drive density ≈ 60/(run+65): run 30 → 0.63, 40 → 0.57,
50 → 0.52, 60 → 0.48, **only run ≥ ~68 clean letters passes 0.45** — higher still under HTR
noise. Verse-chain pieces and statutory formulas are 15–50 letters each, so every hull fails
verification and coverage is exactly 0.00. This also explains WHICH cases it did catch: the
three old-DEM cards with long contiguous runs (scores 72.7–79.1). `GUARD_COVER_MIN=0.55` is
NOT the issue (missed leaks measure 0.00, not 0.45–0.54); `MIN_SPAN=30` is not binding
(padded spans are ≥65 by construction).

**Cause 2: guard-reference gaps.** The 13 Liturgy units are 5×עמידה, 3×ברכות שמע, 2×קידוש,
הלל, ברכת המזון, הגדה. Probed for the formulas behind my statutory leaks: **כל חמירא —
absent from all 60 guard works** (exists in the full ref only inside M:Ytext507000); kaddish
(יתגדל) absent; המפיל absent; שהחיינו present only via the Haggadah unit (spelling שהחינו);
ברכות נישואין absent. No threshold can catch what the reference doesn't contain.

**Cause 3 — ruled out: DF-cap.** Only 1,830 gram codes are DF-dropped (>128 postings) from
the 3.24M-posting guard index; flagship slices retain 65–90% of their grams with ≥1 posting
(e.g. Feierberg 123/141). Anchor recall is fine; verification is what fails.

**Over-kill mechanism: self-match.** Rows whose CLAIMED work is itself in the guard set
(Targum cards) are demoted by their own edition text (S10 Onkelos, cover 1.09). Needs a
claimed-work exemption analogous to the existing Bible exemption — unless demoting
statutory/Targum claims is an explicit deck policy.

### 4. Tested fix — trimmed hulls (numbers on the same graded cases)

Dropping the ±30 padding (verify each cluster over its anchor extent only, window ≥18,
dens ≤0.35, coverage = per-guard-work **union** of verified query intervals):

| config | catches (of 47 graded leaks incl. old-DEM) | kills (of 19 correct+plausible) |
|---|---|---|
| shipped v4 guard | 4 (9%) | 1 (the correct Onkelos card) |
| trimmed, thr 0.45 | **32 (68%)** — incl. Feierberg at 0.94, all 8 old-DEM | 2 (S10 Onkelos self-match; S20 statutory-trivial ברכת השיר) |

Raw anchor-cluster coverage WITHOUT verification does not work (chance 3–4-anchor clusters
give ~0.9 coverage to everything, correct piyyutim included — tested, rejected). The
value-bearing correct cards (YHL, Ibn Ghiyyat, Yannai, Ibn Tzaddik, the 13-middot peticha)
all measure 0.00–0.33 under trimmed hulls. Residual 15 misses are (a) nusach-variant
statutory text whose anchor runs shatter below 18 letters (אהבה רבה vs the guard's version),
(b) וג'-abbreviated chains of <18-letter pieces, (c) formulas absent from the guard
reference (Cause 2). Restoring the old whole-slice partial_ratio guard ALONGSIDE trimmed
hulls (threshold ~60 for works not claimed as Targum/tafsir) would add ~9 of the 15 by their
already-measured v3 scores (60.6–67.5), for an estimated ~87% leak catch at ~zero
value-bearing kills.

### 5. Bottom line (v4)

The v4 top band is **not scholar-ready and is a step backwards from v3** (32% vs 55%
correct+plausible at P≥0.8): the batch guard's margin-padded density verification
structurally cannot see verse chains — the exact failure it was built to close — and the old
guard it replaced was the only thing catching contiguous quotes. The architecture (batch
query of every slice through the production engine against a canonical guard reference) is
right; the fix is small and measured: **verify hulls over the trimmed anchor extent (no ±30
padding, window ≥18, dens ≤0.35), take per-work UNION coverage, keep 0.45–0.55 as the demote
threshold, exempt rows whose claimed work is in the guard set, add the missing statutory
units (כל חמירא, kaddish, המפיל, sheva berachot, uva-letzion closing), and keep the old
partial_ratio test as a second, complementary demoter.** On my graded ground truth that
combination removes ~87% of leaks while preserving every genuine discovery card.

## v5 (2026-07-10 — trimmed-hull guard implemented, deck rebuilt, re-graded)

**Implementation.** `scripts/build_smoke_preview2.py` now carries guard v5 per the approved
recipe: new `query_batch_trimmed()` (same K=5/band=20/min_anchors=2 vectorized pipeline as
`frag1_truncation.query_batch`, but hulls verified over the TRIMMED anchor extent — no ±30
padding), constants `GUARD_HULL_WMIN=18`, `GUARD_HULL_DMAX=0.35`, `GUARD_COVER_MIN=0.45`
with per-work UNION coverage via `merge_iv`; rows whose claimed work is in the guard set are
exempt (plus the existing Bible exemption); stage 2 restores the whole-slice
`partial_ratio_alignment` vs the Bible stream at `BIBLE_ALIGN_MIN=60` for non-Bible/Targum
claims. Cheap guards untouched; DB opened read-only; OUT → `mapv2_smoke_preview_v5.html`;
Hebrew note rewritten to describe v5 honestly incl. the known residuals. Constants carry the
measured basis in comments. Build log: `results/overnight/preview_v5.log`.

**Build numbers** (657,205 tier-B rows): cheap guards → 37,863 survivors (identical to v4);
trimmed canonical-rendering guard dropped **19,405 (51.3%** — vs v4's 3,273/8.6%, right in
the predicted 50–65% band); Bible-align≥60 dropped a further 3,048 → 15,410 rows → **13,933
(ms, work) units** (was 30,626 in v4). Final P-histogram: 0.9: 3,723 · 0.8: 2,207 · 0.7:
1,229 · 0.6: 2,473 · 0.5: 2,239 · 0.4: 1,308 · 0.1: 726 · 0.0: 28.

**Ground-truth verification (68 graded cases, module's own code path):** leaks caught
**40/47 (85%)**, matching the ~87% estimate. Every flagship demoted: Feierberg/Ps-44 (cov
0.94), shehecheyanu (0.55), tafsir Decalogue (0.47), Zech+Targum (0.46), plus all 8
old-verse-guard cases (0.58–0.99). The correct Onkelos card is now EXEMPT (claimed work in
guard set) — over-kill fixed. All discovery/value cards survive: 13-middot peticha (0.33),
Yannai (0.00), Ibn Ghiyyat (0.00), Halevi ונשקפת (0.00), Ibn Tzaddik (0.00), אנשי אמנה
(0.00). Kills: 2/19 correct+plausible — S20 ברכת השיר (statutory-trivial, via trimmed) and
S29 יוצר נחמו (plausible, align60=62; the Isa-40:2-refrain case — accepted cost). The 7
surviving leaks are the documented residuals: כל חמירא + אהבה רבה (statutory units
missing/variant in guard refs) and 5 heavily HTR-garbled short chains.

### New P≥0.8 top-25 re-grade

| verdict | n / 25 | notes |
|---|---|---|
| correct-work | **9** | Rashi (379 letters), סדר פסוקים למנחה ליו"כ (witA=1), דרך ארץ זוטא, מעריב א' פסח, צידוק הדין, **נחום אלברדאני פזמונים לז' פסח (witA=1)**, **קדושה נוסח מעורב בבלי/א"י (witA=1)**, תפילה לאחר סדר עבודה ("אשרי עין ראתה"), **פסקת תפילה בקדושתות ימ"נ (witA=1)** |
| correct-trivial (statutory) | 6 | ברוך שאמר, ברכות ההפטרה, הוספות עשי"ת, ברכת מגילה, הבדלה בעמידה, עננו — pages genuinely contain the claimed statutory unit |
| plausible-same-work | 3 | Ramban via supercommentary; Bavli Eruvin + Rif Shabbat on the same פסקי רי"ד ms (quotation-family ambiguity) |
| wrong-work (NEW class: rabbinic-quotation leak) | 2 | רשב"ח מבוא התלמוד claimed on a Bavli-Pesachim-quoting page (shared memra מברכין על האור); מדרש אגור claimed on a JA Sanhedrin treatise (shared Sanhedrin 7b derasha) |
| verse/liturgy leak | 5 | the בקשה doxology pair ×3 (hoshia+baruch-YY, HTR-garbled below anchor recovery), Palermo letter (tzaddikim psalm chain), ספרי דברים (Micah chain, garbled) |

**Top-band correct+plausible: 72%** (v3: 55% → v4: 32% → v5: **72%**), and the correct set
now includes FOUR single-witness genuine finds. Statutory-trivial cards are 24% of the band
— technically correct identifications of prayers; a deck-policy question (keep/segregate),
not a guard bug.

### Remaining work surfaced by the v5 re-grade

1. **Rabbinic-quotation leak (new named class):** works matched through a shared TALMUDIC
   dictum reach P=1.00 (2/25). The guard reference covers Bible/Targum/tafsir/liturgy but
   not Bavli/Mishnah; adding them as guard works (claimed-work exemption already handles
   legitimate Bavli claims) would close it with the same mechanism.
2. **HTR-garbled short chains** (5/25): verse pieces of 20–45 letters with 3+ HTR errors
   shatter 5-gram anchors below WMIN and stay under align-60 — the בקשה doxology pattern
   alone still burns 3 top-band slots (and its "22 strict witnesses" are the same
   contamination). Options: per-work leak-pattern review, lower-k anchors for the guard, or
   the planned statutory-unit additions (Ps 28:9+89:53 as a closing-doxology unit).
3. **Guard-reference statutory gaps** (unchanged plan): כל חמירא, kaddish, המפיל, ברכות
   נישואין, plus now the closing-doxology unit above.

**Bottom line (v5):** the approved recipe works as measured — the deck's top band flipped
from 2/3 noise (v4) to ~3/4 signal, every previously-flagged flagship leak is gone, and
zero discovery-grade cards were lost. With Bavli/Mishnah added to the guard reference and
the short list of statutory units filled in, the P≥0.8 band should be ready for scholar
review time; below 0.8, sample-grade before spending scholar attention.

## v6 verification (2026-07-10 — Bavli/Mishnah/Yerushalmi/Tosefta added to the guard reference)

**What v6 changed** (per the deck note): the ONLY change vs v5 is that the canonical-rendering
guard reference now also contains Mishnah/Bavli/Yerushalmi/Tosefta (+ JA tafsir) — so shared
rabbinic-dictum evidence is recognized as canonical rendering and demoted, while claims whose
CLAIMED work is itself canonical (a genuine Bavli fragment claimed as Bavli) stay exempt.
Deck: `review/mapv2_smoke_preview_v6.html` (68 cards; P≥0.8=25, 0.5–0.8, 0.2–0.5, <0.2).
DB `data/mapv2_smoke.db` opened read-only for the audit; `data/fullcorpus_v2.db` untouched.

### (a) The two v5 rabbinic-leak cards — BOTH DEAD ✅

Neither work appears as a card anywhere in the 68-card v6 deck; the strings `רשב"ח מבוא
התלמוד` / `מדרש אגור` occur ONLY in the deck's own note text (describing what was removed).
The underlying data confirms this is the guard acting, not missing data:

| work | candidate rows | max P (pre-guard) | in strict tier (track1_matches) | cards in v6 deck |
|---|---|---|---|---|
| רשב"ח, מבוא התלמוד | 30 | **0.9559** | yes (1) | **0** |
| מדרש ״אגור״ | 1,812 | **0.9559** | yes (1) | **0** |

Both carried top-band P in `track1_candidates` (0.9559 → would render "P 1.00"), yet the guard
removed the entire class from the deck. The new named class from the v5 re-grade (rabbinic-
quotation leak) is closed.

### (b) Over-kill check — ZERO over-kill ✅

**No card graded correct or plausible in v5 disappeared from v6.** All 18 v5 correct+plausible
top-band cards are present in the v6 top-25 (matched by work + shelfmark): the 9 correct-work
(Rashi 379L, סדר פסוקים למנחה witA=1, דרך ארץ זוטא, מעריב א פסח, צידוק הדין, נחום אלברדאני
witA=1, קדושה נוסח מעורב witA=1, אשרי עין ראתה, פסקת תפילה בקדושתות witA=1), the 6
correct-trivial (ברוך שאמר, ברכות ההפטרה, הוספות עשי"ת, ברכת מגילה, הבדלה, עננו), and the 3
plausible (Ramban supercommentary, Bavli Eruvin, Rif Shabbat). Critically, **every witA=1
single-witness genuine find survived** — the new Mishnah/Bavli guard did not collaterally kill
any piyyut find (the real over-kill risk of adding rabbinic streams).

Cards that LEFT the v6 top-25 vs v5, all justified (not over-kill):
- רשב"ח מבוא התלמוד, מדרש אגור — the two rabbinic leaks, demoted by the new guard (intended).
- The Palermo letter (v5 verse/liturgy leak, tzaddikim psalm chain) dropped out of the top band
  — a leak removal / stratum drop, not a loss of value.

The 3 freed slots were filled top-down by 3 cards moving up from below, all legitimate: וידוי
(statutory confession, witA=40), יוסף בן אביתור — שבעתות לשבתות מיוחדות (distinctive attributed
piyyut, 197L, correct), ברכת השינה / המפיל (statutory, 243L). This is the expected "sections
fill top-down" movement, not new noise.

### (c) v6 P≥0.8 top-25 re-grade

| # | shelfmark | claimed work | letters / witA | verdict | one-line reason |
|---|---|---|---|---|---|
| 1 | HAS Ms. 108 | רמב״ן פירוש לתורה בראשית | 180 / 43 | plausible | NLI = פרוש סודות התורה לרמב"ן (a Ramban supercommentary); match runs past the ב"ר idiom into Ramban's own linking phrase + "והמשכיל יבין" signature |
| 2 | LON BL Or. 2597 | רש״י פירוש לתורה | 379 / 7 | correct | 379-letter contiguous Rashi (Behar/Emor); flanks continue in Rashi prose |
| 3 | HAS Ms. 135 | סדר פסוקים למנחה ליו״כ | 95 / 1 | correct | piyyut framing lines ("יום קבעת חוק עולם / שוממותינו ראה") match, not just embedded verses; single witness |
| 4 | CUL T-S Or. 1080 1.49 | מסכת דרך ארץ זוטא | 107 / 9 | correct | distinctive "הנושא אשה לשום…" list continues across the passage |
| 5 | CUL T-S Misc. 33/03 | מעריב א פסח | 125 / 3 | correct | Pesach maariv piyyut ("ליל שמורים… יבא פקוד יפקוד") matches across strophes |
| 6 | EVR II A 58 | פתיחה לפסוקי דזמרה | 121 / 40 | correct-trivial | ברוך שאמר litany; page genuinely contains it (statutory) |
| 7 | EVR II A 201/01 | צידוק הדין | 116 / 10 | correct | "הצור תמים בכל פועל / מי יאמר לו מה תפעל" continues past the Deut 32:4 verse into the piyyut |
| 8 | EVR II A 819 | בקשה | 93 / 22 | **leak** | matched span = Ps 28:9 (הושיעה את עמך) + Ps 89:53 (ברוך ה' לעולם) doxology; flanks are JA rubric |
| 9 | EVR II A 856 | בקשה | 93 / 22 | **leak** | same doxology chain (הודו + הושיעה + ברוך ה' לעולם); Karaite siddur |
| 10 | EVR II A 1138 | נחום אלברדאני — פזמונים לז׳ פסח | 114 / 1 | correct | rhymed pizmon ("נחית כצאן… מוליך לימין משה זרוע תפארתו") across strophes; single witness |
| 11 | EVR II A 1163 | ספרי דברים | 99 / 25 | **leak** | Micah 7:18–20 verse chain; page is a Karaite siddur, flanks are other verses |
| 12 | EVR II A 1429 | בקשה | 94 / 22 | **leak** | same הושיעה+ברוך-ה'-לעולם doxology chain |
| 13 | EVR II A 2903 | ברכות ההפטרה | 98 / 46 | correct-trivial | statutory haftarah blessings; page contains them |
| 14 | EVR IV 4 (p161) | תלמוד בבלי, עירובין | 102 / 11 | plausible | ms = פסקי רי"ד (Talmud commentary); page reproduces Bavli Eruvin's mishnah; claimed work = Bavli (exempt) so defensible witness |
| 15 | EVR IV 4 (p111) | הלכות הרי״ף (שבת) | 107 / 44 | plausible | same Piskei-Rid ms; core match = Mishnah Shabbat 19:5 (קטן נימול) shared by Rif; quotation-family ambiguity (see residual note) |
| 16 | EVR ARAB II 1666 | תפילת עמידה, הוספות לעשי״ת | 118 / 14 | correct-trivial | Ten-Days Amida insertions (ובספר חיים…); statutory |
| 17 | Ms. Mittwoch 2 | קדושה, נוסח מעורב (בבלי וא"י) | 95 / 1 | correct | distinctive kedushah composition ("אני הייתי לראשנים ואני אהיה לאחרונים"); single witness |
| 18 | CUL T-S A 42.1 | ברכה לאחר קריאת המגילה | 110 / 4 | correct-trivial | Megillah blessing (הרב את ריבנו); statutory |
| 19 | CUL T-S H 2.86 | הבדלה בתפילה משבת ליו״ט | 180 / 52 | correct-trivial | festival-havdala Amida insertion; statutory |
| 20 | CUL T-S H 4.19 | תפילה לתענית ציבור | 91 / 11 | correct-trivial | עננו (Aneinu) fast-day prayer; statutory |
| 21 | CUL T-S H 5A.1 | תפילה לאחר סדר עבודה ליו״כ | 191 / 16 | correct | "אשרי עין ראתה… עונות אבותינו החריבו נוך" — specific post-Avodah composition, 191L |
| 22 | CUL T-S H 6.98 | פסקת תפילה בקדושתות ימ״נ | 91 / 1 | correct | distinctive "כי מקדישיך בקדושתך קדשתה נאה לקדוש פאר מקדושים"; single witness |
| 23 | CUL T-S H 8.13 | וידוי | 93 / 40 | correct-trivial | Yom Kippur confession (אשמנו בגדנו… צדיקים אנחנו ולא חטאנו); statutory. NEW to top band |
| 24 | CUL T-S H 12.2 | יוסף בן אביתור — שבעתות | 197 / 4 | correct | attributed shivata ("ובהקבץ אצילי עמים… על מקרא מגלה במרץ"), 197L, w/ rubric. NEW to top band |
| 25 | CUL T-S Ar. 36.59 | ברכת השינה | 243 / 19 | correct-trivial | המפיל bedtime blessing at length; statutory. NEW to top band |

**Tally:** correct-work **10**, correct-trivial (statutory) **8**, plausible **3**, verse/liturgy
leak **4**, rabbinic leak **0**.

### (d) Precision v5 vs v6

| version | correct-work | correct-trivial | plausible | leaks | **correct+plausible** |
|---|---|---|---|---|---|
| v3 (P≥0.8) | — | — | — | — | 55% |
| v4 (P=1.00) | — | — | — | — | 32% |
| v5 (P≥0.8) | 9 | 6 | 3 | 7 (2 rabbinic + 5 verse) | **72%** (18/25) |
| **v6 (P≥0.8)** | **10** | **8** | **3** | **4 (0 rabbinic + 4 verse)** | **84%** (21/25) |

**Top-band precision rose 72% → 84%**, and the leak count fell 7 → 4. The entire improvement is
the intended one: the 2 rabbinic-quotation leaks are gone (guard) and a third verse leak (Palermo)
dropped out, with the freed slots taken by correct/statutory cards. Zero discovery-grade or
plausible cards were lost. (Strict alternative: if the two Piskei-Rid Talmud-quotation cards
#14/#15 are counted as shared-rabbinic leaks rather than plausible witnesses, v6 = 76% (19/25) /
6 leaks — still well above v5's 72% and above v5's own count under the identical rubric, since
v5 also graded them plausible.)

Note the statutory-trivial share grew 24% (v5, 6 cards) → 32% (v6, 8 cards): as leaks were
removed and statutory piyyutim/prayers filled up from below, the top band is now ~1/3 technically-
correct-but-near-zero-discovery-value prayer identifications. This is a deck-policy question
(keep / segregate / label), not a guard bug.

### New problem-class scan (all four sections)

**No NEW junk class in the P≥0.8 band.** The 4 remaining top-band leaks are all the previously-
documented verse/doxology-chain residual (the בקשה הושיעה+ברוך-ה'-לעולם doxology pair ×3 and the
ספרי-דברים Micah chain) — HTR-garbled short verse chains under the anchor-recovery floor, exactly
the residual v5 already flagged (the planned closing-doxology unit Ps 28:9+89:53 would close 3 of
the 4). Below the top band, only known classes recur (verse/derasha anthologies — האיי גאון
פתרון תורה ×4; post-classical verse-quoters — ספר הברית 1797 at P 0.2–0.5).

Two observations worth logging (both extensions of already-named classes, both BELOW the top band
or neutral, neither a regression):

1. **Aggadic-midrash sharing (extension of the rabbinic-quotation class the guard just closed).**
   v6 added the *halakhic* rabbinic corpus (Mishnah/Bavli/Yerushalmi/Tosefta) to the guard, but
   NOT the *aggadic* midrashim (Bereshit Rabba, Tanhuma, Mekhilta, פתרון תורה). So midrash-to-
   midrash matches via a shared derasha still leak: מדרש הבאור claimed on a תנחומא page via the
   "הרבה סייחים מתו…" derasha (P 0.80), בראשית רבה on a לקוטי-מדרשים page (P 0.2–0.5), and the
   recurring פתרון תורה. Same mechanism, aggadic side — but all sit at P≤0.80, none in the top
   band. Closing it means adding the major aggadic midrashim as guard works (claimed-work
   exemption already handles legitimate midrash claims).

2. **Talmud-commentary base-text redundancy (extension of P6 quotation-direction).** A single
   manuscript — EVR IV 4 = פסקי רי"ד (Isaiah di Trani's Talmud commentary) — generates a flood of
   high-P candidates against the base texts it reproduces: **566 Bavli + 165 Yerushalmi + 91
   Mishnah + 71 Tosefta + 72 Rif** candidate rows (all at P=0.9559). The claimed-work exemption
   correctly passes the Bavli/Yerushalmi/Mishnah/Tosefta ones (the page genuinely witnesses those
   texts), which is *right* — but it lets one commentary manuscript occupy multiple top-band slots
   (2 of the top-25 here: #14 Bavli-Eruvin, #15 Rif-Shabbat, same ms). Not a precision defect, but
   a dedup/caption question: these are "commentary-that-reproduces-base-text" edges and could be
   labeled as such (or capped per manuscript-across-works) so a single Talmud-commentary codex
   doesn't crowd out other discoveries.

**Bottom line (v6):** the v6 guard change did exactly what it set out to do and nothing it
shouldn't have — both rabbinic-quotation leaks are dead, top-band precision rose 72% → 84% (leaks
7 → 4), and zero correct/plausible/single-witness cards were over-killed. The remaining top-band
noise is the known HTR-garbled verse/doxology residual (≈4 slots, closable with the planned
statutory-doxology unit). The only residual worth new work is symmetric with the fix just shipped:
add the aggadic midrashim to the guard reference the same way the halakhic corpus was just added.

## v7 (production builder) verification (2026-07-10)

**What v7 changed** (per the builder + `review/smoke_deck/mapv2_deck_report.md`): (a) guard adds
13 well-attested rabbinic-genre works (attestation-based version of the v6 aggadic-midrash
recommendation); (b) 15 guard-only statutory units from Sefaria (kol chamira, 4 kaddish
recensions, hamapil, sheva berachot, birkot hashachar, havdalah, festival kiddush); (c)
(sys,work) pairs already in strict tier A excluded (3,012 dropped — the deck is now a pure
DISCOVERY deck), rarity cutoff data-derived q92=**41** (was 60), P from the recalibrated final2
model, range/cap display labels. Deck: `review/smoke_deck/mapv2_discovery_deck.html` — 88 cards
(P≥0.8: 40 · 0.5–0.8: 25 · 0.2–0.5: 15 · <0.2: 8) sampled from 11,751 kept rows / 10,874
(ms,work). Attribution below uses the builder's own per-row checkpoint
(`mapv2_deck_guard_ckpt.ndjson`: v=0 kept 11,751 · v=1 canonical-rendering 15,090 · v=2
verse-align 3,360 — exactly the report funnel) + `data/mapv2_smoke.db` read-only.

### 1. OVER-KILL check — the 21 v6 correct/plausible cards: ZERO collateral damage ✅

Only 1 of the 21 appears as a shown v7 card (עננו/תפילה לתענית ציבור, T-S H 4.19 — now top-band
card #2). But the deck is a small sample of the kept pool, so absence ≠ killed; per-row
attribution of all 21:

| fate | n | cards | judgment |
|---|---|---|---|
| kept (guard v=0), just not sampled into the deck | **14** | סדר פסוקים למנחה (witA=1), דרך ארץ זוטא, מעריב א פסח, פתיחה לפסוקי דזמרה, צידוק הדין, נחום אלברדאני (witA=1), הוספות עשי"ת, קדושה נוסח מעורב (witA=1), ברכת מגילה, עננו (shown), תפילה לאחר סדר עבודה, פסקת תפילה בקדושתות (witA=1), וידוי, ברכת השינה | not over-kill — all four witA=1 genuine finds survive in the kept pool |
| excluded as tier-A-known (sys,work) pair | **6** | רמב"ן (HAS 108), רש"י (BL Or. 2597), ברכות ההפטרה (EVR II A 2903), Piskei-Rid↔בבלי עירובין, Piskei-Rid↔רי"ף שבת, אבן אביתור שבעתות (T-S H 12.2) | JUSTIFIED by design — these identifications are already in the strict tier; a discovery deck should not re-show them |
| killed by rarity q92=41 | **1** | הבדלה בתפילה משבת ליו"ט (witA=52, in the new 42–60 kill band) | JUSTIFIED — statutory Amida insertion with 52 strict witnesses; v6 grade was correct-trivial; zero discovery value lost |

**Guard-stage kills among the 21: 0** (no v=1/v=2). In particular the 15 new statutory units did
NOT collaterally kill the genuinely-statutory v6 cards (ברכת השינה = hamapil text passed v=0 —
the guard units under-catch nusach-variant/HTR-garbled statutory text, which is bad for leak
coverage but means no piyyut was harmed). **No COLLATERAL DAMAGE to flag.**

### 2. v6 residual leaks (4 HTR-garbled verse/doxology cards) — 3 SURVIVE, 1 dead for the wrong reason

| v6 leak card | v7 fate |
|---|---|
| בקשה doxology, EVR II A 819 | guard v=0 — **alive** in kept pool (not shown in deck) |
| בקשה doxology, EVR II A 856 | guard v=0 — **alive** |
| בקשה doxology, EVR II A 1429 | excluded as tier-A-known pair — dead, but only because the leak already contaminates tier A |
| ספרי דברים Micah chain, EVR II A 1163 | guard v=0 — **alive**; a SIBLING card of the same leak pattern (ספרי דברים on EVR II A 2538, same witA=25) even SHOWS in the v7 deck at P 0.80 |

As predicted: the Ps 28:9+89:53 closing-doxology unit was NOT among the 15 statutory units added,
so this residual class persists untouched.

### 3. v7 top-band re-grade (P≥0.8, 40 cards)

Verdicts (card # = deck order): **correct-work 14** (#1 תחינה לשני וחמישי witA=1, #3 סילוקים
מקדושתא, #5 רמב"ם משנה תורה זרעים — 507 letters, discovery-grade, #13 רשות אוחילה לאל, #14 פסקות
קדושתאות ליו"כ witA=1, #17 תפילה לאחר שמו"ע witA=1, #20 תפילה לאחר סדר עבודה, #23 קליר הושענות,
#32 אלברדאני גופי יוצרות למועדים, #34 אבן אביתור קדושתא, #35 פיוט לשמחת תורה, #36 אלברדאני
יוצרות לשבתות מצוינות, #37 סליחת אנשי אמנה אבדו, #40 מעריב לשבועות); **correct-trivial
(statutory) 17** (#2 עננו, #7 ברוך שאמר, #8+#24 ירבו-שמחות litany ×2, #10+#15 וידוי ×2, #16
הימנון=אין כאלהינו, #18 זימון, #19+#26+#30 חתימות קדושת היום ×3, #27 תחנון, #28 יוצר לשבת, #29
מעין שלוש, #31 בקשה אחר העמידה, #33 היום-תאמצנו expansion, #39 השכיבנו); **plausible 2** (#6
רס"ג סדר עבודה — match is the shared Mishnah-Yoma core all Avodot embed; #22 הלכות ותשובות
מסידור — shared Bavli-Berakhot baraita, but page genre matches the claimed work); **leak 7**:

- #4 + #38 **בבלי כריתות** on siddur pages via פיטום הקטורת — the liturgical baraita recitation
  (flanks: אתה-הוא prayer / שיר של יום) claimed as a Bavli witness;
- #11 **בבלי ראש השנה** on a מדרש לקח טוב page (NLI-cataloged as such) via the embedded
  כי-משמש-בארבע-לשונות sugya — flanks are Lekach-Tov commentary;
- #12 + #21 + #25 **תרגום יונתן ישעיהו/יחזקאל** on siddur pages via קדושה דסדרא (ובא לציון's
  targum verses) — flanks are the uva-letzion prayer frame;
- #9 **תרגום פסאודו-יונתן על ויקרא** on a page whose text is verbatim ONKELOS Lev 23 (NLI
  catalog: "Targum Onqelos: Leviticus 23") — the correct (sys, אונקלוס-ויקרא) pair is tier-A-known
  and was EXCLUDED, letting the wrong sibling surface at P 0.99 (**known-sibling shadow**).

**Top-band precision: 33/40 = 82.5%** (correct 35% + statutory-trivial 42.5% + plausible 5%).

| deck | top-band correct+plausible | leaks | note |
|---|---|---|---|
| v5 | 72% (18/25) | 7 | includes tier-A-known cards |
| v6 | 84% (21/25) | 4 | includes tier-A-known cards |
| **v7** | **82.5% (33/40)** | **7** | **discovery-only band** (all known pairs removed) |

Precision is effectively FLAT vs v6 on a strictly harder band (v6's top band included the
easy known-pair cards — Rashi, Ramban, Piskei-Rid — that v7 deliberately excludes). But
discovery-value density fell: statutory-trivial is now 42.5% of the band (v6: 32%), and 6 of
the 14 correct cards are near-statutory piyyutim.

### 4. Display elements — spot-checks (5+ cards)

- **Flank chips:** two variants render — red `השוליים שונים — חשד ציטוט (score)` (69 cards) and
  gray `קצה קטע — אין הקשר לבדיקה` (19 cards). Spot-checks match the evidence: #5 (MT Zeraim,
  קצה — the match spans the entire visible fragment, no flank exists), #2 (עננו — flanks are JA
  halakhic prose, correctly flagged as quotation context), #11 (Lekach Tov — flanks are midrash
  commentary; the chip correctly signals exactly what makes it a leak), #37 (אנשי אמנה — flags
  the selichot-service frame; honest limitation: liturgical-anthology containment ≠ citation, and
  the deck note correctly says "ראיה בלבד, לא מסנן"). **Finding: the positive state `ההקשר
  ממשיך` appears 0/88 times** — every card in the deck is either "flanks differ" or "fragment
  edge". Either the continuation test is unreachable under HTR noise or the positive branch never
  fires; as shipped the chip only distinguishes "has flanks" from "no flanks", so its red state
  should not be read as damning (it fires on correct cards too).
- **P labels:** `P 0.99` ×40 (display cap honest — no more "P 1.00"), `P 0.80` ×25 = the
  singleton band (every card carries `התאמה בודדת`, 25/25 — the [80, 0.799] singleton cap;
  minor quibble: reads as a point estimate rather than a cap), `P 0.2–0.5` ×15 (range label,
  sensible), `P 0.18` ×8. All consistent with the section headers.
- **Provenance chips:** `· עוד N עמודים בכתב־היד הזה` on 9 cards (checked #3, #23, #35 — matches
  multi-page support in candidates), source tags [Maagarim]/[Bavli]/[Targum]/[Sefaria]/[JA] and
  NLI catalog titles render correctly. Sensible throughout.

### New problem class (named): canonical-claim exemption leak

6 of the 7 top-band leaks enter through the **claimed-work exemption**: when the CLAIMED work is
itself canonical (Bavli, Targum), the canonical-rendering guard exempts the row — correct for
genuine Talmud/Targum fragments, but it also waves through liturgy/commentary pages that merely
QUOTE the canonical text (פיטום הקטורת in the siddur, קדושה דסדרא targum verses, a midrash
quoting a sugya). The 7th leak (#9) is the complementary **known-sibling shadow**: excluding a
tier-A-known pair lets a nearly-identical sibling work (PsJ vs Onkelos) surface as a fake "new"
discovery on the same span. Fixes, in leverage order: (1) add ובא לציון/קדושה-דסדרא and
פיטום-הקטורת+שיר-של-יום as statutory guard units AND stop exempting canonical claims from
STATUTORY-unit coverage (exempt them only from their own family's stream); (2) when dropping a
tier-A-known pair, also drop/flag same-family siblings over the same page span (the span-shadowing
mechanism already exists in SEED-029); (3) the still-missing closing-doxology unit (Ps 28:9+89:53)
— the v6 residual survives exactly as predicted; (4) consider wiring the flank chip's strong-red
scores into a demoter for canonical-claim cards specifically (its three top-band firings at
0.78–0.82 are precisely the three Bavli leaks).

**Bottom line (v7):** the production builder is safe on the over-kill axis — zero guard collateral
damage, all four single-witness v6 finds alive, the tier-A exclusion and rarity-41 kills are all
justified — and the top band holds ~82% precision on a discovery-only population, with genuinely
new finds surfacing (MT Zeraim 507L, five witA≤2 piyyut identifications). The cost centers are
(a) the canonical-claim exemption, now the dominant leak channel (6/7), (b) the untouched
doxology/verse-chain residual, and (c) discovery-value dilution: 42.5% of the band is statutory-
trivial prayer identifications — the keep/segregate/label policy question is now the biggest
lever on scholar-time ROI.

## FULL-corpus deck grading (2026-07-10 — 667K pages, v8 recipe)

**What was graded.** `review/full_deck/mapv2_discovery_deck.html` (88 cards: 40 P≥0.8 / 25 / 15 /
8) built from `data/fullcorpus_v2.db` (364,900 pages with candidates; tier A live 87,547
(ms,work); rarity q92=45; funnel per `mapv2_deck_report.md`: 84,131 cheap-guard survivors →
guard_canon_citation 9,551 + canonical 32,325 + verse 8,612 dropped → **43,194 kept / 36,262
(ms,work)**; known_tierA_pair 7,018 excluded; **known_sibling_vgroup: 0**). All 40 top-band cards
read in full and graded by the flank/boundary/citation methodology; mechanics verified against
the DB (read-only) and the builder's checkpoint. The full corpus introduces content classes the
liturgy smoke never had: rabbinic anthologies, Bible commentaries, lexica, the geonic
Talmud-digest family, JA treatises — and the top band's composition flipped accordingly.

### Top-band re-grade (P≥0.8, 40 cards; # = deck order)

**correct-work 10:**
[06] טעמי המקרא תרגום on a JA dikduk ms (374L, example-sequence + connecting prose continue; NLI
"חבור בדקדוק עברי" consistent) · [10] רבנו חננאל ב"מ — NLI itself catalogs the ms as פירוש רבנו
חננאל (282L) · [11] דמות הכיסא וההיפודרום של שלמה — 1069-letter FGP-transcript match to the rare
Solomon midrash, witA=3, **flagship discovery card** · [15] סילוקים מקדושתא (carried from smoke)
· [17] קדיש נוסח ותפילה לשלום גאון — 544L of the rare expanded Aramaic kaddish, witA=2,
discovery-grade · [25] פרק שירה — the page IS Perek Shira (animal-by-animal מה-הוא-אומר chain
continues) · [31] פסקות תפילה בקדושתאות (witA=1) · [32] תפילה לאחר שמו"ע (witA=1) · [34] טעמי
המקרא תרגום on NLI "הדאיה אלקאר" — the Arabic Hidayat al-Qari original matched to the treatise
(+7 more pages in ms) · [38] ברכה לפתיחת התפילה (rare asher-bachar-beDavid opening, witA=3).

**correct-trivial (statutory) 4:** [22] שיר הודיה ושבח (ירבו litany), [33] ברכת מגילה, [37]
ותמלוך/קדושת השם expansion, [40] תחנון.

**plausible 4:** [01] ר"ח פסחים claimed on the ערוך — the Arukh famously transmits RH verbatim,
so the matched text likely IS RH embedded in a lexicon (real reuse edge, wrong "witness" caption)
· [16] רס"ג סדר עבודה (shared Mishnah-Yoma Avodah core, carried from smoke) · [21] PsJ Exodus —
a genuine Palestinian-targum Decalogue page (עמי בני ישראל refrains, 685L; NO tier-A Onkelos on
this ms — family-correct, exact sibling within the Palestinian-targum family uncertain) · [35]
טוביה בן משה אוצר נחמד (Karaite Lev commentary; lemma chain follows Lev 2 order — genre-plausible).

**leak 22**, in three named classes:

1. **Rabbinic/medieval cross-quotation (17)** — the dominant full-corpus failure, three shapes:
   - *Anthology/commentary/list quoting the claimed work* (9): [04] אבדר"נ claimed on מנורת
     המאור ("ותו גרסי' בפרק חלק" — explicit citation formula on the page!) · [12] קטעי מדרש
     בראשית on an Ibn-Ezra commentary page ("אמרו כבר אמר...") · [14] מדרש תהלים on a **printed**
     Bereshit Rabba page (NLI: "בראשית רבה עם מתנות כהונה. דפוס") · [24] קהלת זוטא on מעלות
     המדות ("שכך אמרו חכמ' ז"ל") · [26] מדרש שמואל on ר' בחיי · [27] כוזרי תרגום אבן תיבון on
     דרשות ("וכמו שדרשו בסנהדרין") · [29] מדרש תהלים on מנורת המאור — the page says **"וגרסי'
     במד' תהלים"**, a NAMED citation of the claimed work · [39] קהלת רבה on a ויקרא רבה ms (the
     shared טוב-מלא-כף-נחת petichta; NLI: ויקרא רבה) · [02] קטעי מסורה on הלכות ספר תורה (the
     shared nekudot list; dist 0.52, margin 0.01).
   - *Geonic Talmud-digest family confusion* (6): works that all excerpt the Bavli verbatim
     mistaken for each other — [03] הלכות קצובות on a הלכות גדולות ms (chotam-betoch-chotam
     sugya, margin 0.02) · [05] רשב"ח ספר הבגרות on BHG (margin 0.00) · [18] ר"ח מועד קטן on a
     RIF ms (NLI: הלכות הרי"ף; match = the shared 97-letter baraita only) · [20] תשובות האיי on
     ספר והזהיר · [28] הלכות קצובות on תורת האדם (shared birkat-avelim) · [30] רשב"ח ספר הגירושין
     on a Hebrew rishonic page ("כדמוכח התם פרק התקבל").
   - *Inverted direction — the page IS the canon* (2): [09] הלכות גאונים לשבת claimed on an
     actual Bavli-Shabbat ms (NLI: תלמוד בבלי שבת יח-מז), [13] ספרי במדבר on a Bavli ms ("הכי
     גרסינן..." gemara-analysis flanks). The Bavli claims on those pages are tier-A-known
     (excluded), and HTR garble kept the guard coverage under threshold.
2. **Onkelos→PsJ known-sibling shadow (4):** [07] PsJ-Deut on EVR II C 714 (page = Onkelos: ארי
   + singular forms; **31 tier-A Onkelos-Deut pages on this very ms**), [08] PsJ-Lev on Halper
   40-41 (page: יזדריק…סחור סחור = Onkelos vs PsJ's חזור חזור; 4 tier-A Onkelos-Lev), [23]
   PsJ-Lev on L-G Misc. 96 (the smoke case, unchanged), [36] PsJ-Lev on EVR II C 712 (8 tier-A
   Onkelos-Lev). Mechanically confirmed: in all 4 the correct Onkelos pair is tier-A-known →
   excluded → the wrong sibling fills the "discovery" slot at P 0.99. `known_sibling_vgroup: 0`
   — the vgroup gate caught nothing (cross-work siblings not covered, as suspected).
3. **Statutory prayer missing from the guard units (1):** [19] מעשה מרכבה claimed on a Musaf-RH
   siddur via **עלינו לשבח / על כן נקוה + malkhuyot verses** — Aleinu is not among the 15 units.

### Precision vs the smoke lineage

| deck | corpus | top-band correct+plausible | leaks |
|---|---|---|---|
| v6 | liturgy smoke | 84% (21/25) | 4 |
| v7 | liturgy smoke, discovery-only | 82.5% (33/40) | 7 |
| **v8 FULL** | **full 667K, discovery-only** | **45% (18/40)** | **22** |

The collapse is entirely the new content classes: on liturgy-genre cards the full deck performs
like the smoke (all 8 liturgy carryover cards graded the same), but rabbinic/halakhic/JA pages —
where every work quotes the Bavli and the midrash/digest families quote each other — now fill
28/40 top-band slots and leak at ~70%.

### Known-residual status check

- **Closing-doxology unit: PARTIAL.** 2 of the 4 smoke residual rows (EVR II A 819/856 בקשה) are
  still kept (ckpt v=0) — HTR garble keeps them under the anchor floor; the other 2 died upstream
  (tier-A/cheap gates). None sampled into the 88 shown cards.
- **Canonical-claim exemption (pitum-haketoret / uva-letzion / Lekach-Tov class): CLOSED.** The
  page-coverage ≥0.45 canon-citation gate (9,551 dropped) removed the entire v7 class — zero such
  cards in the v8 top band. The gate works.
- **Known-sibling Onkelos/PsJ: NOT closed** (4 top-band cards = 10% of the band; vgroup=0).
- **HTR-garbled short chains: persists** (doxology rows kept; inverted-Bavli cards [09]/[13]
  survive the same way).

### Lower-strata scan (P 0.5–0.8 especially)

Same class-1 quotation leaks dominate (שאילתות↔פסיקתא/כלה רבתי, ספר מישרים↔תשובות האיי, מורה
נבוכים↔חטר בן שלמה, מקאצד אלפלאספה↔ספר האמתיות, ספר השרשים↔ר"ח ביצה, אבן השהם↔מדרש הנגיד via
the bare sefirot name-list — formulaic-list matching). Gross junk found: **[48] אברהם דב
גוטלובר, זיכרונות מימי נעוריי at P 0.80** on a Karaite miscellany — a 19th-century Haskala
memoir matched via a quoted rhymed epigram (YaShaR of Candia on Ramban) that both texts cite;
**שלשלת הקבלה (16th c.) at P 0.5** — the P4 post-classical-anachronism class is back (no
era-sanity flag). Real finds are present too ([74] משה אבן עזרא poem on a ספר הענק ms — NLI
title matches!; [79] תרגום יונתן שופטים on a נביאים ms; [59] הושענא). Display note: the deck's
final routing section "נוסחי קבע — מופרדים מרובד התגליות" renders **"(אין כרטיסים)"** while 4
statutory cards sit in the discovery top band — the statutory routing didn't fire.

### Verdict: NOT fit to hand to the scholar as-is

45% top-band precision under a "כמעט־ודאי / P 0.99" label would erode trust in one sitting —
especially because most leaks are obvious to the expert within seconds (several carry explicit
citation formulas, and nearly every leak card's NLI catalog title names a DIFFERENT work, while
nearly every correct card's NLI title is consistent or neutral). The fixes are cheap and mostly
mechanical, in leverage order:

1. **Citation-formula lexical demoter/flag** — page-side flank or match-opening containing
   גרסינן / כדתניא / אמרו חכמים / כדמוכח / כמו שדרשו / ואיתא ב־ + tractate names ⇒ demote or
   flag. Kills ~9/22 top-band leaks including the named-citation card [29].
2. **Canonical-family sibling gate** — if (sys, sibling-work-same-book) is tier-A-known, exclude
   or flag the same-family claim (Onkelos↔PsJ↔Yerushalmi-targum; extendable to the geonic-digest
   family). Kills 4; the data needed is already in track1_matches.
3. **NLI-catalog-mismatch flag** on every card (display + rank): the single strongest cheap
   triage signal in this grading.
4. **Widen the rabbinic guard reference** — 13 attestation-selected works is far too small a
   net for the midrash/geonic quotation graph; or gate symmetrically on coverage vs ANY ref work
   with tier-A presence on the same page.
5. **Era-sanity flag** (post-classical claimed works, the v3 P4 recommendation — still absent).
6. **Corpus hygiene:** exclude printed-material pages (NLI "דפוס", card [14]); fix the statutory
   routing (section is empty while statutory cards sit in the discovery band).

**If it must ship tomorrow, the cover note must include:** (a) measured top-band precision ~45%
— the P chip is NOT calibrated for rabbinic-literature pages; (b) how to spot the two dominant
leak shapes in seconds (citation formulas on the page; NLI title naming a different work);
(c) Targum cards labeled פסאודו-יונתן may actually be Onkelos (4 known cases, sibling-shadow);
(d) geonic-digest identifications (הלכות קצובות/גדולות, ר"ח, רי"ף, רשב"ח) are family-level at
best when the match is a bare Bavli excerpt; (e) the genuinely-new flagship cards worth starting
with: the Solomon-throne midrash (T-S C 2.198, 1069L), the expanded gaon-kaddish (T-S NS 309.86,
witA=2), Hidayat al-Qari (EVR ARAB I 2390 + 3944), Perek Shira (EVR II A 390.1), ר"ח בבא מציעא
(T-S NS 311.35), and the witA=1 piyyut finds carried from the smoke deck.

## v9 re-grade (2026-07-10 — full deck rebuilt with grading-derived fixes)

**What v9 added** (same path, rebuilt; funnel per `mapv2_deck_report.md`): `guard_cite_formula`
67 (citation-formula lexicon in the 38 chars BEFORE the span, non-canonical claims),
`known_sibling_targum` 49 (same-book Targum tier-A pairs), `guard_modern_era` 5,211 (361 works
dated ≥1500), Aleinu guard unit, `ייתכן חיבור מקביל קרוב` warning chip on small-margin cards
(16 cards), empty sections hidden. Kept 43,194 → **40,741** rows / 34,055 (ms,work).

### 1. Fate of the 22 v8 top-band leaks: 5 killed, 17 survive

**Killed (5) — all by the two targeted gates, all justified:**
- the 4 Onkelos→PsJ sibling shadows (PsJ-Deut EVR II C 714, PsJ-Lev Halper 40-41, PsJ-Lev L-G
  Misc. 96, PsJ-Lev EVR II C 712) → `known_sibling_targum` ✅
- מעשה מרכבה via Aleinu (Adler 2014.2) → the new Aleinu guard unit ✅
- (Mid/low band: Gottlober memoirs and שלשלת הקבלה are gone too → `guard_modern_era` ✅)

**Survive (17) — the ENTIRE rabbinic/medieval cross-quotation class, unchanged, still at
P 0.99:** all 9 anthology/commentary cards (אבדר"נ on מנורת המאור, קטעי מדרש on אבן עזרא, מדרש
תהלים on printed ב"ר, קהלת זוטא on מעלות המדות, מדרש שמואל on בחיי, כוזרי on דרשות, מדרש תהלים
on מנורת המאור incl. the NAMED "וגרסי' במד' תהלים" citation, קהלת רבה on ויקרא רבה, קטעי מסורה
on הלכות ספר תורה), all 6 geonic-digest confusions (הלכות קצובות ×2, רשב"ח ×2, ר"ח מ"ק on
הרי"ף, תשובות האיי on והזהיר), both inverted-canon cards (הלכות גאונים + ספרי במדבר on actual
Bavli mss). **Why the cite-formula gate missed them:** in the real cards the formula sits INSIDE
the matched span or at its first characters (e.g. the ספר-הגירושין match literally BEGINS
"כד מוכח התם פר' התקבל"; "וגרסי' במד' תהלים" and "ותו גרסי' בפרק חלק" are inside the highlight)
— the 38-chars-before window looks in the wrong place, hence only 67 kills corpus-wide.

### 2. The 5 replacement cards in the freed slots

| v9 # | card | grade |
|---|---|---|
| [36] | ברכת יוצר לשחרית לשבת, T-S NS 157.87 | correct-trivial (smoke-graded statutory yotzer) |
| [37] | יוסף אלברדאני, גופי יוצרות למועדים, T-S NS 199.95 | correct (smoke-graded) |
| [38] | יוסף בן אביתור, קדושתא משחרית ליו"כ, T-S NS 238.6 | correct (smoke-graded) |
| [39] | חתימה לקדושת היום לרגלים, Or. 1080 3.52 (FGP) | correct-trivial (festival chatima + JA kiddush rubric) |
| [40] | אסתר רבה ז-י on MS heb. c.18/20 | **leak** — page is ויקרא רבה מצורע (NLI confirms; context "זאת תהיה תורת המצורע"), claimed EstR via the shared Job-20:6 derasha (אם יעלה לשמים שיאו, applied to Haman in EstR). The surviving aggadic-sibling class self-replenishes |

### 3. v9 top-band precision

correct **12** + correct-trivial **6** + plausible **4** = **22/40 = 55%**; leaks **18**
(17 survivors + 1 new). Lineage: v6 84% → v7 82.5% (liturgy smoke) → v8 full 45% → **v9 full
55%**. All remaining leaks are ONE family (rabbinic/medieval cross-quotation, incl. its
aggadic-sibling and geonic-digest sub-shapes).

### 4. Over-kill check: ZERO ✅

All 10 v8 correct cards, all 4 correct-trivial, all 4 plausible are present in the v9 top band.
All 5 flagships present: Solomon-throne midrash [09], gaon-kaddish [15], Hidayat al-Qari both
mss ([30] EVR ARAB I 2390 + [06] 3944), Perek Shira [21], ר"ח בבא מציעא [08]. The modern-era
kills checked by name (Gottlober, שלשלת הקבלה) — both justified. **No cite-formula false
positive observed** among any graded card; one residual risk to log (not observed, structural):
genuine witnesses of Talmud-commentary works (ר"ח etc.) legitimately carry גרסינן right before
lemmata — the gate should exempt commentary-genre claimed works, and the ckpt should record
per-gate attribution so the 67 kills can be audited (currently they can't be enumerated).

The new warning chip lands well: of its 6 top-band placements, 5 are on leak/family-ambiguous
cards (margins 0.00–0.02) and 1 on the Solomon-throne flagship (margin 0.00 vs sibling versions
of the same midrash — honest). Empty statutory section now hidden; statutory cards (6/40) still
sit in the discovery band (routing still inactive). The printed-ב"ר card [12] remains.

### 5. Verdict: SHIP with cover-note caveats (one optional iteration would pay for itself)

Grounds: zero over-kill across two full iterations, all flagships intact, precision improved
45% → 55%, and the remaining noise is a SINGLE homogeneous family with a reliable two-second
tell. A further cheap pass (NLI-mismatch flag + moving the cite-formula window to cover the span
head + widening the rabbinic guard) would likely reach ~75–80% and is worth doing if the scholar
review can slip a day; otherwise ship with the following cover note.

**Cover note (exact caveats):**
1. **Measured precision:** top band (P≥0.8) = 55% correct/plausible on this full-corpus build;
   liturgy/piyyut cards run far higher (82–84% measured on the liturgy subcorpus); the P chip is
   NOT calibrated for rabbinic-literature pages — treat "P 0.99" as "top decile", not "99%".
2. **The one remaining leak family (18/40): works claimed via a shared rabbinic/medieval
   quotation.** Two-second tells: (a) the קטלוג NLI line names a DIFFERENT work (ערוך, מנורת
   המאור, בחיי, תורת האדם, ויקרא רבה, הרי"ף, "תלמוד בבלי", "דפוס") — present on ~16 of the 18;
   (b) citation formulas at/inside the highlight (גרסינן, כדמוכח התם, כמו שדרשו, אמרו חכמים,
   ותו גרסי').
3. **Geonic-digest warning:** identifications of הלכות קצובות/גדולות, ר"ח, רי"ף, רשב"ח, תשובות
   גאונים are family-level at best when the matched span is a bare Bavli excerpt; small-margin
   cards carry the "ייתכן חיבור מקביל קרוב" chip — read it as "family, not necessarily this
   member".
4. **Targum:** same-book known-Onkelos siblings are now excluded, but Palestinian-targum family
   members can still stand in for one another (the PsJ-Exodus Decalogue card is family-level).
5. **One printed-book page remains** (מדרש תהלים card; NLI "דפוס") — not a Genizah manuscript.
6. **Statutory cards (6/40) are correct but routine** (yotzer, chatimot, tachanun, megilla
   blessing) — near-zero discovery value.
7. **Start here (flagships):** the Solomon-throne midrash (T-S C 2.198, 1069 letters, FGP), the
   expanded gaon-kaddish (T-S NS 309.86, witA=2), Hidayat al-Qari (EVR ARAB I 2390 + 3944),
   Perek Shira (EVR II A 390.1), ר"ח בבא מציעא (T-S NS 311.35), the Ibn Abitur and Albaradani
   piyyut identifications, and the witA=1 finds carried from the liturgy pass.

## v10 final grade (2026-07-10 — cite-formula window widened to [-38,+30) + 266-work exemption)

**What v10 changed:** the cite-formula gate now scans [-38,+30) around the span start (covering
formulas at the highlight head), with an exemption for 266 works whose own reference text uses
citation formulas (protecting genuine Talmud-commentary witnesses). Gate yield 67 → **99**
corpus-wide; kept 40,741 → 40,701.

### 1. The 17 surviving v9 rabbinic leaks: ALL 17 REMAIN — v10 top band is card-for-card identical to v9

Verified by full index diff (same 40 shelfmark/work/length/witA rows in the same order). The +32
new kills all landed outside the sampled deck. Survivors by name: ר"ח פסחים on the ערוך [01],
קטעי מסורה on הלכות ספר תורה [02], הלכות קצובות on BHG [03] and on תורת האדם [24], אבדר"נ on
מנורת המאור [04], רשב"ח ספר הבגרות [05] and ספר הגירושין [26], הלכות גאונים on a Bavli ms [07],
קטעי מדרש on אבן עזרא [10], ספרי במדבר on a Bavli ms [11], מדרש תהלים on printed ב"ר [12] and on
מנורת המאור [25], ר"ח מו"ק on הרי"ף [16], תשובות האיי on והזהיר [17], קהלת זוטא on מעלות המדות
[20], מדרש שמואל on בחיי [22], כוזרי on דרשות [23] — plus the v9-new אסתר רבה on ויקרא רבה [40].
Total top-band leaks: **18** (unchanged).

**Why the widened gate still missed them (attributed against the card texts):**
- **Exemption re-admission (the dominant cause — structural):** the geonic/commentary claimed
  works of the digest-family leaks are exactly the works whose own reference text uses citation
  formulas, so they are among the 266 exempted: [26] ספר הגירושין (match literally BEGINS
  "כד מוכח התם פר' התקבל" — in-window, yet survived), [01]+[16] ר"ח (the exemption built to
  protect genuine RH witnesses also waves through every FALSE RH claim), and the digest family
  [03],[05],[07],[17],[24]. **The exemption is keyed on the wrong side:** the citation formula
  on these pages belongs to the PAGE's host work (the Arukh, the Rif, a rishonic compendium),
  not to the claimed work — exempting by claimed work neutralizes the gate precisely on its
  target class.
- **HTR garble defeats the lexicon:** [25] "וגדסי' במד' תהלים" (ר→ד), [10] "אמרו ככר אמר רשכי",
  [22] "מכאן אח" — the formula is present but misspelled beyond exact lexicon match.
- **Formula deeper than +30 into the span:** [04] ("ותו גרסי' בפרק חלק" ~230 chars in), [11]
  ("הכי גרסינן" past the window).
- The remaining anthology cards ([02],[12],[20],[23],[40]) carry no in-window formula at all
  (shared lists/petichtot with no citation marker near the span head).

### 2. Replacement cards: NONE — no slots freed, nothing new to grade.

### 3. v10 top-band precision (FINAL for the cover note)

**55% (22/40)** — correct 12, correct-trivial 6, plausible 4, leak 18. Identical to v9.
Full lineage: v6 84% → v7 82.5% (liturgy smoke) → v8 full 45% → v9 55% → **v10 55%**.

### 4. Over-kill spot-check: ZERO ✅ — and the exemption re-admission answer is YES

All 12 correct cards and all 5 flagships present ([09] Solomon-throne, [15] gaon-kaddish,
[06]+[30] Hidayat al-Qari, [21] Perek Shira, [08] ר"ח ב"מ — the exemption did its protective job
for [08], a genuine RH witness). No graded genuine card was killed by the widened gate.
**Exemption re-admission confirmed:** the 266-work exemption re-admits the false claims of those
same works on OTHER works' pages — named cases [01], [16], [26] and the digest family
[03]/[05]/[07]/[17]/[24]. Builder note: exempt only when the claimed work's own edition text
contains the matching formula at the ALIGNED position (formula is part of the parallel), or
downgrade the exemption to flag-not-demote; keyed as-is, the gate cannot touch the digest-family
class.

### 5. Cover note — CONFIRMED with two amendments

The v9 caveat list stands verbatim (measured precision, the one leak family + two-second tells,
geonic-digest family-level warning, Palestinian-targum family caution, printed page, statutory
share, flagships-first), with:
- **Amendment to caveat 1:** final measured top-band precision is **55%** (v10; unchanged by the
  last iteration) — liturgy/piyyut cards ~82–84%, rabbinic-literature cards ~30%.
- **Amendment to caveat 2:** state explicitly that the deck could NOT automatically remove this
  family (the citation-formula gate is neutralized on it by the commentary-work exemption and
  HTR garble) — the scholar's two-second tells (NLI line naming a different work, ~16/18 cards;
  citation formulas at/inside the highlight) are the operative filter, not the pipeline.

**Final verdict: SHIP v10 with the amended cover note.** Two consecutive iterations with zero
over-kill, all flagships intact, and the residual noise is one homogeneous, easily-taught family.
Further automatic gains require the structural fixes (host-work-keyed formula logic,
NLI-mismatch flag, wider rabbinic guard) — a next-cycle work item, not a reason to hold the deck.
