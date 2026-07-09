# A3 — Interleaved Bible+Targum/Tafsir class probe

Spike A3 of the SEED-029 wave-1 briefs (2026-07-08). Goal: confirm or refute
that the top UNIDENTIFIED motif-query gainers (+91/+47/+45/+40 new MSS) are
verse-by-verse interleaved Bible+Targum/Tafsir manuscripts, and estimate the
harvest of aligned (Hebrew-verse, JA/Aramaic-block) pairs this class can feed
into the RamBERT cross-lingual training-data plan.

Script: `scripts/probe_interleaved.py` (read-only; reproduces
`growth_unidentified.py`'s exact enumeration, then dumps ALL `track1_matches`
rows — including shadowed and sub-150-letter rows — plus the motif's own
`motif_query_hits` span, as a page timeline projected back onto the
**original** `pages.text`, never `norm_stream`). Raw evidence:
`results/a3_interleaved_probe_dump.json` (22 cards / 12 motifs).

## Verdict: MIXED — three distinct classes, and the brief's hypothesis holds
## for a real but *minority* slice of the heaviest gainers

Sampled 22 pages across the top 10 unidentified-gainer motifs (by construction
this is the **entire +50-or-more growth bucket**, 8 motifs) plus the two
motifs tied at exactly +40 that the brief calls out by name. Reading the full
original-text timeline (all Track-1 rows, any tier, any shadow status, plus
the motif's own span) on each sampled page gives three classes, confirmed by
direct reading and cross-checked against `libraries.csv` catalog titles for
every member (old + new) of all 12 motifs:

| Class | What it is | Why Track-1 misses it | In this sample |
|---|---|---|---|
| **A. Karaite liturgical florilegium + JA rubrics** | A Hebrew mosaic of Psalms/piyyutim/Bible excerpts stitched together (a Karaite *siddur*/*maḥzor*), threaded with **short Judeo-Arabic performative rubrics** telling the reader what to recite next — not a running translation | No single reference work spans ≥150 continuous letters; it's 4-6 *different*, individually-short, correctly-identified pieces glued together | **6 of 12 motifs** (all directly read; dominant class in the +50+ bucket) |
| **B. Genuine verse-by-verse Bible + JA Tafsir/commentary** | Hebrew verse (often partial/paraphrased) immediately followed by a Judeo-Arabic translation or exegetical block, repeating per verse — Yefet ben Eli, Yeshua ben Yehuda, Saadia Gaon-type works; catalog convention "עם תרגום ופרוש... **אחרי כל פסוק**" (with translation+commentary after every verse) | Language alternation breaks Track-1's seed-and-extend span into fragments too short to clear the 150-letter/density bar on **either** language; the JA prose itself is rarely in the reference corpus verbatim | **1 of 12 motifs directly** (135681) by sampled-page reading, but see the catalog-signal analysis below — it is much larger once you look past the 2 pages/motif I sampled |
| **C. Plain, continuous Masoretic Bible/Torah text** | A vocalized Torah/Bible codex or scroll, no JA/Aramaic anywhere | Threshold/mechanical: `matched_letters` on the motif's *original* 3-4 pilot witnesses hovers at/under 150 (near-miss), or a Midrash/Halakha work quoting the same verse collides without a clean shadowing win (mesirah noise) | **5 of 12 motifs** (144874, 511281, 364132, 151250, 280144) |

So: **the brief's hypothesis is real but is NOT the dominant driver of the
biggest gainers.** The single heaviest gainer class (the +50-to-+91 tier) is
mostly Class A — Hebrew-language liturgical medleys with only incidental,
short Arabic *rubrics* — which is exactly the brief's own kill-criterion case
("florilegia/medleys... say so explicitly"). Genuine cross-lingual
verse-alternation (Class B) is real, well-attested, and catalog-confirmed at
scale, but it shows up more in the mid-tier (+10-to-+49) growth than in the
heaviest gainers.

## Per-motif results (direct reading)

For each: motif id, growth, sampled shelfmarks, verdict, and the load-bearing
quote (never from `norm_stream`).

**301507** (3→94, +91) — RNL EVR II A 30/01 (`Ms. EVR II A 30/01`, סדור ר'
ישעיה הכהן) p.85 & RNL EVR II A 1427 p.232. **Class A.** The page is Hodu
(1 Chr 16) / Ashrei-type Psalms text; the motif's own span picks up plain
Hebrew ("ויאמר משה אכלוהו היום כי שבת היום לייי..."); one GAP on the second
page reads `...וג' מתל כל לולא אלי אן תקול שאויריםם קודש...` — a short Arabic
connective ("מתל" = *mithl*, "אלי אן" = *ilā an*) inside an otherwise Hebrew
run, not a translation block. Catalog cross-check: one co-member of this
motif is explicitly catalogued *"תפילות קראיות(?) קטעי מקרא ליטורגיים עם
הוראות (מעטות) בערבית"* — "Karaite prayers, liturgical Bible excerpts **with
(few) instructions in Arabic**" — independently confirming Class A, not B.
[browse](https://genizahsearch.com/browse?sys_id=990001967110205171&page=85)

**349623 / 342384 / 353009 / 330576 / 357450** (all Karaite *siddur*/*maḥzor*
מנהג קראים fragments, RNL EVR collection) — **Class A**, all 10 sampled pages.
Representative GAP from 357450 (RNL EVR II A 2905 p.151):
`...ותם אלצלאה עלי אלעאדה ושלום... מעריב תפלת חולי שלמועד אלי עלי רסמך מתל
כל לילה מן תרתיב אלמו[ער]... תקול הדה אלפסוקים והם פי ספר...` — "*and the
prayer concludes as usual, and peace... Ma'ariv, the weekday festival prayer,
according to your custom, like every night, from the order of the
[service]... you say these verses, and they are in the book of...*" — this is
a **rubric directing the reader what to recite**, textbook Karaite liturgical
stage-direction, not a Bible-verse translation. 330576 additionally contains
genuine **biblical Aramaic** (Daniel 2:20, `ענה דניאל ואמר להוא שמה די אלהא
מברך מן עלמא ועד עלמא...`) picked up correctly by Track-1 as `cat=Bible`
(Daniel's own Aramaic chapters are part of Mikra, not Targum).
[357450 browse](https://genizahsearch.com/browse?sys_id=990001464460205171&page=151)

**144874** (3→68, +65) — RNL EVR II B 79 / EVR II B 17, both *"תורה: עם
ניקוד וטעמים, מסורה קטנה וגדולה"* (vocalized Torah codices, Exodus 28-29,
priestly vestments). **Class C.** No JA/Aramaic anywhere in either sample;
Track-1 DOES catch `cat=Bible` spans plus several SHADOWED collisions
(רמב״ם פרוש המשנה, תנחומא quoting the same verse) — the page is simply
noisy/fragmented plain scripture. Catalog check of ALL 68 members: zero
JA-signaled titles. **Confirmed pure Class C, no B admixture in this motif.**
[browse](https://genizahsearch.com/browse?sys_id=990000989540205171&page=84)

**511281** (3→61, +58) — Manchester Gaster 33 / CUL T-S Misc.3.11
(Genesis 8 Flood, Genesis 14 Sodom/Lot). **Class C on the sampled pages**,
but the motif's full 61-member set includes *"פרוש התורה לעלי בן סולימאן"*
(Ali ibn Suleiman's Karaite JA Torah commentary) and *"מתוך הפוליגלוטה של
קושטא"* (the 1546 **Constantinople Polyglot** — Hebrew + Targum Onkelos +
Judeo-Persian + Saadia's JA Tafsir printed in parallel columns). **Mixed
motif**: the same widely-copied Genesis passage exists both as plain Bible
codices and inside genuinely multilingual witnesses; my 2-page sample just
didn't happen to land on the latter.
[browse](https://genizahsearch.com/browse?sys_id=990053779590205171&page=3)

**364132 / 151250 / 280144** (Exodus 19-20 Sinai theophany, Deut 12-13, Exod
2-3 burning bush) — **Class C on the sampled pages** (plain Masoretic Bible,
one sample of 364132 embedded verbatim, untranslated, inside a Karaite
siddur's Torah-reading section). But catalog cross-check of the FULL member
sets finds, for every one of these three motifs, **explicit JA
translation/commentary co-members**: 364132 → *"תרגום ופרוש התורה ליפת בן
עלי (שמות, קטעים)"*; 151250 → *"תפסיר ערבי: במדבר, דברים"* and *"תרגום
ופרוש ערבי לתורה לישועה בן יהודה (דברים)"*; 280144 → *"תורה (שמות א:א-ו:א):
עם תרגום ופרוש של יפת בן עלי **אחרי כל פסוק**"* and *"תרגום ערבי לתורה
(שמות)"*. **All three are mixed motifs** — the same Bible passage recurs in
plain-Bible AND genuinely interleaved-Tafsir witnesses.

**135681** (3→43, +40) — RNL EVR ARAB I 4626, catalogued *"תרגום ופרוש ערבי
לתורה לישועה בן יהודה (בראשית, קטעים)"* (Yeshua ben Yehuda's Arabic
translation+commentary on Genesis). **Class B, confirmed by direct reading —
the clean positive case.** `t1_rows=0`: Track-1 misses this page *entirely*,
not even the Bible span. The motif's own span (and the surrounding text)
alternates verse/gloss cleanly:
```
Heb: וירא אלהים אל יעקב עוד בבאו מפדן אדם ויברך אתו           (Gen 35:9)
JA:  תם תגלי מלך אללה אלי יעקב איצ̇א ענד מגיה מן פרן אדם ובארכה
Heb: ויאמר לו אלהים שמך יעקב לא יקרא שמך עוד יעקב              (Gen 35:10)
JA:  איכנ איצ̇א יכון אסמך פסמאה (ה)סראיל
Heb: ויאמר לו אלהים אני אלשדי פרה ורבה גוי...                  (Gen 35:11)
JA:  תם קאל לה אללה אנא אלקאדר אלכאפי מעלמך במא יכון מנך פאתמר...
```
Multiple co-occurring, syntactically-coherent JA function words in each gloss
(תם/kāl/אלדי/פאן/איצ̇א/ענד/אלקאדר) — **HIGH confidence**, not a bare particle
hit. [browse](https://genizahsearch.com/browse?sys_id=990001511500205171&page=124)

To directly test whether Class B generalizes beyond this one new witness, I
pulled the page for one of motif 280144's **original pilot members**
(sid `990001522290205171`, catalogued *"תורה (שמות א:א-ו:א): עם תרגום ופרוש
של יפת בן עלי אחרי כל פסוק"*) — Yefet ben Eli's commentary on Exodus 2-6.
`t1_rows=0` again (Track-1 totally blind). The text is Yefet's exegetical
style: a (often partial/paraphrased) Hebrew lemma embedded in running JA
prose, e.g. `...וגם את הג̇וי אשר יעבדו דן אנכי' וקאל קום אנה ישיר בה אלי
קולה לכל ואחד מן אלאבא ונתתי לזרעך את כל הארץ הזאת...` ("*and also that
nation whom they serve I will judge* — and it is said that this refers to
each of the Patriarchs [regarding] 'and to your seed I will give all this
land'...") — verse-quote-then-gloss, same class as 135681, and it shows
**why** Track-1 misses BOTH sides: the Hebrew lemmas are short/partial
paraphrases, and Yefet's own JA phrasing isn't in the reference corpus in
this exact wording. [browse](https://genizahsearch.com/browse?sys_id=990001522290205171&page=47)

## Class-level extrapolation (catalog-signal cross-check)

Direct reading of 22 pages is not enough to size the class across all 1,571
unidentified gainers. To get a defensible number without running the engine,
I cross-referenced **every member (old + new) of every unidentified-gainer
motif** against its `libraries.csv` catalog title (col 7), flagging a title
as "JA-Bible-signaled" if it contains `תפסיר`, `ערבי` (Arabic), `פוליגלוטה`
(Polyglot), or one of the known JA Bible translator/exegete names (יפת בן
עלי, ישועה בן יהודה, רס״ג, אבן בלעם, עלי בן סולימאן, ...) combined with
`תרגום`/`פירוש`/`פרוש`. This is a coarse, **catalog-title proxy**, not a
per-page read — but it is grounded in the same catalog-equivalence idea
`track1_bib.py` already uses, and it is cheap (one CSV pass).

Stratified by the same growth buckets `growth_unidentified.py` already
defines:

| Bucket | motifs | new memberships | JA-signaled motifs | JA-signaled memberships |
|---|---|---|---|---|
| +50+ | 8 | 596 | 2 (25.0%) | 149 (25.0%) |
| +10-49 | 52 | 1,299 | 36 (69.2%) | 965 (74.3%) |
| +3-9 | 205 | 864 | 116 (56.6%) | 483 (55.9%) |
| +1-2 (B3's fragmentary tail) | 1,306 | 1,586 | 1,017 (77.9%) | 1,210 (76.3%) |

This exactly matches the direct-reading pattern: the **heaviest** gainers
(+50+) are dominated by Class A (Hebrew florilegia, only 25% touch any
JA-Tafsir witness at all — and even those, per 301507's case above, tend to
be the *"few Arabic instructions"* rubric subtype, not verse-translation).
Class B's catalog footprint gets progressively **larger** as growth gets
smaller (69→78%) — sensible, since a personal exegete's exact JA phrasing is
copied verbatim far less often than a universally-recited Psalm, so
Tafsir-manuscript witnesses of any one short passage accumulate into smaller
motifs. The fragmentary tail (B3's territory, +1-2) shows the *highest* rate
(78%) and should be flagged to B3 as needing the same JA angle in its
agreement scorer.

**Caveat (lower bound):** ~35-45% of the sys_ids I looked up in the sampled
motifs' member lists have a **blank** `libraries.csv` title (e.g. `Gaster
Printed series 33 | Manchester | ` had none) — those manuscripts cannot be
signaled even if they are genuinely interleaved Tafsir witnesses. The true
Class B prevalence is therefore **higher** than the numbers above.

## Aligned-pair harvest estimate (feeds the RamBERT gold set)

Scope: the n≥3 unidentified-gainer pool that is A3's assignment (265 motifs,
2,759 new memberships) — the +1-2 fragmentary tail is B3's territory and is
reported separately, not double-counted into this estimate.

1. **Pages available (lower bound):** 1,399 new (motif, MS) memberships sit
   on motifs with ≥1 JA-Bible-signaled catalog member (50.7% of the pool).
   Treating each new membership as ≈1 candidate page (a motif's matched span
   is short — one witness essentially = one page bearing that passage) gives
   **≈1,400 candidate pages**, undercounted per the blank-title caveat above.
2. **Pairs per page:** the one page read in full for pair-density (135681,
   RNL EVR ARAB I 4626) yielded **3 clean aligned Hebrew-verse↔JA-block
   pairs in an ~800-character excerpt** (≈1 pair per 250-300 characters).
   The corpus-wide average page is 1,096 characters (`AVG(n_chars)` over
   667,411 pages) — if a confirmed Class-B page follows the same density
   across its full length (plausible: the catalog convention is "after
   **every** verse," a whole-manuscript structural claim, not a one-off), a
   full page yields on the order of **3-4 pairs**. I deliberately shave this
   to a **2-4 pair/page range** since it is anchored on a single directly-
   read example, not a validated average.
3. **Estimate: ≈1,400 pages × 2-4 pairs/page ≈ 2,800-5,600 aligned
   (Hebrew-verse, JA/Aramaic-block) pairs** extractable in principle from
   the n≥3 unidentified-gainer pool alone. If B3's fragmentary tail (1,210
   further signaled memberships, ≈1,200 more candidate pages) is folded in
   at the same rate, the combined ceiling is **≈5,000-10,000 pairs**.

This is an **order-of-magnitude estimate from catalog-title proxy + n=2
directly-read pages**, not a validated count — treat it as a planning number,
not a training-set size commitment. It also does **not** include the much
larger and mechanically easier channel of the reference corpus's own
already-cataloged `cat='JA'` Track-1 identifications (27,122 rows corpus-
wide, e.g. RS"G's Tafsir Torah, Yefet's biblical commentaries already in
`ref_corpus.pkl`) — pages where Track-1 *does* successfully identify a JA
span could be mined directly for Hebrew-verse↔JA-block adjacency without any
of this motif-query machinery; that channel is out of A3's scope but should
be pointed out to whoever plans the RamBERT data pull.

## Open issues / recommended follow-up (wave 2)

1. **Validate the pair-density assumption.** Only one page was read start-to-
   finish for pair counting. A wave-2 pass should pull the FULL text of the
   ~1,400 catalog-signaled pages and directly count Hebrew-verse/JA-block
   alternations (or at minimum a stratified sample of ~30-50), rather than
   projecting from n=1.
2. **Blank-title undercounting.** The catalog-signal method is a lower bound.
   A better signal would cross the same sys_id list against `fist_data/
   fjms_enrichment.db`'s catalog + bibliography tables (richer descriptions
   than `libraries.csv`'s often-empty title field) — not done here to stay
   inside "light SQLite reads only."
3. **Class A is real and needs its own name.** The Karaite florilegium+rubric
   class (dominant in the heaviest gainers) is a legitimate discovery in its
   own right — a `siddur`/`maḥzor` מנהג קראים page that stitches together
   4-6 separately-identifiable canonical pieces plus short Arabic stage
   directions. It is NOT training data for cross-lingual verse alignment
   (the "translation" isn't a translation, it's a rubric), but it IS a
   coherent structural class that `passage_units.py`/B2's residue-mining
   should probably recognize as "florilegium," not leave as noise.
4. **Class C (144874/511281/364132/151250/280144) is a Track-1 tuning
   signal, not a linguistic finding.** These motifs are plain Bible text
   whose ORIGINAL 3-4 pilot witnesses happened to sit at/under the 150-letter
   Track-1 acceptance bar. Worth a one-line flag to A2/A5: some fraction of
   the "unidentified" motif-query pool is really "Track-1 threshold near-
   misses on canonical text," unrelated to interleaving, and A5's
   length-conditional threshold work may already fix a chunk of it for free.
5. **cat='JA' adjacency mining** (see estimate section) is a cheap, higher-
   confidence channel that this spike did not have scope to check — flag for
   whoever scopes the actual RamBERT data-pull phase.
