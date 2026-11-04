# Arabic dictionaries beside Blau: Lane and Lisān al-ʿArab for Judeo-Arabic words

**Status:** plan **rev. 6** (2026-11-04). **The owner ruled D1-D12** (§8): Lane first, Lisān
later; the laneslexicon master files; credit and licence terms at the minimum each source
requires; Ephraim Meiri's parsed files may be used; the stdlib v3 method; the 250-650 AH count;
each dictionary in its own tab of one panel (D12, replacing D8's "under Blau's"); the gate plus
the owner's grading; a closed-class table; Hebrew words given an Arabic card are not a gate (a
reader who asks for Arabic gets Arabic). **Phase A
is built** (§7.1, branch `claude/arabic-lane-sidecar`): `arabic_data/arabic.db` with Lane's
47,855 articles, its headword table, a 247-million-token word count and a 141-row closed-class
table; its own gate passes and fails on mutations; the plan's gate passes on it. Nothing is served,
bundled or deployed; no reader sees anything. **Phase B is built** (§7.2, branch
`claude/arabic-lane-cards`): Lane in its own tab beside Blau's on `/browse`, not deployed; its
wording awaits the owner.
**Origin:** the owner's review of the Blau companion on 2026-11-04 (PR #475,
`docs/plans/BLAU_DICTIONARY_PLAN.md` §6.2.1): Blau records what is particular to Judeo-Arabic, so
an ordinary Arabic word (אבו *abū*, לנא *lanā*) has no entry, and an undotted letter is two
letters, so דלך reached only دلك "to polish" when the reader meant ذلك "that".
**What this PR carries:** this plan; `scripts/measure_arabic_readings.py`, the measurement and its
gate; `scripts/arabic_layer_openiti_sample.json`, which pins every downloaded input by commit and
sha256; `tests/test_measure_arabic_readings.py` (the pure parts, and a gate that fails on broken
input).
**The result in four lines** (§6): ranking the readings by classical-Arabic frequency works (the
edition's dotted letter comes first in 88-94% of the words where anything is shown; ذلك 82,941
against دلك 25). Finding the root is the weak step (Blau's root matched for 75-81% of the words
Blau has, about two thirds of the misses being the method's own). The gate as fixed before the
first run **fails** on the first method with the smaller word count (78.7% against 80%) and
passes with a larger count or the later methods, which were shaped after looking at errors on the
scored data; a fresh check is owed before anything ships.

---

## 1. The proposal in one paragraph

When a reader clicks a Judeo-Arabic word on `/browse` and Blau has no entry (or alongside Blau's
entries), the page reads the word as Arabic: every letter the automatic text leaves undotted is
read both ways (דלך is دلك and ذلك), the readings are ranked by how common each is in classical
Arabic prose (a word count over OpenITI), the root of the first reading is found, and that root's
entries in Lane's *Arabic-English Lexicon* and Ibn Manẓūr's *Lisān al-ʿArab* are shown, under
Blau's cards, with each dictionary's credit line. The reader sees the readings ("ذلك · دلك") and
can pick another. Nothing is guessed about misread letters (ruling (e) of the Blau plan stands).

---

## 2. Sources and licences (checked 2026-11-04)

### 2.1 Lane, *An Arabic-English Lexicon* (1863-1893)

- **Upstream.** The Perseus Digital Library's TEI encoding (`TEI.2`, "Additional XML cleanup by
  Alpheios Technical Services"). It is not in `PerseusDL/lexica` on GitHub, which holds only the
  Greek and Latin lexica. The maintained copy is `laneslexicon/lexicon_xml`, "The Perseus XML
  files with amendments" (the untouched Perseus files are on its `originals` branch): 36 files,
  one per letter or part (`b0.xml`, `_A0.xml`, ...), 68 MB, last commit `f3c19fb2` (2018-02-20).
  Every file is pinned by sha256 in `scripts/arabic_layer_openiti_sample.json`.
- **Format and key.** One `<div2 type="root" n="ktb">` per root (5,317 divisions, 5,219 distinct
  names) and one `<entryFree key="kitaAbN">` per headword (48,076), with the Arabic in Buckwalter
  transliteration (Perseus writes `A^` for an alef with hamza). **Keyed by root**, headwords under
  each root. Two quirks matter for matching: a geminate root has two letters (`$b` = شبب) and an
  initial hamza is often written as a plain alef (`Ax*` = أخذ). Lane finished the letters ا to ق;
  ك to ي were completed from his notes by Stanley Lane-Poole and are thinner.
- **Licence.** The header's availability statement (`status="free"`) allows free distribution on
  three conditions: credit Perseus whenever the text is used, with the line *Text provided by
  Perseus Digital Library, with funding from The U.S. Department of Education and The Max Planck
  Society.*; leave the availability statement intact; offer Perseus any modifications. Lane's
  printed text is in the public domain; the conditions attach to the encoding.

### 2.2 Ibn Manẓūr, *Lisān al-ʿArab*

- **Upstream.** OpenITI, repository `OpenITI/0725AH`, file
  `0711IbnManzurIfriqi.LisanCarab.Shamela0001687-ara1.mARkdown` (the primary version, from
  al-Maktaba al-Shāmila; Dār Ṣādir, Beirut, 3rd ed. 1414 AH, 15 volumes), 30.7 MB, commit
  `056df249` (2025-11-29), sha256 pinned in the script. The same file is in OpenITI release
  2025.1.9 (Zenodo, DOI 10.5281/zenodo.17767721, 2025-12-30).
- **Format and key.** OpenITI mARkdown: one `### $ <root>: <text>` line opens each article
  (9,153 articles, 9,118 distinct heads: 6,545 of three letters, 2,605 of four); `### |` and
  `### ||` lines are the book's chapters (ordered by the root's LAST letter). **Keyed by root**;
  geminates are written in full (شبب), a final weak letter as ا, و or ي.
- **Licence.** The release is CC BY-NC-SA 4.0 (the Zenodo record). OpenITI asks users to cite the
  release and the version URI. Non-commercial fits a free research site; share-alike would apply
  to anything derived from the text (a parsed index, card text) that GenizahSearch publishes.

### 2.3 The emeiri sites (may be used, D5)

`https://semitic.lang.emeiri.org` already serves both dictionaries, parsed. Its manifest
(`/data/manifest.json`, built 2026-11-01) lists `lane` with 47,890 documents ("Perseus free
distribution", the Perseus credit line above) and `lisan` with 9,155 (CC BY-NC-SA 4.0, credited to
OpenITI), beside ORACC, CAD, Dillmann, Fraenkel and Al-Jallad. Its data are sharded
(`data/entries/<n>.json` and `data/cards/<n>.json` answer; the per-letter `data/bodies/<letter>.json`
named in the brief returned 404 when probed). Its counts agree with ours (Lisān 9,155 against the
9,153 heads parsed here). The measurement here did not use its files. **Ruled 2026-11-04 (D5):**
its maintainer, Ephraim Meiri, is the owner's friend, and GenizahSearch may use the parsed files
as is convenient; phase A (§7) takes whichever source is simpler, with the Perseus and OpenITI
credits still on the text.

Its sister site, `https://dicts.lang.emeiri.org`, lists Hebrew and Aramaic dictionaries marked
public domain: Jastrow (32,512 entries), Levy and Levy's Targum dictionary, Dalman, the ʿArukh,
Krauss, BDB and Radak's *Shorashim*, among others. Mentioned only: a later layer. Friedman's
Judeo-Arabic dictionary (ja.genizah.org, per-letter PDFs) and CAL stay link-out only, as the owner
framed them.

### 2.4 The word count: OpenITI

Release 2025.1.9 is CC BY-NC-SA 4.0, a 5.9 GB archive with a 12.1 MB metadata table (sha256
pinned). For this measurement the script picks a seeded sample from that table: primary, non-OCR
Arabic texts dated 250-650 AH (the Geniza's centuries), of 20,000 to 1,500,000 tokens, no
dictionaries (genre tags `_MACAJIM`, `_GHARIB`, "lexicograph"), at most six per first genre tag,
15 million tokens in all. That is **95 texts, 14,923,558 Arabic tokens by our count, 374,715
distinct skeletons** (§3), 144 MB, each file pinned by repository commit and sha256. A second,
disjoint pick (seed 20261105, 95 texts) doubles it to 29,817,827 tokens for the sensitivity check
(§5.3). Whether a frequency table counted from a CC BY-NC-SA corpus is "adapted material" that
must carry the same licence is the owner's question (D4).

### 2.5 Morphology: CAMeL Tools

The code is MIT (NYU Abu Dhabi). Its MSA morphology database `morphology-db-msa-r13`
(calima-msa-r13, 40.5 MB) is **GPL v2**, derived from Buckwalter's Aramorph 1.2.1; the better
`calima-msa-s31` needs a licensed copy of the LDC's SAMA 3.1 and is out. There is no
classical-Arabic database in its catalogue. `camel-tools` 1.6.0 declares torch and transformers,
which the analyzer never imports: it ran here installed with `--no-deps` plus a few small
packages, in a virtual environment of its own. GPL v2 binds distribution, so it matters for the
desktop installer and not for the web server.

---

## 3. Reading a Judeo-Arabic word as Arabic

The letter map, measured on Blau's own one-word spellings (roots and sub-entries whose Hebrew and
Arabic have the same number of letters):

| Letter | Unmarked | Marked (dot above, `'` or `׳`) | Measured on Blau |
|---|---|---|---|
| ג | غ or ج | ج in Blau; **غ in many editions** | ג = غ 151, ج 3; ג̇ = ج 404 |
| ד | د or ذ | ذ | ד = د 647, ذ 2; ד̇ = ذ 125, د 1 |
| ט | ط or ظ | ظ | ט = ط 300; ט̇ = ظ 49 |
| צ ץ | ص or ض | ض | צ = ص 118, ض 1; צ̇ = ض 43 |
| ת | ت or ث | ث | ת = ت 426, ث 3; ת̇ = ث 114 |
| כ ך | ك or خ | خ | כ = ك 337, ך = ك 49; כ̇ = خ 211, ך̇ = خ 32 |
| ה (final) | ه or ة | ה̈ = ة | ה = ه 236, ة 8; ה̈ = ة 983 |
| י | ي, ى or ئ | | ي 1,173, ئ 70, ى 52 |
| א | ا, أ, إ, آ or ء | | ا 1,228, أ 203, إ 100, آ 14, ء 4 |
| ש | ش | | ش 210, ث 2 |

The rest are one letter each. The rules the script follows (`readings`, `skeleton`):

- **The mark.** U+0307, or an apostrophe or geresh right after ג ד ט צ ת כ, as `dot_pattern` of
  PR #475 reads it. A mark after any other letter is an abbreviation (ר', ב׳כ׳ר׳) and changes
  nothing. The automatic text and the editions mostly leave the dot out: the PGP editions write
  דלך 5,789 times and ד'לך 21 times.
- **The mark on ג.** Blau marks ج with it, but the PGP editions mark غ with it as often (בג'יר
  *bi-ghayr*, ג'ריב *gharīb*, צאג'ה *ṣāgha*). A marked ג keeps both readings and frequency
  chooses.
- **What Judeo-Arabic does not write.** It rarely writes the hamza (שי = شيء, אלמא = الماء) and
  does not tell ة from ه or ى from ي. So the frequency key, the *skeleton*, drops the bare hamza,
  folds the hamza seats and alef forms (أ إ آ ٱ to ا, ؤ to و, ئ to ي), ة to ه and ى to ي. The
  readings of a word then differ only at the six dottable letters and a final א, which is also an
  alef maqsura (מוסא = موسى). A word with k undotted dottable letters has 2^k readings (at most
  64). The root finder reads the spelling the sample uses most for that skeleton (عدة, شيء), not
  the skeleton.
- **The silent alif of the plural** (method v2, §6.3): Judeo-Arabic mostly omits it (חצ'רו =
  حضروا), so a word ending in ו is also read with it. The method v1 did not, and that was the
  largest share of its marked words with no reading at all.

---

## 4. Root or lemma

Lane and Lisān are keyed by root, so a reading must become a root. Three methods were measured on
P1 (§6.1: words Blau has, Blau's root as the truth, Blau not used), all with the same readings and
ranking:

| Method | What it is | P1 right | wrong | nothing |
|---|---|---|---|---|
| Affix stripping (v1, stdlib) | peel a proclitic, a pronoun, an ending, a verbal or nominal prefix and at most one inner long vowel or t; keep what is a root of Lane or Lisān; cheapest first, ties to the root with the longer articles | 74.9% | 18.7% | 6.4% |
| CAMeL Tools (v1 readings) | the root of the most probable analysis of the reading (`pos_lex_logprob`), its `#` tried as hamza and as a weak letter | 68.7% | 25.2% | 6.0% |
| **Lane's headwords (v3)** | Lane's 48,076 headwords as a form-to-root table (28,285 one-word skeletons, 1,232 under more than one root); a proclitic and a pronoun peeled, the commoner stem winning a tie; stripping when no headword matches | **80.6%** | 17.6% | 1.8% |

(v3 also carries v2's reading changes; v2 alone is 77.6% / 19.7% / 2.7%. All held-out, 15-million
token count, 67,462 words.)

**What the "wrong" column is.** A seeded sample of 40 v1 "wrong" cases, read in their page context
(graded by Claude, not the owner): **27 are the method's errors**, 10 are the method right and
Blau's filing not the word's (Blau files הדא under هدي, סאר under سرر, פמצינא under مصو, אמר under
a root it spells with و), 2 are defensible either way (الناس under أنس or نوس) and 1 is unclear. So
roughly two thirds of the measured "wrong" is real. The 27 errors: a root letter taken for a
clitic or a prefix (كتبت as ك + تبت, سيدي as س + يدي, والدها as وال + دها; بهار, بقاءك, لفافة,
كثير, كتبي, كتبه, لسان, فلوس, المملوك) 15; the conjunction و kept as a root letter (وربع, وسرني) 2;
a noun pattern not undone (ثقة, الديوان, اسم, شقة, متعين, مائة twice) 7; closed-class words (هذه,
لكن read as ليكن) 2; a proper name (موسى) 1. Lane's headwords undo many of the first and third
groups (checked one by one: سيدي to سود, لسان to لسن, كثير to كثر, ثقة to وثق, الديوان to دون,
لفافة to لفف, المملوك to ملك; and in the P3 sample أمور to أمر, نجيب to نجب), which is why v3 leads.

**CAMeL Tools is not recommended.** It is behind stripping on P1, its MSA database is GPL v2 and
was built for modern newswire, and it still needs our readings (it does not read Hebrew letters).
The dictionaries' own headwords are the better analyzer here, and they cost nothing in licence
terms beyond the dictionary's own.

**The owner's examples.** אבו reads أبو (83,253 in the sample, the only reading) and reaches Lane's
root أبو. לנא reads لنا (5,511): it is a preposition with a pronoun (*li* + *nā*), which no root
method handles well (v1 lands on Lisān's head لنا, v3 on نوأ). Closed-class words (prepositions
with pronouns, demonstratives, relative pronouns, particles: about 150 forms, the commonest words
on any page) need a hand table pointing at Lane's particle articles (ل, من, في, ذا, الذي), not
morphology (D10).

---

## 5. Ranking the readings by frequency

### 5.1 The owner's complaint and its neighbours (15-million token count)

| Word | Readings, commonest first (count) | Plain letters only (what an undotted letter gives today) |
|---|---|---|
| דלך | **ذلك 82,941** · دلك 25 | دلك |
| ודלך | **وذلك 8,089** · ودلك 7 | ودلك |
| אלדי | **الذي 34,117** · الدي 5 | الدي |
| הדא | **هذا 58,305** · هدى 927 · هذي 99 | هدا (27) |
| תם | **ثم 60,645** · تم 519 | تم |
| אכד | **أخذ 4,196** · أكد 132 · أخد 4 | أكد |
| גיר | **غير 23,889** · جير 9 | غير |
| כאן | **كان 73,751** · خان 113 | كان |

### 5.2 Against the editions' own dots (P2, §6.1)

Where the edition marks a dot and the automatic text has the same letters, the layer is given the
letters without any mark. With the 15-million token count, v1: the edition's dotted letter is first
in **78.7%** of 1,215 words, another reading is first in 5.5%, nothing is shown in 15.8%; **93.5%
of what is shown is right**. Reading every letter plain gets 0%; reading every dottable letter
dotted gets 61.2% (and is wrong elsewhere, §6.3); the reversed ranking 51.4%.

Where frequency is wrong: genre (فصل 5,685 against فضل 2,068, because فصل heads the sections of
the sampled books), true ambiguities that only the sentence decides (نقضت / نقصت, خلا / كلا, مدخر /
مذكر), and the editions' own conventions (צ'גיר marks a dot the word صغير does not have). Words with
nothing shown were, in more than half of a sample of 40, verbs with the plural alif (חצ'רו,
יתראצ'ו, יאכ'דו: 8 of 40) and words with clitics the sample never saw whole (ותנפד'ה, ויצ'יפהם,
בחצ'רת): what method v2's two changes address (§3, §6.3).

### 5.3 How big the count must be

Doubling the count to 29.8 million tokens (a second disjoint seeded sample) moves P2 v1 from 78.7%
to **80.4%** (nothing shown 15.8% to 12.8%; right of shown 93.5% to 92.2%) and the share of
words-Blau-lacks with a reading from 67.7% to 72.5%. The production list should be counted over
far more of OpenITI than 15 million tokens (D7).

### 5.4 On the words Blau lacks (P3), graded by hand

The 200-word seeded sample (`--grading-sample 200`) is written for the owner to grade. Claude
graded its first 60 from their page context, as a provisional figure only:

| | v1 | v3 |
|---|---|---|
| Arabic words (40 of 60): the shown root right | 27 | 33 |
| the shown root wrong | 9 | 6 |
| nothing shown, or a reading with no root (a name) | 4 | 1 |
| Hebrew or Aramaic words and names (20 of 60): rightly nothing | 17 | 13 |
| given an Arabic reading | 3 | 7 |

The reading itself was right for 35 of the 36 Arabic words v1 showed (the one miss: ענא, عنّا,
ranked عني first). **Ranking is not the weak step; the root is**, and v3's gain on Arabic words is
paid for on Hebrew words (§9).

**Corrected in rev. 4** (local Codex review): until then each sampled row showed the context of the
first occurrence of its spelling, not of the occurrence drawn. The draw is unchanged (the same 200
words in the same order, the same roots shown), but 164 of the 200 rows now show another context,
47 of the 60 graded above. Those 60 grades were read from the old contexts and stay provisional.
Each row now also names its page (`page`, the transcription header), and the owner's grading
must use a file written by rev. 4 or later.

---

## 6. The gate

### 6.1 What is measured (`scripts/measure_arabic_readings.py measure`)

The pages are the PGP Judeo-Arabic digital editions aligned word by word with the automatic text of
the same manuscripts, as `scripts/measure_blau_close_spellings.py` (PR #475) aligns them: 3,517
documents with an edition, 6,223 pages seen, 3,012 pages used (half or more of the page's words in
matching runs), 388,317 aligned word pairs. Three populations, each with its own truth:

- **P1, root against Blau** (67,462 words). The automatic word equals the edition's and reaches
  exactly one Blau root whose Arabic spelling is a root (three or four letters, no ة, no inner
  alif) whose strong letters sit in the word in order. Blau is not used by the layer; its root is
  compared with the root of the layer's first reading. 115,196 words reaching several Blau roots
  and 22,363 whose Blau root is not usable are left out.
- **P2, the dot against the edition** (1,215 words). The edition marks a dot on ד ט צ ת כ, not on
  the last letter, in a word with no abbreviation mark, and the automatic text has the same letters;
  the layer gets them unmarked. 2,523 marked words were seen, 1,308 left out by those rules.
- **P3, the words Blau lacks** (127,139 occurrences, 28,952 distinct): read right, no Blau entry.
  Measured: a reading shown, its root in Lane or Lisān. Its truth is the hand grading of §5.4.

### 6.2 The criteria, fixed before the first run

**G1** P2: the edition's dotted letter is first in at least 80% (nothing shown counts as a miss).
**G2** P1: right beats the plain-letter baseline by at least 5 points. **G3** P1: wrong at most
30%. The script exits 1 when any fails.

The methods were shaped on 300 seeded development documents (282 pages), which are never scored
unless asked (`--score-dev`, or `--only-dev` for them alone). Since rev. 4 they are pinned in
`scripts/arabic_layer_dev_documents.json`, so a later seed or a newer `pgp.db` cannot re-admit
them (local Codex review). **Disclosed:** v1's last two rules (the future marker سـ only before an imperfect
verb; P2 leaving out words with a mark after a letter that takes no dot) came after reading the
first 15 P1 and 15 P2 errors of one run over all documents, before the split existed. Methods v2
and v3 were added after reading seeded samples of the scored data's errors (the 40 P1 cases of
§4, 40 P2 words shown wrong and 40 with nothing shown), so their scored numbers are not a clean
held-out estimate. The development documents alone give the same direction: P1 right 75.0% (v1),
77.4% (v2), 80.1% (v3); P2 69.1%, 79.4%, 79.4% (68 words only).

**Corrected in rev. 4** (GitHub Codex on PR #477): P2 compared the reading's length with the
page's, so a reading with v2's silent plural alif (حضروا for חצ'רו) was scored wrong even when the
marked letter was right. P2 now compares without that alif. v1 cannot produce one and is unchanged;
v2 and later gain 3.3 to 3.5 points of P2 (40 of the 1,215 words on the 15-million count, 43 on
the built file) and nothing else moves. v4 on the development documents now passes G1 (83.8% of 68
words, from 79.4%). Every v2+ number
below is the corrected one.

### 6.3 Results (scored documents)

| Run | P1 right / wrong / none | P2 right / wrong / none (right of shown) | P3 reading / root | Gate |
|---|---|---|---|---|
| **v1, 15 M tokens (as fixed)** | 74.9 / 18.7 / 6.4 | **78.7** / 5.5 / 15.8 (93.5) | 67.7 / 64.5 | **FAIL G1** |
| v1, 30 M tokens | 76.0 / 18.8 / 5.2 | 80.4 / 6.8 / 12.8 (92.2) | 72.5 / 68.9 | PASS |
| v1, CAMeL roots, 15 M | 68.7 / 25.2 / 6.0 | 78.7 / 5.5 / 15.8 (93.5) | 67.7 / 62.7 | FAIL G1 |
| v2, 15 M | 77.6 / 19.7 / 2.7 | 86.4 / 7.3 / 6.3 (92.2) | 76.9 / 73.3 | PASS |
| v2, 30 M | 77.8 / 19.9 / 2.3 | 87.5 / 8.5 / 4.0 (91.2) | 80.9 / 76.9 | PASS |
| **v3, 15 M** | **80.6** / 17.6 / 1.8 | 86.4 / 7.3 / 6.3 (92.2) | 76.9 / 74.0 | PASS |
| v3, 30 M | 80.8 / 18.0 / 1.3 | 87.5 / 8.5 / 4.0 (91.2) | 80.9 / 77.6 | PASS |

With stripping or Lane's headwords the plain-letter baseline is 63.2-65.7% right, so frequency
ranking adds 11.7 to 15.2 points (with CAMeL, 54.2% to 68.7%). v2 and v3 show more (nothing
shown falls from 15.8% to 6.3%) and are wrong a little more often when they show something (92.2%
of shown right, against 93.5%): the stem backoff guesses where v1 kept quiet.

### 6.4 The gate fails when the ranking is broken

The same runs with a deliberately broken ranking, each exiting 1:

| Ranking | P1 right / wrong | P2 right | Criteria failed |
|---|---|---|---|
| reversed (rarest first), v1 15 M | 64.8 / 28.6 | 51.4 | G1, G2 (+1.6 points) |
| plain letters only (today's undotted reading), v1 15 M | 63.2 / 17.3 | 0.0 | G1, G2 |
| every dottable letter dotted, v1 15 M | 41.0 / 20.8 | 61.2 | G1, G2 (-22.2 points) |
| reversed, v2 30 M | 64.3 / 33.3 | 48.1 | G1, G2, G3 |
| reversed, v3 30 M | 64.8 / 33.9 | 48.1 | G1, G2, G3 |

`tests/test_measure_arabic_readings.py` pins each criterion failing on a made-up result.

### 6.5 What the gate does not show

It does not measure the words the layer is for (P3) except by the hand grading; it does not see
Hebrew words given an Arabic reading (§5.4: 3 of 20 in v1, 7 of 20 in v3; not a criterion, D11); and P1's truth is
Blau's filing, which is not always the word's (§4). Before anything ships, the owner's grading of
the 200-word sample is the check this measurement cannot replace (D9).

---

## 7. Phases (Lane first, D1)

- **A. A Lane sidecar, `arabic_data/arabic.db`: BUILT (2026-11-04, branch
  `claude/arabic-lane-sidecar`), §7.1.**
- **B. The Lane cards on `/browse`**, in the dictionary panel, under the same switch and box
  (rulings (c) and (d) of the Blau plan): the readings as chips, the commonest first; for the chosen
  reading, the root card with Lane's article (English, truncated, "more"), the Perseus credit line
  and a link to the source. Typed Arabic skips the reading step. Lane has its own tab beside Blau's
  (D12, which replaced D8's "under Blau's cards"). Web only first, as Blau. **BUILT
  (2026-11-04, branch `claude/arabic-lane-cards`, not deployed), §7.2.**
- **C. Lisān al-ʿArab**, the same way, after Lane: its articles from the pinned OpenITI file (or
  Ephraim Meiri's parse, D5), its card beside Lane's, with OpenITI's credit and licence (D4).
- **D. The desktop**, on a later ruling (GPL v2 is one reason to keep CAMeL out of it; D6 keeps the
  method stdlib anyway).
- **Gate before deploy (D9):** this script's gate G1-G3 on data not used to shape the method (a
  fresh seed split, or editions added since), the owner's grading of the 200-word P3 sample, and the
  sidecar's own gate.

Not proposed: guessing misread letters (ruling (e)); inferring the page's language (ruling (c)).

### 7.1 Phase A as built

**The source (D5, D2).** Ephraim Meiri's parse at semitic.lang.emeiri.org was taken for Lane: its
1,024 `data/entries/<n>.json` shards (438 MB, every file pinned by sha256 in `emeiri/pins.json`)
carry each Lane article with its headword and root in Arabic script (vowelled), the printed page
and the article text with its Arabic already converted from Buckwalter, which our own parse of the
XML would have had to do. His Lane is quranic-research/lexicon_xml (2019), a restructuring (roots
cleaned, senses marked) of the laneslexicon master files, so it is the text D2 chose. His manifest
carries the Perseus credit line and availability statement; the build checks both against the TEI
header's wording and fails on a difference. His source gives no root on 35 of 47,890 entries (the
roots شأم, أدر, حرث); they are left out, counted and pinned in `meta`, not guessed. 66 root
headings join spellings (`سعو & سعى`, `غو | غوى`, `(دمو or دمى)`, one with an English note), 696
entries in all; until rev. 4 each was keyed as its letters run together (`سعWسعW`), a root no
reading could reach (local Codex review). Each is now split into its spellings and filed under the
first one's root; 57 headings spell one root two ways, and 9 (11 entries, rare loanwords such as
`ثند & ثندأ`) name two roots and file under the first, the heading kept whole in
`lane_entries.root`.

**The tables** (`scripts/build_arabic_sidecar.py`, schema 1.0.0, 73.7 MB, two minutes on 12
workers):

| Table | Rows | What |
|---|---|---|
| `lane_roots` | 4,619 | one per root key, with Lane's spellings of it (a heading that joins spellings, `سعو & سعى`, is split into them) |
| `lane_entries` | 47,855 | every article: headword, root, printed page, text |
| `lane_forms` | 42,351 | the one-word headwords by skeleton: the form-to-root table of method v3 (5,470 headwords of several words are not forms) |
| `freq` | 808,441 | skeleton, count, commonest spelling, at 2 or more: 247,037,983 tokens of 2,816 OpenITI texts (D7's selection is 2,819; three are listed in the release metadata but absent from their repositories), 1,325,038 distinct skeletons |
| `closed_class` | 141 | `scripts/arabic_closed_class.json`: prepositions alone and with the seven commonest pronouns, demonstratives, particles; each names Lane's article, and 105 the headword in it; the build refuses a row Lane cannot answer. Not yet mapped: the relative pronouns (الذي, التي, الذين), نحن and the third-person pronouns, for which no Lane article was found by headword |

`shared/arabic_normalize.py` is the one copy of the readings and keys (moved out of the measuring
script, the numbers unchanged on the development documents); `NORMALIZER_VERSION` is pinned in
`meta`.

**The gate** (`scripts/verify_arabic_sidecar.py --db arabic_data/arabic.db --expect-entries
47890`): integrity, schema (columns, foreign keys, indexes) against the builder's DDL, no form
pointing at no entry, every root spelling plain letters, every `rows_<table>` against its table, the
source's count (stored plus rootless), floors on every table and on the tokens counted, the stored
counts summing to the tokens counted (each type left out was counted once), the texts counted plus
those unavailable equal to D7's selection, every root key, headword skeleton, frequency key and
closed-class key recomputed with the running normalizer, the Perseus credit line and availability
statement intact, the lookup indexes used. The build itself recomputes D7's selection from the
pinned release metadata and fails unless the manifest accounts for every selected text once
(counted from its own file, or listed as unavailable, at most 0.5%). It passes the built file. **Proven to fail on the real file**: ten articles deleted ("meta.rows_lane_entries =
'47855' but the table holds 47,845 rows", the count against the source, four roots' entry counts);
one in a hundred form keys edited ("423 lane_forms keys differ from skeleton(headword)"); the
credit line and a row count doctored ("meta.lane_credit is not the Perseus credit line",
"meta.rows_freq = '808440' but the table holds 808,441 rows"); one frequency count raised by one,
or one row deleted with `rows_freq` fixed to match ("freq counts sum to 246,521,387 with 516,597
types left out, which does not add up to meta.freq_tokens 247,037,983"); the texts counted
doctored ("meta.freq_texts '2000' + 3 unavailable != 2819 selected").

**The plan's gate on the built file** (`measure_arabic_readings.py measure --sidecar`, Lane only,
the full count, scored documents):

| Method | P1 right / wrong / none | P2 right / wrong / none (right of shown) | P3 reading / root | Gate |
|---|---|---|---|---|
| v1 | 77.9 / 18.2 / 4.0 | 85.2 / 8.1 / 6.7 (91.3) | 81.5 / 72.8 | PASS |
| v3 | 81.1 / 17.9 / 1.0 | 87.9 / 9.1 / 3.0 (90.6) | 87.3 / 80.0 | PASS |
| v4 (v3 + the closed-class table) | 81.1 / 17.9 / 1.0 | 87.9 / 9.1 / 3.0 (90.6) | 87.3 / 80.0 | PASS |
| v4, reversed ranking | 57.1 / 37.5 / 5.4 | 35.9 / 61.2 / 3.0 | | FAIL G1, G2, G3 |
| v4, plain letters only | 65.9 / 25.3 / 8.8 | 0.0 | | FAIL G1, G2 |
| v4, every dottable letter dotted | 42.9 / 30.9 / 26.2 | 72.1 / 0.0 / 27.9 | | FAIL G1, G2, G3 |

The full count is what lifts v1's P2 over the line (78.7% with 15 million tokens, 85.2% with
247 million): the method as first fixed now passes all three criteria on the built file. The
closed-class table changes no P1 or P2 word (those are Blau's roots and dotted words), but it is
what gives לנא Lane's article on ل and הדא and דלך the article on ذا.

**What the larger count costs: Hebrew words.** On the 60 graded words of §5.4, v4 on the built file
shows the right root for 32 of the 40 Arabic words (7 wrong, 1 name with no root), but gives an
Arabic card to 13 of the 20 Hebrew or Aramaic words and names (v1 on the 15-million sample: 3):
247 million tokens attest strings like دنن (7), كهر (15), فروطه (12) that the smaller sample did not.
Raising the count a reading needs before it is offered (`--min-count`, 2 so far) measures as
follows on the same 60 words and the same gate:

| Offered at | Arabic words: root right / wrong / no card | Hebrew words and names given a card | P1 right / wrong | P2 right | Gate |
|---|---|---|---|---|---|
| 2 or more | 32 / 7 / 1 | 13 of 20 | 81.1 / 17.9 | 87.9 | PASS |
| 20 or more | 33 / 6 / 1 | 7 of 20 | 80.9 / 17.6 | 86.5 | PASS |
| 100 or more | 33 / 5 / 2 | 4 of 20 | 80.2 / 16.7 | 82.6 | PASS (79.6, FAIL G1, before the rev. 4 correction) |

(The grading of these 60 is Claude's, from the page context, applied the same way to every run;
the owner's grading of the 200 replaces it, D9.)

**The owner's ruling (D11):** this is not a gate. A reader who clicks a word for its Arabic
readings has decided to read it as Arabic, and gets them; the panel does not judge the word's
language, as it does not for Blau (ruling (c) of the Blau plan). Phase B therefore adds no Hebrew
filter and keeps the count at 2. The table above stays as information.

### 7.2 Phase B as built (not deployed)

**One method, served and measured.** The v1-v4 method moved verbatim from the measuring script
into `shared/arabic_method.py`; the script imports it back. Re-run on the built file, v1, v3 and
v4 give the same P1, P2, P3, samples and examples as before the move, so the numbers of §7.1 are
the numbers of what is served.

**The service** (`shared/arabic_service.py`), fail-closed as `shared/blau_service.py`: ready only
when `arabic_data/arabic.db` is where a sidecar lives, opens read-only and passes `quick_check`,
has schema 1.0.0 and the running normalizer, every table and both lookup indexes, matching row
counts, and the Perseus credit line. `LANE_DICTIONARY_ENABLED` (default on, read live) is its kill
switch. A lookup is method v4 at count 2 (D11):

- **A word in Hebrew letters** gives up to three readings, the commonest in the word count first.
  An abbreviation gets none.
- **A word typed in Arabic letters** is its own reading.
- **The first reading's first root** gets a card. The gate measured the first root only (P1), and
  the second was often a weak guess (`كتاب` offered توب), so one card.
- **The card's entries:** the entries whose headword is the reading (or its stem, or the
  closed-class table's headword) come first, then the rest in Lane's order. At most three are shown,
  each cut at 600 characters, and the rest are counted.

The tables the method reads per lookup are held in memory: the root keys, the article size per
root, the headword table and the closed-class table. The word count stays on disk and is read by
key. The real file opens in 0.3 s, and a lookup takes 0 to 16 ms.

**One tab per dictionary (D12).** The panel was first built with Lane's section under Blau's
cards and credit (D8). The owner, trying it the same day: a long Blau entry pushed Lane out of
sight, and the panel's title and hint named Blau alone. So:

- **The tabs.** Each dictionary served has a tab, Blau's then Lane's, in a row between the head
  and the scrolling body. A tab whose dictionary has nothing for the word is greyed, not hidden,
  and says so when opened. With one dictionary served there is no tab row, and the head carries its
  name.
- **Which tab opens** (`blau_panel.pick_tab`): the tab the reader last chose on this manuscript,
  while it has something for the word; else the first that has (Blau's, if it does); else the
  reader's. A tab the panel chose by itself is not remembered as the reader's.
- **The panel appears when either dictionary is served.** Each lookup asks each service again (the
  kill switches are read live); a dictionary not served then has no tab, and with neither the panel
  stays closed.
- **The wording names no one dictionary:** the head says "Dictionaries", the hint "Click a word to
  look it up in the dictionaries", the switch "Turn on/off the dictionaries". Inside each tab, an
  empty answer says "Nothing in the dictionary for this word." (drafts, below).

**Lane's tab** (`web/components/lane_panel.py`, drawn by `web/components/blau_panel.py`;
`web/pages/browse.py` asks both services in one trip off the event loop):

- **The title**, then the readings as chips. The first is marked; a click on another looks that
  reading up in the box, so the panel then shows it in Arabic letters.
- **The root card:** Lane's entries, the matched ones marked. A cut entry links to its full text on
  Ephraim Meiri's site (`?id=<entry id>`), and the root links to all its entries there
  (`?q=<root>&source=lane`).
- **The credits:** the Perseus line verbatim (English in both UIs), then where the entries come from
  and what ranks the readings.
- **When nothing shows:** without the credit line, Lane has no tab (D3). For a word with no reading
  the tab is greyed and says nothing was found.

**Owner decisions still open for phase B** (drafts in place, nothing deployed):

- the English and Hebrew wording of the section and of the panel's title, tabs, hint and switch
  (`shared/genizah_translations.py`, marked DRAFTS);
- whether the two source lines are wanted, or the Perseus line alone (D3 asks only that one);
- the server copy of `arabic_data/arabic.db` (74 MB) and the release.

The gate before deploy is unchanged (D9): your grading of the 200-word sample, written by rev. 4
or later; a check on documents the method was not shaped on; and the sidecar's own gate.

---

## 8. Owner decisions (ruled 2026-11-04)

| # | Decision | Ruling (owner, 2026-11-04) |
|---|---|---|
| D1 | **Sources.** Lane and Lisān both, or one? | **Lane has the priority and comes first**; Lisān after it (§7 phases A-C) |
| D2 | **Which Lane.** `laneslexicon/lexicon_xml` master (amended) or its `originals` branch (Perseus as published)? | **Master** |
| D3 | **Credit lines** on every card. Lane: the Perseus line verbatim (§2.1). Lisān: OpenITI, the version URI and the release DOI, CC BY-NC-SA 4.0 | **Credit as needed**: each card carries the credit its source requires and no more; for Lane, the Perseus line verbatim |
| D4 | **Licences.** Perseus asks for modifications to be offered back; CC BY-NC-SA asks derived material to carry the same licence | **Do the minimum needed.** Lane: the credit line on every surface that shows Lane, the availability statement kept with the data (`meta`); GenizahSearch shows Lane's text, it does not change it, so there is nothing to offer back unless a build ever corrects the source, and then that correction is offered to Perseus (by the owner). Lisān (phase C): credit, the licence named and linked, non-commercial use; no derived file is published, so share-alike asks nothing further. The frequency table is an internal ranking aid, not published |
| D5 | **The emeiri sites.** Reuse their parsed Lane and Lisān? | **Yes, at our convenience**: their maintainer, Ephraim Meiri, is the owner's friend. Phase A takes whichever source is simpler; the Perseus and OpenITI credits still apply to the text |
| D6 | **Morphology.** Stdlib (stripping plus Lane's headwords, v3) or CAMeL Tools (GPL v2 database)? | **Stdlib v3** |
| D7 | **The frequency count.** Which part of OpenITI, which genres? | **All primary non-OCR texts of 250-650 AH, dictionaries excluded** |
| D8 | **Where the cards go.** Under Blau's cards; a tab of their own; or only when Blau has nothing | **Under Blau's cards** for now; may change, since Lane usually gives the broader sense. *Replaced by D12* |
| D9 | **The release gate.** G1-G3 as fixed, with the owner's grading of the 200-word sample as the deciding check | **Keep G1-G3 and add the owner's grading** |
| D10 | **The closed-class table.** A hand list of about 150 forms (prepositions with pronouns, demonstratives, particles) pointing at Lane's particle articles | **Yes** |
| D11 | **Hebrew words given an Arabic card** (13 of 20 in the graded sample on the built file, §7.1). A gate, a count threshold or a Hebrew filter? | **Not a gate** (later the same day): "If someone thinks it's Arabic, let him look for Arabic." No filter; the count stays at 2 |
| D12 | **Where the cards go, after trying phase B.** Under Blau's, the panel's head and hint name Blau alone, and a long Blau entry hides Lane | **A tab per dictionary** (later the same day): "show Lane not under Blau but in a tab", so a long Blau entry needs no scrolling and more dictionaries can be added; the wording becomes general (§7.2) |

---

## 9. Risks

- **Hebrew and Aramaic words.** A page mixes languages and the page does not infer which (ruling
  (c)); a Hebrew word with an Arabic look-alike gets a card (צדיק as صديق, אבר as أبر). v3's backoff
  makes it worse (7 of 20 non-Arabic words in the graded sample), and so does the full count (13 of
  20 on the built file, §7.1). **Accepted (D11):** the reader asked for Arabic. Should it ever
  matter, a threshold on the count or a filter by the word's count in the PGP Hebrew editions are
  the measured options.
- **The truths are imperfect.** P1's truth is Blau's filing (about a third of "wrong" was Blau's
  filing, §4); P2's is the editions' marks, which follow the scribe (צ'גיר).
- **Genre bias** in the count (فصل over فضل): the count should be large and mixed (D7).
- **Licences**: three regimes on one card (Perseus conditions, CC BY-NC-SA, Blau's research-only
  permission); each card carries its own credit line.
- **Size**: the built Lane sidecar is 73.7 MB (the word count is 808,441 rows of it); Lisān will
  add about 31 MB. The deploy is server-copy-first, as Blau's.
- **The source moves.** Meiri's site is rebuilt (its manifest says 2026-11-01); a rebuild of the
  sidecar fetches anew and pins anew, and `meta.lane_source_built_at` and the sha256 of the pins
  say which snapshot a file holds.

---

## 10. Reproduce

The data stay in the main checkout and are read by absolute path; the downloads go to an empty
folder of their own, as untrusted data.

```
python scripts/measure_arabic_readings.py fetch --dest <empty folder>
python scripts/measure_arabic_readings.py measure --sources <that folder> \
    --pgp-db C:\GenizahSearch\pgp_data\pgp.db --transcriptions C:\GenizahSearch\Transcriptions.txt \
    --blau-db C:\GenizahSearch\blau_data\blau.db --frequency-cache _tmp\arabic_freq.json.gz \
    --grading-sample 200 --out _tmp\arabic_layer.json            # v1 as fixed: exits 1 (G1)
... measure ... --ranking reversed                               # exits 1
... measure ... --method-version v3                              # exits 0
```

Phase A (the sidecar; the downloads go to `arabic_data/download/`, gitignored):

```
python scripts/build_arabic_sidecar.py fetch --dest arabic_data/download
python scripts/build_arabic_sidecar.py build --source-dir arabic_data/download --out arabic_data/arabic.db
python scripts/verify_arabic_sidecar.py --db arabic_data/arabic.db --expect-entries 47890
python scripts/measure_arabic_readings.py measure --sidecar arabic_data/arabic.db --method-version v4 \
    --pgp-db ... --transcriptions ... --blau-db ...                 # exits 0; --ranking reversed exits 1
```

`--root-method camel` needs CAMeL Tools and `morphology-db-msa-r13` in the running Python. The
30-million token runs add `--frequency-extra <folder>` with the second pick (seed 20261105 of
`pick_sample`, the same filters) and a cache of their own. A run takes about a minute; the first
count of the 15 million tokens about three.
