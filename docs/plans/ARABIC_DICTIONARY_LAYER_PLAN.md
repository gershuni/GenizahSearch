# Arabic dictionaries beside Blau: Lane and Lisān al-ʿArab for Judeo-Arabic words

**Status:** plan **rev. 2** (2026-11-04): **the owner ruled D1-D10** (§8): Lane first, Lisān
later; the laneslexicon master files; credit and licence terms at the minimum each source
requires; Ephraim Meiri's parsed files may be used; the stdlib v3 method; the 250-650 AH count;
the cards under Blau's for now; the gate plus the owner's grading; a closed-class table. Still
research and measurement only: nothing is built in either app, nothing is deployed, nothing public
changes. The next step is phase A (§7).
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
(`--dev-docs`). **Disclosed:** v1's last two rules (the future marker سـ only before an imperfect
verb; P2 leaving out words with a mark after a letter that takes no dot) came after reading the
first 15 P1 and 15 P2 errors of one run over all documents, before the split existed. Methods v2
and v3 were added after reading seeded samples of the scored data's errors (the 40 P1 cases of
§4, 40 P2 words shown wrong and 40 with nothing shown), so their scored numbers are not a clean
held-out estimate. The development documents alone give the same direction: P1 right 75.0% (v1),
77.4% (v2), 80.1% (v3); P2 69.1%, 77.9%, 77.9% (68 words only).

### 6.3 Results (scored documents)

| Run | P1 right / wrong / none | P2 right / wrong / none (right of shown) | P3 reading / root | Gate |
|---|---|---|---|---|
| **v1, 15 M tokens (as fixed)** | 74.9 / 18.7 / 6.4 | **78.7** / 5.5 / 15.8 (93.5) | 67.7 / 64.5 | **FAIL G1** |
| v1, 30 M tokens | 76.0 / 18.8 / 5.2 | 80.4 / 6.8 / 12.8 (92.2) | 72.5 / 68.9 | PASS |
| v1, CAMeL roots, 15 M | 68.7 / 25.2 / 6.0 | 78.7 / 5.5 / 15.8 (93.5) | 67.7 / 62.7 | FAIL G1 |
| v2, 15 M | 77.6 / 19.7 / 2.7 | 83.1 / 10.6 / 6.3 (88.7) | 76.9 / 73.3 | PASS |
| v2, 30 M | 77.8 / 19.9 / 2.3 | 84.1 / 11.9 / 4.0 (87.7) | 80.9 / 76.9 | PASS |
| **v3, 15 M** | **80.6** / 17.6 / 1.8 | 83.1 / 10.6 / 6.3 (88.7) | 76.9 / 74.0 | PASS |
| v3, 30 M | 80.8 / 18.0 / 1.3 | 84.1 / 11.9 / 4.0 (87.7) | 80.9 / 77.6 | PASS |

With stripping or Lane's headwords the plain-letter baseline is 63.2-65.7% right, so frequency
ranking adds 11.7 to 15.2 points (with CAMeL, 54.2% to 68.7%). v2 and v3 show more (nothing shown falls from 15.8% to 6.3%) and are wrong more often when
they show something (88.7% of shown right, against 93.5%): the stem backoff guesses where v1 kept
quiet.

### 6.4 The gate fails when the ranking is broken

The same runs with a deliberately broken ranking, each exiting 1:

| Ranking | P1 right / wrong | P2 right | Criteria failed |
|---|---|---|---|
| reversed (rarest first), v1 15 M | 64.8 / 28.6 | 51.4 | G1, G2 (+1.6 points) |
| plain letters only (today's undotted reading), v1 15 M | 63.2 / 17.3 | 0.0 | G1, G2 |
| every dottable letter dotted, v1 15 M | 41.0 / 20.8 | 61.2 | G1, G2 (-22.2 points) |
| reversed, v2 30 M | 64.3 / 33.3 | 46.8 | G1, G2, G3 |
| reversed, v3 30 M | 64.8 / 33.9 | 46.8 | G1, G2, G3 |

`tests/test_measure_arabic_readings.py` pins each criterion failing on a made-up result.

### 6.5 What the gate does not show

It does not measure the words the layer is for (P3) except by the hand grading; it does not see
Hebrew words given an Arabic reading (§5.4: 3 of 20 in v1, 7 of 20 in v3); and P1's truth is
Blau's filing, which is not always the word's (§4). Before anything ships, the owner's grading of
the 200-word sample is the check this measurement cannot replace (D9).

---

## 7. Phases (Lane first, D1)

- **A. A Lane sidecar, `arabic_data/arabic.db`**, built and verified like `blau_data/blau.db`
  (`scripts/build_blau_sidecar.py` is the model: fail on a bad download, never shrink silently,
  atomic write, facts about the source pinned in `meta`). It holds Lane's roots and articles from
  the laneslexicon master files (D2), Lane's headword table (§4), the frequency table counted over
  all primary non-OCR OpenITI texts of 250-650 AH, dictionaries excluded (D7), and the
  closed-class table (D10). `meta` keeps the Perseus availability statement and credit line (D3,
  D4). Where Ephraim Meiri's parsed Lane files are simpler to take than our own parse of the XML
  (rendered articles, say), the build may read them instead (D5); the first task of phase A is to
  look at what his `data/entries` and `data/cards` shards carry and pick the cheaper source. A gate
  script recomputes every key, checks counts and the credit line, and fails rather than skips.
- **B. The Lane cards on `/browse`**, inside the Blau panel, under the same switch and box (rulings
  (c) and (d) of the Blau plan): the readings as chips, the commonest first; for the chosen
  reading, the root card with Lane's article (English, truncated, "more"), the Perseus credit line
  and a link to the source. Typed Arabic skips the reading step. The cards sit under Blau's (D8; to
  be revisited, since Lane often gives the broader sense). Web only first, as Blau.
- **C. Lisān al-ʿArab**, the same way, after Lane: its articles from the pinned OpenITI file (or
  Ephraim Meiri's parse, D5), its card beside Lane's, with OpenITI's credit and licence (D4).
- **D. The desktop**, on a later ruling (GPL v2 is one reason to keep CAMeL out of it; D6 keeps the
  method stdlib anyway).
- **Gate before deploy (D9):** this script's gate G1-G3 on data not used to shape the method (a
  fresh seed split, or editions added since), the owner's grading of the 200-word P3 sample, and the
  sidecar's own gate.

Not proposed: guessing misread letters (ruling (e)); inferring the page's language (ruling (c)).

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
| D8 | **Where the cards go.** Under Blau's cards; a tab of their own; or only when Blau has nothing | **Under Blau's cards** for now; may change, since Lane usually gives the broader sense |
| D9 | **The release gate.** G1-G3 as fixed, with the owner's grading of the 200-word sample as the deciding check | **Keep G1-G3 and add the owner's grading** |
| D10 | **The closed-class table.** A hand list of about 150 forms (prepositions with pronouns, demonstratives, particles) pointing at Lane's particle articles | **Yes** |

---

## 9. Risks

- **Hebrew and Aramaic words.** A page mixes languages and the page does not infer which (ruling
  (c)); a Hebrew word with an Arabic look-alike gets a card (צדיק as صديق, אבר as أبر). v3's backoff
  makes it worse (7 of 20 non-Arabic words in the graded sample). A filter by the word's count in
  the corpus's Hebrew pages, or showing the cards only on the reader's click on "Arabic", are the
  options.
- **The truths are imperfect.** P1's truth is Blau's filing (about a third of "wrong" was Blau's
  filing, §4); P2's is the editions' marks, which follow the scribe (צ'גיר).
- **Genre bias** in the count (فصل over فضل): the count should be large and mixed (D7).
- **Licences**: three regimes on one card (Perseus conditions, CC BY-NC-SA, Blau's research-only
  permission); each card carries its own credit line.
- **Size**: Lane's XML is 68 MB and Lisān 31 MB; the sidecar will be tens of megabytes, with the
  same server-copy-first deploy as Blau's.

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

`--root-method camel` needs CAMeL Tools and `morphology-db-msa-r13` in the running Python. The
30-million token runs add `--frequency-extra <folder>` with the second pick (seed 20261105 of
`pick_sample`, the same filters) and a cache of their own. A run takes about a minute; the first
count of the 15 million tokens about three.
