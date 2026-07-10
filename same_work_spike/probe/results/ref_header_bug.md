# Reference-corpus header-stripping bug — quantification for Map-v2 rebuild

**SEED-029 · Track-1 reference quality · read-only investigation · 2026-07-10**

Scope: `track1_build_ref.py` → `ref_corpus.pkl` build path, over the 8,233 Maagarim
`.txt` files in `C:\Users\gersh\Dropbox\דיקטה\מאגרים\AllTextsOnlyText`. All numbers below
were produced by importing the **real** `HEADER_RE` / `MESIRAH_RE` / `norm_stream` from the
pipeline (never reimplemented) and running each file through the exact prep+normalize path.
No pipeline script was modified; no fix was applied.

---

## TL;DR

- The task's hypothesis — that the **section-scoped** mesirah header
  `##סעיף N | המסירה: …##` (form 2) leaks into the reference stream — is **FALSE**.
  `HEADER_RE = ##[^#]*##` strips **both** the plain form (1) and the section form (2)
  cleanly: both are `##…##` with no interior `#`. All 11 section-form files verify clean.
- The **real** defect is a *different* mechanism: a `##…##` header that contains an
  **interior single `#`** is not matched by `##[^#]*##` at all, because `[^#]*` stops at
  that interior `#`. This happens in exactly **17 works** corpus-wide, in two opposite ways:
  - **A. Location noise leaks IN — 16 works, 342 junk Hebrew letters total.**
    All 16 are anonymous epigraphy/inscription files (`אפיגרפיה`) whose mesirah header
    carries an *approximate-date bracket* `[600#]` / `[1050#]` / `[300#]`. The interior
    `#` breaks the match, so the whole header (manuscript-location words) survives into
    the stream: 13–45 junk letters per work.
  - **B. Real body text is DELETED OUT — 1 work, 30,677 Hebrew letters (7.4% of the work).**
    ` יהודה אבן תיבון — ספר הרקמה ליונה אבן ג׳נאח ` (Ibn Janāḥ's *Sefer HaRikmah*, Ibn Tibbon's
    translation, 597 KB). Its 47 mesirah headers each embed the HTML entity `&#39;`
    (an interior `#`) plus stray HTML markup. `##[^#]*##` fails to close each header and
    instead **mis-pairs** a header's closing `##` with the *next* header's opening `##`,
    swallowing the real body text between them. Current stream 386,253 letters vs
    correct 416,930 — **30,677 letters (7.4%) of this major work silently dropped.**
- Both failure modes are fixed by one regex change (below), which is verified to leak
  **0** residual `##`, over-strip **0** real text, and preserve **byte-for-byte** the
  2,571 files / 6.88 M letters that use single-`#` inline text markers.
- **The Saadia (רס"ג) "residue unidentified" finding is unrelated** to this bug (§4).

---

## 1. What `HEADER_RE` (and friends) actually do

`track1_build_ref.py` lines 24–25:

```python
HEADER_RE  = re.compile(r'##[^#]*##')            # STRIP metadata headers from the text
MESIRAH_RE = re.compile(r'##המסירה:\s*([^#]+)##') # EXTRACT the mesirah value to metadata
```

Prep path (line 47–49):

```python
mes    = MESIRAH_RE.search(raw)          # -> works[i]['mesirah'] metadata field
text   = HEADER_RE.sub(' ', raw)         # -> strip ##…## before normalize
stream, _ = norm_stream(text)            # -> letters-only matchable stream
```

`norm_stream` keeps only Hebrew base letters U+05D0–05EA (finals folded); everything else
— combining marks, punctuation, brackets, spaces, digits, **Latin** — is dropped. This is
important: a leaked Latin location like `London, British Library` contributes **0** letters;
only **Hebrew** header words pollute the stream.

**`HEADER_RE = ##[^#]*##`** matches `##`, then any run of characters that are **not `#`**,
then `##`. Consequences:

| header text | matched & stripped? | why |
|---|---|---|
| `##המסירה: London, BL Or.1081##` (form 1, plain) | ✅ yes | no interior `#` |
| `##סעיף 5 \| המסירה: ENA 1501, 1-7##` (form 2, section) | ✅ yes | no interior `#` |
| `##עמ' 13##`, `##פרק א, משנה א##`, `##גוף החיבור, עמ' 8##`, `##קטע 3##`, `##(גרסה)##` … | ✅ yes | no interior `#` |
| `##המסירה: [1050#] כת' אספישי; ליסבון…##` | ❌ **NO** | interior `#` in `[1050#]` |
| `##הקדמה \| המסירה: [מחקר זה (מס&#39; …) …]…##` | ❌ **NO** | interior `#` in `&#39;` |

So `HEADER_RE` is correct for ~99.8 % of headers and **all** of the "shapes" one worries
about (both mesirah forms, all section/verse/page markers). It fails **only** when a `#`
appears *inside* a `##…##` block.

### Related-but-separate: `MESIRAH_RE` metadata gap (not a stream bug)

`MESIRAH_RE = ##המסירה:\s*(…)##` (anchored to `##המסירה`) does **not** capture the
section-scoped form `##סעיף N | המסירה: …##`, so `works[i]['mesirah']` is left empty for
files whose location lives *only* in section headers (e.g. `Ytext689001` ENA 1501). This is
the gap the `mesirah_witnesses.py` docstring flags, and it is a **metadata-completeness**
issue, **not** stream pollution (the stripping via `HEADER_RE` still removes the header).
If Map-v2 wants the `mesirah` field populated for those files, widen the extractor to the
same shape `mesirah_witnesses.py` already uses:
`MES_RE = re.compile(r'##[^#]*?המסירה:\s*([^#]+?)\s*##')`. Out of scope for the stream fix.

---

## 2. Corpus scan — header shapes and what is / isn't matched

8,233 `.txt` files; 8,228 contain at least one `#`.

**Distinct header-template shapes inside `##…##` (top of 8,228 files), all MATCHED by the
current regex:**

| count | template (digits→N) | | count | template |
|---:|---|---|---:|---|
| 6,247 | `עמ' N` | | 320 | `פסקה N` |
| 5,389 | `המסירה` | | 300 | `גוף החיבור, עמ' N, טור א/ב` |
| 4,602 | `גוף החיבור, עמ' N` | | 252 | `גוף החיבור, עמוד N` |
| 1,306 | `קטע N` | | 258/253 | `עמ' N, טור א/ב` |
| 589 | `(גרסה)` | | 448 | `פרק א, משנה א` (+ all פרק/משנה variants) |
| 353 | `הלכות … פרק …` | | 325 | `קטע N, דף N, שו' N` |
| 200 | `סעיף N` | | … | (long tail of section/verse markers) |

None of these has an interior `#`, so all are stripped correctly.

**Header-form / hash census:**

| metric | files |
|---|---:|
| files containing section-form `##סעיף…## + המסירה` (form 2) | **11** — all fully stripped, verified clean |
| files with any `##…המסירה` header | 7,873 |
| files with ≥1 `#` surviving `HEADER_RE.sub` | 2,588 |
| — of which single-`#` `#…#` inline text markers only (see §3b) | 2,571 |
| — of which residual `##` (double) — the genuine defect | **17** |
| files with an ODD total `#` count (unbalanced) | 23 |

**Distinct UNMATCHED `##…##` header shapes (the only two that leak/corrupt):**

| # files | shape (verbatim examples) | interior `#` source |
|---:|---|---|
| 16 | `##המסירה: [600#] כת' אשקלון, כ"ד משמרות, נוה 52 …##` · `##המסירה: [1050#] כת' אספישי; ליסבון, מוזאון קרמו, 3877##` · `##המסירה: [300#] כת' יפו, פריי 892##` | approximate-date bracket `[N#]` |
| 1 | `##הקדמה \| המסירה: [מחקר זה (מס&#39; 2433/20) נעשה בתמיכת הקרן הלאומית למדע]&lt;/small>" mismsira='00' >Paris, Bibliothèque…##` (×47 in the one file) | HTML entity `&#39;` + stray HTML |

---

## 3. Impact measurement (real prep+`norm_stream` path)

Reference = the verified-correct fix `REC_NL = ##(?:[^#\n]|#(?!#))*##` (see §5). Comparing
the **actual pipeline output** (`HEADER_RE.sub` → `norm_stream`) against the fixed output,
**only 17 of 8,233 files** produce a different stream.

### 3a. Failure mode A — manuscript-location noise LEAKED IN (16 works, 342 letters)

All 16 are `מחבר לא ידוע` (anonymous) epigraphy inscriptions (`אפיגרפיה` genre). The leaked
Hebrew is exactly the manuscript-location string the task predicted (place names + the
editor's name), sitting at the head of the work's fingerprint.

| leaked letters | work | leaked header (verbatim) |
|---:|---|---|
| 45 | כתובת אשקלון | `##המסירה: [600#] כת' אשקלון, כ"ד משמרות, נוה 52 (לפי קריאת המהדיר סוקניק)##` |
| 30 | כתובת אספישי (שוורץ 16/17) | `##המסירה: [1050#] כת' אספישי; ליסבון, מוזאון קרמו, 3877##` |
| 25 | כתובת בית אל-חאדר, תימן | `##המסירה: [600#] כת' בית אל–חאצ'ר בתימן, נוה 106##` |
| 23 | כתובת הר הבית (פריי 1398/1399) | `##המסירה: [600#] כת' י, הר–הבית, פריי 1398 (ציור)##` |
| 21 | כתובת קורדובה | `##המסירה: [1050#] כת' קורדובה שבספרד##` |
| 19 | קלטיוד / כפר דבורה / טארנטו | `##המסירה: [600#] כת' קאלאטאיוד, ק"מ 205##` |
| 13–15 | כתובת יפו ×5, ירושלים (פריי 1390) | `##המסירה: [300#] כת' יפו, פריי 892##` |

**Five concrete before/after stream excerpts** (letters-only stream head; the leaked prefix
is exactly the location noise the fix removes):

```
כתובת אשקלון
  CURRENT stream: המסירהכתאשקלונכדמשמרותנוהלפיקריאתהמהדירסוקניק‖ משמרמשמרשמשמרא…
  FIXED   stream: משמרמשמרשמשמרא…
  leaked prefix : "המסירהכתאשקלונכדמשמרותנוהלפיקריאתהמהדירסוקניק"  (45 letters incl. editor 'סוקניק')

כתובת בית אל-חאדר, תימן
  CURRENT stream: המסירהכתביתאלחאצרבתימננוה‖ שערימחיתלומשמרהרביעי…
  FIXED   stream: שערימחיתלומשמרהרביעי…
  leaked prefix : "המסירהכתביתאלחאצרבתימננוה"  (25)

כתובת קורדובה, ספרד
  CURRENT stream: המסירהכתקורדובהשבספרד‖ מאירברגאתנוחנפשובצרורהחיימ…
  FIXED   stream: מאירברגאתנוחנפשובצרורהחיימ…
  leaked prefix : "המסירהכתקורדובהשבספרד"  (21)

כתובת יפו (פריי 892)
  CURRENT stream: המסירהכתיפופריי‖ הדאקבורתאדיודנברהדרביטרפון…
  FIXED   stream: הדאקבורתאדיודנברהדרביטרפון…
  leaked prefix : "המסירהכתיפופריי"  (15)

כתובת אספישי (שוורץ 16)
  leaked prefix : "המסירהכתאספישיליסבוןמוזאוןקרמו"  (30; place 'אספישי'/'ליסבון' + 'מוזאון קרמו')
```

**Magnitude in perspective:** 342 letters across 16 works is *negligible corpus-wide*
(the reference corpus is hundreds of millions of letters). But for these individual
inscriptions — which are very short — the noise is *proportionally* large and sits at the
head of the fingerprint. Secondary risk: `MIN_LETTERS = 150` gates whether a work is kept;
a borderline inscription could be pushed over the threshold by 13–45 junk letters, or its
short real fingerprint diluted by a location prefix — either can cause a spurious match to
an unrelated inscription that shares the same `המסירה כת' … נוה …` boilerplate vocabulary.

### 3b. Failure mode B — real body text DELETED (1 work, 30,677 letters = 7.4%)

`ספר הרקמה` (Ytext280002) has **94 `##` = 47 mesirah headers**, each of the malformed shape
`##…| המסירה: [מחקר זה (מס&#39; 2433/20) …]&lt;/small>" mismsira='00' >Paris…##`. The
`&#39;` puts a `#` inside every header, so `##[^#]*##` never closes a header on its own
opener; instead it pairs *header k's closing `##`* with *header k+1's opening `##`* and
strips the **real body text in between** as though it were a header.

Isolated proof of the mechanism (minimal reproduction):

```
input : ##hdr מס&#39; note## real BODY text אמר המחבר ##hdr2 &#39; note##
CURRENT: ##hdr מס&#39; note‖real BODY text אמר המחבר‖ deleted‖ hdr2 &#39; note##   (body GONE, labels kept)
FIXED  : ‖ real BODY text אמר המחבר ‖                                             (headers gone, body kept)
```

Corpus effect on the real file: **current stream 386,253 letters vs correct 416,930 —
30,677 Hebrew letters (7.4 %) of a major medieval Hebrew grammar silently deleted** from the
reference, *and* the 47 grant-note/`Paris` header labels leaked in their place. This single
file is the most damaging instance of the bug by two orders of magnitude, and it is a
prominent, high-value reference work (not a throwaway inscription).

### 3c. What the fix must NOT touch — single-`#` inline text markers (2,571 files)

`#…#` (single hash) is a **content** convention in Maagarim, not metadata: it brackets runs
of real Hebrew/Judeo-Arabic text (citation formulae `#דכתיב …#`, Arabic terms `#אלרייס#`,
blessing abbreviations `#נט' רח'#`, rubrics `#אלפתה#`, glosses, etc.). 2,571 files carry
**6.88 M** Hebrew letters inside such markers (median 32, max 197,006 per file). These must
stay in the stream — any fix that stripped `#…#` would delete millions of letters of real
text. The recommended fix leaves them **byte-for-byte identical** (verified: only 17 files
change corpus-wide; a 72,276-letter single-`#` control file is unchanged).

---

## 4. Cross-check: does this explain the Saadia (רס"ג) residue anomaly? — NO

REF-2 flagged that Saadia's Tafsir is fully present in the reference corpus, yet the
residue cluster naming it (unit 1430332) stayed **unidentified**. **The header bug is
unrelated**, confirmed three independent ways:

1. **The affected works aren't Saadia.** All 17 are 16 anonymous Hebrew *epigraphy
   inscriptions* + 1 Hebrew *grammar* (Ibn Janāḥ / Ibn Tibbon). None is רס"ג / a Tafsir /
   Judeo-Arabic. There is no clustering of affected works near Saadia — they cluster in one
   narrow niche: `מחבר לא ידוע … אפיגרפיה` with `[N#]` date brackets.
2. **The RSG reference texts never go through `HEADER_RE`.** Saadia's Tafsir units
   (`J:36–40 רסג-*-תפסיר-תורה`, Isaiah/Psalms/Daniel/megillot) live in the **JA corpus**,
   loaded by the second loop of `track1_build_ref.py` (lines 77–91), which calls
   `norm_stream(raw)` **with no `HEADER_RE.sub` at all** (verified: `HEADER_RE` does not
   appear in the JA segment). The bug physically cannot touch them.
3. **`rsg_query_test.md` points to recension distance, not reference noise.** Unit-1430332
   pages score **0/30** production-accept even when re-queried against an *RSG-only* index,
   while the positive control (pages Track-1 already labeled RSG) re-verifies **14/15**.
   That is a Track-1 *matching* problem (recension / textual-tradition distance, plausibly
   Bible+Tafsir interleaving), on the query side — orthogonal to reference-side header
   stripping.

Conclusion: leave the Saadia anomaly to the Track-1 matching post-mortem; fixing this header
bug will not move it.

---

## 5. Recommended fix (specify only — do NOT apply here)

Replace the stripping regex in `track1_build_ref.py` (line 24):

```python
# CURRENT (breaks on any interior '#'):
HEADER_RE = re.compile(r'##[^#]*##')

# RECOMMENDED:
HEADER_RE = re.compile(r'##(?:[^#\n]|#(?!#))*##')
```

**How it reads:** `##`, then a body of "any char that is not `#` or newline, OR a single `#`
not followed by another `#`", then `##`. In words: the body may contain isolated single `#`
characters (as in `[1050#]` and `&#39;`) but terminates at the first `##`, and never crosses
a line. This:

- **strips both mesirah forms** (plain `##המסירה:…##` and section `##סעיף N | המסירה:…##`) —
  identical to today for the 99.8 % common case;
- **strips interior-`#` headers** → removes the 342 leaked location letters (mode A) **and**
  correctly pairs `ספר הרקמה`'s 47 headers → restores the 30,677 deleted body letters (mode B);
- **cannot mis-pair / bridge** across a real header, because the body excludes `##`
  (the `#(?!#)` guard admits a lone `#` but refuses the `##` terminator);
- **cannot run away** on an unbalanced/unclosed `##` opener, because `[^#\n]` forbids
  crossing a newline (all real Maagarim headers are single physical lines);
- **does not touch single-`#` `#…#` text markers** (a lone `#…#` has no `##` closer, so the
  pattern simply doesn't match it) — the 6.88 M letters of real text in 2,571 files are
  preserved byte-for-byte.

**Verification already run (import-the-real-functions, full 8,233-file corpus):**

| check | result |
|---|---|
| files whose stream changes vs current | 17 (16 mode-A + 1 mode-B) |
| residual `##` left after the fix, corpus-wide | **0 files, 0 letters** |
| real text over-stripped vs current | **0** (single-`#` control file 72,276 → 72,276 unchanged) |
| location noise removed (mode A) | 342 letters across 16 works |
| body text restored (mode B) | 30,677 letters in `ספר הרקמה` |
| runtime / backtracking | 8,233 files × 4 normalizations in ~113 s, no catastrophic backtracking |

**Rejected alternatives:**
- `##[^\n]*?##` (lazy, allow interior `#`) — empirically 0 over-strip on *today's* corpus,
  but it *can* bridge two adjacent single-`#` markers into a spurious `##…##` if such a
  layout ever appears; the `#(?!#)` form is strictly safer and equally simple.
- `(?m)^\s*##.*##\s*$` and any greedy line-anchored variant — **over-strips 32,594 real
  letters** in `ספר הרקמה` (greedy `.*` eats body between two same-line headers). Do not use.

**Optional companion (metadata only, §1):** if Map-v2 also wants the `mesirah` field
populated for the 11 section-form files, widen `MESIRAH_RE` to
`re.compile(r'##[^#]*?המסירה:\s*([^#]+?)\s*##')` (matches both forms). Independent of the
stream fix; does not affect the stream.

---

## Provenance

Findings produced by four read-only analysis scripts (in the session scratchpad, not added
to the pipeline) that import the real `track1_build_ref.HEADER_RE` / `MESIRAH_RE` and
`normalize.norm_stream`: `explore_headers.py`, `measure_leak.py`, `choose_fix.py`,
`bidirectional.py` (+ `investigate_rakma.py`, `final_fix.py`). No pipeline script was
modified and no fix was applied — the change above is specified for folding into the Map-v2
reference rebuild.
