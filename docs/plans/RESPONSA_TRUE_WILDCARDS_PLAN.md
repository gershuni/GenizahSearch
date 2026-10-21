# Responsa wildcards: `*` as any sequence, `#` as the grammatical list

**Status:** plan, revision 5: **Phase 0 measured (2026-10-21, §3.6)** -- every item has its
numbers except the two halves of item 6 that cannot run yet: the "after" timings, which run with
Phase 1 and Phase 2 through the same subcommand, and the Linux-worker run, which is the owner's
(§3.6.6 holds today's desktop timings on both regex routes, on the laptop). Revision 4 was
**Codex SHIP** (four rounds on 2026-10-21: DO NOT SHIP, DO NOT SHIP, SHIP WITH FIXES, SHIP; §12).
It is the basis for Phase 1. Not implemented. Decisions 7 and 8 in §9 were taken on 2026-10-21
(exclusion on the page only; counts may change). Phase 0's numbers inform decision 1 (a word list beside the index
gives the same V0.8 pages in 0.14-1.8 s against 4.6-6.1 s per leading-star lookup; §3.6.1),
decision 2 (an OR of 600 or 1,200 scored term queries keeps distinct BM25 scores in under a second;
§3.6.3), decision 3 (`*נים *לים` has 99,981 candidate pages, 100,373 with bracket tolerance; §3.6.4),
decision 5 (one `regex_query` holds at most 60-120 stems: the 1,000-state limit, §3.6.2) and
decision 9 (12.95% of whole-manuscript junctions and 45.95% of part junctions are not consecutive
pages of one volume and one transcription; §3.6.5). Decisions 4 and 6 are still the owner's call
with no new number. Phase 0 also fixed §4's open question: the seven pages stage 1 missed are all
edge brackets, which the contract already covers, and sof pasuq joins maqaf as the punctuation kept
inside tokens (§3.6.4, §3.6.7).
**Tracker rows (P2, `docs/OPEN_ISSUES.md`, 2026-10-21; they reach `master-main` with PR #426):**
- "Responsa `*word` misses pages, and reads every whole-manuscript doc"
- "Responsa: `#word#` never keeps its suffixes; a star inside a word returns nothing"
- "Responsa: the query's first and last words have no word boundary"

**Date:** 2026-10-21. Written on the default effort tier (high). CLAUDE.md routes search-semantics
decisions to `xhigh`, so this plan states the semantics as proposals and leaves every choice to the
owner (§9).

## 1. What the owner asked for

- `*` matches **any sequence of letters inside one word**: at the start, at the end, in the middle,
  or several times, as in the Bar-Ilan Responsa Project. `*נים *לים` finds both `כהנים גדולים` and
  `לבנים כחולים`.
- `#` stays the **grammatical list**: 24 prefix forms for `#word` and 25 suffix forms for `word#`
  (each count includes the bare word), and every combination of the two for `#word#`.
- The first and last words of a query get a word boundary, like every other word.
- `#word#` stops losing its suffixes.
- The owner also asked (2026-10-21) whether indexing whole manuscripts as single documents was a
  mistake, and whether overlapping documents with cross-boundary queries would serve better. See §6.

## 2. How it works today (measured 2026-10-21, local index, 2,220,293 docs, V0.8 + V0.7)

A Responsa search has two stages.
1. **Retrieval.** Tantivy picks the candidate documents. It can only look up whole indexed words
   (tokens of the `hebword` tokenizer).
2. **Check.** The engine's regex runs over each candidate's text, and only regex matches become
   rows.

A page that stage 1 does not return is never checked.

| Query | Stage 1 looks up | Stage 2 accepts | Effect |
|---|---|---|---|
| `#X` | the 24 prefix forms, plus 6 bracket forms (grammatical forms boosted x3) | those forms | as documented |
| `*X` | **the same forms as `#X`** (`build_tantivy_query`, `wildcard == 'prefix'`; unboosted) | `\S*X` | another prefix is found only if the page also holds a listed form |
| `X*` | the 25 suffix forms, plus the non-final letter form | `X\S*`, final/non-final letter tolerant | the same, on the other side |
| `*X*` (main and LOCAL paths) | rewritten to `#X#`, with a warning (`execute_search` ~3843; LOCAL rewrites itself ~2153); the line-break path skips the rewrite and the guard | | next row |
| `#X#` (main and LOCAL paths) | `_apply_explosion_guard` sums an estimate over the whole query. One `#X#` is 24 x 25 = 600 > `MAX_EXPANDED_TERMS` (500), so the guard removes suffix expansion from every component | prefix forms only | `#כהנ# גדולים` and `*כהנ* גדולים`: 2 rows; plain `כהנים גדולים`: 360 |
| `כ*נים` | not parsed as a wildcard (`_parse_single_token`); the tokenizer turns the quoted text into the phrase `כ נים` | a literal `*` | 0 rows measured; only text containing a literal `*` could match |
| `*כ*ה*נ*ים` | `wildcard == 'pattern'`: the pattern is quoted, so it becomes a phrase of fragments | the right regex | 0 candidates and 0 rows measured; anything found would be found by chance |
| first / last word | | ordinary Responsa: **no word boundary** (separators sit only between words; `_whole_words` is off for Responsa). Line-group anchors can bound an edge; `within_document` has its own structure | `כהנים גדול`: Responsa 264 rows (260 are `כהנים גדולים`), Exact 4. A lone `ספר` accepts `מספרים` |

**Ground truth for `*כהנים *גדולים`.** Scanning `Transcriptions.txt` with the engine's own regex
finds **369** V0.8 pages that match. The desktop finds 361 and the website 360.
- The 8 missed pages: 7 Judeo-Arabic forms (`אלכהנים` / `ואלכהנים` / `באלכהנים` … `אלגדולים`) and
  one `גדולים'`.
- 15 of the found pages are found only because their whole manuscript holds a listed form
  elsewhere.

**Speed.** Responsa keeps one mixed query over pages, whole manuscripts and parts.
- For `*כהנים *גדולים` that is 3,723 candidates, 1,829 of them whole manuscripts (619M characters).
- Matching them takes 31 s with the `regex` module (desktop) and 9.8 s with stdlib `re`
  (production workers). The desktop search took 49-58 s in all.
- In the other modes, multi-word Literal and Variants read pages first, and use whole manuscripts
  only for matches across a page break (`_cross_page_terms`, `_cross_page_spans`). Regex-mode and
  position searches still check whole manuscripts. Single-word whole-word searches skip them.

**Two expansion copies and three pipelines.**
- The wildcard retrieval lists exist twice: `build_tantivy_query`, and `_execute_line_break_search`
  (~3380), which also has its own checker, `_build_line_break_regex`.
- LOCAL (`_build_local_responsa_query_and_regex`) shares those builders, but does its own rewrite
  and guard handling.
- The line-break path returns before the main rewrite, the guard and negative-component handling.
- The LOCAL-only early return skips the main restriction and exclusion filtering.
- ALL (main + LOCAL) reparses the main query **string** against the LOCAL schema (~4462).

**Documented but not working.** `docs/SEARCH_API.md` makes three promises the code does not keep:
a plain word is an "exact word match"; `*פ*ט*ר*פ*` finds "letters in order"; and `#שלום#` combines
prefixes and suffixes. The help's own example `*שלום → כבשלום` is found only by chance, because
`כב` is not one of the 24 prefixes.

## 3. Measurements for the new design (local index, this machine)

### 3.1 Looking up patterns in the index (`tantivy.Query.regex_query`, tantivy 0.25)

| Pattern (page docs) | Pages | Time |
|---|---|---|
| `כהנ.*` (star at the end) | 38,697 | 0.02 s |
| `כ.*נים` (star in the middle) | 36,107 | 0.59 s |
| `כ.*` | 1,479,019 | 0.81 s |
| `.*כהנים` (star at the start) | 53,180 | 8.95 s |
| `.*כ.*ה.*נ.*ים` | 54,865 | 9.68 s |
| `.*כהנ.*` | 85,318 | 8.48 s |
| `.*נים` | 518,052 | 10.62 s |
| `.*` | 1,919,738 | 25.85 s |

- **A leading star costs 7-10 s per word, every time.** The index scans its whole word list, and a
  repeat run is no faster. A star anywhere else is cheap.
- **`content` and `content_search` give the same counts.** `content_search` drops combining marks
  and quote marks, but keeps nikud (`strip_search_diacritics`).
- **Words with a bracket glued on** (`כהנים]`) add 59 pages to `.*כהנים`.
- **Every pattern hit gets the same score** (2.1124 for all ten top hits), so a pure star query has
  no relevance order. Plain words keep BM25.
- **`term_set_query`** with 810 terms takes 0.50 s.

### 3.2 A true `*נים *לים` end to end (V0.8 pages)

- 28,829 distinct words end in `נים` and 19,950 end in `לים`. That is why the current design, which
  lists forms, could not do this.
- **Stage 1:** two pattern lookups ANDed give **99,964** candidate pages in 11.4 s.
- **Stage 2:** fetching and checking all of them takes 8.4 s (193M characters) and leaves
  **4,217** pages.
- **Brute-force scan of the whole file:** 948,549 pages in 63 s, **4,231** pages with a match. The
  14-page difference is not explained yet (Phase 0).

### 3.3 Pattern phrases in the index (`regex_phrase_query`)

| Phrase (V0.8 pages) | slop 0 | slop 2 | Time |
|---|---|---|---|
| `.*כהנים .*גדולים` (`content`) | 367 | 374 | 18-20 s |
| `.*כהנים .*גדולים` (`content_search`) | 368 | 375 | 18-20 s |
| `כ.*נים .*גדולים` | 298 | 314 | 7-11 s |
| `.*נים .*לים` | error: "Phrase query exceeded max expansions 16615" | | |
| `כהנים גדולים` (plain words) | 277 | 278 | 0.04 s |

- It is nearly exact, and its hits are scored (53.9, 53.6, 49.4, … for the top five).
- But it is slower than two pattern lookups plus the check, and it refuses broad patterns.
- So it is not the main mechanism. At most it could be a shortcut for narrow patterns.

### 3.4 A word list beside the index

- I collected every word in `Transcriptions.txt` that ends in `נים`, using the `hebword` character
  class: **29,725 words, 16 s.**
- One `term_set_query` of all of them found **252,380** V0.8 pages in **3.09 s**. The pattern
  lookup `.*נים` found the same 252,380 pages in 9.12 s.
- So a word list built from the same text matches the index's own words, and it is 3x faster even
  for the broadest ending.

### 3.5 What whole-manuscript documents cost and add

**What the index holds:**

| | Count |
|---|---|
| V0.8 pages | 941,026 |
| V0.8 whole-manuscript docs | 216,911 (mean about 109,000 chars in a 300-doc sample; largest 5.7M) |
| V0.8 part docs | 3,519 |

**What they add.** Matches that cross a page break between consecutive pages of one volume, from
scanning the file:
- `*כהנים *גדולים`: **0** (all 369 are inside a page).
- `*נים *לים`: **12** (0.3% beside 4,231).
- Exact-mode rows found across a break (engine): 0 for `כהנים גדולים`, 0 for `אמר רבי`, and 28 of
  25,936 for `בני ישראל` (0.1%). All 28 join consecutive pages.

**What they cost under a true star:**
- `*כהנים *גדולים` pulls 1,872 whole manuscripts and 68 parts.
- `*נים *לים` pulls **43,227** whole manuscripts and 1,575 parts. At the sample mean that is about
  4.7 billion characters, several times all the V0.8 page text, just to look for 12 crossings.
- Pages first cuts the regex scanning, but not this: the engine still loads every aggregate
  candidate in full before it windows the text (Codex 1.12; `execute_search` ~4275-4292).

**How they are built.**
- One document per `sys_id`, in file order (`shared/indexer.py:374`). On the desktop, V0.8 pages are
  followed by V0.7 pages in the same document.
- Parts join pages across `sys_id`s.
- Every junction counts as a page break, with no check that the two pages are neighbours. No false
  crossing was seen in the 28 rows checked.

### 3.6 Phase 0 results (2026-10-21)

**Machine and method.** Every number below was measured on 2026-10-21 on the owner's laptop (ASUS ROG
Zephyrus G14, AMD Ryzen AI 9 HX 370: 12 cores / 24 threads, 63.1 GiB RAM, WD SN5000S NVMe SSD,
Windows 11 Home 10.0.26300, Python 3.11.9, tantivy-py 0.25.0, `regex` 2026.9.10) against the local
index (`C:\Users\gersh\Genizah_Tantivy_Index\tantivy_db`: 2,220,293 docs in 150 segments; 1,984,237
page docs of which 941,026 are V0.8; 232,450 whole-manuscript docs of which 216,911 start in V0.8;
3,606 part docs of which 3,519 start in V0.8) and `C:\GenizahSearch\Transcriptions.txt`
(1,470,391,571 bytes). The script is `scripts/measure_responsa_wildcards.py`, one subcommand per
item; each writes `<item>.json` and `<item>.log` under `--out-dir` (`_tmp/responsa_wildcards_phase0/`
here, gitignored). Times are `time.perf_counter()` wall times of one run unless a second run is
named; the items ran one after another, nothing else on the machine. "Pages" are page docs
(`scope:page`, V0.8 and V0.7) unless "V0.8 pages" is said (`AND source:"V0.8"`). "Search+collect"
fetches every hit's address (a set comparison needs them); "count" is `search(limit=1,
count=True)`. Opening the index (6 s) is not counted. The index times here are lower than §3.1's
(same machine, same index: `.*נים` 5.4-6.1 s against 10.62 s); §3.1 was measured with the desktop
app and other work running. Both are kept. Linux worker (the owner's run):
`python -I -X utf8 scripts/measure_responsa_wildcards.py <item> --index-dir /path/tantivy_db --corpus-file /path/Transcriptions.txt`
for `scan`, `wordlist`, `patterns`, `scored-terms`, `missing-pages`, `verify-tolerant`, `topology`,
`desktop-timings` (the last takes `--engines re` for the production route alone).

#### 3.6.1 The reversed word list (§5.6 C) -- `scan` (336 s), then `wordlist` (28 s)

- **Build.** One pass over `Transcriptions.txt`: 941,026 pages (every one with a uid; the same
  count as the index's V0.8 page docs), 796,066,613 characters, **159,793,249 tokens** after
  `strip_search_diacritics` (159,805,602 before folding; §4's 160,171,128 was counted another way,
  0.2% apart), **7,284,528 distinct words** under the tokenizer's class (the `regex` module's `\w`
  agrees with the Rust crate's: Alphabetic, marks, decimal digits, connector punctuation). Scan **313.8 s**, reverse + sort **9.8 s**, write
  **2.2 s**: **325.8 s in all** (§3.4's "16 s" collected one ending only).
- **Size.** 99,508,668 bytes (**94.9 MiB**) on disk as UTF-8 lines; loading it into a Python list
  takes **0.40 s** and **725 MiB** of process RSS (29 -> 754 MiB; 7.3 M `str` objects). The build
  process peaked at 1,682 MiB (the set and the sorted list together).
- **Lookups** (second run of `wordlist`; the first, colder run had the term sets at 0.78 s and
  4.52 s):

| Ending | Words | Binary search | `term_set_query`, pages | `regex_query('.*X')`, pages | V0.8 pages, count only |
|---|---|---|---|---|---|
| `כהנים` (narrow) | 246 | 0.1 ms | 53,010 in 0.17 s | 53,180 in 4.56 s | 26,170 both: **0.14 s** vs **5.54 s** |
| `נים` (broad) | 29,663 | 2.2 ms | 511,430 in 2.22 s | 518,052 in 6.11 s | 252,395 both: **1.76 s** vs **5.43 s** |

- **The page sets are identical on V0.8 pages** for both endings (26,170 and 252,395; §3.4 had
  252,380 with a 29,725-word list). On all pages the regex finds 170 and 6,622 more: V0.7 pages
  whose words are not in a V0.8-built list. The list must be built from every file the index holds
  (the V0.7 file is 1.6 GB more), or the regex stays for V0.7.
- The saving per leading-star word is 3.7 s for the broadest ending (1.8 s against 5.4 s) and
  5.4 s for a narrow one (0.14 s against 5.5 s, 40x).

#### 3.6.2 Bracket- and mark-tolerant patterns, and the pattern size limit -- `patterns` (47 s)

Tolerance classes between letters (and at both edges): brackets `[\[\]]*`; brackets+marks
`[\[\]\u0591-\u05C7]*` (the Hebrew points `content_search` keeps). All on `content_search` AND
`scope:page`; "plain" is the lookup the forms get without tolerance (one `regex_query` for a star
word, one `term_set_query` of the bare forms for a finite word; today's query also adds
edge-bracket terms, which the plain column here does not).

| Case | Pattern chars | Build | Count | Pages | Plain | Pattern-only pages |
|---|---|---|---|---|---|---|
| (a) `כ[\[\]]*ה[\[\]]*נ[\[\]]*.*` (star at the end) | 26 | 0.3 ms | 0.02 s | 38,698 | `כהנ.*`: 38,697 in 0.01 s | 1 |
| (a) the same, brackets+marks | 65 | 0.4 ms | 0.01 s | 38,703 | 38,697 | 6 |
| (a) `.*כ[\[\]]*ה[\[\]]*נ[\[\]]*י[\[\]]*ם[\[\]]*` (star at the start) | 42 | 0.6 ms | **3.8-4.3 s** | 53,239 | `.*כהנים`: 53,180 in 5.2-5.7 s | 59 |
| (a) the same, brackets+marks | 107 | 1.1 ms | 5.2 s | 53,240 | 53,180 | 60 |
| (b) 24 prefix forms of `כהנים`, brackets+marks | 2,947 | 3.6 ms | 0.03-0.04 s | 45,242 | `term_set` 24: 45,086 in 0.03 s | 156: 154 edge bracket, 1 in-word bracket, 1 mark |
| (c) 200 spelling variants of `כהנים` (`variants_extended`), brackets+marks | -- | **refused** (below) | | | | |
| (d) 600 forms of `#כהנ#`, brackets+marks | 70,627 | 43-54 ms | 0.04 s | 63,633 | `term_set` 600: 63,233 in 0.03 s | 400: 393 edge bracket, 3 in-word bracket, 4 mark |
| (d) 600 forms of `#כהנ#`, brackets only | 27,649 | 22 ms | 0.04 s | 63,629 | 63,233 | 396 |

- **A tolerant pattern costs what the plain one costs**: nothing measurable for a pattern that
  starts with a letter (the term-dictionary walk is pruned), and the same 4-6 s for a leading star.
  A 70,627-character pattern of 600 forms builds in 54 ms and searches in 40 ms.
- **What tolerance adds is small and almost all edge brackets**, which today's query already
  reaches through its extra terms: of the 400 pages only the 600-form pattern reaches, 393 are
  `כהנ[` / `]הכהנ`-style edge brackets, **3 have a bracket inside the word** and **4 a mark**. For
  `*נים *לים` the edge bracket is exactly what stage 1 misses today (3.6.4).
- **The size limit is 1,000 DFA states, not a character count.** The error is
  `ValueError: An invalid argument was passed: 'RegexQueryError: Compiled regex exceeds size limit of 1000 states'`
  (tantivy-fst's hard-coded `STATE_LIMIT`; the DFA is built eagerly). The largest alternation that
  builds, found by doubling from 8 and bisecting to the exact count:

| List | brackets+marks | brackets | no tolerance |
|---|---|---|---|
| distinct corpus words ending in `נים` (29,663 available) | **69** | 108 | 127 |
| spelling variants of `כהנים` (200 from `VariantManager`) | **125** | 186 | 186 |
| grammatical forms, `#כהנ#` then `#גדול#` (1,200) | **616** | 935 | 935 |

  One word's 600 `#X#` forms fit (they share prefixes and suffixes, so the DFA stays small), but
  **200 spelling variants in one `regex_query` do not, and neither do two words' forms in one
  pattern**; unrelated words cost about 8-16 states each. Implication for §5.1: "all expanded stems
  in one alternation" must be split into several `regex_query`s ORed (each under the limit), or the
  variant forms stay scored term queries (3.6.3) and the pattern carries only the star and the
  bracket/mark tolerance of the literal part. Decision 5 (stars with variants on) now has its cost:
  one pattern per 60-120 stems.

#### 3.6.3 Scored term queries with 600 and 1,200 forms -- `scored-terms` (3 s)

Boolean `Should` of one `term_query` per form (the 600 forms of `#כהנ#`: 24 prefixes x 25
suffixes from `shared/responsa.py`), ANDed with `scope:page`; `search(limit=10, count=True)`:

| Query | Build | Search | Candidates | Top-10 scores |
|---|---|---|---|---|
| OR of 600 `term_query` (`#כהנ#`) | 0.3 ms | **1.007 s** cold, **0.133 s** second run | 63,233 | 59.87, 49.46, 48.17, 47.46, 46.81, 46.76, 45.42, 44.81, 44.76, 43.39 (10 distinct) |
| `term_set_query`, the same 600 | 0.2 ms | 0.237 s | 63,233 | 1.1124 x 10 (constant) |
| OR 600 (`#כהנ#`) AND OR 600 (`#גדול#`): 1,200 `term_query` | 0.7 ms | **0.820 s** | 16,270 | 73.70, 62.92, 62.09, 61.40, 60.09, 58.97, 57.18, 51.33, 49.39, 49.28 (10 distinct) |
| OR of 1,200 `term_query` (both lists) | 0.6 ms | 0.357 s | 255,345 | 73.70 ... 51.33 (10 distinct) |
| `term_set_query`, 1,200 | 0.2 ms | 0.081 s | 255,345 | constant |

**The top hits keep distinct BM25 scores at 600 and at 1,200 clauses, in under a second**, so the
guard that downgrades `#X#` (`MAX_EXPANDED_TERMS` = 500) is not needed for retrieval cost, and the
scored form can serve decision 2 (order star results by the score of the query's finite words).

#### 3.6.4 The pages the index path misses for `*נים *לים` -- `missing-pages` (247 s), `verify-tolerant` (11 s)

The oracle here is the §8 one: fold with `strip_search_diacritics`, segment into whole words by
Exact's predicate (letters, decimal digits and connector punctuation continue a word; marks,
quotes and brackets are passed over; a run of brackets alone, `][`, is not a word), then `*` =
any letters, never a digit or `_`, applied to the two consecutive words. It is stricter than the
check §3.2 used (which gave 4,217 / 4,231), so the counts differ; the comparison is between the two
paths under one check.

- **Stage 1** (`regex_query('.*נים')` AND `regex_query('.*לים')` AND `scope:page` AND
  `source:"V0.8"`): **99,981** candidate pages in **10.24 s** (§3.2: 99,964).
- **Stage 2** (fetch each candidate, run the oracle): **3,865** pages pass; 193,552,137 characters
  read; 100.9-116.4 s over two runs, of which the fetches are 3.3-13.2 s and the pure-Python oracle
  the rest (the product's compiled regex did the same in 8.4 s in §3.2).
- **Brute force** over all 941,026 pages of the file. The prefilter looks for `נים` and `לים` in
  the page's text with every looked-past character removed (marks, quotes, brackets), so it is
  exactly as permissive as the oracle (`נ[י]ם` and `נִים` pass; a literal test, the first run's,
  skipped them -- Codex on PR #429 -- and gave the same 7 pages below): **101,087** pages
  prefiltered; **3,872** distinct page uids match; 130.2 s. (343 further matches were a second copy
  of a uid already matched: the file holds some pages twice, and both sides here count by uid.)
- **Difference: 7 pages the index path misses, 0 the other way.** All seven have one cause, **a
  bracket glued to the end of the token** (`נתקנים[`, `פנים[`, `כלשונים[`, `ובנים[`, `אדונים[` for the
  first word; `בקולים[`, `גדולים[` for the second): `.*נים` does not match a term that ends in `[`,
  while the word predicate looks past it. The seven (sys_id, page): 990000893670205171 p43,
  990001442940205171 p47, 990001562970205171 p115, 990001935620205171 p1, 990052073290205171 p2,
  990053164840205171 p1, 990053520270205171 p1. For each the index holds exactly one V0.8 page doc
  whose text equals the file's. **No maqaf, digit, Hebrew punctuation or tokenizer split is among
  them.**
- **The §4 contract covers this and is complete for this query**: §5.1's tolerant stage 1
  (`.*נ[\[\]֑-ׇ]*י[\[\]֑-ׇ]*ם[\[\]֑-ׇ]*` AND the same for `לים`)
  returns **100,392** candidates in **9.67 s** (411 more than the plain lookup, at the same cost)
  and **admits all seven** (`verify-tolerant`). The end-only form `.*נים[\[\]]*` gave 100,373 and
  also admitted all seven.

#### 3.6.5 Aggregate topology -- `topology` (27 s)

Read from each aggregate document's stored `boundaries` (what the engine's `_map_span_to_pages`
sees), not from `browse_map.pkl`: that file (133,440,543 bytes here, the real one) is rewritten
deduplicated by `(ie_id, p_num)` on load, so it no longer holds file order. A junction is
"consecutive" when both pages share source, `sys_id` and IE and the page number rises by one.

| | Whole-manuscript docs (`scope:system`) | Part docs (`scope:part`) |
|---|---|---|
| Documents | 232,450 (216,911 start in V0.8, 15,539 in V0.7) | 3,606 (3,519 / 87) |
| Page slots | 1,984,237 (max 2,643 in one doc) | 47,388 (max 544) |
| Junctions | 1,751,787 | 43,782 |
| Consecutive | 1,524,861 | 23,662 |
| **Not consecutive** | **226,926 (12.95%)**, in 216,981 docs | **20,120 (45.95%)**, in 3,546 docs |

Kinds (system docs): **213,566** V0.8 -> V0.7 inside one document (the V0.7 copy of the *same*
volume restarts at page 1 after the last V0.8 page, so the junction joins a manuscript's last page
to its own first page, and every page's text sits in the document twice); **8,113** IE (volume)
changes within one `sys_id`; **3,345** both at once; **1,902** page-number jumps forward (missing
pages). Kinds (part docs): **11,759** V0.8 -> V0.7 inside one `sys_id`; **8,244** V0.7 of one
`sys_id` -> V0.8 of the next (a part's folio order, but with the previous shelfmark's V0.7 text at
the seam); 104 `sys_id` changes in V0.7-only runs; 6 IE changes; 4 page jumps; 3 mixed.

Examples. V0.8 -> V0.7: `sys:990000571860205171` page 23 (V0.8) -> page 1 (V0.7), same
IE47676359; `sys:990000617200205171` page 64 -> page 1; `sys:990000622310205171` page 14 -> page 1.
IE change: `sys:990000465330205171` IE16012604 p3 -> IE160700818 p1, then IE160700818 p5 ->
IE169376719 p1, then IE169376719 p3 -> IE37436063 p1. Page jump: `sys:990000432020205171` p2 -> p4
(in V0.8 and again in V0.7); `sys:990000432000205171` p1 -> p3. Parts: `part:MS. Heb. a. 1/2` runs
990053385800205171 V0.8 p1-2, V0.7 p1-2, 990053385810205171 V0.8 p1-2, V0.7 p1-2, ...;
`part:MS. Heb. b. 18/5` p1 -> p4 -> p6.

So **one junction in eight of a whole-manuscript document is not a page break of the reading
order**, and the V0.8 -> V0.7 restart is nearly all of it: a crossing found there is the end of
the text joined to its own beginning. Seam documents (§6 B) built per source and per IE, with
page-number continuity, remove every kind above. This bears on decision 9.

#### 3.6.6 Desktop timings today for the §8 query set -- `desktop-timings` (442 s): the "before" half of item 6

This is the "before" of item 6, on the laptop. The "after" is the same subcommand once Phase 1 and
Phase 2 land; the Linux-worker run is the owner's and is **not** replaced by the stdlib-`re` column
below, which is the worker's matching route measured on this Windows laptop.

Method: `SearchEngine.execute_search(query, 'exact', 0, responsa_options={'responsa_mode': True,
'variants': False, 'ja': False, 'flex_spacing': False, 'bidirectional': False, 'variant_mode':
'exact'}, corpus_scope='genizah')`, the desktop's own call (`genizah_app.py::start_search`,
`desktop/gui_threads.py::SearchThread.run`), `Config.SEARCH_LIMIT` 50,000, the engine built with
`worker_mode=True` (no background fl_id build competing; the desktop's finishes before the user
searches) and `open_local=False`; one warm-up Exact search first (`ספר`, 25,041 rows, 6.8 s). "N+"
is `consume_last_search_cutoff()['capped']`. Each query ran once per engine: the desktop's `regex`
module route, and the production worker's stdlib `re` route (`isolated_matching(native=True)`),
which `shared/search_regex.py` lets a caller choose. A 600 s stop per query was armed and never fired.

| Query | Rows | `regex` (desktop) | N+ | Note |
|---|---|---|---|---|
| `#כהנים #גדולים` | 401 | **166.8 s** | no | 24 x 24 forms; aggregates read in full |
| `#כהנ# גדולים` | 2 | 20.4 s | no | the guard (600 > 500) drops the suffix list: 2 rows, not the 401+ a kept list gives |
| `כהנים גדול` | 264 | 9.7 s | no | the 264 of §1 (4 are whole words) |
| `ספר` | 24,653 | 2.9 s | **yes** | |
| `כהנים [2] גדולים` | 355 | 7.5 s | no | |
| `כהנים גדולים`, bidirectional | 350 | 10.7 s | no | |
| `כהנים AND גדולים` | 1,462 | 11.3 s | no | |
| `\|כהנים גדולים` (line start) | 41 | 6.8 s | no | |
| `כהנים גדולים -אהרן` | 138 | 6.8 s | no | |
| `*כהנים *גדולים` | 405 | **60.9 s** | no | today `*X` = the 24-prefix list |
| `*נים *לים` | 1,848 | **92.0 s** | no | today the 24 prefixes of `נים` and `לים` (`בנים`, `שנים`, ...) |
| `כ*נים גדולים` | 0 | 0.4 s | no | a star inside a word returns nothing (§2) |
| `*כ*ה*נ*ים גדולים` | 0 | 0.3 s | no | the same |

The same queries on the production worker's stdlib `re` route (`--engines re`, a separate run of
184 s; every row count was the same):

| Query | `regex` (desktop) | `re` (worker route) | `regex` / `re` |
|---|---|---|---|
| `#כהנים #גדולים` | 166.8 s | **35.0 s** | 4.8x |
| `#כהנ# גדולים` | 20.4 s | 6.1 s | 3.3x |
| `כהנים גדול` | 9.7 s | 8.3 s | 1.2x |
| `ספר` | 2.9 s | 2.6 s | 1.1x |
| `כהנים [2] גדולים` | 7.5 s | 6.2 s | 1.2x |
| `כהנים גדולים`, bidirectional | 10.7 s | **16.5 s** | 0.65x |
| `כהנים AND גדולים` | 11.3 s | **30.0 s** | 0.38x |
| `\|כהנים גדולים` | 6.8 s | 4.4 s | 1.5x |
| `כהנים גדולים -אהרן` | 6.8 s | 6.3 s | 1.1x |
| `*כהנים *גדולים` | 60.9 s | **23.8 s** | 2.6x |
| `*נים *לים` | 92.0 s | **36.4 s** | 2.5x |
| `כ*נים גדולים` | 0.4 s | 0.2 s | |
| `*כ*ה*נ*ים גדולים` | 0.3 s | 0.04 s | |

- **The big Responsa alternations are 2.5-4.8x slower on the desktop's `regex` route**, not the
  ~60x of Exact's generated patterns (§10's "desktop speed is out of scope" stands; the 167 s for
  `#כהנים #גדולים` is the number a desktop user sees today). The two paths that run lookaheads or
  reversed branches over whole-manuscript text (`AND`, bidirectional) are **slower on stdlib `re`**
  (2.7x and 1.5x), so a Linux worker is not faster on every query.
- Every heavy query here spends its time in the aggregate documents (§3.5): the same set after
  pages first (§5.4) is what Phase 1 and Phase 2 are measured against, with the same subcommand
  ("before and after", §7 item 6). The Linux-worker run is the owner's: the same command with
  `--index-dir`, `--corpus-file` and `--engines re`.
- Where the time goes (the engine's own `search_perf` line): `#כהנים #גדולים` reads 3,723
  candidate docs (pages and aggregates), keeps 983 after the regex check, 401 after
  de-duplication; the index query took 1.3 s, loading the documents 4.1 s, and **matching 148.6 s
  on `regex` against 26.0 s on `re`**. `*נים *לים`: 23,609 candidates, 2,459 kept, 1,848 rows;
  loading 7.5 s, matching 60.9 s against 18.7 s. The matching time is the aggregate documents'
  text (§3.5), which pages first (§5.4) and §6's crossing source address.

#### 3.6.7 Hebrew-block punctuation kept inside tokens -- `scan`

Counted on the folded tokens (`content_search`'s terms) of the whole V0.8 file:

| Character | Token occurrences | Distinct terms | Pages | Samples |
|---|---|---|---|---|
| maqaf U+05BE (baseline; §4 said 196) | **193** | 95 | 121 | `k־`, `־`, `х־`, `׃μ־`, `־ِ־` |
| paseq U+05C0 | **0** | 0 | 0 | -- |
| sof pasuq U+05C3 | **271** | 121 | 120 | `صب׃ב`, `׃`, `׃ְֶֶ`, runs of `ײ׃ײ׃...` |
| nun hafukha U+05C6 | **0** | 0 | 0 | -- |

Every sample is HTR noise (Cyrillic, Arabic and Yiddish-ligature characters glued to the mark), not
a Hebrew word split by punctuation. Sof pasuq is the one other character of the four the tokenizer
keeps inside tokens, at the same order of magnitude as maqaf (271 against 193 of 159.8 M tokens).
**§4's exception now names both** (maqaf and sof pasuq; Exact's predicate ends a word at either,
the token keeps both), and Phase 1's pinning test covers both; paseq and nun hafukha do not occur.

## 4. The contract (proposed)

- **Whole word = Exact's predicate.** `_starts_word` / `_ends_word` (`shared/search_engine.py:397`,
  `:405`):
  - a word continues through letters and decimal digits of any script and every connector
    punctuation character (Unicode `Pc`: `_`, U+203F and the like; `_continues_word`);
  - marks, and the in-word quotes and brackets of `_IN_WORD_QUOTES`, are looked past.

  Responsa and Exact then agree on what a whole word is. It is **not** the tokenizer's class: that
  keeps maqaf, brackets and marks inside a token, and copying it into lookarounds would disagree
  with Exact (Codex 1.9).
- **Retrieval may be wider than the check, never narrower**, with one measured exception.
  - Every stage-1 lookup must return every document the stage-2 check could accept.
  - A lookup tolerates brackets and marks inside tokens, so it never drops a page the check would
    keep.
  - **Words and tokens differ in two places: maqaf (U+05BE) and sof pasuq (U+05C3).** Exact's
    predicate ends a word at either (categories Pd and Po), but `hebword` keeps both inside the
    token. So `abc` inside `abc־def` is a whole word to the check, but not an indexed token, and a
    finite lookup misses it (Codex round 2, 2.1, reproduced); `abc׃def` is the same case.
  - Measured (Phase 0, §3.6.7): **193 tokens hold a maqaf and 271 a sof pasuq, of 159,793,249**
    V0.8 tokens; paseq (U+05C0) and nun hafukha (U+05C6) do not occur; every sample is HTR noise.
    Underscore (Pc, 970 tokens) continues a word under both rules.
  - Phase 1 accepts this gap explicitly: one test pins both characters, and the contract names
    them. The word list (§5.6 C) closes it, because it maps each word, by Exact's predicate, to the
    index tokens that hold it.
- **Brackets: one acceptance rule for every path.** Today the main path strips brackets from the
  text for a bracket-free query, then drops the page if the match does not reproduce on the
  original text. LOCAL checks the raw text instead (Codex 6.6).
  - Proposed: a bracket-free query matches the bracket-stripped text, and the row is **kept**, with
    its highlight mapped back to original offsets (`_unstripped_span`).
  - This adds rows, so it falls under decision 8.
- **`*`** matches zero or more **letters** (of any script) inside one word, as §1 asks. The
  looked-past marks, quotes and brackets are passed over transparently.
  - It does **not** consume digits or connector punctuation such as `_`, although Exact's predicate
    treats those as continuing a word (Codex PR #428 review). So `א*ב` does not match `א12_ב`.
  - Letting a star cross them would be an owner decision.
  - It never crosses a space or other punctuation. Any number of stars may appear, in any
    position.
- **`#`** means exactly the grammatical forms, in both stages: 24 prefix forms, 25 suffix forms,
  and for `#X#` every combination (600).
- **Every word is a whole word**, the first and last included. A star relaxes only its own side:
  `*נים` is a word that ends in `נים`.
  - `within_document` (textual `AND`): **every required component** must be a whole word, inside
    each lookahead, not only the returned term. Codex reproduced `alpha AND beta` accepting
    `alpha xbeta` under outer-only bounds.
  - `bidirectional`: both branches are bounded.
  - `flex_spacing`: components may join with no separator. The bounds apply to the outer edges of
    the joined unit, and the split forms stay valid.
  - Line groups: each group's edges are bounded. A `|` anchor still means line start or line end.
- **Order of expansion.** `%` (plene/defective), Judeo-Arabic and spelling variants expand the
  literal part, then the star applies. Retrieval and check must apply the same order (today
  wildcard branches skip the expanded forms on one side; Codex 2.17).
- **Negation: the page only, letters anywhere (owner, 2026-10-21; §9, decision 7).** Today it is a
  substring test on the row's `snippet` + `full_text` (`execute_search` ~4519, the line-break path
  ~3591, `ids_only` ~4109): for a row found through its whole manuscript that text is the whole
  manuscript, so `-כהנים` hides such a row because `אלכהנים` appears somewhere else in the
  manuscript. The decided rule: the excluded letters must appear **on the page the match is on**
  (for a crossing, on the pages the span covers), anywhere inside a word, as today. It is one rule
  for Responsa `-word` and for the "Exclude Words" box of Exact, Variants and Fuzzy, which has no
  wildcards: a whole-word form was offered and declined for that reason.
- OR groups and inline alternation are expanded on both sides by the compiler. Today their parser
  branches return before wildcard parsing (Codex 2.16).

## 5. Design

### 5.1 One word compiler, per index

A single compiler, in `shared/responsa.py` or a new `shared/responsa_match.py`, turns each parsed
component into two things.

**`retrieval`** is built **per index**: main, LOCAL, and both for ALL. Each uses its own schema and
its own `_has_content_search` / `_local_has_content_search`, because a query object cannot be
reparsed against another schema the way today's string is (Codex 2.4).
- Finite form lists keep **scored** term queries, so ranking is unchanged. That covers plain words,
  `#`, `%`, JA, variants and OR groups.
- **They also get a bracket-tolerant pattern lookup, ORed with the term queries.** The word's forms
  go in one alternation, with optional `[` / `]` between letters (`ש[\[\]]*ל[\[\]]*ו[\[\]]*ם`).
  - Why: §4's bracket rule accepts `של[ו]ם` for `שלום`, but the term queries add only edge
    brackets (`build_tantivy_query` ~2305). Codex round 3 reproduced zero candidates in an
    in-memory index.
  - Size: **56,131** V0.8 tokens (0.035%) have a bracket inside the word (`הש]חה`, `א[אמר`).
    Today's search misses these as well.
  - Speed: the pattern starts with a letter or a bracket, so the term-dictionary walk is pruned, like
    the cheap middle-star case (§3.1). Phase 0 item 2 measures it, including `#X#`'s 600 forms in
    one alternation.
  - Scoring: pages reached only through the pattern get the constant pattern score; the others keep
    BM25.
  - Phase 1 adds the end-to-end regression (`שלום` finds `של[ו]ם`).
- A star word becomes one `regex_query`, with all its expanded stems in one alternation. Brackets
  and marks are tolerated between letters, and the query runs on `content_search` when present.

**`check`** is the regex fragment for stage 2.
- A star becomes letters with the looked-past marks, quotes and brackets (§4), not `\S*`.
- Literal pieces are mark-tolerant (`make_mark_tolerant_pattern`), as plain words are today.
- `#X#`'s 600 forms are written as **length-grouped prefix trees** (`_variant_alternation`). It must
  not be the raw `_prefix_tree`, which needs equal-length forms: `['abc', 'abcd']` gives a tree that
  rejects `abc` (Codex 6.1, verified).

Every Responsa pipeline calls the compiler: `execute_search` (main), `_execute_line_break_search`
(including `_build_line_break_regex`), the LOCAL-only path and ALL. `_parse_single_token` learns
every star position.

Callers that log or reuse `t_query_str` move to query objects: logging, `_restriction_query`
composition, LOCAL reuse, and the `ids_only` path.

### 5.2 Whole words: membership and highlighting

**Which rows exist** is decided by Exact's code: `_accept` with `_whole_word_span`, through
`_first_accepted_match`, as Exact does today. (`closest()` belongs to `_FuzzyMatcher`; the ordinary
`Pattern` has no such method; Codex round 2, 2.2.) That is applied on every path, including line
groups, LOCAL, `ids_only` and the cross-page windows.

**Highlighting.** The row's `highlight_pattern` gets fixed-width lookarounds that approximate the
same edges. Variable-length lookbehind is impossible here, because `compile_search_regex` validates
with Python `re` (Codex 3.1).
- Exact looks past runs of quotes, brackets and combining marks, and treats every Unicode connector
  punctuation (Pc) as continuing a word. A one-character lookaround cannot follow a run.
- So the approximation can differ from membership, and the cases are pinned by hand-written tests:
  `[abc` vs `x[abc`, `"abc` vs `x"abc`, a combining mark before the match, maqaf, and a Pc character
  other than `_`.
- Where they differ, the row stays (membership rules), and only its highlight may be missing or
  partial. A corpus sample measures how often that happens. The consumers, all Python (Codex 3.2-3.10;
there is no browser-JavaScript consumer):
- desktop `mark_pattern_hits` (stdlib `re`);
- `ResultDialog`;
- the Joins Lab snippet functions (the `regex` adapter);
- the web anchor and compare viewers;
- the web expanded result text.

**Prerequisite bug.** The web expanded result text (`web/pages/search_results.py:1007-1014`) runs
the pattern over **HTML-escaped** text, so `'` and `"` have become `&#x27;` / `&quot;` before
matching. That already breaks highlighting of words with ASCII quotes, and it would break the edges.
Fix: match the raw text, then escape around the spans. Phase 1.

### 5.3 `#X#` and the guard

- **The guard measures real cost**, not an estimate of forms: terms in a term set, stems in one
  pattern alternation, starred words per query.
- **The same guard runs on every path.** The line-break path has none today.
- **Any downgrade is reported in the results header**, through the API and MCP too, not as a
  5-second status line.

### 5.4 Pages first

Responsa reads page documents first, as multi-word Literal and Variants do. The page-break step
must handle what today's crossing branch was not written for (Codex 2.10-2.14, 6.5):
- position validation (`text_position`; today the crossing branch exits before it);
- both directions (`bidirectional`, with reversed per-pair gaps);
- **every effective per-pair gap** in the window size, not one global gap;
- the `²` window joiner, which Python `\w` treats as a word character but Exact does not;
- restrictions (Oxford parts admitted by retrieval and then checked page by page, as today).

**Crossings are still incomplete after pages first.** A failed aggregate query today keeps the
page rows and reports nothing (`_page_hits_then_aggregates` ~4198). It must mark the search
incomplete ("N+").

**Previews and de-duplication.** Enabling Responsa previews needs the web caller changed too
(`web/pages/search.py` ~5263). The previewed rows must not later be withdrawn by exclusion, LOCAL
merging or de-duplication.

**Proposed de-duplication (Codex 6.8):**
- **First wins:** the page row, since pages are read first.
- **Exclusion** reads the matched page's text (decision 7): a page row is checked on its page and a
  crossing row on the pages its span covers, never on the whole aggregate it came through.
- **Crossing rows** keep their `page_highlights` from the aggregate (or seam).
- **Test:** fixture rows that share a uid between a page document and an aggregate, checked on
  which row survives, its `full_text` and its exclusion result, not only on UID counts.

Where crossings come from under a true star is §6.

### 5.5 How many candidates are read

- Today the display path reads `Config.SEARCH_LIMIT` (50,000) candidates. Pages first gives pages
  and aggregates separate limits; `ids_only` reads every document; ALL adds LOCAL candidates.
- `*נים *לים` has 99,964 candidate pages, and star hits all score the same, so a cut at 50,000 would
  keep an arbitrary half.
- Reading page candidates costs about 85 µs each here.
- The Responsa page limit is an owner decision (§9, decision 3), and it must hold on every path
  above.

### 5.6 The leading-star cost

Each word that starts with a star costs 7-10 s in stage 1, on both apps. Options:

- **A. Accept it.**
- **B. A reversed copy of every word in the index**, as a new field. A leading star becomes a
  trailing one (0.02 s). This changes the schema, so it needs a full rebuild on the server and on
  every desktop computer. The desktop keeps working on an old index with option A until the user
  re-indexes, never automatically.
- **C. A word list beside the index, built with it**, then a binary search and one `term_set_query`.
  §3.4 measured the same pages in 3.1 s instead of 9.1 s for the broadest ending. There is no schema
  change, but the list must stay in step with the index. LOCAL keeps option A, since its index is
  small and changes file by file.

Recommendation: A in Phase 2, then C. If §6's seams are chosen, they need a rebuild anyway, and B
can ride the same rebuild.

### 5.7 Texts that change with the behaviour (same PR as the behaviour)

- **Help.** `web/pages/help.py`, English and Hebrew: the syntax tables (~905-936 and ~1705-1741),
  the note that `*word*` is converted, and the `*שלום → כבשלום` example. The desktop help, wherever
  it repeats the table.
- **Search page.** The quick-syntax label at `web/pages/search.py:764`.
- **Query builders.** `web/components/joins_builder.py`, `desktop/join_workbench.py`, and the
  desktop Responsa table. Whether to add a "star inside" option is an owner decision.
- **API and MCP.** `docs/SEARCH_API.md`, the MCP description at `web/mcp_server.py:98`, and the
  `/api/search` field descriptions.
- **Expanded-terms count.** `responsa_expanded_count`: a star word shows "pattern".
- **Changelog.** A `changelog.d/` entry. The release texts follow the owner's rule: speed first,
  fuller results said positively.

## 6. Whole manuscripts or overlapping documents (the owner's question)

**Codex's enumeration (round 1, §4) of what uses whole-manuscript and part documents today:**

| Use | Needs a whole manuscript? |
|---|---|
| Phrase searches across a page break (Exact, Variants, Fuzzy, Responsa) | No. Seams or page pairs serve them up to their size. Today one match may cross two breaks, and long gaps may cross more. |
| Regex mode (a pattern may span any number of pages) | Yes, or text assembled on demand. |
| `within_document` (textual `AND`) | No. Retrieve each component separately and intersect by manuscript (and by Oxford part). Grouping the final page hits cannot do it. |
| Position and line-break searches | Yes, as far as they rely on position fields; pair-local "start" is not manuscript start. |
| Restriction, `_map_span_to_pages`, `ids_only`, `_first_match_in_pages` | Only the provenance, which pairs or seams can carry. |
| Negation | Today it checks whole-manuscript text; the domain is an owner decision (§4). |
| Composition search (unscoped, 50 candidates per chunk, frequency filtering) | Changes behaviour if the unit changes. |
| Lab, browse, `get_full_manuscript`, exports, passages | No. They read pages. Two exceptions: `_get_best_text_for_id` and `get_full_text_by_header` can return an aggregate by accident. |
| LOCAL | Writes pages only. |

**Codex's recommendation** is to keep whole manuscripts, with pages first, and to reconsider a seam
index only after measuring the aggregate-loading cost. That cost is now measured (§3.5). Under a
true star it dominates: about 4.7 billion characters loaded for 12 crossings.

**Options for where crossings come from:**
- **A. Whole manuscripts, as today.** No rebuild. Fine for narrow words; impractical for broad star
  words.
- **B. Add seam documents.**
  - **One small document per junction** between consecutive pages of one volume in one
    transcription (same IE, page number + 1, same source), and in part order for Oxford parts.
  - **It holds K words on each side of the junction, crossing further page breaks when pages are
    shorter than K.** A short match over three pages (`alpha` / `beta` / `gamma`, the existing
    regression `tests/test_cross_page_spans.py:93`) must lie inside the seam centred on its first
    junction. A seam of exactly two pages would miss it (Codex round 2, 2.4, reproduced).
  - **Provenance per page slice:** uid, source, and the slice's **original offsets** in its page.
    `_map_span_to_pages` assumes each boundary starts at a page start, so it must add the offsets
    back (Codex 3.2).
  - **The seam text is stored.** Rebuilding it from page documents would need a source-constrained
    fetch, and `get_full_text_by_id` has no source constraint.
  - **Consumer isolation (Codex 2.5).** A new `scope:seam` must be excluded explicitly from every
    unscoped consumer:
    - `_every_doc_has_scope` accounting;
    - composition's unscoped 50-candidate queries and their frequency counts;
    - `_get_best_text_for_id`;
    - `get_full_text_by_header`;
    - the home-page and `ids_only` document counts;
    - the scripts Codex listed.
  - **What it changes:** crossing retrieval becomes precise, and the check runs on a few thousand
    characters.
  - **What it keeps:** whole manuscripts stay for Regex mode, position searches,
    `within_document`, and spans longer than K words (routing covers multi-junction matches as
    well). They could retire later.
  - **It fixes the topology problem** (V0.8 and V0.7 joined; junctions in file order).
  - **Cost:** one rebuild on the server and on every desktop computer, and old-index compatibility
    (fall back to A, or C below, until the user re-indexes).
- **C. No crossings for star phrase searches.** No rebuild. Measured loss: 0 and 12 of 4,243
  (0-0.3%). Its limits (Codex round 2, 3.3-3.4):
  - **It is not silent anywhere.** The limitation goes in the results header, in the API and MCP
    responses (a completeness field, as for "N+"), and into refinement: `complete_chain` must treat
    such a result set as **incomplete** for restriction purposes. Today completion recognises only
    capped or interrupted searches, plus the line-break exception (`shared/refinement.py` ~190-264).
    Without this, a page-only set would silently become a complete restriction for the next step.
  - **It never applies to `within_document`.** In `alpha* AND beta` the operands can be pages
    apart; dropping aggregates would lose a manuscript-wide match, not a crossing. Such queries keep
    manuscript and part intersection (per-component retrieval, intersected by manuscript and by
    Oxford part) or aggregate checking, or are refused with a message. Manuscript-wide negation
    (decision 7), if chosen, likewise needs evidence beyond the returned page.
- **Page pairs** (pages i and i+1 whole) avoid the K limit within two pages, but index each page's
  text twice and retrieve less precisely than seams. They lose to B.

Recommendation: C in Phase 2 (with the limits above), then B in one index-format change, together
with §5.6 B or C, so that users rebuild once. Owner decision 9. Codex round 2 judged the seam
direction sound, with these corrections.

## 7. Phases

**Phase 0: measurements, no product code.** **Measured 2026-10-21: items 1-5 and 7, and the
"before" half of item 6, have their numbers in §3.6 (item N in §3.6.N), from
`scripts/measure_responsa_wildcards.py`. Still open in item 6: the "after" timings (run the same
subcommand when Phase 1 and Phase 2 land) and the Linux-worker run (the owner's).**
1. The full reversed word list (§5.6 C): build time and size, and lookup plus `term_set_query` time
   for narrow and broad stems.
2. Bracket- and mark-tolerant patterns, for star words and for finite words (§5.1): DFA size and
   time, including 200 stem alternatives (variants on) and `#X#`'s 600 forms. Find the
   tantivy-fst regex size limit before a user does.
3. Scored term queries with 600 and 1,200 forms (two `#X#` words).
4. The 14 pages between 4,217 and 4,231.
5. Aggregate topology: how many junctions in whole-manuscript and part documents are not
   consecutive pages of one volume and one transcription.
6. Desktop timings of the §8 query set before and after; the same on a Linux worker when the owner
   can run it.
7. Hebrew-block punctuation kept inside tokens besides maqaf (paseq, sof pasuq, nun hafukha): a
   count, to size the §4 exception.

**Phase 1: one meaning on every path, for finite words.** It covers the main path, line-break, the
LOCAL-only path, ALL and `ids_only`, all through the compiler (§5.1):
- whole words by Exact's predicate (§5.2), including every `within_document` component, both
  `bidirectional` branches and the `flex_spacing` rule;
- the guard measured by real cost on every path, with length-grouped trees (§5.3);
- exclusion checked on the matched page(s) only, in every mode (decision 7), with a test where the
  excluded letters sit on another page of the same manuscript;
- the web highlight fixed to match raw text before escaping.

**Unsupported star shapes** are refused with a visible message on every surface. That includes Joins
Lab: both executor adapters and the cross-side matching swallow exceptions today
(`web/joins_executor.py:82`, `desktop/join_workbench.py` ~1819, `shared/joins_lab.py:402`), so a
parser error would show as no results. It also includes the API and MCP, which need a defined error
contract for jobs.

**Counts change.** For example, `כהנים גדול` goes from 264 rows to the 4 Exact finds. The owner's
go-ahead is recorded (§9, decision 8, 2026-10-21); the release notes carry a sentence on it.

**Phase 2: true `*`.**
- Star retrieval and pages first (§5.4).
- The crossing source decided in §6 (recommended: C, no crossings for star words, with a notice).
- The Responsa page limit (§5.5).
- Every text in §5.7.

**Phase 3: the index format.** Seam documents (§6 B) and the leading-star speed-up (§5.6 B or C) in
one rebuild, after the owner decides.

## 8. Gates (each with the mutation that must turn it red)

**1. Ground truth on the real data: `scripts/verify_responsa_wildcards.py`, one query set per
phase.** It FAILS, never skips, when `Transcriptions.txt` or the index is missing, as
`scripts/verify_review_artifact.py` does.
- **The oracle is independent.** It does not import `build_regex_pattern` or the compiler.
- **It folds as the index does:** `strip_search_diacritics` first. Tokenising first splits `a'b`
  where the folded field has `ab` (Codex 5.1).
- **It segments into semantic words by Exact's predicate, not into index tokens** (Codex round 2,
  2.1). A token oracle would repeat the maqaf omission.
- **It treats words by the §4 contract:** stars through `fnmatch` over letters (plus the
  looked-past marks, quotes and brackets; never digits or `_`),
  and the grammatical lists.
- **It maps every span back to original (unfolded) coordinates** before comparing.
- **It uses ordered page fixtures with physical order and source,** so it can rule on crossings.
- **It compares witnesses in both directions,** meaning the page plus the span of the first
  acceptable match, not only page sets:
  - missing witnesses fail;
  - extra witnesses fail too (removing `_accept` adds false witnesses rather than losing valid
    ones);
  - a wrong span on a page that also has a valid match must show.
- **The documented maqaf gap (§4)** is listed as an expected difference, by count.
- **Mutations, each named by the witness it loses:**
  - star retrieval back to the fixed list: the 7 Judeo-Arabic witnesses of `*כהנים *גדולים`;
  - the `_accept` edge check removed: the false `כהנים גדול` witnesses whose last word continues,
    which appear as extra witnesses;
  - the raw `_prefix_tree` in place of the grouped trees: a `#X#` witness whose form is a prefix of
    a longer form;
  - the guard restored to 500: the `#כהנ# גדולים` witnesses ending in a suffix;
  - the crossing step disabled: one fixture crossing witness (Phase 1 and the Literal modes).
- **Query set.**
  - Phase 1: `#כהנים #גדולים`, `#כהנ# גדולים`, `כהנים גדול`, a lone `ספר`, a `[2]` gap, a
    bidirectional pair, an `AND` query, a `|` line query, a negation, a crossing fixture.
  - Phase 2 adds `*כהנים *גדולים`, `*נים *לים`, `כ*נים גדולים` and `*כ*ה*נ*ים גדולים`.

**2. Unit tests with expectations written by hand,** never generated by the compiler:
- the parser (every star position; `#`, `%` and `-` combinations; OR groups; inline alternation);
- mixed-length prefix forms;
- every `AND` operand;
- forward and reverse branches;
- the flex-spacing exception;
- raw and normalised quote and bracket cases;
- both regex runtimes (`re` and `regex`).

**3. Real parity**, replacing what `tests/test_responsa_parity.py` claims but does not do (it calls
the same builder twice and compares counts; Codex 5.3):
- one fixture corpus indexed as main and as LOCAL must give the same witnesses **over their common
  domain**: matches inside one page, and `AND` within one page. LOCAL stores pages only, so
  crossings and cross-page `AND` are main-only by design. They get separate tests that pin the
  difference (Codex round 2, 2.6).
- the desktop `SearchThread` and the web `execute_search` caller, with the same query and options,
  must get the same options to the engine and the same error presentation;
- Joins Lab must show a refusal as a message, not as zero results.

**4. Windows.**
- several joined windows;
- both edges and both directions;
- effective per-pair gaps;
- a span that crosses more than one break, including the existing three-page regression
  `tests/test_cross_page_spans.py:93`;
- a match beside the `²` joiner.

In Phase 3 the same cases run against seams.

**5. Existing pins, updated deliberately:**
- `tests/test_responsa_core.py:617-622` (`MAX_EXPANDED_TERMS == 500` and the cascade);
- `tests/test_search_api.py:505-515` (the warning transport, which uses mocked warnings);
- `tests/test_join_workbench_builder.py`, which tests a copy of the builder, so a live-widget test is
  needed too.

There is no dedicated test of the `*word*` rewrite warning. Phase 2 adds one that asserts a
supported true star is **no longer rewritten**, rather than preserving the obsolete warning.

## 9. Decisions for the owner

1. **Speed of a star at the start of a word.** Each such word adds 7-10 seconds to every search.
   - Accept that for now.
   - Add a reversed copy of every word to the index: near-instant, but it needs one rebuild on the
     server and on every desktop computer.
   - Keep a separate word list beside the index: no rebuild, and measured at 3 seconds instead of 9
     for the broadest ending.
2. **Order of results for star searches.** The index gives every star match the same score. Order
   them by shelfmark, by how often the words occur on the page, or by the score of the query's other,
   non-star words?
3. **How many candidate pages a search may read before it says "N+".** Today 50,000. `*נים *לים`
   has about 100,000; reading all of them took 8 seconds here.
4. **Very short or bare stars.** `*` alone matches every word; `*ים` reaches 1.35 million pages.
   Allow, warn, or refuse with a message suggesting a gap `[1]`?
5. **Stars with spelling variants or Judeo-Arabic on.** Allowed (slower; one bigger pattern per
   word), or the star word is searched without them?
6. **Does a star match zero letters?** Recommended yes: `*כהנים` includes `כהנים` itself.
7. **Negated words.** Today `-כהנים` hides a result when the letters `כהנים` appear anywhere in
   the text it was found in, even inside `אלכהנים`. Sometimes that text is the page, sometimes the
   whole manuscript, depending on how the result was found. Recommended: a whole word, checked on
   the matched page only, with `-*כהנים` for the broad form. Or should the whole manuscript count,
   always?
   **Decided 2026-10-21: the page only, letters anywhere, in every mode.** The "Exclude Words" box
   of regular search has no wildcards, so the whole-word form was declined. §4, §5.4, §7 and §10
   are updated.
8. **Counts change for existing searches.** Edge words become whole words, so some searches return
   fewer rows (`כהנים גדול`: 264 to 4), and star searches return more. API and MCP users get the
   same change. This needs a go-ahead and a sentence in the release notes.
   **Decided 2026-10-21: go ahead**, bracket rule included. Phase 1 may start.
9. **Page-break matches under a star.** Whole manuscripts would mean reading billions of characters
   for a handful of matches.
   - Skip page-break matches for star phrase searches (measured loss 0-0.3%). This is said on
     screen and in the API, and a later "search within these results" knows the list is not
     complete. "Anywhere in the manuscript" (`AND`) searches are never cut this way.
   - Or add small "seam" documents at every page break: precise and fast, but it needs one rebuild
     on the server and on every desktop computer.

## 10. What this does not change

- Exact, Variants, Fuzzy and Regex modes, apart from §6's shared crossing source if B is chosen and
  the exclusion domain (decision 7: the matched page, in every mode).
- Composition and letter-level (passage) search.
- The lists themselves (24 prefixes, 25 suffixes).
- The desktop's `regex`-module speed is out of scope. If the desktop is still slow after pages
  first, it gets its own tracker row.

## 11. Risks

- `within_document` with a star needs whole manuscripts, or per-component manuscript intersection.
- tantivy-fst regex size limits, and Rust regex escaping of user-typed stems.
- Website worker memory and the 3-minute limit with large candidate sets.
- Bracket tolerance is not uniform today: the main path may drop a page whose bracket-stripped
  match does not reproduce on the original text, and LOCAL checks raw text (Codex 6.6). The
  contract must state the intended behaviour, and the oracle must test it.
- Saved searches and search-within chains (`complete_chain`, `RefinementStep`) replay with the new
  meaning. Line-break steps cannot be completed today (`shared/refinement.py` ~241).

## 12. Review history

- **Phase 0 round (Codex on PR #429, 2026-10-21): three findings, all folded in.** P1: the
  brute-force oracle's prefilter was a literal substring test, so a page whose ending held an
  internal bracket or mark (`נ[י]ם`, `נִים`) was skipped before the check ran; the prefilter now
  removes the looked-past characters first, and item 4 was re-measured (§3.6.4). P2: sof pasuq
  joins maqaf in §4's exception and in Phase 1's pinning test. P2: Phase 0 is "measured", not
  "done": item 6's "after" timings and its Linux-worker run are still open (status line, §7).
- **Round 1 (Codex, 2026-10-21): DO NOT SHIP.** Four HIGH findings, all folded in:
  - the raw `_prefix_tree` drops forms (§5.1);
  - "whole word" was two conflicting definitions, and outer bounds did not bound `AND` operands
    (§4, §5.2);
  - Phase 1 left the line-break, LOCAL-only and Joins Lab paths with other meanings (§7);
  - the cited parity tests cannot fail on a shared semantic error (§8).

  Also folded in:
  - claim corrections: the counts include the bare word; the guard is query-wide; internal stars
    give phrase fragments; two copies, not three; `content_search` keeps nikud; the per-path caps;
    pages first does not cut aggregate loading;
  - the web highlight escaping bug (§5.2);
  - the silent aggregate-query failure (§5.4);
  - the whole-manuscript consumer enumeration (§6).
- **Round 2 (Codex, 2026-10-21): DO NOT SHIP; "Phase 0 can proceed".**
  - Round-1 status: 6.1, 6.3, 6.5, 6.7, 6.9 and 6.10 fixed; 6.2, 6.4, 6.6 and 6.8 partly fixed.
  - New: maqaf words vs index tokens (HIGH; measured at 196 of 160M tokens and documented as a
    pinned exception, §4); `_first_accepted_match` instead of `closest` (§5.2); a wider
    approximation case list (§5.2); seams across short pages (HIGH, Phase 3; §6); seam consumer
    isolation and provenance (§6); the main/LOCAL parity domain (§8); the interim's reach into the
    API, MCP and refinement, and its exclusion of `within_document` (§6).
  - Decided in the plan: the bracket rule (§4) and the de-duplication rule (§5.4).
- **Round 3 (Codex, 2026-10-21): SHIP WITH FIXES.**
  - All twelve round-2 items fixed.
  - One new MEDIUM: finite-word retrieval did not support the bracket rule (`של[ו]ם`). Folded into
    §5.1, sized at 56,131 V0.8 tokens.
- **Round 4 (Codex, 2026-10-21): SHIP.**
  - The bracket fix was confirmed in an in-memory index with the `hebword` tokenizer: the OR
    retrieves `של[ו]ם` and keeps every term-query hit and its BM25 contribution.
  - No new HIGH or MEDIUM. The pattern compile cost and the 600-form case stay Phase 0
    measurements.
