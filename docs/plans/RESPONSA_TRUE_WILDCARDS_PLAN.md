# Responsa wildcards: `*` as any sequence, `#` as the grammatical list

**Status:** **Phase 3 being built (2026-10-22) in two PRs** (the owner): PR #442 a star at the end of
a word or inside it (`X*`, `X*Y`), deployable on today's format-1 index -- **implemented, with its
fixture gates (the oracle run on format 2 and format 1 built by the Indexer; every new test red
under a named mutation); its real-data gates (oracle, `verify_search_combinations.py`, timings on
both formats) are the local session's, commands in the PR**; the second PR (stacked on it) leading
stars and decision 10 -- **implemented too, with its fixture gates; its real-data gates (the oracle's
leading-star queries, `missing-pages --via-engine`, `crossing-diff`, the timings) are the local
session's, commands in that PR**. The points the plan left open are settled in §7 ("Phase 3, settled
before building"). **Phase 2 implemented (2026-10-22, PR #440 on the design PR #438)**: the index format,
built locally and gated (§7 Phase 2); the server rebuild is the owner's, and today's page-break matches
move to the seams in Phase 3 (decision 10, 2026-10-22).
**Phase 1 implemented (2026-10-22)** -- the engine on
`claude/responsa-wildcards-phase1-36d505`, the surfaces stacked on it on
`claude/responsa-wildcards-phase1-surfaces` -- with its gates: the §8 oracle
(`scripts/verify_responsa_wildcards.py`) agrees with the engine on all nine Phase 1 queries on the
real data (0 differences) and its four named mutations each fail it; the "after" timings are in
§3.6.6. **Phase 2 designed (2026-10-22, §6.1)**: the seams in their own index inside
`tantivy_db/`, K = 30 words a side, runs plus Oxford part bridges (1,541,186 seams), the
reversed-words field on every document, a format marker with today's routes as the fallback;
**built locally 2026-10-22**: the whole index with both in 21.7 min, 15.30 GB (today's: 13.86 GB);
V0.8 only, as production holds it, 7.74 GB against 6.31 GB for format 1 built the same way.
**Decided 2026-10-22 (decision 10): today's page-break matches move to the seams in Phase 3**,
together with Responsa's pages-first change. Phase 3 is not implemented. Plan revision 6: **all nine §9 decisions taken (2026-10-21)** and the phases
resequenced -- Phase 1 unchanged (whole words on every path, no rebuild), **Phase 2 is the index
format** (seam documents and the reversed-words field on one rebuild, the engine's fallback for an
old index, the server rebuild and the desktop re-index), **Phase 3 is true `*`** on the new index.
**Phase 0 measured (2026-10-21, §3.6)** -- every item has its numbers except the two halves of
item 6 that cannot run yet: the "after" timings, which run with Phase 1 and Phase 3 through the
same subcommand, and the Linux-worker run, which is the owner's (§3.6.6 holds today's desktop
timings on both regex routes, on the laptop). Revision 4 was **Codex SHIP** (four rounds on
2026-10-21: DO NOT SHIP, DO NOT SHIP, SHIP WITH FIXES, SHIP; §12). Decisions 7
and 8 were taken first (exclusion on the page only; counts may change); decisions 1-6 and 9 were
taken from Phase 0's numbers: 1 from §3.6.1 (identical page sets, 0.14-1.8 s against 4.6-6.1 s
per leading-star lookup), 2 from §3.6.3 (an OR of 600 or 1,200 scored term queries keeps distinct
BM25 scores in under a second), 3 from §3.6.4 (99,981 candidate pages, 100,392 with bracket
tolerance), 5 from §3.6.2 (one `regex_query` holds at most 60-120 stems: the 1,000-state limit),
9 from §3.6.5 (12.95% of whole-manuscript junctions and 45.95% of part junctions are not
consecutive pages of one volume and one transcription), 4 and 6 on their own terms. Phase 0 also
fixed §4's open question: the seven pages stage 1 missed are all edge brackets, which the contract
already covers; sof pasuq joins maqaf, and a foreign mark inside a Hebrew word is a third, counted
gap (§3.6.4, §3.6.7, §3.6.2).
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

#### 3.6.2 Bracket- and mark-tolerant patterns, and the pattern size limit -- `patterns` (46-70 s), `marks` (4 s)

Tolerance classes between letters (and at both edges): **brackets** `[\[\]]*`; **brackets+Hebrew
marks** `[\[\]...]*` over the Hebrew block's category-M code points only (U+0591-05BD, 05BF, 05C1-05C2, 05C4-05C5, 05C7: te'amim and points, not the block's punctuation, which the oracle treats as word boundaries); **brackets+all marks** `[\[\]\p{M}]*`,
every Unicode mark, which is the class the oracle looks past (`content_search` keeps all marks
outside U+0300-036F; a Hebrew-only class would miss a Hebrew word carrying an Arabic mark, Codex
round 2 on PR #429). All on `content_search` AND `scope:page`; "plain" is the lookup the forms get
without tolerance (one `regex_query` for a star word, one `term_set_query` of the bare forms for a
finite word; today's query also adds edge-bracket terms, which the plain column here does not).

| Case | Pattern chars | Build | Count | Pages | Plain | Pattern-only pages |
|---|---|---|---|---|---|---|
| (a) `כ[\[\]]*ה[\[\]]*נ[\[\]]*.*` (star at the end) | 26 | 0.3 ms | 0.02 s | 38,698 | `כהנ.*`: 38,697 in 0.01 s | 1 |
| (a) the same, brackets+Hebrew marks | 173 | 0.4 ms | 0.01 s | 38,703 | 38,697 | 6 |
| (a) the same, brackets+all marks | 41 | **23-27 ms** | 0.01-0.02 s | 38,703 | 38,697 | 6 |
| (a) `.*כ[\[\]]*ה[\[\]]*נ[\[\]]*י[\[\]]*ם[\[\]]*` (star at the start) | 42 | 0.6 ms | **3.8-4.3 s** | 53,239 | `.*כהנים`: 53,180 in 5.2-5.7 s | 59 |
| (a) the same, brackets+Hebrew marks | 287 | 1.0-1.1 ms | 5.2-5.5 s | 53,240 | 53,180 | 60 |
| (a) the same, brackets+all marks | 67 | **49-70 ms** | 4.0-5.5 s | 53,240 | 53,180 | 60 |
| (b) 24 prefix forms of `כהנים`, brackets+Hebrew marks | 7,915 | 3.6-4.4 ms | 0.03-0.04 s | 45,242 | `term_set` 24: 45,086 in 0.03 s | 156: 154 edge bracket, 1 in-word bracket, 1 mark |
| (b) the same, brackets+all marks | -- | **refused: 1,000 states** | | | | |
| (c) 200 spelling variants of `כהנים` (`variants_extended`), either mark class | -- | **refused** (Hebrew marks: 1,000 states; all marks: 10,485,760 bytes) | | | | |
| (d) 600 forms of `#כהנ#`, brackets+Hebrew marks | 189,643 | 42-68 ms | 0.04 s | 63,633 | `term_set` 600: 63,233 in 0.03 s | 400: 393 edge bracket, 3 in-word bracket, 4 mark |
| (d) the same, brackets+all marks | -- | **refused: 10,485,760 bytes** | | | | |
| (d) 600 forms of `#כהנ#`, brackets only | 27,649 | 22 ms | 0.04 s | 63,629 | 63,233 | 396 |

- **A tolerant pattern costs what the plain one costs at search time**: nothing measurable for a
  pattern that starts with a letter (the term-dictionary walk is pruned), and the same 4-6 s for a
  leading star. Build time is where the class shows: a 189,643-character pattern of 600 forms with
  the Hebrew class builds in 68 ms; the all-marks class makes a single-branch pattern cost 23-70 ms.
- **What tolerance adds is small and almost all edge brackets**, which today's query already
  reaches through its extra terms: of the 400 pages only the 600-form pattern reaches, 393 are
  `כהנ[` / `]הכהנ`-style edge brackets, **3 have a bracket inside the word** and **4 a mark**. For
  `*נים *לים` the edge bracket is exactly what stage 1 misses today (3.6.4).
- **Two size limits, neither a character count.** The first is
  `ValueError: An invalid argument was passed: 'RegexQueryError: Compiled regex exceeds size limit of 1000 states'`
  (tantivy-fst's hard-coded DFA `STATE_LIMIT`; the DFA is built eagerly); the second, reached only
  with the all-marks class, is `... Compiled regex exceeds size limit of 10485760 bytes` (the
  compiled program). The largest alternation that builds, found by doubling from 8 and bisecting
  to the exact count:

| List | brackets+all marks | brackets+Hebrew marks | brackets | no tolerance |
|---|---|---|---|---|
| distinct corpus words ending in `נים` (29,663 available) | **1** | **69** | 108 | 127 |
| spelling variants of `כהנים` (200 from `VariantManager`) | **0** | **125** | 186 | 186 |
| grammatical forms, `#כהנ#` then `#גדול#` (1,200) | **1** | **616** | 935 | 935 |

  So with the Hebrew class one word's 600 `#X#` forms fit (they share prefixes and suffixes, so
  the DFA stays small), but **200 spelling variants in one `regex_query` do not, and neither do two
  words' forms in one pattern**; unrelated words cost about 8-16 states each. **The all-marks class
  (about 2,400 code points, many UTF-8 byte ranges) spends the whole budget on one branch**: it
  fits a single star word (3.6.4 used it) and nothing with an alternation.
- **How much the mark classes are worth** (`marks` subcommand, 4 s over the 7,284,528 distinct
  folded words): **200** words hold a Hebrew mark; **93,673** hold a mark of another script, of
  which **5** also hold a Hebrew letter (HTR noise); the rest are Arabic-script words (hamza above
  U+0654 in 80,028 words, hamza below U+0655 in 10,915, maddah U+0653 in 3,815). The HTR text carries
  almost no nikud, so for Hebrew queries the mark question is about 205 words in 7.3 million;
  brackets are the tolerance that matters (56,131 tokens, §5.1).
- Implication for §5.1: "all expanded stems in one alternation" must be split into several
  `regex_query`s ORed (each under the limit), or the variant forms stay scored term queries (3.6.3)
  and the pattern carries only the star and the bracket tolerance of the literal part, with the
  Hebrew mark class at most; the all-marks class fits a lone star word only. **A finite-word
  lookup without the all-marks class therefore misses a Hebrew word carrying a non-Hebrew mark**
  (stage 1 runs first, so the oracle never sees such a page): that is a retrieval gap of the same
  kind as maqaf's, 5 distinct words in the corpus, and §4 names it with that count; the word list
  (§5.6 C) closes it, because it maps each word, by Exact's predicate, to the index tokens that
  hold it. Decision 5 (stars with variants on) now has its cost: one pattern per 60-120 stems with
  the Hebrew class, 100-190 without marks.

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
  (`.*נ[\[\]\p{M}]*י[\[\]\p{M}]*ם[\[\]\p{M}]*` AND the same for `לים`)
  returns **100,392** candidates in **9.67 s** (411 more than the plain lookup, at the same cost)
  and **admits all seven** (`verify-tolerant`); the Hebrew-marks class gave the same 100,392, and
  the end-only form `.*נים[\[\]]*` gave 100,373, both admitting all seven.

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
Phase 3 land; the Linux-worker run is the owner's and is **not** replaced by the stdlib-`re` column
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
  pages first (§5.4) is what Phase 1 and Phase 3 are measured against, with the same subcommand
  ("before and after", §7 item 6). The Linux-worker run is the owner's: the same command with
  `--index-dir`, `--corpus-file` and `--engines re`.
- Where the time goes (the engine's own `search_perf` line): `#כהנים #גדולים` reads 3,723
  candidate docs (pages and aggregates), keeps 983 after the regex check, 401 after
  de-duplication; the index query took 1.3 s, loading the documents 4.1 s, and **matching 148.6 s
  on `regex` against 26.0 s on `re`**. `*נים *לים`: 23,609 candidates, 2,459 kept, 1,848 rows;
  loading 7.5 s, matching 60.9 s against 18.7 s. The matching time is the aggregate documents'
  text (§3.5), which pages first (§5.4) and §6's crossing source address.
**After Phase 1** (2026-10-22, the same subcommand, the same laptop, nothing else running;
`_tmp/responsa_after/desktop-timings.json`, 06:09-06:23; awake but for 16 s). Rows are the same on both routes:

| Query | Rows before | Rows after | `regex` before | `regex` after | `re` before | `re` after | Note |
|---|---|---|---|---|---|---|---|
| `#כהנים #גדולים` | 401 | 392 | 166.8 s | 88.7 s | 35.0 s | 16.4 s |  |
| `#כהנ# גדולים` | 2 | 374 | 20.4 s | 225.7 s | 6.1 s | 31.8 s | the suffixes are kept now: 600 forms (before, the guard dropped them) |
| `כהנים גדול` | 264 | 4 | 9.7 s | 6.9 s | 8.3 s | 5.0 s | whole words (Exact's 4) |
| `ספר` | 24,653 | 25,041 | 2.9 s | 1.8 s | 2.6 s | 1.8 s | whole words: the same 25,041 as Exact; page docs only, no longer cut off |
| `כהנים [2] גדולים` | 355 | 317 | 7.5 s | 4.8 s | 6.2 s | 3.6 s |  |
| `כהנים גדולים, bidirectional` | 350 | 316 | 10.7 s | 6.7 s | 16.5 s | 8.4 s |  |
| `כהנים AND גדולים` | 1,462 | 1,395 | 11.3 s | 103.3 s | 30.0 s | 26.3 s |  |
| `\|כהנים גדולים` | 41 | 39 | 6.8 s | 85.6 s | 4.4 s | 6.6 s |  |
| `כהנים גדולים -אהרן` | 138 | 204 | 6.8 s | 4.4 s | 6.3 s | 3.6 s | the excluded word is read on the matched page only (decision 7) |
| `*כהנים *גדולים` | 405 | 405 | 60.9 s | 42.9 s | 23.8 s | 15.1 s |  |
| `*נים *לים` | 1,848 | 1,849 | 92.0 s | 68.4 s | 36.4 s | 23.3 s |  |
| `כ*נים גדולים` | 0 | refused | 0.4 s | -- | 0.2 s | -- | refused with a message |
| `*כ*ה*נ*ים גדולים` | 0 | refused | 0.3 s | -- | 0.04 s | -- | refused with a message |

- The `|` line query and `AND` rows above are from before a fix made the same morning (commit
  `9d9a8248`): the large start-lookbehind classes and the looked-past class at a `|` anchor were
  tried at every position the `regex` module backtracked through. Re-measured after it, single runs
  whose standby state was not checked, so upper bounds: `כהנים AND גדולים` 25.4 s and `|כהנים
  גדולים` 13.0 s on `regex`, 5.6 s on `re`. The oracle re-ran on the fixed engine: still 0
  differences.
- `#כהנ# גדולים` is now the slow one on the desktop route (225.7 s, 31.8 s on `re`): it searches
  the 600 forms the guard dropped before, over the whole-manuscript documents. That is the
  `regex`-module cost §10 leaves out of scope; pages first (§5.4, Phase 3) is what removes it.
- A second "after" run (06:52-07:11) is **not** used: Windows' Kernel-Power events show the laptop
  in Modern Standby (event 506 at 06:35:57, no 507 before the run ended), where desktop processes
  are throttled, and every query ran 1.0-1.9x slower than in this run. Two outliers during
  development (`#כהנ# גדולים` 4,512 s; a bidirectional query 2,952 s) were standby too (19:07-20:19
  and 21:05-21:54 on 2026-10-21). A clean re-run on an awake machine is the owner's, with the
  same subcommand, as is the Linux-worker run.

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

**The list is exhaustive** (`token-only`, 5 s over the 7,284,528 distinct folded words): every
character the tokenizer keeps inside a token that Exact's predicate neither continues a word
through (letters, decimal digits, connector punctuation) nor looks past (marks, quotes, brackets)
was counted, which covers whatever else the tokenizer's `\w` admits (join controls U+200C/U+200D,
letter-numbers such as U+2160, alphabetic symbols such as U+24B6; Codex round 3 on PR #429). Only
**two** such characters occur: sof pasuq (247 occurrences in 121 words) and maqaf (161 in 95
words); **165** distinct words hold one, **6** of them with a Hebrew letter. Join controls in
particular: **0** bytes of U+200C or U+200D in the whole corpus file (1.5 s byte count).

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
    Underscore (Pc, 970 tokens) continues a word under both rules. **The two are the whole list:**
    of every character the tokenizer keeps inside a token that the predicate neither continues a
    word through nor looks past (the tokenizer's `\w` also admits join controls U+200C/U+200D,
    letter-numbers and alphabetic symbols), only these two occur in the 7,284,528 distinct indexed
    words (165 words, 6 of them with a Hebrew letter; `token-only`, §3.6.7).
  - **A third, smaller gap is marks of another script inside a Hebrew word** (`אَב`): the predicate
    looks past every mark, `content_search` keeps marks outside U+0300-036F, and a finite lookup
    cannot carry the all-marks tolerance class (one pattern of it exhausts the 1,000-state limit,
    §3.6.2), so such a token is missed by the bare term query and by a bracket- or Hebrew-mark-
    tolerant pattern. Measured: **5** distinct indexed words hold a non-Hebrew mark and a Hebrew
    letter, all HTR noise (`marks`, §3.6.2).
  - Phase 1 accepts these gaps explicitly: one test pins the two characters and the foreign-mark
    case, and the contract names them with their counts. The word list (§5.6 C) closes all three,
    because it maps each word, by Exact's predicate, to the index tokens that hold it.
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
  above. **Decided 2026-10-21: 150,000 candidate pages; aggregates and seams get their own limit.**

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

Recommendation was: A first, then C; B if §6's seams are chosen, on the same rebuild.
**Decided 2026-10-21 (decision 1): B**, the reversed-words field, on the same rebuild as the seam
documents (Phase 2); C is not pursued; A stays for a desktop on an old index until the user
re-indexes, and for `*כהנ*` (stars at both ends). The rebuild step measures the field's size first
(estimate: +2-3 GB on 12.9 GB, without positions). §3.6.1 measured the lookup side: identical page
sets, 0.14-1.8 s against 4.6-6.1 s per leading-star lookup.

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

Recommendation was: C as the interim (with the limits above), then B in one index-format change,
together with §5.6 B or C, so that users rebuild once. Codex round 2 judged the seam direction
sound, with these corrections.
**Decided 2026-10-21 (decision 9): B first, no interim.** The seam documents are built per source
and per IE with consecutive page numbers, so none of §3.6.5's 226,926 false junctions survives
(the V0.8 -> V0.7 restart inside one document above all), on the same rebuild as the
reversed-words field (Phase 2), before true `*` ships (Phase 3). The crossing step of star searches
reads seams, never whole manuscripts. C's limits stay in the text as the record of why an interim
without crossings was not chosen.

### 6.1 Phase 2 design: the seam index and the reversed-words field (2026-10-22)

Settled before building, from measurements on the real corpus files (`Transcriptions.txt` and
`AllGenizah_OLD.txt`): `scripts/measure_responsa_wildcards.py seam-plan` (the seams of the whole
corpus, without an index, 465 s) and `index-sample` (a 1/10 sample of the manuscripts, both
files, built with and without each addition).

**1. Where the seams live: a separate index, `tantivy_db/seams/`, not `scope:seam` in the main
index.** §6 B asked for a new scope excluded from every unscoped consumer. A separate index gives
that by construction, and it also covers three readers no exclusion list can reach:
- **Today's code on the new index.** The web is not continuous-deploy: the index goes up first and
  the code after it (CLAUDE.md). In that window today's code would meet `scope:seam` documents in
  its unscoped queries: Responsa's mixed query, Regex mode's candidates, position searches,
  composition's 50-candidate chunks, `_every_doc_has_scope` and the home-page count.
- **An older desktop installer** opening a folder the new one re-indexed.
- **Scripts** that open `tantivy_db` directly (a dozen under `scripts/`).

The main index keeps exactly today's documents: pages, whole-manuscript documents and Oxford part
documents. The only new thing in it is the reversed-words field (point 5). Each seam document
still says `scope = "seam"`, so a seam is recognisable wherever it turns up. The seam index sits
inside `tantivy_db/`, so the folder is built, copied, swapped and deleted as one unit. The
desktop's Re-index (`shutil.rmtree(tantivy_db)`) removes the seams with it, and an older app's
Re-index cannot leave a stale seam index beside a new main index. Tantivy's garbage collection
deletes only the files listed in `.managed.json`, never a subfolder.

**2. K = 30 words on each side of a junction.** "Words" are what `_cross_page_spans` counts:
whitespace chunks that hold a word character, cut by the same `_left_window_start` and
`_right_window_end`. A seam is that window with K = 30 on the text of its chain (point 3), so it
crosses pages shorter than K. The three-page match of `tests/test_cross_page_spans.py` lies in the
seam centred on its first break.
- **Page lengths** (word chunks per page). V0.8: median 122; a quarter of the pages under 28, a
  tenth under 4; 240,006 of 941,026 pages (25.5%) shorter than 30; 42,685 without a word. V0.7:
  median 115; 287,189 of 1,043,211 pages shorter than 30. So many seams run over more than two
  pages: at K = 30, 195,049 of 1,541,186 seams (12.7%), up to 13 pages and more.
- **What a crossing needs.** A crossing match has at least one word on each side of the break, so
  one side holds at most S chunks:
  - Exact, Variants, Fuzzy: S = (n - 1) x (gap + 1), as `_cross_page_side_words` computes it
    today (`MAX_WORD_GAP` is 10).
  - Responsa (star words and, by decision 10, finite words, both in Phase 3; point 7): S = the sum over the words of the most chunks each can span,
    plus the sum of the per-pair gaps (`[N]`, else the global gap), minus 1. A form spans one
    chunk; under flex spacing a typed word spans up to its letter count, since `\s*` may split it
    at every letter. Bidirectional reverses the order, not the sum. `within_document` (`AND`),
    position searches and line searches never read seams: they keep the whole-manuscript
    documents (§6's table).
  - **S <= K reads the seams.** K = 30 covers a 31-word phrase at gap 0, 16 words at gap 1, 4
    words at gap 9, 3 words at gap 10 (S = 22), and Responsa's `a [10] b [10] c` (S = 22).
  - **S > K, without a star word** (Exact, Variants, Fuzzy; Responsa with finite words only):
    the whole-manuscript documents, today's route with today's false junctions.
  - **S > K with a star word** (Phase 3): never the whole-manuscript documents, as decision 9
    says of star crossings (Codex round 3: the aggregate route would bring back the false
    junctions and the billions of characters seams were built to avoid). The pages and the seams
    are searched as for S <= K; a crossing with more than K words on one side of its break is the
    one thing they cannot hold, so the result is **marked incomplete**, through the same signal
    as "N+" (the header, the API and MCP completeness field, and `complete_chain`), with a
    note that matches across a page break longer than 30 words a side were not searched.
    None of the 90 days of real queries below needed it.
- **Real queries** (PostHog `search_executed`, website, the 90 days to 2026-10-22, bots
  excluded). Of about 13,300 Exact, Variants and Fuzzy searches, 20 had 16-30 words and none had
  more; 146 had 9-15. 62 Responsa queries carried a `[N]` gap, the largest `[10]`, all in 3-4
  words. The website does not record Exact's gap setting, so the gap side of the rule rests on
  `MAX_WORD_GAP`.
- **Seam text per K** (all 1,541,186 seams, in characters): K = 10: 147M; K = 20: 287M;
  **K = 30: 422M (274 per seam on average, 1,333 at most)**; K = 40: 553M; K = 50: 681M. The
  index grows with it (point 8), so K = 40 or 50 would pay for queries no one has typed.

**3. Which junctions get a seam: runs, bridges and chains.**
- **A run** is pages that follow each other in one transcription of one volume: the same source
  (V0.8 or V0.7), `sys_id` and IE, each page number one more than the last. A run breaks at the
  V0.8 -> V0.7 restart, at a volume change, at a page-number jump (the missing page's text stands
  between, so no seam) and at a page without a number. The corpus has 459,376 runs with
  **1,524,861 junctions, exactly §3.6.5's consecutive count**, and none of §3.6.5's 226,926 false
  junctions is a seam.
- **Oxford parts.** A part document joins folio records (one `sys_id` each) in folio order. It
  holds folio N's V0.8 pages, then its V0.7 pages, then folio N+1's; so its 8,244 junctions "V0.7
  of one folio -> V0.8 of the next" are false, and **no part document ever puts folio N's last
  V0.8 page beside folio N+1's first**. A **bridge** joins them, per source, when the two folios
  are consecutive in the part with folio numbers N and N+1 (from the shelfmark,
  `CodicologicalManager._get_folio_number`), each folio is one volume, N's run ends on N's last
  page and N+1's run starts on page 1. Bridged runs form one **chain**, so a seam may cross from
  folio to folio. **16,325 bridges**; 211 refused (206 folio numbers not consecutive, 4 a folio in
  two volumes, 1 a missing first page). Their crossings are new: no index finds them today. Parts
  over 1,000 pages are skipped, as their documents are today.
- **1,541,186 seams in all** (1,524,861 + 16,325), in 443,051 chains.

**4. A seam document: fields and provenance** (`shared/indexer.py::build_seam_schema`).
- `content` (stored, `hebword`, with positions: the phrase candidate queries need them);
  `content_search` (folded, not stored); `content_rev` (point 5); `source` and `full_header` of
  the page before the junction; `sys_ids`, the manuscripts its slices belong to (indexed, for
  search-within; a bridged seam has two); `scope = "seam"`; `cut`, the junction's offset in the
  text; and `boundaries`, one slice per page: `{uid, p_num, full_header, source, sys_id, start,
  end, offset}`. `start` and `end` are in the seam's text. **`offset` is where the slice starts in
  its page**: a seam's first slice starts mid-page. `unique_id` is
  `seam:<source>:<sys_id>:<left uid>><right uid>`: 2,725 junctions of the corpus repeat under a
  second manuscript that shares the same volume's images (one IE filed under two `sys_id`s), and
  each manuscript keeps its own seam, as it keeps its own whole-manuscript document.
- **Mapping a span back.** `_map_span_to_pages` adds each boundary's `offset` (0 for the
  whole-manuscript documents, which carry none), so `page_highlights` and the primary page come
  out in page coordinates, as they do for an aggregate today.
- **Phase 1's bracket mapping** (`_unstripped_span`) works inside the text it is given. The check
  runs on the seam's bracket-stripped text, the span is mapped back onto the seam's own text, and
  then through the slices. A seam starts and ends on whitespace, so it never separates a bracket
  from the letters of its chunk. `_cross_page_spans`' own `to_content` mapping is the same.
- **What a crossing row from a seam carries** (only when a search reads seams, point 7):
  `page_highlights` and the primary page as above; as `full_text`, the text of the pages its span
  covers, fetched from the main index by uid and source (an aggregate's row carries the whole
  manuscript today); and for an excluded word (decision 7), those same whole pages, never the
  slices.

**5. The reversed-words field, `content_rev`.**
- Its text is every token the `hebword` tokenizer makes of the **`content_search`** text (folded:
  marks, quotes and geresh out), each written backwards, space-separated, indexed with the
  `whitespace` tokenizer. The tokens come from the `hebword` analyzer itself
  (`tantivy.TextAnalyzer.analyze`), not from a Python copy of its class, so each reversed term is
  exactly a `content_search` term written backwards. `content_search` is the field a star word is
  looked up on (§5.1); the raw `content` field is not reversed.
- `index_option = "basic"`: document ids only. A star lookup asks which documents hold such a
  word; positions and frequencies would only make the field bigger. Not stored.
- On **every document**: pages, whole-manuscript and part documents, and seams. Under decision 9
  a star's crossings come from seams, but `AND` (`within_document`) still reads the
  whole-manuscript documents (§6's table), and decision 1 leaves only `*X*` on the slow route.
  On the aggregates the field costs about 0.37 GB (point 8), 2.4% of the index.
- **Not in My Library's LOCAL index.** LOCAL is small, so `regex_query('.*X')` stays cheap there,
  and a schema change would make its marker rebuild every user's library on the next start.
  LOCAL keeps the scan route (§5.6 A).
- **The lookup looks past what the forward lookups look past** (Codex round 1 on PR #438).
  `content_search` keeps brackets and Hebrew points inside a token, so `נתקנים[` is the reversed
  token `[םינקתנ` and `[כהנים]` is `]םינהכ[`: a plain prefix `םינ.*` misses both, as the plain
  `.*נים` misses them today (§3.6.4). The pattern (`shared/index_seams.py::reversed_ending_pattern`)
  is the tolerance class of the `content_search` lookups (`_T_CONTENT_SEARCH`: brackets and the
  Hebrew block's marks) before the reversed ending and between its letters, then `.*`. So a token
  matches exactly when, with those characters removed, it ends with the ending. A mark of another
  script inside the ending stays §4's documented gap, counted by gate (c).
- The engine's helper, `SearchEngine.reversed_ending_query(ending, seams=False)`, is that
  `regex_query` on `content_rev`, or None when the index lacks the field. Phase 2 builds and tests
  it; Phase 3 searches through it.

**6. The format marker and the fallback.**
- `tantivy_db/index_format.json` is written **last**, after both indexes commit:
  `{"format": 2, "build_id", "created", "reversed_field": "content_rev", "seams": {"dir": "seams",
  "words_each_side": 30, "count", ...}}`. `tantivy_db/seams/seam_format.json` repeats the build id
  and K. No marker means format 1: every index built before this phase.
- **At open** (`SearchEngine._open_index_format`) the engine believes the marker only where the
  index agrees. The reversed field counts only if the schema has it. The seams count only if the
  seam marker carries the same build id and a positive K, and the seam index opens with its
  fields. Anything else is an old index, with a warning in the log. K is read from the marker,
  never from the code, so a later K needs no code change to read an older build correctly.
- **Fallback on every path.** Exact, Variants and Fuzzy crossings read the whole-manuscript
  documents (today). Responsa keeps today's mixed query in Phase 2. In Phase 3 a leading star on an
  old index keeps `regex_query('.*X')` on `content_search` (5-6 s per star word), and its
  crossings the whole-manuscript documents. Nothing fails on an old index, and nothing re-indexes
  by itself.
- **What the desktop tells a user on an old index.** A line under Settings -> "Build / Rebuild
  Index" says which format the index has. Once the new format gives the user something (Phase 3:
  true `*` and, by decision 10, today's crossings on the seams), a prompt after startup says what a rebuild brings and
  how long it takes, with "Rebuild now" (the existing confirmation follows) and "Later"
  (remembered for that index). Re-indexing is always the user's click: never from `__init__`,
  the UI thread or a timer. The website's index is rebuilt by the owner (point 9).

**7. What the engine uses in Phase 2.**
- **Always:** the format detection and fallbacks above; the seam index opened beside the main one
  and released by `close_index` (Windows keeps the files locked while a reader is open);
  `reversed_ending_query`; and `_map_span_to_pages`' offsets. None of these changes a result.
- **Isolation needs no exclusion list.** Nothing that reads the main index can meet a seam, and
  gate (d) checks it consumer by consumer.
- **Today's crossings stay on the whole-manuscript documents in Phase 2.** Exact, Variants and
  Fuzzy (several words, no position) read crossings from them today, and Responsa reads them
  through its mixed query. Seams remove the crossings found only at a false junction (the end of
  a volume joined to its own V0.7 beginning, a volume change, a missing page) and add the
  part-bridge crossings no index finds today. Measured on the full new index (`crossing-diff`, 21
  queries of the §8 set and its Exact/Variants/Fuzzy pairs, and 6 frequent phrases): 1-3 rows
  removed per query where anything changes, all at false junctions but one in-page Fuzzy row read
  only through its manuscript; none added for Exact, Variants and Fuzzy; +2 for Responsa
  `ברוך אתה`, whose search today checks only each manuscript's first match. **Decided 2026-10-22
  (decision 10): the switch is made in Phase 3, together with Responsa's pages-first change** (§7
  Phase 3).

**8. Size and build time.** Measured on a 1/10 sample (`index-sample`: the manuscripts whose
`sys_id` has a CRC-32 of 0 mod 10, both files: 91,226 V0.8 and 100,030 V0.7 page headers, 190,521
page documents, 9.6% of the index's 1,984,237), one build per variant, the laptop of §3.6, nothing
else running:

| Build (1/10 sample) | Size | Build time |
|---|---|---|
| today's format (no field, no seams) | 1,259.0 MB | 67.9 s |
| + reversed-words field on pages | 1,377.2 MB (+118.3 MB, +9.4%) | 66.9 s |
| + seams, K = 30 (145,462 seams; the field on pages and seams) | 1,575.5 MB, of which seams 193.6 MB (+316.5 MB, +25.1%) | 75.2 s |
| the field on the 23,700 aggregate documents alone (a separate index of that field) | 35.6 MB | 21.3 s |

Scaled by 10.4 (1,984,237 / 190,521 pages), the format-1 sample predicts 13.1 GB against the real
index's 13.86 GB (13,861,494,833 bytes; "12.9 GB" elsewhere in this plan is GiB): the sample
underestimates by 6%, largely because sampling folios breaks most Oxford parts. **Scaled the same
way, the additions are +1.2 GB for the field on pages, +0.4 GB on the aggregates and +2.0 GB of
seams: a full index of about 17.5 GB (+26%).** That was the prediction; the builds below measured
less, because they also merge their segments. The seam index's bytes are mostly stored text
(`.store` 37%) and postings (`.idx` 30%). The sample built 11% slower with both additions.

**The full build** (2026-10-22, `build_index.py main --index-dir C:\Users\gersh\GenizahPhase2\index_v2`,
both corpus files, the laptop of §3.6 with this session's other work running beside it, kept
awake): **12:59:17 -> 13:20:54, 21.5 min** (pages and aggregates 954.5 s, main commit and merges to
1,096.4 s, seams to 1,280.9 s). **15.10 GB in all: the main index 13.18 GB** (`.idx` 3.52, `.store`
3.48, `.pos` 3.08, `.term` 3.06 GB) **and the seams 1.92 GB** (`.store` 0.76, `.idx` 0.59, `.pos`
0.29, `.term` 0.27 GB), against today's 13.86 GB. The main index came out smaller than today's
although it holds the new field, because the build now waits for Tantivy's merges
(`writer.wait_merging_threads()`): 19 segments, where today's index has 155 and repeats each
segment's term dictionary. So the field's own cost is the sample's (+1.6 GB scaled), and the
whole format change costs +1.24 GB (+9%) against today's index as it stands. 1,984,237 page
documents and 1,541,186 seams, as `seam-plan` counted; `browse_map.pkl` is byte-for-byte the size of
today's (133,440,543 bytes).

**The final build** (the code of the code PR, after the seam ids took the `sys_id`; 13:49:03 ->
14:10:48, **21.7 min**, nothing else running, memory sampled every 2 s): **15.30 GB** (main 13.35 GB
in 30 segments, seams 1.96 GB), peak working set 12.4 GB, private 10.1 GB. Two builds of the same
code merge to different segment counts (19 and 30), so their sizes differ by 0.2 GB.

**Like for like, V0.8 only** (production publishes V0.8 only; `build_index.py --v08-only`, each
format built the same way, one after the other): **format 1 6.31 GB in 6.1 min; format 2 7.74 GB
(main 6.81 + seams 0.94 GB) in 12.1 min.** The format costs **+1.43 GB (+23%)** and doubles the
build time (the reversal of every document's words and the seams); private memory is the same
(5.6 and 5.5 GB). Production's format-1 build took 869 s on 2026-08-16, so about 30 min is to be
expected there (`DEPLOYMENT_TECHNICAL.md`, "Rebuild the main index in format 2").

**9. The server rebuild and the desktop re-index** are part of Phase 2 (§7), written up in
`docs/guides/DEPLOYMENT_TECHNICAL.md` ("Rebuild the main index in format 2") by the code PR,
#440: build into a new folder beside the live one (`build_index.py main --index-dir ...
--v08-only`), check it, swap the folders with the restart, and keep the old folder to roll back.
The owner runs it. **These options and the runbook arrive with #440:** until it lands,
`build_index.py` has no `--index-dir` (it reads only its first argument) and rebuilds the live
`Genizah_Index/tantivy_db` in place, so nothing here is to be run from this PR alone (Codex
round 5). The builds measured in point 8 used #440's code.

**10. Gates for Phase 2**, each shown red under the named mutation:
- (a) `measure_responsa_wildcards.py seam-topology`: every junction inside a seam is consecutive
  or a bridge; every consecutive junction of each whole-manuscript document has exactly one
  seam of that manuscript; and every bridge the part metadata calls for, derived by the gate
  itself from the parts' folio lists and each manuscript's pages (not through
  `shared/index_seams.py`), has exactly one seam, and no other bridge has one (Codex round 2: a
  build that dropped every bridge passed the first two). Mutations: seams built over each
  manuscript's pages in file order (no runs); no bridges.
- (b) The Phase 1 oracle (`scripts/verify_responsa_wildcards.py`) on the new index: the same
  counts on all nine queries.
- (c) `measure_responsa_wildcards.py reversed-check`: for `כהנים` and `נים`, the reversed lookup
  gives exactly the documents an independent oracle names: every page of the corpus files (both
  sources), and every whole-manuscript, part and seam document's stored text (the aggregates
  carry the field for Phase 3's `AND`, Codex round 2), cut by the `hebword` analyzer from its folded text,
  holding a word that ends with the ending once brackets and Hebrew marks are removed. Not the
  plain `.*X` lookup, which shares the bracket blind spot (Codex round 1). The plain prefix, the
  plain regex and the words a foreign mark interrupts are counted beside it. Mutations: the
  field built from `content` instead of `content_search`; the lookup as a plain prefix.
- (d) Consumer isolation (`tests/test_index_format.py`): every search mode, the document counts,
  the text fetches and composition give on a new fixture index what they give on an old one, and
  never a seam. Mutation: the seams written into the main index.
- (e) An old-index fixture runs every Responsa path on the fallback with today's results (the
  same file, and the whole suite on today's fixtures).
- (f) `scripts/verify_search_combinations.py` on the new index.
- (g) `desktop-timings` on the new index, on an awake machine, against §3.6.6's Phase 1 numbers.
- §8's "In Phase 2 the same cases run against seams": `tests/test_index_seams.py` checks that the
  seams of a chain give exactly the crossings `_cross_page_spans` finds on the chain's whole text,
  for every side up to K, on both gap paths, brackets included.

## 7. Phases

**Phase 0: measurements, no product code.** **Measured 2026-10-21: items 1-5 and 7, and the
"before" half of item 6, have their numbers in §3.6 (item N in §3.6.N), from
`scripts/measure_responsa_wildcards.py`. Still open in item 6: the "after" timings (run the same
subcommand when Phase 1 and Phase 3 land) and the Linux-worker run (the owner's).**
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

**Phase 1: one meaning on every path, for finite words.** **Implemented 2026-10-22**
(`shared/responsa_match.py`; the status line names the branches). Choices made while building it,
for review: `*X*` keeps today's meaning (`#X#`, with the warning) rather than being refused;
every word of a line query is looked up on the content field (the line fields hold a line's first
or last whitespace token raw, narrower than the check); a page doc's row wins de-duplication over a
whole-manuscript row for the same page (§5.4's "first wins: the page row"); a lone word reads page
docs only, as Exact's does; a line row marks its words, not whole lines; a negated word inside a
line group is excluded, not required (it was). It covers the main path, line-break, the
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

**Phase 2: the index format (resequenced 2026-10-21, decisions 1 and 9).** **Designed 2026-10-22:
§6.1** (the seam index's place, K, runs and bridges, fields and provenance, the reversed field,
the marker and the fallbacks, the gates). **Implemented 2026-10-22 (PR #440), and every gate of
§6.1 point 10 run on a full local build** (21.7 min, 15.30 GB; point 8):
- (a) `seam-topology` PASS: 1,541,186 seams, no junction inside one out of reading order, each
  of the 1,524,861 consecutive junctions once per manuscript, each of the 16,325 bridges the part
  metadata calls for once. Red on 1/10 samples: seams in file order (22,418 central junctions
  not consecutive); no bridges (257 missing).
- (b) the Phase 1 oracle: 9/9 PASSED, the same counts on today's index and the new one; red under
  `accept-off` on the new index.
- (c) `reversed-check` PASS against the token oracle: `כהנים` 53,240 pages, 10,177 aggregates,
  16,868 seams; `נים` 519,402, 80,455 and 212,840 (the plain prefix finds 13-1,350 fewer in each).
  Red: the field missing on aggregates (a sample built before they got it), the field from
  `content`, a plain prefix.
- (d) and (e): `tests/test_index_format.py` (40 tests) and the whole local suite
  (`run_local_tests.py`: 16,904 passed, 3 failed -- one getattr-guard finding, fixed; a browse
  timeout test that passes alone; and a Config test failing only because `build_index.py` makes
  `Genizah_Index/` in the checkout it runs from); `seams-in-main-index` and `offset-default-1`
  red.
- (f) `verify_search_combinations.py` VERIFIED on the new index (and on today's), after two fixes
  of the gate itself, both exposed by the new document order: its completion check takes the
  restriction check's rule for a page filed under two manuscripts, and its copy check reads every
  copy (a printed book's page is filed under 40 and more). Which manuscript such a page is shown
  under follows document order: an uncapped `ישראל` reaches 44,055 / 44,056 / 44,061 manuscripts
  on three builds of one corpus (tracker fragment).
- (g) `desktop-timings`: every row count as in §3.6.6's Phase 1 run. Timed back to back the new
  index looked 1.3-2x slower on the `regex` route; interleaved in one process (two rounds,
  alternating), old and new are equal within noise (`#כהנים #גדולים` 78.9/79.6 s new against
  78.5/82.1 s today's; the other four within 1 s). The laptop's mixed fast and compact cores and
  two Modern Standby spells (13:13-13:19, 13:34-13:50) make single runs unreliable.

Not done here: the server rebuild (the owner's; `DEPLOYMENT_TECHNICAL.md`, "Rebuild the main index
in format 2"), and moving today's crossings to the seams (decision 10: Phase 3). One rebuild
carries both:
- the seam documents (§6 B), built per source and per IE with consecutive page numbers, so none
  of §3.6.5's 226,926 false junctions survives;
- the reversed-words field (§5.6 B), its size measured first (estimate: +2-3 GB on 12.9 GB,
  without positions);
- the indexer changes for both; the engine's use of both, with a fallback to today's routes when
  the open index lacks them (a desktop on an old index until the user re-indexes);
- the server rebuild and the desktop re-index.

**Phase 3: true `*`, on the new index.** *(First PR, #442: `X*` and `X*Y` on both formats,
implemented 2026-10-22, real-data gates pending. Second PR, stacked on it: leading stars (the
reversed-words field on format 2, the scan on format 1; points 6 and 14), decision 10 (points 7
and 12), and the desktop's offer to rebuild an old index (§6.1), implemented 2026-10-22,
real-data gates pending.)*
- Star retrieval and pages first (§5.4); a leading star through the reversed-words field,
  `*כהנ*` (both ends) through today's slow route.
- Crossings read from seams, never from whole manuscripts (decision 9).
- **Today's page-break matches move to the seams too (decision 10, 2026-10-22)**: Exact, Variants
  and Fuzzy with several words and no position, and Responsa's finite words through its
  pages-first change, by §6.1 point 2's routes (S <= K: seams; S > K without a star: the
  whole-manuscript documents; S > K with a star: pages and seams, marked incomplete). The rows it
  changes are measured in §6.1 point 7; a desktop on an old index keeps today's route.
- The Responsa page limit, 150,000 candidate pages (§5.5, decision 3); bare stars refused and
  one- or two-letter star words warned (decision 4); variants and Judeo-Arabic on star words
  through several `regex_query`s ORed (decision 5); order by the finite words' score (decision 2);
  a star matches zero letters (decision 6).
- Every text in §5.7.

**Phase 3, settled before building (2026-10-22, plan amendment; Codex review on PR #442).**
What the sections above leave open, decided so the code, the oracle and the help say one thing.

**Two PRs (the owner, 2026-10-22).** The first (PR #442) ships a star **at the end of a word or
inside it** (`X*`, `X*Y`), working on today's format-1 index, so it can be deployed before the
server rebuild: those lookups are patterns that begin with the word's first letters, cheap on
either format (§3.1). A star **at the start of a word** keeps Phase 1's meaning there (point 14).
The second PR, stacked on it, searches leading stars through the reversed-words field and moves
today's finite crossings to the seams (decision 10).

1. **A star word is a whole word, bounded at both ends.** The star takes letters of any script and
   the looked-past characters (marks, in-word quotes, brackets), never a digit, `_`, a space or
   punctuation, so it reaches the word's own edge: `כהנ*` is a word that begins with `כהנ`, and
   Exact's predicate checks both of its ends like any other word's (§4). `*` alone or a word of
   stars only is refused (decision 4), in a positive or a negated word and in an OR group; so is
   a star word whose letters hold punctuation (`כ*נ.ים`: the lookup reads one token per word).
2. **Expansion runs letter-run by letter-run.** The letters between stars are runs. `%` and
   spelling variants expand every run; `#`'s prefixes and Judeo-Arabic expand the first run when
   the word begins with a letter, `#`'s suffixes the last run when it ends with one (a leading
   star already covers every prefix, a trailing one every suffix). The word's forms are every
   combination of its runs' forms, written in the check and the lookup as one alternation per
   run between the stars, so their size is the sum of the runs' forms, not the product; the guard
   counts that sum (§5.3: stems in one pattern alternation).
3. **A letter right before a star takes its final or its medial form**: `שלום*` finds `שלומות`
   (Phase 1's rule for `word*`), and `שלומ*` finds `שלום`.
4. **OR groups and inline alternation take stars**: `(כהנ*/לוי)`, `(כהנ/גדול)*`, `כה(נ/ן)*`;
   each alternative is its own form, finite or starred.
5. **A negated star word** (`-כ*נים`) excludes a page whose text holds letters matching it
   anywhere (decision 7's letters-anywhere rule, the star as letters): `-כהנ*` is `-כהנ`. With
   `%` each run takes its plene/defective spellings, as in a positive word (`-%כ*נים` excludes
   `כנם` too; Codex review of PR #443).
6. **Retrieval per form.** A word that begins with a letter: a forward pattern on
   `content_search` (the term-dictionary walk is pruned by its first letters), on both formats.
   (Second PR: a leading star and a letter at the end through the reversed-words field on format 2,
   else the `.*` scan; stars at both ends the scan on both formats.) Star lookups score a
   constant, so the finite words' BM25 orders the rows (decision 2), and are case-insensitive
   (`(?i)`), as the check is: the index keeps a token's case (Codex review of PR #442). A lookup past tantivy-fst's
   size limit is split by halving its largest run's alternation.
7. **Where Responsa reads matches that run across pages** (pages are always read first, §5.4):

   | Query | Format 2 (seams) | Format 1 (today's index) |
   |---|---|---|
   | one word (no flex spacing), or a star word alone | none: a word cannot cross a break | none |
   | finite words: `AND`, a text position, a line, a sequence | whole-manuscript documents, as today (the seams take the sequences in the second PR, decision 10) | the same |
   | a star word: `AND`, a text position, a line | whole-manuscript documents, each checked whole (a line query: every match that runs across a page break, not only the first; Codex round 2), **only when at most `RESPONSA_STAR_AGGREGATE_LIMIT` of them hold the words**; else none, marked | the same |
   | a star word in a sequence, S <= K | seams | the whole-manuscript documents' page breaks, a window of S words a side, **bounded as above** |
   | a star word in a sequence, S > K | seams, marked (a crossing with more than K words a side) | the same as S <= K: windows of S words |

   **This amends §6.1 point 6** for star words on format 1: not the whole manuscripts read whole
   (§3.5's 4.7 billion characters for 12 crossings), but their page breaks only, and only when
   the manuscripts holding the words are few enough to read in time:
   `Config.RESPONSA_STAR_AGGREGATE_LIMIT` = 2,000 (counted before reading; a starting value for the
   local gates to confirm against the website's 3-minute limit). Reading break by break also finds
   every crossing of a manuscript, not only its first match: at each break (and at each seam) the
   first match that starts on the page before it, so a crossing from an earlier page -- found at
   the break it crosses first -- cannot hide it (Codex review of PR #443). The format-1 windows keep today's
   false junctions (the V0.8 -> V0.7 restart); the seams do not (§6.1 point 3). `AND`, line and
   position queries read whole manuscripts because the seams hold no line or position fields and
   `AND` is not a crossing (§6's table); a star there is bounded the same way.
8. **"Marked incomplete"** is the cut-off signal: `capped` ("N+", the API's `results_cut_off`),
   with a sentence that says which matches were not searched and why, in the Responsa note of the
   results line (the API carries it as `query_downgraded: ...`, as it carried the guard's
   downgrades and Phase 1's `*word*` note). Crossings left out by these rules also set
   `partial` (and `limited` when a candidate limit cut the search too): reading every candidate
   (`ids_only`) would leave them out again, so `complete_chain` does not run such a step again
   (`RefinementStep.result_partial`, `_cannot_complete`), it stays "+" and Search within says so
   (Codex round 1 on PR #442). A step searched within such a step inherits it unless its own
   candidate limit cut it (`step_partial`: its restriction stays incomplete whatever it reads;
   Codex round 2). A stopped search is never `result_partial`, whatever it had left out by rule:
   the stop left candidates out too, and running it again reads them (`only_partial`; Codex round
   3 on PR #443). A crossing step that fails is `capped` only, never silent.
9. **Limits** (decision 3): `Config.RESPONSA_PAGE_LIMIT` = 150,000 page candidates on the main,
   line-break and My Library Responsa paths; the whole-manuscript documents and the seams each
   read up to `Config.SEARCH_LIMIT` (50,000) of their own. `ids_only` reads every candidate on
   the paths that have it (main and My Library). **The line search has none** (as today): a
   cut-off line step stays "+" and is not completed (`_cannot_complete`; Codex round 1). Exact,
   Variants and Fuzzy keep 50,000 pages.
10. **Order** (decision 2): the page candidates' score order, as today; a query whose every word
    holds a star is sorted after the merge with My Library by the accepted matches on the row's
    page (a row read through a whole manuscript or a seam counts one), then by shelfmark in its
    natural order, T-S 2 before T-S 10 (`natural_sort_key`; Codex round 4 on PR #443).
11. **A crossing row from a seam** has `scope` `seam`; its `full_text` is the whole pages its span
    covers, read from the main index by uid and source (§6.1 point 4); its excluded words are
    read on those pages; its `page_highlights` are in page coordinates.
12. **Exact, Variants and Fuzzy** (second PR, decision 10) read crossings from the seams when the
    query has several words, no text position, the crossing-window path (`_cross_page_terms`),
    S = (n - 1)(gap + 1) <= K and the index has seams; otherwise today's whole-manuscript route.
    A seam counts the first crossing that starts on the page just before its break, as Responsa's
    do (point 7; Codex round 3 on PR #443). The whole-manuscript route still takes the first match
    across each break, so there a match that spans a whole page can hide the next page's own
    crossing: a tracker row (`docs/open_issues.d/2026-10-22-exact-whole-manuscript-crossing-shadow.md`),
    since changing it changes today's rows.
13. **The results line counts a star word as a pattern**: "searching N expanded terms + M
    patterns" (§5.7).
14. **A star at the start of a word, in the first PR**: `*X` keeps Phase 1's meaning (X with its
    24 grammatical prefixes in the lookup, `\S*X` in the check), `*X*` is still rewritten as
    `#X#` with its warning, and any other word that begins with a star (`*כ*נים`, `(*נים/לוי)`)
    is refused with a message that says which shapes work. The second PR searches them all.

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
  - Phase 3 adds `*כהנים *גדולים`, `*נים *לים`, `כ*נים גדולים` and `*כ*ה*נ*ים גדולים`.

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

In Phase 2 the same cases run against seams.

**5. Existing pins, updated deliberately:**
- `tests/test_responsa_core.py:617-622` (`MAX_EXPANDED_TERMS == 500` and the cascade);
- `tests/test_search_api.py:505-515` (the warning transport, which uses mocked warnings);
- `tests/test_join_workbench_builder.py`, which tests a copy of the builder, so a live-widget test is
  needed too.

There is no dedicated test of the `*word*` rewrite warning. Phase 3 adds one that asserts a
supported true star is **no longer rewritten**, rather than preserving the obsolete warning.

## 9. Decisions for the owner

1. **Speed of a star at the start of a word.** Each such word adds 7-10 seconds to every search.
   - Accept that for now.
   - Add a reversed copy of every word to the index: near-instant, but it needs one rebuild on the
     server and on every desktop computer.
   - Keep a separate word list beside the index: no rebuild, and measured at 3 seconds instead of 9
     for the broadest ending.
   **Decided 2026-10-21: the reversed-words field in the index (§5.6 B), on the same rebuild as
   the seam documents (decision 9; Phase 2).** The word list (C) is not pursued. A desktop on an
   old index keeps today's slow route (A) until the user re-indexes, and `*כהנ*` (stars at both
   ends) keeps the slow route too. The rebuild step measures the field's size first (estimate given
   to the owner: +2-3 GB on 12.9 GB, without positions). From §3.6.1: identical page sets, 0.14-1.8 s
   against 4.6-6.1 s per leading-star lookup.
2. **Order of results for star searches.** The index gives every star match the same score. Order
   them by shelfmark, by how often the words occur on the page, or by the score of the query's other,
   non-star words?
   **Decided 2026-10-21: by the score of the query's non-star words** (the scored term OR of
   §3.6.3, which keeps distinct BM25 scores at 600 and 1,200 forms). A query whose words all carry
   stars is ordered by the number of matches on the page, then by shelfmark.
3. **How many candidate pages a search may read before it says "N+".** Today 50,000. `*נים *לים`
   has about 100,000; reading all of them took 8 seconds here.
   **Decided 2026-10-21: 150,000 candidate pages** (§3.6.4 measured 99,981, 100,392 with bracket
   tolerance); aggregates and seams get their own limit (§5.5); beyond it the search says "N+".
4. **Very short or bare stars.** `*` alone matches every word; `*ים` reaches 1.35 million pages.
   Allow, warn, or refuse with a message suggesting a gap `[1]`?
   **Decided 2026-10-21: a bare star (`*` alone, or a word made only of stars) is refused** with a
   message suggesting a gap `[1]`; a star word with one or two letters of its own runs, with a
   warning that the list will be cut ("N+").
5. **Stars with spelling variants or Judeo-Arabic on.** Allowed (slower; one bigger pattern per
   word), or the star word is searched without them?
   **Decided 2026-10-21: they apply.** The expanded stems are split into several `regex_query`s
   ORed, each under the 1,000-state limit (§3.6.2: 60-120 stems per query with the Hebrew mark
   class).
6. **Does a star match zero letters?** Recommended yes: `*כהנים` includes `כהנים` itself.
   **Decided 2026-10-21: yes**, `*כהנים` includes `כהנים`.
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
   **Decided 2026-10-21: seams first.** The seam documents (§6 B), built per source and per IE with
   consecutive page numbers so that none of the 226,926 false junctions of §3.6.5 survives, are
   built before true `*` ships (Phase 2, with the reversed-words field of decision 1). The crossing
   step of star searches reads seams, never whole manuscripts.
10. **Today's page-break matches** (Exact, Variants, Fuzzy, and Responsa's finite words) read the
    whole-manuscript documents, false junctions included. Move them to the seams now (a third
    Phase 2 PR, Exact/Variants/Fuzzy first), or in Phase 3 with Responsa's pages-first change?
    Measured (§6.1 point 7): 1-3 rows removed per affected query, all at false junctions but one;
    none added for Exact, Variants and Fuzzy; +2 for Responsa `ברוך אתה`.
    **Decided 2026-10-22: in Phase 3** (the owner: "In phase 3"), together with Responsa's
    pages-first change, so one behaviour change ships with one release note.

## 10. What this does not change

- Exact, Variants, Fuzzy and Regex modes, apart from §6's seam documents as the shared crossing
  source (decision 9, Phase 2) and the exclusion domain (decision 7: the matched page, in every
  mode).
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

- **Phase 2 design, round 1 (Codex on PR #438, 2026-10-22): one finding, folded in.** P2, a plain
  prefix on the reversed field misses a word whose ending carries a bracket or a Hebrew point
  (`נתקנים[` reversed is `[םינקתנ`), and a gate comparing it with the plain `.*X` lookup would
  repeat the omission on both sides. The lookup now looks past the `content_search` tolerance
  class (§6.1 point 5), and gate (c) compares it with a token oracle over the corpus files and
  the seams.
- **Phase 2 design, round 2 (Codex on PR #438): two findings, folded in.** P2, gate (a) proved the
  created seams valid and the ordinary junctions complete, but not that every eligible Oxford
  bridge produced a seam: it now derives the expected bridges itself from the part metadata
  (16,325 on the full build, all present, each once). P2, gate (c)'s oracle left out the
  whole-manuscript and part documents, which carry the field for Phase 3's `AND`: they are in
  it now.
- **Phase 2 design, round 3 (Codex on PR #438): one finding, folded in.** P2, the S > K rule sent
  a star query's crossings back to the whole-manuscript documents, against decision 9. A star
  query with S > K now reads pages and seams only and is marked incomplete (§6.1 point 2).
- **Phase 2 design, round 4 (Codex on PR #438): one finding, folded in.** P2, the plans index still
  gave the sample's +3.6 GB after the full build had measured less; it now gives the full build
  and the like-for-like V0.8-only builds (+1.43 GB, +23%).
- **Round 5: one finding, folded in.** P2, point 9 named `build_index.py --index-dir`, which
  exists only in the code PR (#440): run from this PR alone, `build_index.py` would ignore the
  option and rebuild the live index in place. Point 9 now says the options and the runbook arrive
  with #440.
- **Phase 0 round (Codex on PR #429, 2026-10-21): three findings, all folded in.** P1: the
  brute-force oracle's prefilter was a literal substring test, so a page whose ending held an
  internal bracket or mark (`נ[י]ם`, `נִים`) was skipped before the check ran; the prefilter now
  removes the looked-past characters first, and item 4 was re-measured (§3.6.4). P2: sof pasuq
  joins maqaf in §4's exception and in Phase 1's pinning test. P2: Phase 0 is "measured", not
  "done": item 6's "after" timings and its Linux-worker run are still open (status line, §7).
  **Round 2 (same PR):** P2, the mark-tolerance class of the probes covered Hebrew marks only
  (U+0591-05C7) while the oracle looks past every Unicode mark; both classes are now measured
  (§3.6.2), and the all-marks class turned out to fit one star word and no alternation.
  **Round 3 (same PR):** P2, join controls (U+200C/U+200D) are kept by the tokenizer's `\w` but end
  a word for Exact's predicate, like maqaf; the `token-only` subcommand now counts every such
  character over the distinct indexed words, and §4's exception says the list is exhaustive.
  **Round 4:** P2, `token-only` and `marks` counted occurrences under a "words" label; both count
  distinct words now. **Round 5:** P2, the Hebrew-marks class `U+0591-05C7` also swallowed the
  block's punctuation (maqaf, paseq, sof pasuq, nun hafukha), which the oracle treats as word
  boundaries; the class is now the block's category-M code points only, and §3.6.2 was re-run.
  **Round 6:** P2, "left to the oracle's side" was wrong for the 5 Hebrew words with a foreign
  mark: stage 1 runs first, so they are a retrieval gap; §4 now names it with its count, pinned in
  Phase 1 and closed by the word list like maqaf's.
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
    approximation case list (§5.2); seams across short pages (HIGH, Phase 2; §6); seam consumer
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
