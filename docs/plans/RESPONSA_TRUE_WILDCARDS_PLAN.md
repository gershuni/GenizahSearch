# Responsa wildcards: `*` as any sequence, `#` as the grammatical list

**Status:** plan, revision 4, **Codex SHIP** (four rounds on 2026-10-21: DO NOT SHIP, DO NOT SHIP,
SHIP WITH FIXES, SHIP; §12). It is the basis for Phase 0 and Phase 1. Not implemented. Decisions 7
and 8 in §9 were taken on 2026-10-21 (exclusion on the page only; counts may change), so Phase 0 and Phase 1
may start; decisions 1-6 and 9 wait for Phase 0's numbers.
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

## 4. The contract (proposed)

- **Whole word = Exact's predicate.** `_starts_word` / `_ends_word` (`shared/search_engine.py:397`,
  `:405`):
  - a word continues through letters and decimal digits of any script and `_`;
  - marks, and the in-word quotes and brackets of `_IN_WORD_QUOTES`, are looked past.

  Responsa and Exact then agree on what a whole word is. It is **not** the tokenizer's class: that
  keeps maqaf, brackets and marks inside a token, and copying it into lookarounds would disagree
  with Exact (Codex 1.9).
- **Retrieval may be wider than the check, never narrower**, with one measured exception.
  - Every stage-1 lookup must return every document the stage-2 check could accept.
  - A lookup tolerates brackets and marks inside tokens, so it never drops a page the check would
    keep.
  - **Words and tokens differ in one place: maqaf (U+05BE).** Exact's predicate ends a word at a
    maqaf (category Pd), but `hebword` keeps it inside the token. So `abc` inside `abc־def` is a
    whole word to the check, but not an indexed token, and a finite lookup misses it (Codex round 2,
    2.1, reproduced).
  - Measured: **196 of 160,171,128** V0.8 tokens contain a maqaf, and the samples are HTR noise.
    Underscore (Pc, 970 tokens) continues a word under both rules.
  - Phase 0 counts the other Hebrew-block punctuation kept inside tokens (paseq U+05C0, sof pasuq
    U+05C3, nun hafukha U+05C6).
  - Phase 1 accepts this gap explicitly: a test pins it, and the contract names it. The word list
    (§5.6 C) closes it, because it maps each word, by Exact's predicate, to the index tokens that
    hold it.
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

**Phase 0: measurements, no product code.**
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

**Counts change.** For example, `כהנים גדול` goes from 264 rows to the 4 Exact finds. This needs the
owner's go-ahead (§9, decision 8).

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
