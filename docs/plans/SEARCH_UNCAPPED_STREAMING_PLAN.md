# Desktop and web search: full coverage and first results under a second — plan (2026-09-30)

**Status (2026-10-05): Phase 1 SHIPPED in v9.5.0 "Faster, complete search"**, released
2026-10-05: desktop installer on GitHub (latest) and web deployed, tag `v9.5.0` on `e3c32666`.
It went out as PR gershuni/GenizahSearch#375 (merged 2026-10-04, `9b4d0b2a`) and its
follow-up gershuni/GenizahSearch#376 (`824a7e36`).

- **What shipped:**
  - Whole words in Exact (desktop), Variants and Fuzzy (Stage W).
  - Fuzzy finds near spellings.
  - Position checks every occurrence on a page.
  - Phrase candidates for Literal, and page-first single-word Literal (Stage 1a/1b where proven).
  - First rows streamed on the desktop (Stage 3).
  - D8 Phase 1: restriction in the query at any size (desktop and web, including Parallels),
    "N+" counts, cut-off steps completed before search-within and the all-terms filter, and steps
    that replay their own run.
  - #376: a preview starts no shelfmark fetch; a replaced fetch hands on the rows it did not reach,
    only those still shown; a cancel drops its queued requests.
- **What did NOT ship (open):**
  - D8 Phase 2: sort all, export all, full session restore, Joins Lab. The owner decides when.
  - Stage 2 (exhaustive desktop over a result store) and Stage 5 (no cap on the web).
  - Whole-word Exact on the web and in the API. They send `exact`; the rule and fast paths key on
    the desktop's `literal` (deferred in `e8decfca`).
  - The tracker rows: `L1:word` near spellings (P3), the loader's blocking cancel-and-wait (P3),
    My Library unrestricted by search-within (P2), line-break steps stay "+" (P2), Lab Mode in
    chains (P2, owner: not now), the `test_findings_page` hang (P3).
- **Gate:** `scripts/verify_search_combinations.py` (real index). Nightly registration via
  `scripts/schedule_nightly_search_gate.ps1` is the owner's step.
- **Public wording (owner, 2026-10-04):** speed first; fuller results said positively; no loss
  figures in release texts. The loss figures stay here and in the tracker archive.

**Web, after v9.5.0 (2026-10-05, branch `claude/web-search-streaming-perf-s01r1t`, not deployed).**
Owner: the web did not stream and was slower (אם אין אני לי: desktop 13 s, web 19 s).
- **Why slower:** every web search ran in a fresh worker process that first loaded the catalogue
  (`MetadataManager._load_heavy_caches_bg`, 5.5-6.4 s in a 4-CPU dev container: CSV 1.7 s, CUDL
  alias index 3.2 s, Oxford parts 0.6 s), and the engine work itself is the desktop's. **Fixed:** each queue
  slot starts its next worker ahead of the search (loads, then waits for `input.pkl`); still one
  search per process. ~400 MB resident per idle slot; `GENIZAH_RESEARCH_PRESTART=0` turns it off.
- **Why no streaming:** the worker had no channel back. **Fixed:** the page passes
  `preview_callback` (and `corpus_scope='genizah'`, which the preview requires; the web has no My
  Library); the worker writes `preview.pkl`, the parent reads it, the page paints up to one page
  of rows under "Still searching". Not with NOT-words, Responsa, Lab Mode, or any post-search
  filter that could hide a row. Stop keeps the rows shown (marked partial; before, Stop showed 0).
- **Measured** (real web app + headless Chromium, synthetic 60,000-page index, so absolute numbers
  are not production's): אם אין אני לי first rows 7.1 s -> 1.1 s, complete 8.4-8.8 s -> 2.6 s;
  שלום first rows 1.1 s of 6.8 s. Tests: `tests/test_web_search_streaming.py`, 13 mutants killed.
- **Open:** one worker serves every visitor (FIFO, `GENIZAH_RESEARCH_WORKERS=1`): raising it is a
  server `.env` decision (CPUs, memory), see `docs/search-matching-timeouts.md`. A restricted
  search (filters, search within) still loads `browse_map.pkl` in each worker (not measured
  here). The web's Exact still goes through the `exact` mode, not `literal` (above).

Everything below is the plan as written and amended stage by stage; read it for the decisions and
measurements behind the code.

**Original status (2026-09-30):** DRAFT, not started. Owner decisions D1-D3 answered 2026-09-30.
Two Codex rounds (read-only, unsandboxed):
- Round 1, an independent assessment run alongside the first draft: *build exhaustive search
  around bounded batches and a lightweight result store, with immediate incremental rendering and
  explicit deduplication semantics, rather than raising the shared cap or blindly skipping system
  documents.*
- Round 2, on the revised plan: **SHIP WITH FIXES**. Its fixes are folded in below, marked (R2).
- Round 3, verification of the fold-in: **SHIP WITH FIXES** — every round-2 finding addressed or
  partial; the remaining items were wording and gate conditions (token examples, one measurement's
  scope, G2 expectations for every intentional change, G2c/G2d fixtures, G5 numbers), all applied.
  Review loop closed here; re-open per stage when code lands.

Follows [`SEARCH_PERF_HANDOFF.md`](SEARCH_PERF_HANDOFF.md) step 1 (`35f856f`). The pre-existing
bugs found along the way are rows in `docs/OPEN_ISSUES.md` (P2): "Bare-word search never returns
a word written between two lacuna brackets", "Search-within drops every cross-page (system/part)
hit", "Position search checks only the first occurrence on a page".

## Goals (owner)

- **A. Fast:** first results visible well under a second for a common word (benchmark שלום,
  literal). Otzaria, also Tantivy-based, is near instant.
- **B. Full coverage:** no real result lost to a cap — desktop AND web (D2).
- V0.7 exists only on the owner's machine: design for V0.8, only avoid breaking V0.7.

## Owner decisions (2026-09-30)

- **D1 — Literal matches the exact word.** A "with prefixes" option comes later, separately (not
  in this plan; a consistent prefix rule would add ~18K pages for שלום, see below). Applied to
  every term of a Literal query, phrase interiors included — **reconfirmed by the owner
  2026-09-30 for phrases**, shown that "ברוך אתה יי" then no longer returns "ברוך אתה ייי"
  (1,510 docs, found today only by accident): ייי is reached by searching it, or through variants.
- **D2 — no cap on the web either.**
- **D3 — controls that need the complete list (sort, filters, export, search-within) wait until
  the run finishes.**

**Superseded the same day (owner, 2026-10-01): no "Words" control.** "I don't need to find שלום
inside ושלום -- Responsa mode is for this kind of search." Exact, Variants and Fuzzy all match
WHOLE words; prefixes stay with Responsa mode (D7 dropped). Lacunae: no option -- a phrase is
found when words between + standalone brackets <= gap + 3; "the user can just increase the gap"
(verified: 6 words + 8 brackets is missed at gap 6 and 10, found at gap 11). Fuzzy = near
spellings (W-Q1) stands. Stage W below is kept for its findings; its "Also inside words" half is
dropped, which also removes the slow per-term substring retrieval.

Owner decisions (2026-10-01, earlier), the "Words" option:
- **D4 — a "Words" choice for Exact, Variants and Fuzzy:** *Whole words* / *Also inside words*.
- **D5 — the default is Whole words in all three** (Variants and Fuzzy too; today they match inside
  words). Measured, whole-manuscript docs skipped: variants שמעון הצדיק 19.8 s -> 0.1 s, אהרן הכהן
  150 s -> 3.7 s; fuzzy שלום 124 s -> 115 s (its time is the 564K-character pattern itself).
- **D6 — lacunae belong to "Also inside words":** that setting also matches a phrase across any
  number of lacuna brackets; Whole words stays strict (pair-phrase slop gap + 3).
- **D7 — prefixes (ו ה ב כ ל מ ש) later, as a third value.** Placement: desktop search row after
  Gap, for those three modes only; web: Advanced options. Kept across restarts.

## Measurements (real index, read-only scripts, warm, 2026-09-30)

| Fact | Number |
|---|---|
| Counting all hits in Tantivy | 10-20 ms (`search(q, 1, count=True).count`) |
| Fetching all hit addresses, even for על (865K) | 225 ms (R2: a Python list of על's 364,259 V0.8 page candidates is 38.9 MiB) |
| Loading the first 500 page docs | **18 ms** |
| שלום hits by scope | V0.8 page 30,833 · V0.8 system 15,490 · V0.8 part 574 · V0.7 page 32,667 · V0.7 other 97 |
| Load time, all 30,833 V0.8 page docs | 0.8 s (avg 1,462 chars) |
| Load time, all 15,490 V0.8 **system** docs | **9.7 s** (avg **62,239 chars**, 1.9 GB of text) |
| Search after step 1, 50K capped (owner) | `total_ms=3887`; wait ~7 s warm (~3 s of it UI), ~20 s on the first search after launch |

Frequent words, V0.8 page docs only (a public install, uncapped):

| Word | V0.8 page hits | Est. load | Est. text if every row keeps `full_text` |
|---|---|---|---|
| שלום | 30,833 | 0.8 s | 59 MB |
| של | 59,090 | 3.2 s | 194 MB |
| ו | 146,633 | 4.5 s | 101 MB |
| אשר | 268,062 | 8.6 s | 609 MB |
| את | 351,235 | 11.7 s | 778 MB |
| על | 364,259 (334,204 distinct uids, R2) | 24.1 s | 878 MB |

### Where the cap loses results today

1. **Frequent words, everyone.** של / את / על / אשר exceed 50,000 candidates on a V0.8-only
   install; the cap counts candidates, not pages. On the owner's machine V0.7 roughly doubles
   candidates (שלום: 21,210 shown of ~31.7K pages); a public install does not lose שלום.
2. **Search-within over more than 500 manuscripts**: the restriction is applied after the cut,
   and every later refinement step inherits the loss.

### Why the cap exists

No recorded reason; it acts as a crude resource limit. Every row carries `full_text` (for a
scope='system' row, the whole manuscript), and the same `execute_search` serves the web page,
the public API (`/api/search` materializes everything before slicing), Joins Lab and research
workers. There is no universal "returned list <= 50K" invariant: metadata search bypasses it and
LOCAL merges separately capped lists.

### What whole-manuscript (system/part) docs contribute — single word שלום, V0.8

`_add_continuous_document` joins a manuscript's pages with `"\n"`. A hit yields ONE row: the page
of its first regex match. Verification is substring (`build_regex_pattern`), retrieval is
whole-token. Engine-faithful replay of the 16,064 system+part hits (bare-word query; R2 showed
the engine's bracket forms reach 106 of the "108" below, so the 2,136 is approximate):

| System/part hit maps to... | Rows |
|---|---|
| a page page-search also returns (pure duplicate) | 13,094 |
| a page page-search does not return, match **inside a longer word** (השלום, ולשלום, בשלום) | **~2,136** |
| a page page-search does not return, whole word | 108 → 106 are reached by the engine's bracket forms; **2 are the real `]שלום[` retrieval gap** (10 pages in all, tracker row) |
| a match crossing a page break | **0** |

A consistent prefix rule, for comparison (66 prefix forms): 27,419 V0.8 pages have the exact word,
23,576 a prefixed form, **18,264** a prefixed form without the exact word.

## Design — in the order R2 recommends

The order reaches first results soonest without a coverage regression: page-first traversal can
paint early rows while system/part docs are still processed afterwards, so skipping them is not
needed for speed, only for total time and memory.

### Stage 0. Correctness contracts (before any speed work)

Progress (2026-09-30, branch `ccr-b3b8c7ac-jyxlw3`, uncommitted): **0b** `]w[` done
(`tests/test_bracket_lacuna_retrieval.py`); **0c** done for system docs and parts in both loops
(`tests/test_search_within_aggregates.py`); **0d** done in the main loop
(`tests/test_position_later_occurrence.py`; real index, שלום: end 168 -> 312, line start
+1,680, line end +1,785) and in the line-break loop's `end` check. Full suite after 0b-0d:
13,534 passed, 0 failed (the line-break `end` fix came after it; 466 search-path tests re-run green). Every new
test was mutation-checked; the fixture snapshot stayed byte-identical. **0a** and **0e** not
started.

Also done 2026-09-30, outside the original stage list:
- **Phrase candidates for multi-word Literal** (`build_phrase_candidate_query`): AND of adjacent
  pair phrases, slop = gap + 3 (stray bracket/quote tokens between words; +2 missed 0 whole-word
  matches over six phrases, +0 missed 2 — the gate caught it). Real-index gate, engine itself,
  uncapped, OFF vs ON: nothing new, every dropped row has no whole-word occurrence; אהרן כהן
  8.7 s -> 0.7 s, ברוך אתה 8.5 -> 3.9, שמע ישראל 8.7 -> 3.8, ויאמר משה אל 13.6 -> 6.2.
  Tantivy's slop also allows reversed order, so a pair found inside a longer word next to the bare
  words stays a candidate and the substring verifier keeps it until 0a (pinned in the test).
  A repeated word (מאד מאד) gains nothing: the slop lets one occurrence match both positions.
  Only the relevance `score` changes in the fixture snapshot.
- **First paint**: `FIRST_PAINT_ROWS = 50` rows before the handler returns, the rest of the first
  page on the next event-loop turn (`_fill_first_results_page`, skipped when the all-terms view
  re-rendered its own page). Owner's machine: building the first rows 1.2-2.7 s -> 90-136 ms.
  New log lines: `search_ui_perf ... since_submit_ms`, `search_first_paint since_submit_ms`.
- **Stage 1b for single-word Literal** (owner go-ahead 2026-09-30): `AND scope:page` when mode is
  Literal, one term, no text position, and every doc of the open index carries a scope
  (`_every_doc_has_scope`, three counts per index load). Real-index gate, engine itself,
  uncapped, OFF vs ON, 8 words: nothing new; no dropped uid has the whole word on its own page
  except one declared case, a bracket inside the word (`ב[לי`, V0.7; tracker row); 5 apparent
  misses were the gate's own error (it treated the combining dot of `אלג̇מאד` as a boundary; the
  hebword token keeps it, so מאד there is inside a longer word). שלום 10.1 s -> 1.7 s, בלי 8.8 ->
  1.0, ירח 6.6 -> 0.3, הצדיק 5.0 -> 0.2, כהן 9.2 -> 1.3, מאד 9.9 -> 2.1, ישראל 25.1 -> 10.7
  (175K rows), אלהים 15.9 -> 5.7. `tests/test_single_word_page_only.py`, 5 mutations killed. The
  snapshot fixture now also indexes the system doc's pages as page docs (as the real index does);
  on it HEAD and the working engine agree on every row and order, only `score` differs.
- **Aggregates add only cross-page matches** for multi-word Literal (`_cross_page_spans`): the
  regex runs only in a window (3 x query length + 30 x gap + 64 chars) around a page break whose
  left side holds the first term and right side the last (folded; a match across the break holds
  its newline and no term can, so this is necessary). Gap 0 joins the windows with '_' for one
  regex pass. Bracket-free offsets map back exactly to the original text. Every crossing is
  returned, not only a manuscript's first match. Real-index gate, HEAD vs working, uncapped, 10
  phrases: all 23 old cross-page rows kept, 97 now; no whole-word drop; every new row a real
  crossing. Time: לי מי לי 9.8 -> 7.0 s, אם אין אני לי 4.1 -> 2.7, אמר רבי 6.4 -> 4.4; בני ישראל
  (38K rows) and על כן unchanged -- what is left is LOADING the manuscript docs (and many rows),
  which only a re-index (small per-break docs) would remove. `tests/test_cross_page_spans.py`,
  7 mutations killed. Profiling showed the first pre-check (both terms anywhere in the window)
  passed almost always for short words; the before/after split fixed that.
- **Stage 3, streaming (a first version)**: `execute_search(preview_callback=)` hands over the
  rows found so far at most every `_PREVIEW_AFTER_S` (0.5 s) while new rows arrive, and a last time
  with the first `_PREVIEW_ROWS` (50); each call extends the previous one and is the start of the
  final list. Offered only for Genizah scope, no exclude_words, not Responsa (LOCAL rank fusion,
  exclusion and Responsa checks could still drop or move rows). To make the prefix hold:
  `_deduplicate(first_wins=True)` outside Responsa (a uid keeps its first row as well as its first
  position), and page docs are processed before aggregates (a second, lazily run query for
  system/part docs within the same total limit), so the page row is that first row. Desktop:
  `SearchThread.preview_signal` -> `_on_search_preview` (current run only, not the all-terms view;
  appends rows that extend the table; sorting stays off; the status line keeps saying it searches);
  `on_search_finished` rebuilds from the full list. Real index, engine: שלום 50 rows at 48 ms (full
  3.4 s), בני ישראל 165 ms, שלום על ישראל 350 ms, לי מי לי 12 rows at 0.5 s (full 14.8 s),
  variants שמעון הצדיק 50 rows at 219 ms (full 27.8 s). Owner: "streaming is very good".
  Declared changes: rows that only an aggregate gives (cross-page, and for variants/fuzzy some
  substring matches) now follow the page rows instead of their score position; a page with both
  rows is shown as its page row. While the 50K cap stays, a very common phrase could fill it with
  page hits and skip its (rare) cross-page matches -- fixed 2026-10-02 (Codex PR review):
  the aggregate query has a limit of its own. Tests: `tests/test_search_preview.py` and the
  preview tests in `tests/test_exclusion_surfaces.py`; 13 mutations killed.
- **Codex review 2026-10-01 (draft PR #375): two phrase losses, both confirmed on the corpus.**
  (1) *Cross-page window in characters*: a long non-word run at a page edge pushed a crossing out
  (V0.8: 333 page ends / 329 page starts with a run over 60 chars). **Fixed:** the window is now
  (terms - 1) x (gap + 1) word-holding whitespace chunks per side (`_left_window_start`,
  `_right_window_end`, one anchored regex per probe; the left side matches the reversed slice).
  A crossing holds at most that many words on one side and the verifier has no lookaround, so the
  window finds exactly what the whole text finds (randomised test against the whole text). Real
  index, HEAD vs working, uncapped, 10 phrases: identical rows (65 cross-page rows both), time equal
  within noise except לי מי לי 5.8 -> 7.2 s (382K breaks). 10 mutations killed.
  (2) *Pair-phrase slop*: 4+ standalone bracket tokens between two words exceed slop gap + 3
  (V0.8: 31,591 places on 23,778 pages, mostly whole lacuna lines). Six common phrases lose 0
  whole-word matches to it, on pages or across breaks; a lossless fallback (old AND-of-terms query
  AND a standalone bracket token) costs +0.1-1.3 s per phrase on pages and +7-23 s across breaks
  (846,476 page docs, 43%, hold a standalone bracket). **Owner (2026-10-01): make it an option**
  -- to be designed together with the whole-word / substring option, before building.
- **Stage W step 1, whole-word Variants (2026-10-01).** Variants retrieve every form the verifier
  accepts (not the first 200) as whole tokens, with each form's edge-bracket forms, raw on
  `content` and folded on `content_search` (term sets, `build_variant_query`, ORed with today's
  clause so its boosts still rank); one word reads page docs only; phrases use aggregates only
  for crossings, with a form-set pre-check. The whole-word rule (`_whole_word_span`: a match
  continuing a Hebrew word -- marks, quotes and lacuna brackets looked past -- is not a match)
  now applies to Exact and Variants in every path: page check, highlight in the original text,
  restricted aggregates, crossings. Real index, HEAD vs working, uncapped: no row with a
  whole-word match on its page lost; every added row a whole-word match. Variants שמעון הצדיק
  11.1 -> 3.5 s, אהרן הכהן 89 -> 8.2 s, ברוך אתה 44 -> 11.6 s, שלום 15.5 -> 8.3 s, הצדיק 33 ->
  1.9 s; rows drop only where the match was inside a longer word (שלום 3,551; one crossing
  `לאהרן\nהכהן`), and come back where a form was never retrieved (שלום +245: forms with a bracket,
  7 such pages were the gate's first FAIL). Exact: same rows and speed for single words; phrases
  lose only inside-word matches (ברוך אתה 35, לי מי לי 8, אהרן כהן 1 -- the pinned known gap,
  now closed). Fixture snapshot: same rows and order, Variants `score` differs.
  `tests/test_whole_word_variants.py`; 16 mutations killed. Still open: Fuzzy (step 2); the
  web's Exact is sent as mode `'exact'`, which the engine does not treat as `'literal'`, so none
  of the Exact paths reach the website yet (decide with the web stage, after the API check).
- **Codex PR review of #375 (2026-10-02), three fixes:** aggregates get their own limit (above);
  the line-break path highlights the occurrence that met the position, not a fresh first match;
  a run that showed a preview and then ended with no rows turns table sorting back on. Two
  earlier P1 comments were already handled: the character window (word window, 9e8d8172) and
  4+ lacuna brackets in a phrase (owner: ignore; a larger gap reaches them).
- **Stage W step 2, Fuzzy = near spellings (2026-10-02).** A near spelling is a whole word within
  OSA distance 1 (3-4 letters) / 2 (5+), none below 3 -- a letter added, dropped or changed, or two
  neighbours swapped, as Tantivy's fuzzy query counts -- and a prefixed form counts (owner).
  `_near_spellings` builds them all (הצדיק 39,687, ירושלים 75,591; the index holds 4,262 / 2,247).
  Retrieval: their bare and edge-bracket forms as term sets on `content_search` only (it holds the
  same bare tokens as `content`: identical page counts, half the time), constant scores 9/3/1 by
  distance, so under a limit the word comes first, then one edit, then two (with one score,
  הצדיק's 50,000 rows were mostly הדין and צדק). Verification: `_FuzzyMatcher`, a word walk -- one
  regex of all spellings is far too big, and a regex per page cost 10 ms to compile (אהרן הכהן 70 of
  92 s); it walks letter runs, reads a word holding a mark, quote or bracket whole, and for phrases
  takes Exact's separator and gap words. Rows highlight with their own page's spellings
  (`pattern_for`) and show the page's nearest one. Same paths as Variants; position searches use
  term sets on the position field; My Library verifies with it and, after the Codex review of
  61d09a7a, retrieves every form too (Variants and Fuzzy; it had searched the typed word only);
  Composition keeps its old Fuzzy. Real index, 50,000 cap: על 6.4 s, שלום 9.4 s, הצדיק 15 s,
  ירושלים 16 s, אהרן הכהן 24 s, שלום עליכם 35 s; first rows 0.2-4.7 s. Gate (scratchpad
  `fuzzy/gate_fuzzy.py`: uncapped; an oracle written apart from the engine, over every page
  Tantivy's own fuzzy query or the engine retrieves; fails on a broken engine): 0 wrong rows, 0 real
  misses over אהרן 73,936 pages, הצדיק 159,401, ירושלים 60,057, על 383,715, שלום 148,622, אהרן הכהן
  2,885 rows, שמעון הצדיק 1,661, שלום עליכם 2,868 (27 across a break). Misses only where the
  index token holds a digit, nikud or bracket inside the word (16ירושלים, אַהרן, ה[דים: 0-23 per
  word), as for Exact. Under the cap a very common word (שלום: 79K pages hold it) shows only the word
  itself -- the cap, stage 2. `tests/test_whole_word_fuzzy.py`; 26 mutations killed.
- **Codex review of 61d09a7a and the two earlier preview comments (2026-10-03), four fixes:** My
  Library retrieves every Variants / Fuzzy form (above); a Variants phrase whose edge word holds a
  Latin letter or a digit is found across a page break (the pre-check looked for Hebrew letter runs
  only; from e8decfca); a preview keeps engine order under a column sort; an error after a preview
  clears its rows. Each with a test that fails without it; 9 mutants killed.
- **Step 3, day 1 (2026-10-04): where an uncapped search spends its time.** Owner chose "measure,
  then the compact list" over a letter-index prototype: a design round (3 backends, 2 judges)
  ranked a word-aware letter index last on correctness (a second word-boundary definition baked
  into a 4-5 GB artifact) and proposed keeping Tantivy + today's verifiers. Real index, real
  MetadataManager, uncapped, one process per search, warm (scratchpad `spike/profile_uncapped.py`):

  | Search | Rows | Total | Find | Load docs | Check | Build rows | Other | Row memory |
  |---|---|---|---|---|---|---|---|---|
  | Exact שלום | 31,400 | 5.2 s | 0.05 | 1.9 | 0.9 | 0.9 | 1.4 | 0.26 GB |
  | Exact ישראל | 175,530 | 32 s | 0.2 | 14.3 | 5.6 | 4.5 | 7.5 | 1.6 GB |
  | Exact על | 383,713 | 66 s | 0.6 | 20.2 | 19.7 | 10.3 | 14.8 | 3.3 GB |
  | Exact בני ישראל | 37,573 | 22 s | 0.4 | 12.3 | 2.4 | 0.7 | 6.4 | 1.5 GB |
  | Variants אהרן הכהן | 1,943 | 17 s | 1.5 | 6.0 | 4.8 | 0.1 | 4.9 | 1.2 GB |
  | Fuzzy ירושלים | 60,057 | 44 s | 2.2 | 5.4 | 10.3 | 18.0 | 8.3 | 0.6 GB |
  | Fuzzy שלום עליכם | 2,868 | 64 s | 3.0 | 16.4 | 33.3 | 1.1 | 10.5 | 0.7 GB |

  Build rows = display metadata + snippet + Fuzzy's row pattern and nearest spelling: the part a
  compact list defers to the rows shown, with nearly all the memory. Phrases spend theirs loading
  whole-manuscript docs (~62K chars, 140-1,650 us each) for page breaks -- a seam doc per break
  (the text around it) would let the page path find crossings; a separate step. Doc loads are
  26-43 us per page; half of these candidates are V0.7 duplicates (owner's machine only). על's
  check is whole-word retries through the budgeted regex (15 s) -- cheap to fix. A separate
  ~3 GB text store is not justified by these numbers.
- **Step 3, owner decision D8 (2026-10-04): complete only when needed.** A design round proposed a
  complete compact list for every search (ResultSet, 13 commits). With the cap gone a very common
  word takes ~15-30 s to finish on a user's machine (first rows in 1-3 s); the owner chose instead:
  keep today's cut-off for the display, say so in the count ("25,000+"), and compute the complete
  set of a step (manuscript and page ids, no rows) only when a combination needs it -- search-within,
  the all-terms filter, replay. Phase 1 first (combinations and honest counts), then review. The
  ResultSet design is kept in the scratchpad (`compact/FINAL_COMPACT_PLAN.md`) for Phase 2 decisions.
- **D8 Phase 1 plan (2026-10-04)**, from a design round with two skeptics (19 findings, all
  confirmed): (1) search-within restricts in the query at any size; (2) refinement steps record the
  run they came from; (3) a cut-off signal from the engine; (4) "+" counts; (5) an ids-only
  complete mode; (6) replay with progress and Stop that never wipes the chain; (7) search-within,
  all-terms and replay complete a cut-off step first; (8) the real-index gate, registered. Owner:
  the restriction fix ships to both apps (it only adds true matches).
- **D8 commit 1, restriction in the query.** Above 500 manuscripts search-within ran the child
  search over the whole corpus, kept its first 50,000 hits and filtered them afterwards. Now a term
  set on `full_header` (the sys_id is one token of it, V0.8 ids and V0.7 paths alike), always in
  the query; Oxford parts always pass and are tested per page, and a match across a part's own page
  break counts when either page is inside (both the crossing path and `_first_match_in_pages` tested
  the first page only). Real index (scratchpad `d8/gate_restriction.py`): within the 44,055
  manuscripts with ישראל, משה Exact finds 72,067 pages = the brute force (today: 22,673, 69% lost);
  Variants 123,186 = the brute force plus 3 page ids catalogued under two manuscripts, each a verified
  match in a copy under a parent manuscript (today: 23,204, 81% lost). Composition's identical
  <=500 path and the LOCAL index (never restricted) are tracker items.
- **D8 commit 2, steps record their run.** The search records what it ran with; both step builders
  take it (the first step of a chain had lost gap, NOT-words, position and Responsa options; the
  committed step its NOT-words; both the corpus and the query's mode prefix). `RefinementStep`
  gains `corpus_scope` ('all' for saved steps: replay never passed one, so that is what they ran).
- **D8 commits 3+5, "+" counts and the ids-only mode.** The engine says per search (thread-local,
  `consume_last_search_cutoff`) whether a query reached its limit -- page docs, aggregates,
  line-break, My Library -- or Stop ended it; the desktop shows "N+" in the status line, the
  search-within button, the chain's chip and history, keeps it in the session (a full 5,000-row
  snapshot counts as cut), and marks every step after a cut one. `execute_search(ids_only=True)`
  reads every candidate and keeps only page id, manuscript and source: the same membership as a
  normal search case by case (tests: whole words, Variants, Fuzzy, crossings, positions, NOT-words
  with V0.8/V0.7 copies that disagree, a page whose original text breaks the match), no display
  metadata, snippets or previews.
- **D8 commits 6+7, cut-off steps completed first.** `complete_chain` (shared/refinement.py) runs
  each cut-off step again with `ids_only` under the complete restriction of the steps before it;
  a step that was not cut off keeps its sets (each step now records its manuscripts,
  `_result_sys_ids`, runtime only). The desktop runs it as a search (`ChainCompletionThread`:
  progress, Pause, Stop, New) before search-within seeds its restriction and before the all-terms
  filter -- the checkbox and the re-apply when a search lands -- which skips the shown step (it
  only filters the shown rows). Stop or a failure keeps the chain: completed steps keep their full
  sets, the rest stay "+", and search-within falls back to the shown list's manuscripts (badge
  "N+"). A filter-scope change forgets every step's set, so the next combination runs all steps
  in the new scope. A Responsa line-break step is not run again (no ids-only path): it stays "+",
  as does every step after it (tracker P2). Real index (scratchpad `d8/gate_completion.py`): Exact
  ישראל cut off at 9,956 manuscripts completes to 44,055 (= an uncapped search); משה within it,
  27,187 pages -> 72,067 (= brute force, 0 missing, 0 extra); 24 s for both steps, 17 s for
  ישראל alone warm (a full-row uncapped search: 19 s -- doc loads dominate, not row building);
  Stop 3 s in returns in 0.08 s with the chain unchanged. Restore replay, chip removal and the
  zero-result undo still replay capped searches (the steps come back "+"); they gain no Stop.
- **D8 commit 8, the gate registered.** `scripts/verify_search_combinations.py` runs both checks on
  the real index -- restriction (Exact and Variants = brute force) and completion (both steps =
  uncapped oracles; Stop leaves the chain) -- with every expected value computed in the same run.
  Missing index, browse map or libraries.csv: exit 2, never a skip; a parent word that no longer
  reaches the limit: exit 1 ("vacuous"). Shown able to fail: with completion running capped
  searches it exits 1 (9,956 of 44,055 manuscripts, 44,880 pages missing). 156 s on the owner's
  machine. Nightly via `scripts/schedule_nightly_search_gate.ps1` (04:30, after the review-artifact
  check); registering the task is the owner's step. No opt-in env var: a script, not a skipping test.
- **Codex rounds 4-5 (2026-10-04), each finding reproduced by a verifier before fixing.** A Latin
  letter or digit continues a word (as Exact's tokens; Variants/Fuzzy had matched `abc` in `xabc`).
  My Library rows take the main index's rule -- whole words, the text position -- and mark the
  occurrence they were accepted for; its LOCAL-only and Responsa queries read every candidate under
  ids_only (completion had re-run them capped); an ids_only limit is one more than the doc count (a
  query every doc matched read as cut off). A stopped Title/Shelfmark search reports the stop. A
  run's domain and measurement data reset at launch, so a preview is not hidden by the last run's.
  Sweeps for siblings found two more, tracked: Lab Mode in chains (P2) and bracket-only matches on
  whole-manuscript rows (P3, predates the PR).
- Owner measurements after both (partly under test-suite load): בלי ירח 1.3 s and שמעון הצדיק
  1.1 s from submit to rows; שלום 3.4 s; בלי 17 s (loaded); variants שמעון הצדיק 75 s, of which
  66.5 s regex over whole-manuscript docs; fuzzy minutes (a 564K-character pattern).

- **0a. Normalized-token contract for Literal (D1, R2).** A term matches where the query token
  equals a document token under the SAME normalization retrieval uses (`hebword` tokens on
  `content`, `strip_search_diacritics` on `content_search`, the bracket expansion), with offsets
  mapped back to the original text for highlighting. Widening the regex boundary class alone
  is wrong in both directions. Concrete cases the contract must get right (all from R2):
  - `שלום'` is retrieved only through `content_search` and a raw-text boundary rejects it
    (`IE48753255_P000155_FL48753811`, `IE48911035_P000036_FL48911291`);
  - `"`, combining marks U+0300-036F and brackets are token-internal for hebword but
    separators for the current regex, so `שלום"על` and `שלום̇א` would be false whole-word hits;
  - curly apostrophes are separators in raw hebword (`שלום’על` → `שלום`, `על`) and are then
    removed by `strip_search_diacritics`, so the folded side joins what the raw side splits;
  - maqaf is token-internal in both (`שלום־עולם` correctly rejected);
  - Python `\w` and Tantivy's word class differ: `שלום²` tokenizes to `שלום` (a real token
    hit) while a Python `\w` boundary rejects it;
  - pointed text: bare `שלום` does not retrieve `שָלום` today (a retrieval limit a
    zero-rejection gate cannot see — recorded, not changed by this plan).
- **0b. Fix the `]word[` bracket gap** in `_add_bracket_variants` (tracker row). Probe on the
  real index, V0.8 pages: a Tantivy term-regex (brackets allowed anywhere in the word) is a
  strict superset of the engine's forms — שלום +12, ברוך +3, אלהים +3, engine-only 0, similar
  time. For שלום, `]w[` covers 10 of the 12; the other 2 have a bracket INSIDE the word
  (`ש[לום`, `של[ום`), which the verifier then drops anyway (original-text re-search fails,
  `orig_match_missing`), so they belong to 0a's offset mapping. `parse_query` rejects a
  bracket-class regex, so the general route means building `Query` objects — also 0a.
- **0c. Fix search-within for aggregate hits**: map to the page first, then test the page's
  manuscript; the <=500-id query filter must not rely on an aggregate's first header (tracker row).
- **0d. Fix position search** to use the first position-valid occurrence (tracker row).
- **0e. Representative and evidence identity.** A row stores the selected document (source +
  scope + doc identity), the match spans and an index-generation stamp — never uid alone:
  `get_full_text_by_id` picks an arbitrary doc (R2: `IE162825191_P000001_FL162825193` hydrated as
  V0.7, 364 chars, where the representative was V0.8, 379 chars).

### Stage 4 (first). The first-search-after-launch delay

- The FL ID index build (973K entries) and the browse_map repair were still running during the
  first search: defer until the first search ends or the app is idle, or build lazily.
- UI tail (~3 s warm): the `search_ui_perf` line (added, uncommitted, `genizah_app.py`) names it.

### Stage 1a. Page-first traversal, system/part docs KEPT for every mode

- **Pass P:** `(<query>) AND scope:page`, score order. **Pass X:** `(<query>) AND (scope:system
  OR scope:part)`, run after P for ALL modes for now, keeping rows whose page P did not produce.
- Aggregates store their FIRST page's `source`; never filter X by `source`.
- Same row set as today by construction, except the intentional differences declared for G2.

### Stage 3. Streaming with a fixed representative (first results < 1 s)

- Engine `batch_callback`; checkpoints tied to candidates EXAMINED, not rows emitted; callbacks
  outside per-item exception handlers; partial results kept on Stop; no perf telemetry for
  stopped runs; every batch carries the run identity.
- **First row per uid wins** (today: last content at first position). V0.7 rows only at the end.
- Everything that can later remove or move a row is decided BEFORE a row is emitted (R2):
  - `exclude_words` evaluated against the representative's own text, and an excluded uid stays
    consumed (a later X/V0.7 row must not resurrect it). Declared change: exclusion now looks at
    the page, not the whole manuscript, for rows that used to be system rows;
  - `text_position` (with 0d);
  - responsa Within Document: whole-document checks finish first; warnings and counts go in run
    metadata, not on the first row (today `search_engine.py:3027` mutates row 0);
  - **LOCAL (corpus_scope != 'genizah'): buffered until RRF fusion** — never append LOCAL after
    emitted Genizah rows (fusion interleaves them);
  - remembered filters: domain enrichment (`genizah_app.py:20305`) and PGP enrichment (`:22834`)
    re-apply filters from callbacks; snapshot the active filters and apply them before emission,
    or restrict the prefix invariant to the unfiltered store. Badges may change; identity,
    evidence and order may not.
- UI: split `on_search_finished` into begin / batch / finish; completion never clears and rebuilds
  emitted rows. First paint ~50 rows. Sorting stays off for the whole run — `load_next_batch`
  re-enables sorting and applies filters (`:21563`), so disabling the header is not enough.
  Enrichment merges per batch and rejects stale runs; the synchronous measurement fetch moves off
  the UI thread.

### Stage 1b. Skip system/part docs where it is proven safe

- Only **Literal**, only after 0a and 0b, and only for query shapes that are page-local:
  single-term Literal (and later, proven one by one: plain page-local Regex, a single Responsa
  component without flexible spacing, `L1:` line start).
- **Variants and fuzzy keep X** (R2: `IE49532141_P000003_FL49532162` for variants — matched inside
  ירושלים via `sys:990001452780205171` — and `IE202425470_P000001_FL202425472` for fuzzy via
  `part:MS. Heb. e. 98/12` are returned only through aggregates). Their retrieval/verification
  contract is a separate decision; D1 does not cover them.
- Multi-term, gap, line-break, Responsa (incl. Within Document, where `cross_page=False` does not
  prove page independence) and `L<n>:` keep X.

### Stage 2. Exhaustive search on the desktop, over a result store

- `execute_search(..., limit=None)`; web callers keep `Config.SEARCH_LIMIT` explicitly until
  stage 5 (also the line-break site and the four LOCAL calls, `limit or Config.SEARCH_LIMIT`).
- Collect addresses once, load/verify in batches. Tantivy `offset` paging is not a cursor (it
  re-collects a growing top-K).
- Rows go to a result store (the same design as stage 5) holding lean rows plus 0e's evidence
  identity; `full_text` is hydrated from that identity, never by uid.
- True counts, kept apart: candidates / verified / filtered / displayed. Existing losses fixed
  with the store: session save/restore keeps 5,000 rows (`genizah_app.py:31626`); a refinement step
  commits only once its parent search is complete; export reads the store, not the loaded rows
  (tracker: "Desktop Search export, filters and counts see only the loaded rows").

### Stage 5. No cap on the web (D2) — per-search disk store (R2's choice)

R2 compared (a) lean rows per session, (b) a per-search disk store, (c) lazy verification over
the address list. For על (local replays/models, not production peaks): (a) ~463 MiB of retained
rows per active search, 1,512 MiB replay RSS, multiplied by sessions; (b) 1,116 MiB replay RSS
with <=500 rows in memory, much of it resident index pages; (c) cannot give complete counts,
global filters, sorts or exports without the same full verification, and addresses do not
survive an index rebuild. **Choose (b)**:
- a worker writes batches to a temporary SQLite store and returns a small handle; the store
  outlives the worker's temp directory (`web/research_jobs.py:182`) and is retained with a
  lifetime and disk budget;
- consumers read the store: result rendering/count/slice (`web/pages/search_results.py:287`),
  list filtering (`web/pages/search.py:4303`), API `total` (today `len(results)`,
  `web/search_api.py:1738`) plus a continuation token that reads the same run, filters and order,
  export (5,000 cap, `web/export_state.py:110`; the workbook build at `web/export_service.py:733`,
  `:910` must also stream), restore (1,000 rows per tab, 250 across tabs/users,
  `web/pages/search_state.py:333`, `:425`), API excerpts (`shared/search_serializer.py:399`);
- worker transfer: today the whole result is loaded into the parent (`web/research_jobs.py:246`,
  limits: 1 worker, 4,096 MiB, 512 MiB compressed transfer) — replaced by the handle.
- The exact server peak is a deployment measurement: parent + workers + index residency + export.

### Stage W. The "Words" option (D4-D7) — the control dropped (owner); whole-word Variants (step 1) and Fuzzy (step 2) built

**Found while designing (pre-existing, both apps):**
- *Fuzzy retrieves only the exact word.* `build_tantivy_query` sends `"term"~1|2`, which the
  parser reads as a one-word PHRASE slop, not a fuzzy term: שלום 79,656 docs either way, while
  `Query.fuzzy_term_query(distance=1)` gives 318,297 (ירושלים 19,401 vs 145,646). Fuzzy rows are
  pages holding the exact word, plus a variant that the 8,000-form regex happens to find inside a
  retrieved whole manuscript.
- *Variants retrieve with at most 200 forms per term* (`get_variants(limit=200)`, `:1684`), while
  the regex verifies up to `REGEX_VARIANTS_LIMIT` = 8,000; a page holding only form 201+ is found
  only incidentally, through a whole-manuscript doc that holds a top-200 form elsewhere.
- *Today's "inside words" is incidental too:* retrieval is whole-token, so a match inside a longer
  word is found only when its manuscript also holds the whole word (the substring regex over the
  whole manuscript then reports the first occurrence).

**Semantics.** Whole words: every query term matches a whole hebword token of the text (brackets
and the mark set of `strip_search_diacritics` tolerated, as today's bracket forms and
`content_search` do). Also inside words: a term matches any substring of a token, and a phrase's
words may be separated by any run of separators, lacuna brackets included (D6). Both apply to the
term *forms* of the mode: Exact = the term; Variants = its variant forms; Fuzzy = see W-Q1.

**Retrieval, per setting** (tantivy-py 0.25 has `term_set_query`, `regex_query`,
`regex_phrase_query(words, slop)`, `fuzzy_term_query`):
- Whole words, one term: the term's forms as whole tokens (a term set over `content` /
  `content_search`, with the bracket forms) AND `scope:page`. Forms are the SAME list the verifier
  uses (no 200-form cut).
- Whole words, several terms: page docs by adjacent-pair `regex_phrase_query` (each position = an
  alternation of that term's forms, bracket-tolerant; slop gap + 3); whole-manuscript docs only for
  crossings, by the word window. If one pair's alternation is too large for the term dictionary
  (measure), fall back to AND of per-term sets for that pair.
- Also inside words: per term `regex_query('.*form.*')` on the folded field (bracket-tolerant
  between letters), AND across terms (no phrase, so no slop: lacuna runs are crossed), page docs;
  whole-manuscript docs for crossings only, by the word window. An unanchored term-dictionary
  pass costs 3.4-5 s per term (PR 3 #9 measurement) -- the price of this setting.
- Verification: whole words -> a token check (each text token, bracket-stripped and folded,
  looked up in the term's form set; phrases: consecutive word tokens with <= gap words between),
  replacing the 564K-character fuzzy/variants alternation; inside words -> today's substring
  regex. Highlight = the verified span.

**Plumbing.** `execute_search(..., word_match=None)`: 'whole' | 'inside'; None = today's behaviour
for that mode, so the public API, Joins Lab, research workers and replayed old refinement steps
are unchanged until they pass it. Desktop: a combo after Gap (Exact/Variants/Fuzzy only),
`SearchThread(word_match=)`, saved in `session.json` on the always-restored path (like corpus
scope), recorded in history params and refinement steps, reset by New. Web: Advanced options,
`_safe_set('search_word_match')`, passed at the `execute_search` call; refinement steps carry it.
Preview/streaming unchanged (page-first). Prefixes (D7) later as a third value.

**Open questions (Codex round, then owner):**
- W-Q1 **answered by the owner 2026-10-01: near spellings** -- edit distance 1 (short words) / 2
  (5+ letters), as the desktop tooltip ("Fuzzy search: Levenshtein distance") always promised;
  retrieval by `fuzzy_term_query`, verification by edit distance, not the variant list.
  **Built 2026-10-02 (step 2, above)** with two changes: a swap of two neighbouring letters is
  one edit (OSA, as Tantivy's fuzzy query counts: שולם for שלום), and retrieval is term sets of
  the near spellings themselves -- `fuzzy_term_query` measured 3-4x slower (הצדיק 6.1 s vs 1.2 s).
  **Owner 2026-10-02: a whole word within that distance counts even when the edit is an added
  prefix letter** ("It's not bad that we include ושלום in fuzzy. It's fuzzy."). So in Fuzzy the
  whole-word rule applies to the matched WORD (a whole token within the distance), not to the
  query string: ושלום matches שלום, but שלום found inside ושלומות (3+ edits) does not.
- W-Q2: is `regex_phrase_query` with large alternations fast enough, and is term-set + phrase
  retrieval provably a superset of the token verifier (bracket forms, marks, finals, geresh)?
- W-Q3: which callers must pass `word_match` explicitly to keep their results (web /search
  defaults to 'whole' per D5; the API keeps None)?

### Out of scope (separate caps)

Composition (50 candidates per chunk), `/api/parallels` (200 groups), Lab engine (own 50K and 5K
limits), Joins Lab cross-side neighbours, Title/Shelfmark search.

## Gates (each shown able to fail; mutation named)

- **G1 fixture equivalence** — `scripts/search_output_snapshot.py` before/after, compared against
  DECLARED expected output, and failing on any unexpected `ERR` value (the script serializes
  exceptions as `ERR`, `scripts/search_output_snapshot.py:85-91`). New fixture rows: a phrase
  across a page break, a single word on a multi-page manuscript, a prefixed-only page, an Oxford
  part, two differing page docs sharing one uid. Mutation: revert first-wins to last-wins.
- **G2 real-index coverage** — old code with the cap raised to the true count vs new code, for
  שלום, של, variants, gap>0, Regex, fuzzy, responsa, `L3:`, and Literal `תעודדו תענגו` gap=0
  (crosses `IE36973187_P000002_FL36973201` → `IE36973187_P000003_FL36973211` in
  `sys:990001669610205171`). uid sets and per-uid reading/spans equal, EXCEPT a declared
  expectation file listing, per uid, every intentional difference and its cause: D1 (0a), the
  bracket fix (0b), restricted aggregate hits (0c), later position-valid occurrences (0d),
  first-wins representatives (stage 3) and page-based exclusion (stage 3). Any difference not in
  the file fails; any declared difference that does not occur also fails. Mutation: disable X →
  the phrase must fail.
- **G2b token agreement, both directions** — (i) retrieved pages the new Literal verifier
  rejects: 0 or each explained (catches the `שלום'` pages); (ii) occurrence-level false
  positives: fixture strings `שלום־עולם`, `שלום"על`, `שלום̇א` must not be whole-word hits;
  (iii) positives: `שלום²`, `שלום’על` (as `שלום`), and pages with `]שלום[`, `[שלום]`, `שלו[ם]` are
  returned and highlighted. Mutation for (ii): drop maqaf from the word class → a false positive.
- **G2c search-within** — separate positive fixtures, each restricted to its manuscript both
  below AND above 500 manuscripts: (i) the cross-page system-doc phrase above; (ii) an Oxford
  part whose matching manuscript is NOT the part's first header; (iii) the same through the
  line-break path. Mutation: keep either the first-header filter or the line-break
  `restrict_uids` check → its fixture fails.
- **G2d stream contract** — one fixture per case with its expected survivors AND order written
  down: LOCAL fusion (G1,L1,G2), exclude_words with a later duplicate of an excluded uid,
  remembered filters applied mid-run, a later position-valid occurrence. Asserted on the engine
  output AND on the displayed table order (the table could sort rows the engine emitted
  correctly).
- **G3 streaming** — on success the concatenated batches EQUAL the final list (not just a prefix);
  at least one batch arrives before completion; batches are immutable once captured; Stop gives an
  explicitly incomplete, consistent set. Mutations: reverse the final rows after two batches;
  emit no batches.
- **G4 first paint** — measured from search submission to the first painted row, cold and warm,
  on the owner's machine: < 1 s for שלום. Mutation: a 2-second sleep before first paint must fail.
  (`search_ui_perf` measures handler work, not paint.)
- **G5 memory budgets** (provisional numbers, owner may change them before stage 2 merges) —
  desktop: peak RSS during an על search <= 2.0 GiB on the owner's machine; web: N = 4 retained
  completed searches for על plus one full export, server process growth <= 1.5 GiB over its
  idle baseline. Derived from R2's replays (1,512 MiB for lean rows in memory, 1,116 MiB for the
  store). Mutation: keep `full_text` in every row → must exceed the ceiling.
- **G6 evidence survives** — the selected version and cross-page evidence survive hydration,
  export and restore; API continuation returns every verified uid exactly once with honest
  incomplete/complete counts; store lifetime, cancellation and an index-generation change never
  lose or misread rows.
- `python scripts/run_local_tests.py`.

Result-affecting stages (0a, 1b, 2, 5): plan and review at `/effort xhigh` (CLAUDE.md).
