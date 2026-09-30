# Desktop and web search: full coverage and first results under a second — plan (2026-09-30)

**Status:** DRAFT, not started. Owner decisions D1-D3 answered 2026-09-30. Two Codex rounds
(read-only, unsandboxed):
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
