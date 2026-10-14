# Composition Search conditions: "must appear", "must not appear", adjacent images

**Status:** plan rev. 4, reviewed: Codex rounds 1-4 (DO NOT SHIP, DO NOT SHIP, SHIP WITH FIXES, SHIP; §11). Not
implemented. Owner decisions recorded in §9.
**Origin:** a heavy desktop user's letter (12 Tishrei 5787), items D1 (exclude text), D2 (all of
these texts) and D3 (two sides of a leaf). The September plan that triaged the letter is
`C:\Users\gersh\.claude\plans\see-c-users-gersh-downloads-glistening-sloth.md` (rev. 4); this
document supersedes its section D for D1-D3. D4 ("this text only") is not in scope.
**Scope:** desktop, letter-level ("passage") Composition Search only. One release, one branch
(`feat/composition-conditions`). The rules live in `shared/` so the website can adopt them later.
**Date:** 2026-10-14.

## 1. What the user asked for

1. **Exclude the quoting works.** Search a Talmud passage, but drop manuscripts that also contain
   Halakhot Gedolot or the Rif, which quote the Talmud. Today's "Filter Text" does something else.
2. **Two sides of a leaf.** Text A in one box and text B in another; find manuscripts where A is
   on one image and B is on the next one, as the desktop Joins Lab already does for word search.
3. **All of these texts** (same letter). 2-4 texts that must all occur in the same manuscript,
   each possibly in several versions. His first example: the prayer text together with the
   halakhic instructions in the siddurim of Saadia, Amram and others.

## 2. Facts this plan rests on (read or measured 2026-10-14)

**Engine and cost**
- A letter-level search on the installed index (702,466 records, `...\passage_index\current\
  manifest.json`) takes p50 0.16-0.66 s for 50-2,000-letter queries, warm, normal depth
  (`docs/specs/passage-index-build-measurements.md` §7). The 7,562-character figures in
  `shared/passage_parallels.py:151` (1.5 s normal, 18.6 s deepest, including rendering) were taken
  on a different, 759,224-record experimental build; they show the depth scaling, not this index.
- A compound run costs one search per main-text witness plus one per condition version. Without a
  bound that is unbounded (the witness list itself allows 25, `desktop/passage_witnesses.py:41`).
- Every hit is one page (image); its identity is the record id `{sys_id}_IE{n}_P{000000}_FL{n}`
  (`shared/passage_corpus.py:9`, `shared/passage_parallels.py:669`). The sys_id regex
  (`shared/sys_id_patterns.py`) and the IE/P shapes in
  `shared/metadata_manager.py::parse_full_id_components` (`:633`, `:643`) parsed all 702,466 ids.
  Nothing in the passage stack parses IE or P today. LOCAL headers use `LOCAL...Fxxxx`, not IE,
  and letter-level search refuses LOCAL scope (`desktop/passage_lifecycle.py:1675`).
- `record_allowed` runs after every admitted gram's postings are fetched corpus-wide
  (`shared/passage_search.py:178`) and before the candidate and verify caps are spent (`:219-239`).
  A restricted search therefore costs the same postings; verification and rendering may shrink.
  Caps by depth: normal 500,000 postings / 200,000 candidates / 3,000 verifications; the deeper
  profiles admit 2M and 5M postings and verify up to 50,000 (`shared/passage_policy.py:283-294`).
- **What can make a search incomplete**, by layer:
  - `QueryReport` (`shared/passage_search.py:59`): `candidates_truncated`, `verify_truncated`,
    `below_min_span_query` (a query under the floor returns nothing, `:368`), and the posting
    budget: `_admit_grams` (`:98-160`) admits grams by frequency band within the budget and drops
    the rest. Codex's probe excluded every posting with both truncation flags still false.
    `grams_excluded` also counts grams absent from the index, so it is not a loss signal by itself.
    Budget losses are routine, not rare: measured 2026-10-14 on the installed index, the median
    300-letter query carries 970,403 postings against the normal budget of 500,000.
  - **Admitting every gram is cheap on a restricted page set.** The same measurement (12 real
    texts per length, an allowed set of 2,000 pages, warm): fetching every gram's postings and
    filtering them by a page mask took a median 0.033 / 0.061 / 0.193 / 0.335 s (worst 0.040 /
    0.099 / 0.312 / 0.632 s) at 100 / 300 / 1,000 / 2,000 letters, leaving a median 880 / 2,725 /
    7,899 / 15,050 postings to cluster. `BUDGET_NO_CAP` already exists (`shared/passage_search.py:111`).
  - The searcher's result: `partial` (stopped, `passage_parallels.py:845`); rows whose text lookup
    failed are dropped and only counted (`:747`); duplicate photographs are demoted to Filtered
    (`:803`); the display cap of 200 manuscripts applies to Main and to Filtered (`:733`).
  - The desktop: fusion caps Main and Filtered separately after fusion
    (`desktop/passage_witnesses.py:437`, `:475`); grouping keeps one page per P number in a
    manuscript or Part, so two images with the same P in different IEs collapse into one
    (`shared/search_engine.py:5325`, pre-existing); title groups over the threshold move to
    Appendix (`:5477`); the session saves at most 5,000 rows per bucket (`genizah_app.py:32486`).
- Stopping has two meanings in two layers. In `search_passage` a checkpoint that returns False
  stops verification and returns the hits found so far, and "nothing else in the result says so"
  (its docstring, `shared/passage_search.py:345`). The searcher's `_Checkpoint` wrapper ignores a
  False return and stops only on `InterruptedError` (`shared/passage_parallels.py:375`). The desktop
  adapter forwards neither a checkpoint nor a record filter today (`desktop/passage_lifecycle.py:662`,
  `:716`); the Stop guard knows only `MultiWitnessCompositionThread` and refuses every other
  letter-level scan (`genizah_app.py:18187-18201`).
- `_run_one_witness` filters hits by `min_boundary_matches` after the search
  (`shared/passage_parallels.py:1003`).
- `render_cap` is fixed when the searcher is constructed (`passage_parallels.py:527`); passed to
  the search method it would vanish into `**_ignored` (`:554`).

**Pages and images**
- The index leaves out 246,083 pages (`excluded_records.tsv` beside the manifest): 231,679 short
  (under 80 letters, `shared/passage_hygiene.py:45`), 9,007 target sheets, 5,397 library stamps.
  50,356 of the short pages have 40-79 letters (37,831 manuscripts); 68,874 have 28 or more
  (48,555 manuscripts). Measured 2026-10-14 from `Transcriptions.txt` with `norm_stream_fast`.
- Image numbers have gaps: of 137,186 multi-page (sys_id, IE) sets, 3,436 have internal gaps
  (6,997 runs, longest 109). Most are left-out pages, but 396 missing P values in 223 sets are not
  in the ledger: the source numbering itself jumps (e.g. P172 to P174). So "adjacent" is literal
  P±1 in the same sys_id and IE: right for left-out pages, and an approximation where the
  numbering jumps.
- P is the corpus page number that browse stores (`shared/indexer.py:326`). The viewer's
  Next/Prev walks the pages that exist, and a missing number is replaced by an earlier page
  (`shared/search_engine.py:5673-5679`). So "adjacent image" is not the same as the viewer's Next.
- No leaf/side data reaches a passage record. Sources exist (NLI image names, FGP `image_side`
  and folio labels `shared/fgp_service.py:286`, FJMS `Image_Side` `shared/fjms_service.py:3525`),
  but none is joined to (sys_id, IE, P); the positional NLI join refuses when it cannot align
  (`fgp_service.py:397`). A true "same leaf" relation is out of scope (§10).

**Desktop composition today**
- `MultiWitnessCompositionThread` (`desktop/gui_threads.py:549-793`) runs one uncapped call per
  witness, checks Stop before each witness and the index generation before and after each
  successful call, keeps each witness's rows (`desktop/passage_witnesses.py::WitnessSet`), drops
  each call's `query_report` (`gui_threads.py:717`), and combines by rank fusion, an OR
  (`shared/passage_fusion.py:226`). Fusion keeps only the winner's evidence (`:234`, `:306`).
- Removing a witness re-fuses cached rows and republishes them as complete, without a search
  (`genizah_app.py:19190`). Auto-expand rounds can be armed and advance on their own
  (`:18517`, `:29216`). The per-run cache keeps rows only (`:19348`).
- Dispatch re-validates the method at click time and can demote an unavailable letter-level
  search to chunk (`genizah_app.py:28199-28251`).
- Session restore accepts snapshots with missing provenance and every chunk snapshot; for passage
  it checks availability and scope, not that the rows came from the saved query
  (`genizah_app.py:18019-18062`); the startup path shows its deferred snapshot unconditionally
  (`:17478`). History stores parameters, not rows (`:32191`; identity compared on `search_params`,
  `shared/session_persistence.py:246`); a legacy history branch restores a stored snapshot directly
  (`genizah_app.py:32128`). Delivery guards distinguish New/reset, not every newer run (`:20903`,
  `:28794`).
- Display: flat mode merges categories into "All" and marks only Filtered and Excluded
  (`genizah_app.py:29493`, `:29821`); the viewer is built from one entry per hit page (`:33571`),
  its page navigation can cross manuscripts and keeps the entry's highlight pattern
  (`desktop/result_dialog.py:3180-3236`); compact controls share `load_page` (`:326`).
- Exports walk five buckets in several separate walkers: selection collector (`genizah_app.py:30081`,
  `:30141`), shared table rows (`:26417`), XLSX hierarchy (`:26648`), TXT (`:27123`), the main walk
  (`:26169`).
- Three "why is this not in Main" mechanisms exist and must stay distinct: Exclude Manuscripts
  (identity, whole manuscript, after the search, `genizah_app.py:27661`); Filter Text (the part of
  the QUERY a record matched is a known source; never searches the corpus,
  `passage_parallels.py:1038`); and Appendix/Filtered routing (`desktop/comp_view_filter.py:218`).
- Joins Lab's cross-side code (`shared/joins_lab.py:294-355`) is keyed `(sys_id, page)` without
  IE and reads neighbours through the page-substituting `get_browse_page`. Reuse the idea only.

## 3. The model

The main text box stays as it is, including its Witnesses panel. Below it, the user adds up to
**four conditions**. Each condition has:

| Field | Values |
|---|---|
| Text | 1-10 versions, separated by a blank line or loaded from files; any one counts |
| Requirement | **must appear** / **must not appear** |
| Where, relative to the main text's page | **same image** · **adjacent image** (P±1, same sys_id and IE) · **same manuscript** (any image of the same sys_id) |
| Label | optional short name shown in results and exports |

A compound run is limited to **25 searches** in all (main-text witnesses plus condition versions),
the existing witness cap; the dialog says so before Search.

The three requests are uses of this one model: "two sides" is *must appear, adjacent image*;
"exclude the quoting works" is *must not appear, same manuscript*; "all of these texts" is the main
text plus *must appear, same manuscript* conditions.

**What a result is.** A result row is still a page the main text matched. Each condition is judged
for that page's unit:

| Condition outcome | Search complete | Search cut, stopped or failed |
|---|---|---|
| must appear, found | satisfied | satisfied (a verified match is valid even if the search was cut) |
| must appear, not found | page **fails** (not shown; counted) | **unresolved** |
| must not appear, found | page **removed**, with the evidence | removed |
| must not appear, not found | satisfied | **unresolved** |

A page goes to: **Main** if every condition is satisfied; **Removed by a condition** (collapsed
group) if any "must not" was found; **Not fully checked** (collapsed group) if nothing removed or
failed it but a condition is unresolved; nowhere if a "must appear" condition was searched
completely and not found. The status line gives the count that failed each "must appear".

**"Not found" means not found in the searchable pages** (owner, 2026-10-14). Pages the index left
out are not searched. The evidence strip and the exports state, per manuscript, how many of its
pages could not be searched (from `excluded_records.tsv`, read once per index generation); the help
text explains why.

**A condition is searched exhaustively over its page set.** Every gram of the version that occurs
in the index is admitted (no posting budget), and each gram's postings are filtered to the allowed
pages before clustering. So the posting budget, the one loss no report flag shows, cannot hide a
condition match. Whether a passage counts as found is then decided by the engine's own matching
rules (anchors, density, length floor) exactly as for the main text. "Exhaustive" means every gram
is admitted over the mask, not that every alignment the verifier could accept is found: a text
built from very few distinct five-letter sequences (e.g. one letter repeated), or one whose
every fifth letter differs, forms no candidate on either path (Codex round 3 probes). That is the
engine's matching rule, shared with the main text, not a condition-specific loss. Condition searches use the
active width and length profiles with the **deepest** depth profile's candidate and verify caps,
because the set is small and those caps then rarely bind.

**When a condition's search counts as cut** (per version): `candidates_truncated` or
`verify_truncated` within the set; or the run was stopped before or during it (a stop inside
verification is recorded by the worker's checkpoint, never inferred from the hits); or the call
failed. A cut version leaves its condition unresolved for every page whose unit it covered. A
version under the length floor, or over 10,000 letters, is refused before any search, naming the
condition.

**Conditions are judged on verified hits, not rendered rows.** A condition search returns the
engine's verified record ids and spans before any rendering, display cap, text lookup, Filter Text
or duplicate-photography routing, so none of these can hide a condition match. Evidence is rendered
later, only for pages that are shown.

**A condition's page set** for the main text's pages:
- same image: the page itself, key `(sys_id, IE, P)`;
- adjacent image: `(sys_id, IE, P-1)` and `(sys_id, IE, P+1)`, literal arithmetic, never "the next
  record present";
- same manuscript: every indexed page of that sys_id, including pages the main text did not match.

The unit "manuscript" is the catalogue record (sys_id). Parts that span several sys_ids are not
merged in this release (§10).

## 4. How a compound run works

New pure module **`shared/compound_query.py`** (no Qt; registered in
`tests/test_no_back_edges_core.py:776`): the condition model, record-key parsing, page sets, the
fingerprint, `judge()`, and `ConditionOutcome`/`CompoundOutcome`.

**Engine** (`shared/passage_search.py`, `shared/passage_index.py`, `shared/passage_parallels.py`):
- `search_passage(..., record_mask=None)`: when a boolean mask over records is given, every gram
  present in the index is admitted and each gram's postings are filtered by the mask before they
  are appended (per gram, so peak memory is one posting list), and `record_allowed` is not needed.
  The report says the search was exhaustive over a mask. Callers without a mask are unchanged.
- A page index, built once per index build identity from the record ids and cached with it:
  `(sys_id, IE, P) -> record` and `sys_id -> records`. It turns a page set into a mask; a page set
  entry with no record (a left-out or missing page) simply contributes nothing.
- `PassageSearcher.search_condition(text, page_keys, checkpoint) -> (hits, report, stopped)`: takes
  the condition's page set as `(sys_id, IE, P)` keys and sys_ids, never a prebuilt mask, and builds
  the mask INSIDE the call from the page index of the index it is running on, checking the page
  index's build identity against that index before any posting is read; it calls
  `search_passage` directly (not `_run_one_witness`, which filters by `min_boundary_matches`
  afterwards) and returns verified hits unrendered. Its checkpoint wrapper returns False to the core
  on Stop and sets `stopped`, so a stop inside verification is always reported. Conditions never
  pass through `search_composition_logic`, so `**_ignored` cannot swallow them; that method keeps
  no condition keyword (a test pins this).

**Desktop adapter** (`desktop/passage_lifecycle.py`): `PassageSearchAdapter.search_condition` under
the same per-call lease, so the mask is built against the leased index and no mask built for one
index generation can reach another; the page index cached by build identity (copied ids and record
numbers, never the index's own mappings);
`get_passage_searcher(..., render_cap=0)` for every compound step (fixed at construction);
`checkpoint` forwarded to both methods. The Stop guard (`genizah_app.py:18187-18201`) treats the
compound worker as stoppable.

**Dispatch** (`genizah_app.py::run_composition`):
1. Validate AFTER the dispatch-time method re-validation and any demotion: if conditions exist and
   the method that will actually run is not letter-level (chunk, Lab, or a passage search demoted to
   chunk), refuse with a message and an offer to switch. Never run while ignoring a condition.
2. Validate versions against the active floor and the 25-search cap.
3. Stamp the compound query and its fingerprint at dispatch (as the witness provenance is stamped).

**Worker** `CompoundCompositionThread` (`desktop/gui_threads.py`), shaped like
`MultiWitnessCompositionThread` but:
- steps = each main-text witness, then each condition version; progress "search 2 of 5";
- Stop is checked before every step and inside every step through a `checkpoint` that raises
  `InterruptedError`; a stopped or failed step marks only its condition unresolved and the run
  goes on to judge what it has; the generation is checked before and after every step, including a step that failed, and a
  change aborts the run;
- keeps every step's `query_report` and outcome, never drops them;
- carries a run token; the scan, grouping and deferred-restore deliveries all check it, so an older
  run can never land after a newer one (`_CrossSideWorker`'s guard, `desktop/join_workbench.py:1832`);
- keeps the existing `perf_signal`/telemetry wiring; any new telemetry property is a count, never
  text, and is added to the allowlist (`desktop/telemetry.py:319`).

**Combining**:
1. Main text: uncapped rows per witness, fused WITHOUT the cap (a fuse-only path beside
   `fuse_and_cap`).
2. Page sets from all main-text pages, Main and Filtered buckets alike, as keys; one
   `search_condition` per version with those keys (the mask is built under the lease, above).
3. `judge(main_rows, condition_results) -> CompoundOutcome`, before any grouping. Each main row gets
   a `compound` record: per condition the state, the evidence record ids, and the unit's unsearchable
   page count.
4. Grouping keyed by the full identity `(sys_id, IE, P)`: fix `group_pages_by_manuscript`'s P-only
   de-duplication (`shared/search_engine.py:5325`) for every composition result, in its own commit
   with its own tracker row and test, because condition evidence pages from another IE would
   otherwise be dropped.
5. Cap after judging: Main and Filtered, 200 manuscripts each, as today. Removed and Not fully
   checked are never capped (they render lazily). Appendix routing then runs on Main as today and
   must carry the `compound` record with each row.
6. Exclude Manuscripts runs last, but merges the PAGES of a manuscript that appears in more than one
   bucket instead of keeping one whole object per sys_id (`genizah_app.py:29252-29266` keeps the
   first and drops the other bucket's pages, with their outcomes).

**Paths that must not bypass the conditions** (each has a test, G9):
- No per-run cache reuse: a compound run always runs every step (it costs seconds). The cache key
  still includes the fingerprint, so a plain run never reuses compound rows or the reverse.
- Removing a witness while conditions exist does not republish (`genizah_app.py:19190`): the results
  are marked out of date and the user is asked to search again.
- Auto-expand: its button is disabled while conditions exist, `_run_auto_expand` and
  `_advance_auto_expand` refuse, and adding a condition clears any armed round.
- Session: the editable conditions are saved separately from the fingerprint of the conditions that
  produced the displayed rows. A snapshot whose fingerprint or outcomes are missing or do not match
  is not displayed (the user searches again); an old snapshot with no conditions restores as today.
  The deferred startup restore and the legacy history snapshot branch apply the same check.
- History: the conditions go into `search_params`, so two compound searches with different
  conditions are different entries; restoring an entry replaces the current conditions, including
  clearing them when the entry has none.
- Flat mode, regrouping, the viewer and every export read the stored `compound` record; none
  recomputes an outcome from the (capped) snapshot.
- Empty Main: the "No Results" exit (`genizah_app.py:28733`) and the export guard (`:26186`) count
  the two new groups, so a run where every page was removed or left unresolved still shows and
  exports them.
- Exports name the main text and conditions that were DISPATCHED (the stamp), not the live boxes
  (`genizah_app.py:26291` reads the live text today), so editing the text after a search cannot
  relabel its results.

## 5. What the user sees

- **Conditions strip** under the main text: one row per condition (requirement · where · label or
  first words · edit · remove) and "+ Add condition"; shows "N searches" against the cap. In chunk
  or Lab mode it stays visible but disabled, with the reason.
- **Condition dialog**: requirement, where, label, a text area for the versions, "Load from files"
  (the Witnesses dialog's split rule), and a live letter count against the floor for each version.
- **Result tree**: Main as today. Under each manuscript, the pages that satisfied a "same
  manuscript" or "adjacent image" condition are listed beside the main-text pages and marked with
  the condition's label. Two new collapsed groups, **Removed by a condition** (each row: which
  condition, on which image) and **Not fully checked** (which condition, and why: cut, stopped,
  failed). Both are stable categories in `desktop/comp_view_filter.py`; flat mode marks their rows
  as it marks Filtered and Excluded today.
- **Manuscript Viewer**: an evidence strip ("Main text: image 12 ✓ · Condition 1, adjacent:
  image 13 ✓ · Condition 2, must not: not found in the searchable pages, 2 pages could not be
  searched"). Evidence is keyed by the full page identity: each image is highlighted with the
  pattern of whichever text matched THERE; on a page outside the judged result (after navigating
  across manuscripts), the strip says so and no condition claim is shown. Compact mode shares
  this.
- **Exports** (xlsx/docx/csv/txt, whole list and selection): one column per condition with its
  stored outcome; the unsearchable-page count; sections for the two new groups.
- **Help text**: conditions search the corpus, unlike Filter Text; give "must not" the distinctive
  part of the quoting work (passages of Halakhot Gedolot that are not Talmud quotations), or it
  will also remove the Talmud manuscripts; "adjacent image" is the next or previous photograph by
  its number in the same image set, usually but not always the other side of the leaf; "not found"
  means not found in the searchable pages.

Proposed Hebrew (owner approves before release): תנאים נוספים · + הוסף תנאי · חייב להופיע · אסור
שיופיע · באותה תמונה · בתמונה סמוכה · באותו כתב יד · הוסרו בגלל תנאי · לא נבדקו עד הסוף · לא נמצא
בדפים הניתנים לחיפוש. Reuse existing strings where they fit (`shared/genizah_translations.py:2463`,
`:4126`). Desktop What's New: runtime content and version per `desktop/update_ui.py:102`, `:311`.

## 6. Gates (each must be shown able to fail by a named code mutation)

- **G1 relations and identity** (`tests/test_compound_query.py`): same image; adjacent P-1 and P+1;
  not adjacent across a gap; not adjacent across two IEs of one sys_id; same manuscript includes an
  unmatched page; LOCAL ids parse (execution stays refused); the page index maps every page-set
  entry to its record and a left-out page to nothing. End to end through grouping: two images with
  the same P in different IEs both survive, two sys_ids of one Part with the same P both survive,
  and the same page from two transcription versions (V0.7 and V0.8) still collapses into one.
  Mutations: "next record present" adjacency; IE dropped from the key; P-only de-duplication
  restored; the transcription source added to the key.
- **G2 outcomes and exhaustiveness**: every cell of the §3 table; report-to-outcome conversion for
  each cut signal; a stop inside verification (core checkpoint returning False) is reported as
  stopped even though hits came back; a condition match whose text lookup fails, or that the
  engine would demote as a duplicate photograph, still removes the page. Exhaustiveness on a fake
  index built like Codex's round-2 counterexample (an 80-letter text, a target differing at eight
  evenly spaced letters, 100 copies of the target and one exact decoy): the budgeted search finds
  no target, the masked search finds it. Mutations: a cut negative passes; the mask applied after
  admission (budget kept); conditions read from rendered rows; the stop flag dropped.
- **G3 cap after judging**: more than 200 distinct sys_ids, with the leaders rejected by a "must
  not"; a qualifying manuscript ranked 250th survives, in Main and in Filtered; every removed page
  is still listed. Mutations: cap before `judge`; the cap applied to Removed.
- **G4 call sites**: worker → adapter → searcher → fake index records the mask, the checkpoint and
  `render_cap=0`, through `search_condition` itself; a Stop click on a running compound search
  stops it (the UI guard, not just the checkpoint); one failing step leaves the run going; a
  generation change aborts it, including when the step itself failed. The swap race is injected
  exactly: the pre-step generation check passes, the index swaps to a build whose record order
  differs, then the lease is taken; the condition must run against keys resolved in the new build
  (or refuse), never against the old order. Mutations: the adapter drops the keys; the Stop guard
  keeps recognising only the witness worker; the mask built before the lease; the post-step check
  skipped on a failed step.
- **G5 fail closed**: conditions with chunk, with Lab, and with a letter-level search demoted to
  chunk at dispatch never reach a non-passage worker; an under-floor or over-10,000-letter version
  never reaches the engine; five conditions are refused; more than 25 searches are refused.
  Mutations: the condition check moved before the dispatch-time demotion (a demoted run then shows
  chunk results under unexecuted conditions); the four-condition limit removed.
- **G6 persistence**: history and session round-trip; a snapshot with a missing, malformed or
  mismatched fingerprint is not displayed; a snapshot with a matching fingerprint but a row missing
  its `compound` record is not displayed; an old condition-free snapshot restores unchanged; the
  deferred startup restore and the legacy history branch apply the check; restoring an entry clears
  conditions it does not have; a restored compound snapshot exports its stored outcomes.
  Mutations: the restore check accepts a matching fingerprint without looking at each row's
  `compound` record; the legacy history branch skips the check; history identity ignores conditions.
- **G7 GUI** (`scripts/run_gui_tests.py`): the strip, the dialog, the two groups, flat-mode markers,
  the viewer's per-image highlight and evidence strip over a navigation sequence that crosses
  manuscripts and over two pages with the same P in different IEs, compact controls, Hebrew labels
  via `tr()`. Mutation: viewer evidence keyed by `(sys_id, P)`.
- **G8 real index** (`scripts/verify_compound_conditions.py`; FAILS, never skips, without the
  index), cases built from real records: (a) adjacent: A cut from image P and B from P+1 → found;
  a decoy B whose absence from P±1 is established independently by reading those records' streams
  → not found; (b) same manuscript: A and B from distant images → found; a manuscript with A only
  (B's absence checked in its streams) → not; (c) must not: the user's Talmud / Halakhot Gedolot
  case; (d) the siddur case once the user supplies his texts; (e) exhaustiveness on real data: a
  negative text with substitutions whose page the normal budgeted search misses and the masked
  search finds. Records timings per case, including the longest allowed version (10,000 letters).
  Proven able to fail by running it once against a mutated build (adjacency off by one).
- **G9 bypass paths**: one test per item of §4's list (cache, witness removal, auto-expand, session,
  history, flat mode, viewer, each export walker, empty Main, the exclusion merge with one
  manuscript in Main and Filtered, the dispatched text in exports). Mutations, one per item: cache
  reuse re-enabled; the witness-removal republish restored; `_advance_auto_expand` unguarded; the
  Main-only "No Results" exit and export guard restored (an all-Removed run disappears); the
  sys_id-only exclusion merge restored; the export reads the live text; flat mode's old two-marker
  rule restored.
- Plus `python scripts/run_local_tests.py`, `ruff check .`, `PYTHONUTF8=1 python scripts/check_docs.py`.

## 7. Order of work on the branch

1. The grouping identity fix (own commit, tracker row, test).
2. `shared/compound_query.py`, the page index, `record_mask`, `search_condition`, G1-G3.
3. Adapter, worker, dispatch validation, Stop, run token, G4-G5.
4. Strip, dialog, tree groups, `comp_view_filter` categories, flat mode, status line.
5. Viewer evidence strip and per-page evidence; exports.
6. History, session, restore checks, the bypass paths; G6, G7, G9.
7. G8 on the real index, help text, What's New draft, CHANGELOG, CODE_INDEX, tracker.

Codex reviews the plan until it approves, then the branch in rounds with a budget, as for PR #395.

## 8. Before the build starts

Ask the user (owner's call): his siddur example texts (G8 d) and one Talmud passage with a
Halakhot Gedolot or Rif passage he knows (G8 c).

## 9. Decisions

Owner, 2026-10-14:
- One desktop release with all three (D1, D2, D3-adjacent), on one base.
- A plan in `docs/plans/` and one branch, not a GSD milestone.
- Conditions sit under the main text, each with its own requirement and place.
- Pages removed by a "must not" are shown in a collapsed group with the reason; pages that could not
  be fully checked get their own group and are never counted as clean.
- A condition accepts several versions, any one of which counts.
- "Not found" means not found in the searchable pages; the count of pages that could not be
  searched is shown, rather than marking such manuscripts "not fully checked".

Made in this plan (open to review):
- Letter-level only; any other method refuses to run while conditions exist.
- "Adjacent" is literal P±1 in the same sys_id and IE.
- The unit "manuscript" is the sys_id; Parts are not merged.
- Conditions are searched restricted to the main text's units and judged on verified hits.
- Conditions are searched exhaustively over their page set (every gram admitted, postings masked),
  with the deepest profile's candidate and verify caps.
- Filter Text applies to the main text only.
- No cache reuse, no witness-removal republish and no auto-expand while conditions exist.
- At most 4 conditions, 10 versions each, 25 searches per run, 10,000 letters per version.

## 10. Not in this release (own tracker rows)

- Strict "two sides of one leaf" from established leaf/side data, with certainty levels.
- A rarer-text-first strategy, if G8 shows the main text's caps losing known manuscripts.
- Website and API use of `shared/compound_query.py` (the API keeps rejecting unknown fields,
  `web/search_api.py:1145`).
- Parts as a unit; "this text only" (D4).

## 11. Review history

- **Round 1 (Codex, 2026-10-14): DO NOT SHIP.** Folded in: the posting budget as a loss signal
  (Codex's probe: every posting excluded, both flags false) → the coverage window and one deeper
  retry; condition matches lost to failed text lookups, duplicate-photography demotion and caps →
  judge on verified hits; P-only de-duplication in grouping → identity fix; `render_cap` and
  `checkpoint` wiring through the adapter (`InterruptedError`, not a false return); validation after
  the dispatch-time demotion to chunk; nine bypass paths (session provenance, history identity and
  legacy branch, auto-expand, cache without reports, witness-removal republish, flat mode, viewer
  navigation, export walkers, stale delivery); left-out pages (owner ruling); a bounded search
  count; gates strengthened (G3 needs 200+ manuscripts, G8 negatives established independently and
  proven by a code mutation, G9 added); facts corrected (timing source, gap explanation, side-data
  sources, cap figures per depth, Filtered also capped).
- **Round 2 (Codex, 2026-10-14): DO NOT SHIP.** It confirmed most round-1 items closed and the
  identity key correct (its probe: 2 pages kept across IEs and Parts, V0.7/V0.8 copies still 1),
  but broke the coverage-window rule with a counterexample: the admitted rare grams can all lie in
  a decoy while a target with 10% substitutions shares only excluded grams, so no window looks
  thin and the restricted search finds nothing. Measured that budget losses are routine (median
  300-letter query 970K postings against 500K) and that admitting every gram over a masked page
  set costs 0.03-0.63 s; replaced the window rule and the deeper retry with an exhaustive masked
  condition search. Also folded in: the Stop guard, the core checkpoint's silent stop, bypassing
  `_run_one_witness`, empty-Main exits, the exclusion merge dropping a bucket's pages, exports
  reading the live text, uncapped Removed/unresolved groups, and the stronger gates it listed.
- **Round 3 (Codex, 2026-10-14): SHIP WITH FIXES, no HIGH.** The masked search found its round-2
  target (budgeted: 40 grams admitted, 0 candidates; all grams: 76 admitted, accepted at density
  0.10); the installed index has 702,466 unique `(sys_id, IE, P)` keys. Folded in: the mask is built
  inside the lease from keys and checked against the leased build (its probe applied a generation-A
  mask to a reordered generation B between the pre-check and the lease), the post-step check on
  failed steps, named mutations for G5, G6 and G9, and what "exhaustive" does and does not promise.
- **Round 4 (Codex, 2026-10-14): SHIP.** Both round-3 MEDIUM items closed; no new contradiction.
