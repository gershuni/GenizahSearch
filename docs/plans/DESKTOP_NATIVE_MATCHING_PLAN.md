# Desktop native matching: engine-generated patterns on stdlib `re`

**Status:** plan, reviewed (Codex gpt-6-astra, 6 rounds, final verdict SHIP WITH FIXES, fixes
folded in below). Not implemented. Owner decisions recorded in §7.
**Tracker row:** "Windows: Extended/Maximum patterns match about 60 times slower with the
`regex` module than with `re`" (P3, 2026-10-11).
**Date:** 2026-10-12.

## 1. Problem

The desktop app (in-process QThread search) and Windows research workers match every search
pattern with the third-party `regex` module; production Linux workers match with stdlib `re`.
On Extended/Maximum patterns `regex` is ~60x slower. Measured 2026-10-12 (Python 3.11.9,
regex 2026.9.10, real index) on the Maximum pattern for 4 Hebrew words (8,000 forms each,
prefix trees, 458,896 chars): 60 real pages, `re` 0.43 ms/page vs `regex` 26-31 ms/page; one
390K-char document 0.11 s vs 7.8 s; compile 2.4 s vs 4.5 s (both are paid today: ~7 s before a
Maximum search starts).

Cause: `regex`'s per-attempt cost on the generated shape (one anchored attempt of a word's
tree: `re` 0.7-0.9 us, `regex` 10-12 us; the same word written with the mark-tolerant inserter
between its letters costs 19x in `regex`). Ruled out by measurement: IGNORECASE, the `\w`
rewrite, `concurrent=True`/`timeout=`, VERSION1, ten alternative inserter shapes, `regex`'s
`\L<list>` named lists, flat alternation. Even with no inserter `regex` is 5x slower. The
pattern cannot be reshaped to make `regex` fast; the engine has to change.

Why `regex` in-process: stdlib `re` holds the GIL and cannot be interrupted, so a user-typed
catastrophic Regex would freeze the Qt app; `regex` releases the GIL and honours a per-call
timeout (`GENIZAH_REGEX_TIMEOUT_SECONDS`, default 10 s). Today, when ONE call hits that timeout
on the desktop, `SearchBudgetExceeded` is re-raised by every handler and `SearchThread.run`
turns it into `error_signal`: the whole search is discarded and an untranslated QMessageBox
appears. On this pattern/text that happens for any candidate document over ~450K chars
(chain completion, which reads every candidate, is the most exposed), and Stop often runs out
its 5 s `wait()` and calls `QThread.terminate()` mid-match.

## 2. What the review established (facts to keep)

- The desktop deadline is `math.inf` (`bounded_search` -> `search_budget()` -> default 0), so
  `_match_timeout()` returns 10.0 there, never None. Routing must test "deadline is None or
  infinite", never the existing `timeout is None`.
- The gap separator `(?:[^W]+W+){0,g}` is uncapped on the desktop box, the API and MCP (the
  website UI caps at 10) and `re` is exponential in it: (g+1)^(words-1). Gap 1000: >240 s on a
  2M-char document; `OverflowError` at >= 2^32-1.
- Largest index document: 5,734,455 chars (full scan of 236,056 system/part docs). Sizes in a
  40,000-doc sample: p99 11K chars; above 100K 0.17%, above 500K 0.03%, above 1M 0.003%.
- Generated patterns have latent super-linear cases that today hide behind the 10 s timeout:
  forms containing apostrophes/quotes (the inserter class) against runs of them; forms
  containing separator-class characters; case-folded duplicate branches under IGNORECASE
  (U+0399/U+03B9/U+1FBE fold into the inserter class); 500+ parts; near-equal-length form sets;
  and the inserter/separator classes SHARE characters (`"`, U+2018, U+2019, U+0300-036F; verified
  by an exhaustive Unicode probe under IGNORECASE) so a subject with one of them between letters
  can be parsed two ways at every part boundary (2^parts). Branch trial is not free either:
  8,000 siblings at one level cost 1.1-1.7 ns per try.
- LOCAL subjects are NOT bounded by the indexer: TXT files index as one document; PDF pages,
  DOCX chunks and CSV windows have no character cap (only HTML/XLSX apply 1M).
- Composition creates and mutates `doc_hits[uid]` BEFORE its second `.search()` on the original
  content (Genizah and LOCAL loops), so a cancel at that call would leave an empty zero-score row.
- `build_regex_pattern` is not the common entry: Fuzzy bypasses it via `_fuzzy_matcher`, and a
  ValueError raised inside the LOCAL-only branch is swallowed by its broad handler.
- Production: `web/main.py` always wraps engines in `IsolatedEngine`; the four research methods
  run in workers under `isolated_matching` (infinite deadline). The "finite deadline -> regex"
  branch is practically unreachable in production.
- The desktop viewer already runs `highlight_pattern` strings (including the user's own Regex)
  through plain `re` on the UI thread with no bound; its MULTILINE check `'^\\' in p` fires on
  the `[^\` inside every generated gap separator, so it recompiles the Maximum pattern on the UI
  thread (1.7 s + 2.0 s). Pre-existing; separate rows (§8).

## 3. Design

### 3.1 `compile_generated()` in `shared/search_regex.py`

- `compile_generated(pattern, flags=0, *, cost_per_start)`: `_check_deadline()`;
  `validated = re.compile(pattern, flags)`; str check; `_check_deadline()`. When
  `_isolated and not _native_matching` (Windows workers) it simply delegates to `compile()`
  (eager `regex` object, exactly today's). Otherwise it returns
  `Pattern(None, validated.pattern, validated.flags, stdlib=validated, generated=True,
  native_max_chars=int(HOLD_BUDGET_S / (cost_per_start * 1e-9)))`.
  No flag mapping, no `_preserve_word_classes`, no `regex` compile on this path.
- `Pattern.__init__(compiled, original, flags, stdlib=None, generated=False,
  native_max_chars=None)`: positional compatibility kept (tests build `Pattern(Mock, 'שלום', 0)`).
- `Pattern._call(name, *args, **kwargs)` dispatches by method NAME (today `search()` evaluates
  `self._compiled.search` before `_call` routes). `groups`/`groupindex` come from `_stdlib`.
- Generated, in-process dispatcher, in this order:
  1. `_check_deadline()` (an already-expired budget raises before any work);
  2. `_check_cancel()` (cooperative; see 3.4) -- before either fallback;
  3. `sub()` is never native: generated `sub` takes the regex route (its subject is the second
     argument; the acceleration sites only search/match);
  4. size: if the effective range (`endpos - pos`, or `len(subject) - pos`) exceeds
     `native_max_chars` -> regex route;
  5. shared-character scan, multi-part patterns only: if `SHARED_RE.search(subject, pos,
     endpos)` finds a character of the inserter/separator intersection (5.1-5.6 ms per
     million chars; single-part patterns are exempt), the cap becomes
     `native_max_chars_shared = native_max_chars >> (parts - 2)` and the range is tested
     against it -> over it, regex route. (Measured 2026-10-12 on degenerate subjects with a
     shared character at every boundary: a failing match costs x0.5 the cost model at 2 parts,
     x1 at 3, x1.7 at 4, x2.9 at 5, x4.9 at 6, x15-20 at 8, independent of the run length; the
     2**(parts-2) factor keeps >= 2x margin. The first version sent every such call to the
     regex route; on a real Extended two-word search 22% of calls did, each 60x slower, and
     the whole search was only 1.5x faster -- the fallback dominated.)
  6. else `self._stdlib.<name>(...)`.
  The regex route for a generated pattern uses a LAZILY compiled regex object (compiled on first
  use, `_check_deadline()` before and after, through one helper `_regex_compile(validated)`
  shared with `compile()` for the flag mapping and `_preserve_word_classes`), and obtains the
  remaining timeout AFTER the lazy compile. Generated patterns never use `regex` otherwise, even
  under a finite deadline (they may overshoot a budget by at most one call).
- `SHARED_RE` is DERIVED at import from both class predicates under the matching flags (the
  intersection of the inserter class and the separator class compiled with IGNORECASE), never a
  hand-written list. Verified set today: U+0022, U+0300-036F, U+2018-2019 (115 chars).
- Constants: `HOLD_BUDGET_S = 1.0`, `NS_LEVEL = 20`, `NS_SIB = 5`, stated in code and docs as an
  EMPIRICAL responsiveness target, not a guarantee across machines: measured 12-17 ns per level
  step and 1.1-1.7 ns per sibling try at full CPU clock, up to ~30 ns a level under sustained
  load (a degenerate run at its cap took 0.5-1.1 s in the timing tests).
- Linux supervised workers: unchanged -- stdlib for everything, no cap, no scan, no timeout.
- `matched_chars` keeps `compile()` (the prefilter reads `_compiled`; Regex mode only).
- Docstrings: `isolated_matching`, `_call`; `GENIZAH_REGEX_TIMEOUT_SECONDS` guards non-generated
  patterns and generated patterns on the two fallback routes.

### 3.2 Eligibility, decided by shape in `build_regex_pattern`

`compile_generated` is used for the non-Regex branch and for the Fuzzy typed-term site only, and
only when ALL of the following hold (class membership tested under the pattern's flags):
  (a) `max_gap == 0` and every per-pair gap is 0/None;
  (b) mode is not Regex; no Responsa component (wildcard `\S*`, within_document, line-break);
  (c) no form is empty;
  (d) no form contains an inserter-class character;
  (e) no form contains a separator-class character anywhere (every form is word characters);
  (f) every multi-form part is a prefix tree (`_variant_alternation` reports fallback: a form
      empty, > 64 chars, or newline-bearing); single-form parts always pass;
  (g) every form is <= `_PREFIX_TREE_MAX_FORM` (64) characters, single forms included;
  (h) every character of every form is caseless (`c == c.lower() == c.upper()`): Hebrew letters,
      digits and marks qualify, Latin does not -- so under IGNORECASE no two branches match the
      same text;
  (i) `native_max_chars >= 10_000`.
Consequences: Exact, Extended and Maximum at gap 0 with plain Hebrew words -> native; Basic's
multi-form flat alternation -> regex (today); composition chunks (2-20 words, gap 0) -> native
through the same rule; quoted or dotted forms, Latin forms, user Regex, Responsa, any gap > 0 ->
regex (today).

### 3.3 Cost model (from the built trees)

`_prefix_tree` reports `depth` and `worst_path` (max over root-to-leaf paths of the sum of the
children counts at each node on the path). `cost_per_start = sum over parts and trees of
(NS_LEVEL * depth + NS_SIB * worst_path)`; cap = `HOLD_BUDGET_S / cost_per_start`. Real Maximum
queries (live variant generator, 8,000 forms/word):

| query | parts | trees | cost/start | cap at 1 s |
|---|---:|---:|---:|---:|
| one word | 1 | 2 | 1.4 us | 716,845 chars |
| 4 words | 4 | 10 | 6.1 us | 164,473 |
| 9 words | 9 | 23 | 11.4 us | 87,604 |
| 11 words | 11 | 29 | 21.0 us | 47,517 |

All far above the p99 page (11K chars); the cap diverts only rare giant documents, which then
take exactly today's route. Codex's calibration probes at these caps (long apostrophe runs, long
separator runs, 512 parts, 8,000 siblings) completed in 0.19-0.62 s.

### 3.4 Cancellation

`_cancel_check` ContextVar (default None) holding a zero-arg predicate; `_check_cancel()` raises
`InterruptedError` when it returns True. Set by the desktop search threads through
`PausableSearchMixin` around the engine call, reset in `finally`. Cooperative only: it bounds the
time from a click to the next checkpoint by one native call; it does not shorten a call and does
not make `terminate()` safe. `except InterruptedError: raise` is added ahead of the per-hit
`except Exception` handler in `execute_search` and ahead of the LOCAL composition handler (the
Genizah composition loop already has it). `RefinementReplayThread` sets no predicate (unchanged).

### 3.5 Composition accumulation

In both composition loops, complete the stripped-content search and the original-content
search (reusing the first match object for the bracket-offset fallback, which removes the third
search) into locals FIRST; touch `doc_hits[uid]` only after verification completes.

### 3.6 Gap validation (same PR) -- capped for the plain modes, open for Responsa and Regex

Owner ruling 2026-10-12: the word-gap cap applies to the regular modes -- Exact/Literal,
Variants (Basic/Extended/Maximum) and Fuzzy -- which are the modes that can take the native
route. Responsa (`[N]` and `[|N]` tokens) and Regex mode keep whatever gap the scholar asks
for; they always stay on the interruptible `regex` route with its per-call timeout, which is
their guard, as today. (Regex mode ignores the gap box altogether: the user's pattern is the
pattern.)

- `MAX_WORD_GAP = 10` for the capped modes, validated at `execute_search` entry (before the
  LOCAL branch and the Fuzzy bypass) and defensively in `build_regex_pattern`/`_gap_separator`
  (`int(max_gap)`). Over the cap: `ValueError` with a clear, translated message, never a silent
  clamp (the worker already maps ValueError to a validation error).
- Responsa: no policy cap. Only the technical ceiling is enforced at both `[N]`/`[|N]` parse
  sites: `re` raises `OverflowError` at a repeat count >= 2^32-1, so the parser rejects such a
  token with a message instead of letting the OverflowError escape (the non-Responsa builder
  catches only `re.error`).
- Interfaces: the API keeps `gap: int = Field(default=0, ge=0)` and adds a model validator
  beside `_check_gap_metadata_coupling`: `gap > 10` is rejected (400) unless `search_mode` is
  responsa or regex; MCP mirrors it; `integrations/chatgpt/build_schema.py` documents the
  mode-dependent limit in the field description (no unconditional `maximum`). Desktop: the
  Search-press handler rejects > 10 with a message when the selected mode is a capped one (no
  reliance on `QIntValidator`, which only marks 11-99 Intermediate).
- Restore: stored values stay visible as stored; a chain replay or session re-run whose stored
  gap exceeds the limit IN A CAPPED MODE stops at that step with a visible explanation and shows
  no results for it; Responsa/Regex steps replay unchanged.

## 4. Gates (each with the mutation that makes it fail)

1. Behavioral, red today: `GENIZAH_REGEX_TIMEOUT_SECONDS=0.001`, stub var_mgr, gap-0 Hebrew
   two-word pattern, `.search` over ~240K chars of word 1 with word 2 absent: today raises
   `SearchBudgetExceeded`; after the change returns None. Rows: no budget; inside
   `search_budget()` (desktop). Windows-worker row: inside `isolated_matching(native=False)` the
   regex object is used with `timeout=None` and `_stdlib` is never called (selection, not timeout).
   Linux row: inside `isolated_matching(native=True)` a 10M-char subject still reaches `_stdlib`.
2. Routing with a Mock on `_stdlib` (never a catastrophic input -- a wrong mutation would hang):
   negative rows for Regex mode, Responsa within_document / wildcard / flex, gap=1,
   per_pair_gaps=[1], Basic multi-form, a 66-char form at Maximum, the ten-apostrophe term, the
   punctuation part, the iota form, `aa=Aa`, `ab=ab.ab`, an empty form; positive rows for
   exact/extended/maximum at gap 0, the Fuzzy typed term, a quoted-free composition chunk,
   geresh/gershayim text (NOT in the shared set). Expired deadline raises before any stdlib
   call; finite budget still stdlib; `sub` never stdlib; cancel on a shared-character subject.
3. Dispatcher: subject one char over the cap -> regex object; at the cap -> `_stdlib`; multi-part
   on a subject with `"`: within the shared cap (`cap >> (parts-2)`) -> `_stdlib`, over it ->
   regex; the same subject, single-part -> `_stdlib`; geresh/gershayim are not shared; the lazy
   compile is not triggered by small subjects (spy on `_regex.compile`), compiles once, and an
   expired deadline raises before it.
4. Cost model: unit tests on hand-built trees (depth, worst_path) and order-of-magnitude
   assertions on the four real queries' caps; Codex's CJK case (8,000 siblings, W=2) gets cap
   25,000 and a 250K subject takes the regex route.
5. Timed regressions in externally timed child processes only (a hanging regression must be
   killed, not measured): degenerate letter runs at W=225 on 1M chars; long apostrophe runs;
   long separator runs; 512 parts; 8,000 siblings; the 19-part `א"` subject; all at their caps.
6. Grammar (regression protection, not proof): real generated gap-0 patterns parsed with
   `re._parser`; no GROUPREF/ASSERT/ASSERT_NOT; the only unbounded repeats are the separator and
   inserter classes; class overlap computed under the matching flags.
7. Cancel: predicate flips True after the first call -> the second `.search` of a generated
   pattern raises InterruptedError; thread-level tests through SearchThread (cutoff
   `interrupted`), ChainCompletionThread (previous step kept), CompositionThread (Genizah and
   LOCAL, `partial=True`, no unfinished row when cancelled at the second search), ContextVar
   reset in `finally`.
8. AST: `compile_generated` referenced only in `shared/search_engine.py`.
9. Parity: `re` vs `regex` compared explicitly (the test compiles the regex route itself),
   spans and captures, on prefix-tree Extended/Maximum shapes; existing pins in
   `tests/test_search_regex.py` and `tests/test_search_budget_engine.py` kept.
10. Gap: entry validation rows (desktop context, LOCAL scope, Fuzzy mode) reject 11 and accept
    10; Responsa rows accept `[50]` and `[|50]` and reject only the technical ceiling at both
    parse sites (no OverflowError escapes); Regex mode ignores the gap; API rows: gap 50 is 400
    for `variants`/`exact`/`fuzzy` and 200 for `responsa`/`regex`, default preserved;
    end-to-end restore/replay test (stale results not shown as the rejected step's results; a
    Responsa step with gap 50 replays).
11. Update the two monkeypatch sites that replace `compile_search_regex` with a 2-arg callable
    (`tests/test_variant_change_limit.py`, `scripts/benchmark_search_compiler.py`).

## 5. Docs and tracker

`docs/search-matching-timeouts.md` states the opposite policy (in-process matching retains
`regex`); update. Comments assuming the in-process matcher is `regex`: `shared/regex_prefilter.py`
(prefilter union logic) and the `matched_chars` docstring. Tracker: the P3 row gets a note, not
a close (gap > 0, Basic multi-form, quoted/dotted subjects for multi-word queries, and LOCAL
composition's per-chunk checkpoint remain on today's route); new rows in §8.

## 6. Expected effect

Desktop Exact/Extended/Maximum (gap 0, plain Hebrew): per page 26-31 ms -> ~0.4 ms; a Maximum
search starts in ~2.4 s instead of ~7 s; ~35 MB less retained per pattern (composition compiles
one per chunk). The "one huge document aborts the whole search" failure disappears for native
calls. On the ~13% of pages that carry a shared character between two letters (14.5% contain
one at all; 40,000-doc sample, 2026-10-12) a multi-word query matches natively under the reduced
cap: a 2-word Maximum query keeps its full 2.2M-char cap, a 4-word one 41K (above the p99 page),
a 9-word one falls to today's route there. Real-index parity (Extended, "ברוך אתה", ids_only):
13,770 rows, identical uid sets on both routes. Residual: up to ~1 s of held GIL per native call
on degenerate text (empirical target), ~0.3 us/char on real text.

## 7. Decisions

- D1 gap 0 only (Codex and panel concur; a measured gap/word-count formula is not a dependable
  bound).
- D2 Basic's multi-form flat alternation stays on `regex` in this PR; moving Basic to the prefix
  tree is a separate decision with its own parity run.
- D3 (owner, 2026-10-12) word gap limit 10 for Exact/Literal, Variants and Fuzzy only,
  validated at entry, rejected visibly, API default preserved. Responsa `[N]`/`[|N]` and Regex
  mode are NOT capped by policy (only the `re` repeat-count ceiling is enforced); they stay on
  the interruptible route, which is their guard.
- Dropped: lazy compile as a general mechanism (kept only for the two fallback routes); the
  Windows Job Object (the existing job in `shared/research_limits.py` is owned by the worker
  itself; Windows workers keep today's route); reshaping the pattern for `regex` (measured:
  nothing helps); a subject-size cap as a substitute for eligibility; moving the desktop search
  into a subprocess (right architecture, out of scope).

## 8. Follow-ups (own tracker rows)

1. Pattern language: make the inserter and separator classes disjoint (which of `"`, U+2018,
   U+2019 and the combining marks count as separators is a scholarly decision) with a parity run
   on real pages; removes the 2^parts ambiguity for the remaining ~13% of pages.
2. Set-based verifier for Variants/composition, like `_FuzzyMatcher` (pure-Python letter-run walk
   with set lookup: 3.7 ms per 110K chars, no compile, no GIL hold) -- the durable fix for the
   remaining 2.4 s stdlib compile and the per-chunk composition compiles.
3. Viewer: the MULTILINE check fires on `[^\` in every generated pattern (two UI-thread compiles
   of the Maximum pattern, 1.7 s + 2.0 s, on first open and in Browse); `text_has_pattern_markers`
   adds a no-op DOTALL that defeats the `re` cache.
4. Web full-text highlighting compiles `f'({pattern})'` (a new string, both engines) inside a
   0.25 s budget on the event loop; fails for Maximum-size patterns on a cold cache.
5. The `\B` test-timeout row: mechanism is `regex`'s per-match GIL handoff (1.1M matches) under
   a competing CPU-busy Python thread (1.07 s alone, >10 s with the thread); the leaked thread is
   a hypothesis, not established; raising the timeout in the full-Unicode equivalence tests is a
   mitigation, not the fix.
6. The desktop viewer runs user Regex highlight patterns through plain `re` on the UI thread
   with no bound (pre-existing).
7. On the regex route, a per-document timeout could mark the search cut off ("N+") and continue
   instead of aborting the whole run.
8. Tracker row 286 (`test_only_supervised_worker` float flake) is already fixed (2026-09-23);
   archive it.

## 9. Review history

Panel 2026-10-12: 4 Opus refuters (pattern safety, call sites, Qt/UI, tests) + 1 independent
designer, read-only. Codex gpt-6-astra, xhigh, live source, read-only: R1 DO NOT SHIP (gap > 0
exponential; cancel cannot shorten `re` calls) -> R2 DO NOT SHIP (`{0,8}` inserter still 9^k;
punctuation-only terms; unfinished composition row) -> R3 DO NOT SHIP (IGNORECASE folds iota
into the inserter; LOCAL unbounded; long single forms) -> R4 DO NOT SHIP (case-duplicate
branches; separator inside a form; 511 parts; nine near-equal trees) -> R5 DO NOT SHIP
(inserter/separator shared characters -> 2^parts; sibling-try cost) -> R6 SHIP WITH FIXES
(shared set corrected to `"`, U+2018-2019, U+0300-036F; dispatcher ordering; `sub` stays on
`regex`; long-run cases in the timed regressions) -- all folded in above. No further explosive
case was found under the final rules.
