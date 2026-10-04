# Desktop search speed — handoff (2026-09-30)

**Status:** step 1 done on branch `ccr-b3b8c7ac-jyxlw3` (commit `35f856f`), not merged,
not yet re-measured on the real index. The tracker entry is the "Desktop search
time-to-first-results is ~30s" row in `docs/OPEN_ISSUES.md`.

## Why this exists

A user (Uri Aharon Ben-Simon, by email) compared us with Otzaria, which returns results
in milliseconds; the owner confirmed Otzaria is that fast. Otzaria is (to our understanding,
unverified) also Tantivy-based, so the gap is not the engine. It is what we do per hit.

## The measurement (owner, real index, שלום, literal mode, before step 1)

```
search_perf mode=literal scope=genizah candidates=50000 regex_kept=50000 final=21210
tantivy_ms=241 materialize_ms=27404 doc_load_ms=7481 candidate_match_ms=5148 total_ms=27711
```

Read it as:

| Finding | Evidence |
|---|---|
| The index is not the cost | `tantivy_ms=241` of 27,711 |
| Loading stored docs: 7.5 s | `doc_load_ms` |
| One regex pass per hit: 5.1 s | `candidate_match_ms` |
| ~14.8 s unattributed | inferred: two more regex passes per hit for the snippets (fixed in step 1) + 10,000 progress ticks (fixed) + meta lookups + bracket strip |
| **Results are silently capped** | `candidates=50000` == `Config.SEARCH_LIMIT` |
| The regex rejected nothing | `regex_kept=50000` |
| **57% of the work was duplicates** | `final=21210`: page vs system-scope docs and V0.7 vs V0.8 of one uid, removed by `_deduplicate` only AFTER full materialization; they also eat cap slots |

## Step 1 — done (`35f856f`)

`shared/search_engine.py::execute_search` materialize loop:
- snippets come from the existing `match_obj` span via `_highlight_pair` (was two full
  `regex.search` re-runs per hit through `highlight()`); the one case the old re-search
  dropped (brackets stripped, original text has no match) is still dropped via
  `orig_match_missing`;
- `progress_callback` every `_PROGRESS_TICK_EVERY = 200` hits (was 5).

Verified in the cloud session: output byte-identical before/after with
`scripts/search_output_snapshot.py`; synthetic 20k-page benchmark 2.00 s → 0.88 s literal,
4.73 s → 2.12 s variants (part of that is a simulated tick cost — not a real-machine number).
89 related test files pass per-file; 9 API test files fail identically on the old code
(the container's newer FastAPI, `add_event_handler`), so they say nothing about this change.
The full suite was NOT run.

## First thing to do locally

1. `git fetch origin ccr-b3b8c7ac-jyxlw3` and check it out.
2. Search שלום (literal) in the desktop app and read the `search_perf` line.
   Expected ~15 s (estimate). If it is not much better, the unattributed 14.8 s is not
   what we think: add timers for `get_display_data`, `_strip_brackets` and the snippet build
   before changing anything else.
3. `python scripts/run_local_tests.py` (never one `pytest tests/` process — see CLAUDE.md).

## Next steps, in order

### 2. Stop materializing duplicates (biggest remaining win; changes which rows can appear)
- First measure on the real index what the 50,000 candidates for שלום are, by
  `(scope, source)`: how many `scope='system'` docs, how many V0.7 vs V0.8 page docs, how
  many share a uid. Decide the approach from that, not before.
- Constraints to respect: `_deduplicate` keeps V0.8 over V0.7 on a uid collision and the
  FIRST V0.7 per uid; V0.8 rows come first in the output. A system-scope hit's uid is the
  boundary page the match falls in (`_map_span_to_pages`), known only after the regex,
  so system docs cannot be deduped by stored uid alone.
- Candidate approaches: restrict the Tantivy query by `source`/`scope` (e.g. V0.8 first,
  then V0.7 only for uids not already seen), or skip a V0.7 doc before `searcher.doc()` if a
  cheap key can be read (Tantivy fast fields would need a re-index — weigh that cost).
- Result-affecting: run it at `/effort xhigh`, and prove equivalence with the snapshot
  script PLUS a real-index comparison (same query, same result uids and order).
- Known quirk: which V0.7 duplicate "wins" depends on Tantivy's order for equal-score hits,
  which varies with segment layout. The snapshot fixture forces one segment for this reason.

### 3. Surface or lift the 50,000 cap
- Get the true hit count (tantivy-py `searcher.search(query, limit, count=True).count`)
  and at least tell the user when it exceeds `SEARCH_LIMIT`. Step 2 frees cap slots
  first, so do it after step 2. Whether to raise the cap is an owner call (memory).

### 4. Stream the first results
- `desktop/gui_threads.py::SearchThread` emits everything at the end. Emit batches (e.g. the
  first 200 verified hits) so time-to-first-result drops below a second, then keep filling.
  Keep pause/stop semantics (`tests/test_pause_core_ticks.py` guards the checkpoints) and
  the final dedup/ordering: streamed rows must not reorder or vanish when the run ends.

### Not worth doing
- "Skip the regex for exact searches": the one remaining pass is what locates the
  highlight span, so it cannot be skipped without losing snippets.

## Tools

- `scripts/search_output_snapshot.py OUT.json` — before/after equivalence on a fixture
  index. Set `$env:PYTHONHASHSEED = "0"` first (variant order comes from sets). Compare
  with `fc /b before.json after.json`. Add fixture rows for any case a change touches.
- `search_perf` INFO log line in `execute_search` — the one number to compare.
