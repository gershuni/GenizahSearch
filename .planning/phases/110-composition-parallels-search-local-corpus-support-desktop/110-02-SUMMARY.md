---
phase: 110-composition-parallels-search-local-corpus-support-desktop
plan: 02
subsystem: search
tags: [composition, parallels, lab-mode, local-corpus, corpus_scope, tantivy, desktop, pyqt6]

# Dependency graph
requires:
  - phase: 110-01
    provides: "Wave-0 pure-engine test scaffold (tests/test_comp_corpus_scope.py) pinning the corpus_scope contract"
provides:
  - "corpus_scope param (last) on SearchEngine.search_composition_logic + LabEngine.lab_composition_search"
  - "fail-closed scope normalizer (unknown -> 'genizah') on both composition engine paths (C4)"
  - "Genizah/LOCAL-LAB query-loop gating: != 'local' (Genizah) and != 'genizah' (LOCAL LAB) on both paths"
  - "per-run staleness verdict (local_lab_stale) + scope echo (corpus_scope) on EVERY return dict incl. both early returns (A2 + Round-2 #4)"
  - "stale != no-index distinction (M2)"
  - "SearchEngine._current_lab_weights_hash override read path (_lab_weights_hash_override) — fixes the live silent-drop of LOCAL LAB hits in 'all' scope (RF-4)"
  - "corpus_scope plumbed through CompositionThread + LabCompositionThread (gui_threads.py), default 'genizah'"
affects: [110-03, 110-04, composition-ui, parallels-ui, desktop-comp-tab, export_comp_report]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "corpus_scope selector as the LAST keyword param on composition engines (orthogonal to mode; Lab Mode NOT hardwired to LOCAL)"
    - "fail-closed scope normalization at method top (any unknown value coerced to 'genizah')"
    - "per-run verdict in the result payload (not only a mutable engine flag) so the UI reads a fresh value each run"
    - "weights-hash override injection point so SearchEngine compares against the LabEngine's build-time hash"

key-files:
  created: []
  modified:
    - genizah_core.py
    - gui_threads.py

key-decisions:
  - "corpus_scope appended as the LAST param of lab_composition_search (after min_delimiter_distance, NOT after scan_limit) to avoid positional shift of boundary_* args (C3)"
  - "Genizah-loop gating done with a wrapping `if corpus_scope != 'local':` at the for-statement level (no body re-indent) — valid Python, minimal diff, M1-safe (accumulators init above the branch)"
  - "Composition merge for 'all' uses the existing score-interleaved doc_hits/results_map accumulator — NO RRF (RRF stays in execute_search only, per RESEARCH RF-2)"
  - "Composition threads default corpus_scope='genizah' (NOT SearchThread's 'all') for D-13 non-regression"

patterns-established:
  - "Pattern: scope-gated index loops with a single freshness compute + per-run stale verdict feeding both the back-compat engine flag and the result dict"
  - "Pattern: every return path (including early returns) carries the same contract keys so downstream UI never defaults a stale Local/ALL run to Genizah/False"

requirements-completed: [COMP-LOC-01, COMP-LOC-02]

# Metrics
duration: ~20min
completed: 2026-06-08
---

# Phase 110 Plan 02: Composition Engine corpus_scope Summary

**`corpus_scope` selector (Genizah / Local / ALL) added as the last param of both composition engines + both composition threads, with fail-closed normalization, per-run staleness verdict on every return path, and the live weights-hash silent-drop fix — all 10 Wave-0 pure-engine tests green.**

## Performance

- **Duration:** ~20 min
- **Tasks:** 2
- **Files modified:** 2 (genizah_core.py, gui_threads.py)

## Accomplishments
- `search_composition_logic` and `lab_composition_search` now accept `corpus_scope` (appended LAST), gate the Genizah loop on `!= 'local'` and the LOCAL LAB loop/hook on `!= 'genizah'`, and fail CLOSED on any unknown scope (C4).
- Per-run staleness verdict (`local_lab_stale`) + scope echo (`corpus_scope`) carried on EVERY return dict — including the LAB empty-text early return and the standard too-short-token early return (A2 + Round-2 #4) — with the stale-vs-no-index distinction (M2).
- Fixed the live silent-drop bug: `SearchEngine._current_lab_weights_hash` now honors an injected `_lab_weights_hash_override`, so `all`-scope standard composition actually surfaces LOCAL LAB hits (RF-4 / Pitfall 6).
- `corpus_scope` plumbed through `CompositionThread` + `LabCompositionThread` (default `'genizah'`), forwarded to the engine call in each `run()`.

## Task Commits

Each task was committed atomically:

1. **Task 1: corpus_scope gating + fail-closed + per-run stale + weights-hash fix (genizah_core.py)** - `80583a60` (feat)
2. **Task 2: thread corpus_scope through both composition threads (gui_threads.py)** - `f44aa0ee` (feat)

_Task 1 is a `tdd="true"` task whose RED was already established by Plan 01's scaffold; it landed as a single GREEN commit since the failing tests pre-existed._

## Files Created/Modified
- `genizah_core.py` - corpus_scope param (last) on both composition engine methods; fail-closed normalizer (twice); Genizah/LOCAL-LAB loop gating; per-run stale verdict in all four return dicts (2 early + 2 main); `_lab_weights_hash_override` read path in `_current_lab_weights_hash`.
- `gui_threads.py` - `corpus_scope` param + `self.corpus_scope` store on `CompositionThread.__init__` and `LabCompositionThread.__init__`; `corpus_scope=self.corpus_scope` forwarded in both `run()` engine calls; defaults `'genizah'`.

## Decisions Made
- Wrapped the Genizah loops with an `if corpus_scope != 'local':` guard at the `for`-statement indent level rather than re-indenting the ~120-line loop bodies. Python only requires the body be more-indented than its header, so this is valid and keeps the diff minimal and review-friendly while preserving M1 (accumulators are initialized above the branch, so a LOCAL-only run never NameErrors).
- Computed LAB freshness ONCE into a local (`_lab_fresh_lab` / `_lab_fresh`) inside the existing D-37 try/except, then derived the per-run `local_lab_stale` from it — instead of calling the freshness function twice.

## Deviations from Plan
None - plan executed exactly as written. All A2/M1/M2/C3/C4/RF-4/Round-2-#4 review deltas were already specified in the plan and implemented as directed.

## Issues Encountered
- The Task 2 acceptance criterion "`corpus_scope=self.corpus_scope` exactly twice" — grep returns 3 occurrences, but the third (gui_threads.py:112) is the pre-existing `SearchThread` forward, unrelated to composition. The two composition `run()` call sites (lines 215, 282) are exactly the two intended. Not a defect.

## Verification Results
- `pytest tests/test_comp_corpus_scope.py` — **10 passed** (whole file green, C2).
- `pytest tests/test_corpus_scope_routing.py tests/test_lab_composition_chunk_hits.py` — **18 passed** (D-13 historical-parity guard intact).
- `python -m ruff check genizah_core.py gui_threads.py` — All checks passed.
- `python -c "import gui_threads, genizah_core"` — exits 0.
- Greps: `'local_lab_stale'` quoted in dicts = 4 (2 early + 2 main returns); fail-closed normalizer = 2; gates `!= 'local'` = 2 and `!= 'genizah'` = 2; `_lab_weights_hash_override` present; `_rrf_merge` inside `search_composition_logic` body = 0.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- Engine + thread layer is corpus-scope aware and default-safe. Plan 03 wires the desktop composition-tab Genizah/Local/ALL selector to pass the real scope into the two `run_composition` call sites, and injects `searcher._lab_weights_hash_override = lab_engine._current_lab_weights_hash()` (at GenizahGUI init + after every LOCAL LAB rebuild) — the read path for that override is now in place.
- Plan 04 (EXP-F3 LOCAL-aware `export_comp_report`) can rely on the per-run `corpus_scope` / `local_lab_stale` payload keys now present on every composition result dict.

## Self-Check: PASSED
- FOUND: .planning/phases/110-composition-parallels-search-local-corpus-support-desktop/110-02-SUMMARY.md
- FOUND: 80583a60 (Task 1)
- FOUND: f44aa0ee (Task 2)

---

## Design correction (Plan 110-03 UAT checkpoint, 2026-06-08)

During human verification of the composition corpus selector (Plan 110-03),
standard (Lab-Mode-OFF) composition with scope=Local returned **nothing**. Root
cause: this plan routed *standard* LOCAL composition through the **LOCAL LAB
side-index** (`local_lab_searcher`), gated on `_check_local_lab_freshness()`. The
LAB index only exists once the user has built it via Lab Mode — so it was `None`,
the freshness gate failed, and the hook was silently skipped.

**Authoritative corrected intent** (110-CONTEXT.md "⚠ DESIGN CORRECTION
2026-06-08" block): the LAB side-index is **opt-in** ("Lab Mode") for both search
and composition; **by default both use the REGULAR index.** This correction was
applied as a follow-up DESIGN-CORRECTION pass:

- **Engine (genizah_core.py):** `search_composition_logic`'s standard LOCAL hook
  re-pointed from `self._local_lab_index` / `self.local_lab_searcher` (LAB) to the
  **regular My-Library index** `self.local_index` / `self.local_searcher` — the
  same index regular search scope=Local uses. The hook now parses
  `["content","content_head","content_tail"]` with the v7.16 Hebrew
  metacharacter-strip fallback (mirrored from `_query_local_index`) and drops the
  `_lab_fresh` / `_check_local_lab_freshness` dependency. The default path reports
  NO staleness (`local_lab_stale = False`); an empty LOCAL result is treated like
  an empty Genizah result. `lab_composition_search` (Lab Mode) is **unchanged** —
  it keeps the LAB side-index + its freshness/staleness.
  Commit `3dfd62a3` (feat).
- **Tests (tests/test_comp_corpus_scope.py):** standard-path routing assertions
  re-pointed to the regular `local_searcher`; added
  `test_std_comp_local_uses_regular_index`; `test_stale_lab_sets_flag` repurposed
  to the Lab path (staleness is now Lab-Mode-only). Commit `dafbb755` (test).
- **UI (genizah_app.py + desktop/my_library_tab.py):** the now-moot
  `_lab_weights_hash_override` machinery introduced for the default path was
  removed by Plan 110-03's correction pass — the staleness label
  (`lbl_comp_local_stale`), `_refresh_comp_stale_label_for_scope`,
  `_refresh_lab_weights_hash_override` (3 call sites), and the post-rebuild
  my_library_tab callback. The `_lab_weights_hash_override` read path in
  `SearchEngine._current_lab_weights_hash` is now dead but left in place (harmless).
  Commit `784368e1` (refactor).

The COMP-LOC-01/02 requirements remain satisfied; the corpus selector,
session/history persistence, and Lab decoupling are unchanged. Plan 110-03 remains
at its human-verify checkpoint pending re-verification under the corrected routing.

---
*Phase: 110-composition-parallels-search-local-corpus-support-desktop*
*Completed: 2026-06-08*
