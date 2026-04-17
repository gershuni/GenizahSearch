# 75-03 Summary — Back-navigation state loss fix

## Self-check

**COMPLETE.** All 3 tasks signed off. User confirmed surface 1 item (d) green
after the Gemini-guided Option B follow-up.

## Confirmed root cause

Two cooperating bugs produced the symptom, both documented in
`75-03-root-cause.md`:

1. **Bootstrap gate** — `resolve_search_bootstrap` treated any non-empty
   `initial_query` as "has route context" and set `restore_saved_results=False`
   (from commit `829cd7cf`, 2026-03-27 — NOT Phase 74). This killed snapshot
   hydration whenever the browser restored a `/search?q=...` URL on Back.
2. **`elif` cascade at search.py:4561** — even when the bootstrap correctly
   restored `search_state.results`, the auto-execute branch
   `elif initial_query:` fired a fresh `execute_search()` 0.5 s later,
   clobbering the restored snapshot. The `elif search_state.results:` render
   branch was unreachable on back-nav because it sat AFTER
   `elif initial_query:` in the cascade.

Both bugs had to be fixed for surface 1 item (d) to go green. The first commit
(`8f9c5ef3`) addressed bug (1) but left bug (2) in place — surface 1 item (d)
still failed. Gemini code review identified bug (2); commit `f40b8eab`
reordered the cascade.

## Files modified

- `web/search_bootstrap.py` — new `saved_results_count` param; `is_back_navigation` guard that overrides `has_route_context` when URL query matches saved_query AND snapshot has results; new elif branch resolves saved_mode on back-nav; `restore_saved_filters` stays False on back-nav (829cd7cf intent preserved).
- `web/pages/search.py`:
  - ~line 106: call site passes `saved_results_count=_saved_results_count`.
  - ~line 4190: Edit 2b — write `app.storage.user['search_query'] = clean_query` at search-execute time, closing the storage-write hole where Enter-to-search (no blur) left `saved_query` stale.
  - ~line 4554-4567: reordered `elif` cascade so `elif search_state.results:` (render restored snapshot) precedes `elif initial_query:` (fresh auto-execute). Follow-up Option B fix from Gemini review.
- `tests/test_search_bootstrap.py` — 5 existing tests updated to pass `saved_results_count=0`; 4 new regression tests added: `test_back_navigation_from_browse_restores_saved_results`, `test_fresh_query_route_with_different_saved_query_still_uses_clean_state`, `test_query_route_with_matching_saved_but_empty_snapshot_does_not_falsely_restore`, `test_back_navigation_restores_saved_mode_when_saved_mode_is_title`.
- `.planning/phases/75-non-regression-verification/75-UAT.md` — status flipped failed → in-progress; test 1 result flipped failed → passed; gap entry marked CLOSED (retained as history); test 5 expected baseline 1067 → 1071.

## Files created

- `.planning/phases/75-non-regression-verification/75-03-root-cause.md` — validates hypothesis (a) from 75-02-SUMMARY.md, cites commit `829cd7cf` as true origin, records STORAGE_WRITE_HOLE_CONFIRMED, quotes verbatim source lines as proof-of-read.

## Storage-write hole disposition

**CONFIRMED and CLOSED** by Edit 2b. Per `75-03-root-cause.md`:
`app.storage.user['search_query']` was previously written only on input blur
(search.py:401) and New Search reset (search.py:2016). Enter-to-search flows
did not update it, so the back-nav guard compared the URL's new `q=` against a
stale `saved_query`. Edit 2b writes `search_query` at search-execute time,
immediately after `persist_search_snapshot`, so `saved_query` authoritatively
mirrors the URL stamped by `history.replaceState`.

## Scoped pytest result

`tests/test_search_bootstrap.py`: **9 passed** (5 existing updated + 4 new). Run
twice — once after Task 2's initial commit, once after Gemini's follow-up edit.

## Full pytest baseline

**DEFERRED to 75-02 test 5 per D-18** (pytest runs LAST, only after all manual
surfaces pass). Expected delta: `1067 passed, 8 skipped` → `1071 passed, 8
skipped` (+4 from 75-03 regression coverage). 75-UAT.md test 5 `expected:` line
updated accordingly. The baseline capture to `75-pytest-baseline.txt` is the
75-02 walkthrough resumption's job, not 75-03's.

## Commits

- `4a04aab3` docs(75-03): root-cause file
- `8f9c5ef3` fix(75-03): bootstrap `is_back_navigation` branch + storage-write hole close + 9 tests
- `f40b8eab` fix(75-03): reorder search.py elif cascade (Gemini Option B) + UAT flip

## Next step

Resume plan 75-02 walkthrough from **surface 2 (Web Browse Responsiveness)**
per D-18 ordering: surfaces 2 → 3 → 4, then pytest test 5 LAST. On completion,
75-02 captures `75-pytest-baseline.txt` showing `1071 passed, 8 skipped`.

## Traceability

- **Requirement:** NREG-01 (back-navigation regression surface 1)
- **True regression origin:** commit `829cd7cf` ("fix: don't restore stale session filters on URL-driven search navigation", 2026-03-27). Phase 74's page-scoped refactor did NOT introduce this; the gap's Phase-74 attribution in 75-UAT.md Gaps is documented as misattribution. Recorded for future milestone retrospective.
- **External review:** Gemini code review of the initial 8f9c5ef3 commit identified the `elif` cascade as the remaining bug; Option B applied in f40b8eab.
