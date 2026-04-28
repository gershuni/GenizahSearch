---
status: passed
phase: 77-serializer-json-export
source: [77-01-SUMMARY.md, 77-02-SUMMARY.md, 77-03-SUMMARY.md, 77-04-SUMMARY.md, 77-05-SUMMARY.md, 77-06-SUMMARY.md]
started: 2026-04-28
updated: 2026-04-28
---

## Current Test
<!-- OVERWRITE each test - shows where we are -->

[testing complete — Tests 8 + 9 fixed by Plan 06 gap-closure (commits 8a95cf9d..7f93b7eb); manual smoke 2026-04-28 confirmed both gaps resolved on /search JSON + xlsx; Test 6 covered programmatically by `test_reset_clears_global_state_then_export_returns_400` in `tests/test_export_state_selection.py`; Test 7 baseline now 1213 passed / 8 skipped (was 1201 → +12 regression tests from Plan 06)]

## Tests

### 1. Search JSON button visible on /search toolbar
expected: After running any search on /search, the toolbar above results includes a JSON download button (data_object icon) alongside Excel and Word. Tooltip reads "Export JSON" (English) or "יצוא ל-JSON" (Hebrew). Button is always enabled — same pattern as Excel/Word neighbors.
result: pass

### 2. Parallels JSON button lifecycle on /parallels
expected: On /parallels, the JSON download button starts DISABLED (no results yet). After running a parallels search and results render, button becomes ENABLED. After clicking Reset (or while result panes are empty), button returns to DISABLED. Mirrors the Excel/Word lifecycle on the same page.
result: pass

### 3. /search JSON download — happy path
expected: |
  Run a Hebrew query (e.g., "שלום"), click the JSON button. Browser downloads a file named `genizah-search-{timestamp}.json`. File is valid JSON with envelope keys: schema_version=1, source="search", query (echoes typed query), mode, gap, filters (10-key dict), count, total, warnings=[] for clean run, generated_at, results array. Per-item shape: locator={sys_id, volume_ie, p_num}, score rounded to 4 decimals (NOT 0.0), no full_text field, domains as array, snippet/excerpt/match_terms populated, image_url server-relative for NLI providers OR null for Oxford-only items.
result: pass

### 4. /parallels JSON download — happy path
expected: |
  Run a parallels search with multi-sentence Hebrew source, click the JSON button. Downloads `genizah-parallels-{timestamp}.json`. Body has source="parallels", separate results[] and filtered[] arrays, one entry per manuscript (grouped by sys_id), each entry has matches[] array sorted by chunk_index ascending, aggregate_score is SUM across uids in the same sys_id, no duplicate (chunk_index, manuscript_snippet) pairs within matches[]. Hebrew text is native UTF-8 (no \uXXXX escaping).
result: pass

### 5. Excel/Word filename now contains query (latent state.current_search_query bug fix)
expected: |
  Pre-Phase-77 regression: Excel/Word downloads were defaulting to filename `genizah.xlsx` because state.current_search_query was declared but never assigned. Now: run a search with a query like "תורה", click Excel or Word export — downloaded filename contains the search query (or a meaningful slug derived from it), not the bare default.
result: pass

### 6. Empty-state guards
expected: |
  GET /api/export/json before any search has run → 400 with body "No results to export". GET /api/export/parallels/json before any parallels search has run → 400 with body "No parallels results to export". (Or, if accessed via UI buttons: search-page button is always-enabled but returns 400 cleanly when nothing's been searched; parallels button stays disabled so the path isn't reachable from UI in the empty case.)
result: pass
notes: "Originally skipped because Test 8 state pollution blocked the empty-state path. Plan 06 closed Gap #1, and the empty-state behavior is now covered programmatically by `tests/test_export_state_selection.py::test_reset_clears_global_state_then_export_returns_400` (Plan 06 commit 55543316) — exercises the exact 7-assignment block `_reset_search` runs and asserts the follow-up `/api/export/json` GET returns 400 with body 'No results to export'."

### 7. pytest baseline
expected: `python -m pytest tests/` returns 1201 passed, 8 skipped (started Phase 77 at 1162 + 39 new tests across the 5 plans). Zero failures, zero new skips.
result: pass
notes: "Final baseline: 1213 passed / 8 skipped (was 1162 at phase start → +51 new tests across the 6 plans, +12 from Plan 06 gap-closure). Confirmed 2026-04-28 by gsd-executor and independent spot-check."

### 8. New Search button clears export state on /search
expected: After clicking "New Search" on /search, subsequent Excel/Word/JSON exports return the cleared/empty state (or 400), NOT the previous search's results. Envelope-echo state (state.last_results, state.current_search_query, state.last_filters_applied, state.last_search_warnings) must be cleared at the New-Search reset path — Plan 01 covered 5 execute-time sites but the New-Search reset was not one of them.
result: pass
fixed_by: "Plan 06 (commit 4944880c) — `_reset_search` now mirrors 7 fields (6 envelope-echo + last_selected_uids) into the global state singleton at the end of the function, mirroring the precedent at parallels.py:1959-1962."
verified: "Manual smoke 2026-04-28 (user confirmed); regression coverage in `tests/test_export_state_selection.py::test_reset_clears_global_state_then_export_returns_400`."

### 9. Export honors row checkbox selection (pre-existing P2 from Phase 75)
expected: When one or more result rows are checked on /search, Export (Excel/Word/JSON) emits ONLY the checked rows, not the entire current result set. Currently exports emit the full list regardless of checkbox state.
result: pass
fixed_by: "Plan 06 (commits 8a95cf9d → d5f603b5) — new `AppState.last_selected_uids` field + `compute_selected_uids` helper mirrored from `toggle_select_all`, `toggle_card_selection`, and `_reset_search`. All 3 search-side handlers in `web/api.py` filter `state.last_results` by uid when `state.last_selected_uids` is truthy; filename gets `-selected-N` suffix when filtered."
verified: "Manual smoke 2026-04-28 (user verified JSON + xlsx with 1-of-2 selection — count flipped from 2 to 1 with only the JTS Ms. 2922 row in the filtered file); 8 regression tests in `tests/test_export_state_selection.py` cover all 3 formats × 3 selection scenarios + filename-suffix invariants."
notes: "Pre-existing OPEN_ISSUES.md L81 issue (Surfaced 2026-04-17 during Phase 75 walkthrough, confirmed pre-existing on live website, NOT a v7.9 decomposition regression). OPEN_ISSUES.md line 81 flipped to ✅ Fixed (2026-04-28) in commit ff620251."

## Summary

total: 9
passed: 9
issues: 0
pending: 0
skipped: 0

## Gaps

- truth: "After New Search on /search, subsequent exports return cleared state, not previous search's data"
  status: resolved
  resolved_by: "Plan 06 commit 4944880c (2026-04-28); manual smoke verified by user 2026-04-28"
  reason: "User reported: I clicked 'new search' and the exports still export the old search"
  severity: major
  test: 8
  root_cause: |
    `_reset_search` at web/pages/search.py:1976-2032 clears the page-scoped `search_state` (results, displayed_results, selected_indices, etc.) and `app.storage.user`, but does NOT touch the global `state` singleton fields that the FastAPI export handlers in web/api.py read from. So `state.last_results`, `state.current_search_query`, `state.current_search_mode`, `state.current_search_gap`, `state.last_filters_applied`, `state.last_search_warnings` all retain values from the previous search after New Search is clicked. Phase 77 Plan 01 populated these fields at 5 execute-time sites but the New-Search reset path was not on that list.
  artifacts:
    - path: "web/pages/search.py"
      line: "1976-2032"
      issue: "_reset_search() function clears search_state and app.storage.user but not the global state singleton's envelope-echo fields"
  missing:
    - "Add 6 state-clearing assignments at the end of _reset_search() (just before the ui.notify call at line 2032), mirroring the pattern at parallels.py:1959-1960 where `_reset_parallels` does `state.parallels_results = []` and `state.parallels_filtered = []`"
    - "Specifically: state.last_results = [], state.current_search_query = '', state.current_search_mode = 'exact', state.current_search_gap = None, state.last_filters_applied = None, state.last_search_warnings = []"
    - "Add a regression test in tests/ that exercises the reset path: run a search → assert state.last_results populated → call _reset_search → assert state.last_results == [] and the 5 envelope-echo fields are at their default values"

- truth: "Export honors row checkbox selection — checked rows only, not full list"
  status: resolved
  resolved_by: "Plan 06 commits 8a95cf9d → d5f603b5 (2026-04-28); manual smoke verified by user 2026-04-28 (JSON + xlsx); OPEN_ISSUES.md L81 flipped to ✅ Fixed"
  reason: "User reported pre-existing OPEN_ISSUES.md line 81 bug — checked results are not the only ones being exported, all results always"
  severity: major
  test: 9
  root_cause: |
    Selection state lives at `search_state.selected_indices: set[int]` (page-scoped, defined at web/pages/search_state.py:38). It's updated in two sites: `toggle_select_all` at search.py:2036-2049 (bulk select all) and the per-row checkbox handler at search_results.py:362-366 (individual row clicks). The set is read for "Add to List" / "Save Excerpts" bulk operations at search.py:2076 and 2216 via `[search_state.results[i] for i in sorted(search_state.selected_indices)]`. But the three export handlers in web/api.py (export_excel at line 1816-1837, export_word at 1839-1860, export_json at 1920-1955) all read from the global `state.last_results` singleton with no awareness of search_state.selected_indices — so checkbox selection is ignored and the full list is always exported.
  artifacts:
    - path: "web/state.py"
      issue: "AppState has no field for selection state — global singleton can't see search_state.selected_indices"
    - path: "web/pages/search.py"
      line: "1976-2032 (_reset_search), 2036-2049 (toggle_select_all)"
      issue: "Reset and bulk-toggle update search_state but don't mirror to global state"
    - path: "web/pages/search_results.py"
      line: "362-366"
      issue: "Per-row checkbox handler updates search_state.selected_indices but doesn't mirror to global state"
    - path: "web/api.py"
      line: "1819, 1825 (excel); 1842, 1848 (word); 1933, 1938 (json)"
      issue: "All three export handlers serialize state.last_results unfiltered — no read of any selection-state field"
  missing:
    - "Add `last_selected_uids: Optional[List[str]] = None` field to AppState in web/state.py (None means 'no selection — export all'; non-empty list means 'export only these uids')"
    - "Mirror selection changes to global state at all 3 toggle sites: toggle_select_all (search.py:2036), per-row checkbox handler (search_results.py:362-366), and _reset_search (search.py:1976) which must set state.last_selected_uids = None"
    - "Helper to compute uids from indices: `[r.get('uid', '') for i, r in enumerate(search_state.results) if i in search_state.selected_indices]` — set state.last_selected_uids to None when selected_indices is empty (so handlers default to full export), otherwise to the uid list"
    - "In all 3 export handlers (web/api.py:1816, 1839, 1920), filter state.last_results by uid before passing to serializer/writer when state.last_selected_uids is truthy: `results = [r for r in state.last_results if r.get('uid') in selected] if state.last_selected_uids else state.last_results`"
    - "Update build_search_filename / Excel filename builder to optionally suffix `-selected-N` when filtering by selection so filenames disambiguate full vs partial exports"
    - "Add regression tests for all 3 formats: (a) no selection → full export, (b) selection of 2 of 5 rows → only 2 rows in output, (c) selection cleared via _reset_search → next export emits 400 (covered by test 8 fix)"
    - "Update docs/OPEN_ISSUES.md line 81 entry: change ❌ Open → ✅ Fixed (2026-04-28) with commit refs"
  open_issues_ref: "docs/OPEN_ISSUES.md line 81 (P2, ❌ Open since 2026-04-17)"
