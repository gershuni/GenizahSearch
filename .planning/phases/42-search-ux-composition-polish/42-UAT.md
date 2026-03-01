---
status: diagnosed
phase: 42-search-ux-composition-polish
source: [42-04-SUMMARY.md, 42-05-SUMMARY.md]
started: 2026-03-01T17:00:00Z
updated: 2026-03-01T17:30:00Z
---

## Current Test

[testing complete]

## Tests

### 1. Desktop composition persistent summary after completion (GAP-2)
expected: After composition search completes, progress bar stays visible with summary text showing duration, chunk count, and result count. Does not disappear when results display.
result: pass

### 2. Desktop regular search elapsed timer (GAP-3)
expected: During a regular (non-composition) search on desktop, the status bar shows an elapsed timer (MM:SS) ticking while the search runs. After completion, a persistent summary shows total duration.
result: issue
reported: "Should translate the term 'searching...' and the 'Showing 500 of X results'"
severity: cosmetic

### 3. Cancel responsiveness -- every chunk check (GAP-4)
expected: Cancelling a web composition/parallels search responds within ~5 seconds (previously ~30s). Partial results appear promptly after clicking cancel.
result: issue
reported: "works. Should fix also the slow response in regular search as well"
severity: minor

### 4. Desktop partial results persistent message (GAP-5)
expected: After cancelling a desktop composition search, the progress bar stays visible showing a "Partial results" message. The message persists while viewing results.
result: pass

### 5. Desktop excluded section grouped reason counts (GAP-7)
expected: Desktop composition excluded section header shows grouped reason counts instead of a plain count. Per-item reasons may also be present.
result: issue
reported: "The filtered should be grouped by the reason of filtering, thus not needed to repeat it in the items themselves"
severity: cosmetic

### 6. Desktop composition dedicated Printed column (GAP-9)
expected: Desktop composition results tree has a dedicated "Printed" column with red text, not a title prefix.
result: issue
reported: "pass. But the print column should be narrower (fit to the word print), resizable and filterable like I wanted to do in search result"
severity: minor

### 7. Web excluded section width fix (GAP-6)
expected: The excluded results section in web search fits within the container width. No horizontal overflow.
result: issue
reported: "pass but they should be able to be clicked, now they are not"
severity: minor

### 8. Web 3-state printed filter toggle (GAP-8)
expected: Web search results have a printed filter button cycling 3 states: show all / hide printed / only printed.
result: issue
reported: "It should be called 'Filter Printed' 'סנן דפוסים'. It works. It should be in desktop app too, a filter icon in the header of the row acts like this (filter look - regular with all [default], light-filled next click [no print], filled next [prints only])"
severity: minor

### 9. Hebrew translations for Phase 42 features (GAP-1)
expected: All Phase 42 UI strings appear in Hebrew when the UI is in Hebrew mode. No English strings leak through.
result: pass

## Summary

total: 9
passed: 3
issues: 6
pending: 0
skipped: 0

## Gaps

- truth: "Desktop search status text appears in Hebrew"
  status: failed
  reason: "User reported: Should translate 'searching...' and 'Showing 500 of X results'"
  severity: cosmetic
  test: 2
  root_cause: "4 translation keys missing in genizah_translations.py: 'Searching', 'Showing {} of {} results', 'Showing {} of {} results (searching {} expanded terms)', 'Showing {} of {} results (filtering {} domains)'. tr() silently returns English when key missing."
  artifacts:
    - path: "genizah_app.py"
      issue: "Lines 17081, 17237, 17233, 16702 use tr() with keys not in TRANSLATIONS dict"
    - path: "genizah_translations.py"
      issue: "Missing 4 translation keys for search status strings"
  missing:
    - "Add 'Searching': 'מחפש' to TRANSLATIONS"
    - "Add 'Showing {} of {} results': 'מציג {} מתוך {} תוצאות'"
    - "Add 'Showing {} of {} results (searching {} expanded terms)': 'מציג {} מתוך {} תוצאות (מחפש {} ביטויים מורחבים)'"
    - "Add 'Showing {} of {} results (filtering {} domains)': 'מציג {} מתוך {} תוצאות (מסנן {} תחומים)'"
  debug_session: ".planning/debug/desktop-search-status-hebrew.md"

- truth: "Regular search cancel responds promptly like composition search"
  status: failed
  reason: "User reported: works for composition but regular search cancel is also slow"
  severity: minor
  test: 3
  root_cause: "SearchThread has no cancel_flag (uses unsafe QThread.terminate()). execute_search() only calls progress_callback every 50 hits (i%50). CompositionThread was fixed to check every chunk but SearchThread was missed."
  artifacts:
    - path: "gui_threads.py"
      issue: "Lines 25-49: SearchThread has no cancel_flag, no cancel check in callback"
    - path: "genizah_core.py"
      issue: "Line 5864: progress_callback called with i%50 modulo guard"
    - path: "genizah_app.py"
      issue: "Line 17066: stop_search() uses unsafe QThread.terminate()"
  missing:
    - "Add cancel_flag to SearchThread (mirror CompositionThread pattern)"
    - "Change stop_search() to set cancel_flag instead of terminate()"
    - "Reduce i%50 to i%5 or remove modulo guard in execute_search()"
  debug_session: ".planning/debug/regular-search-cancel-slow.md"

- truth: "Desktop excluded items grouped under reason headers without per-item reason labels"
  status: failed
  reason: "User reported: The filtered should be grouped by the reason of filtering, thus not needed to repeat it in the items themselves"
  severity: cosmetic
  test: 5
  root_cause: "Filtered section (lines 20457-20510) uses flat structure under ROOT_FILT. Items added directly with [reason] prefix on title. No intermediate reason sub-header nodes."
  artifacts:
    - path: "genizah_app.py"
      issue: "Lines 20487-20500: items added flat under root_filt with per-item [reason] prefix"
    - path: "genizah_app.py"
      issue: "Lines 20955-20979: _collect_checked_comp_items_struct assumes flat children under ROOT_FILT"
  missing:
    - "Group items by reason into sub-header QTreeWidgetItem nodes under root_filt"
    - "Remove per-item [reason] prefix from title column"
    - "Update _collect_checked_comp_items_struct to handle new intermediate depth"
    - "Style reason sub-headers with amber foreground, make collapsible and checkable"
  debug_session: ".planning/debug/comp-filtered-reason-grouping.md"

- truth: "Desktop composition Printed column is narrow, resizable, and filterable"
  status: failed
  reason: "User reported: print column should be narrower (fit to the word print), resizable and filterable like in search results"
  severity: minor
  test: 6
  root_cause: "comp_col_printed is column index 7 (last column) with stretchLastSection(True) overriding width. ResizeToContents prevents user resize. Not included in filter_columns list."
  artifacts:
    - path: "genizah_app.py"
      issue: "Lines 9068/9090: setStretchLastSection(True) stretches last column (Printed)"
    - path: "genizah_app.py"
      issue: "Lines 9057/9089: ResizeToContents prevents manual resize"
    - path: "genizah_app.py"
      issue: "Line 9077: filter_columns list omits comp_col_printed"
  missing:
    - "Set setStretchLastSection(False), let comp_col_ms_context absorb remaining space with Stretch"
    - "Change Printed column to Fixed mode with width ~50px"
    - "Add comp_col_printed to filter_columns in CheckBoxHeader"
    - "Add printed filter logic to _comp_data_matches_filters"
  debug_session: ".planning/debug/comp-printed-column-wide.md"

- truth: "Web excluded results items are clickable"
  status: failed
  reason: "User reported: they should be able to be clicked, now they are not"
  severity: minor
  test: 7
  root_cause: "Excluded items (lines 2455-2474) rendered as plain ui.row + ui.label with no click handler or cursor-pointer. Regular results use .on('click', lambda r=result: load_in_viewer(r)) and cursor-pointer class."
  artifacts:
    - path: "web/pages/search.py"
      issue: "Lines 2461-2463: ui.row() missing click handler and cursor-pointer"
  missing:
    - "Add cursor-pointer class and .on('click', lambda: load_in_viewer(excl_result)) to excluded item rows"
  debug_session: ".planning/debug/excluded-results-not-clickable.md"

- truth: "Printed filter labeled 'Filter Printed'/'סנן דפוסים' and available on desktop with 3-state icon toggle in column header"
  status: failed
  reason: "User reported: Should be called 'Filter Printed' 'סנן דפוסים'. Needs desktop equivalent with filter icon in column header cycling 3 states"
  severity: minor
  test: 8
  root_cause: "Web button uses tr('Printed') at line 779 instead of tr('Filter Printed'). Desktop has no printed filter at all -- COL_PRINTED not in filter_columns, no filter state variable, no filter logic in _apply_results_table_filters."
  artifacts:
    - path: "web/pages/search.py"
      issue: "Line 779: button label uses tr('Printed') not tr('Filter Printed')"
    - path: "genizah_translations.py"
      issue: "Missing 'Filter Printed': 'סנן דפוסים' key"
    - path: "genizah_app.py"
      issue: "Lines 8707-8714: COL_PRINTED not in filter_columns, no 3-state filter"
  missing:
    - "Add 'Filter Printed': 'סנן דפוסים' translation"
    - "Change web button default label to tr('Filter Printed')"
    - "Add _printed_filter_state to desktop, cycle on COL_PRINTED header click"
    - "Add printed filter logic to _apply_results_table_filters"
    - "Update header icon color to reflect filter state"
  debug_session: ".planning/debug/printed-filter-web-label-desktop-missing.md"
