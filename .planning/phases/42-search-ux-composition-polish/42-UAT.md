---
status: diagnosed
phase: 42-search-ux-composition-polish
source: [42-06-SUMMARY.md, 42-07-SUMMARY.md]
started: 2026-03-01T18:00:00Z
updated: 2026-03-01T18:30:00Z
round: 3
---

## Current Test

[testing complete]

## Tests

### 1. Desktop search status Hebrew translations (GAP-R1)
expected: In Hebrew UI mode, desktop search status shows "מחפש..." during search and "מציג X מתוך Y תוצאות" for result counts. No English strings leak through.
result: issue
reported: "Still has 'Searching...', the Showing etc. is in Hebrew"
severity: cosmetic

### 2. Regular search cancel responsiveness (GAP-R2)
expected: Cancelling a desktop regular search responds within ~5 seconds. SearchThread uses cooperative cancel_flag instead of unsafe terminate(). No hang or long delay.
result: issue
reported: "pass. Should add notification that it's partial results"
severity: minor

### 3. Desktop excluded section grouped by reason (GAP-R3)
expected: Desktop composition excluded section shows reason sub-headers as collapsible amber nodes. Items listed under their reason group WITHOUT per-item [reason] prefix.
result: issue
reported: "pass. Need to translate titles like High frequency"
severity: cosmetic

### 4. Desktop composition Printed column narrow/filterable (GAP-R4)
expected: Composition Printed column is narrow (~55px), not stretched. Column header has filter icon cycling 3 states.
result: pass
note: "User requests Printed badge also in ResultDialog, Browse tab, and web equivalents (advanced view, browse module) -- future scope"

### 5. Web excluded results clickable (GAP-R5)
expected: Excluded results in web search are clickable. Clicking opens in viewer. Cursor shows pointer on hover.
result: pass

### 6. Printed filter label + desktop 3-state filter (GAP-R6)
expected: Web button shows "Filter Printed" / "סנן דפוסים". Desktop search results have 3-state printed filter via column header.
result: issue
reported: "Pass but the filter icon in composition search too should act like in the regular search results (desktop)"
severity: minor

### 7. Web regular search cancel responsiveness (GAP-R7, user-reported)
expected: Web regular search cancel responds promptly without ~20s delay.
result: issue
reported: "fix the 20s span in partial results in WEB regular search"
severity: minor

## Summary

total: 7
passed: 2
issues: 5
pending: 0
skipped: 0

## Gaps

- truth: "Desktop search status shows 'מחפש...' in Hebrew mode"
  status: failed
  reason: "User reported: Still has 'Searching...', the Showing etc. is in Hebrew"
  severity: cosmetic
  test: 1
  root_cause: "Line 17089 uses tr('Searching') but the translation key is 'Searching...' (with ellipsis). Code does f\"{tr('Searching')}... {elapsed_str}\" so bare 'Searching' key has no match."
  artifacts:
    - path: "genizah_app.py"
      issue: "Line 17089: tr('Searching') -- key without ellipsis doesn't exist"
    - path: "genizah_translations.py"
      issue: "Has 'Searching...' but not bare 'Searching'"
  missing:
    - "Add 'Searching': 'מחפש' to TRANSLATIONS"

- truth: "Desktop regular search cancel shows partial results notification"
  status: failed
  reason: "User reported: Should add notification that it's partial results"
  severity: minor
  test: 2
  root_cause: "stop_search() at line 17068-17075 calls reset_ui() which hides progress bar. No partial results notification."
  artifacts:
    - path: "genizah_app.py"
      issue: "Line 17068-17075: stop_search() just resets UI, no partial results notification"
  missing:
    - "Show status bar message like 'Partial results' after cancel"

- truth: "Desktop excluded section reason sub-header titles appear in Hebrew"
  status: failed
  reason: "User reported: Need to translate titles like High frequency"
  severity: cosmetic
  test: 3
  root_cause: "_get_filter_reason uses tr('Found in source text') and tr('High frequency') but these keys don't exist in genizah_translations.py."
  artifacts:
    - path: "genizah_translations.py"
      issue: "Missing 'Found in source text', 'High frequency', 'Filtered' translation keys"
  missing:
    - "Add 'Found in source text': 'נמצא בטקסט המקור'"
    - "Add 'High frequency': 'תדירות גבוהה'"
    - "Add 'Filtered': 'סונן'"

- truth: "Desktop composition tree printed filter cycles 3 states like regular search results"
  status: failed
  reason: "User reported: filter icon in composition search too should act like in regular search results"
  severity: minor
  test: 6
  root_cause: "Comp tree CheckBoxHeader treats comp_col_printed as text filter. Regular search intercepts COL_PRINTED for 3-state cycling. Comp needs same intercept."
  artifacts:
    - path: "genizah_app.py"
      issue: "_comp_data_matches_filters treats comp_col_printed as text filter, not 3-state"
  missing:
    - "Add _comp_printed_filter_state, intercept comp_col_printed click for 3-state cycle"
    - "Add 3-state printed filter logic to comp filter application"

- truth: "Web regular search cancel responds promptly without ~20s delay"
  status: failed
  reason: "User reported: fix the 20s span in partial results in WEB regular search"
  severity: minor
  test: 7
  root_cause: "Three bottlenecks: (1) Tantivy searcher.search() is a blocking C-extension call scoring 50K docs (5-20s, un-cancellable), (2) post-cancel enrichment queries (domains, transcriptions, catalog, printed) run unconditionally (1-3s), (3) lab mode cancel bug: bare except Exception swallows InterruptedError at genizah_core.py:779-782."
  artifacts:
    - path: "genizah_core.py"
      issue: "Line 5850: Tantivy search blocks with no cancel (dominant)"
    - path: "web/pages/search.py"
      issue: "Lines 2250-2283: enrichment runs unconditionally after cancel"
    - path: "genizah_core.py"
      issue: "Lines 779-782: bare except Exception swallows InterruptedError in lab mode"
  missing:
    - "Skip enrichment queries when was_cancelled is True"
    - "Fix lab mode bare except to re-raise InterruptedError"
    - "Accept Tantivy call as un-cancellable but make everything after it instant on cancel"

## Future Scope (noted by user)

- Printed badge needed in: ResultDialog, Browse tab, web advanced view, web browse module
