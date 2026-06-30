---
status: partial
phase: 131-dual-mode-parity-desktop-catalog-web-browse-by-identificatio
source: [131-VERIFICATION.md]
started: 2026-06-30T14:30:00Z
updated: 2026-06-30T14:30:00Z
---

## Current Test

[awaiting human testing]

## Tests

### 1. Desktop catalog dialog — mode toggle + in-session persistence + button label
expected: Open Catalog tab → library filter button → toggle Hide vs Show-only, select a subset, Apply, reopen the dialog within the same session. Dialog reopens with the previously selected mode and codes; toggling mode clears all checkboxes (D-04 reset); button shows "Showing N/M libraries" (Show-only) or "Hiding N libraries" (Hide) with the real Phase-130 pluralized keys; total M matches the selectable universe (library_codes_with_manuscripts minus LOCAL, not the broader LIBRARY_CODES count).
result: [pending]

### 2. Desktop — Hide-mode suppression on "Search/Composition in these results"
expected: With a Hide library selection active, click "Search in these results" and "Composition in these results". The library restriction is SUPPRESSED (not silently inverted); status bar shows "Library Hide filter not applied to search/composition" (~5 s); the search/composition scope is NOT narrowed by the Hide selection.
result: [pending]

### 3. Web /catalog Browse-by-Identification — true-facet shortlist, toggles, reload persistence
expected: Open the library filter dialog with NO other filters active → the count-shortlist shows TRUE full-set per-library counts from get_browse_library_facets (off-page libraries appear, not just current PAGE_SIZE=50 page); toggle Show-only/Hide, type in text-search, click sort-by-count vs A-Z, Apply, reload the page → mode+set survive (dict-shape persist); SEED-023 PGP/Editions filters still work alongside the library filter.
result: [pending]

### 4. Web /catalog — Hide-mode handoff to /search preserves mode
expected: With a Hide library selection active, click "Search in these results" → the /search page opens with the filter in Hide mode (button shows "Hiding N libraries", not "Showing N/M"). The {mode,codes} handoff preserves the mode round-trip (not converted to Show-only).
result: [pending]

### 5. Web /parallels — library-only Show-only scope (ungated) + reload persistence
expected: Run a composition search → open the new library-filter button → select a subset in Show-only mode with NO advanced filters active → Apply. Results rescope to the chosen libraries (NOT a no-op — library-only Show-only scope works ungated by _has_active_filters); results outside the selected libraries disappear. Reload the page → mode+set survive (parallels_library_filter key persists).
result: [pending]

### 6. Web /parallels — Hide-mode export scoping
expected: With Hide mode active, export results (XLSX/JSON) → exported rows exclude rows from the hidden libraries (Show-only pre-query and Hide pre-export scoping both hold).
result: [pending]

## Summary

total: 6
passed: 0
issues: 0
pending: 6
skipped: 0
blocked: 0

## Gaps
