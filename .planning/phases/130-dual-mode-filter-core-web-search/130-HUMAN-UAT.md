---
status: partial
phase: 130-dual-mode-filter-core-web-search
source: [130-VERIFICATION.md]
started: 2026-06-30T12:30:00Z
updated: 2026-06-30T12:30:00Z
---

## Current Test

[awaiting human testing]

## Tests

### 1. Hide-mode persistence + intent across searches
test: Open web /search in a browser, apply Hide mode (e.g. hide RNL), run a new search, confirm RNL results are still absent; navigate away and back, confirm the "Hiding N" button state persists.
expected: Button reads "Hiding N" after reload; results exclude RNL across searches; mode toggle initialized to Hide on dialog reopen.
result: [pending]

### 2. Show-only persistence round-trip + show-all normalization
test: Open the library filter dialog in Show-only mode, check 2-3 libraries, click Apply; reload the page; confirm the button shows "Showing N/total" and only those libraries appear. Also check ALL libraries in Show-only and Apply — confirm the button returns to the neutral state (show-all normalization).
expected: Button reads e.g. "Showing 2/18"; reload restores the same state; results contain only the chosen libraries; checking everything normalizes to neutral.
result: [pending]

### 3. Legacy v8.3.0 allowlist migration
test: Load the page with a legacy plain-list `search_library_filter` in safe_storage (inject `{'search_library_filter': ['CUL','JTS']}` via devtools), reload; confirm button reads "Showing 2/N" and dialog opens in Show-only mode.
expected: Legacy list migrates silently to Show-only with ['CUL','JTS']; no error; dialog toggle initialized to "Show only selected".
result: [pending]

### 4. Mode-toggle behavior — D-04 reset + Apply-enable (validates the CR-01 fix)
test: In the dialog, flip the mode toggle from Show-only to Hide; confirm the toggle ACTUALLY changes mode at runtime and the checkbox selection is immediately cleared (D-04); in Hide mode with zero boxes checked, confirm Apply is enabled (not greyed out). Apply in each mode and confirm the filter behaves accordingly (Show-only keeps only checked; Hide removes checked).
expected: Mode flip switches mode and resets all checkboxes; Apply enabled in Hide with empty selection; Show-only Apply restricts to checked libraries, Hide Apply removes them. (This is the live confirmation that the CR-01 `on_value_change` fix wires the toggle correctly.)
result: [pending]

### 5. Browse-to-search handoff
test: Navigate from Browse-by-Identification (catalog) to /search with 2 libraries selected; confirm the library filter button shows "Showing 2/N" in Show-only mode immediately.
expected: filter_panel.consume_incoming_filters writes Show-only mode + dict shape; button reflects it without a separate search run.
result: [pending]

### 6. Hebrew UI label rendering
test: In Hebrew UI, confirm button states read "סינון לפי ספרייה" (neutral), "מציג N מתוך total" (Show-only), "מסתיר N" (Hide); confirm dialog toggle labels read "הצג רק נבחרות" / "הסתר נבחרות".
expected: All four new HE translation keys render correctly; no English fallback under Hebrew UI.
result: [pending]

## Summary

total: 6
passed: 0
issues: 0
pending: 6
skipped: 0
blocked: 0

## Gaps
