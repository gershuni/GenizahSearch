---
status: diagnosed
trigger: "Diagnose printed filter: web label wrong, desktop needs 3-state filter in Printed column header"
created: 2026-03-01T00:00:00Z
updated: 2026-03-01T00:00:00Z
---

## Current Focus

hypothesis: CONFIRMED -- Web uses "Printed"/"Hiding printed"/"Only printed" labels (not "Filter Printed"); Desktop has no printed filter at all
test: Code search confirmed both findings
expecting: N/A -- diagnosis complete
next_action: Return diagnosis

## Symptoms

expected: Web button labeled "Filter Printed"/"סנן דפוסים"; Desktop has 3-state printed filter in column header
actual: Web uses "Printed" / "Hiding printed" / "Only printed" labels; Desktop only displays printed badges, no filtering
errors: N/A (label mismatch + feature gap)
reproduction: Visual inspection of both apps
started: Current state

## Eliminated

## Evidence

- timestamp: 2026-03-01
  checked: web/pages/search.py lines 753-783 -- printed filter toggle button
  found: Button created at line 779 with label tr('Printed'), cycles through 3 states with labels "Printed", "Hiding printed", "Only printed"
  implication: The button label in default state is "Printed" (Hebrew "דפוס"), NOT "Filter Printed" / "סנן דפוסים"

- timestamp: 2026-03-01
  checked: genizah_translations.py lines 2481-2484 -- translation keys
  found: Keys are "Printed"="דפוס", "Hiding printed"="מסתיר דפוסים", "Only printed"="דפוסים בלבד". No "Filter Printed"/"סנן דפוסים" key exists.
  implication: Need to add new translation key if relabeling, or use existing keys

- timestamp: 2026-03-01
  checked: genizah_app.py line 8697-8714 -- search results table setup
  found: COL_PRINTED=11, header label is tr("Printed"), column is 50px Fixed. CheckBoxHeader has filter_columns for other cols but NOT for COL_PRINTED. desc_first_cols includes COL_PRINTED (descending-first sort only).
  implication: Desktop has no filter icon/callback on Printed column, only sort capability

- timestamp: 2026-03-01
  checked: genizah_app.py lines 17461-17475 -- _on_printed_badges_loaded
  found: Desktop stores printed sys_ids in self._printed_sys_ids set after PrintedBadgeWorker completes. Data is available for filtering but no filter mechanism exists.
  implication: Data infrastructure (printed_sys_ids set) already exists, just needs filter UI + logic

- timestamp: 2026-03-01
  checked: genizah_app.py lines 1161-1262 -- CheckBoxHeader class
  found: Supports filter_columns (funnel icon + callback), star_columns (star icon + callback), non_sortable_cols, desc_first_cols. No third icon type for printed/toggle filter.
  implication: Need to either add a new icon type to CheckBoxHeader or repurpose filter_columns with custom behavior for COL_PRINTED

- timestamp: 2026-03-01
  checked: genizah_app.py lines 17388-17444 -- _apply_results_table_filters
  found: Existing filter framework handles column text filters, list filters, and domain exclusions. No printed filter logic. Could be extended with a printed filter state.
  implication: The general row-visibility filter loop is the right place to add printed filtering

## Resolution

root_cause: |
  TWO ISSUES:
  1. WEB: The printed filter button (search.py:779) uses label "Printed" (tr key "Printed" = "דפוס") in its default/all state. User wants "Filter Printed" / "סנן דפוסים" instead. The translation key "Filter Printed" does not exist in genizah_translations.py.
  2. DESKTOP: The Printed column (COL_PRINTED=11) only displays badges. No filter mechanism exists. The CheckBoxHeader supports filter_columns and star_columns icon types, but COL_PRINTED is not in either list. The _apply_results_table_filters method has no printed filter logic. However, self._printed_sys_ids (populated by PrintedBadgeWorker) already contains the data needed for filtering.

fix:
verification:
files_changed: []
