---
status: diagnosed
trigger: "Web excluded results items in search are not clickable"
created: 2026-03-01T00:00:00Z
updated: 2026-03-01T00:00:00Z
---

## Current Focus

hypothesis: Excluded results are rendered as plain ui.row with ui.label elements -- no click handler, no cursor:pointer, no link to load_in_viewer or open_advanced_dialog
test: Compare excluded rendering (lines 2455-2474) vs regular card (lines 2494-2594)
expecting: Regular cards have .on('click', load_in_viewer); excluded items have nothing
next_action: Return diagnosis

## Symptoms

expected: Clicking an excluded result item should open the manuscript detail (same as regular results)
actual: Excluded result items are not clickable at all -- no visual affordance, no handler
errors: None (silent missing functionality)
reproduction: Run a search with domain exclusions active, expand "Excluded Results" section, click any item -- nothing happens
started: Since excluded results feature was implemented

## Eliminated

(none -- root cause found on first pass)

## Evidence

- timestamp: 2026-03-01
  checked: Lines 2455-2474 -- excluded results rendering
  found: Each excluded item is a plain ui.row containing ui.label for shelfmark, title, and reason. No .on('click') handler, no cursor-pointer class, no link.
  implication: Excluded items are purely display-only, missing all interactivity

- timestamp: 2026-03-01
  checked: Lines 2494-2513 -- regular result card rendering
  found: Regular cards use ui.card with 'cursor-pointer' class (line 2495) and the main content column has `.on('click', lambda r=result: load_in_viewer(r))` (line 2513). Also has open_advanced_dialog button (line 2581).
  implication: The click-to-view pattern exists and works; it was simply never applied to excluded items.

- timestamp: 2026-03-01
  checked: load_in_viewer function (line 3727)
  found: Takes a `result` dict, reads display.shelfmark, display.id, snippet, full_text, etc. The excluded items store the full result object in excl_item['result'] (line 2456).
  implication: The excluded items already have the full result data needed to call load_in_viewer.

## Resolution

root_cause: The excluded results section (lines 2455-2474) renders items as plain, non-interactive ui.row + ui.label elements. Unlike regular result cards (line 2513) which attach `.on('click', lambda r=result: load_in_viewer(r))` to the content column and have `cursor-pointer` CSS class, the excluded items have NO click handler and NO visual cursor affordance.

fix: Add click handler to each excluded item row, calling load_in_viewer(excl_result), and add cursor-pointer styling.

verification: (pending implementation)
files_changed: []
