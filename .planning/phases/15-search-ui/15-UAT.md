---
status: complete
phase: 15-search-ui
source: 15-01-SUMMARY.md, 15-02-SUMMARY.md
started: 2026-02-10T00:00:00Z
updated: 2026-02-10T00:05:00Z
---

## Current Test

[testing complete]

## Tests

### 1. Web Responsa Checkboxes Visible
expected: On the web search page in Exact or Variants mode, a Responsa Mode checkbox is visible. Checking it reveals sub-checkboxes: Variants, Judeo-Arabic, Flexible Spacing.
result: issue
reported: "No responsa mode checkbox seen. Responsa should be a mode option in the Mode dropdown (after Exact and Variants), not a separate checkbox row. At least for phase 1. Also needs a legend explaining the Responsa syntax shortcuts when Responsa mode is active."
severity: major

### 2. Web Mode Dropdown Hides on Responsa Toggle
expected: When Responsa Mode is checked, the search mode dropdown disappears and an amber "Responsa" badge/indicator appears. Unchecking restores the dropdown to its previous value.
result: skipped
reason: UX approach changed — Responsa will be a dropdown mode, not a checkbox toggle

### 3. Web Responsa Search Executes
expected: With Responsa Mode ON, searching for a Hebrew term (e.g. `#שלום`) returns results. An expanded term count appears in the results header.
result: skipped
reason: Cannot test until dropdown mode approach is implemented

### 4. Web Explosion Guard Warning
expected: Searching a complex query that triggers the explosion guard (e.g. many prefix/suffix/JA expansions) shows a brief auto-dismissing warning notification.
result: skipped
reason: Cannot test until dropdown mode approach is implemented

### 5. Web URL State Persistence
expected: After performing a Responsa search with checkboxes on, the browser URL contains parameters like `?responsa=1&variants=1&ja=1&flex_spaces=1`. Reloading with those params restores checkbox states.
result: skipped
reason: URL state design will change with dropdown approach

### 6. Web Mobile Responsa Controls
expected: On a narrow viewport (mobile), Responsa controls collapse behind an icon button. Tapping it opens a popup menu with the same checkbox options, synced to the desktop row.
result: skipped
reason: Mobile UX will change with dropdown approach

### 7. Web Responsa Hidden in PGP Tags Mode
expected: When switching to PGP Tags search mode, all Responsa checkboxes are hidden (not just disabled). Switching back to Exact/Variants makes them visible again.
result: skipped
reason: Responsa will be a mode option — not applicable in PGP Tags by default

### 8. Desktop Responsa Checkboxes Visible
expected: In the desktop app search tab, a Responsa Mode checkbox row appears below the mode/params row. Checking it reveals sub-checkboxes: Variants, Judeo-Arabic, Flex Spacing, Bidirectional.
result: skipped
reason: UX approach changed — desktop will also use combo box mode entry

### 9. Desktop Mode Combo Hides on Responsa Toggle
expected: When Responsa Mode is checked in desktop, the mode combo box disappears and an amber "Responsa Mode" label appears. Unchecking restores the combo to its previous selection.
result: skipped
reason: UX approach changed — no checkbox toggle, Responsa is a combo mode

### 10. Desktop Responsa Search Executes
expected: With Responsa Mode ON in desktop, searching for a Hebrew term returns results. The status label shows expanded term count.
result: skipped
reason: Cannot test until dropdown mode approach is implemented

### 11. Desktop Explosion Guard Warning
expected: A complex Responsa query in desktop that triggers the explosion guard shows a warning in the status label that auto-dismisses after ~5 seconds.
result: skipped
reason: Cannot test until dropdown mode approach is implemented

### 12. Desktop Responsa Hidden in Non-Applicable Modes
expected: In desktop, when switching to PGP Tags, Shelfmark, Title, Fuzzy, or Regex mode, the Responsa row is hidden. Switching to Exact or Variants makes it visible again.
result: skipped
reason: Responsa will be a combo mode — test no longer applicable

### 13. Desktop Checkboxes Reset on Startup
expected: Closing and reopening the desktop app resets all Responsa checkboxes to their defaults (unchecked).
result: skipped
reason: No separate checkboxes with dropdown approach

## Summary

total: 13
passed: 0
issues: 1
pending: 0
skipped: 12

## Gaps

- truth: "Responsa Mode accessible as a search mode with sub-options and syntax legend"
  status: failed
  reason: "User reported: Responsa should be a mode option in the Mode dropdown (after Exact and Variants), not a separate checkbox row. When Responsa mode is selected, sub-options (Variants, JA, Flex Spacing, Bidirectional) appear, plus a legend explaining Responsa syntax shortcuts (#prefix, #both#, %plene, *wildcard)."
  severity: major
  test: 1
  root_cause: "UX design mismatch — built as separate checkbox row but user wants it as a dropdown mode entry. Entire Phase 15 UI approach needs rework."
  artifacts:
    - path: "web/pages/search.py"
      issue: "Responsa checkbox row (lines ~452-509) and toggle logic (~548-644) need replacement with dropdown mode approach"
    - path: "genizah_app.py"
      issue: "Desktop Responsa checkbox row (~6280) and toggle method (~12388) need replacement with combo mode approach"
  missing:
    - "Add 'Responsa' as a mode option in web mode_select dropdown"
    - "Add 'Responsa' as a mode option in desktop mode_combo"
    - "Show sub-option checkboxes (Variants, JA, Flex Spacing, Bidirectional) when Responsa mode is selected"
    - "Show syntax legend/explanation when Responsa mode is active"
    - "Remove separate checkbox row and toggle logic from both apps"
    - "Update URL state to use mode=responsa instead of separate responsa=1 param"
  debug_session: ""
