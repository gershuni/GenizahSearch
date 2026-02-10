---
status: complete
phase: 17-integration-testing
source: 17-UAT.md (retest of 6 diagnosed gaps after fix plans 17-03/04/05)
started: 2026-02-10T19:00:00Z
updated: 2026-02-10T19:30:00Z
---

## Current Test

[testing complete]

## Tests

### 1. R+Space Shortcut Shows Sub-Options
expected: In web app, type "R " (R+space) in search field. Responsa mode activates AND sub-options row appears with Variants/JA/Flex Spacing checkboxes visible.
result: pass
note: Activates only after keystroke AFTER the space, not immediately on space press. Minor UX timing issue.

### 2. Large Result Set Without Crash
expected: With Responsa mode active, search for `#שלום` (prefix expansion). Search completes without "Connection lost" error. Results display (capped at 200 rendered). No WebSocket crash or stuck animation.
result: pass

### 3. Wildcard Suffix Matches Past Sofit
expected: With Responsa mode active, search for `שלום*` (suffix wildcard). Results include words like שלומו, שלומם, שלומנו — not just exact שלום. The sofit ם is converted to [םמ] char class in regex.
result: pass

### 4. Toolbar Stays Visible After Search
expected: With Responsa mode active + Variants ON, perform a search. After results load, the search toolbar (mode dropdown, options row) remains visible. No disappearing UI elements.
result: pass

### 5. Explosion Guard Cascade Downgrades
expected: With Responsa + Variants + JA all ON, search for `#%שלום# #%עולם#`. If expansion exceeds 500 terms, a warning toast/notification appears in the web UI explaining the cascade downgrade. NOT a silent 0-results page.
result: pass
note: Cascade works. Separate observation: `*word*` (both-side wildcard) returns 0 results because Tantivy can't find candidates without a stem — known two-phase architecture limitation.

### 6. Desktop Tabular Builder RTL
expected: In desktop app, open Responsa mode and click "Query Builder". The dialog layout is RTL (right-to-left) regardless of language setting. Word inputs and preview text flow RTL.
result: pass
note: Explosion guard warning in desktop is English-only. Should audit Responsa strings for untranslated text.

## Summary

total: 6
passed: 6
issues: 0
pending: 0
skipped: 0

## Gaps

[none yet]
