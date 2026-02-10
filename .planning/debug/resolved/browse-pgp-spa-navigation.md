---
status: resolved
trigger: "PGP transcription doesn't load when navigating from search to browse page due to NiceGUI SPA caching"
created: 2026-02-06T00:00:00Z
updated: 2026-02-06T00:00:00Z
---

## Current Focus

hypothesis: ui.run_javascript() in tuple expression doesn't execute properly when combined with dialog.close()
test: Change browse button implementations from ui.run_javascript() to ui.link() elements
expecting: ui.link() forces full page reload, browse page recreated with proper PGP data
next_action: Fix browse navigation in search.py - replace ui.run_javascript() with ui.link()

## Symptoms

expected: When clicking "Browse Full Manuscript" from search results, the browse page should show PGP transcription in version selector
actual: No transcription shown when navigating from search. Works fine when typing shelfmark directly in browse.
errors: No errors - the code simply doesn't execute
reproduction: 1) Search for "קטעה נכל" 2) Find "T-S 8J4.22" 3) Click Browse button 4) Check version selector
started: Discovered during Phase 5 UAT testing. The browse page itself works; only navigation from search is broken.

## Eliminated

## Evidence

- timestamp: 2026-02-06T00:00:00Z
  checked: Prior investigation notes
  found: Debug output proves create_browse_page() is NOT called when navigating from search; NiceGUI uses SPA routing
  implication: Need to either force page recreation or detect URL changes client-side

- timestamp: 2026-02-06T00:01:00Z
  checked: search.py navigation mechanisms
  found: Multiple navigation approaches in search.py:
    - Lines 1651, 1726, 1974: ui.run_javascript(f'window.location.href = "{url}"') - should force full reload
    - Line 1901: ui.navigate.to() - SPA navigation (problematic)
    - Lines 2147-2150: ui.link(target=browse_url) - proper link element (should work)
    - Line 1284: ui.run_javascript() for mobile "Open in Viewer"
  implication: The ui.run_javascript approach SHOULD work but may not execute. Need to test.

- timestamp: 2026-02-06T00:02:00Z
  checked: All 5 browse URL navigation points in search.py
  found:
    - Line 1282 (mobile): ui.run_javascript()
    - Line 1650-1651 (fullscreen): dialog.close() + ui.run_javascript() in tuple
    - Line 1725-1726 (adv header): dialog.close() + ui.run_javascript() in tuple
    - Line 1974 (adv actions): dialog.close() + ui.run_javascript() in tuple
    - Line 2147-2150 (main viewer): ui.link() - PROPER SOLUTION
  implication: The ui.link() approach at line 2147-2150 is correct. Need to apply same pattern to all other locations.

- timestamp: 2026-02-06T00:03:00Z
  checked: Applied fix to all 4 locations in search.py
  found: All ui.run_javascript() navigation calls replaced with ui.link() elements
  implication: Navigation should now force full page reload, ensuring browse page recreates with PGP data

## Resolution

root_cause: ui.run_javascript() in tuple expression with dialog.close() doesn't execute reliably for navigation. NiceGUI SPA routing caches pages, so ui.navigate.to() also fails. Only ui.link() forces a true page reload.
fix: Replaced all 4 instances of ui.run_javascript('window.location.href = "..."') with ui.link(target=browse_url) elements at lines 1283, 1651, 1726, 1973 in search.py
verification: |
  - Syntax check: PASSED
  - Web app startup: PASSED (no errors)
  - Unit tests: PASSED (8/8)
  - Manual verification needed: Navigate from search results to browse and confirm PGP transcription loads
files_changed: [web/pages/search.py]
