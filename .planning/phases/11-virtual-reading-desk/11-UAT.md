---
status: diagnosed
phase: 11-virtual-reading-desk
source: 11-01-SUMMARY.md, 11-02-SUMMARY.md, 11-03-SUMMARY.md, 11-04-SUMMARY.md, 11-05-SUMMARY.md, 11-06-SUMMARY.md, 11-07-SUMMARY.md
started: 2026-02-08T12:00:00Z
updated: 2026-02-08T13:00:00Z
---

## Current Test

[testing complete]

## Tests

### 1. Web - Enter Reading Desk from Joins
expected: In the web app, navigate to a manuscript that has PGP joins (multi-fragment document). Click "View All Fragments" from the joins panel. The page should switch to a dual-pane layout: images stacked on the left, texts stacked on the right. Each fragment should have a shelfmark header in both panes. The header should show "Document #X" with the PGP ID.
result: pass

### 2. Web - Per-Image Controls
expected: In the web reading desk, each image should have its own zoom in (+), zoom out (-), rotate left, and rotate right controls. Clicking zoom on one image should only affect that image, not others. Drag to pan should work on individual images.
result: pass

### 3. Web - Per-Fragment Version Selector
expected: In the web reading desk text pane, each fragment that has PGP sources should show a dropdown selector. The dropdown should list editions (by scholar name) and translations (by language). Selecting a different version should update only that fragment's text display. RTL text for Hebrew editions, LTR for English translations.
result: pass

### 4. Web - Synchronized Scrolling
expected: In the web reading desk with multiple fragments, scrolling past a fragment boundary in the image pane should auto-scroll the text pane to the matching fragment header (and vice versa).
result: pass

### 5. Web - "Add to View" Button
expected: When viewing a single manuscript, there should be an "Add to View" button (library_add icon) in the header bar. Clicking it when no reading desk is active should start a new reading desk with that manuscript. The header should show "Reading Desk" (not "Document #X" since it wasn't entered from joins).
result: pass

### 6. Web - Toolbar Shelfmark Add
expected: While in reading desk mode, a toolbar should appear with a shelfmark text input and "Add" button. Typing a valid shelfmark (e.g., "T-S 12.123") and clicking Add (or pressing Enter) should add that manuscript to the reading desk. A new fragment section should appear in both panes.
result: pass

### 7. Web - Add from List Dialog
expected: While in reading desk mode, clicking "Add from List" in the toolbar should open a dialog. The dialog should show your personal lists as expandable panels. Expanding a list should show the manuscripts inside it with their shelfmarks. An "Add All" button should add all items from that list. Items already in the desk should show a check icon.
result: issue
reported: "You can only add all - I want to be able to add selected also"
severity: minor

### 8. Web - Remove Fragment
expected: Each fragment header in the reading desk should have an X/close button. Clicking it should remove that fragment from both panes. If all fragments are removed, the reading desk should exit and return to normal browse mode.
result: pass

### 9. Web - Back to Page View
expected: The "Back to Page View" button in the reading desk header should be visible in both Light Mode and Dark Mode (white text on the green header). Clicking it should exit the reading desk and return to normal single-manuscript browse view.
result: issue
reported: "Back to Page View button and Reading Desk label invisible in Light Mode. Only badge shows. Dark Mode is fine."
severity: major

### 10. Web - Language Switch Preserves Reading Desk
expected: While in reading desk mode with one or more manuscripts, switch the UI language (e.g., English to Hebrew or vice versa). After the page reloads, the reading desk should still be active with all the same manuscripts loaded.
result: issue
reported: "doesn't work, goes back to blank browse page with no manuscript loaded"
severity: major

### 11. Web - Text Pane Word Wrap
expected: In the reading desk text pane, long lines of text should wrap within the pane width rather than causing horizontal overflow or clipping.
result: issue
reported: "No wrap"
severity: minor

### 12. Desktop - Enter Reading Desk via "Add to View"
expected: In the desktop app, the "Add to View" button should appear right after the "Go" button in the browse tab. Clicking it should enter reading desk mode showing the current manuscript. The text pane should show the transcription and the image pane should show stacked images with per-image zoom/rotate controls. A green toolbar should appear at the top.
result: issue
reported: "The screen shows up but no scrolling is working properly and only one fragment shows up"
severity: major

### 13. Desktop - Synchronized Scrolling
expected: In the desktop reading desk with multiple fragments, scrolling the text pane should proportionally scroll the image pane (and vice versa). Both directions should work.
result: issue
reported: "No, scrolling is broken"
severity: major

### 14. Desktop - Per-Fragment Version Selector
expected: In the desktop reading desk, each fragment with PGP sources should have a clickable "[change version]" link. Clicking it should open a QInputDialog listing available editions and translations. Selecting one should update that fragment's text.
result: skipped
reason: Blocked by broken scrolling (Test 12/13)

### 15. Desktop - Toolbar Shelfmark Add
expected: The green reading desk toolbar should have a shelfmark input field and "Add to Desk" button. Typing a valid shelfmark and clicking the button should add that manuscript to the desk. The fragment count should update.
result: issue
reported: "Should add but only one fragment shows always, the first"
severity: major

### 16. Desktop - Joins "Open in Reading Desk"
expected: When viewing a manuscript that has joins, the joins dropdown should include an "Open in Reading Desk" option. Clicking it should open the reading desk with all joined fragments loaded.
result: skipped
reason: PGP joins not available in desktop (Phase 12 scope)

### 17. Desktop - Exit Reading Desk
expected: Clicking "Exit Reading Desk" (red button in toolbar) should close the reading desk, restore the normal image viewer and text display, and re-enable page navigation controls.
result: pass

## Additional Issues (observed during passing tests)

### A1. Web - Navigation from elsewhere breaks reading desk
observed_during: Test 5, Test 6
reported: "When in Reading Desk and go somewhere else and try to view ms (such as from list) it does not work, goes to blank browse page. Entering shelfmark shows previous Reading Desk and have to go back to page view to see it."
severity: major

### A2. Web - Console error on toolbar shelfmark add
observed_during: Test 6
reported: "Console error: 'The parent element this slot belongs to has been deleted' — RuntimeError in toolbar_add_by_shelfmark at browse.py:2236 when ui.notify is called"
severity: minor

## Summary

total: 17
passed: 8
issues: 7
pending: 0
skipped: 2

additional_issues: 2

## Gaps

- truth: "Add from List dialog allows selecting individual manuscripts"
  status: failed
  reason: "User reported: You can only add all - I want to be able to add selected also"
  severity: minor
  test: 7
  root_cause: "Dialog only has Add All button per list, no per-manuscript checkboxes or selection"
  artifacts:
    - path: "web/pages/browse.py"
      issue: "show_add_from_list_dialog only has bulk add, no individual selection"
  missing:
    - "Add checkboxes per manuscript in expansion panel"
    - "Add 'Add Selected' button alongside 'Add All'"
  debug_session: ""

- truth: "Back to Page View button and Reading Desk label visible in Light Mode"
  status: failed
  reason: "User reported: invisible in Light Mode, only badge shows, Dark Mode fine"
  severity: major
  test: 9
  root_cause: "Label uses .classes('text-white') CSS class overridden by Quasar Light Mode. Button uses .style('color: white !important;') but Quasar .q-btn internals override inline styles. Badge uses same inline style but simpler component hierarchy. 20+ other buttons in codebase use .props('text-color=white') which is Quasar's official API."
  artifacts:
    - path: "web/pages/browse.py"
      issue: "Lines 2380 (icon), 2385 (label), 2396 (button) use wrong styling approach"
  missing:
    - "Button: change to .props('flat dense text-color=white')"
    - "Label: use .style('color: white !important;') (labels don't have text-color prop)"
    - "Icon: use .style('color: white !important;')"
  debug_session: ".planning/debug/web-light-mode-visibility.md"

- truth: "Language switch preserves reading desk state"
  status: failed
  reason: "User reported: doesn't work, goes back to blank browse page with no manuscript loaded"
  severity: major
  test: 10
  root_cause: "Reading desk state persistence interacts badly with cross-page navigation. Persisted state takes priority over URL sys_id params, but restoration may fail silently. Language not persisted to storage (resets to 'he' hardcoded default). Need to distinguish language-switch reload from cross-page navigation."
  artifacts:
    - path: "web/pages/browse.py"
      issue: "Lines 3648-3667: initialization flow doesn't distinguish reload types"
    - path: "web/pages/browse.py"
      issue: "Lines 1143-1181: persist/restore logic"
    - path: "web/main.py"
      issue: "Line 1645-1649: toggle_lang doesn't persist language to storage"
  missing:
    - "Add error logging to _restore_reading_desk_state() to identify silent failures"
    - "Distinguish language-switch reload from cross-page navigation"
    - "Consider using app.storage.browser for more robust persistence"
  debug_session: ".planning/debug/web-language-switch-state.md"

- truth: "Text pane word wrap prevents horizontal overflow"
  status: failed
  reason: "User reported: No wrap"
  severity: minor
  test: 11
  root_cause: "Text container at line 2728 has style('overflow: hidden;') which clips horizontally overflowing text instead of allowing it to wrap. The ui.label word-wrap CSS is correct, but parent container's overflow:hidden prevents proper text reflow."
  artifacts:
    - path: "web/pages/browse.py"
      issue: "Line 2728: text container has overflow:hidden that clips wrapped text"
  missing:
    - "Remove overflow:hidden from text container"
    - "Verify width constraint via w-full class is sufficient"
  debug_session: ".planning/debug/web-word-wrap.md"

- truth: "Desktop reading desk shows all fragments with working scrolling"
  status: failed
  reason: "User reported: no scrolling working properly and only one fragment shows up"
  severity: major
  test: 12
  root_cause: "Three interrelated bugs: (1) _browse_rd_render_images() destroys/recreates QScrollArea on every render using deleteLater() which is deferred, causing splitter corruption with phantom widgets. (2) _browse_rd_setup_sync_scroll() calls text_bar.valueChanged.disconnect() which severs ALL connected slots including QTextEdit internal scroll handling. (3) QTextEdit height may not expand properly for multi-fragment HTML content."
  artifacts:
    - path: "genizah_app.py"
      issue: "Line 7756-7771: deleteLater causes splitter corruption on re-render"
    - path: "genizah_app.py"
      issue: "Line 7909: disconnect() removes all valueChanged signals"
  missing:
    - "Create scroll area once, clear/repopulate on re-render instead of recreating"
    - "Store sync handler refs and disconnect specifically"
    - "Ensure proper size policies for multi-fragment content"
  debug_session: ".planning/debug/desktop-multi-fragment-render.md"

- truth: "Desktop synchronized scrolling works bidirectionally"
  status: failed
  reason: "User reported: scrolling is broken"
  severity: major
  test: 13
  root_cause: "Same as test 12 — disconnect() at line 7909 removes ALL valueChanged signals including internal Qt scroll handling. After first sync setup, basic scrolling stops working."
  artifacts:
    - path: "genizah_app.py"
      issue: "Line 7909: text_bar.valueChanged.disconnect() severs all signals"
  missing:
    - "Store handler references for targeted disconnect"
  debug_session: ".planning/debug/desktop-multi-fragment-render.md"

- truth: "Desktop toolbar add shows all fragments, not just first"
  status: failed
  reason: "User reported: only one fragment shows always, the first"
  severity: major
  test: 15
  root_cause: "Same as test 12 — _browse_rd_render_images() recreates scroll area on every render, splitter corruption causes only first fragment's worth of space to be visible."
  artifacts:
    - path: "genizah_app.py"
      issue: "Line 7748-7891: full scroll area recreation on each render"
  missing:
    - "Create scroll area once in enter, repopulate content on re-render"
  debug_session: ".planning/debug/desktop-multi-fragment-render.md"

- truth: "Web navigation from other pages works when reading desk was previously active"
  status: failed
  reason: "User reported: goes to blank browse page from list, entering shelfmark shows previous Reading Desk"
  severity: major
  test: A1
  root_cause: "Reading desk state persists to app.storage.user across ALL navigations (designed for language-switch only). When user navigates from another page with ?sys_id=X, initialization code at lines 3651-3652 restores persisted reading desk state instead of loading the requested sys_id. State only cleared by explicit 'Back to Page View' click."
  artifacts:
    - path: "web/pages/browse.py"
      issue: "Lines 3648-3667: reading desk restore takes priority over URL sys_id"
    - path: "web/pages/browse.py"
      issue: "Line 1060-1072: exit_joined_view only called on explicit button click"
  missing:
    - "Check if initial_sys_id differs from persisted desk entries — if so, clear desk state"
    - "Only restore desk on language-switch (same URL params), not cross-page navigation"
  debug_session: ".planning/debug/web-navigation-stale-state.md"

- truth: "Web toolbar shelfmark add does not throw console errors"
  status: failed
  reason: "User reported: RuntimeError 'parent element this slot belongs to has been deleted' in toolbar_add_by_shelfmark"
  severity: minor
  test: A2
  root_cause: "Stale UI element references when reading desk UI is rebuilt. toolbar_add_by_shelfmark calls ui.notify() but the parent slot was deleted during a re-render cycle. Related to navigation stale state (A1) — UI elements from previous page load are still referenced."
  artifacts:
    - path: "web/pages/browse.py"
      issue: "Line 2236: ui.notify called from stale slot context"
  missing:
    - "Wrap ui.notify in try/except RuntimeError"
    - "Fix root cause in A1 to prevent stale state"
  debug_session: ".planning/debug/web-navigation-stale-state.md"
