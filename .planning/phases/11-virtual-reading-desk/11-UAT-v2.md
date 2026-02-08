---
status: diagnosed
phase: 11-virtual-reading-desk
source: 11-01-SUMMARY.md, 11-02-SUMMARY.md, 11-03-SUMMARY.md, 11-04-SUMMARY.md, 11-06-SUMMARY.md, 11-07-SUMMARY.md, 11-08-SUMMARY.md, 11-09-SUMMARY.md, 11-10-SUMMARY.md, 11-11-SUMMARY.md
started: 2026-02-08T18:00:00Z
updated: 2026-02-08T18:30:00Z
---

## Current Test

[testing complete]

## Tests

### 1. Web - Dual pane rendering with joined document
expected: Open a joined document in web browse, click "View All" joins link. Both panes show all fragments stacked — images left, texts right. Each fragment has its own image controls and shelfmark header.
result: pass

### 2. Web - Synchronized scrolling
expected: In reading desk with multiple fragments, scroll past a fragment boundary in one pane. The other pane auto-scrolls to the matching fragment header.
result: pass

### 3. Web - Per-fragment version selector
expected: Each fragment in the text pane has a version dropdown. Changing the version updates only that fragment's text (e.g., switching from edition to translation).
result: issue
reported: "RuntimeError: The parent slot of the element has been deleted — repeated crash on version change"
severity: blocker

### 4. Web - Add to View button
expected: When viewing a single manuscript, an "Add to View" button (library_add icon) appears in the header. Clicking it starts the reading desk with that manuscript.
result: pass

### 5. Web - Toolbar shelfmark add
expected: In reading desk mode, a toolbar appears with a shelfmark text input. Typing a shelfmark and pressing Enter or clicking Add adds that manuscript to the reading desk.
result: pass

### 6. Web - Add from List dialog with per-manuscript checkboxes
expected: Clicking "Add from List" opens a dialog showing personal lists. Clicking a list expands it to show individual manuscripts with checkboxes. Can select specific manuscripts and click "Add Selected".
result: pass

### 7. Web - Remove fragment
expected: Each fragment header has a small X button. Clicking it removes that fragment from both panes. Removing all fragments exits reading desk mode.
result: pass

### 8. Web - Light Mode visibility
expected: Switch to Light Mode. "Back to Page View" button, header icons, and fragment count badge are all visible (not invisible white-on-white).
result: issue
reported: "white-on-white"
severity: cosmetic

### 9. Web - Language switch preserves state
expected: While in reading desk, switch language (Hebrew/English). The reading desk state is preserved — same fragments, same view.
result: issue
reported: "lost"
severity: major

### 10. Web - Word wrap in text pane
expected: Long text lines in the reading desk text pane wrap properly instead of overflowing or requiring horizontal scroll.
result: issue
reported: "works in goitein's text, not the other texts weirdly"
severity: minor

### 11. Desktop - Dual pane rendering with Add to View
expected: In desktop browse tab, load a manuscript, click "Add to View" (near Go button). Reading desk activates with images on right, text on left. Both panes show the manuscript.
result: issue
reported: "Activated with only 1 fragment (2 pages). Add to View should add the typed shelfmark to the current one, not just show current manuscript alone."
severity: major

### 12. Desktop - Add second manuscript via Add to View
expected: While in reading desk, navigate to a different manuscript and click "Add to View" again. Both fragments now appear stacked in both panes (images stacked right, texts stacked left).
result: issue
reported: "No, only one fragment shown — second manuscript not added"
severity: major

### 13. Desktop - Joins "Open in Reading Desk"
expected: For a joined document, the joins dropdown includes "Open in Reading Desk". Clicking it opens ALL connected fragments in the reading desk (not just one).
result: skipped
reason: Joins not yet imported to desktop (Phase 12 scope)

### 14. Desktop - Add from list
expected: While in reading desk, click "Add from List" in the green toolbar. Select items from your lists. They are added to the reading desk — both image and text panes show the new fragments alongside existing ones.
result: issue
reported: "added but the image pane gone"
severity: blocker

### 15. Desktop - Synchronized scrolling
expected: With multiple fragments in reading desk, scroll the text pane. The image pane scrolls proportionally to stay in sync (and vice versa).
result: pass

### 16. Desktop - Per-fragment version selector
expected: Each fragment in the text pane has a [change version] link. Clicking it opens a dialog to choose edition/translation. Selection updates that fragment's text.
result: skipped
reason: Unable to test — blocked by image pane disappearing issue

### 17. Desktop - Remove fragment
expected: Each fragment has a [remove] link. Clicking it removes the fragment from both panes. Removing all fragments exits reading desk mode.
result: pass

### 18. Desktop - Per-image controls
expected: Each image in the reading desk has zoom in/out and rotate buttons. Controls affect only that specific image.
result: pass
note: Scroll-to-zoom interferes with scrolling between images — consider disabling mouse wheel zoom in reading desk mode

## Summary

total: 18
passed: 9
issues: 7
pending: 0
skipped: 2

## Gaps

- truth: "Version selector updates only affected fragment's text without errors"
  status: failed
  reason: "User reported: RuntimeError: The parent slot of the element has been deleted — repeated crash on version change"
  severity: blocker
  test: 3
  root_cause: "ui.timer elements created by version_selector.py:185, notes_display.py:416, joins_panel.py:291 survive content_container.clear() as pending asyncio tasks. Timer's _run_once() accesses parent_slot weakref before checking _deleted, raising RuntimeError when parent is GC'd."
  artifacts:
    - path: "web/components/version_selector.py"
      line: 185
      issue: "ui.timer(0.1, once=True) created inside content_container subtree"
    - path: "web/components/notes_display.py"
      line: 416
      issue: "ui.timer(0.1, once=True) created inside content_container subtree"
    - path: "web/components/joins_panel.py"
      line: 291
      issue: "ui.timer(0.1, once=True) created inside content_container subtree"
  missing:
    - "Cancel pending timer tasks before content_container.clear(), or wrap timer callbacks with try/except for RuntimeError"
  debug_session: ".planning/debug/reading-desk-timer-runtime-error.md"

- truth: "Back to Page View button, header icons, and badge visible in Light Mode"
  status: failed
  reason: "User reported: white-on-white"
  severity: cosmetic
  test: 8
  root_cause: "Global CSS rule '.q-card { background: var(--bg-card) !important; }' in web/main.py:596 overrides the reading desk header card's inline green gradient to white in Light Mode. The 11-08 fix addressed text color but not background override."
  artifacts:
    - path: "web/main.py"
      line: 596
      issue: ".q-card !important background overrides inline style"
    - path: "web/pages/browse.py"
      line: 2426
      issue: "Card inline background lacks !important"
  missing:
    - "Add !important to card background, OR use a non-card element (ui.row) for header bar, OR apply gradient on child row inside card"
  debug_session: ".planning/debug/light-mode-reading-desk.md"

- truth: "Language switch preserves reading desk state"
  status: failed
  reason: "User reported: lost"
  severity: major
  test: 9
  root_cause: "update_content() guard at browse.py:1665 returns early when state.current_page is None, showing welcome prompt. enter_joined_view() restores state.view_joined and reading_desk_entries but never sets state.current_page. On reload, BrowseState is fresh with current_page=None, blocking the elif state.view_joined branch at line 2198."
  artifacts:
    - path: "web/pages/browse.py"
      line: 1665
      issue: "Guard 'if not state.current_page: return' blocks reading desk rendering on restore"
    - path: "web/pages/browse.py"
      line: 1024
      issue: "enter_joined_view() never sets state.current_page"
  missing:
    - "Change guard to 'if not state.current_page and not state.view_joined: return'"
  debug_session: ".planning/debug/reading-desk-lang-switch.md"

- truth: "Word wrap works for all texts in reading desk"
  status: failed
  reason: "User reported: works in goitein's text, not the other texts weirdly"
  severity: minor
  test: 10
  root_cause: "Right pane card (browse.py:2610) uses 'flex: 1 1 auto' without 'min-width: 0'. CSS flexbox default min-width:auto prevents flex items from shrinking below content's intrinsic width. Goitein text wraps due to natural newlines (pre-wrap), not container constraint. Other texts with long unbroken lines expand the container instead of wrapping."
  artifacts:
    - path: "web/pages/browse.py"
      line: 2610
      issue: "Right pane card missing min-width:0"
    - path: "web/pages/browse.py"
      line: 2774
      issue: "Text container column missing min-width:0"
  missing:
    - "Add min-width:0 to right pane card (line 2610) and text container column (line 2774)"
  debug_session: ".planning/debug/reading-desk-word-wrap-inconsistent.md"

- truth: "Add to View adds typed shelfmark alongside current manuscript in reading desk"
  status: failed
  reason: "User reported: Activated with only 1 fragment (2 pages). Should add typed shelfmark to current one."
  severity: major
  test: 11
  root_cause: "browse_load() (line 16021) does not guard against reading desk mode. When user navigates to new manuscript via Go, browse_load() overwrites browse_text with single-page HTML (line 16031/16379), clears browse_viewer images (line 16032), destroying reading desk visual rendering without resetting browse_reading_desk_active. Second Add to View attempts _browse_rd_add_entry but the visual state is already corrupted."
  artifacts:
    - path: "genizah_app.py"
      line: 16021
      issue: "browse_load() has no reading desk guard"
    - path: "genizah_app.py"
      line: 7071
      issue: "on_browse_enriched_loaded() has no reading desk guard"
  missing:
    - "Guard browse_load() to not overwrite text/image panes when reading desk is active, OR intercept navigation during reading desk mode"
  debug_session: ".planning/debug/reading-desk-add-to-view.md"

- truth: "Second manuscript added via Add to View appears stacked in both panes"
  status: failed
  reason: "User reported: No, only one fragment shown — second manuscript not added"
  severity: major
  test: 12
  root_cause: "Same as Test 11 — browse_load() destroys reading desk visual state during navigation between Add to View clicks. See debug session for Test 11."
  artifacts:
    - path: "genizah_app.py"
      line: 16021
      issue: "browse_load() has no reading desk guard"
  missing:
    - "Same fix as Test 11"
  debug_session: ".planning/debug/reading-desk-add-to-view.md"

- truth: "Adding from list shows new fragments in both image and text panes"
  status: failed
  reason: "User reported: added but the image pane gone"
  severity: blocker
  test: 14
  root_cause: "browse_set_lists_panel_visible (line 6939) computes splitter sizes as 3-element list [lists, text, viewer]. During reading desk mode, splitter has 4 widgets (lists_panel, text_widget, browse_viewer, rd_image_scroll). setSizes with 3 elements on 4-widget splitter collapses 4th widget (rd_image_scroll) to 0 width."
  artifacts:
    - path: "genizah_app.py"
      line: 6939
      issue: "browse_set_lists_panel_visible computes 3-element sizes for 4-widget splitter"
  missing:
    - "Make browse_set_lists_panel_visible reading-desk-aware: compute 4-element sizes when rd_image_scroll is present"
  debug_session: ".planning/debug/reading-desk-image-pane-vanish.md"
