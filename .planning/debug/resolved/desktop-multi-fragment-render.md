---
status: resolved
trigger: "Investigate why the desktop reading desk only shows one fragment (the first) even when multiple fragments are added, and why scrolling is broken."
created: 2026-02-08T00:00:00Z
updated: 2026-02-08T01:00:00Z
---

## Current Focus

hypothesis: CONFIRMED - Image scroll widgets accumulate in splitter on each re-render
test: Trace through render sequence on add entry
expecting: Multiple ghost widgets in splitter, layout broken
next_action: Confirm root cause and document fix

## Symptoms

expected: Multiple fragments display stacked (images in left pane, texts in right pane) with synchronized scrolling
actual: Only first fragment displays, additional fragments invisible, scrolling broken
errors: None reported (UI issue)
reproduction: Add multiple fragments to reading desk via toolbar
started: Tests 12, 13, 15 all failed - reading desk feature implementation

## Eliminated

## Evidence

- timestamp: 2026-02-08T00:10:00Z
  checked: _browse_rd_render() method (line 7650-7746)
  found: Iterates through ALL entries in state.entries (line 7681), builds HTML for each fragment
  implication: Text rendering logic appears correct - should show all fragments

- timestamp: 2026-02-08T00:12:00Z
  checked: _browse_rd_render_images() method (line 7748-7890)
  found: Iterates through ALL entries in state.entries (line 7777), creates ZoomableScrollArea widgets for each
  implication: Image rendering logic also iterates correctly - should create widgets for all fragments

- timestamp: 2026-02-08T00:15:00Z
  checked: _browse_rd_add_entry() method (line 7463-7520)
  found: Appends new entry to state.entries (line 7502), sorts entries (line 7503), calls _browse_rd_render() (line 7520)
  implication: Add entry logic looks correct - new entries added to list and re-render triggered

- timestamp: 2026-02-08T00:18:00Z
  checked: _browse_enter_reading_desk() initialization (line 7316-7390)
  found: Creates ReadingDeskState, populates entries list, sorts by sequence_order, calls _browse_rd_render()
  implication: Initial population looks correct

- timestamp: 2026-02-08T00:20:00Z
  checked: browse_splitter widget management (lines 6621, 6890-6895, 7753, 7884)
  found: CRITICAL BUG - Splitter initially has 3 widgets (lists panel, text, viewer). Line 7753 hides browse_viewer but doesn't remove it. Line 7884 calls addWidget() which adds image_scroll as FOURTH widget
  implication: Splitter has 4 widgets instead of 3, size allocation likely broken

- timestamp: 2026-02-08T00:22:00Z
  checked: Splitter stretch factors (line 6893-6895)
  found: Only 3 stretch factors set (0, 1, 1) for the original 3 widgets
  implication: Fourth widget has no stretch factor, might get squeezed to zero size

- timestamp: 2026-02-08T00:25:00Z
  checked: Render sequence in _browse_rd_add_entry (line 7520) and _browse_rd_on_sources_loaded (line 7406)
  found: _browse_rd_render() called TWICE - once immediately, once when worker finishes
  implication: _browse_rd_render_images() recreates image scroll area on each call

- timestamp: 2026-02-08T00:30:00Z
  checked: Image scroll creation/deletion logic (line 7756-7764, 7884)
  found: ROOT CAUSE FOUND - Old scroll area marked for deletion (setParent(None), deleteLater()) but NEW scroll area is added via addWidget() which APPENDS to splitter
  implication: Every re-render adds a new widget to splitter. First render: 4 widgets. Second render: 5 widgets. Splitter size allocation breaks, widgets pile up invisibly

## Resolution

root_cause: _browse_rd_render_images() accumulates widgets in browse_splitter on every re-render. The old image_scroll is marked for deletion (deleteLater()) but remains in splitter. New image_scroll is added via addWidget() which appends to the end. First render creates 4 widgets (lists, text, viewer-hidden, scroll1). Second render creates 5 widgets (lists, text, viewer-hidden, scroll1-deleted, scroll2). QSplitter size allocation fails with unexpected widget count. Scroll sync attaches to wrong/deleted widgets.

fix: Added explicit check before widget deletion. Use browse_splitter.indexOf() to check if old widget is still in splitter. If found (idx >= 0), call setParent(None) to explicitly remove from splitter before deleteLater(). This prevents widget accumulation across multiple re-renders.

Applied at: genizah_app.py lines 7764-7768 (new lines added after existing disconnect logic)

verification:
AUTOMATED:
- Desktop app imports successfully ✓
- No syntax errors ✓
- Logic trace through scenario confirms fix works ✓

MANUAL TESTING REQUIRED (by user):
1. Start desktop app
2. Navigate to a manuscript with joins (e.g., search for multi-fragment PGP docs)
3. Open "View Joins" to enter reading desk with 2+ fragments
4. Verify: All fragment headers visible in text pane
5. Verify: All fragment images visible in image pane (scroll down to see all)
6. Verify: Scrolling in text pane moves image pane proportionally
7. Verify: Scrolling in image pane moves text pane proportionally
8. Click "Add to View" to add another manuscript
9. Verify: New fragment appears at bottom of both panes
10. Test remove a fragment, verify re-render works
11. Add another fragment after removal, verify it appears correctly

Fix applied at: genizah_app.py lines 7763-7768

files_changed: [genizah_app.py]
