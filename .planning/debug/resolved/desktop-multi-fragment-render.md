---
status: resolved
trigger: "Desktop reading desk only shows one fragment, scrolling broken"
created: 2026-02-08T00:00:00Z
updated: 2026-02-08T01:00:00Z
---

## Current Focus

hypothesis: CONFIRMED - Multiple interrelated issues with desktop reading desk rendering
test: Code analysis of _browse_rd_render, _browse_rd_render_images, _browse_rd_setup_sync_scroll
expecting: Issues with splitter layout, scroll area recreation, and signal disconnection
next_action: Document root causes

## Symptoms

expected: Multiple fragments displayed with working scroll sync
actual: Only first fragment shows, scrolling broken
errors: None reported (visual/functional issues)
reproduction: Add to View with multiple fragments or use toolbar to add more

## Evidence

- timestamp: 2026-02-08T00:30:00Z
  checked: _browse_rd_render (line 7650-7746)
  found: |
    Text pane renders correctly — loops ALL state.entries and builds HTML string, calls
    self.browse_text.setHtml(full_html). This should display all fragments in the text pane.
    If only one shows, the issue is either:
    1. state.entries only has one entry (add_entry duplicate check?)
    2. QTextEdit not scrollable (height issue)

- timestamp: 2026-02-08T00:35:00Z
  checked: _browse_rd_render_images (line 7748-7897)
  found: |
    IMAGE PANE: Key issues identified:

    1. Every call to _browse_rd_render_images destroys and recreates the scroll area.
       Line 7756-7771: if scroll exists, disconnect signals, setParent(None), deleteLater()
       Line 7773: create new QScrollArea
       Line 7891: self.browse_splitter.addWidget(self._browse_rd_image_scroll)

       PROBLEM: deleteLater() is DEFERRED — the old widget is still in the splitter when
       addWidget is called. So the splitter now has 4+ widgets (list, text, old_scroll, new_scroll)
       instead of 3 (list, text, scroll). The splitter distributes space among all visible widgets.

    2. setParent(None) at line 7768 should remove from splitter, BUT it's conditional on
       indexOf returning >= 0. If the widget was already removed or the index check fails,
       the old widget stays in the splitter.

    3. Line 7770: setVisible(False) on old scroll — this hides it but it's still in the layout
       taking up space until deleteLater runs.

- timestamp: 2026-02-08T00:40:00Z
  checked: _browse_rd_setup_sync_scroll (line 7899-7940)
  found: |
    SCROLL SYNC: Critical bug:

    Line 7909: text_bar.valueChanged.disconnect()
    Line 7913: image_bar.valueChanged.disconnect()

    These disconnect ALL signals from valueChanged, not just our sync handlers.
    QTextEdit's vertical scroll bar may have other internal connections that get severed.
    This explains "scrolling doesn't work" — basic scroll functionality is broken.

    FIX: Should use specific disconnect: text_bar.valueChanged.disconnect(handler)
    Or store the lambda/slot reference for targeted disconnection.

- timestamp: 2026-02-08T00:45:00Z
  checked: _browse_add_to_view (Add to View entry point)
  found: |
    When "Add to View" is clicked for first manuscript, it calls
    _browse_enter_reading_desk([{current}]). This creates the state with 1 entry.

    When toolbar "Add to Desk" adds another, it calls _browse_rd_add_entry().
    This appends to state.entries, then calls _browse_rd_render() which calls
    _browse_rd_render_images() again — which recreates the scroll area.

    The recreation cycle (destroy old + create new) is the core problem.

## Resolution

root_cause: |
  THREE interrelated bugs in desktop reading desk:

  **Bug 1: Image scroll area recreation causes splitter corruption**
  _browse_rd_render_images() destroys and recreates the QScrollArea on every render.
  deleteLater() is deferred, so old widget is still in splitter when new one is added.
  Result: splitter accumulates phantom widgets, layout breaks.

  **Bug 2: disconnect() severs all valueChanged signals**
  _browse_rd_setup_sync_scroll() calls text_bar.valueChanged.disconnect() which removes
  ALL connected slots (not just the sync handler). This breaks QTextEdit's internal
  scroll handling. After first sync setup, basic scrolling stops working.

  **Bug 3: QTextEdit height not expanding for multi-fragment content**
  The text pane (browse_text QTextEdit) may not properly expand to show all fragments
  if it's inside a QSplitter without proper size policies. The HTML is all there but
  the visible area shows only the first fragment worth of content.

fix: |
  **Fix 1:** Don't recreate QScrollArea on every render. Instead:
  - Create it once in _browse_enter_reading_desk()
  - Clear and repopulate its layout in _browse_rd_render_images()
  - Only destroy in _browse_exit_reading_desk()

  **Fix 2:** Store sync handler references and disconnect specifically:
  ```python
  self._rd_text_sync_handler = lambda val: ...
  self._rd_image_sync_handler = lambda val: ...
  # Disconnect only our handlers:
  text_bar.valueChanged.disconnect(self._rd_text_sync_handler)
  ```

  **Fix 3:** Ensure browse_text has proper size policy for expanding content,
  or use QTextEdit.document().adjustSize() after setHtml().

verification: |
  1. Add manuscript via "Add to View"
  2. Add second manuscript via toolbar
  3. Both fragments should display in both panes
  4. Scrolling should work in both panes independently
  5. Scroll sync should work bidirectionally

files_changed: [genizah_app.py]
