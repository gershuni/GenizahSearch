## [Unreleased] -- Reading Desk: switching the image view keeps the page where it was (web, branch `claude/desk-view-keeps-scroll`)

### Fixed
- **Switching the Reading Desk between "One at a time", "All images" and "Puzzle images" took the
  page to its top.** The switch redraws the desk, and the redraw destroyed the toggle a mouse click
  had left the focus in; as the focused element left the page the browser scrolled to the top
  (measured in the local app: 250 px to 0, about a second after the click, with no script asking
  for it; a click that left no focus did not jump). The controls that redraw the desk (both view
  toggles, a fragment's up, down and remove) now let go of the focus as they are clicked
  (`web/static/reading_desk_place.js`, class `rd-redraws`).
- **The two panes of "All images" moved the page as they followed each other.** They scrolled with
  `scrollIntoView`, which scrolls the page too, and followed the observers' first report, made as
  soon as they were set up: every redraw brought the top of the text side into view. Each pane now
  scrolls on its own, and the first report is not followed (`rdSyncPanes`, moved out of
  `web/pages/browse.py`).

### Tests
- `tests/test_reading_desk_place_js.py` runs `tests/js/reading_desk_place_check.js` under node
  against the real script with a small fake DOM: focus let go on a click in a redrawing control
  (mouse or keyboard), kept elsewhere; the panes follow each other by their own scroll, never the
  page's, and not on the first report. The browse page loads the script, and every control that
  redraws the desk carries `rd-redraws` (render-smoke). Mutations of each part fail a test.
