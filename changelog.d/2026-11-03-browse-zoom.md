## [Unreleased] -- Zoom on the manuscript page answers at once (web, branch `claude/browse-zoom`)

### Changed
- **Zooming the image on `/browse`**: the + and − buttons, the + / − keys and
  the mouse wheel now zoom in the browser itself, with no round-trip to the
  server, and each step multiplies the level by 1.5 (100% → 150% → 225% → 338%
  → 506% → 759% → 800%) instead of adding 25%; the range is 25%–800%. The
  wheel zooms at the pointer; a double-click (double-tap on a phone) zooms to
  200% at that point and, from any zoomed level, back to the whole page.
  PostHog's 30 days to 2026-11-03 held 50 rage clicks by 19 readers on the +
  button (9 by 8 on −, 4 by 3 on reset), the only real rage-click target on
  the site: each click added a linear 0.25 and went through the server.
- The page still learns the level (`browse_zoom` event, `web/pages/browse_state.py::clamp_zoom`),
  so a page it rebuilds (Prev/Next, Show Metadata, Edit) draws the image at the
  zoom the reader sees; a rotation never pushes a scale back (`setRotation`).
  Where the toolbar's percentage is hidden (a phone), a badge over the image
  shows the level for a moment after each change. The fullscreen editor's
  image uses the same shared viewer (`createManuscriptViewer`) instead of an
  inline copy with its own wheel step, so it pinches and double-taps too.
- `web/static/manuscript_viewer.js` (shared with `/search` and the Compare
  panes): `zoomBy(factor, x, y)`, `fit()`, `toggleZoomAt()`, `setRotation()`,
  `onChange` option, `MAX_ZOOM` 8; `zoomStep` and the 0.25 steps are gone.
  `/browse` stamps the script URL with the file's hash so a page built after a
  deploy never runs an hour-old cached viewer.

### Tests
- `tests/test_browse_zoom_steps.py` (the step arithmetic, the clamps, and the
  JS and Python constants in step), `tests/test_manuscript_viewer_js.py`
  (`tests/js/manuscript_viewer_zoom_check.js` under node: anchoring, wheel,
  double-click/tap, pinch, debounced reporting, take-over of a rebuilt image),
  `tests/render_smoke/test_browse_zoom_render_smoke.py` (the buttons carry no
  server click handler; the event sets the state; a rebuilt page keeps it).
