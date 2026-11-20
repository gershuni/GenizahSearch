## [Unreleased] -- The image stays full screen when you turn the page (web, branch `claude/fullscreen-image-browse-arrows-5622e9`)

### Fixed
- **On /browse, the arrow keys left the image's full screen instead of showing the next folio in
  it.** The full screen (F) was a class the browser put on the image card, and a page turn rebuilds
  the page, card included, so the next folio opened out of it. The page now holds the state
  (`BrowseState.image_fullscreen`) and draws the card with it, so Prev/Next, the arrow keys and any
  other rebuild (Show Metadata, Edit) keep it. Esc leaves through the Fullscreen button, so the
  state follows; a page with no image ends it. The unused `toggle_fullscreen` and
  `BrowseState.is_fullscreen` are gone.
- **Each page turn in full screen flickered.** The page drew the 400 px preview first, which showed
  at its own size, small in the middle of the screen, before the full image filled it (for seconds
  on a first visit); and the page dimmed while the next folio loaded, the full-screen image with it.
  In full screen the image is now drawn full size at once (its spinner shows the wait; the
  viewer's upgrade step, `progressiveLoad`, now compares the `src` as written, so it no longer
  fetches that same full image a second time), and
  `.browse-loading` no longer dims a page that holds `.fullscreen-mode` (`common.css`). Out of full
  screen both are as before.
- **In full screen a tall folio ran off the bottom of the screen.** The image box was 100vh tall
  under the toolbar, and the image's `max-height: 100%` had nothing to resolve against (ui.html
  wraps the `<img>` in a div of auto height): a 2000x3179 folio drew 1400x2225 in a 1400x900
  window. The card also kept its white padding. The card is now a column over the whole window
  (`inset: 0`, no padding, black, no fade-in), the image box takes what the toolbar leaves, and
  the wrapper is `display: contents`, so the folio fits (500x795 there; at phone width too). Zoom
  works from the fitted size.

### Tests
- `tests/render_smoke/test_browse_image_fullscreen_render_smoke.py`: full screen survives Next and
  stays off after leaving it, drawn without the preview while in it; Esc clicks the Fullscreen
  button. The first fails with the class line removed, and with the preview kept in full screen.
- `tests/test_browse_rebuilds_per_page_change.py` pins the new `.browse-loading` rules; the
  full-screen file pins the fit rules (the layout itself was measured in a browser).
