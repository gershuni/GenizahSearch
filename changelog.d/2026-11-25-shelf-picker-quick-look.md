## [Unreleased] -- Pick from the shelf: a quick look at a shelfmark's image, filling the window (web + desktop, branch `claude/happy-gates-jd3rzz`)

Asked by a reader who sifts through the shelf by its thumbnails: to read a fragment without leaving
the shelf.

- **Quick look.** Q on a shelfmark (from any list; by the key, so ק on a Hebrew keyboard does the
  same), or a new full-screen button under the details' image, shows that shelfmark's image filling
  the picker (desktop) or the browser window (website). The picture already at hand shows at once
  (the tile's, or the page the details show); then the same image comes sized to the window: asked at
  the width that fills it, in steps of 200 pixels between 400 and 1,600, so a laptop asks about 800
  and a large screen more, never the full-size scan. Zoomed in, it is asked for once more at twice
  that width (up to 2,000).
- **Moving in it.** The arrows go to the previous / next shelfmark of the list under it (mirrored in
  Hebrew), Home / End to the first / last; past the list's ends they go on along the shelf, into the
  next or previous volume or collection (its bar then names that group, and closing takes the picker
  there). Shift or Ctrl + an arrow, Up / Down or Page Up / Page Down go through the manuscript's images
  ("Image 2 of 6"). The next image and the next shelfmark's first image are fetched before they are
  asked for. Its bar says where it is ("12 of 48").
- **The first image first** (desktop): the details and the quick look open on the manuscript's first
  image. The catalogue's thumbnail need not be it (T-S 6 H 2.5 opened on its second); it shows until the
  first comes.
- **A microfilm opens on its middle image** (both apps, the details and the quick look): the National
  Library of Russia's images begin with a START frame and a catalogue card (`opening_image`; three
  images: the third). Its other images are a step away as before.
- **Q on the shelfmark the details show** opens the quick look at the details' image and turn, as their
  button does.
- **Website: never a broken picture while it loads.** The quick look shows the picture at hand blurred
  (the details' page, the tile's, else that page small) with a spinner over it until the sharp one has
  come; with nothing yet, an empty place and the spinner.
- **The National Library of Russia's images on the desktop** (blank in the details, the tiles and the quick
  look, while Browse and the website showed them): NLI gives them only to a request from its own site, and
  the picker sent no `Referer`. Every desktop request to an NLI image host now carries it
  (`shared/nli_fetch.py::nli_headers_for`): also the Join Workbench's thumbnails and the puzzle's images.
- **A refused image is not an NLI outage** (`nli_refused`, both apps): three refusals no longer close NLI off
  for a minute.
- **A tile that did not come is asked for again** (desktop, `NOT_COME`): after 65 seconds, three times at
  most; before, it stayed at "none" until the picker was closed.
- **A microfilm's tile is its middle image**, the one its details open on, so they fetch no second picture.
  Desktop: `_tile_url`. Website: `/api/nli_image_by_sysid/{id}?middle=1` works it out from the image list
  the route reads anyway.
- **Shift + an arrow** from a list is the details' previous / next image too, as Ctrl + an arrow.
- **The keys in the tooltips**: zoom ("Zoom in (+)"), turn ("Rotate left (Shift+R)"), the images'
  and the shelfmarks' arrows, in the details and in the quick look, both apps.
- **Zoom and turn** as in the details: the mouse wheel at the pointer, + / - / 0, a drag once zoomed;
  R / Shift+R turn it a quarter (also from a list, for the details' image). Opened from the details it
  starts at their page and turn; another shelfmark starts upright.
- **Choosing from it.** Space chooses the shelfmark (with several at once: ticks it; its box on the
  bar shows it), Enter opens it as Enter in the list does (one: the picker's own button; several:
  Open). Esc, Q or its close button goes back to the list, at the shelfmark looked at last, chosen as a
  click chooses it, with the keyboard on its row.
- Desktop: the quick look covers the picker's window; Tab, Ctrl+F and F6 stay with it while it is
  open. Website: it is a dialog over the picker, which Esc closes alone; its bar holds the way back and
  the shelfmark at the start, the images in the middle, the tools at the end (two lines on a phone), the
  picture fitted to the place under it.
- Shared: `shared/shelfmark_tree_details.py` (`look_width`, `look_sharp_width`, `image_key`). Tests:
  `tests/test_shelf_picker_desktop_quicklook.py`, `tests/test_shelf_picker_desktop_nli_images.py`,
  `tests/render_smoke/test_shelf_picker_quicklook_render_smoke.py`, `tests/test_shelf_picker_web_keys.py`.
