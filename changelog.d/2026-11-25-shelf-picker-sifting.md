## [Unreleased] -- Pick from the shelf: room for the thumbnails, a manuscript's pages in the pane, its joins at once (web + desktop, branch `claude/happy-gates-jd3rzz`)

Asked by a reader who sifts through the shelf by its thumbnails.

- **The details pane can be hidden.** A button in the picker's header (desktop: "Details"; website:
  its side-panel icon) hides the pane and gives its room to the shelfmarks and their thumbnails;
  pressed again, the pane comes back with the shelfmark clicked last. The choice is kept. On the
  desktop a hidden pane reads nothing (no network call) until it is shown. A phone keeps folding the
  pane under the list as before.
- **Extra large thumbnails**, in the picker, the desktop Join Workbench and the website's Joins Lab
  (one card a row there). The larger sizes ask NLI for sharper pictures: the desktop picker's Large
  at 600 pixels (as the website's has been) and Extra large at 800, the website's Extra large at
  800, the Join Workbench's Extra large at 600. The picture already fetched stays on show until the
  sharper one comes; a smaller answer that comes late never replaces a sharper one. Small and
  Medium ask as before.
- **A manuscript's other images in the pane.** Under the pane's picture, previous / next and where
  it is ("Image 2 of 6") when the manuscript has more than one image; the next one is fetched before
  it is asked for, and the images seen stay at hand (desktop: the image list Browse uses; website:
  the site's own image route by page). On the desktop the thumbnail is the catalogue record's first
  image, which need not be the image list's first: the pane counts it where it is in the list
  ("Image 2 of 3"), and one the list does not name gives way to the list's first image, so a step
  never shows the same picture again.
- **A box before the pane's "Joins"** (with two partners or more on the shelf): it shows whether
  all, some or none of the partners are chosen; a click chooses them all, the multiple choice turned
  on first as one partner's box does, and when all are chosen a click unticks them (the shelfmark
  shown stays); one change, so the limit counts them together.
- **The pane's picture zooms**: + / - under it, or the mouse wheel over it (at the pointer), a drag
  to move around once zoomed, a double click to fit it again. Once zoomed in, the picture on show is
  asked for again at 1,200 pixels, for that page only, and put in place at the same zoom (desktop:
  NLI's image server; website: the site's own image route). Another page opens fitted. The desktop
  pane's picture is a little taller (220 pixels).
- **The pane's image arrows are the style's own arrows** on the desktop: the text arrows (‹ ›) are a
  Unicode mirrored pair and came out both pointing right in Hebrew. As on Browse's previous / next
  manuscript, in Hebrew previous is on the right pointing right and next on the left pointing left.
- **On the website, ticking a join now ticks the shelfmark the pane shows too**, as the desktop
  always has: the fragments of a join are chosen together (it first, then the partner, in one
  change). Unticking a join takes out the partner alone.
- **The keyboard, the same in both apps.** Tab / Shift+Tab go from list to list alone, the filters and
  buttons passed by (desktop: round, from the last to the first; website: past the ends the page's
  order goes on); Tab in a filter goes into its list; F6 / Shift+F6 leave the lists for the buttons and
  the details, and come back to the list last in use. On the website each
  list is now one Tab stop (its rows were each their own, so a column of 200 shelfmarks took 200 Tabs),
  and the arrows, Home / End and Page Up / Page Down move in it, by rows and columns in the thumbnails.
  In a column the arrow forward opens the row and goes into its list once it is drawn, the arrow back
  goes to the list before (from the first, one level up); mirrored in Hebrew. Space chooses (ticks),
  Enter opens, as before. From a list: Ctrl + an arrow is the pane's previous / next image (as its arrows
  on screen), + / - / 0 zoom its picture, 1 / 2 / 3 the shelfmarks as a list, a table or thumbnails,
  [ / ] the thumbnails smaller / larger (by the key, so a Hebrew layout works the same). Ctrl+F the
  filter of the list in use, Esc back to the list (it no longer closes the picker from a filter on the
  website), Alt+D the details pane. Alt+D and Ctrl+F go by the key, so they work on a Hebrew
  keyboard too (on the desktop they matched the letter, and Alt+D there is Alt+ג; both now answer
  the key on the application while the picker is in front, as the main window's own keys do).
- **Website, as on the desktop:** the keyboard starts in a list when the picker opens (the list of the
  next choice: the column none of whose rows is open, else the shelfmarks), and an arrow pressed while
  it is nowhere goes there; a column's row opened (by a click, Enter or Space) takes the keyboard into
  the list it opens; moving through the shelfmarks with the arrows selects the one it rests on (the
  pane shows it, Open takes it), as a click does; the column row the keyboard is on looks chosen. Desktop: a table's Tab no longer walks its cells.
- **Website: the next image's prefetch** no longer raised in the server's console (it ran from a
  background task without a page); the next image is now asked for ahead, as intended.
- **Several at once by the keyboard** (both apps): S turns Open more than one on or off; then Space
  ticks and Enter is the Open button (it used to tick like Space): what is ticked opens, and with
  nothing ticked the row Enter was pressed on first.
- **W opens Pick from the shelf on any web page** (outside a box you type in; the desktop's Ctrl+W,
  which a browser keeps for closing its tab): Browse's own button, at the manuscript shown, else the
  header's. On the keyboard card.
- **The picture turns a quarter** (both apps): rotate left / right beside the zoom; another page of
  the manuscript keeps the turn, another shelfmark starts upright.
- **Website: the row the keyboard is on is framed inside it.** The site's focus ring is drawn outside
  the element, and in a list that scrolls it showed as green lines on the neighbouring rows' edges.
- Shared: `shared/shelfmark_tree_rows.py` (`SIZES` with `xl`, `DETAILS_KEY`, `details_shown`),
  `shared/shelfmark_tree_details.py` (`sized_image_url`, `thumbnail_url(width=)`, `page_image_urls`).
  Tests: `tests/test_shelfmark_tree_round5.py`, `tests/test_shelf_picker_desktop_round5.py`,
  `tests/render_smoke/test_shelf_picker_round5_render_smoke.py`.
