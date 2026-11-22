## [Unreleased] -- Pick from the shelf: a click chooses, a button opens, and a look before opening (web, branch `claude/shelf-picker-round3-web`)

### Changed
- **A click chooses, a button acts.** In "Pick from the shelf" a click on a shelfmark now selects
  it (shown chosen) instead of opening it at once; the button at the foot of the dialog acts on it:
  "Open" where the pick opens the manuscript (Browse, the header, the home page), the page's own
  words elsewhere ("Load Anchor", "Add shelfmark", "Select"). A double click, or Enter on the
  selected one, is that button.
- **Add to List and Add to Puzzle** beside Open on Browse, the header and the home page, for the
  selected shelfmark or the ticked ones. Add to List opens the list chooser over the picker, which
  stays open; with several ticked it adds them to one of your lists or a new one ("Save as a list"
  is now this "Add to List" everywhere). Add to Puzzle opens the Puzzle with them added (up to its
  30 fragments; the Puzzle takes what fits its canvas and says how many it left out). The ticks stay
  after either: only "Open them in the Reading Desk" takes them.
- **Ticks on shelfmarks only.** With several to choose, a whole volume or collection is taken by
  "Select all in ..." over its shelfmarks; the columns have no boxes. The search's "Exclude
  manuscripts from the shelf" keeps them, since a whole collection is what it is for.
- On Browse a tick is limited by the choice itself (30 manuscripts, 1,000 pages); what the Reading
  Desk already holds is counted when "Open them in the Reading Desk" is pressed.
- The Puzzle takes up to 30 fragments handed over at once (it took 21), from a join too.

### Added
- **A look before opening**: a pane beside the columns (below them on a phone) shows the shelfmark
  clicked last and what the manuscript has, each under its own label so it is easy to spot: its
  image when there is one, the shelfmark and its other shelfmarks, the title, its text page by page
  in a box that scrolls, a line each for the pages, the transcriptions, the catalogue and
  bibliography entries and the comments, the joins known for it, and the links (Ktiv, the library's
  own viewer such as Cambridge Digital Library or Manchester, PGP), right under the shelfmark on a
  phone.
- **Joins in the pane**: a join's shelfmark is a link that takes the dialog to it; its box adds it
  to the shelfmarks chosen, turning "Open more than one" on where it is off.
- **Several at once from the home page and the header too**: their "Pick from the shelf" is Browse's,
  with "Open more than one": the shelfmarks ticked open together in Browse's Reading Desk, and what
  is ticked is the same choice Browse's picker shows.
- **Signs on the shelfmarks**: a PGP badge, a mark for a scholarly transcription and one for a join,
  as in the search's results; the tooltip names them.
- A shelfmark's tooltip has the manuscript's title on a line of its own.
- The library filter finds a library by its codes too ("RNL", "CUL").

### Fixed
- The arrows between the names of a place on the shelf (Browse and the picker) point the way
  Hebrew is read.

### Data and deploy
- No new data. Everything appears only where the shelf picker does (`SHELFMARK_TREE_ENABLED` and
  `shelfmark_tree_data/shelfmark_tree.db` served).
