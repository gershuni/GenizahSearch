## [Unreleased] -- The shelf picker: choose, then act; a details pane; Ctrl+W (desktop, branch `claude/shelf-picker-round3-desktop`)

### Changed
- **A click chooses, a button acts.** In the shelf picker a click on a shelfmark selects it and shows
  it in a new pane beside the columns; the button at the foot takes it: "Open" where the picker opens
  a manuscript (Browse, Go to, Ctrl+W), the window's own words elsewhere. A double-click or Enter is
  the button.
- **Add to List and Add to Puzzle** beside "Open" in the picker that opens manuscripts, for the
  shelfmark chosen or the ones ticked (they stay ticked): the list menu opens over the picker, which
  stays open; Add to Puzzle closes it and adds them to the puzzle, as many as it has room for.
- **Whole volumes** are chosen with "Select all in <name>" above the shelfmarks; the columns have no
  tick boxes (the Exclude picker keeps them).
- **The details pane** shows the shelfmark clicked last: its image when there is one, its other
  shelfmarks, its title, a few lines of its text, and what it has (pages with text, transcriptions,
  joins, comments, catalogue and bibliography entries), with links. A shelfmark's tooltip adds its
  title.
- **The columns** start at the window's edge (the right in Hebrew), each at its own width.
- **Go to's** shelf is an icon button at the end of its box. **Ctrl+W** opens the shelf picker from
  any section, at the manuscript Browse shows.
