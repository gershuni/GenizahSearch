## [Unreleased] -- The shelf picker: labelled details, signs on the shelfmarks, several from Go to (desktop, branch `claude/shelf-picker-round3-desktop`)

### Fixed
- **The list of shelfmarks no longer jumps.** In some collections (the Schøyen collection among them)
  the shelfmarks moved between two and three columns without end; they now stay put.
- **No blank line above the shelfmarks** (T-S C3, T-S C4 and many others), and their rows are as tall
  as the rows of the columns beside them.
- **Opening several brings Browse to the front.** Ctrl+W from another section, several shelfmarks
  ticked and opened: the window now goes to Browse, where they open, as it does for one.
- **The library's own viewer is linked in the details pane** (Cambridge Digital Library, Manchester
  and the others): once a manuscript had been opened in Browse, the pane lost that link.
- **Tick boxes show on a dark theme** before they are ticked: the boxes of the shelfmarks, and those
  of the collections in "Exclude manuscripts from the shelf".

### Changed
- **Go to (Ctrl+G)** opens the same shelf picker as Browse: its switch "Open more than one" ticks
  several shelfmarks and opens them all in the Reading Desk.
- **The details pane is labelled fields**: the shelfmark and its other shelfmarks, the title, the text
  of its first pages as stored (in a box that scrolls, each page under its number), the pages and how
  many have text, the catalogue and bibliography entries, the scholarly transcriptions, the comments,
  the date and subjects, then its joins and its links (Ktiv, Cambridge Digital Library, Manchester,
  Princeton and the others).
- **Joins in the pane**: each joined fragment's shelfmark goes to it on the shelf; its box adds it to
  the shelfmarks chosen together with the shelfmark shown, turning on "Open more than one" when it is
  off.
- **Signs on the shelfmarks**: a green "PGP" where the Princeton Geniza Project knows the fragment (as
  in the search results), a document icon for a scholarly transcription and a link mark for a join;
  the tooltip names them.
