## [Unreleased] -- Pick from the shelf: the shelfmarks as a table or as thumbnails (desktop, branch `claude/shelf-views-desktop`)

### Added
- **A table and thumbnails in Pick from the shelf.** In the shelfmarks pane's header, List / Table /
  Thumbnails: the table has the search results table's columns (shelfmark, title, snippet, domain, pages,
  printed, PGP, edition) and its badges, every column in sight; the thumbnails show each manuscript's first
  image, small, medium or large, as in the Join Workbench, with the start of its title under it. In the table and the thumbnails the shelfmarks take the room: only the
  column of volumes beside them stays, and the breadcrumb goes back. The choice is kept in the app's
  settings. The table's details are read a page at a time off the UI thread; only the thumbnails in sight
  are fetched, three at a time, and closing the picker waits for none of them. Ticks, signs, "Select all",
  the details pane, Open, a double click, Enter and the foot's buttons work in every view. The table's
  columns can be resized (kept while the app runs). A thumbnail is fetched once: the details pane and the
  gallery share it, so clicking the same shelfmark again shows its picture at once. Opened at a
  manuscript, the picker shows it in the details pane; Browse's picker confirms several with "Open".

### Fixed
- A thumbnail answered by NLI with something that is not an image (an error page) no longer counts as a
  success for the NLI circuit breaker.
