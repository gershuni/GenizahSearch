## [Unreleased] -- Pick from the shelf: the shelfmarks as a table or as thumbnails (web, branch `claude/shelf-views-web`)

### Added
- **A table and thumbnails in Pick from the shelf.** A switch in the shelfmarks pane's header shows a
  volume's shelfmarks as a list (as before), as a table (shelfmark, title, a snippet of the text, domain,
  pages, printed, PGP and edition, with the search results' badges; one line a row) or as thumbnails of
  each manuscript's first image, small, medium or large, with the start of its title under it. In the table and the
  thumbnails the shelfmarks take the room: only the column of volumes beside them stays, and the breadcrumb goes back. The choice is kept for the next
  time. The details are read a page of shelfmarks at a time, off the event loop; the images load only once
  in sight, four at a time. Ticks, signs, "Select all", the details pane and Open / Add to List / Add to
  Puzzle work in every view. On a phone the table scrolls sideways in its own box and the thumbnails wrap;
  on a small or short screen the details pane keeps a share of its own and the view switch stays in sight.
  The table's columns can be resized (a grip at the end of each heading; kept in this browser, a double
  click restores them); the details pane shows the thumbnail the tiles already have, from the browser's
  cache. The header's, the home page's and Browse's pickers confirm several with "Open".
