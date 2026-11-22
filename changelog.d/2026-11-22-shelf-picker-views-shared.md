## [Unreleased] -- Pick from the shelf: the details of a page of shelfmarks for a table and thumbnails (web + desktop, branch `claude/shelf-views-shared`)

### Added
- `shared/shelfmark_tree_rows.py`: what both apps' shelf pickers need to show a node's shelfmarks as a
  table or as thumbnails as well as a list. `row_details` answers a page of shelfmarks at once (title, a
  snippet of the text, domains, pages, printed, PGP, edition and the image's address where the app names it
  without the network), each source in one batch, off the UI thread; the stored choice of view and
  thumbnail size is read safely; the layout rule (the table and the thumbnails keep only the last column of
  volumes or collections) is one function both apps call. Strings for the view switch.
