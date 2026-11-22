## [Unreleased] -- Reading Desk: "Puzzle images", several puzzles at once (web, branch `claude/desk-puzzle-images`)

### Changed
- **The Reading Desk's third image view is "Puzzle images" ("תמונות פאזל"), not "Assembled".**
  One join of fragments may have several puzzles -- its recto and its verso, or one for each page
  of a document on several leaves -- and the view showed one of them at a time, chosen from a list.
  It now shows them one under another, each with its own image controls and "Open in Puzzle"
  (`web/pages/browse_desk_puzzle.py::render_join_view`). The first four found are shown (each of
  the reader's own puzzles is composed by the server when shown); a list with chips chooses which,
  and the choice is kept with the desk. "New Puzzle" beside it takes the desk's fragments to the
  Puzzle to arrange another one (another page, or the other side). The desk's image count is not
  shown over the puzzles. The view's other texts say "puzzle" too ("No puzzle of these fragments
  yet.", "Preparing the puzzle image…").

### Tests
- `tests/render_smoke/test_reading_desk_document_render_smoke.py`: two puzzles found are both
  shown, each its own viewer, controls and "Open in Puzzle"; one taken off leaves the other alone
  and is kept with the desk; all taken off shows none; `shown_joins` (the first four, else the
  reader's choice in the order found) and each picture in its own viewer.
