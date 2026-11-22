## [Unreleased] -- Pick from the shelf in the Puzzle, the Reading Desk, the Joins Lab, a new join and the home page (web, branch `claude/shelf-picker-web-hosts`)

### Added
- **Pick from the shelf wherever a page asks for one manuscript**: beside the Fragment Puzzle's
  shelfmark box, in the Reading Desk's toolbar, beside the Joins Lab's Load Anchor, above the
  choices for a new join's second fragment (new there: that form had no box to type in), under
  the home page's search box, and beside the go-to box in the header on every page (on a phone,
  once the search is opened). The picked manuscript goes where a typed shelfmark goes. The picker
  opens again where the last pick was, so adding several fragments walks along the shelf; a new
  join's picker opens at the fragment being joined, and refuses that fragment itself.
- From the home page the pick opens in Browse at the place it was picked at; Browse keeps a place
  other than a manuscript's first in its address (`/browse?sys_id=..&place=..`), so a reload or a
  shared link opens there again.

### Data and deploy
- No new data. The buttons appear only where `/browse`'s "Pick from the shelf" does
  (`SHELFMARK_TREE_ENABLED` and `shelfmark_tree_data/shelfmark_tree.db` served).