## [Unreleased] -- A join of several fragments from the shelf (web, branch `claude/shelf-picker-web-joins-several`)

### Added
- **A join of several fragments**: in the new-join form, "Pick from the shelf" now takes one
  fragment or several (a join may hold many). Each one ticked is joined to the fragment shown, with
  the same relationship and notes, and all are saved together: either every join is made or none
  is. One can be taken out before saving. The fragment itself is left out, and at most 30 are
  taken at a time.

### Data and deploy
- No new data. The button appears only where the shelf picker does (`SHELFMARK_TREE_ENABLED` and
  `shelfmark_tree_data/shelfmark_tree.db` served).
