## [Unreleased] -- The shelf tree: every manuscript on its library's shelf (web + desktop, branch `claude/shelfmark-tree-data`)

### Added
- **The data and the reader behind "Pick from the shelf"** (nothing is shown yet; the picker and
  Browse's previous / next on the shelf come in the following PRs).
  `scripts/build_shelfmark_tree.py` builds `shelfmark_tree_data/shelfmark_tree.db` from the FGP
  dump and `libraries.csv`: library > collection > volume > shelfmark, in FGP's order, with range
  nodes over any node of more than 1,000 shelfmarks and an "Other shelfmarks" node per library.
  FGP places 253,089 of our 255,723 manuscripts; the other 2,634 are placed by fixed rules (155 by
  an exact shelfmark, 1,735 by the collection their shelfmark names, 46 as the leftovers of a
  single-collection library, 698 in "Other shelfmarks"). A manuscript is never placed by a
  collector's library code, nor when its catalogue form names another institution.
  `scripts/verify_shelfmark_tree.py` gates the file and fails rather than skips;
  `shared/shelfmark_tree_service.py` reads it (children, paged leaves with a filter, every place of
  a manuscript, previous / next across volumes and collections, everything under a node, where a
  typed shelfmark starts). Oxford leaves carry their Neubauer Part as information.
