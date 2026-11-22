## [Unreleased] -- Several from the shelf at once: the Reading Desk, the Puzzle and your lists (web, branch `claude/shelf-picker-web-multi-hosts`)

### Added
- **Several manuscripts from the shelf at once**: tick shelfmarks or whole volumes in the shelf
  picker and add them all.
  - **Reading Desk**: from the desk's toolbar, or from Browse's own "Pick from the shelf" with its
    switch "Open more than one" on: "Open them in the Reading Desk" opens them side by side, with
    the manuscript shown, or, when none is shown, starting with the first one ticked. The desk
    holds up to 30 manuscripts and 1,000 pages, counting what is on it already; one already on the
    desk is not added twice.
  - **Fragment Puzzle**: the shelf button adds fragments onto the canvas, up to 30 on it. An image
    that could not be drawn stays ticked.
  - **Lists**: "Add from the shelf" on a list adds what is ticked to that list, up to 2,000 at a
    time. One the list already holds is not added twice.
- **Limited when ticking, not afterwards**: a shelfmark or volume that would pass the limit is not
  ticked and the picker says why ("Up to 30 manuscripts here", or the desk's pages); a collection
  too big for it is faded. Unticking always works. Excluding from the shelf has no limit: a whole
  collection can still be excluded.
- What is ticked is kept for each purpose and each account until it is added or cleared, and can
  be saved as a list.

### Data and deploy
- No new data. The buttons appear only where the shelf picker does (`SHELFMARK_TREE_ENABLED` and
  `shelfmark_tree_data/shelfmark_tree.db` served).
