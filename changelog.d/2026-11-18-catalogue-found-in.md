## [Unreleased] -- Where a catalogue search found a manuscript (web + desktop, branch `claude/catalogue-found-in`)

### Added
- **Found in the catalogue records**: a manuscript opened from a catalogue search's result shows
  the catalogue records the search matched, whole, each under its source line ("FJMS Catalog ·
  Note"), the searched words marked. The records come from the catalogue search's own file, so a
  hit in a field no catalogue panel shows (an FJMS note or colophon, a mention's description, a
  record from a general source, an automatic translation) is shown too. Where: the website's
  Quick View (first) and Browse (under the header, for the manuscript the link opened); the
  desktop viewer (the lower pane beside the text) and the Browse tab (a box under the header).
  The links from a catalogue result to Browse carry the search (`&cq=`, `&cnear=1`, `&ctr=0`),
  not a transcription phrase.

### Changed
- A catalogue result row carries the search that found it (`catalogue.searched`: the query and
  the two switches, one object shared by every row), so a restored session or history entry
  still shows where.
