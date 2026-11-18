## [Unreleased] -- Title search finds abbreviations (web + desktop, branch `claude/title-search-quotes-bug-92be21`)

### Fixed
- **A title search for an abbreviation found everything but the abbreviation.** The quote mark
  was taken out of the query and not out of the titles, so `רי"ד` was looked for as `ריד`: the
  titles holding `רי"ד` were never found, and every title with those letters inside a longer word
  came back instead. A query typed with a quote mark now finds the titles holding the
  abbreviation with its mark, whichever way the mark is written (`"`, `״`, `'`, `׳`, curly
  quotes); a query typed without one finds them too, along with the plain-letter matches as
  before (`shared/metadata_manager.py::_title_matcher`). Double quotes around the whole query
  are not read as a mark. Shelfmark search is unchanged.
