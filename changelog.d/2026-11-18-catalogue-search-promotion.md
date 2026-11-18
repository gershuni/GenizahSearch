## [Unreleased] -- "New" beside the catalogue search and Title's Fuzzy search, and three startup tips (web + desktop, branch `claude/catalogue-promotion`)

### Added
- **A one-time "New"** (owner, 2026-11-18), in both apps, as letter-level search and the
  dictionaries were announced: the control highlighted and its label saying it is new, no badge.
  - While the catalogue search is offered, the search-mode list is highlighted, and its Catalog
    item reads "New! Catalog (%)", highlighted in the open list too. The list's tooltip says
    "Search the catalogue information".
  - In Title mode the Fuzzy search switch is highlighted and reads "New! Fuzzy search".
  - Each is shown one time. It is spent on the launch (on the website, the visit) the reader
    first sees it.
  - It goes at once when the reader chooses a mode, works the switch, or uses the feature. Only
    the reader's own acts count: a mode set in code or restored does not.
  - It is never shown where the feature is already in use.
  - Website: `web/components/new_mark.py`, kept per reader; `/reset-hints` forgets it. Desktop:
    `GenizahGUI._update_new_feature_marks`, spent in config.pkl as letter-level search's marker
    is.
- **The Catalog item's tooltip** in both apps: "Search the catalogue information" ("חיפוש במידע
  קטלוגי").
- **Three desktop startup tips**, approved by the owner, right after the strongest five:
  - Catalog mode and its % sign.
  - "Found in the catalogue records".
  - Title's Fuzzy search.

  The two catalogue tips are shown only where the catalogue search is offered (`Tip.requires`).
  Each tip's Show me opens its mode.

### Changed
- **Desktop: Catalog is listed right under Title** in the search-mode list, as on the website
  (it came last, after PGP Tags). Every mode keeps its index, so the search history and the
  steps saved before read as they did (`desktop/catalog_search.py::ModeCombo`).
- The mode-signs tip says "$ for titles, % for the catalogues" (it said "$ for catalog titles",
  which now reads as the Catalog mode).

### Fixed
- **Desktop: PGP Tags could not be left.** The mode hid the whole query row, the mode list with
  it. Now it hides the row's text controls and keeps the list (`_show_row1_for_tags`).
