## [Unreleased] -- Keyboard: Tab in visual order and Enter searches from the search bar (desktop, branch `fix/desktop-search-bar-keys`)

- **Search tab:** Tab now goes through the search bar in the order it shows (right to
  left in Hebrew, left to right in English): query, search type, Gap, Focus Search,
  Search, corpus, Transcriptions, New, then the second row. It skipped Gap and Focus
  Search, which are inserted into the row after its other controls (Qt's default Tab
  order is the order widgets got their parent). `desktop/widgets/tab_order.py`
  chains a row's controls in layout order.
- **Enter searches** from the Gap and Exclude Words fields, as from the query box.
- **Composition Search:** Tab in the source text moves on to the controls below it;
  it typed a tab character, so nothing after the box could be reached with Tab.
- **Joins Lab:** Tab goes from a line's ⓘ to its word box, its buttons and then the
  next line (it reached the word box last); Enter in "Enter shelfmark…" opens it.
- Tests: `tests/test_desktop_search_bar_keys.py`, real key events in English and Hebrew.
