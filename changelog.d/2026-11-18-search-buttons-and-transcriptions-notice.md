## [Unreleased] -- Search page: clearer buttons, and a search of one kind of transcription says so (web, branch `claude/website-buttons-search-clarity-f34d99`)

### Fixed
- **Buttons that looked disabled on the dark theme.** Advanced Options, Search only in..., the
  row-density toggle and the sections of the criteria panel were Quasar's fixed grey while off,
  which reads as a disabled button on the dark theme. Off now takes the theme's secondary text
  colour, as the result filter chips already did (`web/components/toggle_button.py`,
  `.q-btn.btn-off` in `common.css`); on stays the primary colour.

### Changed
- **A search of the scholarly or the automatic transcriptions only says so.** While the
  Transcriptions selector is not on All it is framed and tinted in the primary colour, its value
  in bold; the results of such a search carry a line over them -- "Search restricted to the
  scholarly transcriptions (FGP and PGP)" or "... to the automatic transcriptions" -- with a
  "Search all transcriptions" button that sets the selector to All and runs the search again
  (`web/pages/search_editions.py::sources_notice`). The line follows the shown search: a restored
  search shows its own, and New Search clears it.
- **Advanced Options and Search only in... are icon buttons**, named by their tooltips (and
  aria-labels); the active-filter count stays on the second.
- **Filter Results and Export left the "More actions" menu**, where they were hard to find: the
  results header has a Filter Results button (on while its panel is open) and an Export button
  whose menu holds Word, Excel and JSON. The "More actions" menu is gone.
