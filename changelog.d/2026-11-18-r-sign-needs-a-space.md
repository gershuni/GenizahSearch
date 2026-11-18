## [Unreleased] -- A query beginning with R is no longer a Responsa search (web + desktop, branch `claude/r-sign-needs-space`)

### Fixed
- **`Rashi` searched Responsa for `ashi`.** The engine's own reading of the search-mode signs
  (`shared/search_engine.py::SearchEngine.parse_query_syntax`) took any sign with no space after
  it, and `R`, the Responsa sign, is a letter: every query beginning with a capital R lost that
  letter and ran as a Responsa search (`Rabbi` for `abbi`, `R. Isaac` for `. Isaac`). On the
  website this also left the mode selector on Responsa for the searches after it, and kept such a
  query from the catalogue, where a Latin-script query now goes. `R` now counts only with a space
  after it, as Help has always described it (`R שלום`); the symbol signs still need none
  (`$שלום`, `#T-S 12.1`). The website's /search box, the addresses the homepage and header boxes
  open (`/search?q=Rashi`) and the desktop app all read the query through this one method.

### Tests
- `tests/test_responsa_integration.py::TestResponsaSignNeedsASpace` pins the rule on the engine;
  `tests/render_smoke/test_search_box_input_render_smoke.py` pins it on the /search page, opened
  from its address and typed, with and without the catalogue served. Both fail on the code before
  the fix.
