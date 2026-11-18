## [Unreleased] -- The search-mode signs work in the homepage box and the header box (web, branch `claude/go-to-box-mode-signs`)

### Added
- **A sign and a space choose the search mode in the homepage box and the header box**, as they
  already did in the /search box: `$ שלום` opens /search in Title mode with `שלום` as the query,
  the sign removed. Every sign of the /search box works: `?` `??` `???` Spelling Variants (with
  the slider, at the level the sign names, on that page only), `=` Exact, `~` Fuzzy, `/` Regex, `$` Title,
  `#` Shelfmark, `R` Responsa, and `%` Catalog where the catalogue text search is served
  (elsewhere `%` is no sign, as before). The sign alone (`$ `) opens /search in its mode.
  `R` is the one sign that is a letter, and 33 shelfmarks begin with it: text of their shape
  (`R 15` Mosseri, `R II 5` Turin) is routed as before; anything else after `R` is a Responsa
  query, digits only too (`R 99 [3] 100`). A sign with no space after it is routed as before.

### Changed
- **One table of signs** (`shared/search_mode_signs.py`): the engine's prefix parsing
  (`SearchEngine.parse_query_syntax`), the /search box's live switch and the go-to boxes
  (`web/home_query.py::mode_sign_target`) read it; the /search page and the engine each kept a
  copy before. The desktop app's own copy in `genizah_app.py` is unchanged.
