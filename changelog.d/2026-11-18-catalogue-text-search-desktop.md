## [Unreleased] -- Catalogue text search in the desktop app (desktop, branch `claude/catalogue-desktop-3589e0`)

### Added
- **Catalog mode in the desktop app** (also by typing `%` and a space), as on the website: the FJMS
  catalogue's titles, descriptions and full entries, PGP's descriptions and the library titles,
  with their automatic translations, one result per manuscript. The Source column names where
  each result's words were found ("FJMS Catalog · Free Description"). Two switches beside the
  mode: **Fuzzy search** (near spellings: plene/defective Hebrew, near Latin spellings; off) and
  **Automatic translations** (on). A search within, and a restored session, replay them.
- A text search that finds nothing offers the catalogue's count in the results box ("No matches
  in the transcriptions. N manuscripts whose catalogue records mention X"), when no "Did you
  mean" is shown; the link runs the catalogue search.
- The installer carries `catalog_data/catalog_text.db` (about 190 MB compressed):
  `build_app.bat` refuses to build without it and checks it first
  (`scripts/verify_catalog_text_sidecar.py --rows-from-meta`). The mode shows only where the file
  is installed; `CATALOG_TEXT_SEARCH_ENABLED=0` hides it.

### Changed
- The search mode combo's rows are written once (`desktop/catalog_search.py`), not five times in
  `genizah_app.py`; the catalogue is appended at index 8, so no existing index moves.
