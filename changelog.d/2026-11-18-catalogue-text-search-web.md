## [Unreleased] -- Catalogue text search on the website, with near spellings (web, branch `claude/catalogue-search-fuzzy-3589e0`)

### Added
- **Search what the catalogues say about a manuscript** (switched off until the file is on the
  server: `CATALOG_TEXT_SEARCH_ENABLED`, and `catalog_data/catalog_text.db` passing its checks,
  `web/catalog_assets.py`). A **Catalog** mode on `/search`, also by typing `%` and a space,
  searches the FJMS catalogue's titles, descriptions and full entries, PGP's descriptions and the
  library titles, in Hebrew and English, with their automatic translations: one result per
  manuscript, ranked titles first, original text before translation, the words as typed before
  their Hebrew prefixes. Each result shows the catalogue text that matched and where it comes from
  ("FJMS Catalog · Free Description · automatic translation"). Catalogue results are never mixed
  into a transcription search's.
- **Fuzzy search** (near spellings), a switch in Catalog mode (off by default; the text search's own
  word for it, with a rule of its own): Hebrew words also in their plene or
  defective spelling (one ו or י added or dropped, as Responsa's `%word`: כתובה finds כתבה), Latin
  words with one letter different from five letters, two from nine (Saadia finds the catalogue's
  Saadya and Saadiah: 281 manuscripts as typed, 10,125 with them). Only spellings the catalogue
  holds are searched; manuscripts found only through one come after the rest, and the line under
  the count names the spellings added (`shared/near_spellings.py`). The text search's Fuzzy rule
  was measured first and floods single catalogue words (כתובה: 2,216 manuscripts as typed,
  135,943 with it).
- **Automatic translations**, a second switch (on by default), leaves the catalogue's automatic
  translations out.
- An empty text search offers the catalogue's count ("No matches in the transcriptions. N
  manuscripts whose catalogue records mention X"), after "Did you mean" when that applies; a
  Latin-script query, which the transcriptions never answer, is searched in the catalogue, with
  the transcription search one click away. PostHog `search_suggestion` gains `kind='catalogue'`.
- Public API and MCP: `search_mode='catalog'` with `near_spellings` and `include_translations`;
  each result adds `catalogue_source`, `catalogue_field`, `automatic_translation` and
  `near_spelling`. Listed in `/api/capabilities` only where served; elsewhere 400
  `catalog_unavailable`.
- The web app opens the file once at startup (when the flag is on) and logs when it was built
  from FJMS or PGP data other than the installed ones; `scripts/deploy_catalog_text_sidecar.ps1`
  gates, uploads and swaps it (code first, then the file, then the flag).
