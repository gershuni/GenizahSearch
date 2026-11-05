## [Unreleased] -- Catalogue text search, part 1: the data file (web + desktop, branch `claude/catalogue-text-data`)

The data layer of a new search over what the catalogues say about each manuscript
(`docs/plans/CATALOGUE_TEXT_SEARCH_PLAN.md`, settled by the owner 2026-11-05). Nothing is
served yet: no search mode, no page, no API change.

- `scripts/build_catalog_text_sidecar.py` builds `catalog_data/catalog_text.db` (about 730 MB,
  3.5 minutes) from the FJMS catalogue text and its automatic translations, the PGP English
  descriptions, `libraries.csv` titles and their English translations: one row per record and
  manuscript, its words in a contentless FTS5 index weighted title > description > full text.
  PGP's Hebrew translations stay out (withheld since 2026-09-21).
- `shared/catalog_text_normalize.py`: one key for text and query (nikud and accents dropped,
  abbreviation quote marks dropped so רמב"ם = רמב״ם = רמבם, final letters folded, English made
  singular), Hebrew prefixes on query words, quoted phrases, and no route from the query to FTS5
  syntax.
- `shared/catalog_text_service.py`: the fail-closed reader (one hit per manuscript, words as
  typed before prefixed forms, originals before automatic translations, a translations switch,
  snippets with the matched words marked); `CATALOG_TEXT_SEARCH_ENABLED`, default off.
- `scripts/verify_catalog_text_sidecar.py`: the gate, shown failing on ten kinds of damage.
- `scripts/sample_fjms_translations.py` and a two-reader audit of 150 FJMS automatic
  translations (decision D2): 14.0% wrong or garbage (95% CI 8.4-19.6%), against 27.8% for the
  withheld PGP translations, so translations are searched, labelled, with a switch.
