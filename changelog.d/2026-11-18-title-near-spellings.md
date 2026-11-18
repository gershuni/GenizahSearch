## [Unreleased] -- Fuzzy search (near spellings) in the Title search (web + desktop, branch `claude/title-near-spellings`)

### Added
- **Fuzzy search in Title mode**: the catalogue search's near-spellings switch, off by default,
  now in Title mode too, in both apps (website: beside the mode, without the translations switch,
  `&near=1` in the address; desktop: a switch under the search bar in Title mode, kept in the
  search history). With it each word of the query is found on its own, anywhere in the title and
  in any order, as typed or in its plene or defective spelling (one ו or י added or dropped, as
  Responsa's `%word`): תפילין finds תפלין, פירוש תהלים finds "פרוש כתובים (תהלים, קטע)". A
  Latin-letter word of five letters or more also matches one letter different (two from nine),
  only where some title holds that spelling as a word: Saadia finds Saadya. A word with a quote
  mark is an abbreviation and is found with its mark, as without the switch. Without the switch
  the Title search is unchanged: the query is one phrase. Search within and Back replay it
  (`RefinementStep.catalog_options`).
- Measured over the 192,426 titles of `libraries.csv` (the scan alone): 0.25-0.6 s a search with
  the switch, against 0.15-0.4 s without; the titles' Latin words (10,812) are read once, 0.4 s.
  תפילין 194 -> 197 titles, ירושלים 228 -> 237, תהילים 3,038 -> 4,181, saadia 1,340 -> 1,447; a
  short word gathers more (קדיש 220 -> 871, mostly קדש).
- Public API and MCP: `near_spellings` is valid with `search_mode='title'` too (echoed in
  `request`); `include_translations` stays `catalog` only.
