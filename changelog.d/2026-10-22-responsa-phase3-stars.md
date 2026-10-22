## [Unreleased] -- Responsa search: `*` finds any letters at the end of a word or inside it (web + desktop, branch `claude/gifted-brahmagupta-yy43jt`)

Phase 3, first part, of `docs/plans/RESPONSA_TRUE_WILDCARDS_PLAN.md` (§7, "Phase 3, settled before
building"). It works on today's index, on the website and on every desktop.

- **A `*` stands for any letters inside one word, none included**, at the end of a word or inside
  it, as many times as typed, as in the Bar-Ilan Responsa Project: `מאמ* קור*` finds `מאמתי
  קורין`, `כ*נים` finds `כהנים`, `כוהנים` and `כנים`. A star never reaches past a space, a digit
  or punctuation, and every star word is a whole word. Spelling variants, Judeo-Arabic, `%` and `#`
  apply to the letters around the star. A star word is looked up through the index's pattern
  search, so it is quick on today's index too.
- **Matches across a page break** are found for star searches: on a rebuilt index from the seams,
  on today's index from each manuscript's page breaks when the manuscripts holding the words are few
  enough to read in time; when they are not, the count shows "+" and the results line says which
  matches were not searched. Search within such a list searches within the shown results.
- **Responsa reads the pages first**, then the matches that run across pages, and reads up to
  150,000 candidate pages (it was 50,000), so a broad star search shows its full list. A search whose
  every word has a star is ordered by the matches on each page, then by shelfmark. The results line
  counts a star word as a pattern.
- Counts change: a `word*` search now finds every word that begins with the letters, not only the
  word with its 25 grammatical suffixes.
- A `*` at the start of a word works as before for now (`*word` with the grammatical prefixes,
  `*word*` as `#word#`); a word that begins with `*` and holds another `*`, a word made only of
  `*`, and a star word with punctuation inside are refused with a message (API: 400
  `responsa_unsupported_star`). A star word with only one or two letters of its own carries a note
  that its list may be cut.
- `Config.RESPONSA_PAGE_LIMIT` (150,000) and `Config.RESPONSA_STAR_AGGREGATE_LIMIT` (2,000); the
  cut-off signal gains `partial` / `limited` for matches left out by rule
  (`RefinementStep.result_partial`); seam rows have `scope` `seam` and carry the pages they cover.
