## [Unreleased] -- Manuscript features on the website: one panel for Catalog Browse and "Search only in..." (web, branch `claude/criteria-1b-web-44557e`)

PR 1b of `docs/plans/CATALOGUE_CRITERIA_PLAN.md`. One **Manuscript features** button
(`web/components/criteria_panel.py`) replaces the date range, the script lists, the pre-search
Measurements group and the "Script and size" card on `/catalog-browse`, `/search` and `/parallels`.
It opens a window over the page (full screen on a phone): the ten sections beside their controls
(period of copy, language and script, script, vocalization, columns, lines, physical status,
material, attributes, computer generated data), value lists with counts and a filter box, "only
these / all except these", catalogue / measured / either, "or: not recorded", each criterion's
coverage, a live total and Clear section / Clear all / Apply. A badge counts the criteria; a chip per
criterion removes it.

- Catalog Browse: the address carries one parameter per criterion (D8: `style=Square`,
  `region=!Oriental`, `lines=meas:20..25`, `columns=2..2,_none`); an unknown or malformed parameter
  is dropped and named in a notice. The domain tree gains "No catalogue record (N)". "Search in these
  results" and "Parallel search in these results" hand the criteria on unchanged. The "16-19" period
  button sets 1500-1899 (it set 1600-1999).
- `/search`, `/parallels`: the criteria are saved (`{prefix}_filter_criteria`), counted, sent with the
  search, kept in history entries and runs; a saved date range is read in as the period of copy
  (D18); saved measurement filters are not restored, and the rows saved with them are dropped so the
  search runs again (R2).
- `/search` post-search Measurements mean what the panel means (D20): width, height and lines from
  the catalogue or a measured page, every measured range on one page, material from either source
  (`FjmsService.match_result_criteria`). A lookup that cannot run leaves the rows and says so.
- A 5.0.0 catalogue file disables the criteria it cannot answer and drops them, with a notice, from a
  saved session or an address (rule 9).
- Shared (also used by the desktop, PR 1c): chip text (`criteria_chips`, `criteria_summary`,
  `FjmsService.criteria_chips`), `match_result_criteria`, `period_from_dates`, `NEW_TABLE_KEYS`;
  fixed `match_criteria` failing every "all except" search when the corpus came as a frozenset.
