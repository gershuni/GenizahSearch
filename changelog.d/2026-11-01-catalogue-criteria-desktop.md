## [Unreleased] -- Catalogue criteria, desktop: the Manuscript features window (desktop, PR 1c)

PR 1c of `docs/plans/CATALOGUE_CRITERIA_PLAN.md`: the desktop surfaces of the catalogue criteria
(released together with the web panel and the API, ruling D11).

- **"Manuscript features" / "מאפייני כתב היד"**: one window, the same on the Catalog tab, in "Search only
  in..." and for the post-search Measurements filter. Sections on the side (period of copy, language and
  script, script, vocalization, columns, lines, physical status, material, attributes, computer-generated
  data), each section's controls beside them; every list sorted by count, then Unknown, Cannot be
  determined, Not recorded, with "only these / all except these", and a filter box on long lists; ranges
  with "or: not recorded"; a catalogue / measured / either choice for lines, size and material. The total
  updates as you choose, and says how many of an "all except" list have nothing recorded.
- **Catalog tab**: the date fields, century buttons, "Include undated" and "Script and size..." are now
  inside the window; its choices show as chips above the list. The domain tree no longer collapses to two
  rows when the column is full (it keeps 240 px and the column scrolls), and it shows "No catalogue record
  (N)" beside "Unclassified".
- **"Search only in..."**: the date range, script lists and Measurements group are replaced by the one
  button; chips and the history summary name the chosen features.
- **Post-search Measurements** filter by the window's rules (ruling D20): either source, one measured page.
- **Saved searches**: a saved date range becomes Period of copy (D18, which now matches a copy span that
  overlaps the range). Old measurement filters are not restored, and the stored results of a search saved
  with them are not shown: search again (ruling R2).
- On a 5.0.0 catalogue data file, the criteria that need 5.1.0 are shown disabled with a note, and saved
  ones are removed with a notice (rule 9).
