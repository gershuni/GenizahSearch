## [Unreleased] -- Search filters, counts and Export cover every result, not only the loaded rows (desktop, branch `claude/gs-9-result-view`)

GS-9, part 1 of 2. The Search tab loads its results 500 at a time, and the
column filters, the exclusions' count, "Showing X of N", "Load more" and
Export all read the rows loaded so far. A 1,200-result search not scrolled
exported 500 rows; a filter whose only match was result 900 showed an empty
table under "Load more results (700 not loaded)".

- `desktop/result_view.py` (new): one verdict per result, over the whole
  list, for every filter and exclusion. The batches draw only the results it
  keeps, so a filter never leaves an empty table with results left.
- "Showing X of N", "(N excluded)" and "Load more" count the whole list;
  Export writes every kept result (or the ticked ones, as before).
- A result whose shelfmark or title is not fetched yet passes a Shelfmark or
  Title filter until it is, so its fetch starts and the fetched text decides.
- A loosened filter that lets in results passed over before draws the table
  again from the top; ticked results stay ticked.
- Metadata arriving for many manuscripts runs one filter pass, not one per
  manuscript; the title of each new row reads the translations setting once
  per batch, not once per row. Turning translations on or off filters the
  titles again.
- "Only results with all terms" is one more rule of the view: "Load more"
  now pages its matching results.
- Tests: `tests/test_result_view.py`, `tests/test_result_view_window.py`;
  `tests/test_exclusion_surfaces.py` follows the new rule that a result
  excluded before its batch is drawn has no row.
