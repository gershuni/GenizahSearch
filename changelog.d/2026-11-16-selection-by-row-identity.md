## [Unreleased] -- Selected results stay the ones ticked when a filter is on (web, branch `claude/selection-by-row-identity`)

With a result filter on, a ticked row could stand for another result: a row's checkbox stored its
number among the rows shown, and **Add Selected to List**, **Copy Selected Text** and the exports
of the selected rows read that number in the full, unfiltered list. Once a filter hid an earlier
row they acted on a different manuscript. A result is now selected by its identity (its uid, else
its manuscript: `web/pages/search_helpers.py::selection_key`), and the selection is read against
the rows the page shows (`selected_rows`), in their order.

- **Select all** ticks the rows shown, every page of them, none a filter hides, and keeps them on
  screen: it used to re-render the full list, which put back every row the result filters hid.
  Unticking it unticks the rows shown only.
- "N selected" counts the selected rows shown. A selected row a filter hides is not acted on, and
  shows ticked again once the filter is off.
- The export's selection follows every render, not only a click.
