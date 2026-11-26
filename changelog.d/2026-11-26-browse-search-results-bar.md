## [Unreleased] -- Browse: a bar to the other search results (web, branch `feat/browse-search-results-bar`)

A result opened in Browse from the search page left no way to the next result but going back to
the search. Browse opened from a result now shows a thin bar above the manuscript: close,
previous and next result, and "Search results · Result 7 of 143", which opens a scrolling list of
the results (number, shelfmark and the matched words; the current one marked) with "Back to the
search results" at its top.

- `web/components/results_trail.py` (new): the lists, kept in memory for the browser that made
  them (its session code), at most 2,000 results each, 200 lists, six hours; the bar and its list.
  A restart, another browser or a link sent to someone shows no bar, and Browse opens as before.
- `web/pages/search_results.py`: `render_results` keeps the list once for each list it shows
  (`keep_results_trail`); each card's Browse link carries `trail=` and `r=`. The link's address is
  built by `result_browse_url` / `result_page_ids` (moved out of the card, unchanged).
- `web/pages/browse.py`, `web/main.py`: the `/browse` route takes `trail` and `r`; Browse keeps
  them in its address while the bar is shown, and the close button takes both away.
- A result opened from the bar replaces the page in the tab's history, so the browser's Back
  returns to the search. "Back to the search results" opens the search page the result was clicked
  on (its address, kept in the browser when the link is clicked), else `/search`.
- `tests/test_size_ratchet.py`: the `create_browse_page` ceiling is raised by 8 lines for the wiring
  (the parameter, its docstring, the call that places the bar and the address parameters).
