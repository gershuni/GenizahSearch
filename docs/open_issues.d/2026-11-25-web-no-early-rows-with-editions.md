## P2 web: With the scholarly editions searched, the default where they are served, no search shows early rows
Where: `web/pages/search.py::_run_search` (`_preview_wanted` needs `run_sources` None or 'automatic'), `web/research_jobs.py` (the two legs' workers), `shared/editions_search.py::group_results` / `fold_same_text`
Opened: 2026-11-25

The website shows a search's first rows while it runs (2026-10-05; Responsa too since GS-310,
dev PR #127) only when the scholarly editions are not read. Where they are served, the source
choice defaults to "All" (`saved_sources`), and then no search shows early rows: the page
waits for both legs, their grouping and folding (editions plan, §3.3 item 7: "a stable-prefix
merged preview is later work"). Measured 2026-11-25 on the real index, in process, stdlib re:
`#שלום` is 13.3 s in the main leg (51,208 rows), 3.0 s in the editions leg (7,551) and 3.0 s
grouping them (53,096 results); an Exact `שלום` 3.2 s, 0.7 s and 1.8 s. In a research worker the
transfer of the rows adds to that. The API is not affected: since PR #127 a search with editions
that returns its first `limit` rows runs the editions leg first and builds only what can be
among them (`SearchEngine._first_grouped_rows`).

Why it is not simple: the grouped order is a reciprocal rank fusion of both legs' ranks, and
`fold_same_text` folds edition-only results into earlier ones and joins a later automatic page
to an earlier unplaced edition of its manuscript, so a row can move into the first page after
both legs are far along. A sketch: the same rule `_first_grouped_rows` uses -- the editions leg
first (small), then the main leg's early rows -- gives a stable first page once the main leg has
settled that many rows, if the pages `editions_search.counterparts` names are built as they are
found; the preview is then the grouped first page of what is settled. Check it against the
whole list on the real index, as PR #127's equivalence scripts did.
