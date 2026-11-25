## [Unreleased] -- Search: the API builds only the results it returns, also with the scholarly editions; Responsa shows its first results at once (web + desktop, branch `claude/responsa-common-word-speed`)

- **API: only the returned results are built.** `POST /api/search` passes its `limit` to the
  engine (`execute_search(max_rows=...)`, `shared/search_engine.py`): the engine builds the
  first `limit` results and checks every other match only to count it; `total` is as before.
  With the scholarly editions (the default where they are served) the search runs in one
  research worker: the editions first, then only the transcription results that can be among
  the first `limit` (`SearchEngine._first_grouped_rows`, `shared/editions_search.py::counterparts`).
  Measured through the API with the server's matching: `#שלום` 12-16 seconds instead of
  20-30, with variants and Judaeo-Arabic 20 instead of 23 or a 504 at the 30-second limit;
  without the editions 10 instead of 18, and 13-15 instead of 22 or a 504. `#שלום [3] עולם` is
  not faster (its time is checking the candidates, which counting needs as well), and with the
  editions about 2 seconds slower (16-22 instead of 14-18): the editions are now searched
  before the transcriptions instead of beside them. The other text modes gain on common words.
- **Website and desktop: early rows in Responsa.** A Responsa search shows its first results
  while it runs, as Exact does: after 1-1.5 seconds for `#שלום`, 2-3 for `#שלום [3] עולם`,
  where nothing showed until the whole search ended (13-33 seconds on the server's matching).
  Not with a `-word`, not for a query whose every word holds a star (ordered by its matches
  once all are found), and -- as for every mode -- not when the scholarly editions are searched
  too, which is the website's default where they are served (a tracker issue). The desktop
  shows them when it searches the Genizah alone. The complete list, its count, sorting,
  filters and export are as before.
- **Faster rows everywhere.** Mapping a match back over the brackets of a transcription walks
  the brackets, not the letters (`_unstripped_span`): it was a third of a common Responsa
  search's time.
- Tests: `tests/test_search_max_rows.py`, the first results with the editions in
  `tests/test_editions_search.py`; the Responsa benchmarks in `tests/test_responsa_performance.py`
  run again (local-data lane), timing the website's first rows and the API's answer, with and
  without the editions. Closes the tracker issue filed as
  `docs/open_issues.d/2026-11-24-responsa-common-word-searches-slow.md`.
