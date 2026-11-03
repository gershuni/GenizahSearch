## [Unreleased] -- Shelfmark lookups answer at once (web + desktop, branch `claude/api-shelfmark-perf-a70155`)

A public-API search with `search_mode=shelfmark` took 10 s or more on most calls
(PostHog, 2026-11-03: 262 of 437 requests in 14 days, 56 of them a 504
`core_timeout`), and the website's Shelfmark mode 3 s at the median. Two causes:

- `MetadataManager.search_by_meta` normalized every one of the 255,723 catalogue
  records on every search, 3.5-4.8 s for a lookup that returns one manuscript. The
  catalogue is now normalized once into an index (`_ShelfmarkSearchIndex`, ~2 s,
  ~45 MiB, built at server start in the background and on the desktop's first
  Shelfmark search) that narrows a query to the records that can match it; the
  old loop's rules then decide over those, so the answer is the loop's
  (`tests/test_shelfmark_search_path.py` checks it against the loop, on a synthetic
  catalogue that exercises every rule and on the real corpus). The NLI cache's
  shelfmarks are normalized as they are met and kept while unchanged.
- On the web, every search went to a research worker: a subprocess that loads the
  catalogue (~6 s unless one was started ahead) and serves one search, behind a
  queue with one slot by default, so a lookup waited for the worker and for whoever
  was searching. `IsolatedEngine` now resolves a Title/Shelfmark search's manuscripts
  in the server process and runs a lookup of up to 100 manuscripts
  (`INLINE_METADATA_ROWS`) in the calling thread; a broader search still goes to a
  worker, with its manuscripts, so the worker skips the catalogue too. The API's
  response envelope and `search_mode_value` telemetry are unchanged.

Measured in-process on the dev machine (the ratio is what counts; production is
Linux): `execute_search` for 13 single-shelfmark lookups across CUL, JTS, Oxford,
RNL, Mosseri, Halper, BL and Gaster went from 3.7-4.7 s each to 4-26 ms; the broad
`T-S 12` (10,401 rows) from 9.9 s to 1.2 s.
