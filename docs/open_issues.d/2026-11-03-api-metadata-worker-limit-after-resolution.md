## P3 api: A broad Title/Shelfmark search from a background API job gets the worker's full time limit after the server already resolved its manuscripts
Where: `web/research_jobs.py::IsolatedEngine.__getattr__` (the `_TO_WORKER` branch after `_metadata_search`)
Opened: 2026-11-03

Since PR #468 the server process resolves a Title/Shelfmark search's manuscripts
(`SearchEngine.metadata_search_ids`) before the search is handed to a research worker,
and the worker is given `Config.WEB_SEARCH_TIME_LIMIT` as its `time_limit`. Inside a
`shared_time_limit()` (a refinement chain on the website) the resolution time is
deducted and the worker gets what is left; with no shared limit, which is every
background job of `POST /api/search/jobs` (`api_engine` sets no deadline there), the
payload keeps the full limit, so the job's allowance grows by the resolution time.
Before, the worker resolved them itself, inside its own `_time_limited` deadline
(counted from when the job left the queue). Codex review of PR #468, round 3.

Size: milliseconds once the catalogue index is built (`prewarm_shelfmark_search` at
server start); up to ~2 s for a Shelfmark search in the first seconds after a restart
(a cold index build) and ~0.3 s for a Title search (its substring scan). Fix sketch:
in the `_TO_WORKER` branch, with `shared is None`, set
`time_limit = max(time_limit - elapsed, 0.001)` where `elapsed` is what
`_metadata_search` spent (return it beside the sentinel, or stamp it on `arguments`
and pop it), and pin it with a test like
`test_a_broad_search_s_resolution_comes_off_the_worker_s_time_limit` without the
shared context.
