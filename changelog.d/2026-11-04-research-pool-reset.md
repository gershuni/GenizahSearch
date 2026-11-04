## [Unreleased] -- A closed research pool is never reused (web, branch `claude/arabic-lane-sidecar`)

`web/research_jobs.py::shutdown_research` now resets the research queue and its wait pool after
closing them, so a later lifespan in the same process builds fresh ones. Before, a test that ran the
app's lifespan left a shut-down pool behind, and any later `/api/search` or `/api/parallels` test in
that process answered 500 ("cannot schedule new futures after shutdown"). It surfaced when CI's chunks
reshuffled on PR #478 (`tests/test_parallels_api.py`, then `tests/test_web_saved_joins_isolation.py`,
then `tests/test_search_api_soak.py`). The server calls it once, at exit, so serving is unchanged.
`tests/test_research_jobs.py::test_a_shutdown_leaves_no_closed_pool_behind` pins it.
