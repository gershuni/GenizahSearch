# Search matching time limits

The September 14, 2026 incident was sampled twice with `py-spy`: the same
search worker held the GIL inside `regex.search(match_content)` in
`shared/search_engine.py`. That variable previously held a standard-library
`re.Pattern`. Running it in a thread did not isolate the web event loop.

Query-derived matching now uses the pinned `regex` package with bounded
operations and GIL release. Ordinary metadata parsing continues to use `re`.
Search timeouts abort with an explicit UI error or API `core_timeout` (504),
rather than skipping expensive candidates and reporting incomplete results as
complete. Optional viewer highlighting can fall back to escaped plain text.

Configuration:

| Variable | Default | Purpose |
| --- | --- | --- |
| `GENIZAH_REGEX_TIMEOUT_SECONDS` | 0.25 | Maximum duration of one matching operation |
| `GENIZAH_SEARCH_BUDGET_SECONDS` | 60 | Shared worker deadline for a search |

The search API supplies its existing per-mode time budget to the worker.
The chunk-mode parallels API likewise installs `SEARCH_API_PARALLELS_TIMEOUT`
(110 seconds by default) inside its executor worker, overriding the generic
60-second default for that request. Passage-mode deadlines remain unchanged.
Cancellation of an awaiting UI task keeps its admission slot occupied until
the underlying work finishes.

These are cooperative worker deadlines checked around matching operations,
not process termination. They do not interrupt a blocked database call or
regex compilation. Existing query expansion limits still matter. A separately
managed search process would provide stronger isolation for those cases.

Deployment requires installing the updated requirements before restarting the
service. A running old worker is not changed by updating source files; it must
finish or the service must be restarted. The local change does not itself
deploy or restart production.
