## P2 search: A Responsa search for a common word takes minutes, shows no early rows, and times out on the API
Where: `shared/search_engine.py::execute_search` (every candidate becomes a row), `web/search_api.py` (`limit` applied after all rows are built)
Opened: 2026-11-24

Measured 2026-11-24 on the real index, in-process: the engine call the website and the API
make (web/pages/search.py::_run_core_search; web/search_api.py's /api/search), for `#שלום`
(prefix expansion) and `#שלום [3] עולם`:

| Search | Rows | Website | API (30 s limit for Responsa) |
|---|---|---|---|
| `#שלום` | 51,208 | 26-34 s | 34 s: 504 `core_timeout` |
| `#שלום`, variants + JA | 51,529 | 101-104 s | 109 s: 504 |
| `#שלום [3] עולם`, variants | 510 | 95-100 s | 114 s: 504 |

The same searches run alone, as tests/test_responsa_performance.py calls them, took 17.9 s,
55.6 s and 67.5 s. Another session was loading the machine during the website and API runs,
so treat those as upper bounds and the solo times as lower bounds: even the solo times put
the last two over the API's 30 s. A plain Exact search for `שלום` is fine: 3-6 s, its first
rows at once.

Why: `execute_search` turns every Tantivy candidate (up to `Config.RESPONSA_PAGE_LIMIT`,
150,000) into a full row, reading each document, before it returns. The website shows early
rows only for plain searches (its `preview_callback` is not asked for in Responsa mode), so a
Responsa user sees nothing until the end. The API builds all the rows and only then cuts
them to `limit` (at most 100). Neither reads past the first few hundred rows. Not checked:
whether production sets `SEARCH_API_CORE_TIMEOUT` above 30 s.

Fix sketch, in line with the v9.5.0 decision to complete a result set only when it is
needed: build the rows a page or the API shows, and count the rest without building them.
Give Responsa the early-row preview. Pass the API's `limit` into the engine. Then lift the
skip on tests/test_responsa_performance.py::TestPerformanceBenchmarks, and give its ceilings
numbers that describe the fixed path.
