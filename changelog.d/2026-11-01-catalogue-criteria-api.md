## [Unreleased] -- Manuscript features in the API and the MCP tools (web, branch `claude/criteria-pr2-api`)

PR 2 of `docs/plans/CATALOGUE_CRITERIA_PLAN.md`. The public API and the MCP server filter by the same
manuscript features as the website's panel.

- `filters.criteria` on `POST /api/search` and `POST /api/parallels` (their background jobs, the
  ChatGPT facade and the MCP tools too): one entry per criterion, as the panel stores them — lists
  with `values`, `mode` ("only" / "except") and `source`, ranges with `min`, `max`, `not_recorded`
  and `source`; "_none" is Not recorded. The request model is built from the registry
  (`web/criteria_models.py`). The corpus is the universe, so "not recorded" and "all except" reach
  the 29,169 manuscripts with no catalogue record, as on the website.
- `GET /api/criteria`: every section and criterion with its kind, sources, unit and coverage, and a
  list's values with corpus counts (per source for material); a criterion a 5.0.0 file cannot answer
  is `available: false`. Built once per process (about 20 s cold on the full file).
  `FjmsService.criteria_vocabulary`.
- Checks: a malformed entry is 400 `invalid_request`; a value the catalogue does not record under the
  chosen source is 400 `unresolvable_filter_value`; a criterion the file cannot answer, or "all
  except" / "not recorded" before the corpus list has loaded, is 503 `filter_unavailable`.
- Older keys: `materials` is read as Material from the catalogue (unchanged results); `date_from` /
  `date_to` as Period of copy, so they now match a copy-date span overlapping the range (1000-1200:
  4,702 manuscripts, 4,561 by start year). Either one beside its criterion is 400
  `invalid_combination`. The unreleased `script_styles` / `script_places` are refused.
- MCP: a read-only tool `list_manuscript_features` (`GET /api/chatgpt/criteria`) and `criteria` in
  every tool's `filters`; the server instructions (and the Claude and Gemini packages) point filters
  at it. `/api/capabilities` lists `/api/criteria` and its rate-limit bucket.
- `docs/SEARCH_API.md`: the endpoint, the `filters.criteria` shape, the changed date meaning, and the
  corrected `materials` examples (`Paper`, `Vellum`; `paper` / `parchment` were always a 400).
- Tests: `tests/test_criteria_api.py` (real endpoints over the fixture sidecar), MCP and facade tests,
  the documented-example gate.
