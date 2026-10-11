# Chunk parallels and variant levels: MCP server and public API

**Status:** rev 3, 2026-10-11: built; scope narrowed by measurement (section 6) and owner decisions
4-5. Rev 1 reviewed by Codex gpt-6-astra: SHIP WITH FIXES, folded into rev 2. Branch
`feat/mcp-chunk-variant-levels` (from `896ec8f4`).

**What ships (rev 3):** chunk parallels in MCP (`find_chunk_parallels`: exact, variants, fuzzy; any
chunk size); the Extended level for search (`variants_extended`) in MCP and the public API, as
background jobs only. **Held:** Maximum everywhere, and both higher levels for chunk search. Where
sections 4-5 below describe Maximum or chunk levels, they record the rev 2 design, superseded by
decisions 4-5.
**Parent:** [`MCP_SERVER_PLAN.md`](MCP_SERVER_PLAN.md) (the MCP server, live since 2026-10-10).

## 1. Why

The first real ChatGPT test of the live plugin asked for "chunk search, chunk = 3, extended
variants" (Hebrew: חיפוש מקטעים, מקטע=3, וריאנטים מורחב). ChatGPT correctly refused: the plugin
cannot run it. Two limits, both inherited from the GPT:

1. `find_passage_parallels` always sends `method=passage`; the facade refuses anything else
   (`web/chatgpt_api.py::_validate_pilot_body`, "chunk search is not supported here"). The public
   API already runs chunk parallels (`chunk_size` 2-20, `mode` exact/variants/fuzzy).
2. The website has three variant levels -- Basic (30 letter-confusion pairs), Extended (70),
   Maximum (150) -- but the public API, the facade and MCP accept only Basic, by Phase 81A
   decision D-03.

## 2. Owner decisions (2026-10-11)

1. Add chunk parallels to the MCP server: **yes**.
2. Extended and Maximum: **both** the MCP server and the public API (reverses D-03).
3. **Hold** the directory submissions (OpenAI, Anthropic) until this ships.
4. After the first measurements: synchronous `/api/search` and `/api/parallels` **refuse** the higher
   levels (400 `background_job_required`) and point to the `/jobs` routes.
5. After the full measurements: the **safe set** -- search Basic + Extended (jobs only); chunk search
   exact, Basic and fuzzy; hold Maximum everywhere and Extended for chunk search until the engine stops
   overrunning its time limit (a separate task).

## 3. The parity contract (variant levels only)

For the three variant levels, an API search matches as the website does for a visitor who has
chosen nothing, **per page**, because the two website pages run the levels differently:

| Website page | Level | Engine mode | Pairs | Num Changes (default) |
|---|---|---|---|---|
| Search | Basic (`variants`) | `variants` | 30 | x1 |
| Search | Extended | `variants_extended` | 70 | x2 |
| Search | Maximum | `variants_maximum` | 150 | x2 |
| Parallels (chunk) | Basic | `variants` | 30 | x1 |
| Parallels (chunk) | Extended | `variants` | 70 | x1 (Basic's: owner ruling K-20) |
| Parallels (chunk) | Maximum | `variants` | 150 | x1 (Basic's: owner ruling K-20) |

Sources: `web/pages/search.py::execute_search` (`for_search(mode, pairs_count)`),
`web/pages/parallels.py` (`for_search('variants', level, changes)` and the "Composition runs at
Basic's x1-x3 at every level" comment), `shared/variants.py::DEFAULT_MAX_CHANGES_BY_PRESET`.
With settings bound, `VariantManager` takes the pair count from `variant_pairs_count` (ahead of the
mode, through `slider_map`, plus the website's custom pairs) and the changes per word from
`variant_max_changes`; the engine paths pass explicit spelling limits (chunk retrieval 200, regex
verification 8,000), so the tier's default per-term limit plays no part.

Scope of the promise: **matching** for the variant levels. Not covered: Exact (the website's Exact
runs engine mode `literal`, the API's `exact` -- an existing difference, out of scope here), and
the returned lists (the API slices search results and caps parallels at 200 groups).

## 4. Design

**D1. One pure helper.** `web/variant_preferences.py::default_search(mode, *, composition=False)`
returns `(engine mode, settings)` for a visitor who has chosen nothing, by the table above.
`for_search` keeps its visitor-storage logic; tests pin `default_search(...)` to `for_search(...)`
called the way each page calls it, with empty visitor storage, for every mode on both pages.

**D2. `/api/search`.** `search_mode` gains `variants_extended` and `variants_maximum`, added to the
Pydantic enum AND `_SEARCH_MODE_TO_INTERNAL` (dispatch, capabilities and provisional telemetry all
read the mapping). The handler binds the helper's settings **after** `api_engine`
(`with_request_settings(api_engine(...), **settings)`; binding first is silently undone, since
`api_engine` builds a fresh wrapper) and passes the engine mode. This replaces today's fuzzy-only
x2 binding with the same helper. Both are heavy modes (`HEAVY_SEARCH_MODES`). `limit` stays
[1, 100]. Sync timeouts (provisional, decided by section 6): Extended uses
`SEARCH_API_VARIANTS_TIMEOUT` (60 s); Maximum uses `SEARCH_API_FUZZY_TIMEOUT` (110 s) -- the
longest sync ceiling the edge proxy's budget allows, chosen for headroom, not because fuzzy and
Maximum cost the same (they do not: fuzzy runs near-spelling queries). Never raised past 110 s.

**D3. `/api/parallels`, `method=chunk`.** `mode` gains the same two values. Engine mode `variants`;
settings from `default_search(mode, composition=True)` (70 or 150 pairs at x1), bound after
`api_engine` as in D2. The response's `mode` and the request echo report the mode the caller sent,
not the engine mode. `method=passage` with either value is rejected like `variants` is today
(400 `passage_option_unsupported`).

**D4. `/api/capabilities`.** `search_modes` lists the two values (it reads the mapping); a new
additive `parallels.chunk_modes` lists the five chunk modes; `timeouts` gains `variants_extended`
and `variants_maximum`. Additive under the Stability section, so `schema_version` stays 1.

**D5. Facade (`/api/chatgpt/*`).**
- Search jobs accept the two new modes (limit up to 100).
- Parallels accept `method=chunk` with `text` only (1-5,000 characters, the facade's existing cap;
  no `witnesses`, no `sort`), passing `chunk_size` and `mode` through to the API's validation.
- **Worker guard.** Production runs two research workers (`GENIZAH_RESEARCH_WORKERS=2`), shared with
  website searches, and facade jobs bypass the heavy semaphore. At most **one** facade chunk job runs
  at a time; another gets 429 `research_jobs_busy` with `Retry-After`, so AI chunk searches can never
  hold both workers.
- **One oversized row no longer fails a job.** `build_pages` today raises when a single row cannot fit
  a page, failing the whole job. Chunk rows carry an uncapped `snippet` (concatenated manuscript
  fragments). Before raising, the row's `snippet` and `excerpt` are dropped and the row is marked
  `snippet_omitted: true` (its locator and its match spans stay: at most three, each complete, with
  `matches_available` counting the rest, as pagination already did); only a row that still cannot fit
  fails the job.
- The GPT's schema is frozen: neither `integrations/chatgpt/openapi.json` nor its builder
  `integrations/chatgpt/build_schema.py` changes (running the builder rewrites the JSON). The GPT
  retires 2026-12-11 and keeps sending passage only.

**D6. MCP.**
- `search_manuscripts`: the two modes in `search_mode`, described in `SEARCH_MODES` (70/150 pairs,
  up to two changes per word, slower than variants).
- New tool **`find_chunk_parallels`** ("Find parallels by word chunks"): `text` (1-5,000),
  `chunk_size` (2-20, default 5), `mode` (exact | variants | variants_extended | variants_maximum |
  fuzzy, default exact), `filters`. Its description explains its own levels (70/150 pairs at ONE
  change per word, as the website's composition search) rather than reusing search's x2 text.
  `STARTS_A_JOB` annotations, `_start_and_wait`. Separate from `find_passage_parallels` because the
  two engines' method-specific options differ (witnesses and sort vs chunk size and mode); with one
  tool, `_RequestGuard` would still accept declared-but-incompatible combinations.
- `find_passage_parallels` and `get_research_job` descriptions name the new tool;
  `server_instructions.md` step 4 says when to use which ("chunk search" / "חיפוש מקטעים" =
  `find_chunk_parallels`).
- `integrations/mcp/smoke_test.py::EXPECTED_TOOLS` gains the tool.
- Tool names are frozen after this for the directory submissions.

**D7. Packages.** Regenerate the skill and `GEMINI.md`; bump the three manifests to 1.1.0; re-export
and push `gershuni/genizahsearch-ai-plugins`.

**D8. Docs and clients.** `docs/SEARCH_API.md` (request fields, capabilities, timeouts, a changelog
entry); `skills/cairo-genizah-research/scripts/{search,parallels,stage}.py` mode choices and help;
`integrations/mcp/README.md` and `acceptance-tests.md` (a chunk prompt); `docs/CODE_INDEX.md`;
the stale enum comment in `web/api_hardening.py` (records any mode string; no whitelist).

**Out of scope.** Num Changes x1-x3 and slider pair counts per request; Responsa levels (Basic
only, owner ruling 2026-09-28; the Responsa-options rejection test stays); chunk `max_freq` and
boundary options in MCP (the API keeps them); Exact parity; the LOCAL corpus.

## 5. Tests

- Parity helper vs `for_search` (done: `tests/test_variant_levels_api_parity.py`).
- **Queued payloads through the real `IsolatedEngine`** (the RecordingQueue harness of
  `tests/test_web_variant_settings_per_visitor.py`): the engine mode and the settings snapshot a
  worker receives, for search and chunk, sync and background job, each level -- and the website's
  `?? word` and API `variants_extended` give identical payloads. Bare fake engines cannot catch a
  binding-order bug, so these go through the wrapper.
- Enums: API search/parallels accept both values; passage rejects them; capabilities lists them and
  their timeouts; heavy-mode gate applies; Responsa-options rejection unchanged.
- Facade: chunk accepted, witnesses/sort with chunk refused, 5,000-character cap, one chunk job at a
  time (second gets 429), oversized row keeps the job with `snippet_omitted`.
- MCP: tool list and schemas, the guard's declared arguments, `get_research_job` paging a chunk job,
  smoke script expects seven tools.
- Packages regenerate cleanly at 1.1.0.

Every new test is proven able to fail with the scratchpad mutation harness.

## 6. Measurement (done 2026-10-11)

Local real index, dev server with two research workers (as production), through MCP background jobs
(the worker's website time limit is 180 s; a worker still running 60 s later is stopped by force and
its replacement reloads the engine). Timings are noisy (one phrase took 14 s synchronously and 65 s as
a job) -- orders of magnitude, not exact figures.

| | Basic | Extended | Maximum |
|---|---|---|---|
| Search, 3 common phrases | 27-129 s, cut off, 50-97 results | 113-185 s, cut off, 50-100 results | 187-202 s, cut off, 100 results from 760-1,620 candidates (Extended: 5,300-9,228) |
| Search, 3 rare phrases | 6-65 s, complete | 60-66 s complete; 184 s cut off (one) | 201-233 s cut off with 0 found; one **killed** at 240 s |
| Chunk, 500 chars, size 3 | 110 s, 200 groups | 189 s cut off, **1** result | 200 s cut off, 0 (size 3); **killed** (size 2) |
| Chunk, 2,000 chars, size 3 | 189 s cut off, 200 groups | 220 s cut off, **0** | **killed** (sizes 2 and 3; also 5,000 chars) |

Synchronous `/api/search` (before decision 4): Extended passed 60 s on 4 of 6 phrases; Maximum timed
out at 110 s on all 6. While a killed worker reloaded, an ordinary exact search timed out at 30 s --
what website visitors would meet in production. Pagination, both MCP protocol generations and the
one-chunk-job guard worked; no row needed `snippet_omitted`; the largest page was about 40 KB. Result
parity with the website was not compared by locator: the queued worker payloads are proven identical
(tests), and the website runs the same worker code, so its Extended and Maximum composition searches
very likely behave the same (CHANGELOG 9.6.0 already records Maximum searches over five minutes on
the desktop).

## 7. Risks (rev 3)

- Extended search on common phrases runs to the 180 s worker limit and returns what it found with
  `results_cut_off`; it holds one of the two production research workers meanwhile. It runs only as a
  background job (synchronously refused), so it never holds a heavy-budget slot or a sync timeout.
- One chunk job runs at a time across all facade callers (MCP, GPT); others get `research_jobs_busy`
  with `Retry-After: 30`. Chunk search runs only as a facade job, never on the synchronous facade
  route (Codex build review: the sync route bypassed the guard).
- Each page repeats the envelope's `source_text` (up to 5,000 characters, about 30 KB escaped in MCP),
  so chunk pages hold fewer rows than passage pages of short texts.
- Held, pending the engine fix (separate task): Maximum everywhere, Extended for chunk search.
