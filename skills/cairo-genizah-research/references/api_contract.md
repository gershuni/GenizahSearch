# GenizahSearch API notes for research agents

Read this when interpreting responses or configuring parallels. Full maintained
reference: https://github.com/gershuni/GenizahSearch/blob/master-main/docs/SEARCH_API.md
Interactive schemas: https://genizahsearch.com/api/docs

## Requests

- POST /api/search: `query`, `search_mode` (exact, variants, fuzzy, responsa,
  title, shelfmark), `limit`, optional `gap`, `filters`, `responsa_options`.
  Use search.py or stage.py; parallels uses `mode`, not `search_mode`.
- GET /api/browse: use the returned `locator.sys_id`, `p_num` (1-based), and
  `volume_ie`. Use browse.py, with `--text-cap 10000` for detailed reading.
  Do not construct UIDs from shelfmarks. UID-only lookup can fail on deployments.
- GET /api/capabilities: check `features.passage`, `features.passage_multi_witness`,
  `parallels.methods`, `parallels.max_witnesses`, and live limits/timeouts.

## Letter-level parallels

POST /api/parallels:

```json
{"method":"passage", "text":"Text to match"}
```

CLI: `python scripts/parallels.py --method passage --text-file passage.txt`.
The text limit is 20,000 characters. The index covers Genizah transcription data,
not the user's local library. Passage tolerates recognition noise and changed line
breaks. Do not send nondefault chunk_size, mode, max_freq, or boundary_mode.
They have no letter-level equivalent. A 503 passage_unavailable is an unavailable
engine, not zero results; never silently substitute chunk matching.

For several witnesses of ONE work, replace text with a witnesses array:

```json
{"method":"passage", "witnesses":[
  {"label":"A", "text":"First witness"},
  {"label":"B", "raw_header":"EXACT_HEADER_FROM_A_RESULT"}
], "sort":"fused"}
```

Each witness has exactly one of text or raw_header. Use actual returned headers,
not invented locators. Do not concatenate witnesses. `sort` requires witnesses;
choices are fused, best_match, witness_count. The server validates count and cost
limits. CLI accepts the array alone in `--witnesses-file witnesses.json`.
Multi-witness results may include `witness_fusion` with witness_count,
witness_ids, fusion_score, and best_witness_score.

## Legacy word chunks

`--method chunk` is the script's compatibility default. It accepts chunk_size
(2–20, default 5), mode (exact, variants, fuzzy), max_freq, and boundary_mode
(full, boundary, combined). `max_freq` is a document-count cutoff, not a ratio;
values >=50 cannot filter the engine's top-50 per-chunk retrieval.

## Response interpretation

Search and parallels return source, generated_at, results, count, total,
warnings, and request (effective options, not a literal input echo). Browse
instead has locator, text, text_source, text_truncated, metadata, and image;
it has no results/count/request. Library attribution is `library: {code, name}`.
Image URLs can be relative to the API base.

Parallels results include shelfmark, locator, score, snippet, and matches.
Each match includes chunk_index, source_chunk_text, manuscript_snippet, score.
Passage matches are contiguous spans; score counts matched query letters, not a
probability. Do not compare passage scores with chunk scores. Inspect all returned
rows/pages relevant to a grouped result. Count and total are equal for parallels,
and do not represent a complete census. `filtered` holds demoted results, including
apparent duplicate photography, and should not be ignored when tracing witnesses.

Warnings can be strings OR objects. Important warnings:

- passage_results_truncated: candidate/verification budget hit. Report it; neither
  zero results nor a small list proves that no other parallels exist.
- truncated_to_200: only the top 200 manuscript groups returned.
- duplicate_photography_demoted: apparent copies of the same photographed page.
- witness_ref_unresolved / witness_duplicate_skipped: some input witnesses skipped.
- sort_not_applied: too few witnesses resolved to apply fusion ordering.

`text_source` is pgp_transcription, snippet, or none. Use
`format_output.honesty_annotation(response)` in research notes for API-derived
quotations. A nontruncated snippet is not automatically a scholarly transcription.
Describe any additional image checking separately; do not claim independent
palaeographic decipherment when merely editing automatic output.

## Errors and network access

Errors usually return `error: {code, message}`; edge proxies may return non-JSON
bodies. Scripts handle JSON decoding failure and retain HTTP status in the message.
Common errors: rate_limited, heavy_search_busy, core_timeout, invalid_request,
manuscript_page_not_found, locator_conflict, passage_unavailable,
passage_multi_witness_unavailable, passage_option_unsupported.
Report failures and continue independent work without automatic retry loops.
Retry-After can accompany 429 or 503. Scripts self-throttle each endpoint.
Chunk requests allow 130 seconds for the documented 110-second server ceiling;
passage's documented ceiling is 30 seconds. Live capabilities may differ.
