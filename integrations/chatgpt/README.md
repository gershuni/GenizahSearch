# GenizahSearch ChatGPT pilot

Researchers open a shared custom GPT and ask questions. They need no Python,
local corpus, GenizahSearch API key, or software installation. GPT access and
sharing depend on their ChatGPT account and workspace policies; test the actual
target account before announcing availability.

## Status

Version 0.3 adds pagination to the background jobs introduced in 0.2. Each search
runs once; subsequent pages read prebuilt response bytes, without reloading an
index, invoking a search worker, or browsing manuscript pages. The pagination
upgrade needs deployment and testing inside ChatGPT.

To upgrade: deploy, re-import `https://genizahsearch.com/api/chatgpt/openapi.json`
in the GPT's Action editor, replace Instructions with `instructions.md`, and save.
Confirm the fifth action `getResearchJob` appears. Merely deploying the server
does not replace the schema saved in an existing GPT.
For version 0.3, `getResearchJob` must also show `page` and `collection` parameters.
Start a new search after upgrading: previous jobs contain only the old preview
and cannot recover candidates that were discarded.

Validation: targeted adapter and job-lifecycle tests cover limits, polling across
IP changes, failure preservation, expiry, cancellation, and shutdown. OpenAPI, request combinations, and
all seven captured response contracts validated. The real passage and multi-witness
responses compacted to 29,113 and 27,516 UTF-8 bytes respectively. These are local
checks against captured live responses, not tests of a deployed facade or GPT.

The live public API supports search, browse, passage, and multiple witnesses.
Direct testing on 2026-09-14 found a passage response of 241,654 characters,
exceeding ChatGPT Actions' 100,000-character ceiling. Importing the general API
schema directly would not fix that. This pilot includes a small API facade to
bound responses without cutting quotations or hiding truncation.

## Deploy the API facade first

Deploy these files together through the project's normal reviewed release:

- `web/chatgpt_api.py`
- `web/chatgpt_jobs.py` (uses the deployed `web/research_api.py` APIJob)
- `web/chatgpt_pagination.py`
- The registration and shutdown lines added to `web/main.py`, immediately after
  `init_search_api(app_override=_search_helper_app, path_prefix="")`.
- `integrations/chatgpt/openapi.json`
- `integrations/chatgpt/privacy.html`

After deployment the schema URL is
`https://genizahsearch.com/api/chatgpt/openapi.json`. Under `/api/chatgpt`, it points
to `/capabilities`, `/browse`, `/search/jobs`, `/parallels/jobs`, and `/jobs/{job_id}`.
No API credentials or OpenAI API key are used by the facade.
The existing hardened handlers still enforce mode gates, rate limiting, validation,
and concurrency limits. Existing public endpoint contracts are unchanged.

Search/parallels submission returns 202 and a private job ID immediately. Each
poll waits at most ten seconds, returning 202 while pending or the bounded result
and original HTTP status when finished. The same original handler runs with the
research_job cancellation/progress context, avoiding synchronous API deadlines.
The job allowance is ten minutes including worker startup and queue time. Expiry
or shutdown signals cancellation to the isolated worker; it does not recycle a
worker as soon as a polling request disconnects. Old synchronous facade routes
remain available for compatibility and retain their old deadlines.
Completed jobs return at most ten candidates and 80,000 UTF-8 bytes per page.
Oversized rows shrink the page without discarding candidates. Use the same job ID
and returned `pagination.next_page` to continue; null ends that collection. Main
results and demoted/filtered results have independent page sequences, selected
with `collection=results|filtered`. `pagination.available` counts saved candidates
in that collection. `total` and `request` retain upstream meanings; `count` counts
main results on the current page (zero on filtered pages). Three complete matching
spans per candidate are shown; `matches_available` records additional spans.
Byte/storage overflow returns an error rather than silent loss of later candidates.
Old synchronous routes retain their five-result preview behavior.

Background searches save up to 100 candidates by default for non-fuzzy modes and
500 for fuzzy; a smaller explicit limit is respected. These are retrieval caps,
not page sizes or guarantees of exhaustive corpus coverage. Existing engine caps
and truncation warnings still apply. Passage input remains limited to 5000
characters per witness and three witnesses. The server validates these bounds.

ChatGPT jobs are separate from the general API's IP-bound jobs. An opaque,
unguessable 256-bit job ID authorizes retrieval across different egress IPs.
Treat it as a private bearer token: do not publish IDs, cite them, or send them to
web search. There is no listing endpoint. Admission is capped at two active jobs
per resolved client IP and 32 retained jobs per process. Completed results expire
after ten minutes; memory is reclaimed on later job requests or shutdown. Storage
is capped at 8 MiB per job and 64 MiB across jobs. Admission reserves the full
per-job allowance before starting work, and rejects new searches when full.
Raw result input is capped at 16 MiB before parsing. Pages are serialized once
off the event loop; later reads serve their cached bytes. A job permits 60 reads
per minute and one outstanding pending poll; 429 includes Retry-After. These
limits apply across IP changes using that job ID. No automatic eviction makes
another researcher's live pagination disappear. The existing API mode gate still
applies to polling.
Restart loses jobs; deployment assumes one web process (multiple processes would
require shared storage or sticky routing). Existing general research jobs keep
their IP ownership rules unchanged.

## Create the GPT (owner setup, once)

1. Open [the GPT editor](https://chatgpt.com/gpts/editor) in your signed-in account.
2. Set name **GenizahSearch**.
3. Description: **Find Cairo Genizah manuscripts, read source evidence, and
   compare Hebrew and Judeo-Arabic passages with links to the manuscripts.**
4. Paste the contents of `instructions.md` into Instructions.
5. Add an Action. Authentication: **None**. Paste `openapi.json`, or import the
   schema URL above after deployment. Do not upload the schema as a knowledge file.
6. Verify five actions appear: `getGenizahCapabilities`, `searchManuscripts`,
   `browseManuscriptPage`, `findPassageParallels`, `getResearchJob`.
7. Add these conversation starters:
   - Find manuscripts containing this Hebrew phrase and show the matching text.
   - Show me ENA 1628.38 and explain what evidence is available.
   - Find parallels to this passage, allowing spelling differences.
   - חפש מקבילות לקטע הזה והצג סימני מדף וקישורים למקורות.
8. Run the tests in `acceptance-tests.md` in the Action tester and GPT preview.
9. Set the Action privacy policy URL to
   `https://genizahsearch.com/api/chatgpt/privacy` after deploying this update.
   Open it signed out to verify it loads. The policy is maintained in `privacy.html`;
   keep it aligned with actual hosting, logging and analytics practices.
10. Test a shared link from a separate researcher account before public distribution.

The instructions require source-specific credits inside generated files, including
the full MiDRASH citation from `shared/export_utils.py` when its automatic text is
used, and actual scholar/provider credits for PGP/FGP material. They also include
Web Search and Data Analysis guidance; enable these capabilities for external
scholarship and downloadable files. Replace the old Instructions text rather than
appending duplicate guidance. No Action schema reimport is needed for credits or
the privacy page.

No custom visual interface, Python execution, image generation, or knowledge-file
upload is required for the actions. An image link alone does not enable or
prove image inspection. ChatGPT may ask the user to allow API calls.

## Verify and maintain

```bash
python integrations/chatgpt/build_schema.py
pytest tests/test_chatgpt_api.py tests/test_chatgpt_jobs.py tests/test_chatgpt_pagination.py -q
python integrations/chatgpt/validate.py --captures scratch/chatgpt-smoke
# After deployment; these calls use public example manuscript text:
python integrations/chatgpt/smoke_test.py --api-prefix /api/chatgpt
```

Schema validation needs `jsonschema` and `openapi-spec-validator`; adapter tests
need the project's FastAPI, httpx, and pytest dependencies. The smoke test itself
uses only Python's standard library. It records latency, status, errors, warnings,
and size. Direct HTTP success is necessary but does not prove ChatGPT connectivity.
Broad fuzzy searches may still time out; test and report that behavior honestly.

The facade reuses this repository's API and research guidance; no code from
Yanir's unofficial MCP implementation was copied. A hosted MCP integration is a
separate follow-on, not part of this custom GPT pilot.

Official requirements checked 2026-09-14:
[GPT Actions setup](https://developers.openai.com/api/docs/actions/getting-started),
[timeouts, payload limits and read-only confirmation flags](https://developers.openai.com/api/docs/actions/production).
