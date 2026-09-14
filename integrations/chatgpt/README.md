# GenizahSearch ChatGPT pilot

Researchers open a shared custom GPT and ask questions. They need no Python,
local corpus, GenizahSearch API key, or software installation. GPT access and
sharing depend on their ChatGPT account and workspace policies; test the actual
target account before announcing availability.

## Status

The owner deployed and used the original synchronous GPT pilot. Search worker
startup/queue time then triggered 504s, including the exact-search 30-second
deadline. Version 0.2 uses background search jobs and short polling requests.
The asynchronous upgrade still needs deployment and testing inside ChatGPT.

To upgrade: deploy, re-import `https://genizahsearch.com/api/chatgpt/openapi.json`
in the GPT's Action editor, replace Instructions with `instructions.md`, and save.
Confirm the fifth action `getResearchJob` appears. Merely deploying the server
does not replace the schema saved in an existing GPT.

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
- The registration and shutdown lines added to `web/main.py`, immediately after
  `init_search_api(app_override=_search_helper_app, path_prefix="")`.
- `integrations/chatgpt/openapi.json`

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
It returns at most 80,000 UTF-8 bytes, five main results, three filtered results,
and three complete matching spans per retained result. More rows can be omitted
to fit the byte budget. Oversized fixed envelopes/browse responses return an
explicit error. `total` and `request` keep their upstream values; `count` describes
the preview, and a `chatgpt_output_limited` warning reports upstream/returned counts.

These are pilot display limits, not changes to search algorithms. Search requests
are limited to 10 results (default 5); passage input to 5000 characters per witness
and three witnesses. The server validates these bounds too.

ChatGPT jobs are separate from the general API's IP-bound jobs. An opaque,
unguessable 256-bit job ID authorizes retrieval across different egress IPs.
Treat it as a private bearer token: do not publish IDs, cite them, or send them to
web search. There is no listing endpoint. Admission is capped at two active jobs
per resolved client IP and 32 retained jobs per process. Completed results expire
after ten minutes; memory is reclaimed on later job requests or shutdown. Only
bounded results are retained. The existing API mode gate still applies to polling.
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
9. Save privately for the pilot. Test a shared link from a separate researcher
   account before distributing it. If public/link sharing requires a privacy URL,
   supply an accurate published policy covering this integration. The site's
   `/privacy-extension` page is for a different product and should not be reused.

No custom visual interface, Python execution, image generation, or knowledge-file
upload is required for the actions. An image link alone does not enable or
prove image inspection. ChatGPT may ask the user to allow API calls.

## Verify and maintain

```bash
python integrations/chatgpt/build_schema.py
pytest tests/test_chatgpt_api.py tests/test_chatgpt_jobs.py -q
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
