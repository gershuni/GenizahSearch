# GenizahSearch ChatGPT pilot

Researchers open a shared custom GPT and ask questions. They need no Python,
local corpus, GenizahSearch API key, or software installation. GPT access and
sharing depend on their ChatGPT account and workspace policies; test the actual
target account before announcing availability.

## Status

Implementation prepared locally. **Not deployed or created in ChatGPT yet.**
The available browser is signed out (the GPT editor rendered a blank page), so
schema import, tool execution from ChatGPT, and external-user access remain unverified.

Validation: 10 targeted adapter tests passed; OpenAPI, request combinations, and
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
- The two registration lines added to `web/main.py`, immediately after
  `init_search_api(app_override=_search_helper_app, path_prefix="")`.
- `integrations/chatgpt/openapi.json`

After deployment the schema URL is
`https://genizahsearch.com/api/chatgpt/openapi.json`. It points to four new routes:
`/api/chatgpt/capabilities`, `/search`, `/browse`, and `/parallels` under the same
`/api/chatgpt` prefix. No API credentials or OpenAI API key are used by the facade.
The existing hardened handlers still enforce mode gates, rate limiting, validation,
and concurrency limits. Existing public endpoint contracts are unchanged.

The facade waits up to 40 seconds for the handler, then cancels its awaiting task
and reports an error. Cancellation does not guarantee that native work already
running in a thread stops immediately; existing backend controls still apply.
It returns at most 80,000 UTF-8 bytes, five main results, three filtered results,
and three complete matching spans per retained result. More rows can be omitted
to fit the byte budget. Oversized fixed envelopes/browse responses return an
explicit error. `total` and `request` keep their upstream values; `count` describes
the preview, and a `chatgpt_output_limited` warning reports upstream/returned counts.

These are pilot display limits, not changes to search algorithms. Search requests
are limited to 10 results (default 5); passage input to 5000 characters per witness
and three witnesses. The server validates these bounds too.

Do not route ChatGPT to the current background-job endpoints yet: job retrieval
is tied to the client IP and process, and stable ChatGPT egress across calls has
not been demonstrated. Larger/longer research needs a separately tested integration.

## Create the GPT (owner setup, once)

1. Open [the GPT editor](https://chatgpt.com/gpts/editor) in your signed-in account.
2. Set name **GenizahSearch**.
3. Description: **Find Cairo Genizah manuscripts, read source evidence, and
   compare Hebrew and Judeo-Arabic passages with links to the manuscripts.**
4. Paste the contents of `instructions.md` into Instructions.
5. Add an Action. Authentication: **None**. Paste `openapi.json`, or import the
   schema URL above after deployment. Do not upload the schema as a knowledge file.
6. Verify four actions appear: `getGenizahCapabilities`, `searchManuscripts`,
   `browseManuscriptPage`, `findPassageParallels`.
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
upload is required for the four actions. An image link alone does not enable or
prove image inspection. ChatGPT may ask the user to allow API calls.

## Verify and maintain

```bash
python integrations/chatgpt/build_schema.py
pytest tests/test_chatgpt_api.py -q
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
