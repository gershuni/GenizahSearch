# 0010 -- The data API is not cached by Cloudflare

- **Status:** binding
- **Date:** 2026-10-20
- **Source:** owner decision ("Not worth it", 2026-10-20) on the API part of the
  `docs/OPEN_ISSUES.md` row "Cloudflare caches no page or API response". It followed an
  investigation with a working prototype: branch `claude/api-cdn-cache-prototype`, commit
  `95cd351`.

## Decision

No `/api/*` data route skips the session middleware or gets a cacheable `Cache-Control` so
that Cloudflare can keep it. The only `/api/*` routes Cloudflare may cache stay the six
library image proxies (`_is_public_image_route`, PR #409). The rest of the tracker row
(page HTML, `/robots.txt`, `/sitemap.xml`, the atlas manifest, 404s) is unaffected.

## Why

A CDN cache cannot reach the expensive or popular calls:

- **Search and parallels are POST.** Cloudflare never caches POST. `POST /api/search`,
  `POST /api/parallels`, `POST /api/chatgpt/{search,parallels}` and every `.../jobs` submit
  have no GET form.
- **MCP never crosses Cloudflare for data.** `web/mcp_server.py::_facade` calls the `/api`
  sub-app in process (`httpx.ASGITransport`); only `POST /mcp` itself is an HTTP request.
- **ChatGPT** crosses Cloudflare only for `GET /api/chatgpt/browse`, `/capabilities` and
  `/manuscript-details`. Its searches are POST jobs; job polling and manuscript-details
  (snapshot paging) are already `no-store` and must stay so.
- That leaves `GET /api/browse` and `/api/chatgpt/browse` as the only worthwhile candidates,
  for repeat lookups of the same page. The repository has no traffic figures showing that
  repetition, and the caching would add a purge step to every data deploy.

## What was learnt

**Measured on a local run (2026-10-20, before the prototype):** every `/api/*` GET checked
(`/api/capabilities`, `/api/browse`, `/api/openapi.json`, `/api/chatgpt/capabilities`,
`/api/visual_suggestions/version`, `/api/fl_ids/{id}`) and `/robots.txt` answered with
`set-cookie: session=...` and no `Cache-Control`, even when the request already carried a
cookie.

**Route inventory.** Session/language dependence was read from the code, not measured:

| Route | Method | Depends on the visitor? | Verdict |
|---|---|---|---|
| `/api/search`, `/api/parallels`, `/api/chatgpt/search`, `/api/chatgpt/parallels` | POST | no | cannot be cached (POST) |
| `/api/{search,parallels}/jobs`, `/api/research/jobs/*`, `/api/chatgpt/*/jobs`, `/api/chatgpt/jobs/{id}` | POST/GET/DELETE | owned by one caller | never (already `no-store`) |
| `/api/browse`, `/api/chatgpt/browse` | GET | no: query string only (D-22 statelessness); rate limited 120/min | safe, but only complete answers (see the caveats below) |
| `/api/capabilities`, `/api/chatgpt/capabilities` | GET | no, but reports live flags, limits and concurrency that change without a restart | at most a short lifetime; cheap anyway |
| `/api/chatgpt/manuscript-details` | GET | snapshot paging; page budget differs for MCP | never (already `no-store`) |
| `/api/openapi.json`, `/api/docs`, `/api/redoc`, `/api/chatgpt/openapi.json`, `/api/chatgpt/privacy` | GET | no | safe; negligible value |
| `/api/fl_ids/{id}`, `/api/oxford_images/{id}`, `/api/oxford_image_url/{id}`, `/api/puzzle_folios/{id}`, `/api/visual_suggestions/{id}`, `/api/visual_suggestions/version` | GET | no | safe in principle; called by the site's own pages, not by API clients |
| `/api/oxford_debug`, `/api/browse_debug/{id}` | GET | no | debug output; not cached |
| `/api/visual_suggestions/batch_check`, `/api/puzzle_process`, `/api/puzzle_export` | POST | the puzzle routes are per visitor | never |
| `/api/puzzle_image`, `/api/puzzle_ext_image`, `/api/proxy_image` | GET | puzzle uploads belong to one browser | never |
| `/api/export/*` | GET | the visitor's stored results, language, sign-in | never |

**Caveats any future attempt must handle.** The prototype implements and tests all of them:

- **Partial answers.** A browse answer with an `enrichment_timeout` or `enrichment_failed`
  warning (`shared/browse_service.py`) must be `no-store`, or a passing outage is served
  from cache.
- **Errors.** With a Cache Rule and no origin `Cache-Control`, Cloudflare applies its default
  edge lifetime to some statuses (a 404 for minutes). Every non-success answer on a cached
  path must therefore say `no-store`: the 400s, 404s, the 429 and the
  `SEARCH_API_MODE` 403/503. The prototype sets this as a fail-closed default in
  `_CacheSafeRequestTrackingMiddleware` for those paths.
- **Side effects of a cache hit.** It skips the per-IP rate limiter and the PostHog
  `capture_api_event` (it shows only in Cloudflare analytics). It also outlives a
  `SEARCH_API_MODE` switch-off and a data redeploy until it expires or is purged.
- **The cache key.** The Cache Rule must name exact paths (they have no file extension), never
  a wildcard over `/api/*`. The query string is part of the key, so the same parameters in a
  different order are a separate entry.

**The prototype** (`claude/api-cdn-cache-prototype`) does the following:

- adds `_is_public_api_read_route` in `web/framework_patches.py`;
- adds `API_BROWSE_HTTP_MAX_AGE` (3600) in `web/search_api.py`;
- adds `tests/test_api_route_sessions.py` (19 tests in the style of
  `tests/test_image_route_sessions.py`).

Two mutation checks were run: removing the route check failed 15 of the tests, and removing
the `no-store` default failed 8. The live success path was not exercised; the cloud container
had no corpus.

## Consequences

- Do not reopen API caching for search, parallels or MCP performance: a CDN cannot reach
  them. Speed there comes from the server side (the search engine and job workers).
- Reopen browse caching only with traffic evidence: Cloudflare analytics or PostHog `browse`
  events showing the same URL repeated, by channel. If that evidence appears, start from the
  prototype branch and decide its lifetime, the purge step on data deploys, and whether a
  cache hit may bypass the rate limiter.

## Supersedes

Nothing. It narrows the open tracker row to the page-level responses.

## Enforced by

Convention. `tests/test_image_route_sessions.py` already asserts that `/api/search` and
`/api/oxford_image_url/{id}` keep their session.
