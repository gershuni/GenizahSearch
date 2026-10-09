# GenizahSearch MCP server — plan

**Status:** Rev. 5, **Codex SHIP** (gpt-6-astra, round 5, 2026-10-08). All owner decisions
recorded 2026-10-09 (§10). Nothing built yet; next step is the spike (§7). Review history: §11.
**Deadline:** OpenAI retires custom GPTs on **Friday 2026-12-11**. The GenizahSearch GPT stops
working that day, along with the "Open GPT" link on `/ai` (`web/pages/ai_tools.py::GPT_URL`) and
the `/chatgpt` redirect (`web/main.py`, `chatgpt_redirect`).

## 1. Why an MCP server, and not "Migrate to plugin"

The GPT is its six custom actions over `/api/chatgpt/*`. OpenAI's migration copies a GPT's
instructions and files into a plugin but **drops custom actions**, and the migrated plugin starts
private (the migration dialog: "Plugins can't be shared publicly"). A migrated GenizahSearch plugin
would be private and unable to search.

OpenAI's stated replacement for an action is "a supported connector or custom MCP server". Public
distribution in ChatGPT now goes through OpenAI's reviewed **plugin submission** (developer
platform), and a plugin's tool calls must come from an MCP server (one per plugin). A remote MCP
server is also what Claude's connectors, and most other AI clients, use. So one server replaces
the GPT, and Claude users get it as well.

**Do not click "Migrate to plugin" now.** Migration makes the GPT read-only, and before
2026-12-11 we want to edit its description to point users to the new plugin. Not migrating costs
nothing: the GPT runs unchanged until retirement either way.

## 2. Decisions

| # | Decision | Recommendation | Why |
|---|---|---|---|
| D1 | Where it runs | **Inside the existing web process**, public URL exactly `https://genizahsearch.com/mcp` | The facade owns job state in this one web process (`web/chatgpt_jobs.py`, a process-local dict); the searches themselves run in worker subprocesses (`web/research_jobs.py`). Running in-process needs no new service, nginx unit or deploy step. Fallback if the MCP SDK's dependencies conflict with the locked stack: a separate small process that calls the facade over loopback; job state still lives in the web process, so ownership is unchanged. |
| D2 | How tools reach the corpus | **A thin adapter over the existing facade**, called in-process through `httpx.ASGITransport(app=<private wrapper around _search_helper_app>, client=(ip, 0))`, where `ip = _resolve_rate_limit_key(outer request)`. Internal paths are the sub-app's own (`/chatgpt/search/jobs`, …, **no `/api` prefix**). No forwarding headers are passed. | The facade's checks live in its handlers, not in outer middleware: mode gate, per-endpoint rate-limit buckets, request validation, research-job admission (2 active per client IP; 32 jobs / 64 MiB shared), per-job retrieval limits (60 reads/min, one pending poll). Background jobs deliberately skip the synchronous search-concurrency gate in favour of job admission and worker controls. One tested contract, no corpus logic in the adapter. |
| D3 | Transport | **Streamable HTTP, stateless, JSON responses**, **subscriptions disabled** (v2's server enables them by default, and `subscriptions/listen` always answers as an SSE stream, even in JSON mode). The endpoint is an **exact Starlette `Route('/mcp', StreamableHTTPASGIApp(manager), methods=['POST'])`** (the SDK's callable ASGI object, not its bound `handle_request` method), not a `Mount` (a `/mcp` mount matches `/mcp/` but not `/mcp`) and never a root mount that would shadow site routes. **`GET` and `DELETE /mcp` answer 405** from that method restriction: the legacy path opens a standalone SSE stream on GET regardless of JSON mode, we offer no server-initiated messages, and stateless mode has no session for DELETE to end (the spec lets a server answer 405 there). | Nothing to lose on restart, nothing for Cloudflare to buffer, every tool call is one short request. Long work already has its own job IDs. |
| D4 | Auth | **None** (public), same as the GPT action and the public API | The corpus is public. Both platforms explicitly allow it (§6). |
| D5 | UI widget | **None in v1** (tools only) | OpenAI: "Custom UI is optional". No iframe, so no CSP declarations. |
| D6 | Feature flag | **`MCP_ENABLED`, default `false`**, ANDed with a readiness check that the facade registered | House pattern for new public surfaces. Deploy unannounced, test in real clients, then submit. |
| D7 | Publisher identity | **Hillel Gershuni personally** (owner, 2026-10-09) | OpenAI individual verification with the owner's ID. The privacy policy, terms and support contact name him as operator, "with the support of Dicta" (as the About text already says), so all four required pages are visibly from the same publisher. |
| D8 | Result format | **One `text` content block holding the facade JSON; no `structuredContent`/`outputSchema` in v1** | The spec says a tool that returns `structuredContent` SHOULD also return the same JSON as text, which doubles a page. Text alone is read by every client. |
| D9 | Size bound | **≤ 100,000 bytes for the full HTTP response body, under BOTH protocol generations**, enforced on all six tools | The two SDK paths serialize differently: the legacy path writes UTF-8 (Pydantic); the 2026-07-28 path uses `json.dumps` with the default `ensure_ascii=True`, so every Hebrew character becomes a 6-byte `\uXXXX` and the text block's quotes are escaped again. Round 2 measured an 80,000-byte facade page at **239,113 bytes** (Hebrew-heavy) and **159,631 bytes** (quote-heavy) on the wire. So: (a) MCP-created jobs and MCP details requests use **serialization-aware page budgets**, measured as the worst-case escaped size (`ensure_ascii` plus the text block's second escaping), never raw bytes, because a stored page may later be polled under either generation. Roughly 33 KB of Hebrew-heavy UTF-8 per page, less envelope; pagination absorbs the smaller pages. Plumbing: `web/chatgpt_jobs.py::run()` reads `request.scope[API_CHANNEL_SCOPE_KEY]` **before** the pagination closure is handed to the thread and passes the budget to `build_pages` (which gains a `page_bytes` argument); `web/chatgpt_details.py::details(request)` reads it and applies it where the response page is selected, cached snapshots included. The GPT's 80 KB / 70 KB pages are unchanged. (b) A final adapter guard on every tool measures the response the way the negotiated protocol will serialize it (`ctx.protocol_version` is known before the tool returns), including the real request ID and the SDK-added response metadata; anything over the bound becomes an explicit `mcp_response_too_large` error, never a truncation. A GPT-created job polled through MCP hits (b). |

## 3. Architecture

```
ChatGPT / Claude ──HTTPS──> Cloudflare ──> nginx ──> uvicorn (web process)
                                                     ├── /mcp            web/mcp_server.py   (new; stateless)
                                                     │      │ in-process httpx.ASGITransport, caller IP as client,
                                                     │      │ private ASGI wrapper sets channel='mcp'
                                                     │      v
                                                     └── /api sub-app: /chatgpt/* facade -> existing hardened handlers
```

New files:

- `web/mcp_server.py`: server and tool definitions, adapter, private ASGI wrapper, flag/readiness.
  Registered in `web/main.py` as the exact `/mcp` route (D3), right after
  `app.mount("/api", _search_helper_app)` and before the site's page routes. The SDK's session
  manager is started inside the **host** app's lifespan (a mounted sub-app's lifespan does not
  run), alongside NiceGUI's existing startup/shutdown hooks, not replacing them.
- `integrations/mcp/`: README (setup in ChatGPT and Claude, submission notes),
  `acceptance-tests.md` (port of `integrations/chatgpt/acceptance-tests.md` plus §8's live cases),
  `smoke_test.py` (MCP client against production), `server_instructions.md`, and the OpenAI plugin
  package source (manifest, MCP configuration, `SKILL.md`).
- `tests/test_mcp_server.py` (tool contracts) and `tests/test_mcp_http.py` (through the real outer
  app; §8).

Changed: `web/framework_patches.py` (MCP session bypass), `web/chatgpt_api.py::_mark_chatgpt`
(keep an existing `mcp` marker), `web/api_hardening.py::api_channel` (recognize `mcp`/`mcp_job`),
`web/chatgpt_jobs.py::run()`, `web/chatgpt_pagination.py::build_pages` and
`web/chatgpt_details.py` (per-channel page budget, D9), `shared/transcription_credits.py::page_citation` (explicit no-transcription option, §5.3).

Unchanged: the public `/api/search|browse|parallels` contracts and the `/api/chatgpt/*` routes.
The GPT keeps working on them until 2026-12-11, and afterwards they stay as the MCP backend.

## 4. Tools

Annotations follow OpenAI's plugin guidelines (explicit booleans for `readOnlyHint`,
`destructiveHint`, `openWorldHint`; `readOnlyHint=false` for "starting stateful jobs or workflows,
queuing work"). Every tool also has a `title` (Anthropic requires it).

| MCP tool | Facade route (sub-app path) | readOnly | idempotent | Notes |
|---|---|---|---|---|
| `get_capabilities` | `GET /chatgpt/capabilities` | true | true | Modes, passage features, flags. Runtime errors override it. |
| `search_manuscripts` | `POST /chatgpt/search/jobs`, then `GET /chatgpt/jobs/{id}` | **false** | **false** | Starts a private, temporary background search (said so in the description: "no lasting effects; results expire after ten minutes"). Waits inside the tool up to ~30 s (three 10 s polls); returns page 0 if finished, otherwise `job_id` + state. |
| `find_passage_parallels` | `POST /chatgpt/parallels/jobs`, then poll | **false** | **false** | Same. `method=passage` is fixed by the tool. |
| `get_research_job` | `GET /chatgpt/jobs/{job_id}?page=&collection=` | true | true | Polling and pagination, unchanged semantics. |
| `browse_manuscript_page` | `GET /chatgpt/browse` | true | true | Adds `how_to_cite` (§5.3). |
| `get_manuscript_details` | `GET /chatgpt/manuscript-details` | **false** | true | Builds a ten-minute section snapshot and can answer `details_pending`; that is queued work under OpenAI's rule. Same section shares one snapshot, so a repeat is idempotent. |

All tools: `destructiveHint=false`, `openWorldHint=false` (the corpus is a bounded catalog, which
OpenAI's guideline allows to be `false`).

**ChatGPT will ask the user to confirm the first search in each conversation.** OpenAI's
custom-MCP guide: "Write actions by default require confirmation"; tools without `readOnlyHint`
are write actions; the user can remember the choice for the rest of the conversation, and "New
conversations will prompt for confirmation again." This is the cost of labelling the job-starting
tools honestly, and it applies to search, passage parallels and details. The dark deploy records
how the installed public plugin behaves (it may differ from developer mode). Appeals exist only
for annotation-review findings; they do not switch confirmations off. Tool descriptions say the
job is private, temporary and has no lasting effect, so the prompt reads as harmless.

**Errors** keep the facade's code, message and `Retry-After`, returned as `isError: true` results,
never as empty results ("Failures are errors, never 'no matches'").

**Retries are not deduplicated in v1.** OpenAI: tools should be safe to retry "or explicitly
indicate when retries may cause repeated effects". If a submission's response is lost during the
30 s wait, the job keeps running, and a retry starts a second one. That is bounded: two active jobs
per client IP (a third gets the facade's `research_jobs_busy` 429, which carries no `Retry-After`;
the adapter passes `Retry-After` through only when the facade sends one), and the orphan expires
ten minutes after it finishes. The tool description says so. Reusing a job for "same IP + same body"
was rejected in review: behind a shared egress IP it cannot tell a retry from another person, and
would hand that person the first caller's bearer token and polling allowance.

**Shared egress IPs.** ChatGPT and Claude call from their own cloud IPs, so many users share one
client IP, and per-IP limits apply across them. The GPT has the same exposure today. v1 measures
it (telemetry below) and promises no fix: MCP `clientInfo.name` is optional, caller-supplied, and
may be absent in stateless requests, so it is a diagnostic label only, never an admission key. If
contention shows up, the remedy is an owner decision made with that data.

**Telemetry.** The adapter's private ASGI wrapper sets `scope[API_CHANNEL_SCOPE_KEY] = 'mcp'` before
the facade runs. `_mark_chatgpt` keeps an existing `mcp` value instead of overwriting it, and
`api_channel` recognizes `mcp`/`mcp_job`. The marker is never derived from a header or a tool
argument; external callers cannot reach the wrapper. Because the facade emits no event for job
admission refusals, polls or details, the adapter records its own `mcp_tool_call` event per call:
tool, HTTP status, error code, duration, the **negotiated protocol version**, and the
`clientInfo.name` label if present.

**Mode meanings are an acceptance requirement.** In the spike ChatGPT guessed what each search mode
means, partly wrongly. The `search_manuscripts` description and the server instructions define every
mode in one line, from the implementation rather than the API schema (whose "variants
(morphological)" is itself misleading: the variant tiers are letter-confusion spellings). `responsa`
is the Responsa-Project query syntax and its expansion options, not a filter for responsa
literature.

## 5. Instructions and credits

The GPT's rules (`integrations/chatgpt/instructions.md`, 46 lines) must survive without a GPT:

1. **Tool descriptions carry the load-bearing rules.** Every client reads them: poll until
   complete, never resubmit, page with the same `job_id`, browse before quoting, failures are not
   empty results, never invent sys_ids/page numbers, link format
   `https://genizahsearch.com/browse?sys_id=…&page=…&volume_ie=…`.
2. **The MCP server `instructions` field carries the full research workflow** (port of
   `instructions.md`, minus ChatGPT-specific "Data Analysis"/"Web Search" wording). OpenAI's docs
   say ChatGPT uses these "alongside tool metadata" and advise putting the key points in the
   **first 512 characters**, so those go first: poll, don't resubmit, browse before quoting,
   errors aren't empty results, cite from `how_to_cite`.
3. **A page-level citation comes from the server.** Manuscript details already return
   bibliography citations and source credits (`shared/manuscript_details.py`), but no response
   gives a ready-made "how to cite this page", so the GPT pastes a hard-coded MiDRASH citation from
   its instructions. `browse_manuscript_page` adds `how_to_cite`, computed by
   `shared/transcription_credits.py::page_citation` (the single decision behind the printed sheet,
   the web chip, Word exports and the desktop), with library, shelfmark, folio, page URL and the
   retrieval date supplied by the adapter. Provenance is decided from browse's **`text_source`**
   (`shared/search_serializer.py`):
   - `pgp_transcription` → PGP credit, from `metadata.pgp.transcription_source` and the returned
     scholar/attribution fields;
   - `snippet` → the indexed automatic transcription, `version_info=None` (MiDRASH), **even when
     PGP metadata is present** on the page;
   - `none` → no text credit; the citation names the manuscript page only. **The helper cannot do
     this today**: `resolve_transcription_credit` sends `None`, `{}` and unknown sources to
     MiDRASH by design, and `page_citation` always adds the source clause. Build step: give
     `page_citation` an explicit keyword for "no transcription shown" that omits the clause, with
     the existing default unchanged and a test that the current callers' output is byte-identical.
   Browse never returns FGP text, so FGP stays covered by the helper's own regression tests, not
   by an adapter test. Adapter credit tests call the real helper, not a stub.
4. **Packaging.** The ChatGPT submission is a **plugin = the instructions as a skill + this MCP
   server** (OpenAI allows one MCP server per plugin), so the full workflow ships with it. On
   Claude, the directory lists the connector alone; bundling it with the `cairo-genizah-research`
   skill as a Claude plugin is an optional later step.

## 6. Platform requirements

Researched 2026-10-08 and checked again by the round-1 review. **[o]** = vendor documentation;
**[s]** = third-party or search snippet only. OpenAI's help center returns 403 to automated
fetches. Re-check every row against the live pages before submitting.

**OpenAI (ChatGPT): plugin submission** (`developers.openai.com/apps-sdk/deploy/submission`,
`developers.openai.com/plugins/plugin-guidelines`)

| Requirement | What we need | Src |
|---|---|---|
| Submitter | Org owner or a role with apps-management write; **individual or business verification** | o |
| Public pages, same publisher, HTTPS | **Privacy policy** (generalize `/api/chatgpt/privacy`), **terms of use (none exists)**, **support contact** (`/help` has a questions/bug-report/contact section: candidate), website URL | o |
| Package | Plugin ZIP: manifest, MCP configuration, `SKILL.md`; must pass the required automated scans; release notes | o |
| Icon | ≥ 48×48 PNG/JPEG/WebP/SVG | o |
| Test cases | **5 positive + 3 negative** MCP test cases; test credentials only for authenticated servers | o |
| Demo video | Required for MCP review | o |
| Domain verification | Challenge file at `/.well-known/openai-apps-challenge` | o |
| Annotations | Explicit `readOnlyHint`, `destructiveHint`, `openWorldHint`; justifications no longer required; flagged annotations can be appealed | o |
| Response content | "Only data that is directly relevant"; no size limit stated | o |
| Server instructions | Used "alongside tool metadata"; key points in first 512 chars | o |
| Review turnaround | **Not stated.** After approval, publishing is a separate explicit step | o |
| Adding a server by URL without review | Exists, subject to plan and workspace policy; sources **conflict** on which plans | s |

**Anthropic (Claude): connector directory** (`claude.com/docs/connectors/building/submission`)

| Requirement | What we need | Src |
|---|---|---|
| Submit | Developer portal (`claude.ai/directory/manage` → MCP connector). **Pro/Max: from your own account. Team/Enterprise: an Owner** (Enterprise: or a custom role with the Directory permission). **The listing belongs to the organization you submit from.** Free accounts cannot submit. No verified status needed for an authless server. Connectors are scanned and listed as Community by default; Anthropic may escalate to Verified review | o |
| Plugin bundle (optional) | Anthropic recommends also submitting a plugin bundle whose skills teach Claude to use the connector, from a **public GitHub repo**, paired with the connector when both come from the same organization | o |
| Auth | "No authentication for public data" is allowed | o |
| Tools | Every tool has a **`title`** and **`readOnlyHint` or `destructiveHint`** | o |
| Listing | Documentation URL; privacy-policy URL | o |
| Review | Automatic policy scan, possibly human review, no stated duration; escalation `mcp-review@anthropic.com` | o |
| Without review | A user can add a custom connector by URL: Free plan 1 connector, paid plans several | s |

**Google (Gemini): no public directory** (checked 2026-10-09; third-party guides only, Google's
own help page did not surface; verify before writing setup steps)

| Route | What it means for us | Src |
|---|---|---|
| Gemini app, "Add a custom app" | Users paste our `/mcp` address under Settings → Connected Apps (web only). Reported limits: **US only, English, personal Google account, paid tier** (AI Pro vs Ultra: sources conflict), Keep Activity on. No developer submission; named partners are business deals | s |
| Gemini CLI extensions gallery | Self-serve, free, unvetted: a public GitHub repo with a `gemini-extension.json` manifest pointing at our server. Audience: developers using the CLI | s |
| Gemini Enterprise / Business | Each organization's admin registers the server; no public listing | s |

**MCP protocol and SDK**

- Spec **2026-07-28** removes the `initialize` handshake and protocol-level sessions **[o]**. The
  Python SDK's v2 line serves both the new and the older protocol generations, and its server
  class and configuration differ from v1's `FastMCP` (SDK "What's new" and migration guide, cited
  by the round-1 review) **[o]**. The spike pins one concrete version and uses that API.
- Mounting inside an existing app: the host app's lifespan must start the SDK's manager; the
  default transport security accepts only localhost `Host` headers, so `genizahsearch.com` must be
  allow-listed (otherwise 421) **[s, SDK docs]**.
- Cloudflare's origin read timeout is **125 s** below Enterprise **[o]**, matching the 125.2 s
  give-up measured on 2026-09-08 (`web/search_api.py`, timeout ladder). Tool calls stay at
  ≤ ~30 s of waiting.

## 7. Work breakdown and dates

| By | Step | Exit check |
|---|---|---|
| Tue 10-13 | **Spike.** Pin a concrete SDK release (v2 line) and its server API; add to `requirements.txt`/lock; confirm it resolves against fastapi 0.135.1 / starlette 0.52.1 / pydantic 2.12.5 / nicegui 3.8.0. Register a one-tool server as an exact `Route('/mcp')`, manager in the host lifespan, `Host` allow-list set, subscriptions disabled; record which protocol generation each client actually sends. Call it from MCP Inspector and Claude Code locally. **Then real clients:** with owner approval, a temporary Cloudflare quick tunnel to the local server, added once in ChatGPT developer mode and once as a Claude custom connector, closed afterwards. | Inspector and both real clients get a real `get_capabilities` result; `run_local_tests.py` green. If the owner declines the tunnel, real-client compatibility is proven only at the dark deploy (10-28), and the plan says so. If deps conflict, switch to D1's fallback here. |
| Fri 10-23 | **Build.** All six tools with §4's annotations and descriptions; server instructions; `how_to_cite` with the helper's new no-transcription option (§5.3); flag + readiness; telemetry (§4); D9 serialization-aware page budgets and the final-response guard; `Host`/`Origin` checks. **Session bypass for `/mcp` in both** `_CacheSafeSessionMiddleware` **and** `_CacheSafeRequestTrackingMiddleware` (`web/framework_patches.py`), through a separate session-free predicate, not the public-cacheable-asset one. **Public pages:** privacy policy covering MCP, terms of use (new; Claude drafts, owner approves, §10.4), new `/support` page, `/.well-known/openai-apps-challenge`, icon. All name Hillel Gershuni as operator (D7). **Plugin package:** manifest, MCP configuration, `SKILL.md` from `instructions.md`; validate it locally. Docs: `docs/SEARCH_API.md` (contract doc; header date gated by `check_docs.py`), `integrations/mcp/README.md`, `docs/guides/ENV_VARS.md`, CLAUDE.md flag table. PR with Codex review. | §8's tests pass and each is shown able to fail; ruff clean; `check_docs.py` clean. |
| Wed 10-28 | **Deploy dark.** Web deploy with `MCP_ENABLED=1`, not announced. Run `integrations/mcp/acceptance-tests.md` in ChatGPT (Settings → Plugins → + → Add custom MCP server) and as a Claude custom connector (from a personal Pro/Max account or with an org Owner), English and Hebrew, **from each account type we will tell users to use** (at least ChatGPT Plus and Claude Free). Install the plugin package as a private plugin and run the same tests through it. `smoke_test.py` against production. | Every acceptance case recorded pass/fail with transcripts, including whether ChatGPT asks to confirm each search; Cloudflare not challenging OpenAI/Anthropic egress. |
| Fri 10-30 | **Submit.** OpenAI: plugin package, 5 positive + 3 negative test cases from the acceptance tests, demo video from the dark deploy, domain verification, release notes. Anthropic: connector listing with documentation URL. | Submission confirmations; automated scans passed. |
| Nov | Review fixes as they come. Buffer: ~6 weeks before retirement. **After approval, publish explicitly** (approval alone does not list it). | Listed and reachable by search in both directories. |
| Nov | **Gemini (best effort, owner 2026-10-09).** Publish a Gemini CLI extension (public repo with `gemini-extension.json` pointing at `/mcp`) to its gallery. Run the acceptance tests in the Gemini web app only if someone with an eligible account (US, personal, paid tier) can do it; otherwise the `/ai` Gemini steps say "reported to work" and name the eligibility limits. | Extension listed; Gemini app result recorded or marked untested. |
| Fri 12-04 | **Switch surfaces.** `/ai`: replace the GPT card with the ChatGPT plugin and the Claude connector, plus the MCP URL and Gemini setup steps; drop the "MCP · Planned" badge. `/chatgpt` redirect → the plugin listing (or `/ai` if not yet approved). Edit the GPT's description to point to the plugin. Announce. | Links checked signed-out. |
| Fri 12-11 | GPT retires. Remove `GPT_URL`. | — |

**Spike progress (2026-10-09, branch `feat/mcp-server`, uncommitted).** Local half done:
`mcp==2.3.0` pinned (first release with `subscriptions=False`; adds only new packages plus `idna`
3.11 → 3.20; FastAPI/Starlette/Pydantic/NiceGUI unchanged; `pip check` clean). `web/mcp_server.py`
serves one tool (`get_capabilities`) from an exact POST-only `/mcp` behind `MCP_ENABLED`. Measured:
GET/DELETE → 405, `/mcp/` → 307, `/mcp/mcp` → 404, wrong `Host` → 421, a later site route still
answers; the facade sees the caller's IP; both protocol generations (2025-11-25 handshake and
2026-07-28) list and call the tool against the real local app with Hebrew/quotes intact; `/` and
`/api/chatgpt/capabilities` unaffected; no server errors. `/mcp` still sets a session cookie
(build step). Full local suite: 14,769 passed, 2 failed — the outbound-call guard (fixed: the
in-process call is listed in its `EXEMPT` with a reason) and the known batch-only flake
`test_discard_run_short_circuits_sqlite_on_tantivy_failure` (OPEN_ISSUES, passes alone).

**Real clients (2026-10-09, temporary `cloudflared` quick tunnel with
`--http-host-header localhost:8084`, closed and the binary deleted afterwards):**
- **ChatGPT: works.** Added at Settings → **Plugins** → **+** → **Add custom MCP server** (the old
  "Apps & Connectors / Developer mode" path no longer exists; use this in §7 and on `/ai`). It
  listed and called `get_capabilities` and answered from the real payload. **No confirmation
  prompt** for the read-only tool, as documented. It *guessed* what each search mode means
  ("likely purposes"), partly wrongly (e.g. `responsa`), because capabilities lists mode names
  only → build step: each tool description and the server instructions give a one-line meaning
  per mode. (Passage search showed "not supported" because the laptop runs with passage flags
  off; production has them on.)
- **claude.ai: not testable from the owner's account** — it belongs to an organization, and
  "Ask an organization owner to add this for your team". Consequences: (a) `/ai` setup steps
  must say that Team/Enterprise members need their org owner; (b) **directory submission: only
  an Owner can submit from a Team/Enterprise account, and the listing belongs to that
  organization** (§6). Under D7 (personal publisher) that means a personal Pro/Max account —
  owner decision pending. The claude.ai client stays an explicit untested gate for the dark deploy.
- **Claude Code (Anthropic's MCP client): works** — a one-off headless run with a temporary
  `--mcp-config` (no persistent config) called `get_capabilities` through the tunnel and returned
  the six modes and API 9.6.0. Evidence for Anthropic's client, not for the claude.ai web client.
- Which protocol generation ChatGPT used is not visible in the server log → the `mcp_tool_call`
  event (§4) records the negotiated protocol version.

**Build order** (Codex consultation on the spike, 2026-10-09; spike code verdict SHIP WITH
FIXES). `MCP_ENABLED` stays off by default throughout. Each commit carries its own §8 tests **and
their mutations with the recorded red result**, not saved for the end.

1. **Isolated registration + transport tests.** Bind the facade per server (the spike's module
   global let a second registered app answer for the first); validate `Host`/`Origin` before the
   slash redirect; spell out bare and default-port host forms (`localhost`, `[::1]`,
   `genizahsearch.com:443`). New `tests/test_mcp_http.py`: flag/readiness 404s, exact route, 307,
   404, 405s, subscriptions, allowed/rejected hosts, lifespan enter/exit, two-app isolation.
2. **No sessions for MCP.** Separate session-free predicate in both middleware classes
   (`web/framework_patches.py`), covering `/mcp` and its slash redirect, not the cacheable-asset
   one. Tests: no cookie and no user storage on success, error and redirect; browser sessions
   unchanged.
3. **Error and telemetry contract.** Tool errors become one JSON text block with code, message,
   original status and `Retry-After` when present, `isError: true` (the spike's `ToolError` kept
   them only inside SDK-prefixed text); private channel wrapper, `_mark_chatgpt` preserves `mcp`,
   `api_channel` knows `mcp`/`mcp_job`, one `mcp_tool_call` per call with the protocol version.
   GPT channels unchanged.
4. **D9 size budgets** before any large result is exposed: channel read in `run()` before the
   thread handoff, `build_pages(page_bytes)`, details budget at page selection (cached snapshots
   included), storage accounting in real bytes, final guard incl. request ID and metadata. GPT
   output byte-identical.
5. **Citations + browse tool:** `page_citation` no-transcription keyword (default unchanged,
   existing callers byte-identical), `how_to_cite` from `text_source`, browse tool.
6. **The other four tools + instructions:** search, passage parallels, job retrieval, details;
   descriptions with mode meanings; `integrations/mcp/server_instructions.md`; polling, pending
   IDs, pagination, capacity limits, cancellation, shutdown; the all-six-tools wire-size matrix.
7. **Public pages and assets:** privacy generalized (old GPT URL kept), terms (only after the
   owner approves the draft), `/support`, icon, `/.well-known/openai-apps-challenge`.
8. **Package, document, close the Build gates:** plugin manifest + MCP config + skill, README,
   acceptance tests, smoke client, `docs/SEARCH_API.md`, `ENV_VARS.md`, CLAUDE.md; full §8 suite,
   ruff, `check_docs.py`. Live client tests wait for the dark deploy.

Commits 1-3 touch shared code only behind the flag or additively; 4 and 5 change code the live GPT
uses, so their "GPT output byte-identical" tests are the gate.

**Build progress (2026-10-09, branch `feat/mcp-server`, not pushed):** commits 1-6 done —
`07dd2939` `b40aeea7` `4fa217fd` `c6f398dd` `df877fb6` `46cec34e` — each with its mutations
recorded RED in its message. Codex reviewed 1-4 (SHIP WITH FIXES → `b60dc93b`), 5-6 (SHIP WITH
FIXES, no citation defect → `82c43c80`), then verified the fixes (three conditional LOW in the reply
cap → `770dbe8a`) and returned **SHIP** on `770dbe8a`. Beyond the plan, the build added: a request
guard (64 KiB body, id/name/undeclared-argument checks, strict integers) and a reply cap on every
response, both in front of the SDK. MCP + GPT facade tests: 199 passed. **Commit 7 waits on the
owner:** terms text (governing law; the full-corpus-access line), the privacy-policy extension,
and a directory icon.

**If OpenAI's review isn't done by 12-04:** `/ai` gives the MCP URL with setup steps for the
account types that passed the dark-deploy fallback test, and says plainly which accounts cannot
add a custom server. It does not promise access to everyone.

## 8. Tests and gates

Contract tests (SDK in-memory client → adapter → the real `/api` sub-app with captured facade
responses):

- Each tool hits the right sub-app route with the right parameters; errors keep code, message and
  `Retry-After` and set `isError`; a pending job returns its `job_id`; pagination passes through.
- **Credits** (§5.3), on real browse shapes and the real helper: a `pgp_transcription` page
  credits its PGP scholars and not MiDRASH; a `snippet` page that also carries PGP metadata credits
  MiDRASH and not PGP; a `none` page carries no text credit; missing attribution fields do not
  invent a name; the helper's existing callers produce byte-identical output.
- **Telemetry:** `channel` is `mcp`/`mcp_job` for MCP calls, still `chatgpt` for GPT calls, and an
  external request with any header or argument claiming `mcp` is not counted as MCP.
- **Saturation:** with the shared store full (32 jobs or 64 MiB), a submission returns an
  `isError` result with the facade's code, not an empty result.
HTTP tests through the real outer app, its middleware and its lifespan (pattern:
`tests/test_static_asset_sessions.py`):

- **Size, on the wire:** for every tool, under both protocol generations, Hebrew-heavy and
  quote-heavy fixtures at the page budget's edge produce HTTP bodies ≤ 100,000 bytes; one row over
  produces `mcp_response_too_large`, not a truncated page; a GPT-sized job page polled through MCP
  produces the same explicit error.
- Flag on, checked **without** following redirects: `POST /mcp` → 200; `/mcp/` → 307 to `/mcp`;
  `/mcp/mcp` → 404; site routes after `/mcp` still answer.
- **Host/Origin are validated before routing can redirect**: `POST /mcp/` with a disallowed `Host`
  → 421, never a 307 to that host (the spike redirected to `http://evil.invalid/mcp`); with an
  allowed host, the 307 `Location` is `https://` behind the proxy.
- `GET /mcp` and `DELETE /mcp` → 405, and a `subscriptions/listen` request cannot hold open an
  SSE stream.
- Flag off ⇒ 404; flag on with the facade not registered ⇒ 404.
- A disallowed `Host` or `Origin` is rejected.
- No `Set-Cookie` on `/mcp` responses, and no NiceGUI user storage is created.
- Behind the same proxy-header setup as `tests/test_api_hardening_behind_cloudflare.py`, the
  facade sees the visitor IP, not `127.0.0.1` or the edge.
- Cancelling the MCP request during the 30 s wait leaves the job running and retrievable.
- App shutdown cancels running jobs and stops the SDK manager cleanly.

**Able to fail:** for each test, break the guarded thing on purpose and record red: remove the
request-tracking bypass, drop the `Host` allow-list, use a `Mount` instead of the exact route,
re-enable subscriptions, drop `methods=['POST']` from the route, hard-code the client IP, let `_mark_chatgpt` overwrite `mcp`, swallow an
error into an empty list, size MCP pages by raw UTF-8 bytes instead of escaped size, credit PGP on
a snippet page, let `none` fall through to MiDRASH.

**Live:** `integrations/mcp/smoke_test.py` against production after deploy, and the acceptance
transcripts from both clients and from the installed plugin. A green direct-HTTP smoke is
necessary, not sufficient.

## 9. Risks

| Risk | Mitigation |
|---|---|
| OpenAI review slower than 6 weeks, or rejects | Account types verified in the dark deploy get URL setup steps on `/ai`; Claude listing is independent of OpenAI. |
| ChatGPT asks the user to confirm the first search, parallels or details call in each conversation (documented default for `readOnlyHint=false`) | Accepted cost of honest labels; descriptions state the job is private and temporary; installed-plugin behavior recorded in the dark deploy. |
| Hebrew responses triple in size under the 2026-07-28 serializer | Serialization-aware page budgets plus a final guard on every tool, tested on the wire under both protocol generations (D9). |
| Shared egress IPs hit per-IP limits (§4) | Measured from day one (`mcp_tool_call` events include refusals); no promised fix in v1. |
| The shared job store (32 jobs / 64 MiB) fills while the GPT and MCP overlap until 12-11 | Saturation surfaces as a coded error (tested); `mcp_tool_call` shows how often; limits are revisited with data. |
| A lost submission response leads to a duplicate job | Not deduplicated (§4); bounded by two active jobs per IP and ten-minute expiry; disclosed in the tool description. |
| A client underweights server `instructions` | Rules repeated in tool descriptions and carried in results (`how_to_cite`, warnings); the ChatGPT plugin carries them as a skill. |
| The SDK API differs from what the plan assumes | Spike pins the release and verifies both real clients before tool code. |
| No terms page today blocks OpenAI submission | Written in the build step, approved by the owner (public legal text). |
| Cloudflare bot rules or WAF block a client's egress | Tested from the dark deploy before submitting; allow-rule `/mcp` if needed. |
| Web process restarts lose running jobs | Already true for the GPT; the facade says "lost after a server restart, not an empty result". |
| A third-party MCP wrapper already exists (the 2026-09-08 incident) | Tell its author when the official server is live; `docs/SEARCH_API.md` header must be current (gated). |

## 10. Owner decisions

Answered 2026-10-09:

1. **Publisher:** Hillel Gershuni personally (D7).
2. **Directories:** ChatGPT and Claude, **plus Gemini where possible** (no Gemini directory
   exists; CLI gallery + setup steps, §6, §7).
3. **Citation:** `how_to_cite` in the MCP browse tool only for v1; the public `/api/browse` may
   follow after 2026-12-11.
4. **Terms of use and support:** support address **gershuni@gmail.com** (already on `/help` and
   the privacy page); a new short **`/support` page** (contact address, how to report a wrong or
   failed AI answer, links to `/help` and `/ai`) is the listings' support URL. **Terms of use:
   Claude drafts in plain English, owner edits and approves before anything is published.** They
   cover: AI answers can be wrong (check the manuscript and catalogue); transcriptions, editions
   and images keep their makers' and libraries' rights (MiDRASH, PGP, FGP, holding libraries);
   usage limits apply and bulk scraping is not allowed; provided free and as is, may change or
   stop. Governing law (Israel or none): ask at draft review.
6. **Claude directory account (2026-10-09):** the owner submits from his **personal** Claude
   Pro/Max account (not the organization account he works in), so the listing is his, matching
   D7. The same account runs the claude.ai acceptance tests at the dark deploy.
5. **Spike tunnel:** approved, about an hour. Installing `cloudflared` (not on this machine) is a
   download that is asked for separately at spike time.

## 11. Review log

**Round 1 (Codex gpt-6-astra, 2026-10-08): SHIP WITH FIXES, 4 HIGH / 11 MED / 4 LOW.** Changes:
citation provenance now from `text_source` with `metadata.pgp.transcription_source` (§5.3);
D8/D9 drop the doubled `structuredContent` and bound the whole serialized response; job-starting
tools and details annotated `readOnlyHint=false`, `idempotentHint=false` where they enqueue (§4);
session bypass in both middleware classes via a separate predicate (§7); internal sub-app paths
and the `/mcp` inner path specified (D2, D3); telemetry via a private wrapper plus `mcp_tool_call`
events (§4); guards described accurately (D2); `clientInfo` demoted to a diagnostic label (§4);
HTTP-level tests through the outer app and their mutations (§8); FGP adapter test replaced by real
browse shapes (§5.3, §8); plugin ZIP, scans, release notes, installed-package test and explicit
publish (§7); SDK v2 pinned in the spike, real clients verified (§7); job-store saturation and
lost-submission risks plus safe retry (§4, §9); fallback restricted to verified account types
(§7, §9); "no citation in any payload" narrowed (§5.3); `/help` as support candidate (§6, §10);
D1 wording on job ownership vs worker subprocesses; Cloudflare 125 s.

**Round 2 (2026-10-08): SHIP WITH FIXES.** Round-1 items: 15 resolved, 4 partial. New: 2 HIGH,
3 MED, 1 LOW. Changes: same-IP/same-body job reuse **removed** (would hand a bearer token to
another person behind a shared egress IP); retries disclosed and bounded instead (§4, §8, §9).
D9 rewritten after the review measured 80,000 → 239,113 bytes for a Hebrew page under the
2026-07-28 serializer (`json.dumps`, default `ensure_ascii`): serialization-aware page budgets
for MCP jobs and details, a final guard on all six tools, wire-level tests under both protocol
generations (D9, §8). `page_citation` gains an explicit no-transcription option, since `none`
otherwise falls through to MiDRASH (§5.3). Exact `Route('/mcp')` instead of a `Mount` (D3).
Subscriptions disabled, with a gate (D3, §8). ChatGPT's documented confirmation default for
non-read-only tools stated; appeals scoped to annotation findings (§4, §9). Two vendor claims
re-checked by Claude against the SDK source and OpenAI's custom-MCP guide before folding in.

**Round 3 (2026-10-08): SHIP WITH FIXES.** Round-2 items: 9 resolved, 1 partial. New: 1 LOW.
Changes: `GET /mcp` → 405, since the legacy path opens SSE on GET regardless of JSON mode (D3, §8);
the `research_jobs_busy` 429 has no `Retry-After`, so it is passed through only when present (§4,
checked in `web/chatgpt_jobs.py`); exact route uses `StreamableHTTPASGIApp(manager)` (D3); D9
plumbing named (channel read in `run()` before the thread handoff, `build_pages(page_bytes)`,
details at page selection incl. cached snapshots; stored pages sized for the worst case; final
guard uses `ctx.protocol_version`, real request ID and SDK metadata); `chatgpt_jobs.py` added to
changed files (§3); `/mcp/` → 307 and `/mcp/mcp` → 404 stated as expected statuses (§8).

**Round 4 (2026-10-08): SHIP WITH FIXES.** Round-3 items: 5 resolved, 1 partial; 1 LOW. Change:
the route is restricted to `methods=['POST']`, which is what produces the promised 405 for `GET`
(and `DELETE`); test and mutation updated (D3, §8).

**Round 5 (2026-10-08): SHIP.** Round-4 LOW resolved; no new findings. Codex confirmed in
Starlette 0.52.1 that the restricted route returns 405 for GET/DELETE and that removing the
restriction forwards both; DELETE → 405 is permitted by the 2025-11-25 spec and recommended by
2026-07-28.

**Build consultation (Codex, 2026-10-09, on spike commit 59e731ec): SHIP WITH FIXES.** Spike-code
findings folded into the build order (§7): error metadata reduced to text (MED), slash redirect
before Host/Origin validation (MED, reproduced by Claude: `307 → http://evil.invalid/mcp`), MCP
creating browser sessions (MED, planned build item), MCP not yet distinguishable from GPT traffic
(MED, planned), module-global facade (LOW), host spellings (LOW), no committed MCP tests (MED).
Confirmed sound: the lifespan nesting (NiceGUI startup → MCP manager → serve → manager exits →
NiceGUI shutdown), route placement, and per-call ASGI transports. Plan changes: Anthropic
submission eligibility (§6; verified by Claude on claude.com/docs/directory/publish), mode
meanings as an acceptance requirement and protocol version in telemetry (§4), the redirect/Host
test (§8), the ChatGPT Plugins path (§7).
