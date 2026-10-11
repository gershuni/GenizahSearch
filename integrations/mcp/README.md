# GenizahSearch MCP server

A public, authless [MCP](https://modelcontextprotocol.io) server at `https://genizahsearch.com/mcp`
for ChatGPT (as a plugin), Claude (as a connector), Gemini and other MCP clients. It replaces the
GenizahSearch custom GPT, which stops working when OpenAI retires custom GPTs on 2026-12-11.

Plan, decisions and review history: [`docs/plans/MCP_SERVER_PLAN.md`](../../docs/plans/MCP_SERVER_PLAN.md).
Contract: the "MCP server" section of [`docs/SEARCH_API.md`](../../docs/SEARCH_API.md).

## How it works

`web/mcp_server.py` registers an exact `POST /mcp` route on the web app when `MCP_ENABLED=1` and the
`/api/chatgpt` facade is registered. Its seven tools call that facade in-process as the visitor who
called them, so the facade's mode gate, rate limits, validation and research jobs apply unchanged. No
corpus logic lives in the adapter.

In front of the SDK a request guard refuses oversized or malformed requests (64 KiB body, ids,
names, undeclared arguments) and a reply cap keeps every reply within 100,000 bytes. Pages built for
MCP are sized by their escaped form (`web/chatgpt_pagination.py`), because the 2026-07-28 serializer
escapes non-ASCII. `/mcp` never touches browser sessions (`web/framework_patches.py`).

| File | What it is |
| --- | --- |
| `server_instructions.md` | The research workflow sent to clients; the rules that matter are in the first 512 characters. |
| `acceptance-tests.md` | Prompts to run in each real client after a deploy, and the submission test cases. |
| `smoke_test.py` | An MCP client run against a deployment. |
| `terms.html`, `support.html` | Served at `/terms` and `/support` (`web/public_pages.py`). |
| `TERMS_DRAFT.md` | The owner-approved terms text; `tests/test_public_pages.py` checks `/terms` carries it word for word. |
| `plugin/openai/` | The ChatGPT plugin manifest (`plugin.json`) and server reference (`mcp.json`). |
| `plugin/claude/` | The Claude plugin bundle (`.claude-plugin/plugin.json`, `.mcp.json`, skill, README). |
| `plugin/gemini/` | The Gemini CLI extension. |
| `build_packages.py` | Generates the skills from `server_instructions.md`, checks the manifests (see its docstring for exactly what), builds the ChatGPT plugin ZIP, and exports the separate distribution repository. |

The Claude and Gemini packages are published from a **separate small public repository**, not from
this one: the Gemini gallery reads `gemini-extension.json` only at a repository root, and the Claude
directory validates the whole repository (under 50 MiB archived, 256 MiB unpacked), which this
repository far exceeds. `build_packages.py --export DIR` writes that repository: the Gemini extension
at its root and the Claude plugin in `claude-plugin/`.

The privacy policy is `integrations/chatgpt/privacy.html`, served at `/ai/privacy` and, for the GPT's
existing link, `/api/chatgpt/privacy`. The directory icon is `web/static/genizahsearch-icon-512.png`.

## Deploy

1. Install the pinned SDK with the rest of the lock: `mcp==2.3.0` (2.3.0 is the first release with
   `MCPServer(subscriptions=False)`).
2. Set `MCP_ENABLED=1` and restart the web service. Without it `/mcp` is a plain 404.
3. Check from outside: `python integrations/mcp/smoke_test.py https://genizahsearch.com`.
4. Cloudflare and nginx: `/mcp` is ordinary POST JSON (no SSE). The three job tools wait about 30 s
   (three facade polls of up to 10 s); `get_research_job` and `get_manuscript_details` wait up to
   10 s; `browse_manuscript_page` and `get_capabilities` run under the facade's 40 s deadline. Every
   call stays inside Cloudflare's 125 s. Make sure no bot rule challenges OpenAI's or Anthropic's
   servers on `/mcp`.

## Connecting a client

- **ChatGPT:** Settings → Plugins → **+** → *Add custom MCP server*; URL `https://genizahsearch.com/mcp`,
  no authentication. ChatGPT asks the user to confirm the first search, passage search or details call
  in each conversation (those tools start server-side jobs, so they are honestly not read-only).
- **Claude (claude.ai):** individuals: Customize → Connectors → *Add custom connector*, same URL.
  In a Team or Enterprise organization an Owner adds it under Organization settings → Connectors.
- **Claude Code:** `claude mcp add --transport http genizahsearch https://genizahsearch.com/mcp`.
- **Gemini app:** Settings → Connected Apps → *Add a custom app* (web only; reported to need a US,
  personal, paid account).

## Verify

```bash
python -m pytest tests/test_mcp_http.py tests/test_mcp_sessions.py tests/test_mcp_server.py tests/test_mcp_budget.py tests/test_mcp_citations.py tests/test_mcp_tools.py tests/test_mcp_reply_cap.py tests/test_public_pages.py -q
```

Then run `acceptance-tests.md` in each real client. A green smoke test proves the HTTP path, not the
client.

## Submitting

After any change to `server_instructions.md` or a manifest:

```bash
python integrations/mcp/build_packages.py --write --zip dist/genizahsearch-openai-plugin.zip --export dist/genizahsearch-ai-plugins
```

Copy `dist/genizahsearch-ai-plugins` into the distribution repository and push it there.

- **OpenAI (plugin):** upload `dist/genizahsearch-openai-plugin.zip`; a verified OpenAI platform account; the privacy, terms and support URLs above;
  the icon; the five positive and three negative test cases in `acceptance-tests.md`; a demo video;
  domain verification by setting `OPENAI_APPS_CHALLENGE` to the value OpenAI gives (served at
  `/.well-known/openai-apps-challenge`); after approval, publish explicitly.
- **Anthropic:** from the owner's personal Pro/Max account (a Team/Enterprise listing belongs to that
  organization), at `claude.ai/directory/manage`: the server as an MCP connector, then optionally the
  plugin bundle from the distribution repository with plugin path `claude-plugin`.
- **Gemini CLI gallery:** add the GitHub topic `gemini-cli-extension` to the distribution repository;
  the gallery's daily crawl lists it.
