# AI listings launch: the `/ai` page, the homepage strip and `/chatgpt`

**Status:** plan, 2026-10-11. Owner decision: change the site only when each listing is available,
announce it more prominently then, and switch it on by configuration, not by a release.
**Parents:** [`MCP_SERVER_PLAN.md`](MCP_SERVER_PLAN.md), [`MCP_CHUNK_AND_VARIANT_LEVELS_PLAN.md`](MCP_CHUNK_AND_VARIANT_LEVELS_PLAN.md).

## 1. Where things stand

- Submitted 2026-10-11: the ChatGPT plugin (OpenAI review; publishing after approval is a separate,
  explicit step) and the Claude connector (Anthropic directory review). Review times are unpublished.
- The GPT keeps working until OpenAI retires GPTs on **2026-12-11**; after that its link is dead.
- Today the site advertises only the GPT, in three places:
  - `web/pages/ai_tools.py::create_ai_page` (`/ai`): the hero's "Open in ChatGPT" opens `/chatgpt`,
    "A ChatGPT account with access to GPTs is required", an "MCP · Planned" badge, and a link to the
    GPT privacy policy.
  - `create_ai_card` (the homepage strip, `web/pages/home.py`): "Explore the Genizah with AI" with
    "Open in ChatGPT".
  - `web/main.py` `GET /chatgpt`: a 307 to `GPT_URL`.

## 2. Design: one build, switched on per listing

The new page ships dark: with no listing configured it renders exactly as today. Each listing is
switched on independently by putting its public URL in production `.env` and restarting.

| Setting | When set |
|---|---|
| `AI_CHATGPT_PLUGIN_URL` | `/ai` and the homepage strip offer "Add to ChatGPT" (the plugin listing); `/chatgpt` redirects to the plugin, not the GPT; the GPT card and its "GPTs required" note go. |
| `AI_CLAUDE_CONNECTOR_URL` | `/ai` and the homepage strip offer "Add to Claude" (the connector listing). |
| `AI_LAUNCH_ANNOUNCE_UNTIL` (a date) | Until then, the homepage strip carries a "New" announcement naming what is live; afterwards it returns to its quiet form. |

The GPT is offered until the ChatGPT plugin replaces it, even beside a live Claude connector (if
Claude is listed first, ChatGPT readers keep their way in). From **2026-12-11**, whatever is set,
`/ai` never offers it and `/chatgpt` goes to the plugin listing if set, else to `/ai`.

With either listing live, `/ai` shows:

- A hero naming the live assistants, each with its own button.
- What it can do, in four lines: find manuscripts by phrase (exact, spelling variants, near
  spellings), find parallels to a passage or by word chunks, read a page with its citation,
  catalogue records and bibliography.
- "Other AI tools": any MCP client (the server URL `https://genizahsearch.com/mcp`, no sign-in), the
  Gemini CLI extension (`gemini extensions install https://github.com/gershuni/genizahsearch-ai-plugins`),
  the research skill and the public API (both as today).
- The privacy policy, terms and support links (`/ai/privacy`, `/terms`, `/support`) in place of the
  GPT privacy link.
- No "Planned" badge.

Each piece is a small function of the settings (`web/ai_listings.py`), which reads the environment
on every call (cheap), so a test can render every combination (none, ChatGPT only, Claude only,
both; before and after 2026-12-11) without a restart. A setting counts only when it is a
well-formed `https` link on its provider's host (chatgpt.com/openai.com; claude.ai/claude.com), not a
custom GPT (`/g/`), without user info: a typo or wrong link publishes nothing (Codex launch review).
If the real listing URL is refused, widen `LISTING_HOSTS`. Dates are the server's (UTC in
production); the announcement runs through its last day.

## 3. Launch steps (each listing, independently)

1. The listing goes live (OpenAI: approve, then publish; Anthropic: listed). Copy its public URL.
2. Add the setting to production `.env` (back it up first), set `AI_LAUNCH_ANNOUNCE_UNTIL` the first
   time, restart.
3. Check `/ai`, the homepage strip and `/chatgpt` on the live site; run `smoke_test.py`.
4. Announce outside the site: the owner's channels; `CHANGELOG.md` and the README's AI section in
   a small docs PR.
5. After both are live, edit the GPT's description to point to the plugin (owner, before 12-11).

## 4. Build and review

One PR, built now and deployed dark: `web/pages/ai_tools.py`, `web/main.py` (`/chatgpt`), the
settings in `docs/guides/ENV_VARS.md`, render tests for every combination, the translations (English
and Hebrew), and a browser check of each combination on the dev server (light/dark, phone width).
Codex reviews the PR. No change to the MCP tools (both reviews are in progress).

## 5. Owner decisions (2026-10-11)

1. **Announcement:** "New: GenizahSearch in ChatGPT and Claude" (naming what is live) on the homepage
   strip, highlighted for **four weeks** (`AI_LAUNCH_ANNOUNCE_UNTIL`). Also the header's What's New
   button: its first entry announces the live listings, replacing the GPT entry, and its unread
   key gains the live listings' names, so the dot lights again for readers who dismissed the 9.6.0
   announcement, and once more when the second listing goes live.
2. **GPT card:** replaced at once when the ChatGPT plugin goes live; `/chatgpt` follows.
3. **Other AI tools:** listed at launch: the bare MCP URL and the Gemini CLI command, beside the
   research skill and the API.
