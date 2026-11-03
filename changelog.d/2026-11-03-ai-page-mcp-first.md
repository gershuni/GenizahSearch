## [Unreleased] -- The /ai page leads with the MCP server; the GPT is retired gracefully (web, branch `claude/ai-page-mcp-b134e8`)

With `MCP_ENABLED` on, `/ai` opens with the MCP server (address, how to add it in Claude and in
any MCP client, example questions), then the skill and the public API, and shows the GPT card
last with a note that OpenAI retires custom GPTs on 2026-12-11. The GPT card disappears when
its setting is off (a ChatGPT plugin live, or after the retirement); `/chatgpt` keeps redirecting
until then. With `MCP_ENABLED` off the page is unchanged.
