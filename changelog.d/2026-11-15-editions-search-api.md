## [Unreleased] -- Scholarly editions in the public API and MCP, two search workers at once (web, branch `claude/editions-web`)

Behind `EDITIONS_SEARCH_ENABLED` (off by default): nothing changes until it is turned on and the
editions index is installed. The website's pages come in the next change.

- **The public Search API and MCP read the scholarly editions** (FGP and PGP) where the switch is
  `on`: `POST /api/search` takes `sources` (`all` by default there, `editions`, `automatic`) and
  answers one result per page, each with `sources`, `edition` and `members` (every matching text of
  the page, with its own snippet, locator and credit). `GET /api/browse?version=fgp:<id>` reads one
  edition, with its credit; MCP's `search_manuscripts` and `browse_manuscript_page` take the same,
  and `how_to_cite` credits the edition. New codes: `editions_unavailable`, `edition_not_found`,
  `editions_skipped`, `page_not_identified` (`docs/SEARCH_API.md`, Changelog).
- **On the website a search with editions runs its two legs at once**, the main index in the
  research queue and the editions in a worker of their own (`GENIZAH_EDITIONS_WORKERS`, default 1),
  merged in the web process exactly as the in-process coordinator merges them; Stop and the time
  limit end both, and a failed editions worker leaves the automatic results with a note.
- **Result pages for ChatGPT and MCP** shorten, then omit, the members' snippets of one oversized
  page before anything else, so its members, credits and locators always fit.
- `scripts/verify_editions_search.py --web-timing` measures the website's wall-clock time through
  real research workers on the real indexes: with editions at most 20% above the automatic search,
  median and 90th percentile per mode; its `--self-test` runs the legs one after the other and must
  fail.
