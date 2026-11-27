## [Unreleased] -- ShapBot blocked; a daily budget for manuscript details (web, branch `claude/ai-traffic-limits`)

- **ShapBot is turned away like the SEO-tool crawlers.** It made 278,784 requests in the 14 days
  to 2026-11-27, about 28,855 of them `/browse` page builds in one day. It now gets 403 before
  any page is built, and a `robots.txt` Disallow; `robots.txt` itself stays reachable
  (`web/crawler_visibility.py`).
- **`/api/chatgpt/manuscript-details` has its own daily budget per address:** 2,000 requests in a
  trailing 24 hours, set by `MANUSCRIPT_DETAILS_DAILY_LIMIT`, separate from the search budget.
  - Calls made through the MCP server count too, since they reach the route as the visitor.
  - Over the budget, the route answers 429 `daily_limit_reached` with Retry-After, pointing to
    the terms and the support page.
  - One client had fetched 54,044 details in 14 days under the 30-a-minute limiter.
  - `DailyBudget` now takes its environment variable and message as arguments; the search
    budget is unchanged.
