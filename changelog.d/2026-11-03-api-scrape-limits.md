## [Unreleased] -- Public API: a daily budget per address and a concurrency budget for interactive searches (web, branch `claude/api-scrape-limits`)

After the 2026-11-03 PostHog review: one address pulled 8,036 requests through the API in six hours (7,459 `browse`, 573 `search`) at about 20 a minute, under the per-minute bucket almost throughout, and reader search p50 server time doubled that evening because the API's synchronous exact/title/shelfmark/responsa searches had no concurrency budget and stacked up in the research queue every visitor shares.

### Added
- **`SEARCH_API_DAILY_LIMIT`** (default 2,000; 0 disables): one trailing-24 h request budget per address across `/api/search`, `/api/browse` and `/api/parallels` (`web/api_hardening.py::DailyBudget`). Over it: `429 daily_limit_reached` with `Retry-After` set to the time until enough of the oldest requests leave the window for the bucket to fall under the current limit (the oldest one normally; a later one right after the limit was lowered below a bucket's size). Checked after the per-minute limiter and before validation, so a rate-refused request and the over-budget attempt itself spend nothing, while a request later refused as invalid (400) or busy (503) still counts. Capabilities and criteria are not counted.
- **`SEARCH_API_INTERACTIVE_CONCURRENCY`** (default 2): a semaphore budget for synchronous non-heavy `/api/search` modes, the same machinery as the heavy budget (`_InteractiveSemaphoreState`). Over it: `503 search_busy` at once, with `Retry-After`, instead of a 30 s wait for a 504. Background jobs bypass it, as they bypass the heavy budget.
- `/api/capabilities` `limits` now carries `interactive_concurrency` and `daily_requests_per_address`, from the same resolvers the handlers use.
- Error codes `search_busy` (503) and `daily_limit_reached` (429) in `shared/api_errors.py`.

### Not covered
- A pull spread over many addresses, each under both budgets (eight addresses ran ~350 `browse` calls each in one hour on 2026-10-28), is not stopped; `browse` is cheap and the data is public on the site. An API-key tier for bulk access is the owner's call.
