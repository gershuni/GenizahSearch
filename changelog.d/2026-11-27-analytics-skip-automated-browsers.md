## [Unreleased] -- Analytics no longer count automated browsers (web, branch `claude/posthog-google-analytics-comparison-589ca7`)

In the 28 days to 2026-11-26, about three in four of Google Analytics' "active users"
(1,441 of 1,919 "Direct" users; 1,529 PostHog persons) were one automated visitor: desktop
Chrome on Linux from a data centre in The Dalles, Oregon, one page, zero seconds, never
returning. The real audience is about 700 people a month.

Both tags now check once per page whether the browser is automated (`navigator.webdriver`,
or a crawler user agent such as Googlebot, Google-Agent, HeadlessChrome or Lighthouse) and, if
so, Google Analytics is not loaded and `window.posthog` is cleared before PostHog starts, so every
`if (window.posthog)` caller skips cleanly. Pages are served to crawlers unchanged. New module
`web/analytics_bot_filter.py`; `ANALYTICS_SCRIPT` comes from it. A node test runs both tags' head
scripts in a mock page, as a person and as an automated browser.

The The Dalles visitor turned out to be an AI agent's cloud browser with an ordinary user agent:
this filter does not catch it, and it is counted apart in reports instead.
