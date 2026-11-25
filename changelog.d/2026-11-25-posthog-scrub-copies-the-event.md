## [Unreleased] -- Analytics: events after a page's first seconds reach PostHog again (web, branch `fix/posthog-scrub-copy`)

From v10.0.0 the website lost most of what a reader does after a page opens: clicks, page leaves,
web vitals, opened results, dictionary lookups, later page turns and most of the session recordings.
Page views, the API, MCP and the desktop app were not affected.

- `web/static/js/posthog_scrub.js` (PostHog's `before_send`, which keeps sign-in tokens out of the
  events) redacted each event in place. The session recorder's first `$snapshot` carries
  `posthog.config` itself, so its `token`, PostHog's project key, became `[redacted]` and every later
  event of the page was rejected. The scrub now returns a redacted copy and never writes to the event
  it was given; tokens are redacted as before.
- `tests/test_posthog_scrub.py`: the live config keeps its key, a frozen event is scrubbed and left
  as it was, and dates and URL objects are sent as JSON writes them.
- Web analytics from 2026-11-17 until this is deployed under-count these events; do not compare across
  that gap.
