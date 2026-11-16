## [Unreleased] -- Sign-in tokens no longer reach the analytics (web, branch `fix/posthog-scrub-auth-tokens`)

A confirmation, magic-link or password-reset link opens the site with the visitor's session in
the address (`#access_token=...&refresh_token=...`), and PostHog recorded that address with the
page's events. Every event now passes through `before_send` (`web/static/js/posthog_scrub.js`),
which replaces the value of each token parameter with `[redacted]` in the event, the person
properties and the session recording. Events recorded before the deploy still hold them.
Pinned by `tests/test_posthog_scrub.py`.
