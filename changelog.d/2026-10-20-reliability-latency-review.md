## [Unreleased] -- Reliability fixes and user-perceived latency (web, branch `claude/reliability-latency-review`)

A review of visitor isolation, the NiceGUI 3.18 upgrade and web latency
(2026-10-20). Every finding acted on was reproduced first and checked by a
second reviewer; each fix has a test that fails on the old code, run on NiceGUI
3.8.0 and 3.18.0. No search semantics, public API behaviour or data-access
permission changed.

### Fixed
- **English visitors saw Hebrew library names and image credits** on /browse
  (header, Library field, Quick View, print plate, "How to cite") and in the
  Joins Lab / Compare viewers. Since the UI language became per visitor, a
  worker thread had no language and took the process default.
  `web.translations.in_visitor_language` carries only the language into it.
- **A website Responsa downgrade notice reached another caller's
  /api/search response** (and MCP/ChatGPT) on the same wait thread, with a
  stale `responsa_options_effective`. Each call now clears the signal first.
  The old regression test could not fail; it now stages the signal on the
  right thread.
- **Fragment Puzzle "Add from list" said "No lists found" to every signed-in
  user**: it read the lists in a worker thread as the anonymous client. The
  client is now built on the loop, and the account is checked again on every
  pick in the open picker. A guard test flags any user-scoped Supabase helper
  offloaded without a client.
- **Web sign-up ignored email confirmation**, which production requires. After
  Register, every new visitor saw the English error "Email not confirmed"
  (someone re-registering an address saw "Invalid login credentials"), and the
  username, name and affiliation they typed were never saved. Register now
  answers "check your inbox" (the same for an address that already has an
  account, so the form does not reveal who is registered), and the two common
  sign-in errors are translated and say what to do next. The typed fields
  travel in the sign-up metadata and the database writes them when the account
  is created (below).
- **Hardening: a new account's username is what the person typed, or empty --
  never the email address** (`migrations/profiles_no_email_username.sql`, run
  in the Supabase SQL editor, with `scripts/verify_profiles_no_email_username.sql`
  before and after). Existing usernames that were email addresses are replaced
  the same way: the username typed at sign-up where the account's metadata
  has one, otherwise empty; each change is recorded so it can be undone.
  Pages that show an author fall back to their usual placeholder ("Unknown",
  "Anonymous") for an empty username instead of showing "None".
- **The "How to cite" chip stopped following the page after a network blip**:
  it was forgotten on every disconnect, and on NiceGUI 3.18 the page survives
  the reconnect. It is now forgotten when the client is deleted.
- **One stale `/_nicegui/<old version>/...` request stopped user-storage
  pruning for a minute** (a session-less 404 client made `prune_user_storage`
  raise). Such 404s are answered without a NiceGUI client.
- **Six async pages answered a 3 s stall with a reload loop (3.8) or a 500
  page (3.18)**, /auth/callback after signing the user in. They now wait up to
  20 s (`ASYNC_PAGE_RESPONSE_TIMEOUT`).
- **`/api/puzzle_process` and `/api/puzzle_export` ran image work on the event
  loop** (up to ~3 s and ~6 s per request). Both now run it in a thread;
  the export also gets the puzzle rate limit, a 20-fragment cap and a 400 for a
  malformed body.
- **The `browse_manuscript` event arrived for about one page view in ten**:
  sent through `ui.run_javascript` from a task with no page. `posthog_capture`
  takes the page's client; the password-recovery events go out inside the page.
- **A signed-in /browse page turn waited for 3-4 Supabase round trips** (the
  Recently Viewed write) before it showed, blocking every visitor meanwhile.
  It now runs after the page shows, in a thread, one write at a time per
  account across its tabs, and a write that waited is dropped if the browser
  has switched account.
- **Building a signed-in Supabase client re-read the CA bundle four times on
  the event loop** (1.56 s -> 0.17 s per build locally): httpx's default SSL
  context is built from the bundle read once.
- **A search started from the URL** (the homepage box) no longer waits a fixed
  0.5 s before it starts.
- **A test overwrote the developer's real `browse_map.pkl`**
  (`tests/test_browse_map_atomic_write.py` faked the dedupe before redirecting
  the path). The order is fixed and guarded.
- **Deploys installed whatever Starlette / python-socketio / engineio was
  newest**: `requirements.txt` now pins them to the lock CI tests, and
  `deploy.sh` installs with the lock as constraints, so transitive packages
  match CI too.

### Added
- **`ux_timing`** (PostHog): the user-perceived time of a Browse page turn or
  first page, a search's first visible rows, a Quick View, an add-to-list and a
  comment, from the click (or the navigation) to the next paint, split into
  server, transport and render time, with the page image apart. Low-cardinality
  properties only. `UX_TIMING_ENABLED` (default on) and
  `UX_TIMING_SAMPLE_RATE` (default 1.0); docs/guides/ENV_VARS.md.

### Changed
- **The public API's `exact` (and MCP, the ChatGPT app, and the web Joins
  Lab's single-line Exact) now matches whole words, as the website's Exact
  does** (owner decision 2026-10-21). A word inside a longer one is no longer
  matched and a phrase across a page break is found; on the real index the
  old `exact` returned up to 2.5 times the rows (all matches inside longer
  words) and ran about five times slower at the API's cap. Every response
  field still says `"exact"`; `/api/parallels` and the Joins Lab's
  Responsa-style search are unchanged. docs/SEARCH_API.md, Changelog.
