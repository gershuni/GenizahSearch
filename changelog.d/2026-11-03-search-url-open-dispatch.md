## [Unreleased] -- A link-opened search is pinned to start before the page connects (web, branch `claude/search-nav-latency-85bf15`)

### Tests
- `tests/render_smoke/test_search_url_open_dispatch.py`: on the real `/search` page in a NiceGUI
  User simulation whose socket handshake is withheld, a search opened from its address
  (`/search?q=...`) must reach the engine before the browser connects, and a typed
  search must reach it without a browser round trip. Pins the v9.11.1 fix (`bde98fb7`:
  the phone fold's awaited `window.innerWidth` held every link-opened search until the
  page had loaded and connected, +1.6 s of server time in production, ux_timing
  2026-10-24..29). Against that code both tests fail; measured on it with the handshake
  1 s late: 2.07 s link-opened / 1.08 s typed server time against 0.12 / 0.12 s now.
- The 2026-11-03 PostHog review's "search by URL 2.4x slower than typed" (30-day p50) is
  that pre-fix window: from the 10-30 deploy the `nav` server p50 is 0.99 s against
  1.02 s typed (48 and 125 events). No page change.
