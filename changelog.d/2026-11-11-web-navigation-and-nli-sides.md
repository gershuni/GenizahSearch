## [Unreleased] -- Faster moves between pages, library links that always open, the right side of the leaf (web, branch `claude/website-performance-debug-a6212c`)

Owner report, 2026-11-11: several seconds from one module to another; the NLI / CUDL
buttons sometimes did nothing; T-S 12.782 showed one side twice on NLI, and Quick View
opened on NLI where Browse opens on Cambridge.

- **Moving between pages starts at once.** The sidebar, the header's logo and Help button,
  and the homepage's cards and chips sent the click to the server, which then told the
  browser where to go: a round trip to Oregon (about half a second from Israel, PostHog
  `ux_timing`) before the next page even started, seconds while the server was busy, and
  nothing while the connection was being re-made. They are now links, or handlers that
  run in the browser (`web/client_nav.py`).
- **Library viewer buttons always open.** "Open in NLI Ktiv / Cambridge / Manchester /
  Princeton / Bodleian" opened their tab from the server after a round trip, late enough
  for a popup blocker to stop it. They are links now; NLI's opens its tab during the click
  and then sends it to the page's own image (`_nli_viewer_open_js`). The Joins dialog's
  Joins Lab buttons and the Download page's button are links too.
- **Switching NLI / Cambridge images is immediate.** A source chip reloaded the whole page
  and asked every enrichment source again (seconds, with a second full rebuild); it now
  only draws the image again.
- **NLI no longer shows the wrong side.** When NLI did not deliver a page's own image, the
  proxy answered with another page's (`_fetch_nli_image_bytes`'s fallback loop) and the
  browser and Cloudflare kept it for a week: T-S 12.782 showed its verso as 1r. A page
  with an image of its own is now answered with nothing (the viewer then asks NLI itself),
  and a stand-in (Rosetta, or any image for a page with no FL id) is `no-store`.
  Entries Cloudflare already holds expire within 7 days or with a purge.
- **Quick View opens on the image Browse opens on** (an aligned Cambridge image before
  NLI's), via the same `resolve_image_url` (`quick_view_image`), with that library's credit.
- **Quick View's image is no longer left blurry.** A 400-px preview already in the
  browser's cache finished loading before the viewer started listening for it, so the
  full-size image never replaced it (`progressiveLoad` now upgrades an image that has
  already loaded).
