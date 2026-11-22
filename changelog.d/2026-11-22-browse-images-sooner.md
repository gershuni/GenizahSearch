## [Unreleased] -- Manuscript images appear sooner on Browse (web, branch `claude/faster-image-loading-98bdea`)

### Changed
- **A low-quality preview first for every library, not only NLI.** Browse drew a 400-px preview
  only for images that come from the NLI; Cambridge, Manchester, JTS, Katz and Oxford pages showed
  nothing until their full image came (3-4 s from Cambridge, measured on the live site). Every
  library route that takes a width now has its preview, as Quick View already did
  (`web/components/image_resolution.py::preview_url`).
- **The sharp image is asked for together with the preview.** `progressiveLoad`
  (`web/static/manuscript_viewer.js`) asked for the full image only after the preview had arrived,
  so the two waits added up (about a second). Both now start together; a full image that arrives
  first replaces the preview at once, one that fails leaves the preview, a fallback to another
  library is never overridden, and an image that left the page is neither swapped nor kept
  downloading. With Save-Data or on a 2G connection the old order stays. The two requests for a
  manuscript nobody has opened no longer each download its NLI manifest: the second waits for the
  first, at most a manifest's own time limit (`web/api.py::_fl_fetch_turn`; only requests for the
  same manuscript wait).

### Fixed
- **A Cambridge page turn fetched the NLI image first and threw it away.** The first draw of a page
  (Phase A) had no library images, so it showed the NLI image; the enrichment (Phase B) then drew
  the Cambridge one: two or three image requests, and the image shown the slowest of them. Phase A
  now takes the manuscript's library images from memory when they are already there and current
  (`shared/cudl_identity.py::cached_external_entry`, which never enriches and never touches the
  network), with the credit that goes with them, and Phase B replaces them as a whole, so images it
  no longer finds are withdrawn. Both draws take the credit from one rule
  (`web/components/image_resolution.py::image_credits`).
- **Oxford pages waited about 30 s for "Image not available" and its Bodleian link.** The
  Bodleian's image host has been behind a bot-check page since September (GS-255), which no
  server fetch passes; `/api/oxford_image` waited its 30 s timeout first. It now waits at most a few
  seconds (`web/api.py::OXFORD_IMAGE_TIMEOUT`).

### Measuring
- The `ux_timing` event (v2) now also reports the image the reader ends up seeing, followed across
  the page's second draw: `img_final`, `img_full_ms` and `img_source`.
