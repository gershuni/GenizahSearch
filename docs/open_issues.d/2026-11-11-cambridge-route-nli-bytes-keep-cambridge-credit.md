## P3 web: An NLI image served by the Cambridge route keeps the Cambridge credit
Where: `web/api.py::_cambridge_image`, `web/pages/browse.py` (image credit footer), `web/pages/search_results.py::open_advanced_dialog`
Opened: 2026-11-11

`/api/cambridge_image/{sys_id}` answers with NLI's image, HTTP 200, when the manuscript has no
CUDL canvases or a page has no CUDL match (`X-Image-Fallback-Source: nli` on the response). No
image error happens, so `handleImageError` never runs and nothing switches the credit: Browse and
Quick View show NLI's image under the Cambridge credit. Found by Codex reviewing the 2026-11-11
Quick View change (round 2); Browse has always behaved this way. An `<img>` cannot read the
response headers. Fix sketch: load the viewer image with `fetch()` (reading
`X-Image-Fallback-Source`) into an object URL and call `switchImageCredit('nli')` on that header,
or let the route redirect to `/api/nli_image_by_sysid/...` instead of answering with NLI's bytes
under the Cambridge URL.
