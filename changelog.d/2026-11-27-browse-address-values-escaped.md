## [Unreleased] -- Browse escapes the values of its address (web, branch `fix/browse-url-escaping`)

`/browse` wrote the `sys_id` and `shelfmark` of its address into the page's head as they came:
into `og:url`, `twitter:url` and the canonical link, and into both JSON-LD blocks. The line
meant to escape `<` in the Manuscript block replaced it with itself. So a crafted link could
close an attribute or a block and add its own markup to the page.

Now these values reach the head only through the new `web/browse_head.py`:

- a `sys_id` is used in the tags only when all of it is a corpus system number; any other
  value gets the page's generic tags, as an address with no `sys_id` does;
- both JSON-LD blocks are written with `json.dumps`, and `<`, `>` and `&` become their JSON
  escapes, so no value can close a block;
- `page_meta` escapes the address it writes into attributes, for every page.

Also: the Manuscript block names the holding library when the address carries a shelfmark as
well as a `sys_id`. It was left out in that case.

Tests request the real page with inert markers in each value and read the served HTML
(`tests/render_smoke/test_browse_head_escaping.py`); each part of the fix was removed in turn
and a test failed for it.
