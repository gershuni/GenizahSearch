## [Unreleased] -- Search API: a parallel found across a page break is its page (web, branch `claude/parallels-part-uid`)

- `POST /api/parallels` gave a manuscript or codicological part found only in its text read
  whole (a match across a page break, for one) with a `part:` / `sys:` uid that
  `/api/browse` refused (400 `locator_conflict`), and a locator naming its first page. The
  result is now the page its match starts on: that page's uid and locator
  (`shared/search_engine.py::_note_composition_match_page`, `shared/search_serializer.py`).
  Where the page cannot be told, `uid` is null and the locator is the first page. The web
  page and the desktop are unchanged.
