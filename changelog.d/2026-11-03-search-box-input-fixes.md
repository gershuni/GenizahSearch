## [Unreleased] -- Shelfmarks and wrong-layout Hebrew in the search box (web, branch `claude/search-box-input-fixes`)

Two kinds of zero-result search the 2026-11-03 PostHog review found (readers with an
English browser hit zero results 53% of the time against 33% for Hebrew-browser readers;
every Latin-script search found nothing).

### Shelfmarks typed into the search box
- A shelfmark typed into the /search box (`ts 20.57`, `t-s ns 307.61b`, `add 3356`,
  `ts k2.27`, `t-s 10g4.5`, `tad c2.1`, `5559a.10`) ran as a text search and found nothing.
  In a text mode it now runs in the page's Shelfmark mode, as the `#` prefix would, with
  "Searched as a shelfmark." under the count and a "Search as text instead" link that runs
  the same query as text; a corpus system number opens that manuscript in Browse, as the
  homepage box does. The mode selector is left as it was, so the next query is text again;
  a Hebrew query, a prefixed one, and every other mode are untouched. The recogniser is the
  homepage box's (`web/home_query.py`: a corpus sys_id, a Hebrew or Arabic letter means
  text), narrowed for a box that also takes words: a shelfmark must have a digit, every run
  of four or more letters in it must be a word catalogue shelfmarks carry (`Moss`, `Misc`,
  `Gaster`; a Roman numeral; a library name in front is taken off first) and it must have
  the shape of one -- a dotted number or a letter-digit run, or a collection prefix (`T-S`,
  `ENA`, `Add.`), a library name or the NLI catalogue's own form first
  (`web/search_box_input.py::search_box_input_kind`), so `Psalm 23`, `version 1.2`,
  `COVID19` and a bare `12` stay text. New Search clears the line with everything else.

### Hebrew typed with the keyboard still on English
- "Did you mean <Hebrew>?" under "0 Results" for a Latin-script query that spells Hebrew on
  the Israeli standard layout (`dcuk` = גבול, `fufch anh` = כוכבי שמי, `bghkv` = נעילה;
  `shared/keyboard_layout.py`), offered only when the corpus holds those words
  (`SearchEngine.words_occur`: one count query in the server process, limit 1, off the
  loop -- never a research worker). The Hebrew is a link to the same search in the same
  mode; the click runs it on the page. PostHog `search_suggestion` (`kind='layout'`,
  `taken` false when shown, true when taken) measures uptake.

### Tests
- `tests/test_keyboard_layout.py`, `tests/test_search_box_input.py` (pure);
  `tests/render_smoke/test_search_box_input_render_smoke.py` pins the page: which mode
  reaches the engine for which input, the notice and its link, the suggestion, its link's
  address, the two captures, and that a Hebrew query runs as before.
