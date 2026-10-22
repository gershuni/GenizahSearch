## [Unreleased] -- Back to a Responsa search shows its results instead of searching again (web, branch `claude/responsa-back-restore`)

A finished Responsa search writes `q=..&mode=responsa` and its option flags
(`variants`, `ja`, `flex_spaces`, `bidirectional`) into the address bar. The
page recognised Back only for an address with no `mode` in it, so opening a
result and pressing Back ran the whole Responsa search again. In production on
2026-10-22 one 45 s search (2,456 candidates, 81 results) ran three times for
one visitor.

`web/search_bootstrap.py::resolve_search_bootstrap` now treats a Responsa
address as Back when its query and options match the run the saved results came
from (`last_run` in the same snapshot as the results); the page passes both in.
Any mismatch -- another query, another option, a saved run in another mode, no
saved run -- still searches, as a link should. Other modes are unchanged.

Tests: `tests/test_search_bootstrap.py` (the match and each way it must not
match, plus the `search.py` call site and the option names the saved run uses).
