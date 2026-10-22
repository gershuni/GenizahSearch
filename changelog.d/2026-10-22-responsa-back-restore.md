## [Unreleased] -- Back to a Responsa search shows its results instead of searching again (web, branch `claude/responsa-back-restore`)

A finished Responsa search writes `q=..&mode=responsa` and its option flags
(`variants`, `ja`, `flex_spaces`, `bidirectional`) into the address bar. The
page recognised Back only for an address with no `mode` in it, so opening a
result and pressing Back ran the whole Responsa search again. In production on
2026-10-22 one 45 s search (2,456 candidates, 81 results) ran three times for
one visitor.

A finished Responsa search now also writes a random `run=<stamp>` into the
address and keeps the same stamp in its saved run (`last_run['url_stamp']`).
`web/search_bootstrap.py::resolve_search_bootstrap` treats a Responsa address as
Back only when the stamp, the query and the options all match the saved run from
the same snapshot as the results; that Back restores the saved filters with the
rows. A typed or pasted link without the stamp, another run's stamp (another
tab, an older search), or any other mismatch still searches, as a link should --
equal values alone could hide a gap, NOT-words, filters or a refinement the
address does not carry (Codex, PR #436). Other modes are unchanged.

Tests: `tests/test_search_bootstrap.py` (the match and each way it must not
match, filters on the stamped Back, and the call sites: the route hands `run` to
the page, the page hands it and the four options to the bootstrap, and the URL
and the saved run get the same stamp).
