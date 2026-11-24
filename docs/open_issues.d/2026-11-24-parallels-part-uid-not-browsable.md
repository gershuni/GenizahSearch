## P3 api: /api/parallels can return a part's uid (`part:<id>`), which /api/browse refuses
Where: `shared/search_engine.py::search_composition_logic` (grouping), `shared/search_serializer.py` (item `uid`)
Opened: 2026-11-24

docs/SEARCH_API.md says a result's `uid` is `IE{N}_P{M}_FL{K}` and is "safe to feed
verbatim into `/api/browse?uid=...`". When a codicological part matches only in its
continuous document (the match crosses a page break), `search_composition_logic` keeps
that continuous result ("use page results if available, otherwise fall back to
continuous"), and the serializer passes its uid through. Measured against the real index:
POST /api/parallels with `אדון עולם אשר מלך`, `chunk_size` 3, `mode` exact gave as its
first result uid `part:MS. Heb. e. 95/27`, locator `{sys_id, volume_ie: null, p_num: null}`;
GET /api/browse with that uid answered 400 `locator_conflict` ("uid is malformed"). A
client that follows the documented drill-down cannot open the result: the locator has no
page either. /api/search was not checked.

`tests/test_parallels_api.py::test_parallels_locator_round_trip_real` asserts the round
trip. It could not run before 2026-11-24 (it needed web.state loaded). It now calls
`pytest.xfail` naming this issue, only when a successful search reproduces exactly this 400
on a `part:`/`sys:` uid; anything else fails. Fix sketch: resolve a continuous result to the
page where its match starts (the grouping already knows the part's folios) and emit that
page's uid and locator, or emit `uid: null` with the part's first page in the locator; then
remove the xfail branch.
