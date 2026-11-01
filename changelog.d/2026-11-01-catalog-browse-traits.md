## [Unreleased] -- List manuscripts by script and size in Catalog Browse (web + desktop, branch `script-filter`)

"Search only in..." could only narrow a text search: there was no way to list every manuscript with a
given script or size. Catalog Browse -- the page that lists manuscripts with no text -- now has a
**Script and size** card (web) / button (desktop): Script Style, Script Place, Width, Height and Lines
(min/max) and Material (measured). Example: Script Place = Yemenite and Lines >= 20 lists 85
manuscripts; Square script on vellum, 1,312.

- One definition of each trait: `shared/fjms_service.py` `_script_conditions` /
  `_measurement_conditions` now build both the "Search only in..." lookup (`get_filter_sys_ids`, its
  measurement SQL moved there unchanged) and the browse query (`get_browse_results`,
  `get_browse_library_facets`, new `traits` argument; `clean_traits`, `BROWSE_TRAIT_KEYS`). A test
  checks that browse lists exactly the set the search would be restricted to.
- Web `/catalog-browse`: chips, Clear All, the address (`script_style=`, `script_place=`, `width=10-20`,
  `height=`, `lines=20-`, `material=`) and the session restore carry the traits; "Search in these
  results" hands all of them to /search, "Parallel search in these results" only the script ones
  (/parallels has no size controls) and says so. Measurement controls appear only when the catalog is
  known to have the data; a saved value it lacks is dropped with a notice.
- Desktop Catalog tab: "Script and size..." opens `desktop/dialogs_filter.py::CatalogTraitsDialog`
  (its script lists shared with "Search only in..." via `build_script_lists`); chips, Clear All, both
  workers, the session and "Search/Parallel search in these results" carry the traits.
- Fixed on the way (web Catalog Browse): removing ANY filter chip left the removed filter in the address
  and the saved session -- the refresh's `ui.run_javascript` ran in the chip's own, just-deleted
  context and raised. It now goes through the page's client.
- Tests: `tests/test_catalog_traits.py`, `tests/test_catalog_traits_desktop.py`,
  `tests/render_smoke/test_catalog_browse_traits_render_smoke.py`.
