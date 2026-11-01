## P3 desktop: "Search in these results" drops the Catalog tab's text, PGP and edition filters
Where: `genizah_app.py::_catalog_build_browse_filters`, `_apply_catalog_handoff_filters`
Opened: 2026-11-01

Found by Codex reviewing PR 1c (catalogue criteria, desktop); it predates that PR. The Catalog tab lists
manuscripts under domain/author/work, catalogue text (all/any/not), PGP and scholarly-edition filters,
library and the manuscript features, but the hand-off to Search / Composition carries only
domain/author/work, the Show-only library and the features. Example on `tests/criteria_sidecar.py`:
catalogue Material = Vellum plus the catalogue-text term `10205171` lists A alone; the hand-off searches
A and B (both Vellum). Fix sketch: carry `text_all` / `text_any` / `text_not` into `pre_search_filters`
(the main window already has chips for them and `FilterCountWorker` applies them); for PGP and editions,
which "Search only in..." has no control for, either intersect the browse list's own sys_ids into the
scope or say before the hand-off that those two are not carried.

## P3 desktop: Opening "Search only in..." loses a library restriction handed over from the Catalog tab
Where: `desktop/dialogs_filter.py::PreSearchFilterDialog._get_current_filter_dict`
Opened: 2026-11-01

Found by Codex reviewing PR 1c; it predates that PR. A Catalog hand-off in Show-only library mode puts
`library` into `pre_search_filters` (`genizah_app.py::_catalog_build_browse_filters`), but the dialog
rebuilds its filter dict from its own controls and has no library control, so its count, and the scope
OK installs, drop the library: Vellum + CUL hands over {A}, opening the dialog and pressing OK gives
{A, B}. Fix sketch: keep keys the dialog does not edit (`library`) in the dict it returns, and pass
`meta_mgr` to its FilterCountWorker so the count intersects them, as `_remove_filter` already does.
