## P3 desktop: A saved Catalog search shows the Exact marker in the history menu
Where: `genizah_app.py::GenizahGUI._MODE_CHARS`, `GenizahGUI._add_history_menu_item`
Opened: 2026-11-18

The Catalog mode is row 8 of the mode list (`desktop/catalog_search.py::COMBO_ROWS`), but
`_MODE_CHARS` still has eight markers, for rows 0-7. `_add_history_menu_item` shows `=` for a row
it has no marker for, so every saved Catalog search is listed with the Exact marker. Restoring it
still runs the catalogue search (the entry keeps its row and switches); only the marker is wrong.

Found by an automated review of the desktop catalogue PR (2026-11-18), confirmed by reading the
code, not seen in the app. Fix sketch: add `'%'` as the ninth marker, and a test that the marker
list has one entry per row of `COMBO_ROWS`.

## P3 desktop: A search-within step records the mode shown when the results arrive, not the one searched
Where: `genizah_app.py` (`_combo_step_mode(self.mode_combo.currentIndex())` where the refinement step is built; `_apply_run_params`)
Opened: 2026-11-18

When results arrive in a search-within chain, the new step takes its mode from the mode list as
it is then. The list can be changed while the search runs. `_apply_run_params` then copies the
run's own query, settings and catalogue switches from `_last_search_params`, but not its mode. A
Catalog search whose mode list was changed to Exact before it finished is recorded as an Exact
step carrying catalogue switches, so a replay or a restored chain runs a different search from
the one that gave the results. The same live read was already there for the other modes; the
catalogue switches make the mismatch visible.

Found by an automated review of the desktop catalogue PR (2026-11-18), confirmed by reading the
code (the mode list is not disabled during a search), not reproduced in the app. Fix sketch: in
`_apply_run_params`, set `step.mode` from `_last_search_params['mode_index']` (recorded at
dispatch) through `_combo_step_mode`, as the history entry already does.
