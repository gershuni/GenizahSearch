## P3 web: "Search all transcriptions" reruns with the current "Search only in..." filters
Where: `web/pages/search.py::_search_all_sources`, `execute_search`
Opened: 2026-11-18

The button over the results of a scholarly-only or automatic-only search reruns that search in
every transcription. It puts back the shown run's query, mode, Responsa options and every control
`_show_run_controls` restores (gap, NOT words, text position, variant level, Num Changes, Lab
Mode), and passes its recorded variant settings. It does not put back the run's "Search only
in..." filters (`last_run['pre_search_filters']`): `execute_search` reads them from
`search_state` (`_dispatch_criteria = copy.deepcopy(search_state.filter_criteria)` and the other
`RUN_FILTER_FIELDS`), so a reader who changed those filters after the search gets a rerun under
the new ones. They stay visible as chips and on the button's badge, so the rerun is not silent,
but it is not the search the notice describes.

Fix sketch: `apply_run_filter_state(search_state, shown['pre_search_filters'])`, then refresh the
filter panel's controls, the criteria chips and the badge from `search_state`, as
`_on_history_item_clicked` does for the fields it restores (it covers domains, authors, works,
include mode, material, criteria and the text filters; the measurement fields would need the same).
Found during the Codex review of gershuni/GenizahSearch-dev#50.
