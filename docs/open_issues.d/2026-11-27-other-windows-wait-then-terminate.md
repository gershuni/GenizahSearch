## P3 desktop: Other desktop windows still wait for a worker on the UI thread, then terminate it
Where: `desktop/join_workbench.py` (`_stop_search`, `do_search`, `closeEvent`), `desktop/puzzle.py::closeEvent`, `desktop/result_dialog.py` (`on_metadata_loaded`, `_teardown_workers`), `desktop/viewers.py::_preload`, `desktop/catalog_search.py::stop`
Opened: 2026-11-27

GS-14 removed every UI-thread wait and `terminate()` from the main window (`genizah_app.py`:
the Search tab in #147, Composition, grouping, Browse, the Reading Desk and the lists
preview in PR 5, `tests/test_pause_stop_lifecycle.py` now bans `.terminate()` there). The
same pattern is still in five other windows: `wait(2000-5000)` on the UI thread, then
`terminate()` if the worker has not finished. The Join Workbench's candidate search
(`_stop_search` waits 5 s, `do_search` 3 s) shares the main index searcher; a terminate
there can kill a thread inside the Tantivy searcher or an SQLite handle, as GS-14
described for the Search tab. Fix sketch, per window: ask the worker to stop and hold it
until its thread ends (`desktop/gui_threads.py::_keep_until_finished` or `_retire_worker`,
or a `desktop/search_run.py::SearchRun` where a run can be replaced), never wait, never
terminate; the exit settles what still runs (`desktop/qthread_exit.py`).
