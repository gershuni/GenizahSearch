## [Unreleased] -- Stop, New and Close no longer freeze the window or kill a search (desktop, branch `claude/gs-14-search-run`)

GS-14. Stop waited up to five seconds on the window's own thread for the
search to reach a point where it could stop (New three, Close two), then
`QThread.terminate()`d it, which can kill a thread inside the index searcher
or a database handle. Some steps of a search have no such point, so no wait
was long enough and no terminate was safe.

- `desktop/search_run.py` (new): Stop asks the search to stop and lets it
  finish in the background; what it found so far still shows as partial
  results. New, Close and a new search over a running one also stop listening
  to it, so nothing it delivers later reaches the window. Nothing waits, and
  nothing is terminated (owner ruling: "detach").
- A stopped search is held until it ends; at exit `desktop/qthread_exit.py`
  settles one still running. At most one stopped search may still be
  finishing when a new one starts: a third waits, with "Still stopping the
  previous search".
- The index install also waits for a stopped search still finishing.
- `_drain_previous_worker` now serves Composition only (its stop paths are
  unchanged).
- Tests: `tests/test_search_run.py`; `tests/test_pause_stop_lifecycle.py`
  follows the search's stop paths into the new module and pins that nothing
  in `genizah_app.py` waits on or terminates `search_thread`.
