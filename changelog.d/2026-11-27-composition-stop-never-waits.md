## [Unreleased] -- Composition's Stop, New and Close no longer freeze the window (desktop, branch `claude/gs-14-composition`)

GS-14, second half. The Search tab stopped waiting in #147; the Composition tab, its
grouping, the Reading Desk and Browse still waited on the window, and then terminated a
worker that had not finished.

- Stop during grouping shows the ungrouped results at once; the grouping ends in the
  background. Before, the window froze until it ended, with no time limit.
- New clears the Composition tab at once, also during a letter-level search (which
  cannot be stopped once it has started: it now finishes in the background, and what it
  finds is dropped). Before, New refused, or waited up to three seconds and terminated
  the scan.
- Close, leaving the Reading Desk and the lists preview no longer wait for or terminate
  the Composition, grouping, Browse image and Reading Desk workers: each is held until
  its thread ends (`desktop/gui_threads.py::_keep_until_finished`). A close still waits
  for a letter-level search, also one New has dropped.
- At most one stopped worker may still be finishing when a Composition run or a grouping
  starts (from Analyze, a header click or a display-mode change); otherwise "Still
  stopping the previous search" asks to try again. The index install waits for them.
- A late progress update from a dropped run no longer moves the next run's bar.
- `desktop/search_run.py`: `SearchRun` takes the signals to cut and a tag, and
  `comp_run_of` is the Composition tab's holder. The 400 ms reset retry
  (`_reset_pending`, `_retry_pending_reset`, `_reset_is_pending`) is gone; `GenizahGUI`
  is 143 lines shorter.
- Tests: `tests/test_comp_run.py`; the reset tests in
  `tests/test_new_keeps_session_file.py`, `tests/test_desktop_passage_gate.py` and
  `tests/test_desktop_multi_witness.py` rewritten to the new behaviour.
