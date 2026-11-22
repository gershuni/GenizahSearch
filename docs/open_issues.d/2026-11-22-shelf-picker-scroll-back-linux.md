## P3 desktop: On Linux the shelf picker does not scroll back to the chosen row after its rows are measured again
Where: `desktop/widgets/shelfmark_tree_picker.py::_NodeColumn.size_rows` / `_show_chosen`
Opened: 2026-11-22

The public CI's Ubuntu runner (gui-tests, release 11.0.0, runs 45051796232 and 45057645144) fails
`tests/test_shelf_picker_desktop.py::test_the_chosen_row_stays_in_sight_when_the_rows_are_measured_again`
every time, also with a 5 s wait: after the column narrows and the names wrap, the chosen row
(250 of 300) is at y 6,457 in a viewport 557 high, i.e. the list kept the scroll position it had
at the old row heights. It passes on Windows (locally, natively and offscreen, and on the CI's
Windows runner), where the desktop app ships, so no user sees it today. The test is marked
Windows-only until this is fixed.

Likely cause: `size_rows` calls `resizeRowsToContents()` and then posts `_show_chosen` with
`QTimer.singleShot(0)`. The table takes the new heights into its scroll range in its own
zero-interval timer (`QTableView::rowResized` -> `timerEvent` -> `updateGeometries`). Qt's
Windows event dispatcher runs zero timers as posted events, in posting order, so the range is
updated first; on Linux posted events run before timers, so `scrollToItem` runs against the old
range and nothing scrolls again.

Fix sketch: while `_keep_chosen`, call `_show_chosen` also on the vertical scroll bar's
`rangeChanged` (not while the column is being filled, when updates are off), then lift the skip.
Check on Linux first (a scratch venv with PyQt6 in WSL is enough for a small repro).
