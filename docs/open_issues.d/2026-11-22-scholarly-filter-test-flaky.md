## P3 build-ci: The scholarly-filter panel test fails now and then on a busy machine
Where: `tests/test_scholarly_transcription_filter.py::test_the_filters_panel_keeps_the_scholarly_filter` (`_count_line_after`, 15 s)
Opened: 2026-11-22

The test waits up to 15 s for the results count line to change after Apply Filters, and fails with
"Apply Filters: the count line stayed '1 of 3 Results (Has scholarly transcription)'". On 2026-11-22
it failed in `scripts/run_local_tests.py` lane chunk 17 twice, and when run after
`tests/test_single_word_page_only.py` or `tests/test_discovery_composition.py`, on `master-main`
(85af603) as on a branch, and once on its own; it passed on other runs of the same commands. The same
two files took 7 s on one run and 52 s on another, so the wait is starved rather than the filter
broken (not shown). Look at what the earlier files leave running (search tasks, worker threads)
before raising the wait.
