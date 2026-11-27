## [Unreleased] -- A large Search export no longer freezes the window (desktop, branch `claude/gs-9-export`)

GS-9, part 2 of 2. Part 1 made Export write every result the filters keep, not
only the 500 loaded; an export of 28,000 results then froze the window for
about a minute while each page's text was read from the index.

- `desktop/search_export.py` (new): above 2,000 rows the file is built on a
  worker, with a progress dialog and Cancel, and the window stays usable. The
  export reads a snapshot taken when it starts, so a new search or New
  meanwhile does not change the file; the index install waits for it. The
  save, its locked-file question and the result message stay on the window.
- The saved message says how many results were written.
- While the results table is sorted by Shelfmark, System ID, Library, Title
  or Domain, Export writes the whole list in that order, not the loaded rows
  sorted and the rest after them.
- The numbers down the side of the results table count the shown rows only:
  with a filter on, Qt's own numbers counted the hidden rows too, so 143
  matches ended at "638".
- `export_results` is split: `_search_export_job` (the snapshot),
  `genizah_app._build_search_export` (the bytes, for xlsx, csv, docx and txt)
  and `_finish_search_export` (the save). `GenizahGUI` is about 300 lines
  shorter.
- Tests: `tests/test_search_export.py`.
