## [Unreleased] -- Six quick fixes from the tracker (web + desktop, branch `ccr-6beb2d2c-dg78a3`)

Six open rows of `docs/OPEN_ISSUES.md`, picked as the most pressing of the
quick ones. Each has a regression test that fails on the old code.

### Fixed
- **Desktop discoveries list** no longer shows the author of an anonymous
  discovery. It printed the username the client fetched from `profiles`, and a
  new account's username is its email address. The detail dialogs already hid
  it.
- **Quitting the desktop app** waits at most 2 s for a metadata loader stuck in
  an NLI request (it waited without limit, so the process and its single-copy
  lock stayed alive and the app could not be opened again). No `terminate()`:
  the exit's own thread settling holds a loader still running.
- **AND, OR and NOT in a text** are searched as words. Passed into the Tantivy
  query bare, they were a parse error: Composition Search and Parallels (web
  and `POST /api/parallels`) dropped the chunk while reporting success, and an
  Exact search for them returned nothing. Hebrew text was unaffected; Latin
  headers, shelfmarks and apparatus were not.
- **`crash_log.txt`** is written to the data folder, next to
  `faulthandler_dump.txt`. An installed copy tried to write it under Program
  Files, which is read-only, so a crash with telemetry declined left no trace.
- **Web Fragment Puzzle:** Delete no longer says "Deleted" when the delete ran
  and removed nothing; it shows the failure message and the document stays.
- **Desktop Excel export** writes "N+" in the Result count row for a list the
  search cut off or Stop ended, as the website has since 2026-10-06. The "+"
  is now added in one place (`shared/export_dossier.py`), for both apps.
