## [Unreleased] -- The search index rebuilds beside the one in use (desktop, branch `claude/awesome-euclid-02adfd`)

Rebuilding the desktop's search index (Settings, or the offer an index of an earlier format
gets after startup) used to delete the index first and build in its place: for the twenty-odd
minutes of the build nothing could be searched, the main window showed nothing of it (the only
progress bar is in the Settings dialog), and a crash left no index at all.

- **Built beside, swapped at the end.** When the disk has room for a second index (about the
  current index's size plus 1.5 GB), `Indexer.create_index(beside=True)` builds into
  `tantivy_db.new` (its own seams and marker, the browse map into `browse_map.pkl.new`) while
  the open index keeps serving searches. At the end the app closes its index, renames the
  folders (`shared/index_swap.py::swap_in_staged_index`), reopens the new one, updates the
  Settings line and removes the old folder in the background (retried: on Windows the old
  memory-mapped files refuse deletion until released; what refuses goes at the next start).
  Without the room the build runs in place as before, and the confirmation says so, with the
  figures. A build interrupted by a crash or a shutdown is cleaned up at the next start before
  the index opens (`recover_index_dirs`: an interrupted swap is finished, a half-built
  `tantivy_db.new` discarded), so a user is never left without an index. `build_index.py
  --index-dir` keeps working unchanged.
- **Progress where the user looks.** A line at the top of the main window
  (`desktop/index_rebuild_bar.py`): "Rebuilding the search index: 37%, about 14 minutes left.
  Searching uses the current index meanwhile." The phases after the corpus lines (the
  whole-manuscript documents, the commit and merges, the seam index) now report progress too
  (`Indexer.create_index(phase_callback=)`), weighted into one bar and one estimate
  (`shared/index_build_progress.py`), where the Settings bar used to sit at the last line for
  minutes. For an in-place build the line says searching is unavailable until the build ends,
  and the Search and Analyze buttons are disabled with that sentence as their tooltip.
- **"Rebuild when I exit."** The offer's third answer, beside "Rebuild now" and "Later"; its
  text names what each does, in English and Hebrew. Recorded for that index like "Later"; the
  close path then hides the main window, runs the build in a small modal progress window and
  quits when it is done. Its Cancel keeps the index you have (the build runs beside it). A
  build already running when the program is closed goes to the same window instead of being
  thrown away. Cancel of a running build (`IndexerThread.request_cancel`,
  `Indexer.create_index(cancel=)`) removes what the build wrote and nothing else.
